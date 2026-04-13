import asyncio
import html
import json
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import aiohttp
import discord


EASTERN = ZoneInfo("America/New_York")
PACIFIC = ZoneInfo("America/Los_Angeles")

VIDEO_SEARCH_DELAY_MINUTES = 30  # Wait this long after a game goes final before searching YouTube
MAX_SEARCH_ATTEMPTS = 25         # Give up on a game after this many failed YouTube searches

logger = logging.getLogger("recap_bot")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(asctime)s] [RECAP_BOT] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class RecapBot:
    """MLB game recap bot - posts YouTube highlight videos when games finish."""

    MLB_YOUTUBE_CHANNEL_ID = "UCoLrcjPV5PbUrUyXq5mjc_A"

    FINAL_STATUSES = {"final", "game over", "completed early", "completed"}
    SKIP_STATUSES  = {"postponed", "suspended", "delayed start", "cancelled"}

    def __init__(
        self,
        discord_client: discord.Client,
        http_session: aiohttp.ClientSession,
        channel_id: int,
        state_path: Path,
        youtube_api_key: str,
    ):
        self.client = discord_client
        self.http_session = http_session
        self.channel_id = channel_id
        self.state_path = state_path
        self.youtube_api_key = youtube_api_key

        self.posted_game_ids: set[str] = set()
        self.checked_no_recap: dict[str, int] = {}   # game_pk -> attempt count
        self.game_final_times: dict[str, str] = {}   # game_pk -> ISO timestamp when first detected final
        self._quota_exhausted: bool = False

        self._load_state()
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop())
            logger.info("RECAP_BOT: Started")

    def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("RECAP_BOT: Stopped")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        logger.info("RECAP_BOT: Waiting for Discord client...")
        await self.client.wait_until_ready()
        logger.info("RECAP_BOT: Client ready, running startup poll for yesterday's games")
        try:
            await self._poll_completed_games(include_yesterday=True)
        except Exception as exc:
            logger.exception("RECAP_BOT: Error in startup poll: %s", exc)

        # Sleep before the first main-loop iteration so startup and the loop
        # don't fire back-to-back and double up API calls.
        await asyncio.sleep(random.uniform(300, 420))

        while not self.client.is_closed():
            try:
                if self._quota_exhausted:
                    secs = self._seconds_until_quota_reset()
                    logger.warning(
                        "RECAP_BOT: YouTube quota exhausted — sleeping %.0f seconds until midnight PT",
                        secs,
                    )
                    self._quota_exhausted = False
                    await asyncio.sleep(secs)
                    continue

                today = datetime.now(EASTERN).date()
                todays_games = await self._fetch_schedule(today)

                if not self._any_games_final(todays_games):
                    logger.info("RECAP_BOT: No games final yet today — checking again in 30 min")
                    await asyncio.sleep(1800)
                    continue

                # At least one game is final — run the poll (today only; yesterday handled at startup)
                await self._poll_completed_games()

                if self._is_day_complete(todays_games):
                    sleep_secs = await self._seconds_until_first_game(today + timedelta(days=1))
                    logger.info(
                        "RECAP_BOT: Day complete — sleeping %.0f seconds until tomorrow's first game",
                        sleep_secs,
                    )
                    await asyncio.sleep(sleep_secs)
                else:
                    sleep_secs = random.uniform(300, 420)
                    logger.info("RECAP_BOT: Next poll in %.0f seconds", sleep_secs)
                    await asyncio.sleep(sleep_secs)

            except Exception as exc:
                logger.exception("RECAP_BOT: Unhandled error in run loop: %s", exc)
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        logger.info("RECAP_BOT: State file path: %s", self.state_path)
        if not self.state_path.exists():
            logger.warning("RECAP_BOT: State file does not exist — starting fresh")
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self.posted_game_ids  = set(data.get("posted_game_ids", []))
            self.checked_no_recap = data.get("checked_no_recap", {})
            self.game_final_times = data.get("game_final_times", {})
            logger.info(
                "RECAP_BOT: Loaded state — %d posted, %d pending, %d final-time records",
                len(self.posted_game_ids),
                len(self.checked_no_recap),
                len(self.game_final_times),
            )
        except Exception as exc:
            logger.warning("RECAP_BOT: Could not load state: %s", exc)

    def _save_state(self) -> None:
        if len(self.posted_game_ids) > 1000:
            self._cleanup_old_state()
        data = {
            "posted_game_ids":  sorted(self.posted_game_ids),
            "checked_no_recap": self.checked_no_recap,
            "game_final_times": self.game_final_times,
            "saved_at": datetime.now(EASTERN).isoformat(),
        }
        self.state_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _cleanup_old_state(self) -> None:
        if len(self.posted_game_ids) <= 800:
            return
        sorted_ids = sorted(self.posted_game_ids, key=lambda x: int(x) if x.isdigit() else 0)
        self.posted_game_ids = set(sorted_ids[-800:])
        logger.info("RECAP_BOT: Trimmed state to 800 most recent game IDs")

    # ------------------------------------------------------------------
    # Schedule helpers
    # ------------------------------------------------------------------

    def _any_games_final(self, games: list[dict]) -> bool:
        return any(
            self._game_status(g) in self.FINAL_STATUSES
            for g in games
        )

    def _is_day_complete(self, games: list[dict]) -> bool:
        """True when every non-skipped game is final AND handled (posted or retries exhausted)."""
        for game in games:
            status = self._game_status(game)
            if status in self.SKIP_STATUSES:
                continue
            if status not in self.FINAL_STATUSES:
                return False  # Still in progress or scheduled
            game_pk = str(game.get("gamePk") or "")
            if not game_pk:
                continue
            if game_pk in self.posted_game_ids:
                continue
            if self.checked_no_recap.get(game_pk, 0) >= MAX_SEARCH_ATTEMPTS:
                continue
            return False  # Final but not yet handled
        return True

    async def _seconds_until_first_game(self, date) -> float:
        """Seconds from now until the first scheduled game on the given date."""
        games = await self._fetch_schedule(date)
        if not games:
            return 86400  # No games — sleep 24 hours

        earliest: Optional[datetime] = None
        for game in games:
            raw = game.get("gameDate") or ""
            if not raw:
                continue
            try:
                t = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if earliest is None or t < earliest:
                    earliest = t
            except Exception:
                continue

        if earliest is None:
            return 86400

        now = datetime.now(earliest.tzinfo)
        delta = (earliest - now).total_seconds() - 300  # arrive 5 min early
        return max(delta, 300)

    @staticmethod
    def _game_status(game: dict) -> str:
        return (((game.get("status") or {}).get("detailedState") or "").strip().lower())

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    async def _poll_completed_games(self, include_yesterday: bool = False) -> None:
        channel = self.client.get_channel(self.channel_id)
        if channel is None:
            logger.error("RECAP_BOT: Channel %s not found", self.channel_id)
            return
        if not isinstance(channel, discord.abc.Messageable):
            logger.error("RECAP_BOT: Channel %s is not messageable", self.channel_id)
            return

        today_et     = datetime.now(EASTERN).date()
        yesterday_et = today_et - timedelta(days=1)

        scan_dates = [yesterday_et, today_et] if include_yesterday else [today_et]

        finished_games = []
        for scan_date in scan_dates:
            games = await self._fetch_schedule(scan_date)
            for game in games:
                status = self._game_status(game)
                if status in self.SKIP_STATUSES:
                    continue
                if status in self.FINAL_STATUSES:
                    game_pk = str(game.get("gamePk") or "")
                    # Only record final time for today's games — yesterday's games
                    # are already old enough to bypass the 30-min delay
                    if game_pk and scan_date == today_et and game_pk not in self.game_final_times:
                        self.game_final_times[game_pk] = datetime.now(EASTERN).isoformat()
                        self._save_state()
                        logger.info("RECAP_BOT: Game %s went final, recorded timestamp", game_pk)
                    finished_games.append(game)

        label = "yesterday + today" if include_yesterday else "today"
        logger.info("RECAP_BOT: %d finished games across %s", len(finished_games), label)

        for game in finished_games:
            try:
                await self._process_game(channel, game)
            except Exception as exc:
                logger.exception(
                    "RECAP_BOT: Failed processing game %s: %s",
                    game.get("gamePk") or "unknown",
                    exc,
                )

    async def _fetch_json(self, url: str) -> dict[str, Any]:
        for attempt in range(3):
            try:
                async with self.http_session.get(url) as response:
                    response.raise_for_status()
                    return await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == 2:
                    raise
                logger.warning("RECAP_BOT: Fetch failed (attempt %d/3) %s: %s", attempt + 1, url, exc)
                await asyncio.sleep(2 ** attempt)
        return {}

    async def _fetch_schedule(self, date_obj) -> list[dict[str, Any]]:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_obj.isoformat()}"
        payload = await self._fetch_json(url)
        dates = payload.get("dates", [])
        if not dates:
            return []
        return dates[0].get("games", [])

    # ------------------------------------------------------------------
    # Per-game processing
    # ------------------------------------------------------------------

    async def _process_game(self, channel: discord.abc.Messageable, game: dict[str, Any]) -> None:
        game_pk = str(game.get("gamePk") or "")
        if not game_pk or game_pk in self.posted_game_ids:
            return
        if self.checked_no_recap.get(game_pk, 0) >= MAX_SEARCH_ATTEMPTS:
            return

        away = (((game.get("teams") or {}).get("away") or {}).get("team") or {}).get("name", "Away Team")
        home = (((game.get("teams") or {}).get("home") or {}).get("team") or {}).get("name", "Home Team")
        game_date_raw = game.get("gameDate") or ""

        # Enforce 30-minute wait after game goes final before searching YouTube
        final_time_str = self.game_final_times.get(game_pk)
        if final_time_str:
            final_time = datetime.fromisoformat(final_time_str)
            elapsed = (datetime.now(EASTERN) - final_time).total_seconds() / 60
            if elapsed < VIDEO_SEARCH_DELAY_MINUTES:
                remaining = VIDEO_SEARCH_DELAY_MINUTES - elapsed
                logger.info(
                    "RECAP_BOT: %s at %s — waiting %.0f more min before searching",
                    away, home, remaining,
                )
                return

        logger.info("RECAP_BOT: Searching for %s at %s (attempt %d)",
                    away, home, self.checked_no_recap.get(game_pk, 0) + 1)

        youtube_url = await self._search_youtube_highlights(away, home, game_date_raw)

        if not youtube_url:
            attempts = self.checked_no_recap.get(game_pk, 0) + 1
            self.checked_no_recap[game_pk] = attempts
            self._save_state()
            logger.info(
                "RECAP_BOT: No video yet for %s at %s — attempt %d/%d",
                away, home, attempts, MAX_SEARCH_ATTEMPTS,
            )
            return

        if game_pk in self.checked_no_recap:
            del self.checked_no_recap[game_pk]

        await channel.send(content=youtube_url)

        logger.info("RECAP_BOT: ✓ Posted %s at %s | %s", away, home, youtube_url)
        self.posted_game_ids.add(game_pk)
        self._save_state()

        # Random delay between posts so the channel doesn't get flooded
        await asyncio.sleep(random.uniform(15, 45))

    # ------------------------------------------------------------------
    # YouTube search
    # ------------------------------------------------------------------

    async def _search_youtube_highlights(
        self,
        away_team: str,
        home_team: str,
        game_date: str,
    ) -> Optional[str]:
        if not self.youtube_api_key:
            logger.warning("RECAP_BOT_YT: No YOUTUBE_API_KEY set")
            return None

        away_short = self._shorten_team_name(away_team)
        home_short = self._shorten_team_name(home_team)

        try:
            date_obj    = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
            # Convert to Eastern so late west-coast games (e.g. 10pm ET = next day UTC)
            # use the correct local date in the search query
            date_et     = date_obj.astimezone(EASTERN)
            date_str    = date_et.strftime("%B %d, %Y")
            # publishedAfter = midnight ET on game day, converted to UTC RFC 3339
            midnight_et      = date_et.replace(hour=0, minute=0, second=0, microsecond=0)
            published_after  = midnight_et.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            date_str        = datetime.now(EASTERN).strftime("%B %d, %Y")
            published_after = None

        query = f"{away_short} vs. {home_short} Highlights {date_str}"
        logger.info("RECAP_BOT_YT: Searching for: %s (publishedAfter: %s)", query, published_after)

        params: dict[str, Any] = {
            "part": "snippet",
            "channelId": self.MLB_YOUTUBE_CHANNEL_ID,
            "q": query,
            "type": "video",
            "order": "date",
            "maxResults": 5,
            "key": self.youtube_api_key,
        }
        if published_after:
            params["publishedAfter"] = published_after

        try:
            async with self.http_session.get(
                "https://www.googleapis.com/youtube/v3/search", params=params
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    logger.warning("RECAP_BOT_YT: API error %s — %s", response.status, " ".join(body.split()))
                    return None

                data = await response.json(content_type=None)
                items = data.get("items", [])
                if not items:
                    logger.info("RECAP_BOT_YT: No results for: %s", query)
                    return None

                # Check each result until we find one whose title matches both teams
                for item in items:
                    video_id  = item["id"]["videoId"]
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    title     = html.unescape(item["snippet"]["title"])
                    title_lower = title.lower()

                    if away_short.lower() in title_lower and home_short.lower() in title_lower:
                        logger.info("RECAP_BOT_YT: ✓ Found — %s | %s", title, video_url)
                        return video_url

                    logger.warning(
                        "RECAP_BOT_YT: Skipping — expected '%s vs. %s', got: %s",
                        away_short, home_short, title,
                    )

                logger.info("RECAP_BOT_YT: No matching title in %d results for: %s", len(items), query)
                return None

        except Exception as exc:
            logger.exception("RECAP_BOT_YT: Unexpected error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Team name helpers
    # ------------------------------------------------------------------

    def _shorten_team_name(self, full_name: str) -> str:
        shortenings = {
            "Arizona Diamondbacks": "D-backs",
            "Atlanta Braves": "Braves",
            "Baltimore Orioles": "Orioles",
            "Boston Red Sox": "Red Sox",
            "Chicago Cubs": "Cubs",
            "Chicago White Sox": "White Sox",
            "Cincinnati Reds": "Reds",
            "Cleveland Guardians": "Guardians",
            "Colorado Rockies": "Rockies",
            "Detroit Tigers": "Tigers",
            "Houston Astros": "Astros",
            "Kansas City Royals": "Royals",
            "Los Angeles Angels": "Angels",
            "Los Angeles Dodgers": "Dodgers",
            "Miami Marlins": "Marlins",
            "Milwaukee Brewers": "Brewers",
            "Minnesota Twins": "Twins",
            "New York Mets": "Mets",
            "New York Yankees": "Yankees",
            "Athletics": "A's",
            "Philadelphia Phillies": "Phillies",
            "Pittsburgh Pirates": "Pirates",
            "San Diego Padres": "Padres",
            "San Francisco Giants": "Giants",
            "Seattle Mariners": "Mariners",
            "St. Louis Cardinals": "Cardinals",
            "Tampa Bay Rays": "Rays",
            "Texas Rangers": "Rangers",
            "Toronto Blue Jays": "Blue Jays",
            "Washington Nationals": "Nationals",
        }
        return shortenings.get(full_name, full_name)


# ============================================================================
# Entry point for main.py
# ============================================================================

async def start_recap_bot() -> None:
    """Entry point called by main.py's run_forever wrapper."""
    import os

    logger.info("RECAP_BOT: === STARTING UP ===")

    HIGHLIGHTS_BOT_TOKEN = os.getenv("HIGHLIGHTS_BOT_TOKEN", "")
    RECAP_CHANNEL_ID     = int(os.getenv("RECAP_CHANNEL_ID", "0"))
    YOUTUBE_API_KEY      = os.getenv("YOUTUBE_API_KEY", "")

    logger.info("RECAP_BOT: Token present:          %s", "Yes" if HIGHLIGHTS_BOT_TOKEN else "NO - MISSING!")
    logger.info("RECAP_BOT: Channel ID:             %s", RECAP_CHANNEL_ID)
    logger.info("RECAP_BOT: YouTube API key present: %s", "Yes" if YOUTUBE_API_KEY else "NO - MISSING!")

    if not HIGHLIGHTS_BOT_TOKEN:
        logger.error("RECAP_BOT: FATAL - Missing HIGHLIGHTS_BOT_TOKEN")
        raise SystemExit("Missing HIGHLIGHTS_BOT_TOKEN environment variable")
    if RECAP_CHANNEL_ID <= 0:
        logger.warning("RECAP_BOT: RECAP_CHANNEL_ID not set — bot will not run")
        await asyncio.sleep(float("inf"))
        return

    state_dir = Path("/opt/render/project/.data")
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "recap_bot_state.json"

    intents = discord.Intents.default()
    client  = discord.Client(intents=intents)

    http_session: Optional[aiohttp.ClientSession] = None
    recap_bot_instance: Optional[RecapBot] = None

    @client.event
    async def on_ready() -> None:
        nonlocal http_session, recap_bot_instance
        logger.info("RECAP_BOT: Logged in as %s (%s)", client.user, client.user.id if client.user else "n/a")

        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

        recap_bot_instance = RecapBot(
            discord_client=client,
            http_session=http_session,
            channel_id=RECAP_CHANNEL_ID,
            state_path=state_path,
            youtube_api_key=YOUTUBE_API_KEY,
        )
        recap_bot_instance.start()

    try:
        logger.info("RECAP_BOT: Connecting to Discord...")
        await client.start(HIGHLIGHTS_BOT_TOKEN)
    except Exception as e:
        logger.exception("RECAP_BOT: FATAL - Failed to start Discord client: %s", e)
        raise
    finally:
        if recap_bot_instance:
            recap_bot_instance.stop()
        if http_session and not http_session.closed:
            await http_session.close()
        if not client.is_closed():
            await client.close()

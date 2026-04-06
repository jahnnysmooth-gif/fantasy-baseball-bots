import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import aiohttp
import discord


EASTERN = ZoneInfo("America/New_York")

TEAM_COLORS = {
    "Arizona Diamondbacks": 0xA71930,
    "Atlanta Braves": 0xCE1141,
    "Baltimore Orioles": 0xDF4601,
    "Boston Red Sox": 0xBD3039,
    "Chicago Cubs": 0x0E3386,
    "Chicago White Sox": 0x27251F,
    "Cincinnati Reds": 0xC6011F,
    "Cleveland Guardians": 0x0C2340,
    "Colorado Rockies": 0x33006F,
    "Detroit Tigers": 0x0C2340,
    "Houston Astros": 0xEB6E1F,
    "Kansas City Royals": 0x004687,
    "Los Angeles Angels": 0xBA0021,
    "Los Angeles Dodgers": 0x005A9C,
    "Miami Marlins": 0x00A3E0,
    "Milwaukee Brewers": 0x12284B,
    "Minnesota Twins": 0x002B5C,
    "New York Mets": 0x002D72,
    "New York Yankees": 0x0C2340,
    "Athletics": 0x003831,
    "Philadelphia Phillies": 0xE81828,
    "Pittsburgh Pirates": 0xFDB827,
    "San Diego Padres": 0x2F241D,
    "San Francisco Giants": 0xFD5A1E,
    "Seattle Mariners": 0x005C5C,
    "St. Louis Cardinals": 0xC41E3A,
    "Tampa Bay Rays": 0x092C5C,
    "Texas Rangers": 0x003278,
    "Toronto Blue Jays": 0x134A8E,
    "Washington Nationals": 0xAB0003,
}
DEFAULT_EMBED_COLOR = 0x1D428A

logger = logging.getLogger("recap_bot")


class RecapBot:
    """MLB game recap bot - posts YouTube highlight videos when games finish."""
    
    def __init__(
        self,
        discord_client: discord.Client,
        http_session: aiohttp.ClientSession,
        channel_id: int,
        state_path: Path,
        poll_seconds: int = 300,
    ):
        self.client = discord_client
        self.http_session = http_session
        self.channel_id = channel_id
        self.state_path = state_path
        self.poll_seconds = poll_seconds
        
        self.posted_game_ids: set[str] = set()
        self.checked_no_recap: dict[str, int] = {}  # game_pk -> attempt_count
        
        self._load_state()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """Start the recap bot polling loop."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop())
            logger.info("Recap bot started, polling every %s seconds", self.poll_seconds)

    def stop(self) -> None:
        """Stop the recap bot polling loop."""
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("Recap bot stopped")

    async def _run_loop(self) -> None:
        """Main polling loop."""
        await self.client.wait_until_ready()
        
        # Run immediately on startup (don't wait for first interval)
        logger.info("Running initial poll immediately...")
        try:
            await self._poll_completed_games()
        except Exception as exc:
            logger.exception("Error in initial recap bot poll: %s", exc)
        
        while not self.client.is_closed():
            try:
                await asyncio.sleep(self.poll_seconds)
                await self._poll_completed_games()
            except Exception as exc:
                logger.exception("Error in recap bot poll loop: %s", exc)

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self.posted_game_ids = set(data.get("posted_game_ids", []))
            self.checked_no_recap = data.get("checked_no_recap", {})
            logger.info(
                "Loaded state: %s game ids, %s pending recaps",
                len(self.posted_game_ids),
                len(self.checked_no_recap),
            )
        except Exception as exc:
            logger.warning("Could not load state file %s: %s", self.state_path, exc)

    def _save_state(self) -> None:
        # Cleanup old entries if state gets too large (keep ~10 days worth)
        if len(self.posted_game_ids) > 1000:
            self._cleanup_old_state()
        
        data = {
            "posted_game_ids": sorted(self.posted_game_ids),
            "checked_no_recap": self.checked_no_recap,
            "saved_at": datetime.now(EASTERN).isoformat(),
        }
        self.state_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _cleanup_old_state(self) -> None:
        """Keep only the most recent 800 game IDs to prevent unbounded growth."""
        if len(self.posted_game_ids) <= 800:
            return
        
        sorted_ids = sorted(self.posted_game_ids)
        keep_count = 800
        self.posted_game_ids = set(sorted_ids[-keep_count:])
        logger.info("Cleaned up state: kept %s most recent game IDs", keep_count)

    async def _poll_completed_games(self) -> None:
        channel = self.client.get_channel(self.channel_id)
        if channel is None:
            logger.error("Channel %s not found. Check RECAP_CHANNEL_ID.", self.channel_id)
            return
        if not isinstance(channel, discord.abc.Messageable):
            logger.error("Channel %s is not messageable.", self.channel_id)
            return

        today_et = datetime.now(EASTERN).date()
        dates_to_scan = [today_et]  # Only today for testing

        logger.info("Scanning MLB schedule for %s", ", ".join(str(d) for d in dates_to_scan))

        all_finished_games = []
        for scan_date in dates_to_scan:
            games = await self._fetch_schedule(scan_date)
            for game in games:
                status = (((game.get("status") or {}).get("detailedState") or "").strip().lower())
                skip_statuses = {"postponed", "suspended", "delayed start", "cancelled"}
                if status in skip_statuses:
                    continue
                if status in {"final", "game over", "completed early", "completed"}:
                    all_finished_games.append(game)
        
        # For testing: only process the last 3 finished games
        games_to_post = all_finished_games[-3:] if len(all_finished_games) > 3 else all_finished_games
        logger.info("Found %d finished games, posting last %d for testing", 
                    len(all_finished_games), len(games_to_post))
        
        for game in games_to_post:
            try:
                await self._process_game(channel, game)
            except Exception as exc:
                logger.exception("Failed processing game %s: %s", game.get("gamePk") or "unknown", exc)

    async def _fetch_json(self, url: str) -> dict[str, Any]:
        for attempt in range(3):
            try:
                async with self.http_session.get(url) as response:
                    response.raise_for_status()
                    return await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == 2:
                    raise
                logger.warning("Fetch failed (attempt %d/3) for %s: %s", attempt + 1, url, exc)
                await asyncio.sleep(2 ** attempt)
        return {}

    async def _fetch_schedule(self, date_obj) -> list[dict[str, Any]]:
        url = (
            "https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&date={date_obj.isoformat()}"
        )
        payload = await self._fetch_json(url)
        dates = payload.get("dates", [])
        if not dates:
            return []
        return dates[0].get("games", [])

    async def _process_game(self, channel: discord.abc.Messageable, game: dict[str, Any]) -> None:
        game_pk = str(game.get("gamePk") or "")
        if not game_pk or game_pk in self.posted_game_ids:
            return

        away_team = (((game.get("teams") or {}).get("away") or {}).get("team") or {})
        home_team = (((game.get("teams") or {}).get("home") or {}).get("team") or {})
        away = away_team.get("name", "Away Team")
        home = home_team.get("name", "Home Team")
        away_id = away_team.get("id")
        home_id = home_team.get("id")
        away_score = int((((game.get("teams") or {}).get("away") or {}).get("score") or 0))
        home_score = int((((game.get("teams") or {}).get("home") or {}).get("score") or 0))
        game_date_raw = game.get("gameDate") or ""
        game_number = game.get("gameNumber", 1)

        # Search YouTube for highlights
        youtube_url = await self._search_youtube_highlights(away, home, game_date_raw)
        
        if not youtube_url:
            # Track attempts to find video - retry up to 5 times before giving up
            attempts = self.checked_no_recap.get(game_pk, 0)
            if attempts < 5:
                self.checked_no_recap[game_pk] = attempts + 1
                self._save_state()
                logger.info(
                    "No YouTube video found yet for %s at %s (%s) - attempt %d/5",
                    away,
                    home,
                    game_pk,
                    attempts + 1,
                )
            return

        # Video found - remove from retry tracking if present
        if game_pk in self.checked_no_recap:
            del self.checked_no_recap[game_pk]

        winner_name = self._winner_name(away, home, away_score, home_score)
        winner_id = away_id if away_score > home_score else home_id
        
        embed = self._build_recap_embed(
            away=away,
            home=home,
            away_score=away_score,
            home_score=home_score,
            winner_name=winner_name,
            winner_id=winner_id,
            game_date_raw=game_date_raw,
            game_number=game_number,
            video_url=youtube_url,
        )

        # Post YouTube URL - Discord will auto-embed the video
        # Try posting JUST the URL first to see if that embeds
        await channel.send(content=youtube_url)

        logger.info("RECAP_BOT_YT: Posted highlights for %s at %s | %s", away, home, youtube_url)
        self.posted_game_ids.add(game_pk)
        self._save_state()
        
        # Throttle to avoid blasting channel when many games finish simultaneously
        await asyncio.sleep(2)

    async def _search_youtube_highlights(
        self, 
        away_team: str, 
        home_team: str, 
        game_date: str
    ) -> Optional[str]:
        """Search YouTube for the game's highlights video."""
        
        # Clean team names for search
        away_clean = away_team.replace("Los Angeles ", "").replace("New York ", "").replace("Chicago ", "").replace("San Francisco ", "")
        home_clean = home_team.replace("Los Angeles ", "").replace("New York ", "").replace("Chicago ", "").replace("San Francisco ", "")
        
        # Format date as "April 5 2026"
        try:
            date_obj = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
            # Format as "April 5 2026" (remove leading zero from day)
            month = date_obj.strftime("%B")
            day = str(date_obj.day)  # No leading zero
            year = date_obj.strftime("%Y")
            date_str = f"{month} {day} {year}"
        except:
            date_str = ""
        
        # Search query: "Dodgers vs Yankees Highlights April 5 2026"
        query = f"{away_clean} vs {home_clean} Highlights {date_str}"
        logger.info("RECAP_BOT_YT: === YOUTUBE SEARCH ===")
        logger.info("RECAP_BOT_YT: Away team: %s -> %s", away_team, away_clean)
        logger.info("RECAP_BOT_YT: Home team: %s -> %s", home_team, home_clean)
        logger.info("RECAP_BOT_YT: Search query: %s", query)
        
        # YouTube search via scraping (no API key needed)
        search_url = f"https://www.youtube.com/results?search_query={quote_plus(query)}"
        logger.info("RECAP_BOT_YT: Search URL: %s", search_url)
        
        try:
            async with self.http_session.get(search_url) as response:
                logger.info("RECAP_BOT_YT: YouTube response status: %s", response.status)
                html = await response.text()
                logger.info("RECAP_BOT_YT: YouTube HTML length: %d bytes", len(html))
                
                # Extract video ID from search results
                # Look for "videoId":"XXXXXXXXXXX" pattern
                video_ids = re.findall(r'"videoId":"([^"]{11})"', html)
                logger.info("RECAP_BOT_YT: Found %d video IDs in HTML", len(video_ids))
                
                if video_ids:
                    video_id = video_ids[0]
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    logger.info("RECAP_BOT_YT: ✓ SUCCESS - Found video: %s", video_url)
                    return video_url
                else:
                    logger.warning("RECAP_BOT_YT: ✗ FAILED - No videos found for: %s", query)
                    return None
                    
        except Exception as e:
            logger.exception("RECAP_BOT_YT: ✗ ERROR searching YouTube: %s", e)
            return None

    def _build_recap_embed(
        self,
        *,
        away: str,
        home: str,
        away_score: int,
        home_score: int,
        winner_name: str,
        winner_id: Optional[int],
        game_date_raw: str,
        game_number: int,
        video_url: str,
    ) -> discord.Embed:
        display_date = self._format_game_date(game_date_raw)
        color = TEAM_COLORS.get(winner_name, DEFAULT_EMBED_COLOR)
        
        # Handle doubleheader games
        title = f"{away} at {home} Highlights"
        if game_number > 1:
            title += f" (Game {game_number})"
        
        description = f"**{away} {away_score}, {home} {home_score}** — {winner_name} won on {display_date}."

        embed = discord.Embed(
            title=title,
            url=video_url,
            description=description,
            color=color,
            timestamp=datetime.now(EASTERN),
        )
        embed.add_field(name="Final", value=f"{away} {away_score} • {home} {home_score}", inline=False)
        
        # Add winner team logo if available (ESPN CDN)
        if winner_id:
            logo_url = f"https://a.espncdn.com/i/teamlogos/mlb/500/{winner_id}.png"
            embed.set_thumbnail(url=logo_url)
        
        embed.set_footer(text=f"MLB Highlights • {display_date}")
        return embed

    def _winner_name(self, away: str, home: str, away_score: int, home_score: int) -> str:
        if away_score > home_score:
            return away
        if home_score > away_score:
            return home
        return home

    def _format_game_date(self, game_date_raw: str) -> str:
        if not game_date_raw:
            return datetime.now(EASTERN).strftime("%b %d, %Y")
        try:
            parsed = datetime.fromisoformat(game_date_raw.replace("Z", "+00:00"))
            return parsed.astimezone(EASTERN).strftime("%b %d, %Y")
        except ValueError:
            return game_date_raw[:10]


# ============================================================================
# Entry point for main.py
# ============================================================================

async def start_recap_bot() -> None:
    """Entry point called by main.py's run_forever wrapper."""
    import os
    
    logger.info("Recap bot starting up...")
    
    HIGHLIGHTS_BOT_TOKEN = os.getenv("HIGHLIGHTS_BOT_TOKEN", "")
    RECAP_CHANNEL_ID = int(os.getenv("RECAP_CHANNEL_ID", "0"))
    RECAP_BOT_POLL_SECONDS = int(os.getenv("RECAP_BOT_POLL_SECONDS", "300"))
    
    logger.info("Token present: %s", "Yes" if HIGHLIGHTS_BOT_TOKEN else "NO - MISSING!")
    logger.info("Channel ID: %s", RECAP_CHANNEL_ID)
    logger.info("Poll interval: %s seconds", RECAP_BOT_POLL_SECONDS)
    
    if not HIGHLIGHTS_BOT_TOKEN:
        logger.error("FATAL: Missing HIGHLIGHTS_BOT_TOKEN environment variable")
        raise SystemExit("Missing HIGHLIGHTS_BOT_TOKEN environment variable")
    if RECAP_CHANNEL_ID <= 0:
        logger.warning("RECAP_CHANNEL_ID not set - recap bot will not run")
        # Sleep forever to keep the task alive but inactive
        await asyncio.sleep(float('inf'))
        return
    
    # State directory (Render persistent disk)
    state_dir = Path("/opt/render/project/.data")
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "recap_bot_state.json"
    
    intents = discord.Intents.default()
    client = discord.Client(intents=intents)
    
    http_session: Optional[aiohttp.ClientSession] = None
    recap_bot_instance: Optional[RecapBot] = None
    
    @client.event
    async def on_ready() -> None:
        nonlocal http_session, recap_bot_instance
        logger.info("Recap bot logged in as %s (%s)", client.user, client.user.id if client.user else "n/a")
        
        # Create shared HTTP session
        timeout = aiohttp.ClientTimeout(total=30)
        http_session = aiohttp.ClientSession(timeout=timeout)
        
        # Initialize and start the recap bot
        recap_bot_instance = RecapBot(
            discord_client=client,
            http_session=http_session,
            channel_id=RECAP_CHANNEL_ID,
            state_path=state_path,
            poll_seconds=RECAP_BOT_POLL_SECONDS,
        )
        recap_bot_instance.start()
        logger.info("Recap bot started - polling every %s seconds", RECAP_BOT_POLL_SECONDS)
    
    try:
        logger.info("Attempting to connect to Discord...")
        await client.start(HIGHLIGHTS_BOT_TOKEN)
    except Exception as e:
        logger.exception("Failed to start Discord client: %s", e)
        raise
    finally:
        # Cleanup
        if recap_bot_instance:
            recap_bot_instance.stop()
        if http_session and not http_session.closed:
            await http_session.close()
        if not client.is_closed():
            await client.close()

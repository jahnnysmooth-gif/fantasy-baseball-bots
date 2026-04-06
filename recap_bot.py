import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import aiohttp
import discord
from discord.ext import tasks


EASTERN = ZoneInfo("America/New_York")

# Search priority for MLB video packages.
RECAP_KEYWORDS = [
    "recap",
    "game recap",
    "daily recap",
    "game highlights recap",
]
REJECT_KEYWORDS = [
    "condensed game",
    "extended highlights",
    "top plays",
    "must-c",
]

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
    "Athletics": 0x003831,  # Now just "Athletics" (Sacramento-bound)
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


@dataclass
class RecapCandidate:
    title: str
    url: str
    score: int


class RecapBot:
    """MLB game recap bot - posts video recaps when games finish."""
    
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
        self.posted_urls: set[str] = set()
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
            self.posted_urls = set(data.get("posted_urls", []))
            self.checked_no_recap = data.get("checked_no_recap", {})
            logger.info(
                "Loaded state: %s game ids, %s urls, %s pending recaps",
                len(self.posted_game_ids),
                len(self.posted_urls),
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
            "posted_urls": sorted(self.posted_urls),
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
        status = (((game.get("status") or {}).get("detailedState") or "").strip().lower())
        
        # Skip postponed, suspended, cancelled games
        skip_statuses = {"postponed", "suspended", "delayed start", "cancelled"}
        if status in skip_statuses:
            return
        
        if status not in {"final", "game over", "completed early", "completed"}:
            return

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

        recap = await self._fetch_recap_for_game(game_pk)
        if not recap:
            # Track attempts to find recap - retry up to 5 times before giving up
            attempts = self.checked_no_recap.get(game_pk, 0)
            if attempts < 5:
                self.checked_no_recap[game_pk] = attempts + 1
                self._save_state()
                logger.info(
                    "No recap found yet for %s at %s (%s) - attempt %d/5",
                    away,
                    home,
                    game_pk,
                    attempts + 1,
                )
            return

        # Recap found - remove from retry tracking if present
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
            recap_url=recap.url,
        )

        # If it's a direct video URL (MP4), Discord will auto-embed it
        # If it's an MLB page, include it in content for unfurling
        if recap.url.endswith(".mp4"):
            # Direct video - put in embed
            await channel.send(embed=embed)
        else:
            # MLB page or M3U8 - put in content for Discord to unfurl
            await channel.send(content=recap.url, embed=embed)

        logger.info("Posted recap for %s at %s | %s", away, home, recap.url)
        self.posted_game_ids.add(game_pk)
        self.posted_urls.add(recap.url)
        self._save_state()
        
        # Throttle to avoid blasting channel when many games finish simultaneously
        await asyncio.sleep(2)

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
        recap_url: str,
    ) -> discord.Embed:
        display_date = self._format_game_date(game_date_raw)
        color = TEAM_COLORS.get(winner_name, DEFAULT_EMBED_COLOR)
        
        # Handle doubleheader games
        title = f"{away} at {home} Recap"
        if game_number > 1:
            title += f" (Game {game_number})"
        
        description = f"**{away} {away_score}, {home} {home_score}** — {winner_name} won on {display_date}."

        embed = discord.Embed(
            title=title,
            url=recap_url,
            description=description,
            color=color,
            timestamp=datetime.now(EASTERN),
        )
        embed.add_field(name="Final", value=f"{away} {away_score} • {home} {home_score}", inline=False)
        
        # If it's a direct video URL, add it to the embed for playback
        if recap_url.endswith(".mp4") or "m3u8" in recap_url:
            embed.add_field(name="🎥 Video", value=f"[Watch Recap]({recap_url})", inline=False)
        
        # Add winner team logo if available (ESPN CDN)
        if winner_id:
            logo_url = f"https://a.espncdn.com/i/teamlogos/mlb/500/{winner_id}.png"
            embed.set_thumbnail(url=logo_url)
        
        embed.set_footer(text=f"MLB Recap • {display_date}")
        return embed

    async def _fetch_recap_for_game(self, game_pk: str) -> Optional[RecapCandidate]:
        content_url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/content"
        payload = await self._fetch_json(content_url)
        
        # Debug: Log the structure to see what we're getting
        logger.info("=== Fetching recap for game %s ===", game_pk)

        candidates: list[RecapCandidate] = []

        # 1) Direct editorial recap / media blobs.
        self._collect_candidates(payload, candidates)

        # 2) Some games expose recap links under editorial > recap.
        editorial_recap = ((((payload.get("editorial") or {}).get("recap") or {}).get("mlb") or {}))
        self._collect_candidates(editorial_recap, candidates)

        if not candidates:
            logger.warning("No recap candidates found for game %s", game_pk)
            return None

        candidates.sort(key=lambda c: (-c.score, c.title.lower(), c.url))
        logger.info("Found %d recap candidates for game %s", len(candidates), game_pk)
        logger.info("Best candidate: title='%s', url='%s', score=%d", 
                    candidates[0].title, candidates[0].url, candidates[0].score)
        return candidates[0]

    def _collect_candidates(self, node: Any, out: list[RecapCandidate]) -> None:
        if isinstance(node, dict):
            title = self._extract_title(node)
            url = self._extract_best_url(node)
            if title and url:
                score = self._score_candidate(title, url)
                if score > 0:
                    candidate = RecapCandidate(title=title, url=url, score=score)
                    out.append(candidate)
                    # Log the successful candidate with its structure
                    logger.info("✓ CANDIDATE: title='%s', url='%s', score=%d", title, url, score)
                    # Log what keys this node has to understand structure
                    logger.info("  Node keys: %s", list(node.keys())[:10])  # First 10 keys

            for value in node.values():
                self._collect_candidates(value, out)

        elif isinstance(node, list):
            for item in node:
                self._collect_candidates(item, out)

    def _extract_title(self, node: dict[str, Any]) -> str:
        for key in ("title", "headline", "name", "description", "blurb"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _extract_best_url(self, node: dict[str, Any]) -> str:
        # First priority: Direct playback URLs (MP4, M3U8) for Discord embedding
        playbacks = node.get("playbacks")
        if isinstance(playbacks, list) and len(playbacks) > 0:
            logger.info("Found playbacks array with %d items", len(playbacks))
            mp4_url = ""
            m3u8_url = ""
            for playback in playbacks:
                if not isinstance(playback, dict):
                    continue
                for key in ("url", "src"):
                    value = playback.get(key)
                    if not isinstance(value, str):
                        continue
                    url = self._normalize_url(value)
                    logger.info("Found playback URL: %s", url)
                    # Prefer MP4 for Discord native playback
                    if url.endswith(".mp4"):
                        mp4_url = url
                        logger.info("✓ Found MP4 URL: %s", url)
                    elif "m3u8" in url and not m3u8_url:
                        m3u8_url = url
                        logger.info("Found M3U8 URL: %s", url)
            
            # Return MP4 first (best for Discord), then M3U8
            if mp4_url:
                logger.info(">>> Using MP4 URL for Discord embed")
                return mp4_url
            if m3u8_url:
                logger.info(">>> Using M3U8 URL (no MP4 found)")
                return m3u8_url
        
        # Second priority: MLB video page URLs (fallback if no direct video)
        direct_keys = [
            "url",
            "shareUrl",
            "canonicalUrl",
            "webVtt",
        ]
        for key in direct_keys:
            value = node.get(key)
            if isinstance(value, str):
                url = self._normalize_url(value)
                if self._looks_like_video_page(url):
                    # Only log this if we're actually returning it
                    return url

        # Third priority: construct from slug
        slug = node.get("slug") or node.get("seoName")
        if isinstance(slug, str) and slug.strip():
            url = f"https://www.mlb.com/video/{slug.strip('/')}"
            # Only log if we're returning it
            return url

        # Don't spam logs - most nodes won't have URLs
        return ""

    def _normalize_url(self, value: str) -> str:
        url = value.strip()
        if url.startswith("//"):
            url = f"https:{url}"
        return url

    def _looks_like_video_page(self, url: str) -> bool:
        return url.startswith("https://www.mlb.com/video/") or url.startswith("https://mlb.com/video/")

    def _score_candidate(self, title: str, url: str) -> int:
        text = f"{title} {url}".lower()
        score = 0
        for keyword in RECAP_KEYWORDS:
            if keyword in text:
                score += 100
        for keyword in REJECT_KEYWORDS:
            if keyword in text:
                score -= 80
        if "/video/" in url:
            score += 15
        return score

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

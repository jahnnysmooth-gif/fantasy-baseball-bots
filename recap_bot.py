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

# Team ID to ESPN abbreviation mapping for logos
TEAM_ID_TO_ABBR = {
    109: "ari",  # Diamondbacks
    144: "atl",  # Braves
    110: "bal",  # Orioles
    111: "bos",  # Red Sox
    112: "chc",  # Cubs
    145: "chw",  # White Sox (chw not cws!)
    113: "cin",  # Reds
    114: "cle",  # Guardians
    115: "col",  # Rockies
    116: "det",  # Tigers
    117: "hou",  # Astros
    118: "kc",   # Royals
    108: "laa",  # Angels
    119: "lad",  # Dodgers
    146: "mia",  # Marlins
    158: "mil",  # Brewers
    142: "min",  # Twins
    121: "nym",  # Mets
    147: "nyy",  # Yankees
    133: "oak",  # Athletics
    143: "phi",  # Phillies
    134: "pit",  # Pirates
    135: "sd",   # Padres
    137: "sf",   # Giants
    136: "sea",  # Mariners
    138: "stl",  # Cardinals
    139: "tb",   # Rays
    140: "tex",  # Rangers
    141: "tor",  # Blue Jays
    120: "wsh",  # Nationals
}

logger = logging.getLogger("recap_bot")
# Configure logger to output to stdout so Render can see it
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(asctime)s] [RECAP_BOT] %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False  # Don't send to root logger


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
            logger.info("RECAP_BOT: Started, polling every %s seconds", self.poll_seconds)

    def stop(self) -> None:
        """Stop the recap bot polling loop."""
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("RECAP_BOT: Stopped")

    async def _run_loop(self) -> None:
        """Main polling loop."""
        logger.info("RECAP_BOT: _run_loop started, waiting for client ready...")
        await self.client.wait_until_ready()
        logger.info("RECAP_BOT: Client ready!")
        
        # Run immediately on startup (don't wait for first interval)
        logger.info("RECAP_BOT: Running initial poll immediately...")
        try:
            await self._poll_completed_games()
        except Exception as exc:
            logger.exception("RECAP_BOT: Error in initial poll: %s", exc)
        
        logger.info("RECAP_BOT: Entering main polling loop...")
        while not self.client.is_closed():
            try:
                await asyncio.sleep(self.poll_seconds)
                await self._poll_completed_games()
            except Exception as exc:
                logger.exception("RECAP_BOT: Error in poll loop: %s", exc)

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self.posted_game_ids = set(data.get("posted_game_ids", []))
            self.checked_no_recap = data.get("checked_no_recap", {})
            logger.info(
                "RECAP_BOT: Loaded state - %s game ids, %s pending recaps",
                len(self.posted_game_ids),
                len(self.checked_no_recap),
            )
        except Exception as exc:
            logger.warning("RECAP_BOT: Could not load state file %s: %s", self.state_path, exc)

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
        logger.info("RECAP_BOT: Cleaned up state - kept %s most recent game IDs", keep_count)

    async def _poll_completed_games(self) -> None:
        logger.info("RECAP_BOT: === _poll_completed_games CALLED ===")
        channel = self.client.get_channel(self.channel_id)
        if channel is None:
            logger.error("RECAP_BOT: Channel %s not found. Check RECAP_CHANNEL_ID.", self.channel_id)
            return
        if not isinstance(channel, discord.abc.Messageable):
            logger.error("RECAP_BOT: Channel %s is not messageable.", self.channel_id)
            return

        today_et = datetime.now(EASTERN).date()
        dates_to_scan = [today_et]  # Only today for testing

        logger.info("RECAP_BOT: Scanning MLB schedule for %s", ", ".join(str(d) for d in dates_to_scan))

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
        logger.info("RECAP_BOT: Found %d finished games, posting last %d for testing", 
                    len(all_finished_games), len(games_to_post))
        
        for game in games_to_post:
            try:
                await self._process_game(channel, game)
            except Exception as exc:
                logger.exception("RECAP_BOT: Failed processing game %s: %s", game.get("gamePk") or "unknown", exc)

    async def _fetch_json(self, url: str) -> dict[str, Any]:
        for attempt in range(3):
            try:
                async with self.http_session.get(url) as response:
                    response.raise_for_status()
                    return await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == 2:
                    raise
                logger.warning("RECAP_BOT: Fetch failed (attempt %d/3) for %s: %s", attempt + 1, url, exc)
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

        logger.info("RECAP_BOT: Processing game %s - %s at %s", game_pk, away, home)

        # Search YouTube for highlights
        youtube_url = await self._search_youtube_highlights(away, home, game_date_raw)
        
        if not youtube_url:
            # Track attempts to find video - retry up to 5 times before giving up
            attempts = self.checked_no_recap.get(game_pk, 0)
            if attempts < 5:
                self.checked_no_recap[game_pk] = attempts + 1
                self._save_state()
                logger.info(
                    "RECAP_BOT: No YouTube video found yet for %s at %s (%s) - attempt %d/5",
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

        # Post YouTube URL first (Discord will auto-embed)
        await channel.send(content=youtube_url)
        
        # Post custom embed as a follow-up (doesn't break YouTube embed)
        await channel.send(embed=embed)

        logger.info("RECAP_BOT: ✓ Posted %s at %s (%d-%d) | %s", away, home, away_score, home_score, youtube_url)
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
        """Get game highlights from MLB Stats API content endpoint."""
        
        # We already have the game_pk from _process_game, but we need to pass it here
        # For now, let's try a different approach - search MLB.com's video feed
        
        logger.info("RECAP_BOT_MLB: === TRYING MLB VIDEO FEED ===")
        
        # MLB.com has a video feed API
        # Format: https://www.mlb.com/video/search?q=Cardinals+Tigers
        away_short = self._shorten_team_name(away_team)
        home_short = self._shorten_team_name(home_team)
        
        try:
            date_obj = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
            date_str = date_obj.strftime("%Y-%m-%d")
        except:
            date_str = datetime.now(EASTERN).strftime("%Y-%m-%d")
        
        # Try MLB's video search
        query = f"{away_short} {home_short} highlights {date_str}"
        mlb_search_url = f"https://www.mlb.com/video/search?q={quote_plus(query)}"
        
        logger.info("RECAP_BOT_MLB: Search URL: %s", mlb_search_url)
        
        try:
            async with self.http_session.get(mlb_search_url) as response:
                logger.info("RECAP_BOT_MLB: Response status: %s", response.status)
                html = await response.text()
                
                # Look for YouTube video IDs in MLB.com's search results
                # They embed YouTube videos, so we can extract the IDs
                youtube_pattern = r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})'
                matches = re.findall(youtube_pattern, html)
                
                if matches:
                    video_id = matches[0]  # First result
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    logger.info("RECAP_BOT_MLB: ✓ Found YouTube video from MLB.com: %s", video_url)
                    return video_url
                
                logger.warning("RECAP_BOT_MLB: No YouTube videos found in MLB.com search")
        except Exception as e:
            logger.warning("RECAP_BOT_MLB: MLB.com search failed: %s", e)
        
        # FALLBACK: Direct YouTube search with very specific query
        return await self._search_youtube_fallback(away_short, home_short, date_str)
    
    async def _search_youtube_fallback(
        self,
        away_short: str,
        home_short: str,
        date_str: str
    ) -> Optional[str]:
        """Fallback: general YouTube search for MLB highlights."""
        
        logger.info("RECAP_BOT_YT: === YOUTUBE FALLBACK ===")
        
        # Simple YouTube search
        query = f"MLB {away_short} vs {home_short} highlights {date_str}"
        search_url = f"https://www.youtube.com/results?search_query={quote_plus(query)}"
        
        logger.info("RECAP_BOT_YT: Search URL: %s", search_url)
        
        try:
            async with self.http_session.get(search_url) as response:
                html = await response.text()
                
                # Simple pattern - just find first video ID
                pattern = r'"videoId":"([a-zA-Z0-9_-]{11})"'
                match = re.search(pattern, html)
                
                if match:
                    video_id = match.group(1)
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    logger.info("RECAP_BOT_YT: ✓ Found video: %s", video_url)
                    return video_url
                
                logger.warning("RECAP_BOT_YT: No videos found")
                return None
        except Exception as e:
            logger.exception("RECAP_BOT_YT: Error: %s", e)
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
        title = f"⚾ {away} at {home}"
        if game_number > 1:
            title += f" (Game {game_number})"

        # No URL in embed - it's already posted above
        embed = discord.Embed(
            title=title,
            color=color,
        )
        
        # Put winner first in score line
        if away_score > home_score:
            # Away team won
            score_text = f"{away} {away_score}, {home} {home_score}"
        else:
            # Home team won
            score_text = f"{home} {home_score}, {away} {away_score}"
        
        embed.add_field(name="Final", value=score_text, inline=False)
        
        # Normalize embed width by padding to minimum character count
        # Discord embed width is based on content length, so we pad short content
        MIN_WIDTH_CHARS = 200
        current_length = len(score_text)
        if current_length < MIN_WIDTH_CHARS:
            # Add invisible zero-width spaces to reach minimum
            padding = "\u200B" * (MIN_WIDTH_CHARS - current_length)
            embed.description = padding
        
        # Add winner team logo (ESPN CDN - uses lowercase abbreviations)
        if winner_id and winner_id in TEAM_ID_TO_ABBR:
            team_abbr = TEAM_ID_TO_ABBR[winner_id]
            embed.set_thumbnail(url=f"https://a.espncdn.com/i/teamlogos/mlb/500/{team_abbr}.png")
        
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
    
    def _shorten_team_name(self, full_name: str) -> str:
        """Convert full team name to MLB's shortened format used in video titles."""
        # MLB uses shortened versions like: "D-backs", "A's", "Rays", "Tigers"
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
    RECAP_CHANNEL_ID = int(os.getenv("RECAP_CHANNEL_ID", "0"))
    RECAP_BOT_POLL_SECONDS = int(os.getenv("RECAP_BOT_POLL_SECONDS", "300"))
    
    logger.info("RECAP_BOT: Token present: %s", "Yes" if HIGHLIGHTS_BOT_TOKEN else "NO - MISSING!")
    logger.info("RECAP_BOT: Channel ID: %s", RECAP_CHANNEL_ID)
    logger.info("RECAP_BOT: Poll interval: %s seconds", RECAP_BOT_POLL_SECONDS)
    
    if not HIGHLIGHTS_BOT_TOKEN:
        logger.error("RECAP_BOT: FATAL - Missing HIGHLIGHTS_BOT_TOKEN environment variable")
        raise SystemExit("Missing HIGHLIGHTS_BOT_TOKEN environment variable")
    if RECAP_CHANNEL_ID <= 0:
        logger.warning("RECAP_BOT: RECAP_CHANNEL_ID not set - bot will not run")
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
        logger.info("RECAP_BOT: Logged in as %s (%s)", client.user, client.user.id if client.user else "n/a")
        
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
        logger.info("RECAP_BOT: Started - polling every %s seconds", RECAP_BOT_POLL_SECONDS)
    
    try:
        logger.info("RECAP_BOT: Connecting to Discord...")
        await client.start(HIGHLIGHTS_BOT_TOKEN)
    except Exception as e:
        logger.exception("RECAP_BOT: FATAL - Failed to start Discord client: %s", e)
        raise
    finally:
        # Cleanup
        if recap_bot_instance:
            recap_bot_instance.stop()
        if http_session and not http_session.closed:
            await http_session.close()
        if not client.is_closed():
            await client.close()

import asyncio
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import discord
import feedparser

os.makedirs("state", exist_ok=True)

BASE_DIR = Path(__file__).resolve().parent
SOURCES_FILE = BASE_DIR / "news_sources.json"
STATE_FILE = "state/news_posted_ids.json"
THREAD_MAP_FILE = BASE_DIR / "state/player_profiles/player_threads.json"

DISCORD_TOKEN = os.getenv("NEWS_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
NEWS_CHANNEL_ID = int(os.getenv("NEWS_CHANNEL_ID", "0"))
NEWS_POLL_SECONDS = int(os.getenv("NEWS_POLL_SECONDS", "300"))
MAX_ENTRIES_PER_FEED = int(os.getenv("NEWS_MAX_ENTRIES_PER_FEED", "5"))

INCLUDE_PATTERNS: Dict[str, List[str]] = {
    "INJURY": [
        "injury", "injured", "il", "10-day il", "15-day il", "60-day il",
        "placed on", "activated", "reinstated", "scratched", "day-to-day",
        "day to day", "out for", "headed to the il", "returns", "returning",
        "available", "unavailable", "underwent", "soreness", "tightness",
        "will miss", "won't start", "not expected to", "shut down"
    ],
    "LINEUP": [
        "starting", "starts tonight", "in the lineup", "not in the lineup",
        "batting", "leading off", "gets the start", "starting at", "will start",
        "lineup", "rest day", "off day", "bench", "scratched from the lineup"
    ],
    "CALL-UP": [
        "called up", "recalled", "selected the contract", "promotion",
        "promoted", "optioned", "sent down", "demoted", "designated for assignment",
        "dfa", "added to the roster"
    ],
    "TRANSACTION": [
        "traded", "trade", "sign", "signed", "signing", "released",
        "waived", "claimed", "acquired", "deal", "contract"
    ],
    "CLOSER": [
        "closer", "save chance", "ninth inning", "bullpen", "committee",
        "high leverage", "setup man", "save opportunity"
    ],
    "ROTATION": [
        "rotation", "starter", "starting pitcher", "probable", "bullpen game",
        "opens the season", "slot in the rotation", "start sunday", "start monday",
        "start tuesday", "start wednesday", "start thursday", "start friday",
        "start saturday"
    ],
    "WEATHER": [
        "postponed", "rain delay", "weather", "delayed", "cancelled", "canceled"
    ],
}

EXCLUDE_PATTERNS = [
    "ticket", "tickets", "giveaway", "promotion", "promo", "sweepstakes",
    "podcast", "newsletter", "column", "game story", "live blog",
    "pregame show", "postgame show", "watch live", "stream live",
    "subscribe", "merch", "shop now", "sale", "odds", "betting"
]

TEAM_LOGOS = {
    "ARI": "https://a.espncdn.com/i/teamlogos/mlb/500/ari.png",
    "ATL": "https://a.espncdn.com/i/teamlogos/mlb/500/atl.png",
    "BAL": "https://a.espncdn.com/i/teamlogos/mlb/500/bal.png",
    "BOS": "https://a.espncdn.com/i/teamlogos/mlb/500/bos.png",
    "CHC": "https://a.espncdn.com/i/teamlogos/mlb/500/chc.png",
    "CWS": "https://a.espncdn.com/i/teamlogos/mlb/500/chw.png",
    "CIN": "https://a.espncdn.com/i/teamlogos/mlb/500/cin.png",
    "CLE": "https://a.espncdn.com/i/teamlogos/mlb/500/cle.png",
    "COL": "https://a.espncdn.com/i/teamlogos/mlb/500/col.png",
    "DET": "https://a.espncdn.com/i/teamlogos/mlb/500/det.png",
    "HOU": "https://a.espncdn.com/i/teamlogos/mlb/500/hou.png",
    "KC": "https://a.espncdn.com/i/teamlogos/mlb/500/kc.png",
    "LAA": "https://a.espncdn.com/i/teamlogos/mlb/500/laa.png",
    "LAD": "https://a.espncdn.com/i/teamlogos/mlb/500/lad.png",
    "MIA": "https://a.espncdn.com/i/teamlogos/mlb/500/mia.png",
    "MIL": "https://a.espncdn.com/i/teamlogos/mlb/500/mil.png",
    "MIN": "https://a.espncdn.com/i/teamlogos/mlb/500/min.png",
    "NYM": "https://a.espncdn.com/i/teamlogos/mlb/500/nym.png",
    "NYY": "https://a.espncdn.com/i/teamlogos/mlb/500/nyy.png",
    "ATH": "https://a.espncdn.com/i/teamlogos/mlb/500/oak.png",
    "PHI": "https://a.espncdn.com/i/teamlogos/mlb/500/phi.png",
    "PIT": "https://a.espncdn.com/i/teamlogos/mlb/500/pit.png",
    "SD": "https://a.espncdn.com/i/teamlogos/mlb/500/sd.png",
    "SF": "https://a.espncdn.com/i/teamlogos/mlb/500/sf.png",
    "SEA": "https://a.espncdn.com/i/teamlogos/mlb/500/sea.png",
    "STL": "https://a.espncdn.com/i/teamlogos/mlb/500/stl.png",
    "TB": "https://a.espncdn.com/i/teamlogos/mlb/500/tb.png",
    "TEX": "https://a.espncdn.com/i/teamlogos/mlb/500/tex.png",
    "TOR": "https://a.espncdn.com/i/teamlogos/mlb/500/tor.png",
    "WSH": "https://a.espncdn.com/i/teamlogos/mlb/500/wsh.png",
    "MLB": "https://a.espncdn.com/i/teamlogos/mlb/500/mlb.png",
}

TEAM_COLORS = {
    "ARI": 0xA71930,
    "ATL": 0xCE1141,
    "BAL": 0xDF4601,
    "BOS": 0xBD3039,
    "CHC": 0x0E3386,
    "CWS": 0x27251F,
    "CIN": 0xC6011F,
    "CLE": 0xE31937,
    "COL": 0x33006F,
    "DET": 0x0C2340,
    "HOU": 0xEB6E1F,
    "KC": 0x004687,
    "LAA": 0xBA0021,
    "LAD": 0x005A9C,
    "MIA": 0x00A3E0,
    "MIL": 0x12284B,
    "MIN": 0x002B5C,
    "NYM": 0x002D72,
    "NYY": 0x0C2340,
    "ATH": 0x003831,
    "PHI": 0xE81828,
    "PIT": 0xFDB827,
    "SD": 0x2F241D,
    "SF": 0xFD5A1E,
    "SEA": 0x005C5C,
    "STL": 0xC41E3A,
    "TB": 0x092C5C,
    "TEX": 0x003278,
    "TOR": 0x134A8E,
    "WSH": 0xAB0003,
    "MLB": 0x2B2D31,
}

WHITESPACE_RE = re.compile(r"\s+")
URL_RE = re.compile(r"https?://\S+")


def strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


def normalize_for_match(text: str) -> str:
    text = strip_accents(text or "").lower()
    text = text.replace("&amp;", "&")
    text = URL_RE.sub(" ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return f" {text} " if text else " "


def load_posted_ids() -> Set[str]:
    try:
        if not os.path.exists(STATE_FILE):
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump([], f)
            return set()

        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return set(str(x) for x in data)

        return set()
    except Exception as e:
        print(f"[NEWS] Failed loading state file: {e}")
        return set()


def save_posted_ids(posted_ids: Set[str]) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(posted_ids), f)
    except Exception as e:
        print(f"[NEWS] Failed saving state file: {e}")


def load_sources() -> List[dict]:
    if not SOURCES_FILE.exists():
        raise FileNotFoundError(f"{SOURCES_FILE} not found")

    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("news_sources.json must contain a JSON array")

    cleaned = []
    for item in data:
        if not isinstance(item, dict):
            continue

        handle = str(item.get("handle", "")).strip()
        rss = str(item.get("rss", "")).strip()
        team = str(item.get("team", "MLB")).strip().upper() or "MLB"
        name = str(item.get("name", handle)).strip() or handle

        if not handle or not rss:
            continue

        cleaned.append({
            "name": name,
            "handle": handle,
            "team": team,
            "rss": rss,
        })

    return cleaned


def load_player_threads() -> Dict[str, int]:
    try:
        if not THREAD_MAP_FILE.exists():
            return {}

        with open(THREAD_MAP_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)

        cleaned: Dict[str, int] = {}
        if isinstance(raw, dict):
            for player_name, thread_id in raw.items():
                try:
                    cleaned[str(player_name).strip()] = int(thread_id)
                except Exception:
                    continue
        return cleaned
    except Exception as e:
        print(f"[NEWS] Failed loading player thread map: {e}")
        return {}


def build_player_match_index(player_threads: Dict[str, int]) -> List[Tuple[str, str, int]]:
    indexed = []
    for player_name, thread_id in player_threads.items():
        norm = normalize_for_match(player_name).strip()
        if norm:
            indexed.append((norm, player_name, thread_id))

    indexed.sort(key=lambda item: len(item[0]), reverse=True)
    return indexed


def normalize_text(text: str) -> str:
    text = text or ""
    text = URL_RE.sub("", text)
    text = text.replace("&amp;", "&")
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def get_entry_text(entry) -> Tuple[str, str]:
    title = normalize_text(getattr(entry, "title", "") or "")
    summary = normalize_text(getattr(entry, "summary", "") or "")
    return title, summary


def entry_unique_id(entry) -> Optional[str]:
    for attr in ("id", "guid", "link", "title"):
        value = getattr(entry, attr, None)
        if value:
            return str(value).strip()
    return None


def looks_like_reply_or_repost(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()

    bad_starts = ["rt @", "repost:", "retweeted", "replying to", "@"]
    if any(text.startswith(x) for x in bad_starts):
        return True

    if "replying to" in text:
        return True

    return False


def classify_news(title: str, summary: str) -> Optional[str]:
    text = f"{title} {summary}".lower()

    if any(bad in text for bad in EXCLUDE_PATTERNS):
        return None

    for tag in ["INJURY", "LINEUP", "CALL-UP", "TRANSACTION", "CLOSER", "ROTATION", "WEATHER"]:
        patterns = INCLUDE_PATTERNS[tag]
        if any(p.lower() in text for p in patterns):
            return tag

    return None


def detect_player(title: str, summary: str, player_index: List[Tuple[str, str, int]]) -> Tuple[Optional[str], Optional[int]]:
    haystack = normalize_for_match(f"{title} {summary}")

    for normalized_name, original_name, thread_id in player_index:
        needle = f" {normalized_name} "
        if needle in haystack:
            return original_name, thread_id

    return None, None


def extract_team_from_thread_name(thread_name: str) -> Optional[str]:
    if not thread_name:
        return None

    for sep in [" — ", " - ", " | ", "("]:
        if sep in thread_name:
            part = thread_name.split(sep, 1)[-1].strip(" )")
            maybe = part.upper().strip()
            if len(maybe) in (2, 3, 4):
                return maybe
    return None


async def resolve_player_thread(client: discord.Client, thread_id: Optional[int]) -> Optional[discord.abc.GuildChannel]:
    if not thread_id:
        return None

    thread = client.get_channel(thread_id)
    if thread is not None:
        return thread

    try:
        fetched = await client.fetch_channel(thread_id)
        return fetched
    except Exception:
        return None


async def infer_team(
    client: discord.Client,
    source_team: str,
    player_name: Optional[str],
    player_thread_id: Optional[int],
) -> str:
    if source_team and source_team != "MLB":
        return source_team

    thread = await resolve_player_thread(client, player_thread_id)
    if thread is not None:
        inferred = extract_team_from_thread_name(getattr(thread, "name", ""))
        if inferred:
            return inferred

    return "MLB"


def build_embed(
    source: dict,
    team: str,
    tag: str,
    title: str,
    summary: str,
    link: str,
    player_name: Optional[str] = None,
    player_thread_url: Optional[str] = None,
) -> discord.Embed:
    handle = source["handle"]
    clean_title = title.strip() or "New update"
    clean_summary = summary.strip()

    if len(clean_title) > 256:
        clean_title = clean_title[:253] + "..."

    color = TEAM_COLORS.get(team, TEAM_COLORS["MLB"])
    embed = discord.Embed(
        title=clean_title,
        description=clean_summary[:3500] if clean_summary else None,
        color=color,
        url=link,
    )

    logo = TEAM_LOGOS.get(team)
    if logo:
        embed.set_thumbnail(url=logo)

    embed.add_field(name="Tag", value=tag.title(), inline=True)
    embed.add_field(name="Source", value=f"@{handle}", inline=True)

    if player_name and player_thread_url:
        embed.add_field(
            name="Player Profile",
            value=f"[Open {player_name} thread]({player_thread_url})",
            inline=False,
        )

    footer_team = team if team else "MLB"
    embed.set_footer(text=f"{footer_team} • Fantasy Baseball Geek")
    return embed


async def parse_feed(url: str):
    return await asyncio.to_thread(feedparser.parse, url)


class NewsBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.bg_task: Optional[asyncio.Task] = None
        self.posted_ids: Set[str] = load_posted_ids()
        self.sources: List[dict] = []
        self.started_loop = False
        self.player_threads: Dict[str, int] = {}
        self.player_index: List[Tuple[str, str, int]] = []

    def refresh_player_threads(self):
        self.player_threads = load_player_threads()
        self.player_index = build_player_match_index(self.player_threads)

    async def on_ready(self):
        print(f"[NEWS] Logged in as {self.user}")
        print("[NEWS] RSS news bot started")

        if self.started_loop:
            return

        self.started_loop = True
        self.sources = load_sources()
        self.refresh_player_threads()

        print(f"[NEWS] Loaded {len(self.sources)} sources")
        print(f"[NEWS] Loaded {len(self.player_threads)} player thread mappings")
        print(f"[NEWS] Poll interval: {NEWS_POLL_SECONDS} seconds")
        print(f"[NEWS] State file: {STATE_FILE}")

        self.bg_task = asyncio.create_task(self.news_loop())

    async def news_loop(self):
        await self.wait_until_ready()

        while not self.is_closed():
            try:
                self.refresh_player_threads()
                await self.check_news_feeds()
            except Exception as e:
                print(f"[NEWS] Loop error: {e}")

            await asyncio.sleep(NEWS_POLL_SECONDS)

    async def check_news_feeds(self):
        if NEWS_CHANNEL_ID == 0:
            print("[NEWS] NEWS_CHANNEL_ID is not set")
            return

        channel = self.get_channel(NEWS_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.fetch_channel(NEWS_CHANNEL_ID)
            except Exception as e:
                print(f"[NEWS] Could not fetch news channel {NEWS_CHANNEL_ID}: {e}")
                return

        print("[NEWS] Checking RSS feeds")

        new_posts = 0

        for source in self.sources:
            handle = source["handle"]
            rss = source["rss"]

            try:
                feed = await parse_feed(rss)

                if getattr(feed, "bozo", 0):
                    print(f"[NEWS] Feed warning for @{handle}: {getattr(feed, 'bozo_exception', 'unknown')}")

                entries = getattr(feed, "entries", [])[:MAX_ENTRIES_PER_FEED]

                for entry in entries:
                    uid = entry_unique_id(entry)
                    if not uid or uid in self.posted_ids:
                        continue

                    title, summary = get_entry_text(entry)
                    if not title and not summary:
                        continue

                    if looks_like_reply_or_repost(title, summary):
                        continue

                    tag = classify_news(title, summary)
                    if not tag:
                        continue

                    link = getattr(entry, "link", "") or source["rss"]

                    player_name, player_thread_id = detect_player(title, summary, self.player_index)
                    player_thread = await resolve_player_thread(self, player_thread_id)
                    player_thread_url = getattr(player_thread, "jump_url", None)

                    team = await infer_team(
                        client=self,
                        source_team=source["team"],
                        player_name=player_name,
                        player_thread_id=player_thread_id,
                    )

                    embed = build_embed(
                        source=source,
                        team=team,
                        tag=tag,
                        title=title,
                        summary=summary,
                        link=link,
                        player_name=player_name,
                        player_thread_url=player_thread_url,
                    )

                    await channel.send(embed=embed)

                    if player_thread is not None:
                        try:
                            await player_thread.send(embed=embed)
                            print(f"[NEWS] Also posted to thread for {player_name}")
                        except Exception as e:
                            print(f"[NEWS] Failed posting to player thread for {player_name}: {e}")

                    self.posted_ids.add(uid)
                    new_posts += 1
                    print(
                        f"[NEWS] Posted [{team}] [{tag}] @{handle}: "
                        f"{title[:120]}"
                        + (f" | player={player_name}" if player_name else "")
                    )

                save_posted_ids(self.posted_ids)

            except Exception as e:
                print(f"[NEWS] Feed error for @{handle}: {e}")

        print(f"[NEWS] Done. New posts: {new_posts}")


async def start_news_bot():
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN (or NEWS_BOT_TOKEN) is not set")

    if NEWS_CHANNEL_ID == 0:
        raise RuntimeError("NEWS_CHANNEL_ID is not set")

    bot = NewsBot()
    await bot.start(DISCORD_TOKEN)

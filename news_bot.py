import asyncio
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import discord
import feedparser

os.makedirs("state", exist_ok=True)


BASE_DIR = Path(__file__).resolve().parent
SOURCES_FILE = BASE_DIR / "news_sources.json"
STATE_FILE = "state/news_posted_ids.json"

DISCORD_TOKEN = os.getenv("NEWS_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
NEWS_CHANNEL_ID = int(os.getenv("NEWS_CHANNEL_ID", "0"))
NEWS_POLL_SECONDS = int(os.getenv("NEWS_POLL_SECONDS", "300"))

# Keep this modest at first. You can raise it later.
MAX_ENTRIES_PER_FEED = int(os.getenv("NEWS_MAX_ENTRIES_PER_FEED", "5"))

# Simple include filters for fantasy-relevant news
INCLUDE_PATTERNS: Dict[str, List[str]] = {
    "INJURY": [
        "injury", "injured", "il", "10-day il", "15-day il", "60-day il",
        "placed on", "activated", "reinstated", "scratched", "day-to-day",
        "day to day", "out for", "headed to the il", "returns", "returning",
        "available", "unavailable", "underwent", "soreness", "tightness"
    ],
    "LINEUP": [
        "starting", "starts tonight", "in the lineup", "not in the lineup",
        "batting", "leading off", "gets the start", "starting at", "will start",
        "lineup", "rest day", "off day"
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
        "opens the season", "slot in the rotation"
    ],
    "WEATHER": [
        "postponed", "rain delay", "weather", "delayed", "cancelled", "canceled"
    ],
}

# Noise filters
EXCLUDE_PATTERNS = [
    "ticket", "tickets", "giveaway", "promotion", "promo", "sweepstakes",
    "podcast", "newsletter", "column", "game story", "live blog",
    "pregame show", "postgame show", "watch live", "stream live",
    "subscribe", "merch", "shop now", "sale", "odds", "betting"
]

# Some public RSS mirrors prepend handle/title formats inconsistently.
# We'll normalize and classify from the combined title + summary.
WHITESPACE_RE = re.compile(r"\s+")
URL_RE = re.compile(r"https?://\S+")


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

    # rough filters for replies / repost-ish content
    bad_starts = [
        "rt @", "repost:", "retweeted", "replying to", "@"
    ]
    if any(text.startswith(x) for x in bad_starts):
        return True

    if "replying to" in text:
        return True

    return False


def classify_news(title: str, summary: str) -> Optional[str]:
    text = f"{title} {summary}".lower()

    if any(bad in text for bad in EXCLUDE_PATTERNS):
        return None

    # Most important buckets first
    for tag in ["INJURY", "LINEUP", "CALL-UP", "TRANSACTION", "CLOSER", "ROTATION", "WEATHER"]:
        patterns = INCLUDE_PATTERNS[tag]
        if any(p.lower() in text for p in patterns):
            return tag

    return None


def build_embed(source: dict, tag: str, title: str, summary: str, link: str) -> discord.Embed:
    team = source["team"]
    handle = source["handle"]
    header = f"[{team}] [{tag}]"

    clean_title = title.strip() or "New update"
    if len(clean_title) > 256:
        clean_title = clean_title[:253] + "..."

    description_parts = []
    if summary and summary.lower() != title.lower():
        description_parts.append(summary[:3500])

    description_parts.append(f"Source: @{handle}")
    description_parts.append(link)

    embed = discord.Embed(
        title=f"{header} {clean_title}",
        description="\n\n".join(description_parts),
    )
    return embed


async def parse_feed(url: str):
    # feedparser is sync, so run it off the event loop
    return await asyncio.to_thread(feedparser.parse, url)


class NewsBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.bg_task: Optional[asyncio.Task] = None
        self.posted_ids: Set[str] = load_posted_ids()
        self.sources: List[dict] = []
        self.started_loop = False

    async def on_ready(self):
        print(f"[NEWS] Logged in as {self.user}")
        print("[NEWS] RSS news bot started")

        if self.started_loop:
            return

        self.started_loop = True
        self.sources = load_sources()

        print(f"[NEWS] Loaded {len(self.sources)} sources")
        print(f"[NEWS] Poll interval: {NEWS_POLL_SECONDS} seconds")
        print(f"[NEWS] State file: {STATE_FILE}")

        self.bg_task = asyncio.create_task(self.news_loop())

    async def news_loop(self):
        await self.wait_until_ready()

        while not self.is_closed():
            try:
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
                    # bozo_exception can still return usable entries, so do not skip immediately
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

                    embed = build_embed(
                        source=source,
                        tag=tag,
                        title=title,
                        summary=summary,
                        link=link,
                    )

                    await channel.send(embed=embed)

                    self.posted_ids.add(uid)
                    new_posts += 1
                    print(f"[NEWS] Posted [{source['team']}] [{tag}] @{handle}: {title[:120]}")

                # save after each source so crashes do less damage
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

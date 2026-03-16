import asyncio
import json
import os
from typing import Optional, Set

import discord
import requests
from bs4 import BeautifulSoup

STATE_DIR = "state"
STATE_FILE = os.path.join(STATE_DIR, "news_ids.json")

DISCORD_TOKEN = os.getenv("NEWS_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
NEWS_CHANNEL_ID = int(os.getenv("NEWS_CHANNEL_ID", "0"))
NEWS_POLL_SECONDS = int(os.getenv("NEWS_POLL_SECONDS", "180"))

NEWS_URL = "https://www.fantasypros.com/mlb/player-news.php"

os.makedirs(STATE_DIR, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        )
    }
)


def load_ids() -> Set[str]:
    if not os.path.exists(STATE_FILE):
        return set()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(x) for x in data}
    except Exception as e:
        print(f"[NEWS] Failed loading state: {e}")
    return set()


def save_ids(ids: Set[str]) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f, indent=2)
    except Exception as e:
        print(f"[NEWS] Failed saving state: {e}")


def clean_text(text: Optional[str]) -> str:
    return " ".join((text or "").split()).strip()


def fetch_news_items():
    response = SESSION.get(NEWS_URL, timeout=30)
    response.raise_for_status()

    print(f"[NEWS] FantasyPros status: {response.status_code}")
    print(f"[NEWS] FantasyPros html length: {len(response.text)}")

    soup = BeautifulSoup(response.text, "html.parser")

    containers = (
        soup.select(".player-news-item")
        or soup.select(".news-item")
        or soup.select("article")
    )

    print(f"[NEWS] Candidate containers found: {len(containers)}")

    for idx, container in enumerate(containers[:2]):
        html = str(container)
        print(f"[NEWS] Container {idx} html preview: {html[:2000]}")

    items = []

    for idx, container in enumerate(containers):
        text = clean_text(container.get_text(" ", strip=True))
        if not text:
            continue

        links = container.select("a[href]")
        link = NEWS_URL
        for a in links:
            href = a.get("href", "").strip()
            if href:
                if href.startswith("http://") or href.startswith("https://"):
                    link = href
                elif href.startswith("/"):
                    link = "https://www.fantasypros.com" + href
                break

        headline = ""
        player = ""
        summary = ""
        timestamp = ""

        for sel in ["h1", "h2", "h3", "h4", ".headline", ".news-title", ".title"]:
            el = container.select_one(sel)
            if el:
                headline = clean_text(el.get_text(" ", strip=True))
                if headline:
                    break

        for sel in [".player-name", ".name", ".player", "strong", "b"]:
            el = container.select_one(sel)
            if el:
                player = clean_text(el.get_text(" ", strip=True))
                if player:
                    break

        for sel in [".timestamp", ".time", "time", ".date"]:
            el = container.select_one(sel)
            if el:
                timestamp = clean_text(el.get_text(" ", strip=True))
                if timestamp:
                    break

        for sel in [".news-content", ".content", ".summary", ".excerpt", "p"]:
            el = container.select_one(sel)
            if el:
                summary = clean_text(el.get_text(" ", strip=True))
                if summary:
                    break

        if not headline:
            headline = text[:160]
        if not player:
            player = "FantasyPros MLB"
        if not summary:
            summary = text[:1000]

        uid = f"{headline}|{summary[:120]}"

        items.append(
            {
                "id": uid,
                "player": player,
                "headline": headline,
                "summary": summary,
                "timestamp": timestamp,
                "link": link,
            }
        )

    if not items:
        debug_path = os.path.join(STATE_DIR, "fantasypros_debug.html")
        try:
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"[NEWS] Wrote debug HTML to {debug_path}")
        except Exception as e:
            print(f"[NEWS] Failed writing debug HTML: {e}")

        raise RuntimeError("Could not parse any FantasyPros player news items")

    return items


class NewsBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.posted_ids: Set[str] = load_ids()
        self.bg_task = None
        self.started_loop = False

    async def on_ready(self):
        print(f"[NEWS] Logged in as {self.user}")
        print("[NEWS] FantasyPros news bot started")
        print(f"[NEWS] Poll interval: {NEWS_POLL_SECONDS} seconds")
        print(f"[NEWS] State file: {STATE_FILE}")
        print(f"[NEWS] Source page: {NEWS_URL}")

        if self.started_loop:
            return

        self.started_loop = True
        self.bg_task = asyncio.create_task(self.news_loop())

    async def news_loop(self):
        await self.wait_until_ready()

        while not self.is_closed():
            try:
                await self.check_news()
            except Exception as e:
                print(f"[NEWS] Loop error: {e}")

            await asyncio.sleep(NEWS_POLL_SECONDS)

    async def check_news(self):
        if NEWS_CHANNEL_ID == 0:
            print("[NEWS] NEWS_CHANNEL_ID is not set")
            return

        channel = self.get_channel(NEWS_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.fetch_channel(NEWS_CHANNEL_ID)
            except Exception as e:
                print(f"[NEWS] Could not fetch channel {NEWS_CHANNEL_ID}: {e}")
                return

        print("[NEWS] Checking FantasyPros player news page")

        items = await asyncio.to_thread(fetch_news_items)

        print(f"[NEWS] Parsed items: {len(items)}")
        print(f"[NEWS] Posted ID count: {len(self.posted_ids)}")

        new_posts = 0

        for item in reversed(items):
            if item["id"] in self.posted_ids:
                continue

            embed = discord.Embed(
                title=item["headline"][:256],
                description=item["summary"][:3500],
                url=item["link"],
                color=0x2ECC71,
            )
            embed.set_author(name=item["player"])
            embed.add_field(name="Source", value="FantasyPros", inline=True)

            if item["timestamp"]:
                embed.set_footer(text=item["timestamp"])
            else:
                embed.set_footer(text="Fantasy Baseball Geek")

            try:
                await channel.send(embed=embed)
            except Exception as e:
                print(f"[NEWS] Failed sending embed: {e}")
                continue

            self.posted_ids.add(item["id"])
            new_posts += 1
            print(f"[NEWS] Posted: {item['player']} | {item['headline'][:100]}")

        save_ids(self.posted_ids)
        print(f"[NEWS] Done. New posts: {new_posts}")


async def start_news_bot():
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN (or NEWS_BOT_TOKEN) is not set")

    if NEWS_CHANNEL_ID == 0:
        raise RuntimeError("NEWS_CHANNEL_ID is not set")

    bot = NewsBot()
    await bot.start(DISCORD_TOKEN)

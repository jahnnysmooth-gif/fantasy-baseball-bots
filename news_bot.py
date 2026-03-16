import asyncio
import json
import os

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


def load_ids():
    if not os.path.exists(STATE_FILE):
        return set()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(str(x) for x in data)
    except Exception:
        return set()


def save_ids(ids):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(ids)), f, indent=2)


def clean_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def fetch_news():
    response = SESSION.get(NEWS_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    print(f"[NEWS] FantasyPros status: {response.status_code}")
    print(f"[NEWS] FantasyPros html length: {len(response.text)}")

    cards = soup.select(".player-news-item")
    print(f"[NEWS] Found .player-news-item cards: {len(cards)}")

    news = []

    for card in cards:
        player_el = card.select_one(".player-name")
        headline_el = card.select_one("h3")
        summary_el = card.select_one(".news-content")
        timestamp_el = card.select_one(".timestamp")

        player = clean_text(player_el.get_text(" ", strip=True) if player_el else "")
        headline = clean_text(headline_el.get_text(" ", strip=True) if headline_el else "")
        summary = clean_text(summary_el.get_text(" ", strip=True) if summary_el else "")
        timestamp = clean_text(timestamp_el.get_text(" ", strip=True) if timestamp_el else "")

        if not player and not headline and not summary:
            continue

        uid = f"{player}|{headline}|{summary[:120]}"

        news.append(
            {
                "id": uid,
                "player": player or "MLB News",
                "headline": headline or "Player News Update",
                "summary": summary or "No summary available.",
                "timestamp": timestamp,
                "link": NEWS_URL,
            }
        )

    if not news:
        debug_path = os.path.join(STATE_DIR, "fantasypros_debug.html")
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        raise RuntimeError("Could not parse any FantasyPros player news items")

    return news


class NewsBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.posted = load_ids()
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

        items = await asyncio.to_thread(fetch_news)
        print(f"[NEWS] Parsed items: {len(items)}")
        print(f"[NEWS] Posted ID count: {len(self.posted)}")

        new_posts = 0

        for item in reversed(items):
            if item["id"] in self.posted:
                continue

            embed = discord.Embed(
                title=item["headline"],
                description=item["summary"],
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

            self.posted.add(item["id"])
            new_posts += 1
            print(f"[NEWS] Posted: {item['player']} | {item['headline'][:100]}")

        save_ids(self.posted)
        print(f"[NEWS] Done. New posts: {new_posts}")


async def start_news_bot():
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN (or NEWS_BOT_TOKEN) is not set")

    if NEWS_CHANNEL_ID == 0:
        raise RuntimeError("NEWS_CHANNEL_ID is not set")

    bot = NewsBot()
    await bot.start(DISCORD_TOKEN)

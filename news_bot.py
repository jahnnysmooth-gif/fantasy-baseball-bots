import asyncio
import discord
import requests
from bs4 import BeautifulSoup
import os
import json

TOKEN = os.getenv("DISCORD_TOKEN")
NEWS_CHANNEL_ID = int(os.getenv("NEWS_CHANNEL_ID"))

NEWS_URL = "https://www.fantasypros.com/mlb/player-news.php"

STATE_FILE = "state/news_ids.json"

os.makedirs("state", exist_ok=True)


def load_ids():
    if not os.path.exists(STATE_FILE):
        return set()
    with open(STATE_FILE) as f:
        return set(json.load(f))


def save_ids(ids):
    with open(STATE_FILE, "w") as f:
        json.dump(list(ids), f)


def fetch_news():

    r = requests.get(NEWS_URL, timeout=30)

    soup = BeautifulSoup(r.text, "html.parser")

    cards = soup.select(".player-news-item")

    news = []

    for card in cards:

        player = card.select_one(".player-name")

        if not player:
            continue

        player = player.text.strip()

        headline = card.select_one("h3")

        if headline:
            headline = headline.text.strip()
        else:
            headline = ""

        summary = card.select_one(".news-content")

        if summary:
            summary = summary.text.strip()
        else:
            summary = ""

        timestamp = card.select_one(".timestamp")

        if timestamp:
            timestamp = timestamp.text.strip()
        else:
            timestamp = ""

        uid = f"{player}-{headline}"

        news.append(
            {
                "id": uid,
                "player": player,
                "headline": headline,
                "summary": summary,
                "timestamp": timestamp,
            }
        )

    return news


class NewsBot(discord.Client):

    async def on_ready(self):

        print(f"[NEWS] Logged in as {self.user}")

        self.posted = load_ids()

        self.channel = self.get_channel(NEWS_CHANNEL_ID)

        self.loop.create_task(self.news_loop())

    async def news_loop(self):

        await self.wait_until_ready()

        while True:

            try:

                items = fetch_news()

                new_posts = 0

                for item in reversed(items):

                    if item["id"] in self.posted:
                        continue

                    embed = discord.Embed(
                        title=item["headline"],
                        description=item["summary"],
                        url=NEWS_URL,
                        color=0x2ecc71,
                    )

                    embed.set_author(name=item["player"])

                    if item["timestamp"]:
                        embed.set_footer(text=item["timestamp"])

                    await self.channel.send(embed=embed)

                    self.posted.add(item["id"])

                    new_posts += 1

                save_ids(self.posted)

                print(f"[NEWS] New posts: {new_posts}")

            except Exception as e:

                print("[NEWS] Error:", e)

            await asyncio.sleep(180)


intents = discord.Intents.default()

client = NewsBot(intents=intents)

client.run(TOKEN)

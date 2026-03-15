import discord
import json
import os
from pathlib import Path

TOKEN = os.getenv("DISCORD_TOKEN")

FORUM_CHANNEL_ID = 1480692093893476523

STATE_DIR = Path("state/player_profiles")
STATE_DIR.mkdir(parents=True, exist_ok=True)

THREAD_MAP_FILE = STATE_DIR / "player_threads.json"

intents = discord.Intents.default()
intents.guilds = True

client = discord.Client(intents=intents)


@client.event
async def on_ready():

    print("Scanning player threads...")

    forum = client.get_channel(FORUM_CHANNEL_ID)

    mapping = {}

    # Active threads
    for thread in forum.threads:
        player = thread.name.split(" — ")[0]
        mapping[player] = thread.id

    # Archived threads
    async for thread in forum.archived_threads(limit=None):
        player = thread.name.split(" — ")[0]
        mapping[player] = thread.id

    with open(THREAD_MAP_FILE, "w") as f:
        json.dump(mapping, f, indent=2)

    print(f"Saved {len(mapping)} player threads")

    await client.close()


client.run(TOKEN)
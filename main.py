import asyncio
import os
import json

from player_profiles_bot import start_player_profiles_bot
from closer_bot import start_closer_bot
from injury_bot import start_injury_bot
from lineup_bot import start_lineup_bot
from news_bot import start_news_bot


# try to use persistent disk if available
DATA_DIR = "/data"

if os.path.exists(DATA_DIR) and os.access(DATA_DIR, os.W_OK):
    NEWS_STATE_FILE = f"{DATA_DIR}/news_posted_ids.json"
else:
    # fallback to local directory
    DATA_DIR = "."
    NEWS_STATE_FILE = f"{DATA_DIR}/news_posted_ids.json"


# create state file if missing
if not os.path.exists(NEWS_STATE_FILE):
    with open(NEWS_STATE_FILE, "w") as f:
        json.dump([], f)


async def main():
    await asyncio.gather(
        start_player_profiles_bot(),
        start_closer_bot(),
        start_injury_bot(),
        start_lineup_bot(),
        start_news_bot(),
    )


if __name__ == "__main__":
    asyncio.run(main())

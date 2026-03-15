import asyncio
import os
import json

from player_profiles_bot import start_player_profiles_bot
from closer_bot import start_closer_bot
from injury_bot import start_injury_bot
from lineup_bot import start_lineup_bot
from news_bot import start_news_bot


# persistent state file for news bot
NEWS_STATE_FILE = "/data/news_posted_ids.json"

if not os.path.exists(NEWS_STATE_FILE):
    with open(NEWS_STATE_FILE, "w") as f:
        json.dump([], f)


async def main():
    await asyncio.gather(
        start_player_profiles_bot(),
        start_closer_bot(),
        start_injury_bot(),
        start_lineup_bot(),
        start_news_bot(),   # NEW NEWS BOT
    )


if __name__ == "__main__":
    asyncio.run(main())

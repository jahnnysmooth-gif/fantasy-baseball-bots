import asyncio

from player_profiles_bot import start_player_profiles_bot
from closer_bot import start_closer_bot
from mlb_rss_news_bot import start_mlb_rss_news_bot


async def main():
    await asyncio.gather(
        start_player_profiles_bot(),
        start_closer_bot(),
        asyncio.to_thread(start_mlb_rss_news_bot),
    )


if __name__ == "__main__":
    asyncio.run(main())

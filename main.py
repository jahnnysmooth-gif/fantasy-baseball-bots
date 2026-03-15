import asyncio

from player_profiles_bot import start_player_profiles_bot
from closer_bot import start_closer_bot
from injury_bot import start_injury_bot
from lineup_bot import start_lineup_bot


async def main():
    await asyncio.gather(
        start_player_profiles_bot(),
        start_closer_bot(),
        start_injury_bot(),
        start_lineup_bot(),
    )


if __name__ == "__main__":
    asyncio.run(main())

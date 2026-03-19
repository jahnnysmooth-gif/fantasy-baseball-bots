import asyncio
import traceback

from player_profiles_bot import start_player_profiles_bot
from closer_bot import start_closer_bot
from injury_bot import start_injury_bot
from lineup_bot import start_lineup_bot


def log(msg: str) -> None:
    print(f"[MAIN] {msg}", flush=True)


async def run_forever(name, coro_func):
    while True:
        try:
            log(f"Starting {name}")
            await coro_func()
            log(f"{name} exited unexpectedly without error. Restarting in 10 seconds.")
        except Exception as e:
            log(f"{name} crashed: {e}")
            traceback.print_exc()

        await asyncio.sleep(10)


async def main():
    await asyncio.gather(
        run_forever("player_profiles_bot", start_player_profiles_bot),
        run_forever("closer_bot", start_closer_bot),
        run_forever("injury_bot", start_injury_bot),
        run_forever("lineup_bot", start_lineup_bot),
    )


if __name__ == "__main__":
    asyncio.run(main())

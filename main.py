import asyncio
import os
import sys
import traceback

from player_profiles_bot import start_player_profiles_bot
from closer_bot import start_closer_bot
from injury_bot import start_injury_bot
from lineup_bot import start_lineup_bot


RESTART_DELAY_SECONDS = 10


def log(msg: str) -> None:
    print(f"[MAIN] {msg}", flush=True)


def handle_asyncio_exception(loop, context) -> None:
    msg = context.get("message", "Unhandled asyncio exception")
    exc = context.get("exception")
    log(msg)
    if exc is not None:
        traceback.print_exception(type(exc), exc, exc.__traceback__)
    else:
        log(repr(context))


async def run_forever(name: str, coro_func):
    while True:
        try:
            log(f"Starting {name}")
            await coro_func()
            log(
                f"{name} exited without raising an exception. "
                f"Restarting in {RESTART_DELAY_SECONDS} seconds."
            )
        except asyncio.CancelledError:
            log(f"{name} was cancelled")
            raise
        except Exception as e:
            log(f"{name} crashed: {e!r}")
            traceback.print_exc()

        log(f"Sleeping {RESTART_DELAY_SECONDS} seconds before restarting {name}")
        await asyncio.sleep(RESTART_DELAY_SECONDS)


async def main() -> None:
    log("Booting service")
    log(f"Python version: {sys.version.split()[0]}")
    log(f"Working directory: {os.getcwd()}")

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_asyncio_exception)

    tasks = [
        asyncio.create_task(run_forever("player_profiles_bot", start_player_profiles_bot)),
        asyncio.create_task(run_forever("closer_bot", start_closer_bot)),
        asyncio.create_task(run_forever("injury_bot", start_injury_bot)),
        asyncio.create_task(run_forever("lineup_bot", start_lineup_bot)),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for index, result in enumerate(results):
        if isinstance(result, Exception):
            log(f"Task {index} ended with exception: {result!r}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Service stopped by user")
    except Exception as e:
        log(f"Fatal top-level crash: {e!r}")
        traceback.print_exc()
        raise

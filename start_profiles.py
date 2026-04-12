#!/usr/bin/env python3
"""
Standalone launcher for Player Profiles Bot
Run this on a separate Render service for isolated memory and scaling
"""
import asyncio
import sys
import traceback

print("=== PLAYER PROFILES BOT STARTING ===", flush=True)

try:
    from player_profiles_bot import start_player_profiles_bot
    print("Loaded player_profiles_bot module", flush=True)
except Exception as e:
    print(f"IMPORT CRASH: {e!r}", flush=True)
    traceback.print_exc()
    sys.exit(1)

RESTART_DELAY_SECONDS = 30


def log(msg: str) -> None:
    print(f"[PROFILES] {msg}", flush=True)


async def main():
    while True:
        try:
            log("Starting player profiles bot")
            await start_player_profiles_bot()
            log(f"Bot exited cleanly. Restarting in {RESTART_DELAY_SECONDS}s")
        except asyncio.CancelledError:
            log("Bot cancelled")
            raise
        except Exception as e:
            log(f"Bot crashed: {e!r}")
            traceback.print_exc()

        log(f"Restarting in {RESTART_DELAY_SECONDS}s")
        await asyncio.sleep(RESTART_DELAY_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Service stopped by user")
    except Exception as e:
        log(f"Fatal crash: {e!r}")
        traceback.print_exc()
        raise

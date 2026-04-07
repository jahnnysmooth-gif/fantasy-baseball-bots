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


async def main():
    """Run the player profiles bot"""
    print("Starting player profiles bot...", flush=True)
    try:
        await start_player_profiles_bot()
    except KeyboardInterrupt:
        print("Bot stopped by user", flush=True)
    except Exception as e:
        print(f"Bot crashed: {e!r}", flush=True)
        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())

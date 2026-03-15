import os

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))

import os
import asyncio
import re
import json
import hashlib
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
import requests
from bs4 import BeautifulSoup

DISCORD_TOKEN = os.getenv("INJURY_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("INJURY_CHANNEL_ID", "0"))
POLL_INTERVAL = int(os.getenv("INJURY_POLL_INTERVAL", "900"))

ESPN_URL = "https://www.espn.com/mlb/injuries"
ET = ZoneInfo("America/New_York")

CUTOFF_DATE_STR = os.getenv("CUTOFF_DATE_ET", "2026-03-01")
CUTOFF_DATE_ET = datetime.strptime(CUTOFF_DATE_STR, "%Y-%m-%d").replace(tzinfo=ET)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
}

STATE_DIR = Path("state/injury")
STATE_FILE = STATE_DIR / "posted_injuries.json"
STATE_FILE_TMP = STATE_DIR / "posted_injuries.json.tmp"

TEAM_NAME_TO_ABBR = {
    "Arizona Diamondbacks": "ARI",
    "Athletics": "ATH",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

TEAM_COLORS = {
    "ARI": 0xA71930,
    "ATH": 0x003831,
    "ATL": 0xCE1141,
    "BAL": 0xDF4601,
    "BOS": 0xBD3039,
    "CHC": 0x0E3386,
    "CWS": 0x27251F,
    "CIN": 0xC6011F,
    "CLE": 0xE31937,
    "COL": 0x33006F,
    "DET": 0x0C2340,
    "HOU": 0xEB6E1F,
    "KC": 0x004687,
    "LAA": 0xBA0021,
    "LAD": 0x005A9C,
    "MIA": 0x00A3E0,
    "MIL": 0x12284B,
    "MIN": 0x002B5C,
    "NYM": 0x002D72,
    "NYY": 0x132448,
    "PHI": 0xE81828,
    "PIT": 0xFDB827,
    "SD": 0x2F241D,
    "SF": 0xFD5A1E,
    "SEA": 0x0C2C56,
    "STL": 0xC41E3A,
    "TB": 0x092C5C,
    "TEX": 0x003278,
    "TOR": 0x134A8E,
    "WSH": 0xAB0003,
}

TEAM_LOGOS = {
    "ARI": "https://a.espncdn.com/i/teamlogos/mlb/500/ari.png",
    "ATH": "https://a.espncdn.com/i/teamlogos/mlb/500/oak.png",
    "ATL": "https://a.espncdn.com/i/teamlogos/mlb/500/atl.png",
    "BAL": "https://a.espncdn.com/i/teamlogos/mlb/500/bal.png",
    "BOS": "https://a.espncdn.com/i/teamlogos/mlb/500/bos.png",
    "CHC": "https://a.espncdn.com/i/teamlogos/mlb/500/chc.png",
    "CWS": "https://a.espncdn.com/i/teamlogos/mlb/500/chw.png",
    "CIN": "https://a.espncdn.com/i/teamlogos/mlb/500/cin.png",
    "CLE": "https://a.espncdn.com/i/teamlogos/mlb/500/cle.png",
    "COL": "https://a.espncdn.com/i/teamlogos/mlb/500/col.png",
    "DET": "https://a.espncdn.com/i/teamlogos/mlb/500/det.png",
    "HOU": "https://a.espncdn.com/i/teamlogos/mlb/500/hou.png",
    "KC": "https://a.espncdn.com/i/teamlogos/mlb/500/kc.png",
    "LAA": "https://a.espncdn.com/i/teamlogos/mlb/500/laa.png",
    "LAD": "https://a.espncdn.com/i/teamlogos/mlb/500/lad.png",
    "MIA": "https://a.espncdn.com/i/teamlogos/mlb/500/mia.png",
    "MIL": "https://a.espncdn.com/i/teamlogos/mlb/500/mil.png",
    "MIN": "https://a.espncdn.com/i/teamlogos/mlb/500/min.png",
    "NYM": "https://a.espncdn.com/i/teamlogos/mlb/500/nym.png",
    "NYY": "https://a.espncdn.com/i/teamlogos/mlb/500/nyy.png",
    "PHI": "https://a.espncdn.com/i/teamlogos/mlb/500/phi.png",
    "PIT": "https://a.espncdn.com/i/teamlogos/mlb/500/pit.png",
    "SD": "https://a.espncdn.com/i/teamlogos/mlb/500/sd.png",
    "SF": "https://a.espncdn.com/i/teamlogos/mlb/500/sf.png",
    "SEA": "https://a.espncdn.com/i/teamlogos/mlb/500/sea.png",
    "STL": "https://a.espncdn.com/i/teamlogos/mlb/500/stl.png",
    "TB": "https://a.espncdn.com/i/teamlogos/mlb/500/tb.png",
    "TEX": "https://a.espncdn.com/i/teamlogos/mlb/500/tex.png",
    "TOR": "https://a.espncdn.com/i/teamlogos/mlb/500/tor.png",
    "WSH": "https://a.espncdn.com/i/teamlogos/mlb/500/wsh.png",
}

VALID_POSITIONS = {
    "SP", "RP", "P", "C", "1B", "2B", "3B", "SS",
    "LF", "CF", "RF", "OF", "DH", "INF", "UTIL"
}

VALID_STATUSES = {
    "60-Day-IL",
    "15-Day-IL",
    "10-Day-IL",
    "7-Day-IL",
    "Day-To-Day",
    "Out",
    "Suspension",
    "Bereavement",
    "Paternity",
}

DEFAULT_COLOR = 0x5865F2
MAX_UPDATE_LEN = 220
MAX_STORED_IDS = 5000

COMMENT_DATE_RE = re.compile(r"^([A-Z][a-z]{2}\s+\d{1,2}):")
PAGE_HEADER_TOKENS = {"NAME", "POS", "EST. RETURN DATE", "STATUS", "COMMENT"}


def log(msg: str) -> None:
    print(f"[INJURY] {msg}", flush=True)


def clean_text(text: str) -> str:
    return " ".join(text.split()).strip()


def clamp_update(text: str, max_len: int = MAX_UPDATE_LEN) -> str:
    text = clean_text(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def short_date(date_str: str) -> str:
    now_year = datetime.now(ET).year
    for fmt in ("%b %d", "%B %d"):
        try:
            dt = datetime.strptime(f"{date_str} {now_year}", f"{fmt} %Y")
            return dt.strftime("%b %d")
        except ValueError:
            continue
    return date_str


def should_run_now() -> bool:
    return True


def build_embed(item: dict) -> discord.Embed:
    team = item["team"]
    color = TEAM_COLORS.get(team, DEFAULT_COLOR)
    logo_url = TEAM_LOGOS.get(team)
    position = item["position"]

    status_title = "🚑 MLB INJURY UPDATE"
    if item["status"] == "60-Day-IL":
        status_title = "🧊 60-DAY IL"
    elif item["status"] == "Day-To-Day":
        status_title = "⚠️ DAY-TO-DAY"
    elif "IL" in item["status"]:
        status_title = "🚨 IL PLACEMENT"

    embed = discord.Embed(
        title=f"{item['player']} | {team} | {position}",
        color=color,
        timestamp=datetime.now(ET)
    )

    embed.description = f"**{status_title}**"

    embed.add_field(name="Status", value=f"`{item['status']}`", inline=True)
    embed.add_field(name="Est. Return", value=f"`{short_date(item['est_return'])}`", inline=True)
    embed.add_field(name="Source", value="`ESPN`", inline=True)
    embed.add_field(name="Update", value=clamp_update(item["comment"]), inline=False)

    if logo_url:
        embed.set_thumbnail(url=logo_url)

    embed.set_footer(text="ESPN MLB Injuries")
    return embed

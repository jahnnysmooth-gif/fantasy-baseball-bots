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

# Keep the same practical behavior as your current bot.
# You can override this in Render later if you ever want:
# CUTOFF_DATE_ET=2026-03-01
CUTOFF_DATE_STR = os.getenv("CUTOFF_DATE_ET", "2026-03-01")
CUTOFF_DATE_ET = datetime.strptime(CUTOFF_DATE_STR, "%Y-%m-%d").replace(tzinfo=ET)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
}

STATE_DIR = Path("state")
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
    print(f"[BOT] {msg}", flush=True)


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
    now_et = datetime.now(ET)
    return 7 <= now_et.hour < 24


def load_state() -> dict:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if not STATE_FILE.exists():
        return {"posted_ids": []}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        posted_ids = state.get("posted_ids", [])
        if not isinstance(posted_ids, list):
            posted_ids = []

        # Deduplicate while preserving order
        seen = set()
        ordered = []
        for item in posted_ids:
            if isinstance(item, str) and item not in seen:
                seen.add(item)
                ordered.append(item)

        return {"posted_ids": ordered[-MAX_STORED_IDS:]}
    except Exception as e:
        log(f"Failed to load state, starting fresh: {e}")
        return {"posted_ids": []}


def save_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    posted_ids = state.get("posted_ids", [])
    if not isinstance(posted_ids, list):
        posted_ids = []

    payload = {"posted_ids": posted_ids[-MAX_STORED_IDS:]}

    with open(STATE_FILE_TMP, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    STATE_FILE_TMP.replace(STATE_FILE)


def make_update_id(item: dict) -> str:
    raw = "|".join([
        item["team"],
        item["player"],
        item["position"],
        item["est_return"],
        item["status"],
        item["comment"],
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_html() -> str:
    response = requests.get(ESPN_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def parse_comment_date(comment: str) -> datetime | None:
    match = COMMENT_DATE_RE.match(comment)
    if not match:
        return None

    month_day = match.group(1)
    now_year = datetime.now(ET).year

    # Try current year first, then previous year.
    # This keeps the bot working across year changes without manual edits.
    for year in (now_year, now_year - 1):
        try:
            dt = datetime.strptime(f"{month_day} {year}", "%b %d %Y").replace(tzinfo=ET)

            # Guard against parsing something absurdly far in the future
            if dt > datetime.now(ET).replace(hour=23, minute=59, second=59, microsecond=0):
                continue

            return dt
        except ValueError:
            continue

    return None


def is_allowed_update(comment: str) -> bool:
    comment_dt = parse_comment_date(comment)
    if comment_dt is None:
        return False
    return comment_dt >= CUTOFF_DATE_ET


def looks_like_valid_row(player: str, position: str, est_return: str, status: str, comment: str, team_names: set[str]) -> bool:
    if not player or player in team_names or player in PAGE_HEADER_TOKENS:
        return False
    if position not in VALID_POSITIONS:
        return False
    if status not in VALID_STATUSES:
        return False
    if not est_return or est_return in PAGE_HEADER_TOKENS:
        return False
    if ":" not in comment:
        return False
    if not is_allowed_update(comment):
        return False
    return True


def parse_espn_injuries(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    lines = [clean_text(x) for x in soup.get_text("\n").splitlines()]
    lines = [x for x in lines if x]

    items = []
    team_names = set(TEAM_NAME_TO_ABBR.keys())

    start_index = 0
    for idx, line in enumerate(lines):
        if line in team_names:
            start_index = idx
            break

    i = start_index
    current_team = None

    while i < len(lines):
        line = lines[i]

        if line in team_names:
            current_team = line
            i += 1

            while i < len(lines) and lines[i] in PAGE_HEADER_TOKENS:
                i += 1
            continue

        if not current_team:
            i += 1
            continue

        if i + 4 >= len(lines):
            break

        player = lines[i]
        position = lines[i + 1]
        est_return = lines[i + 2]
        status = lines[i + 3]
        comment = lines[i + 4]

        if looks_like_valid_row(player, position, est_return, status, comment, team_names):
            items.append({
                "team_name": current_team,
                "team": TEAM_NAME_TO_ABBR[current_team],
                "player": player,
                "position": position,
                "est_return": est_return,
                "status": status,
                "comment": comment,
            })
            i += 5
            continue

        i += 1

    return items


def build_embed(item: dict) -> discord.Embed:
    team = item["team"]
    color = TEAM_COLORS.get(team, DEFAULT_COLOR)
    logo_url = TEAM_LOGOS.get(team)

    title = "🚑 MLB INJURY UPDATE"
    if item["status"] == "60-Day-IL":
        title = "🧊 60-DAY IL"
    elif item["status"] == "Day-To-Day":
        title = "⚠️ DAY-TO-DAY"
    elif "IL" in item["status"]:
        title = "🚨 IL PLACEMENT"

    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=datetime.now(ET)
    )
    embed.description = f"**{item['player']}**\n`{team} • {item['position']}`"
    embed.add_field(name="Status", value=f"`{item['status']}`", inline=True)
    embed.add_field(name="Est. Return", value=f"`{short_date(item['est_return'])}`", inline=True)
    embed.add_field(name="Source", value="`ESPN`", inline=True)
    embed.add_field(name="Update", value=clamp_update(item["comment"]), inline=False)

    if logo_url:
        embed.set_thumbnail(url=logo_url)

    embed.set_footer(text="ESPN MLB Injuries")
    return embed


intents = discord.Intents.default()
client = discord.Client(intents=intents)
background_task_started = False


async def post_allowed_updates() -> None:
    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        log("Channel not found.")
        return

    state = load_state()
    posted_ids_list = state.get("posted_ids", [])
    posted_ids_set = set(posted_ids_list)
    log(f"Loaded posted_ids: {len(posted_ids_list)}")

    try:
        html = fetch_html()
        items = parse_espn_injuries(html)
        log(f"Parsed {len(items)} allowed injury items")

        for item in items[:5]:
            log(f"Sample allowed item: {item}")
    except Exception as e:
        log(f"Failed to fetch/parse ESPN page: {e}")
        return

    if not items:
        log("No allowed items found.")
        return

    items.sort(
        key=lambda x: (
            parse_comment_date(x["comment"]) or CUTOFF_DATE_ET,
            x["team"],
            x["player"]
        )
    )

    new_items = []
    for item in items:
        update_id = make_update_id(item)
        item["update_id"] = update_id
        if update_id not in posted_ids_set:
            new_items.append(item)

    log(f"New items: {len(new_items)}")
    for item in new_items[:5]:
        log(f"Will post: {item['player']} {item['team']} {item['status']}")

    for item in new_items:
        try:
            embed = build_embed(item)
            await channel.send(embed=embed)

            posted_ids_set.add(item["update_id"])
            posted_ids_list.append(item["update_id"])

            log(f"Posted: {item['player']} | {item['team']} | {item['status']}")
            await asyncio.sleep(1.0)
        except Exception as e:
            log(f"Failed to post {item['player']}: {e}")

    # Deduplicate again while preserving order, then trim oldest
    seen = set()
    ordered = []
    for uid in posted_ids_list:
        if uid not in seen:
            seen.add(uid)
            ordered.append(uid)

    state["posted_ids"] = ordered[-MAX_STORED_IDS:]
    save_state(state)
    log(f"Saved posted_ids: {len(state['posted_ids'])}")


async def background_loop() -> None:
    await client.wait_until_ready()
    log("ESPN injury bot started")

    while not client.is_closed():
        if should_run_now():
            log("Running injury check")
            await post_allowed_updates()
        else:
            log("Outside allowed hours. Skipping check.")

        await asyncio.sleep(POLL_INTERVAL)


@client.event
async def on_ready():
    global background_task_started
    log(f"Logged in as {client.user}")

    if not background_task_started:
        background_task_started = True
        asyncio.create_task(background_loop())


async def start_injury_bot():
    if not DISCORD_TOKEN:
        raise RuntimeError("INJURY_BOT_TOKEN is not set")
    if not CHANNEL_ID:
        raise RuntimeError("INJURY_CHANNEL_ID is not set")

    log(f"Using cutoff date: {CUTOFF_DATE_ET.strftime('%Y-%m-%d')}")
    await client.start(DISCORD_TOKEN)

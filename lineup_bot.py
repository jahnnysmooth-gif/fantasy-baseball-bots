import os
import re
import json
import time
import asyncio
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import discord
import requests
from bs4 import BeautifulSoup

DISCORD_TOKEN = os.getenv("LINEUP_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("LINEUP_CHANNEL_ID", "0"))
POLL_INTERVAL = int(os.getenv("LINEUP_POLL_INTERVAL", "300"))

# Use Render persistent disk so duplicate protection survives redeploys/restarts
STATE_DIR = Path("/var/data/lineup")
STATE_FILE = STATE_DIR / "posted_lineups.json"
STATE_FILE_TMP = STATE_DIR / "posted_lineups.json.tmp"

LINEUPS_URL = "https://www.rotowire.com/baseball/daily-lineups.php"
ET = ZoneInfo("America/New_York")

VALID_TEAMS = {
    "ARI", "ATL", "BAL", "BOS", "CHC", "CWS", "CIN", "CLE", "COL", "DET",
    "HOU", "KC", "LAA", "LAD", "MIA", "MIL", "MIN", "NYM", "NYY", "ATH",
    "PHI", "PIT", "SD", "SF", "SEA", "STL", "TB", "TEX", "TOR", "WSH"
}

POSITIONS = {"C", "1B", "2B", "2B/SS", "3B", "SS", "LF", "CF", "RF", "DH"}

BAD_VALUES = {
    "RotoWire", "Alerts", "alert", "Menu", "Confirmed Lineup", "Expected Lineup",
    "Unknown Lineup", "L", "R", "S", "ERA", "MLB", "Baseball"
}

LINEUP_TYPES = {"Confirmed Lineup", "Expected Lineup", "Unknown Lineup"}

TEAM_COLORS = {
    "ARI": 0xA71930, "ATL": 0xCE1141, "BAL": 0xDF4601, "BOS": 0xBD3039,
    "CHC": 0x0E3386, "CWS": 0x27251F, "CIN": 0xC6011F, "CLE": 0xE31937,
    "COL": 0x33006F, "DET": 0x0C2340, "HOU": 0xEB6E1F, "KC": 0x004687,
    "LAA": 0xBA0021, "LAD": 0x005A9C, "MIA": 0x00A3E0, "MIL": 0x12284B,
    "MIN": 0x002B5C, "NYM": 0x002D72, "NYY": 0x0C2340, "ATH": 0x003831,
    "PHI": 0xE81828, "PIT": 0xFDB827, "SD": 0x2F241D, "SF": 0xFD5A1E,
    "SEA": 0x005C5C, "STL": 0xC41E3A, "TB": 0x092C5C, "TEX": 0x003278,
    "TOR": 0x134A8E, "WSH": 0xAB0003
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

TIME_RE = re.compile(r"^\d{1,2}:\d{2} [AP]M ET$")
WEATHER_HINT_RE = re.compile(r"(rain|precipitation|wind|mph|degrees|°)", re.IGNORECASE)


def log(msg: str) -> None:
    print(f"[BOT] {msg}", flush=True)


def load_state():
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            posted = data.get("posted", {})
            if not isinstance(posted, dict):
                posted = {}

            return {"posted": posted}
        except Exception as e:
            log(f"Failed to load state: {e}")

    return {"posted": {}}


def save_state(state):
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    with open(STATE_FILE_TMP, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    STATE_FILE_TMP.replace(STATE_FILE)


def within_run_window():
    now = datetime.now(ET)
    start = now.replace(hour=10, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=0, second=0, microsecond=0)
    return start <= now <= end


def fetch_page():
    log("Fetching RotoWire page...")
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(LINEUPS_URL, headers=headers, timeout=30)
    log(f"RotoWire status: {r.status_code}")
    r.raise_for_status()
    return r.text


def clean(text):
    return " ".join(text.split()).strip()


def get_lines(html):
    soup = BeautifulSoup(html, "html.parser")
    lines = [clean(x) for x in soup.get_text("\n").splitlines()]
    return [x for x in lines if x]


def split_game_blocks(lines):
    time_indexes = [i for i, line in enumerate(lines) if TIME_RE.match(line)]
    blocks = []

    for n, start_idx in enumerate(time_indexes):
        end_idx = time_indexes[n + 1] if n + 1 < len(time_indexes) else len(lines)
        game_time = lines[start_idx]
        block = lines[start_idx:end_idx]
        blocks.append((game_time, block))

    return blocks


def extract_lineup_from_block(block, start_idx):
    lineup = []
    i = start_idx + 1

    while i < len(block) and len(lineup) < 9:
        token = block[i]

        if token in LINEUP_TYPES:
            break
        if token.startswith("Umpire:"):
            break
        if token.startswith("LINE "):
            break
        if token.startswith("O/U"):
            break
        if TIME_RE.match(token):
            break

        if token in POSITIONS and i + 1 < len(block):
            player = block[i + 1]

            if (
                player
                and player not in BAD_VALUES
                and player not in POSITIONS
                and "$" not in player
                and "ERA" not in player
                and player not in VALID_TEAMS
                and not player.startswith("The ")
                and not player.startswith("Watch Now")
            ):
                lineup.append({
                    "name": player,
                    "pos": token
                })
                i += 2
                continue

        i += 1

    return lineup


def find_pitcher_in_block(block, lineup_idx):
    window = block[max(0, lineup_idx - 12):lineup_idx]

    for text in reversed(window):
        if (
            text not in BAD_VALUES
            and text not in VALID_TEAMS
            and "ERA" not in text
            and 2 <= len(text.split()) <= 4
            and not text.startswith("Umpire:")
            and not TIME_RE.match(text)
            and "Watch Now" not in text
            and "Tickets" not in text
        ):
            return text

    return None


def find_weather_in_block(block):
    for text in block:
        if WEATHER_HINT_RE.search(text):
            return text
    return None


def find_teams_in_block(block):
    teams = []
    for text in block:
        if text in VALID_TEAMS:
            teams.append(text)
            if len(teams) == 2:
                return teams[0], teams[1]
    return None, None


def parse_game_block(game_time, block):
    away_team, home_team = find_teams_in_block(block)
    if not away_team or not home_team:
        return []

    matchup = f"{away_team} @ {home_team}"
    weather = find_weather_in_block(block)

    lineup_markers = [(i, text) for i, text in enumerate(block) if text in LINEUP_TYPES]
    results = []

    for marker_num, (idx, lineup_type) in enumerate(lineup_markers[:2]):
        if lineup_type == "Unknown Lineup":
            continue

        team = away_team if marker_num == 0 else home_team
        lineup = extract_lineup_from_block(block, idx)

        if len(lineup) != 9:
            continue

        pitcher = find_pitcher_in_block(block, idx)

        results.append({
            "team": team,
            "matchup": matchup,
            "game_time": game_time,
            "ballpark": None,
            "weather": weather,
            "rain": None,
            "pitcher": pitcher,
            "lineup": lineup,
            "lineup_type": lineup_type
        })

    return results


def parse_lineups(lines):
    items = []

    for game_time, block in split_game_blocks(lines):
        items.extend(parse_game_block(game_time, block))

    deduped = {}
    for item in items:
        key = f"{item['matchup']}|{item['team']}"
        deduped[key] = item

    return list(deduped.values())


def fingerprint(item):
    enriched = dict(item)
    enriched["logo"] = TEAM_LOGOS.get(item["team"])
    raw = json.dumps(enriched, sort_keys=True)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def build_embed(item, is_update=False):
    team = item["team"]
    matchup = item.get("matchup")
    time_et = item.get("game_time")
    park = item.get("ballpark")
    weather = item.get("weather")
    rain = item.get("rain")
    pitcher = item.get("pitcher")
    lineup = item.get("lineup", [])
    lineup_type = item.get("lineup_type")
    logo = TEAM_LOGOS.get(team)

    date_str = datetime.now(ET).strftime("%B %d, %Y")

    if lineup_type == "Expected Lineup":
        title = f"👀 ⚾ {team} Expected Lineup"
    elif is_update:
        title = f"🔄 ⚾ {team} Lineup Updated"
    else:
        title = f"⚾ {team} Confirmed Lineup"

    lines = [
        f"**Matchup:** {matchup}",
        f"**Date:** {date_str}",
    ]

    if time_et:
        lines.append(f"**Game Time:** {time_et}")

    if park:
        lines.append(f"**Ballpark:** {park}")

    if weather:
        lines.append(f"**Weather:** {weather}")

    if rain not in (None, "", 0, "0"):
        lines.append(f"**Rain Risk:** {rain}%")

    lines.append("")

    if pitcher:
        lines.append(f"**SP:** {pitcher}")
        lines.append("")

    for i, p in enumerate(lineup, start=1):
        name = p.get("name", "")
        pos = p.get("pos", "")
        lines.append(f"**{i}.** {name} — {pos}")

    embed = discord.Embed(
        title=title,
        description="\n".join(lines),
        color=TEAM_COLORS.get(team, 0x5865F2),
        timestamp=datetime.now(ET)
    )

    if logo:
        embed.set_thumbnail(url=logo)

    embed.set_footer(text="Old ESPN Fantasy Baseball Boards")
    return embed


intents = discord.Intents.default()
client = discord.Client(intents=intents)
background_task_started = False


async def post_new_embed(channel, embed):
    message = await channel.send(embed=embed)
    return message.id


async def edit_existing_embed(channel, message_id, embed):
    message = channel.get_partial_message(message_id)
    await message.edit(embed=embed)


async def run_once():
    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        log("Channel not found.")
        return

    state = load_state()
    posted = state.get("posted", {})

    html = fetch_page()
    lines = get_lines(html)
    items = parse_lineups(lines)

    log(f"Parsed {len(items)} lineups")

    for item in items:
        key = f"{item['matchup']}|{item['team']}"
        fp = fingerprint(item)
        existing = posted.get(key)

        if existing and existing.get("fingerprint") == fp:
            log(f"Skipping unchanged {key}")
            continue

        embed = build_embed(item, is_update=existing is not None)

        try:
            if existing:
                log(f"Updating {key}")
                await edit_existing_embed(channel, existing["message_id"], embed)
                posted[key]["fingerprint"] = fp
            else:
                log(f"Posting {key}")
                msg_id = await post_new_embed(channel, embed)
                posted[key] = {
                    "fingerprint": fp,
                    "message_id": msg_id
                }

            save_state(state)
            await asyncio.sleep(1)

        except Exception as e:
            log(f"Failed on {key}: {e}")


async def background_loop():
    await client.wait_until_ready()
    log("Lineup bot started")

    while not client.is_closed():
        now_str = datetime.now(ET).strftime("%Y-%m-%d %I:%M:%S %p %Z")
        log(f"Run started at {now_str}")

        if not within_run_window():
            log("Outside run window (10AM–11PM ET). Sleeping 10 minutes.")
            await asyncio.sleep(600)
            continue

        try:
            await run_once()
        except Exception as e:
            log(f"Error: {e}")

        log(f"Sleeping {POLL_INTERVAL} seconds")
        await asyncio.sleep(POLL_INTERVAL)


@client.event
async def on_ready():
    global background_task_started
    log(f"Logged in as {client.user}")

    if not background_task_started:
        background_task_started = True
        asyncio.create_task(background_loop())


async def start_lineup_bot():
    if not DISCORD_TOKEN:
        raise RuntimeError("LINEUP_BOT_TOKEN is not set")
    if not CHANNEL_ID:
        raise RuntimeError("LINEUP_CHANNEL_ID is not set")

    await client.start(DISCORD_TOKEN)

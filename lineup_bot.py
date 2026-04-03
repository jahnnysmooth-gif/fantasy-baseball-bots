import os
import re
import json
import asyncio
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import discord
import requests
from bs4 import BeautifulSoup

DISCORD_TOKEN = os.getenv("PETER_GAMMONS_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("LINEUP_CHANNEL_ID", "0"))
POLL_INTERVAL = int(os.getenv("LINEUP_POLL_INTERVAL", "300"))

# Prefer Render persistent disk if mounted
DATA_DIR = Path("/data") if Path("/data").exists() and os.access("/data", os.W_OK) else Path(".")
STATE_DIR = DATA_DIR / "state" / "lineup"
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
    print(f"[LINEUP] {msg}", flush=True)


def load_state():
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            posted = data.get("posted", {})
            if not isinstance(posted, dict):
                posted = {}

            normalized_posted = {}
            for key, value in posted.items():
                if not isinstance(key, str) or not isinstance(value, dict):
                    continue

                fingerprint = value.get("fingerprint")
                message_id = value.get("message_id")

                if not isinstance(fingerprint, str):
                    continue

                if message_id is not None and not isinstance(message_id, int):
                    continue

                normalized_posted[key] = {
                    "fingerprint": fingerprint,
                    "message_id": message_id,
                }

            return {"posted": normalized_posted}
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
    start = now.replace(hour=7, minute=0, second=0, microsecond=0)
    return now >= start


def seconds_until_window_open():
    """Return seconds until 7 AM ET if currently outside the run window."""
    from datetime import timedelta
    now = datetime.now(ET)
    next_open = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now >= next_open:
        next_open += timedelta(days=1)
    return max(0, (next_open - now).total_seconds())


def last_game_has_started(items):
    """Return True if the latest scheduled game time is in the past."""
    now = datetime.now(ET)
    today_str = now.strftime("%Y-%m-%d")
    latest = None

    for item in items:
        raw = item.get("game_time", "")
        if not raw:
            continue
        try:
            # game_time is like "10:10 PM ET"
            dt = datetime.strptime(f"{today_str} {raw.replace(' ET', '')}", "%Y-%m-%d %I:%M %p")
            dt = dt.replace(tzinfo=ET)
            if latest is None or dt > latest:
                latest = dt
        except ValueError:
            continue

    if latest is None:
        return False

    return now >= latest


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
                lineup.append({"name": player, "pos": token})
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
            "lineup_type": lineup_type,
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

    if lineup_type == "Confirmed Lineup":
        lines.append("**Status:** ✅ Confirmed")
    else:
        lines.append("**Status:** 🟡 Projected")

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
        timestamp=datetime.now(ET),
    )

    if logo:
        embed.set_thumbnail(url=logo)

    embed.set_footer(text="Old ESPN Fantasy Baseball Boards")
    return embed


intents = discord.Intents.default()
client = discord.Client(intents=intents)
background_task = None


async def post_new_embed(channel, embed):
    message = await channel.send(embed=embed)
    return message.id


async def edit_existing_embed(channel, message_id, embed):
    message = channel.get_partial_message(message_id)
    await message.edit(embed=embed)


async def run_once(html=None, lines=None, items=None):
    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        log("Channel not found.")
        return

    state = load_state()
    posted = state.get("posted", {})

    if items is None:
        html = fetch_page()
        lines = get_lines(html)
        items = parse_lineups(lines)

    log(f"Parsed {len(items)} lineups")

    today_key = datetime.now(ET).strftime("%Y-%m-%d")

    # First-run safeguard:
    # If no saved lineup state exists yet, seed all current lineups without posting.
    if not posted and items:
        log("First run detected. Seeding current lineups without posting.")

        for item in items:
            key = f"{today_key}|{item['matchup']}|{item['team']}"
            fp = fingerprint(item)
            posted[key] = {
                "fingerprint": fp,
                "message_id": None,
            }

        save_state(state)
        log(f"Seeded {len(posted)} lineup entries. No posts sent.")
        return

    for item in items:
        key = f"{today_key}|{item['matchup']}|{item['team']}"
        fp = fingerprint(item)
        existing = posted.get(key)

        if existing and existing.get("fingerprint") == fp:
            log(f"Skipping unchanged {key}")
            continue

        embed = build_embed(
            item,
            is_update=existing is not None and existing.get("message_id") is not None,
        )

        try:
            if existing and existing.get("message_id"):
                log(f"Updating {key}")
                await edit_existing_embed(channel, existing["message_id"], embed)
                posted[key]["fingerprint"] = fp
            else:
                log(f"Posting {key}")
                msg_id = await post_new_embed(channel, embed)
                posted[key] = {
                    "fingerprint": fp,
                    "message_id": msg_id,
                }

            save_state(state)
            await asyncio.sleep(1)

        except Exception as e:
            log(f"Failed on {key}: {e}")


def prune_old_state(state, keep_days=3):
    posted = state.get("posted", {})
    if not isinstance(posted, dict) or not posted:
        return False

    today = datetime.now(ET).date()
    keys_to_delete = []

    for key in posted.keys():
        parts = key.split("|", 1)
        if len(parts) != 2:
            continue

        date_part = parts[0]
        try:
            entry_date = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            continue

        age = (today - entry_date).days
        if age > keep_days:
            keys_to_delete.append(key)

    for key in keys_to_delete:
        posted.pop(key, None)

    if keys_to_delete:
        log(f"Pruned {len(keys_to_delete)} old lineup state entries")
        return True

    return False


async def background_loop():
    await client.wait_until_ready()
    log("Lineup bot started")

    while not client.is_closed():
        try:
            now_str = datetime.now(ET).strftime("%Y-%m-%d %I:%M:%S %p %Z")
            log(f"Run started at {now_str}")

            state = load_state()
            if prune_old_state(state):
                save_state(state)

            if not within_run_window():
                secs = seconds_until_window_open()
                from datetime import timedelta
                wake_dt = datetime.now(ET) + timedelta(seconds=secs)
                wake_time = wake_dt.strftime("%I:%M %p ET")
                log(f"Outside run window. Sleeping until {wake_time} ({int(secs)}s).")
                await asyncio.sleep(secs)
                continue

            html = fetch_page()
            lines = get_lines(html)
            items = parse_lineups(lines)

            await run_once(html=html, lines=lines, items=items)

            if last_game_has_started(items):
                secs = seconds_until_window_open()
                from datetime import timedelta
                wake_dt = datetime.now(ET) + timedelta(seconds=secs)
                wake_time = wake_dt.strftime("%I:%M %p ET")
                log(f"Last game has started. Sleeping until {wake_time} ({int(secs)}s).")
                await asyncio.sleep(secs)
                continue

        except Exception as e:
            log(f"Background loop crashed: {e}")

        try:
            log(f"Sleeping {POLL_INTERVAL} seconds")
            await asyncio.sleep(POLL_INTERVAL)
        except Exception as e:
            log(f"Sleep interrupted: {e}")
            await asyncio.sleep(30)


@client.event
async def on_ready():
    global background_task
    log(f"Logged in as {client.user}")
    await client.change_presence(status=discord.Status.invisible)

    if background_task is None or background_task.done():
        background_task = asyncio.create_task(background_loop())
        log("Background task created")


@client.event
async def on_disconnect():
    log("Discord connection lost")


@client.event
async def on_resumed():
    log("Discord session resumed")

    global background_task
    if background_task is None or background_task.done():
        background_task = asyncio.create_task(background_loop())
        log("Background task recreated after resume")


@client.event
async def on_error(event, *args, **kwargs):
    log(f"Unhandled Discord event error in: {event}")


async def start_lineup_bot():
    if not DISCORD_TOKEN:
        raise RuntimeError("PETER_GAMMONS_BOT_TOKEN is not set")
    if not CHANNEL_ID:
        raise RuntimeError("LINEUP_CHANNEL_ID is not set")

    log(f"Using state file: {STATE_FILE}")
    await client.start(DISCORD_TOKEN, reconnect=True)

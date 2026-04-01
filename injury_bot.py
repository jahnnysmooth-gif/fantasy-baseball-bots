import os
import asyncio
import re
import json
import hashlib
import random
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import discord

DISCORD_TOKEN = os.getenv("INJURY_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("INJURY_CHANNEL_ID", "0"))
POLL_INTERVAL_MIN = 180   # 3 minutes
POLL_INTERVAL_MAX = 300   # 5 minutes

ESPN_NEWS_URL = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news?limit=50"
ET = ZoneInfo("America/New_York")
TEST_MODE = os.getenv("INJURY_TEST_MODE", "").lower() in ("1", "true", "yes")

ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
player_headshot_index = None

# Use Render persistent disk so duplicate protection survives redeploys/restarts
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

DEFAULT_COLOR = 0x5865F2
MAX_UPDATE_LEN = 220
MAX_STORED_IDS = 5000

INJURY_KEYWORDS = {
    "injured", "il", "day-to-day", "dtd", "placed on", "activated",
    "out for", "fracture", "strain", "sprain", "surgery", "torn",
    "inflammation", "concussion", "suspension", "bereavement", "paternity",
}


def log(msg: str) -> None:
    print(f"[INJURY] {msg}", flush=True)


def clean_text(text: str) -> str:
    return " ".join(text.split()).strip()


def normalize_team_abbr(team: str) -> str:
    key = str(team or "").strip().upper()
    alias_map = {
        "AZ": "ARI", "ARI": "ARI", "CHW": "CWS", "CWS": "CWS",
        "WAS": "WSH", "WSN": "WSH", "WSH": "WSH", "TBR": "TB", "TB": "TB",
        "KCR": "KC", "KC": "KC", "SDP": "SD", "SD": "SD",
        "SFG": "SF", "SF": "SF", "OAK": "ATH", "ATH": "ATH",
    }
    return alias_map.get(key, key)


def normalize_lookup_name(name: str) -> str:
    if not name:
        return ""
    cleaned = name.lower()
    for ch in [".", ",", "'", "`", "-", "_", "(", ")", "[", "]"]:
        cleaned = cleaned.replace(ch, " ")
    return " ".join(cleaned.split())


def load_player_headshot_index() -> dict:
    global player_headshot_index
    if player_headshot_index is not None:
        return player_headshot_index

    player_headshot_index = {}
    if not os.path.exists(ESPN_PLAYER_IDS_PATH):
        log(f"Player ID file not found: {ESPN_PLAYER_IDS_PATH}")
        return player_headshot_index

    try:
        with open(ESPN_PLAYER_IDS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        log(f"Could not load player ID file: {e}")
        return player_headshot_index

    if not isinstance(raw, dict):
        log("Player ID file is not a dict mapping of names to ids/headshots")
        return player_headshot_index

    for raw_name, raw_value in raw.items():
        entries = raw_value if isinstance(raw_value, list) else [raw_value]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            headshot_url = entry.get("headshot_url")
            espn_id = entry.get("espn_id")
            if not headshot_url and espn_id:
                headshot_url = f"https://a.espncdn.com/i/headshots/mlb/players/full/{espn_id}.png"
            if not headshot_url:
                continue
            team = entry.get("team")
            payload = {
                "name": raw_name,
                "team": normalize_team_abbr(team),
                "headshot_url": headshot_url,
                "espn_id": entry.get("espn_id"),
            }
            player_headshot_index.setdefault(raw_name, []).append(payload)
            normalized = normalize_lookup_name(raw_name)
            if normalized:
                player_headshot_index.setdefault(normalized, []).append(payload)

    log(f"Loaded player headshot index from {ESPN_PLAYER_IDS_PATH}")
    return player_headshot_index


def choose_headshot_entry(entries, team: str = None):
    if not entries:
        return None
    normalized_team = normalize_team_abbr(team) if team else None
    if normalized_team:
        for entry in entries:
            if isinstance(entry, dict) and normalize_team_abbr(entry.get("team")) == normalized_team:
                return entry
    for entry in entries:
        if isinstance(entry, dict) and entry.get("headshot_url"):
            return entry
    return None


def find_headshot_entry_by_last_name(index: dict, name: str, team: str = None):
    normalized_name = normalize_lookup_name(name)
    if not normalized_name:
        return None

    parts = normalized_name.split()
    if not parts:
        return None

    last_name = parts[-1]
    matches = []
    seen_urls = set()
    for key, entries in index.items():
        key_parts = normalize_lookup_name(key).split()
        if not key_parts or key_parts[-1] != last_name:
            continue
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            url = entry.get("headshot_url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            matches.append(entry)

    if not matches:
        return None

    normalized_team = normalize_team_abbr(team) if team else None
    if normalized_team:
        team_matches = [e for e in matches if normalize_team_abbr(e.get("team")) == normalized_team]
        if team_matches:
            return team_matches[0]

    if len(matches) == 1:
        return matches[0]

    return None


def get_player_headshot(name: str, team: str = None) -> str | None:
    index = load_player_headshot_index()
    if not index or not name:
        return None

    exact = choose_headshot_entry(index.get(name), team)
    if exact:
        return exact.get("headshot_url")

    normalized = normalize_lookup_name(name)
    normalized_match = choose_headshot_entry(index.get(normalized), team)
    if normalized_match:
        return normalized_match.get("headshot_url")

    last_name_match = find_headshot_entry_by_last_name(index, name, team)
    if last_name_match:
        return last_name_match.get("headshot_url")

    return None


def clamp_update(text: str, max_len: int = MAX_UPDATE_LEN) -> str:
    text = clean_text(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"



def should_run_now() -> bool:
    return True


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


def normalize_posted_ids(posted_ids_list: list[str]) -> list[str]:
    seen = set()
    ordered = []
    for uid in posted_ids_list:
        if uid not in seen:
            seen.add(uid)
            ordered.append(uid)
    return ordered[-MAX_STORED_IDS:]


def make_update_id(item: dict) -> str:
    raw = "|".join([
        item.get("team", ""),
        item.get("player", ""),
        item.get("headline", ""),
        item.get("published", ""),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def is_injury_item(article: dict) -> bool:
    """Return True if the news article looks injury/transaction related."""
    categories = article.get("categories", [])
    for cat in categories:
        ctype = str(cat.get("type", "")).lower()
        desc = str(cat.get("description", "")).lower()
        if ctype in ("injury", "transaction") or "injur" in desc or "transaction" in desc:
            return True
    headline = str(article.get("headline", "")).lower()
    description = str(article.get("description", "")).lower()
    combined = headline + " " + description
    return any(kw in combined for kw in INJURY_KEYWORDS)


def parse_published(ts: str) -> datetime | None:
    """Parse ESPN's ISO 8601 published timestamp to a timezone-aware datetime."""
    if not ts:
        return None
    try:
        # ESPN returns e.g. "2026-04-01T14:32:00Z"
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone(ET)
    except Exception:
        return None


def extract_team_from_article(article: dict) -> tuple[str, str]:
    """Return (team_abbr, team_full_name) from the article's categories."""
    for cat in article.get("categories", []):
        if cat.get("type", "").lower() in ("team",):
            name = cat.get("description", "")
            abbr = TEAM_NAME_TO_ABBR.get(name, "")
            if abbr:
                return abbr, name
    # Fall back: look in links for a team slug
    for link in article.get("links", {}).get("web", {}).values() if isinstance(article.get("links"), dict) else []:
        pass
    return "", ""


def extract_player_from_article(article: dict) -> str:
    """Return player name from athletes category if present."""
    for cat in article.get("categories", []):
        if cat.get("type", "").lower() == "athlete":
            return cat.get("description", "")
    return ""


async def fetch_injury_items() -> list[dict]:
    """Fetch ESPN MLB news feed and return injury/transaction items sorted by published time (newest first)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(ESPN_NEWS_URL, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
    except Exception as e:
        log(f"ESPN API fetch failed: {e}")
        return []

    articles = data.get("articles", [])
    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc.astimezone(ET) if not TEST_MODE else None
    items = []

    for article in articles:
        if not is_injury_item(article):
            continue

        published_str = article.get("published", "")
        published_dt = parse_published(published_str)

        # In test mode: only include items from the last 6 hours
        if TEST_MODE and published_dt:
            age_hours = (now_utc - published_dt.astimezone(timezone.utc)).total_seconds() / 3600
            if age_hours > 6:
                continue

        team_abbr, team_name = extract_team_from_article(article)
        player = extract_player_from_article(article)
        headline = clean_text(article.get("headline", ""))
        description = clean_text(article.get("description", "") or article.get("headline", ""))

        headshot_url = None
        images = article.get("images", [])
        if images and isinstance(images, list):
            headshot_url = images[0].get("url")

        items.append({
            "team": team_abbr,
            "team_name": team_name,
            "player": player,
            "headline": headline,
            "description": description,
            "published": published_str,
            "published_dt": published_dt,
            "headshot_url": headshot_url,
        })

    # Sort newest first
    items.sort(key=lambda x: x["published_dt"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return items




def build_embed(item: dict) -> discord.Embed:
    team = item.get("team", "")
    color = TEAM_COLORS.get(team, DEFAULT_COLOR)
    logo_url = TEAM_LOGOS.get(team)
    headline = item.get("headline", "MLB Injury Update")

    # Derive a status emoji from the headline
    hl_lower = headline.lower()
    if "60-day" in hl_lower:
        status_title = "🧊 60-DAY IL"
    elif "15-day" in hl_lower or "10-day" in hl_lower or "7-day" in hl_lower:
        status_title = "🚨 IL PLACEMENT"
    elif "day-to-day" in hl_lower or "dtd" in hl_lower:
        status_title = "⚠️ DAY-TO-DAY"
    elif "activated" in hl_lower or "reinstated" in hl_lower:
        status_title = "✅ ACTIVATED"
    elif "suspend" in hl_lower:
        status_title = "🔴 SUSPENSION"
    else:
        status_title = "🚑 MLB INJURY UPDATE"

    embed = discord.Embed(color=color, timestamp=datetime.now(ET))

    author_name = item.get("player") or headline
    if team:
        author_name = f"{author_name} | {team}"

    if logo_url:
        embed.set_author(name=author_name, icon_url=logo_url)
    else:
        embed.set_author(name=author_name)

    embed.description = f"**{status_title}**"
    embed.add_field(name="Update", value=clamp_update(item.get("description", headline)), inline=False)

    # Reported time
    published_dt = item.get("published_dt")
    if published_dt:
        reported_str = published_dt.strftime("%-I:%M %p ET")
        embed.add_field(name="🕐 Reported", value=reported_str, inline=True)

    embed.add_field(name="Source", value="`ESPN`", inline=True)

    # Headshot: try player lookup first, then article image, then team logo
    headshot_url = None
    player = item.get("player")
    if player:
        headshot_url = get_player_headshot(player, team)
    if not headshot_url:
        headshot_url = item.get("headshot_url")
    if headshot_url:
        embed.set_thumbnail(url=headshot_url)
    elif logo_url:
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

    items = await fetch_injury_items()
    log(f"Fetched {len(items)} injury items from ESPN API")

    if not items:
        log("No injury items found.")
        return

    if TEST_MODE:
        log(f"TEST MODE: bypassing dedup, posting all {len(items)} items from last 6 hours")
        for item in items:
            try:
                embed = build_embed(item)
                await channel.send(embed=embed)
                log(f"[TEST] Posted: {item.get('player') or item.get('headline')} | {item.get('team')} | {item.get('published_dt')}")
                await asyncio.sleep(1.0)
            except Exception as e:
                log(f"[TEST] Failed to post item: {e}")
        return

    current_ids = []
    new_items = []

    for item in items:
        update_id = make_update_id(item)
        item["update_id"] = update_id
        current_ids.append(update_id)

        if update_id not in posted_ids_set:
            new_items.append(item)

    # First-run safeguard: seed without posting
    if not posted_ids_list:
        state["posted_ids"] = normalize_posted_ids(current_ids)
        save_state(state)
        log(f"First run detected. Seeded posted_ids with {len(state['posted_ids'])} existing injuries. No posts sent.")
        return

    log(f"New items to post: {len(new_items)}")

    for item in new_items:
        try:
            embed = build_embed(item)
            await channel.send(embed=embed)

            posted_ids_set.add(item["update_id"])
            posted_ids_list.append(item["update_id"])
            state["posted_ids"] = normalize_posted_ids(posted_ids_list)
            save_state(state)

            log(f"Posted: {item.get('player') or item.get('headline')} | {item.get('team')} | {item.get('published_dt')}")
            await asyncio.sleep(1.0)
        except Exception as e:
            log(f"Failed to post item: {e}")

    state["posted_ids"] = normalize_posted_ids(posted_ids_list)
    save_state(state)
    log(f"Saved posted_ids: {len(state['posted_ids'])}")


async def background_loop() -> None:
    await client.wait_until_ready()
    log("ESPN injury bot started (API mode, 3–5 min random interval)")

    while not client.is_closed():
        if should_run_now():
            log("Running injury check")
            await post_allowed_updates()
        else:
            log("Outside allowed hours. Skipping check.")

        interval = random.randint(POLL_INTERVAL_MIN, POLL_INTERVAL_MAX)
        log(f"Next check in {interval}s ({interval // 60}m {interval % 60}s)")
        await asyncio.sleep(interval)


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

    log("Injury bot starting (ESPN API mode)")
    await client.start(DISCORD_TOKEN)

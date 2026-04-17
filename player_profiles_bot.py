import re
import math
import os
import json
import time
import asyncio
import random
import unicodedata
from datetime import datetime, date
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import discord
import pandas as pd
import requests
from bs4 import BeautifulSoup
from discord.ext import commands

# =========================
# CONFIG
# =========================
TOKEN = os.getenv("PLAYER_PROFILES_TOKEN")
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0"))
ANTHROPIC_API_KEY = os.getenv("PROFILES_REWRITE_KEY", "")

# Discord Channel IDs
FORUM_CHANNEL_ID = int(os.getenv("FORUM_CHANNEL_ID", "1480692093893476523"))
REQUEST_THREAD_ID = int(os.getenv("REQUEST_THREAD_ID", "1482429894003785882"))

PROFILE_SEASON = 2026
PREV_SEASON = 2025
PROFILE_YEAR_LABEL = 2026  # Outlook for rest of current season

ADP_URL = "https://www.fantasypros.com/mlb/adp/overall.php"
SEED_COUNT = 250
SEED_SLEEP_MIN_SECONDS = 600   # 10 min
SEED_SLEEP_MAX_SECONDS = 900   # 15 min

STATE_DIR = Path("state/player_profiles")
STATE_DIR.mkdir(parents=True, exist_ok=True)

SEED_STATE_FILE = STATE_DIR / "seed_state.json"
THREAD_COMMAND_STATE_FILE = STATE_DIR / "thread_command_state.json"
USER_RATE_LIMIT_FILE = STATE_DIR / "user_rate_limits.json"

THREAD_MAP_FILE = STATE_DIR / "player_threads.json"

MAX_PROFILE_REQUESTS_PER_DAY = 5  # For non-admins

if THREAD_MAP_FILE.exists():
    with open(THREAD_MAP_FILE) as f:
        PLAYER_THREADS = json.load(f)
else:
    PLAYER_THREADS = {}

BOT_TIMEZONE = "America/New_York"
OUTLOOK_UNLOCK_DATE = date(2026, 4, 15)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)
bot.remove_command("stats")

# =========================
# LOGGING
# =========================
def log_profiles(msg: str) -> None:
    print(f"[PROFILES] {msg}", flush=True)


def log_adp(msg: str) -> None:
    print(f"[ADP] {msg}", flush=True)


# =========================
# TEAM COLORS / ABBREVS / LOGOS
# =========================
TEAM_COLORS = {
    "Arizona Diamondbacks": 0xA71930,
    "Athletics": 0x003831,
    "Atlanta Braves": 0xCE1141,
    "Baltimore Orioles": 0xDF4601,
    "Boston Red Sox": 0xBD3039,
    "Chicago Cubs": 0x0E3386,
    "Chicago White Sox": 0x27251F,
    "Cincinnati Reds": 0xC6011F,
    "Cleveland Guardians": 0x0C2340,
    "Colorado Rockies": 0x333366,
    "Detroit Tigers": 0x0C2340,
    "Houston Astros": 0xEB6E1F,
    "Kansas City Royals": 0x004687,
    "Los Angeles Angels": 0xBA0021,
    "Los Angeles Dodgers": 0x005A9C,
    "Miami Marlins": 0x00A3E0,
    "Milwaukee Brewers": 0x12284B,
    "Minnesota Twins": 0x002B5C,
    "New York Mets": 0x002D72,
    "New York Yankees": 0x0C2340,
    "Philadelphia Phillies": 0xE81828,
    "Pittsburgh Pirates": 0xFDB827,
    "San Diego Padres": 0x2F241D,
    "San Francisco Giants": 0xFD5A1E,
    "Seattle Mariners": 0x005C5C,
    "St. Louis Cardinals": 0xC41E3A,
    "Tampa Bay Rays": 0x092C5C,
    "Texas Rangers": 0x003278,
    "Toronto Blue Jays": 0x134A8E,
    "Washington Nationals": 0xAB0003,
}

TEAM_ABBREVS = {
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

TEAM_LOGOS = {
    "Arizona Diamondbacks": "https://a.espncdn.com/i/teamlogos/mlb/500/ari.png",
    "Athletics": "https://a.espncdn.com/i/teamlogos/mlb/500/oak.png",
    "Atlanta Braves": "https://a.espncdn.com/i/teamlogos/mlb/500/atl.png",
    "Baltimore Orioles": "https://a.espncdn.com/i/teamlogos/mlb/500/bal.png",
    "Boston Red Sox": "https://a.espncdn.com/i/teamlogos/mlb/500/bos.png",
    "Chicago Cubs": "https://a.espncdn.com/i/teamlogos/mlb/500/chc.png",
    "Chicago White Sox": "https://a.espncdn.com/i/teamlogos/mlb/500/chw.png",
    "Cincinnati Reds": "https://a.espncdn.com/i/teamlogos/mlb/500/cin.png",
    "Cleveland Guardians": "https://a.espncdn.com/i/teamlogos/mlb/500/cle.png",
    "Colorado Rockies": "https://a.espncdn.com/i/teamlogos/mlb/500/col.png",
    "Detroit Tigers": "https://a.espncdn.com/i/teamlogos/mlb/500/det.png",
    "Houston Astros": "https://a.espncdn.com/i/teamlogos/mlb/500/hou.png",
    "Kansas City Royals": "https://a.espncdn.com/i/teamlogos/mlb/500/kc.png",
    "Los Angeles Angels": "https://a.espncdn.com/i/teamlogos/mlb/500/laa.png",
    "Los Angeles Dodgers": "https://a.espncdn.com/i/teamlogos/mlb/500/lad.png",
    "Miami Marlins": "https://a.espncdn.com/i/teamlogos/mlb/500/mia.png",
    "Milwaukee Brewers": "https://a.espncdn.com/i/teamlogos/mlb/500/mil.png",
    "Minnesota Twins": "https://a.espncdn.com/i/teamlogos/mlb/500/min.png",
    "New York Mets": "https://a.espncdn.com/i/teamlogos/mlb/500/nym.png",
    "New York Yankees": "https://a.espncdn.com/i/teamlogos/mlb/500/nyy.png",
    "Philadelphia Phillies": "https://a.espncdn.com/i/teamlogos/mlb/500/phi.png",
    "Pittsburgh Pirates": "https://a.espncdn.com/i/teamlogos/mlb/500/pit.png",
    "San Diego Padres": "https://a.espncdn.com/i/teamlogos/mlb/500/sd.png",
    "San Francisco Giants": "https://a.espncdn.com/i/teamlogos/mlb/500/sf.png",
    "Seattle Mariners": "https://a.espncdn.com/i/teamlogos/mlb/500/sea.png",
    "St. Louis Cardinals": "https://a.espncdn.com/i/teamlogos/mlb/500/stl.png",
    "Tampa Bay Rays": "https://a.espncdn.com/i/teamlogos/mlb/500/tb.png",
    "Texas Rangers": "https://a.espncdn.com/i/teamlogos/mlb/500/tex.png",
    "Toronto Blue Jays": "https://a.espncdn.com/i/teamlogos/mlb/500/tor.png",
    "Washington Nationals": "https://a.espncdn.com/i/teamlogos/mlb/500/wsh.png",
}

# =========================
# GLOBAL CACHES / STATE
# =========================
BATTING_EV_DF_BY_SEASON = {}
BATTING_X_DF_BY_SEASON = {}
PITCHING_EV_DF_BY_SEASON = {}
PITCHING_X_DF_BY_SEASON = {}

# Track last time Statcast was accessed for auto-cleanup
STATCAST_LAST_ACCESSED = {}

seeder_task = None
adp_cache = None
backfill_done = asyncio.Event()

seed_state = {
    "paused": False,
    "completed": False,
    "current_index": 0,
    "total_players": 0,
    "last_run_ts": 0,
    "seed_players": [],
}

thread_command_state = {}
user_rate_limit_state = {}  # user_id -> {date: request_count}

# Profile creation queue to prevent concurrent memory spikes
profile_creation_lock = asyncio.Lock()
profile_creation_queue = []  # List of (user_id, player_name, message) tuples

PLAYER_NAME_OVERRIDES = {
    "julio rodriguez": 677594,  # Julio Rodríguez
}

# =========================
# BASIC HELPERS
# =========================
def current_local_date() -> date:
    return datetime.now(ZoneInfo(BOT_TIMEZONE)).date()


def current_local_date_str() -> str:
    return current_local_date().isoformat()


def normalize_text(text: str) -> str:
    if text is None:
        return ""

    text = str(text).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = text.replace("'", "")
    text = text.replace("’", "")
    text = text.replace(".", "")
    text = re.sub(r"[^a-z0-9\s\-()]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def strip_injury_tags(name: str) -> str:
    """
    Strip injury/status tags from Fantasy Pros player names.
    Examples:
        'Juan Soto (NYM - LF,RF) MiLB' -> 'Juan Soto'
        'Gerrit Cole (NYY - SP) IL15' -> 'Gerrit Cole'
        'Mookie Betts (LAD - SS) IL10' -> 'Mookie Betts'
    """
    # Remove everything from the first parenthesis onwards
    name = re.sub(r'\s*\([^)]*\).*$', '', name)
    return name.strip()


def get_overridden_player_id(player_name: str) -> int | None:
    return PLAYER_NAME_OVERRIDES.get(normalize_text(player_name))


def team_abbrev_from_profile(profile: dict) -> str:
    team_name = (profile.get("team") or "").strip()
    return TEAM_ABBREVS.get(team_name, "")


def thread_title(player_name: str, team_abbrev: str = "") -> str:
    if team_abbrev:
        return f"{player_name} ({team_abbrev})"
    return player_name


def old_profile_title(player_name: str) -> str:
    return f"{player_name} {PROFILE_YEAR_LABEL} Profile"


def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        num = float(value)
        if math.isnan(num):
            return default
        return num
    except Exception:
        return default


def safe_int_like(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def clean_num(value, decimals=1, default="N/A"):
    num = safe_float(value)
    if num is None:
        return default
    return f"{num:.{decimals}f}"


def pct_str(value, decimals=1, default="N/A"):
    num = safe_float(value)
    if num is None:
        return default
    return f"{num:.{decimals}f}%"


def row_get_any(row: pd.Series, aliases, default=None):
    if row is None:
        return default
    for alias in aliases:
        if alias in row.index:
            value = row[alias]
            if pd.isna(value):
                continue
            return value
    return default


def first_non_empty(*values, default="N/A"):
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return default


def is_blank_or_zero(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return True
        if v.upper() == "N/A":
            return True
        try:
            return float(v) == 0
        except Exception:
            return False
    try:
        return float(value) == 0
    except Exception:
        return False


def add_line_if_meaningful(lines: list[str], label: str, value):
    if is_blank_or_zero(value):
        return
    lines.append(f"{label}: {value}")


def delta(curr, prev):
    c = safe_float(curr)
    p = safe_float(prev)
    if c is None or p is None:
        return None
    return c - p


def choose_line(options, seed_text=""):
    if not options:
        return ""
    idx = abs(hash(seed_text)) % len(options)
    return options[idx]


def choose_structure(seed_text: str, count: int) -> int:
    if count <= 0:
        return 0
    return abs(hash(seed_text + "-structure")) % count


def player_name_from_adp_cell(value: str) -> str:
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*\([^)]*\)\s*$", "", text)
    text = re.sub(r"\s+[A-Z]{2,4}\s*-\s*[A-Z0-9/, -]+$", "", text)
    text = re.sub(r"^\d+\s+", "", text)
    return text.strip()


def get_team_color(team_name: str | None) -> int:
    if not team_name:
        return 0x0B5FFF
    return TEAM_COLORS.get(team_name, 0x0B5FFF)


def get_team_logo(team_name: str | None) -> str | None:
    if not team_name:
        return None
    return TEAM_LOGOS.get(team_name)


def infer_tag_name(profile: dict) -> str | None:
    pos = (profile.get("position") or "").upper()
    p_stats = profile.get("pitching_stats") or {}

    if pos == "P":
        saves = safe_int_like(p_stats.get("saves"), 0) or 0
        holds = safe_int_like(p_stats.get("holds"), 0) or 0
        if saves > 0 or holds >= 5:
            return "RP"
        return "SP"

    if pos in {"C", "1B", "2B", "3B", "SS", "OF", "DH"}:
        return pos

    return None


def get_forum_tag_by_name(forum_channel: discord.ForumChannel, tag_name: str | None):
    if not tag_name:
        return None

    for tag in forum_channel.available_tags:
        if tag.name.strip().upper() == tag_name.strip().upper():
            return tag

    return None


def player_name_from_thread_name(thread_name: str) -> str:
    name = thread_name.strip()
    name = re.sub(r"\s+\([A-Z]{2,4}\)\s*$", "", name)
    return name.strip()


# =========================
# THREAD COMMAND STATE
# =========================
def load_thread_command_state():
    global thread_command_state
    if not THREAD_COMMAND_STATE_FILE.exists():
        thread_command_state = {}
        return

    try:
        with open(THREAD_COMMAND_STATE_FILE, "r", encoding="utf-8") as f:
            thread_command_state = json.load(f)
    except Exception as e:
        log_profiles(f"Failed to load thread command state: {e}")
        thread_command_state = {}


def save_thread_command_state():
    try:
        with open(THREAD_COMMAND_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(thread_command_state, f, indent=2)
    except Exception as e:
        log_profiles(f"Failed to save thread command state: {e}")


def was_thread_command_used_today(thread_id: int, command_name: str) -> bool:
    thread_key = str(thread_id)
    today = current_local_date_str()
    last_used = (thread_command_state.get(thread_key) or {}).get(command_name)
    return last_used == today


def mark_thread_command_used_today(thread_id: int, command_name: str):
    thread_key = str(thread_id)
    if thread_key not in thread_command_state:
        thread_command_state[thread_key] = {}
    thread_command_state[thread_key][command_name] = current_local_date_str()
    save_thread_command_state()


# =========================
# USER RATE LIMIT STATE
# =========================
def load_user_rate_limit_state():
    global user_rate_limit_state
    if not USER_RATE_LIMIT_FILE.exists():
        user_rate_limit_state = {}
        return
    
    try:
        with open(USER_RATE_LIMIT_FILE, "r", encoding="utf-8") as f:
            user_rate_limit_state = json.load(f)
    except Exception as e:
        log_profiles(f"Failed to load user rate limit state: {e}")
        user_rate_limit_state = {}


def save_user_rate_limit_state():
    try:
        with open(USER_RATE_LIMIT_FILE, "w", encoding="utf-8") as f:
            json.dump(user_rate_limit_state, f, indent=2)
    except Exception as e:
        log_profiles(f"Failed to save user rate limit state: {e}")


def check_user_rate_limit(user_id: int) -> tuple[bool, int]:
    """
    Check if user has exceeded daily profile request limit.
    Returns (can_proceed, requests_today)
    """
    user_key = str(user_id)
    today = current_local_date_str()
    
    if user_key not in user_rate_limit_state:
        user_rate_limit_state[user_key] = {}
    
    requests_today = user_rate_limit_state[user_key].get(today, 0)
    
    if requests_today >= MAX_PROFILE_REQUESTS_PER_DAY:
        return False, requests_today
    
    return True, requests_today


def increment_user_rate_limit(user_id: int):
    """Increment user's profile request count for today."""
    user_key = str(user_id)
    today = current_local_date_str()
    
    if user_key not in user_rate_limit_state:
        user_rate_limit_state[user_key] = {}
    
    user_rate_limit_state[user_key][today] = user_rate_limit_state[user_key].get(today, 0) + 1
    save_user_rate_limit_state()


# =========================
# SEEDER STATE HELPERS
# =========================
def save_seed_state():
    payload = {
        "paused": seed_state.get("paused", False),
        "completed": seed_state.get("completed", False),
        "current_index": seed_state.get("current_index", 0),
        "total_players": seed_state.get("total_players", 0),
        "last_run_ts": seed_state.get("last_run_ts", 0),
        "seed_players": seed_state.get("seed_players", []),
    }
    with open(SEED_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def load_seed_state():
    global seed_state
    if not SEED_STATE_FILE.exists():
        return

    try:
        with open(SEED_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        seed_state["paused"] = data.get("paused", False)
        seed_state["completed"] = data.get("completed", False)
        seed_state["current_index"] = data.get("current_index", 0)
        seed_state["total_players"] = data.get("total_players", 0)
        seed_state["last_run_ts"] = data.get("last_run_ts", 0)
        seed_state["seed_players"] = data.get("seed_players", [])
    except Exception as e:
        log_adp(f"Failed to load seed state: {e}")


def init_seed_state_if_needed(seed_players: list[dict]):
    if seed_state["seed_players"]:
        seed_state["total_players"] = len(seed_state["seed_players"])
        if seed_state["current_index"] >= seed_state["total_players"]:
            seed_state["completed"] = True
        save_seed_state()
        return

    seed_state["seed_players"] = seed_players[:]
    seed_state["total_players"] = len(seed_players)
    seed_state["current_index"] = 0
    seed_state["completed"] = len(seed_players) == 0
    seed_state["last_run_ts"] = 0
    save_seed_state()


def is_seed_admin(member: discord.Member) -> bool:
    if member is None:
        return False

    if BOT_OWNER_ID and member.id == BOT_OWNER_ID:
        return True

    perms = getattr(member, "guild_permissions", None)
    if perms:
        if perms.administrator:
            return True
        if perms.manage_guild:
            return True

    return False


def get_seed_status_text() -> str:
    idx = seed_state.get("current_index", 0)
    total = seed_state.get("total_players", 0)
    remaining = max(total - idx, 0)

    if seed_state.get("completed"):
        state_label = "completed"
    elif seed_state.get("paused"):
        state_label = "paused"
    else:
        state_label = "running"

    lines = [
        f"Seeder status: **{state_label}**",
        f"Progress: **{idx}/{total}**",
        f"Remaining: **{remaining}**",
    ]

    last_run_ts = seed_state.get("last_run_ts", 0)
    if last_run_ts:
        lines.append(f"Last seeded: <t:{int(last_run_ts)}:R>")

    players = seed_state.get("seed_players", [])
    if idx < len(players):
        lines.append(f"Next up: **{players[idx]['name']}**")

    return "\n".join(lines)


# =========================
# DISCORD THREAD HELPERS
# =========================
def canonical_thread_name(thread_name: str) -> str:
    name = thread_name.strip()
    name = re.sub(r"\s+\([A-Z]{2,4}\)\s*$", "", name)
    return normalize_text(name)


def thread_matches_player(thread_name: str, player_name: str) -> bool:
    normalized_thread = canonical_thread_name(thread_name)
    return normalized_thread in {
        normalize_text(player_name),
        normalize_text(old_profile_title(player_name)),
    }


async def find_existing_profile_thread(
    forum_channel: discord.ForumChannel,
    player_name: str,
    player_id: int | None = None,
):
    # Fast path: check persisted thread map by name (exact + normalized) and MLB ID
    id_key = f"mlb_id:{player_id}" if player_id is not None else None
    norm_name = normalize_text(player_name)

    async def try_thread_id(thread_id: int):
        thread = forum_channel.guild.get_thread(thread_id)
        if thread:
            return thread
        try:
            thread = await bot.fetch_channel(thread_id)
            if thread:
                return thread
        except Exception:
            pass
        return None

    # Exact key checks first (mlb_id: and raw name)
    for key in ([id_key, player_name] if id_key else [player_name]):
        if key and key in PLAYER_THREADS:
            result = await try_thread_id(PLAYER_THREADS[key])
            if result:
                return result

    # Normalized name scan — catches accent mismatches (e.g. "Julio Rodriguez" vs "Julio Rodríguez")
    for key, thread_id in PLAYER_THREADS.items():
        if key.startswith("mlb_id:"):
            continue
        if normalize_text(key) == norm_name:
            result = await try_thread_id(thread_id)
            if result:
                return result
            break

    async def thread_matches(thread: discord.Thread):
        if player_id is not None:
            try:
                starter = thread.starter_message
                if starter is None:
                    starter = await thread.fetch_message(thread.id)

                if starter and starter.embeds:
                    embed = starter.embeds[0]
                    footer_text = embed.footer.text if embed.footer else ""
                    if footer_text and footer_text.strip() == f"MLB_ID:{player_id}":
                        return True
            except Exception:
                pass

        return thread_matches_player(thread.name, player_name)

    for thread in forum_channel.threads:
        if await thread_matches(thread):
            return thread

    try:
        async for thread in forum_channel.archived_threads(limit=200):
            if await thread_matches(thread):
                return thread
    except Exception as e:
        log_profiles(f"Archived thread check error: {e}")

    return None


# =========================
# MLB API HELPERS
# =========================
async def fetch_json(url: str, params=None):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=30) as resp:
            if resp.status != 200:
                log_profiles(f"HTTP {resp.status} for {url}")
                return None
            return await resp.json()


async def fetch_text(url: str, params=None):
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, params=params, timeout=30) as resp:
            if resp.status != 200:
                log_profiles(f"HTTP {resp.status} for {url}")
                return None
            return await resp.text()


async def search_player_candidates(player_query: str):
    candidates = []
    seen_ids = set()

    url = "https://statsapi.mlb.com/api/v1/people/search"
    params = {"names": player_query}
    data = await fetch_json(url, params=params)

    if data:
        people = data.get("people", [])
        for p in people:
            full_name = p.get("fullName")
            player_id = p.get("id")
            if not full_name or not player_id or player_id in seen_ids:
                continue

            seen_ids.add(player_id)
            candidates.append(
                {
                    "id": player_id,
                    "full_name": full_name,
                    "full_name_normalized": normalize_text(full_name),
                    "current_age": p.get("currentAge"),
                    "active": p.get("active", False),
                    "primary_position": (p.get("primaryPosition") or {}).get("abbreviation"),
                    "position_type": (p.get("primaryPosition") or {}).get("type"),
                    "team_name": (p.get("currentTeam") or {}).get("name"),
                }
            )

    nq = normalize_text(player_query)
    if any(c.get("full_name_normalized") == nq for c in candidates):
        return candidates

    try:
        parts = player_query.strip().split()
        if len(parts) >= 2:
            first = parts[0]
            last = " ".join(parts[1:])
        elif len(parts) == 1:
            first = None
            last = parts[0]
        else:
            return candidates

        from pybaseball import playerid_lookup

        def _lookup():
            return playerid_lookup(last, first, fuzzy=True)

        df = await asyncio.to_thread(_lookup)

        if df is not None and not df.empty:
            for _, row in df.iterrows():
                mlbam_id = row.get("key_mlbam")
                name_first = row.get("name_first")
                name_last = row.get("name_last")

                if pd.isna(mlbam_id) or pd.isna(name_first) or pd.isna(name_last):
                    continue

                try:
                    player_id = int(mlbam_id)
                except Exception:
                    continue

                if player_id in seen_ids:
                    continue

                full_name = f"{str(name_first).strip()} {str(name_last).strip()}"
                last_year = row.get("mlb_played_last")
                try:
                    is_active = int(last_year) >= current_local_date().year - 1
                except Exception:
                    is_active = False
                seen_ids.add(player_id)
                candidates.append(
                    {
                        "id": player_id,
                        "full_name": full_name,
                        "full_name_normalized": normalize_text(full_name),
                        "current_age": None,
                        "active": is_active,
                        "primary_position": None,
                        "position_type": None,
                        "team_name": None,
                    }
                )
    except Exception as e:
        log_profiles(f"pybaseball fallback lookup error for '{player_query}': {e}")

    return candidates


def choose_best_candidate(query: str, candidates: list[dict]):
    if not candidates:
        return None, []

    nq = normalize_text(query)

    exact = [c for c in candidates if c.get("full_name_normalized") == nq]
    if exact:
        active_exact = [c for c in exact if c.get("active")]
        if active_exact:
            active_exact.sort(key=lambda c: (0 if c.get("team_name") else 1, c.get("full_name", "")))
            return active_exact[0], candidates
        # Exact match exists but is inactive — prefer a single active startswith match instead
        active_startswith = [
            c for c in candidates
            if c.get("active") and c.get("full_name_normalized", "").startswith(nq)
        ]
        if len(active_startswith) == 1:
            return active_startswith[0], candidates
        # Fall back to the inactive exact match
        exact.sort(key=lambda c: (0 if c.get("team_name") else 1, c.get("full_name", "")))
        return exact[0], candidates

    startswith_matches = [c for c in candidates if c.get("full_name_normalized", "").startswith(nq)]
    if len(startswith_matches) == 1:
        return startswith_matches[0], candidates
    # Multiple startswith — prefer active
    active_startswith = [c for c in startswith_matches if c.get("active")]
    if len(active_startswith) == 1:
        return active_startswith[0], candidates

    contains_matches = [c for c in candidates if nq in c.get("full_name_normalized", "")]
    if len(contains_matches) == 1:
        return contains_matches[0], candidates

    active_candidates = [c for c in candidates if c.get("active")]
    if len(active_candidates) == 1:
        return active_candidates[0], candidates

    if len(candidates) == 1:
        return candidates[0], candidates

    return None, candidates


async def fetch_player_full_profile(player_id: int, season: int):
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
    params = {
        "hydrate": f"currentTeam,stats(group=[hitting,pitching],type=[season],season={season})"
    }
    data = await fetch_json(url, params=params)
    if not data or not data.get("people"):
        return None

    person = data["people"][0]
    stats = person.get("stats", [])

    hitting = {}
    pitching = {}

    for stat_block in stats:
        group_name = ((stat_block.get("group") or {}).get("displayName") or "").lower()
        splits = stat_block.get("splits") or []
        if not splits:
            continue
        stat = splits[0].get("stat") or {}

        if group_name == "hitting":
            hitting = stat
        elif group_name == "pitching":
            pitching = stat

    return {
        "id": person.get("id"),
        "full_name": person.get("fullName"),
        "age": person.get("currentAge"),
        "team": (person.get("currentTeam") or {}).get("name"),
        "position": ((person.get("primaryPosition") or {}).get("abbreviation")),
        "position_type": ((person.get("primaryPosition") or {}).get("type")),
        "bat_side": ((person.get("batSide") or {}).get("code")),
        "pitch_hand": ((person.get("pitchHand") or {}).get("code")),
        "hitting_stats": hitting,
        "pitching_stats": pitching,
        "season": season,
    }


async def resolve_player_id_from_name(raw_player: str) -> int | None:
    override_player_id = get_overridden_player_id(raw_player)
    if override_player_id:
        return override_player_id

    candidates = await search_player_candidates(raw_player)
    best, _ = choose_best_candidate(raw_player, candidates)
    if not best:
        return None
    return best["id"]


# =========================
# ADP HELPERS
# =========================
async def fetch_adp_top_250():
    global adp_cache
    if adp_cache:
        return adp_cache

    html = await fetch_text(ADP_URL)
    if not html:
        return []

    names = []

    try:
        dfs = pd.read_html(StringIO(html))
        for df in dfs:
            player_col = None
            pos_col = None

            for c in df.columns:
                col_name = str(c).strip().lower()
                if col_name in {"player", "player (team)", "player/team", "player team"}:
                    player_col = c
                if col_name in {"pos", "position"}:
                    pos_col = c

            if player_col is None:
                for c in df.columns:
                    series = df[c].astype(str)
                    if series.str.contains("[A-Za-z]").mean() > 0.8:
                        player_col = c
                        break

            if player_col is not None:
                temp = []
                for _, row in df.iterrows():
                    raw_name = row.get(player_col)
                    if pd.isna(raw_name):
                        continue
                    player_name = player_name_from_adp_cell(str(raw_name))
                    if player_name and player_name.lower() not in {"player", "nan"}:
                        position = (
                            str(row.get(pos_col)).strip()
                            if pos_col is not None and not pd.isna(row.get(pos_col))
                            else ""
                        )
                        temp.append({
                            "name": player_name,
                            "pos": position.upper(),
                        })
                if len(temp) >= 150:
                    names = temp
                    break
    except Exception as e:
        log_adp(f"ADP pandas parse failed: {e}")

    if not names:
        try:
            soup = BeautifulSoup(html, "lxml")
            for a in soup.select("a"):
                text = a.get_text(" ", strip=True)
                if not text:
                    continue
                if re.match(r"^[A-Z][a-zA-Z' .-]+(?:\s[A-Z][a-zA-Z' .-]+)+$", text):
                    names.append({"name": text, "pos": ""})

            dedup = []
            seen = set()
            for row in names:
                key = normalize_text(row["name"])
                if key in seen:
                    continue
                seen.add(key)
                dedup.append(row)
            names = dedup
        except Exception as e:
            log_adp(f"ADP soup fallback failed: {e}")

    cleaned = []
    seen = set()
    for row in names:
        name = player_name_from_adp_cell(row["name"])
        if not name:
            continue
        key = normalize_text(name)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({
            "name": name,
            "pos": (row.get("pos") or "").upper()
        })
        if len(cleaned) >= SEED_COUNT:
            break

    adp_cache = tiered_seed_order(cleaned[:SEED_COUNT])
    log_adp(f"Loaded ADP seed list: {len(adp_cache)} players")
    return adp_cache


def tiered_seed_order(players: list[dict]) -> list[dict]:
    waves = [
        players[0:25],
        players[25:75],
        players[75:150],
        players[150:250],
    ]

    result = []

    for wave in waves:
        hitters = []
        starters = []
        relievers = []
        unknown = []

        for row in wave:
            pos = row.get("pos", "")
            if "RP" in pos:
                relievers.append(row)
            elif "SP" in pos or pos == "P":
                starters.append(row)
            elif pos:
                hitters.append(row)
            else:
                unknown.append(row)

        buckets = [hitters, starters, relievers, unknown]
        made_progress = True
        while made_progress:
            made_progress = False
            for bucket in buckets:
                if bucket:
                    result.append(bucket.pop(0))
                    made_progress = True

    return result


# =========================
# PYBASEBALL / STATCAST HELPERS
# =========================
def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]

    if "player_id" in out.columns:
        out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    elif "id" in out.columns:
        out["player_id"] = pd.to_numeric(out["id"], errors="coerce").astype("Int64")

    return out


def _load_batter_ev_df(season: int):
    from pybaseball import statcast_batter_exitvelo_barrels
    return _normalize_df(statcast_batter_exitvelo_barrels(season, 1))


def _load_batter_x_df(season: int):
    from pybaseball import statcast_batter_expected_stats
    return _normalize_df(statcast_batter_expected_stats(season, 1))


def _load_pitcher_ev_df(season: int):
    from pybaseball import statcast_pitcher_exitvelo_barrels
    return _normalize_df(statcast_pitcher_exitvelo_barrels(season, 1))


def _load_pitcher_x_df(season: int):
    from pybaseball import statcast_pitcher_expected_stats
    return _normalize_df(statcast_pitcher_expected_stats(season, 1))


async def ensure_statcast_loaded(season: int):
    """Load Statcast data for a season if not already cached."""
    import time
    
    # Update last accessed timestamp
    STATCAST_LAST_ACCESSED[season] = time.time()
    
    if season not in BATTING_EV_DF_BY_SEASON:
        BATTING_EV_DF_BY_SEASON[season] = await asyncio.to_thread(_load_batter_ev_df, season)
        log_profiles(f"Loaded hitter EV/barrels rows for {season}: {len(BATTING_EV_DF_BY_SEASON[season])}")

    if season not in BATTING_X_DF_BY_SEASON:
        BATTING_X_DF_BY_SEASON[season] = await asyncio.to_thread(_load_batter_x_df, season)
        log_profiles(f"Loaded hitter expected stats rows for {season}: {len(BATTING_X_DF_BY_SEASON[season])}")

    if season not in PITCHING_EV_DF_BY_SEASON:
        PITCHING_EV_DF_BY_SEASON[season] = await asyncio.to_thread(_load_pitcher_ev_df, season)
        log_profiles(f"Loaded pitcher EV/barrels rows for {season}: {len(PITCHING_EV_DF_BY_SEASON[season])}")

    if season not in PITCHING_X_DF_BY_SEASON:
        PITCHING_X_DF_BY_SEASON[season] = await asyncio.to_thread(_load_pitcher_x_df, season)
        log_profiles(f"Loaded pitcher expected stats rows for {season}: {len(PITCHING_X_DF_BY_SEASON[season])}")


def clear_statcast_cache():
    """Clear Statcast DataFrames from memory to free up space."""
    import time
    
    current_time = time.time()
    IDLE_THRESHOLD = 1800  # 30 minutes
    
    seasons_to_clear = []
    for season, last_accessed in list(STATCAST_LAST_ACCESSED.items()):
        if current_time - last_accessed > IDLE_THRESHOLD:
            seasons_to_clear.append(season)
    
    for season in seasons_to_clear:
        if season in BATTING_EV_DF_BY_SEASON:
            del BATTING_EV_DF_BY_SEASON[season]
        if season in BATTING_X_DF_BY_SEASON:
            del BATTING_X_DF_BY_SEASON[season]
        if season in PITCHING_EV_DF_BY_SEASON:
            del PITCHING_EV_DF_BY_SEASON[season]
        if season in PITCHING_X_DF_BY_SEASON:
            del PITCHING_X_DF_BY_SEASON[season]
        if season in STATCAST_LAST_ACCESSED:
            del STATCAST_LAST_ACCESSED[season]
        
        log_profiles(f"Cleared Statcast cache for {season} after 30min idle")
    
    return len(seasons_to_clear)


def get_statcast_row(df: pd.DataFrame, player_id: int):
    if df is None or df.empty or "player_id" not in df.columns:
        return None
    matches = df[df["player_id"] == player_id]
    if matches.empty:
        return None
    return matches.iloc[0]


# =========================
# PLAYER TYPE / METRICS
# =========================
def infer_is_pitcher(profile: dict) -> bool:
    pos_type = (profile.get("position_type") or "").lower()
    pos = (profile.get("position") or "").upper()

    if pos == "P":
        return True
    if "pitcher" in pos_type:
        return True

    pitching_stats = profile.get("pitching_stats") or {}
    hitting_stats = profile.get("hitting_stats") or {}

    ip = safe_float(pitching_stats.get("inningsPitched"))
    pa = safe_int_like(hitting_stats.get("plateAppearances"))

    if ip and ip > 0 and not pa:
        return True
    return False


def build_hitter_metrics(profile: dict, season: int):
    player_id = profile["id"]
    ev_row = get_statcast_row(BATTING_EV_DF_BY_SEASON.get(season), player_id)
    x_row = get_statcast_row(BATTING_X_DF_BY_SEASON.get(season), player_id)

    metrics = {
        "avg_ev": row_get_any(ev_row, ["avg_hit_speed", "avg_hitspeed"]),
        "hard_hit": row_get_any(ev_row, ["hard_hit_percent", "hardhitpercent"]),
        "barrel": row_get_any(ev_row, ["brl_percent", "barrel_batted_rate", "barrel_percent"]),
        "sweet_spot": row_get_any(ev_row, ["anglesweetspotpercent", "sweet_spot_percent"]),
        "xba": row_get_any(x_row, ["xba"]),
        "xslg": row_get_any(x_row, ["xslg"]),
        "xwoba": row_get_any(x_row, ["xwoba"]),
    }

    h = profile.get("hitting_stats") or {}
    pa = safe_float(h.get("plateAppearances"), 0)
    so = safe_float(h.get("strikeOuts"), 0)
    bb = safe_float(h.get("baseOnBalls"), 0)

    metrics["k_pct"] = round((so / pa) * 100, 1) if pa else None
    metrics["bb_pct"] = round((bb / pa) * 100, 1) if pa else None

    return metrics


def build_pitcher_metrics(profile: dict, season: int):
    player_id = profile["id"]
    ev_row = get_statcast_row(PITCHING_EV_DF_BY_SEASON.get(season), player_id)
    x_row = get_statcast_row(PITCHING_X_DF_BY_SEASON.get(season), player_id)

    metrics = {
        "avg_ev_allowed": row_get_any(ev_row, ["avg_hit_speed", "avg_hitspeed"]),
        "hard_hit_allowed": row_get_any(ev_row, ["hard_hit_percent", "hardhitpercent"]),
        "barrel_allowed": row_get_any(ev_row, ["brl_percent", "barrel_batted_rate", "barrel_percent"]),
        "xba_allowed": row_get_any(x_row, ["xba"]),
        "xslg_allowed": row_get_any(x_row, ["xslg"]),
        "xwoba_allowed": row_get_any(x_row, ["xwoba"]),
        "xera": row_get_any(x_row, ["xera"]),
    }

    p = profile.get("pitching_stats") or {}
    bf = safe_float(first_non_empty(p.get("battersFaced"), p.get("bf"), default=None), None)
    so = safe_float(p.get("strikeOuts"), 0)
    bb = safe_float(p.get("baseOnBalls"), 0)

    metrics["k_pct"] = round((so / bf) * 100, 1) if bf else None
    metrics["bb_pct"] = round((bb / bf) * 100, 1) if bf else None

    if metrics["k_pct"] is not None and metrics["bb_pct"] is not None:
        metrics["k_minus_bb"] = round(metrics["k_pct"] - metrics["bb_pct"], 1)
    else:
        metrics["k_minus_bb"] = None

    return metrics


# =========================
# TREND HELPERS
# =========================
def hitter_trend_sentence(curr_metrics: dict, prev_metrics: dict):
    if not prev_metrics:
        return None

    barrel_delta = delta(curr_metrics.get("barrel"), prev_metrics.get("barrel"))
    hh_delta = delta(curr_metrics.get("hard_hit"), prev_metrics.get("hard_hit"))
    ev_delta = delta(curr_metrics.get("avg_ev"), prev_metrics.get("avg_ev"))
    k_delta = delta(curr_metrics.get("k_pct"), prev_metrics.get("k_pct"))
    bb_delta = delta(curr_metrics.get("bb_pct"), prev_metrics.get("bb_pct"))
    xwoba_delta = delta(curr_metrics.get("xwoba"), prev_metrics.get("xwoba"))

    trend_lines = []

    if (
        barrel_delta is not None and barrel_delta >= 1.5 and
        ((hh_delta is not None and hh_delta >= 3) or (ev_delta is not None and ev_delta >= 1.0))
    ):
        trend_lines.append(
            f"The trend line is encouraging too, because the quality of contact was better in {PROFILE_SEASON} than it was in {PREV_SEASON}."
        )

    if (
        barrel_delta is not None and barrel_delta <= -1.5 and
        ((hh_delta is not None and hh_delta <= -3) or (ev_delta is not None and ev_delta <= -1.0))
    ):
        trend_lines.append(
            f"The one thing to watch is that the contact quality was a little softer in {PROFILE_SEASON} than it was in {PREV_SEASON}, so there is at least some trend risk under the hood."
        )

    if k_delta is not None and k_delta <= -2 and bb_delta is not None and bb_delta >= 1:
        trend_lines.append(
            f"The plate-skill trend was positive from {PREV_SEASON} to {PROFILE_SEASON}, which makes the profile feel steadier going into {PROFILE_YEAR_LABEL}."
        )

    if xwoba_delta is not None and xwoba_delta >= 0.020:
        trend_lines.append(
            f"The underlying offensive trend also moved the right way from {PREV_SEASON} to {PROFILE_SEASON}, which adds another reason to trust the bat."
        )

    if xwoba_delta is not None and xwoba_delta <= -0.020:
        trend_lines.append(
            f"The underlying offensive trend was not quite as strong in {PROFILE_SEASON} as it was in {PREV_SEASON}, so that is worth keeping in mind even if the surface line still looks good."
        )

    if not trend_lines:
        return None

    return choose_line(trend_lines, f"{curr_metrics.get('xwoba')}-{curr_metrics.get('barrel')}")


def pitcher_trend_sentence(curr_metrics: dict, prev_metrics: dict):
    if not prev_metrics:
        return None

    kbb_delta = delta(curr_metrics.get("k_minus_bb"), prev_metrics.get("k_minus_bb"))
    xera_delta = delta(curr_metrics.get("xera"), prev_metrics.get("xera"))
    ev_delta = delta(curr_metrics.get("avg_ev_allowed"), prev_metrics.get("avg_ev_allowed"))
    barrel_delta = delta(curr_metrics.get("barrel_allowed"), prev_metrics.get("barrel_allowed"))

    trend_lines = []

    if kbb_delta is not None and kbb_delta >= 2:
        trend_lines.append(
            f"The skill trend improved from {PREV_SEASON} to {PROFILE_SEASON}, especially in the strikeout-to-command shape of the profile."
        )

    if kbb_delta is not None and kbb_delta <= -2:
        trend_lines.append(
            f"The underlying dominance backed up a little from {PREV_SEASON} to {PROFILE_SEASON}, which is worth noting even if the overall profile still looks strong."
        )

    if (
        xera_delta is not None and xera_delta <= -0.30 and
        ((ev_delta is not None and ev_delta <= -0.8) or (barrel_delta is not None and barrel_delta <= -1.0))
    ):
        trend_lines.append(
            f"The contact-management trend also moved in the right direction from {PREV_SEASON} to {PROFILE_SEASON}, which helps the profile feel even stronger."
        )

    if (
        xera_delta is not None and xera_delta >= 0.30 and
        ((ev_delta is not None and ev_delta >= 0.8) or (barrel_delta is not None and barrel_delta >= 1.0))
    ):
        trend_lines.append(
            f"There was a little more contact danger in {PROFILE_SEASON} than in {PREV_SEASON}, so the ratios may have a bit less cushion than they used to."
        )

    if not trend_lines:
        return None

    return choose_line(trend_lines, f"{curr_metrics.get('k_minus_bb')}-{curr_metrics.get('xera')}")


# =========================
# UNIQUE CLOSING BUCKETS
# =========================
def hitter_closing_line(profile: dict, curr_metrics: dict):
    h = profile.get("hitting_stats") or {}
    name = profile["full_name"]

    hr = safe_int_like(h.get("homeRuns"))
    sb = safe_int_like(h.get("stolenBases"))
    avg = safe_float(h.get("avg"))
    barrel = safe_float(curr_metrics.get("barrel"))
    xwoba = safe_float(curr_metrics.get("xwoba"))
    k_pct = safe_float(curr_metrics.get("k_pct"))
    bb_pct = safe_float(curr_metrics.get("bb_pct"))

    if hr and sb and hr >= 25 and sb >= 20:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, this is the type of profile that can still change a standings column or two by itself.",
            f"Going into {PROFILE_YEAR_LABEL}, the category juice here is strong enough that he can anchor an offense in more than one way.",
            f"Going into {PROFILE_YEAR_LABEL}, the appeal is obvious because few hitters can bring this kind of two-category pressure."
        ]
        return choose_line(options, f"{name}-ps")

    if hr and hr >= 35:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, the fantasy value is clear, and the real debate is just how high the power ceiling belongs in the overall hitting pool.",
            f"Going into {PROFILE_YEAR_LABEL}, he still looks like one of the cleaner bets to deliver impact power without needing everything else to break perfectly.",
            f"Going into {PROFILE_YEAR_LABEL}, this is still the kind of bat that can carry a roster when the power environment gets thin."
        ]
        return choose_line(options, f"{name}-power")

    if sb and sb >= 30:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, the speed alone keeps the profile dangerous, and any extra power just pushes the ceiling higher.",
            f"Going into {PROFILE_YEAR_LABEL}, this is the kind of player who can tilt roster construction because the speed base is so hard to replace.",
            f"Going into {PROFILE_YEAR_LABEL}, the category advantage here gives him more fantasy leverage than a standard surface line might suggest."
        ]
        return choose_line(options, f"{name}-speed")

    if bb_pct is not None and bb_pct >= 12 and k_pct is not None and k_pct <= 18:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, this feels like one of the steadier offensive profiles on the board because the approach gives it real staying power.",
            f"Going into {PROFILE_YEAR_LABEL}, there is a lot to like in how stable the offensive foundation looks from skill to skill.",
            f"Going into {PROFILE_YEAR_LABEL}, the profile has enough discipline and contact quality underneath it to trust the floor."
        ]
        return choose_line(options, f"{name}-discipline")

    if xwoba is not None and xwoba >= 0.370:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, he still looks like the sort of hitter who can sit comfortably near the top of a fantasy lineup build.",
            f"Going into {PROFILE_YEAR_LABEL}, this still reads like a premium fantasy bat with very few soft spots in the profile.",
            f"Going into {PROFILE_YEAR_LABEL}, there is enough here to treat him like a lineup anchor rather than just another strong starter."
        ]
        return choose_line(options, f"{name}-elite")

    if avg is not None and avg >= 0.290:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, the batting-average support gives the profile a little more comfort than a lot of similarly valued bats.",
            f"Going into {PROFILE_YEAR_LABEL}, the overall shape still looks like a hitter who can help more categories than he hurts.",
            f"Going into {PROFILE_YEAR_LABEL}, the floor looks a little sturdier than it does for the average mid-tier fantasy bat."
        ]
        return choose_line(options, f"{name}-avg")

    if k_pct is not None and k_pct >= 27:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, the upside is still worth paying attention to, but the strikeout pressure keeps some volatility in the profile.",
            f"Going into {PROFILE_YEAR_LABEL}, there is still plenty to like here, even if the miss tendency means the ride may not always be smooth.",
            f"Going into {PROFILE_YEAR_LABEL}, the talent is obvious, but this still feels like a profile where week-to-week swings come with the package."
        ]
        return choose_line(options, f"{name}-swingmiss")

    if barrel is not None and barrel >= 10:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, there is enough thump underneath this profile that another strong season would not be surprising at all.",
            f"Going into {PROFILE_YEAR_LABEL}, the batted-ball quality gives the profile more upside than a bland stat line might suggest.",
            f"Going into {PROFILE_YEAR_LABEL}, there is still enough impact contact here to keep dreaming on a meaningful ceiling."
        ]
        return choose_line(options, f"{name}-impact")

    options = [
        f"Going into {PROFILE_YEAR_LABEL}, this still looks like a useful fantasy bat, with the final value likely coming down to roster fit and category needs.",
        f"Going into {PROFILE_YEAR_LABEL}, the profile is playable enough that the next debate is more about rank than relevance.",
        f"Going into {PROFILE_YEAR_LABEL}, he still looks like the kind of hitter who can quietly return value if the role stays steady."
    ]
    return choose_line(options, f"{name}-default-h")


def pitcher_closing_line(profile: dict, curr_metrics: dict):
    p = profile.get("pitching_stats") or {}
    name = profile["full_name"]

    saves = safe_int_like(p.get("saves"))
    holds = safe_int_like(p.get("holds"))
    kbb = safe_float(curr_metrics.get("k_minus_bb"))
    xera = safe_float(curr_metrics.get("xera"))
    avg_ev_allowed = safe_float(curr_metrics.get("avg_ev_allowed"))
    hard_hit_allowed = safe_float(curr_metrics.get("hard_hit_allowed"))

    if saves and saves >= 20:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, the fantasy appeal is obvious because there are only so many relievers with both role clarity and real swing-and-miss ability.",
            f"Going into {PROFILE_YEAR_LABEL}, this is one of the more bankable bullpen profiles as long as the ninth inning stays in his hands.",
            f"Going into {PROFILE_YEAR_LABEL}, the role gives him immediate value, and the skill set is good enough to justify taking it seriously."
        ]
        return choose_line(options, f"{name}-closer")

    if holds and holds >= 15:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, he looks more format-dependent than some bigger names, but the role still has real utility in the right setup.",
            f"Going into {PROFILE_YEAR_LABEL}, the relief value is more specialized, though there is enough skill here to matter in deeper formats.",
            f"Going into {PROFILE_YEAR_LABEL}, he still feels like the type of arm who can help once league settings start to reward bullpen depth."
        ]
        return choose_line(options, f"{name}-holds")

    if kbb is not None and kbb >= 20:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, this is the kind of arm that deserves to be discussed right near the top of the pitching board.",
            f"Going into {PROFILE_YEAR_LABEL}, the upside is big enough that finishing near the very top of the position would not be a surprise.",
            f"Going into {PROFILE_YEAR_LABEL}, the skill set still looks strong enough to separate from a lot of otherwise good fantasy arms."
        ]
        return choose_line(options, f"{name}-ace")

    if xera is not None and xera <= 3.30:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, the run-prevention base looks strong enough that he can stabilize a fantasy staff even when the strikeouts are not doing all the work.",
            f"Going into {PROFILE_YEAR_LABEL}, the profile still looks built to help ratios in a meaningful way.",
            f"Going into {PROFILE_YEAR_LABEL}, there is enough underneath the hood to trust the profile as more than just a surface-stat success."
        ]
        return choose_line(options, f"{name}-ratio")

    if avg_ev_allowed is not None and avg_ev_allowed <= 88:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, the contact-management piece gives the profile a little more security than many similarly ranked arms.",
            f"Going into {PROFILE_YEAR_LABEL}, there is enough weak-contact support here to believe the ratios can hold up.",
            f"Going into {PROFILE_YEAR_LABEL}, the way he limits damage gives the profile real staying power."
        ]
        return choose_line(options, f"{name}-contact")

    if hard_hit_allowed is not None and hard_hit_allowed <= 35:
        options = [
            f"Going into {PROFILE_YEAR_LABEL}, there is enough quality underneath the ratios to keep treating him like a legitimate fantasy weapon.",
            f"Going into {PROFILE_YEAR_LABEL}, the profile still looks sturdy enough to trust across a full season.",
            f"Going into {PROFILE_YEAR_LABEL}, there is a good case for this arm as a steady fantasy difference-maker."
        ]
        return choose_line(options, f"{name}-steady")

    options = [
        f"Going into {PROFILE_YEAR_LABEL}, the profile still looks clearly fantasy relevant, with most of the debate coming down to rank within the broader pitching pool.",
        f"Going into {PROFILE_YEAR_LABEL}, there is enough here to trust him as a real fantasy contributor if the role and workload stay in place.",
        f"Going into {PROFILE_YEAR_LABEL}, this still feels like a pitcher who belongs squarely in the useful fantasy arm conversation."
    ]
    return choose_line(options, f"{name}-default-p")


# =========================
# EXTRA SENTENCE HELPERS
# =========================
def hitter_extra_sentence(profile: dict, curr_metrics: dict, prev_metrics: dict | None):
    name = profile["full_name"]
    h = profile.get("hitting_stats") or {}

    ops = safe_float(h.get("ops"))
    avg = safe_float(h.get("avg"))
    hr = safe_int_like(h.get("homeRuns"))
    sb = safe_int_like(h.get("stolenBases"))
    xwoba = safe_float(curr_metrics.get("xwoba"))
    barrel = safe_float(curr_metrics.get("barrel"))
    bb_pct = safe_float(curr_metrics.get("bb_pct"))
    k_pct = safe_float(curr_metrics.get("k_pct"))

    options = []

    if ops is not None and ops >= 0.900:
        options.extend([
            "There is enough top-end production here that he can still be viewed as a lineup-driving bat in fantasy.",
            "The offensive ceiling still looks high enough to matter in the top part of a draft board.",
        ])

    if avg is not None and avg >= 0.285:
        options.extend([
            "The batting-average support also makes the profile easier to build around than a lot of all-or-nothing sluggers.",
            "That average base gives the profile a little more comfort than many similarly powerful hitters.",
        ])

    if hr and hr >= 30:
        options.extend([
            "If the power output holds near this level, the margin for fantasy impact remains very strong.",
            "The home-run foundation is sturdy enough that the profile does not need much else to stay highly relevant.",
        ])

    if sb and sb >= 20:
        options.extend([
            "The added speed keeps the profile from being overly dependent on one category.",
            "That running game adds another layer of fantasy value that many comparable hitters simply do not have.",
        ])

    if xwoba is not None and xwoba >= 0.360:
        options.extend([
            "The underlying quality still looks strong enough to support another big offensive season.",
            "The skill indicators are good enough that another strong year would not feel forced at all.",
        ])

    if barrel is not None and barrel >= 12:
        options.extend([
            "There is still enough impact contact here to believe the power can hold up against tougher projection systems.",
            "The barrel damage is strong enough that the power does not look like smoke and mirrors.",
        ])

    if bb_pct is not None and bb_pct >= 10 and k_pct is not None and k_pct <= 20:
        options.extend([
            "The approach gives him a better chance than most to avoid prolonged cold stretches.",
            "That plate-skill base gives the profile some real week-to-week stability.",
        ])

    if not options:
        options = [
            "There is enough skill support here that another useful fantasy season is easy to see.",
            "The overall profile still gives fantasy managers a lot to work with.",
            "He still looks like the kind of hitter who can matter without needing a perfect outcome.",
        ]

    return choose_line(options, name + "-extra-h")


def pitcher_extra_sentence(profile: dict, curr_metrics: dict, prev_metrics: dict | None):
    name = profile["full_name"]
    p = profile.get("pitching_stats") or {}

    era = safe_float(p.get("era"))
    whip = safe_float(p.get("whip"))
    k = safe_int_like(p.get("strikeOuts"))
    saves = safe_int_like(p.get("saves"))
    kbb = safe_float(curr_metrics.get("k_minus_bb"))
    xera = safe_float(curr_metrics.get("xera"))
    avg_ev_allowed = safe_float(curr_metrics.get("avg_ev_allowed"))

    options = []

    if saves and saves >= 20:
        options.extend([
            "As long as the role stays intact, the fantasy utility here is pretty straightforward.",
            "There is enough role-driven value here that he will stay relevant even through the normal reliever volatility.",
        ])

    if kbb is not None and kbb >= 20:
        options.extend([
            "The bat-missing ability is strong enough that he can still separate from a lot of otherwise good arms.",
            "That strikeout-to-command shape is what gives the profile genuine top-end fantasy upside.",
        ])

    if xera is not None and xera <= 3.30:
        options.extend([
            "The ratio foundation looks good enough that the profile can help even when the strikeout totals are merely strong instead of outrageous.",
            "There is enough run-prevention support here that the floor looks steadier than it does for many pitchers in his range.",
        ])

    if avg_ev_allowed is not None and avg_ev_allowed <= 88:
        options.extend([
            "The contact-management piece also gives the profile some extra margin for error.",
            "That weak-contact ability adds another reason to trust the ratios.",
        ])

    if era is not None and era <= 3.30 and whip is not None and whip <= 1.10:
        options.extend([
            "The overall shape is exactly what fantasy managers want from an arm they plan to lean on.",
            "That combination of ratio support and underlying skill keeps the profile very easy to buy into.",
        ])

    if k and k >= 180:
        options.extend([
            "The strikeout volume alone gives him a chance to shape a fantasy staff in a meaningful way.",
            "There is enough strikeout punch here to create real separation over the course of a season.",
        ])

    if not options:
        options = [
            "There is enough skill underneath all of this to keep treating him like a real fantasy asset.",
            "The profile still has enough substance to matter even if the exact surface numbers move around a bit.",
            "He still looks like the type of pitcher fantasy managers can work with confidently.",
        ]

    return choose_line(options, name + "-extra-p")


# =========================
# OUTLOOK LOGIC
# =========================
def summarize_hitter(profile: dict, curr_metrics: dict, prev_metrics: dict | None):
    h = profile.get("hitting_stats") or {}

    name = profile["full_name"]
    hr = safe_int_like(h.get("homeRuns"))
    sb = safe_int_like(h.get("stolenBases"))
    barrel = safe_float(curr_metrics.get("barrel"))
    hh = safe_float(curr_metrics.get("hard_hit"))
    avg_ev = safe_float(curr_metrics.get("avg_ev"))
    k_pct = safe_float(curr_metrics.get("k_pct"))
    bb_pct = safe_float(curr_metrics.get("bb_pct"))

    if hr and sb and hr >= 25 and sb >= 20:
        openings = [
            f"{name} brings one of the most dangerous power-speed profiles in the game.",
            f"Few hitters combine power and speed the way {name} does.",
            f"{name}'s fantasy value starts with the rare power-speed combination.",
            f"There's a reason {name} is viewed as a category-shifting hitter.",
            f"The biggest appeal with {name} is how much category pressure he creates."
        ]
        opening = choose_line(openings, name + "-open-ps")
    elif hr and hr >= 35:
        openings = [
            f"{name} still profiles as one of the more dangerous power bats in baseball.",
            f"The calling card with {name} remains the home-run power.",
            f"{name}'s profile starts with impact power.",
            f"When people draft {name}, they are betting on the power ceiling.",
            f"{name} continues to sit near the top of the power conversation."
        ]
        opening = choose_line(openings, name + "-open-power")
    elif sb and sb >= 30:
        openings = [
            f"{name}'s biggest fantasy weapon continues to be the speed.",
            f"Speed is what separates {name} from many other hitters.",
            f"The fantasy appeal of {name} starts with how much damage he can do on the bases.",
            f"{name} brings one of the most valuable speed profiles in the game.",
            f"The stolen-base upside is what keeps {name} extremely interesting."
        ]
        opening = choose_line(openings, name + "-open-speed")
    else:
        openings = [
            f"{name} profiles as a strong all-around fantasy contributor.",
            f"{name}'s value comes from the overall shape of the offensive profile.",
            f"The appeal with {name} is the balanced skill set.",
            f"{name} brings a well-rounded offensive profile into {PROFILE_YEAR_LABEL}.",
            f"{name} looks like a hitter who can contribute across several categories."
        ]
        opening = choose_line(openings, name + "-open-balanced")

    if barrel is not None and hh is not None:
        if barrel >= 12 and hh >= 45:
            skill_sentence = choose_line([
                "The quality of contact backed it up too, with the barrel rate and hard-hit profile both supporting the idea that the production was earned.",
                "The underlying contact quality was strong enough to validate the output, especially once you look at the barrel and hard-hit rates.",
                "The Statcast indicators support the surface line, and the contact damage looks more real than lucky."
            ], name + "-skill-elitecontact")
        elif barrel < 7 and hh < 38:
            skill_sentence = choose_line([
                "The underlying contact quality was less convincing, which means there may be a little less room for error if the results cool off.",
                "There is at least some reason for caution underneath the hood, because the quality-of-contact indicators were not especially loud.",
                "The surface line may have had a little more support from outcomes than from truly dominant contact quality."
            ], name + "-skill-softcontact")
        else:
            skill_sentence = choose_line([
                "The contact profile was solid enough to buy into, even if it did not scream some huge hidden level beyond the surface line.",
                "The underlying contact mix looks good enough to trust, even if it does not necessarily point to another massive jump.",
                "There is enough support in the contact profile to take the production seriously."
            ], name + "-skill-solidcontact")
    elif avg_ev is not None:
        if avg_ev >= 92:
            skill_sentence = choose_line([
                "The average exit velocity stayed loud, which is usually a good sign that the bat speed and contact quality are still carrying real fantasy weight.",
                "The ball still came off the bat with authority, which helps explain why the profile remains so fantasy friendly.",
                "The average exit velocity gives the profile another layer of legitimacy heading into the new season."
            ], name + "-skill-ev")
        else:
            skill_sentence = choose_line([
                "The underlying batted-ball quality was respectable enough that the production does not look fluky.",
                "Nothing underneath looks wildly out of line, which helps keep the profile believable.",
                "The supporting indicators are stable enough that the offensive value makes sense."
            ], name + "-skill-fallback")
    else:
        skill_sentence = choose_line([
            "There is enough skill under the surface to keep taking the profile seriously.",
            "The profile still has enough real substance underneath it to matter in fantasy.",
            "The underlying shape is stable enough that the bat still deserves attention."
        ], name + "-skill-default")

    trend_line = hitter_trend_sentence(curr_metrics, prev_metrics)
    if trend_line:
        mid_sentence = trend_line
    else:
        if k_pct is not None and bb_pct is not None:
            if k_pct <= 18 and bb_pct >= 10:
                mid_sentence = choose_line([
                    "The plate skills help the floor a lot here, because that kind of strikeout and walk mix tends to keep the profile stable.",
                    "The approach gives this profile some real staying power, which matters when you’re paying for a hitter to hold value across a full season.",
                    "The strikeout and walk mix adds a nice layer of stability that fantasy managers usually appreciate."
                ], name + "-discipline-mid")
            elif k_pct >= 27:
                mid_sentence = choose_line([
                    "The swing-and-miss is still the one thing that can introduce some volatility, especially if the batting average gets squeezed.",
                    "The miss tendency still leaves some room for cold stretches, even if the overall upside remains easy to see.",
                    "There is still enough swing-and-miss here that the floor can wobble when the balls in play stop finding holes."
                ], name + "-swingmiss-mid")
            else:
                mid_sentence = choose_line([
                    "The overall approach looks good enough that there are not many obvious red flags in the offensive shape.",
                    "The skill mix underneath the surface line looks stable enough to trust.",
                    "There is enough balance in the profile that it does not feel overly dependent on one fragile skill."
                ], name + "-balanced-mid")
        else:
            mid_sentence = choose_line([
                "The overall profile still holds together well enough to trust.",
                "There is enough support across the profile to keep buying into the value.",
                "The skill base remains stable enough to matter."
            ], name + "-mid-default")

    extra_sentence = hitter_extra_sentence(profile, curr_metrics, prev_metrics)
    closing = hitter_closing_line(profile, curr_metrics)

    structures = [
        [opening, skill_sentence, mid_sentence, extra_sentence, closing],
        [opening, mid_sentence, skill_sentence, extra_sentence, closing],
        [opening, skill_sentence, extra_sentence, mid_sentence, closing],
        [opening, mid_sentence, extra_sentence, skill_sentence, closing],
        [opening, extra_sentence, skill_sentence, mid_sentence, closing],
    ]

    chosen = structures[choose_structure(name + "-hitter", len(structures))]
    return " ".join(chosen[:5])


def summarize_pitcher(profile: dict, curr_metrics: dict, prev_metrics: dict | None):
    p = profile.get("pitching_stats") or {}

    name = profile["full_name"]
    saves = safe_int_like(p.get("saves"))
    holds = safe_int_like(p.get("holds"))
    kbb = safe_float(curr_metrics.get("k_minus_bb"))
    xera = safe_float(curr_metrics.get("xera"))
    era = safe_float(p.get("era"))
    hard_hit = safe_float(curr_metrics.get("hard_hit_allowed"))
    avg_ev_allowed = safe_float(curr_metrics.get("avg_ev_allowed"))

    if saves and saves >= 20:
        openings = [
            f"{name} enters the season as one of the more established closers in the game.",
            f"The fantasy value for {name} begins with the ninth-inning role.",
            f"{name} continues to anchor the back end of a bullpen.",
            f"When healthy, {name} is one of the more reliable sources of saves.",
            f"{name} remains firmly in the closer conversation."
        ]
        opening = choose_line(openings, name + "-open-closer")
    elif holds and holds >= 15:
        openings = [
            f"{name} continues to fill an important bullpen role.",
            f"{name} profiles as one of the more useful setup arms.",
            f"{name} remains a meaningful piece of his team's bullpen.",
            f"The appeal with {name} comes from the high-leverage bullpen role.",
            f"{name} looks like a steady late-inning reliever."
        ]
        opening = choose_line(openings, name + "-open-holds")
    else:
        openings = [
            f"{name} still looks like a frontline fantasy starter.",
            f"{name} enters {PROFILE_YEAR_LABEL} with one of the stronger pitching profiles in the league.",
            f"The appeal with {name} starts with the overall pitching skill set.",
            f"{name} profiles as a pitcher capable of anchoring a fantasy rotation.",
            f"{name} continues to sit near the top of the starting pitcher pool."
        ]
        opening = choose_line(openings, name + "-open-sp")

    if kbb is not None:
        if kbb >= 20:
            skill_sentence = choose_line([
                "The strikeout-minus-walk profile is exactly what you want to see from a frontline pitcher, and it backs up the idea that the skill level is real.",
                "The K-BB shape still looks elite, which is one of the clearest markers of true frontline quality.",
                "The bat-missing and command foundation is strong enough to support a top-tier fantasy outcome."
            ], name + "-skill-elitekbb")
        elif kbb <= 10:
            skill_sentence = choose_line([
                "The strikeout and command combination was more ordinary than dominant, which leaves less room for error if the ratios move the wrong way.",
                "The core strikeout-to-command mix was not overwhelming, so the profile needs the run prevention to stay in place.",
                "There is a little less margin here because the K-BB profile was good rather than overpowering."
            ], name + "-skill-lowkbb")
        else:
            skill_sentence = choose_line([
                "The core command-and-miss profile was plenty good enough to support useful fantasy value.",
                "The strikeout and control foundation still looks strong enough to keep the profile trustworthy.",
                "The base skill set still reads like a pitcher who can help in meaningful fantasy innings."
            ], name + "-pitch-mid")
    else:
        skill_sentence = choose_line([
            "There is still enough core skill here to keep the profile fantasy relevant.",
            "The underlying pitching shape remains good enough to matter.",
            "The baseline skill set still gives the profile real fantasy value."
        ], name + "-skill-default-p")

    trend_line = pitcher_trend_sentence(curr_metrics, prev_metrics)
    if trend_line:
        mid_sentence = trend_line
    else:
        if xera is not None and era is not None:
            if xera + 0.40 < era:
                mid_sentence = choose_line([
                    "The expected indicators were actually a little kinder than the ERA, so there may have been some missed upside in the surface line.",
                    "The expected numbers suggest the actual ERA may have been leaving a little on the table.",
                    "There is at least a case that the surface ratios did not fully capture how good the underlying work was."
                ], name + "-under")
            elif xera - 0.40 > era:
                mid_sentence = choose_line([
                    "The expected indicators were not quite as clean as the ERA, so there may be a little more ratio risk here than the headline numbers suggest.",
                    "There is at least some chance the surface ERA was doing a bit more of the work than the underlying indicators.",
                    "The ratio line looked better than some of the expected metrics, which is worth keeping in the back of your mind."
                ], name + "-over")
            else:
                mid_sentence = choose_line([
                    "The expected indicators generally matched the run prevention, which makes the profile easier to trust.",
                    "The expected stats did a decent job backing up the actual results, which is a good sign.",
                    "The ERA line and the expected profile were close enough that the skill level looks believable."
                ], name + "-match")
        elif hard_hit is not None or avg_ev_allowed is not None:
            if (hard_hit is not None and hard_hit <= 35) or (avg_ev_allowed is not None and avg_ev_allowed <= 88):
                mid_sentence = choose_line([
                    "He also did a good job limiting dangerous contact, which adds another layer of confidence to the profile.",
                    "The contact suppression piece gives the profile a little more safety than a standard strikeout-first arm.",
                    "There is enough damage control here that the ratios feel more repeatable than random."
                ], name + "-contact-mid")
            else:
                mid_sentence = choose_line([
                    "There is still enough underneath the surface to take the profile seriously.",
                    "The supporting indicators remain stable enough to keep trusting the arm.",
                    "The overall run-prevention shape still makes sense."
                ], name + "-mid-fallback-p")
        else:
            mid_sentence = choose_line([
                "There is still enough underneath the surface to take the profile seriously.",
                "The supporting indicators remain stable enough to keep trusting the arm.",
                "The overall run-prevention shape still makes sense."
            ], name + "-mid-default-p")

    extra_sentence = pitcher_extra_sentence(profile, curr_metrics, prev_metrics)
    closing = pitcher_closing_line(profile, curr_metrics)

    structures = [
        [opening, skill_sentence, mid_sentence, extra_sentence, closing],
        [opening, mid_sentence, skill_sentence, extra_sentence, closing],
        [opening, skill_sentence, extra_sentence, mid_sentence, closing],
        [opening, mid_sentence, extra_sentence, skill_sentence, closing],
        [opening, extra_sentence, skill_sentence, mid_sentence, closing],
    ]

    chosen = structures[choose_structure(name + "-pitcher", len(structures))]
    return " ".join(chosen[:5])


# =========================
# METRIC SELECTION FOR CLEAN POSTS
# =========================
def select_hitter_metric_lines(curr_metrics: dict, prev_metrics: dict | None):
    barrel = safe_float(curr_metrics.get("barrel"))
    hard_hit = safe_float(curr_metrics.get("hard_hit"))
    avg_ev = safe_float(curr_metrics.get("avg_ev"))
    k_pct = safe_float(curr_metrics.get("k_pct"))
    bb_pct = safe_float(curr_metrics.get("bb_pct"))
    xwoba = safe_float(curr_metrics.get("xwoba"))

    selected = []

    if barrel is not None and barrel >= 10:
        selected.append(("Barrel%", pct_str(barrel)))
    if hard_hit is not None and hard_hit >= 40:
        selected.append(("Hard-Hit%", pct_str(hard_hit)))
    if avg_ev is not None and avg_ev >= 91:
        selected.append(("Avg EV", f"{clean_num(avg_ev)} mph"))
    if bb_pct is not None and bb_pct >= 10:
        selected.append(("BB%", pct_str(bb_pct)))
    if k_pct is not None and k_pct <= 20:
        selected.append(("K%", pct_str(k_pct)))
    if xwoba is not None and xwoba >= 0.340:
        selected.append(("xwOBA", clean_num(xwoba, 3)))

    if prev_metrics:
        barrel_delta = delta(curr_metrics.get("barrel"), prev_metrics.get("barrel"))
        xwoba_delta = delta(curr_metrics.get("xwoba"), prev_metrics.get("xwoba"))
        if barrel_delta is not None and barrel_delta >= 1.5:
            if ("Barrel%", pct_str(barrel)) not in selected and not is_blank_or_zero(pct_str(barrel)):
                selected.insert(0, ("Barrel%", pct_str(barrel)))
        elif xwoba_delta is not None and xwoba_delta >= 0.020:
            if ("xwOBA", clean_num(xwoba, 3)) not in selected and not is_blank_or_zero(clean_num(xwoba, 3)):
                selected.insert(0, ("xwOBA", clean_num(xwoba, 3)))

    fallback_order = [
        ("Barrel%", pct_str(barrel)),
        ("Hard-Hit%", pct_str(hard_hit)),
        ("Avg EV", f"{clean_num(avg_ev)} mph" if avg_ev is not None else "N/A"),
        ("K%", pct_str(k_pct)),
        ("BB%", pct_str(bb_pct)),
        ("xwOBA", clean_num(xwoba, 3)),
    ]

    final_selected = []
    for label, value in selected + fallback_order:
        if is_blank_or_zero(value):
            continue
        if label not in [x[0] for x in final_selected]:
            final_selected.append((label, value))
        if len(final_selected) >= 4:
            break

    return final_selected[:4]


def select_pitcher_metric_lines(curr_metrics: dict, prev_metrics: dict | None):
    kbb = safe_float(curr_metrics.get("k_minus_bb"))
    xera = safe_float(curr_metrics.get("xera"))
    avg_ev = safe_float(curr_metrics.get("avg_ev_allowed"))
    barrel = safe_float(curr_metrics.get("barrel_allowed"))
    hard_hit = safe_float(curr_metrics.get("hard_hit_allowed"))
    k_pct = safe_float(curr_metrics.get("k_pct"))
    bb_pct = safe_float(curr_metrics.get("bb_pct"))

    selected = []

    if kbb is not None and kbb >= 15:
        selected.append(("K-BB%", pct_str(kbb)))
    if xera is not None:
        selected.append(("xERA", clean_num(xera, 2)))
    if avg_ev is not None and avg_ev <= 89:
        selected.append(("Avg EV Allowed", f"{clean_num(avg_ev)} mph"))
    if barrel is not None and barrel <= 7:
        selected.append(("Barrel% Allowed", pct_str(barrel)))
    if hard_hit is not None and hard_hit <= 37:
        selected.append(("Hard-Hit% Allowed", pct_str(hard_hit)))
    if k_pct is not None and k_pct >= 25:
        selected.append(("K%", pct_str(k_pct)))
    if bb_pct is not None and bb_pct <= 7:
        selected.append(("BB%", pct_str(bb_pct)))

    if prev_metrics:
        kbb_delta = delta(curr_metrics.get("k_minus_bb"), prev_metrics.get("k_minus_bb"))
        if kbb_delta is not None and kbb_delta >= 2:
            if ("K-BB%", pct_str(kbb)) not in selected and not is_blank_or_zero(pct_str(kbb)):
                selected.insert(0, ("K-BB%", pct_str(kbb)))

    fallback_order = [
        ("K-BB%", pct_str(kbb)),
        ("xERA", clean_num(xera, 2)),
        ("Avg EV Allowed", f"{clean_num(avg_ev)} mph" if avg_ev is not None else "N/A"),
        ("Barrel% Allowed", pct_str(barrel)),
        ("Hard-Hit% Allowed", pct_str(hard_hit)),
        ("K%", pct_str(k_pct)),
        ("BB%", pct_str(bb_pct)),
    ]

    final_selected = []
    for label, value in selected + fallback_order:
        if is_blank_or_zero(value):
            continue
        if label not in [x[0] for x in final_selected]:
            final_selected.append((label, value))
        if len(final_selected) >= 4:
            break

    return final_selected[:4]


# =========================
# PLAYER DATA BUNDLE
# =========================
async def build_player_bundle(player_id: int):
    current_profile = await fetch_player_full_profile(player_id, PROFILE_SEASON)
    if not current_profile:
        return None

    previous_profile = await fetch_player_full_profile(player_id, PREV_SEASON)

    await ensure_statcast_loaded(PROFILE_SEASON)
    await ensure_statcast_loaded(PREV_SEASON)

    is_pitcher = infer_is_pitcher(current_profile)

    if is_pitcher:
        curr_metrics = build_pitcher_metrics(current_profile, PROFILE_SEASON)
        prev_metrics = build_pitcher_metrics(previous_profile, PREV_SEASON) if previous_profile else None
    else:
        curr_metrics = build_hitter_metrics(current_profile, PROFILE_SEASON)
        prev_metrics = build_hitter_metrics(previous_profile, PREV_SEASON) if previous_profile else None

    return {
        "profile": current_profile,
        "previous_profile": previous_profile,
        "curr_metrics": curr_metrics,
        "prev_metrics": prev_metrics,
        "is_pitcher": is_pitcher,
    }


# =========================
# AI-GENERATED OUTLOOK
# =========================
async def generate_ai_outlook(profile: dict, curr_metrics: dict, prev_metrics: dict | None, is_pitcher: bool) -> str:
    """Generate a 2026 mid-season outlook using Claude API."""
    if not ANTHROPIC_API_KEY:
        log_profiles("No ANTHROPIC_API_KEY set - falling back to template outlook")
        if is_pitcher:
            return summarize_pitcher(profile, curr_metrics, prev_metrics)
        else:
            return summarize_hitter(profile, curr_metrics, prev_metrics)
    
    name = profile["full_name"]
    
    # Build the stats context
    if is_pitcher:
        stats = profile.get("pitching_stats", {})
        stat_context = f"""
**2026 Stats (so far):**
- IP: {stats.get('inningsPitched', 'N/A')}
- ERA: {stats.get('era', 'N/A')}
- WHIP: {stats.get('whip', 'N/A')}
- K: {stats.get('strikeOuts', 'N/A')}
- BB: {stats.get('baseOnBalls', 'N/A')}
- Saves: {stats.get('saves', 'N/A')}
- Holds: {stats.get('holds', 'N/A')}

**Underlying Metrics:**
- K-BB%: {curr_metrics.get('k_minus_bb', 'N/A')}
- Hard-Hit% Allowed: {curr_metrics.get('hard_hit_allowed', 'N/A')}
- xERA: {curr_metrics.get('xera', 'N/A')}
"""
    else:
        stats = profile.get('hitting_stats', {})
        stat_context = f"""
**2026 Stats (so far):**
- AVG: {stats.get('avg', 'N/A')}
- HR: {stats.get('homeRuns', 'N/A')}
- RBI: {stats.get('rbi', 'N/A')}
- R: {stats.get('runs', 'N/A')}
- SB: {stats.get('stolenBases', 'N/A')}
- OPS: {stats.get('ops', 'N/A')}

**Underlying Metrics:**
- Barrel%: {curr_metrics.get('barrel', 'N/A')}
- Avg EV: {curr_metrics.get('avg_ev', 'N/A')} mph
- Hard-Hit%: {curr_metrics.get('hard_hit', 'N/A')}
- K%: {curr_metrics.get('k_pct', 'N/A')}
- BB%: {curr_metrics.get('bb_pct', 'N/A')}
"""
    
    prompt = f"""You are writing a fantasy baseball REST-OF-SEASON outlook for {name}.

We're in early April 2026. The season just started (only a few games played).

{stat_context}

Write a 3-4 sentence outlook (600-700 characters MAXIMUM) analyzing:
- How {name} has performed in the early weeks of 2026
- What the underlying metrics say about sustainability
- What to expect from {name} for the REST of the 2026 season

IMPORTANT GUIDELINES:
- It's VERY early in the season - acknowledge small sample size for extreme stats
- DO NOT recommend dropping players or say they're "droppable"
- Focus on buy/hold decisions and realistic projections
- DO NOT mention drafts, draft rounds, or ADP
- Be analytical but avoid overly harsh language for early-season struggles
- When stats look bad but metrics are good, emphasize the metrics (and vice versa)

Write in a direct, analytical style. Be confident and specific about projections."""

    try:
        log_profiles(f"API: Calling Claude for {name} outlook...")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    log_profiles(f"Claude API error: {resp.status}")
                    raise Exception(f"API returned {resp.status}")
                
                data = await resp.json()
                outlook = data["content"][0]["text"].strip()
                
                # Truncate at sentence boundary if too long
                if len(outlook) > 1020:
                    sentences = outlook.split('. ')
                    truncated = []
                    current_len = 0
                    for sent in sentences:
                        if current_len + len(sent) + 2 > 1020:
                            break
                        truncated.append(sent)
                        current_len += len(sent) + 2
                    outlook = '. '.join(truncated)
                    if not outlook.endswith('.'):
                        outlook += '.'
                
                log_profiles(f"Generated AI outlook for {name}: {len(outlook)} chars")
                return outlook
                
    except Exception as e:
        log_profiles(f"Failed to generate AI outlook for {name}: {e}")
        # Fallback to template
        if is_pitcher:
            return summarize_pitcher(profile, curr_metrics, prev_metrics)
        else:
            return summarize_hitter(profile, curr_metrics, prev_metrics)


# =========================
# PROFILE EMBED BUILDERS
# =========================
def build_hitter_profile_embed(profile: dict, curr_metrics: dict, prev_metrics: dict | None, ai_outlook: str | None = None):
    h = profile.get("hitting_stats") or {}

    team = first_non_empty(profile.get("team"), default="TBD")
    pos = first_non_empty(profile.get("position"), default="TBD")
    age = first_non_empty(profile.get("age"), default="TBD")
    player_id = profile["id"]

    embed = discord.Embed(
        title=profile["full_name"],
        description="━━━━━━━━━━━━━━━━",
        color=get_team_color(profile.get("team")),
    )

    team_logo = get_team_logo(profile.get("team"))
    if team_logo:
        embed.set_author(name=profile["full_name"], icon_url=team_logo)
        embed.title = ""

    embed.add_field(
        name="Player Info",
        value=f"**Team:** {team}\n**Position:** {pos}\n**Age:** {age}",
        inline=False,
    )

    stat_lines = []
    add_line_if_meaningful(stat_lines, "AVG", h.get("avg"))
    add_line_if_meaningful(stat_lines, "HR", h.get("homeRuns"))
    add_line_if_meaningful(stat_lines, "RBI", h.get("rbi"))
    add_line_if_meaningful(stat_lines, "R", h.get("runs"))
    add_line_if_meaningful(stat_lines, "SB", h.get("stolenBases"))
    add_line_if_meaningful(stat_lines, "OPS", h.get("ops"))

    if stat_lines:
        embed.add_field(
            name=f"{PROFILE_SEASON} Stats",
            value="\n".join(stat_lines),
            inline=False,
        )

    metric_lines = []
    for label, value in select_hitter_metric_lines(curr_metrics, prev_metrics):
        if not is_blank_or_zero(value):
            metric_lines.append(f"{label}: {value}")

    if metric_lines:
        embed.add_field(
            name="Underlying Metrics",
            value="\n".join(metric_lines),
            inline=False,
        )

    # Use AI outlook if provided, otherwise use template
    summary = ai_outlook if ai_outlook else summarize_hitter(profile, curr_metrics, prev_metrics)
    embed.add_field(
        name=f"{PROFILE_YEAR_LABEL} Outlook",
        value=summary,
        inline=False,
    )

    headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current"
    embed.set_thumbnail(url=headshot_url)
    embed.set_footer(text=f"MLB_ID:{player_id}")

    return embed


def build_pitcher_profile_embed(profile: dict, curr_metrics: dict, prev_metrics: dict | None, ai_outlook: str | None = None):
    p = profile.get("pitching_stats") or {}

    team = first_non_empty(profile.get("team"), default="TBD")
    pos = first_non_empty(profile.get("position"), default="P")
    age = first_non_empty(profile.get("age"), default="TBD")
    player_id = profile["id"]

    embed = discord.Embed(
        title=profile["full_name"],
        description="━━━━━━━━━━━━━━━━",
        color=get_team_color(profile.get("team")),
    )

    team_logo = get_team_logo(profile.get("team"))
    if team_logo:
        embed.set_author(name=profile["full_name"], icon_url=team_logo)
        embed.title = ""

    embed.add_field(
        name="Player Info",
        value=f"**Team:** {team}\n**Position:** {pos}\n**Age:** {age}",
        inline=False,
    )

    stat_lines = []
    add_line_if_meaningful(stat_lines, "W", p.get("wins"))
    add_line_if_meaningful(stat_lines, "ERA", p.get("era"))
    add_line_if_meaningful(stat_lines, "WHIP", p.get("whip"))
    add_line_if_meaningful(stat_lines, "IP", p.get("inningsPitched"))
    add_line_if_meaningful(stat_lines, "K", p.get("strikeOuts"))
    add_line_if_meaningful(stat_lines, "SV", p.get("saves"))
    add_line_if_meaningful(stat_lines, "HLD", p.get("holds"))

    if stat_lines:
        embed.add_field(
            name=f"{PROFILE_SEASON} Stats",
            value="\n".join(stat_lines),
            inline=False,
        )

    metric_lines = []
    for label, value in select_pitcher_metric_lines(curr_metrics, prev_metrics):
        if not is_blank_or_zero(value):
            metric_lines.append(f"{label}: {value}")

    if metric_lines:
        embed.add_field(
            name="Underlying Metrics",
            value="\n".join(metric_lines),
            inline=False,
        )

    # Use AI outlook if provided, otherwise use template
    summary = ai_outlook if ai_outlook else summarize_pitcher(profile, curr_metrics, prev_metrics)
    embed.add_field(
        name=f"{PROFILE_YEAR_LABEL} Outlook",
        value=summary,
        inline=False,
    )

    headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current"
    embed.set_thumbnail(url=headshot_url)
    embed.set_footer(text=f"MLB_ID:{player_id}")

    return embed


def build_stats_embed(bundle: dict):
    profile = bundle["profile"]
    curr_metrics = bundle["curr_metrics"]
    prev_metrics = bundle["prev_metrics"]
    is_pitcher = bundle["is_pitcher"]
    player_id = profile["id"]

    embed = discord.Embed(
        title=f"{profile['full_name']} — {PROFILE_SEASON} Stats",
        color=get_team_color(profile.get("team")),
    )

    team_logo = get_team_logo(profile.get("team"))
    if team_logo:
        embed.set_author(name=profile["full_name"], icon_url=team_logo)
        embed.title = f"{PROFILE_SEASON} Stats"

    if is_pitcher:
        p = profile.get("pitching_stats") or {}
        stat_lines = []
        add_line_if_meaningful(stat_lines, "W", p.get("wins"))
        add_line_if_meaningful(stat_lines, "ERA", p.get("era"))
        add_line_if_meaningful(stat_lines, "WHIP", p.get("whip"))
        add_line_if_meaningful(stat_lines, "IP", p.get("inningsPitched"))
        add_line_if_meaningful(stat_lines, "K", p.get("strikeOuts"))
        add_line_if_meaningful(stat_lines, "SV", p.get("saves"))
        add_line_if_meaningful(stat_lines, "HLD", p.get("holds"))
        embed.add_field(name="Surface Stats", value="\n".join(stat_lines) if stat_lines else "N/A", inline=False)

        metric_lines = []
        for label, value in select_pitcher_metric_lines(curr_metrics, prev_metrics):
            metric_lines.append(f"{label}: {value}")
        embed.add_field(name="Underlying Metrics", value="\n".join(metric_lines) if metric_lines else "N/A", inline=False)
    else:
        h = profile.get("hitting_stats") or {}
        stat_lines = []
        add_line_if_meaningful(stat_lines, "AVG", h.get("avg"))
        add_line_if_meaningful(stat_lines, "HR", h.get("homeRuns"))
        add_line_if_meaningful(stat_lines, "RBI", h.get("rbi"))
        add_line_if_meaningful(stat_lines, "R", h.get("runs"))
        add_line_if_meaningful(stat_lines, "SB", h.get("stolenBases"))
        add_line_if_meaningful(stat_lines, "OPS", h.get("ops"))
        embed.add_field(name="Surface Stats", value="\n".join(stat_lines) if stat_lines else "N/A", inline=False)

        metric_lines = []
        for label, value in select_hitter_metric_lines(curr_metrics, prev_metrics):
            metric_lines.append(f"{label}: {value}")
        embed.add_field(name="Underlying Metrics", value="\n".join(metric_lines) if metric_lines else "N/A", inline=False)

    headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current"
    embed.set_thumbnail(url=headshot_url)
    embed.set_footer(text=f"MLB_ID:{player_id}")

    return embed


def build_outlook_embed(bundle: dict):
    profile = bundle["profile"]
    curr_metrics = bundle["curr_metrics"]
    prev_metrics = bundle["prev_metrics"]
    is_pitcher = bundle["is_pitcher"]
    player_id = profile["id"]

    embed = discord.Embed(
        title=f"{profile['full_name']} — {PROFILE_YEAR_LABEL} Outlook",
        color=get_team_color(profile.get("team")),
    )

    team_logo = get_team_logo(profile.get("team"))
    if team_logo:
        embed.set_author(name=profile["full_name"], icon_url=team_logo)
        embed.title = f"{PROFILE_YEAR_LABEL} Outlook"

    if is_pitcher:
        p = profile.get("pitching_stats") or {}
        key_lines = []
        add_line_if_meaningful(key_lines, "ERA", p.get("era"))
        add_line_if_meaningful(key_lines, "WHIP", p.get("whip"))
        add_line_if_meaningful(key_lines, "K", p.get("strikeOuts"))
        add_line_if_meaningful(key_lines, "SV", p.get("saves"))
        embed.add_field(name="Key Stats", value="\n".join(key_lines) if key_lines else "N/A", inline=False)
        embed.add_field(
            name="Outlook",
            value=summarize_pitcher(profile, curr_metrics, prev_metrics),
            inline=False,
        )
    else:
        h = profile.get("hitting_stats") or {}
        key_lines = []
        add_line_if_meaningful(key_lines, "AVG", h.get("avg"))
        add_line_if_meaningful(key_lines, "HR", h.get("homeRuns"))
        add_line_if_meaningful(key_lines, "RBI", h.get("rbi"))
        add_line_if_meaningful(key_lines, "SB", h.get("stolenBases"))
        embed.add_field(name="Key Stats", value="\n".join(key_lines) if key_lines else "N/A", inline=False)
        embed.add_field(
            name="Outlook",
            value=summarize_hitter(profile, curr_metrics, prev_metrics),
            inline=False,
        )

    headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current"
    embed.set_thumbnail(url=headshot_url)
    embed.set_footer(text=f"MLB_ID:{player_id}")

    return embed




async def build_profile_text_for_player(player_id: int, use_ai_outlook: bool = False):
    bundle = await build_player_bundle(player_id)
    if not bundle:
        return None, None

    profile = bundle["profile"]
    curr_metrics = bundle["curr_metrics"]
    prev_metrics = bundle["prev_metrics"]
    
    # Generate AI outlook if requested
    ai_outlook = None
    if use_ai_outlook:
        ai_outlook = await generate_ai_outlook(
            profile, 
            curr_metrics, 
            prev_metrics, 
            bundle["is_pitcher"]
        )

    if bundle["is_pitcher"]:
        embed = build_pitcher_profile_embed(profile, curr_metrics, prev_metrics, ai_outlook)
    else:
        embed = build_hitter_profile_embed(profile, curr_metrics, prev_metrics, ai_outlook)

    return profile, embed


# =========================
# SPLITS & TRENDS COMMANDS
# =========================
async def fetch_player_splits(player_id: int, season: int):
    """Fetch split stats from MLB API."""
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=statSplits&group=hitting,pitching&sitCodes=h,a,d,n,vL,vR&season={season}"
    
    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: requests.get(url, timeout=10))
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log_profiles(f"Failed to fetch splits for player {player_id}: {e}")
        return None


def parse_split_stats(splits_data, is_pitcher=False):
    """Parse splits data into home/away, day/night, vs hand."""
    if not splits_data or "stats" not in splits_data:
        return None
    
    splits = {}
    
    for stat_group in splits_data.get("stats", []):
        for split in stat_group.get("splits", []):
            split_code = split.get("split", {}).get("code", "")
            stats = split.get("stat", {})
            
            if split_code == "h":  # Home
                splits["home"] = stats
            elif split_code == "a":  # Away
                splits["away"] = stats
            elif split_code == "d":  # Day
                splits["day"] = stats
            elif split_code == "n":  # Night
                splits["night"] = stats
            elif split_code == "vL":  # vs Left
                splits["vs_left"] = stats
            elif split_code == "vR":  # vs Right
                splits["vs_right"] = stats
    
    return splits


def build_splits_embed(profile: dict, splits: dict, is_pitcher: bool):
    """Build Discord embed showing player splits."""
    player_id = profile["id"]
    name = profile["full_name"]
    
    embed = discord.Embed(
        title=f"{name} — {PROFILE_SEASON} Splits",
        color=get_team_color(profile.get("team")),
    )
    
    team_logo = get_team_logo(profile.get("team"))
    if team_logo:
        embed.set_author(name=name, icon_url=team_logo)
    
    if not splits:
        embed.add_field(
            name="No Split Data",
            value="Not enough games played to show meaningful splits.",
            inline=False
        )
    else:
        if is_pitcher:
            # Pitcher splits
            home = splits.get("home", {})
            away = splits.get("away", {})
            vs_l = splits.get("vs_left", {})
            vs_r = splits.get("vs_right", {})
            day = splits.get("day", {})
            night = splits.get("night", {})
            
            if home or away:
                home_away = f"**Home:** {home.get('era', 'N/A')} ERA, {home.get('whip', 'N/A')} WHIP\n"
                home_away += f"**Away:** {away.get('era', 'N/A')} ERA, {away.get('whip', 'N/A')} WHIP"
                embed.add_field(name="Home/Away", value=home_away, inline=False)
            
            if vs_l or vs_r:
                vs_hand = f"**vs LHB:** {vs_l.get('avg', 'N/A')} AVG, {vs_l.get('ops', 'N/A')} OPS\n"
                vs_hand += f"**vs RHB:** {vs_r.get('avg', 'N/A')} AVG, {vs_r.get('ops', 'N/A')} OPS"
                embed.add_field(name="vs Batter Handedness", value=vs_hand, inline=False)
            
            if day or night:
                day_night = f"**Day:** {day.get('era', 'N/A')} ERA, {day.get('strikeOuts', 'N/A')} K\n"
                day_night += f"**Night:** {night.get('era', 'N/A')} ERA, {night.get('strikeOuts', 'N/A')} K"
                embed.add_field(name="Day/Night", value=day_night, inline=False)
        else:
            # Hitter splits
            home = splits.get("home", {})
            away = splits.get("away", {})
            vs_l = splits.get("vs_left", {})
            vs_r = splits.get("vs_right", {})
            day = splits.get("day", {})
            night = splits.get("night", {})
            
            if home or away:
                home_away = f"**Home:** {home.get('avg', 'N/A')} AVG, {home.get('homeRuns', 'N/A')} HR, {home.get('ops', 'N/A')} OPS\n"
                home_away += f"**Away:** {away.get('avg', 'N/A')} AVG, {away.get('homeRuns', 'N/A')} HR, {away.get('ops', 'N/A')} OPS"
                embed.add_field(name="Home/Away", value=home_away, inline=False)
            
            if vs_l or vs_r:
                vs_hand = f"**vs LHP:** {vs_l.get('avg', 'N/A')} AVG, {vs_l.get('ops', 'N/A')} OPS\n"
                vs_hand += f"**vs RHP:** {vs_r.get('avg', 'N/A')} AVG, {vs_r.get('ops', 'N/A')} OPS"
                embed.add_field(name="vs Pitcher Handedness", value=vs_hand, inline=False)
            
            if day or night:
                day_night = f"**Day:** {day.get('avg', 'N/A')} AVG, {day.get('homeRuns', 'N/A')} HR\n"
                day_night += f"**Night:** {night.get('avg', 'N/A')} AVG, {night.get('homeRuns', 'N/A')} HR"
                embed.add_field(name="Day/Night", value=day_night, inline=False)
    
    headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current"
    embed.set_thumbnail(url=headshot_url)
    embed.set_footer(text=f"MLB_ID:{player_id}")
    
    return embed


def calculate_trend(recent_avg, season_avg):
    """Determine if stat is trending hot, cold, or steady."""
    if season_avg == 0 or recent_avg is None or season_avg is None:
        return "➡️", "Steady"
    
    pct_diff = ((recent_avg - season_avg) / season_avg) * 100
    
    if pct_diff >= 15:
        return "🔥", "Hot"
    elif pct_diff <= -15:
        return "❄️", "Cold"
    else:
        return "➡️", "Steady"


def build_trends_embed(profile: dict, is_pitcher: bool):
    """Build Discord embed showing recent trends vs season average."""
    player_id = profile["id"]
    name = profile["full_name"]
    
    embed = discord.Embed(
        title=f"{name} — Recent Trends",
        description="Last 7 days vs Season Average",
        color=get_team_color(profile.get("team")),
    )
    
    team_logo = get_team_logo(profile.get("team"))
    if team_logo:
        embed.set_author(name=name, icon_url=team_logo)
    
    # Note: This is a simplified version showing season stats with trend indicators
    # Full implementation would need game-by-game data from MLB API
    
    if is_pitcher:
        p = profile.get("pitching_stats", {})
        era = safe_float(p.get("era"))
        whip = safe_float(p.get("whip"))
        k = safe_int_like(p.get("strikeOuts"))
        
        # Placeholder for actual 7-day trending (would need game logs)
        embed.add_field(
            name="📊 Season Stats",
            value=f"ERA: {era or 'N/A'}\nWHIP: {whip or 'N/A'}\nK: {k or 'N/A'}",
            inline=False
        )
        
        embed.add_field(
            name="ℹ️ Note",
            value="Full trending analysis requires game-by-game data. Check back soon for rolling 7/14/30 day trends!",
            inline=False
        )
    else:
        h = profile.get("hitting_stats", {})
        avg = safe_float(h.get("avg"))
        hr = safe_int_like(h.get("homeRuns"))
        rbi = safe_int_like(h.get("rbi"))
        ops = safe_float(h.get("ops"))
        
        embed.add_field(
            name="📊 Season Stats",
            value=f"AVG: {avg or 'N/A'}\nHR: {hr or 'N/A'}\nRBI: {rbi or 'N/A'}\nOPS: {ops or 'N/A'}",
            inline=False
        )
        
        embed.add_field(
            name="ℹ️ Note",
            value="Full trending analysis requires game-by-game data. Check back soon for rolling 7/14/30 day trends!",
            inline=False
        )
    
    headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_213,q_auto:best/v1/people/{player_id}/headshot/67/current"
    embed.set_thumbnail(url=headshot_url)
    embed.set_footer(text=f"MLB_ID:{player_id}")
    
    return embed


# =========================
# PROFILE CREATION HELPERS
# =========================
async def create_profile_for_name(
    raw_player: str,
    forum_channel: discord.ForumChannel,
):
    log_profiles(f"Creating profile for: {raw_player}")
    override_player_id = get_overridden_player_id(raw_player)
    if override_player_id:
        log_profiles(f"Using override ID: {override_player_id}")
        existing = await find_existing_profile_thread(
            forum_channel,
            raw_player,
            player_id=override_player_id,
        )
        if existing:
            log_profiles(f"Found existing profile for {raw_player}")
            return {
                "status": "exists",
                "player_name": raw_player,
                "url": existing.jump_url,
                "message": f"**{existing.name}** already has a profile:\n{existing.jump_url}",
            }

        log_profiles(f"Building profile with AI outlook for {raw_player}...")
        profile, profile_embed = await build_profile_text_for_player(override_player_id, use_ai_outlook=True)
        if profile and profile_embed:
            tag_name = infer_tag_name(profile)
            tag_obj = get_forum_tag_by_name(forum_channel, tag_name)
            applied_tags = [tag_obj] if tag_obj else []

            team_abbrev = team_abbrev_from_profile(profile)

            created = await forum_channel.create_thread(
                name=thread_title(profile["full_name"], team_abbrev),
                embed=profile_embed,
                applied_tags=applied_tags,
            )

            created_thread = created.thread if hasattr(created, "thread") else created

            PLAYER_THREADS[profile["full_name"]] = created_thread.id
            PLAYER_THREADS[normalize_text(profile["full_name"])] = created_thread.id
            PLAYER_THREADS[f"mlb_id:{override_player_id}"] = created_thread.id
            with open(THREAD_MAP_FILE, "w") as f:
                json.dump(PLAYER_THREADS, f, indent=2)

            return {
                "status": "created",
                "player_name": profile["full_name"],
                "url": created_thread.jump_url,
                "message": f"Profile created: {created_thread.jump_url}",
            }

    candidates = await search_player_candidates(raw_player)
    best, all_candidates = choose_best_candidate(raw_player, candidates)

    if not all_candidates:
        return {"status": "no_match", "message": f'No matching player found for "{raw_player}".'}

    if best is None:
        names = []
        seen = set()
        for c in all_candidates[:5]:
            nm = c["full_name"]
            norm_nm = normalize_text(nm)
            if norm_nm not in seen:
                seen.add(norm_nm)
                names.append(nm)

        suggestion_text = "\n".join(f"• {nm}" for nm in names)
        return {
            "status": "ambiguous",
            "message": f'I found multiple players for "{raw_player}". Try one of these:\n{suggestion_text}'
        }

    player_name = best["full_name"]
    player_id = best["id"]

    existing = await find_existing_profile_thread(forum_channel, player_name, player_id=player_id)
    if existing:
        return {
            "status": "exists",
            "player_name": player_name,
            "url": existing.jump_url,
            "message": f"**{player_name}** already has a profile:\n{existing.jump_url}",
        }

    profile, profile_embed = await build_profile_text_for_player(player_id, use_ai_outlook=True)
    log_profiles(f"Building profile with AI outlook for {player_name}...")
    if not profile or not profile_embed:
        return {
            "status": "error",
            "message": "I found the player, but I couldn’t build the profile right now."
        }

    tag_name = infer_tag_name(profile)
    tag_obj = get_forum_tag_by_name(forum_channel, tag_name)
    applied_tags = [tag_obj] if tag_obj else []

    team_abbrev = team_abbrev_from_profile(profile)

    created = await forum_channel.create_thread(
        name=thread_title(profile["full_name"], team_abbrev),
        embed=profile_embed,
        applied_tags=applied_tags,
    )

    created_thread = created.thread if hasattr(created, "thread") else created

    PLAYER_THREADS[profile["full_name"]] = created_thread.id
    PLAYER_THREADS[normalize_text(profile["full_name"])] = created_thread.id
    PLAYER_THREADS[f"mlb_id:{player_id}"] = created_thread.id
    with open(THREAD_MAP_FILE, "w") as f:
        json.dump(PLAYER_THREADS, f, indent=2)

    return {
        "status": "created",
        "player_name": profile["full_name"],
        "url": created_thread.jump_url,
        "message": f"Profile created: {created_thread.jump_url}",
    }


# =========================
# THREAD COMMAND HELPERS
# =========================
async def resolve_bundle_from_thread(thread: discord.Thread):
    player_name = player_name_from_thread_name(thread.name)
    player_id = await resolve_player_id_from_name(player_name)
    if not player_id:
        return None
    return await build_player_bundle(player_id)


async def resolve_bundle_from_name(player_name: str):
    player_id = await resolve_player_id_from_name(player_name)
    if not player_id:
        return None
    return await build_player_bundle(player_id)


# =========================
# ADP SEEDER
# =========================
async def ensure_seed_queue_loaded():
    adp_players = await fetch_adp_top_250()
    if not adp_players:
        return False

    init_seed_state_if_needed(adp_players)
    return True


async def seed_next_player_from_state(forum_channel: discord.ForumChannel):
    loaded = await ensure_seed_queue_loaded()
    if not loaded:
        log_adp("No ADP players loaded")
        return False, False

    if seed_state["completed"]:
        return False, True

    players = seed_state["seed_players"]

    while seed_state["current_index"] < len(players):
        row = players[seed_state["current_index"]]
        raw_name = row["name"]
        
        # Strip injury/status tags from Fantasy Pros names
        # 'Juan Soto (NYM - LF,RF) MiLB' -> 'Juan Soto'
        name = strip_injury_tags(raw_name)

        existing = await find_existing_profile_thread(forum_channel, name)
        if existing:
            log_adp(f"Already existed {name}")
            seed_state["current_index"] += 1
            if seed_state["current_index"] >= len(players):
                seed_state["completed"] = True
            save_seed_state()
            continue

        log_adp(f"Creating {name}")
        result = await create_profile_for_name(name, forum_channel)

        if result["status"] == "created":
            log_adp(f"Created {name}")
            seed_state["current_index"] += 1
            seed_state["last_run_ts"] = int(time.time())
            if seed_state["current_index"] >= len(players):
                seed_state["completed"] = True
            save_seed_state()
            return True, seed_state["completed"]

        if result["status"] == "exists":
            log_adp(f"Already existed {name}")
            seed_state["current_index"] += 1
            if seed_state["current_index"] >= len(players):
                seed_state["completed"] = True
            save_seed_state()
            continue

        if result["status"] in {"ambiguous", "no_match", "error"}:
            log_adp(f"Skipped {name} -> {result['status']}")
            seed_state["current_index"] += 1
            if seed_state["current_index"] >= len(players):
                seed_state["completed"] = True
            save_seed_state()
            continue

    seed_state["completed"] = True
    save_seed_state()
    log_adp("Top 250 complete")
    return False, True


async def background_seeder_loop():
    await bot.wait_until_ready()
    await backfill_done.wait()
    forum_channel = bot.get_channel(FORUM_CHANNEL_ID)

    if forum_channel is None:
        log_adp("Forum channel not found")
        return

    while not bot.is_closed():
        try:
            await ensure_seed_queue_loaded()

            if seed_state["completed"]:
                log_adp("Seeder complete. Stopping background seeder loop.")
                return

            if seed_state["paused"]:
                await asyncio.sleep(30)
                continue

            created_one, finished = await seed_next_player_from_state(forum_channel)

            if finished:
                log_adp("Seeder complete. Stopping background seeder loop.")
                return

            if created_one:
                sleep_for = random.randint(SEED_SLEEP_MIN_SECONDS, SEED_SLEEP_MAX_SECONDS)
                log_adp(f"Sleeping {sleep_for} seconds")
                await asyncio.sleep(sleep_for)
            else:
                await asyncio.sleep(1800)

        except Exception as e:
            log_adp(f"Seeder loop error: {e}")
            await asyncio.sleep(1800)


# =========================
# STATCAST CACHE CLEANUP
# =========================
async def periodic_statcast_cleanup():
    """Background task that clears idle Statcast cache every 30 minutes."""
    await bot.wait_until_ready()
    
    while not bot.is_closed():
        try:
            await asyncio.sleep(1800)  # 30 minutes
            cleared = clear_statcast_cache()
            if cleared > 0:
                log_profiles(f"Periodic cleanup freed {cleared} season(s) from Statcast cache")
        except Exception as e:
            log_profiles(f"Error in periodic cleanup: {e}")


# =========================
# BOT EVENTS
# =========================
async def backfill_thread_map():
    await bot.wait_until_ready()
    forum_channel = bot.get_channel(FORUM_CHANNEL_ID)
    if forum_channel is None:
        log_profiles("[BACKFILL] Forum channel not found, skipping backfill")
        return

    log_profiles("[BACKFILL] Scanning forum threads to populate thread map...")
    added = 0

    def index_thread(thread: discord.Thread):
        nonlocal added
        name = re.sub(r"\s+\([A-Z]{2,4}\)\s*$", "", thread.name.strip())
        if not name:
            return
        norm = normalize_text(name)
        for key in (name, norm):
            if key not in PLAYER_THREADS:
                PLAYER_THREADS[key] = thread.id
                added += 1

    for thread in forum_channel.threads:
        index_thread(thread)

    try:
        async for thread in forum_channel.archived_threads(limit=None):
            index_thread(thread)
    except Exception as e:
        log_profiles(f"[BACKFILL] Archived thread scan error: {e}")

    if added:
        with open(THREAD_MAP_FILE, "w") as f:
            json.dump(PLAYER_THREADS, f, indent=2)
        log_profiles(f"[BACKFILL] Added {added} threads to map ({len(PLAYER_THREADS)} total)")
    else:
        log_profiles(f"[BACKFILL] Thread map already up to date ({len(PLAYER_THREADS)} entries)")

    backfill_done.set()


@bot.event
async def on_ready():
    global seeder_task
    log_profiles(f"Logged in as {bot.user}")

    load_seed_state()
    load_thread_command_state()
    load_user_rate_limit_state()

    asyncio.create_task(backfill_thread_map())

    if seed_state.get("completed"):
        log_adp("Seeder already complete. Background seeder will not start.")
    elif seeder_task is None or seeder_task.done():
        seeder_task = asyncio.create_task(background_seeder_loop())
        log_adp("Seeder task started")

    # Start periodic Statcast cache cleanup (every 30 minutes)
    asyncio.create_task(periodic_statcast_cleanup())


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    content = message.content.strip()
    if not content.startswith("!"):
        return

    command_text = content[1:].strip()
    if not command_text:
        return

    lowered = command_text.lower()

    # =========================
    # THREAD COMMANDS
    # =========================
    if isinstance(message.channel, discord.Thread) and message.channel.parent_id == FORUM_CHANNEL_ID:
        thread = message.channel

        if lowered == "stats":
            if was_thread_command_used_today(thread.id, "stats"):
                await message.reply("Stats were already posted in this thread today.", mention_author=False)
                return

            bundle = await resolve_bundle_from_thread(thread)
            if not bundle:
                await message.reply("I couldn’t load this player’s stats right now.", mention_author=False)
                return

            embed = build_stats_embed(bundle)
            await thread.send(embed=embed)
            mark_thread_command_used_today(thread.id, "stats")
            return

        if lowered == "outlook":
            if current_local_date() < OUTLOOK_UNLOCK_DATE:
                await message.reply("`!outlook` is disabled until April 15, 2026.", mention_author=False)
                return

            if was_thread_command_used_today(thread.id, "outlook"):
                await message.reply("Outlook was already posted in this thread today.", mention_author=False)
                return

            bundle = await resolve_bundle_from_thread(thread)
            if not bundle:
                await message.reply("I couldn’t load this player’s outlook right now.", mention_author=False)
                return

            embed = build_outlook_embed(bundle)
            await thread.send(embed=embed)
            mark_thread_command_used_today(thread.id, "outlook")
            return


        if lowered == "splits":
            if was_thread_command_used_today(thread.id, "splits"):
                await message.reply("Splits were already posted in this thread today.", mention_author=False)
                return

            bundle = await resolve_bundle_from_thread(thread)
            if not bundle:
                await message.reply("I couldn't load this player's data right now.", mention_author=False)
                return

            player_id = bundle["profile"]["id"]
            is_pitcher = bundle["is_pitcher"]
            
            splits_data = await fetch_player_splits(player_id, PROFILE_SEASON)
            splits = parse_split_stats(splits_data, is_pitcher)
            
            embed = build_splits_embed(bundle["profile"], splits, is_pitcher)
            await thread.send(embed=embed)
            mark_thread_command_used_today(thread.id, "splits")
            return

        if lowered == "trends":
            if was_thread_command_used_today(thread.id, "trends"):
                await message.reply("Trends were already posted in this thread today.", mention_author=False)
                return

            bundle = await resolve_bundle_from_thread(thread)
            if not bundle:
                await message.reply("I couldn't load this player's data right now.", mention_author=False)
                return

            is_pitcher = bundle["is_pitcher"]
            embed = build_trends_embed(bundle["profile"], is_pitcher)
            await thread.send(embed=embed)
            mark_thread_command_used_today(thread.id, "trends")
            return

    # =========================
    # REQUEST THREAD COMMANDS
    # =========================
    if message.channel.id != REQUEST_THREAD_ID:
        return

    forum_channel = bot.get_channel(FORUM_CHANNEL_ID)
    if forum_channel is None:
        await message.reply(
            "I couldn’t find the player-profiles forum channel.",
            mention_author=False,
        )
        return

    if lowered in {"seedstatus", "seedpause", "seedresume", "seednext"}:
        if not is_seed_admin(message.author):
            await message.reply(
                "You are not allowed to use that command.",
                mention_author=False,
            )
            return

        try:
            await ensure_seed_queue_loaded()
        except Exception as e:
            log_adp(f"Seed queue load error: {e}")

        if lowered == "seedstatus":
            await message.reply(get_seed_status_text(), mention_author=False)
            return

        if lowered == "seedpause":
            seed_state["paused"] = True
            save_seed_state()
            await message.reply("Seeder paused.", mention_author=False)
            return

        if lowered == "seedresume":
            if seed_state.get("completed"):
                await message.reply("Seeder is already complete.", mention_author=False)
                return

            seed_state["paused"] = False
            save_seed_state()

            global seeder_task
            if seeder_task is None or seeder_task.done():
                seeder_task = asyncio.create_task(background_seeder_loop())
                log_adp("Seeder task restarted via seedresume")

            await message.reply("Seeder resumed.", mention_author=False)
            return

        if lowered == "seednext":
            try:
                created_one, finished = await seed_next_player_from_state(forum_channel)

                if created_one:
                    await message.reply(
                        f"Seeder advanced.\n{get_seed_status_text()}",
                        mention_author=False,
                    )
                    return

                if finished:
                    await message.reply(
                        f"Seeder is complete.\n{get_seed_status_text()}",
                        mention_author=False,
                    )
                    return

                await message.reply(
                    f"Seeder did not create a profile on this pass.\n{get_seed_status_text()}",
                    mention_author=False,
                )
                return

            except Exception as e:
                log_adp(f"Seed next error: {e}")
                await message.reply(
                    "Something went wrong while advancing the seeder.",
                    mention_author=False,
                )
                return

    raw_player = command_text
    
    # Rate limit check for non-admins
    if not is_seed_admin(message.author):
        can_proceed, requests_today = check_user_rate_limit(message.author.id)
        if not can_proceed:
            await message.reply(
                f"You've reached the daily limit of {MAX_PROFILE_REQUESTS_PER_DAY} profile requests. Try again tomorrow!",
                mention_author=False,
            )
            return

    # Use the queue to prevent concurrent profile creations (memory spikes)
    async with profile_creation_lock:
        queue_position = len(profile_creation_queue) + 1
        
        if queue_position > 1:
            await message.reply(
                f"Working on **{raw_player}**... (Position {queue_position} in queue)",
                mention_author=False,
            )
        else:
            await message.reply(
                f"Working on **{raw_player}**...",
                mention_author=False,
            )
        
        try:
            result = await create_profile_for_name(raw_player, forum_channel)
            
            # Only increment rate limit if profile was actually created (not if it already existed)
            if not is_seed_admin(message.author) and result.get("status") == "created":
                increment_user_rate_limit(message.author.id)
            
            await message.reply(result["message"], mention_author=False)

        except Exception as e:
            log_profiles(f"Create thread error: {e}")
            await message.reply(
                "Something went wrong while creating that profile.",
                mention_author=False,
            )


if not TOKEN:
    raise ValueError("PLAYER_PROFILES_TOKEN environment variable is not set.")


async def start_player_profiles_bot():
    await bot.start(TOKEN)

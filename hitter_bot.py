import asyncio
import json
import os
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import discord
import requests

# ---------------- CONFIG ----------------

TOKEN = os.getenv("ANALYTIC_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("HITTER_WATCH_CHANNEL_ID", "0"))

STATE_DIR = Path("state/hitter")
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state.json"

ET = ZoneInfo("America/New_York")
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType=R"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = int(os.getenv("HITTER_POLL_MINUTES", "10"))
RESET_HITTER_STATE = os.getenv("RESET_HITTER_STATE", "").lower() in {"1", "true", "yes"}
MIN_HITTER_SCORE = float(os.getenv("HITTER_MIN_SCORE", "2.0"))
MAX_CARDS_PER_GAME = int(os.getenv("HITTER_MAX_CARDS_PER_GAME", "10"))
MAX_BAD_CARDS_PER_GAME = int(os.getenv("HITTER_MAX_BAD_CARDS_PER_GAME", "3"))
REQUEST_TIMEOUT = float(os.getenv("HITTER_REQUEST_TIMEOUT", "30"))
MAX_POSTS_PER_SCAN = int(os.getenv("HITTER_MAX_POSTS_PER_SCAN", "20"))
MAX_POSTS_PER_GAME_PER_SCAN = int(os.getenv("HITTER_MAX_POSTS_PER_GAME_PER_SCAN", "18"))
POST_DELAY_SECONDS = float(os.getenv("HITTER_POST_DELAY_SECONDS", "1.25"))
AWAKE_SCAN_MIN_MINUTES = int(os.getenv("HITTER_AWAKE_SCAN_MIN_MINUTES", "10"))
AWAKE_SCAN_MAX_MINUTES = int(os.getenv("HITTER_AWAKE_SCAN_MAX_MINUTES", "8"))
HITTER_TEST_DATE = os.getenv("HITTER_TEST_DATE", "")  # e.g. "2026-04-01" — overrides today's date for testing
HITTER_TEST_LIMIT = int(os.getenv("HITTER_TEST_LIMIT", "0"))  # if > 0, cap games processed to this number
SLEEP_START_HOUR_ET = int(os.getenv("HITTER_SLEEP_START_HOUR_ET", "3"))
SLEEP_END_HOUR_ET = int(os.getenv("HITTER_SLEEP_END_HOUR_ET", "13"))
ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
ERROR_CHANNEL_ID = int(os.getenv("HITTER_ERROR_CHANNEL_ID", "0"))
ANTHROPIC_API_KEY = os.getenv("HITTER_BOT_SUMMARY")

intents = discord.Intents.default()
client: discord.Client | None = None
background_task: asyncio.Task | None = None
player_headshot_index: dict | None = None
hitter_stats_cache: dict = {}
decisive_event_cache: dict = {}


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
    "NYM": 0xFF5910,
    "NYY": 0x0C2340,
    "PHI": 0xE81828,
    "PIT": 0xFDB827,
    "SD": 0x2F241D,
    "SF": 0xFD5A1E,
    "SEA": 0x005C5C,
    "STL": 0xC41E3A,
    "TB": 0x092C5C,
    "TEX": 0x003278,
    "TOR": 0x134A8E,
    "WSH": 0xAB0003,
}

TEAM_NAME_MAP = {
    "ARI": "Diamondbacks",
    "ATH": "Athletics",
    "ATL": "Braves",
    "BAL": "Orioles",
    "BOS": "Red Sox",
    "CHC": "Cubs",
    "CWS": "White Sox",
    "CIN": "Reds",
    "CLE": "Guardians",
    "COL": "Rockies",
    "DET": "Tigers",
    "HOU": "Astros",
    "KC": "Royals",
    "LAA": "Angels",
    "LAD": "Dodgers",
    "MIA": "Marlins",
    "MIL": "Brewers",
    "MIN": "Twins",
    "NYM": "Mets",
    "NYY": "Yankees",
    "PHI": "Phillies",
    "PIT": "Pirates",
    "SD": "Padres",
    "SF": "Giants",
    "SEA": "Mariners",
    "STL": "Cardinals",
    "TB": "Rays",
    "TEX": "Rangers",
    "TOR": "Blue Jays",
    "WSH": "Nationals",
}


# ---------------- LOGGING / HELPERS ----------------

def log(msg: str) -> None:
    print(f"[HITTER] {msg}", flush=True)


def safe_int(value, default: int = 0) -> int:
    try:
        if value in (None, "", "-"):
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def normalize_team_abbr(team: str | None) -> str:
    key = str(team or "").strip().upper()
    aliases = {
        "AZ": "ARI",
        "CHW": "CWS",
        "WAS": "WSH",
        "WSN": "WSH",
        "TBR": "TB",
        "KCR": "KC",
        "SDP": "SD",
        "SFG": "SF",
        "OAK": "ATH",
    }
    return aliases.get(key, key)


def team_name_from_abbr(team: str | None) -> str:
    normalized = normalize_team_abbr(team)
    return TEAM_NAME_MAP.get(normalized, normalized or "opponent")


def team_possessive(team_name: str) -> str:
    cleaned = (team_name or "team").strip()
    if cleaned.endswith("s"):
        return f"{cleaned}'"
    return f"{cleaned}'s"


def get_logo(team: str | None) -> str:
    normalized = normalize_team_abbr(team)
    logo_key_map = {
        "CWS": "chw",
        "ATH": "oak",
        "ARI": "ari",
        "WSH": "wsh",
        "TB": "tb",
        "KC": "kc",
        "SD": "sd",
        "SF": "sf",
    }
    key = logo_key_map.get(normalized, normalized.lower())
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"


def normalize_lookup_name(name: str) -> str:
    cleaned = (name or "").lower()
    for ch in [".", ",", "'", "`", "-", "_", "(", ")", "[", "]"]:
        cleaned = cleaned.replace(ch, " ")
    return " ".join(cleaned.split())


# ---------------- HEADSHOTS ----------------

def load_player_headshot_index() -> dict:
    global player_headshot_index
    if player_headshot_index is not None:
        return player_headshot_index

    player_headshot_index = {}
    path = Path(ESPN_PLAYER_IDS_PATH)
    if not path.exists():
        log(f"Player ID file not found: {ESPN_PLAYER_IDS_PATH}")
        return player_headshot_index

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"Could not load player ID file: {exc}")
        return player_headshot_index

    if not isinstance(raw, dict):
        return player_headshot_index

    def coerce_headshot_payload(raw_name: str, entry) -> dict | None:
        if isinstance(entry, dict):
            headshot_url = entry.get("headshot_url")
            espn_id = entry.get("espn_id")
            team = normalize_team_abbr(entry.get("team"))
        elif isinstance(entry, str):
            stripped = entry.strip()
            if not stripped:
                return None
            if stripped.startswith("http://") or stripped.startswith("https://"):
                headshot_url = stripped
                espn_id = None
            elif stripped.isdigit():
                headshot_url = None
                espn_id = stripped
            else:
                return None
            team = None
        else:
            return None

        if not headshot_url and espn_id:
            headshot_url = f"https://a.espncdn.com/i/headshots/mlb/players/full/{espn_id}.png"
        if not headshot_url:
            return None

        return {
            "name": raw_name,
            "team": team,
            "headshot_url": headshot_url,
        }

    for raw_name, raw_value in raw.items():
        entries = raw_value if isinstance(raw_value, list) else [raw_value]
        seen_urls = set()
        for entry in entries:
            payload = coerce_headshot_payload(raw_name, entry)
            if not payload:
                continue
            headshot_url = payload.get("headshot_url")
            if not headshot_url or headshot_url in seen_urls:
                continue
            seen_urls.add(headshot_url)
            player_headshot_index.setdefault(raw_name, []).append(payload)
            normalized = normalize_lookup_name(raw_name)
            if normalized:
                player_headshot_index.setdefault(normalized, []).append(payload)

    log(f"Loaded player headshot index from {ESPN_PLAYER_IDS_PATH}")
    return player_headshot_index


def choose_headshot_entry(entries, team: str | None = None):
    if not entries:
        return None

    valid_entries = [entry for entry in entries if isinstance(entry, dict)]
    if not valid_entries:
        return None

    normalized_team = normalize_team_abbr(team) if team else None
    if normalized_team:
        for entry in valid_entries:
            if normalize_team_abbr(entry.get("team")) == normalized_team:
                return entry

    for entry in valid_entries:
        if entry.get("headshot_url"):
            return entry
    return valid_entries[0]


def get_player_headshot(name: str, team: str | None = None) -> str | None:
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

    last = normalized.split()[-1] if normalized else ""
    if not last:
        return None

    for key, entries in index.items():
        key_norm = normalize_lookup_name(key)
        if key_norm.split() and key_norm.split()[-1] == last:
            picked = choose_headshot_entry(entries, team)
            if picked:
                return picked.get("headshot_url")
    return None


def apply_player_card_chrome(embed: discord.Embed, name: str, team: str) -> None:
    display_team = normalize_team_abbr(team) or "UNK"
    logo_url = get_logo(display_team)
    headshot = get_player_headshot(name, team)

    try:
        embed.set_author(name=f"{name} | {display_team}", icon_url=logo_url)
    except Exception:
        embed.set_author(name=f"{name} | {display_team}")

    try:
        embed.set_thumbnail(url=headshot or logo_url)
    except Exception:
        pass


# ---------------- STATE ----------------

def load_state() -> dict:
    base = {"posted": []}
    if RESET_HITTER_STATE:
        return base
    if not STATE_FILE.exists():
        return base
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            base.update(data)
    except Exception:
        pass
    return base


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps({"posted": state.get("posted", [])}, indent=2), encoding="utf-8")




# ---------------- SEASON CONSTANTS ----------------

# Opening Day 2025 — opponent record context gates 7 days after this
OPENING_DAY = date(2025, 3, 27)

# Standings cache to avoid repeated API calls
_standings_cache: dict = {}
_standings_cache_date: str = ""

# Next game cache
_next_game_cache: dict = {}
_next_game_cache_date: str = ""


def get_next_opponent(team_abbr: str) -> str:
    """Return a plain-English next game description like 'at the Rockies tomorrow'
    or empty string if unavailable."""
    global _next_game_cache, _next_game_cache_date

    today = now_et().date()
    today_str = str(today)
    cache_key = normalize_team_abbr(team_abbr)

    if _next_game_cache_date == today_str and cache_key in _next_game_cache:
        return _next_game_cache.get(cache_key, "")

    # Reset cache for new day
    if _next_game_cache_date != today_str:
        _next_game_cache = {}
        _next_game_cache_date = today_str

    try:
        tomorrow = today + timedelta(days=1)
        # Check tomorrow and day after
        for target_date in [tomorrow, today + timedelta(days=2)]:
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType=R&date={target_date.isoformat()}"
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            for date_block in data.get("dates", []):
                for game in date_block.get("games", []):
                    teams = game.get("teams", {})
                    away_abbr = normalize_team_abbr(
                        teams.get("away", {}).get("team", {}).get("abbreviation", "")
                    )
                    home_abbr = normalize_team_abbr(
                        teams.get("home", {}).get("team", {}).get("abbreviation", "")
                    )

                    if cache_key not in (away_abbr, home_abbr):
                        continue

                    # Found the next game
                    is_home = cache_key == home_abbr
                    opp_abbr = away_abbr if is_home else home_abbr
                    opp_name = team_name_from_abbr(opp_abbr)
                    days_away = (target_date - today).days

                    when = "tomorrow" if days_away == 1 else target_date.strftime("%A")
                    location = "vs. the" if is_home else "at the"
                    result = f"{location} {opp_name} {when}"

                    # Cache all teams from this date to avoid repeat calls
                    for g in date_block.get("games", []):
                        gt = g.get("teams", {})
                        ga = normalize_team_abbr(gt.get("away", {}).get("team", {}).get("abbreviation", ""))
                        gh = normalize_team_abbr(gt.get("home", {}).get("team", {}).get("abbreviation", ""))
                        opp_a = team_name_from_abbr(gh)
                        opp_h = team_name_from_abbr(ga)
                        d = (target_date - today).days
                        w = "tomorrow" if d == 1 else target_date.strftime("%A")
                        if ga:
                            _next_game_cache[ga] = f"at the {opp_a} {w}"
                        if gh:
                            _next_game_cache[gh] = f"vs. the {opp_h} {w}"

                    return result
    except Exception:
        pass

    _next_game_cache[cache_key] = ""
    return ""


# Injury/transaction cache
_injury_cache: dict = {}
_injury_cache_date: str = ""


def get_player_injury_status(player_id: int, player_name: str) -> str:
    """Return injury note if player was recently activated from IL (within 14 days).
    Returns empty string if no recent IL stint found."""
    global _injury_cache, _injury_cache_date

    today = now_et().date()
    today_str = str(today)

    if _injury_cache_date != today_str:
        _injury_cache = {}
        _injury_cache_date = today_str

    cache_key = str(player_id)
    if cache_key in _injury_cache:
        return _injury_cache[cache_key]

    try:
        # Check MLB transactions for this player
        start_date = (today - timedelta(days=14)).isoformat()
        url = f"https://statsapi.mlb.com/api/v1/transactions?startDate={start_date}&endDate={today.isoformat()}"
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        for txn in data.get("transactions", []):
            p = txn.get("person", {}) or {}
            if safe_int(p.get("id", 0), 0) != player_id:
                continue
            txn_type = str(txn.get("typeDesc") or "").lower()
            if "activate" in txn_type or "reinstate" in txn_type:
                txn_date_str = txn.get("date") or txn.get("effectiveDate") or ""
                if txn_date_str:
                    try:
                        txn_date = date.fromisoformat(txn_date_str[:10])
                        days_ago = (today - txn_date).days
                        if days_ago == 0:
                            note = f"{_last_name(player_name)} was activated from the IL today."
                        elif days_ago == 1:
                            note = f"{_last_name(player_name)} was activated from the IL yesterday."
                        elif days_ago <= 7:
                            note = f"{_last_name(player_name)} was activated from the IL {days_ago} days ago."
                        else:
                            note = f"{_last_name(player_name)} returned from the IL {days_ago} days ago — worth noting as he builds back up."
                        _injury_cache[cache_key] = note
                        return note
                    except Exception:
                        pass
    except Exception:
        pass

    _injury_cache[cache_key] = ""
    return ""


def get_team_record(team_abbr: str) -> tuple[int, int]:
    """Return (wins, losses) for a team. Returns (-1, -1) if unavailable
    or if it's too early in the season to use records meaningfully."""
    global _standings_cache, _standings_cache_date

    today = now_et().date()
    days_into_season = (today - OPENING_DAY).days
    if days_into_season < 7:
        return (-1, -1)  # Too early — don't use records

    today_str = str(today)
    if _standings_cache_date != today_str or not _standings_cache:
        try:
            url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason"
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            _standings_cache = {}
            for record in data.get("records", []):
                for team_record in record.get("teamRecords", []):
                    abbr = normalize_team_abbr(
                        team_record.get("team", {}).get("abbreviation", "")
                    )
                    wins = safe_int(team_record.get("wins", 0), 0)
                    losses = safe_int(team_record.get("losses", 0), 0)
                    _standings_cache[abbr] = (wins, losses)
            _standings_cache_date = today_str
        except Exception:
            return (-1, -1)

    return _standings_cache.get(normalize_team_abbr(team_abbr), (-1, -1))


def _opponent_record_phrase(opponent_abbr: str) -> str:
    """Return a short phrase like 'the first-place Phillies (8-2)' or empty string."""
    wins, losses = get_team_record(opponent_abbr)
    if wins < 0:
        return ""
    total = wins + losses
    if total < 5:
        return ""  # Too few games for record to mean anything

    opp_name = team_name_from_abbr(opponent_abbr)
    record_str = f"{wins}-{losses}"

    # Add standing context if record is notable
    if wins > losses and wins >= 7:
        return random.choice([
            f"the {wins}-{losses} {opp_name}",
            f"a {opp_name} team that came in at {record_str}",
            f"the hot {opp_name} ({record_str})",
        ])
    if losses > wins and losses >= 7:
        return random.choice([
            f"the {wins}-{losses} {opp_name}",
            f"a struggling {opp_name} squad ({record_str})",
        ])
    return f"the {opp_name} ({record_str})"




# ---------------- SCHEDULING ----------------

def now_et() -> datetime:
    return datetime.now(ET)


def is_sleep_window(current_dt: datetime) -> bool:
    hour = current_dt.hour
    start = SLEEP_START_HOUR_ET
    end = SLEEP_END_HOUR_ET

    if start == end:
        return False
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def seconds_until_wake(current_dt: datetime) -> int:
    wake_today = current_dt.replace(hour=SLEEP_END_HOUR_ET, minute=0, second=0, microsecond=0)
    if current_dt < wake_today and is_sleep_window(current_dt):
        target = wake_today
    else:
        target = (current_dt + timedelta(days=1)).replace(hour=SLEEP_END_HOUR_ET, minute=0, second=0, microsecond=0)
    return max(int((target - current_dt).total_seconds()), 60)


def get_random_awake_interval_seconds() -> int:
    """Return a random interval in seconds between AWAKE_SCAN_MIN and MAX minutes."""
    import random as _random
    min_secs = max(AWAKE_SCAN_MIN_MINUTES, 1) * 60
    max_secs = max(AWAKE_SCAN_MAX_MINUTES, AWAKE_SCAN_MIN_MINUTES) * 60
    return _random.randint(min_secs, max_secs)




def parse_game_date_et(game: dict):
    game_date = game.get("gameDate")
    if not game_date:
        return now_et().date()

    try:
        parsed = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
        return parsed.astimezone(ET).date()
    except Exception:
        return now_et().date()


# ---------------- MLB DATA ----------------

def get_games() -> list[dict]:
    if HITTER_TEST_DATE:
        try:
            fetch_date = date.fromisoformat(HITTER_TEST_DATE)
        except ValueError:
            log(f"Invalid HITTER_TEST_DATE '{HITTER_TEST_DATE}', falling back to today")
            fetch_date = datetime.now(ET).date()
    else:
        fetch_date = datetime.now(ET).date()

    games: list[dict] = []
    try:
        response = requests.get(f"{SCHEDULE_URL}&date={fetch_date.isoformat()}", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        for date_block in payload.get("dates", []):
            games.extend(date_block.get("games", []))
    except Exception as exc:
        log(f"Schedule fetch error for {fetch_date}: {exc}")

    if HITTER_TEST_LIMIT > 0:
        games = games[:HITTER_TEST_LIMIT]
        log(f"HITTER_TEST_LIMIT={HITTER_TEST_LIMIT}: capped to {len(games)} games")

    return games


def get_feed(game_id: int) -> dict:
    response = requests.get(LIVE_URL.format(game_id), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def get_hitting_stats_for_date(target_date):
    if target_date in hitter_stats_cache:
        return hitter_stats_cache[target_date]

    stats_by_hitter: dict[int, dict] = {}
    try:
        response = requests.get(f"{SCHEDULE_URL}&date={target_date.isoformat()}", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        games = []
        for date_block in payload.get("dates", []):
            games.extend(date_block.get("games", []))

        for game in games:
            game_id = game.get("gamePk")
            if not game_id:
                continue
            try:
                feed = get_feed(int(game_id))
            except Exception:
                continue

            box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
            game_teams = feed.get("gameData", {}).get("teams", {})
            for side in ["home", "away"]:
                team = normalize_team_abbr(game_teams.get(side, {}).get("abbreviation"))
                if not team:
                    team = normalize_team_abbr(box.get(side, {}).get("team", {}).get("abbreviation", "UNK"))
                players = box.get(side, {}).get("players", {})
                for player in players.values():
                    stats = player.get("stats", {}).get("batting")
                    if not stats:
                        continue
                    hitter_id = player.get("person", {}).get("id")
                    if hitter_id is None:
                        continue
                    ab = safe_int(stats.get("atBats", 0), 0)
                    pa = ab + safe_int(stats.get("baseOnBalls", 0), 0) + safe_int(stats.get("hitByPitch", 0), 0) + safe_int(stats.get("sacFlies", 0), 0) + safe_int(stats.get("sacBunts", 0), 0)
                    if pa <= 0:
                        continue
                    stats_by_hitter[int(hitter_id)] = {
                        "ab": ab,
                        "h": safe_int(stats.get("hits", 0), 0),
                        "hr": safe_int(stats.get("homeRuns", 0), 0),
                        "rbi": safe_int(stats.get("rbi", 0), 0),
                        "r": safe_int(stats.get("runs", 0), 0),
                        "bb": safe_int(stats.get("baseOnBalls", 0), 0),
                        "k": safe_int(stats.get("strikeOuts", 0), 0),
                        "sb": safe_int(stats.get("stolenBases", 0), 0),
                        "2b": safe_int(stats.get("doubles", 0), 0),
                        "3b": safe_int(stats.get("triples", 0), 0),
                        "team": team,
                    }
    except Exception as exc:
        log(f"Hitting stats cache load failed for {target_date}: {exc}")

    hitter_stats_cache[target_date] = stats_by_hitter
    return stats_by_hitter


def get_recent_hitter_games(hitter_id: int, game_date_et, limit: int = 7, max_days: int = 30) -> list[dict]:
    recent_games: list[dict] = []
    if hitter_id is None or game_date_et is None:
        return recent_games

    check_date = game_date_et - timedelta(days=1)
    for _ in range(max_days):
        stats_by_hitter = get_hitting_stats_for_date(check_date)
        if hitter_id in stats_by_hitter:
            recent_games.append(stats_by_hitter[hitter_id])
            if len(recent_games) >= limit:
                break
        check_date -= timedelta(days=1)
    return recent_games


def _number_word(value: int) -> str:
    words = {0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten"}
    return words.get(int(value), str(value))


def _small_count_phrase(value: int, noun: str, plural: str | None = None, include_article: bool = False) -> str:
    plural = plural or f"{noun}s"
    value = int(value)
    if value == 1:
        return f"a {noun}" if include_article else noun
    if 2 <= value <= 10:
        return f"{_number_word(value)} {plural}"
    return f"{value} {plural}"


def _ordinal(value: int) -> str:
    value = int(value)
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def build_game_detail_sentences(context: dict, team: str, opponent_text: str, team_won: bool) -> list[str]:
    details: list[str] = []
    homers = context.get("homers") or []
    extra_base_hits = context.get("extra_base_hits") or []

    if homers:
        first_homer = homers[0]
        inning = safe_int(first_homer.get("inning", 0), 0)
        rbi = safe_int(first_homer.get("rbi", 0), 0)
        if context.get("go_ahead_homer") and inning:
            details.append(f"His homer in the {_ordinal(inning)} gave {team} the lead for good.")
        elif inning:
            details.append(f"He also went deep in the {_ordinal(inning)} against the {opponent_text}.")
        if rbi >= 3:
            details.append("That swing accounted for a huge chunk of the scoring in one shot.")

    if extra_base_hits:
        first_xbh = extra_base_hits[0]
        inning = safe_int(first_xbh.get("inning", 0), 0)
        hit_type = "double" if first_xbh.get("type") == "double" else "triple"
        if inning:
            details.append(f"He added a {hit_type} in the {_ordinal(inning)} to keep pressure on the pitching staff.")
        else:
            details.append(f"He also chipped in an extra-base hit against the {opponent_text}.")

    if context.get("first_run_hit"):
        details.append(f"He was responsible for getting {team} on the board first.")
    if context.get("first_lead_hit") and not context.get("go_ahead_hit"):
        details.append(f"He also helped {team} grab its first lead of the game.")
    if context.get("insurance_hit"):
        details.append(f"He later added insurance that helped {team} open up some breathing room.")
    elif context.get("late_rbi_hit"):
        details.append("His biggest damage also came once the game moved into the late innings.")

    balls_100 = safe_int(context.get("balls_100", 0), 0)
    hardest_ev = context.get("hardest_ev")
    if balls_100 >= 3:
        details.append(f"He produced {balls_100} batted balls at 100-plus mph in the game.")
    elif hardest_ev:
        if float(hardest_ev) >= 108:
            details.append(f"His loudest contact came at {float(hardest_ev):.1f} mph.")

    if team_won and not any(context.get(key) for key in ["walkoff", "go_ahead_hit", "go_ahead_homer"]):
        details.append(f"He was a big part of why {team} came away with this one.")

    return details



def _rbi_phrase(value: int) -> str:
    value = int(value)
    if value == 1:
        return "a run"
    if 2 <= value <= 10:
        return _number_word(value)
    return str(value)


def _homered_phrase(homers: int, rbi: int = 0) -> str:
    homers = int(homers)
    rbi = int(rbi)
    if homers <= 0:
        return ""
    if homers == 1:
        options = [
            "went deep",
            "homered",
            "left the yard",
            "launched a homer",
            "connected for a homer",
        ]
        if rbi == 4:
            options.extend(["hit a grand slam", "cleared the bases with a grand slam", "went deep with the bases loaded"])
        elif rbi == 3:
            options.extend(["launched a three-run homer", "hit a three-run shot"])
        elif rbi == 2:
            options.extend(["launched a two-run shot", "connected for a two-run homer"])
        return random.choice(options)
    return random.choice([
        f"homered {homers} times",
        f"left the yard {homers} times",
        f"went deep {homers} times",
    ])


def _join_phrases(parts: list[str]) -> str:
    parts = [p for p in parts if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return ", ".join(parts[:-1]) + f", and {parts[-1]}"


def _pick_context_sentences(context_pool: list[str], count: int = 2) -> list[str]:
    seen = set()
    chosen = []
    for sentence in context_pool:
        if sentence and sentence not in seen:
            chosen.append(sentence)
            seen.add(sentence)
        if len(chosen) >= count:
            break
    return chosen


def _final_score_by_side(feed: dict) -> tuple[int, int]:
    linescore = feed.get("liveData", {}).get("linescore", {}) or {}
    teams = linescore.get("teams", {}) or {}
    away = safe_int(teams.get("away", {}).get("runs", 0), 0)
    home = safe_int(teams.get("home", {}).get("runs", 0), 0)
    return away, home


def _get_decisive_event(feed: dict) -> dict:
    game_pk = str(feed.get("gameData", {}).get("game", {}).get("pk", ""))
    if game_pk in decisive_event_cache:
        return decisive_event_cache[game_pk]

    final_away, final_home = _final_score_by_side(feed)
    winner_side = "away" if final_away > final_home else "home" if final_home > final_away else ""
    if not winner_side:
        decisive_event_cache[game_pk] = {}
        return {}

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
    decisive: dict = {}
    for idx, play in enumerate(plays):
        result = play.get("result", {}) or {}
        about = play.get("about", {}) or {}
        if not about.get("isScoringPlay"):
            continue

        away_after = safe_int(result.get("awayScore", 0), 0)
        home_after = safe_int(result.get("homeScore", 0), 0)

        if winner_side == "away":
            if away_after <= home_after:
                continue
            remained_ahead = True
            for later in plays[idx + 1:]:
                later_result = later.get("result", {}) or {}
                later_away = safe_int(later_result.get("awayScore", away_after), 0)
                later_home = safe_int(later_result.get("homeScore", home_after), 0)
                if later_away <= later_home:
                    remained_ahead = False
                    break
            if remained_ahead:
                decisive = play
                break
        else:
            if home_after <= away_after:
                continue
            remained_ahead = True
            for later in plays[idx + 1:]:
                later_result = later.get("result", {}) or {}
                later_away = safe_int(later_result.get("awayScore", away_after), 0)
                later_home = safe_int(later_result.get("homeScore", home_after), 0)
                if later_home <= later_away:
                    remained_ahead = False
                    break
            if remained_ahead:
                decisive = play
                break

    decisive_event_cache[game_pk] = decisive or {}
    return decisive_event_cache[game_pk]


def build_hitter_game_context(feed: dict, hitter: dict) -> dict:
    hitter_id = hitter.get("id")
    side = hitter.get("side")
    context = {
        "go_ahead_hit": False,
        "go_ahead_homer": False,
        "game_tying_hit": False,
        "walkoff": False,
        "insurance_hit": False,
        "late_rbi_hit": False,
        "multi_rbi_hit": False,
        "first_run_hit": False,
        "first_lead_hit": False,
        "hardest_ev": None,
        "balls_100": 0,
        "homers": [],
        "extra_base_hits": [],
        "steals": [],         # {base: str, inning: int}
        "pa": 0,              # plate appearances
    }
    if hitter_id is None:
        return context

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in plays:
        matchup = play.get("matchup", {}) or {}
        batter = matchup.get("batter", {}) or {}
        if batter.get("id") != hitter_id:
            continue

        result = play.get("result", {}) or {}
        event = str(result.get("event") or "").lower()
        event_type = str(result.get("eventType") or "").lower()
        rbi = safe_int(result.get("rbi", 0), 0)
        about = play.get("about", {}) or {}
        inning = safe_int(about.get("inning", 0), 0)
        is_scoring_play = bool(about.get("isScoringPlay"))
        half = str(about.get("halfInning") or "").lower()

        if side == "away":
            before_team = safe_int(result.get("awayScore", 0), 0) - rbi
            before_opp = safe_int(result.get("homeScore", 0), 0)
            after_team = safe_int(result.get("awayScore", 0), 0)
            after_opp = safe_int(result.get("homeScore", 0), 0)
        else:
            before_team = safe_int(result.get("homeScore", 0), 0) - rbi
            before_opp = safe_int(result.get("awayScore", 0), 0)
            after_team = safe_int(result.get("homeScore", 0), 0)
            after_opp = safe_int(result.get("awayScore", 0), 0)

        if is_scoring_play and rbi > 0:
            if before_team == 0 and before_opp == 0 and after_team > 0:
                context["first_run_hit"] = True
            if before_team <= before_opp and after_team > after_opp:
                context["go_ahead_hit"] = True
                context["first_lead_hit"] = True
                if "home run" in event or event_type == "home_run":
                    context["go_ahead_homer"] = True
            elif before_team < before_opp and after_team == after_opp:
                context["game_tying_hit"] = True
            elif inning >= 7 and after_team > before_team and after_team > after_opp and (after_team - before_opp) >= 2:
                context["insurance_hit"] = True
            elif inning >= 7:
                context["late_rbi_hit"] = True

        if rbi >= 2:
            context["multi_rbi_hit"] = True

        if inning >= 9 and is_scoring_play and after_team > after_opp:
            if (side == "home" and half == "bottom") or (side == "away" and half == "top"):
                context["walkoff"] = side == "home" and half == "bottom"

        for event_obj in play.get("playEvents", []) or []:
            hit_data = event_obj.get("hitData") if isinstance(event_obj, dict) else None
            if not isinstance(hit_data, dict):
                continue
            launch_speed = hit_data.get("launchSpeed")
            if launch_speed not in (None, ""):
                ev = float(launch_speed)
                if context["hardest_ev"] is None or ev > context["hardest_ev"]:
                    context["hardest_ev"] = ev
                if ev >= 100:
                    context["balls_100"] += 1

        # Count plate appearances
        context["pa"] += 1

        # Extract pitch type and hit location for homers/XBH
        pitch_type = ""
        pitch_speed = 0
        hit_trajectory = ""
        hit_location = ""  # "pull", "center", "oppo"

        for event_obj in play.get("playEvents", []) or []:
            if not isinstance(event_obj, dict):
                continue
            # Find the pitch that ended the at-bat
            if event_obj.get("type") == "pitch" and event_obj.get("atBatIndex") is not None:
                pitch_details = event_obj.get("pitchData", {}) or {}
                pitch_type_obj = event_obj.get("details", {}) or {}
                raw_type = pitch_type_obj.get("type", {}) or {}
                pitch_type = str(raw_type.get("description") or "").strip()
                pitch_speed = float(pitch_details.get("startSpeed") or 0)
            # Hit data for trajectory/location
            hit_data = event_obj.get("hitData") if isinstance(event_obj, dict) else None
            if isinstance(hit_data, dict):
                hit_trajectory = str(hit_data.get("trajectory") or "").lower()
                # Determine pull/center/oppo from hit coordinates
                coords = hit_data.get("coordinates") or {}
                coord_x = float(coords.get("coordX") or 0)
                # MLB coordinate system: 125 = roughly center
                # Left side of field = lower x (pull for RHH, oppo for LHH)
                if coord_x > 0:
                    if coord_x < 100:
                        hit_location = "left"
                    elif coord_x > 155:
                        hit_location = "right"
                    else:
                        hit_location = "center"

        # Stolen base events
        if event_type in {"stolen_base_2b", "stolen_base_3b", "stolen_base_home"}:
            base_map = {
                "stolen_base_2b": "second",
                "stolen_base_3b": "third",
                "stolen_base_home": "home",
            }
            context["steals"].append({
                "base": base_map.get(event_type, "second"),
                "inning": inning,
            })

        if "home run" in event or event_type == "home_run":
            context["homers"].append({
                "inning": inning,
                "rbi": rbi,
                "pitch_type": pitch_type,
                "pitch_speed": pitch_speed,
                "trajectory": hit_trajectory,
                "location": hit_location,
            })
        if event_type in {"double", "triple"}:
            context["extra_base_hits"].append({
                "type": event_type,
                "inning": inning,
                "rbi": rbi,
                "trajectory": hit_trajectory,
                "location": hit_location,
            })

    decisive_play = _get_decisive_event(feed)
    decisive_batter_id = ((decisive_play.get("matchup", {}) or {}).get("batter", {}) or {}).get("id") if decisive_play else None
    if decisive_batter_id != hitter_id:
        context["walkoff"] = False
        context["go_ahead_hit"] = False
        context["go_ahead_homer"] = False

    if context["hardest_ev"] is not None:
        context["hardest_ev"] = round(float(context["hardest_ev"]), 1)
    return context

def build_recent_form_blurb(recent_games: list[dict], stats: dict) -> str:
    if not recent_games:
        return ""
    total_hits = sum(g.get("h", 0) for g in recent_games[:5]) + safe_int(stats.get("hits", 0), 0)
    total_hr = sum(g.get("hr", 0) for g in recent_games[:5]) + safe_int(stats.get("homeRuns", 0), 0)
    total_rbi = sum(g.get("rbi", 0) for g in recent_games[:5]) + safe_int(stats.get("rbi", 0), 0)
    games = len(recent_games[:5]) + 1

    streak_hits = 0
    for game in recent_games:
        if game.get("h", 0) > 0:
            streak_hits += 1
        else:
            break
    hit_streak = streak_hits + (1 if safe_int(stats.get("hits", 0), 0) > 0 else 0)

    hitless_tail = 0
    for game in recent_games:
        if game.get("h", 0) == 0:
            hitless_tail += 1
        else:
            break

    if hitless_tail >= 3 and safe_int(stats.get("hits", 0), 0) >= 2:
        return random.choice([
            "It also looked like a possible step out of a recent skid.",
            "After a quiet stretch, this looked more like the hitter they needed.",
            "It felt like the sort of night that can pull a hitter out of a slump.",
        ])
    if hit_streak >= 5:
        return random.choice([
            f"He has now pushed his hitting streak to {hit_streak} games.",
            f"That keeps a hot stretch going, with hits now in {hit_streak} straight games.",
            f"He has kept the heater going with hits in {hit_streak} straight games.",
        ])
    if games >= 5 and total_hits >= 8:
        return random.choice([
            f"He has piled up {total_hits} hits over his last {games} games.",
            f"That gives him {total_hits} hits across his last {games} games and keeps the recent run rolling.",
        ])
    if games >= 5 and total_hr >= 3:
        return random.choice([
            f"He has now left the yard {total_hr} times over his last {games} games.",
            f"The power has been carrying over lately, with {total_hr} homers across his last {games} games.",
        ])
    if games >= 5 and total_rbi >= 8:
        return random.choice([
            f"He has also driven in {total_rbi} runs over his last {games} games.",
            f"The recent production keeps building, with {total_rbi} RBI across his last {games} games.",
        ])
    return ""


def get_hitters(feed: dict) -> list[dict]:
    results: list[dict] = []
    box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    game_teams = feed.get("gameData", {}).get("teams", {})

    for side in ["home", "away"]:
        team = normalize_team_abbr(game_teams.get(side, {}).get("abbreviation"))
        if not team:
            team = normalize_team_abbr(box.get(side, {}).get("team", {}).get("abbreviation", "UNK"))

        players = box.get(side, {}).get("players", {})
        for player in players.values():
            stats = player.get("stats", {}).get("batting")
            if not stats:
                continue

            ab = safe_int(stats.get("atBats", 0), 0)
            pa = (
                ab
                + safe_int(stats.get("baseOnBalls", 0), 0)
                + safe_int(stats.get("hitByPitch", 0), 0)
                + safe_int(stats.get("sacFlies", 0), 0)
                + safe_int(stats.get("sacBunts", 0), 0)
            )
            if pa <= 0:
                continue

            season_stats_block = player.get("seasonStats", {})
            if isinstance(season_stats_block, dict) and "batting" in season_stats_block:
                season_stats = season_stats_block.get("batting", {})
            elif isinstance(season_stats_block, dict):
                season_stats = season_stats_block
            else:
                season_stats = {}

            results.append(
                {
                    "id": player.get("person", {}).get("id"),
                    "name": player.get("person", {}).get("fullName", "Unknown Hitter"),
                    "team": team,
                    "side": side,
                    "position": player.get("position", {}).get("abbreviation", ""),
                    "stats": stats,
                    "season_stats": season_stats,
                }
            )
    return results


# ---------------- HITTER SCORING ----------------

def hitter_total_bases(stats: dict) -> int:
    hits = safe_int(stats.get("hits", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    singles = max(hits - doubles - triples - homers, 0)
    return singles + doubles * 2 + triples * 3 + homers * 4


def score_hitter(stats: dict) -> float:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    total_bases = hitter_total_bases(stats)
    return round(
        total_bases * 1.55
        + rbi * 1.35
        + runs * 1.1
        + walks * 0.6
        + steals * 1.25
        + homers * 1.5
        + hits * 0.25
        - strikeouts * 0.25,
        2,
    )


def classify_hitter(stats: dict) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    xbh = homers + doubles + triples

    if homers >= 2:
        return "power_show"
    if hits >= 4:
        return "hit_parade"
    if hits >= 3 and xbh >= 2:
        return "loud_three_hit"
    if homers >= 1 and rbi >= 3:
        return "impact_power"
    if steals >= 2:
        return "speed_pressure"
    if rbi >= 4:
        return "run_producer"
    if hits >= 3:
        return "steady_attack"
    if walks >= 2 and hits <= 1:
        return "on_base_grinder"
    return "solid_night"



# ---------------- CARD TEXT ----------------

_PREMIUM_POSITIONS = {"C", "SS", "2B", "3B"}
_MIDDLE_ORDER_SPOTS = {3, 4, 5}

SUBJECT_OPENING_FAMILIES = {
    "walkoff": [
        "{name} delivers the walk-off for his team",
        "{name} ends it with the walk-off swing",
        "{name} is the difference with a walk-off finish",
        "{name} plays hero with the walk-off hit",
        "{name} closes it out with the final swing",
        "{name} sends everyone home with a walk-off knock",
        "{name} finishes it with the game-ending swing",
        "{name} comes through with the walk-off winner",
        "{name} seals it in the final at-bat",
        "{name} provides the walk-off moment",
        "{name} breaks it open at the very end",
        "{name} delivers the final blow",
        "{name} sends the home crowd home happy",
        "{name} is the hero in the final frame",
        "{name} walks it off with one swing",
    ],
    "go_ahead_homer": [
        "{name} launches the homer that decided it",
        "{name} puts the club ahead for good with a big homer",
        "{name} changes the game with a go-ahead blast",
        "{name} breaks it open with the decisive homer",
        "{name} flips the game with one swing",
        "{name} delivers the swing that held up",
        "{name} swings the game with a late homer",
        "{name} provides the game-changing homer",
        "{name} gives his squad the lead for keeps with a blast",
        "{name} leaves the yard for the decisive swing",
        "{name} turns the game with a timely homer",
        "{name} provides the go-ahead power",
        "{name} hits the homer that proved to be enough",
        "{name} delivers the biggest swing of the night",
        "{name} changes the scoreboard with one loud swing",
    ],
    "go_ahead_hit": [
        "{name} comes through with the hit that changed the game",
        "{name} delivers the hit that put his side ahead for good",
        "{name} provides the decisive hit",
        "{name} comes up with the swing that held up",
        "{name} turns the game with one key knock",
        "{name} lines the hit that proved to be enough",
        "{name} changes the night with a timely hit",
        "{name} provides the late swing that mattered most",
        "{name} comes through when the game was hanging there",
        "{name} puts the offense ahead with the deciding hit",
        "{name} delivers in the biggest spot",
        "{name} breaks it open with the key hit",
        "{name} pushes his team in front with a timely knock",
        "{name} provides the swing that became the difference",
        "{name} cashes in with the decisive knock",
    ],
    "game_tying": [
        "{name} pulls his team even with one swing",
        "{name} brings the club back level with a big hit",
        "{name} ties the game with a timely blow",
        "{name} helps erase the deficit with his biggest swing",
        "{name} comes through with the equalizer",
        "{name} drags his squad back even",
        "{name} supplies the game-tying damage",
        "{name} wipes out the deficit with a clutch hit",
        "{name} keeps the game alive with a timely swing",
        "{name} delivers the hit that reset the game",
        "{name} changes the tone with the tying blow",
        "{name} gives his side new life with the equalizer",
        "{name} produces the game-tying moment",
        "{name} helps turn the game back into a coin flip",
        "{name} pulls the offense back into it",
    ],
    "two_homer": [
        "{name} turns in a two-homer night",
        "{name} homers twice in a standout game",
        "{name} leaves the yard twice in a huge line",
        "{name} puts together a two-homer performance",
        "{name} goes deep twice in a monster effort",
        "{name} powers his way to a two-homer game",
        "{name} drives the offense with two long balls",
        "{name} delivers a multi-homer outburst",
        "{name} blasts two homers in a big night",
        "{name} puts on a power show with two homers",
        "{name} piles up damage with a pair of homers",
        "{name} fuels his team with two long balls",
        "{name} supplies the thunder with two homers",
        "{name} has his power on full display with two homers",
        "{name} launches a pair in a huge fantasy line",
    ],
    "four_hit": [
        "{name} piles up four hits in a standout game",
        "{name} fills the box score with a four-hit night",
        "{name} racks up four hits in a huge effort",
        "{name} keeps the hit parade rolling with four hits",
        "{name} puts together a four-hit performance",
        "{name} sprays four hits all over the yard",
        "{name} strings together a four-hit game",
        "{name} turns in one of the louder four-hit lines of the night",
        "{name} reaches four hits in a complete effort",
        "{name} has a four-hit night to remember",
        "{name} does damage all night with four hits",
        "{name} keeps finding barrel after barrel in a four-hit game",
        "{name} stacks up four hits in a big fantasy line",
        "{name} powers through with four hits",
        "{name} produces from first inning to last with four hits",
    ],
    "three_hit_xbh": [
        "Not just a volume game either, he mixed in extra-base damage.",
        "Three hits and extra-base damage in the same night is a tough line to ignore.",
        "The extra-base work gave the performance some real teeth.",
        "He didn't just put the ball in play, he put it in play hard.",
        "Three hits is one thing, but adding extra-base damage makes it a different kind of night.",
        "The extra-base hit was the exclamation point on an already strong line.",
        "He made the pitcher pay for every mistake, and there was extra-base damage to show for it.",
        "Three hits with some pop mixed in is exactly what you want to see.",
        "The damage wasn't all singles, which made the line stand out more.",
        "He stayed aggressive at the plate all night and it showed in the extra-base work.",
        "Volume and impact on the same night is a hard combination to beat.",
        "He was putting good swings on everything, and the extra-base hit proved it.",
        "Three hits and extra bases in one game is a real quality performance.",
        "The contact was crisp all night, and the extra-base damage was proof of that.",
        "He had his A-swing working from the first at-bat.",
    ],
    "middle_order": [
        "{name} drives the offense from a middle of the order role",
        "{name} cashes in from the heart of the order",
        "{name} makes the most of his run-producing spot",
        "{name} delivers the kind of production you want from the middle of the order",
        "{name} comes through in a run-producing role",
        "{name} turns chances into damage from the heart of the order",
        "{name} gives his team what it needed from a premium RBI role",
        "{name} provides middle-order thump",
        "{name} drives in damage from a key run-producing role",
        "{name} cashes in the traffic around him",
        "{name} anchors the offense with a productive game",
        "{name} turns his place in the heart of the order into real fantasy value",
        "{name} makes his heart of the order at-bats count",
        "{name} does middle-order work all night",
        "{name} turns opportunity into production from the heart of the order",
    ],
    "speed": [
        "{name} makes things happen with his legs and his bat",
        "{name} adds speed to a productive night",
        "{name} changes the game with his legs and his bat",
        "{name} gives the box score extra fantasy value with his speed",
        "{name} makes a multi-category impact",
        "{name} uses both contact and speed to build a useful line",
        "{name} piles up value with his wheels",
        "{name} adds another layer with his running game",
        "{name} turns a good night into a better one with his speed",
        "{name} finds a way to help in several categories",
        "{name} brings plenty of speed to the box score",
        "{name} boosts the box score with his legs",
        "{name} creates extra value once he gets on base",
        "{name} makes a real fantasy impact on the bases",
        "{name} turns his speed into extra category juice",
    ],
    "hot_streak": [
        "{name} keeps the hot streak going",
        "{name} stays hot with another productive night",
        "{name} keeps the recent run rolling",
        "{name} continues his strong stretch",
        "{name} stays locked in at the plate",
        "{name} keeps adding to his hot run",
        "{name} keeps the momentum moving",
        "{name} stays in rhythm with another big game",
        "{name} keeps the heater going",
        "{name} turns in another strong line during a hot stretch",
        "{name} keeps the recent production flowing",
        "{name} continues to build on a good run",
        "{name} stays productive as the streak continues",
        "{name} keeps showing up in the box score",
        "{name} keeps carrying the bat well",
    ],
    "single_homer": [
        "{name} leaves the yard in a productive night",
        "{name} provides the big swing in a strong line",
        "{name} does his damage with one loud swing",
        "{name} supplies the homer in a useful fantasy line",
        "{name} adds a needed homer to a productive game",
        "{name} drives the ball out and turns in a strong night",
        "{name} delivers the power in a useful performance",
        "{name} turns one swing into a big line",
        "{name} gives the club a loud moment with a homer",
        "{name} produces a homer as part of a quality night",
        "{name} changes the box score with one long ball",
        "{name} turns the power on in a strong effort",
        "{name} makes the box score pop with a homer",
        "{name} brings the thunder in a productive performance",
        "{name} doesn't need many swings to make an impact",
    ],
    "solid": [
        "{name} puts together a useful night at the plate",
        "{name} chips in with a steady offensive game",
        "{name} turns in a productive night at the plate",
        "{name} gives his squad a solid offensive game",
        "{name} quietly builds a helpful line",
        "{name} finds a way to contribute across the board",
        "{name} pieces together a steady fantasy line",
        "{name} comes away with a quietly useful game",
        "{name} adds a helpful offensive line",
        "{name} puts together a line that plays in fantasy leagues",
        "{name} does enough to matter in several categories",
        "{name} turns a modest stat line into a helpful one",
        "{name} contributes more than a basic box score would suggest",
        "{name} gives managers a steady return",
        "{name} turns in a line with some sneaky value",
    ],
}

OPENING_FAMILY_POOL = [
    # Name-first openers
    "{name} went {stat_phrase} {result_phrase}.",
    "{name} finished {stat_phrase} {result_phrase}.",
    "{name} was {stat_phrase} {result_phrase}.",
    "{name} went {stat_phrase} as the {team_name} {result_verb} the {opponent_text}.",
    "{name} had {stat_phrase} {result_phrase}.",
    "Good night for {name}, who went {stat_phrase} {result_phrase}.",
    "{name} did some damage {result_phrase}, going {stat_phrase}.",
    "Another productive night for {name}: {stat_phrase} {result_phrase}.",
    "{name} kept rolling {result_phrase}, going {stat_phrase}.",
    "{name} came up big {result_phrase}, going {stat_phrase}.",
    "{name} delivered {result_phrase}, finishing {stat_phrase}.",
    "{name} made his at-bats count {result_phrase}, going {stat_phrase}.",
    "{name} had a strong one {result_phrase}: {stat_phrase}.",
    # Result-first openers — lead with the team/moment
    "The {team_name} {result_verb} the {opponent_text} and {name} was a big reason why, going {stat_phrase}.",
    "When the {team_name} needed offense, {name} delivered — {stat_phrase} {result_phrase}.",
    "{stat_phrase} {result_phrase} for {name}, who was one of the best bats on the field.",
    "It was a good night to be {name} — {stat_phrase} {result_phrase}.",
    "{name} did his part {result_phrase}: {stat_phrase}.",
]

CONTEXT_FAMILY_POOL = [
    "He {event_text} {inning_text}.",
    "{inning_text}, he {event_text}.",
    "The big moment came {inning_text}: he {event_text}.",
    "He {event_text} {inning_text} to do the most damage.",
    "Most of the damage was done {inning_text}, when he {event_text}.",
    "He {event_text} {inning_text}, which was the swing that really mattered.",
    "The night turned {inning_text} when he {event_text}.",
    "His biggest moment came {inning_text}, when he {event_text}.",
    "He came through {inning_text} and {event_text}.",
    "The key blow came {inning_text}: he {event_text}.",
    "He {event_text} {inning_text} for the biggest hit of his night.",
    "Things got interesting {inning_text} when he {event_text}.",
    "He {event_text} {inning_text} to give his side a lift.",
    "His best swing came {inning_text}, when he {event_text}.",
    "The {inning_text} was when he {event_text} and changed the game.",
    "He {event_text} {inning_text} and that was the ballgame.",
    "The offense really got going {inning_text}, with him leading the way.",
    "He made it count {inning_text} when he {event_text}.",
    "That {inning_short} at-bat was the one that defined his night.",
    "He stepped up {inning_text} and {event_text}.",
]

FANTASY_CLOSING_POOL = [
    "He made every at-bat count.",
    "Complete game from top to bottom.",
    "He gave the team everything it needed.",
    "The production was consistent all night.",
    "He showed up when it counted.",
    "One of his better all-around games of the year.",
    "He was a problem every time he came up.",
    "There was nothing fluky about it.",
    "He earned every bit of that line.",
    "The at-bats were quality from start to finish.",
    "He put the team on his back and delivered.",
    "The numbers don't lie — that was a good night.",
    "He did the work and the box score shows it.",
    "Hard to find a complaint with that line.",
    "He was at his best when the team needed him.",
    "The production held up from start to finish.",
    "He gave the pitching staff something to work with.",
    "That's the version of him everyone wants to see.",
    "He made pitchers work and it paid off.",
    "The results matched the effort tonight.",
]

EV_HIT_FAMILIES = [
    "He also {verb} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "The loudest swing of his night was {article_hit}{inning_piece} struck at {ev:.1f} mph.",
    "His hardest-hit ball was {article_hit}{inning_piece} that left the bat at {ev:.1f} mph.",
    "He backed up the production by {verb_ing} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "The contact quality showed up on {article_hit}{inning_piece}, which came off the bat at {ev:.1f} mph.",
    "He added another layer to the box score by {verb_ing} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "There was authority behind {article_hit}{inning_piece}, which registered at {ev:.1f} mph.",
    "He didn't just produce; he {verb_past} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "The hardest contact came on {article_hit}{inning_piece}, measured at {ev:.1f} mph.",
    "He also {verb_past} {article_hit}{inning_piece} that jumped off the bat at {ev:.1f} mph.",
    "A particularly loud swing came on {article_hit}{inning_piece}, which left the bat at {ev:.1f} mph.",
    "He produced {article_hit}{inning_piece} with an exit velocity of {ev:.1f} mph.",
    "The performance had some real impact contact too, including {article_hit}{inning_piece} at {ev:.1f} mph.",
    "He showed off the bat speed on {article_hit}{inning_piece}, which came off at {ev:.1f} mph.",
    "One of the louder moments came when he {verb_past} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "He also squared up {article_hit}{inning_piece} at {ev:.1f} mph.",
    "The contact quality popped on {article_hit}{inning_piece}, which was clocked at {ev:.1f} mph.",
    "He added some real thump with {article_hit}{inning_piece} struck at {ev:.1f} mph.",
    "The bat did serious work on {article_hit}{inning_piece}, which registered {ev:.1f} mph.",
    "He paired the production with {article_hit}{inning_piece} that came off the bat at {ev:.1f} mph.",
]

PITCHER_EVENT_FAMILIES = [
    "Doing that against {pitcher} adds a little more weight to the {event_phrase}.",
    "The {event_phrase} came against {pitcher}, which is not exactly a soft matchup.",
    "There is a little more substance here because the {event_phrase} came against {pitcher}.",
    "That {event_phrase} plays up a bit more given that {pitcher} was on the mound.",
    "The quality of opponent matters here too, as the {event_phrase} came against {pitcher}.",
    "It wasn't just who he did it against, but what he did against {pitcher}: the {event_phrase}.",
    "The matchup adds some context here, since the {event_phrase} came against {pitcher}.",
    "That line looks a bit better when you remember the {event_phrase} came against {pitcher}.",
    "The opponent quality helps this stand out, with the {event_phrase} coming against {pitcher}.",
    "This wasn't empty production, especially with the {event_phrase} coming against {pitcher}.",
    "It is worth giving the box score a little extra credit because the {event_phrase} came off {pitcher}.",
    "There was some real matchup difficulty here, and he still got to {pitcher} for the {event_phrase}.",
    "The {event_phrase} came against one of the tougher arms he will see in {pitcher}.",
    "That production looks a little sturdier with {pitcher} attached to the {event_phrase}.",
    "The degree of difficulty rises a bit when the {event_phrase} comes against {pitcher}.",
]

LINEUP_FAMILIES = {
    "leadoff": [
        "Batting leadoff, he did a good job setting the tone.",
        "Working from the leadoff role, he gave the offense the kind of table-setting it needed.",
        "Hitting first, he kept finding ways to get on and make things happen.",
        "This was the sort of table-setting work you want from a leadoff hitter.",
        "He made the top of the order work in his favor.",
        "That is a strong showing for a leadoff man, especially with how often he pressured the defense.",
        "He gave the offense a steady push from the leadoff spot.",
        "There was some real top of the order value in the way he built this game.",
        "He made life easier on the bats behind him while hitting first.",
        "The offense got real momentum from the leadoff spot here.",
        "He looked comfortable doing table-setting work from the top of the order.",
        "This was a good example of how a leadoff hitter can influence the whole game.",
        "Batting first helped the box score play up in both real baseball and fantasy terms.",
        "He did what a leadoff man is supposed to do and then some.",
        "The leadoff spot fit him well tonight.",
    ],
    "middle": [
        "He batted in the heart of the order and delivered.",
        "That's exactly what you want from a middle of the order bat.",
        "He hit where the damage is supposed to happen and did damage.",
        "Cleanup type production, he cashed in with runners on.",
        "The offense ran through him tonight.",
        "He was the best hitter in the lineup when it mattered.",
        "Batting third through fifth means opportunity, and he didn't waste it.",
        "He did what the middle of the order is supposed to do.",
        "The run-producing role fit him perfectly tonight.",
        "He turned chances into runs, that's the job.",
        "Middle of the order bats live for nights like this.",
        "He was the engine of the offense hitting where he hit.",
        "That slot in the lineup exists for guys who can do what he did tonight.",
        "He was productive in the spots that matter most.",
        "The big RBI opportunities came his way and he didn't miss them.",
    ],
    "bottom": [
        "Bonus production from the bottom of the order.",
        "He was hitting sixth or lower and still found a way to hurt them.",
        "That's the kind of night that makes a lineup deeper than it looks on paper.",
        "He wasn't expected to do that much damage from where he was hitting, but he did.",
        "Lower-order guys are not supposed to carry games, he didn't care.",
        "That's a nice surprise from the back end of the lineup.",
        "He came up in a low-leverage spot and delivered anyway.",
        "The bottom third of the order gave the club something extra tonight.",
        "He was flying under the radar in the lineup and still made his mark.",
        "Unexpected production from the bottom of the order.",
        "He hit in the back of the lineup and acted like he was in the middle.",
        "That's a real contribution from someone hitting down in the order.",
        "The lineup got longer and more dangerous because of what he did.",
        "Not supposed to be a run producer from that spot, did it anyway.",
        "He gave the club something extra from a spot where you don't always get it.",
    ],
}

TREND_FAMILIES = {
    "bounce_back": [
        "It looked like the kind of game that can pull a hitter out of a quiet stretch.",
        "After a few quieter games, this looked more like the hitter he can be.",
        "It had some bounce-back feel to it after a quieter run.",
        "This looked like a step back in the right direction after a slower patch.",
        "There was a little get-right energy to the performance after a modest skid.",
        "It felt like the sort of game that can settle a bat back down in a good way.",
        "After a modest lull, this looked like a healthier version of his profile.",
        "He looked more like himself again after a quieter run.",
        "It was the kind of line that can reset the mood around a hitter.",
        "The bat looked livelier again after a stretch without much noise.",
        "This was a helpful course correction after a less productive patch.",
        "The performance had some welcome rebound value to it.",
        "It felt like a game that could get him rolling again.",
        "After some quieter work lately, this was a better look.",
        "There was some needed life back in the profile here.",
    ],
    "hit_streak": [
        "He has now hit safely in {n} straight games.",
        "That pushes his hitting streak to {n} games.",
        "The steady contact has now produced a {n}-game hitting streak.",
        "Another game, another hit, and the streak is now {n} long.",
        "He continues to stack games with a hit, and the streak is now {n}.",
        "The bat has kept showing up, and the hitting streak is now at {n}.",
        "He keeps extending the run of games with a hit, now up to {n}.",
        "This line keeps a solid contact streak moving at {n} games.",
        "There has been some real consistency here, with hits now in {n} straight.",
        "He has turned the recent stretch into a {n}-game hitting streak.",
        "The streak keeps moving, now sitting at {n} games.",
        "He's making a habit of finding at least one hit, and the streak is now {n}.",
        "The contact floor has been steady, with hits now in {n} straight games.",
        "That keeps a useful little streak alive at {n} games.",
        "He has made the recent run count, with hits now in {n} straight.",
    ],
    "homer_streak": [
        "He has now homered in {n} straight games.",
        "The power has shown up in consecutive games now, with homers in {n} straight.",
        "He has taken the homer into another game, making it {n} in a row.",
        "The home-run swing has now traveled across {n} straight games.",
        "It is another homer, and that makes {n} straight games with one.",
        "The power run continues, with homers now in {n} straight.",
        "He keeps carrying the power from game to game, now {n} straight with a homer.",
        "The long ball has been a recent habit, with homers in {n} straight games.",
        "That keeps the current power streak alive at {n} games.",
        "He has kept the homer pace going from one game to the next.",
        "This is no longer a one-off homer; it has become a streak at {n} games.",
        "The power has become a trend, with homers now in {n} consecutive games.",
        "He continues to bring the long ball with him, now {n} straight.",
        "That makes another game with a homer, pushing the run to {n}.",
        "The recent power burst keeps rolling at {n} straight games with a homer.",
    ],
    "multi_hit_streak": [
        "This gives him back-to-back multi-hit efforts.",
        "He now has multi-hit games in {n} straight contests.",
        "The multi-hit production has now carried into {n} straight games.",
        "He has put together another multi-hit effort, making it {n} in a row.",
        "There is some consistency building here, with multi-hit efforts in {n} straight games.",
        "That is another multi-hit game, and the recent contact has been steady.",
        "He keeps stringing together multi-hit efforts during this stretch.",
        "The bat has been loud enough lately to produce multi-hit games on a regular basis.",
        "Another multi-hit line keeps the recent form pointing up.",
        "There is some real consistency in the hit column right now.",
        "The recent profile has leaned toward repeat multi-hit production.",
        "This is turning into a nice little run of multi-hit games.",
        "The current stretch now includes another multi-hit effort.",
        "He keeps giving the offense more than one knock a night lately.",
        "The recent contact quality has translated into repeated multi-hit games.",
    ],
    "steal_streak": [
        "He has now swiped a bag in {n} straight games.",
        "The running game has shown up in consecutive contests now.",
        "He has taken a steal into another game, making it {n} straight with one.",
        "The speed has traveled from game to game here, with steals in {n} straight.",
        "Another game, another steal, and the streak is now {n}.",
        "The legs keep adding value, with steals now in {n} straight games.",
        "There has been some real consistency in the running game lately.",
        "He keeps turning opportunities on the bases into steals.",
        "The current run now includes a steal in {n} straight games.",
        "He has made the speed part of the profile carry from game to game.",
        "The recent stretch has featured a lot of running, and it continues here.",
        "He keeps layering steals onto the box score during this stretch.",
        "The speed trend holds, with another game featuring a steal.",
        "The running game remains a regular part of his profile right now.",
        "He continues to add value with his legs on a game-to-game basis.",
    ],
}

def get_mid_game_exit(feed: dict, hitter: dict) -> dict:
    """Detect if a player left the game early via substitution.
    Returns {'exited': bool, 'inning': int, 'reason': str}"""
    hitter_id = hitter.get("id")
    side = hitter.get("side", "")
    result = {"exited": False, "inning": 0, "reason": ""}
    if not hitter_id:
        return result

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
    # Find the last inning the hitter actually batted
    last_bat_inning = 0
    for play in plays:
        batter = (play.get("matchup", {}) or {}).get("batter", {}) or {}
        if batter.get("id") == hitter_id:
            about = play.get("about", {}) or {}
            last_bat_inning = max(last_bat_inning, safe_int(about.get("inning", 0), 0))

    # Check substitution events for this player being removed
    for play in plays:
        for event in play.get("playEvents", []) or []:
            if not isinstance(event, dict):
                continue
            if event.get("type") != "action":
                continue
            details = event.get("details", {}) or {}
            event_type = str(details.get("eventType") or "").lower()
            if "substitution" not in event_type and "ejection" not in event_type:
                continue
            player = details.get("player", {}) or {}
            if player.get("id") != hitter_id:
                continue
            about = play.get("about", {}) or {}
            sub_inning = safe_int(about.get("inning", 0), 0)
            reason = "ejected" if "ejection" in event_type else "left the game"
            result = {"exited": True, "inning": sub_inning, "reason": reason}
            return result

    # Fallback: if last at-bat was inning 5 or earlier and game went 9, flag it
    linescore = feed.get("liveData", {}).get("linescore", {}) or {}
    total_innings = safe_int(linescore.get("currentInning", 0), 0)
    if last_bat_inning > 0 and total_innings >= 8 and last_bat_inning <= 5:
        result = {"exited": True, "inning": last_bat_inning, "reason": "left the game early"}

    return result


def get_opposing_starter(feed: dict, hitter_side: str) -> dict:
    """Return the actual starting pitcher by finding the first pitcher
    to face a batter in the game via play-by-play. Falls back to pitchers[0].
    Also returns games_started so ERA framing can be gated on sample size."""
    pitcher_side = "home" if hitter_side == "away" else "away"
    box_team = feed.get("liveData", {}).get("boxscore", {}).get("teams", {}).get(pitcher_side, {})
    players = box_team.get("players", {}) or {}

    # Find the actual starter from play-by-play (first pitcher to face a batter)
    starter_id = None
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
    for play in plays:
        matchup = play.get("matchup", {}) or {}
        pitcher = matchup.get("pitcher", {}) or {}
        pid = pitcher.get("id")
        if pid:
            starter_id = pid
            break

    # Fallback to pitchers array if play-by-play didn't yield a result
    if not starter_id:
        pitchers = box_team.get("pitchers", []) or []
        starter_id = pitchers[0] if pitchers else None

    if not starter_id:
        return {"name": "", "era": "", "games_started": 0}

    player_key = f"ID{starter_id}"
    player = players.get(player_key, {}) or {}
    season_stats = player.get("seasonStats", {}) or {}
    pitching = season_stats.get("pitching", season_stats) if isinstance(season_stats, dict) else {}

    # Get games started to gate ERA framing on sample size
    games_started = safe_int(pitching.get("gamesStarted", 0), 0)
    if games_started == 0:
        # Try gamesPlayed as fallback
        games_started = safe_int(pitching.get("gamesPlayed", 0), 0)

    return {
        "name": player.get("person", {}).get("fullName", ""),
        "era": str(pitching.get("era", "") or ""),
        "games_started": games_started,
    }


def get_game_time_of_day(feed: dict) -> str:
    """Return 'day' or 'night' based on game start time. Empty string if unknown."""
    game_data = feed.get("gameData", {}) or {}
    datetime_str = game_data.get("datetime", {}).get("dateTime", "") or ""
    if not datetime_str:
        return ""
    try:
        from datetime import timezone as _tz
        dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
        et_dt = dt.astimezone(ET)
        hour = et_dt.hour
        # Day game = before 5pm ET
        return "day" if hour < 17 else "night"
    except Exception:
        return ""


def get_milestone_notes(hitter: dict, stats: dict) -> list[str]:
    """Return a list of milestone strings if the player crossed a notable threshold today."""
    notes = []
    season = hitter.get("season_stats", {}) or {}

    season_hr = safe_int(season.get("homeRuns", 0), 0)
    season_rbi = safe_int(season.get("rbi", 0), 0)
    season_hits = safe_int(season.get("hits", 0), 0)
    season_sb = safe_int(season.get("stolenBases", 0), 0)

    today_hr = safe_int(stats.get("homeRuns", 0), 0)
    today_rbi = safe_int(stats.get("rbi", 0), 0)
    today_hits = safe_int(stats.get("hits", 0), 0)
    today_sb = safe_int(stats.get("stolenBases", 0), 0)

    # HR milestones (season_hr is AFTER today's game per boxscore)
    prev_hr = season_hr - today_hr
    for milestone in [5, 10, 15, 20, 25, 30, 40, 50]:
        if prev_hr < milestone <= season_hr:
            notes.append(f"homer No. {season_hr} on the season")
            break

    # First homer of the season
    if today_hr >= 1 and season_hr == today_hr:
        notes.append("first homer of the season")

    # RBI milestones
    prev_rbi = season_rbi - today_rbi
    for milestone in [25, 50, 75, 100]:
        if prev_rbi < milestone <= season_rbi:
            notes.append(f"{season_rbi} RBI on the year")
            break

    # Hit milestones
    prev_hits = season_hits - today_hits
    for milestone in [25, 50, 75, 100, 150, 200]:
        if prev_hits < milestone <= season_hits:
            notes.append(f"hit No. {season_hits} on the season")
            break

    # SB milestones
    prev_sb = season_sb - today_sb
    for milestone in [10, 20, 30, 40, 50]:
        if prev_sb < milestone <= season_sb:
            notes.append(f"stolen base No. {season_sb} on the year")
            break

    return notes


def get_batting_order_spot(feed: dict, hitter: dict) -> int:
    side = hitter.get("side")
    hitter_id = hitter.get("id")
    if not side or hitter_id is None:
        return 0

    players = (
        feed.get("liveData", {})
        .get("boxscore", {})
        .get("teams", {})
        .get(side, {})
        .get("players", {})
    )
    for player in players.values():
        if player.get("person", {}).get("id") == hitter_id:
            order = safe_int(player.get("battingOrder", 0), 0)
            return order // 100 if order else 0
    return 0


def _word_or_number(value: int) -> str:
    value = int(value)
    if 0 <= value <= 10:
        return _number_word(value)
    return str(value)


def _join_text(parts: list[str]) -> str:
    parts = [p for p in parts if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return ", ".join(parts[:-1]) + f", and {parts[-1]}"


def _last_name(full_name: str) -> str:
    """Return the display last name, stripping suffixes like Jr., Sr., III.
    Luis Robert Jr. -> Robert
    Pete Crow-Armstrong -> Crow-Armstrong
    Bobby Witt Jr. -> Witt
    Vladimir Guerrero Jr. -> Guerrero
    """
    parts = (full_name or "").strip().split()
    suffixes = {"jr.", "sr.", "ii", "iii", "iv", "v", "jr", "sr"}
    # Strip ALL trailing suffixes
    while parts and parts[-1].lower().rstrip(".").rstrip(",") in suffixes:
        parts = parts[:-1]
    if not parts:
        return full_name
    # Return last part (handles hyphenated names like Crow-Armstrong)
    return parts[-1]


def _stat_phrase(stats: dict) -> str:
    """Headline stat phrase for the opener. Leads with impact stats only.
    Walks and runs are shown in the game line field already.
    Uses PA context to frame efficiency (2-for-3 vs 2-for-5)."""
    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    hbp = safe_int(stats.get("hitByPitch", 0), 0)
    sac = safe_int(stats.get("sacFlies", 0), 0) + safe_int(stats.get("sacBunts", 0), 0)
    pa = ab + walks + hbp + sac
    rbi = safe_int(stats.get("rbi", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)

    extras: list[str] = []
    if homers:
        extras.append(_small_count_phrase(homers, "homer", include_article=(homers == 1)))
    elif doubles:
        extras.append(_small_count_phrase(doubles, "double", include_article=(doubles == 1)))
    elif triples:
        extras.append(_small_count_phrase(triples, "triple", include_article=(triples == 1)))
    if rbi:
        extras.append(f"{_word_or_number(rbi)} RBI")
    if steals:
        extras.append(_small_count_phrase(steals, "stolen base", include_article=(steals == 1)))

    # Show walks naturally in the stat line instead of raw PA count
    if walks >= 2:
        walk_str = f"{_word_or_number(walks)} walks" if walks > 1 else "a walk"
        extras.append(walk_str)
    base = f"{hits}-for-{ab}"
    return f"{base} with {_join_text(extras)}" if extras else base


def _position_phrase(position: str) -> str:
    pos = (position or "").upper()
    if pos == "C":
        return "from the catcher spot"
    if pos == "SS":
        return "from shortstop"
    if pos == "2B":
        return "from second base"
    if pos == "3B":
        return "from third base"
    return ""


def _event_phrase_from_stats(stats: dict) -> str:
    homers = safe_int(stats.get("homeRuns", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)

    if homers >= 2:
        return "two-homer game"
    if homers == 1 and rbi >= 3:
        return "homer"
    if triples >= 1:
        return "triple"
    if doubles >= 1 and rbi >= 2:
        return "run-scoring double"
    if doubles >= 1:
        return "double"
    if steals >= 2 and hits >= 2:
        return "multi-category game"
    if hits >= 4:
        return "four-hit game"
    if hits >= 3:
        return "three-hit game"
    if steals >= 2:
        return "speed-driven line"
    if rbi >= 3:
        return "run-producing game"
    return "line"


def _starter_context_sentence(pitcher: dict | None, stats: dict, context: dict) -> str:
    if not pitcher or not pitcher.get("name"):
        return ""
    name = pitcher["name"]
    era_raw = pitcher.get("era", "")
    games_started = safe_int(pitcher.get("games_started", 0), 0)
    try:
        era = float(era_raw)
    except Exception:
        era = None

    event_phrase = _event_phrase_from_stats(stats)

    # Only apply ERA-based framing if the pitcher has enough starts for the number to mean something
    MIN_STARTS_FOR_ERA_FRAMING = 3

    if era is not None and era <= 3.50 and games_started >= MIN_STARTS_FOR_ERA_FRAMING:
        return random.choice([
            f"Doing that against {name} (ERA: {era:.2f}) adds real credibility to the {event_phrase}.",
            f"The {event_phrase} came against {name}, one of the better arms in the league right now.",
            f"{name} is not an easy out, which makes the {event_phrase} that much more meaningful.",
            f"He got to {name}, who came in with a {era:.2f} ERA. That's a quality win.",
            f"The {event_phrase} off {name} is the kind of thing that gets noticed.",
            f"{name} has been one of the better starters out there, but he couldn't stop the {event_phrase}.",
            f"The matchup was a tough one with {name} on the mound, and he delivered anyway.",
            f"Going up against {name} and getting a {event_phrase} is not a small thing.",
            f"He produced the {event_phrase} against {name}, who doesn't give those up easily.",
            f"The quality of opponent mattered here: {name} came in with a {era:.2f} ERA.",
        ])
    if era is not None and era >= 5.00 and games_started >= MIN_STARTS_FOR_ERA_FRAMING:
        return random.choice([
            f"The matchup was a favorable one with {name} on the mound.",
            f"{name} has been one of the more hittable starters around, and it showed.",
            f"The {event_phrase} came against {name}, who has been giving up damage this year.",
            f"He took advantage of a soft matchup with {name} starting.",
            f"The spot set up well with {name} on the hill.",
            f"{name} has been hittable lately, and he made him pay.",
            f"The opportunity was there with {name} pitching, and he took it.",
            f"He got what he was looking for against {name}.",
        ])
    # Neutral — either ERA not meaningful, sample too small, or ERA missing
    return random.choice([
        f"He did the damage against {name}.",
        f"The {event_phrase} came against {name}.",
        f"{name} was on the mound, and he had a good night against him.",
        f"He got to {name} for the {event_phrase}.",
        f"He found his spots against {name}.",
        f"The production came with {name} pitching.",
        f"He handled {name} well tonight.",
        f"{name} couldn't slow him down.",
        f"He made {name} pay for any mistake.",
        f"The {event_phrase} came in a good at-bat against {name}.",
        f"He was locked in against {name} tonight.",
        f"{name} had no answer for him in the key spot.",
        f"He put together a strong game with {name} on the other side.",
        f"The matchup with {name} went in his favor tonight.",
        f"He took {name} deep and never looked back.",
    ])


def _lineup_context_sentence(lineup_spot: int, stats: dict) -> str:
    rbi = safe_int(stats.get("rbi", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)

    if lineup_spot == 1:
        # Only fire for leadoff if they did leadoff things
        if runs >= 2 or steals >= 1:
            return random.choice(LINEUP_FAMILIES["leadoff"])
        return ""
    if lineup_spot in _MIDDLE_ORDER_SPOTS:
        # Middle order only worth noting when RBI total is exceptional
        if rbi >= 4:
            return random.choice(LINEUP_FAMILIES["middle"])
        return ""
    if lineup_spot >= 7:
        # Bottom order production is always worth a mention
        return random.choice(LINEUP_FAMILIES["bottom"])
    return ""


def _close_game_context(team_score: int, opp_score: int, team_won: bool, context: dict, rbi: int) -> str:
    if team_score < 0 or opp_score < 0:
        return ""
    margin = abs(team_score - opp_score)
    if margin == 1:
        if team_won and (context.get("go_ahead_hit") or context.get("go_ahead_homer") or context.get("walkoff")):
            return random.choice([
                "In a one-run game, that swing was the difference.",
                "With the margin that tight, his biggest hit carried real weight.",
                "When the game ends up that close, one swing like that matters even more.",
                "The final score stayed tight enough that his biggest swing loomed over the whole game.",
                "That kind of contribution stands out more when the margin is only one run.",
                "The production played up because the game never gave anyone much breathing room.",
                "In a game that tight, one big swing can define the night, and his did.",
                "The one-run margin only made his biggest moment look larger.",
                "The closeness of the final score gave the performance even more importance.",
                "That contribution held extra weight once the game finished that tight.",
                "A one-run game leaves very little room for empty production, and this wasn't empty.",
                "The final margin kept the focus on his most important swing.",
                "That kind of line gets louder when the scoreboard never opens up.",
                "With so little separation on the scoreboard, his biggest hit mattered even more.",
                "The game stayed narrow enough that his contribution never stopped mattering.",
            ])
        if team_won and rbi >= 2:
            return random.choice([
                "It turned out to be a one-run game, so every bit of that production mattered.",
                "The final margin stayed tight enough that his line mattered all the way through.",
                "His run production carried a little more weight once the score settled in so close.",
                "It was the sort of tight game where even secondary damage can matter a lot.",
                "Because the game stayed close, his full line had real weight to it.",
                "A one-run final makes the whole line look more useful.",
                "The scoreboard stayed tight enough that every extra run felt important.",
                "There wasn't much room on the scoreboard, so his production kept its value all night.",
                "The closeness of the game made the entire line play up.",
                "In a tight finish, every bit of offense tends to matter more.",
                "His production held up because the margin never gave the game much cushion.",
                "Once the game stayed close, the smaller pieces of the box score mattered too.",
                "That line didn't have much room to hide in a one-run finish.",
                "The final score made the whole box-score line more meaningful.",
                "Tight games tend to magnify useful offense, and this was useful offense.",
            ])
        if not team_won and rbi >= 2:
            return random.choice([
                "Even though the club came up short, he still left a mark on a close game.",
                "The result went the wrong way, but his line still held up in a game that stayed tight.",
                "The loss doesn't erase the fact that he was part of a close contest all night.",
                "He still managed to matter in a game that never got away.",
                "The final result wasn't there, but the production still carried weight in a close game.",
                "There was still real value in the performance, even with the loss.",
                "The production held up better because the game stayed within reach all night.",
                "His work didn't disappear just because the club finished one run short.",
                "There was enough here to matter, even if the club couldn't finish it off.",
                "The close score helped keep his line relevant all the way to the end.",
                "He still made his mark in a game that remained in the balance.",
                "The tight finish kept the value of his line intact despite the loss.",
                "There wasn't much room for empty production in a game like that, and this wasn't empty.",
                "The scoreboard stayed tight enough that his contribution kept its meaning.",
                "Even in defeat, there was some real weight behind the production.",
            ])
    if margin >= 5 and team_won and (context.get("insurance_hit") or rbi >= 3):
        return random.choice([
            "Once the game opened up, he kept piling on.",
            "He also helped turn a competitive game into a more comfortable finish.",
            "He had a hand in stretching the scoreboard once the opening was there.",
            "The production also helped give the final score some extra breathing room.",
            "He contributed to turning a close game into a cleaner finish.",
            "His production helped create separation once the offense got rolling.",
            "He was part of the push that made the final margin look more comfortable.",
            "The production also played into the game getting away from the other side.",
            "He helped build the cushion once the chance appeared.",
            "The production mattered in the part of the game where the lead started to grow.",
            "He had a role in widening the gap late.",
            "His work added to the breathing room on the scoreboard.",
            "The production also helped the game tilt more firmly in his team's direction.",
            "He was part of the offense that turned some daylight into real separation.",
            "The final margin had his fingerprints on it too.",
        ])
    return ""



def _recent_trend_note(recent_games: list[dict], stats: dict) -> str:
    if not recent_games:
        return ""

    recent_slice = recent_games[:5]
    if len(recent_slice) < 3:
        return ""

    today_hits = safe_int(stats.get("hits", 0), 0)
    today_hr = safe_int(stats.get("homeRuns", 0), 0)
    today_sb = safe_int(stats.get("stolenBases", 0), 0)

    streak_hits = 0
    for game in recent_slice:
        if game.get("h", 0) > 0:
            streak_hits += 1
        else:
            break
    hit_streak = streak_hits + (1 if today_hits > 0 else 0)

    hr_streak = 0
    for game in recent_slice:
        if game.get("hr", 0) > 0:
            hr_streak += 1
        else:
            break
    homer_streak = hr_streak + (1 if today_hr > 0 else 0)

    sb_streak = 0
    for game in recent_slice:
        if game.get("sb", 0) > 0:
            sb_streak += 1
        else:
            break
    steal_streak = sb_streak + (1 if today_sb > 0 else 0)

    multi_hit_tail = 0
    for game in recent_slice:
        if game.get("h", 0) >= 2:
            multi_hit_tail += 1
        else:
            break
    multi_hit_streak = multi_hit_tail + (1 if today_hits >= 2 else 0)

    hitless_tail = 0
    for game in recent_slice:
        if game.get("h", 0) == 0:
            hitless_tail += 1
        else:
            break

    # Cold streak bounce-back (3+ hitless games before tonight)
    if hitless_tail >= 3 and today_hits >= 2:
        return random.choice(TREND_FAMILIES["bounce_back"])
    if hitless_tail >= 2 and today_hits >= 2 and today_hr >= 1:
        return random.choice([
            f"That snapped a quiet stretch for {_word_or_number(hitless_tail + 1)} games.",
            "He had been quiet lately, but not tonight.",
            "Good timing on the breakout after a couple of slow games.",
            f"He had been held hitless in {_word_or_number(hitless_tail)} straight before this.",
        ])

    if homer_streak >= 2:
        return random.choice(TREND_FAMILIES["homer_streak"]).format(n=homer_streak)
    if hit_streak >= 3:
        return random.choice(TREND_FAMILIES["hit_streak"]).format(n=hit_streak)
    if multi_hit_streak >= 2:
        return random.choice(TREND_FAMILIES["multi_hit_streak"]).format(n=multi_hit_streak)
    if steal_streak >= 2:
        return random.choice(TREND_FAMILIES["steal_streak"]).format(n=steal_streak)
    if today_hr == 1 and today_hits >= 2:
        return random.choice([
            "This was a nice little across-the-board line, with both the power and hit columns getting attention.",
            "The box score wasn't built on the homer alone, which makes it more appealing.",
            "There was enough substance around the homer to make the performance feel complete.",
            "It wasn't just about the homer, as the rest of the box score gave it better shape.",
            "The homer grabbed the eye, but there was more here than just that one swing.",
            "The box score held together nicely around the power.",
            "He didn't need the homer to carry the whole line by itself.",
            "The production around the homer helped keep the night from feeling one-note.",
            "There was some healthy support around the power in the rest of the box score.",
            "The box score had enough around the homer to feel balanced.",
            "It was a more complete game than just one swing leaving the yard.",
            "The extra hits around the homer gave the performance a stronger overall profile.",
            "There was enough supporting work to make the power feel even more valuable.",
            "The homer stood out, but the rest of the box score mattered too.",
            "The overall shape of the box score made the homer play up even more.",
        ])
    if today_sb >= 2:
        return random.choice([
            "The speed element gave the box score a lot of extra fantasy appeal.",
            "Multiple steals can change the shape of a fantasy line in a hurry.",
            "The legs added a lot of value on top of whatever he did with the bat.",
            "A couple of steals will always make a line look much more useful.",
            "The running game turned a good line into a better one.",
            "That much speed can do serious work for a fantasy line.",
            "The steals gave the whole performance another layer.",
            "There was a lot of added fantasy value once the steals showed up.",
            "The box score picked up real juice because of what he did on the bases.",
            "The speed played up the entire performance.",
            "Two steals can swing a category, and this line had that kind of impact.",
            "The legs made the whole night look more fantasy-friendly.",
            "The running game added some serious value to the final line.",
            "It was the kind of speed contribution that can move a category on its own.",
            "The steals made sure the value wasn't limited to the bat alone.",
        ])
    return ""


def build_hitter_subject(name: str, stats: dict, label: str, context: dict, recent_games: list[dict], position: str = "", lineup_spot: int = 0) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    pos = (position or "").upper()
    pos_phrase = _position_phrase(pos)

    streak_hits = 0
    recent_slice = recent_games[:5]
    for game in recent_slice:
        if game.get("h", 0) > 0:
            streak_hits += 1
        else:
            break
    hit_streak = streak_hits + (1 if hits > 0 else 0)

    if context.get("walkoff"):
        return random.choice(SUBJECT_OPENING_FAMILIES["walkoff"]).format(name=name)
    if context.get("go_ahead_homer") and rbi >= 2:
        return random.choice(SUBJECT_OPENING_FAMILIES["go_ahead_homer"]).format(name=name)
    if context.get("go_ahead_hit"):
        return random.choice(SUBJECT_OPENING_FAMILIES["go_ahead_hit"]).format(name=name)
    if context.get("game_tying_hit") and homers >= 1 and rbi >= 2:
        return random.choice(SUBJECT_OPENING_FAMILIES["game_tying"]).format(name=name)
    if hit_streak >= 10 and homers >= 1:
        return random.choice([
            f"{name} keeps a {hit_streak}-game streak rolling with another homer",
            f"{name} stays scorching hot and extends his streak to {hit_streak}",
            f"{name} keeps the streak alive and adds another homer",
            f"{name} extends a long hitting streak with more power",
            f"{name} brings both streak and power into another game",
            f"{name} stays on a tear and leaves the yard again",
            f"{name} keeps the run going with another loud game",
            f"{name} extends the heater and adds another homer",
            f"{name} keeps the bat hot and the streak moving",
            f"{name} keeps a long run rolling with more damage",
            f"{name} pushes the streak deeper with another homer",
            f"{name} stays blazing and leaves the yard again",
            f"{name} extends the run with another power game",
            f"{name} keeps a hot stretch alive with more thump",
            f"{name} turns a long streak into another loud night",
        ])
    if hit_streak >= 10:
        return random.choice([
            f"{name} extends his hitting streak to {hit_streak} games",
            f"{name} keeps the streak alive for the {hit_streak}th straight game",
            f"{name} pushes the hitting streak to {hit_streak}",
            f"{name} keeps a long run of hits moving",
            f"{name} adds another game to his hitting streak",
            f"{name} keeps the contact streak rolling",
            f"{name} brings the streak into another productive game",
            f"{name} keeps stacking games with a hit",
            f"{name} stretches the hitting run even further",
            f"{name} keeps the bat moving during a long streak",
            f"{name} adds another chapter to his hitting streak",
            f"{name} stays on time and keeps the streak alive",
            f"{name} keeps the streak intact with another hit",
            f"{name} keeps a long hot stretch going",
            f"{name} turns another game into another hit",
        ])
    if homers >= 2 and pos in _PREMIUM_POSITIONS:
        return random.choice([
            f"{name} brings rare power {pos_phrase or 'from a premium spot'} with a two-homer night",
            f"{name} turns in a two-homer game {pos_phrase or ''}".strip(),
            f"{name} flashes premium-position thump with two homers",
            f"{name} delivers a two-homer game {pos_phrase or ''}".strip(),
            f"{name} powers up {pos_phrase or 'from a premium position'} with two homers",
            f"{name} finds two homers {pos_phrase or 'from a premium spot'}".strip(),
            f"{name} shows off rare pop {pos_phrase or 'from a premium spot'}".strip(),
            f"{name} turns premium-position production into a two-homer night",
            f"{name} brings the thunder {pos_phrase or 'from a premium spot'}".strip(),
            f"{name} supplies a rare two-homer game {pos_phrase or ''}".strip(),
            f"{name} turns the box score loose {pos_phrase or ''} with two homers".strip(),
            f"{name} lights it up {pos_phrase or ''} with a pair of homers".strip(),
            f"{name} piles up rare power {pos_phrase or ''}".strip(),
            f"{name} makes a premium spot look loud with two homers",
            f"{name} delivers a big power night {pos_phrase or ''}".strip(),
        ])
    if homers >= 2:
        return random.choice(SUBJECT_OPENING_FAMILIES["two_homer"]).format(name=name)
    if hits >= 4:
        return random.choice(SUBJECT_OPENING_FAMILIES["four_hit"]).format(name=name)
    if hits >= 3 and (doubles + triples + homers) >= 2 and pos in _PREMIUM_POSITIONS:
        return random.choice([
            f"{name} does real damage {pos_phrase or ''} in a three-hit game".strip(),
            f"{name} stuffs the box score {pos_phrase or ''} with three hits and extra-base damage".strip(),
            f"{name} turns a premium spot into a three-hit, extra-base night",
            f"{name} piles up three hits and impact contact {pos_phrase or ''}".strip(),
            f"{name} delivers a strong multi-hit line {pos_phrase or ''}".strip(),
            f"{name} puts extra-base thump into a three-hit game {pos_phrase or ''}".strip(),
            f"{name} gives a premium position some real fantasy juice",
            f"{name} fills the box score {pos_phrase or ''} with real damage".strip(),
            f"{name} produces a big three-hit game {pos_phrase or ''}".strip(),
            f"{name} turns volume and punch into a premium-position line",
            f"{name} gets loud {pos_phrase or ''} with three hits and extra-base damage".strip(),
            f"{name} adds real impact {pos_phrase or ''} in a three-hit night".strip(),
            f"{name} gives his team a loaded box score {pos_phrase or ''}".strip(),
            f"{name} stacks up three hits and quality contact {pos_phrase or ''}".strip(),
            f"{name} turns a premium lineup card into a strong fantasy night",
        ])
    if hits >= 3 and (doubles + triples + homers) >= 2:
        return random.choice(SUBJECT_OPENING_FAMILIES["three_hit_xbh"]).format(name=name)
    if homers >= 1 and rbi >= 3 and lineup_spot in _MIDDLE_ORDER_SPOTS:
        return random.choice(SUBJECT_OPENING_FAMILIES["middle_order"]).format(name=name)
    if homers >= 1 and rbi >= 3:
        return random.choice([
            f"{name} goes deep and drives in {_word_or_number(rbi)}",
            f"{name} does most of his damage with one big swing",
            f"{name} turns one swing into a major RBI line",
            f"{name} delivers the thump in a strong run-producing game",
            f"{name} leaves his mark with a run-producing homer",
            f"{name} turns the power on and cashes in runs",
            f"{name} pairs the homer with a big RBI total",
            f"{name} drives the offense with one loud swing",
            f"{name} converts a homer into a big RBI night",
            f"{name} builds a strong fantasy line around one big swing",
            f"{name} does serious damage with the long ball",
            f"{name} brings the thunder and the RBI",
            f"{name} supplies both the homer and the run production",
            f"{name} doesn't waste the homer, piling on RBI too",
            f"{name} uses the long ball to drive a big line",
        ])
    if steals >= 2:
        return random.choice(SUBJECT_OPENING_FAMILIES["speed"]).format(name=name)
    if hit_streak >= 7:
        return random.choice(SUBJECT_OPENING_FAMILIES["hot_streak"]).format(name=name)
    if lineup_spot == 1 and (safe_int(stats.get("runs", 0), 0) >= 2 or steals >= 1):
        return random.choice([
            f"{name} sets the tone from the leadoff spot",
            f"{name} sparks the offense from the leadoff spot",
            f"{name} gets things moving from the top of the order",
            f"{name} gives the club early life from the leadoff spot",
            f"{name} turns the leadoff role into a productive night",
            f"{name} makes the top spot in the order count",
            f"{name} brings table-setting value from the leadoff role",
            f"{name} helps the offense hum from leadoff",
            f"{name} builds a useful line from the top of the order",
            f"{name} gives his squad some energy from the first spot",
            f"{name} makes things happen from leadoff",
            f"{name} keeps the offense moving from the top",
            f"{name} turns the leadoff job into fantasy value",
            f"{name} gives his side a strong table-setting line",
            f"{name} provides some top of the order spark",
        ])
    if hits >= 3:
        return random.choice([
            f"{name} turns in a three-hit game",
            f"{name} keeps the hit column moving all night",
            f"{name} strings together three knocks in a useful line",
            f"{name} puts together a three-hit performance",
            f"{name} delivers a steady three-hit game for the offense",
            f"{name} piles up three hits in a strong effort",
            f"{name} turns contact into a productive night",
            f"{name} sprays three hits around the yard",
            f"{name} builds a strong fantasy line with three hits",
            f"{name} turns a lot of contact into a useful game",
            f"{name} reaches three hits in a complete effort",
            f"{name} keeps finding holes in a three-hit game",
            f"{name} stacks up three hits and a lot of value",
            f"{name} does steady damage with three hits",
            f"{name} keeps adding to the box score with three hits",
        ])
    if homers == 1 and pos == "C":
        return random.choice([
            f"{name} provides pop from behind the plate",
            f"{name} leaves the yard from the catcher spot",
            f"{name} gives the catcher slot some needed thump",
            f"{name} supplies rare power from behind the dish",
            f"{name} turns catcher production into real value",
            f"{name} brings power to the catcher spot",
            f"{name} gives his team some catcher thump",
            f"{name} turns one swing into strong catcher value",
            f"{name} makes the catcher slot matter with a homer",
            f"{name} adds a loud swing from behind the plate",
            f"{name} brings rare offense to the catcher position",
            f"{name} finds power from the catcher spot",
            f"{name} gets the catcher slot into the fantasy picture",
            f"{name} delivers a catcher homer in a useful line",
            f"{name} makes his catcher eligibility feel more interesting",
        ])
    if homers == 1:
        return random.choice(SUBJECT_OPENING_FAMILIES["single_homer"]).format(name=name)
    return random.choice(SUBJECT_OPENING_FAMILIES["solid"]).format(name=name)


def _subject_emoji(stats: dict, label: str, context: dict, recent_games: list[dict]) -> str:
    """Return an emoji prefix for the subject line based on performance type."""
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)

    # Check for active hit streak
    streak = 0
    for g in (recent_games or [])[:7]:
        if g.get("h", 0) > 0:
            streak += 1
        else:
            break
    hit_streak = streak + (1 if hits > 0 else 0)

    if context.get("walkoff"):
        return "🎯 "
    if hit_streak >= 6:
        return "🔥 "
    if homers >= 2:
        return "💣 "
    if homers >= 1:
        return "⚡ "
    if steals >= 2:
        return "💨 "
    if hits >= 4:
        return "🔥 "
    return ""


def _tier_color(score: float, team: str) -> int:
    """Return embed color based on performance score tier."""
    if score >= 12.0:
        return 0xFFD700  # Gold — elite night
    if score >= 7.0:
        return 0x2ECC71  # Green — solid night
    return TEAM_COLORS.get(normalize_team_abbr(team), 0x2ECC71)





SUMMARY_OPENING_FAMILIES = {
    "walkoff": [
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, ending the game with the walk-off swing.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, and his final swing ended it.",
        "{name} turned in a {stat_phrase} performance in the {team_name} win over the {opponent_text}, then put the game away in the final at-bat.",
        "{name} delivered a {stat_phrase} showing in the {team_name} win over the {opponent_text}, with the last swing serving as the game-winner.",
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, then ended it himself in the final frame.",
        "{name} built a {stat_phrase} box score in the {team_name} win over the {opponent_text}, and the last swing was the one everyone remembered.",
        "{name} finished {stat_phrase} in the {team_name} win over the {opponent_text}, punctuating the night with the walk-off hit.",
        "{name} turned in a {stat_phrase} performance as the {team_name} beat the {opponent_text}, and he handled the final swing too.",
        "{name} put together a {stat_phrase} showing in the {team_name} win over the {opponent_text}, then delivered the game-ending moment.",
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, with the walk-off swing serving as the finishing touch.",
        "{name} finished {stat_phrase} in the {team_name} win over the {opponent_text}, and he made sure the final at-bat belonged to him.",
        "{name} built a {stat_phrase} line in the {team_name} win over the {opponent_text}, then closed the book with the walk-off swing.",
        "{name} turned in a {stat_phrase} line as the {team_name} beat the {opponent_text}, capping it with the final blow.",
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, and his last swing was the difference.",
        "{name} gave the {team_name} a {stat_phrase} performance in the win over the {opponent_text}, then ended it himself.",
    ],
    "go_ahead_homer": [
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, with his homer providing the swing that decided the game.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, and his homer changed the shape of the night.",
        "{name} turned in a {stat_phrase} performance in the {team_name} win over the {opponent_text}, with his biggest damage coming on the decisive swing.",
        "{name} built a {stat_phrase} box score in the {team_name} win over the {opponent_text}, and the homer proved to be the turning point.",
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, with the long ball standing up as the key swing.",
        "{name} finished {stat_phrase} in the {team_name} win over the {opponent_text}, and the homer ended up carrying the most weight.",
        "{name} turned in a {stat_phrase} performance as the {team_name} beat the {opponent_text}, with the homer serving as the difference-maker.",
        "{name} gave the {team_name} a {stat_phrase} showing in the win over the {opponent_text}, and his homer was the loudest moment.",
        "{name} posted a {stat_phrase} box score in the {team_name} win over the {opponent_text}, with the homer proving to be enough.",
        "{name} built a {stat_phrase} line in the {team_name} win over the {opponent_text}, and the game turned on his homer.",
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, and the long ball tilted the game for good.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, with the homer putting a permanent swing into the scoreboard.",
        "{name} came away with a {stat_phrase} line in the {team_name} win over the {opponent_text}, and his homer ended up being the one that held.",
        "{name} built a {stat_phrase} box score in the win over the {opponent_text}, with the homer carrying decisive weight.",
        "{name} turned in a {stat_phrase} performance in the {team_name} win over the {opponent_text}, and the homer was the lasting swing.",
    ],
    "go_ahead_hit": [
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, and he came through with the hit that ultimately decided it.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, with his biggest contribution arriving in a key late spot.",
        "{name} turned in a {stat_phrase} performance in the {team_name} win over the {opponent_text}, and his go-ahead hit proved to be the difference.",
        "{name} built a {stat_phrase} box score in the {team_name} win over the {opponent_text}, with the deciding hit standing out above the rest.",
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, and his key hit ended up holding all the way through.",
        "{name} finished {stat_phrase} in the {team_name} win over the {opponent_text}, with his most important swing coming in the biggest spot.",
        "{name} turned in a {stat_phrase} performance as the {team_name} beat the {opponent_text}, and the go-ahead knock gave the box score its shape.",
        "{name} gave the {team_name} a {stat_phrase} showing in the win over the {opponent_text}, and his biggest hit came when the game was still hanging there.",
        "{name} posted a {stat_phrase} box score in the {team_name} win over the {opponent_text}, and his timely hit ended up being the one that stood.",
        "{name} built a {stat_phrase} box score in the win over the {opponent_text}, with the most valuable piece coming on the go-ahead hit.",
        "{name} went {stat_phrase} in the {team_name} win over the {opponent_text}, and his key knock turned into the deciding moment.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, with the biggest hit coming exactly when it needed to.",
        "{name} turned in a {stat_phrase} performance in the win over the {opponent_text}, and his biggest swing came in the deciding spot.",
        "{name} came away with a {stat_phrase} showing in the {team_name} win over the {opponent_text}, and his key hit pushed the game in the right direction for good.",
        "{name} built a {stat_phrase} box score in the victory over the {opponent_text}, and the go-ahead hit gave the performance real weight.",
    ],
    "game_tying": [
        "{name} went {stat_phrase} against the {opponent_text}, helping the {team_name} stay in the game with a key equalizer.",
        "{name} finished {stat_phrase} against the {opponent_text}, and one of his biggest swings pulled the {team_name} back even.",
        "{name} turned in a {stat_phrase} performance against the {opponent_text}, and his biggest hit came when the {team_name} needed to claw back.",
        "{name} built a {stat_phrase} box score against the {opponent_text}, with the game-tying hit standing out the most.",
        "{name} went {stat_phrase} against the {opponent_text}, and his biggest swing brought the {team_name} back to level footing.",
        "{name} finished {stat_phrase} against the {opponent_text}, with the equalizer becoming the most important part of the box score.",
        "{name} turned in a {stat_phrase} performance against the {opponent_text}, and the tying hit changed the feel of the night.",
        "{name} gave the {team_name} a {stat_phrase} showing against the {opponent_text}, and his biggest swing erased the deficit.",
        "{name} posted a {stat_phrase} box score against the {opponent_text}, with the most important moment coming when he tied it.",
        "{name} built a {stat_phrase} box score against the {opponent_text}, and the equalizer put the game back on even terms.",
        "{name} went {stat_phrase} against the {opponent_text}, and his best swing arrived when the game needed to be reset.",
        "{name} finished {stat_phrase} against the {opponent_text}, with the tying moment giving the performance some real leverage.",
        "{name} turned in a {stat_phrase} performance against the {opponent_text}, and his most meaningful hit wiped out the gap.",
        "{name} came away with a {stat_phrase} showing against the {opponent_text}, and his key swing gave the {team_name} another breath.",
        "{name} built a {stat_phrase} box score against the {opponent_text}, with the tying hit giving the night a lot of its shape.",
    ],
    "general": OPENING_FAMILY_POOL,
}

SUMMARY_CONTEXT_FAMILIES = CONTEXT_FAMILY_POOL

def _build_position_power_sentence(pos_phrase: str, stats: dict, hitter: dict | None, recent_games: list[dict]) -> str:
    """Build a position-power sentence with a second clause grounding it in context."""
    rbi = safe_int(stats.get("rbi", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)

    # Get season HR total for milestone context
    season_hr = 0
    if hitter:
        season = hitter.get("season_stats", {}) or {}
        season_hr = safe_int(season.get("homeRuns", 0), 0)

    # Check recent homer streak
    hr_streak = 0
    for game in (recent_games or [])[:5]:
        if game.get("hr", 0) > 0:
            hr_streak += 1
        else:
            break
    homer_streak = hr_streak + (1 if homers > 0 else 0)

    # Build the second clause based on available context
    if rbi == 4:
        second_clause = random.choice([
            "and he hit a grand slam to do it.",
            "and he did it with the bases loaded.",
            "and it was a grand slam.",
        ])
    elif rbi == 3:
        second_clause = random.choice([
            "and he did it with a three-run shot.",
            "and it came with runners on base.",
            "and he drove in three on that swing.",
        ])
    elif rbi >= 2:
        second_clause = random.choice([
            "and it came with a runner on base.",
            "and he drove in two on that swing.",
            "and the two-run shot made it count.",
        ])
    elif rbi == 2:
        second_clause = random.choice([
            "and it came with a runner on base.",
            "and he drove in two on that swing.",
            "and the two-run shot made it count.",
        ])
    elif homer_streak >= 3:
        second_clause = random.choice([
            f"He's now homered in {homer_streak} straight games.",
            f"That's {homer_streak} games in a row with a homer.",
            "He's making a habit of it.",
        ])
    elif season_hr >= 2:
        second_clause = random.choice([
            f"He's got {season_hr} on the season now.",
            f"That's {season_hr} already this year.",
            "The power has been showing up early in the season.",
        ])
    else:
        second_clause = random.choice([
            "He's one of the few at that position who can do it.",
            "Teams don't find that very often.",
            "It's part of what makes him valuable.",
            "That's a rare combination.",
        ])

    # Build the first clause
    first_clause = random.choice([
        f"Power {pos_phrase} is not something you see every day",
        f"Not many guys {pos_phrase} hit the ball that hard",
        f"Homers {pos_phrase} don't grow on trees",
        f"That kind of pop {pos_phrase} is rare",
        f"You don't find that kind of thump {pos_phrase} very often",
        f"The homer carried extra weight because it came {pos_phrase}",
        f"Power {pos_phrase} is something teams pay a premium for",
        f"That's a legitimately impressive swing {pos_phrase}",
        f"The long ball {pos_phrase} is always going to stand out",
        f"He's one of the few guys who can do that {pos_phrase}",
        f"That kind of home run production {pos_phrase} is genuinely uncommon",
        f"Clubs covet that kind of power {pos_phrase}",
        f"Not a lot of lineups have someone who can do that {pos_phrase}",
        f"The position makes the homer even more interesting",
        f"That's a different kind of value when the power comes {pos_phrase}",
        f"The damage {pos_phrase} is what separates him from most at that spot",
        f"Finding that kind of pop {pos_phrase} is a real challenge for front offices",
        f"The bat speed {pos_phrase} is elite",
        f"That swing {pos_phrase} is the kind scouts remember",
        f"He's built differently for that spot in the field",
    ])

    # Connect them naturally — always lowercase the second clause after a comma
    sc = second_clause[0].lower() + second_clause[1:] if second_clause else second_clause
    # Use period only if second clause is a full independent sentence starting with a name/proper noun
    if second_clause.startswith("He's") or second_clause.startswith("Teams"):
        return f"{first_clause}. {second_clause}"
    return f"{first_clause}, {sc}"

FANTASY_FAMILIES = {
    "two_homer": FANTASY_CLOSING_POOL + [
        "Two homers in one game doesn't happen often.",
        "He went deep twice. Hard to ask for more than that.",
        "Multi-homer games are rare, and he made the most of it.",
        "Two home runs is a big night by anyone's standard.",
        "The power was on full display.",
        "He squared up everything they threw at him.",
        "Both homers were legitimate. This wasn't a cheap park job.",
        "That's the kind of game you circle on the calendar.",
        "Two-homer nights don't come around often, and he delivered one.",
        "He was locked in at the plate all night.",
    ],
    "impact_homer": [
        "One swing did a lot of the work.",
        "The homer was the difference-maker, but there was more around it.",
        "He didn't need much. One big swing and the damage was done.",
        "Power with runners on base is a dangerous combination.",
        "When he connects with men on, it gets ugly fast for the other team.",
        "He made them pay with one big cut.",
        "The homer carried most of the night, but the rest of the line filled in nicely.",
        "A homer with RBI behind it changes the whole complexion of a box score.",
        "He only needed one swing to put his stamp on the game.",
        "That's the kind of at-bat that wins games.",
        "The big blow came with runners on and he didn't miss it.",
        "One swing, a lot of damage.",
        "He picked his spot and delivered.",
        "The homer was the highlight but the full line held up well.",
        "That kind of production from one swing is hard to ignore.",
        "He was patient all night, and when he got his pitch, he didn't miss.",
        "There's a reason pitchers don't want to give him anything to hit.",
        "He looked comfortable at the plate all night, and the homer was the proof.",
        "He's one of those hitters who only needs one chance to change a game.",
        "The damage could have been worse for the opposing pitcher — he left some out there.",
    ],
    "four_hit": [
        "He was on base all night, pitchers just couldn't get him out.",
        "Four hits in one game is a legitimately great performance.",
        "He had a hit every time he came up. Outstanding night.",
        "The contact was there from the first at-bat to the last.",
        "He hit everything hard. Four-hit games don't happen by accident.",
        "That's a special night at the plate.",
        "He was locked in, four hits and he made every one count.",
        "Four hits against MLB pitching is a special kind of night.",
        "He found a way to get on base nearly every trip.",
        "Four hits is the kind of output that carries an offense.",
        "He was the best hitter on the field tonight.",
        "Couldn't miss, four hits and he was dangerous every at-bat.",
        "That's one of the cleaner hitting performances you'll see.",
        "He put the barrel on everything and it showed.",
        "Four-hit games are rare. This one was earned.",
    ],
    "three_hit_xbh": [
        "Not just a volume game either, he mixed in extra-base damage.",
        "Hits and extra-base damage together make this one of the cleaner fantasy lines of the day.",
        "The performance played up because it mixed hit volume with impact contact.",
        "There was more than just batting average help here, thanks to the extra-base work.",
        "Three hits with some thump behind them will usually play in fantasy leagues.",
        "The extra-base damage kept the box score from feeling one-dimensional.",
        "There was enough impact in the quality of contact to go with the hit total.",
        "A multi-hit line gets more interesting fast when the extra-base work shows up too.",
        "The box score had both quantity and punch, which is a good fantasy combination.",
        "That blend of contact and damage gave the performance a strong overall shape.",
        "The extra-base component made this more useful than a basic multi-hit game.",
        "The production carried a little more fantasy force because the hits were not all singles.",
        "That is a strong mix of hit volume and impact for fantasy purposes.",
        "There was enough damage behind the hits to make the performance stand out.",
        "The overall shape of the performance was stronger because the contact came with some authority.",
    ],
    "three_hit": [
        "Consistent night, he put something together every time he came up.",
        "Three hits without a homer is still a very good night.",
        "He kept finding holes all game.",
        "He was tough to retire, three hits and he made pitchers work.",
        "Just a clean, steady performance at the plate.",
        "The bat was on time all night.",
        "He sprayed it around and the hits kept coming.",
        "Didn't need the long ball to make an impact.",
        "The contact was there from start to finish.",
        "Three hits is three hits, a solid outing.",
        "He put the barrel on the ball all night.",
        "Simple but effective, kept the line moving.",
        "He was in rhythm and it showed.",
        "No flash, just production.",
        "He got his work in quietly and the numbers reflected it.",
        "Three-hit games don't happen by accident. He was locked in.",
        "He was seeing the ball well and it translated to hits.",
        "Every at-bat felt productive, and three hits confirmed it.",
        "He worked counts, stayed patient, and got rewarded with three hits.",
        "Pitchers couldn't find a way to get him out consistently.",
    ],
    "speed": [
        "He didn't just hit; he ran too, and that changed everything.",
        "The stolen bases were the difference-maker in this one.",
        "Two steals is a big night on the bases.",
        "His legs gave him extra value on top of what he did with the bat.",
        "The running game was on full display.",
        "The steals alone added a lot of fantasy force to the box score.",
        "There was a lot of added value once the speed showed up more than once.",
        "A multi-steal game tends to matter fast in fantasy leagues.",
        "The legs gave the whole line more category impact.",
        "That much speed can do serious work for a fantasy matchup.",
        "The steals turned a decent line into a much more useful one.",
        "There was extra fantasy life in the box score because of the running game.",
        "The box score picked up a lot of value once the steals stacked up.",
        "Multiple steals gave the box score a lot more fantasy appeal.",
        "The speed transformed the box score into something much more useful.",
    ],
    "walks": [
        "Didn't do much with the bat but made pitchers work all night.",
        "He reached base more than the hit total suggests, the walks tell the real story.",
        "Took what the pitchers gave him, and they gave him a lot of free passes.",
        "Didn't chase junk all night, the patience paid off.",
        "The walks piled up because he refused to chase.",
        "He made the pitcher earn every out and it didn't always happen.",
        "The plate discipline was the story here more than the hit count.",
        "He had pitchers rattled, they couldn't find the zone against him.",
        "A couple of walks can quietly be as valuable as hits.",
        "He worked deep counts all night and wore the pitcher down.",
        "The hit total was light but he was on base a lot.",
        "He saw a ton of pitches and took his walks.",
        "Patient approach tonight, lots of pitches, lots of walks.",
        "He let the game come to him and it worked.",
        "The at-bats were quality even when they didn't result in hits.",
    ],
    "general": [
        "There was enough here to matter across multiple categories.",
        "He did the work and the box score reflects it.",
        "The line held up from start to finish.",
        "He contributed in more ways than one tonight.",
        "Not a flashy line, but a useful one.",
        "The counting stats were there when it counted.",
        "He kept pressure on the pitching staff all night.",
        "A complete effort — he didn't waste many trips to the plate.",
        "He gave the offense something to build on.",
        "The production was real, not empty.",
        "He found a way to help even without a big swing.",
        "Every trip to the plate had a purpose.",
        "He made the most of what the pitcher gave him.",
        "The box score doesn't fully capture how steady he was.",
        "Hard to find a weak at-bat in there.",
        "He was locked in from the jump and it showed.",
        "The effort matched the results tonight.",
        "He was a reliable presence in the lineup all game.",
        "Quietly effective — the kind of night that wins games.",
        "He brought value in several different ways.",
    ],
}

QUALITY_100_FAMILIES = [
    "He also put {balls_100} balls in play at 100-plus mph.",
    "The quality of contact stood out as well, with {balls_100} batted balls at 100-plus mph.",
    "He backed up the production by producing {balls_100} balls at 100-plus mph.",
    "There was some real quality of contact here too, including {balls_100} batted balls over 100 mph.",
    "The contact quality was there in a big way, with {balls_100} balls struck at 100-plus mph.",
    "He paired the production with {balls_100} balls off the bat at triple-digit exit velocities.",
    "The performance came with some real authority too, as he produced {balls_100} 100-plus mph batted balls.",
    "Quality of contact was part of the story too, with {balls_100} balls leaving the bat at 100-plus mph.",
    "He didn't just produce; he hit the ball hard too, logging {balls_100} batted balls over 100 mph.",
    "The contact quality supported the box score, including {balls_100} triple-digit bolts.",
    "He gave the box score some extra support by hitting {balls_100} balls at 100-plus mph.",
    "The box score looked even better once the quality of contact was factored in, including {balls_100} triple-digit batted balls.",
    "He added some hard-contact backing with {balls_100} balls at 100-plus mph.",
    "The batted-ball quality kept pace with the box score, including {balls_100} balls over 100 mph.",
    "He paired the final line with {balls_100} examples of triple-digit contact.",
]

SUMMARY_FILLER_POOL = [
    "He contributed across multiple categories, which is the kind of line that holds up anywhere.",
    "The box score touched enough spots to matter in several formats.",
    "He did a little of everything, which can be hard to find in a single game.",
    "There was enough multi-category production here that no single stat had to carry it alone.",
    "He spread the value around the box score rather than stacking it in one place.",
    "His line did work in more than one category, which gives it real staying power.",
    "The performance had enough variety in it to matter in most formats.",
    "He gave his team something useful in several parts of the game.",
    "It wasn't a one-trick line, which tends to age well over a full matchup.",
    "He filled out the box score in a way that doesn't require any one number to stand alone.",
]

# Short punchy sentences to vary rhythm — sprinkled in occasionally
PUNCHY_SENTENCES = {
    "homer": [
        "One swing. That was the difference.",
        "He didn't miss it.",
        "Gone.",
        "That one had no doubt.",
        "He got all of it.",
    ],
    "multi_hit": [
        "Just locked in.",
        "Couldn't get him out.",
        "Pitchers had no answers.",
        "Every trip, a contribution.",
        "Consistent from first to last.",
    ],
    "speed": [
        "The legs were alive.",
        "He was a nightmare on the bases.",
        "Fast. Really fast.",
        "They had no shot.",
    ],
    "clutch": [
        "Delivered when it mattered.",
        "Came through.",
        "Big spot. Bigger hit.",
        "That's what he does.",
    ],
    "general": [
        "Good player. Good night.",
        "Everything working.",
        "Hard to stop.",
        "Start to finish.",
    ],
}


def _build_summary_opening(
    name: str,
    stats: dict,
    context: dict,
    opponent_text: str,
    team_name: str,
    team_won: bool,
    pitcher: dict | None = None,
    team_score: int = -1,
    opp_score: int = -1,
) -> str:
    stat_phrase = _stat_phrase(stats)
    result_verb = random.choice(["beat", "topped", "downed", "took down", "handled", "got past"]) if team_won else random.choice(["fell to", "lost to", "dropped one to", "couldn't get past"])

    # Score suffix removed, keep opener clean
    score_suffix = ""

    # Pitcher handled as body sentence, keep opener clean
    pitcher_suffix = ""
    pitcher_name = (pitcher or {}).get("name", "")

    if context.get("walkoff"):
        family = "walkoff"
    elif context.get("go_ahead_homer"):
        family = "go_ahead_homer"
    elif context.get("go_ahead_hit"):
        family = "go_ahead_hit"
    elif context.get("game_tying_hit"):
        family = "game_tying"
    else:
        family = "general"

    result_phrase = f"in the {team_name} win over the {opponent_text}" if team_won else f"in the {team_name} loss to the {opponent_text}"
    template = random.choice(SUMMARY_OPENING_FAMILIES[family])
    opening = template.format(
        name=name,
        stat_phrase=stat_phrase,
        opponent_text=opponent_text,
        team_name=team_name,
        result_phrase=result_phrase,
        result_verb=result_verb,
    )

    return opening


def _event_specific_ev_sentence(context: dict, hardest_ev: float | None) -> str:
    """Build an EV sentence with full in-game context: hit type, inning, RBI result."""
    if not hardest_ev:
        return ""
    homers = context.get("homers") or []
    xbh = context.get("extra_base_hits") or []

    if homers:
        first = homers[0]
        inning = safe_int(first.get("inning", 0), 0)
        rbi = safe_int(first.get("rbi", 0), 0)
        inning_piece = f" in the {_ordinal(inning)}" if inning else ""

        # Build a rich result phrase
        if rbi == 4:
            result_phrase = f"a grand slam{inning_piece}"
        elif rbi == 3:
            result_phrase = f"a three-run homer{inning_piece}"
        elif rbi == 2:
            result_phrase = f"a two-run homer{inning_piece}"
        elif rbi == 1:
            result_phrase = f"a solo shot{inning_piece}"
        else:
            result_phrase = f"a homer{inning_piece}"

        options = [
            f"His hardest-hit ball was {result_phrase}, which left the bat at {hardest_ev:.1f} mph.",
            f"The loudest contact of his night was {result_phrase} at {hardest_ev:.1f} mph.",
            f"He squared up {result_phrase} at {hardest_ev:.1f} mph, that one had some serious carry.",
            f"The {result_phrase} came off the bat at {hardest_ev:.1f} mph.",
            f"That {result_phrase} registered {hardest_ev:.1f} mph off the bat.",
            f"He put his best swing on {result_phrase}, {hardest_ev:.1f} mph exit velo.",
            f"The {result_phrase} was the hardest ball he hit all night at {hardest_ev:.1f} mph.",
            f"His top exit velocity came on {result_phrase}: {hardest_ev:.1f} mph.",
        ]
        return random.choice(options)

    if xbh:
        first = xbh[0]
        hit_type = "double" if first.get("type") == "double" else "triple"
        inning = safe_int(first.get("inning", 0), 0)
        rbi = safe_int(first.get("rbi", 0), 0)
        inning_piece = f" in the {_ordinal(inning)}" if inning else ""
        rbi_piece = f", driving in {_word_or_number(rbi)}" if rbi else ""

        options = [
            f"His hardest-hit ball was a {hit_type}{inning_piece}{rbi_piece}, leaving the bat at {hardest_ev:.1f} mph.",
            f"The loudest swing of his night was a {hit_type}{inning_piece} at {hardest_ev:.1f} mph{rbi_piece}.",
            f"He ripped a {hit_type}{inning_piece} at {hardest_ev:.1f} mph{rbi_piece}, his best contact of the game.",
            f"That {hit_type}{inning_piece} came off the bat at {hardest_ev:.1f} mph{rbi_piece}.",
            f"His top exit velo came on a {hit_type}{inning_piece}: {hardest_ev:.1f} mph{rbi_piece}.",
            f"The {hit_type}{inning_piece} was his hardest ball all night at {hardest_ev:.1f} mph{rbi_piece}.",
        ]
        return random.choice(options)

    # Fallback, no homer or XBH context, just report the number
    options = [
        f"He also barreled one at {hardest_ev:.1f} mph, his hardest contact of the night.",
        f"The contact quality was there too, with his hardest ball checking in at {hardest_ev:.1f} mph.",
        f"He hit one {hardest_ev:.1f} mph that showed off the raw power.",
        f"His loudest contact came at {hardest_ev:.1f} mph.",
        f"He had at least one at-bat where the bat really sang, {hardest_ev:.1f} mph off the barrel.",
        f"The exit velo peaked at {hardest_ev:.1f} mph, which gets anyone's attention.",
    ]
    return random.choice(options)


def _simplify_pitch_type(pitch_type: str, speed: float) -> str:
    """Convert MLB pitch type description to plain English."""
    pt = (pitch_type or "").lower()
    if not pt:
        return ""
    if "four-seam" in pt or "4-seam" in pt or "fastball" in pt:
        if speed >= 95:
            return f"a {speed:.0f} mph fastball"
        elif speed > 0:
            return f"a {speed:.0f} mph heater"
        return "a fastball"
    if "sinker" in pt or "two-seam" in pt or "2-seam" in pt:
        return f"a sinker" if speed == 0 else f"a {speed:.0f} mph sinker"
    if "cutter" in pt:
        return "a cutter"
    if "slider" in pt:
        return "a slider"
    if "curveball" in pt or "curve" in pt:
        return "a curveball"
    if "changeup" in pt or "change" in pt:
        return "a changeup"
    if "splitter" in pt or "split" in pt:
        return "a splitter"
    if "sweeper" in pt:
        return "a sweeper"
    if "knuckleball" in pt:
        return "a knuckleball"
    return ""


def _hit_location_phrase(location: str, trajectory: str, batter_side: str = "") -> str:
    """Return a descriptive phrase for hit location."""
    loc = (location or "").lower()
    traj = (trajectory or "").lower()

    traj_word = ""
    if "line" in traj:
        traj_word = "line drive"
    elif "fly" in traj:
        traj_word = "fly ball"
    elif "ground" in traj:
        traj_word = "ground ball"
    elif "popup" in traj or "pop" in traj:
        traj_word = "popup"

    if loc == "left":
        if traj_word:
            return f"a {traj_word} to left"
        return "to left field"
    if loc == "right":
        if traj_word:
            return f"a {traj_word} to right"
        return "to right field"
    if loc == "center":
        if traj_word:
            return f"a {traj_word} to center"
        return "to center field"

    return traj_word if traj_word else ""


def _event_text_from_context(context: dict) -> tuple[str, str]:
    homers = context.get("homers") or []
    xbh = context.get("extra_base_hits") or []

    if context.get("walkoff"):
        if homers:
            return "ended it with a walk-off homer", f"in the {_ordinal(safe_int(homers[0].get('inning', 0), 0))}" if safe_int(homers[0].get('inning', 0), 0) else ""
        return "delivered the walk-off hit", ""
    if context.get("go_ahead_homer") and homers:
        inning = safe_int(homers[0].get("inning", 0), 0)
        return "went deep to put his team ahead for good", f"in the {_ordinal(inning)}" if inning else ""
    if context.get("go_ahead_hit"):
        hit_text = "lined the go-ahead hit" if xbh else "delivered the go-ahead hit"
        inning = 0
        if homers:
            inning = safe_int(homers[0].get("inning", 0), 0)
        elif xbh:
            inning = safe_int(xbh[0].get("inning", 0), 0)
        return hit_text, f"in the {_ordinal(inning)}" if inning else ""
    if context.get("game_tying_hit"):
        inning = 0
        if homers:
            inning = safe_int(homers[0].get("inning", 0), 0)
        elif xbh:
            inning = safe_int(xbh[0].get("inning", 0), 0)
        return "delivered the game-tying hit", f"in the {_ordinal(inning)}" if inning else ""
    if homers:
        first = homers[0]
        inning = safe_int(first.get("inning", 0), 0)
        rbi = safe_int(first.get("rbi", 0), 0)
        pitch_type = first.get("pitch_type", "")
        pitch_speed = float(first.get("pitch_speed") or 0)
        trajectory = first.get("trajectory", "")
        location = first.get("location", "")

        pitch_phrase = _simplify_pitch_type(pitch_type, pitch_speed)
        loc_phrase = _hit_location_phrase(location, trajectory)

        # Build RBI description
        if rbi == 4:
            rbi_desc = random.choice([
                "hit a grand slam",
                "cleared the bases with a grand slam",
                "went deep with the bases loaded",
            ])
        elif rbi == 3:
            rbi_desc = random.choice([
                "hit a three-run homer",
                "went deep with a three-run shot",
                "launched a three-run blast",
            ])
        elif rbi == 2:
            rbi_desc = random.choice([
                "hit a two-run homer",
                "went deep with a two-run shot",
                "launched a two-run blast",
            ])
        elif rbi == 1:
            rbi_desc = random.choice([
                "hit a solo homer",
                "went deep on a solo shot",
                "took one deep on his own",
            ])
        else:
            rbi_desc = random.choice(["went deep", "left the yard", "hit a homer"])

        # Weave in pitch type ~60% of the time if available
        # Build a clean location string for attaching to rbi_desc
        # Strip "a fly ball/line drive" prefix since rbi_desc already names the hit
        clean_loc = ""
        if loc_phrase:
            if loc_phrase.startswith("a "):
                # "a fly ball to left" → "to left"
                parts = loc_phrase.split(" to ", 1)
                clean_loc = "to " + parts[1] if len(parts) == 2 else ""
            else:
                clean_loc = loc_phrase  # already plain like "to left field"

        if pitch_phrase and random.random() < 0.6:
            hr_text = f"{rbi_desc} off {pitch_phrase}"
        elif clean_loc and random.random() < 0.5:
            hr_text = f"{rbi_desc} {clean_loc}"
        else:
            hr_text = rbi_desc

        return hr_text, f"in the {_ordinal(inning)}" if inning else ""
    if xbh:
        hit_type = "double" if xbh[0].get("type") == "double" else "triple"
        inning = safe_int(xbh[0].get("inning", 0), 0)
        rbi = safe_int(xbh[0].get("rbi", 0), 0)
        rbi_piece = f", driving in {_word_or_number(rbi)}" if rbi else ""
        loc_xbh = ""
        xbh_location = xbh[0].get("location", "")
        xbh_trajectory = xbh[0].get("trajectory", "")
        if xbh_location == "left":
            loc_xbh = " to left"
        elif xbh_location == "right":
            loc_xbh = " to right"
        elif xbh_location == "center":
            loc_xbh = " to center"
        xbh_text = random.choice([
            f"ripped a {hit_type}{loc_xbh}{rbi_piece}",
            f"drove a {hit_type} into the gap{rbi_piece}",
            f"lined a {hit_type}{loc_xbh}{rbi_piece}",
            f"punched a {hit_type} the other way{rbi_piece}",
        ])
        return xbh_text, f"in the {_ordinal(inning)}" if inning else ""
    if context.get("first_run_hit"):
        return "got the club on the board first", ""
    if context.get("insurance_hit"):
        return "added a late insurance hit", ""
    if context.get("late_rbi_hit"):
        return "did damage in a key late spot", ""
    return "", ""


def _steal_context_sentence(context: dict) -> str:
    """Build a natural sentence about stolen base context."""
    steals = context.get("steals") or []
    if not steals:
        return ""

    if len(steals) >= 2:
        bases = [s.get("base", "second") for s in steals[:2]]
        innings = [safe_int(s.get("inning", 0), 0) for s in steals[:2]]
        if all(i > 0 for i in innings):
            return random.choice([
                f"He swiped {bases[0]} base in the {_ordinal(innings[0])} and {bases[1]} in the {_ordinal(innings[1])}.",
                f"The stolen bases came in the {_ordinal(innings[0])} and {_ordinal(innings[1])} innings.",
                f"He was a threat on the bases all night, stealing in the {_ordinal(innings[0])} and {_ordinal(innings[1])}.",
            ])
        return random.choice([
            "He was a threat on the bases all night.",
            "The stolen bases came at different points in the game.",
            "He kept the defense honest with multiple swipes.",
        ])

    steal = steals[0]
    base = steal.get("base", "second")
    inning = safe_int(steal.get("inning", 0), 0)

    if base == "home":
        return random.choice([
            f"He even stole home{f' in the {_ordinal(inning)}' if inning else ''} — that doesn't happen often.",
            f"The steal of home{f' in the {_ordinal(inning)}' if inning else ''} was the highlight on the bases.",
        ])
    if base == "third":
        inning_str = f" in the {_ordinal(inning)}" if inning else ""
        return random.choice([
            f"He took third{inning_str} and immediately became a scoring threat.",
            f"Stealing third{inning_str} put him in prime position to score.",
            f"He was aggressive on the bases, taking third{inning_str}.",
        ])
    # Second base
    inning_str = f" in the {_ordinal(inning)}" if inning else ""
    late = inning >= 7 if inning else False
    if late:
        return random.choice([
            f"The stolen base{inning_str} came at a key moment late in the game.",
            f"He stole second{inning_str} and put himself in scoring position at a critical time.",
        ])
    return random.choice([
        f"He took second{inning_str} and put himself in scoring position.",
        f"The stolen base{inning_str} showed what he can do on the bases.",
        f"He swiped second{inning_str} and kept the pressure on.",
    ])


async def generate_ai_hitter_summary(
    name: str,
    team: str,
    stats: dict,
    label: str,
    context: dict,
    opponent: str,
    team_won: bool,
    recent_games: list[dict],
    pitcher: dict | None = None,
    lineup_spot: int = 0,
    position: str = "",
    team_score: int = -1,
    opp_score: int = -1,
    hitter: dict | None = None,
    opponent_abbr: str = "",
    injury_note: str = "",
) -> str | None:
    """Call Claude API to generate a hitter summary. Returns None on failure."""
    if not ANTHROPIC_API_KEY:
        return None

    try:
        team_name = team_name_from_abbr(team)
        opponent_text = opponent or "the opposing club"

        hits = safe_int(stats.get("hits", 0), 0)
        ab = safe_int(stats.get("atBats", 0), 0)
        runs = safe_int(stats.get("runs", 0), 0)
        rbi = safe_int(stats.get("rbi", 0), 0)
        homers = safe_int(stats.get("homeRuns", 0), 0)
        doubles = safe_int(stats.get("doubles", 0), 0)
        triples = safe_int(stats.get("triples", 0), 0)
        walks = safe_int(stats.get("baseOnBalls", 0), 0)
        steals = safe_int(stats.get("stolenBases", 0), 0)
        strikeouts = safe_int(stats.get("strikeOuts", 0), 0)

        stat_parts = [f"{hits}-for-{ab}"]
        if homers:
            stat_parts.append(f"{homers} HR")
        if doubles:
            stat_parts.append(f"{doubles} 2B")
        if triples:
            stat_parts.append(f"{triples} 3B")
        if rbi:
            stat_parts.append(f"{rbi} RBI")
        if runs:
            stat_parts.append(f"{runs} R")
        if walks:
            stat_parts.append(f"{walks} BB")
        if steals:
            stat_parts.append(f"{steals} SB")
        if strikeouts:
            stat_parts.append(f"{strikeouts} K")
        stat_line = ", ".join(stat_parts)

        # Score margin
        if team_score >= 0 and opp_score >= 0:
            if team_won:
                margin = abs(team_score - opp_score)
                score_str = f"{team_name} won {team_score}-{opp_score} ({'blowout' if margin >= 6 else 'comfortable win' if margin >= 4 else 'close game'})"
            else:
                margin = abs(team_score - opp_score)
                score_str = f"{team_name} lost {team_score}-{opp_score} ({'lopsided loss' if margin >= 6 else 'close loss' if margin <= 2 else 'loss'})"
        else:
            score_str = f"{team_name} {'won' if team_won else 'lost'}"

        # Pitcher context
        pitcher_str = ""
        if pitcher and pitcher.get("name"):
            pitcher_str = f"Opposing starter: {pitcher['name']}"
            if pitcher.get("era"):
                pitcher_str += f" (ERA {pitcher['era']})"
            if pitcher.get("games_started"):
                pitcher_str += f", {pitcher['games_started']} GS"

        # Lineup spot
        if lineup_spot == 1:
            lineup_label = "leadoff"
        elif lineup_spot == 2:
            lineup_label = "second"
        elif lineup_spot in (3, 4, 5):
            lineup_label = "middle of the order"
        elif lineup_spot in (6, 7):
            lineup_label = "lower middle of the order"
        elif lineup_spot in (8, 9):
            lineup_label = "bottom of the order"
        else:
            lineup_label = ""
        lineup_str = f"Lineup spot: {lineup_spot} ({lineup_label})" if lineup_spot and lineup_label else ""

        # Position context
        pos_str = f"Position: {position}" if position else ""

        # Game context flags
        context_flags = []
        if context.get("walkoff"):
            context_flags.append("delivered the walk-off hit/homer")
        if context.get("go_ahead_homer"):
            context_flags.append("hit the go-ahead home run")
        elif context.get("go_ahead_hit"):
            context_flags.append("delivered the go-ahead hit")
        if context.get("game_tying_hit"):
            context_flags.append("delivered the game-tying hit")
        if context.get("insurance_hit"):
            context_flags.append("added an insurance hit")
        if context.get("first_run_hit"):
            context_flags.append("drove in the game's first run")
        homers_info = context.get("homers") or []
        for h in homers_info:
            inn = safe_int(h.get("inning", 0), 0)
            h_rbi = safe_int(h.get("rbi", 0), 0)
            pitch_type = h.get("pitch_type", "")
            pitch_speed = float(h.get("pitch_speed") or 0)
            pitch_str = _simplify_pitch_type(pitch_type, pitch_speed) if pitch_type else ""
            homer_desc = f"HR in inning {inn}" if inn else "HR"
            if h_rbi == 4:
                homer_desc += " (grand slam)"
            elif h_rbi >= 2:
                homer_desc += f" ({h_rbi}-run)"
            if pitch_str:
                homer_desc += f" off {pitch_str}"
            context_flags.append(homer_desc)
        xbh = context.get("extra_base_hits") or []
        for x in xbh:
            hit_type = x.get("type", "extra-base hit")
            inn = safe_int(x.get("inning", 0), 0)
            x_rbi = safe_int(x.get("rbi", 0), 0)
            xbh_desc = f"{hit_type} in inning {inn}" if inn else hit_type
            if x_rbi:
                xbh_desc += f" ({x_rbi} RBI)"
            context_flags.append(xbh_desc)
        steals_ctx = context.get("steals") or []
        for s in steals_ctx:
            base = s.get("base", "second")
            inn = safe_int(s.get("inning", 0), 0)
            steal_desc = f"stolen base ({base})"
            if inn:
                steal_desc += f" in inning {inn}"
            context_flags.append(steal_desc)
        hardest_ev = context.get("hardest_ev")
        if hardest_ev and hardest_ev >= 100:
            context_flags.append(f"hardest hit ball: {hardest_ev:.1f} mph exit velo")
        balls_100 = safe_int(context.get("balls_100", 0), 0)
        if balls_100 >= 2:
            context_flags.append(f"{balls_100} balls hit at 100+ mph")

        # Recent trend
        trend_str = ""
        if recent_games:
            recent_hits = [g.get("h", 0) for g in recent_games[:5]]
            hit_streak = 0
            for h_count in recent_hits:
                if h_count > 0:
                    hit_streak += 1
                else:
                    break
            if hit_streak >= 3:
                trend_str = f"Active hitting streak: {hit_streak + (1 if hits > 0 else 0)} games"
            hitless_streak = 0
            for h_count in recent_hits:
                if h_count == 0:
                    hitless_streak += 1
                else:
                    break
            if hitless_streak >= 2 and hits > 0:
                trend_str = f"Bounce-back game after {hitless_streak} hitless games"
            recent_hr = sum(g.get("hr", 0) for g in recent_games[:5])
            if recent_hr >= 3:
                trend_str = (trend_str + f"; on power surge, {recent_hr} HR in last 5 games") if trend_str else f"Power surge: {recent_hr} HR in last 5 games"

        # Opponent record
        opp_record_str = ""
        if opponent_abbr:
            wins, losses = get_team_record(opponent_abbr)
            if wins >= 0 and (wins + losses) >= 5:
                opp_record_str = f"Opponent ({opponent_text}) record: {wins}-{losses}"

        # Milestones
        milestone_str = ""
        if hitter:
            milestones = get_milestone_notes(hitter, stats)
            if milestones:
                milestone_str = f"Milestone: {milestones[0]}"

        # Injury note passthrough
        injury_str = injury_note if injury_note else ""

        # Build the full context block for Claude
        context_lines = [
            f"Player: {name} ({team_name})",
            f"Tonight's line: {stat_line}",
            f"Game result: {score_str}",
        ]
        if pitcher_str:
            context_lines.append(pitcher_str)
        if lineup_str:
            context_lines.append(lineup_str)
        if pos_str:
            context_lines.append(pos_str)
        if context_flags:
            context_lines.append("Key moments: " + "; ".join(context_flags))
        if trend_str:
            context_lines.append(f"Recent trend: {trend_str}")
        if opp_record_str:
            context_lines.append(opp_record_str)
        if milestone_str:
            context_lines.append(milestone_str)
        if injury_str:
            context_lines.append(f"Injury/IL note: {injury_str}")

        context_block = "\n".join(context_lines)

        system_prompt = (
            "You write post-game fantasy baseball hitter cards for a Discord server. "
            "Your voice is that of a sharp beat reporter who follows fantasy baseball closely — "
            "confident, natural, and concise. You prioritize fantasy value (HR, RBI, SB, AVG) "
            "but also capture the human side of the game.\n\n"
            "Rules:\n"
            "- Write exactly 3-4 sentences. No more.\n"
            "- Bold the player's full name on its first mention using **Name**.\n"
            "- Start with an engaging opener that is NOT just a dry recitation of the stat line. "
            "Open with the narrative (walkoff, go-ahead hit, hot streak, etc.) and weave in stats.\n"
            "- Use the player's last name after the first mention, not 'he' every time.\n"
            "- Vary sentence length. Mix a punchy short sentence with longer ones.\n"
            "- Be specific: use the actual game context (pitch type, inning, score margin, lineup spot) "
            "when relevant, but only if it adds something natural. Don't cram everything in.\n"
            "- Mention fantasy relevance at least once (category impact, multi-cat value, matchup context).\n"
            "- If a pitcher ERA/quality is provided, mention it briefly when it adds context to the performance.\n"
            "- Do not use em dashes (—). Use commas or periods instead.\n"
            "- Do not use the words: 'showcased', 'impressive', 'impressive performance', "
            "'notable', 'demonstrated', 'incredible', 'amazing', 'fantastic'.\n"
            "- Do not start sentences with 'Additionally' or 'Furthermore'.\n"
            "- Do not use the phrase 'on the mound tonight'.\n"
            "- Output only the summary text. No labels, no preamble, no quotation marks."
        )

        user_message = f"Write a fantasy baseball summary card for this hitter's performance tonight:\n\n{context_block}"

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 300,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    log(f"Anthropic API error {resp.status} for {name}")
                    return None
                data = await resp.json()
                text = data.get("content", [{}])[0].get("text", "").strip()
                if not text:
                    return None
                # Bold the player's name if the model forgot
                if name not in text and f"**{name}**" not in text:
                    text = text.replace(name.split()[-1], f"**{name}**", 1) if name.split()[-1] in text else text
                return text

    except Exception as exc:
        log(f"AI summary failed for {name}: {exc}")
        return None


def build_hitter_summary(
    name: str,
    team: str,
    stats: dict,
    label: str,
    context: dict,
    opponent: str,
    team_won: bool,
    recent_games: list[dict],
    pitcher: dict | None = None,
    lineup_spot: int = 0,
    position: str = "",
    team_score: int = -1,
    opp_score: int = -1,
    feed: dict | None = None,
    hitter: dict | None = None,
    opponent_abbr: str = "",
    injury_note: str = "",
) -> str:
    team_name = team_name_from_abbr(team)
    opponent_text = opponent or "the opposing club"
    last_name = _last_name(name)

    hits = safe_int(stats.get("hits", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    hardest_ev = context.get("hardest_ev")
    balls_100 = safe_int(context.get("balls_100", 0), 0)
    pos_phrase = _position_phrase(position)
    score = score_hitter(stats)

    # --- Quiet night: one-liner only ---
    if score < 3.5 and homers == 0 and steals == 0:
        opener = _build_summary_opening(name, stats, context, opponent_text, team_name, team_won, pitcher=pitcher, team_score=team_score, opp_score=opp_score)
        return opener

    sentences: list[str] = [
        _build_summary_opening(name, stats, context, opponent_text, team_name, team_won, pitcher=pitcher, team_score=team_score, opp_score=opp_score)
    ]

    used_signatures: set[str] = {"opening"}

    event_text, inning_text = _event_text_from_context(context)
    # inning_short strips "in the " for templates like "That 5th at-bat..."
    inning_short = inning_text.replace("in the ", "").strip() if inning_text else ""
    context_pool: list[str] = []
    if event_text:
        for template in SUMMARY_CONTEXT_FAMILIES:
            try:
                context_pool.append(template.format(
                    event_text=event_text,
                    inning_text=inning_text or "",
                    inning_short=inning_short or "",
                ))
            except KeyError:
                context_pool.append(template.format(event_text=event_text, inning_text=inning_text or ""))

    fantasy_key = "general"
    if homers >= 2:
        fantasy_key = "two_homer"
    elif homers == 1 and rbi >= 3:
        fantasy_key = "impact_homer"
    elif hits >= 4:
        fantasy_key = "four_hit"
    elif hits >= 3 and (doubles + triples) >= 1:
        fantasy_key = "three_hit_xbh"
    elif hits >= 3:
        fantasy_key = "three_hit"
    elif steals >= 2:
        fantasy_key = "speed"
    elif walks >= 2 and hits <= 1:
        fantasy_key = "walks"

    fantasy_pool = list(FANTASY_FAMILIES[fantasy_key])
    if pos_phrase and homers >= 1:
        pos_sentence = _build_position_power_sentence(pos_phrase, stats, hitter, recent_games)
        fantasy_pool = [pos_sentence] + fantasy_pool

    # --- Pitcher sentence (always included when available) ---
    pitcher_sentence = _starter_context_sentence(pitcher, stats, context)

    # --- Other meta sentences ---
    meta_pool: list[str] = []
    lineup_sentence = _lineup_context_sentence(lineup_spot, stats)
    if lineup_sentence:
        meta_pool.append(lineup_sentence)
    close_game_sentence = _close_game_context(team_score, opp_score, team_won, context, rbi)
    if close_game_sentence:
        meta_pool.append(close_game_sentence)

    # Stolen base context — always include for speed nights
    if steals >= 1:
        steal_sentence = _steal_context_sentence(context)
        if steal_sentence:
            meta_pool.insert(0, steal_sentence)

    # Opponent record sentence (gated to 7 days after opening day)
    if opponent_abbr:
        opp_record_phrase = _opponent_record_phrase(opponent_abbr)
        if opp_record_phrase:
            meta_pool.append(random.choice([
                f"The win came against {opp_record_phrase}.",
                f"That production came against {opp_record_phrase}.",
                f"Worth noting: the damage was done against {opp_record_phrase}.",
            ]))


    # --- Quality pool (EV / hard contact) ---
    quality_pool: list[str] = []
    if hardest_ev and hardest_ev >= 108:
        quality_pool.append(_event_specific_ev_sentence(context, hardest_ev))
    elif balls_100 >= 3:
        quality_pool.extend([s.format(balls_100=balls_100) for s in QUALITY_100_FAMILIES])

    trend_note = _recent_trend_note(recent_games, stats)
    if trend_note:
        quality_pool.append(trend_note)

    def _sig(sentence: str) -> str:
        lowered = sentence.lower()
        if "lead for good" in lowered or "ultimately decided" in lowered or "proved to be the difference" in lowered or "difference-maker" in lowered:
            return "decisive"
        if "walk-off" in lowered:
            return "walkoff"
        if "mph" in lowered or "exit velocity" in lowered or "100-plus" in lowered or "triple-digit" in lowered or "exit velo" in lowered:
            return "ev"
        if "leadoff" in lowered or "middle of the order" in lowered or "heart of the order" in lowered or "bottom third" in lowered or "lineup" in lowered:
            return "lineup"
        if "streak" in lowered or "quiet stretch" in lowered or "straight games" in lowered or "bounce-back" in lowered:
            return "trend"
        if "fantasy" in lowered or "category" in lowered or "matchup" in lowered or "season-long" in lowered or "daily leagues" in lowered:
            return "fantasy"
        if "against " in lowered or "on the mound" in lowered or "facing " in lowered or " off " in lowered:
            return "pitcher"
        if "inning" in lowered or "swing" in lowered or "gap" in lowered or "equalizer" in lowered:
            return "context"
        return lowered

    def _pick_unique(pool: list[str], fallback_sig: str | None = None) -> str:
        shuffled = list(pool)
        random.shuffle(shuffled)
        for sentence in shuffled:
            signature = _sig(sentence) if fallback_sig is None else fallback_sig
            if sentence and sentence not in sentences and signature not in used_signatures:
                used_signatures.add(signature)
                return sentence
        return ""

    # Build the ordered pool list, randomize where EV lands relative to fantasy/meta
    # so the hard-hit sentence doesn't always appear last
    ordered_pools: list[tuple[list[str], str | None]] = [
        (context_pool, "context"),
        (fantasy_pool, "fantasy"),
    ]

    # Insert quality (EV/trend) and meta (lineup/close game) in random order
    extra_pools = [
        (meta_pool, None),
        (quality_pool, None),
    ]
    random.shuffle(extra_pools)
    ordered_pools.extend(extra_pools)

    for pool, forced_sig in ordered_pools:
        picked = _pick_unique(pool, forced_sig)
        if picked:
            sentences.append(picked)

    # --- Inject pitcher sentence at a natural position (not always last) ---
    if pitcher_sentence and "pitcher" not in used_signatures:
        used_signatures.add("pitcher")
        # Insert after sentence 1 (opener) ~50% of the time, otherwise append
        if len(sentences) >= 2 and random.random() < 0.5:
            sentences.insert(2, pitcher_sentence)
        else:
            sentences.append(pitcher_sentence)

    # --- Injury return note (always append if present) ---
    if injury_note:
        sentences.append(injury_note)

    # --- Mid-game exit note ---
    if feed and hitter:
        exit_info = get_mid_game_exit(feed, hitter)
        if exit_info.get("exited"):
            inning = exit_info.get("inning", 0)
            reason = exit_info.get("reason", "left the game")
            if inning:
                sentences.append(f"{last_name} {reason} in the {_ordinal(inning)} inning — worth monitoring.")
            else:
                sentences.append(f"{last_name} {reason} — worth monitoring.")

    # --- Day game note (subtle) ---
    if feed:
        time_of_day = get_game_time_of_day(feed)
        if time_of_day == "day" and hits >= 3 and random.random() < 0.4:
            sentences.append(random.choice([
                f"Not bad for a day game.",
                f"He was locked in early, a day game didn't slow him down.",
                f"The day game didn't matter to {last_name}.",
            ]))

    # --- Milestone notes ---
    if hitter:
        milestones = get_milestone_notes(hitter, stats)
        for milestone in milestones[:1]:  # Max one milestone per card
            sentences.append(random.choice([
                f"{last_name} also hit his {milestone} tonight.",
                f"That was his {milestone}.",
                f"Worth noting: {last_name} hit his {milestone} with that swing.",
                f"He also knocked in his {milestone} on the night.",
            ]))

    # --- Next game opponent ---
    next_opp = get_next_opponent(team)
    if next_opp and len(sentences) < max_sentences:
        sentences.append(random.choice([
            f"He's got a date {next_opp}.",
            f"Up next for {last_name}: {next_opp}.",
            f"The {team_name} head {next_opp}.",
            f"Next up for him is {next_opp}.",
        ]))

    # --- Use last name to vary pronoun in later sentences ---
    # Swap "He " at the start of sentence 2 with last name ~40% of the time
    # Guard: only swap if last_name is a real surname (not a suffix like Jr.)
    _suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
    safe_last = last_name if last_name.lower().rstrip(".") not in _suffixes else ""
    if safe_last and len(sentences) >= 2 and random.random() < 0.4:
        s = sentences[1]
        if s.startswith("He "):
            sentences[1] = safe_last + " " + s[3:]
        elif s.startswith("His "):
            sentences[1] = f"{safe_last}'s " + s[4:]

    standout = homers >= 2 or rbi >= 4 or hits >= 4 or steals >= 2 or (homers >= 1 and rbi >= 3)
    max_sentences = 5 if standout else 4


        # --- Deduplicate key words across sentences ---
    KEY_SYNONYMS = {
        "homer": ["long ball", "blast", "shot", "home run"],
        "swing": ["cut", "at-bat", "knock", "hit"],
        "damage": ["production", "output", "contribution", "work"],
        "production": ["output", "contribution", "numbers", "line"],
        "performance": ["night", "outing", "game", "effort"],
    }
    if len(sentences) >= 2:
        # Find words used in opener
        opener_lower = sentences[0].lower()
        for word, synonyms in KEY_SYNONYMS.items():
            if word in opener_lower:
                # Replace word in sentences 2+ with a synonym
                for i in range(1, len(sentences)):
                    if word in sentences[i].lower():
                        replacement = random.choice(synonyms)
                        sentences[i] = sentences[i].replace(word, replacement, 1)
                        sentences[i] = sentences[i].replace(word.capitalize(), replacement.capitalize(), 1)
                        break

    # --- Add punchy sentence occasionally (~25% of standout nights) ---
    if standout and random.random() < 0.25 and len(sentences) < max_sentences:
        if context.get("walkoff") or context.get("go_ahead_homer"):
            punchy = random.choice(PUNCHY_SENTENCES["clutch"])
        elif homers >= 1:
            punchy = random.choice(PUNCHY_SENTENCES["homer"])
        elif steals >= 2:
            punchy = random.choice(PUNCHY_SENTENCES["speed"])
        elif hits >= 3:
            punchy = random.choice(PUNCHY_SENTENCES["multi_hit"])
        else:
            punchy = random.choice(PUNCHY_SENTENCES["general"])
        sentences.append(punchy)

    # --- Callback ending — reference opener theme (~30% of the time) ---
    if len(sentences) >= 3 and random.random() < 0.3 and len(sentences) < max_sentences:
        opener_lower = sentences[0].lower()
        callback = ""
        if "homer" in opener_lower or "deep" in opener_lower or "yard" in opener_lower:
            callback = random.choice([
                "One swing was all it took.",
                "That's the kind of power that changes games.",
                "When he gets one to hit, he doesn't miss.",
            ])
        elif "streak" in " ".join(s.lower() for s in sentences):
            callback = random.choice([
                "The hot stretch just keeps going.",
                "He's been doing this night after night.",
                "No signs of slowing down.",
            ])
        elif "steal" in opener_lower or "speed" in opener_lower or "swipe" in " ".join(s.lower() for s in sentences):
            callback = random.choice([
                "The legs make everything more dangerous.",
                "Speed like that changes how a team plays defense.",
            ])
        elif hits >= 3:
            callback = random.choice([
                "The bat just wouldn't stop.",
                "He made it look easy.",
            ])
        if callback and callback not in sentences:
            sentences.append(callback)

    # Only pad with a closing sentence if we need it,
    # and only ~40% of the time — don't force a generic closer onto every card
    if len(sentences) < 2:
        filler = _pick_unique(SUMMARY_FILLER_POOL, "filler")
        if filler:
            sentences.append(filler)
    elif len(sentences) == 2 and random.random() < 0.4:
        filler = _pick_unique(FANTASY_CLOSING_POOL, "closing")
        if filler:
            sentences.append(filler)

    final = " ".join(sentences[:max_sentences]).strip()
    # Bold the first occurrence of the player's full name in the summary
    if name in final:
        final = final.replace(name, f"**{name}**", 1)
    return final

# ---------------- BAD NIGHT CARDS ----------------

def is_bad_night(stats: dict) -> bool:
    """Return True if the player qualifies for a bad night card.
    Condition 1: 0-for-4 or worse WITH 2+ strikeouts
    Condition 2: 3+ strikeouts with no hits
    """
    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    # Condition 1: 0-for-4 or worse AND at least 2 strikeouts
    if hits == 0 and ab >= 4 and strikeouts >= 2:
        return True
    # Condition 2: 3+ strikeouts with no hits
    if strikeouts >= 3 and hits == 0:
        return True
    return False


def is_slump(recent_games: list[dict], stats: dict) -> tuple[bool, int]:
    """Return (is_slumping, hitless_streak_length).
    A slump is defined as 0-for in 3+ straight games including tonight."""
    if not recent_games:
        return False, 0
    today_hits = safe_int(stats.get("hits", 0), 0)
    if today_hits > 0:
        return False, 0  # Got a hit tonight, no slump

    hitless_streak = 0
    for g in recent_games:
        if g.get("h", 0) == 0:
            hitless_streak += 1
        else:
            break
    total_streak = hitless_streak + 1  # +1 for tonight
    return total_streak >= 3, total_streak


def should_post_slump_card(hitter_id: int, hitless_streak: int, state: dict) -> bool:
    """Return True if we should post a slump card for this player.
    Posts on streak start (day 3) then every 3 games after."""
    if hitless_streak < 3:
        return False
    slump_log = state.get("slump_log", {})
    last_posted = slump_log.get(str(hitter_id), 0)
    # Post on day 3, then every 3 games (6, 9, 12...)
    if last_posted == 0:
        return hitless_streak == 3
    return hitless_streak >= last_posted + 3


def classify_bad_night(stats: dict) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)

    if strikeouts >= 4:
        return "whiff_fest"
    if strikeouts >= 3 and hits == 0:
        return "rough_all_around"
    if strikeouts >= 3:
        return "strikeout_heavy"
    if hits == 0 and ab >= 5:
        return "hitless_deep"
    if hits == 0 and rbi == 0 and walks == 0:
        return "silent"
    return "hitless"


def build_bad_night_subject(name: str, stats: dict, label: str, opponent: str) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    last = _last_name(name)
    opp = opponent or "the opposition"

    if label == "whiff_fest":
        return random.choice([
            f"{name} struggles at the plate with {strikeouts} strikeouts",
            f"A tough night for {name}, who fanned {strikeouts} times",
            f"{name} runs into trouble, striking out {strikeouts} times",
            f"The strikeouts pile up for {name}",
        ])
    if label == "rough_all_around":
        return random.choice([
            f"{name} goes hitless with {strikeouts} strikeouts against the {opp}",
            f"A rough one for {name} — no hits and {strikeouts} punchouts",
            f"{name} has a night to forget against the {opp}",
        ])
    if label == "strikeout_heavy":
        return random.choice([
            f"{name} fans {strikeouts} times in a tough outing",
            f"Strikeouts are the story for {name} tonight",
            f"{name} struggles to make contact, striking out {strikeouts} times",
        ])
    if label == "hitless_deep":
        return random.choice([
            f"{name} goes 0-for-{ab} in a quiet night",
            f"No hits for {name} in {ab} at-bats",
            f"{name} goes hitless against the {opp}",
        ])
    return random.choice([
        f"{name} goes hitless against the {opp}",
        f"A quiet night for {name} at the plate",
        f"{name} is held without a hit by the {opp}",
        f"Nothing doing for {name} offensively tonight",
    ])


def build_bad_night_summary(
    name: str,
    team: str,
    stats: dict,
    label: str,
    opponent: str,
    team_won: bool,
    recent_games: list[dict],
    pitcher: dict | None = None,
) -> str:
    team_name = team_name_from_abbr(team)
    opponent_text = opponent or "the opposing club"
    last = _last_name(name)

    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)

    result = "the " + team_name + " win" if team_won else "a " + team_name + " loss"

    # Build opener with "with" connector and word numbers
    extras = []
    if strikeouts:
        k_word = _word_or_number(strikeouts)
        extras.append(f"{k_word} strikeout{'s' if strikeouts != 1 else ''}")
    if walks:
        extras.append("a walk" if walks == 1 else f"{_word_or_number(walks)} walks")

    base = f"{hits}-for-{ab}"
    stat_str = f"{base} with {_join_text(extras)}" if extras else base

    opener = random.choice([
        f"Tough night for **{name}**, who went {stat_str} in {result}.",
        f"**{name}** went {stat_str} in {result}.",
        f"Not much going offensively for **{name}**, going {stat_str} in {result}.",
        f"A quiet one for **{name}**: {stat_str} in {result}.",
    ])

    sentences = [opener]

    # Strikeout context
    if strikeouts >= 3:
        k_word = _word_or_number(strikeouts)
        sentences.append(random.choice([
            f"The strikeouts were the story, with {last} punching out {k_word} times.",
            f"He had trouble putting the ball in play, fanning {k_word} times.",
            f"Contact was hard to come by, with {k_word} punchouts on the night.",
            f"The bat was slow tonight, and {k_word} strikeouts tells the story.",
        ]))

    # Pitcher context — good pitcher makes a bad night more excusable
    if pitcher and pitcher.get("name"):
        pname = pitcher["name"]
        era_raw = pitcher.get("era", "")
        games_started = safe_int(pitcher.get("games_started", 0), 0)
        try:
            era = float(era_raw)
        except Exception:
            era = None
        if era is not None and era <= 3.50 and games_started >= 3:
            sentences.append(random.choice([
                f"To be fair, {pname} was on the mound and has been tough on everyone.",
                f"He was facing {pname}, who has been one of the harder starters to hit this year.",
                f"{pname} has been sharp, so this is not the first lineup he's quieted.",
                f"Not a lot of hitters have had success against {pname} lately.",
            ]))
        else:
            sentences.append(random.choice([
                f"He was facing {pname} and couldn't get anything going.",
                f"The matchup with {pname} didn't work in his favor.",
            ]))

    # Cold streak context
    if recent_games:
        recent_slice = recent_games[:5]
        hitless_streak = 0
        for g in recent_slice:
            if g.get("h", 0) == 0:
                hitless_streak += 1
            else:
                break
        total_hitless = hitless_streak + (1 if hits == 0 else 0)

        if total_hitless >= 3:
            n_word = _word_or_number(total_hitless)
            sentences.append(random.choice([
                f"This is now {n_word} straight games without a hit, a real cold stretch.",
                f"He's now gone {n_word} straight without a hit. Worth monitoring.",
                f"The cold stretch continues with {n_word} consecutive hitless games.",
                f"That makes {n_word} games in a row without a hit.",
            ]))
        elif total_hitless >= 2:
            sentences.append(random.choice([
                f"He's gone back-to-back games without a hit now.",
                f"Two straight hitless games for {last}.",
            ]))

    # Keep it short — 2-3 sentences max for bad cards
    return " ".join(sentences[:3]).strip()


def build_slump_subject(name: str, hitless_streak: int) -> str:
    last = _last_name(name)
    n_word = _word_or_number(hitless_streak)
    return random.choice([
        f"{name} extends hitless streak to {n_word} games",
        f"The cold stretch continues for {name}, now {n_word} straight without a hit",
        f"{name} still searching for a hit, now {n_word} games deep",
        f"No end in sight for {last} slump, {n_word} games without a hit",
        f"{name} goes hitless for the {_ordinal(hitless_streak)} straight game",
    ])


def build_slump_summary(
    name: str,
    team: str,
    stats: dict,
    hitless_streak: int,
    opponent: str,
    team_won: bool,
    recent_games: list[dict],
    pitcher: dict | None = None,
) -> str:
    team_name = team_name_from_abbr(team)
    last = _last_name(name)
    result = "the " + team_name + " win" if team_won else "a " + team_name + " loss"

    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)

    parts = [f"{hits}-for-{ab}"]
    if strikeouts:
        parts.append(f"{strikeouts} strikeout{'s' if strikeouts != 1 else ''}")
    if walks:
        parts.append("a walk" if walks == 1 else f"{_word_or_number(walks)} walks")
    stat_str = ", ".join(parts[:-1]) + (" and " + parts[-1] if len(parts) > 1 else parts[0])

    n_word = _word_or_number(hitless_streak)
    opener = random.choice([
        f"**{name}** went {stat_str} in {result}, extending his hitless streak to {n_word} games.",
        f"Still no hits for **{name}**, going {stat_str} in {result} and making it {n_word} straight.",
        f"**{name}** is now {n_word} games without a hit after going {stat_str} in {result}.",
        f"The drought continues for **{name}**: {stat_str} in {result}, now {n_word} straight hitless.",
    ])

    sentences = [opener]

    # Recent slump context
    recent_ab = sum(g.get("ab", 0) for g in recent_games[:hitless_streak - 1])
    if recent_ab > 0:
        total_ab = recent_ab + ab
        ab_word = _word_or_number(total_ab) if total_ab <= 10 else str(total_ab)
        g_word = _word_or_number(hitless_streak)
        sentences.append(random.choice([
            f"He's gone {ab_word} at-bats without a hit over this stretch.",
            f"Over the last {g_word} games he's been held hitless across {ab_word} at-bats.",
            f"That's {ab_word} consecutive at-bats without a knock.",
        ]))

    # Pitcher context — good arm softens the slump narrative
    if pitcher and pitcher.get("name"):
        pname = pitcher["name"]
        era_raw = pitcher.get("era", "")
        games_started = safe_int(pitcher.get("games_started", 0), 0)
        try:
            era = float(era_raw)
        except Exception:
            era = None
        if era is not None and era <= 3.50 and games_started >= 3:
            sentences.append(random.choice([
                f"Tonight he faced {pname}, who has been one of the harder arms to hit this year.",
                f"To be fair, {pname} is a tough out for anyone right now.",
            ]))

    sentences.append(random.choice([
        f"Something has to give soon — {last} is too good a hitter for this to last.",
        f"The talent is there. It's just a matter of when it turns around.",
        f"Slumps happen. The question is how long this one runs.",
        f"He'll be worth monitoring closely over the next few games.",
        f"A hitter of his caliber will find a way out of this.",
    ]))

    # Enforce minimum 2 sentences
    if len(sentences) < 2:
        sentences.append(random.choice([
            f"He'll be worth monitoring closely over the next few games.",
            f"A hitter of his caliber will find a way out of this.",
            f"Slumps happen. The question is how long this one runs.",
        ]))
    return " ".join(sentences[:4]).strip()


async def post_slump_card(
    channel: discord.abc.Messageable,
    hitter: dict,
    opponent: str,
    team_won: bool,
    feed: dict,
    game_date_et,
    hitless_streak: int,
) -> None:
    stats = hitter["stats"]
    recent_games = get_recent_hitter_games(hitter.get("id"), game_date_et)
    pitcher = get_opposing_starter(feed, hitter.get("side", "home"))

    embed = discord.Embed(
        color=0x2C2F33,  # Slightly darker than bad night cards
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, hitter["name"], hitter["team"])
    subject = build_slump_subject(hitter["name"], hitless_streak)
    embed.add_field(name="", value=f"**🥶 {subject}**", inline=False)
    embed.add_field(
        name="Summary",
        value=build_slump_summary(
            hitter["name"], hitter["team"], stats, hitless_streak,
            opponent, team_won, recent_games, pitcher=pitcher,
        ),
        inline=False,
    )
    embed.add_field(name="Game Line", value=format_hitter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_hitter_season_line(hitter.get("season_stats", {})), inline=False)
    await channel.send(embed=embed)


async def post_bad_card(
    channel: discord.abc.Messageable,
    hitter: dict,
    opponent: str,
    team_won: bool,
    feed: dict,
    game_date_et,
) -> None:
    stats = hitter["stats"]
    label = classify_bad_night(stats)
    recent_games = get_recent_hitter_games(hitter.get("id"), game_date_et)
    pitcher = get_opposing_starter(feed, hitter.get("side", "home"))

    embed = discord.Embed(
        color=0x36393F,  # Dark gray — visually distinct from good cards
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, hitter["name"], hitter["team"])
    subject = build_bad_night_subject(hitter["name"], stats, label, opponent)
    embed.add_field(name="", value=f"**📉 {subject}**", inline=False)
    embed.add_field(
        name="Summary",
        value=build_bad_night_summary(
            hitter["name"], hitter["team"], stats, label, opponent, team_won, recent_games, pitcher=pitcher,
        ),
        inline=False,
    )
    embed.add_field(name="Game Line", value=format_hitter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_hitter_season_line(hitter.get("season_stats", {})), inline=False)
    await channel.send(embed=embed)


# ---------------- EMBED POSTING ----------------

async def post_card(channel: discord.abc.Messageable, hitter: dict, opponent: str, team_won: bool, feed: dict, game_date_et, team_score: int = -1, opp_score: int = -1, opponent_abbr: str = "") -> None:
    stats = hitter["stats"]
    label = classify_hitter(stats)
    recent_games = get_recent_hitter_games(hitter.get("id"), game_date_et)
    game_context = build_hitter_game_context(feed, hitter)
    position = hitter.get("position", "")
    lineup_spot = get_batting_order_spot(feed, hitter)
    pitcher = get_opposing_starter(feed, hitter.get("side", "home"))
    injury_note = get_player_injury_status(hitter.get("id", 0), hitter.get("name", ""))
    score_value = score_hitter(stats)
    emoji = _subject_emoji(stats, label, game_context, recent_games)

    summary = await generate_ai_hitter_summary(
        name=hitter["name"],
        team=hitter["team"],
        stats=stats,
        label=label,
        context=game_context,
        opponent=opponent,
        team_won=team_won,
        recent_games=recent_games,
        pitcher=pitcher,
        lineup_spot=lineup_spot,
        position=position,
        team_score=team_score,
        opp_score=opp_score,
        hitter=hitter,
        opponent_abbr=opponent_abbr,
        injury_note=injury_note,
    )
    if not summary:
        summary = build_hitter_summary(
            hitter["name"], hitter["team"], stats, label, game_context, opponent, team_won, recent_games,
            pitcher=pitcher, lineup_spot=lineup_spot, position=position, team_score=team_score, opp_score=opp_score,
            feed=feed, hitter=hitter, opponent_abbr=opponent_abbr, injury_note=injury_note,
        )

    embed = discord.Embed(
        color=_tier_color(score_value, hitter["team"]),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, hitter["name"], hitter["team"])
    subject_text = build_hitter_subject(hitter["name"], stats, label, game_context, recent_games, position=position, lineup_spot=lineup_spot)
    embed.add_field(name="", value=f"**{emoji}{subject_text}**", inline=False)
    embed.add_field(name="Summary", value=summary, inline=False)
    embed.add_field(name="Game Line", value=format_hitter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_hitter_season_line(hitter.get("season_stats", {})), inline=False)
    await channel.send(embed=embed)



def format_hitter_game_line(stats: dict) -> str:
    ab = safe_int(stats.get("atBats", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)

    parts = [f"{hits}-{ab}", f"{runs} R", f"{rbi} RBI"]
    if homers:
        parts.append(f"{homers} HR")
    if doubles:
        parts.append(f"{doubles} 2B")
    if triples:
        parts.append(f"{triples} 3B")
    if walks:
        parts.append(f"{walks} BB")
    if steals:
        parts.append(f"{steals} SB")
    if strikeouts:
        parts.append(f"{strikeouts} K")
    return " • ".join(parts)


def format_hitter_season_line(season_stats: dict) -> str:
    season = season_stats or {}
    avg = season.get("avg") or season.get("battingAverage") or ".000"
    obp = season.get("obp") or season.get("onBasePercentage") or ".000"
    hr = safe_int(season.get("homeRuns", 0), 0)
    rbi = safe_int(season.get("rbi", 0), 0)
    runs = safe_int(season.get("runs", 0), 0)
    sb = safe_int(season.get("stolenBases", 0), 0)

    parts = [f"AVG {avg}", f"OBP {obp}"]
    if hr > 0:
        parts.append(f"{hr} HR")
    if rbi > 0:
        parts.append(f"{rbi} RBI")
    if runs > 0:
        parts.append(f"{runs} R")
    if sb > 0:
        parts.append(f"{sb} SB")
    return " • ".join(parts)

# ---------------- LOOP ----------------

async def hitter_loop() -> None:
    assert client is not None
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    load_player_headshot_index()

    state = load_state()
    posted = set(state.get("posted", []))
    if RESET_HITTER_STATE:
        log("RESET_HITTER_STATE enabled — posted state cleared for this run")
        posted = set()

    while True:
        sleep_seconds = get_random_awake_interval_seconds()
        try:
            scan_start = datetime.now(timezone.utc)
            games = get_games()
            log(f"Checking {len(games)} games")
            posts_this_scan = 0
            api_calls_this_scan = 0

            for game in games:
                if posts_this_scan >= MAX_POSTS_PER_SCAN:
                    break

                if game.get("status", {}).get("detailedState") != "Final":
                    continue

                game_id = game.get("gamePk")
                if not game_id:
                    continue

                try:
                    feed = get_feed(game_id)
                    api_calls_this_scan += 1
                except Exception as exc:
                    log(f"Feed fetch error for {game_id}: {exc}")
                    # Post to error channel if configured
                    if ERROR_CHANNEL_ID > 0:
                        try:
                            err_channel = await client.fetch_channel(ERROR_CHANNEL_ID)
                            away_t = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "Away")
                            home_t = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "Home")
                            err_embed = discord.Embed(
                                title="⚠️ Feed Fetch Failed",
                                description=f"Could not load game data for **{away_t} @ {home_t}** (Game ID: {game_id})\n`{exc}`",
                                color=0xFF4444,
                                timestamp=datetime.now(timezone.utc),
                            )
                            await err_channel.send(embed=err_embed)
                        except Exception:
                            pass
                    continue

                hitters = get_hitters(feed)
                if not hitters:
                    continue

                game_teams = feed.get("gameData", {}).get("teams", {})
                away_abbr = normalize_team_abbr(
                    game_teams.get("away", {}).get("abbreviation")
                    or game.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation")
                    or "AWAY"
                )
                home_abbr = normalize_team_abbr(
                    game_teams.get("home", {}).get("abbreviation")
                    or game.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation")
                    or "HOME"
                )
                away_score = safe_int(game.get("teams", {}).get("away", {}).get("score", 0), 0)
                home_score = safe_int(game.get("teams", {}).get("home", {}).get("score", 0), 0)
                matchup = f"{away_abbr} @ {home_abbr}"
                away_name = team_name_from_abbr(away_abbr)
                home_name = team_name_from_abbr(home_abbr)
                game_date_et = parse_game_date_et(game)

                ranked: list[tuple[float, dict]] = []
                for hitter in hitters:
                    score_value = score_hitter(hitter["stats"])
                    if score_value < MIN_HITTER_SCORE:
                        continue
                    ranked.append((score_value, hitter))

                ranked.sort(
                    key=lambda item: (
                        item[0],
                        hitter_total_bases(item[1]["stats"]),
                        safe_int(item[1]["stats"].get("rbi", 0), 0),
                        safe_int(item[1]["stats"].get("hits", 0), 0),
                    ),
                    reverse=True,
                )

                posted_this_game = 0
                for score_value, hitter in ranked:
                    if posts_this_scan >= MAX_POSTS_PER_SCAN:
                        break

                    hitter_id = hitter.get("id")
                    if hitter_id is None:
                        continue

                    post_key = f"{game_id}_{hitter_id}"
                    if post_key in posted:
                        continue
                    # Double-check against persisted state to prevent duplicates on restart
                    if post_key in set(load_state().get("posted", [])):
                        posted.add(post_key)  # Sync in-memory set
                        continue

                    player_team = normalize_team_abbr(hitter.get("team"))
                    opponent = home_name if player_team == away_abbr else away_name
                    team_won = (player_team == away_abbr and away_score > home_score) or (
                        player_team == home_abbr and home_score > away_score
                    )

                    opp_abbr = home_abbr if player_team == away_abbr else away_abbr
                    log(f"Posting {hitter['name']} | {hitter['team']} | {matchup} | score={score_value}")
                    await post_card(channel, hitter, opponent, team_won, feed, game_date_et, team_score=away_score if player_team == away_abbr else home_score, opp_score=home_score if player_team == away_abbr else away_score, opponent_abbr=opp_abbr)
                    posted.add(post_key)
                    posted_this_game += 1
                    posts_this_scan += 1


                    # Save state immediately after each post to prevent duplicates on crash
                    # Skip during testing (RESET_HITTER_STATE) to avoid disrupting test runs
                    if not RESET_HITTER_STATE:
                        state["posted"] = sorted(posted)
                        save_state(state)

                    if posts_this_scan < MAX_POSTS_PER_SCAN:
                        await asyncio.sleep(max(POST_DELAY_SECONDS, 0.0))


                # --- Bad night + slump cards ---
                bad_posted_this_game = 0
                for hitter in hitters:
                    if posts_this_scan >= MAX_POSTS_PER_SCAN:
                        break
                    if bad_posted_this_game >= MAX_BAD_CARDS_PER_GAME:
                        break

                    hitter_id = hitter.get("id")
                    if hitter_id is None:
                        continue

                    # Skip if already got a good card
                    post_key = f"{game_id}_{hitter_id}"
                    if post_key in posted:
                        continue

                    player_team = normalize_team_abbr(hitter.get("team"))
                    opponent = home_name if player_team == away_abbr else away_name
                    team_won = (player_team == away_abbr and away_score > home_score) or (
                        player_team == home_abbr and home_score > away_score
                    )

                    recent_games_h = get_recent_hitter_games(hitter_id, game_date_et)
                    slumping, hitless_streak = is_slump(recent_games_h, hitter["stats"])

                    # Check slump card first (takes priority over plain bad night)
                    if slumping and should_post_slump_card(hitter_id, hitless_streak, state):
                        slump_key = f"{game_id}_{hitter_id}_slump"
                        if slump_key not in posted and slump_key not in set(load_state().get("posted", [])):
                            log(f"Slump card: {hitter['name']} | {hitless_streak} games hitless")
                            await post_slump_card(channel, hitter, opponent, team_won, feed, game_date_et, hitless_streak)
                            posted.add(slump_key)
                            state.setdefault("slump_log", {})[str(hitter_id)] = hitless_streak
                            bad_posted_this_game += 1
                            posts_this_scan += 1
                            if not RESET_HITTER_STATE:
                                state["posted"] = sorted(posted)
                                save_state(state)
                            if posts_this_scan < MAX_POSTS_PER_SCAN:
                                await asyncio.sleep(max(POST_DELAY_SECONDS, 0.0))
                            continue

                    # Regular bad night card
                    bad_key = f"{game_id}_{hitter_id}_bad"
                    if bad_key in posted:
                        continue
                    if bad_key in set(load_state().get("posted", [])):
                        posted.add(bad_key)
                        continue

                    if not is_bad_night(hitter["stats"]):
                        continue

                    log(f"Bad night: {hitter['name']} | {hitter['team']} | {matchup}")
                    await post_bad_card(channel, hitter, opponent, team_won, feed, game_date_et)
                    posted.add(bad_key)
                    bad_posted_this_game += 1
                    posts_this_scan += 1

                    if not RESET_HITTER_STATE:
                        state["posted"] = sorted(posted)
                        save_state(state)

                    if posts_this_scan < MAX_POSTS_PER_SCAN:
                        await asyncio.sleep(max(POST_DELAY_SECONDS, 0.0))

            state["posted"] = sorted(posted)
            save_state(state)
            scan_elapsed = (datetime.now(timezone.utc) - scan_start).total_seconds()
            log(f"Scan complete: {posts_this_scan} posts, {api_calls_this_scan} API calls, {scan_elapsed:.1f}s elapsed")
        except Exception as exc:
            log(f"Loop error: {exc}")

        log(f"Sleeping {sleep_seconds} seconds before next scan")
        await asyncio.sleep(sleep_seconds)


# ---------------- DISCORD LIFECYCLE ----------------

async def on_ready() -> None:
    global background_task
    assert client is not None
    log(f"Logged in as {client.user}")
    if background_task is None or background_task.done():
        background_task = asyncio.create_task(hitter_loop())
        log("Hitter background task created")



async def start_hitter_bot() -> None:
    global client, background_task
    if not TOKEN:
        raise RuntimeError("ANALYTIC_BOT_TOKEN is not set")
    if CHANNEL_ID <= 0:
        raise RuntimeError("HITTER_WATCH_CHANNEL_ID is not set")

    background_task = None
    client = discord.Client(intents=intents)
    client.event(on_ready)

    # Let main.py own the restart loop. reconnect=False avoids the discord.py
    # resume path that has been crashing with self.ws=None after connect timeouts.
    await client.start(TOKEN, reconnect=False)

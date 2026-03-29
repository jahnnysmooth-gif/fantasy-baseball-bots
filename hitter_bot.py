import asyncio
import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
import requests

# ---------------- CONFIG ----------------

TOKEN = os.getenv("ANALYTIC_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("HITTER_WATCH_CHANNEL_ID", "0"))

STATE_DIR = Path("state/hitter")
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state.json"

ET = ZoneInfo("America/New_York")
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = int(os.getenv("HITTER_POLL_MINUTES", "10"))
RESET_HITTER_STATE = os.getenv("RESET_HITTER_STATE", "").lower() in {"1", "true", "yes"}
MIN_HITTER_SCORE = float(os.getenv("HITTER_MIN_SCORE", "5.0"))
MAX_CARDS_PER_GAME = int(os.getenv("HITTER_MAX_CARDS_PER_GAME", "10"))
REQUEST_TIMEOUT = float(os.getenv("HITTER_REQUEST_TIMEOUT", "30"))
MAX_POSTS_PER_SCAN = int(os.getenv("HITTER_MAX_POSTS_PER_SCAN", "20"))
MAX_POSTS_PER_GAME_PER_SCAN = int(os.getenv("HITTER_MAX_POSTS_PER_GAME_PER_SCAN", "18"))
POST_DELAY_SECONDS = float(os.getenv("HITTER_POST_DELAY_SECONDS", "1.25"))
AWAKE_SCAN_MIN_MINUTES = int(os.getenv("HITTER_AWAKE_SCAN_MIN_MINUTES", "10"))
AWAKE_SCAN_MAX_MINUTES = int(os.getenv("HITTER_AWAKE_SCAN_MAX_MINUTES", "10"))
SLEEP_START_HOUR_ET = int(os.getenv("HITTER_SLEEP_START_HOUR_ET", "3"))
SLEEP_END_HOUR_ET = int(os.getenv("HITTER_SLEEP_END_HOUR_ET", "13"))
ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")

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
    return 10 * 60




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
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)
    games: list[dict] = []

    for target_date in [today, yesterday]:
        try:
            response = requests.get(f"{SCHEDULE_URL}&date={target_date.isoformat()}", timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            for date_block in payload.get("dates", []):
                games.extend(date_block.get("games", []))
        except Exception as exc:
            log(f"Schedule fetch error for {target_date}: {exc}")
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
        if rbi == 2:
            options.extend(["launched a two-run shot", "connected for a two-run homer"])
        elif rbi >= 3:
            options.extend(["launched a three-run homer", "cleared the bases with a homer"])
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

        if "home run" in event or event_type == "home_run":
            context["homers"].append({"inning": inning, "rbi": rbi})
        if event_type in {"double", "triple"}:
            context["extra_base_hits"].append({"type": event_type, "inning": inning, "rbi": rbi})

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
    return "solid_night"



# ---------------- CARD TEXT ----------------

_PREMIUM_POSITIONS = {"C", "SS", "2B", "3B"}
_MIDDLE_ORDER_SPOTS = {3, 4, 5}

SUBJECT_OPENING_FAMILIES = {
    "walkoff": [
        "{name} delivers the walk-off for his club",
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
        "{name} puts his club ahead for good with a big homer",
        "{name} changes the game with a go-ahead blast",
        "{name} breaks it open with the decisive homer",
        "{name} flips the game with one swing",
        "{name} delivers the swing that held up",
        "{name} swings the game with a late homer",
        "{name} provides the game-changing homer",
        "{name} gives his club the lead for keeps with a blast",
        "{name} leaves the yard for the decisive swing",
        "{name} turns the game with a timely homer",
        "{name} provides the go-ahead power",
        "{name} hits the homer that proved to be enough",
        "{name} delivers the biggest swing of the night",
        "{name} changes the scoreboard with one loud swing",
    ],
    "go_ahead_hit": [
        "{name} comes through with the hit that changed the game",
        "{name} delivers the hit that put his club ahead for good",
        "{name} provides the decisive hit",
        "{name} comes up with the swing that held up",
        "{name} turns the game with one key knock",
        "{name} lines the hit that proved to be enough",
        "{name} changes the night with a timely hit",
        "{name} provides the late swing that mattered most",
        "{name} comes through when the game was hanging there",
        "{name} puts his club ahead with the deciding hit",
        "{name} delivers in the biggest spot",
        "{name} breaks it open with the key hit",
        "{name} pushes his club in front with a timely knock",
        "{name} provides the swing that became the difference",
        "{name} cashes in with the decisive knock",
    ],
    "game_tying": [
        "{name} pulls his club even with one swing",
        "{name} brings his club back level with a big hit",
        "{name} ties the game with a timely blow",
        "{name} helps erase the deficit with his biggest swing",
        "{name} comes through with the equalizer",
        "{name} drags his club back even",
        "{name} supplies the game-tying damage",
        "{name} wipes out the deficit with a clutch hit",
        "{name} keeps the game alive with a timely swing",
        "{name} delivers the hit that reset the game",
        "{name} changes the tone with the tying blow",
        "{name} gives his club new life with the equalizer",
        "{name} produces the game-tying moment",
        "{name} helps turn the game back into a coin flip",
        "{name} pulls his club back into it",
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
        "{name} fuels his club with two long balls",
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
        "{name} strings together three hits and extra-base damage",
        "{name} does real damage in a three-hit game",
        "{name} stuffs the box score with three hits and loud contact",
        "{name} mixes a three-hit night with extra-base thump",
        "{name} builds a strong line with three hits and impact contact",
        "{name} piles up three hits while doing extra-base damage",
        "{name} posts a three-hit game with some thunder behind it",
        "{name} turns three hits into a big night",
        "{name} delivers a three-hit line with some authority",
        "{name} comes away with three hits and more than just singles",
        "{name} adds quality contact to a three-hit line",
        "{name} turns in a three-hit effort with extra-base punch",
        "{name} turns volume and damage into a standout game",
        "{name} reaches three hits and keeps the ball jumping",
        "{name} combines a three-hit game with real impact",
    ],
    "middle_order": [
        "{name} drives the offense from a middle-of-the-order role",
        "{name} cashes in from the heart of the order",
        "{name} makes the most of his run-producing spot",
        "{name} delivers the kind of production you want from the middle of the order",
        "{name} comes through in a run-producing role",
        "{name} turns chances into damage from the heart of the order",
        "{name} gives his club what it needed from a premium RBI role",
        "{name} provides middle-order thump",
        "{name} drives in damage from a key run-producing role",
        "{name} cashes in the traffic around him",
        "{name} anchors the offense with a productive game",
        "{name} turns his place in the heart of the order into real fantasy value",
        "{name} makes his heart-of-the-order at-bats count",
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
        "{name} gives his club a loud moment with a homer",
        "{name} produces a homer as part of a quality night",
        "{name} changes the box score with one long ball",
        "{name} turns the power on in a strong effort",
        "{name} makes the box score pop with a homer",
        "{name} brings the thunder in a productive performance",
        "{name} does not need many swings to make an impact",
    ],
    "solid": [
        "{name} puts together a useful night at the plate",
        "{name} chips in with a steady offensive game",
        "{name} turns in a productive night at the plate",
        "{name} gives his club a solid offensive game",
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
    "{name} went {stat_phrase} {result_phrase}.",
    "{name} finished {stat_phrase} {result_phrase}.",
    "{name} turned in a {stat_phrase} performance {result_phrase}.",
    "{name} came away with a {stat_phrase} box score {result_phrase}.",
    "{name} helped drive the offense with a {stat_phrase} showing {result_phrase}.",
    "{name} put together a {stat_phrase} performance {result_phrase}.",
    "{name} gave his club a {stat_phrase} showing {result_phrase}.",
    "{name} turned his night into a {stat_phrase} box score {result_phrase}.",
    "{name} checked in with a {stat_phrase} performance {result_phrase}.",
    "{name} was one of the more productive bats on the field, going {stat_phrase} {result_phrase}.",
    "{name} built a {stat_phrase} box score {result_phrase}.",
    "{name} posted a {stat_phrase} performance {result_phrase}.",
    "{name} added a {stat_phrase} showing {result_phrase}.",
    "{name} made his mark with a {stat_phrase} performance {result_phrase}.",
    "{name} turned opportunity into a {stat_phrase} showing {result_phrase}.",
]

CONTEXT_FAMILY_POOL = [
    "The biggest swing came {inning_text}, when he {event_text}.",
    "His key damage came {inning_text}, when he {event_text}.",
    "The box score picked up real weight {inning_text}, as he {event_text}.",
    "He made his loudest impact {inning_text}, when he {event_text}.",
    "The turning point of his night came {inning_text}, when he {event_text}.",
    "His biggest contribution arrived {inning_text}, as he {event_text}.",
    "A lot of the value in his night came {inning_text}, when he {event_text}.",
    "The stat line really took shape {inning_text}, when he {event_text}.",
    "He came through {inning_text}, where he {event_text}.",
    "The production gained another layer {inning_text}, when he {event_text}.",
    "He delivered his most important swing {inning_text}, when he {event_text}.",
    "The box score got a lot more interesting {inning_text}, when he {event_text}.",
    "His night really swung {inning_text}, when he {event_text}.",
    "The most valuable part of his night arrived {inning_text}, as he {event_text}.",
    "A key moment came {inning_text}, when he {event_text}.",
]

FANTASY_CLOSING_POOL = [
    "The overall production carried real value in both season-long and daily leagues.",
    "Fantasy managers will gladly take that kind of production.",
    "There was enough here to help in multiple categories.",
    "This was the type of line that can quietly swing a matchup.",
    "There was category juice all over this performance.",
    "It was a productive night with real fantasy impact.",
    "The all-around production made this one stand out.",
    "This line should draw attention in most fantasy formats.",
    "It was the type of stat line that can move the needle.",
    "Managers looking for category production got it here.",
    "The performance played well across several fantasy formats.",
    "This was more than just an empty stat line.",
    "The combination of counting stats gave this outing added value.",
    "It was a useful performance with real category impact.",
    "The production gave fantasy managers something to work with in several spots.",
]

EV_HIT_FAMILIES = [
    "He also {verb} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "The loudest swing of his night was {article_hit}{inning_piece} struck at {ev:.1f} mph.",
    "His hardest-hit ball was {article_hit}{inning_piece} that left the bat at {ev:.1f} mph.",
    "He backed up the production by {verb_ing} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "The contact quality showed up on {article_hit}{inning_piece}, which came off the bat at {ev:.1f} mph.",
    "He added another layer to the box score by {verb_ing} {article_hit}{inning_piece} at {ev:.1f} mph.",
    "There was authority behind {article_hit}{inning_piece}, which registered at {ev:.1f} mph.",
    "He did not just produce; he {verb_past} {article_hit}{inning_piece} at {ev:.1f} mph.",
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
    "It was not just who he did it against, but what he did against {pitcher}: the {event_phrase}.",
    "The matchup adds some context here, since the {event_phrase} came against {pitcher}.",
    "That line looks a bit better when you remember the {event_phrase} came against {pitcher}.",
    "The opponent quality helps this stand out, with the {event_phrase} coming against {pitcher}.",
    "This was not empty production, especially with the {event_phrase} coming against {pitcher}.",
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
        "There was some real top-of-the-order value in the way he built this game.",
        "He made life easier on the bats behind him while hitting first.",
        "The offense got real momentum from the leadoff spot here.",
        "He looked comfortable doing table-setting work from the top of the order.",
        "This was a good example of how a leadoff hitter can influence the whole game.",
        "Batting first helped the box score play up in both real baseball and fantasy terms.",
        "He did what a top-of-the-order bat is supposed to do and then some.",
        "The leadoff assignment added another layer to the overall production.",
    ],
    "middle": [
        "Batting in the heart of the order, he did exactly what the club needed from that role.",
        "This was middle-of-the-order production, plain and simple.",
        "Working out of a run-producing role, he cashed in the chances that came his way.",
        "Hitting in the middle third, he turned traffic on the bases into real damage.",
        "This was the kind of output a club wants from the heart of the order.",
        "He made his middle-of-the-order trips count in a big way.",
        "Batting third through fifth gave him chances, and he did not waste many of them.",
        "He brought real run-producing value from the middle of the order.",
        "It was a clean example of how a middle-of-the-order bat can shape a game.",
        "The heart of the order got what it needed from him here.",
        "This was the sort of damage a club hopes for from a cleanup-type role.",
        "He did his share of the heavy lifting while hitting in the middle third.",
        "Hitting third through fifth only added more value to the production.",
        "There was real middle-order substance to this performance.",
        "He took advantage of a batting slot built for run production.",
    ],
    "bottom": [
        "Production like that from the bottom third of the order is a bonus in any lineup.",
        "Coming from the lower third, this performance carried a little extra weight.",
        "A club does not always get that kind of output from the sixth through ninth spots.",
        "There was some bonus offense here given that he was hitting near the bottom.",
        "That is the sort of production that can deepen an order in a hurry.",
        "The lower part of the order gave the club more than expected here.",
        "This was real help from a hitter working near the back of the order.",
        "The production played up because it came from the lower half.",
        "He gave his club some surprise value while hitting in the lower third.",
        "That kind of showing from the sixth, seventh, eighth, or ninth spot can change the shape of a game.",
        "It is always useful when the bottom of the order chips in like this.",
        "There was some added value because the damage came from near the back of the order.",
        "He made the lower part of the batting order look a lot deeper.",
        "The order got a little extra length from his spot here.",
        "This was more than a small contribution given where he was hitting.",
    ],
}

TREND_FAMILIES = {
    "bounce_back": [
        "It looked like the kind of game that can pull a hitter out of a quiet stretch.",
        "After a few quieter games, this looked more like the hitter fantasy managers were hoping to see.",
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
        "He is making a habit of finding at least one hit, and the streak is now {n}.",
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
        "He keeps giving his club more than one knock a night lately.",
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

def get_opposing_starter(feed: dict, hitter_side: str) -> dict:
    pitcher_side = "home" if hitter_side == "away" else "away"
    box_team = feed.get("liveData", {}).get("boxscore", {}).get("teams", {}).get(pitcher_side, {})
    pitchers = box_team.get("pitchers", []) or []
    players = box_team.get("players", {}) or {}
    starter_id = pitchers[0] if pitchers else None
    if not starter_id:
        return {"name": "", "era": ""}

    player_key = f"ID{starter_id}"
    player = players.get(player_key, {}) or {}
    season_stats = player.get("seasonStats", {}) or {}
    pitching = season_stats.get("pitching", season_stats) if isinstance(season_stats, dict) else {}
    return {
        "name": player.get("person", {}).get("fullName", ""),
        "era": str(pitching.get("era", "") or ""),
    }


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


def _stat_phrase(stats: dict) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)

    extras: list[str] = []
    if homers:
        extras.append(_small_count_phrase(homers, "homer"))
    if doubles:
        extras.append(_small_count_phrase(doubles, "double"))
    if triples:
        extras.append(_small_count_phrase(triples, "triple"))
    if rbi:
        extras.append(f"{_word_or_number(rbi)} RBI")
    if runs:
        extras.append(_small_count_phrase(runs, "run"))
    if walks:
        extras.append(_small_count_phrase(walks, "walk"))
    if steals:
        extras.append(_small_count_phrase(steals, "stolen base"))

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
    try:
        era = float(era_raw)
    except Exception:
        era = None

    event_phrase = _event_phrase_from_stats(stats)

    if era is not None and era <= 3.50:
        return random.choice(PITCHER_EVENT_FAMILIES).format(pitcher=name, event_phrase=event_phrase)
    if era is not None and era >= 5.00:
        return random.choice([
            f"The matchup helped a bit, as the {event_phrase} came against {name}.",
            f"It came against {name}, who has been more hittable than most so far.",
            f"The {event_phrase} came in a matchup that looked favorable on paper against {name}.",
            f"He took advantage of a matchup that tilted his way against {name}.",
            f"The setting helped, but he still did the damage with the {event_phrase} against {name}.",
            f"The opponent profile helped a little, as the {event_phrase} came against {name}.",
            f"He was able to capitalize against {name}, who has been more vulnerable than most.",
            f"The production came in a spot where the matchup looked workable against {name}.",
            f"He got into a favorable matchup and made it count against {name}.",
            f"The {event_phrase} came in a game where the matchup leaned in his favor against {name}.",
            f"He was not facing an ace here, and he made the most of it against {name}.",
            f"There was some matchup help on the table against {name}, and he took it.",
            f"He did what he should have done in a favorable spot against {name}.",
            f"The profile of the matchup helped, though he still had to do the work against {name}.",
            f"He turned a decent matchup against {name} into a productive night.",
        ])
    return random.choice([
        f"The {event_phrase} came against {name}.",
        f"He did that work with {name} on the other side.",
        f"That production came facing {name}.",
        f"The damage came with {name} on the mound.",
        f"He built that box-score damage while facing {name}.",
        f"The opponent on the mound was {name}, and he still got to his spots.",
        f"He pieced that together against {name}.",
        f"The production came with {name} working the other side.",
        f"He found some success against {name}.",
        f"The damage was done against {name}.",
        f"He built the night against {name}.",
        f"The work came while facing {name}.",
        f"He did his damage opposite {name}.",
        f"The box score took shape against {name}.",
        f"It all came with {name} on the mound.",
    ])


def _lineup_context_sentence(lineup_spot: int, stats: dict) -> str:
    if lineup_spot == 1:
        return random.choice(LINEUP_FAMILIES["leadoff"])
    if lineup_spot in _MIDDLE_ORDER_SPOTS:
        return random.choice(LINEUP_FAMILIES["middle"])
    if lineup_spot >= 7:
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
                "A one-run game leaves very little room for empty production, and this was not empty.",
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
                "There was not much room on the scoreboard, so his production kept its value all night.",
                "The closeness of the game made the entire line play up.",
                "In a tight finish, every bit of offense tends to matter more.",
                "His production held up because the margin never gave the game much cushion.",
                "Once the game stayed close, the smaller pieces of the box score mattered too.",
                "That line did not have much room to hide in a one-run finish.",
                "The final score made the whole box-score line more meaningful.",
                "Tight games tend to magnify useful offense, and this was useful offense.",
            ])
        if not team_won and rbi >= 2:
            return random.choice([
                "Even though the club came up short, he still left a mark on a close game.",
                "The result went the wrong way, but his line still held up in a game that stayed tight.",
                "The loss does not erase the fact that he was part of a close contest all night.",
                "He still managed to matter in a game that never got away.",
                "The final result was not there, but the production still carried weight in a close game.",
                "There was still real value in the performance, even with the loss.",
                "The production held up better because the game stayed within reach all night.",
                "His work did not disappear just because the club finished one run short.",
                "There was enough here to matter, even if the club could not finish it off.",
                "The close score helped keep his line relevant all the way to the end.",
                "He still made his mark in a game that remained in the balance.",
                "The tight finish kept the value of his line intact despite the loss.",
                "There was not much room for empty production in a game like that, and this was not empty.",
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
            "The production also helped the game tilt more firmly in his club's direction.",
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

    if hitless_tail >= 3 and today_hits >= 2:
        return random.choice(TREND_FAMILIES["bounce_back"])
    if homer_streak >= 2:
        return random.choice(TREND_FAMILIES["homer_streak"]).format(n=homer_streak)
    if hit_streak >= 4:
        return random.choice(TREND_FAMILIES["hit_streak"]).format(n=hit_streak)
    if multi_hit_streak >= 2:
        return random.choice(TREND_FAMILIES["multi_hit_streak"]).format(n=multi_hit_streak)
    if steal_streak >= 2:
        return random.choice(TREND_FAMILIES["steal_streak"]).format(n=steal_streak)
    if today_hr == 1 and today_hits >= 2:
        return random.choice([
            "This was a nice little across-the-board line, with both the power and hit columns getting attention.",
            "The box score was not built on the homer alone, which makes it more appealing.",
            "There was enough substance around the homer to make the performance feel complete.",
            "It was not just about the homer, as the rest of the box score gave it better shape.",
            "The homer grabbed the eye, but there was more here than just that one swing.",
            "The box score held together nicely around the power.",
            "He did not need the homer to carry the whole line by itself.",
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
            "The steals made sure the value was not limited to the bat alone.",
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
            f"{name} gives his club a loaded box score {pos_phrase or ''}".strip(),
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
            f"{name} does not waste the homer, piling on RBI too",
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
            f"{name} gives his club early life from the leadoff spot",
            f"{name} turns the leadoff role into a productive night",
            f"{name} makes the top spot in the order count",
            f"{name} brings table-setting value from the leadoff role",
            f"{name} helps the offense hum from leadoff",
            f"{name} builds a useful line from the top of the order",
            f"{name} gives his club some energy from the first spot",
            f"{name} makes things happen from leadoff",
            f"{name} keeps the offense moving from the top",
            f"{name} turns the leadoff job into fantasy value",
            f"{name} gives his club a strong table-setting line",
            f"{name} provides some top-of-the-order spark",
        ])
    if hits >= 3:
        return random.choice([
            f"{name} turns in a three-hit game",
            f"{name} keeps the hit column moving all night",
            f"{name} strings together three knocks in a useful line",
            f"{name} puts together a three-hit performance",
            f"{name} delivers a steady three-hit game for his club",
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
            f"{name} gives his club some catcher thump",
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





SUMMARY_OPENING_FAMILIES = {
    "walkoff": [
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, ending the game with the walk-off swing.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, and his final swing ended it.",
        "{name} turned in a {stat_phrase} performance in {possessive} win over the {opponent_text}, then put the game away in the final at-bat.",
        "{name} delivered a {stat_phrase} showing in {possessive} win over the {opponent_text}, with the last swing serving as the game-winner.",
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, then ended it himself in the final frame.",
        "{name} built a {stat_phrase} box score in {possessive} win over the {opponent_text}, and the last swing was the one everyone remembered.",
        "{name} finished {stat_phrase} in {possessive} win over the {opponent_text}, punctuating the night with the walk-off hit.",
        "{name} turned in a {stat_phrase} performance as the {team_name} beat the {opponent_text}, and he handled the final swing too.",
        "{name} put together a {stat_phrase} showing in {possessive} win over the {opponent_text}, then delivered the game-ending moment.",
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, with the walk-off swing serving as the finishing touch.",
        "{name} finished {stat_phrase} in {possessive} win over the {opponent_text}, and he made sure the final at-bat belonged to him.",
        "{name} built a {stat_phrase} line in {possessive} win over the {opponent_text}, then closed the book with the walk-off swing.",
        "{name} turned in a {stat_phrase} line as the {team_name} beat the {opponent_text}, capping it with the final blow.",
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, and his last swing was the difference.",
        "{name} gave the {team_name} a {stat_phrase} performance in the win over the {opponent_text}, then ended it himself.",
    ],
    "go_ahead_homer": [
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, with his homer providing the swing that decided the game.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, and his homer changed the shape of the night.",
        "{name} turned in a {stat_phrase} performance in {possessive} win over the {opponent_text}, with his biggest damage coming on the decisive swing.",
        "{name} built a {stat_phrase} box score in {possessive} win over the {opponent_text}, and the homer proved to be the turning point.",
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, with the long ball standing up as the key swing.",
        "{name} finished {stat_phrase} in {possessive} win over the {opponent_text}, and the homer ended up carrying the most weight.",
        "{name} turned in a {stat_phrase} performance as the {team_name} beat the {opponent_text}, with the homer serving as the difference-maker.",
        "{name} gave the {team_name} a {stat_phrase} showing in the win over the {opponent_text}, and his homer was the loudest moment.",
        "{name} posted a {stat_phrase} box score in {possessive} win over the {opponent_text}, with the homer proving to be enough.",
        "{name} built a {stat_phrase} line in {possessive} win over the {opponent_text}, and the game turned on his homer.",
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, and the long ball tilted the game for good.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, with the homer putting a permanent swing into the scoreboard.",
        "{name} came away with a {stat_phrase} line in {possessive} win over the {opponent_text}, and his homer ended up being the one that held.",
        "{name} built a {stat_phrase} box score in the win over the {opponent_text}, with the homer carrying decisive weight.",
        "{name} turned in a {stat_phrase} performance in {possessive} win over the {opponent_text}, and the homer was the lasting swing.",
    ],
    "go_ahead_hit": [
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, and he came through with the hit that ultimately decided it.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, with his biggest contribution arriving in a key late spot.",
        "{name} turned in a {stat_phrase} performance in {possessive} win over the {opponent_text}, and his go-ahead hit proved to be the difference.",
        "{name} built a {stat_phrase} box score in {possessive} win over the {opponent_text}, with the deciding hit standing out above the rest.",
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, and his key hit ended up holding all the way through.",
        "{name} finished {stat_phrase} in {possessive} win over the {opponent_text}, with his most important swing coming in the biggest spot.",
        "{name} turned in a {stat_phrase} performance as the {team_name} beat the {opponent_text}, and the go-ahead knock gave the box score its shape.",
        "{name} gave the {team_name} a {stat_phrase} showing in the win over the {opponent_text}, and his biggest hit came when the game was still hanging there.",
        "{name} posted a {stat_phrase} box score in {possessive} win over the {opponent_text}, and his timely hit ended up being the one that stood.",
        "{name} built a {stat_phrase} box score in the win over the {opponent_text}, with the most valuable piece coming on the go-ahead hit.",
        "{name} went {stat_phrase} in {possessive} win over the {opponent_text}, and his key knock turned into the deciding moment.",
        "{name} finished {stat_phrase} as the {team_name} beat the {opponent_text}, with the biggest hit coming exactly when it needed to.",
        "{name} turned in a {stat_phrase} performance in the win over the {opponent_text}, and his biggest swing came in the deciding spot.",
        "{name} came away with a {stat_phrase} showing in {possessive} win over the {opponent_text}, and his key hit pushed the game in the right direction for good.",
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

POSITION_POWER_FAMILIES = [
    "That kind of pop {pos_phrase} will get noticed in fantasy leagues.",
    "Power {pos_phrase} is not something fantasy managers ignore for long.",
    "There is extra fantasy value when that kind of damage comes {pos_phrase}.",
    "That is a useful kind of power to see {pos_phrase}.",
    "The homer carried some extra weight because it came {pos_phrase}.",
    "Fantasy managers always notice when power shows up {pos_phrase}.",
    "It is not every day you get that kind of thump {pos_phrase}.",
    "There is some added fantasy appeal when the damage comes {pos_phrase}.",
    "The positional angle made the power play up even more.",
    "The power stood out a bit more because of where it came from defensively.",
    "There was some position-scarcity value tied into the power here.",
    "The homer played up because the position does not always offer that much thump.",
    "The fantasy value climbed a little because of the position attached to the damage.",
    "His club got some uncommon pop from that defensive spot.",
    "The position only made the power more useful.",
]

FANTASY_FAMILIES = {
    "two_homer": FANTASY_CLOSING_POOL + [
        "Two-homer games like this will always stand out on a fantasy slate.",
        "That kind of power output is going to move the needle in any format.",
        "A multi-homer game does a lot of category work in one shot.",
        "This was the sort of power game that can win a fantasy matchup by itself.",
        "There is no such thing as a quiet two-homer line in fantasy leagues.",
        "A pair of homers is enough to make this one of the louder fantasy lines of the day.",
        "The power alone was enough to make this line matter in a big way.",
        "This is exactly the kind of power spike that swings categories.",
        "A game with this much thump tends to carry itself in fantasy terms.",
        "The home-run output put a lot of weight behind the rest of the box score.",
    ],
    "impact_homer": [
        "Most of his fantasy value came on one swing, but it was a massive one.",
        "It was the sort of line that can move the needle quickly in fantasy leagues.",
        "A homer with that kind of RBI support tends to do a lot of work in the box score.",
        "The long ball did plenty of lifting here, especially with the RBI attached.",
        "That is the kind of swing that can make an entire fantasy line look different.",
        "The homer gave the whole box score some real category punch.",
        "One swing did a lot of the heavy lifting for the fantasy value here.",
        "The power and RBI pairing gave the box score plenty of fantasy weight.",
        "That much damage on one swing tends to play in any format.",
        "The box score picked up real fantasy force once the homer came with runners on.",
        "The RBI attached to the homer made the whole line play up a level.",
        "The big swing turned a good line into a highly useful one.",
        "There was enough impact behind the homer to make the performance matter everywhere.",
        "The power came with enough run production to make it a real fantasy result.",
        "That one swing carried enough damage to define the night.",
    ],
    "four_hit": [
        "He was on base all night, which is exactly the kind of volume fantasy managers want to see.",
        "A four-hit game is going to matter in any format, even without multiple homers attached.",
        "That kind of hit volume can carry a fantasy line all by itself.",
        "There is a lot of category value packed into a four-hit game.",
        "A line built on that much contact tends to play everywhere.",
        "Four hits is enough to put real pressure on multiple categories.",
        "The volume of contact gave this line a lot of fantasy utility.",
        "This was the kind of hit total that can quietly swing a week.",
        "There was a lot of category help packed into the hit count alone.",
        "A four-hit game usually takes care of itself in fantasy terms.",
        "The box score had plenty of value simply because he kept stacking hits.",
        "That level of hit volume gave the performance a strong fantasy floor.",
        "There was no shortage of category help once the fourth hit showed up.",
        "The box score got very fantasy-friendly once the hit total kept climbing.",
        "It is hard for a four-hit game not to matter in a fantasy matchup.",
    ],
    "three_hit_xbh": [
        "It was not just a volume game either, as he mixed in real extra-base damage.",
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
        "Multi-hit games like this still matter, especially for managers chasing average and runs.",
        "He kept the hit column moving all night and gave fantasy managers a little of everything.",
        "Three-hit games still carry a lot of value, even without a huge power spike.",
        "There was a steady kind of fantasy usefulness to the way he built this line.",
        "It was a strong batting-average line with enough around it to matter elsewhere.",
        "The contact volume did most of the fantasy work here, and that is fine.",
        "This was the kind of simple, useful multi-hit game that holds up well.",
        "Three hits tend to take care of a lot of fantasy business on their own.",
        "There was enough hit volume here to make the performance matter in a lot of places.",
        "The box score gave a steady push to several categories without needing a huge swing.",
        "A clean multi-hit game like this still carries useful fantasy weight.",
        "The box score did not need a homer to hold up because the contact was steady enough.",
        "There was some real value in the hit total even before looking at the rest.",
        "A lot of the fantasy appeal came from the simple fact that he kept hitting.",
        "The performance leaned on contact more than thunder, but it still played.",
    ],
    "speed": [
        "Even without a huge hit total, the speed made the box score play up in fantasy.",
        "His legs did a lot of the fantasy heavy lifting here.",
        "Two steals can change the shape of a fantasy line in a hurry.",
        "The speed element did a lot to elevate the overall value of the performance.",
        "The running game gave this one some real category juice.",
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
        "The hit total was light, but the on-base work still gave the performance some fantasy usefulness.",
        "There is sneaky value in a line like this when the walks pile up.",
        "The on-base work did a fair amount of the lifting here.",
        "Even without a lot of hits, the plate discipline kept the box score usable.",
        "Walks like that can quietly make a fantasy line hold up.",
        "There was some useful value in simply reaching base that often.",
        "The production did not need a lot of hits to stay fantasy-relevant.",
        "The patience helped keep the floor of the box score intact.",
        "There was a little more fantasy value here than the hit total alone would suggest.",
        "The on-base component gave the performance some quiet usefulness.",
        "That much traffic on the bases can still matter in fantasy terms.",
        "The box score held together because of the plate discipline.",
        "The performance found a way to stay useful through on-base volume.",
        "There was some subtle fantasy help here because of the walks.",
        "The hit total did not tell the whole story once the walks were added in.",
    ],
    "general": FANTASY_CLOSING_POOL,
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
    "He did not just produce; he hit the ball hard too, logging {balls_100} batted balls over 100 mph.",
    "The contact quality supported the box score, including {balls_100} triple-digit bolts.",
    "He gave the box score some extra support by hitting {balls_100} balls at 100-plus mph.",
    "The box score looked even better once the quality of contact was factored in, including {balls_100} triple-digit batted balls.",
    "He added some hard-contact backing with {balls_100} balls at 100-plus mph.",
    "The batted-ball quality kept pace with the box score, including {balls_100} balls over 100 mph.",
    "He paired the final line with {balls_100} examples of triple-digit contact.",
]

SUMMARY_FILLER_POOL = [
    "The production had enough real-game and fantasy value to stand up on its own.",
    "There was enough substance here that the production did not feel empty.",
    "This was a line with both baseball and fantasy weight behind it.",
    "There was enough here for the performance to hold up both in real-game terms and in fantasy leagues.",
    "The overall line carried more than just surface-level fantasy value.",
    "There was enough shape to the box score for it to matter in several places.",
    "The production brought more than just one isolated category to the table.",
    "This was not just noise in the box score; there was some real weight to it.",
    "The production came with enough substance to hold up under a closer look.",
    "The final line felt useful for more than one reason.",
    "There was enough underneath the box score to make the performance feel legitimate.",
    "It was a line with some real structure behind it.",
    "The production did not need much help to look useful in fantasy terms.",
    "This was the kind of line that can hold up beyond the headline stat.",
    "The performance brought a little more to the table than a simple box-score glance might suggest.",
]


def _build_summary_opening(name: str, stats: dict, context: dict, opponent_text: str, team_name: str, team_won: bool) -> str:
    stat_phrase = _stat_phrase(stats)
    possessive = team_possessive(team_name)

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

    result_phrase = f"in {possessive} win over the {opponent_text}" if team_won else f"in {possessive} loss to the {opponent_text}"
    template = random.choice(SUMMARY_OPENING_FAMILIES[family])
    return template.format(
        name=name,
        stat_phrase=stat_phrase,
        opponent_text=opponent_text,
        team_name=team_name,
        possessive=possessive,
        result_phrase=result_phrase,
    )


def _event_specific_ev_sentence(context: dict, hardest_ev: float | None) -> str:
    if not hardest_ev:
        return ""
    homers = context.get("homers") or []
    xbh = context.get("extra_base_hits") or []
    if homers:
        inning = safe_int(homers[0].get("inning", 0), 0)
        inning_piece = f" in the {_ordinal(inning)}" if inning else ""
        data = {
            "article_hit": f"a homer{inning_piece}",
            "inning_piece": inning_piece,
            "ev": hardest_ev,
            "verb": random.choice(["crushed", "hammered", "launched", "blasted", "drove"]),
            "verb_past": random.choice(["crushed", "hammered", "launched", "blasted", "drove"]),
            "verb_ing": random.choice(["crushing", "hammering", "launching", "blasting", "driving"]),
        }
        return random.choice(EV_HIT_FAMILIES).format(**data)
    if xbh:
        first = xbh[0]
        hit_type = "double" if first.get("type") == "double" else "triple"
        inning = safe_int(first.get("inning", 0), 0)
        inning_piece = f" in the {_ordinal(inning)}" if inning else ""
        data = {
            "article_hit": f"a {hit_type}{inning_piece}",
            "inning_piece": inning_piece,
            "ev": hardest_ev,
            "verb": random.choice(["ripped", "smoked", "drilled", "lined", "hammered"]),
            "verb_past": random.choice(["ripped", "smoked", "drilled", "lined", "hammered"]),
            "verb_ing": random.choice(["ripping", "smoking", "drilling", "lining", "hammering"]),
        }
        return random.choice(EV_HIT_FAMILIES).format(**data)
    data = {
        "article_hit": "his hardest-hit ball",
        "inning_piece": "",
        "ev": hardest_ev,
        "verb": random.choice(["barreled", "smoked", "drove", "lined", "hammered"]),
        "verb_past": random.choice(["barreled", "smoked", "drove", "lined", "hammered"]),
        "verb_ing": random.choice(["barreling", "smoking", "driving", "lining", "hammering"]),
    }
    return random.choice(EV_HIT_FAMILIES).format(**data)


def _event_text_from_context(context: dict) -> tuple[str, str]:
    homers = context.get("homers") or []
    xbh = context.get("extra_base_hits") or []

    if context.get("walkoff"):
        if homers:
            return "ended it with a walk-off homer", f"in the {_ordinal(safe_int(homers[0].get('inning', 0), 0))}" if safe_int(homers[0].get('inning', 0), 0) else ""
        return "delivered the walk-off hit", ""
    if context.get("go_ahead_homer") and homers:
        inning = safe_int(homers[0].get("inning", 0), 0)
        return "went deep to put his club ahead for good", f"in the {_ordinal(inning)}" if inning else ""
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
        inning = safe_int(homers[0].get("inning", 0), 0)
        return "left the yard for his biggest swing", f"in the {_ordinal(inning)}" if inning else ""
    if xbh:
        hit_type = "double" if xbh[0].get("type") == "double" else "triple"
        inning = safe_int(xbh[0].get("inning", 0), 0)
        return f"drove a {hit_type} into the gap", f"in the {_ordinal(inning)}" if inning else ""
    if context.get("first_run_hit"):
        return "got his club on the board first", ""
    if context.get("insurance_hit"):
        return "added a late insurance hit", ""
    if context.get("late_rbi_hit"):
        return "did damage in a key late spot", ""
    return "", ""


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
) -> str:
    team_name = team_name_from_abbr(team)
    opponent_text = opponent or "the opposing club"

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

    sentences: list[str] = [
        _build_summary_opening(name, stats, context, opponent_text, team_name, team_won)
    ]

    used_signatures: set[str] = {"opening"}

    event_text, inning_text = _event_text_from_context(context)
    context_pool: list[str] = []
    if event_text:
        for template in SUMMARY_CONTEXT_FAMILIES:
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
        fantasy_pool = [s.format(pos_phrase=pos_phrase) for s in POSITION_POWER_FAMILIES] + fantasy_pool

    meta_pool: list[str] = []
    lineup_sentence = _lineup_context_sentence(lineup_spot, stats)
    if lineup_sentence:
        meta_pool.append(lineup_sentence)
    starter_sentence = _starter_context_sentence(pitcher, stats, context)
    if starter_sentence:
        meta_pool.append(starter_sentence)
    close_game_sentence = _close_game_context(team_score, opp_score, team_won, context, rbi)
    if close_game_sentence:
        meta_pool.append(close_game_sentence)

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
        if "mph" in lowered or "exit velocity" in lowered or "100-plus" in lowered or "triple-digit" in lowered:
            return "ev"
        if "leadoff" in lowered or "middle of the order" in lowered or "heart of the order" in lowered or "bottom third" in lowered or "lineup" in lowered:
            return "lineup"
        if "streak" in lowered or "quiet stretch" in lowered or "straight games" in lowered or "bounce-back" in lowered:
            return "trend"
        if "fantasy" in lowered or "category" in lowered or "matchup" in lowered or "season-long" in lowered or "daily leagues" in lowered:
            return "fantasy"
        if "against " in lowered or "on the mound" in lowered or "facing " in lowered:
            return "pitcher"
        if "inning" in lowered or "swing" in lowered or "gap" in lowered or "equalizer" in lowered:
            return "context"
        return lowered

    def _pick_unique(pool: list[str], fallback_sig: str | None = None) -> str:
        for sentence in pool:
            signature = _sig(sentence) if fallback_sig is None else fallback_sig
            if sentence and sentence not in sentences and signature not in used_signatures:
                used_signatures.add(signature)
                return sentence
        return ""

    for pool, forced_sig in [
        (context_pool, "context"),
        (fantasy_pool, "fantasy"),
        (meta_pool, None),
        (quality_pool, None),
    ]:
        picked = _pick_unique(pool, forced_sig)
        if picked:
            sentences.append(picked)

    standout = homers >= 2 or rbi >= 4 or hits >= 4 or steals >= 2 or (homers >= 1 and rbi >= 3)
    max_sentences = 5 if standout else 4

    while len(sentences) < 4:
        filler = _pick_unique(SUMMARY_FILLER_POOL, "filler")
        if filler:
            sentences.append(filler)
        else:
            break

    return " ".join(sentences[:max_sentences]).strip()

# ---------------- EMBED POSTING ----------------

async def post_card(channel: discord.abc.Messageable, hitter: dict, opponent: str, team_won: bool, feed: dict, game_date_et, team_score: int = -1, opp_score: int = -1) -> None:
    stats = hitter["stats"]
    label = classify_hitter(stats)
    recent_games = get_recent_hitter_games(hitter.get("id"), game_date_et)
    game_context = build_hitter_game_context(feed, hitter)
    position = hitter.get("position", "")
    lineup_spot = get_batting_order_spot(feed, hitter)
    pitcher = get_opposing_starter(feed, hitter.get("side", "home"))
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(hitter["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, hitter["name"], hitter["team"])
    embed.add_field(name="", value=f"**{build_hitter_subject(hitter['name'], stats, label, game_context, recent_games, position=position, lineup_spot=lineup_spot)}**", inline=False)
    embed.add_field(
        name="Summary",
        value=build_hitter_summary(
            hitter["name"], hitter["team"], stats, label, game_context, opponent, team_won, recent_games,
            pitcher=pitcher, lineup_spot=lineup_spot, position=position, team_score=team_score, opp_score=opp_score,
        ),
        inline=False,
    )
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
            games = get_games()
            log(f"Checking {len(games)} games")
            posts_this_scan = 0

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
                except Exception as exc:
                    log(f"Feed fetch error for {game_id}: {exc}")
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

                    player_team = normalize_team_abbr(hitter.get("team"))
                    opponent = home_name if player_team == away_abbr else away_name
                    team_won = (player_team == away_abbr and away_score > home_score) or (
                        player_team == home_abbr and home_score > away_score
                    )

                    log(f"Posting {hitter['name']} | {hitter['team']} | {matchup} | score={score_value}")
                    await post_card(channel, hitter, opponent, team_won, feed, game_date_et, team_score=away_score if player_team == away_abbr else home_score, opp_score=home_score if player_team == away_abbr else away_score)
                    posted.add(post_key)
                    posted_this_game += 1
                    posts_this_scan += 1

                    if posts_this_scan < MAX_POSTS_PER_SCAN:
                        await asyncio.sleep(max(POST_DELAY_SECONDS, 0.0))

            state["posted"] = sorted(posted)
            save_state(state)
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

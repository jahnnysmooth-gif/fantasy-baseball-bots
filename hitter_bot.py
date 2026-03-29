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
RESET_MARKER_FILE = STATE_DIR / "reset_marker.json"

ET = ZoneInfo("America/New_York")
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = int(os.getenv("HITTER_POLL_MINUTES", "10"))
RESET_HITTER_STATE = os.getenv("RESET_HITTER_STATE", "").lower() in {"1", "true", "yes"}
MIN_HITTER_SCORE = float(os.getenv("HITTER_MIN_SCORE", "5.0"))
MAX_CARDS_PER_GAME = int(os.getenv("HITTER_MAX_CARDS_PER_GAME", "10"))
REQUEST_TIMEOUT = float(os.getenv("HITTER_REQUEST_TIMEOUT", "30"))
MAX_POSTS_PER_SCAN = int(os.getenv("HITTER_MAX_POSTS_PER_SCAN", "18"))
MAX_POSTS_PER_GAME_PER_SCAN = int(os.getenv("HITTER_MAX_POSTS_PER_GAME_PER_SCAN", "18"))
POST_DELAY_SECONDS = float(os.getenv("HITTER_POST_DELAY_SECONDS", "1.25"))
AWAKE_SCAN_MIN_MINUTES = int(os.getenv("HITTER_AWAKE_SCAN_MIN_MINUTES", "2"))
AWAKE_SCAN_MAX_MINUTES = int(os.getenv("HITTER_AWAKE_SCAN_MAX_MINUTES", "5"))
SLEEP_START_HOUR_ET = int(os.getenv("HITTER_SLEEP_START_HOUR_ET", "3"))
SLEEP_END_HOUR_ET = int(os.getenv("HITTER_SLEEP_END_HOUR_ET", "13"))
ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")

intents = discord.Intents.default()
client: discord.Client | None = None
background_task: asyncio.Task | None = None
player_headshot_index: dict | None = None
hitter_stats_cache: dict = {}


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

def _should_reset_posted_state_once() -> bool:
    if not RESET_HITTER_STATE:
        return False

    marker_value = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        if RESET_MARKER_FILE.exists():
            raw = json.loads(RESET_MARKER_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and raw.get("marker") == marker_value:
                return False
    except Exception:
        pass

    try:
        RESET_MARKER_FILE.write_text(json.dumps({"marker": marker_value}, indent=2), encoding="utf-8")
    except Exception:
        pass
    return True


def load_state() -> dict:
    base = {"posted": []}
    if _should_reset_posted_state_once():
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
    return max(POLL_MINUTES, 1) * 60




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


def _recent_trend_note(recent_games: list[dict], stats: dict) -> str:
    if not recent_games:
        return ""

    today_hits = safe_int(stats.get("hits", 0), 0)
    today_hr = safe_int(stats.get("homeRuns", 0), 0)
    today_rbi = safe_int(stats.get("rbi", 0), 0)

    streak_hits = 0
    for game in recent_games:
        if game.get("h", 0) > 0:
            streak_hits += 1
        else:
            break
    hit_streak = streak_hits + (1 if today_hits > 0 else 0)

    hitless_tail = 0
    for game in recent_games:
        if game.get("h", 0) == 0:
            hitless_tail += 1
        else:
            break

    recent_slice = recent_games[:5]
    total_hits = sum(g.get("h", 0) for g in recent_slice) + today_hits
    total_hr = sum(g.get("hr", 0) for g in recent_slice) + today_hr
    total_rbi = sum(g.get("rbi", 0) for g in recent_slice) + today_rbi
    games = len(recent_slice) + 1

    if hitless_tail >= 3 and today_hits >= 2:
        return "It also looked like a possible step out of a recent slump."
    if hit_streak >= 6:
        return f"He has now hit safely in {hit_streak} straight games."
    if games >= 5 and total_hr >= 3:
        return f"He now has {total_hr} homers over his last {games} games."
    if games >= 5 and total_hits >= 9:
        return f"That gives him {total_hits} hits over his last {games} games."
    if games >= 5 and total_rbi >= 9:
        return f"He has also driven in {total_rbi} runs over his last {games} games."
    return ""


def build_hitter_subject(name: str, stats: dict, label: str, context: dict, recent_games: list[dict]) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)

    streak_hits = 0
    for game in recent_games:
        if game.get("h", 0) > 0:
            streak_hits += 1
        else:
            break
    hit_streak = streak_hits + (1 if hits > 0 else 0)

    if context.get("walkoff"):
        return f"{name} delivers the walk-off hit"
    if context.get("go_ahead_homer") and rbi >= 2:
        return f"{name} hits the go-ahead homer and drives in {rbi}"
    if context.get("go_ahead_homer"):
        return f"{name} breaks it open with the go-ahead homer"
    if context.get("game_tying_hit") and homers >= 1 and rbi >= 2:
        return f"{name} ties it up with a {rbi}-run homer"
    if context.get("go_ahead_hit") and rbi >= 2:
        return f"{name} comes through with the go-ahead hit and {rbi} RBI"
    if context.get("go_ahead_hit"):
        return f"{name} comes through with the go-ahead hit"
    if homers >= 2:
        return f"{name} homers twice in a big night at the plate"
    if homers == 1 and doubles + triples >= 1:
        return f"{name} does damage with multiple extra-base hits"
    if homers == 1 and rbi >= 3:
        return f"{name} goes deep and drives in {rbi}"
    if hits >= 4:
        return f"{name} collects four hits in a standout game"
    if hits >= 3 and rbi >= 3:
        return f"{name} piles up three hits and {rbi} RBI"
    if hits >= 3 and doubles + triples >= 1:
        return f"{name} strings together three hits and extra-base damage"
    if steals >= 2 and hits >= 2:
        return f"{name} reaches, runs, and swipes {steals} bags"
    if steals >= 2:
        return f"{name} changes the game with {steals} stolen bases"
    if rbi >= 4:
        return f"{name} drives in {rbi} runs in a big fantasy line"
    if hit_streak >= 6 and homers >= 1:
        return f"{name} stays hot with another homer"
    if hit_streak >= 6:
        return f"{name} stays hot with another multi-hit game"
    if hits >= 3:
        return f"{name} turns in a three-hit game"
    if homers == 1:
        return f"{name} leaves the yard in a productive night"
    return f"{name} puts together a useful night at the plate"


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


def _build_summary_opening(name: str, stats: dict, label: str, context: dict, opponent_text: str, team_name: str) -> str:
    """Return a varied opening sentence for the hitter summary, chosen randomly from
    situation-aware pools so no two cards read the same way."""

    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    xbh = doubles + triples + homers

    stat_line = f"{hits}-for-{ab}"
    rbi_str = f"{rbi} RBI" if rbi else ""
    hr_str = f"{homers} home run{'s' if homers != 1 else ''}" if homers else ""
    sb_str = f"{steals} stolen base{'s' if steals != 1 else ''}" if steals else ""
    run_str = f"{runs} run{'s' if runs != 1 else ''}" if runs else ""

    # --- Walk-off ---
    if context.get("walkoff"):
        return random.choice([
            f"{name} saved his best for last, delivering the walk-off hit to send the {team_name} home winners.",
            f"It all came down to {name}, and he delivered — the walk-off swing gave the {team_name} the win.",
            f"{name} ended the night on his terms, coming through with the walk-off hit for the {team_name}.",
            f"When the {team_name} needed it most, {name} stepped up and ended it with a walk-off.",
        ])

    # --- Go-ahead homer ---
    if context.get("go_ahead_homer"):
        homers_info = context.get("homers") or []
        inning = safe_int(homers_info[0].get("inning", 0), 0) if homers_info else 0
        inn_str = f" in the {_ordinal(inning)}" if inning else ""
        return random.choice([
            f"{name} did the damage that mattered most, going{inn_str} with the homer that put the {team_name} ahead for good.",
            f"The {team_name} needed a spark and {name} provided it, launching the go-ahead shot{inn_str}.",
            f"{name} flipped the game with one swing{inn_str}, giving the {team_name} a lead they would not give back.",
            f"The big blow came from {name}, whose homer{inn_str} handed the {team_name} the lead for good.",
        ])

    # --- Go-ahead hit (non-homer) ---
    if context.get("go_ahead_hit"):
        return random.choice([
            f"{name} came through in the clutch, delivering the hit that gave the {team_name} the lead for good.",
            f"When the game was on the line, {name} stepped up with the hit the {team_name} needed to take the lead.",
            f"{name} put the {team_name} in front and kept them there, coming through with the go-ahead knock.",
            f"The decisive blow came off the bat of {name}, whose hit gave the {team_name} a lead they would not surrender.",
        ])

    # --- Game-tying hit ---
    if context.get("game_tying_hit"):
        return random.choice([
            f"{name} kept the {team_name} in it, delivering the hit that knotted things back up.",
            f"The {team_name} were not done yet, and {name} made sure of it with the hit that tied the game.",
            f"{name} answered back with the equalizer, keeping the {team_name}'s night alive.",
        ])

    # --- Multi-homer game ---
    if homers >= 2:
        return random.choice([
            f"{name} went deep twice against the {opponent_text}, turning in one of the louder power performances of the day.",
            f"It was a two-homer night for {name}, who punished {opponent_text} pitching in a big way.",
            f"{name} went to work against the {opponent_text}, leaving the yard twice and making it count.",
            f"The {opponent_text} had no answer for {name}, who homered twice and did serious damage at the plate.",
        ])

    # --- Big hit parade (4+ hits) ---
    if hits >= 4:
        return random.choice([
            f"{name} was a nightmare for the {opponent_text} all night, going {stat_line} and reaching base in nearly every trip.",
            f"There was no cooling {name} off — he went {stat_line} against the {opponent_text} and never let up.",
            f"{name} picked apart the {opponent_text} pitching staff, collecting {hits} hits and staying on base all night.",
            f"The {opponent_text} could not find a way to retire {name}, who went {stat_line} in a dominant showing.",
        ])

    # --- Loud three-hit game with extra bases ---
    if hits >= 3 and xbh >= 2:
        return random.choice([
            f"{name} came loaded against the {opponent_text}, going {stat_line} with multiple extra-base hits to show for it.",
            f"It was not just the volume — {name} went {stat_line} and mixed in the kind of extra-base damage that makes a line jump off the page.",
            f"{name} did real damage against the {opponent_text}, collecting {hits} hits including multiple extra-base knocks.",
        ])

    # --- Solo homer with big RBI night ---
    if homers == 1 and rbi >= 3:
        return random.choice([
            f"{name} made his at-bats count against the {opponent_text}, going {stat_line} with {rbi_str} highlighted by a big homer.",
            f"One swing defined the night for {name} — the homer accounted for a huge chunk of the damage in a {stat_line} outing.",
            f"{name} did not need a lot of trips to make an impact, going {stat_line} with {rbi_str} and a homer in the mix.",
        ])

    # --- Speed-based line ---
    if steals >= 2 and hits >= 2:
        return random.choice([
            f"{name} created chaos against the {opponent_text}, going {stat_line} and swiping {sb_str} to make every base count.",
            f"Speed was the story for {name}, who went {stat_line} and kept the {opponent_text} defense on edge with {sb_str}.",
            f"{name} was a problem at every level tonight — {stat_line} at the plate and {sb_str} on the bases.",
        ])
    if steals >= 2:
        return random.choice([
            f"{name} turned a modest hit night into a useful fantasy line with {sb_str} against the {opponent_text}.",
            f"The bat was quiet but the legs were not — {name} swiped {sb_str} and made the {opponent_text} pay on the bases.",
        ])

    # --- Heavy RBI game without a homer ---
    if rbi >= 4 and homers == 0:
        return random.choice([
            f"{name} kept driving runs in all night against the {opponent_text}, finishing with {rbi_str} without going deep.",
            f"No homer needed — {name} went {stat_line} with {rbi_str} and was the engine of the {team_name} offense.",
            f"{name} did his damage the quiet way, collecting {hits} hits and knocking in {rbi} against the {opponent_text}.",
        ])

    # --- Solid three-hit game ---
    if hits >= 3:
        return random.choice([
            f"{name} had a consistent night at the dish, going {stat_line} and keeping the pressure on {opponent_text} pitching.",
            f"Nothing flashy, just effective — {name} went {stat_line} against the {opponent_text} and contributed up and down the lineup.",
            f"{name} put together a clean {stat_line} night against the {opponent_text}, staying productive across multiple trips.",
            f"The bat stayed hot for {name}, who went {stat_line} and gave the {team_name} another reliable offensive night.",
        ])

    # --- Single homer game ---
    if homers == 1:
        return random.choice([
            f"{name} provided the pop the {team_name} needed, going {stat_line} with a homer against the {opponent_text}.",
            f"The big swing came from {name}, who went deep against the {opponent_text} in a {stat_line} showing.",
            f"{name} did not need a big hit total to make an impact — one swing off the {opponent_text} said plenty.",
            f"It was a productive night for {name}, who went {stat_line} with a homer in the mix against the {opponent_text}.",
        ])

    # --- Run-scorer without big counting stats ---
    if runs >= 2 and hits >= 2:
        return random.choice([
            f"{name} was a constant presence on the basepaths against the {opponent_text}, going {stat_line} and crossing the plate {runs} times.",
            f"The contributions were real even if the line was quiet — {name} went {stat_line} and scored {runs} times for the {team_name}.",
        ])

    # --- Walk-heavy on-base game ---
    if walks >= 2 and hits >= 1:
        return random.choice([
            f"{name} was tough to put away against the {opponent_text}, going {stat_line} with {walks} walks and staying on base all night.",
            f"The {opponent_text} pitchers had no interest in giving {name} anything to hit — he went {stat_line} with {walks} free passes.",
        ])

    # --- Default fallback with variety ---
    fallback_options = [
        f"{name} finished {stat_line} against the {opponent_text} in a useful night at the plate.",
        f"{name} contributed a {stat_line} showing against the {opponent_text}.",
        f"It was a productive evening for {name}, who went {stat_line} against the {opponent_text}.",
        f"{name} put the ball in play against the {opponent_text}, going {stat_line} to round out the night.",
        f"Against the {opponent_text}, {name} went {stat_line} and gave the {team_name} another reliable outing.",
    ]
    return random.choice(fallback_options)


def build_hitter_summary(name: str, team: str, stats: dict, label: str, context: dict, opponent: str, team_won: bool, recent_games: list[dict]) -> str:
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

    first_sentence = _build_summary_opening(name, stats, label, context, opponent_text, team_name)

    extra_sentences: list[str] = []

    homers_info = context.get("homers") or []
    if context.get("walkoff"):
        # Opening already covered the walkoff — skip redundant context sentence
        pass
    elif context.get("go_ahead_homer"):
        # Opening already covered the go-ahead homer
        pass
    elif context.get("go_ahead_hit"):
        # Opening already covered the go-ahead hit
        pass
    elif context.get("game_tying_hit"):
        if homers_info and safe_int(homers_info[0].get("rbi", 0), 0) >= 2:
            extra_sentences.append(random.choice([
                "His homer pulled the game back even and changed the tone of the night.",
                "That multi-run blast knotted things up and swung the momentum.",
            ]))
        else:
            extra_sentences.append(random.choice([
                "He also delivered the hit that tied the game.",
                "He came through with the equalizer when the team needed it.",
            ]))
    elif context.get("insurance_hit"):
        extra_sentences.append(random.choice([
            f"He later added insurance that helped the {team_name} create some separation.",
            f"He chipped in with an insurance knock that gave the {team_name} a little more breathing room.",
            f"The late insurance hit from {name} took some of the pressure off the bullpen.",
        ]))
    elif context.get("first_run_hit"):
        extra_sentences.append(random.choice([
            f"He was the one who got the {team_name} on the board first.",
            f"He drew first blood for the {team_name}, opening the scoring early.",
            f"It was {name} who kicked things off, delivering the hit that got the {team_name} going.",
        ]))

    if homers >= 2:
        extra_sentences.append(random.choice([
            "It was one of the louder power lines of the day for fantasy purposes.",
            "Two-homer games do not come around often — this one is worth noting.",
            "The double-dip in the home run column makes this line stand out on the day.",
        ]))
    elif homers == 1 and rbi >= 3:
        extra_sentences.append(random.choice([
            "Most of his fantasy value came on one swing, but it was a big one.",
            "The homer did the heavy lifting, but the full line held up well around it.",
            "One big swing accounted for a chunk of the damage, and the rest of the line filled in nicely.",
        ]))
    elif hits >= 4:
        extra_sentences.append(random.choice([
            "He was on base all night and kept pressure on the pitching staff from one trip to the next.",
            "Four-hit games do not happen by accident — he was locked in from the first at-bat.",
            "He picked up a hit in nearly every trip and never let the at-bats go to waste.",
        ]))
    elif hits >= 3 and doubles + triples >= 1:
        extra_sentences.append(random.choice([
            "It was not just a volume game either, as he mixed in real extra-base damage.",
            "The extra-base hit gave the line some teeth beyond just the raw hit count.",
            "Hits and extra bases together make this one of the cleaner offensive lines of the night.",
        ]))
    elif hits >= 3:
        extra_sentences.append(random.choice([
            "He kept the line moving all night and turned nearly every trip into something useful.",
            "Three hits across the night shows he was locked in from start to finish.",
            "He stayed consistent across the lineup card and gave the team a reliable bat all game.",
        ]))
    elif steals >= 2:
        extra_sentences.append(random.choice([
            "Even without a huge hit total, the speed made the line play up in fantasy.",
            "The stolen bases are what separate this line — speed like that adds value even on a quieter night at the plate.",
        ]))

    hardest_ev = context.get("hardest_ev")
    balls_100 = safe_int(context.get("balls_100", 0), 0)
    if hardest_ev and hardest_ev >= 108:
        extra_sentences.append(random.choice([
            f"He also produced a top exit velocity of {hardest_ev:.1f} mph.",
            f"The contact was loud too — his hardest-hit ball registered {hardest_ev:.1f} mph off the bat.",
            f"He was squaring the ball up all night, topping out at {hardest_ev:.1f} mph on exit velo.",
        ]))
    elif balls_100 >= 3:
        extra_sentences.append(random.choice([
            f"He also put {balls_100} balls in play at 100-plus mph.",
            f"The quality of contact stood out as well — {balls_100} batted balls at 100-plus mph.",
        ]))

    trend_note = _recent_trend_note(recent_games, stats)
    if trend_note:
        extra_sentences.append(trend_note)

    cleaned: list[str] = []
    seen = set()
    for sentence in extra_sentences:
        if sentence and sentence not in seen:
            cleaned.append(sentence)
            seen.add(sentence)
        if len(cleaned) >= 3:
            break

    return " ".join([first_sentence] + cleaned).strip()

# ---------------- EMBED POSTING ----------------

async def post_card(channel: discord.abc.Messageable, hitter: dict, opponent: str, team_won: bool, feed: dict, game_date_et) -> None:
    stats = hitter["stats"]
    label = classify_hitter(stats)
    recent_games = get_recent_hitter_games(hitter.get("id"), game_date_et)
    game_context = build_hitter_game_context(feed, hitter)
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(hitter["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, hitter["name"], hitter["team"])
    embed.add_field(name="", value=f"**{build_hitter_subject(hitter['name'], stats, label, game_context, recent_games)}**", inline=False)
    embed.add_field(
        name="Summary",
        value=build_hitter_summary(hitter["name"], hitter["team"], stats, label, game_context, opponent, team_won, recent_games),
        inline=False,
    )
    embed.add_field(name="Game Line", value=format_hitter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_hitter_season_line(hitter.get("season_stats", {})), inline=False)
    await channel.send(embed=embed)


# ---------------- LOOP ----------------

async def hitter_loop() -> None:
    assert client is not None
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    load_player_headshot_index()

    state = load_state()
    posted = set(state.get("posted", []))
    if RESET_HITTER_STATE and not posted:
        log("RESET_HITTER_STATE enabled — posted state cleared for this run")

    while True:
        sleep_seconds = get_random_awake_interval_seconds()
        try:
            current_dt = now_et()
            if is_sleep_window(current_dt):
                sleep_seconds = seconds_until_wake(current_dt)
                wake_time = current_dt + timedelta(seconds=sleep_seconds)
                log(f"Sleeping until {wake_time.strftime('%Y-%m-%d %I:%M %p ET')}")
                continue

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
                game_scan_limit = max(1, MAX_CARDS_PER_GAME)
                for score_value, hitter in ranked:
                    if posts_this_scan >= MAX_POSTS_PER_SCAN or posted_this_game >= game_scan_limit:
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
                    await post_card(channel, hitter, opponent, team_won, feed, game_date_et)
                    posted.add(post_key)
                    posted_this_game += 1
                    posts_this_scan += 1

                    if posts_this_scan < MAX_POSTS_PER_SCAN:
                        await asyncio.sleep(max(POST_DELAY_SECONDS, 0.0))

            state["posted"] = sorted(posted)
            save_state(state)
        except Exception as exc:
            log(f"Loop error: {exc}")

        if is_sleep_window(now_et()):
            continue

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

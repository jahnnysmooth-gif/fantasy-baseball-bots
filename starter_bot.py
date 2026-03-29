import asyncio
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord
import requests

# ---------------- CONFIG ----------------

TOKEN = os.getenv("ANALYTIC_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("STARTER_WATCH_CHANNEL_ID", "0"))

STATE_FILE = "state/starter/state.json"
os.makedirs("state/starter", exist_ok=True)

ET = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"
TEAM_STATS_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?stats=season&group=hitting&season={}"
TEAM_ID_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1&season={}"

API_RETRY_ATTEMPTS = 3
API_RETRY_BACKOFF_SECONDS = 2
OPP_QUALITY_MIN_GAMES = 10

SLEEP_START_HOUR_ET = 3
SLEEP_END_HOUR_ET = 11
AWAKE_POLL_MIN_MINUTES = 6
AWAKE_POLL_MAX_MINUTES = 12
RESET_STARTER_STATE = os.getenv("RESET_STARTER_STATE", "").lower() in {"1", "true", "yes"}

MIN_STARTER_SCORE = float(os.getenv("STARTER_MIN_SCORE", "5.0"))
MAX_STARTER_CARDS_PER_GAME = int(os.getenv("STARTER_MAX_CARDS_PER_GAME", "2"))

VELOCITY_MIN_PITCHES = 10
VELOCITY_MIN_FASTBALLS = 3
FASTBALL_PITCH_CODES = {"FF", "FT", "SI", "FC", "FA", "FS"}

TEAM_COLORS = {
    "ARI": 0xA71930, "ATH": 0x003831, "ATL": 0xCE1141, "BAL": 0xDF4601,
    "BOS": 0xBD3039, "CHC": 0x0E3386, "CWS": 0x27251F, "CIN": 0xC6011F,
    "CLE": 0xE31937, "COL": 0x33006F, "DET": 0x0C2340, "HOU": 0xEB6E1F,
    "KC": 0x004687, "LAA": 0xBA0021, "LAD": 0x005A9C, "MIA": 0x00A3E0,
    "MIL": 0x12284B, "MIN": 0x002B5C, "NYM": 0xFF5910, "NYY": 0x0C2340,
    "PHI": 0xE81828, "PIT": 0xFDB827, "SD": 0x2F241D, "SF": 0xFD5A1E,
    "SEA": 0x005C5C, "STL": 0xC41E3A, "TB": 0x092C5C, "TEX": 0x003278,
    "TOR": 0x134A8E, "WSH": 0xAB0003,
}

TEAM_NAME_MAP = {
    "ARI": "Diamondbacks", "ATH": "Athletics", "ATL": "Braves", "BAL": "Orioles",
    "BOS": "Red Sox", "CHC": "Cubs", "CWS": "White Sox", "CIN": "Reds",
    "CLE": "Guardians", "COL": "Rockies", "DET": "Tigers", "HOU": "Astros",
    "KC": "Royals", "LAA": "Angels", "LAD": "Dodgers", "MIA": "Marlins",
    "MIL": "Brewers", "MIN": "Twins", "NYM": "Mets", "NYY": "Yankees",
    "PHI": "Phillies", "PIT": "Pirates", "SD": "Padres", "SF": "Giants",
    "SEA": "Mariners", "STL": "Cardinals", "TB": "Rays", "TEX": "Rangers",
    "TOR": "Blue Jays", "WSH": "Nationals",
}


def normalize_team_abbr(team: str) -> str:
    key = str(team or "").strip().upper()
    alias_map = {
        "AZ": "ARI", "ARI": "ARI", "CHW": "CWS", "CWS": "CWS",
        "WAS": "WSH", "WSN": "WSH", "WSH": "WSH", "TBR": "TB", "TB": "TB",
        "KCR": "KC", "KC": "KC", "SDP": "SD", "SD": "SD",
        "SFG": "SF", "SF": "SF", "OAK": "ATH", "ATH": "ATH",
    }
    return alias_map.get(key, key)


def team_name_from_abbr(team: str) -> str:
    normalized = normalize_team_abbr(team)
    return TEAM_NAME_MAP.get(normalized, normalized or "club")


pitching_stats_cache = {}
player_meta_cache = {}
team_hitting_cache = {}   # (team_abbr, season) -> hitting stats dict or None
team_id_cache = {}        # abbr -> mlb team id

ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
player_headshot_index = None

NUMBER_WORDS = {
    0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
    6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten",
    11: "eleven", 12: "twelve",
}


def log(msg: str):
    print(f"[STARTER] {msg}", flush=True)


# ---------------- HTTP / RETRY ----------------

def fetch_with_retry(url: str, timeout: int = 30) -> dict | None:
    for attempt in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < API_RETRY_ATTEMPTS:
                wait = API_RETRY_BACKOFF_SECONDS * attempt
                log(f"Request failed (attempt {attempt}/{API_RETRY_ATTEMPTS}): {e} — retrying in {wait}s")
                time.sleep(wait)
            else:
                log(f"Request failed after {API_RETRY_ATTEMPTS} attempts: {url} — {e}")
    return None


# ---------------- SLEEP HELPERS ----------------

def is_sleep_window_et(now_et: datetime | None = None) -> bool:
    now_et = now_et or datetime.now(ET)
    return SLEEP_START_HOUR_ET <= now_et.hour < SLEEP_END_HOUR_ET


def seconds_until_next_wake_et(now_et: datetime | None = None) -> int:
    now_et = now_et or datetime.now(ET)
    next_wake = now_et.replace(hour=SLEEP_END_HOUR_ET, minute=0, second=0, microsecond=0)
    if now_et >= next_wake:
        next_wake = (now_et + timedelta(days=1)).replace(
            hour=SLEEP_END_HOUR_ET, minute=0, second=0, microsecond=0
        )
    return max(1, int((next_wake - now_et).total_seconds()))


def random_awake_sleep_seconds() -> int:
    minutes = random.randint(AWAKE_POLL_MIN_MINUTES, AWAKE_POLL_MAX_MINUTES)
    return minutes * 60


# ---------------- TEAM / LOGO ----------------

def get_logo(team: str) -> str:
    normalized_team = normalize_team_abbr(team)
    logo_key_map = {
        "CWS": "chw", "ATH": "oak", "ARI": "ari",
        "WSH": "wsh", "TB": "tb", "KC": "kc", "SD": "sd", "SF": "sf",
    }
    key = logo_key_map.get(normalized_team, normalized_team.lower())
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"


# ---------------- NAME NORMALIZATION ----------------

def normalize_lookup_name(name: str) -> str:
    if not name:
        return ""
    cleaned = name.lower()
    for ch in [".", ",", "'", "`", "-", "_", "(", ")", "[", "]"]:
        cleaned = cleaned.replace(ch, " ")
    return " ".join(cleaned.split())


# ---------------- HEADSHOT INDEX ----------------

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


def apply_player_card_chrome(embed: discord.Embed, name: str, team: str):
    display_team = normalize_team_abbr(team) or "UNK"
    header_text = f"{name} | {display_team}"
    logo_url = get_logo(display_team)
    try:
        embed.set_author(name=header_text, icon_url=logo_url)
    except Exception:
        embed.set_author(name=header_text)

    headshot_url = get_player_headshot(name, team)
    if headshot_url:
        try:
            embed.set_thumbnail(url=headshot_url)
            return
        except Exception:
            pass

    try:
        embed.set_thumbnail(url=logo_url)
    except Exception:
        pass


# ---------------- STATE ----------------

def load_state():
    base = {"posted": []}
    if RESET_STARTER_STATE:
        return base

    if not os.path.exists(STATE_FILE):
        return base

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            base.update(data)
    except Exception:
        pass

    return base


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"posted": state.get("posted", [])}, f, indent=2)


# ---------------- SAFE CONVERSIONS ----------------

def safe_int(value, default=0):
    try:
        if value in (None, "", "-"):
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def safe_float(value, default=0.0):
    try:
        if value in (None, "", "-"):
            return default
        return float(value)
    except Exception:
        return default


def number_word(n: int) -> str:
    return NUMBER_WORDS.get(n, str(n))


def stat_phrase(count: int, singular: str, plural_form: str | None = None, zero_text: str | None = None) -> str:
    if plural_form is None:
        plural_form = f"{singular}s"
    if count == 0:
        return zero_text or f"no {plural_form}"
    if count == 1:
        return f"one {singular}"
    return f"{number_word(count)} {plural_form}"


# ---------------- IP HELPERS ----------------

def baseball_ip_to_outs(ip: str) -> int:
    text = str(ip).strip()
    if not text:
        return 0
    if "." not in text:
        return safe_int(text, 0) * 3
    whole_str, frac_str = text.split(".", 1)
    whole = safe_int(whole_str, 0)
    frac = min(safe_int(frac_str, 0), 2)
    return whole * 3 + frac


def format_ip_for_summary(ip: str) -> str:
    outs = baseball_ip_to_outs(ip)
    if outs == 1:
        return "one out"
    if outs == 2:
        return "two outs"
    if outs == 3:
        return "an inning"
    if outs == 6:
        return "two innings"
    text = str(ip).strip()
    if text.endswith(".0"):
        whole = safe_int(text.split(".")[0], 0)
        return f"{whole} innings"
    if text.endswith(".1"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅓ of an inning" if whole == 0 else f"{whole}⅓ innings"
    if text.endswith(".2"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅔ of an inning" if whole == 0 else f"{whole}⅔ innings"
    return f"{text} innings"


def format_starter_ip_for_summary(ip: str) -> str:
    outs = baseball_ip_to_outs(ip)
    if outs == 1:
        return "one out"
    if outs == 2:
        return "two outs"
    if outs == 3:
        return "one inning"
    if outs == 6:
        return "two innings"
    text = str(ip).strip()
    if text.endswith(".0"):
        whole = safe_int(text.split(".")[0], 0)
        return f"{number_word(whole)} inning" if whole == 1 else f"{number_word(whole)} innings"
    if text.endswith(".1"):
        whole = safe_int(text.split(".")[0], 0)
        if whole == 0:
            return "one out"
        return f"{number_word(whole)} and one-third innings"
    if text.endswith(".2"):
        whole = safe_int(text.split(".")[0], 0)
        if whole == 0:
            return "two outs"
        return f"{number_word(whole)} and two-thirds innings"
    return format_ip_for_summary(ip)


# ---------------- DATE PARSING ----------------

def parse_game_date_et(game: dict):
    game_date = game.get("gameDate")
    if not game_date:
        return None
    try:
        dt = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
        return dt.astimezone(ET).date()
    except Exception:
        return None


# ---------------- PITCH EVENT HELPERS ----------------

def parse_pitch_type_code(event: dict) -> str:
    details = event.get("details", {}) if isinstance(event, dict) else {}
    pitch_type = details.get("type") if isinstance(details, dict) else None
    if isinstance(pitch_type, dict):
        return str(pitch_type.get("code") or pitch_type.get("description") or "").strip().upper()
    return ""


def is_called_strike_event(event: dict) -> bool:
    details = event.get("details", {}) if isinstance(event, dict) else {}
    desc = str(details.get("description") or "").lower()
    code = str(details.get("code") or "").upper()
    call = details.get("call") if isinstance(details, dict) else None
    call_desc = str((call or {}).get("description") or "").lower() if isinstance(call, dict) else ""
    if "called strike" in desc or "called strike" in call_desc:
        return True
    return code in {"C", "AC"}


def is_whiff_event(event: dict) -> bool:
    details = event.get("details", {}) if isinstance(event, dict) else {}
    desc = str(details.get("description") or "").lower()
    code = str(details.get("code") or "").upper()
    call = details.get("call") if isinstance(details, dict) else None
    call_desc = str((call or {}).get("description") or "").lower() if isinstance(call, dict) else ""
    if "swinging strike" in desc or "swinging strike" in call_desc:
        return True
    if "foul tip" in desc or "missed bunt" in desc:
        return True
    return code in {"S", "T", "M", "Q"}


# ---------------- PITCH METRICS ----------------

def build_starter_pitch_metrics(feed: dict, pitcher_id: int):
    if not feed or pitcher_id is None:
        return {}

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    total_pitches = 0
    strikes = 0
    called_strikes = 0
    whiffs = 0
    fastball_velos = []
    pitch_type_counts = {}

    for play in plays:
        matchup = play.get("matchup", {}) if isinstance(play, dict) else {}
        pitcher = matchup.get("pitcher", {}) if isinstance(matchup, dict) else {}
        if pitcher.get("id") != pitcher_id:
            continue

        for event in play.get("playEvents", []):
            if not isinstance(event, dict) or not event.get("isPitch"):
                continue
            total_pitches += 1
            details = event.get("details", {})
            if details.get("isStrike"):
                strikes += 1
            if is_called_strike_event(event):
                called_strikes += 1
            if is_whiff_event(event):
                whiffs += 1

            pitch_data = event.get("pitchData", {})
            start_speed = safe_float(pitch_data.get("startSpeed"), 0.0)
            pitch_code = parse_pitch_type_code(event)

            if pitch_code:
                pitch_type_counts[pitch_code] = pitch_type_counts.get(pitch_code, 0) + 1

            if start_speed > 0 and pitch_code in FASTBALL_PITCH_CODES:
                fastball_velos.append(start_speed)

    payload = {
        "pitch_count": total_pitches,
        "strikes": strikes,
        "whiffs": whiffs,
        "called_strikes": called_strikes,
        "pitch_type_counts": pitch_type_counts,
    }
    if total_pitches > 0:
        payload["csw_percent"] = round(((called_strikes + whiffs) / total_pitches) * 100.0, 1)
    if len(fastball_velos) >= VELOCITY_MIN_FASTBALLS and total_pitches >= VELOCITY_MIN_PITCHES:
        payload["avg_fastball_velocity"] = round(sum(fastball_velos) / len(fastball_velos), 1)
        payload["fastball_count"] = len(fastball_velos)
    return payload


# ---------------- GAME FLOW ----------------

def build_starter_game_flow(feed: dict, pitcher_id: int, side: str):
    default = {
        "innings_sequence": [], "runs_by_inning": {}, "scoreless_to_start": 0,
        "only_damage_in_one_inning": False, "biggest_inning_runs": 0,
        "scored_in_first": False, "settled_after_rough": False,
        "late_damage": False, "team_runs_while_in": 0, "opp_runs_while_in": 0,
        "entry_margin": 0, "exit_margin": 0, "first_inning": None, "last_inning": None,
    }
    if not feed or pitcher_id is None:
        return default

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    if not plays:
        return default

    pitcher_plays = []
    for idx, play in enumerate(plays):
        matchup = play.get("matchup", {}) if isinstance(play, dict) else {}
        pitcher = matchup.get("pitcher", {}) if isinstance(matchup, dict) else {}
        if pitcher.get("id") == pitcher_id:
            pitcher_plays.append((idx, play))

    if not pitcher_plays:
        return default

    def score_tuple(play):
        result = play.get("result", {}) if isinstance(play, dict) else {}
        return safe_int(result.get("awayScore", 0), 0), safe_int(result.get("homeScore", 0), 0)

    first_idx = pitcher_plays[0][0]
    prev_away, prev_home = score_tuple(plays[first_idx - 1]) if first_idx > 0 else (0, 0)

    entry_team = prev_home if side == "home" else prev_away
    entry_opp = prev_away if side == "home" else prev_home

    innings_sequence = []
    runs_by_inning = {}

    for _, play in pitcher_plays:
        about = play.get("about", {}) if isinstance(play, dict) else {}
        inning = safe_int(about.get("inning", 0), 0)
        if inning and inning not in innings_sequence:
            innings_sequence.append(inning)

        away_after, home_after = score_tuple(play)
        opp_runs_on_play = (away_after - prev_away) if side == "home" else (home_after - prev_home)
        if opp_runs_on_play > 0 and inning:
            runs_by_inning[inning] = runs_by_inning.get(inning, 0) + opp_runs_on_play

        prev_away, prev_home = away_after, home_after

    exit_team = prev_home if side == "home" else prev_away
    exit_opp = prev_away if side == "home" else prev_home

    inning_runs = [runs_by_inning.get(inning, 0) for inning in innings_sequence]
    scoreless_to_start = 0
    for runs in inning_runs:
        if runs == 0:
            scoreless_to_start += 1
        else:
            break

    run_innings = [r for r in inning_runs if r > 0]
    scored_in_first = bool(inning_runs and inning_runs[0] > 0)
    settled_after_rough = scored_in_first and len(inning_runs) >= 3 and all(r == 0 for r in inning_runs[1:3])
    late_damage = len(inning_runs) >= 2 and inning_runs[-1] > 0 and sum(inning_runs[:-1]) == 0

    payload = dict(default)
    payload.update({
        "innings_sequence": innings_sequence,
        "runs_by_inning": runs_by_inning,
        "scoreless_to_start": scoreless_to_start,
        "only_damage_in_one_inning": len(run_innings) == 1 and sum(run_innings) > 0,
        "biggest_inning_runs": max(run_innings) if run_innings else 0,
        "scored_in_first": scored_in_first,
        "settled_after_rough": settled_after_rough,
        "late_damage": late_damage,
        "team_runs_while_in": max(exit_team - entry_team, 0),
        "opp_runs_while_in": max(exit_opp - entry_opp, 0),
        "entry_margin": entry_team - entry_opp,
        "exit_margin": exit_team - exit_opp,
        "first_inning": innings_sequence[0] if innings_sequence else None,
        "last_inning": innings_sequence[-1] if innings_sequence else None,
    })
    return payload


# ---------------- GET STARTERS ----------------

def get_starters(feed: dict):
    result = []
    box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    game_teams = feed.get("gameData", {}).get("teams", {})

    for side in ["home", "away"]:
        team = normalize_team_abbr(game_teams.get(side, {}).get("abbreviation"))
        if not team:
            team = normalize_team_abbr(box.get(side, {}).get("team", {}).get("abbreviation", "UNK"))
        players = box.get(side, {}).get("players", {})

        probable_id = None
        probable = feed.get("gameData", {}).get("probablePitchers", {}).get(side, {})
        if isinstance(probable, dict):
            probable_id = probable.get("id")

        candidates = []
        for p in players.values():
            stats = p.get("stats", {}).get("pitching")
            if not stats or not stats.get("inningsPitched"):
                continue

            season_stats_block = p.get("seasonStats", {})
            if isinstance(season_stats_block, dict) and "pitching" in season_stats_block:
                season_stats = season_stats_block.get("pitching", {})
            elif isinstance(season_stats_block, dict):
                season_stats = season_stats_block
            else:
                season_stats = {}

            person_id = p.get("person", {}).get("id")
            metrics = build_starter_pitch_metrics(feed, person_id)
            game_flow = build_starter_game_flow(feed, person_id, side)
            box_pitch_count = safe_int(stats.get("numberOfPitches", 0), 0)
            box_strikes = safe_int(stats.get("strikes", 0), 0)

            candidate = {
                "id": person_id,
                "name": p.get("person", {}).get("fullName", "Unknown Pitcher"),
                "team": team,
                "side": side,
                "stats": stats,
                "season_stats": season_stats,
                "pitch_count": box_pitch_count if box_pitch_count > 0 else metrics.get("pitch_count", 0),
                "strikes": box_strikes if box_strikes > 0 else metrics.get("strikes", 0),
                "whiffs": metrics.get("whiffs", 0),
                "called_strikes": metrics.get("called_strikes", 0),
                "csw_percent": metrics.get("csw_percent"),
                "avg_fastball_velocity": metrics.get("avg_fastball_velocity"),
                "fastball_count": metrics.get("fastball_count", 0),
                "pitch_type_counts": metrics.get("pitch_type_counts", {}),
                "game_flow": game_flow,
            }
            candidates.append(candidate)

            if person_id is not None:
                player_meta_cache[person_id] = {
                    "name": candidate["name"],
                    "team": candidate["team"],
                    "season_stats": season_stats,
                }

        if not candidates:
            continue

        selected = None
        if probable_id is not None:
            for c in candidates:
                if c.get("id") == probable_id:
                    selected = c
                    break

        if selected is None:
            candidates.sort(
                key=lambda c: (
                    safe_float(c["stats"].get("inningsPitched", "0.0"), 0.0),
                    -safe_int(c["stats"].get("gamesFinished", 0), 0),
                ),
                reverse=True,
            )
            selected = candidates[0]

        result.append(selected)

    return result


# ---------------- SCORING / CLASSIFICATION ----------------

def starter_score(stats: dict) -> float:
    ip = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    h = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    qs = 1 if ip >= 6.0 and er <= 3 else 0
    win = safe_int(stats.get("wins", 0), 0)
    return round(ip * 2.1 + k * 0.9 + qs * 3.0 + win * 1.5 - er * 2.0 - h * 0.35 - bb * 0.5, 2)


def classify_starter(stats: dict) -> str:
    ip = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    traffic = hits + bb

    if ip >= 7.0 and er == 0 and traffic <= 5:
        return "GEM"
    if ip >= 6.0 and er <= 2 and k >= 8:
        return "DOMINANT"
    if ip >= 6.0 and er <= 3:
        return "QUALITY"
    if k >= 9 and er <= 4:
        return "STRIKEOUT"
    if outs < 9:
        return "SHORT" if er <= 2 else "ROUGH"
    if bb >= 4 and er >= 2:
        return "NO_COMMAND"
    if hits >= 8 or (hits >= 6 and bb <= 1 and er >= 4):
        return "HIT_HARD"
    if er >= 5:
        return "ROUGH"
    if hits <= 4 and er <= 2:
        return "SHARP"
    if traffic >= 8 or (hits >= 6 and er <= 2):
        return "UNEVEN"
    return "SOLID"


GOOD_STARTER_LABELS = {"GEM", "DOMINANT", "QUALITY", "STRIKEOUT", "SHARP", "SOLID"}
BAD_STARTER_LABELS = {"ROUGH", "NO_COMMAND", "HIT_HARD", "SHORT"}
SUBPAR_STARTER_LABELS = BAD_STARTER_LABELS | {"UNEVEN"}
POSITIVE_STARTER_LABELS = GOOD_STARTER_LABELS


def is_bad_starter_label(label: str) -> bool:
    return label in BAD_STARTER_LABELS


def starter_impact_tag(label: str) -> str:
    return {
        "GEM": "💎 Pitching Gem", "DOMINANT": "🔥 Dominant Start",
        "QUALITY": "✅ Quality Start", "STRIKEOUT": "🧨 Strikeout Juice",
        "SHARP": "🎯 Sharp Outing", "SOLID": "📈 Solid Start",
        "UNEVEN": "📉 Uneven Start", "SHORT": "⏱️ Short Outing",
        "ROUGH": "⚠️ Rough Start", "NO_COMMAND": "🧭 Command Wasn't There",
        "HIT_HARD": "💥 Hit Hard",
    }.get(label, "📈 Solid Start")


# ---------------- FORMAT LINES ----------------

def format_starter_game_line(stats: dict) -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    return " • ".join([f"{ip} IP", f"{h} H", f"{er} ER", f"{bb} BB", f"{k} K"])


def format_starter_season_line(season_stats: dict) -> str:
    season = season_stats or {}
    era = season.get("era") or season.get("earnedRunAverage") or "0.00"
    whip = season.get("whip") or season.get("walksAndHitsPerInningPitched")
    w = safe_int(season.get("wins", 0), 0)
    losses = safe_int(season.get("losses", 0), 0)
    k = safe_int(season.get("strikeOuts", 0), 0)
    ip = season.get("inningsPitched") or "0.0"
    parts = [f"{w}-{losses}", f"ERA {era}"]
    if whip not in (None, ""):
        parts.append(f"WHIP {whip}")
    parts.append(f"{k} K")
    parts.append(f"{ip} IP")
    return " • ".join(parts)


def build_starter_score_display(away_abbr: str, away_score: int, home_abbr: str, home_score: int) -> str:
    if home_score > away_score:
        return f"{home_abbr} {home_score} - {away_abbr} {away_score}"
    if away_score > home_score:
        return f"{away_abbr} {away_score} - {home_abbr} {home_score}"
    return f"{away_abbr} {away_score} - {home_abbr} {home_score}"


def format_percent_text(value) -> str:
    try:
        return f"{int(round(float(value)))} percent"
    except Exception:
        return "0 percent"


# ---------------- PITCHING STATS CACHE (recent appearances) ----------------

def get_pitching_stats_for_date(target_date):
    if target_date in pitching_stats_cache:
        return pitching_stats_cache[target_date]

    stats_by_pitcher = {}

    try:
        data = fetch_with_retry(f"{SCHEDULE_URL}&date={target_date.isoformat()}")
        if data is None:
            pitching_stats_cache[target_date] = stats_by_pitcher
            return stats_by_pitcher

        games = []
        for date_block in data.get("dates", []):
            games.extend(date_block.get("games", []))

        for game in games:
            game_id = game.get("gamePk")
            if not game_id:
                continue

            feed = get_feed(game_id)
            if feed is None:
                continue

            box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
            for side in ["home", "away"]:
                players = box.get(side, {}).get("players", {})
                for p in players.values():
                    stats = p.get("stats", {}).get("pitching")
                    if not stats or not stats.get("inningsPitched"):
                        continue

                    pid = p.get("person", {}).get("id")
                    if pid is None:
                        continue

                    metrics = build_starter_pitch_metrics(feed, pid)
                    stats_by_pitcher[pid] = {
                        "ip": str(stats.get("inningsPitched", "0.0")),
                        "h": safe_int(stats.get("hits", 0), 0),
                        "er": safe_int(stats.get("earnedRuns", 0), 0),
                        "bb": safe_int(stats.get("baseOnBalls", 0), 0),
                        "k": safe_int(stats.get("strikeOuts", 0), 0),
                        "avg_fastball_velocity": metrics.get("avg_fastball_velocity"),
                    }

    except Exception as e:
        log(f"Pitching stats cache load failed for {target_date}: {e}")

    pitching_stats_cache[target_date] = stats_by_pitcher
    return stats_by_pitcher


def get_recent_appearances(pitcher_id: int, game_date_et, limit=3, max_days=45):
    appearances = []
    if pitcher_id is None or game_date_et is None:
        return appearances

    check_date = game_date_et - timedelta(days=1)

    for _ in range(max_days):
        stats_by_pitcher = get_pitching_stats_for_date(check_date)
        if pitcher_id in stats_by_pitcher:
            appearances.append(stats_by_pitcher[pitcher_id])
            if len(appearances) >= limit:
                break
        check_date -= timedelta(days=1)

    return appearances


# ---------------- OPPONENT QUALITY ----------------

def get_mlb_team_id(abbr: str, season: int) -> int | None:
    """Resolve a team abbreviation to its MLB Stats API numeric team ID."""
    if abbr in team_id_cache:
        return team_id_cache[abbr]

    data = fetch_with_retry(TEAM_ID_URL.format(season))
    if not data:
        return None

    for team in data.get("teams", []):
        t_abbr = normalize_team_abbr(team.get("abbreviation", ""))
        t_id = team.get("id")
        if t_id:
            team_id_cache[t_abbr] = t_id

    return team_id_cache.get(abbr)


def get_team_hitting_stats(team_abbr: str, season: int) -> dict | None:
    """
    Return season hitting stats for a team.
    Returns None if the team has played fewer than OPP_QUALITY_MIN_GAMES
    (too early in the season to be meaningful). Results are cached per session.
    """
    key = (team_abbr, season)
    if key in team_hitting_cache:
        return team_hitting_cache[key]

    team_id = get_mlb_team_id(team_abbr, season)
    if not team_id:
        team_hitting_cache[key] = None
        return None

    data = fetch_with_retry(TEAM_STATS_URL.format(team_id, season))
    if not data:
        team_hitting_cache[key] = None
        return None

    stats = None
    for block in data.get("stats", []):
        splits = block.get("splits", [])
        if splits:
            stats = splits[0].get("stat", {})
            break

    if not stats:
        team_hitting_cache[key] = None
        return None

    games_played = safe_int(stats.get("gamesPlayed", 0), 0)
    if games_played < OPP_QUALITY_MIN_GAMES:
        team_hitting_cache[key] = None
        return None

    result = {
        "games_played": games_played,
        "avg": safe_float(stats.get("avg", "0"), 0.0),
        "ops": safe_float(stats.get("ops", "0"), 0.0),
        "obp": safe_float(stats.get("obp", "0"), 0.0),
        "slg": safe_float(stats.get("slg", "0"), 0.0),
        "runs": safe_int(stats.get("runs", 0), 0),
        "strikeOuts": safe_int(stats.get("strikeOuts", 0), 0),
        "homeRuns": safe_int(stats.get("homeRuns", 0), 0),
    }
    team_hitting_cache[key] = result
    return result


def classify_offense(ops: float) -> str:
    """Bucket a team OPS into a qualitative tier."""
    if ops >= 0.800:
        return "elite"
    if ops >= 0.740:
        return "above_average"
    if ops >= 0.690:
        return "average"
    if ops >= 0.640:
        return "below_average"
    return "weak"


# ---------------- SEASON CONTEXT ----------------

def detect_season_bests(stats: dict, season_stats: dict) -> dict:
    """
    Compare today's game line against season averages to flag notable
    single-game highs or lows. Returns a dict of boolean flags.
    """
    flags = {}
    if not season_stats:
        return flags

    k_game = safe_int(stats.get("strikeOuts", 0), 0)
    er_game = safe_int(stats.get("earnedRuns", 0), 0)
    ip_game = safe_float(stats.get("inningsPitched", "0.0"), 0.0)

    season_k = safe_int(season_stats.get("strikeOuts", 0), 0)
    season_gs = safe_int(season_stats.get("gamesStarted", 0), 0) or safe_int(season_stats.get("gamesPitched", 0), 0)
    season_er = safe_int(season_stats.get("earnedRuns", 0), 0)

    if season_gs >= 3:
        avg_k_per_start = season_k / season_gs if season_gs > 0 else 0
        avg_er_per_start = season_er / season_gs if season_gs > 0 else 0

        if k_game >= 8 and k_game >= avg_k_per_start * 1.4:
            flags["k_high_vs_avg"] = True

        if k_game >= 10:
            flags["k_best"] = True

        if er_game >= 5 and er_game >= avg_er_per_start * 1.8:
            flags["er_worst"] = True

    if er_game == 0 and ip_game >= 6.0:
        flags["clean_sheet"] = True

    return flags


# ---------------- SEED ----------------

def build_starter_summary_seed(name: str, stats: dict, game_context: dict) -> int:
    seed_text = (
        f"{name}|{stats.get('inningsPitched', '0.0')}|{stats.get('hits', 0)}|"
        f"{stats.get('earnedRuns', 0)}|{stats.get('baseOnBalls', 0)}|"
        f"{stats.get('strikeOuts', 0)}|{game_context.get('score_display', '')}"
    )
    return sum(ord(ch) for ch in seed_text)


# ---------------- SENTENCE BUILDERS ----------------

def build_starter_overview(name: str, label: str, stats: dict, seed: int, team_name: str = "club", opp_name: str = "opponent") -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    ip_text = format_starter_ip_for_summary(ip)
    outs = baseball_ip_to_outs(ip)

    if outs < 3:
        variants = {
            "ROUGH": [
                f"{name} was gone before he could get through the first inning, and things got ugly in a hurry.",
                f"{name} only recorded {ip_text}, and the outing was sideways almost immediately.",
                f"{name} barely had time to settle in before the start got away from him.",
            ],
            "HIT_HARD": [
                f"{name} only got {ip_text}, with hitters doing damage almost from the opening pitch.",
                f"{name} did not make it out of the first, and the contact against him was loud right away.",
                f"{name} was knocked around early and never had a chance to stabilize the start.",
            ],
            "NO_COMMAND": [
                f"{name} only lasted {ip_text}, and too many missed spots put him in trouble immediately.",
                f"{name} was out after {ip_text}, with the count getting away from him before he could regain control.",
                f"{name} never really found the zone early enough to let the outing breathe.",
            ],
            "SHORT": [
                f"{name} only recorded {ip_text}, so this never had the shape of a normal start.",
                f"{name} was lifted after {ip_text}, forcing the bullpen into the game almost right away.",
                f"{name} did not last long, and the outing was over before it had much chance to develop.",
            ],
        }
        choices = variants.get(label, variants["SHORT"])
        return choices[seed % len(choices)]

    variants = {
        "GEM": [
            f"{name} turned in one of the cleanest starts of the day, covering {ip_text} without allowing an earned run.",
            f"{name} set the tone early and stayed in command through {ip_text} of scoreless work.",
            f"{name} was in control all night and gave the {opp_name} very little to work with.",
            f"{name} gave the {team_name} a true stopper-type outing and never let the game drift.",
        ],
        "DOMINANT": [
            f"{name} took over this start early and stayed on top of it for {ip_text}.",
            f"{name} looked overpowering and spent most of the night dictating every at-bat.",
            f"{name} had real finish to his stuff and was clearly the one in control.",
            f"{name} looked like the best version of himself and never really let the {opp_name} breathe.",
        ],
        "QUALITY": [
            f"{name} gave the {team_name} a strong {ip_text} and kept the damage to a minimum.",
            f"{name} turned in a quality outing and kept the game from getting away in any one inning.",
            f"{name} gave the {team_name} exactly the kind of stable start it needed.",
            f"{name} did not dominate from start to finish, but he gave the {team_name} a steady outing overall.",
        ],
        "STRIKEOUT": [
            f"{name} missed bats all night, even if the outing was not completely clean.",
            f"{name} leaned on putaway stuff and kept finding strikeouts when he needed them.",
            f"{name} had enough swing-and-miss to overpower stretches of the {opp_name} lineup.",
            f"{name} brought real bat-missing tonight, and that was the biggest story of the outing.",
        ],
        "SHARP": [
            f"{name} looked sharp and kept most of the night under control.",
            f"{name} gave a crisp outing and rarely let the {opp_name} get anything going.",
            f"{name} was steady from the outset and did not give hitters many clean openings.",
            f"{name} kept the pressure light for most of the night and never looked rushed.",
        ],
        "SOLID": [
            f"{name} gave the {team_name} a useful start and kept things steady while he was on the mound.",
            f"{name} turned in a steady outing and did the job without much extra drama.",
            f"{name} was not overpowering, but he gave the {team_name} the kind of start it could work with.",
            f"{name} kept this thing under control well enough to hand over a playable game.",
        ],
        "UNEVEN": [
            f"{name} got through {ip_text}, working around traffic in multiple innings along the way.",
            f"{name} covered enough ground, but there was pressure on the line for much of the night.",
            f"{name} never looked fully comfortable, even though he kept the outing from breaking apart.",
            f"{name} had to grind through this one more than the final line might suggest.",
        ],
        "SHORT": [
            f"{name} did not last as long as his team needed, getting through only {ip_text}.",
            f"{name} was out earlier than expected, and the outing never found much rhythm.",
            f"{name} came up short on length, which changed the shape of the game pretty quickly.",
            f"{name} could not give the {team_name} enough innings, even if the damage stayed somewhat limited.",
        ],
        "ROUGH": [
            f"{name} had a rough night and could not stop the game from leaning the wrong way.",
            f"{name} never really got settled, and the outing kept getting heavier on him.",
            f"{name} was chasing the start more than controlling it, and that showed in the final line.",
            f"{name} ran into trouble early and never found the reset he needed.",
        ],
        "NO_COMMAND": [
            f"{name} spent too much of the night fighting the zone, which kept every inning from calming down.",
            f"{name} did not find enough strikes, and the extra traffic kept pushing the line around.",
            f"{name} was working from behind too often, and it left him with very little margin.",
            f"{name} never got ahead consistently enough to let the outing settle into a rhythm.",
        ],
        "HIT_HARD": [
            f"{name} got hit hard, and there were too many pitches left in damage spots.",
            f"{name} did not have much margin, and too much hittable stuff got punished.",
            f"{name} was around the zone plenty, but too many balls were squared up.",
            f"{name} paid for too many mistakes in hittable areas, and the lineup made him wear it.",
        ],
    }
    choices = variants.get(label, variants["SOLID"])
    return choices[seed % len(choices)]


def build_starter_stat_sentence(stats: dict, seed: int) -> str:
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    hit_text = stat_phrase(h, "hit")
    er_text = stat_phrase(er, "earned run")
    bb_text = stat_phrase(bb, "walk")
    k_text = stat_phrase(k, "strikeout")
    choices = [
        f"He allowed {hit_text} and {er_text}, issued {bb_text}, and finished with {k_text}.",
        f"The final line was {hit_text}, {er_text}, {bb_text}, and {k_text}.",
        f"He finished with {k_text} while allowing {hit_text} and {er_text}, along with {bb_text}.",
        f"The box score closed with {hit_text}, {er_text}, {bb_text}, and {k_text}.",
        f"He gave up {hit_text} and {er_text}, with {bb_text} against {k_text}.",
    ]
    return choices[(seed // 3) % len(choices)]


def build_starter_positive_sentence(stats: dict, label: str, seed: int) -> str:
    if label not in BAD_STARTER_LABELS:
        return ""
    h = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    positives = []
    if k >= 7:
        positives.extend([
            f"One thing he did have was swing-and-miss, still punching out {number_word(k)} despite the rest of the line.",
            f"There was at least one real positive in the bat-missing, as he still struck out {number_word(k)}.",
        ])
    if bb <= 1 and (h >= 5 or er >= 4):
        positives.extend([
            f"The command was not the main problem, with only {stat_phrase(bb, 'walk')} and far more damage coming on contact.",
            f"He only issued {stat_phrase(bb, 'walk')}, so this was more about the balls in play than scattered command.",
        ])
    if er <= 2 and k >= 5 and label == "SHORT":
        positives.append("There was at least some bat-missing underneath the short outing.")
    if not positives:
        return ""
    return positives[(seed // 13) % len(positives)]


def build_starter_pressure_sentence(stats: dict, label: str, seed: int, opp_name: str = "opponent") -> str:
    h = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    traffic = h + bb
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))

    if label in GOOD_STARTER_LABELS:
        choices = [
            "When hitters did get on, he usually found a way to keep the inning from turning ugly.",
            "The few threats against him never had time to turn into a big inning.",
            "He worked around traffic in multiple innings and kept the bigger inning off the board.",
            f"Even when the {opp_name} pushed a little, he was usually the one who got the last word.",
            f"There were not many clean looks for the {opp_name}, and that kept the pressure light for most of the night.",
            "Most of the hits and baserunners stayed scattered, which kept the game from swinging against him.",
        ]
        if k >= 8:
            choices.append("When things tightened up, he still had enough putaway stuff to end the threat himself.")
        elif traffic <= 4:
            choices.append("There were very few real openings for the lineup, which helped the outing stay under control from start to finish.")
    elif label in BAD_STARTER_LABELS:
        if outs < 3:
            choices = [
                "The trouble was there before he had any chance to settle into the outing.",
                "There was no real reset once the first wave of damage started.",
                "It got loud almost immediately, and the start never recovered from that first hit of trouble.",
            ]
        else:
            choices = [
                "He never found the clean inning that might have slowed the game down.",
                "Too many hitters kept reaching, which left him with almost no room to work.",
                "Once the baserunners started piling up, the outing became much tougher to control.",
                "He spent too much of the night pitching under stress, and it finally caught up with him.",
                "There were too many leverage pitches for this to ever feel stable.",
            ]
        if bb >= 4:
            choices.append("He kept falling behind in counts, and that made every baserunner feel bigger.")
        elif h >= 6 and bb <= 1:
            choices.append("This was more about too much hittable contact than scattered command.")
        elif traffic >= 10:
            choices.append("The baserunners kept coming, and he never really found a clean stretch.")
    else:
        choices = [
            "There were enough baserunners to keep the outing from ever feeling comfortable.",
            "He had to work through a few jams, which gave the line more stress than the runs alone suggest.",
            "The outing held together, but there were still a couple moments where he had to work for the escape.",
            "He was not cruising, but he did enough in the tougher spots to keep the line usable.",
            "It was more workmanlike than easy, though he still kept the game from getting away on him.",
        ]
        if traffic >= 7:
            choices.append("He did well to keep the damage from getting bigger, because there were enough runners for this to get messy.")
    return choices[(seed // 5) % len(choices)]


def build_starter_team_context(p: dict, stats: dict, label: str, game_context: dict, seed: int) -> str:
    away_score = safe_int(game_context.get("away_score", 0), 0)
    home_score = safe_int(game_context.get("home_score", 0), 0)
    team_name = team_name_from_abbr(p.get("team"))
    if p.get("side") == "away":
        team_runs = away_score
        opp_runs = home_score
        opp_name = team_name_from_abbr(game_context.get("home_abbr"))
    else:
        team_runs = home_score
        opp_runs = away_score
        opp_name = team_name_from_abbr(game_context.get("away_abbr"))

    win_decision = safe_int(stats.get("wins", 0), 0) > 0
    won = team_runs > opp_runs
    margin = abs(team_runs - opp_runs)

    if won and label in POSITIVE_STARTER_LABELS:
        choices = [
            f"Those innings let the {team_name} stay in front instead of scrambling to catch up.",
            f"He kept the {team_name} in control for most of the night.",
            f"He left with the {team_name} still in front and in position to close it out.",
            f"He made sure the {opp_name} never really got the game swinging their way.",
            f"The {team_name} were still in a good spot once he turned it over late.",
        ]
        if win_decision:
            choices.append("He wound up with the win, and the outing put him in line for it from the start.")
        elif team_runs <= 2:
            choices.append(f"He did it without much offensive cushion, which made every cleaner inning bigger for the {team_name}.")
        elif margin >= 4:
            choices.append(f"Once the {team_name} built some room, he kept the {opp_name} from putting together the inning that could change it.")
    elif won and label in SUBPAR_STARTER_LABELS:
        choices = [
            f"The {team_name} scored enough to move past it, even if the start itself stayed shaky.",
            "The bats covered for it, though the outing itself was bumpier than the final margin suggests.",
            f"He still left the {team_name} needing more cleanup than they would have liked.",
            "The final result worked out, but the outing itself was rougher than the scoreboard alone implies.",
        ]
    elif (not won) and label in POSITIVE_STARTER_LABELS:
        choices = [
            f"He kept the {team_name} within reach, but the support around the outing never quite matched it.",
            f"The line was good enough to keep the {team_name} hanging around, even if the result went the other way.",
            f"He gave the {team_name} a chance, even if the rest of the game never quite tilted back toward them.",
            f"He did enough to keep the {opp_name} from running away with it early.",
            "It was the kind of start that usually keeps a team alive deep into the game.",
        ]
    else:
        choices = [
            f"Once the damage landed, the {team_name} spent the rest of the night trying to claw back.",
            f"The early hole left the {team_name} playing uphill for the rest of the game.",
            f"From there, the {team_name} were chasing more than controlling things.",
            f"It gave the {opp_name} too much room to play from ahead.",
        ]

    if margin >= 5 and won and label in SUBPAR_STARTER_LABELS:
        choices.append("The final margin looked comfortable, but the start itself was shakier than that score suggests.")
    elif margin == 1 and won and label in POSITIVE_STARTER_LABELS:
        choices.append("In a tight game, those innings carried more weight than they might look on paper.")
    return choices[(seed // 7) % len(choices)]


def build_starter_game_flow_sentence(p: dict, label: str, seed: int, opp_name: str = "opponent") -> str:
    flow = p.get("game_flow") or {}
    scoreless_to_start = safe_int(flow.get("scoreless_to_start", 0), 0)
    opp_runs_while_in = safe_int(flow.get("opp_runs_while_in", 0), 0)
    team_runs_while_in = safe_int(flow.get("team_runs_while_in", 0), 0)
    exit_margin = safe_int(flow.get("exit_margin", 0), 0)
    biggest_inning_runs = safe_int(flow.get("biggest_inning_runs", 0), 0)
    team_name = team_name_from_abbr(p.get("team"))

    if flow.get("settled_after_rough"):
        choices = [
            f"After a shaky opening frame, he settled down and gave the {team_name} several quieter innings behind it.",
            "He wobbled early, then found a much better rhythm once he got deeper into the start.",
            "The beginning was messy, but he recovered well enough to keep the outing from spiraling.",
        ]
        return choices[(seed // 23) % len(choices)]

    if flow.get("only_damage_in_one_inning") and opp_runs_while_in > 0:
        choices = [
            "Most of the damage came in one inning, and the rest of the outing was much steadier.",
            "Almost all of the trouble came in one stretch, and he was steadier outside of that pocket.",
            "One rough inning did most of the damage against him, but the rest of the night was much calmer.",
        ]
        return choices[(seed // 23) % len(choices)]

    if flow.get("late_damage") and label in POSITIVE_STARTER_LABELS:
        choices = [
            f"He kept the {opp_name} quiet for most of the night and did not see real damage until late.",
            "He looked in control through the middle innings before the only real trouble arrived near the end.",
            f"He was rolling for a while before the {opp_name} finally scratched out something late.",
        ]
        return choices[(seed // 23) % len(choices)]

    if scoreless_to_start >= 4:
        choices = [
            f"He opened with {number_word(scoreless_to_start)} straight scoreless innings and set a strong tone right away.",
            f"He stacked {number_word(scoreless_to_start)} quiet innings to begin the night before anything changed.",
            f"He gave the opposition very little early, opening with {number_word(scoreless_to_start)} scoreless frames before anything changed.",
        ]
        return choices[(seed // 23) % len(choices)]

    if team_runs_while_in >= 4 and exit_margin > 0 and label in POSITIVE_STARTER_LABELS:
        choices = [
            f"The {team_name} gave him room to work, and he mostly kept the game tilted in their direction.",
            "With run support behind him, he stayed on the attack instead of pitching from behind.",
            f"The {team_name} scored enough while he was in there to let him pitch with a little more freedom.",
        ]
        return choices[(seed // 23) % len(choices)]

    if exit_margin == 0 and label in POSITIVE_STARTER_LABELS:
        choices = [
            "He left with the game still within reach for the bullpen.",
            "When he left, the game was still very much in the balance.",
            "He kept the game close enough to give the bullpen a chance once he exited.",
        ]
        return choices[(seed // 23) % len(choices)]

    if exit_margin > 0 and label in POSITIVE_STARTER_LABELS:
        choices = [
            f"By the time he left, the {team_name} were still in a good spot to finish the job.",
            "He turned it over with the game still under control for the bullpen.",
            f"He exited with the {team_name} still out in front.",
        ]
        return choices[(seed // 23) % len(choices)]

    if biggest_inning_runs >= 2 and label in BAD_STARTER_LABELS | {"UNEVEN"}:
        choices = [
            "One crooked inning changed the feel of the whole start, and he never fully got back on top of it.",
            "A multi-run inning did most of the damage, and it left him chasing the line after that.",
            "The start took a hard turn once one inning got away from him.",
        ]
        return choices[(seed // 23) % len(choices)]

    return ""


def build_starter_velocity_sentence(p: dict, label: str, seed: int, recent_appearances=None) -> str:
    velo = p.get("avg_fastball_velocity")
    if velo in (None, ""):
        return ""
    try:
        current_v = float(velo)
    except Exception:
        return ""

    prev_v = None
    if recent_appearances:
        for app in recent_appearances:
            app_v = app.get("avg_fastball_velocity")
            if app_v in (None, ""):
                continue
            try:
                prev_v = float(app_v)
                break
            except Exception:
                continue

    drop = (prev_v - current_v) if prev_v is not None else None
    mention_drop = drop is not None and drop >= 1.0
    if current_v < 95.0 and not mention_drop:
        return ""

    velo_text = f"{current_v:.1f} mph"
    if mention_drop:
        drop_text = f"{drop:.1f} mph"
        choices = [
            f"His fastball averaged {velo_text}, down {drop_text} from his previous outing, so the dip stood out right away.",
            f"The heater checked in at {velo_text}, which was {drop_text} lighter than his last start and worth keeping an eye on.",
        ]
        return choices[(seed // 11) % len(choices)]

    bb = safe_int(p.get("stats", {}).get("baseOnBalls", 0), 0)
    hits = safe_int(p.get("stats", {}).get("hits", 0), 0)
    if label in GOOD_STARTER_LABELS:
        choices = [
            f"His fastball averaged {velo_text}, and the life on it showed up whenever he needed to finish a count.",
            f"The fastball sat {velo_text}, giving him enough finish to stay aggressive when he got ahead.",
        ]
    elif is_bad_starter_label(label):
        if bb <= 1 and hits >= 5:
            choices = [
                f"He still averaged {velo_text} on the fastball, so this was more about the contact against him than a lack of arm strength.",
                f"The heater checked in at {velo_text}, and the trouble came more from the quality of contact than from missing with the fastball.",
            ]
        else:
            choices = [
                f"He still averaged {velo_text} on the fastball, but the outing never came together around it.",
                f"The heater sat {velo_text}, though the raw velocity never translated into a cleaner line.",
            ]
    else:
        choices = [
            f"His fastball averaged {velo_text}, which gave the outing some carry even when the line was not fully clean.",
            f"The heater sat {velo_text}, and there were stretches where that helped steady him.",
        ]
    return choices[(seed // 11) % len(choices)]


def build_starter_csw_sentence(p: dict, label: str, seed: int) -> str:
    csw = p.get("csw_percent")
    if csw in (None, ""):
        return ""
    try:
        csw_val = float(csw)
    except Exception:
        return ""

    whiffs = safe_int(p.get("whiffs", 0), 0)
    csw_text = format_percent_text(csw_val)
    good = csw_val >= 31.0
    elite = csw_val >= 35.0
    poor = csw_val <= 24.0

    if label in GOOD_STARTER_LABELS and not good:
        return ""
    if label in SUBPAR_STARTER_LABELS and not poor:
        return ""
    if label == "SOLID" and not (elite or poor):
        return ""

    if good:
        choices = [
            f"He kept hitters on the defensive, posting a CSW of {csw_text} and winning plenty of the key counts.",
            f"The bat-missing and called strikes both showed up, as he finished with a CSW of {csw_text}.",
        ]
        if whiffs >= 10:
            choices.extend([
                f"He generated {number_word(whiffs)} swings and misses to post a CSW of {csw_text}, which helped him finish innings once he got ahead.",
                f"Generating {number_word(whiffs)} swings and misses, he turned that into a CSW of {csw_text} and kept hitters reacting to him.",
            ])
        if elite:
            choices.append(
                f"He was ahead of hitters most of the way, and the CSW of {csw_text} tells the story of how much he controlled counts."
            )
        return choices[(seed // 17) % len(choices)]

    choices = [
        f"He did not miss enough bats or steal enough called strikes, finishing with a CSW of {csw_text}.",
        f"The outing lacked much count leverage, and the CSW of {csw_text} reflects how little swing-and-miss he found.",
    ]
    if whiffs >= 8:
        choices.append(
            f"Even with {number_word(whiffs)} swings and misses, he only managed a CSW of {csw_text}, so too many pitches still led to contact or hitter-friendly counts."
        )
    return choices[(seed // 17) % len(choices)]


def build_starter_pitch_count_sentence(p: dict, label: str, seed: int) -> str:
    pitches = safe_int(p.get("pitch_count", 0), 0)
    strikes = safe_int(p.get("strikes", 0), 0)
    if pitches <= 0:
        return ""

    stats = p.get("stats", {})
    ip_raw = str(stats.get("inningsPitched", "0.0"))
    ip = safe_float(ip_raw, 0.0)
    outs = baseball_ip_to_outs(ip_raw)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    strike_pct = (strikes / pitches * 100.0) if pitches and strikes else 0.0

    choices = []

    if ip >= 6.0 and pitches <= 85:
        choices.extend([
            f"He stayed efficient, needing only {pitches} pitches to do his work.",
            f"It was a clean workload, with just {pitches} pitches needed to get the job done.",
        ])

    if ip < 5.0 and pitches >= 85:
        choices.extend([
            f"The pitch count got heavy early, and {pitches} pitches were all he could squeeze out of the outing.",
            f"He needed {pitches} pitches just to get that far, which helps explain why the start ended so soon.",
        ])

    if outs < 9 and pitches >= 45:
        choices.extend([
            f"He burned through {pitches} pitches in a short stint, leaving almost no room for the game to settle down.",
            f"It took {pitches} pitches to record just {number_word(outs)} outs, so the outing never gave him much breathing room.",
        ])

    if strikes > 0 and strike_pct >= 69.0 and bb <= 1 and label in POSITIVE_STARTER_LABELS:
        choices.extend([
            f"He was in the zone most of the night, landing {strikes} of {pitches} pitches for strikes.",
            f"He kept the count moving by throwing {strikes} of {pitches} pitches for strikes.",
        ])

    if strikes > 0 and strike_pct <= 58.0 and (bb >= 3 or label in {"NO_COMMAND", "ROUGH", "HIT_HARD"}):
        choices.extend([
            f"He only landed {strikes} of {pitches} pitches for strikes, and too many counts drifted the wrong way.",
            f"Just {strikes} of {pitches} pitches went for strikes, which left him pitching from behind too often.",
        ])

    if strikes > 0 and bb >= 4 and pitches >= 70:
        choices.append(
            f"He still threw {strikes} of {pitches} pitches for strikes, but the misses that mattered stretched too many at-bats out."
        )

    if strikes > 0 and hits >= 7 and bb <= 1 and er >= 3:
        choices.append(f"He threw {strikes} of {pitches} pitches for strikes, but too much of that contact was loud.")

    if not choices:
        return ""
    return choices[(seed // 19) % len(choices)]


def build_starter_pitch_mix_sentence(p: dict, label: str, seed: int) -> str:
    """Describe the dominant pitch type if the pitcher leaned heavily on one offering."""
    counts = p.get("pitch_type_counts", {})
    if not counts:
        return ""

    total = sum(counts.values())
    if total < 30:
        return ""

    PITCH_NAMES = {
        "FF": "four-seam fastball", "FT": "two-seam fastball", "SI": "sinker",
        "FC": "cutter", "FA": "fastball", "FS": "split-finger",
        "SL": "slider", "ST": "sweeper", "CU": "curveball", "KC": "knuckle curve",
        "CH": "changeup", "CS": "slow curve", "EP": "eephus",
        "KN": "knuckleball", "SC": "screwball",
    }

    sorted_pitches = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    top_code, top_count = sorted_pitches[0]
    top_pct = top_count / total

    if top_pct < 0.40 and not (
        len(sorted_pitches) >= 2 and
        (sorted_pitches[0][1] + sorted_pitches[1][1]) / total >= 0.75
    ):
        return ""

    pitch_name = PITCH_NAMES.get(top_code, top_code.lower())
    pct_text = f"{int(round(top_pct * 100))} percent"
    is_fastball = top_code in FASTBALL_PITCH_CODES

    if label in GOOD_STARTER_LABELS:
        if not is_fastball:
            choices = [
                f"He leaned heavily on his {pitch_name}, throwing it {pct_text} of the time, and hitters had no real answer for it.",
                f"The {pitch_name} was the go-to weapon tonight, accounting for {pct_text} of his pitches and doing most of the damage.",
            ]
        else:
            choices = [
                f"He went to his {pitch_name} {pct_text} of the time and got consistent results from it all night.",
                f"The {pitch_name} was his primary weapon, making up {pct_text} of the pitch mix and giving hitters fits.",
            ]
    elif label in BAD_STARTER_LABELS:
        if is_fastball:
            choices = [
                f"He leaned on his {pitch_name} {pct_text} of the time, but it was not enough to slow the lineup down.",
                f"The {pitch_name} was his most used pitch at {pct_text}, though the results were not there tonight.",
            ]
        else:
            choices = [
                f"He went to his {pitch_name} {pct_text} of the time, but hitters got comfortable with it as the night went on.",
                f"The {pitch_name} made up {pct_text} of his arsenal tonight, and it did not have the same impact it usually does.",
            ]
    else:
        choices = [
            f"He featured his {pitch_name} heavily, going to it {pct_text} of the time throughout the outing.",
            f"The {pitch_name} was his most-used offering at {pct_text}, and it kept the lineup guessing through most of the start.",
        ]

    return choices[(seed // 29) % len(choices)]


def build_starter_season_context_sentence(p: dict, label: str, seed: int) -> str:
    """Flag when a game line is notably above or below the pitcher's season norm."""
    stats = p.get("stats", {})
    season_stats = p.get("season_stats", {})
    flags = detect_season_bests(stats, season_stats)

    if not flags:
        return ""

    k_game = safe_int(stats.get("strikeOuts", 0), 0)
    choices = []

    if flags.get("k_best") and label in GOOD_STARTER_LABELS:
        choices.extend([
            f"Punching out {number_word(k_game)} is a new season high, and tonight showed what his stuff can do when everything is working.",
            f"That strikeout total was a season best, and it was not a fluke — he had real late movement all night.",
        ])

    if flags.get("k_high_vs_avg") and not flags.get("k_best") and label in GOOD_STARTER_LABELS:
        choices.extend([
            f"The {number_word(k_game)} strikeouts were well above his season average, which made this one of his better showings.",
            f"He punched out {number_word(k_game)}, noticeably more than he typically does, and the swing-and-miss was real.",
        ])

    if flags.get("clean_sheet") and label in {"GEM", "DOMINANT", "QUALITY", "SHARP"}:
        choices.extend([
            "Keeping the board clean while going deep into the game is not something that happens every time out, and he earned this one.",
            "The shutout innings are hard to come by, and he made it look easier than it is.",
        ])

    if flags.get("er_worst") and label in BAD_STARTER_LABELS:
        choices.extend([
            "The earned run total was one of the higher marks of his season, which puts this one in the rougher outings column.",
            "This was one of the harder nights on the ERA, and the damage was real compared to how he has usually pitched.",
        ])

    if not choices:
        return ""
    return choices[(seed // 31) % len(choices)]


def build_starter_no_decision_sentence(p: dict, label: str, seed: int) -> str:
    """Surface when a quality start ends without a win decision."""
    stats = p.get("stats", {})
    wins = safe_int(stats.get("wins", 0), 0)
    losses_stat = safe_int(stats.get("losses", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    ip = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    k = safe_int(stats.get("strikeOuts", 0), 0)

    if wins > 0 or losses_stat > 0:
        return ""
    if label not in GOOD_STARTER_LABELS:
        return ""
    if ip < 5.0:
        return ""

    choices = [
        "He walked away without a decision, which does not tell the full story of what he did out there.",
        "The no-decision does not do this outing justice — he gave them everything they needed to win.",
        "He left with nothing to show for it in the decision column, even though the start was exactly what you ask for.",
        "No decision for him tonight, even after putting up a line like that.",
    ]

    if er == 0:
        choices.extend([
            "He kept the board clean and still did not get the win — that is a tough ledger entry.",
            "Zeroes on the board and nothing in the decision column is a rough way to end an outing like that.",
        ])
    elif k >= 8:
        choices.extend([
            f"Punching out {number_word(k)} and walking away without a decision is not the reward that kind of stuff deserves.",
            f"He missed {number_word(k)} bats and still left empty-handed — that is how the game goes sometimes.",
        ])

    return choices[(seed // 37) % len(choices)]


def build_starter_opp_quality_sentence(p: dict, label: str, seed: int, opp_hitting: dict | None) -> str:
    """Add context about the opposing lineup's season OPS tier."""
    if not opp_hitting:
        return ""

    ops = opp_hitting.get("ops", 0.0)
    tier = classify_offense(ops)

    # Only surface for clearly good or clearly bad matchups — skip average
    if tier == "average":
        return ""

    if label in GOOD_STARTER_LABELS:
        if tier == "elite":
            choices = [
                "Doing it against one of the better offenses in the league makes this one stand out even more.",
                "That lineup has been one of the tougher ones to navigate this season, which adds some weight to what he did tonight.",
                "He held down an offense that has been among the league's best, and that does not happen without good stuff.",
            ]
        elif tier == "above_average":
            choices = [
                "The lineup he faced has been above average this season, so this was not a soft matchup.",
                "He was not working against a pushover offense, and the line still held up.",
            ]
        elif tier in ("below_average", "weak"):
            choices = [
                "The lineup he faced has been one of the weaker offenses this season, which gives some context to the numbers.",
                "He was working against a struggling offense, which is worth keeping in mind.",
            ]
        else:
            return ""
    elif label in BAD_STARTER_LABELS:
        if tier == "elite":
            choices = [
                "That lineup has been one of the best in the league this season, so some of the damage was coming regardless.",
                "He ran into one of the most dangerous offenses in baseball, which explains some of the rough line.",
            ]
        elif tier == "above_average":
            choices = [
                "The offense he faced has been above average this year, so it was not an easy night even before the trouble started.",
            ]
        elif tier in ("below_average", "weak"):
            choices = [
                "The lineup he gave it up to has been one of the weaker ones in the league, which makes the damage harder to explain away.",
                "That offense has struggled this season, which gives this outing less of an excuse to lean on.",
            ]
        else:
            return ""
    else:
        return ""

    return choices[(seed // 41) % len(choices)]


# ---------------- SUBJECT LINE ----------------

def build_starter_subject_line(p: dict, label: str, game_context: dict, seed: int) -> str:
    stats = p.get("stats", {})
    name = p.get("name", "This starter")
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    wins = safe_int(stats.get("wins", 0), 0)
    losses_stat = safe_int(stats.get("losses", 0), 0)
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))
    ip_text = format_starter_ip_for_summary(str(stats.get("inningsPitched", "0.0")))
    flow = p.get("game_flow") or {}
    scoreless_to_start = safe_int(flow.get("scoreless_to_start", 0), 0)
    only_damage_in_one_inning = bool(flow.get("only_damage_in_one_inning"))
    team_runs = game_context.get("home_score", 0) if p.get("side") == "home" else game_context.get("away_score", 0)
    opp_runs = game_context.get("away_score", 0) if p.get("side") == "home" else game_context.get("home_score", 0)
    team_name = team_name_from_abbr(p.get("team"))

    no_decision = (
        wins == 0 and losses_stat == 0 and
        label in GOOD_STARTER_LABELS and
        safe_float(stats.get("inningsPitched", "0.0"), 0.0) >= 5.0
    )

    if outs < 3:
        choices = [
            f"⚠️ {name} is knocked out in the opening inning",
            f"⚠️ {name} exits before he can settle in",
            f"⚠️ {name} never gets the start off the ground",
        ]
    elif label == "GEM":
        choices = [
            f"🔥 {name} controls the game from the jump",
            f"🔥 {name} cruises through a gem",
            f"🔥 {name} barely gives the lineup a pulse",
            f"🔥 {name} puts a lid on the lineup all night",
        ]
    elif label == "DOMINANT":
        choices = [
            f"🔥 {name} dominates over {ip_text}",
            f"🔥 {name} powers through a dominant start",
            f"🔥 {name} overmatches hitters all night",
            f"🔥 {name} takes over with swing-and-miss stuff",
        ]
        if k >= 8:
            choices.extend([
                f"🔥 {name} piles up strikeouts in a dominant start",
                f"🔥 {name} works deep while racking up {number_word(k)} strikeouts",
            ])
    elif label == "STRIKEOUT":
        choices = [
            f"🎯 {name} punches out {number_word(k)} in a power outing",
            f"🎯 {name} misses bats all night in a high-whiff start",
            f"🎯 {name} leans on swing-and-miss to carry the outing",
            f"🎯 {name} rides the strikeouts through a lively start",
        ]
        if bb >= 3:
            choices.append(f"🎯 {name} piles up strikeouts despite the extra traffic")
        if no_decision:
            choices.append(f"🎯 {name} punches out {number_word(k)} and walks away empty-handed")
    elif label in {"QUALITY", "SHARP"}:
        choices = [
            f"✅ {name} turns in a strong night on the mound",
            f"✅ {name} gives the {team_name} a steady start",
            f"✅ {name} keeps the game under control",
            f"✅ {name} holds the line with a clean effort",
        ]
        if k >= 7:
            choices.append(f"✅ {name} pairs length with {number_word(k)} strikeouts")
        elif er == 0:
            choices.append(f"✅ {name} keeps the board clean through {ip_text}")
        elif scoreless_to_start >= 4:
            choices.append(f"✅ {name} opens with {number_word(scoreless_to_start)} scoreless innings and never loses the feel for it")
        if no_decision:
            choices.append(f"✅ {name} deals a strong start but leaves empty-handed")
    elif label == "SOLID":
        choices = [
            f"📈 {name} gives the {team_name} a useful start",
            f"📈 {name} steadies things on the mound",
            f"📈 {name} turns in a workmanlike outing",
            f"📈 {name} does enough to keep the game in order",
        ]
        if k >= 7:
            choices.append(f"📈 {name} adds punchouts to a solid night")
        elif only_damage_in_one_inning and er <= 3:
            choices.append(f"📈 {name} keeps things afloat outside of one rough patch")
        if no_decision:
            choices.append(f"📈 {name} goes deep and gets nothing to show for it")
    elif label == "HIT_HARD":
        choices = [
            f"💥 {name} gets hit hard despite some swing-and-miss",
            f"💥 {name} pays for too many hittable pitches",
            f"💥 {name} cannot escape the damage contact",
            f"💥 {name} loses too many balls over the heart of the plate",
        ]
        if k >= 7:
            choices.append(f"💥 {name} misses bats but gets punished on contact")
    elif label == "NO_COMMAND":
        choices = [
            f"🧭 {name} never gets the count working for him",
            f"🧭 {name} fights the zone all night",
            f"🧭 {name} cannot get comfortable in the strike zone",
        ]
    elif label == "UNEVEN":
        choices = [
            f"📉 {name} grinds through an uneven outing",
            f"📉 {name} battles traffic all night",
            f"📉 {name} gets through it, but not cleanly",
            f"📉 {name} never quite finds an easy inning",
        ]
    else:
        choices = [
            f"⚠️ {name} cannot keep the outing from getting away",
            f"⚠️ {name} runs into trouble and does not recover",
            f"⚠️ {name} never finds his footing on the mound",
        ]
        if hits >= 7 and bb <= 1:
            choices.append(f"⚠️ {name} gets tagged even without many walks")
        elif bb >= 4:
            choices.append(f"⚠️ {name} falls behind too often to settle in")
        elif er >= 4:
            choices.append(f"⚠️ {name} cannot stop the damage from building")

    if team_runs > opp_runs and label in POSITIVE_STARTER_LABELS:
        choices.append(f"🏁 {name} helps set up a winning night for the {team_name}")
    elif team_runs < opp_runs and label in BAD_STARTER_LABELS | {"UNEVEN"}:
        choices.append(f"📉 {name} leaves the {team_name} chasing the game")

    subject = choices[seed % len(choices)].strip().rstrip(".!?:;,-")
    return subject.replace("...", "").strip()


# ---------------- SUMMARY ASSEMBLY ----------------

def build_starter_summary(
    p: dict,
    label: str,
    game_context: dict,
    recent_appearances=None,
    opp_hitting: dict | None = None,
) -> str:
    stats = p["stats"]
    seed = build_starter_summary_seed(p["name"], stats, game_context)
    opp_name = (
        team_name_from_abbr(game_context.get("home_abbr"))
        if p.get("side") == "away"
        else team_name_from_abbr(game_context.get("away_abbr"))
    )

    overview           = build_starter_overview(p["name"], label, stats, seed, team_name=team_name_from_abbr(p.get("team")), opp_name=opp_name)
    stat_sentence      = build_starter_stat_sentence(stats, seed)
    pressure_sentence  = build_starter_pressure_sentence(stats, label, seed, opp_name=opp_name)
    team_sentence      = build_starter_team_context(p, stats, label, game_context, seed)
    flow_sentence      = build_starter_game_flow_sentence(p, label, seed, opp_name=opp_name)
    velocity_sentence  = build_starter_velocity_sentence(p, label, seed, recent_appearances=recent_appearances)
    csw_sentence       = build_starter_csw_sentence(p, label, seed)
    pitch_sentence     = build_starter_pitch_count_sentence(p, label, seed)
    positive_sentence  = build_starter_positive_sentence(stats, label, seed)
    pitch_mix_sentence = build_starter_pitch_mix_sentence(p, label, seed)
    season_sentence    = build_starter_season_context_sentence(p, label, seed)
    nd_sentence        = build_starter_no_decision_sentence(p, label, seed)
    opp_sentence       = build_starter_opp_quality_sentence(p, label, seed, opp_hitting)

    if is_bad_starter_label(label):
        order_options = [
            [overview, flow_sentence, stat_sentence, positive_sentence, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence, csw_sentence, opp_sentence, pitch_mix_sentence],
            [overview, pressure_sentence, stat_sentence, positive_sentence, flow_sentence, pitch_sentence, team_sentence, velocity_sentence, csw_sentence, opp_sentence, season_sentence],
            [overview, stat_sentence, flow_sentence, positive_sentence, csw_sentence, pitch_sentence, team_sentence, velocity_sentence, opp_sentence, pitch_mix_sentence],
            [overview, stat_sentence, pitch_sentence, flow_sentence, pressure_sentence, positive_sentence, team_sentence, velocity_sentence, opp_sentence],
        ]
    elif label == "STRIKEOUT":
        order_options = [
            [overview, csw_sentence, stat_sentence, flow_sentence, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence, season_sentence, nd_sentence, opp_sentence],
            [overview, flow_sentence, csw_sentence, stat_sentence, team_sentence, pitch_sentence, velocity_sentence, pressure_sentence, season_sentence, opp_sentence],
            [overview, stat_sentence, csw_sentence, flow_sentence, team_sentence, velocity_sentence, pressure_sentence, pitch_sentence, nd_sentence, opp_sentence],
        ]
    elif label in {"GEM", "DOMINANT"}:
        order_options = [
            [overview, flow_sentence, csw_sentence, pressure_sentence, stat_sentence, team_sentence, velocity_sentence, pitch_sentence, season_sentence, nd_sentence, opp_sentence],
            [overview, pressure_sentence, stat_sentence, flow_sentence, csw_sentence, team_sentence, pitch_sentence, velocity_sentence, season_sentence, opp_sentence],
            [overview, csw_sentence, stat_sentence, flow_sentence, team_sentence, pressure_sentence, velocity_sentence, pitch_sentence, nd_sentence, opp_sentence],
        ]
    else:
        order_options = [
            [overview, flow_sentence, stat_sentence, pressure_sentence, team_sentence, pitch_sentence, csw_sentence, velocity_sentence, season_sentence, nd_sentence, opp_sentence, pitch_mix_sentence],
            [overview, pitch_sentence, flow_sentence, stat_sentence, pressure_sentence, team_sentence, csw_sentence, velocity_sentence, season_sentence, opp_sentence],
            [overview, pressure_sentence, stat_sentence, flow_sentence, csw_sentence, team_sentence, velocity_sentence, pitch_sentence, nd_sentence, opp_sentence, pitch_mix_sentence],
            [overview, stat_sentence, team_sentence, flow_sentence, pressure_sentence, pitch_sentence, velocity_sentence, csw_sentence, season_sentence, opp_sentence],
        ]

    ordered = [s for s in order_options[seed % len(order_options)] if s]
    final_sentences = []
    for sentence in ordered:
        if sentence and sentence not in final_sentences:
            final_sentences.append(sentence)
        if len(final_sentences) >= 4:
            break

    if len(final_sentences) < 3:
        fillers = [
            flow_sentence, stat_sentence, pressure_sentence, team_sentence,
            csw_sentence, pitch_sentence, velocity_sentence, positive_sentence,
            pitch_mix_sentence, season_sentence, nd_sentence, opp_sentence,
        ]
        for sentence in fillers:
            if sentence and sentence not in final_sentences:
                final_sentences.append(sentence)
            if len(final_sentences) >= 4:
                break

    return " ".join(final_sentences[:4])


# ---------------- API FETCHERS ----------------

def get_games():
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)
    games = []

    for d in [today, yesterday]:
        data = fetch_with_retry(f"{SCHEDULE_URL}&date={d.isoformat()}")
        if data is None:
            log(f"Schedule fetch failed for {d}")
            continue
        for date_block in data.get("dates", []):
            games.extend(date_block.get("games", []))

    return games


def get_feed(game_id):
    data = fetch_with_retry(LIVE_URL.format(game_id))
    if data is None:
        log(f"Feed fetch error for game {game_id}: all retries exhausted")
    return data


# ---------------- DISCORD ----------------

async def post_card(
    channel,
    p: dict,
    game_context: dict,
    score_value: str,
    recent_appearances=None,
    opp_hitting: dict | None = None,
):
    stats = p["stats"]
    label = classify_starter(stats)
    seed = build_starter_summary_seed(p["name"], stats, game_context)

    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])
    embed.add_field(name="", value=f"**{build_starter_subject_line(p, label, game_context, seed)}**", inline=False)
    embed.add_field(name="Summary", value=build_starter_summary(p, label, game_context, recent_appearances=recent_appearances, opp_hitting=opp_hitting), inline=False)
    embed.add_field(name="Game Line", value=format_starter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_starter_season_line(p.get("season_stats", {})), inline=False)
    embed.add_field(name="⚾ Score", value=score_value, inline=False)
    await channel.send(embed=embed)


async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    now_et = datetime.now(ET)
    if is_sleep_window_et(now_et):
        initial_sleep_seconds = seconds_until_next_wake_et(now_et)
        wake_time = now_et + timedelta(seconds=initial_sleep_seconds)
        log(
            f"Sleep window active at startup ({SLEEP_START_HOUR_ET}:00-{SLEEP_END_HOUR_ET}:00 ET). "
            f"Sleeping until {wake_time.strftime('%Y-%m-%d %I:%M %p ET')}"
        )
        await asyncio.sleep(initial_sleep_seconds)

    state = load_state()
    posted = set(state.get("posted", []))

    if RESET_STARTER_STATE:
        log("RESET_STARTER_STATE enabled — posted state cleared for this run")
        posted = set()

    while True:
        try:
            games = await asyncio.to_thread(get_games)
            log(f"Checking {len(games)} games")

            for g in games:
                if g.get("status", {}).get("detailedState") != "Final":
                    continue

                game_id = g.get("gamePk")
                if not game_id:
                    continue

                feed = await asyncio.to_thread(get_feed, game_id)
                if feed is None:
                    continue
                starters = get_starters(feed)

                game_teams = feed.get("gameData", {}).get("teams", {})
                away_abbr = (
                    game_teams.get("away", {}).get("abbreviation")
                    or g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation")
                    or "AWAY"
                )
                home_abbr = (
                    game_teams.get("home", {}).get("abbreviation")
                    or g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation")
                    or "HOME"
                )
                away_score = safe_int(g.get("teams", {}).get("away", {}).get("score", 0), 0)
                home_score = safe_int(g.get("teams", {}).get("home", {}).get("score", 0), 0)
                score_value = build_starter_score_display(away_abbr, away_score, home_abbr, home_score)
                game_context = {
                    "away_abbr": away_abbr,
                    "home_abbr": home_abbr,
                    "away_score": away_score,
                    "home_score": home_score,
                    "score_display": score_value,
                }
                game_date_et = parse_game_date_et(g)
                season = game_date_et.year if game_date_et else datetime.now(ET).year

                # Fetch both teams' hitting stats for opponent quality context
                away_hitting = await asyncio.to_thread(
                    get_team_hitting_stats, normalize_team_abbr(away_abbr), season
                )
                home_hitting = await asyncio.to_thread(
                    get_team_hitting_stats, normalize_team_abbr(home_abbr), season
                )

                ordered = sorted(
                    starters,
                    key=lambda p: (
                        -starter_score(p["stats"]),
                        0 if p.get("side") == "away" else 1,
                        p.get("name", ""),
                    ),
                )

                posted_this_game = 0
                for p in ordered:
                    pid = p.get("id")
                    if pid is None:
                        continue

                    key = f"{game_id}_{pid}"
                    if key in posted:
                        continue

                    score = starter_score(p["stats"])
                    if score < MIN_STARTER_SCORE:
                        continue

                    if posted_this_game >= MAX_STARTER_CARDS_PER_GAME:
                        break

                    # Opponent hitting is the other team's stats from this pitcher's perspective
                    opp_hitting = home_hitting if p.get("side") == "away" else away_hitting

                    recent_appearances = await asyncio.to_thread(
                        get_recent_appearances, pid, game_date_et, limit=3, max_days=45
                    )
                    log(f"Posting {p['name']} | {p['team']} | {score_value} | score={score}")
                    await post_card(
                        channel, p, game_context, score_value,
                        recent_appearances=recent_appearances,
                        opp_hitting=opp_hitting,
                    )
                    posted.add(key)
                    posted_this_game += 1

            state["posted"] = list(posted)
            save_state(state)

        except Exception as e:
            log(f"Loop error: {e}")

        now_et = datetime.now(ET)
        if is_sleep_window_et(now_et):
            sleep_seconds = seconds_until_next_wake_et(now_et)
            wake_time = now_et + timedelta(seconds=sleep_seconds)
            log(
                f"Sleep window active ({SLEEP_START_HOUR_ET}:00-{SLEEP_END_HOUR_ET}:00 ET). "
                f"Sleeping until {wake_time.strftime('%Y-%m-%d %I:%M %p ET')}"
            )
        else:
            sleep_seconds = random_awake_sleep_seconds()
            log(f"Awake window active. Next scan in {sleep_seconds // 60} minutes")

        await asyncio.sleep(sleep_seconds)


intents = discord.Intents.default()
client = discord.Client(intents=intents)
background_task = None


@client.event
async def on_ready():
    global background_task
    log(f"Logged in as {client.user}")
    if background_task is None or background_task.done():
        background_task = asyncio.create_task(loop())
        log("Starter background task created")


async def start_starter_bot():
    if not TOKEN:
        raise RuntimeError("ANALYTIC_BOT_TOKEN is not set")
    await client.start(TOKEN, reconnect=True)

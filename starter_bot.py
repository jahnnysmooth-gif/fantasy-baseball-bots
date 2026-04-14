import asyncio
import json
import os
import random
import re
import time
import traceback
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

import anthropic
import discord
import requests
from utils.team_data import TEAM_COLORS, TEAM_NAME_MAP, normalize_team_abbr, get_logo, normalize_lookup_name

# ---------------- CONFIG ----------------

TOKEN = os.getenv("ANALYTIC_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("STARTER_WATCH_CHANNEL_ID", "0"))
ANTHROPIC_API_KEY = os.getenv("STARTER_BOT_SUMMARY", "")
CLAUDE_MODEL = "claude-sonnet-4-6"

STATE_FILE = "state/starter/state.json"
os.makedirs("state/starter", exist_ok=True)

ET = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"
TEAM_STATS_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?stats=season&group=hitting&season={}"
TEAM_ID_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1&season={}"
PROBABLE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={}&hydrate=probablePitchers"
PITCH_ARSENAL_URL = "https://statsapi.mlb.com/api/v1/people/{}/stats?stats=pitchArsenal&season={}&group=pitching"
PLAYER_STATS_URL = "https://statsapi.mlb.com/api/v1/people/{}/stats?stats=career&group=pitching"

API_RETRY_ATTEMPTS = 3
API_RETRY_BACKOFF_SECONDS = 2
OPP_QUALITY_MIN_GAMES = 10

SLEEP_START_HOUR_ET = 3
SLEEP_END_HOUR_ET = 11
AWAKE_POLL_MIN_MINUTES = 6
AWAKE_POLL_MAX_MINUTES = 12
RESET_STARTER_STATE = os.getenv("RESET_STARTER_STATE", "").lower() in {"1", "true", "yes"}

MAX_STARTER_CARDS_PER_GAME = int(os.getenv("STARTER_MAX_CARDS_PER_GAME", "2"))

VELOCITY_MIN_PITCHES = 10
VELOCITY_MIN_FASTBALLS = 3
FASTBALL_PITCH_CODES = {"FF", "FT", "SI", "FC", "FA", "FS"}

def team_name_from_abbr(team: str) -> str:
    normalized = normalize_team_abbr(team)
    return TEAM_NAME_MAP.get(normalized, normalized or "club")


pitching_stats_cache = {}
player_meta_cache = {}
team_hitting_cache = {}   # (team_abbr, season) -> hitting stats dict or None
team_id_cache = {}        # abbr -> mlb team id
next_start_cache = {}     # pitcher_id -> next start info dict or None
pitch_mix_cache = {}      # (pitcher_id, season) -> {pitch_code: pct} or None
career_stats_cache = {}   # pitcher_id -> career IP float or None

ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
player_headshot_index = None

NUMBER_WORDS = {
    0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
    6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten",
    11: "eleven", 12: "twelve",
}


def log(msg: str):
    print(f"[STARTER] {msg}", flush=True)


def log_exception(context: str):
    log(context)
    traceback.print_exc()


MAX_PLAYER_CACHE_SIZE = 1500


def cleanup_starter_caches():
    """Prune stale entries from all starter bot caches."""
    today_et = datetime.now(ET).date()
    current_season = today_et.year

    # Prune date-keyed pitching stats (keep last 30 days)
    cutoff = today_et - timedelta(days=30)
    stale_dates = [d for d in list(pitching_stats_cache) if d < cutoff]
    for d in stale_dates:
        del pitching_stats_cache[d]

    # Prune season-keyed caches (drop prior seasons)
    stale_team = [k for k in list(team_hitting_cache) if k[1] < current_season]
    for k in stale_team:
        del team_hitting_cache[k]

    stale_mix = [k for k in list(pitch_mix_cache) if k[1] < current_season]
    for k in stale_mix:
        del pitch_mix_cache[k]

    # Cap player-id caches to prevent unbounded growth
    if len(player_meta_cache) > MAX_PLAYER_CACHE_SIZE:
        player_meta_cache.clear()
        log("player_meta_cache cleared (size cap reached)")

    if len(career_stats_cache) > MAX_PLAYER_CACHE_SIZE:
        career_stats_cache.clear()
        log("career_stats_cache cleared (size cap reached)")

    total_pruned = len(stale_dates) + len(stale_team) + len(stale_mix)
    if total_pruned:
        log(f"Cache cleanup: pruned {total_pruned} stale entries")


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
                log_exception(f"Request failed after {API_RETRY_ATTEMPTS} attempts: {url} — {e}")
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
            log_exception(f"Failed to set player headshot thumbnail for {name}")

    try:
        embed.set_thumbnail(url=logo_url)
    except Exception:
        log_exception(f"Failed to set fallback team logo thumbnail for {name}")


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
        log_exception(f"Failed to load starter state from {STATE_FILE}")

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
    first_pitch_strikes = 0
    first_pitch_total = 0

    k_by_pitch_code = {}  # pitch code -> number of strikeouts recorded on that pitch

    for play in plays:
        matchup = play.get("matchup", {}) if isinstance(play, dict) else {}
        pitcher = matchup.get("pitcher", {}) if isinstance(matchup, dict) else {}
        if pitcher.get("id") != pitcher_id:
            continue

        play_events = play.get("playEvents", [])
        is_strikeout = str(play.get("result", {}).get("event") or "").strip() == "Strikeout"
        first_pitch_seen = False
        last_pitch_code = ""
        for event in play_events:
            if not isinstance(event, dict) or not event.get("isPitch"):
                continue
            total_pitches += 1
            details = event.get("details", {})
            is_strike = bool(details.get("isStrike"))
            if is_strike:
                strikes += 1
            if is_called_strike_event(event):
                called_strikes += 1
            if is_whiff_event(event):
                whiffs += 1

            # First pitch of the plate appearance
            if not first_pitch_seen:
                first_pitch_seen = True
                first_pitch_total += 1
                if is_strike or is_called_strike_event(event) or is_whiff_event(event):
                    first_pitch_strikes += 1

            pitch_data = event.get("pitchData", {})
            start_speed = safe_float(pitch_data.get("startSpeed"), 0.0)
            pitch_code = parse_pitch_type_code(event)

            if pitch_code:
                pitch_type_counts[pitch_code] = pitch_type_counts.get(pitch_code, 0) + 1
                last_pitch_code = pitch_code

            if start_speed > 0 and pitch_code in FASTBALL_PITCH_CODES:
                fastball_velos.append(start_speed)

        if is_strikeout and last_pitch_code:
            k_by_pitch_code[last_pitch_code] = k_by_pitch_code.get(last_pitch_code, 0) + 1

    payload = {
        "pitch_count": total_pitches,
        "strikes": strikes,
        "whiffs": whiffs,
        "called_strikes": called_strikes,
        "pitch_type_counts": pitch_type_counts,
        "k_by_pitch_code": k_by_pitch_code,
        "first_pitch_strikes": first_pitch_strikes,
        "first_pitch_total": first_pitch_total,
    }
    if total_pitches > 0:
        payload["csw_percent"] = round(((called_strikes + whiffs) / total_pitches) * 100.0, 1)
    if first_pitch_total > 0:
        payload["fp_strike_pct"] = round((first_pitch_strikes / first_pitch_total) * 100.0, 1)
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
        # new fields
        "leverage_damage_runs": 0,       # opp runs scored while margin was <= 2
        "garbage_time_runs": 0,           # opp runs scored while pitcher's team led 4+
        "stranded_runners": 0,            # baserunners left on when outs made / inning ended
        "runners_on_exit": 0,             # runners on base when pitcher left game
        "bullpen_blew_inherited": False,  # those exit runners scored after pitcher left
        "longest_scoreless_streak": 0,    # longest consecutive scoreless inning run mid-outing
        "high_leverage_clean": False,     # pitched in tight game (margin <=1) and kept it scoreless
        "error_contributed": False,       # error occurred in a damage inning
        "damage_inning": None,            # inning number where most damage occurred
        "damage_inning_runs": 0,          # how many runs scored in that inning
        "key_play_descriptions": [],      # select play description strings for Claude context
        "exit_win_probability": None,     # team's win probability when pitcher exited (0-1)
    }
    if not feed or pitcher_id is None:
        return default

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    if not plays:
        return default

    # Index all plays by pitcher id
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

    def runners_after_play(play):
        """Count baserunners in play result (runners still on after play ends)."""
        result = play.get("result", {}) if isinstance(play, dict) else {}
        runners = play.get("runners", []) if isinstance(play, dict) else []
        count = 0
        for r in runners:
            if not isinstance(r, dict):
                continue
            movement = r.get("movement", {})
            end_base = movement.get("end") or movement.get("endBase")
            if end_base and end_base not in ("score", "Score", "4B", None):
                count += 1
        return count

    first_idx = pitcher_plays[0][0]
    prev_away, prev_home = score_tuple(plays[first_idx - 1]) if first_idx > 0 else (0, 0)

    entry_team = prev_home if side == "home" else prev_away
    entry_opp  = prev_away if side == "home" else prev_home

    innings_sequence = []
    runs_by_inning   = {}
    leverage_damage_runs = 0
    garbage_time_runs    = 0
    stranded_total       = 0
    error_innings        = set()   # innings where an error event occurred
    key_play_descriptions = []     # notable play descriptions for Claude

    ERROR_EVENTS = {
        "Field Error", "Throwing Error", "Passed Ball", "Wild Pitch",
        "Fielding Error", "Error",
    }

    last_idx_in_pitcher_plays = pitcher_plays[-1][0]

    for list_pos, (play_idx, play) in enumerate(pitcher_plays):
        about  = play.get("about", {}) if isinstance(play, dict) else {}
        inning = safe_int(about.get("inning", 0), 0)
        is_last_play = (list_pos == len(pitcher_plays) - 1)

        if inning and inning not in innings_sequence:
            innings_sequence.append(inning)

        away_after, home_after = score_tuple(play)
        opp_runs_on_play = (away_after - prev_away) if side == "home" else (home_after - prev_home)

        if opp_runs_on_play > 0 and inning:
            runs_by_inning[inning] = runs_by_inning.get(inning, 0) + opp_runs_on_play

            # Margin BEFORE this play scored
            team_score_before = prev_home if side == "home" else prev_away
            opp_score_before  = prev_away if side == "home" else prev_home
            margin_before = team_score_before - opp_score_before

            if margin_before <= 2:
                leverage_damage_runs += opp_runs_on_play
            if margin_before >= 4:
                garbage_time_runs += opp_runs_on_play

            if abs(margin_before) <= 1:
                pass  # high_leverage tracking reserved for future use

        # Error detection: track innings where errors occurred
        result = play.get("result", {}) if isinstance(play, dict) else {}
        event = str(result.get("event") or "").strip()
        if event in ERROR_EVENTS and inning:
            error_innings.add(inning)

        # Collect key play descriptions (HRs, big multi-run plays, errors)
        description = str(result.get("description") or "").strip()
        if description:
            is_hr = event == "Home Run"
            is_error = event in ERROR_EVENTS
            is_big_play = opp_runs_on_play >= 2
            if (is_hr or is_error or is_big_play) and len(key_play_descriptions) < 4:
                key_play_descriptions.append(description)

        prev_away, prev_home = away_after, home_after

        # Stranded runners: after last play of each inning (half), count runners left
        # We approximate by detecting inning change or end of pitcher's tenure
        if list_pos < len(pitcher_plays) - 1:
            next_inning = safe_int(pitcher_plays[list_pos + 1][1].get("about", {}).get("inning", 0), 0)
            if next_inning != inning and inning:
                on_base = runners_after_play(play)
                stranded_total += on_base
        # On the last play, count runners left as "runners on exit"
        if is_last_play:
            runners_on_exit = runners_after_play(play)
        else:
            runners_on_exit = 0

    exit_team = prev_home if side == "home" else prev_away
    exit_opp  = prev_away if side == "home" else prev_home

    # Did the bullpen blow the inherited runners?
    # Look at plays AFTER the pitcher's last play and see if those runners scored
    bullpen_blew_inherited = False
    if runners_on_exit > 0 and last_idx_in_pitcher_plays < len(plays) - 1:
        # Score right when pitcher exited
        exit_away, exit_home = score_tuple(plays[last_idx_in_pitcher_plays])
        exit_opp_score = exit_away if side == "home" else exit_home
        # Find final score of that same inning (last play of that half-inning)
        exit_inning = safe_int(plays[last_idx_in_pitcher_plays].get("about", {}).get("inning", 0), 0)
        exit_half   = plays[last_idx_in_pitcher_plays].get("about", {}).get("isTopInning", None)
        later_opp_runs = 0
        for future_play in plays[last_idx_in_pitcher_plays + 1:]:
            if not isinstance(future_play, dict):
                continue
            f_about = future_play.get("about", {})
            f_inning = safe_int(f_about.get("inning", 0), 0)
            f_half   = f_about.get("isTopInning", None)
            if f_inning != exit_inning or f_half != exit_half:
                break  # moved to next half-inning
            f_away, f_home = score_tuple(future_play)
            f_opp = f_away if side == "home" else f_home
            later_opp_runs = f_opp - exit_opp_score
        bullpen_blew_inherited = later_opp_runs > 0

    inning_runs = [runs_by_inning.get(inning, 0) for inning in innings_sequence]
    scoreless_to_start = 0
    for runs in inning_runs:
        if runs == 0:
            scoreless_to_start += 1
        else:
            break

    # Longest mid-outing consecutive scoreless streak
    longest_streak = 0
    current_streak = 0
    for runs in inning_runs:
        if runs == 0:
            current_streak += 1
            longest_streak = max(longest_streak, current_streak)
        else:
            current_streak = 0

    run_innings    = [r for r in inning_runs if r > 0]
    scored_in_first = bool(inning_runs and inning_runs[0] > 0)
    settled_after_rough = scored_in_first and len(inning_runs) >= 3 and all(r == 0 for r in inning_runs[1:3])
    late_damage = len(inning_runs) >= 2 and inning_runs[-1] > 0 and sum(inning_runs[:-1]) == 0

    high_leverage_clean = (leverage_damage_runs == 0 and
                           len(innings_sequence) >= 5 and
                           abs(exit_team - exit_opp) <= 2)

    # Identify the worst damage inning
    damage_inning = None
    damage_inning_runs = 0
    if runs_by_inning:
        damage_inning = max(runs_by_inning, key=runs_by_inning.get)
        damage_inning_runs = runs_by_inning[damage_inning]

    error_contributed = bool(error_innings & set(runs_by_inning.keys()))

    # Win probability at pitcher's exit (home team perspective, convert to pitcher's team)
    exit_win_probability = None
    last_play = plays[last_idx_in_pitcher_plays] if last_idx_in_pitcher_plays < len(plays) else None
    if last_play and isinstance(last_play, dict):
        about = last_play.get("about", {})
        home_wp = safe_float(about.get("homeTeamWinProbability", None), None)
        if home_wp is not None:
            exit_win_probability = home_wp / 100.0 if home_wp > 1 else home_wp
            # Convert to pitcher's team perspective
            if side == "away":
                exit_win_probability = round(1.0 - exit_win_probability, 3)
            else:
                exit_win_probability = round(exit_win_probability, 3)

    payload = dict(default)
    payload.update({
        "innings_sequence":          innings_sequence,
        "runs_by_inning":            runs_by_inning,
        "scoreless_to_start":        scoreless_to_start,
        "only_damage_in_one_inning": len(run_innings) == 1 and sum(run_innings) > 0,
        "biggest_inning_runs":       max(run_innings) if run_innings else 0,
        "scored_in_first":           scored_in_first,
        "settled_after_rough":       settled_after_rough,
        "late_damage":               late_damage,
        "team_runs_while_in":        max(exit_team - entry_team, 0),
        "opp_runs_while_in":         max(exit_opp - entry_opp, 0),
        "entry_margin":              entry_team - entry_opp,
        "exit_margin":               exit_team - exit_opp,
        "first_inning":              innings_sequence[0] if innings_sequence else None,
        "last_inning":               innings_sequence[-1] if innings_sequence else None,
        "leverage_damage_runs":      leverage_damage_runs,
        "garbage_time_runs":         garbage_time_runs,
        "stranded_runners":          stranded_total,
        "runners_on_exit":           runners_on_exit,
        "bullpen_blew_inherited":    bullpen_blew_inherited,
        "longest_scoreless_streak":  longest_streak,
        "high_leverage_clean":       high_leverage_clean,
        "error_contributed":         error_contributed,
        "damage_inning":             damage_inning,
        "damage_inning_runs":        damage_inning_runs,
        "key_play_descriptions":     key_play_descriptions,
        "exit_win_probability":      exit_win_probability,
    })
    return payload


# ---------------- CONTACT PROFILE ----------------

def build_contact_profile(feed: dict, pitcher_id: int) -> dict:
    """
    Scan play results to classify the quality of contact allowed.
    Returns counts: home_runs, extra_base_hits, singles, weak_contact (K+GB outs).
    Also captures HR hitter names + their season HR total for notable homer detection.
    """
    profile = {
        "home_runs": 0, "extra_base_hits": 0, "singles": 0, "total_batted_balls": 0,
        "hr_hitters": [],   # list of {"name": str, "season_hrs": int}
    }
    if not feed or pitcher_id is None:
        return profile

    # Build a quick lookup: player_id -> season HR total from boxscore
    season_hr_lookup = {}
    box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for side in ("home", "away"):
        for player in box.get(side, {}).get("players", {}).values():
            pid = player.get("person", {}).get("id")
            if pid is None:
                continue
            season_stats = player.get("seasonStats", {})
            batting = (
                season_stats.get("batting")
                if isinstance(season_stats.get("batting"), dict)
                else season_stats
            )
            hrs = safe_int(batting.get("homeRuns", 0), 0)
            season_hr_lookup[pid] = {
                "name": player.get("person", {}).get("fullName", ""),
                "season_hrs": hrs,
            }

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in plays:
        if not isinstance(play, dict):
            continue
        matchup = play.get("matchup", {}) if isinstance(play, dict) else {}
        pitcher = matchup.get("pitcher", {}) if isinstance(matchup, dict) else {}
        if pitcher.get("id") != pitcher_id:
            continue

        result = play.get("result", {}) if isinstance(play, dict) else {}
        event = str(result.get("event") or "").strip()

        if event == "Home Run":
            profile["home_runs"] += 1
            profile["total_batted_balls"] += 1
            batter = matchup.get("batter", {}) if isinstance(matchup, dict) else {}
            batter_id = batter.get("id")
            if batter_id and batter_id in season_hr_lookup:
                info = season_hr_lookup[batter_id]
                profile["hr_hitters"].append({
                    "name": info["name"] or batter.get("fullName", ""),
                    "season_hrs": info["season_hrs"],
                })
        elif event in ("Double", "Triple"):
            profile["extra_base_hits"] += 1
            profile["total_batted_balls"] += 1
        elif event == "Single":
            profile["singles"] += 1
            profile["total_batted_balls"] += 1
        elif event in ("Groundout", "Flyout", "Pop Out", "Lineout", "Forceout",
                       "Grounded Into DP", "Double Play", "Field Error",
                       "Fielders Choice", "Fielders Choice Out", "Sac Fly",
                       "Sac Bunt", "Bunt Groundout", "Bunt Pop Out"):
            profile["total_batted_balls"] += 1

    return profile

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
            contact = build_contact_profile(feed, person_id)
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
                "k_by_pitch_code": metrics.get("k_by_pitch_code", {}),
                "fp_strike_pct": metrics.get("fp_strike_pct"),
                "first_pitch_total": metrics.get("first_pitch_total", 0),
                "contact_profile": contact,
                "game_flow": game_flow,
                "is_opener": False,         # resolved in loop after recent_appearances fetched
                "platoon_context": build_platoon_context(feed, person_id, side),
                "pitch_mix_shift": [],      # resolved in loop after season pitch mix fetched
                "is_career_debut": False,   # resolved in loop after career stats fetched
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

POST_STAGGER_SECONDS = 45  # pause between cards when multiple post in the same cycle

# Classic rivalries — frozenset so both directions match
RIVALRIES: set = {
    frozenset({"NYY", "BOS"}), frozenset({"LAD", "SF"}),  frozenset({"LAD", "SD"}),
    frozenset({"CHC", "STL"}), frozenset({"NYY", "NYM"}), frozenset({"LAD", "HOU"}),
    frozenset({"BOS", "TB"}),  frozenset({"NYY", "TB"}),  frozenset({"CHC", "MIL"}),
    frozenset({"ATL", "NYM"}), frozenset({"ATL", "PHI"}), frozenset({"LAD", "ARI"}),
    frozenset({"HOU", "TEX"}), frozenset({"CLE", "DET"}), frozenset({"MIN", "CWS"}),
    frozenset({"SF",  "LAA"}), frozenset({"SEA", "HOU"}), frozenset({"NYY", "HOU"}),
    frozenset({"STL", "CIN"}), frozenset({"ATL", "MIA"}), frozenset({"PHI", "NYM"}),
}


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
                    app_ip  = str(stats.get("inningsPitched", "0.0"))
                    app_er  = safe_int(stats.get("earnedRuns", 0), 0)
                    app_k   = safe_int(stats.get("strikeOuts", 0), 0)
                    app_h   = safe_int(stats.get("hits", 0), 0)
                    app_bb  = safe_int(stats.get("baseOnBalls", 0), 0)
                    stats_by_pitcher[pid] = {
                        "ip": app_ip,
                        "h":  app_h,
                        "er": app_er,
                        "bb": app_bb,
                        "k":  app_k,
                        "avg_fastball_velocity": metrics.get("avg_fastball_velocity"),
                        "label": classify_starter({
                            "inningsPitched": app_ip, "earnedRuns": app_er,
                            "strikeOuts": app_k, "hits": app_h, "baseOnBalls": app_bb,
                        }),
                    }

    except Exception as e:
        log(f"Pitching stats cache load failed for {target_date}: {e}")

    pitching_stats_cache[target_date] = stats_by_pitcher
    return stats_by_pitcher


def get_recent_appearances(pitcher_id: int, game_date_et, limit=5, max_days=45):
    appearances = []
    if pitcher_id is None or game_date_et is None:
        return appearances

    # Never look back before the regular season start to avoid spring training noise
    season_year = game_date_et.year
    # MLB regular season typically starts late March — use March 20 as a safe floor
    season_floor = date(season_year, 3, 20)

    check_date = game_date_et - timedelta(days=1)

    for _ in range(max_days):
        if check_date < season_floor:
            break
        stats_by_pitcher = get_pitching_stats_for_date(check_date)
        if pitcher_id in stats_by_pitcher:
            appearances.append(stats_by_pitcher[pitcher_id])
            if len(appearances) >= limit:
                break
        check_date -= timedelta(days=1)

    return appearances


def is_opener(p: dict, recent_appearances: list) -> bool:
    """
    Returns True if this pitcher is a reliever being used as an opener rather
    than a true starter. Criteria:
    - Pitched 2.2 IP or fewer tonight (less than 3 full innings), AND
    - Has at least 3 recent appearances on record, AND
    - ALL of those recent appearances were also under 3.0 IP
    A real starter knocked out early will have normal-length recent starts.
    """
    stats = p.get("stats", {})
    tonight_outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))

    # Must have gone 2.2 IP or fewer tonight (8 outs = 2.2 IP)
    if tonight_outs > 8:
        return False

    # Need enough recent data to make the call
    if not recent_appearances or len(recent_appearances) < 3:
        return False

    # All recent appearances must also be under 3.0 IP
    for app in recent_appearances:
        app_outs = baseball_ip_to_outs(str(app.get("ip", "0.0")))
        if app_outs >= 9:  # 3.0 IP or more = not a reliever pattern
            return False

    return True

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


def get_season_pitch_mix(pitcher_id: int, season: int) -> dict | None:
    """
    Fetch season pitch arsenal percentages from MLB Stats API.
    Returns {pitch_code: pct_float} e.g. {"FF": 0.52, "SL": 0.31, "CH": 0.17}
    Returns None if unavailable or too few pitches.
    """
    key = (pitcher_id, season)
    if key in pitch_mix_cache:
        return pitch_mix_cache[key]

    data = fetch_with_retry(PITCH_ARSENAL_URL.format(pitcher_id, season))
    if not data:
        pitch_mix_cache[key] = None
        return None

    splits = []
    for stat_group in data.get("stats", []):
        splits.extend(stat_group.get("splits", []))

    if not splits:
        pitch_mix_cache[key] = None
        return None

    PITCH_CODE_MAP = {
        "Four-Seam Fastball": "FF", "Two-Seam Fastball": "FT", "Sinker": "SI",
        "Cutter": "FC", "Slider": "SL", "Sweeper": "ST", "Curveball": "CU",
        "Knuckle Curve": "KC", "Changeup": "CH", "Split-Finger": "FS",
    }

    mix = {}
    for split in splits:
        stat = split.get("stat", {})
        pitch_name = stat.get("type", {}).get("description", "")
        usage_pct = safe_float(stat.get("percentage", 0), 0.0)
        code = PITCH_CODE_MAP.get(pitch_name)
        if code and usage_pct > 0:
            mix[code] = round(usage_pct, 3)

    result = mix if mix else None
    pitch_mix_cache[key] = result
    return result


def get_career_ip(pitcher_id: int) -> float | None:
    """
    Fetch career innings pitched to detect first career starts.
    Returns career IP as a float, or None if unavailable.
    """
    if pitcher_id in career_stats_cache:
        return career_stats_cache[pitcher_id]

    data = fetch_with_retry(PLAYER_STATS_URL.format(pitcher_id))
    if not data:
        career_stats_cache[pitcher_id] = None
        return None

    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            ip = safe_float(split.get("stat", {}).get("inningsPitched", 0), 0.0)
            if ip > 0:
                career_stats_cache[pitcher_id] = ip
                return ip

    career_stats_cache[pitcher_id] = None
    return None


def compute_pitch_mix_shift(tonight_counts: dict, season_mix: dict) -> list[dict]:
    """
    Compare tonight's pitch type percentages vs season averages.
    Returns list of {code, tonight_pct, season_pct, delta, direction}
    for pitches that shifted 15%+ from their season norm.
    Only fires when tonight has enough pitches (>= 30) and season mix is available.
    """
    if not tonight_counts or not season_mix:
        return []

    total = sum(tonight_counts.values())
    if total < 30:
        return []

    tonight_pcts = {code: count / total for code, count in tonight_counts.items()}
    shifts = []
    all_codes = set(tonight_pcts) | set(season_mix)

    for code in all_codes:
        tonight = tonight_pcts.get(code, 0.0)
        season  = season_mix.get(code, 0.0)
        delta   = tonight - season
        if abs(delta) >= 0.15 and (tonight >= 0.10 or season >= 0.10):
            shifts.append({
                "code":        code,
                "tonight_pct": round(tonight * 100),
                "season_pct":  round(season * 100),
                "delta":       round(delta * 100),
                "direction":   "up" if delta > 0 else "down",
            })

    # Sort by magnitude
    shifts.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return shifts[:2]  # max 2 shifts to keep facts block clean


def build_platoon_context(feed: dict, pitcher_id: int, pitcher_side: str) -> dict:
    """
    Count LHB vs RHB faced, hits and runs allowed to each.
    Returns dict with platoon summary if a notable split exists.
    """
    result = {"lhb_faced": 0, "rhb_faced": 0, "lhb_hits": 0, "rhb_hits": 0,
              "notable_split": False, "split_description": ""}

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in plays:
        if not isinstance(play, dict):
            continue
        matchup = play.get("matchup", {}) if isinstance(play, dict) else {}
        if matchup.get("pitcher", {}).get("id") != pitcher_id:
            continue

        bat_side = matchup.get("batSide", {}).get("code", "")
        result_data = play.get("result", {})
        event = str(result_data.get("event") or "").strip()

        if bat_side == "L":
            result["lhb_faced"] += 1
            if event in ("Single", "Double", "Triple", "Home Run"):
                result["lhb_hits"] += 1
        elif bat_side == "R":
            result["rhb_faced"] += 1
            if event in ("Single", "Double", "Triple", "Home Run"):
                result["rhb_hits"] += 1

    lhb = result["lhb_faced"]
    rhb = result["rhb_faced"]
    total = lhb + rhb
    if total < 10:
        return result  # not enough data

    # Check for notable platoon advantage (one side < .150 BA, other > .300)
    lhb_avg = result["lhb_hits"] / lhb if lhb >= 4 else None
    rhb_avg = result["rhb_hits"] / rhb if rhb >= 4 else None

    if lhb_avg is not None and rhb_avg is not None:
        if lhb_avg <= 0.150 and rhb_avg >= 0.280:
            result["notable_split"] = True
            result["split_description"] = f"Dominated LHB (.{int(lhb_avg*1000):03d}), struggled vs RHB (.{int(rhb_avg*1000):03d})"
        elif rhb_avg <= 0.150 and lhb_avg >= 0.280:
            result["notable_split"] = True
            result["split_description"] = f"Dominated RHB (.{int(rhb_avg*1000):03d}), struggled vs LHB (.{int(lhb_avg*1000):03d})"
        elif lhb_avg <= 0.125 and lhb >= 5:
            result["notable_split"] = True
            result["split_description"] = f"Held LHB to .{int(lhb_avg*1000):03d} batting average"
        elif rhb_avg <= 0.125 and rhb >= 5:
            result["notable_split"] = True
            result["split_description"] = f"Held RHB to .{int(rhb_avg*1000):03d} batting average"

    return result


def classify_ballpark(venue_name: str) -> str:
    """
    Return 'hitter' or 'pitcher' for notable parks, '' for neutral/unknown.
    Keyed to the venue name string returned by MLB Stats API.
    """
    name = venue_name.lower()
    hitter_parks = {
        "coors field",               # Colorado — extreme hitter's park
        "great american ball park",  # Cincinnati
        "citizens bank park",        # Philadelphia
        "yankee stadium",            # New York Yankees
        "globe life field",          # Texas — indoor dome, plays big
        "american family field",     # Milwaukee
        "fenway park",               # Boston — Green Monster
        "truist park",               # Atlanta
        "chase field",               # Arizona — thin air, retractable roof
        "great american ballpark",   # alternate spelling
    }
    pitcher_parks = {
        "oracle park",               # San Francisco
        "petco park",                # San Diego
        "dodger stadium",            # Los Angeles Dodgers
        "t-mobile park",             # Seattle
        "oakland coliseum",          # Oakland (large foul territory)
        "kauffman stadium",          # Kansas City
        "comerica park",             # Detroit
        "progressive field",         # Cleveland
        "busch stadium",             # St. Louis
        "loanDepot park",            # Miami (fast surface, humid)
        "loandepot park",
    }
    if any(p in name for p in hitter_parks):
        return "hitter"
    if any(p in name for p in pitcher_parks):
        return "pitcher"
    return ""


def compute_fip(stats: dict) -> float | None:
    """
    FIP = (13*HR + 3*BB - 2*K) / IP + 3.15
    Returns None if IP < 1.0 to avoid noise on very short outings.
    """
    ip  = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    hr  = safe_int(stats.get("homeRuns", 0), 0)
    bb  = safe_int(stats.get("baseOnBalls", 0), 0)
    k   = safe_int(stats.get("strikeOuts", 0), 0)

    if ip < 1.0:
        return None

    fip = (13 * hr + 3 * bb - 2 * k) / ip + 3.15
    return round(fip, 2)


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
            f"{name} was crisp and efficient, giving hitters very little to work with all night.",
            f"{name} gave a clean outing and rarely let the {opp_name} get anything going.",
            f"{name} was steady from the outset and did not give hitters many clean openings.",
            f"{name} kept the pressure light for most of the night and never looked like he was in trouble.",
            f"{name} carved through the lineup with minimal resistance and never let the start drift.",
            f"{name} was in command of his outings from the first pitch and made it look relatively easy.",
            f"{name} mixed well and kept hitters off-balance — this was one of his cleaner nights.",
            f"{name} did not waste pitches or innings, and the lineup never really had an answer for him.",
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
            f"{name} came up short on innings, which changed the shape of the game pretty quickly.",
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
        f"The final line read {hit_text}, {er_text}, {bb_text}, and {k_text}.",
        f"He finished with {k_text} while allowing {hit_text} and {er_text}, along with {bb_text}.",
        f"When it was over: {hit_text}, {er_text}, {bb_text}, and {k_text}.",
        f"He gave up {hit_text} and {er_text}, with {bb_text} against {k_text}.",
        f"On the night: {hit_text}, {er_text}, {bb_text}, and {k_text}.",
        f"He logged {k_text}, surrendered {hit_text} and {er_text}, and issued {bb_text}.",
        f"He walked away with {k_text}, {hit_text} allowed, {er_text}, and {bb_text}.",
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
            f"Even when the {opp_name} pushed a little, he was usually the one who got the last word.",
            f"There were not many clean looks for the {opp_name}, and that kept the pressure light for most of the night.",
            "Most of the hits and baserunners stayed scattered, which kept the game from swinging against him.",
            "He consistently found a way to end the inning before the damage could compound.",
            "The lineup could not string things together against him, and that was the story every time they got someone on.",
            "Any time he got into trouble, he had an answer — nothing snowballed.",
            "Baserunners came and went without doing much, which is the quietest form of dominance a pitcher can show.",
            f"The {opp_name} could not put two hard at-bats back to back, and he made sure of that.",
            "He got the outs he needed when the inning mattered, and that was enough to keep the damage contained.",
            "He never let a baserunner turn into a problem — each threat got answered before it could breathe.",
        ]
        if k >= 8:
            choices.append("When things tightened up, he still had enough putaway stuff to end the threat himself.")
            choices.append("He had a strikeout available whenever the situation called for one, which took the pressure off every jam.")
        elif traffic <= 4:
            choices.append("There were very few real openings for the lineup, which helped the outing stay under control from start to finish.")
            choices.append("The lineup had almost nothing to work with — he barely put himself in a difficult spot all night.")
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
        outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))
        choices = [
            f"He kept the {team_name} within reach, but the support around the outing never quite matched it.",
            f"The line was good enough to keep the {team_name} hanging around, even if the result went the other way.",
            f"He gave the {team_name} a chance, even if the rest of the game never quite tilted back toward them.",
            f"He did enough to keep the {opp_name} from running away with it early.",
        ]
        if outs >= 21:
            choices.append("It was the kind of start that usually keeps a team alive deep into the game.")
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
        ip_game = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
        if ip_game >= 7.0:
            choices.extend([
                "Keeping the board clean while working deep into the game is not something that happens every time out, and he earned this one.",
                "The shutout innings are hard to come by, and he made it look easier than it is.",
            ])
        else:
            choices.extend([
                "Keeping the board clean for six innings is a solid achievement, and he earned every bit of it.",
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


# ---------------- NEW SENTENCE BUILDERS ----------------

def build_starter_kbb_sentence(p: dict, label: str, seed: int) -> str:
    """K:BB ratio — fires for both strong command and poor command."""
    stats = p.get("stats", {})
    k  = safe_int(stats.get("strikeOuts", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    ip = safe_float(stats.get("inningsPitched", "0.0"), 0.0)

    # Need at least 4 IP and meaningful K total to say anything useful
    if ip < 4.0 or k < 3:
        return ""

    # --- Elite command: BB = 0 ---
    if bb == 0 and k >= 5:
        choices = [
            "He did not walk a single batter, which made every inning feel like it belonged to him.",
            "No walks in the book for him — when he needed a strikeout he found one, and when he needed a groundball he got that too.",
            "He went the whole way without issuing a free pass, and that kind of command changes the entire shape of a start.",
            "Zero walks is the cleanest command story you can write, and he earned every bit of that line.",
            "The walk column stayed empty all night, and you could feel the lineup's frustration building around it.",
            "He never gave anyone a free base, which is the kind of discipline that makes everything else downstream easier.",
            "Not a single walk the entire time he was out there — the zone was his property all night.",
            "Walking nobody is the ultimate form of working ahead, and he made it look almost routine.",
        ]
        if k >= 10:
            choices.extend([
                f"He struck out {number_word(k)} and walked nobody — that is the cleanest version of a dominant outing.",
                "Double-digit strikeouts and not one walk is the kind of line that does not need any further explanation.",
            ])
        return choices[(seed // 43) % len(choices)]

    ratio_text = f"{k}:{bb} K/BB"

    # --- Great ratio: K >= 3x BB, at least 2 BB ---
    if bb >= 2 and k >= bb * 3:
        choices = [
            f"The {ratio_text} ratio tells the command story well — he was in control of counts from the first inning on.",
            f"He finished with a {ratio_text} ratio, which is exactly the kind of dominance-to-mistake balance you want from a starter.",
            f"A {ratio_text} mark is hard to argue with, and it explains why the lineup never found much traction.",
            f"He punched out {number_word(k)} and walked only {number_word(bb)} — that {ratio_text} split alone tells you how much he was in control.",
            f"The {ratio_text} ratio was the clearest sign he had his best stuff working tonight.",
            f"Striking out {number_word(k)} while only walking {number_word(bb)} is efficient, aggressive pitching — and the {ratio_text} shows it.",
        ]
        return choices[(seed // 43) % len(choices)]

    # --- Decent ratio: 2:1 or better, but not 3:1 ---
    if bb >= 1 and k >= bb * 2 and label in GOOD_STARTER_LABELS:
        choices = [
            f"He struck out {number_word(k)} and walked {number_word(bb)}, and that {ratio_text} ratio kept the damage from getting out of hand.",
            f"The {ratio_text} split shows he was ahead in counts more often than not.",
            f"He punched out {number_word(k)} against {number_word(bb)} walks — not perfectly clean, but that {ratio_text} is efficient enough.",
            f"Finishing with a {ratio_text} ratio is a solid command line for a long outing.",
        ]
        return choices[(seed // 43) % len(choices)]

    # --- Bad ratio: walks competing with or exceeding strikeouts ---
    if bb >= 3 and k <= bb * 1.5 and label in SUBPAR_STARTER_LABELS | {"NO_COMMAND"}:
        choices = [
            f"He struck out {number_word(k)} but walked {number_word(bb)}, and those free passes kept every inning more stressful than it needed to be.",
            f"The {ratio_text} ratio is the kind of split that makes a long night feel even longer.",
            f"He only had {number_word(k)} strikeouts against {number_word(bb)} walks — a {ratio_text} mark that meant too many counts went the wrong way.",
            f"Punching out {number_word(k)} while walking {number_word(bb)} is hard to sustain — and eventually it caught up with him.",
            f"The walk total was too close to the strikeout total, and that {ratio_text} math almost never works out well over a full start.",
            f"He struck out {number_word(k)} but the {number_word(bb)} walks bled into every inning and kept the pressure building.",
            f"When your K/BB is {ratio_text}, too many counts are going the wrong direction — and the line shows it.",
        ]
        return choices[(seed // 43) % len(choices)]

    # --- High BB even with good K: wild strikeout arm ---
    if bb >= 4 and k >= 7 and label in {"STRIKEOUT", "UNEVEN"}:
        choices = [
            f"He struck out {number_word(k)} but the {number_word(bb)} walks kept offsetting everything — a {ratio_text} night that was electric in spots and loose in others.",
            f"The strikeout total was impressive but the {number_word(bb)} walks were always waiting in the background, and the {ratio_text} ratio shows the tension.",
            f"He had {number_word(k)} punchouts to his name but {number_word(bb)} walks alongside them — a {ratio_text} split that is the definition of a hard night to manage.",
            f"Big strikeout total, big walk total — {ratio_text} — he was electric in spots and loose in others, and both showed up in the line.",
        ]
        return choices[(seed // 43) % len(choices)]

    return ""


def build_starter_fp_strike_sentence(p: dict, label: str, seed: int) -> str:
    """First-pitch strike rate — fires when clearly high or clearly low."""
    fp_pct = p.get("fp_strike_pct")
    fp_total = safe_int(p.get("first_pitch_total", 0), 0)

    if fp_pct is None or fp_total < 10:
        return ""

    try:
        fp_val = float(fp_pct)
    except Exception:
        return ""

    elite   = fp_val >= 70.0
    good    = fp_val >= 63.0
    poor    = fp_val <= 50.0
    bad     = fp_val <= 44.0

    fp_text = f"{int(round(fp_val))} percent"

    if label in GOOD_STARTER_LABELS and good:
        if elite:
            choices = [
                f"He got ahead on the first pitch {fp_text} of the time, and that kind of early count control is the foundation everything else is built on.",
                f"His first-pitch strike rate of {fp_text} meant he was dictating the at-bat before it even got started.",
                f"He threw first-pitch strikes at a {fp_text} clip, which tells you how much he was in attack mode from pitch one.",
                f"Getting ahead {fp_text} of the time on the first pitch makes every subsequent pitch easier, and he used that all night.",
                f"A first-pitch strike rate of {fp_text} is elite territory — he never let hitters dig in and get comfortable.",
                f"He pounded the zone early and often, hitting first-pitch strikes {fp_text} of the time and never letting batters settle in.",
            ]
        else:
            choices = [
                f"He was ahead in the count early, getting first-pitch strikes {fp_text} of the time and consistently working from a position of strength.",
                f"Getting ahead {fp_text} of the time on first pitches helped him stay efficient and keep deep counts to a minimum.",
                f"His first-pitch strike rate of {fp_text} was a big reason counts stayed in his favor for most of the night.",
                f"He was in control from pitch one more often than not, landing first-pitch strikes at a {fp_text} rate.",
            ]
        return choices[(seed // 47) % len(choices)]

    if label in SUBPAR_STARTER_LABELS and poor:
        if bad:
            choices = [
                f"He was only getting first-pitch strikes {fp_text} of the time, which meant he was playing from behind almost every at-bat.",
                f"A first-pitch strike rate of {fp_text} is a rough way to go through a lineup — you're giving away the count before the battle even starts.",
                f"He could not get ahead consistently, hitting first-pitch strikes just {fp_text} of the time and letting hitters set the terms.",
                f"When you're only throwing first-pitch strikes {fp_text} of the time, every at-bat becomes a grind, and the innings reflect that.",
                f"The first-pitch strike numbers were ugly at {fp_text} — he was behind in the count before he even made his second pitch.",
            ]
        else:
            choices = [
                f"He got ahead early in only {fp_text} of at-bats, which kept too many counts tilted in the hitter's favor.",
                f"The first-pitch strike rate of {fp_text} was below where it needed to be, and it set up a lot of the trouble that followed.",
                f"He struggled to get ahead on the first pitch — only {fp_text} — and that dug him into holes he could not always escape.",
            ]
        return choices[(seed // 47) % len(choices)]

    # Interesting contrast: bad label but good FP rate (stuff issue, not approach)
    if label in BAD_STARTER_LABELS and good:
        choices = [
            f"He got first-pitch strikes {fp_text} of the time, so this was not a command or approach problem — the damage came after he was already ahead.",
            f"His first-pitch strike rate of {fp_text} shows he was attacking the zone early; the issue was what happened once hitters made contact.",
            f"He was ahead in the count plenty — first-pitch strikes {fp_text} of the time — so the rough line was more about the quality of contact than the approach.",
        ]
        return choices[(seed // 47) % len(choices)]

    return ""


def build_starter_leverage_sentence(p: dict, label: str, seed: int) -> str:
    """Contextualizes when damage came relative to game state."""
    flow = p.get("game_flow") or {}
    leverage_damage = safe_int(flow.get("leverage_damage_runs", 0), 0)
    garbage_runs    = safe_int(flow.get("garbage_time_runs", 0), 0)
    opp_runs_total  = safe_int(flow.get("opp_runs_while_in", 0), 0)
    innings         = flow.get("innings_sequence") or []
    high_lev_clean  = bool(flow.get("high_leverage_clean"))

    if not innings or opp_runs_total == 0 and not high_lev_clean:
        return ""

    name = p.get("name", "He")

    # All damage came in garbage time
    if opp_runs_total > 0 and garbage_runs >= opp_runs_total and garbage_runs >= 2:
        choices = [
            "Most of the damage on his ledger came after the lead was already well in hand, so the ERA hit is a bit misleading.",
            "The runs he gave up came late with the game already decided — that is a different kind of damage than runs that actually change an outcome.",
            "A lot of the damage was in low-stakes situations, which makes the earned run total look worse than the outing actually felt.",
            "He absorbed some runs when the game was effectively over, which inflates the line a little.",
            "The earned runs came with a comfortable cushion behind him, so they cost less than a scoreline alone would suggest.",
        ]
        return choices[(seed // 53) % len(choices)]

    # Damage came almost entirely in tight situations
    if leverage_damage >= 2 and leverage_damage >= opp_runs_total * 0.75:
        choices = [
            "The trouble came when the game was closest, which made every run feel heavier than it might have in another context.",
            "The damage landed in the high-stakes frames, not in mop-up time — those runs actually shaped the game.",
            "Most of what he gave up came while the margin was tight, which is the worst time for a pitcher to spring a leak.",
            "The runs hit in the innings that mattered most, and that is what made the outing harder to absorb than the total alone suggests.",
            "He gave up runs when the game was in the balance, not after the outcome was settled — that is a meaningful distinction.",
        ]
        return choices[(seed // 53) % len(choices)]

    # Held up in close game / high-leverage clean
    if high_lev_clean and label in GOOD_STARTER_LABELS:
        choices = [
            "He worked through several innings where the game was tight and never let the pressure create a big inning.",
            "The game stayed close for stretches, and he kept his composure and the scoreboard in check throughout.",
            "He was pitching with the margin slim for portions of the night, and his ability to hold the line there was the real story.",
            "Some of the quietest innings came when the game was at its tightest — that is when it is hardest to be clean, and he was.",
            "He pitched well in the frames that actually mattered, keeping the game within reach even when the margin was thin.",
        ]
        return choices[(seed // 53) % len(choices)]

    # Mixed: some leverage damage, some not — only worth noting if contrast is notable
    if leverage_damage >= 1 and garbage_runs >= 2 and label in BAD_STARTER_LABELS:
        choices = [
            "Some of the damage came with the game already out of hand, so the line overstates how badly he was beaten.",
            "Not all of those runs came in critical moments — a couple arrived after the game had already turned.",
        ]
        return choices[(seed // 53) % len(choices)]

    return ""


def build_starter_stranded_sentence(p: dict, label: str, seed: int) -> str:
    """Credit or note stranded baserunners depending on start quality."""
    flow     = p.get("game_flow") or {}
    stranded = safe_int(flow.get("stranded_runners", 0), 0)
    stats    = p.get("stats", {})
    er       = safe_int(stats.get("earnedRuns", 0), 0)
    ip       = safe_float(stats.get("inningsPitched", "0.0"), 0.0)

    if ip < 4.0 or stranded < 3:
        return ""

    name = p.get("name", "He")

    if label in GOOD_STARTER_LABELS:
        if stranded >= 7:
            choices = [
                f"He left {number_word(stranded)} runners stranded across the night, which means the damage count was much lower than the traffic would have predicted.",
                f"The ability to pitch out of trouble was a huge part of this start — {number_word(stranded)} runners left on base, and almost none of them crossed the plate.",
                f"He stranded {number_word(stranded)} baserunners, and that capacity to strand traffic was the difference between a good line and a great one.",
                f"Leaving {number_word(stranded)} runners on base is not luck — that is pitching with something in reserve when it counts.",
                f"With {number_word(stranded)} runners stranded, he gave away very little when he was in trouble, which is the mark of a pitcher who knows how to close out an inning.",
            ]
        else:
            choices = [
                f"He stranded {number_word(stranded)} runners along the way, keeping the damage below what the baserunner count might have suggested.",
                f"The {number_word(stranded)} stranded runners tell the story of a pitcher who consistently found the out he needed to end the inning.",
                f"He worked out of traffic well, leaving {number_word(stranded)} on base and keeping the run total manageable throughout.",
                f"Leaving {number_word(stranded)} runners on base across the night helped him keep the earned run total honest.",
            ]
        return choices[(seed // 59) % len(choices)]

    elif label in BAD_STARTER_LABELS:
        if stranded >= 5:
            choices = [
                f"He actually stranded {number_word(stranded)} runners, so the damage could have been even worse — the line was bad but the bullpen did not inherit a complete mess.",
                f"For all the trouble he was in, he did strand {number_word(stranded)} runners, which kept the damage from escalating further.",
            ]
        else:
            choices = [
                f"He only stranded {number_word(stranded)} runners across the night, which means most of the damage that came through, he could not stop.",
            ]
        return choices[(seed // 59) % len(choices)]

    elif label == "UNEVEN":
        choices = [
            f"He left {number_word(stranded)} runners on base, and those escapes were the main reason the line stayed in the range it did.",
            f"Stranding {number_word(stranded)} runners in a night with this much traffic is part of why the outing did not fall apart completely.",
            f"He worked his way out of trouble more than once, leaving {number_word(stranded)} on base and keeping the run total from getting too ugly.",
            f"The {number_word(stranded)} stranded runners kept the line honest — this was a grinding outing, but he found the escape hatch often enough.",
        ]
        return choices[(seed // 59) % len(choices)]

    return ""


def build_starter_inherited_sentence(p: dict, label: str, seed: int) -> str:
    """Only fires when the bullpen definitively blew inherited runners after pitcher exited."""
    flow = p.get("game_flow") or {}
    if not flow.get("bullpen_blew_inherited"):
        return ""

    runners_on_exit = safe_int(flow.get("runners_on_exit", 0), 0)
    if runners_on_exit == 0:
        return ""

    stats = p.get("stats", {})
    ip    = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    er    = safe_int(stats.get("earnedRuns", 0), 0)
    name  = p.get("name", "He")

    on_text = "a runner" if runners_on_exit == 1 else f"{number_word(runners_on_exit)} runners"

    choices = [
        f"He left {on_text} on base when he exited, and the bullpen let them score — those runs are on the ledger but not really on him.",
        f"The {on_text} he left on base came around to score after he was already out of the game, so his actual earned run line looks a bit heavier than it should.",
        f"He exited with {on_text} on base, and the relief corps could not get out of the inning clean — that is a different story than the raw numbers tell.",
        f"The runs that followed his exit were not his to give up in the traditional sense — he left {on_text} on base and the pen could not hold them.",
        f"There was {on_text} on base when he handed off, and they came around to score, which inflates the damage assigned to this start.",
        f"When he left, there were still {on_text} on base — and they scored — so his ERA will feel this one a little more than the outing actually warranted.",
    ]
    if runners_on_exit >= 2:
        choices.extend([
            f"He gave up the base-runners, but what happened after he exited with {on_text} still on base is on the bullpen, not on him.",
            f"Leaving with {on_text} on base and watching them score is one of the more frustrating ways to see your line get padded.",
        ])

    return choices[(seed // 61) % len(choices)]


def build_starter_scoreless_streak_sentence(p: dict, label: str, seed: int) -> str:
    """Mid-outing consecutive scoreless innings streak (min 4 innings)."""
    flow            = p.get("game_flow") or {}
    longest_streak  = safe_int(flow.get("longest_scoreless_streak", 0), 0)
    scoreless_start = safe_int(flow.get("scoreless_to_start", 0), 0)
    innings_total   = len(flow.get("innings_sequence") or [])

    MIN_STREAK = 4  # per user preference

    if longest_streak < MIN_STREAK:
        return ""

    # If the streak is just the opening run, scoreless_to_start already covers it
    # Only fire here if there's a notable mid-outing streak AFTER taking damage
    scored_in_first = bool(flow.get("scored_in_first"))
    only_from_start = (longest_streak == scoreless_start)

    if only_from_start and not scored_in_first:
        return ""  # already handled by scoreless_to_start logic in flow_sentence

    streak_text = number_word(longest_streak)

    if label in GOOD_STARTER_LABELS:
        choices = [
            f"He put together a stretch of {streak_text} straight scoreless innings at one point, which gave the game a sense of inevitability.",
            f"There was a run of {streak_text} consecutive scoreless frames in there that changed the feel of the entire night.",
            f"His best stretch was {streak_text} innings in a row without allowing a run, and that is when the start really locked in.",
            f"He ran off {streak_text} clean innings in a row, and that streak was the backbone of the whole outing.",
            f"A {streak_text}-inning scoreless stretch at one point made this outing feel even more controlled than the final line shows.",
            f"He was at his sharpest during a run of {streak_text} straight scoreless frames — nothing was getting through during that stretch.",
        ]
    elif scored_in_first and longest_streak >= 4:
        # He recovered from early damage and went on a clean streak
        choices = [
            f"After the early trouble, he settled into a stretch of {streak_text} consecutive scoreless innings — that recovery was the whole story of this start.",
            f"He found his footing and ran off {streak_text} scoreless innings in a row after the rough opening, which put the game back on track.",
            f"He answered the early damage with {streak_text} straight clean frames, and that run of control is what made the line salvageable.",
            f"The {streak_text}-inning scoreless streak he put together after being touched early is really what defined the outing.",
            f"Once he settled in, he was nearly untouchable — {streak_text} straight scoreless innings after the rough start is a real story.",
            f"After giving up runs early, he went {streak_text} innings without allowing another — that kind of bounce-back is harder than it looks.",
        ]
    else:
        choices = [
            f"He had a run of {streak_text} straight scoreless innings in there, even if the full picture was messier than that stretch.",
            f"There was a {streak_text}-inning clean stretch buried in the outing that kept the damage from being worse.",
        ]

    return choices[(seed // 67) % len(choices)]


def build_starter_contact_sentence(p: dict, label: str, seed: int) -> str:
    """Describe the quality of contact allowed — HRs, XBH, soft contact."""
    contact = p.get("contact_profile") or {}
    hrs  = safe_int(contact.get("home_runs", 0), 0)
    xbh  = safe_int(contact.get("extra_base_hits", 0), 0)
    total_xbh = hrs + xbh
    singles = safe_int(contact.get("singles", 0), 0)
    total_bb = safe_int(contact.get("total_batted_balls", 0), 0)
    stats = p.get("stats", {})
    hits = safe_int(stats.get("hits", 0), 0)
    er   = safe_int(stats.get("earnedRuns", 0), 0)
    ip   = safe_float(stats.get("inningsPitched", "0.0"), 0.0)

    if ip < 3.0 or total_bb < 5:
        return ""

    # Story 1: got hit hard with power (HRs)
    if hrs >= 2:
        hr_text = stat_phrase(hrs, "home run")
        choices = [
            f"He gave up {hr_text}, and those were the swings that really shaped the line.",
            f"The {hr_text} he surrendered were the loudest moments — most of the other contact was more manageable.",
            f"Power was the problem: {hr_text} accounted for a big chunk of the damage.",
            f"He gave up {hr_text}, and those balls left no room for error in the innings that mattered.",
            f"The {hr_text} were the decisive blows — take those away and this reads as a much cleaner night.",
        ]
        return choices[(seed // 71) % len(choices)]

    if hrs == 1 and er >= 3 and label in BAD_STARTER_LABELS:
        choices = [
            "He gave up a home run that changed the complexion of the outing, and the damage never fully reversed.",
            "One ball left the yard and rearranged the whole shape of the start.",
            "The home run hurt more than most — it came at a moment where the outing could have gone either way.",
        ]
        return choices[(seed // 71) % len(choices)]

    # Story 2: lots of extra base hits (not HR)
    if xbh >= 3 and label in SUBPAR_STARTER_LABELS:
        choices = [
            f"He gave up {stat_phrase(xbh, 'extra-base hit')}, and the gap power from the lineup is what drove the damage.",
            f"The {stat_phrase(xbh, 'extra-base hit')} against him stretched the line — too many pitches found the gaps.",
            f"It was not just the singles stacking up; {stat_phrase(xbh, 'extra-base hit')} did most of the real damage.",
        ]
        return choices[(seed // 71) % len(choices)]

    # Story 3: hit hard but no HRs — contact was loud overall
    if total_xbh >= 2 and hits >= 7 and label in BAD_STARTER_LABELS:
        choices = [
            "The contact against him was loud — multiple balls in the gaps and not much soft stuff mixed in.",
            "Hitters were squaring him up all night, and the quality of contact reflected it.",
            "The hits he allowed were not weak — they were hard contact that found open grass.",
        ]
        return choices[(seed // 71) % len(choices)]

    # Story 4: all soft contact, clean despite traffic (good starts)
    if hrs == 0 and total_xbh <= 1 and hits >= 5 and label in GOOD_STARTER_LABELS:
        choices = [
            "The contact against him was mostly soft — a lot of singles and weak grounders that never turned into the big inning.",
            "He gave up hits, but nothing hard enough to really threaten him — the quality of contact stayed in his favor.",
            "For as many balls in play as there were, almost none of them were squared up, and that kept the damage from compounding.",
            "He was hit, but not hit hard — the contact profile tells a better story than the hit total alone.",
        ]
        return choices[(seed // 71) % len(choices)]

    # Story 5: clean contact profile, low hits (dominant)
    if hrs == 0 and total_xbh == 0 and hits <= 4 and label in {"GEM", "DOMINANT", "SHARP"}:
        choices = [
            "He kept the ball in the yard all night, and there was very little hard contact to speak of.",
            "Not a single extra-base hit against him — hitters could not get enough of the ball to do real damage.",
            "The contact profile was as clean as the line: nothing left the yard and nothing found the gaps.",
            "He kept the ball in the park and kept it off the barrel, which is the double-lock that makes a start like this possible.",
        ]
        return choices[(seed // 71) % len(choices)]

    return ""


def build_starter_hr_hitter_sentence(p: dict, label: str, seed: int) -> str:
    """
    Name a notable hitter who went deep — fires when a batter with 15+ season HRs
    hit a home run against this pitcher. Only surfaces for one batter (the most
    notable). Does not fire if the pitcher allowed 3+ HRs total (contact_sentence
    already covers that story).
    """
    contact = p.get("contact_profile") or {}
    total_hrs = safe_int(contact.get("home_runs", 0), 0)
    hr_hitters = contact.get("hr_hitters", [])

    # Don't double-up with contact_sentence when it's a multi-HR blowup
    if total_hrs >= 3 or not hr_hitters:
        return ""

    # Find the most notable hitter (highest season HR total above threshold)
    NOTABLE_HR_THRESHOLD = 15
    notable = [h for h in hr_hitters if safe_int(h.get("season_hrs", 0), 0) >= NOTABLE_HR_THRESHOLD]
    if not notable:
        return ""

    # Pick the one with the most season HRs
    top = max(notable, key=lambda h: safe_int(h.get("season_hrs", 0), 0))
    name = top.get("name", "")
    season_hrs = safe_int(top.get("season_hrs", 0), 0)
    if not name:
        return ""

    # Shorten to last name for readability in sentences
    last_name = name.split()[-1] if name else name

    if label in GOOD_STARTER_LABELS:
        choices = [
            f"{last_name} got him with a home run, but that was about as much as the lineup could manage against him.",
            f"He ran into {last_name}, who has {season_hrs} home runs on the year, but that was a rare win for the offense against him tonight.",
            f"{last_name}'s homer was the one real damage pitch of the night — outside of that, he kept the ball in the park.",
            f"He gave up a home run to {last_name}, who has been one of the more dangerous bats in the league, but it was the only real mistake he made.",
        ]
    else:
        choices = [
            f"{last_name} made him pay with a home run — and with {season_hrs} on the year, that is exactly the kind of at-bat you cannot afford to lose.",
            f"He gave up a home run to {last_name}, who came in with {season_hrs} on the season, and that swing changed the shape of the outing.",
            f"{last_name} got him for a long ball, and against a hitter with that kind of power, there is very little margin for a mistake.",
            f"A home run from {last_name} was one of the louder moments — you know coming in that a hitter with {season_hrs} home runs can end an inning in one swing.",
        ]

    return choices[(seed // 103) % len(choices)]


def build_starter_run_support_sentence(p: dict, label: str, game_context: dict, seed: int) -> str:
    """Note when a pitcher was let down by minimal run support or bailed by big offense."""
    flow = p.get("game_flow") or {}
    team_runs = safe_int(flow.get("team_runs_while_in", 0), 0)
    stats = p.get("stats", {})
    wins  = safe_int(stats.get("wins", 0), 0)
    ip    = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    er    = safe_int(stats.get("earnedRuns", 0), 0)
    team_name = team_name_from_abbr(p.get("team"))

    if ip < 4.0:
        return ""

    # Gem/dominant with zero or one run of support
    if team_runs <= 1 and label in {"GEM", "DOMINANT", "QUALITY", "SHARP"}:
        choices = [
            f"He did all of that on {number_word(team_runs) if team_runs else 'no'} run{'s' if team_runs != 1 else ''} of support — this was entirely his doing.",
            f"The {team_name} gave him almost nothing to work with offensively, which makes the line even harder to put up.",
            f"He worked with a skeleton crew on the scoreboard and still made it look easy.",
            f"Barely any run support, and he made it not matter — that is a different kind of dominant.",
            f"He got {'one run' if team_runs == 1 else 'no runs'} to work with and did not need anything more than that.",
        ]
        return choices[(seed // 73) % len(choices)]

    # Quality start let down — good label but team still lost
    away_score = safe_int(game_context.get("away_score", 0), 0)
    home_score = safe_int(game_context.get("home_score", 0), 0)
    team_final = home_score if p.get("side") == "home" else away_score
    opp_final  = away_score if p.get("side") == "home" else home_score
    team_lost  = team_final < opp_final

    if team_lost and label in GOOD_STARTER_LABELS and team_runs <= 2 and er <= 2:
        choices = [
            f"He gave the {team_name} every chance to win and got {number_word(team_runs) if team_runs else 'nothing'} back for it.",
            f"The support was not there, and a start that deserved a win ended up on the wrong side of the ledger.",
            f"He held up his end — it just was not enough given what the offense produced.",
            f"He pitched well enough to win on most nights. Tonight was not most nights for the {team_name} bats.",
        ]
        return choices[(seed // 73) % len(choices)]

    # Big offensive support on a rough outing
    if team_runs >= 6 and label in BAD_STARTER_LABELS:
        choices = [
            f"The {team_name} gave him {number_word(team_runs)} runs to work with, which is the only reason this one did not get away from them completely.",
            f"He needed every bit of the {number_word(team_runs)} runs the {team_name} put up — the offense covered for a rough night.",
            f"A {number_word(team_runs)}-run cushion kept this one survivable despite the struggles on the mound.",
        ]
        return choices[(seed // 73) % len(choices)]

    return ""


def build_starter_day_night_sentence(p: dict, label: str, game_context: dict, seed: int) -> str:
    """Subtle day/night flavor — only fires occasionally to avoid feeling formulaic."""
    time_of_day = game_context.get("day_night", "")
    if not time_of_day:
        return ""

    stats = p.get("stats", {})
    ip = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    if ip < 5.0:
        return ""

    # Fire on roughly 1 in 3 seeds — use a less predictable mix
    if (seed // 7 + seed % 11) % 3 != 0:
        return ""

    if time_of_day == "day":
        if label in GOOD_STARTER_LABELS:
            choices = [
                "Day games tend to separate pitchers quickly, and he was the one in control from the first inning on.",
                "He handled the afternoon setting cleanly — no slow start, no sluggish middle innings, just a steady day's work.",
                "Day baseball has a different tempo, and he matched it perfectly from pitch one.",
                "He made this look like a comfortable afternoon out there — nothing forced, nothing labored.",
                "Not every pitcher thrives in a day game, but he looked right at home under the sun.",
            ]
        else:
            choices = [
                "He could not find his footing in the day game setting, and the lineup made him pay for it early.",
                "Day games can expose a pitcher who is not fully locked in, and the lineup sensed it tonight.",
                "He never settled into a rhythm in the afternoon, and the outing had a choppy feel from the start.",
            ]
    else:
        if label in GOOD_STARTER_LABELS:
            choices = [
                "He was locked in under the lights from the opening frame and never gave the crowd anything to get loud about.",
                "Night games tend to get tighter as they go, and he kept tightening the screws with every inning.",
                "He was sharp under the lights all the way through — the longer the game went, the more comfortable he looked.",
                "Under the lights he was at his best — the late innings felt like his territory.",
                "He made the most of the prime-time stage and never let the lineup get anything going.",
            ]
        else:
            choices = [
                "The night game setting did not help — things got away from him before he could find any real momentum.",
                "He could not get his footing under the lights, and once the lineup smelled blood the outing got away from him.",
                "Night games have a way of amplifying mistakes, and he could not contain the damage once it started.",
            ]

    return choices[(seed // 79) % len(choices)]


def build_starter_rivalry_sentence(p: dict, label: str, game_context: dict, seed: int) -> str:
    """Flag when the matchup is a classic rivalry."""
    away = normalize_team_abbr(game_context.get("away_abbr", ""))
    home = normalize_team_abbr(game_context.get("home_abbr", ""))
    pair = frozenset({away, home})

    if pair not in RIVALRIES:
        return ""

    opp_abbr = home if p.get("side") == "away" else away
    opp_name = team_name_from_abbr(opp_abbr)
    team_name = team_name_from_abbr(p.get("team"))

    if label in GOOD_STARTER_LABELS:
        choices = [
            f"Doing it against the {opp_name} carries a little more weight — those matchups always seem to mean something.",
            f"Beating the {opp_name} in a start like this is the kind of thing that gets remembered.",
            f"The rivalry backdrop made this one a little louder, and he delivered exactly when it counted.",
            f"He rose to the moment in a matchup the {team_name} always circled on the calendar.",
            f"There is always extra voltage in a {team_name}–{opp_name} game, and he handled it.",
            f"Against the {opp_name} you want your best, and tonight he brought it.",
            f"The {opp_name} are not a lineup you can be sloppy against, and he was not — start to finish.",
        ]
    elif label in BAD_STARTER_LABELS:
        choices = [
            f"Rough timing for a bad outing — the {opp_name} are not the team you want to hand a short start to.",
            f"The {opp_name} made him pay, which makes this one sting a little more than usual.",
            f"Against the {opp_name}, you cannot afford a start like this, and the lineup took full advantage.",
            f"Of all the nights to struggle, doing it against the {opp_name} is the one that sticks.",
            f"The {opp_name} are exactly the kind of offense that punishes a shaky start, and they did.",
        ]
    else:
        choices = [
            f"Against the {opp_name} even a mixed outing carries some weight — the rivalry has a way of raising the stakes.",
            f"A {team_name}–{opp_name} game has its own pressure, and you could feel it in the innings that mattered.",
            f"The {opp_name} kept him honest when he needed clean innings most — that is what rivalry games do.",
        ]

    return choices[(seed // 83) % len(choices)]


def build_starter_trend_sentence(p: dict, label: str, seed: int, recent_appearances=None) -> str:
    """Surface multi-start streaks: consecutive quality starts, rough run, bounceback."""
    if not recent_appearances or len(recent_appearances) < 1:
        return ""

    def is_quality(app):
        ip = safe_float(app.get("ip", "0.0"), 0.0)
        er = safe_int(app.get("er", 0), 0)
        return ip >= 6.0 and er <= 3

    def is_rough(app):
        return app.get("label", "") in BAD_STARTER_LABELS

    recent = recent_appearances

    # Bounceback: today is good, previous 1-2 were rough
    prev_were_rough = sum(1 for a in recent[:2] if is_rough(a))
    is_bounceback = label in GOOD_STARTER_LABELS and prev_were_rough >= 1

    # Build full streak list: today + recent (most-recent-first)
    this_is_quality = is_quality({
        "ip": p.get("stats", {}).get("inningsPitched", "0.0"),
        "er": safe_int(p.get("stats", {}).get("earnedRuns", 0), 0),
    })
    full_streak = [this_is_quality] + [is_quality(a) for a in recent]
    streak_len = 0
    for v in full_streak:
        if v:
            streak_len += 1
        else:
            break

    # Compute ERA/K context over the prior starts in the streak
    streak_apps = recent[:streak_len - 1] if streak_len > 1 else []
    streak_stats = compute_streak_stats(streak_apps) if streak_apps else {}
    era_stretch  = streak_stats.get("era_stretch")
    k_trend      = streak_stats.get("k_trend")

    if streak_len >= 3 and label in GOOD_STARTER_LABELS:
        streak_text = number_word(streak_len)
        choices = [
            f"That is {streak_text} quality starts in a row now — he has been one of the more reliable arms in the rotation lately.",
            f"He has turned in a quality start in {streak_text} straight outings, and the consistency is starting to tell a real story.",
            f"Going back {streak_text} starts, the line has been clean every single time — this is a pitcher in a really good stretch.",
            f"That run of {streak_text} consecutive quality starts makes him one of the harder starters in the league to plan around right now.",
            f"He is on a {streak_text}-start quality run, and the sustained sharpness is not an accident.",
            f"Back-to-back-to-back quality outings — {streak_text} straight now — and this looks less like a hot stretch and more like the real level.",
        ]
        if era_stretch is not None and era_stretch <= 2.50:
            era_str = f"{era_stretch:.2f}"
            choices.extend([
                f"He has posted quality starts in {streak_text} straight outings with a {era_str} ERA over that stretch — that kind of run is hard to ignore.",
                f"Over his last {streak_text} starts he has been almost untouchable, carrying a {era_str} ERA into tonight.",
            ])
        elif era_stretch is not None and era_stretch <= 3.50:
            era_str = f"{era_stretch:.2f}"
            choices.append(
                f"He has been steady in {streak_text} consecutive quality starts, putting up a {era_str} ERA over that run."
            )
        if k_trend == "rising":
            choices.extend([
                f"Not only is he in the middle of a {streak_text}-start quality run, but the strikeout numbers have been climbing — the stuff is trending up.",
                f"The quality starts are piling up and the swing-and-miss is getting sharper — the whole package is moving in the right direction.",
            ])
        return choices[(seed // 89) % len(choices)]

    if streak_len == 2 and label in GOOD_STARTER_LABELS:
        choices = [
            "Back-to-back quality starts — he is in the middle of a real run of good form.",
            "Two straight quality outings now, and the recent momentum is worth noting.",
            "He backed up his last start with another strong one — that kind of consistency matters in a rotation.",
            "Two in a row, and tonight looked like a pitcher who knows exactly what he is doing right now.",
        ]
        if k_trend == "rising":
            choices.append("He has turned in back-to-back quality starts and the strikeout rate is climbing — the arrow is pointing up.")
        return choices[(seed // 89) % len(choices)]

    # Bounceback
    if is_bounceback:
        choices = [
            "He needed a response after a rough previous outing, and this was exactly that.",
            "After struggling last time out, he came back with an answer — that kind of reset is not always easy to find.",
            "He bounced back in a big way after his last start, and the line tonight shows what he is capable of.",
            "His last outing was a tough one, but he did not let it carry over — this was a clean reset.",
            "The bounceback was real. Whatever he was working on between starts showed up tonight.",
            "He came back sharp after a rough outing and looked like a completely different pitcher.",
        ]
        return choices[(seed // 89) % len(choices)]

    # Rough skid context
    this_is_rough = label in BAD_STARTER_LABELS
    full_rough = [this_is_rough] + [is_rough(a) for a in recent]
    rough_streak = 0
    for v in full_rough:
        if v:
            rough_streak += 1
        else:
            break

    if rough_streak >= 3:
        choices = [
            f"This is the {number_word(rough_streak)} straight rough outing now — there is a pattern developing that will need to get addressed.",
            f"He has been in a tough stretch, and {number_word(rough_streak)} consecutive poor starts is a real concern for the rotation.",
            f"Three straight difficult outings tells you something is off — this is not just a one-night problem.",
            f"The skid is at {number_word(rough_streak)} starts now, and at some point a difficult stretch becomes a trend.",
        ]
        return choices[(seed // 89) % len(choices)]

    if rough_streak == 2:
        choices = [
            "Two rough starts back to back — the skid is real and something is not clicking right now.",
            "Back-to-back poor outings puts him in a tough spot heading into his next turn.",
            "He could not stop the slide tonight — two straight poor starts and still searching for answers.",
        ]
        return choices[(seed // 89) % len(choices)]

    return ""


def build_starter_debut_sentence(p: dict, label: str, seed: int, recent_appearances=None) -> str:
    """Flag season debut or likely IL return (large gap between appearances)."""
    season_stats = p.get("season_stats", {})
    gs = safe_int(season_stats.get("gamesStarted", 0), 0) or safe_int(season_stats.get("gamesPitched", 0), 0)
    name = p.get("name", "He")
    year = datetime.now(ET).year

    # Season debut
    if gs == 1:
        if label in GOOD_STARTER_LABELS:
            choices = [
                f"He opened his {year} season on the right note, and that line is a statement.",
                f"That was {name}'s first start of the {year} season, and he picked up right where he left off.",
                f"Hard to ask for a better way to open the {year} campaign than that.",
                f"{name} wasted no time making an impression in his first outing of {year}.",
                f"First start of the year, and he looked like he never missed a beat.",
            ]
        else:
            choices = [
                f"That was {name}'s first start of the {year} season — not the opening he wanted, but there is time to find the form.",
                f"Opening the {year} campaign with a tough outing is not ideal, but one start does not define anything.",
                f"First outing of {year} for {name}, and the rust was visible — something to build off going forward.",
                f"{name} opened {year} with a shaky one, but there is plenty of runway to reset.",
            ]
        return choices[(seed // 97) % len(choices)]

    # IL return heuristic: has season stats (gs >= 2) but no recent appearances in past 30 days
    if gs >= 2 and (not recent_appearances or len(recent_appearances) == 0):
        if label in GOOD_STARTER_LABELS:
            choices = [
                f"That looked like a return from a long absence for {name}, and he came back with a strong one.",
                f"Coming back after time away is never simple, but {name} made it look like he never left.",
                f"If that was a return start for {name}, the time off did not seem to cost him anything.",
            ]
        else:
            choices = [
                f"Coming back from time away is hard, and the rust showed for {name} tonight.",
                f"That looked like a return start for {name} — not an easy night, but getting back out there is the first step.",
            ]
        return choices[(seed // 97) % len(choices)]

    return ""


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
    opp_name = (
        team_name_from_abbr(game_context.get("home_abbr"))
        if p.get("side") == "away"
        else team_name_from_abbr(game_context.get("away_abbr"))
    )

    no_decision = (
        wins == 0 and losses_stat == 0 and
        label in GOOD_STARTER_LABELS and
        safe_float(stats.get("inningsPitched", "0.0"), 0.0) >= 5.0
    )

    # Opener override — bypass normal subject line tree entirely
    if p.get("is_opener"):
        if er == 0:
            opener_choices = [
                f"⚡ {name} handles the opener role cleanly for the {team_name}",
                f"⚡ {name} keeps it scoreless in the opener spot",
                f"⚡ {name} sets a clean table in the opener role",
            ]
        else:
            opener_choices = [
                f"⚡ {name} works the opener role for the {team_name}",
                f"⚡ {name} takes the ball first out of the {team_name} bullpen",
                f"⚡ {name} opens the game for the {team_name}",
            ]
        subject = opener_choices[seed % len(opener_choices)].strip().rstrip(".!?:;,-")
        return subject.replace("...", "").strip()

    if outs < 3:
        choices = [
            f"⚠️ {name} is knocked out in the opening inning against {opp_name}",
            f"⚠️ {name} exits before he can record an out",
            f"⚠️ {name} never gets the start off the ground against {opp_name}",
        ]
    elif label == "GEM":
        choices = [
            f"🔥 {name} puts a lid on the {opp_name} lineup all night",
            f"🔥 {name} barely gives {opp_name} a pulse",
            f"🔥 {name} controls the game from the first pitch against {opp_name}",
            f"🔥 {name} silences {opp_name} with a gem",
        ]
        if k >= 8:
            choices.extend([
                f"🔥 {name} carves up {opp_name} with {number_word(k)} strikeouts",
                f"🔥 {name} dominates {opp_name} from wire to wire",
            ])
        if outs >= 24:
            choices.append(f"🔥 {name} goes the distance and shuts down {opp_name}")
    elif label == "DOMINANT":
        choices = [
            f"🔥 {name} overpowers {opp_name} over {ip_text}",
            f"🔥 {name} gives {opp_name} no answers all night",
            f"🔥 {name} takes over and never lets {opp_name} settle in",
        ]
        if k >= 10:
            choices.extend([
                f"🔥 {name} reaches double digits against {opp_name}",
                f"🔥 {name} fans {number_word(k)} and runs the {opp_name} lineup ragged",
            ])
        elif k >= 8:
            choices.extend([
                f"🔥 {name} punches out {number_word(k)} in a dominant outing against {opp_name}",
                f"🔥 {name} works deep while racking up {number_word(k)} strikeouts",
            ])
        else:
            choices.append(f"🔥 {name} powers through a dominant start against {opp_name}")
    elif label == "STRIKEOUT":
        choices = [
            f"🎯 {name} punches out {number_word(k)} against {opp_name}",
            f"🎯 {name} misses bats all night and makes {opp_name} look overmatched",
            f"🎯 {name} leans on the swing-and-miss to carry the outing against {opp_name}",
        ]
        if k >= 10:
            choices.extend([
                f"🎯 {name} fans {number_word(k)} and makes {opp_name} look helpless",
                f"🎯 {name} reaches double digits and takes over the game",
            ])
        if bb >= 3:
            choices.append(f"🎯 {name} piles up strikeouts against {opp_name} despite the extra traffic")
        if no_decision:
            choices.append(f"🎯 {name} punches out {number_word(k)} against {opp_name} and walks away empty-handed")
    elif label in {"QUALITY", "SHARP"}:
        choices = [
            f"✅ {name} keeps {opp_name} in check and gives the {team_name} a quality start",
            f"✅ {name} gives the {team_name} exactly what they needed against {opp_name}",
            f"✅ {name} handles {opp_name} and hands the bullpen a lead to work with",
            f"✅ {name} shuts {opp_name} down with a clean, efficient outing",
        ]
        if k >= 7 and outs >= 21:
            choices.append(f"✅ {name} pairs length with {number_word(k)} strikeouts against {opp_name}")
        elif er == 0:
            choices.append(f"✅ {name} blanks {opp_name} through {ip_text}")
        elif scoreless_to_start >= 4:
            choices.append(f"✅ {name} rolls through {opp_name} in the early innings and holds on")
        if no_decision:
            choices.append(f"✅ {name} deals well against {opp_name} but leaves without the win")
    elif label == "SOLID":
        choices = [
            f"📈 {name} keeps {opp_name} off the board long enough to matter",
            f"📈 {name} gives the {team_name} length without drama against {opp_name}",
            f"📈 {name} holds {opp_name} at bay through {ip_text}",
            f"📈 {name} does the job against {opp_name} without much margin for error",
        ]
        if k >= 7:
            choices.append(f"📈 {name} mixes in the strikeouts through a solid night against {opp_name}")
        elif only_damage_in_one_inning and er <= 3:
            choices.append(f"📈 {name} keeps {opp_name} quiet outside of one rough frame")
        if no_decision:
            if outs >= 21:
                choices.append(f"📈 {name} goes deep against {opp_name} and gets nothing to show for it")
            else:
                choices.append(f"📈 {name} pitches well enough to win against {opp_name} but walks away empty-handed")
    elif label == "HIT_HARD":
        choices = [
            f"💥 {name} gets tagged by {opp_name} despite the swing-and-miss",
            f"💥 {name} leaves too many pitches in the zone and {opp_name} does not miss them",
            f"💥 {name} pays for the hard contact against {opp_name}",
            f"💥 {name} cannot keep {opp_name} from squaring the ball up",
        ]
        if k >= 7:
            choices.append(f"💥 {name} misses bats but gets punished on contact by {opp_name}")
    elif label == "NO_COMMAND":
        choices = [
            f"🧭 {name} never gets comfortable in the zone against {opp_name}",
            f"🧭 {name} puts too many runners on and {opp_name} makes him pay",
            f"🧭 {name} fights the strike zone all night against {opp_name}",
        ]
        if bb >= 4:
            choices.extend([
                f"🧭 {name} issues {number_word(bb)} free passes and never recovers",
                f"🧭 {name} walks {opp_name} into trouble all night long",
            ])
    elif label == "UNEVEN":
        choices = [
            f"📉 {name} has no answer for {opp_name} when it matters most",
            f"📉 {name} alternates clean innings with trouble all night against {opp_name}",
            f"📉 {name} never quite settles into a rhythm against {opp_name}",
            f"📉 {name} gets through it, but one bad frame does all the damage",
        ]
        if scoreless_to_start >= 3:
            choices.append(f"📉 {name} cruises early before {opp_name} catches up")
        elif only_damage_in_one_inning:
            choices.append(f"📉 {name} looks good until one inning against {opp_name} unravels it")
    else:
        # ROUGH / SHORT
        choices = [
            f"⚠️ {name} runs into trouble early and cannot stop {opp_name}",
            f"⚠️ {name} never finds his footing against {opp_name}",
            f"⚠️ {name} cannot keep the outing from getting away against {opp_name}",
        ]
        if hits >= 7 and bb <= 1:
            choices.append(f"⚠️ {name} gets tagged for {hits} hits and {opp_name} keeps the pressure on")
        elif bb >= 4:
            choices.append(f"⚠️ {name} walks {opp_name} into a lead he never recovers from")
        elif er >= 5:
            choices.append(f"⚠️ {name} gives up {number_word(er)} runs and hands {opp_name} control early")

    if team_runs > opp_runs and label in POSITIVE_STARTER_LABELS:
        choices.append(f"🏁 {name} sets the {team_name} up right against {opp_name}")
    elif team_runs < opp_runs and label in BAD_STARTER_LABELS | {"UNEVEN"}:
        bad_loss_choices = [
            f"📉 {name} puts the {team_name} in a hole against {opp_name}",
            f"📉 {name} hands {opp_name} an early advantage the {team_name} cannot overcome",
            f"📉 {name} struggles to keep the {team_name} in it against {opp_name}",
            f"📉 {name} gives {opp_name} too much room to work with",
            f"📉 {name} lets this one get away from the {team_name}",
            f"📉 {name} sends the {team_name} to a rough night against {opp_name}",
        ]
        choices.append(bad_loss_choices[seed % len(bad_loss_choices)])

    subject = choices[seed % len(choices)].strip().rstrip(".!?:;,-")
    return subject.replace("...", "").strip()


# ---------------- SUMMARY ASSEMBLY ----------------

def _semantic_overlap(a: str, b: str, threshold: int = 8) -> bool:
    """Return True if two sentences share a suspiciously long common substring."""
    if not a or not b:
        return False
    words_a = a.lower().split()
    words_b = set(b.lower().split())
    # sliding window of `threshold` consecutive words
    for i in range(len(words_a) - threshold + 1):
        window = " ".join(words_a[i:i + threshold])
        if window in b.lower():
            return True
    return False


def build_starter_summary(
    p: dict,
    label: str,
    game_context: dict,
    recent_appearances=None,
    opp_hitting: dict | None = None,
    next_start: dict | None = None,
    season: int = 0,
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
    kbb_sentence       = build_starter_kbb_sentence(p, label, seed)
    fp_sentence        = build_starter_fp_strike_sentence(p, label, seed)
    leverage_sentence  = build_starter_leverage_sentence(p, label, seed)
    stranded_sentence  = build_starter_stranded_sentence(p, label, seed)
    inherited_sentence = build_starter_inherited_sentence(p, label, seed)
    streak_sentence    = build_starter_scoreless_streak_sentence(p, label, seed)
    contact_sentence   = build_starter_contact_sentence(p, label, seed)
    hr_hitter_sentence = build_starter_hr_hitter_sentence(p, label, seed)
    support_sentence   = build_starter_run_support_sentence(p, label, game_context, seed)
    daynight_sentence  = build_starter_day_night_sentence(p, label, game_context, seed)
    rivalry_sentence   = build_starter_rivalry_sentence(p, label, game_context, seed)
    trend_sentence     = build_starter_trend_sentence(p, label, seed, recent_appearances)
    debut_sentence     = build_starter_debut_sentence(p, label, seed, recent_appearances)
    next_start_sentence = build_starter_next_start_sentence(p, label, seed, next_start, season)

    cap = 6 if label in {"GEM", "DOMINANT"} else 5

    if is_bad_starter_label(label):
        order_options = [
            [overview, flow_sentence, contact_sentence, hr_hitter_sentence, stat_sentence, positive_sentence, pressure_sentence, leverage_sentence, kbb_sentence, pitch_sentence, team_sentence, support_sentence, velocity_sentence, csw_sentence, opp_sentence, inherited_sentence, rivalry_sentence, trend_sentence, next_start_sentence],
            [overview, pressure_sentence, stat_sentence, positive_sentence, flow_sentence, contact_sentence, hr_hitter_sentence, kbb_sentence, leverage_sentence, pitch_sentence, team_sentence, velocity_sentence, csw_sentence, opp_sentence, season_sentence, trend_sentence, rivalry_sentence, next_start_sentence],
            [overview, stat_sentence, flow_sentence, contact_sentence, hr_hitter_sentence, positive_sentence, leverage_sentence, csw_sentence, pitch_sentence, team_sentence, support_sentence, velocity_sentence, opp_sentence, kbb_sentence, inherited_sentence, debut_sentence, next_start_sentence],
            [overview, stat_sentence, pitch_sentence, flow_sentence, contact_sentence, hr_hitter_sentence, pressure_sentence, positive_sentence, kbb_sentence, team_sentence, velocity_sentence, opp_sentence, leverage_sentence, rivalry_sentence, trend_sentence, next_start_sentence],
        ]
    elif label == "STRIKEOUT":
        order_options = [
            [overview, csw_sentence, kbb_sentence, stat_sentence, flow_sentence, contact_sentence, hr_hitter_sentence, pressure_sentence, fp_sentence, pitch_sentence, team_sentence, velocity_sentence, season_sentence, nd_sentence, opp_sentence, trend_sentence, rivalry_sentence, next_start_sentence],
            [overview, flow_sentence, csw_sentence, kbb_sentence, stat_sentence, team_sentence, contact_sentence, hr_hitter_sentence, fp_sentence, pitch_sentence, velocity_sentence, pressure_sentence, season_sentence, opp_sentence, trend_sentence, next_start_sentence],
            [overview, stat_sentence, csw_sentence, flow_sentence, kbb_sentence, contact_sentence, hr_hitter_sentence, team_sentence, velocity_sentence, pressure_sentence, fp_sentence, pitch_sentence, nd_sentence, opp_sentence, trend_sentence, debut_sentence, next_start_sentence],
        ]
    elif label in {"GEM", "DOMINANT"}:
        order_options = [
            [overview, flow_sentence, streak_sentence, csw_sentence, kbb_sentence, contact_sentence, hr_hitter_sentence, pressure_sentence, stat_sentence, stranded_sentence, support_sentence, team_sentence, velocity_sentence, fp_sentence, pitch_sentence, season_sentence, nd_sentence, opp_sentence, rivalry_sentence, trend_sentence, debut_sentence, next_start_sentence],
            [overview, pressure_sentence, kbb_sentence, stat_sentence, flow_sentence, streak_sentence, contact_sentence, hr_hitter_sentence, csw_sentence, stranded_sentence, team_sentence, fp_sentence, support_sentence, pitch_sentence, velocity_sentence, season_sentence, opp_sentence, trend_sentence, rivalry_sentence, next_start_sentence],
            [overview, csw_sentence, kbb_sentence, stat_sentence, contact_sentence, hr_hitter_sentence, flow_sentence, streak_sentence, team_sentence, pressure_sentence, stranded_sentence, velocity_sentence, fp_sentence, support_sentence, pitch_sentence, nd_sentence, opp_sentence, trend_sentence, debut_sentence, next_start_sentence],
        ]
    else:
        order_options = [
            [overview, flow_sentence, streak_sentence, stat_sentence, pressure_sentence, kbb_sentence, contact_sentence, hr_hitter_sentence, team_sentence, pitch_sentence, csw_sentence, fp_sentence, stranded_sentence, support_sentence, velocity_sentence, season_sentence, nd_sentence, opp_sentence, pitch_mix_sentence, rivalry_sentence, trend_sentence, debut_sentence, next_start_sentence],
            [overview, pitch_sentence, flow_sentence, kbb_sentence, stat_sentence, contact_sentence, hr_hitter_sentence, pressure_sentence, team_sentence, csw_sentence, fp_sentence, streak_sentence, support_sentence, velocity_sentence, season_sentence, opp_sentence, trend_sentence, rivalry_sentence, next_start_sentence],
            [overview, pressure_sentence, stat_sentence, flow_sentence, kbb_sentence, contact_sentence, hr_hitter_sentence, csw_sentence, team_sentence, velocity_sentence, pitch_sentence, stranded_sentence, nd_sentence, opp_sentence, streak_sentence, pitch_mix_sentence, support_sentence, debut_sentence, trend_sentence, next_start_sentence],
            [overview, stat_sentence, team_sentence, flow_sentence, kbb_sentence, contact_sentence, hr_hitter_sentence, pressure_sentence, pitch_sentence, fp_sentence, velocity_sentence, csw_sentence, stranded_sentence, support_sentence, season_sentence, opp_sentence, rivalry_sentence, trend_sentence, next_start_sentence],
        ]

    ordered = [s for s in order_options[seed % len(order_options)] if s]
    final_sentences = []
    for sentence in ordered:
        if not sentence:
            continue
        # Exact dedup
        if sentence in final_sentences:
            continue
        # Semantic dedup — skip if it heavily overlaps with something already chosen
        if any(_semantic_overlap(sentence, existing) for existing in final_sentences):
            continue
        final_sentences.append(sentence)
        if len(final_sentences) >= cap:
            break

    if len(final_sentences) < 4:
        fillers = [
            flow_sentence, stat_sentence, pressure_sentence, team_sentence,
            csw_sentence, pitch_sentence, velocity_sentence, positive_sentence,
            pitch_mix_sentence, season_sentence, nd_sentence, opp_sentence,
            kbb_sentence, fp_sentence, leverage_sentence, stranded_sentence,
            inherited_sentence, streak_sentence, contact_sentence, hr_hitter_sentence, support_sentence,
            trend_sentence, rivalry_sentence, debut_sentence, next_start_sentence,
        ]
        for sentence in fillers:
            if not sentence or sentence in final_sentences:
                continue
            if any(_semantic_overlap(sentence, existing) for existing in final_sentences):
                continue
            final_sentences.append(sentence)
            if len(final_sentences) >= cap:
                break

    return " ".join(final_sentences[:cap])


def compute_streak_stats(recent_appearances: list) -> dict:
    """
    Given a list of recent appearance dicts (most recent first), compute:
    - era_stretch: ERA over the stretch
    - k_trend: 'rising', 'falling', or None
    - avg_k_per_start: average Ks over the stretch
    """
    if not recent_appearances:
        return {}

    total_er = 0
    total_ip_outs = 0
    k_values = []

    for app in recent_appearances:
        total_er += safe_int(app.get("er", 0), 0)
        total_ip_outs += baseball_ip_to_outs(str(app.get("ip", "0.0")))
        k_values.append(safe_int(app.get("k", 0), 0))

    era_stretch = None
    if total_ip_outs > 0:
        era_stretch = round((total_er / total_ip_outs) * 27, 2)  # 27 outs = 9 innings

    avg_k = round(sum(k_values) / len(k_values), 1) if k_values else None

    k_trend = None
    if len(k_values) >= 3:
        # Compare first half vs second half (second half = older starts)
        mid = len(k_values) // 2
        recent_avg = sum(k_values[:mid]) / mid if mid else 0
        older_avg  = sum(k_values[mid:]) / (len(k_values) - mid) if len(k_values) - mid else 0
        if recent_avg >= older_avg * 1.25:
            k_trend = "rising"
        elif older_avg >= recent_avg * 1.25:
            k_trend = "falling"

    return {
        "era_stretch": era_stretch,
        "avg_k_per_start": avg_k,
        "k_trend": k_trend,
        "num_starts": len(recent_appearances),
    }


def get_next_start(pitcher_id: int, pitcher_team_abbr: str, after_date) -> dict | None:
    """
    Scan the next 7 days of schedule for a game where this pitcher is listed
    as a confirmed probable. Returns a dict with game info or None.
    """
    if pitcher_id is None or after_date is None:
        return None

    # Check session cache
    if pitcher_id in next_start_cache:
        return next_start_cache[pitcher_id]

    team_abbr = normalize_team_abbr(pitcher_team_abbr)

    for days_ahead in range(1, 8):
        check_date = after_date + timedelta(days=days_ahead)
        data = fetch_with_retry(PROBABLE_URL.format(check_date.isoformat()))
        if not data:
            continue

        for date_block in data.get("dates", []):
            for game in date_block.get("games", []):
                game_teams = game.get("teams", {})
                for side in ("home", "away"):
                    t = game_teams.get(side, {})
                    t_abbr = normalize_team_abbr(
                        t.get("team", {}).get("abbreviation") or
                        t.get("team", {}).get("name", "")
                    )
                    if t_abbr != team_abbr:
                        continue

                    probable = t.get("probablePitcher") or {}
                    if not isinstance(probable, dict):
                        continue
                    if probable.get("id") != pitcher_id:
                        continue

                    # Found the next start
                    opp_side = "home" if side == "away" else "away"
                    opp = game_teams.get(opp_side, {})
                    opp_abbr = normalize_team_abbr(
                        opp.get("team", {}).get("abbreviation") or
                        opp.get("team", {}).get("name", "")
                    )
                    result = {
                        "date": check_date,
                        "side": side,          # "home" or "away"
                        "opp_abbr": opp_abbr,
                        "opp_name": team_name_from_abbr(opp_abbr),
                        "game_pk": game.get("gamePk"),
                    }
                    next_start_cache[pitcher_id] = result
                    return result

    next_start_cache[pitcher_id] = None
    return None


def build_starter_next_start_sentence(p: dict, label: str, seed: int, next_start: dict | None, season: int) -> str:
    """
    Look-ahead sentence woven into the summary.
    Only fires when next start is confirmed (probable pitcher set).
    """
    if not next_start:
        return ""

    opp_name  = next_start.get("opp_name", "opponent")
    opp_abbr  = next_start.get("opp_abbr", "")
    side      = next_start.get("side", "home")
    date      = next_start.get("date")

    at_vs = "against" if side == "home" else "on the road against"

    # Date phrasing
    if date:
        days_away = (date - datetime.now(ET).date()).days
        if days_away == 1:
            when = "tomorrow"
        elif days_away <= 6:
            when = date.strftime("%A")          # "Friday"
        else:
            when = date.strftime("%A, %b %-d")  # "Saturday, Apr 12"
    else:
        when = "next time out"

    # Opponent quality context
    opp_hitting = get_team_hitting_stats(opp_abbr, season) if opp_abbr else None
    tier = classify_offense(opp_hitting.get("ops", 0.0)) if opp_hitting else None

    tier_phrases = {
        "elite":         "one of the tougher offenses in the league",
        "above_average": "an above-average offense",
        "below_average": "one of the weaker lineups in the league",
        "weak":          "a struggling offense",
    }
    opp_quality = tier_phrases.get(tier, "") if tier and tier != "average" else ""

    if label in GOOD_STARTER_LABELS:
        if opp_quality:
            choices = [
                f"He will look to carry this into {when}, when he faces {opp_quality} in the {opp_name}.",
                f"Next up is {when} {at_vs} the {opp_name} — {opp_quality} awaits.",
                f"He gets {when} {at_vs} the {opp_name}, who have been {opp_quality} this season.",
                f"The next test comes {when} {at_vs} the {opp_name}, and they have been {opp_quality}.",
            ]
        else:
            choices = [
                f"He will look to carry this form into his next start, which comes {when} {at_vs} the {opp_name}.",
                f"Next up for him is {when} {at_vs} the {opp_name}.",
                f"He gets another crack at it {when} — this time {at_vs} the {opp_name}.",
                f"The {opp_name} are on deck for {when}, and he will be looking to build on this.",
            ]
    else:
        if opp_quality:
            choices = [
                f"He will need to regroup before {when}, when he draws {opp_quality} in the {opp_name}.",
                f"The next chance to reset comes {when} {at_vs} the {opp_name} — {opp_quality}.",
                f"He gets {when} {at_vs} the {opp_name}, who have been {opp_quality}, so the turnaround will not be easy.",
            ]
        else:
            choices = [
                f"He will get another chance {when} {at_vs} the {opp_name}.",
                f"The next start comes {when} {at_vs} the {opp_name} — a chance to reset.",
                f"He draws the {opp_name} {when}, and will be looking to bounce back from this one.",
            ]

    return choices[(seed // 101) % len(choices)]

def get_games():
    today = datetime.now(ET).date()
    games = []

    data = fetch_with_retry(f"{SCHEDULE_URL}&date={today.isoformat()}")
    if data is None:
        log(f"Schedule fetch failed for {today}")
        return games
    for date_block in data.get("dates", []):
        games.extend(date_block.get("games", []))

    return games


def get_feed(game_id):
    data = fetch_with_retry(LIVE_URL.format(game_id))
    if data is None:
        log(f"Feed fetch error for game {game_id}: all retries exhausted")
    return data


# ---------------- DISCORD ----------------

def score_field_emoji(game_context: dict) -> str:
    """Pick a contextual emoji for the score field."""
    away = safe_int(game_context.get("away_score", 0), 0)
    home = safe_int(game_context.get("home_score", 0), 0)
    total = away + home
    margin = abs(away - home)
    is_rivalry = frozenset({
        normalize_team_abbr(game_context.get("away_abbr", "")),
        normalize_team_abbr(game_context.get("home_abbr", "")),
    }) in RIVALRIES

    if is_rivalry:
        return "🔥"
    if margin >= 8:
        return "💥"
    if margin == 1 or margin == 0:
        return "⚡"
    if total >= 16:
        return "🎰"
    if game_context.get("day_night") == "day":
        return "☀️"
    return "⚾"


async def build_claude_card(
    p: dict,
    label: str,
    game_context: dict,
    recent_appearances=None,
    opp_hitting: dict | None = None,
    next_start: dict | None = None,
    season: int = 0,
    seed: int = 0,
) -> dict | None:
    """
    Use the Claude API to write both the subject line and summary in one call.
    Returns {"subject": str, "summary": str} or None on any failure so
    post_card falls back to templates for both fields.
    """
    if not ANTHROPIC_API_KEY:
        log("Claude card skipped — STARTER_BOT_SUMMARY env var not set")
        return None

    stats   = p.get("stats", {})
    flow    = p.get("game_flow") or {}
    contact = p.get("contact_profile") or {}
    name    = p.get("name", "the pitcher")
    team    = team_name_from_abbr(p.get("team", ""))
    opp_abbr = game_context.get("home_abbr") if p.get("side") == "away" else game_context.get("away_abbr")
    opp_name = team_name_from_abbr(opp_abbr)

    ip     = str(stats.get("inningsPitched", "0.0"))
    ip_float = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    h      = safe_int(stats.get("hits", 0), 0)
    er     = safe_int(stats.get("earnedRuns", 0), 0)
    bb     = safe_int(stats.get("baseOnBalls", 0), 0)
    k      = safe_int(stats.get("strikeOuts", 0), 0)
    wins   = safe_int(stats.get("wins", 0), 0)
    losses = safe_int(stats.get("losses", 0), 0)

    # Pitch metrics
    csw       = p.get("csw_percent")
    velo      = p.get("avg_fastball_velocity")
    fp_pct    = p.get("fp_strike_pct")
    pitch_cnt = safe_int(p.get("pitch_count", 0), 0)
    pitch_counts = p.get("pitch_type_counts", {})

    # Dominant pitch type and secondary pitch
    dominant_pitch = ""
    secondary_pitch = ""
    COMMON_FASTBALLS = {"FF", "FA"}
    k_by_pitch = p.get("k_by_pitch_code", {})
    total_ks = safe_int(p.get("stats", {}).get("strikeOuts", 0), 0)
    if pitch_counts:
        total_pitches = sum(pitch_counts.values())
        if total_pitches >= 30:
            PITCH_NAMES = {
                "FF": "four-seam fastball", "FT": "two-seam fastball", "SI": "sinker",
                "FC": "cutter", "SL": "slider", "ST": "sweeper", "CU": "curveball",
                "KC": "knuckle curve", "CH": "changeup", "FS": "split-finger",
            }
            top_code = max(pitch_counts, key=pitch_counts.get)
            top_pct  = pitch_counts[top_code] / total_pitches
            # Only flag a dominant fastball when it's unusually heavy (60%+); any
            # other pitch type leading the mix is interesting at 30%+.
            fastball_threshold = 0.60 if top_code in COMMON_FASTBALLS else 0.30
            if top_pct >= fastball_threshold:
                dominant_pitch = f"{PITCH_NAMES.get(top_code, top_code.lower())} ({int(top_pct*100)}%)"

            # Secondary pitch: non-fastball at 30%+ usage that also generated
            # a meaningful share of strikeouts (at least 2 Ks on that pitch).
            for code, cnt in sorted(pitch_counts.items(), key=lambda x: x[1], reverse=True):
                if code in COMMON_FASTBALLS:
                    continue
                pct = cnt / total_pitches
                pitch_ks = k_by_pitch.get(code, 0)
                if pct >= 0.30 and pitch_ks >= 2:
                    pname = PITCH_NAMES.get(code, code.lower())
                    secondary_pitch = f"{pname} ({int(pct*100)}%, {pitch_ks} of {total_ks} Ks)"
                    break

    # Opponent quality
    opp_tier = ""
    if opp_hitting:
        tier = classify_offense(opp_hitting.get("ops", 0.0))
        if tier != "average":
            opp_tier = tier.replace("_", " ")

    # Recent form
    form_notes = []
    if recent_appearances:
        def _is_qs(a): return safe_float(a.get("ip","0"),0) >= 6.0 and safe_int(a.get("er",0),0) <= 3
        streak = 0
        for a in ([{"ip": ip, "er": er}] + list(recent_appearances)):
            if _is_qs(a): streak += 1
            else: break
        if streak >= 3:
            # Compute ERA over the streak
            streak_apps = recent_appearances[:streak - 1]
            ss = compute_streak_stats(streak_apps) if streak_apps else {}
            era_note = f", {ss['era_stretch']:.2f} ERA over that stretch" if ss.get("era_stretch") else ""
            form_notes.append(f"{streak} consecutive quality starts{era_note}")
        elif streak == 2:
            form_notes.append("back-to-back quality starts")
        elif recent_appearances and recent_appearances[0].get("label","") in BAD_STARTER_LABELS:
            if label in GOOD_STARTER_LABELS:
                form_notes.append("bounceback after a rough previous outing")
            else:
                rough_count = sum(1 for a in recent_appearances[:3] if a.get("label","") in BAD_STARTER_LABELS)
                if rough_count >= 2:
                    form_notes.append(f"{rough_count + 1} straight rough outings")

    # Contact profile
    hrs        = safe_int(contact.get("home_runs", 0), 0)
    xbh        = safe_int(contact.get("extra_base_hits", 0), 0)
    hr_hitters = contact.get("hr_hitters", [])
    notable_hr = next((x for x in hr_hitters if safe_int(x.get("season_hrs",0),0) >= 15), None)

    # Leverage context
    lev_damage   = safe_int(flow.get("leverage_damage_runs", 0), 0)
    garbage_runs = safe_int(flow.get("garbage_time_runs", 0), 0)
    stranded     = safe_int(flow.get("stranded_runners", 0), 0)

    # Next start
    next_start_note = ""
    if next_start:
        ns_opp  = next_start.get("opp_name", "")
        ns_date = next_start.get("date")
        if ns_date:
            days_away = (ns_date - datetime.now(ET).date()).days
            when = "tomorrow" if days_away == 1 else ns_date.strftime("%A")
        else:
            when = "next time out"
        at_vs = "vs." if next_start.get("side") == "home" else "@"
        ns_tier = ""
        if ns_opp:
            ns_hitting = get_team_hitting_stats(normalize_team_abbr(next_start.get("opp_abbr", "")), season)
            if ns_hitting:
                ns_t = classify_offense(ns_hitting.get("ops", 0.0))
                if ns_t not in ("average", ""):
                    ns_tier = f" ({ns_t.replace('_',' ')} offense)"
        next_start_note = f"{when} {at_vs} {ns_opp}{ns_tier}"

    # Rivalry
    is_rivalry = frozenset({
        normalize_team_abbr(game_context.get("away_abbr", "")),
        normalize_team_abbr(game_context.get("home_abbr", "")),
    }) in RIVALRIES

    # Season debut / IL return
    gs        = safe_int(p.get("season_stats", {}).get("gamesStarted", 0), 0)
    is_debut  = gs == 1
    is_return = gs >= 2 and (not recent_appearances or len(recent_appearances) == 0)

    # ---------- Build context block ----------
    # Include season stats explicitly so Claude never infers career status from training data
    season_stats = p.get("season_stats", {})
    season_w  = safe_int(season_stats.get("wins", 0), 0)
    season_l  = safe_int(season_stats.get("losses", 0), 0)
    season_era_raw = season_stats.get("era", None)
    season_gs_total = safe_int(season_stats.get("gamesStarted", 0), 0)
    season_ip_raw = season_stats.get("inningsPitched", None)

    season_line_parts = []
    if season_gs_total:  season_line_parts.append(f"{season_gs_total} GS")
    if season_w or season_l: season_line_parts.append(f"{season_w}-{season_l}")
    if season_era_raw:   season_line_parts.append(f"ERA {float(season_era_raw):.2f}")
    if season_ip_raw:    season_line_parts.append(f"{season_ip_raw} IP")
    season_line = ", ".join(season_line_parts) if season_line_parts else "season stats not available"

    facts = [
        f"Pitcher: {name} ({team})",
        f"Opponent: {opp_name}",
        f"Game line: {ip} IP  {h} H  {er} ER  {bb} BB  {k} K",
        f"Decision: {'Win' if wins else 'Loss' if losses else 'No decision'}",
        f"Classification: {label}",
        f"Season stats (before this game): {season_line}",
    ]

    # Natural language score — winner first, no abbreviations, so Claude writes it correctly
    away_score_val = safe_int(game_context.get("away_score", 0), 0)
    home_score_val = safe_int(game_context.get("home_score", 0), 0)
    away_name = team_name_from_abbr(game_context.get("away_abbr", ""))
    home_name = team_name_from_abbr(game_context.get("home_abbr", ""))
    if away_score_val > home_score_val:
        score_sentence = f"Final score: {away_name} {away_score_val}, {home_name} {home_score_val}"
    elif home_score_val > away_score_val:
        score_sentence = f"Final score: {home_name} {home_score_val}, {away_name} {away_score_val}"
    else:
        score_sentence = f"Final score: {away_name} {away_score_val}, {home_name} {home_score_val} (tie)"
    facts.append(score_sentence)

    # Pitch quality
    if velo:      facts.append(f"Avg fastball velocity: {velo} mph")
    if csw:       facts.append(f"CSW (called strikes + whiffs): {csw}%")
    if fp_pct:    facts.append(f"First-pitch strike rate: {int(fp_pct)}%")
    if pitch_cnt: facts.append(f"Pitch count: {pitch_cnt}")
    if dominant_pitch: facts.append(f"Dominant pitch: {dominant_pitch}")
    if secondary_pitch: facts.append(f"Key secondary pitch: {secondary_pitch}")

    # Game flow
    scs = safe_int(flow.get("scoreless_to_start", 0), 0)
    if scs >= 3:  facts.append(f"Opened with {scs} consecutive scoreless innings")
    if flow.get("settled_after_rough"):
        facts.append("Rough first inning, then settled down")
    longest = safe_int(flow.get("longest_scoreless_streak", 0), 0)
    if longest >= 4 and longest != scs:
        facts.append(f"Best stretch: {longest} consecutive scoreless innings mid-outing")

    # Damage inning detail — always surface when there was a bad inning
    damage_inning     = flow.get("damage_inning")
    damage_inning_runs = safe_int(flow.get("damage_inning_runs", 0), 0)
    error_contrib     = bool(flow.get("error_contributed"))
    if damage_inning and damage_inning_runs >= 2:
        inning_ord = {1:"1st",2:"2nd",3:"3rd"}.get(damage_inning, f"{damage_inning}th")
        error_note = ", which included an error" if error_contrib else ""
        facts.append(f"Gave up {damage_inning_runs} runs in the {inning_ord}{error_note}")
        if flow.get("only_damage_in_one_inning"):
            facts.append("That was the only inning where runs scored")
    elif flow.get("only_damage_in_one_inning") and er > 0:
        facts.append("All runs came in a single inning")
        if error_contrib:
            facts.append("An error contributed to the damage in that inning")
    elif error_contrib:
        facts.append("An error contributed to runs scoring during his outing")

    if flow.get("bullpen_blew_inherited"):
        facts.append("Bullpen let his inherited runners score after he exited")
    if garbage_runs > 0 and garbage_runs >= er:
        facts.append("Most/all earned runs came in garbage time (team led by 4+)")
    elif lev_damage >= 2:
        facts.append(f"{lev_damage} runs allowed while game was within 2")
    if stranded >= 4:
        facts.append(f"Stranded {stranded} runners")

    # Key play descriptions from the feed (HRs, multi-run plays, errors)
    key_plays = flow.get("key_play_descriptions", [])
    if key_plays:
        facts.append(f"Notable plays: {' | '.join(key_plays[:3])}")

    # Ballpark context
    venue_name = game_context.get("venue_name", "")
    park_type = classify_ballpark(venue_name)
    if park_type == "hitter" and venue_name:
        facts.append(f"Ballpark: {venue_name} is a hitter-friendly environment — context matters for this line")
    elif park_type == "pitcher" and venue_name:
        facts.append(f"Ballpark: {venue_name} is a pitcher-friendly environment — context matters for this line")
    if pitch_cnt and flow.get("innings_sequence"):
        innings_pitched_count = len(flow.get("innings_sequence", []))
        if innings_pitched_count >= 4:
            p_per_inning = pitch_cnt / innings_pitched_count
            if p_per_inning <= 12.0:
                facts.append(f"Pitch efficiency: {p_per_inning:.1f} pitches per inning (very efficient)")
            elif p_per_inning >= 18.0:
                facts.append(f"Pitch efficiency: {p_per_inning:.1f} pitches per inning (labored)")

    # Pitch mix shift
    shifts = p.get("pitch_mix_shift", [])
    PITCH_NAMES_FULL = {
        "FF": "four-seam fastball", "FT": "two-seam fastball", "SI": "sinker",
        "FC": "cutter", "SL": "slider", "ST": "sweeper", "CU": "curveball",
        "KC": "knuckle curve", "CH": "changeup", "FS": "splitter",
    }
    for shift in shifts[:2]:
        pname = PITCH_NAMES_FULL.get(shift["code"], shift["code"])
        direction = "more" if shift["direction"] == "up" else "less"
        facts.append(
            f"Pitch mix shift: used {shift['tonight_pct']}% {pname} tonight vs "
            f"{shift['season_pct']}% season average ({shift['delta']:+d}% — leaned {direction} than usual)"
        )

    # Platoon split
    platoon = p.get("platoon_context", {})
    if platoon.get("notable_split"):
        facts.append(f"Platoon split: {platoon['split_description']}")

    # Win probability context at exit
    exit_wp = flow.get("exit_win_probability")
    if exit_wp is not None:
        if exit_wp >= 0.80:
            facts.append(f"Left with team's win probability at {int(exit_wp*100)}% — handed over a comfortable lead")
        elif exit_wp <= 0.30:
            facts.append(f"Left with team's win probability at {int(exit_wp*100)}% — team was in a tough spot at exit")

    # Career debut
    if p.get("is_career_debut"):
        facts.append("CAREER DEBUT — first major league appearance")

    # Contact
    if hrs >= 2:   facts.append(f"Gave up {hrs} home runs")
    elif notable_hr:
        facts.append(f"Gave up a home run to {notable_hr['name']} ({notable_hr['season_hrs']} HR this season)")
    if xbh >= 3:   facts.append(f"{xbh} extra-base hits (non-HR) allowed")
    elif hrs == 0 and xbh == 0 and h >= 5:
        facts.append("All hits were singles — no real power damage")

    # Opponent quality
    if opp_tier:  facts.append(f"Opponent offense this season: {opp_tier}")

    # Form / streak
    if form_notes: facts.append(f"Recent form: {'; '.join(form_notes)}")

    # Context flags
    if is_rivalry: facts.append(f"Rivalry game ({team} vs. {opp_name})")
    if is_debut:   facts.append(f"First start of the {season} season")
    if is_return:  facts.append("Return from likely IL stint (no recent appearances found)")
    if game_context.get("day_night") == "day": facts.append("Day game")

    # Next start
    if next_start_note: facts.append(f"Next start: {next_start_note}")

    context_block = "\n".join(facts)

    # Cap logic: base is 3 (or 4 for GEM/DOMINANT), raise by 1 if there's a
    # damage inning or error worth explaining, max 5 to stay within Discord limits
    pitcher_is_opener = bool(p.get("is_opener"))
    if pitcher_is_opener:
        facts.insert(1, "Role: opener (reliever used to start the game, not a traditional starter)")
        context_block = "\n".join(facts)
        cap = 2
    else:
        base_cap = 4 if label in {"GEM", "DOMINANT"} else 3
        has_trouble_inning = damage_inning_runs >= 3 or error_contrib
        cap = min(base_cap + (1 if has_trouble_inning else 0), 5)

    # Static system instructions — eligible for prompt caching (paid once, reused)
    system_instructions = """You write the Summary field for a fantasy baseball Discord bot card recapping a starting pitcher's outing. You write in the voice of a conversational beat writer — quick, sharp, knowledgeable. Think Bob Nightengale or Ken Rosenthal filing a quick post-game note, not an AI assistant summarizing data.

The most important rule: write about the game, not the stats. Stats are supporting color. The story is what actually happened on the field — which inning turned things, what pitch got hit hard, when the pitcher found his rhythm or lost it. A reader should feel like they got a quick debrief from someone who watched the game, not a summary of a box score.

Core rules:
- Lead with something that happened in the game — a specific inning, a sequence, a turning point — not a stat
- Stats can appear but always as evidence of something that happened, never as the subject of a sentence
- Do not repeat raw stats already in the Game Line field (IP, H, ER, BB, K) — reference them indirectly
- ERA may be referenced in the closing only if stated as a complete thought — never write "his ERA sits" or "ERA heading into" without completing the sentence with the actual number; if avoiding the number feels awkward, rephrase the closing entirely
- Opponent quality from the facts (e.g. "above average offense", "below average offense") informs your framing but the label itself is banned from the prose — never write "a below-average offense", "an above-average lineup", "a weak offense", or any variation; instead describe what the lineup actually did or did not do tonight
- Do not start two consecutive sentences with the same word
- No dashes of any kind — no em dash (—), no en dash (–), no spaced hyphen ( - ) — use commas, periods, or rewrite the sentence instead
- Third person, past tense, plain prose — no bullet points, no markdown
- Output only the summary paragraph, nothing else, with no ellipsis (...)
- When mentioning the score, write it naturally: "the Twins beat Detroit 8-6" or "Minnesota won 8-6" — winner first, never abbreviations like MIN or DET in a sentence

Filler and AI-sounding language — never use any of these:
- Transition phrases: "Digging deeper", "When you look at", "What stands out", "What makes this interesting", "At the end of the day", "All in all", "Make no mistake", "When it was all said and done", "Worth noting", "It is worth mentioning"
- Hollow openers: "In a night that", "In what was", "On a night when", "In a performance that", "Getting ahead of hitters was the defining problem"
- Explanation chains: do not attach "which means", "which suggests", "which explains", "which indicates" to every stat — state the fact and move on. One explained connection per summary is enough
- No "he showed" or "he demonstrated" — say what happened
- No "perhaps", "arguably", "it could be said"
- No "underlying numbers", "underlying performance", "underlying metrics" — too clinical
- No "in X% of his attempts", "in fewer than half his attempts", "on X out of Y opportunities", "in X of Y tries" — never frame stats as attempts or opportunities. Say "he got ahead of fewer than half the hitters" or "a 48% first-pitch strike rate" instead

Closing sentence rules:
- Vary the closing every card — no two cards should end the same way
- Good options: a sharp one-line fact about what happened, a single consequence of the outing, a question the start leaves open, a look-ahead to the next start if one is listed in the facts, a blunt observation about the pitcher's night
- Keep the closing under 20 words when possible — short endings land harder than long ones
- Never close with the formula "Until X improves, outings like this will keep happening" — that is a cliché
- Never close with a long conditional sentence that diagnoses what the pitcher needs to fix — that is analysis, not writing
- A good closing sounds like something you would say out loud after watching the game, not something you would write in a report

Sentence variety:
- Actively mix sentence lengths — at least one sentence under 10 words, at least one over 20 words
- Do not start every sentence with a noun phrase — use participial phrases, prepositional phrases, or dependent clauses to open some sentences

Innings pitched language rules (strictly follow these):
- 7+ innings = "went deep", "worked deep into the game", "gave the team length", "a deep outing"
- 6 innings = "a quality start", "six solid innings", "gave the team six innings" — never "went deep"
- 5 innings or fewer = "a shortened start", "could not get deep", "did not give the team length" — never "went deep"
- Never describe any outing under 7 innings as "going deep" or "working deep into the game"

Damage inning rules:
- If the facts mention a specific inning with 3+ runs: discuss that inning specifically — what happened, why it snowballed
- If key plays are provided: weave the most relevant one naturally into the summary
- When referencing a hit from the key plays, always preserve the hit type — if the play says "doubles", write double; if it says "homers", write home run; if it says "singles", write single. Never reduce a double or home run to just "a line drive" or "a fly ball"
- If an error contributed: mention it factually without over-dramatizing
- Do not use the word "collapse" to describe an inning unless 5 or more runs scored in it
- Do not use "implode", "meltdown", or similar catastrophic language for 2-3 run innings

Season debut rules:
- If the facts say "First start of the [year] season": frame it as opening their season, not as a debut
- Do not imply it is a career debut or rookie appearance
- If the facts say "CAREER DEBUT": this IS his first major league start — treat it as a significant milestone

Pitch mix shift rules:
- If the facts include a pitch mix shift: weave it in as something that happened tactically — "he leaned on his slider far more than usual" not "pitch mix data shows a shift"
- Describe it as an observer would, not as an analyst reading a chart

Pitch naming rules:
- If the facts include a "Key secondary pitch": you may name it naturally when it fits the story — "the slider was the strikeout pitch tonight" or "he kept hitters honest with the changeup" — but only if it is relevant, not just because it appears in the facts
- If the facts do not provide a specific secondary pitch, do not invent one or default to mentioning the four-seam fastball

Ballpark rules:
- Only use the ballpark fact if it genuinely explains why the run total was higher or lower than expected — a pitcher-friendly park with 7 runs allowed, or a hitter-friendly park with a gem
- Do not mention it as a side note or "which makes the damage stand out even more" aside — if it does not change how you read the line, leave it out

CRITICAL — Do not use any career or experience labels (rookie, veteran, ace, young pitcher, sophomore) that are not explicitly stated in the facts.

Subject line rules:
- Write a subject line that reads like a tweet or press-box one-liner — specific, sharp, under 12 words
- Reference the opponent or a key moment from the game — never write a generic label like "has a strong outing" or "struggles on the mound"
- The subject line should tell the story of the night in one line, not just categorize it
- Do not include an emoji in the subject line — it will be added automatically
- No punctuation at the end of the subject line
- Never include a final score in the subject line — it is already shown in the Score field on the card; writing it risks getting it wrong
- Examples of the right tone: "deGrom carves up the Dodgers with nine strikeouts", "Cease gives the White Sox six before the bullpen blows it", "Burnes walks four and hands Atlanta the game in the fifth"
"""

    # --- Narrative angle: rotate based on seed so each card leads differently ---
    last_name = name.split()[-1] if name else name
    first_name = name.split()[0] if name else name

    # Label-specific tone instruction
    tone_by_label = {
        "GEM":       "Tone: this was a masterclass. Write with quiet admiration — not hype, just the steady recognition that something special happened tonight.",
        "DOMINANT":  "Tone: this pitcher was in command. Write with authority — the lineup never had a real answer and the summary should feel that way.",
        "QUALITY":   "Tone: this was a job well done. Write with workmanlike respect — he gave his team what it needed and that matters.",
        "SHARP":     "Tone: this outing was clean and efficient. Write with appreciation for the craft — few wasted pitches, few wasted words.",
        "STRIKEOUT": "Tone: the swing-and-miss was the story. Lead there — the stuff was electric even if the command came and went.",
        "SOLID":     "Tone: this was a steady, useful start. Write matter-of-factly — he did his job without fireworks and that is fine.",
        "UNEVEN":    "Tone: this was a grind. Write with honest ambivalence — good stretches and bad ones, and the line somewhere in between.",
        "SHORT":     "Tone: this outing did not go as long as needed. Write with straightforward honesty — it was a short night and the bullpen had to pick up the slack.",
        "ROUGH":     "Tone: this was a tough night. Write with honest accountability — do not pile on but do not sugarcoat either.",
        "NO_COMMAND":"Tone: the walks were the story. Write with frustration on behalf of the defense — the stuff may have been there but the zone was not.",
        "HIT_HARD":  "Tone: contact was the issue. Write with clear eyes — the hard contact was real and the line reflects it.",
    }
    tone_instruction = tone_by_label.get(label, "Tone: conversational, honest, knowledgeable.")

    # Narrative angle pool — keyed by label group, picked by seed
    good_angles = [
        f"Lead with what made the stuff work tonight — was it command, a specific pitch, or something in the sequencing?",
        f"Lead with how the game felt from the hitter's side — what made this pitcher so difficult to square up?",
        f"Lead with the moment that defined the outing — the inning or at-bat where it became clear this was his night.",
        f"Lead with the pitch that carried him — identify the one offering that the lineup could not solve.",
        f"Lead with what this outing means for his place in the rotation right now.",
        f"Lead with how efficiently he worked — pitches per inning, getting ahead early, never letting counts get away.",
        f"Lead with the texture of how he got outs — was it swing-and-miss, soft contact, pounding the zone?",
        f"Lead with the competitive context — who was in the lineup, how stiff the test was, and how he handled it.",
    ]
    bad_angles = [
        f"Lead with what went wrong and when — identify the turning point in the outing.",
        f"Lead with what the lineup figured out — what did hitters do to get on base and score?",
        f"Lead with the damage inning if there was one — what happened in that frame and why could he not stop it?",
        f"Lead with command — was he ahead in counts or falling behind? That usually explains everything.",
        f"Lead with what the final line does not fully capture — was it better or worse than the numbers suggest?",
        f"Lead with what needs to change for his next start — what was the core issue tonight?",
        f"Lead with the contrast between his good stretches and his bad ones — what derailed the outing?",
    ]
    mixed_angles = [
        f"Lead with the inning that mattered most — good or bad, one frame usually defines a start like this.",
        f"Lead with what he did well and what ultimately cost him.",
        f"Lead with how the game unfolded — did he look sharp early, or did he have to find it as the night went on?",
        f"Lead with how he matched up against this specific lineup tonight.",
        f"Lead with whether the stuff was there and whether the command matched it.",
    ]

    if label in {"GEM", "DOMINANT", "QUALITY", "SHARP"}:
        angle_pool = good_angles
    elif label in {"ROUGH", "NO_COMMAND", "HIT_HARD"}:
        angle_pool = bad_angles
    else:
        angle_pool = mixed_angles

    angle = angle_pool[(seed // 7) % len(angle_pool)]

    # Sentence opener ban — use actual pitcher name to prevent lazy openers
    opener_ban = (
        f"Do not start any sentence with: He, It, This, That, The pitcher, {first_name}, {last_name}. "
        f"Find a different subject or construction for each sentence."
    )

    opp_name = (
        team_name_from_abbr(game_context.get("home_abbr"))
        if p.get("side") == "away"
        else team_name_from_abbr(game_context.get("away_abbr"))
    )

    user_message = f"""Facts about the outing:
{context_block}

Opponent tonight: {opp_name}

{tone_instruction}
Narrative angle: {angle}
{opener_ban}

Write exactly {cap} sentences for the summary.

Respond with valid JSON only, no markdown, no extra text:
{{"subject": "<subject line here>", "summary": "<summary paragraph here>"}}
"""

    try:
        log(f"Calling Claude API for {name} card...")
        client_ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = await asyncio.to_thread(
            client_ai.messages.create,
            model=CLAUDE_MODEL,
            max_tokens=550,
            system=[
                {
                    "type": "text",
                    "text": system_instructions,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_message}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown code fences if present
        raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.IGNORECASE)
        raw = re.sub(r'\s*```$', '', raw)
        parsed = json.loads(raw)
        subject = str(parsed.get("subject") or "").strip().rstrip(".!?:;,-")
        summary = str(parsed.get("summary") or "").strip()
        if subject and summary:
            # Hard strip dash variants from both fields
            for pattern, repl in [(r'\s*[—–]\s*', ', '), (r'\s+-\s+', ', ')]:
                subject = re.sub(pattern, repl, subject)
                summary = re.sub(pattern, repl, summary)
            subject = subject.strip(', ')
            summary = summary.strip(', ')
            log(f"Claude card generated for {name} (subject: {len(subject)} chars, summary: {len(summary)} chars)")
            return {"subject": subject, "summary": summary}
        log(f"Claude card for {name} returned incomplete fields — falling back to templates")
    except Exception as e:
        log(f"Claude card failed for {name}: {e} — falling back to templates")

    return None


async def post_card(
    channel,
    p: dict,
    game_context: dict,
    score_value: str,
    recent_appearances=None,
    opp_hitting: dict | None = None,
    next_start: dict | None = None,
    season: int = 0,
):
    stats = p["stats"]
    label = classify_starter(stats)
    seed = build_starter_summary_seed(p["name"], stats, game_context)

    LABEL_EMOJI = {
        "GEM": "🔥", "DOMINANT": "🔥", "STRIKEOUT": "🎯",
        "QUALITY": "✅", "SHARP": "✅", "SOLID": "📈",
        "UNEVEN": "📉", "HIT_HARD": "💥", "NO_COMMAND": "🧭",
        "ROUGH": "⚠️", "SHORT": "⚠️",
    }
    label_emoji = LABEL_EMOJI.get(label, "📈")

    # Career debut overrides subject line entirely — skip Claude for this
    is_career_debut = p.get("is_career_debut")
    if is_career_debut:
        name = p.get("name", "This pitcher")
        team_name = team_name_from_abbr(p.get("team", ""))
        debut_subjects = [
            f"🌟 {name} makes his major league debut for the {team_name}",
            f"🌟 {name} takes the mound for the first time in the big leagues",
            f"🌟 {name} debuts for the {team_name}",
        ]
        subject = debut_subjects[seed % len(debut_subjects)]
        claude_subject = None
    else:
        claude_subject = None

    # Try Claude API for both subject and summary in one call.
    # Career debuts always use the template subject but still get a Claude summary.
    claude_result = await build_claude_card(
        p, label, game_context,
        recent_appearances=recent_appearances,
        opp_hitting=opp_hitting,
        next_start=next_start,
        season=season,
        seed=seed,
    )

    if claude_result:
        claude_subject = claude_result.get("subject", "")
        summary = claude_result.get("summary", "")
    else:
        summary = ""

    # Subject line: career debuts always use template; otherwise prefer Claude
    if not is_career_debut:
        if claude_subject:
            subject = f"{label_emoji} {claude_subject}"
        else:
            subject = build_starter_subject_line(p, label, game_context, seed)

    # Summary: fall back to templates if Claude did not return one
    if not summary:
        summary = build_starter_summary(
            p, label, game_context,
            recent_appearances=recent_appearances,
            opp_hitting=opp_hitting,
            next_start=next_start,
            season=season,
        )

    # Build QS streak field if pitcher is on a streak of 3+
    streak_field_value = None
    if recent_appearances and label in GOOD_STARTER_LABELS:
        def _is_qs(a): return safe_float(a.get("ip","0"),0) >= 6.0 and safe_int(a.get("er",0),0) <= 3
        tonight_ip  = safe_float(stats.get("inningsPitched","0.0"), 0.0)
        tonight_er  = safe_int(stats.get("earnedRuns", 0), 0)
        tonight_qs  = tonight_ip >= 6.0 and tonight_er <= 3
        streak = 0
        for app in ([{"ip": str(tonight_ip), "er": tonight_er}] + list(recent_appearances)):
            if _is_qs(app): streak += 1
            else: break
        if streak >= 3:
            streak_field_value = f"🔥 {streak} straight quality starts"

    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])
    embed.add_field(name="", value=f"**{subject}**", inline=False)
    embed.add_field(name="Summary", value=summary, inline=False)
    embed.add_field(name="Game Line", value=format_starter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_starter_season_line(p.get("season_stats", {})), inline=False)
    if streak_field_value:
        embed.add_field(name="Streak", value=streak_field_value, inline=False)
    embed.add_field(name=f"{score_field_emoji(game_context)} Score", value=score_value, inline=False)
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

            cleanup_starter_caches()

            # Clear next_start_cache each cycle so stale probable data doesn't persist
            next_start_cache.clear()

            # Count how many cards already posted today to enforce daily cap
            MAX_POSTS_PER_DAY = 30
            today_game_ids = {str(g.get("gamePk")) for g in games if g.get("gamePk")}
            posted_today = sum(
                1 for key in posted
                if key.split("_")[0] in today_game_ids
            )
            log(f"{posted_today}/{MAX_POSTS_PER_DAY} cards posted today so far")

            for g in games:
                if posted_today >= MAX_POSTS_PER_DAY:
                    log(f"Daily cap of {MAX_POSTS_PER_DAY} reached — stopping for today")
                    break

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

                # day/night from feed
                game_datetime = feed.get("gameData", {}).get("datetime", {})
                day_night = str(game_datetime.get("dayNight") or "").lower()

                # Venue name for ballpark context
                venue_name = str(feed.get("gameData", {}).get("venue", {}).get("name") or "").strip()

                game_context = {
                    "away_abbr": away_abbr,
                    "home_abbr": home_abbr,
                    "away_score": away_score,
                    "home_score": home_score,
                    "score_display": score_value,
                    "day_night": day_night,
                    "venue_name": venue_name,
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
                        log(f"Skipping {p['name']} | {p['team']} — already posted")
                        continue

                    if posted_this_game >= MAX_STARTER_CARDS_PER_GAME:
                        log(f"Skipping {p['name']} | {p['team']} — per-game cap ({MAX_STARTER_CARDS_PER_GAME}) reached")
                        break

                    # Opponent hitting is the other team's stats from this pitcher's perspective
                    opp_hitting = home_hitting if p.get("side") == "away" else away_hitting

                    recent_appearances = await asyncio.to_thread(
                        get_recent_appearances, pid, game_date_et, limit=5, max_days=45
                    )
                    # Resolve opener status now that we have recent appearances
                    p["is_opener"] = is_opener(p, recent_appearances)
                    if p["is_opener"]:
                        log(f"  -> Identified as opener based on recent appearance pattern")

                    # Resolve season pitch mix shift
                    season_mix = await asyncio.to_thread(
                        get_season_pitch_mix, pid, season
                    )
                    p["pitch_mix_shift"] = compute_pitch_mix_shift(
                        p.get("pitch_type_counts", {}), season_mix or {}
                    )

                    # Detect career debut (GS=1 this season AND negligible career IP)
                    gs_this_season = safe_int(p.get("season_stats", {}).get("gamesStarted", 0), 0)
                    if gs_this_season == 1 and not recent_appearances:
                        career_ip = await asyncio.to_thread(get_career_ip, pid)
                        tonight_ip = safe_float(p["stats"].get("inningsPitched", "0.0"), 0.0)
                        p["is_career_debut"] = (career_ip is None or career_ip <= tonight_ip + 1.0)
                    else:
                        p["is_career_debut"] = False
                    next_start = await asyncio.to_thread(
                        get_next_start, pid, p.get("team", ""), game_date_et
                    )
                    log(f"Posting {p['name']} | {p['team']} | {score_value}")
                    await post_card(
                        channel, p, game_context, score_value,
                        recent_appearances=recent_appearances,
                        opp_hitting=opp_hitting,
                        next_start=next_start,
                        season=season,
                    )
                    posted.add(key)
                    posted_this_game += 1
                    posted_today += 1
                    # Stagger between cards to avoid channel spam
                    await asyncio.sleep(POST_STAGGER_SECONDS)

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

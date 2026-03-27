import asyncio
import json
import os
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord
import requests

from utils.closer_depth_chart import fetch_closer_depth_chart
from utils.closer_tracker import build_tracked_relief_map, normalize_name

# ---------------- CONFIG ----------------

TOKEN = os.getenv("ANALYTIC_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("STARTER_WATCH_CHANNEL_ID", "0"))

STATE_FILE = "state/starter/state.json"
os.makedirs("state/starter", exist_ok=True)

ET = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = 10
RESET_CLOSER_STATE = os.getenv("RESET_STARTER_STATE", "").lower() in {"1", "true", "yes"}
DEPTH_CHART_OVERRIDE_CHANNEL_ID = int(os.getenv("DEPTH_CHART_OVERRIDE_CHANNEL_ID", "1484232761597366412"))
TREND_STATE_FILE = "state/starter/trend_state.json"

TREND_FAMILY_COOLDOWN_MINUTES = {
    "scoreless": 180,
    "strikeout": 90,
    "role": 90,
    "dominance": 90,
    "usage": 90,
    "command": 90,
    "rough": 90,
    "misc": 90,
}
TREND_RANDOM_INTERVAL_MIN_MINUTES = 6
TREND_RANDOM_INTERVAL_MAX_MINUTES = 16
VELOCITY_DELTA_THRESHOLD = 1.0
VELOCITY_MIN_PITCHES = 10
VELOCITY_MIN_FASTBALLS = 3
FASTBALL_PITCH_CODES = {"FF", "FT", "SI", "FC", "FA", "FS"}

# ---------------- TEAM STYLE ----------------

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

appearance_cache = {}
pitching_stats_cache = {}
player_meta_cache = {}

ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
player_headshot_index = None


def log(msg: str):
    print(f"[STARTER] {msg}", flush=True)


def normalize_team_abbr(team: str) -> str:
    key = str(team or "").strip().upper()
    alias_map = {
        "AZ": "ARI",
        "ARI": "ARI",
        "CHW": "CWS",
        "CWS": "CWS",
        "WAS": "WSH",
        "WSN": "WSH",
        "WSH": "WSH",
        "TBR": "TB",
        "TB": "TB",
        "KCR": "KC",
        "KC": "KC",
        "SDP": "SD",
        "SD": "SD",
        "SFG": "SF",
        "SF": "SF",
        "OAK": "ATH",
        "ATH": "ATH",
    }
    return alias_map.get(key, key)


def get_logo(team: str) -> str:
    normalized_team = normalize_team_abbr(team)
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
    key = logo_key_map.get(normalized_team, normalized_team.lower())
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"


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

    norm_from_tracker = normalize_name(name)
    if norm_from_tracker and norm_from_tracker != name:
        tracker_exact = choose_headshot_entry(index.get(norm_from_tracker), team)
        if tracker_exact:
            return tracker_exact.get("headshot_url")
        tracker_normalized = choose_headshot_entry(index.get(normalize_lookup_name(norm_from_tracker)), team)
        if tracker_normalized:
            return tracker_normalized.get("headshot_url")

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
    base = {"posted": [], "trend_posted": {}, "trend_history": {}, "trend_last_post_at": None, "trend_next_eligible_at": None, "trend_post_count_by_hour": {}, "trend_total_by_date": {}, "trend_family_last_post_at": {}, "velocity_posted": {}}

    if RESET_CLOSER_STATE:
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

    if os.path.exists(TREND_STATE_FILE):
        try:
            with open(TREND_STATE_FILE, "r", encoding="utf-8") as f:
                tdata = json.load(f)
            if isinstance(tdata, dict):
                for key in ["trend_posted", "trend_history", "trend_last_post_at", "trend_next_eligible_at", "trend_post_count_by_hour", "trend_total_by_date", "trend_family_last_post_at", "velocity_posted"]:
                    if key in tdata:
                        base[key] = tdata[key]
        except Exception:
            pass

    return base


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"posted": state.get("posted", [])}, f, indent=2)

    trend_payload = {
        "trend_posted": state.get("trend_posted", {}),
        "trend_history": state.get("trend_history", {}),
        "trend_last_post_at": state.get("trend_last_post_at"),
        "trend_next_eligible_at": state.get("trend_next_eligible_at"),
        "trend_post_count_by_hour": state.get("trend_post_count_by_hour", {}),
        "trend_total_by_date": state.get("trend_total_by_date", {}),
        "trend_family_last_post_at": state.get("trend_family_last_post_at", {}),
        "velocity_posted": state.get("velocity_posted", {}),
    }
    with open(TREND_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(trend_payload, f, indent=2)


# ---------------- BASIC HELPERS ----------------

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


def plural(word: str, count: int) -> str:
    return word if count == 1 else f"{word}s"

def baserunner_count(s: dict) -> int:
    return safe_int(s.get("h", 0), 0) + safe_int(s.get("bb", 0), 0) + safe_int(s.get("hbp", 0), 0)



NUMBER_WORDS = {
    0: "zero",
    1: "one",
    2: "two",
    3: "three",
    4: "four",
    5: "five",
    6: "six",
    7: "seven",
    8: "eight",
    9: "nine",
    10: "ten",
    11: "eleven",
    12: "twelve",
}


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


def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def baseball_ip_to_outs(ip: str) -> int:
    """
    Baseball IP format:
      1.0 = 3 outs
      1.1 = 4 outs
      1.2 = 5 outs
    """
    text = str(ip).strip()
    if not text:
        return 0

    if "." not in text:
        return safe_int(text, 0) * 3

    whole_str, frac_str = text.split(".", 1)
    whole = safe_int(whole_str, 0)
    frac = safe_int(frac_str, 0)
    frac = min(frac, 2)
    return whole * 3 + frac


def format_ip_for_line(ip: str) -> str:
    text = str(ip).strip()

    if text.endswith(".0"):
        return f"{safe_int(float(text), 0)} IP"

    if text.endswith(".1"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅓ IP" if whole == 0 else f"{whole}⅓ IP"

    if text.endswith(".2"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅔ IP" if whole == 0 else f"{whole}⅔ IP"

    return f"{text} IP"


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
        whole = safe_int(float(text), 0)
        return f"{whole} innings"

    if text.endswith(".1"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅓ of an inning" if whole == 0 else f"{whole}⅓ innings"

    if text.endswith(".2"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅔ of an inning" if whole == 0 else f"{whole}⅔ innings"

    return f"{text} innings"


def format_game_line(s: dict) -> str:
    return f"{format_ip_for_line(s['ip'])} • {s['h']} H • {s['er']} ER • {s['bb']} BB • {s['k']} K"


def format_pitch_count(stats: dict) -> str:
    pitches = safe_int(stats.get("numberOfPitches", 0), 0)
    strikes = safe_int(stats.get("strikes", 0), 0)

    if pitches <= 0:
        return "N/A"
    if strikes <= 0:
        return f"{pitches} pitches"
    return f"{pitches} pitches • {strikes} strikes"


def build_score_line(away_abbr: str, away_score: int, home_abbr: str, home_score: int) -> str:
    if home_score > away_score:
        return f"{home_abbr} {home_score}, {away_abbr} {away_score}"
    return f"{away_abbr} {away_score}, {home_abbr} {home_score}"


def parse_game_date_et(game: dict):
    game_date = game.get("gameDate")
    if not game_date:
        return None
    try:
        dt = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
        return dt.astimezone(ET).date()
    except Exception:
        return None


def parse_pitch_type_code(event: dict) -> str:
    details = event.get("details", {}) if isinstance(event, dict) else {}
    pitch_type = details.get("type") if isinstance(details, dict) else None
    if isinstance(pitch_type, dict):
        return str(pitch_type.get("code") or pitch_type.get("description") or "").strip().upper()
    return ""


def get_fastball_velocity_summary(feed: dict, pitcher_id: int):
    if not feed or pitcher_id is None:
        return None

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    fastball_velos = []
    total_pitches = 0

    for play in plays:
        matchup = play.get("matchup", {})
        pitcher = matchup.get("pitcher", {})
        if pitcher.get("id") != pitcher_id:
            continue

        for event in play.get("playEvents", []):
            if not isinstance(event, dict):
                continue
            if not event.get("isPitch"):
                continue

            pitch_data = event.get("pitchData", {})
            start_speed = safe_float(pitch_data.get("startSpeed"), 0.0)
            if start_speed <= 0:
                continue

            total_pitches += 1
            pitch_code = parse_pitch_type_code(event)
            if pitch_code in FASTBALL_PITCH_CODES:
                fastball_velos.append(start_speed)

    if total_pitches < VELOCITY_MIN_PITCHES or len(fastball_velos) < VELOCITY_MIN_FASTBALLS:
        return None

    avg_fb = round(sum(fastball_velos) / len(fastball_velos), 1)
    return {
        "avg_fastball_velocity": avg_fb,
        "fastball_count": len(fastball_velos),
        "total_pitches": total_pitches,
    }


# ---------------- SEASON STATS ----------------

def format_season_line(season: dict) -> str:
    saves = safe_int(season.get("saves", 0), 0)
    holds = safe_int(season.get("holds", 0), 0)
    strikeouts = safe_int(season.get("strikeOuts", 0), 0)

    era = season.get("era") or season.get("earnedRunAverage") or "0.00"
    try:
        era = f"{float(era):.2f}"
    except Exception:
        era = "0.00"

    season_ip = str(season.get("inningsPitched", "0.0"))
    season_outs = baseball_ip_to_outs(season_ip)
    hits = safe_int(season.get("hits", 0), 0)
    walks = safe_int(season.get("baseOnBalls", 0), 0)

    whip = season.get("whip")
    if whip in (None, "", "-"):
        if season_outs > 0:
            ip_float = season_outs / 3.0
            whip = f"{((hits + walks) / ip_float):.2f}"
        else:
            whip = "0.00"
    else:
        try:
            whip = f"{float(whip):.2f}"
        except Exception:
            whip = "0.00"

    k9 = season.get("strikeoutsPer9Inn")
    if k9 in (None, "", "-"):
        if season_outs > 0:
            ip_float = season_outs / 3.0
            k9 = f"{(strikeouts * 9 / ip_float):.1f}"
        else:
            k9 = "0.0"
    else:
        try:
            k9 = f"{float(k9):.1f}"
        except Exception:
            k9 = "0.0"

    parts = []

    if saves > 0:
        parts.append(f"{saves} SV")
    if holds > 0:
        parts.append(f"{holds} HLD")

    parts.extend([
        f"{era} ERA",
        f"{whip} WHIP",
        f"{strikeouts} K",
        f"{k9} K/9",
    ])

    return " • ".join(parts)


# ---------------- CLASSIFICATION ----------------

def classify(s: dict) -> str:
    outs = baseball_ip_to_outs(s["ip"])
    baserunners = baserunner_count(s)

    if s.get("saves"):
        return "SAVE"

    if s.get("blownSaves"):
        return "BLOWN"

    if s.get("holds"):
        return "HOLD"

    if outs >= 3 and s["er"] == 0 and baserunners == 0:
        return "DOM"

    if s["er"] >= 3:
        return "ROUGH"

    if s["er"] == 0 and baserunners >= 1 and outs >= 3:
        return "TRAFFIC"

    if s["er"] == 0 and baserunners == 0 and outs >= 3:
        return "CLEAN"

    if s["er"] == 0:
        return "RELIEF"

    return "RELIEF"


def grade_outing(s: dict) -> str:
    outs = baseball_ip_to_outs(s["ip"])
    baserunners = baserunner_count(s)

    if outs <= 1:
        return "MICRO"

    if s["er"] >= 3:
        return "ROUGH"

    if s["er"] in {1, 2}:
        return "SHAKY"

    if s["er"] == 0 and baserunners == 0 and outs >= 3:
        return "DOMINANT"

    if s["er"] == 0 and baserunners == 0 and outs >= 3:
        return "CLEAN"

    if s["er"] == 0 and baserunners >= 1 and outs >= 3:
        return "TRAFFIC"

    return "NEUTRAL"


def impact_tag(label: str, s: dict) -> str:
    outs = baseball_ip_to_outs(s["ip"])

    if label == "SAVE":
        if outs >= 6:
            return "🧰 Finished the job"
        return "🔒 Locked it down"

    if label == "BLOWN":
        return "💥 Lead blown"

    if label == "HOLD":
        if s["er"] == 0:
            return "🧱 Held the line"
        return "⚠️ Hold with traffic"

    if label == "DOM":
        return "🔥 Dominant outing"

    if label == "TRAFFIC":
        return "⚠️ Navigated traffic"

    if label == "ROUGH":
        return "💀 Rough outing"

    if label == "CLEAN":
        return "🧊 Clean inning"

    return "⚾ Relief outing"


# ---------------- DEPTH CHART TRACKING ----------------

async def refresh_tracked_pitchers():
    try:
        teams = await fetch_closer_depth_chart(client, DEPTH_CHART_OVERRIDE_CHANNEL_ID)
        if not teams:
            log("Depth chart override returned no teams, using saved depth chart")
    except Exception as e:
        log(f"Depth chart override refresh failed: {e}")

    tracked = build_tracked_relief_map()
    log(f"Loaded {len(tracked)} tracked relievers from depth chart")
    return tracked


def find_tracked_pitcher_info(raw_name: str, team_abbr: str, tracked: dict):
    """
    Exact normalized name match first, with team validation.
    Fallback to unique last-name match, also with team validation.
    """
    norm = normalize_name(raw_name)
    if not norm:
        return None

    exact = tracked.get(norm)
    if exact and exact.get("team") == team_abbr:
        return exact

    last = norm.split()[-1] if norm else ""
    if not last:
        return None

    matches = []
    for tracked_norm, info in tracked.items():
        tracked_last = tracked_norm.split()[-1] if tracked_norm else ""
        if tracked_last == last and info.get("team") == team_abbr:
            matches.append(info)

    if len(matches) == 1:
        return matches[0]

    return None


def infer_role_from_tracked_info(tracked_info: dict) -> str:
    if not tracked_info:
        return "relief"

    explicit_role = str(tracked_info.get("role", "")).strip().lower()
    mapping = {
        "closer": "closer",
        "co-closer": "co_closer",
        "co closer": "co_closer",
        "committee": "committee",
        "setup": "setup",
        "leverage arm": "leverage_arm",
        "next in line": "committee",
        "second in line": "setup",
    }
    if explicit_role in mapping:
        return mapping[explicit_role]

    combined = " | ".join(str(v).strip().lower() for v in tracked_info.values() if v is not None)
    if "co-closer" in combined or "co closer" in combined:
        return "co_closer"
    if "committee" in combined:
        return "committee"
    if "leverage" in combined:
        return "leverage_arm"
    if "closer" in combined:
        return "closer"
    if "setup" in combined:
        return "setup"
    return "relief"

    # Conservative fallback:
    # tracked, but role not explicit -> setup mix language is safer than closer language.
    return "setup"


# ---------------- ENTRY CONTEXT ----------------

def get_pitcher_entry_context(feed: dict, pitcher_id: int, pitcher_side: str):
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    if not plays:
        return {
            "entry_phrase": "",
            "entry_outs_text": "",
            "entry_state_text": "",
            "entry_state_kind": "",
            "entry_margin": 0,
            "entry_inning": None,
            "finished_game": False,
        }

    pitcher_indices = []
    for idx, play in enumerate(plays):
        pitcher = play.get("matchup", {}).get("pitcher", {})
        if pitcher.get("id") == pitcher_id:
            pitcher_indices.append(idx)

    if not pitcher_indices:
        return {
            "entry_phrase": "",
            "entry_outs_text": "",
            "entry_state_text": "",
            "entry_state_kind": "",
            "entry_margin": 0,
            "entry_inning": None,
            "finished_game": False,
        }

    first_idx = pitcher_indices[0]
    last_idx = pitcher_indices[-1]
    first_play = plays[first_idx]

    about = first_play.get("about", {})
    inning = about.get("inning")
    half = about.get("halfInning", "")
    outs = safe_int(first_play.get("count", {}).get("outs", 0), 0)

    entry_phrase = ""
    if inning is not None and half:
        entry_phrase = f"in the {half.lower()} of the {ordinal(inning)}"

    if outs == 0:
        entry_outs_text = "with nobody out"
    elif outs == 1:
        entry_outs_text = "with one out"
    else:
        entry_outs_text = "with two outs"

    if first_idx > 0:
        prev_result = plays[first_idx - 1].get("result", {})
        prev_away = safe_int(prev_result.get("awayScore", 0), 0)
        prev_home = safe_int(prev_result.get("homeScore", 0), 0)
    else:
        prev_away = 0
        prev_home = 0

    if pitcher_side == "home":
        team_score = prev_home
        opp_score = prev_away
    else:
        team_score = prev_away
        opp_score = prev_home

    diff = team_score - opp_score
    abs_diff = abs(diff)

    if diff > 0:
        state_kind = "lead"
        if diff == 1:
            state_text = "holding a one-run lead"
        elif diff == 2:
            state_text = "holding a two-run lead"
        elif diff == 3:
            state_text = "holding a three-run lead"
        else:
            state_text = f"holding a {diff}-run lead"
    elif diff < 0:
        state_kind = "trailing"
        if abs_diff == 1:
            state_text = "trailing by one"
        elif abs_diff == 2:
            state_text = "trailing by two"
        else:
            state_text = f"trailing by {abs_diff}"
    else:
        state_kind = "tie"
        state_text = "in a tie game"

    return {
        "entry_phrase": entry_phrase,
        "entry_outs_text": entry_outs_text,
        "entry_state_text": state_text,
        "entry_state_kind": state_kind,
        "entry_margin": abs_diff,
        "entry_inning": inning,
        "finished_game": (last_idx == len(plays) - 1),
    }



def build_context_phrase(context: dict) -> str:
    bits = []
    if context.get("entry_phrase"):
        bits.append(context["entry_phrase"])
    if context.get("entry_outs_text"):
        bits.append(context["entry_outs_text"])
    if context.get("entry_state_text"):
        bits.append(context["entry_state_text"])

    if not bits:
        return "in relief"

    if len(bits) == 1:
        return bits[0]

    if len(bits) == 2:
        return f"{bits[0]} {bits[1]}"

    return f"{bits[0]} {bits[1]}, {bits[2]}"


# ---------------- RECENT APPEARANCES / TRENDS ----------------

def get_pitching_stats_for_date(target_date):
    if target_date in pitching_stats_cache:
        return pitching_stats_cache[target_date]

    stats_by_pitcher = {}

    try:
        r = requests.get(f"{SCHEDULE_URL}&date={target_date.isoformat()}", timeout=30)
        r.raise_for_status()
        data = r.json()

        games = []
        for date_block in data.get("dates", []):
            games.extend(date_block.get("games", []))

        for game in games:
            game_id = game.get("gamePk")
            if not game_id:
                continue

            try:
                feed = get_feed(game_id)
            except Exception:
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

                    velo = get_fastball_velocity_summary(feed, pid)
                    stats_by_pitcher[pid] = {
                        "ip": str(stats.get("inningsPitched", "0.0")),
                        "h": safe_int(stats.get("hits", 0), 0),
                        "er": safe_int(stats.get("earnedRuns", 0), 0),
                        "bb": safe_int(stats.get("baseOnBalls", 0), 0),
                        "k": safe_int(stats.get("strikeOuts", 0), 0),
                        "saves": safe_int(stats.get("saves", 0), 0),
                        "holds": safe_int(stats.get("holds", 0), 0),
                        "blownSaves": safe_int(stats.get("blownSaves", 0), 0),
                        "avg_fastball_velocity": velo.get("avg_fastball_velocity") if velo else None,
                        "fastball_count": velo.get("fastball_count", 0) if velo else 0,
                        "pitch_count": velo.get("total_pitches", safe_int(stats.get("numberOfPitches", 0), 0)) if velo else safe_int(stats.get("numberOfPitches", 0), 0),
                    }

    except Exception as e:
        log(f"Pitching stats cache load failed for {target_date}: {e}")

    pitching_stats_cache[target_date] = stats_by_pitcher
    return stats_by_pitcher


def get_recent_appearances(pitcher_id: int, game_date_et, limit=5, max_days=21):
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


def get_recent_trend(recent_appearances):
    if len(recent_appearances) < 3:
        return "NONE"

    scoreless_count = sum(1 for app in recent_appearances[:5] if app["er"] == 0)
    runs_count = sum(1 for app in recent_appearances[:5] if app["er"] > 0)

    first_three = recent_appearances[:3]
    if len(first_three) == 3 and all(app["er"] == 0 for app in first_three):
        return "UP"

    if scoreless_count >= 4 and len(recent_appearances) >= 5:
        return "UP"

    first_two = recent_appearances[:2]
    if len(first_two) == 2 and all(app["er"] >= 2 for app in first_two):
        return "DOWN"

    if runs_count >= 3 and len(recent_appearances) >= 5:
        return "DOWN"

    return "STABLE"


# ---------------- STREAK TRACKING ----------------

def get_pitcher_ids_for_date(target_date):
    if target_date in appearance_cache:
        return appearance_cache[target_date]

    pitcher_ids = set()

    try:
        stats_by_pitcher = get_pitching_stats_for_date(target_date)
        pitcher_ids = set(stats_by_pitcher.keys())
    except Exception as e:
        log(f"Appearance cache load failed for {target_date}: {e}")

    appearance_cache[target_date] = pitcher_ids
    return pitcher_ids


def get_streak_count(pitcher_id: int, game_date_et):
    if pitcher_id is None or game_date_et is None:
        return 0

    yesterday = game_date_et - timedelta(days=1)
    two_days_ago = game_date_et - timedelta(days=2)

    yesterday_ids = get_pitcher_ids_for_date(yesterday)
    two_days_ids = get_pitcher_ids_for_date(two_days_ago)

    if pitcher_id in yesterday_ids and pitcher_id in two_days_ids:
        return 3

    if pitcher_id in yesterday_ids:
        return 2

    return 0


def count_recent_appearances_in_window(pitcher_id: int, game_date_et, days: int = 15) -> int:
    if pitcher_id is None or game_date_et is None:
        return 0

    count = 1
    check_date = game_date_et - timedelta(days=1)
    for _ in range(max(days - 1, 0)):
        if pitcher_id in get_pitcher_ids_for_date(check_date):
            count += 1
        check_date -= timedelta(days=1)
    return count


def get_streak_sentence(streak_count: int) -> str:
    if streak_count == 2:
        return random.choice([
            "Second straight appearance.",
            "It was his second straight day of work.",
        ])
    if streak_count == 3:
        return random.choice([
            "Third straight appearance.",
            "It was his third straight day of work.",
        ])
    return ""


def ordinal_word(n: int) -> str:
    mapping = {1: "first", 2: "second", 3: "third", 4: "fourth", 5: "fifth"}
    return mapping.get(n, f"{number_word(n)}th")


def get_recent_usage_snapshot(pitcher_id: int, game_date_et):
    if pitcher_id is None or game_date_et is None:
        return {"pitched_yesterday": False, "pitched_two_days_ago": False, "apps_last4": 1, "apps_last6": 1}

    yesterday = game_date_et - timedelta(days=1)
    two_days_ago = game_date_et - timedelta(days=2)

    pitched_yesterday = pitcher_id in get_pitcher_ids_for_date(yesterday)
    pitched_two_days_ago = pitcher_id in get_pitcher_ids_for_date(two_days_ago)

    apps_last4 = 1
    check_date = yesterday
    for _ in range(3):
        if pitcher_id in get_pitcher_ids_for_date(check_date):
            apps_last4 += 1
        check_date -= timedelta(days=1)

    apps_last6 = 1
    check_date = yesterday
    for _ in range(5):
        if pitcher_id in get_pitcher_ids_for_date(check_date):
            apps_last6 += 1
        check_date -= timedelta(days=1)

    return {
        "pitched_yesterday": pitched_yesterday,
        "pitched_two_days_ago": pitched_two_days_ago,
        "apps_last4": apps_last4,
        "apps_last6": apps_last6,
    }


def build_usage_sentence(usage: dict) -> str:
    if not usage:
        return ""

    apps_last4 = safe_int(usage.get("apps_last4", 0), 0)
    apps_last6 = safe_int(usage.get("apps_last6", 0), 0)

    if apps_last4 >= 3:
        return random.choice([
            f"It was already his {ordinal_word(apps_last4)} appearance in four days, so the recent workload is worth keeping in mind.",
            f"He has now worked {number_word(apps_last4)} times in four days, which matters for short-term availability.",
            f"The recent usage has been fairly active, with {number_word(apps_last4)} appearances in four days.",
        ])

    if usage.get("pitched_yesterday"):
        return random.choice([
            "It was his second straight day of work.",
            "He was back out there after pitching yesterday.",
            "This came on back-to-back days for him.",
        ])

    if apps_last6 >= 4:
        return random.choice([
            f"He has been in the mix often lately, with {number_word(apps_last6)} appearances in six days.",
            f"This was already his {ordinal_word(apps_last6)} outing in six days, so the recent usage is starting to stack up.",
        ])

    return ""


def leverage_bucket(context: dict) -> str:
    if not context:
        return "neutral"

    inning = safe_int(context.get("entry_inning"), 0)
    state = context.get("entry_state_kind", "")
    margin = safe_int(context.get("entry_margin", 0), 0)

    if state == "tie" and inning >= 7:
        return "high"
    if state == "lead" and inning >= 8 and margin <= 2:
        return "high"
    if state == "lead" and inning >= 7 and margin <= 3:
        return "medium"
    if state == "trailing" and inning >= 7 and margin <= 2:
        return "medium"
    if margin >= 5:
        return "low"
    return "neutral"


def build_velocity_inline_sentence(velocity_alert: dict) -> str:
    if not velocity_alert:
        return ""

    current_v = velocity_alert.get("current_velocity")
    baseline_v = velocity_alert.get("baseline_velocity")
    delta = velocity_alert.get("delta")
    baseline_type = velocity_alert.get("baseline_type", "recent average")

    if current_v is None or baseline_v is None or delta is None:
        return ""

    change = abs(delta)
    if delta > 0:
        return random.choice([
            f"His fastball averaged {current_v:.1f} MPH here, up from {baseline_v:.1f} MPH in his {baseline_type}.",
            f"He also got a little extra life on the fastball, averaging {current_v:.1f} MPH after sitting at {baseline_v:.1f} MPH in his {baseline_type}.",
            f"The fastball ticked up to {current_v:.1f} MPH in this outing, a {change:.1f} MPH jump from his {baseline_type}.",
        ])

    return random.choice([
        f"His fastball averaged {current_v:.1f} MPH here, down from {baseline_v:.1f} MPH in his {baseline_type}.",
        f"The fastball backed up a bit to {current_v:.1f} MPH in this outing, a {change:.1f} MPH dip from his {baseline_type}.",
        f"He averaged {current_v:.1f} MPH on the fastball, which was a little below the {baseline_v:.1f} MPH mark from his {baseline_type}.",
    ])


def trend_window_for_code(code: str) -> int:
    if code in {"scoreless5", "scoreless4of5", "ks10last5", "runs3of5"}:
        return 5
    if code == "k_3_of_4":
        return 4
    if code == "saves2":
        return 2
    return 3


def outs_to_baseball_ip(outs: int) -> str:
    return f"{outs // 3}.{outs % 3}"


def summarize_trend_span(recent_appearances, code: str):
    window = trend_window_for_code(code)
    span = list(recent_appearances[:window])
    if not span:
        return {"window": window, "span": [], "outs": 0, "ip": "0.0", "k": 0, "bb": 0, "h": 0, "er": 0, "avg_fastball_velocity": None}

    outs = sum(baseball_ip_to_outs(app.get("ip", "0.0")) for app in span)
    velos = [safe_float(app.get("avg_fastball_velocity"), 0.0) for app in span if app.get("avg_fastball_velocity") is not None]
    return {
        "window": window,
        "span": span,
        "outs": outs,
        "ip": outs_to_baseball_ip(outs),
        "k": sum(safe_int(app.get("k", 0), 0) for app in span),
        "bb": sum(safe_int(app.get("bb", 0), 0) for app in span),
        "h": sum(safe_int(app.get("h", 0), 0) for app in span),
        "er": sum(safe_int(app.get("er", 0), 0) for app in span),
        "avg_fastball_velocity": round(sum(velos) / len(velos), 1) if velos else None,
    }


def extract_prior_season_velocity(season_stats: dict):
    if not isinstance(season_stats, dict):
        return None

    for key in [
        "avgFastballVelocity",
        "averageFastballVelocity",
        "fastballVelocity",
        "fourSeamFastballVelocity",
        "fbVelocity",
        "avg_fastball_velocity",
    ]:
        value = season_stats.get(key)
        if value not in (None, "", "-"):
            parsed = safe_float(value, 0.0)
            if parsed > 0:
                return round(parsed, 1)
    return None


def build_trend_stat_sentence(name: str, code: str, span_stats: dict):
    k_text = stat_phrase(span_stats.get("k", 0), "strikeout")
    bb_text = stat_phrase(span_stats.get("bb", 0), "walk")
    ip_text = f"{span_stats.get('ip', '0.0')} innings"
    window = span_stats.get("window", 3)

    if code in {"scoreless3", "scoreless5", "scoreless4of5"}:
        return random.choice([
            f"During that {window}-appearance stretch, he has {k_text} against {bb_text}.",
            f"He has covered {ip_text} during that run, with {k_text} and {bb_text}.",
            f"The stretch has come with {k_text} over {ip_text}, and only {bb_text}.",
        ])

    if code in {"ks10last5", "ks7last3", "k_streak3", "dominant_last3", "dominant_k_combo", "no_walk3", "k_3_of_4"}:
        return random.choice([
            f"That run has also come with {bb_text} over {ip_text}.",
            f"He has paired the swing-and-miss with {bb_text} over {ip_text} in that span.",
            f"Over that stretch, he has worked {ip_text} while keeping it to {bb_text}.",
        ])

    if code in {"runs2of3", "runs3of5", "scoreless_snapped", "first_rough_after_hot"}:
        return random.choice([
            f"Across that stretch, he has still managed {k_text}, but the run prevention has slipped.",
            f"The recent stretch covers {ip_text}, with {k_text} and {bb_text}, but too many runs have crossed.",
        ])

    return random.choice([
        f"Over that stretch, he has logged {ip_text} with {k_text} against {bb_text}.",
        f"That span has come with {k_text} and only {bb_text} over {ip_text}.",
    ])


def build_trend_velocity_sentence(name: str, span_stats: dict, season_stats: dict):
    span_v = span_stats.get("avg_fastball_velocity")
    season_v = extract_prior_season_velocity(season_stats)
    if span_v is None or season_v is None:
        return ""

    delta = round(span_v - season_v, 1)
    if abs(delta) < 0.7:
        return ""

    diff_text = f"{abs(delta):.1f} MPH"
    if delta > 0:
        return random.choice([
            f"His fastball has also averaged {span_v:.1f} MPH during that stretch, up {diff_text} from last season.",
            f"There has been a little more life on the fastball too, with that run coming at {span_v:.1f} MPH, or {diff_text} above last season.",
            f"That stretch has come with a fastball average of {span_v:.1f} MPH, which sits {diff_text} above last season.",
        ])

    return random.choice([
        f"The one thing to watch is the fastball, which has averaged {span_v:.1f} MPH during that stretch, down {diff_text} from last season.",
        f"That run has come with a fastball average of {span_v:.1f} MPH, which sits {diff_text} below last season.",
        f"The fastball has been a little lighter during that stretch, averaging {span_v:.1f} MPH and sitting {diff_text} below last season.",
    ])


# ---------------- LANGUAGE HELPERS ----------------

def strikeout_phrase(k: int) -> str:
    if k <= 0:
        return ""

    batter_text = stat_phrase(k, "batter")
    if k >= 3:
        return f"while punching out {batter_text}"

    return f"while striking out {batter_text}"


# ---------------- ANALYSIS ----------------

def build_analysis(p: dict, s: dict, label: str, context: dict, tracked_info: dict, recent_appearances):
    role = infer_role_from_tracked_info(tracked_info)
    outing_grade = grade_outing(s)
    trend = get_recent_trend(recent_appearances)
    leverage = leverage_bucket(context)

    inning = context.get("entry_inning")
    state_kind = context.get("entry_state_kind", "")
    early_closer_usage = (
        role == "closer"
        and inning is not None
        and inning < 9
        and state_kind in {"lead", "tie"}
    )

    if outing_grade == "MICRO":
        if role == "closer" and early_closer_usage:
            return random.choice([
                "He was called on early for a leverage pocket and got the out.",
                "He handled an early high-leverage matchup and recorded the out.",
                "The usage itself says a lot here, as he was trusted in an important spot before the ninth.",
            ])
        if leverage == "high":
            return random.choice([
                "He was asked to get one important out and did it.",
                "He got the key matchup in a leverage spot and finished the job.",
                "This was brief work, but it came in a meaningful moment.",
            ])
        return random.choice([
            "He got the one hitter he faced.",
            "He retired the lone batter he faced.",
            "He handled his brief assignment cleanly.",
        ])

    if outing_grade in {"SHAKY", "ROUGH"}:
        traffic = s["h"] + s["bb"]
        if label == "SAVE":
            return random.choice([
                "He got the save, but this was shakier than you would want from a closer.",
                "The save counts, though the outing itself left more damage than expected.",
                "He converted the chance, but the line was not especially crisp.",
            ])
        if label == "HOLD":
            if traffic >= 3:
                return random.choice([
                    "He still got the hold, but too much traffic built up for this to feel clean.",
                    "The hold is there, though the inning got away from him more than you would like.",
                    "He escaped with the hold, but it was a messy bridge inning.",
                ])
            return random.choice([
                "He got the hold, but this was not a clean outing.",
                "The hold is there, though the appearance itself was shakier than the label suggests.",
                "He picked up the hold, but the line did not come with much comfort.",
            ])
        if label == "BLOWN":
            return random.choice([
                "This was a costly miss in a leverage spot.",
                "He could not keep the inning under control when the game tightened up.",
                "It was a rough result in a moment that mattered.",
            ])
        if s["bb"] >= 2:
            return random.choice([
                "The command backed up on him, and the inning got loose in a hurry.",
                "Too many free passes put him in a bad spot here.",
                "The line points more to command trouble than clean execution.",
            ])
        if s["h"] >= 2:
            return random.choice([
                "He gave up too much contact for the outing to hold together.",
                "The traffic turned into real damage before he could settle in.",
                "There was too much contact here for this to qualify as a usable line.",
            ])
        if trend == "DOWN":
            return random.choice([
                "This was another uneven outing, and the recent form is starting to matter.",
                "He has been in a rougher stretch lately, and this one kept that going.",
                "The recent trend has been shaky, and this did not change it.",
            ])
        return random.choice([
            "This was more survival than execution.",
            "He got through part of the inning, but the line was far from sharp.",
            "The outing brought more trouble than help.",
        ])

    if role == "closer":
        if early_closer_usage and leverage in {"high", "medium"} and label in {"SAVE", "HOLD"}:
            return random.choice([
                "The early usage shows this was one of the biggest spots in the game, and he answered it.",
                "Being used before the ninth says plenty about the leverage of the moment, and he handled it.",
                "He got the toughest spot before the ninth and came through.",
            ])
        if label == "SAVE":
            if outing_grade == "DOMINANT":
                return random.choice([
                    "He still looks like the clear closer here, and outings like this only reinforce it.",
                    "He remains firmly in control of the ninth inning.",
                    "He keeps a strong grip on save chances in this bullpen.",
                ])
            return random.choice([
                "He still looks like the primary answer for saves here.",
                "He remains the top save arm in this bullpen.",
                "The role still runs through him in the ninth.",
            ])
        if label == "BLOWN":
            return random.choice([
                "One outing does not rewrite the hierarchy, but it does bring a little short-term pressure.",
                "The role may still be his, though this is the kind of outing that gets noticed.",
                "This does not erase his place, but it does turn up the attention for the next outing.",
            ])
        return random.choice([
            "He remains one of the key late-game arms in this bullpen.",
            "He still looks like a central leverage piece for this staff.",
            "He continues to work the innings that matter most.",
        ])

    if role == "committee":
        if label == "SAVE":
            return random.choice([
                "This keeps him firmly in the save mix.",
                "In a fluid bullpen, this outing helps his case for the next chance.",
                "He stays squarely in the late-inning conversation here.",
            ])
        return random.choice([
            "He remains in the late-inning mix for this bullpen.",
            "In a fluid bullpen, outings like this help his standing.",
            "This keeps him relevant in a bullpen without a locked-in pecking order.",
        ])

    if role in {"setup", "leverage_arm"}:
        if leverage == "high":
            if outing_grade == "DOMINANT":
                return random.choice([
                    "He handled a leverage spot the way trusted bridge arms are supposed to.",
                    "This was high-value work, and he looked the part.",
                    "He answered a meaningful spot and strengthened his standing in the late-inning mix.",
                ])
            return random.choice([
                "He keeps showing up in meaningful innings for this bullpen.",
                "The usage still points to a trusted late-inning role.",
                "He remains one of the steadier bridge options here.",
            ])
        if outing_grade == "DOMINANT":
            return random.choice([
                "He looked sharp again and keeps himself in the leverage mix.",
                "This was another strong step for a reliever already working in useful spots.",
                "He continues to make the case for more meaningful innings.",
            ])

    if leverage == "low":
        if outing_grade in {"CLEAN", "DOMINANT"}:
            return random.choice([
                "The game state was softer, but he still did exactly what he needed to do.",
                "It was lower-leverage work, though he handled it efficiently.",
                "He took care of a softer spot without much trouble.",
            ])
        return random.choice([
            "The spot carried lighter leverage, but the line still got messy.",
            "Even in a softer game state, the inning brought more trouble than expected.",
        ])

    if outing_grade == "DOMINANT":
        if trend == "UP":
            return random.choice([
                "He is putting together a strong recent run of work.",
                "The recent form has been good, and this outing fit that trend.",
                "He has been stringing together sharper appearances lately.",
            ])
        return random.choice([
            "This was a strong outing.",
            "He turned in one of his sharper appearances here.",
            "He handled the inning cleanly and effectively.",
        ])

    if outing_grade == "CLEAN":
        if leverage == "high":
            return random.choice([
                "That is the sort of clean inning that can earn more trust.",
                "A clean line in a meaningful inning will play well in this bullpen.",
                "Handling a real leverage spot cleanly is always useful for a reliever's standing.",
            ])
        return random.choice([
            "He handled the inning cleanly.",
            "This was a steady, effective appearance.",
            "He did his job without much trouble.",
        ])

    if outing_grade == "TRAFFIC":
        if leverage == "high":
            return random.choice([
                "He worked through traffic in a meaningful spot and still kept the inning from turning.",
                "It was not spotless, but he got through an inning that mattered.",
                "He bent some in leverage, though he still kept things intact.",
            ])
        return random.choice([
            "He worked through traffic and still got the job done.",
            "It was not spotless, but he managed the inning well enough.",
            "He navigated some traffic and kept the inning from turning.",
        ])

    return random.choice([
        "He turned in a usable inning for this bullpen.",
        "This was a neutral relief appearance overall.",
        "He got through the inning without changing much about his standing.",
    ])



def build_summary(name: str, team: str, s: dict, label: str, context: dict, streak_count: int, tracked_info: dict, recent_appearances, usage_note: str = "", velocity_alert: dict = None):
    ip_text = format_ip_for_summary(s["ip"])
    outs_recorded = baseball_ip_to_outs(s["ip"])
    er = s["er"]
    h = s["h"]
    bb = s["bb"]
    k = s["k"]

    ctx = build_context_phrase(context)
    finished_game = context.get("finished_game", False)
    role = infer_role_from_tracked_info(tracked_info)
    early_closer_usage = (
        role == "closer"
        and context.get("entry_inning") is not None
        and context.get("entry_inning") < 9
        and context.get("entry_state_kind") in {"lead", "tie"}
    )

    if label == "SAVE":
        if early_closer_usage and finished_game:
            line1 = f"{name} was called on {ctx} before the ninth in a high-leverage spot and finished the game for the save."
        elif outs_recorded >= 6:
            line1 = f"{name} entered {ctx} and covered the final {ip_text} to earn the save."
        elif finished_game and context.get("entry_inning") == 9:
            line1 = f"{name} entered {ctx} and shut the door for the save."
        else:
            line1 = f"{name} entered {ctx} and locked down the save."
    elif label == "BLOWN":
        line1 = f"{name} entered {ctx} but could not hold the lead and was charged with a blown save."
    elif label == "HOLD":
        line1 = f"{name} entered {ctx} and held the line to earn the hold."
    elif label == "DOM":
        line1 = f"{name} entered {ctx} and dominated."
    elif label == "TRAFFIC":
        line1 = f"{name} entered {ctx} and navigated traffic to keep things under control."
    elif label == "ROUGH":
        line1 = f"{name} entered {ctx} but was hit hard in a rough outing."
    elif label == "CLEAN":
        line1 = f"{name} entered {ctx} and turned in a clean outing."
    else:
        line1 = f"{name} entered {ctx} in relief."

    hit_text = stat_phrase(h, "hit")
    walk_text = stat_phrase(bb, "walk")
    run_text = stat_phrase(er, "run")

    if outs_recorded == 1:
        if er == 0 and h == 0 and bb == 0:
            line2 = "He retired the lone batter he faced."
        elif er == 0:
            line2 = f"He got the out he was asked to get, allowing {hit_text} and {walk_text}."
        else:
            line2 = f"He recorded one out while allowing {run_text} on {hit_text} and {walk_text}."
    elif er == 0 and h == 0 and bb == 0:
        if k > 0:
            line2 = f"He retired all hitters he faced over {ip_text} {strikeout_phrase(k).replace('while ', '')}."
        else:
            line2 = f"He retired all hitters he faced over {ip_text}."
    elif er == 0:
        line2 = f"He worked {ip_text}, allowing {hit_text} and {walk_text}"
        k_part = strikeout_phrase(k)
        line2 += f" {k_part}." if k_part else "."
    else:
        line2 = f"He allowed {run_text} over {ip_text} on {hit_text} and {walk_text}"
        k_part = strikeout_phrase(k)
        line2 += f" {k_part}." if k_part else "."

    analysis = build_analysis(
        p={"name": name, "team": team},
        s=s,
        label=label,
        context=context,
        tracked_info=tracked_info,
        recent_appearances=recent_appearances,
    )

    pieces = [line1, line2]
    velocity_sentence = build_velocity_inline_sentence(velocity_alert) if tracked_info else ""

    if velocity_sentence and random.random() < 0.4:
        pieces.append(velocity_sentence)
        pieces.append(analysis)
    else:
        pieces.append(analysis)
        if velocity_sentence:
            pieces.append(velocity_sentence)

    if usage_note:
        pieces.append(usage_note)

    streak_sentence = get_streak_sentence(streak_count)
    if streak_sentence and not usage_note:
        pieces.append(streak_sentence)

    return " ".join(piece for piece in pieces if piece)



def get_all_tracked_names(tracked: dict):
    return {k for k in tracked.keys()}


def recent_window_summary(recent_appearances):
    apps = recent_appearances[:5]
    if not apps:
        return {}
    last3 = apps[:3]
    last5 = apps[:5]
    return {
        "last3": last3,
        "last5": last5,
        "scoreless3": len(last3) == 3 and all(a.get("er", 0) == 0 for a in last3),
        "scoreless5": len(last5) == 5 and all(a.get("er", 0) == 0 for a in last5),
        "scoreless4of5": len(last5) == 5 and sum(1 for a in last5 if a.get("er", 0) == 0) >= 4,
        "runs2of3": len(last3) == 3 and sum(1 for a in last3 if a.get("er", 0) > 0) >= 2,
        "runs3of5": len(last5) == 5 and sum(1 for a in last5 if a.get("er", 0) > 0) >= 3,
        "ks_last3": sum(safe_int(a.get("k", 0), 0) for a in last3),
        "ks_last5": sum(safe_int(a.get("k", 0), 0) for a in last5),
        "k_streak3": len(last3) == 3 and all(safe_int(a.get("k", 0), 0) >= 1 for a in last3),
        "dominant_last3": sum(1 for a in last3 if grade_outing(a) == "DOMINANT") >= 2,
        "dominant_k_combo": len(last3) == 3 and sum(1 for a in last3 if a.get("er",0)==0 and safe_int(a.get("k",0),0)>=2) >= 2,
        "no_walk3": len(last3) == 3 and all(safe_int(a.get("bb", 0), 0) == 0 for a in last3),
        "k_3_of_4": len(apps[:4]) == 4 and sum(1 for a in apps[:4] if safe_int(a.get("k", 0), 0) >= 1) >= 3,
        "saves2": len(last3) >= 2 and sum(1 for a in last3[:2] if safe_int(a.get("saves", 0), 0) > 0) == 2,
        "holds3": len(last3) == 3 and all(safe_int(a.get("holds", 0), 0) > 0 for a in last3),
        "multi_inning3": len(last3) == 3 and all(baseball_ip_to_outs(a.get("ip", "0.0")) >= 4 and a.get("er", 0) == 0 for a in last3),
        "inherit_zero3": False,
    }


def build_trend_candidates(current_app: dict, recent_appearances, tracked_info, context: dict):
    if tracked_info:
        return []
    if not recent_appearances:
        return []
    info = recent_window_summary(recent_appearances)
    if not info:
        return []
    candidates = []
    current_grade = grade_outing(current_app)
    current_save = safe_int(current_app.get("saves", 0), 0) > 0
    current_hold = safe_int(current_app.get("holds", 0), 0) > 0
    prev = recent_appearances[1] if len(recent_appearances) > 1 else None
    prev2 = recent_appearances[2] if len(recent_appearances) > 2 else None

    def add(code, subject, emoji, priority, family, detail=None):
        candidates.append({"code": code, "subject": subject, "emoji": emoji, "priority": priority, "family": family, "detail": detail or {}})

    if info.get("scoreless5"):
        add("scoreless5", "Scoreless Streak: 5 Straight", "🔥", 100, "scoreless")
    elif info.get("scoreless3"):
        add("scoreless3", "Scoreless Streak: 3 Straight", "🔥", 80, "scoreless")
    elif info.get("scoreless4of5"):
        add("scoreless4of5", "Strong Recent Run", "🔥", 72, "scoreless")

    if info.get("ks_last5", 0) >= 10:
        add("ks10last5", "Bat-Missing Run", "⚡", 96, "strikeout", {"ks": info.get("ks_last5", 0), "window": 5})
    elif info.get("ks_last3", 0) >= 7:
        add("ks7last3", "Strikeout Surge", "⚡", 86, "strikeout", {"ks": info.get("ks_last3", 0), "window": 3})

    if info.get("k_streak3"):
        add("k_streak3", "Strikeout in 3 Straight", "⚡", 70, "strikeout")
    if info.get("dominant_last3"):
        add("dominant_last3", "Dominant Stretch", "⚡", 78, "dominance")
    if info.get("dominant_k_combo"):
        add("dominant_k_combo", "Power Outings Stacking Up", "⚡", 82, "dominance")
    if info.get("no_walk3"):
        add("no_walk3", "No-Walk Run", "🧠", 68, "command")
    if info.get("k_3_of_4"):
        add("k_3_of_4", "Steady Swing-and-Miss", "⚡", 62, "strikeout")

    if current_save and info.get("saves2"):
        add("saves2", "Back-to-Back Saves", "📈", 90, "role")
    if current_hold and info.get("holds3"):
        add("holds3", "Three Straight Holds", "📈", 84, "role")
    if info.get("multi_inning3"):
        add("multi_inning3", "Multi-Inning Success", "📈", 66, "usage")

    if prev and safe_int(prev.get("blownSaves", 0), 0) > 0 and current_app.get("er", 0) == 0:
        add("bounce_blown", "Bounce-Back Outing", "🔁", 74, "bounce")
    if prev and prev.get("er", 0) > 0 and current_app.get("er", 0) == 0:
        add("bounce_rough", "Rebound Performance", "🔁", 60, "bounce")
    if prev and prev2 and prev.get("er", 0) > 0 and prev2.get("er", 0) > 0 and current_app.get("er", 0) == 0:
        add("clean_rebound_2bad", "Steadier After Trouble", "🔁", 76, "bounce")

    if info.get("runs2of3"):
        add("runs2of3", "Recent Form Trending Down", "⚠️", 64, "rough")
    if info.get("runs3of5"):
        add("runs3of5", "Rough Stretch", "⚠️", 69, "rough")
    if prev and info.get("scoreless3") is False and prev.get("er",0) == 0 and prev2 and prev2.get("er",0)==0 and current_app.get("er",0) > 0:
        add("scoreless_snapped", "Scoreless Run Snapped", "⚠️", 58, "rough")
    if prev and prev.get("er",0)==0 and prev2 and prev2.get("er",0)==0 and current_app.get("er",0) > 0:
        add("first_rough_after_hot", "First Rough Turn After Hot Stretch", "⚠️", 61, "rough")

    if context.get("entry_inning") is not None and safe_int(context.get("entry_inning"), 0) >= 7 and current_app.get("er",0) == 0:
        add("late_inning_clean", "Late-Inning Look", "📈", 63, "usage")
    if context.get("entry_inning") is not None and safe_int(context.get("entry_inning"), 0) >= 8 and current_app.get("er",0) == 0:
        add("higher_leverage_usage", "More Meaningful Work", "📈", 67, "usage")

    if context.get("finished_game") and current_save:
        add("save_conversion", "Emerging Late-Inning Option", "📈", 73, "role")

    # choose strongest family per family will happen later; return all candidates
    return candidates


def choose_best_trend(candidates):
    if not candidates:
        return None

    best_by_family = {}
    for candidate in candidates:
        family = candidate.get("family", "misc")
        current_best = best_by_family.get(family)
        if current_best is None or candidate.get("priority", 0) > current_best.get("priority", 0):
            best_by_family[family] = candidate

    family_winners = sorted(best_by_family.values(), key=lambda x: x.get("priority", 0), reverse=True)
    if not family_winners:
        return None

    top_priority = family_winners[0].get("priority", 0)
    top_pool = [c for c in family_winners if c.get("priority", 0) >= top_priority - 12][:4]
    if len(top_pool) == 1:
        return top_pool[0]

    weights = []
    for candidate in top_pool:
        priority = max(float(candidate.get("priority", 0)), 1.0)
        weights.append(priority)

    return random.choices(top_pool, weights=weights, k=1)[0]


def build_trend_analysis(name: str, team: str, trend: dict, recent_appearances, season_stats: dict):
    code = trend.get("code")
    info = recent_window_summary(recent_appearances)
    span_stats = summarize_trend_span(recent_appearances, code)

    templates = {
        "scoreless3": [
            f"{name} has now strung together three straight scoreless outings, and the recent form has been sharp.",
            f"{name} has put together three straight scoreless appearances and keeps stacking clean work.",
            f"Three straight scoreless outings have started to put {name} on the radar in this bullpen.",
        ],
        "scoreless5": [
            f"{name} has now turned in five straight scoreless outings, one of the better recent runs in this bullpen.",
            f"{name} has now logged five straight scoreless outings, giving him one of the stronger recent runs in this bullpen.",
            f"Five straight scoreless appearances have put {name} firmly on the radar in this bullpen.",
        ],
        "scoreless4of5": [
            f"{name} has been scoreless in four of his last five outings and is building real momentum.",
            f"{name} has put together scoreless work in four of his last five appearances and the recent body of work is starting to stand out.",
        ],
        "ks10last5": [
            f"{name} has racked up {number_word(info.get('ks_last5', 0))} strikeouts over his last five appearances and the bat-missing has become impossible to miss.",
            f"Over his last five outings, {name} has piled up {number_word(info.get('ks_last5', 0))} strikeouts and the swing-and-miss has jumped off the page.",
        ],
        "ks7last3": [
            f"{name} has piled up {number_word(info.get('ks_last3', 0))} strikeouts across his last three outings and is missing bats at a high rate.",
            f"Strikeouts are starting to pile up for {name}, who has {number_word(info.get('ks_last3', 0))} over his last three appearances.",
        ],
        "k_streak3": [
            f"{name} has recorded a strikeout in three straight outings and keeps bringing swing-and-miss to the mound.",
        ],
        "dominant_last3": [
            f"{name} has delivered multiple dominant appearances over his last three turns and is clearly in a strong stretch.",
        ],
        "dominant_k_combo": [
            f"{name} has stacked power outings lately, with multiple recent appearances featuring scoreless work and at least two strikeouts.",
        ],
        "no_walk3": [
            f"{name} has gone three straight outings without issuing a walk, and the recent command has been a real positive.",
        ],
        "saves2": [
            f"{name} has converted saves in back-to-back appearances and is starting to show up in more meaningful spots.",
        ],
        "holds3": [
            f"{name} has now collected holds in three straight appearances and continues to show up in useful spots.",
        ],
        "multi_inning3": [
            f"{name} has put together a string of successful multi-inning outings, giving this bullpen useful length without sacrificing results.",
        ],
        "bounce_blown": [
            f"After a blown save last time out, {name} answered with a clean bounce-back appearance.",
        ],
        "bounce_rough": [
            f"{name} bounced back with a cleaner outing after running into trouble previously.",
        ],
        "clean_rebound_2bad": [
            f"After two rougher appearances, {name} steadied things with a clean rebound outing.",
        ],
        "runs2of3": [
            f"{name} has now allowed runs in two of his last three appearances, and the recent form is starting to turn shaky.",
        ],
        "runs3of5": [
            f"{name} has been tagged in three of his last five outings, and the recent trend has gone in the wrong direction.",
        ],
        "scoreless_snapped": [
            f"A scoreless run came to an end for {name}, who had been putting together cleaner work lately.",
        ],
        "first_rough_after_hot": [
            f"{name} hit his first real bump after a stronger recent stretch.",
        ],
        "late_inning_clean": [
            f"{name} handled another clean late-inning look and continues to see work that matters more than ordinary middle relief.",
        ],
        "higher_leverage_usage": [
            f"The recent usage for {name} has started to creep into more meaningful territory, and he answered with another clean line.",
        ],
        "save_conversion": [
            f"{name} closed the door in his latest chance and is beginning to show up in spots that matter.",
        ],
    }

    opener = random.choice(templates.get(code, [f"{name} has put together a notable recent run and is worth monitoring more closely."]))
    stat_sentence = build_trend_stat_sentence(name, code, span_stats)
    velocity_sentence = build_trend_velocity_sentence(name, span_stats, season_stats)
    implication_sentence = random.choice([
        "That is the sort of stretch that can earn more meaningful work.",
        "Runs like this tend to get a reliever noticed a little more.",
        "This is a trend worth monitoring moving forward.",
        "This kind of run can change where a reliever sits in the pecking order.",
        "In deeper formats, this is the sort of trend that matters.",
    ])

    placement = random.choice(["middle", "end", "none"])
    sentences = [opener]

    if placement == "middle" and velocity_sentence:
        sentences.append(velocity_sentence)
        if stat_sentence:
            sentences.append(stat_sentence)
    else:
        if stat_sentence:
            sentences.append(stat_sentence)
        if placement == "end" and velocity_sentence:
            sentences.append(velocity_sentence)
        else:
            sentences.append(implication_sentence)

    return " ".join(sentences[:3])



def get_trend_family_cooldown_minutes(trend_family: str) -> int:
    key = str(trend_family or "misc").strip().lower()
    return safe_int(TREND_FAMILY_COOLDOWN_MINUTES.get(key, TREND_FAMILY_COOLDOWN_MINUTES.get("misc", 90)), 90)


def get_next_trend_delay_minutes() -> int:
    low = max(safe_int(TREND_RANDOM_INTERVAL_MIN_MINUTES, 6), 1)
    high = max(safe_int(TREND_RANDOM_INTERVAL_MAX_MINUTES, 16), low)
    return random.randint(low, high)


def can_post_trend_now(state, now_et: datetime):
    next_eligible_at = state.get("trend_next_eligible_at")
    if not next_eligible_at:
        return True
    try:
        next_dt = datetime.fromisoformat(next_eligible_at)
        if next_dt.tzinfo is None:
            next_dt = next_dt.replace(tzinfo=ET)
        else:
            next_dt = next_dt.astimezone(ET)
        return now_et >= next_dt
    except Exception:
        return True


def is_trend_family_on_cooldown(state, trend_family: str, now_et: datetime) -> bool:
    family_key = str(trend_family or "misc").strip().lower()
    last_post_at = state.get("trend_family_last_post_at", {}).get(family_key)
    if not last_post_at:
        return False
    try:
        last_dt = datetime.fromisoformat(last_post_at)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=ET)
        else:
            last_dt = last_dt.astimezone(ET)
        cooldown_minutes = get_trend_family_cooldown_minutes(family_key)
        return (now_et - last_dt).total_seconds() < cooldown_minutes * 60
    except Exception:
        return False


def mark_trend_posted(state, pitcher_id, trend_code, appearance_sig, now_et: datetime, trend_family: str = "misc"):
    day_key = now_et.strftime("%Y-%m-%d")
    hour_key = now_et.strftime("%Y-%m-%d-%H")
    state.setdefault("trend_posted", {})[str(pitcher_id)] = {
        "code": trend_code,
        "sig": appearance_sig,
        "date": day_key,
        "family": trend_family,
    }
    state.setdefault("trend_history", {})[f"{pitcher_id}:{trend_family}:{appearance_sig}"] = trend_code
    state["trend_last_post_at"] = now_et.isoformat()
    next_delay_minutes = get_next_trend_delay_minutes()
    state["trend_next_eligible_at"] = (now_et + timedelta(minutes=next_delay_minutes)).isoformat()
    state.setdefault("trend_post_count_by_hour", {})[hour_key] = safe_int(state.setdefault("trend_post_count_by_hour", {}).get(hour_key, 0), 0) + 1
    state.setdefault("trend_total_by_date", {})[day_key] = safe_int(state.setdefault("trend_total_by_date", {}).get(day_key, 0), 0) + 1
    state.setdefault("trend_family_last_post_at", {})[str(trend_family or "misc").strip().lower()] = now_et.isoformat()


def appearance_signature(recent_appearances):
    if not recent_appearances:
        return "none"
    parts = []
    for app in recent_appearances[:5]:
        parts.append(f"{app.get('ip','0.0')}-{app.get('h',0)}-{app.get('er',0)}-{app.get('bb',0)}-{app.get('k',0)}-{app.get('saves',0)}-{app.get('holds',0)}")
    return "|".join(parts)


async def post_trend_card(channel, meta: dict, trend: dict, recent_appearances):
    team = meta.get("team", "UNK")
    name = meta.get("name", "Unknown Pitcher")
    subject = f"{trend.get('emoji', '🧠')} {trend.get('subject', 'Bullpen Trend')}"
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(team), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, name, team)
    embed.add_field(name="", value=f"**{subject}**", inline=False)
    embed.add_field(name="Season", value=format_season_line(meta.get("season_stats", {})), inline=False)
    embed.add_field(name="Summary", value=build_trend_analysis(name, team, trend, recent_appearances, meta.get("season_stats", {})), inline=False)
    await channel.send(embed=embed)


def build_velocity_alert(current_app: dict, recent_appearances):
    current_v = current_app.get("avg_fastball_velocity")
    current_pitches = safe_int(current_app.get("pitch_count", 0), 0)
    current_fastballs = safe_int(current_app.get("fastball_count", 0), 0)

    if current_v is None or current_pitches < VELOCITY_MIN_PITCHES or current_fastballs < VELOCITY_MIN_FASTBALLS:
        return None

    previous_with_velo = None
    prior_velos = []
    for app in recent_appearances[1:]:
        app_v = app.get("avg_fastball_velocity")
        if app_v is None:
            continue
        if previous_with_velo is None:
            previous_with_velo = app
        prior_velos.append(app_v)
        if len(prior_velos) >= 5:
            break

    if not previous_with_velo and not prior_velos:
        return None

    candidates = []
    if previous_with_velo and previous_with_velo.get("avg_fastball_velocity") is not None:
        prev_v = safe_float(previous_with_velo.get("avg_fastball_velocity"), 0.0)
        delta = round(current_v - prev_v, 1)
        if abs(delta) >= VELOCITY_DELTA_THRESHOLD:
            candidates.append({
                "baseline_type": "last outing",
                "baseline_velocity": prev_v,
                "delta": delta,
                "priority": abs(delta),
            })

    if prior_velos:
        recent_avg = round(sum(prior_velos) / len(prior_velos), 1)
        delta_avg = round(current_v - recent_avg, 1)
        if abs(delta_avg) >= VELOCITY_DELTA_THRESHOLD:
            candidates.append({
                "baseline_type": "recent average",
                "baseline_velocity": recent_avg,
                "delta": delta_avg,
                "priority": abs(delta_avg) + 0.05,
            })

    if not candidates:
        return None

    best = sorted(candidates, key=lambda x: x.get("priority", 0), reverse=True)[0]
    direction = "up" if best["delta"] > 0 else "down"
    emoji = "⚡" if direction == "up" else "⚠️"
    subject = "Velocity Spike" if direction == "up" else "Velocity Drop"
    return {
        "code": f"velo_{direction}",
        "subject": subject,
        "emoji": emoji,
        "current_velocity": round(current_v, 1),
        "baseline_velocity": round(best["baseline_velocity"], 1),
        "baseline_type": best["baseline_type"],
        "delta": round(best["delta"], 1),
    }


def build_velocity_analysis(name: str, velocity_alert: dict):
    current_v = velocity_alert.get("current_velocity")
    baseline_v = velocity_alert.get("baseline_velocity")
    delta = velocity_alert.get("delta")
    baseline_type = velocity_alert.get("baseline_type", "recent average")

    if delta is None:
        return f"{name} showed a notable fastball velocity change in this outing. It is worth monitoring moving forward."

    change_text = f"{abs(delta):.1f} MPH"
    if delta > 0:
        starters = [
            f"{name} averaged {current_v:.1f} MPH on his fastball in this outing, up from {baseline_v:.1f} MPH in his {baseline_type}.",
            f"{name}'s fastball averaged {current_v:.1f} MPH here after sitting at {baseline_v:.1f} MPH in his {baseline_type}.",
        ]
        middles = [
            f"The +{change_text} jump stands out and suggests his stuff had extra life in this appearance.",
            f"That +{change_text} bump is noticeable and could be a sign that his stuff is trending in the right direction.",
        ]
        thirds = [
            "It is the kind of change worth keeping an eye on if it holds into his next outing.",
            "If that carries forward, it adds another reason to pay attention to his recent run.",
        ]
    else:
        starters = [
            f"{name} averaged {current_v:.1f} MPH on his fastball in this outing, down from {baseline_v:.1f} MPH in his {baseline_type}.",
            f"{name}'s fastball averaged {current_v:.1f} MPH here after sitting at {baseline_v:.1f} MPH in his {baseline_type}.",
        ]
        middles = [
            f"The dip of {change_text} is noticeable and worth monitoring moving forward.",
            f"That -{change_text} shift is enough to stand out and is something to keep an eye on in his next appearance.",
        ]
        thirds = [
            "Changes like this can simply reflect a single-night blip, but it is still meaningful enough to flag.",
            "One outing does not make a trend, but velocity changes like this are worth noting when they show up.",
        ]

    if random.random() < 0.55:
        return f"{random.choice(starters)} {random.choice(middles)} {random.choice(thirds)}"
    return f"{random.choice(starters)} {random.choice(middles)}"



async def post_velocity_card(channel, meta: dict, velocity_alert: dict):
    team = meta.get("team", "UNK")
    name = meta.get("name", "Unknown Pitcher")
    subject = f"{velocity_alert.get('emoji', '⚠️')} {velocity_alert.get('subject', 'Velocity Alert')}"
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(team), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, name, team)
    embed.add_field(name="", value=f"**{subject}**", inline=False)
    embed.add_field(name="Season", value=format_season_line(meta.get("season_stats", {})), inline=False)
    embed.add_field(name="Summary", value=build_velocity_analysis(name, velocity_alert), inline=False)
    await channel.send(embed=embed)


def should_post_velocity_alert(state, pitcher_id: int, game_id: int, velocity_alert: dict, now_et: datetime):
    if pitcher_id is None or game_id is None or not velocity_alert:
        return False

    posted_map = state.setdefault("velocity_posted", {})
    entry = posted_map.get(str(pitcher_id), {})
    today_key = now_et.strftime("%Y-%m-%d")
    signature = f"{game_id}:{velocity_alert.get('code')}:{velocity_alert.get('current_velocity')}:{velocity_alert.get('baseline_velocity')}"

    if entry.get("date") == today_key and entry.get("signature") == signature:
        return False

    return True


def mark_velocity_posted(state, pitcher_id: int, game_id: int, velocity_alert: dict, now_et: datetime):
    posted_map = state.setdefault("velocity_posted", {})
    posted_map[str(pitcher_id)] = {
        "date": now_et.strftime("%Y-%m-%d"),
        "game_id": game_id,
        "signature": f"{game_id}:{velocity_alert.get('code')}:{velocity_alert.get('current_velocity')}:{velocity_alert.get('baseline_velocity')}",
    }




def gather_trend_candidates_from_recent_games(tracked: dict, processed_pitchers_by_game):
    tracked_names = get_all_tracked_names(tracked)
    candidates = []
    for item in processed_pitchers_by_game:
        p = item["pitcher"]
        pid = p.get("id")
        if pid is None:
            continue
        norm = normalize_name(p.get("name", ""))
        if norm in tracked_names:
            continue

        game_date_et = item.get("game_date_et")
        appearance_count_15 = count_recent_appearances_in_window(pid, game_date_et, days=15)
        if appearance_count_15 < 4:
            continue

        recent = item.get("recent_appearances") or []
        if not recent:
            continue
        trend_options = build_trend_candidates(item["current_app"], recent, None, item.get("context", {}))
        best = choose_best_trend(trend_options)
        if not best:
            continue
        candidates.append({
            "pitcher_id": pid,
            "meta": player_meta_cache.get(pid, {"name": p.get("name"), "team": p.get("team"), "season_stats": p.get("season_stats", {})}),
            "trend": best,
            "recent_appearances": recent,
        })
    candidates.sort(key=lambda x: x["trend"].get("priority", 0), reverse=True)
    return candidates


# ---------------- CORE ----------------

def get_games():
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)

    games = []

    for d in [today, yesterday]:
        try:
            r = requests.get(f"{SCHEDULE_URL}&date={d.isoformat()}", timeout=30)
            r.raise_for_status()
            data = r.json()
            for date_block in data.get("dates", []):
                games.extend(date_block.get("games", []))
        except Exception as e:
            log(f"Schedule fetch error for {d}: {e}")

    return games


def get_feed(game_id):
    r = requests.get(LIVE_URL.format(game_id), timeout=30)
    r.raise_for_status()
    return r.json()


def get_pitchers(feed: dict):
    result = []
    box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    game_teams = feed.get("gameData", {}).get("teams", {})

    for side in ["home", "away"]:
        team = normalize_team_abbr(game_teams.get(side, {}).get("abbreviation"))
        if not team:
            team = normalize_team_abbr(box.get(side, {}).get("team", {}).get("abbreviation", "UNK"))

        players = box.get(side, {}).get("players", {})

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

            velo = get_fastball_velocity_summary(feed, p.get("person", {}).get("id"))
            player_obj = {
                "id": p.get("person", {}).get("id"),
                "name": p.get("person", {}).get("fullName", "Unknown Pitcher"),
                "team": normalize_team_abbr(team),
                "side": side,
                "stats": stats,
                "season_stats": season_stats,
                "avg_fastball_velocity": velo.get("avg_fastball_velocity") if velo else None,
                "fastball_count": velo.get("fastball_count", 0) if velo else 0,
                "pitch_count": velo.get("total_pitches", safe_int(stats.get("numberOfPitches", 0), 0)) if velo else safe_int(stats.get("numberOfPitches", 0), 0),
            }
            result.append(player_obj)
            if player_obj["id"] is not None:
                player_meta_cache[player_obj["id"]] = {
                    "name": player_obj["name"],
                    "team": normalize_team_abbr(team),
                    "season_stats": season_stats,
                }

    return result


# ---------------- POST ----------------

async def post_card(channel, p: dict, matchup: str, score: str, context: dict, streak_count: int, tracked_info: dict, recent_appearances, usage_note: str = "", velocity_alert: dict = None):
    s = {
        "ip": str(p["stats"].get("inningsPitched", "0.0")),
        "h": safe_int(p["stats"].get("hits", 0), 0),
        "er": safe_int(p["stats"].get("earnedRuns", 0), 0),
        "bb": safe_int(p["stats"].get("baseOnBalls", 0), 0),
        "k": safe_int(p["stats"].get("strikeOuts", 0), 0),
        "saves": safe_int(p["stats"].get("saves", 0), 0),
        "holds": safe_int(p["stats"].get("holds", 0), 0),
        "blownSaves": safe_int(p["stats"].get("blownSaves", 0), 0),
    }

    label = classify(s)

    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])

    embed.add_field(name="", value=f"**{impact_tag(label, s)}**", inline=False)
    embed.add_field(name="⚾ Matchup", value=matchup, inline=False)
    embed.add_field(name="Game Line", value=format_game_line(s), inline=False)
    embed.add_field(name="Pitch Count", value=format_pitch_count(p["stats"]), inline=False)
    embed.add_field(name="Season", value=format_season_line(p.get("season_stats", {})), inline=False)
    embed.add_field(
        name="Summary",
        value=build_summary(
            p["name"],
            p["team"],
            s,
            label,
            context,
            streak_count,
            tracked_info,
            recent_appearances,
            usage_note=usage_note,
            velocity_alert=velocity_alert,
        ),
        inline=False,
    )

    await channel.send(embed=embed)



async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    state = load_state()
    posted = set(state.get("posted", []))

    if RESET_CLOSER_STATE:
        log("RESET_CLOSER_STATE enabled — posted state cleared for this run")

    tracked = await refresh_tracked_pitchers()
    last_refresh_date = datetime.now(ET).date()

    while True:
        try:
            now_et = datetime.now(ET)
            current_date = now_et.date()
            if current_date != last_refresh_date:
                tracked = await refresh_tracked_pitchers()
                last_refresh_date = current_date

            games = get_games()
            log(f"Checking {len(games)} games")
            processed_pitchers_by_game = []

            for g in games:
                if g.get("status", {}).get("detailedState") != "Final":
                    continue

                game_id = g.get("gamePk")
                if not game_id:
                    continue

                feed = get_feed(game_id)
                pitchers = get_pitchers(feed)
                game_date_et = parse_game_date_et(g)

                game_teams = feed.get("gameData", {}).get("teams", {})
                away_abbr = game_teams.get("away", {}).get("abbreviation") or g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation") or "AWAY"
                home_abbr = game_teams.get("home", {}).get("abbreviation") or g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation") or "HOME"
                away_score = safe_int(g.get("teams", {}).get("away", {}).get("score", 0), 0)
                home_score = safe_int(g.get("teams", {}).get("home", {}).get("score", 0), 0)
                matchup = f"{away_abbr} @ {home_abbr}"
                score = build_score_line(away_abbr, away_score, home_abbr, home_score)

                for p in pitchers:
                    pitcher_id = p.get("id")
                    if pitcher_id is None:
                        continue

                    context = get_pitcher_entry_context(feed, pitcher_id, p["side"])
                    recent_appearances = get_recent_appearances(pitcher_id, game_date_et, limit=5, max_days=21)
                    current_app = {
                        "ip": str(p["stats"].get("inningsPitched", "0.0")),
                        "h": safe_int(p["stats"].get("hits", 0), 0),
                        "er": safe_int(p["stats"].get("earnedRuns", 0), 0),
                        "bb": safe_int(p["stats"].get("baseOnBalls", 0), 0),
                        "k": safe_int(p["stats"].get("strikeOuts", 0), 0),
                        "saves": safe_int(p["stats"].get("saves", 0), 0),
                        "holds": safe_int(p["stats"].get("holds", 0), 0),
                        "blownSaves": safe_int(p["stats"].get("blownSaves", 0), 0),
                        "avg_fastball_velocity": p.get("avg_fastball_velocity"),
                        "fastball_count": safe_int(p.get("fastball_count", 0), 0),
                        "pitch_count": safe_int(p.get("pitch_count", 0), 0),
                    }
                    recent_for_trend = [current_app] + recent_appearances
                    processed_pitchers_by_game.append({
                        "pitcher": p,
                        "current_app": current_app,
                        "recent_appearances": recent_for_trend,
                        "context": context,
                        "game_date_et": game_date_et,
                    })

                    key = f"{game_id}_{pitcher_id}"
                    if key in posted:
                        continue

                    tracked_info = find_tracked_pitcher_info(p["name"], p["team"], tracked)
                    is_save = safe_int(p["stats"].get("saves", 0), 0) > 0
                    is_tracked = tracked_info is not None
                    if not (is_save or is_tracked):
                        continue

                    streak_count = get_streak_count(pitcher_id, game_date_et)
                    usage_note = build_usage_sentence(get_recent_usage_snapshot(pitcher_id, game_date_et))
                    velocity_alert = build_velocity_alert(current_app, recent_for_trend)

                    log(f"Posting {p['name']} | {p['team']} | {matchup}")
                    await post_card(
                        channel,
                        p,
                        matchup,
                        score,
                        context,
                        streak_count,
                        tracked_info,
                        recent_appearances,
                        usage_note=usage_note,
                        velocity_alert=velocity_alert if is_tracked else None,
                    )

                    if (not is_tracked) and should_post_velocity_alert(state, pitcher_id, game_id, velocity_alert, now_et):
                        log(f"Velocity alert {p['name']} | {p['team']} | {velocity_alert.get('subject')}")
                        await post_velocity_card(
                            channel,
                            {
                                "name": p["name"],
                                "team": p["team"],
                                "season_stats": p.get("season_stats", {}),
                            },
                            velocity_alert,
                        )
                        mark_velocity_posted(state, pitcher_id, game_id, velocity_alert, now_et)

                    posted.add(key)

            # trend blurbs use the same channel, but only for non-depth-chart relievers
            if can_post_trend_now(state, now_et):
                trend_candidates = gather_trend_candidates_from_recent_games(tracked, processed_pitchers_by_game)
                for candidate in trend_candidates:
                    pid = candidate["pitcher_id"]
                    sig = appearance_signature(candidate["recent_appearances"])
                    existing = state.get("trend_posted", {}).get(str(pid), {})
                    trend_family = candidate["trend"].get("family", "misc")
                    if existing.get("sig") == sig and existing.get("family") == trend_family:
                        continue
                    if is_trend_family_on_cooldown(state, trend_family, now_et):
                        continue
                    if state.get("trend_history", {}).get(f"{pid}:{trend_family}:{sig}"):
                        continue
                    last_date = existing.get("date")
                    today_key = now_et.strftime("%Y-%m-%d")
                    if last_date == today_key:
                        continue
                    log(f"Trend blurb {candidate['meta'].get('name')} | {candidate['meta'].get('team')} | {candidate['trend'].get('subject')}")
                    await post_trend_card(channel, candidate["meta"], candidate["trend"], candidate["recent_appearances"])
                    mark_trend_posted(state, pid, candidate["trend"].get("code"), sig, now_et, trend_family=trend_family)
                    break

            state["posted"] = list(posted)
            save_state(state)

        except Exception as e:
            log(f"Loop error: {e}")

        await asyncio.sleep(POLL_MINUTES * 60)


# ---------------- START ----------------

intents = discord.Intents.default()
client = discord.Client(intents=intents)
background_task = None


@client.event
async def on_ready():
    global background_task
    log(f"Logged in as {client.user}")

    if background_task is None or background_task.done():
        background_task = asyncio.create_task(loop())
        log("Closer background task created")


async def start_closer_bot():
    if not TOKEN:
        raise RuntimeError("ANALYTIC_BOT_TOKEN is not set")

    await client.start(TOKEN, reconnect=True)


# ================= STARTER BOT OVERRIDES =================

MIN_STARTER_SCORE = float(os.getenv("STARTER_MIN_SCORE", "5.0"))
MAX_STARTER_CARDS_PER_GAME = int(os.getenv("STARTER_MAX_CARDS_PER_GAME", "2"))


def format_starter_season_line(season_stats: dict) -> str:
    season = season_stats or {}
    era = season.get("era") or season.get("earnedRunAverage") or "0.00"
    whip = season.get("whip") or season.get("walksAndHitsPerInningPitched")
    w = safe_int(season.get("wins", 0), 0)
    l = safe_int(season.get("losses", 0), 0)
    k = safe_int(season.get("strikeOuts", 0), 0)
    ip = season.get("inningsPitched") or "0.0"
    parts = [f"{w}-{l}", f"ERA {era}"]
    if whip not in (None, ""):
        parts.append(f"WHIP {whip}")
    parts.append(f"{k} K")
    parts.append(f"{ip} IP")
    return " • ".join(parts)


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
            candidate = {
                "id": person_id,
                "name": p.get("person", {}).get("fullName", "Unknown Pitcher"),
                "team": team,
                "side": side,
                "stats": stats,
                "season_stats": season_stats,
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
            candidates.sort(key=lambda c: (safe_float(c["stats"].get("inningsPitched", "0.0"), 0.0), -safe_int(c["stats"].get("gamesFinished", 0), 0)), reverse=True)
            selected = candidates[0]
        result.append(selected)
    return result


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
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    if ip >= 7.0 and er == 0:
        return "GEM"
    if ip >= 6.0 and er <= 2 and k >= 8:
        return "DOMINANT"
    if ip >= 6.0 and er <= 3:
        return "QUALITY"
    if k >= 9 and er <= 4:
        return "STRIKEOUT"
    if ip < 5.0 or er >= 5:
        return "ROUGH"
    if hits <= 4 and er <= 2:
        return "SHARP"
    return "SOLID"


def starter_impact_tag(label: str) -> str:
    return {
        "GEM": "💎 Pitching Gem",
        "DOMINANT": "🔥 Dominant Start",
        "QUALITY": "✅ Quality Start",
        "STRIKEOUT": "🧨 Strikeout Juice",
        "SHARP": "🎯 Sharp Outing",
        "ROUGH": "⚠️ Rough Start",
        "SOLID": "📈 Solid Start",
    }.get(label, "📈 Solid Start")


def format_starter_game_line(stats: dict) -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    pitches = safe_int(stats.get("numberOfPitches", 0), 0)
    strikes = safe_int(stats.get("strikes", 0), 0)
    parts = [f"{ip} IP", f"{h} H", f"{er} ER", f"{bb} BB", f"{k} K"]
    if pitches:
        if strikes:
            parts.append(f"{pitches}-{strikes} P-S")
        else:
            parts.append(f"{pitches} P")
    return " • ".join(parts)


def build_starter_summary(name: str, stats: dict, label: str, matchup: str, score: str) -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    win = safe_int(stats.get("wins", 0), 0) > 0

    opening_map = {
        "GEM": f"{name} authored one of the best starts on the slate in {matchup}, working {ip} with no earned runs allowed.",
        "DOMINANT": f"{name} controlled {matchup} from the jump and missed bats all night in a dominant outing.",
        "QUALITY": f"{name} gave his club exactly what it needed in {matchup}, turning in a sturdy quality start.",
        "STRIKEOUT": f"{name} piled up swing-and-miss in {matchup} and gave hitters very little breathing room.",
        "ROUGH": f"{name} had a tough time holding the line in {matchup} and never fully settled into the outing.",
        "SHARP": f"{name} kept traffic down in {matchup} and worked efficiently through most of the night.",
        "SOLID": f"{name} turned in a useful start in {matchup} and kept his club in the game.",
    }
    second = f"He finished with {h} hits allowed, {er} earned runs, {bb} walks, and {k} strikeouts."
    third = "He was credited with the win." if win else f"Final score: {score}."
    if win:
        third += f" Final score: {score}."
    return " ".join([opening_map.get(label, opening_map['SOLID']), second, third])


async def post_card(channel, p: dict, matchup: str, score: str, context=None, streak_count: int = 0, tracked_info=None, recent_appearances=None, usage_note: str = "", velocity_alert=None):
    stats = p["stats"]
    label = classify_starter(stats)
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])
    embed.add_field(name="", value=f"**{starter_impact_tag(label)}**", inline=False)
    embed.add_field(name="⚾ Matchup", value=matchup, inline=False)
    embed.add_field(name="Game Line", value=format_starter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_starter_season_line(p.get("season_stats", {})), inline=False)
    embed.add_field(name="Summary", value=build_starter_summary(p["name"], stats, label, matchup, score), inline=False)
    await channel.send(embed=embed)


async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    state = load_state()
    posted = set(state.get("posted", []))

    if os.getenv("RESET_STARTER_STATE", "").lower() in {"1", "true", "yes"}:
        log("RESET_STARTER_STATE enabled — posted state cleared for this run")
        posted = set()

    while True:
        try:
            games = get_games()
            log(f"Checking {len(games)} games")

            for g in games:
                if g.get("status", {}).get("detailedState") != "Final":
                    continue
                game_id = g.get("gamePk")
                if not game_id:
                    continue
                feed = get_feed(game_id)
                starters = get_starters(feed)

                game_teams = feed.get("gameData", {}).get("teams", {})
                away_abbr = game_teams.get("away", {}).get("abbreviation") or g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation") or "AWAY"
                home_abbr = game_teams.get("home", {}).get("abbreviation") or g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation") or "HOME"
                away_score = safe_int(g.get("teams", {}).get("away", {}).get("score", 0), 0)
                home_score = safe_int(g.get("teams", {}).get("home", {}).get("score", 0), 0)
                matchup = f"{away_abbr} @ {home_abbr}"
                score = build_score_line(away_abbr, away_score, home_abbr, home_score)

                ranked = []
                for p in starters:
                    value = starter_score(p["stats"])
                    if value < MIN_STARTER_SCORE:
                        continue
                    ranked.append((value, p))
                ranked.sort(key=lambda x: (x[0], safe_int(x[1]["stats"].get("strikeOuts", 0), 0)), reverse=True)

                posted_this_game = 0
                for value, p in ranked:
                    if posted_this_game >= MAX_STARTER_CARDS_PER_GAME:
                        break
                    pid = p.get("id")
                    if pid is None:
                        continue
                    key = f"{game_id}_{pid}"
                    if key in posted:
                        continue
                    log(f"Posting {p['name']} | {p['team']} | {matchup} | score={value}")
                    await post_card(channel, p, matchup, score)
                    posted.add(key)
                    posted_this_game += 1

            state["posted"] = list(posted)
            save_state(state)
        except Exception as e:
            log(f"Loop error: {e}")

        await asyncio.sleep(POLL_MINUTES * 60)


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


# ================= STARTER BOT SUMMARY OVERRIDES V2 =================

GOOD_STARTER_LABELS = {"GEM", "DOMINANT", "QUALITY", "STRIKEOUT", "SHARP"}
BAD_STARTER_LABELS = {"ROUGH", "NO_COMMAND", "HIT_HARD", "SHORT"}
SUBPAR_STARTER_LABELS = BAD_STARTER_LABELS | {"UNEVEN"}


def format_starter_game_line(stats: dict) -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    return " • ".join([f"{ip} IP", f"{h} H", f"{er} ER", f"{bb} BB", f"{k} K"])


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


def starter_impact_tag(label: str) -> str:
    return {
        "GEM": "💎 Pitching Gem",
        "DOMINANT": "🔥 Dominant Start",
        "QUALITY": "✅ Quality Start",
        "STRIKEOUT": "🧨 Strikeout Juice",
        "SHARP": "🎯 Sharp Outing",
        "SOLID": "📈 Solid Start",
        "UNEVEN": "📉 Uneven Start",
        "SHORT": "⏱️ Short Outing",
        "ROUGH": "⚠️ Rough Start",
        "NO_COMMAND": "🧭 Command Wasn't There",
        "HIT_HARD": "💥 Hit Hard",
    }.get(label, "📈 Solid Start")


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


def build_starter_pitch_metrics(feed: dict, pitcher_id: int):
    if not feed or pitcher_id is None:
        return {}

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    total_pitches = 0
    strikes = 0
    called_strikes = 0
    whiffs = 0
    fastball_velos = []

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
            if start_speed > 0 and pitch_code in FASTBALL_PITCH_CODES:
                fastball_velos.append(start_speed)

    payload = {
        "pitch_count": total_pitches,
        "strikes": strikes,
        "whiffs": whiffs,
        "called_strikes": called_strikes,
    }
    if total_pitches > 0:
        payload["csw_percent"] = round(((called_strikes + whiffs) / total_pitches) * 100.0, 1)
    if len(fastball_velos) >= VELOCITY_MIN_FASTBALLS and total_pitches >= VELOCITY_MIN_PITCHES:
        payload["avg_fastball_velocity"] = round(sum(fastball_velos) / len(fastball_velos), 1)
        payload["fastball_count"] = len(fastball_velos)
    return payload


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
            candidate = {
                "id": person_id,
                "name": p.get("person", {}).get("fullName", "Unknown Pitcher"),
                "team": team,
                "side": side,
                "stats": stats,
                "season_stats": season_stats,
                "pitch_count": metrics.get("pitch_count", safe_int(stats.get("numberOfPitches", 0), 0)),
                "strikes": metrics.get("strikes", safe_int(stats.get("strikes", 0), 0)),
                "whiffs": metrics.get("whiffs", 0),
                "called_strikes": metrics.get("called_strikes", 0),
                "csw_percent": metrics.get("csw_percent"),
                "avg_fastball_velocity": metrics.get("avg_fastball_velocity"),
                "fastball_count": metrics.get("fastball_count", 0),
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
            candidates.sort(key=lambda c: (safe_float(c["stats"].get("inningsPitched", "0.0"), 0.0), -safe_int(c["stats"].get("gamesFinished", 0), 0)), reverse=True)
            selected = candidates[0]
        result.append(selected)
    return result


def build_starter_score_display(away_abbr: str, away_score: int, home_abbr: str, home_score: int) -> str:
    return f"{away_abbr} {away_score} - {home_abbr} {home_score}"


def format_percent_text(value) -> str:
    try:
        return f"{int(round(float(value)))} percent"
    except Exception:
        return "0 percent"


def build_starter_overview(name: str, label: str, stats: dict, seed: int) -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    ip_text = format_starter_ip_for_summary(ip)
    outs = baseball_ip_to_outs(ip)

    if outs < 3:
        variants = {
            "ROUGH": [
                f"{name} was out before he could get through the first inning, and the trouble showed up almost immediately.",
                f"{name} only recorded {ip_text}, and the start got away from him before he had any chance to settle it down.",
            ],
            "HIT_HARD": [
                f"{name} only got {ip_text}, and hitters were on him from the opening inning.",
                f"{name} did not get out of the first, with the damage arriving before he could slow the game down.",
            ],
            "NO_COMMAND": [
                f"{name} only lasted {ip_text}, and the lack of command left him chasing the outing from the start.",
                f"{name} was gone after {ip_text}, with too many missed spots to ever get comfortable.",
            ],
            "SHORT": [
                f"{name} only recorded {ip_text}, so this never took the shape of a normal start.",
                f"{name} was lifted after {ip_text}, leaving the bullpen to absorb the game almost right away.",
            ],
        }
        choices = variants.get(label, variants["SHORT"])
        return choices[seed % len(choices)]

    variants = {
        "GEM": [
            f"{name} turned in one of the cleanest starts of the day, covering {ip_text} without allowing an earned run.",
            f"{name} controlled this game from the first few batters and carried that grip through {ip_text}.",
        ],
        "DOMINANT": [
            f"{name} took charge early and looked overpowering for {ip_text}.",
            f"{name} dictated this start over {ip_text} and never gave the lineup much room to breathe.",
        ],
        "QUALITY": [
            f"{name} gave his club a sturdy {ip_text} and kept the damage in check.",
            f"{name} turned in the kind of {ip_text} that usually gives a team a real chance to win.",
        ],
        "STRIKEOUT": [
            f"{name} missed a lot of bats across {ip_text}, even if the outing was not completely breeze-free.",
            f"{name} leaned on swing-and-miss stuff for {ip_text} and piled up strikeouts along the way.",
        ],
        "SHARP": [
            f"{name} gave a crisp {ip_text} and rarely let the game speed up on him.",
            f"{name} looked sharp over {ip_text} and kept the innings from spilling on him.",
        ],
        "SOLID": [
            f"{name} gave his club a usable {ip_text} and kept the game from getting out of hand while he was in it.",
            f"{name} turned in a steady {ip_text} and left his side something to work from.",
        ],
        "UNEVEN": [
            f"{name} pieced together {ip_text}, though the outing never really felt clean from start to finish.",
            f"{name} had enough workable stretches to get through {ip_text}, but there was traffic almost the whole way.",
        ],
        "SHORT": [
            f"{name} did not last as long as his club needed, getting through only {ip_text}.",
            f"{name} was out earlier than expected after {ip_text}, and the start never found much rhythm.",
        ],
        "ROUGH": [
            f"{name} never really found a comfortable lane, and the outing got away from him over {ip_text}.",
            f"{name} had a rough start and could not keep the game from turning the wrong way over {ip_text}.",
        ],
        "NO_COMMAND": [
            f"{name} spent too much of {ip_text} fighting his command, and the extra traffic kept the line unstable.",
            f"{name} never found enough strikes over {ip_text}, which made every inning feel heavier than it needed to.",
        ],
        "HIT_HARD": [
            f"{name} got hit hard over {ip_text}, and there was no real point where the contact stopped stinging.",
            f"{name} never found much margin through {ip_text}, and too many hittable pitches turned into damage.",
        ],
    }
    return variants.get(label, variants["SOLID"])[seed % len(variants.get(label, variants["SOLID"]))]


def build_starter_stat_sentence(stats: dict, seed: int) -> str:
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    ip_text = format_starter_ip_for_summary(str(stats.get("inningsPitched", "0.0")))
    hit_text = stat_phrase(h, "hit")
    er_text = stat_phrase(er, "earned run")
    bb_text = stat_phrase(bb, "walk")
    k_text = stat_phrase(k, "strikeout")
    choices = [
        f"Across {ip_text}, he allowed {hit_text} and {er_text}, issued {bb_text}, and finished with {k_text}.",
        f"He wound up allowing {hit_text} and {er_text}, with {bb_text} against {k_text} over {ip_text}.",
        f"The stat line came in at {hit_text}, {er_text}, {bb_text}, and {k_text} over {ip_text}.",
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
            f"The bat-missing at least stayed intact, as he still struck out {number_word(k)}.",
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


def build_starter_pressure_sentence(stats: dict, label: str, seed: int) -> str:
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    traffic = h + bb
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))

    if label in GOOD_STARTER_LABELS:
        choices = [
            "Even when runners reached, he closed innings before the game could swing on him.",
            "The few threats against him never really had time to become a crooked number.",
            "He did a good job cutting off trouble before any inning could turn into the whole story.",
        ]
        if k >= 8:
            choices.append("When the lineup threatened, he had enough putaway stuff to shut the inning down himself.")
        if traffic <= 4:
            choices.append("There just were not many clean looks for the lineup, which kept the pressure light most of the way.")
    elif label in BAD_STARTER_LABELS:
        if outs < 3:
            choices = [
                "The trouble showed up before he had any chance to settle the start into a rhythm.",
                "There was no clean reset once the first wave of trouble hit, and the inning kept rolling on him.",
            ]
        else:
            choices = [
                "He never found the quick inning that might have settled things down.",
                "Too many hitters kept reaching, which left him pitching with almost no margin.",
                "Once traffic built, he could not quite stop the outing from dragging in the wrong direction.",
            ]
        if bb >= 4:
            choices.append("He kept working from behind in counts, which made every baserunner feel a little heavier.")
        elif h >= 6 and bb <= 1:
            choices.append("This was less about wildness than hittable pitches finding too much plate.")
        elif traffic >= 10:
            choices.append("There were runners all over the place, and that kept the outing from ever calming down.")
    else:
        choices = [
            "There was enough traffic to keep the outing from feeling clean, even if it never fully cracked.",
            "He had to pitch around a few jams, which gave the line a little more tension than the runs alone suggest.",
            "The outing held together, but there were still a few innings where he had to work for the escape hatch.",
        ]
        if er <= 1 and traffic >= 7:
            choices.append("He did well to limit the damage, because there were enough baserunners for this to get out of hand.")
    return choices[(seed // 5) % len(choices)]


def build_starter_team_context(p: dict, stats: dict, label: str, game_context: dict, seed: int) -> str:
    away_score = safe_int(game_context.get("away_score", 0), 0)
    home_score = safe_int(game_context.get("home_score", 0), 0)
    if p.get("side") == "away":
        team_runs = away_score
        opp_runs = home_score
    else:
        team_runs = home_score
        opp_runs = away_score

    win_decision = safe_int(stats.get("wins", 0), 0) > 0
    won = team_runs > opp_runs
    margin = abs(team_runs - opp_runs)

    if won and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices = [
            "That start let his club play from ahead instead of spending the night trying to chase.",
            "It gave his side a stable middle to hand off to the bullpen.",
            "That line put his club in position to control the rest of the night.",
        ]
        if win_decision:
            choices.append("He wound up with the win, and the start gave him a strong case for it.")
        elif team_runs <= 2:
            choices.append("He did it without much offensive room, which made every clean inning matter a little more.")
    elif won and label in SUBPAR_STARTER_LABELS:
        choices = [
            "His club had enough offense to survive the line, though the start itself was shakier than the final margin.",
            "The bats gave him room to survive it, even if the outing never fully settled.",
            "He had team support behind him, but the line still forced more cleanup than a club wants from its starter.",
        ]
    elif (not won) and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices = [
            "He kept the game within reach, but there was not much support around the outing.",
            "The line was good enough to keep his club in it; the rest of the game just leaned the other way.",
            "He did enough to give his side a chance, even if the support around him never fully matched it.",
        ]
    else:
        choices = [
            "Once the damage landed, his club spent the rest of the night trying to climb back.",
            "The early hole shaped the game from there, and there was not much room to recover after it.",
            "It left his side playing uphill once the outing turned.",
        ]

    if margin >= 5 and won and label in SUBPAR_STARTER_LABELS:
        choices.append("The scoreboard wound up looking comfortable, but the start itself was shakier than that final margin.")
    elif margin == 1 and won and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices.append("In a tight game, those innings wound up carrying real weight.")
    return choices[(seed // 7) % len(choices)]


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
            if app_v is None:
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
            f"His fastball averaged {velo_text}, down {drop_text} from his previous start, and the dip stood out.",
            f"The heater checked in at {velo_text}, which was {drop_text} lighter than his last outing and worth watching.",
        ]
        return choices[(seed // 11) % len(choices)]

    bb = safe_int(p.get("stats", {}).get("baseOnBalls", 0), 0)
    hits = safe_int(p.get("stats", {}).get("hits", 0), 0)
    if label in GOOD_STARTER_LABELS:
        choices = [
            f"His fastball averaged {velo_text}, and that extra life showed up when he needed a putaway pitch.",
            f"The heater sat {velo_text}, which helped him stay aggressive once he got ahead in counts.",
            f"He averaged {velo_text} on the fastball, and it gave him something extra when the inning started to tighten.",
        ]
    elif label in BAD_STARTER_LABELS:
        if bb <= 1 and hits >= 5:
            choices = [
                f"He still averaged {velo_text} on the fastball, so this was more about the contact than the raw arm strength.",
                f"The heater checked in at {velo_text}, but too many hittable pitches still found damage.",
            ]
        else:
            choices = [
                f"He still averaged {velo_text} on the fastball, but the misses around it never came together.",
                f"The heater sat {velo_text}, though the velocity alone was not enough to hold the outing together.",
            ]
    else:
        choices = [
            f"His fastball averaged {velo_text}, which helped him keep competing even when the line got a little uneven.",
            f"The heater sat {velo_text}, and there were stretches where that gave him enough to steady the outing.",
        ]
    return choices[(seed // 11) % len(choices)]


def build_starter_csw_sentence(p: dict, label: str, seed: int) -> str:
    csw = p.get("csw_percent")
    whiffs = safe_int(p.get("whiffs", 0), 0)
    if csw in (None, ""):
        return ""
    try:
        csw_value = float(csw)
    except Exception:
        return ""

    good_threshold = 30.0
    bad_threshold = 25.0
    include = False
    if label in GOOD_STARTER_LABELS and csw_value >= good_threshold:
        include = True
    elif label in SUBPAR_STARTER_LABELS and csw_value <= bad_threshold:
        include = True
    elif label == "SOLID" and (csw_value >= 32.0 or csw_value <= 24.0):
        include = True
    if not include:
        return ""

    csw_text = format_percent_text(csw_value)
    whiff_text = stat_phrase(whiffs, "swing and miss", plural_form="swings and misses")

    if csw_value >= good_threshold:
        if whiffs >= 8:
            choices = [
                f"He generated {whiff_text} and posted a CSW of {csw_text}, which helped him stay ahead once the lineup turned over.",
                f"The bat-missing showed up all night, with {whiff_text} feeding into a CSW of {csw_text}.",
                f"He kept winning pitches, generating {whiff_text} to post a CSW of {csw_text} and close off rallies before they grew.",
            ]
        else:
            choices = [
                f"He posted a CSW of {csw_text}, which says a lot about how often hitters were behind the action.",
                f"A CSW of {csw_text} helped him keep at-bats tilted his way even when runners reached.",
                f"He worked to a CSW of {csw_text}, and that helped him keep control of counts throughout the outing.",
            ]
    else:
        if whiffs >= 1:
            choices = [
                f"He only generated {whiff_text} and finished with a CSW of {csw_text}, so too many plate appearances stayed alive.",
                f"The swing-and-miss never really arrived, as he produced just {whiff_text} and a CSW of {csw_text}.",
                f"He posted a CSW of {csw_text} despite only {whiff_text}, which left very little margin once traffic started.",
            ]
        else:
            choices = [
                f"He finished with a CSW of {csw_text}, which points to how little swing-and-miss he had to work with.",
                f"A CSW of {csw_text} left him pitching to contact too often once counts moved forward.",
            ]
    return choices[(seed // 17) % len(choices)]


def build_starter_pitch_sentence(p: dict, label: str, seed: int) -> str:
    pitches = safe_int(p.get("pitch_count", 0), 0)
    strikes = safe_int(p.get("strikes", 0), 0)
    outs = baseball_ip_to_outs(str(p.get("stats", {}).get("inningsPitched", "0.0")))
    if pitches <= 0 or outs <= 0:
        return ""

    strike_pct = (strikes / pitches) if pitches else 0.0
    strikes_text = f"{number_word(strikes) if strikes <= 12 else strikes} of {number_word(pitches) if pitches <= 12 else pitches} pitches for strikes"
    ip_text = format_starter_ip_for_summary(str(p.get("stats", {}).get("inningsPitched", "0.0")))

    efficient = outs >= 15 and pitches <= max(78, outs * 4.4)
    heavy = pitches >= 95 or (outs <= 15 and pitches >= 80) or strike_pct < 0.58
    strike_throwing = strike_pct >= 0.67

    if efficient:
        choices = [
            f"He stayed efficient, getting through {ip_text} on just {pitches} pitches.",
            f"It only took {pitches} pitches for him to cover {ip_text}, which helped him keep the tempo in his hands.",
            f"He kept the outing moving, finishing {ip_text} on {pitches} pitches without a lot of wasted work.",
        ]
        if strike_throwing:
            choices.append(f"He was on the attack, landing {strikes_text} and keeping most counts short.")
    elif heavy:
        choices = [
            f"The outing got laborious, and he needed {pitches} pitches to get through {ip_text}.",
            f"He had to grind for most of those outs, finishing {ip_text} on {pitches} pitches.",
            f"The pitch count climbed on him quickly enough that {pitches} pitches only bought {ip_text}.",
        ]
        if strikes > 0:
            choices.append(f"Only {strikes_text} kept him from working from ahead often enough.")
    else:
        choices = [
            f"He threw {strikes_text}, which helped him keep enough control of the count to work through {ip_text}.",
            f"The strike throwing mattered here, with {strikes_text} over {ip_text}.",
            f"He got {strikes_text}, and that gave the outing a little more structure than the line might suggest.",
            f"It took {pitches} pitches to finish {ip_text}, a fair snapshot of how much work each inning asked of him.",
        ]
    return choices[(seed // 19) % len(choices)]


def build_starter_summary(p: dict, label: str, game_context: dict, recent_appearances=None) -> str:
    stats = p["stats"]
    seed = build_starter_summary_seed(p["name"], stats, game_context)
    overview = build_starter_overview(p["name"], label, stats, seed)
    stat_sentence = build_starter_stat_sentence(stats, seed)
    pressure_sentence = build_starter_pressure_sentence(stats, label, seed)
    team_sentence = build_starter_team_context(p, stats, label, game_context, seed)
    positive_sentence = build_starter_positive_sentence(stats, label, seed)
    csw_sentence = build_starter_csw_sentence(p, label, seed)
    pitch_sentence = build_starter_pitch_sentence(p, label, seed)
    velocity_sentence = build_starter_velocity_sentence(p, label, seed, recent_appearances=recent_appearances)

    if label in BAD_STARTER_LABELS:
        order_options = [
            [overview, positive_sentence, stat_sentence, pressure_sentence, csw_sentence, pitch_sentence, velocity_sentence, team_sentence],
            [overview, stat_sentence, positive_sentence, csw_sentence, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, pressure_sentence, stat_sentence, positive_sentence, pitch_sentence, csw_sentence, team_sentence, velocity_sentence],
            [overview, stat_sentence, pressure_sentence, pitch_sentence, positive_sentence, team_sentence, csw_sentence, velocity_sentence],
        ]
    else:
        order_options = [
            [overview, csw_sentence, stat_sentence, pressure_sentence, pitch_sentence, velocity_sentence, team_sentence],
            [overview, stat_sentence, pitch_sentence, csw_sentence, pressure_sentence, team_sentence, velocity_sentence],
            [overview, pressure_sentence, stat_sentence, csw_sentence, team_sentence, pitch_sentence, velocity_sentence],
            [overview, pitch_sentence, stat_sentence, pressure_sentence, csw_sentence, velocity_sentence, team_sentence],
        ]

    ordered = [s for s in order_options[seed % len(order_options)] if s]
    final_sentences = []
    for sentence in ordered:
        if sentence and sentence not in final_sentences:
            final_sentences.append(sentence)
        if len(final_sentences) >= 5:
            break

    if len(final_sentences) < 4:
        fillers = [overview, stat_sentence, pressure_sentence, positive_sentence, csw_sentence, pitch_sentence, velocity_sentence, team_sentence]
        for sentence in fillers:
            if sentence and sentence not in final_sentences:
                final_sentences.append(sentence)
            if len(final_sentences) >= 4:
                break

    return " ".join(final_sentences[:5])


async def post_card(channel, p: dict, game_context: dict, score_value: str, recent_appearances=None):
    stats = p["stats"]
    label = classify_starter(stats)
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])
    embed.add_field(name="", value=f"**{starter_impact_tag(label)}**", inline=False)
    embed.add_field(name="⚾ Score", value=score_value, inline=False)
    embed.add_field(name="Game Line", value=format_starter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_starter_season_line(p.get("season_stats", {})), inline=False)
    embed.add_field(name="Summary", value=build_starter_summary(p, label, game_context, recent_appearances=recent_appearances), inline=False)
    await channel.send(embed=embed)


async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    state = load_state()
    posted = set(state.get("posted", []))

    if os.getenv("RESET_STARTER_STATE", "").lower() in {"1", "true", "yes"}:
        log("RESET_STARTER_STATE enabled — posted state cleared for this run")
        posted = set()

    while True:
        try:
            games = get_games()
            log(f"Checking {len(games)} games")

            for g in games:
                if g.get("status", {}).get("detailedState") != "Final":
                    continue
                game_id = g.get("gamePk")
                if not game_id:
                    continue
                feed = get_feed(game_id)
                starters = get_starters(feed)
                game_date_et = parse_game_date_et(g)

                game_teams = feed.get("gameData", {}).get("teams", {})
                away_abbr = game_teams.get("away", {}).get("abbreviation") or g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation") or "AWAY"
                home_abbr = game_teams.get("home", {}).get("abbreviation") or g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation") or "HOME"
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

                ordered = sorted(
                    starters,
                    key=lambda p: (0 if p.get("side") == "away" else 1, -starter_score(p["stats"]), p.get("name", "")),
                )

                for p in ordered:
                    pid = p.get("id")
                    if pid is None:
                        continue
                    key = f"{game_id}_{pid}"
                    if key in posted:
                        continue
                    recent_appearances = get_recent_appearances(pid, game_date_et, limit=5, max_days=21) if game_date_et else []
                    log(f"Posting {p['name']} | {p['team']} | {score_value} | score={starter_score(p['stats'])}")
                    await post_card(channel, p, game_context, score_value, recent_appearances=recent_appearances)
                    posted.add(key)

            state["posted"] = list(posted)
            save_state(state)
        except Exception as e:
            log(f"Loop error: {e}")

        await asyncio.sleep(POLL_MINUTES * 60)


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

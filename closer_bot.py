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

TOKEN = os.getenv("CLOSER_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CLOSER_WATCH_CHANNEL_ID", "0"))

STATE_FILE = "state/closer/state.json"
os.makedirs("state/closer", exist_ok=True)

ET = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = 10
RESET_CLOSER_STATE = os.getenv("RESET_CLOSER_STATE", "").lower() in {"1", "true", "yes"}
DEPTH_CHART_OVERRIDE_CHANNEL_ID = int(os.getenv("DEPTH_CHART_OVERRIDE_CHANNEL_ID", "1484232761597366412"))
TREND_STATE_FILE = "state/closer/trend_state.json"
TREND_OVERNIGHT_START_HOUR = 2
TREND_MAX_PER_HOUR = 3
TREND_MIN_SPACING_MINUTES = 20
TREND_MAX_OVERNIGHT_TOTAL = 25
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


def log(msg: str):
    print(f"[CLOSER] {msg}", flush=True)


def get_logo(team: str) -> str:
    key = team.lower()
    if team == "CWS":
        key = "chw"
    elif team == "ATH":
        key = "oak"
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"


# ---------------- STATE ----------------

def load_state():
    base = {"posted": [], "trend_posted": {}, "trend_history": {}, "trend_last_post_at": None, "trend_post_count_by_hour": {}, "trend_total_by_date": {}, "velocity_posted": {}}

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
                for key in ["trend_posted", "trend_history", "trend_last_post_at", "trend_post_count_by_hour", "trend_total_by_date", "velocity_posted"]:
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
        "trend_post_count_by_hour": state.get("trend_post_count_by_hour", {}),
        "trend_total_by_date": state.get("trend_total_by_date", {}),
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

    if s.get("saves"):
        return "SAVE"

    if s.get("blownSaves"):
        return "BLOWN"

    if s.get("holds"):
        return "HOLD"

    if outs >= 3 and s["er"] == 0 and s["h"] == 0 and s["bb"] == 0:
        return "DOM"

    if s["er"] >= 3:
        return "ROUGH"

    if s["er"] == 0 and (s["h"] + s["bb"]) >= 2:
        return "TRAFFIC"

    if s["er"] == 0 and outs >= 3:
        return "CLEAN"

    if s["er"] == 0:
        return "RELIEF"

    return "RELIEF"


def grade_outing(s: dict) -> str:
    outs = baseball_ip_to_outs(s["ip"])
    baserunners = s["h"] + s["bb"]

    if outs <= 1:
        return "MICRO"

    if s["er"] >= 3:
        return "ROUGH"

    if s["er"] in {1, 2}:
        return "SHAKY"

    if s["er"] == 0 and s["h"] == 0 and s["bb"] == 0 and outs >= 3:
        return "DOMINANT"

    if s["er"] == 0 and baserunners <= 1 and outs >= 3:
        return "CLEAN"

    if s["er"] == 0 and baserunners >= 2:
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
        deficit = abs(diff)
        if deficit == 1:
            state_text = "trailing by one"
        elif deficit == 2:
            state_text = "trailing by two"
        else:
            state_text = f"trailing by {deficit}"
    else:
        state_kind = "tie"
        state_text = "in a tie game"

    return {
        "entry_phrase": entry_phrase,
        "entry_outs_text": entry_outs_text,
        "entry_state_text": state_text,
        "entry_state_kind": state_kind,
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

    outs = baseball_ip_to_outs(s["ip"])
    inning = context.get("entry_inning")
    state_kind = context.get("entry_state_kind", "")
    finished_game = context.get("finished_game", False)

    early_closer_usage = (
        role == "closer"
        and inning is not None
        and inning < 9
        and state_kind in {"lead", "tie"}
    )

    # Micro outings should stay short and neutral.
    if outing_grade == "MICRO":
        if role == "closer" and early_closer_usage:
            options = [
                "He was called on early for a high-leverage spot and got the out.",
                "He handled an early leverage pocket and recorded the out.",
                "He was used before the ninth in an important spot and did his job.",
                "He got the lone hitter he faced in a leverage spot.",
            ]
            return random.choice(options)

        if label == "HOLD":
            options = [
                "He got the one hitter he faced to help secure the hold.",
                "He retired the lone batter he faced and helped finish the bridge inning.",
                "He handled his one matchup cleanly in a hold situation.",
            ]
            return random.choice(options)

        options = [
            "He got the one hitter he faced.",
            "He retired the lone batter he faced.",
            "He handled his brief assignment cleanly.",
            "He recorded the out he was asked to get.",
        ]
        return random.choice(options)

    # Bad lines override label positivity.
    if outing_grade in {"SHAKY", "ROUGH"}:
        if label == "SAVE":
            options = [
                "He got the save, but this wasn’t a clean outing.",
                "He finished it off, though the line was shakier than you’d want from a closer.",
                "The save counts, but this one came with some blemishes.",
                "He converted the chance, but the outing itself was far from crisp.",
            ]
            return random.choice(options)

        if label == "HOLD":
            options = [
                "He got the hold, but this wasn’t a clean outing.",
                "The hold is there, though the line itself was shaky.",
                "He picked up the hold, but the appearance brought more damage than you’d like.",
                "He escaped with the hold, even if the outing itself was messy.",
            ]
            return random.choice(options)

        if label == "BLOWN":
            options = [
                "This was a rough result in a leverage spot.",
                "He couldn’t keep the inning under control when the game tightened up.",
                "It was a tough look in a high-leverage chance.",
                "The line reflects a costly outing late in the game.",
            ]
            return random.choice(options)

        if trend == "DOWN":
            options = [
                "This was another shaky outing, and the recent form isn’t helping his case.",
                "He’s in a rough patch right now, and this one added to it.",
                "The recent trend has been uneven, and this outing didn’t change that.",
            ]
            return random.choice(options)

        options = [
            "This wasn’t a clean line, even if he got through the inning.",
            "He allowed too much traffic for this to qualify as a sharp outing.",
            "The line was more uneven than effective here.",
            "This was more survival than dominance.",
        ]
        return random.choice(options)

    # Closer-specific clean outcomes, only if role is verified.
    if role == "closer":
        if early_closer_usage and label in {"SAVE", "HOLD"}:
            early_options = [
                "He was called on before the ninth in a high-leverage spot and handled it like the bullpen anchor.",
                "The early usage shows this was one of the biggest spots in the game, and he answered it.",
                "Being used before the ninth speaks to the leverage of the moment, and he came through.",
                "He got the biggest pocket before the ninth and handled it cleanly.",
            ]
            if outing_grade in {"DOMINANT", "CLEAN", "TRAFFIC"}:
                return random.choice(early_options)

        if label == "SAVE":
            if outing_grade == "DOMINANT":
                base = [
                    "He remains firmly in control of the ninth inning.",
                    "He still looks like the clear closer here.",
                    "He keeps a strong hold on save chances in this bullpen.",
                    "He continues to look like the go-to arm in the ninth.",
                ]
                trend_up = [
                    "He remains firmly in control of the ninth and has backed it up over his recent outings.",
                    "He still looks like the clear closer here, and the recent form supports it.",
                    "He keeps a strong hold on the ninth with another good outing in a solid recent stretch.",
                ]
                if trend == "UP":
                    return random.choice(trend_up)
                return random.choice(base)

            if outing_grade in {"CLEAN", "TRAFFIC", "NEUTRAL"}:
                options = [
                    "He still looks like the clear ninth-inning option here.",
                    "He remains the top save arm in this bullpen.",
                    "He continues to hold the closer role here.",
                    "He still looks like the primary answer for saves.",
                ]
                return random.choice(options)

        if label == "BLOWN":
            options = [
                "One outing doesn’t rewrite the hierarchy, but this does add a little short-term pressure.",
                "He’s still the closer here, though this result will draw more attention than usual.",
                "The role may still be his, but this is the kind of outing that gets noticed.",
                "This doesn’t erase the role, but it does create some short-term doubt.",
            ]
            return random.choice(options)

        # verified closer, but not in a save event
        options = [
            "He remains one of the key late-game arms in this bullpen.",
            "He still looks like a central leverage piece for this staff.",
            "He continues to handle important late-game work.",
        ]
        return random.choice(options)

    # Committee / save-mix language
    if role == "committee":
        if label == "SAVE":
            options = [
                "This keeps him firmly in the save mix.",
                "He helped his case for future save chances.",
                "In a fluid bullpen, this outing keeps him squarely in the conversation.",
                "This should keep him in the late-inning mix for chances.",
            ]
            return random.choice(options)

        if label in {"HOLD", "DOM", "CLEAN"}:
            options = [
                "He remains in the late-inning mix for this bullpen.",
                "This keeps him relevant in a bullpen without a fully settled pecking order.",
                "He continues to strengthen his case for meaningful leverage work.",
                "In a fluid bullpen, outings like this help his standing.",
            ]
            return random.choice(options)

    # Setup / high-leverage but not closer
    if role == "setup":
        if label == "HOLD":
            if outing_grade == "DOMINANT" and trend == "UP":
                options = [
                    "He remains a trusted setup option and has been trending the right way lately.",
                    "He continues to handle leverage work well, and the recent run supports that.",
                    "He’s one of the steadier bridge arms here, and the recent form has been strong.",
                ]
                return random.choice(options)

            if outing_grade == "DOMINANT":
                options = [
                    "He continues to handle important late-inning work.",
                    "He remains a trusted setup option here.",
                    "He keeps himself firmly in the leverage mix.",
                    "He continues to hold a meaningful late-inning role.",
                ]
                return random.choice(options)

            if outing_grade in {"CLEAN", "TRAFFIC", "NEUTRAL"}:
                options = [
                    "He remains one of the more trusted bridge arms here.",
                    "He continues to work meaningful late-inning spots.",
                    "He stays in the leverage mix with another useful outing.",
                    "He continues to be a factor in setup situations.",
                ]
                return random.choice(options)

        if outing_grade == "DOMINANT":
            if trend == "UP":
                options = [
                    "He’s putting together a strong run and looks like a rising leverage arm.",
                    "The recent form has been good, and outings like this keep him moving up the leverage ladder.",
                    "He’s been building momentum lately, and this was another strong step.",
                ]
                return random.choice(options)

            options = [
                "This was a strong outing from a pitcher already working in meaningful spots.",
                "He looked sharp in another important inning for this bullpen.",
                "He continues to make his case as a trusted late-inning arm.",
            ]
            return random.choice(options)

    # Generic relief language
    if outing_grade == "DOMINANT":
        if trend == "UP":
            options = [
                "He’s putting together a solid recent run of work.",
                "The recent form has been good, and this outing fit that trend.",
                "He’s been stringing together cleaner appearances lately.",
            ]
            return random.choice(options)

        options = [
            "This was a strong outing.",
            "He turned in one of his sharper appearances here.",
            "He handled the inning cleanly and effectively.",
            "It was a crisp line from start to finish.",
        ]
        return random.choice(options)

    if outing_grade == "CLEAN":
        options = [
            "He handled the inning cleanly.",
            "This was a steady, effective appearance.",
            "He did his job without much trouble.",
            "He gave them a solid inning here.",
        ]
        return random.choice(options)

    if outing_grade == "TRAFFIC":
        options = [
            "He worked through traffic and still got the job done.",
            "It wasn’t spotless, but he managed the inning well enough.",
            "He navigated some traffic and kept the game from turning.",
            "He bent a bit but kept the inning intact.",
        ]
        return random.choice(options)

    options = [
        "He turned in a usable inning for this bullpen.",
        "This was a neutral relief appearance overall.",
        "He got through the inning without changing much about his standing.",
        "He gave them a serviceable inning here.",
    ]
    return random.choice(options)


# ---------------- SUMMARY ----------------

def build_summary(name: str, team: str, s: dict, label: str, context: dict, streak_count: int, tracked_info: dict, recent_appearances):
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
        line1 = f"{name} entered {ctx} but couldn’t hold the lead and was charged with a blown save."

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

    # One-batter / one-out handling
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
        if k_part:
            line2 += f" {k_part}."
        else:
            line2 += "."
    else:
        line2 = f"He allowed {run_text} over {ip_text} on {hit_text} and {walk_text}"
        k_part = strikeout_phrase(k)
        if k_part:
            line2 += f" {k_part}."
        else:
            line2 += "."

    analysis = build_analysis(
        p={"name": name, "team": team},
        s=s,
        label=label,
        context=context,
        tracked_info=tracked_info,
        recent_appearances=recent_appearances,
    )

    streak_sentence = get_streak_sentence(streak_count)

    if streak_sentence:
        return f"{line1} {line2} {analysis} {streak_sentence}"

    return f"{line1} {line2} {analysis}"



# ---------------- TREND ALERT ENGINE ----------------

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
    templates = {
        "scoreless3": [
            f"{name} has now strung together three straight scoreless outings. The recent form has been sharp, and he is forcing his way onto the radar in this bullpen.",
            f"{name} has put together three straight scoreless appearances and keeps stacking clean work. This is the kind of run that can push a reliever into more meaningful opportunities.",
        ],
        "scoreless5": [
            f"{name} has now turned in five straight scoreless outings, one of the better recent runs in this bullpen. The consistency has stood out, and his name is becoming harder to ignore.",
            f"Five straight scoreless appearances have put {name} firmly on the radar in this bullpen. He keeps getting results, and stretches like this can change a reliever's place quickly.",
        ],
        "scoreless4of5": [
            f"{name} has been scoreless in four of his last five outings and is building real momentum. The recent body of work is strong enough to make him worth tracking more closely.",
        ],
        "ks10last5": [
            f"{name} has racked up {info.get('ks_last5', 0)} strikeouts over his last five appearances and the bat-missing has become impossible to miss. This is the kind of swing-and-miss stretch that can open bigger doors.",
        ],
        "ks7last3": [
            f"{name} has piled up {info.get('ks_last3', 0)} strikeouts across his last three outings and is missing bats at a high rate. The recent stuff has jumped off the page.",
            f"Strikeouts are starting to pile up for {name}, who has {info.get('ks_last3', 0)} over his last three appearances. That kind of bat-missing can move a reliever up the ladder in a hurry.",
        ],
        "k_streak3": [
            f"{name} has recorded a strikeout in three straight outings and keeps bringing swing-and-miss to the mound. The recent run gives him some real momentum.",
        ],
        "dominant_last3": [
            f"{name} has delivered multiple dominant appearances over his last three turns and is clearly in a strong stretch. The recent form has looked a level above ordinary middle relief work.",
        ],
        "dominant_k_combo": [
            f"{name} has stacked power outings lately, with multiple recent appearances featuring scoreless work and at least two strikeouts. That kind of combination gets attention fast.",
        ],
        "no_walk3": [
            f"{name} has gone three straight outings without issuing a walk, and the recent command has been a real positive. Clean strike-throwing runs like this tend to matter.",
        ],
        "saves2": [
            f"{name} has converted saves in back-to-back appearances and is starting to show up in more meaningful spots. That usage is worth keeping an eye on.",
        ],
        "holds3": [
            f"{name} has now collected holds in three straight appearances and continues to show up in useful spots. The trust level looks to be moving in the right direction.",
        ],
        "multi_inning3": [
            f"{name} has put together a string of successful multi-inning outings, giving this bullpen useful length without sacrificing results. That kind of work can earn a bigger role over time.",
        ],
        "bounce_blown": [
            f"After a blown save last time out, {name} answered with a clean bounce-back appearance. That helps settle the recent form and keeps him from drifting the wrong way.",
        ],
        "bounce_rough": [
            f"{name} bounced back with a cleaner outing after running into trouble previously. It was a needed response and a step back in the right direction.",
        ],
        "clean_rebound_2bad": [
            f"After two rougher appearances, {name} steadied things with a clean rebound outing. He needed a better line, and this was at least a start.",
        ],
        "runs2of3": [
            f"{name} has now allowed runs in two of his last three appearances, and the recent form is starting to turn shaky. He will need a cleaner outing soon to stop the slide.",
        ],
        "runs3of5": [
            f"{name} has been tagged in three of his last five outings, and the recent trend has gone in the wrong direction. The results have been too uneven to ignore.",
        ],
        "scoreless_snapped": [
            f"A scoreless run came to an end for {name}, who had been putting together cleaner work lately. The recent momentum took a hit with this one.",
        ],
        "first_rough_after_hot": [
            f"{name} hit his first real bump after a stronger recent stretch. One outing does not erase the progress, but it does cool the momentum a bit.",
        ],
        "late_inning_clean": [
            f"{name} handled another clean late-inning look and continues to see work that matters more than ordinary middle relief. That is the kind of deployment shift worth noticing.",
        ],
        "higher_leverage_usage": [
            f"The recent usage for {name} has started to creep into more meaningful territory, and he answered with another clean line. This looks like a reliever getting a stronger look.",
        ],
        "save_conversion": [
            f"{name} closed the door in his latest chance and is beginning to show up in spots that matter. Save chances like this can change a bullpen conversation quickly.",
        ],
    }
    options = templates.get(code, [f"{name} has put together a notable recent run and is worth monitoring more closely. The recent trend has started to stand out."])
    body = random.choice(options)
    second = random.choice([
        f"He remains a name to watch if this keeps going.",
        f"This is the sort of stretch that can earn more meaningful work.",
        f"It is a trend worth monitoring moving forward.",
    ])
    if body.count('.') >= 2:
        return body
    return f"{body} {second}"


def can_post_trend_now(state, now_et: datetime):
    hour_key = now_et.strftime("%Y-%m-%d-%H")
    day_key = now_et.strftime("%Y-%m-%d")
    count_this_hour = safe_int(state.get("trend_post_count_by_hour", {}).get(hour_key, 0), 0)
    total_today = safe_int(state.get("trend_total_by_date", {}).get(day_key, 0), 0)
    if now_et.hour >= TREND_OVERNIGHT_START_HOUR and total_today >= TREND_MAX_OVERNIGHT_TOTAL:
        return False
    if count_this_hour >= TREND_MAX_PER_HOUR:
        return False
    last_post = state.get("trend_last_post_at")
    if last_post:
        try:
            last_dt = datetime.fromisoformat(last_post)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=ET)
            else:
                last_dt = last_dt.astimezone(ET)
            if (now_et - last_dt).total_seconds() < TREND_MIN_SPACING_MINUTES * 60:
                return False
        except Exception:
            pass
    return True


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
    state.setdefault("trend_post_count_by_hour", {})[hour_key] = safe_int(state.setdefault("trend_post_count_by_hour", {}).get(hour_key, 0), 0) + 1
    state.setdefault("trend_total_by_date", {})[day_key] = safe_int(state.setdefault("trend_total_by_date", {}).get(day_key, 0), 0) + 1


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
        title=f"{name} | {team}",
        color=TEAM_COLORS.get(team, 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    try:
        embed.set_thumbnail(url=get_logo(team))
    except Exception:
        pass
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
            f"{name} averaged **{current_v:.1f} MPH** on his fastball in this outing, up from **{baseline_v:.1f} MPH** in his {baseline_type}.",
            f"{name}'s fastball averaged **{current_v:.1f} MPH** here after sitting at **{baseline_v:.1f} MPH** in his {baseline_type}.",
        ]
        closers = [
            f"The **+{change_text}** jump stands out and suggests his stuff had extra life in this appearance.",
            f"That **+{change_text}** bump is noticeable and could be a sign that his stuff is trending in the right direction.",
        ]
        thirds = [
            "It is the kind of change worth keeping an eye on if it holds into his next outing.",
            "If that carries forward, it adds another reason to pay attention to his recent run.",
        ]
    else:
        starters = [
            f"{name} averaged **{current_v:.1f} MPH** on his fastball in this outing, down from **{baseline_v:.1f} MPH** in his {baseline_type}.",
            f"{name}'s fastball averaged **{current_v:.1f} MPH** here after sitting at **{baseline_v:.1f} MPH** in his {baseline_type}.",
        ]
        closers = [
            f"The dip of **{change_text}** is noticeable and worth monitoring moving forward.",
            f"That **-{change_text}** shift is enough to stand out and is something to keep an eye on in his next appearance.",
        ]
        thirds = [
            "Changes like this can simply reflect a single-night blip, but it is still meaningful enough to flag.",
            "One outing does not make a trend, but velocity changes like this are worth noting when they show up.",
        ]

    sentence1 = random.choice(starters)
    sentence2 = random.choice(closers)
    if random.random() < 0.55:
        return f"{sentence1} {sentence2} {random.choice(thirds)}"
    return f"{sentence1} {sentence2}"


async def post_velocity_card(channel, meta: dict, velocity_alert: dict):
    team = meta.get("team", "UNK")
    name = meta.get("name", "Unknown Pitcher")
    subject = f"{velocity_alert.get('emoji', '⚠️')} {velocity_alert.get('subject', 'Velocity Alert')}"
    embed = discord.Embed(
        title=f"{name} | {team}",
        color=TEAM_COLORS.get(team, 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    try:
        embed.set_thumbnail(url=get_logo(team))
    except Exception:
        pass
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
        team = game_teams.get(side, {}).get("abbreviation")
        if not team:
            team = box.get(side, {}).get("team", {}).get("abbreviation", "UNK")

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
                "team": team,
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
                    "team": team,
                    "season_stats": season_stats,
                }

    return result


# ---------------- POST ----------------

async def post_card(channel, p: dict, matchup: str, score: str, context: dict, streak_count: int, tracked_info: dict, recent_appearances):
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

    if label == "SAVE":
        title = f"🚨 SAVE — {p['name']} ({p['team']})"
    elif label == "BLOWN":
        title = f"⚠️ BLOWN SAVE — {p['name']} ({p['team']})"
    else:
        title = f"{p['name']} ({p['team']})"

    embed = discord.Embed(
        title=title,
        color=TEAM_COLORS.get(p["team"], 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )

    try:
        embed.set_thumbnail(url=get_logo(p["team"]))
    except Exception:
        pass

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
        ),
        inline=False,
    )
    embed.add_field(name="Final", value=score, inline=False)

    await channel.send(embed=embed)


# ---------------- LOOP ----------------

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
                    log(f"Posting {p['name']} | {p['team']} | {matchup}")
                    await post_card(channel, p, matchup, score, context, streak_count, tracked_info, recent_appearances)

                    velocity_alert = build_velocity_alert(current_app, recent_for_trend)
                    if should_post_velocity_alert(state, pitcher_id, game_id, velocity_alert, now_et):
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
            if now_et.hour >= TREND_OVERNIGHT_START_HOUR or now_et.hour < 12:
                trend_candidates = gather_trend_candidates_from_recent_games(tracked, processed_pitchers_by_game)
                for candidate in trend_candidates:
                    if not can_post_trend_now(state, now_et):
                        break
                    pid = candidate["pitcher_id"]
                    sig = appearance_signature(candidate["recent_appearances"])
                    existing = state.get("trend_posted", {}).get(str(pid), {})
                    trend_family = candidate["trend"].get("family", "misc")
                    if existing.get("sig") == sig and existing.get("family") == trend_family:
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
        raise RuntimeError("CLOSER_BOT_TOKEN is not set")

    await client.start(TOKEN, reconnect=True)

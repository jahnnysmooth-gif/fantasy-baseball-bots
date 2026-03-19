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
DEPTH_CHART_CHANNEL_ID = int(os.getenv("DEPTH_CHART_CHANNEL_ID", "1484232761597366412"))

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
    if RESET_CLOSER_STATE:
        return {"posted": []}

    if not os.path.exists(STATE_FILE):
        return {"posted": []}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"posted": []}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


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


# ---------------- CLOSER MONKEY TRACKING ----------------

def refresh_tracked_pitchers():
    try:
        teams = await fetch_closer_depth_chart(client, 1484232761597366412)
        if not teams:
            log("Closer Monkey refresh returned no teams, using saved depth chart")
    except Exception as e:
        log(f"Closer Monkey refresh failed: {e}")

    tracked = build_tracked_relief_map()
    log(f"Loaded {len(tracked)} tracked relievers from Closer Monkey")
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
    """
    Conservative role inference:
    - only call someone a closer if the depth chart clearly says so
    - otherwise fall back to committee/setup/relief language
    """
    if not tracked_info:
        return "relief"

    fields_to_check = [
        "role",
        "tier",
        "slot",
        "label",
        "bucket",
        "depth_role",
        "status",
        "type",
        "group",
        "rank_label",
    ]

    texts = []
    for field in fields_to_check:
        value = tracked_info.get(field)
        if value is not None:
            texts.append(str(value).strip().lower())

    combined = " | ".join(texts)

    if any(term in combined for term in ["committee", "co-closer", "co closer", "save mix", "shared", "timeshare"]):
        return "committee"

    if any(term in combined for term in ["closer", "primary closer", "clear closer"]):
        return "closer"

    if any(term in combined for term in ["setup", "set-up", "high leverage", "high-leverage", "8th inning", "8th", "fireman"]):
        return "setup"

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

                    stats_by_pitcher[pid] = {
                        "ip": str(stats.get("inningsPitched", "0.0")),
                        "h": safe_int(stats.get("hits", 0), 0),
                        "er": safe_int(stats.get("earnedRuns", 0), 0),
                        "bb": safe_int(stats.get("baseOnBalls", 0), 0),
                        "k": safe_int(stats.get("strikeOuts", 0), 0),
                        "saves": safe_int(stats.get("saves", 0), 0),
                        "holds": safe_int(stats.get("holds", 0), 0),
                        "blownSaves": safe_int(stats.get("blownSaves", 0), 0),
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

    if k >= 3:
        return f"while punching out {k} {plural('batter', k)}"

    return f"while striking out {k} {plural('batter', k)}"


# ---------------- ANALYSIS ----------------

def build_analysis(p: dict, s: dict, label: str, context: dict, tracked_info: dict, recent_appearances, streak_count: int = 0):
    role = infer_role_from_tracked_info(tracked_info)
    outing_grade = grade_outing(s)
    trend = get_recent_trend(recent_appearances)

    inning = context.get("entry_inning")
    state_kind = context.get("entry_state_kind", "")
    finished_game = context.get("finished_game", False)

    early_closer_usage = (
        role == "closer"
        and inning is not None
        and inning < 9
        and state_kind in {"lead", "tie"}
    )

    usage_sentence = build_usage_sentence(streak_count, recent_appearances)
    implication_sentence = build_implication_sentence(role, label, outing_grade, trend, early_closer_usage)

    if outing_grade == "ONE_BATTER":
        if role == "closer" and early_closer_usage:
            base = random.choice([
                "He was used before the ninth because this was one of the biggest leverage pockets in the game, and he got the only hitter he faced.",
                "The early call shows the leverage of the moment, and he retired the lone batter he faced.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if label == "HOLD":
            base = random.choice([
                "He got the one out he was asked to get in a hold situation.",
                "He handled a one-batter bridge assignment and recorded the out.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        base = random.choice([
            "He handled a one-batter assignment cleanly.",
            "He retired the only batter he faced.",
            "He recorded the out he was asked to get.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if outing_grade in {"SHAKY", "ROUGH"}:
        if role == "closer":
            base = random.choice([
                "He is still the clear closer here, even if this outing was not sharp.",
                "The outing was messy, but he still sits at the top of this bullpen.",
                "This was not clean, but it does not change who leads this bullpen.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if role == "co_closer":
            base = random.choice([
                "He is still part of the shared closer picture, even if this outing was not sharp.",
                "This was a shaky line, but he remains in the late-game split here.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if role == "setup":
            base = random.choice([
                "This was not a sharp setup outing.",
                "He ran into trouble in a meaningful bridge spot.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if role == "leverage_arm":
            base = random.choice([
                "He ran into trouble in this middle-relief outing.",
                "This was not a sharp middle-relief appearance.",
                "He could not keep this middle-relief outing under control.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if label == "SAVE":
            base = random.choice([
                "He got the save, but this was not a clean outing.",
                "The save counts, but the line itself was shakier than you want from a closer.",
                "He converted the chance, though the outing itself came with some real stress.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if label == "HOLD":
            base = random.choice([
                "He got the hold, but this was not a clean outing.",
                "The hold is there, though the appearance itself was shaky.",
                "He came away with the hold, but the line was messier than it needed to be.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if label == "BLOWN":
            base = random.choice([
                "This was a rough result in a leverage spot.",
                "He could not keep the inning under control when the game tightened up.",
                "It was a costly late-game outing.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        base = random.choice([
            "This was more damage control than a sharp relief appearance.",
            "He allowed too much trouble for this to count as a clean outing.",
            "The line was more uneven than effective here.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if role == "closer":
        if early_closer_usage and label in {"SAVE", "HOLD"}:
            if finished_game:
                base = random.choice([
                    "He was used before the ninth because this was one of the biggest leverage spots in the game, and he finished the job.",
                    "The early deployment shows this was the key pocket of the game, and he closed it out from there.",
                ])
            else:
                base = random.choice([
                    "He was used before the ninth because this looked like the biggest leverage pocket in the game.",
                    "The early call shows the staff treated this as the key spot, even before the ninth arrived.",
                ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if label == "SAVE":
            if outing_grade == "DOMINANT":
                base = random.choice([
                    "This was the kind of clean save line you want from a closer.",
                    "He looked every bit like the bullpen anchor in this save chance.",
                ])
                return join_sentences(base, implication_sentence, usage_sentence)
            base = random.choice([
                "He remains the clear closer here.",
                "He still looks like the bullpen anchor for this staff.",
                "He is still the top late-game arm in this bullpen.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if label == "BLOWN":
            base = random.choice([
                "One outing does not rewrite the hierarchy, but this will draw attention.",
                "He is still the closer here, though this result adds some short-term pressure.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        base = random.choice([
            "He remains the clear closer here.",
            "He still looks like the bullpen anchor for this staff.",
            "He is still the top late-game arm in this bullpen.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if role == "co_closer":
        if label == "SAVE":
            base = random.choice([
                "He remains part of the shared closer picture and converted this chance cleanly.",
                "This was another useful finish from one of the shared late-game arms here.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        base = random.choice([
            "He still looks like one of the primary late-game options in this bullpen.",
            "This keeps him firmly in the shared closer mix.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if role == "committee":
        if label == "SAVE":
            base = random.choice([
                "This keeps him squarely in the save mix.",
                "In a bullpen without a locked hierarchy, this helps his case for future chances.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        base = random.choice([
            "He remains in the late-inning conversation for this bullpen.",
            "This keeps him relevant in a bullpen that still has some fluidity.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if role == "setup":
        if label == "HOLD":
            if outing_grade == "DOMINANT" and trend == "UP":
                base = random.choice([
                    "He remains a trusted setup option and the recent form has been strong.",
                    "He keeps handling meaningful bridge work well, and the recent trend supports it.",
                ])
                return join_sentences(base, implication_sentence, usage_sentence)
            base = random.choice([
                "He remains one of the more trusted bridge arms here.",
                "He continues to work meaningful late-inning spots for this bullpen.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if outing_grade == "DOMINANT":
            if trend == "UP":
                base = random.choice([
                    "He is putting together a strong recent run and looks like a rising leverage arm.",
                    "The recent form has been good, and outings like this keep him moving up the leverage ladder.",
                ])
                return join_sentences(base, implication_sentence, usage_sentence)
            base = random.choice([
                "This was a strong outing from a pitcher already working in meaningful spots.",
                "He looked sharp in another important inning for this bullpen.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        base = random.choice([
            "He still projects as a trusted bridge arm here.",
            "He remains part of the regular setup mix.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if role == "leverage_arm":
        if outing_grade == "DOMINANT":
            base = random.choice([
                "He looked like a leverage arm in this outing.",
                "This was the kind of line you want from a high-strikeout middle reliever.",
                "He pitched like a middle relief weapon here.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        if outing_grade in {"CLEAN", "TRAFFIC"}:
            base = random.choice([
                "He gave them a useful middle-innings line here.",
                "This was a solid middle-relief appearance.",
                "He turned in a helpful inning from the middle-relief group.",
            ])
            return join_sentences(base, implication_sentence, usage_sentence)
        base = random.choice([
            "He gave them a serviceable middle-relief inning here.",
            "This was a usable middle-relief appearance overall.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if outing_grade == "DOMINANT":
        base = random.choice([
            "This was a strong outing.",
            "He turned in one of his sharper appearances here.",
            "It was a crisp line from start to finish.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if outing_grade == "CLEAN":
        base = random.choice([
            "He handled the inning cleanly.",
            "This was a steady, effective appearance.",
            "He did his job without much trouble.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    if outing_grade == "TRAFFIC":
        base = random.choice([
            "He worked through traffic and still got the job done.",
            "It was not spotless, but he kept the inning from turning.",
            "He navigated some traffic and came out of it scoreless.",
        ])
        return join_sentences(base, implication_sentence, usage_sentence)

    base = random.choice([
        "He turned in a usable inning for this bullpen.",
        "This was a neutral relief appearance overall.",
        "He gave them a serviceable inning here.",
    ])
    return join_sentences(base, implication_sentence, usage_sentence)


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

    # One-batter / one-out handling
    if outs_recorded == 1:
        if er == 0 and h == 0 and bb == 0:
            line2 = "He retired the lone batter he faced."
        elif er == 0:
            line2 = f"He got the out he was asked to get, allowing {h} {plural('hit', h)} and {bb} {plural('walk', bb)}."
        else:
            line2 = f"He recorded one out while allowing {er} {plural('run', er)} on {h} {plural('hit', h)} and {bb} {plural('walk', bb)}."
    elif er == 0 and h == 0 and bb == 0:
        if k > 0:
            line2 = f"He retired all hitters he faced over {ip_text} {strikeout_phrase(k).replace('while ', '')}."
        else:
            line2 = f"He retired all hitters he faced over {ip_text}."
    elif er == 0:
        line2 = f"He worked {ip_text}, allowing {h} {plural('hit', h)} and {bb} {plural('walk', bb)}"
        k_part = strikeout_phrase(k)
        if k_part:
            line2 += f" {k_part}."
        else:
            line2 += "."
    else:
        line2 = f"He allowed {er} {plural('run', er)} over {ip_text} on {h} {plural('hit', h)} and {bb} {plural('walk', bb)}"
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

            result.append({
                "id": p.get("person", {}).get("id"),
                "name": p.get("person", {}).get("fullName", "Unknown Pitcher"),
                "team": team,
                "side": side,
                "stats": stats,
                "season_stats": season_stats,
            })

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
            current_date = datetime.now(ET).date()
            if current_date != last_refresh_date:
                tracked = await refresh_tracked_pitchers()
                last_refresh_date = current_date

            games = get_games()
            log(f"Checking {len(games)} games")

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
                away_abbr = game_teams.get("away", {}).get("abbreviation")
                home_abbr = game_teams.get("home", {}).get("abbreviation")

                if not away_abbr:
                    away_abbr = g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation") or "AWAY"
                if not home_abbr:
                    home_abbr = g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation") or "HOME"

                away_score = safe_int(g.get("teams", {}).get("away", {}).get("score", 0), 0)
                home_score = safe_int(g.get("teams", {}).get("home", {}).get("score", 0), 0)

                matchup = f"{away_abbr} @ {home_abbr}"
                score = build_score_line(away_abbr, away_score, home_abbr, home_score)

                for p in pitchers:
                    pitcher_id = p.get("id")
                    if pitcher_id is None:
                        continue

                    key = f"{game_id}_{pitcher_id}"
                    if key in posted:
                        continue

                    tracked_info = find_tracked_pitcher_info(p["name"], p["team"], tracked)
                    is_save = safe_int(p["stats"].get("saves", 0), 0) > 0
                    is_tracked = tracked_info is not None

                    if not (is_save or is_tracked):
                        continue

                    context = get_pitcher_entry_context(feed, pitcher_id, p["side"])
                    streak_count = get_streak_count(pitcher_id, game_date_et)
                    recent_appearances = get_recent_appearances(pitcher_id, game_date_et, limit=5, max_days=21)

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
                    )
                    posted.add(key)

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

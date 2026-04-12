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
from utils.team_data import TEAM_COLORS, normalize_team_abbr, get_logo, normalize_lookup_name

# ---------------- CONFIG ----------------

TOKEN = os.getenv("CLOSER_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CLOSER_WATCH_CHANNEL_ID", "0"))

STATE_FILE = "state/closer/state.json"
os.makedirs("state/closer", exist_ok=True)

ET = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType=R"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"
PLAYER_SEASON_STATS_URL = "https://statsapi.mlb.com/api/v1/people/{}/stats?stats=season&group=pitching&sportId=1&gameType=R&season={}"

POLL_MINUTES = 10
POST_STAGGER_SECONDS = 45
MAX_POSTS_PER_LOOP = 4
GAME_RECENCY_HOURS = 15  # use start time as proxy; 15h covers a game starting at 10 PM + 3h game + buffer
RESET_CLOSER_STATE = os.getenv("RESET_CLOSER_STATE", "").lower() in {"1", "true", "yes"}
ANTHROPIC_API_KEY = os.getenv("bullpen_bot_summary", "")
DEPTH_CHART_OVERRIDE_CHANNEL_ID = int(os.getenv("DEPTH_CHART_OVERRIDE_CHANNEL_ID", "1484232761597366412"))
TREND_STATE_FILE = "state/closer/trend_state.json"

TREND_FAMILY_COOLDOWN_MINUTES = {
    "strikeout": 240,    # raised — bat-missing trends are high signal, don't flood
    "dominance": 240,    # raised — dominant streak/run are premium signals
    "role": 200,         # emerging closers/holds — meaningful but can repeat if different pitchers
    "rough": 180,        # blown save watch is top priority rough signal
    "usage": 180,        # late-inning leverage patterns
    "specialist": 180,   # inherited runner stopper
    "bounce": 150,       # bounce-back requires 2 clean outings now, less noise
    "command": 150,      # command spike — solid but secondary
    "workload": 120,     # fatigue flags can fire more often, different pitchers
    "misc": 120,
}
TREND_RANDOM_INTERVAL_MIN_MINUTES = 6
TREND_RANDOM_INTERVAL_MAX_MINUTES = 16
TREND_HOURS_START = 2   # 2 AM ET — trend blurbs + velocity alerts window opens
TREND_HOURS_END = 14    # 2 PM ET — trend blurbs + velocity alerts window closes
CARD_HOURS_START = 14   # 2 PM ET — tracked card posting window opens
CARD_HOURS_END = 2      # 2 AM ET — tracked card posting window closes (crosses midnight)
TREND_MAX_PER_HOUR = 2
VELOCITY_DELTA_THRESHOLD = 1.0
VELOCITY_MIN_PITCHES = 10
VELOCITY_MIN_FASTBALLS = 3
FASTBALL_PITCH_CODES = {"FF", "FT", "SI", "FC", "FA", "FS"}

appearance_cache = {}
pitching_stats_cache = {}
player_meta_cache = {}
season_stats_cache = {}  # pid -> regular season pitching stats dict

ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
player_headshot_index = None


def log(msg: str):
    print(f"[CLOSER] {msg}", flush=True)


MAX_PLAYER_CACHE_SIZE = 1500


def cleanup_closer_caches():
    """Prune date-keyed caches to 30 days; cap player-id caches at MAX_PLAYER_CACHE_SIZE."""
    from datetime import date as _date
    cutoff = datetime.now(ET).date() - timedelta(days=30)

    pruned = 0
    for cache in (pitching_stats_cache, appearance_cache):
        stale = [k for k in list(cache) if isinstance(k, _date) and k < cutoff]
        for k in stale:
            del cache[k]
        pruned += len(stale)

    if len(player_meta_cache) > MAX_PLAYER_CACHE_SIZE:
        player_meta_cache.clear()
        log("player_meta_cache cleared (size cap reached)")

    if pruned:
        log(f"Cache cleanup: pruned {pruned} stale date entries from pitching/appearance caches")



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


def stat_phrase(count: int, singular: str, plural_form: str | None = None, zero_text: str | None = None, use_article: bool = False) -> str:
    if plural_form is None:
        plural_form = f"{singular}s"
    if count == 0:
        return zero_text or f"no {plural_form}"
    if count == 1:
        return f"a {singular}" if use_article else f"one {singular}"
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

def classify(s: dict, context: dict | None = None) -> str:
    outs = baseball_ip_to_outs(s["ip"])
    baserunners = baserunner_count(s)

    if s.get("saves"):
        return "SAVE"

    if s.get("blownSaves"):
        return "BLOWN"

    # Extra inning loss: ghost runner scored, 0 ER in box score but pitcher lost the game
    if context and context.get("extra_inning_loss"):
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

    # Check run outcomes first regardless of IP length
    if s["er"] >= 3:
        return "ROUGH"

    if s["er"] in {1, 2}:
        return "SHAKY"

    # From here er == 0
    if outs <= 1:
        return "MICRO"

    if baserunners == 0 and outs >= 6:
        return "DOMINANT"

    if baserunners == 0 and outs >= 3:
        return "CLEAN"

    if baserunners >= 1 and outs >= 3:
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

    if label == "SHAKY_HOLD":
        return "⚠️ Shaky in the ninth"

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


# ---------------- ENTRY CONTEXT ----------------

def get_pitcher_entry_context(feed: dict, pitcher_id: int, pitcher_side: str):
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    if not plays:
        return {
            "entry_phrase": "",
            "entry_outs_text": "",
            "entry_outs": 0,
            "entry_state_text": "",
            "entry_state_kind": "",
            "entry_margin": 0,
            "entry_inning": None,
            "inherited_runners": 0,
            "relieved_pitcher": "",
            "finished_game": False,
            "ghost_runner": False,
            "extra_inning_loss": False,
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
            "entry_outs": 0,
            "entry_state_text": "",
            "entry_state_kind": "",
            "entry_margin": 0,
            "entry_inning": None,
            "inherited_runners": 0,
            "relieved_pitcher": "",
            "finished_game": False,
            "ghost_runner": False,
            "extra_inning_loss": False,
        }

    first_idx = pitcher_indices[0]
    last_idx = pitcher_indices[-1]
    first_play = plays[first_idx]

    about = first_play.get("about", {})
    inning = about.get("inning")
    half = about.get("halfInning", "")

    # Entry outs: use endOuts from the play immediately before this pitcher's first appearance.
    # count.outs on the first play reflects outs at pitch time, not entry time.
    # If this pitcher started the inning (first_idx == 0 or previous play was a different inning),
    # outs at entry = 0.
    entry_outs = 0
    relieved_pitcher = ""
    if first_idx > 0:
        prev_play = plays[first_idx - 1]
        prev_about = prev_play.get("about", {})
        prev_inning = prev_about.get("inning")
        prev_half = prev_about.get("halfInning", "")
        if prev_inning == inning and prev_half == half:
            entry_outs = safe_int(prev_about.get("endOuts", prev_about.get("outs", 0)), 0)
            relieved_pitcher = _fix_name(prev_play.get("matchup", {}).get("pitcher", {}).get("fullName", ""))

    entry_phrase = ""
    if inning is not None and half:
        entry_phrase = f"in the {half.lower()} of the {ordinal(inning)}"

    # Only mention outs if the pitcher entered with runners/outs already on board
    if entry_outs == 0:
        entry_outs_text = ""  # started the inning clean — no need to mention
    elif entry_outs == 1:
        entry_outs_text = "with one out"
    else:
        entry_outs_text = "with two outs"

    # Inherited runners: runners already on base when this pitcher's first play began.
    # A truly inherited runner has originBase == start (they haven't moved yet)
    # and they are not being put out or scoring on this play.
    inherited_runners = []
    for runner_event in first_play.get("runners", []):
        movement = runner_event.get("movement", {})
        details = runner_event.get("details", {})
        origin = movement.get("originBase")
        start = movement.get("start")
        is_out_on_play = movement.get("isOut", False)
        is_scoring = details.get("isScoringEvent", False)
        # Runner was already on base: originBase equals start (hasn't moved),
        # not being retired, not scoring
        if origin and start and origin == start and not is_out_on_play and not is_scoring:
            inherited_runners.append(start)

    inherited_count = len(inherited_runners)

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

    # --- Extra inning ghost runner detection ---
    # In extras (inning >= 10), MLB places an automatic runner on 2nd to start each half.
    # If this pitcher entered a tie game with that ghost runner and his team lost,
    # the run that scored is unearned (0 ER) but he was still the losing pitcher.
    ghost_runner = False
    extra_inning_loss = False

    if inning is not None and inning >= 10:
        # Check if there's a runner on 2nd at entry with 0 outs — signature of the ghost runner
        if entry_outs == 0 and inherited_count >= 1:
            ghost_runner = True

        # Determine if his team lost — compare final score to entry score
        last_play = plays[-1]
        last_result = last_play.get("result", {})
        final_away = safe_int(last_result.get("awayScore", 0), 0)
        final_home = safe_int(last_result.get("homeScore", 0), 0)

        if pitcher_side == "home":
            team_final = final_home
            opp_final = final_away
        else:
            team_final = final_away
            opp_final = final_home

        team_lost = team_final < opp_final

        # Extra inning loss: entered tie game in extras with ghost runner and team lost
        if ghost_runner and state_kind == "tie" and team_lost:
            extra_inning_loss = True

    return {
        "entry_phrase": entry_phrase,
        "entry_outs_text": entry_outs_text,
        "entry_outs": entry_outs,
        "entry_state_text": state_text,
        "entry_state_kind": state_kind,
        "entry_margin": abs_diff,
        "entry_inning": inning,
        "inherited_runners": inherited_count,
        "relieved_pitcher": relieved_pitcher,
        "finished_game": (last_idx == len(plays) - 1),
        "ghost_runner": ghost_runner,
        "extra_inning_loss": extra_inning_loss,
    }



# ---------------- OUTING DETAIL ----------------

# Common last names that appear for multiple active MLB players — first initial not needed
# since we're always last-name only, but keeping a small set of genuinely ambiguous pairs
# as a future hook. For now: always last name only per design decision.

EVENT_TO_HIT_TYPE = {
    "Single": "single",
    "Double": "double",
    "Triple": "triple",
    "Home Run": "homer",
    "Ground Rule Double": "ground-rule double",
}

# Result events that end an at-bat with an out recorded by the pitcher
OUT_EVENTS = {
    "Strikeout", "Groundout", "Flyout", "Pop Out", "Lineout",
    "Forceout", "Double Play", "Triple Play", "Grounded Into DP",
    "Bunt Groundout", "Bunt Pop Out", "Fielders Choice Out",
    "Sac Fly", "Sac Bunt",
}


def _batter_display_name(full_name: str) -> str:
    """Return properly capitalized full name for display in summaries."""
    return _fix_name(full_name)


def _fix_name(name: str) -> str:
    """Title-case a player name as a safety net against API sending lowercase."""
    n = str(name or "").strip()
    if not n:
        return n
    return " ".join(part.capitalize() for part in n.split())


def _batting_order_slot(about: dict) -> int:
    """
    MLB API stores battingOrder as a 3-digit int: 100=1st, 200=2nd, ... 900=9th.
    Returns 1-9, or 0 if not available.
    """
    raw = about.get("battingOrder")
    if raw is None:
        return 0
    try:
        return int(str(raw)[0])
    except Exception:
        return 0


def get_pitcher_outing_detail(feed: dict, pitcher_id: int, ip: str = "0.0", er: int = 0) -> dict:
    """
    Parse play-by-play for a pitcher's outing and return structured detail:
    - strikeouts: list of {name, slot} for batters K'd
    - notable_ks: subset where slot <= 6
    - run_events: list of {batter, hit_type, rbi, slot} for plays that scored runs
    - runners_left_on: total LOB across the outing
    - heart_of_order_retired: list of last names retired in slots 3-5
    - heart_of_order_faced: list of last names faced in slots 3-5
    """
    empty = {
        "strikeouts": [],
        "notable_ks": [],
        "run_events": [],
        "runners_left_on": 0,
        "heart_of_order_retired": [],
        "heart_of_order_faced": [],
        "finished_inning": True,
        "departure_outs": 0,
        "departure_runners": 0,
        "departure_inning": 0,
        "departure_half": "",
        "replaced_by": "",
    }

    if not feed or pitcher_id is None:
        return empty

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    if not plays:
        return empty

    strikeouts = []
    notable_ks = []
    run_events = []
    runners_left_on = 0
    heart_faced = []
    heart_retired = []
    pitcher_play_indices = []

    for idx, play in enumerate(plays):
        matchup = play.get("matchup", {})
        if matchup.get("pitcher", {}).get("id") != pitcher_id:
            continue

        pitcher_play_indices.append(idx)
        about = play.get("about", {})
        result = play.get("result", {})
        batter = matchup.get("batter", {})
        batter_name = batter.get("fullName", "")
        last_name = _batter_display_name(batter_name)
        slot = _batting_order_slot(about)
        event = result.get("event", "")
        rbi = safe_int(result.get("rbi", 0), 0)

        # strikeouts
        if event == "Strikeout":
            entry = {"name": last_name, "slot": slot}
            strikeouts.append(entry)
            if slot >= 1 and slot <= 6:
                notable_ks.append(entry)

        # runs scored on this play — only record if this pitcher was actually
        # charged with runs (cross-reference against box score ER count).
        # RBI can be credited to a batter while this pitcher faces them but the
        # run was charged to a previous pitcher (inherited runner scenario).
        if rbi > 0 and len(run_events) < er:
            hit_type = EVENT_TO_HIT_TYPE.get(event, "hit")
            run_events.append({
                "batter": last_name,
                "hit_type": hit_type,
                "rbi": rbi,
                "slot": slot,
            })

        # runners left on base — count when 3rd out ends the inning
        runners_on = len(play.get("runners", []))
        is_out = result.get("isOut", False)
        end_outs = safe_int(about.get("endOuts", about.get("outs", 0)), 0)
        if is_out and end_outs == 3:
            runners_left_on += runners_on

        # heart of order (slots 3-5)
        if slot >= 3 and slot <= 5:
            heart_faced.append(last_name)
            if event in OUT_EVENTS or event == "Strikeout":
                heart_retired.append(last_name)

    # Determine if pitcher finished his last inning
    # Must check all half-innings he appeared in — a pitcher can finish one inning
    # cleanly then come back and get pulled mid-inning in the next.
    finished_inning = True
    departure_outs = 0
    departure_runners = 0
    replaced_by = ""
    departure_inning = 0
    departure_half = ""

    if pitcher_play_indices:
        last_idx = pitcher_play_indices[-1]
        last_play = plays[last_idx]
        last_about = last_play.get("about", {})

        # Use the maximum endOuts seen across all his plays
        max_end_outs = 0
        for idx in pitcher_play_indices:
            about_i = plays[idx].get("about", {})
            end_outs_i = safe_int(about_i.get("endOuts", about_i.get("outs", 0)), 0)
            if end_outs_i > max_end_outs:
                max_end_outs = end_outs_i

        # Check if he was pulled in any inning he appeared in by scanning forward
        # from each of his plays for a same-half different-pitcher transition
        for check_idx in reversed(pitcher_play_indices):
            check_play = plays[check_idx]
            check_about = check_play.get("about", {})

            if check_idx + 1 < len(plays):
                next_play = plays[check_idx + 1]
                next_matchup = next_play.get("matchup", {})
                next_pitcher_id = next_matchup.get("pitcher", {}).get("id")
                next_about = next_play.get("about", {})
                same_half = (
                    next_about.get("inning") == check_about.get("inning")
                    and next_about.get("halfInning") == check_about.get("halfInning")
                )
                if same_half and next_pitcher_id != pitcher_id:
                    # Pulled mid-inning — record which inning
                    finished_inning = False
                    # Outs recorded in this specific inning at point of pull
                    inning_outs = safe_int(check_about.get("endOuts", check_about.get("outs", 0)), 0)
                    ip_outs = baseball_ip_to_outs(ip) if ip else max_end_outs
                    departure_outs = max(ip_outs, inning_outs)
                    departure_runners = sum(
                        1 for r in check_play.get("runners", [])
                        if not r.get("movement", {}).get("isOut", False)
                        and not r.get("details", {}).get("isScoringEvent", False)
                    )
                    replaced_by = _fix_name(next_matchup.get("pitcher", {}).get("fullName", ""))
                    # Store which inning he was pulled from for the summary
                    departure_inning = safe_int(check_about.get("inning"), 0)
                    departure_half = check_about.get("halfInning", "")
                    break

    return {
        "strikeouts": strikeouts,
        "notable_ks": notable_ks,
        "run_events": run_events,
        "runners_left_on": runners_left_on,
        "heart_of_order_retired": heart_retired,
        "heart_of_order_faced": heart_faced,
        "finished_inning": finished_inning,
        "departure_outs": departure_outs,
        "departure_runners": departure_runners,
        "departure_inning": departure_inning if not finished_inning else 0,
        "departure_half": departure_half if not finished_inning else "",
        "replaced_by": replaced_by,
    }


def _name_list(names: list) -> str:
    """Format a list of last names naturally: 'Tatis', 'Tatis and Machado', 'Tatis, Machado, and Freeman'."""
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return ", ".join(names[:-1]) + f", and {names[-1]}"


def build_line2_from_detail(s: dict, detail: dict, ip_text: str, opp: str = "") -> str:
    """
    Build the outing detail sentence using play-by-play data.
    Falls back to stat-based prose if detail is sparse.
    """
    er = s["er"]
    h = s["h"]
    bb = s["bb"]
    k = s["k"]
    outs_recorded = baseball_ip_to_outs(s["ip"])

    notable_ks = detail.get("notable_ks", [])
    notable_k_names = [e["name"] for e in notable_ks]
    run_events = detail.get("run_events", [])
    lob = detail.get("runners_left_on", 0)
    heart_retired = detail.get("heart_of_order_retired", [])
    heart_faced = detail.get("heart_of_order_faced", [])

    pieces = []

    # --- runs allowed ---
    if run_events:
        run_parts = []
        for ev in run_events:
            batter = ev["batter"]
            hit_type = ev["hit_type"]
            rbi = ev["rbi"]
            if rbi == 1:
                run_parts.append(random.choice([
                    f"an RBI {hit_type} by {batter}",
                    f"a {hit_type} from {batter} that plated a run",
                ]))
            else:
                run_parts.append(random.choice([
                    f"a {rbi}-run {hit_type} by {batter}",
                    f"{batter}'s {hit_type} that scored {number_word(rbi)}",
                ]))

        if len(run_parts) == 1:
            part = run_parts[0]
            part_cap = part[0].upper() + part[1:] if part else part
            pieces.append(random.choice([
                f"{part_cap} did the damage.",
                f"The damage came on {part}.",
                f"He gave up {part} to allow the run{'s' if er > 1 else ''}.",
            ]))
        else:
            joined = _name_list(run_parts)
            joined_cap = joined[0].upper() + joined[1:] if joined else joined
            pieces.append(random.choice([
                f"{joined_cap} did the damage.",
                f"The damage came on {joined}.",
            ]))
    elif s["er"] > 0:
        # Runs scored but no RBI play detected — score via walks, wild pitches, passed balls, etc.
        # Use the box score stats to describe what happened as specifically as possible.
        run_text = stat_phrase(s["er"], "run")
        if bb > 0 and h == 0:
            walk_text = stat_phrase(bb, "walk")
            pieces.append(random.choice([
                f"He allowed {run_text} to score after issuing {walk_text} and loading the bases.",
                f"The {run_text} scored after he walked {number_word(bb)} — no hits required.",
                f"He put {number_word(bb)} on via walks and the damage came without a hit.",
            ]))
        elif bb > 0 and h > 0:
            hit_text = stat_phrase(h, "hit", use_article=True)
            walk_text = stat_phrase(bb, "walk", use_article=True)
            pieces.append(random.choice([
                f"He allowed {run_text} to score on {hit_text} and {walk_text}.",
                f"A combination of {hit_text} and {walk_text} led to {run_text} scoring.",
            ]))
        else:
            pieces.append(random.choice([
                f"{run_text.capitalize()} scored during his appearance.",
                f"He allowed {run_text} to cross the plate.",
            ]))
    elif er == 0 and h == 0 and bb == 0:
        # perfect outing
        if outs_recorded == 1:
            pieces.append("He retired the lone batter he faced.")
        else:
            if opp:
                pieces.append(random.choice([
                    f"He retired the {opp} in order over {ip_text}.",
                    f"He was spotless over {ip_text}, keeping the {opp} off the bases entirely.",
                    f"He set the {opp} down in order.",
                ]))
            else:
                pieces.append(random.choice([
                    f"He retired all hitters he faced over {ip_text}.",
                    f"He was spotless over {ip_text}, getting through the inning without allowing a baserunner.",
                ]))
    elif er == 0:
        # runners on but none scored
        if lob > 0:
            lob_text = "a runner" if lob == 1 else f"{number_word(lob)} runners"
            if opp:
                pieces.append(random.choice([
                    f"He stranded {lob_text} and kept the {opp} off the board.",
                    f"He left {lob_text} on base but held the {opp} scoreless.",
                    f"There were baserunners, but he kept the {opp} from scoring.",
                ]))
            else:
                pieces.append(random.choice([
                    f"He stranded {lob_text} and kept the inning scoreless.",
                    f"He left {lob_text} on base but held the line.",
                    f"There were baserunners, but he kept {lob_text} stranded.",
                ]))
        else:
            hit_text = stat_phrase(h, "hit", use_article=True)
            walk_text = stat_phrase(bb, "walk", use_article=True)
            if opp:
                pieces.append(random.choice([
                    f"He worked around {hit_text} and {walk_text} to keep the {opp} off the board.",
                    f"He allowed {hit_text} and {walk_text} but kept the {opp} scoreless.",
                ]))
            else:
                pieces.append(random.choice([
                    f"He worked around {hit_text} and {walk_text} to keep the inning scoreless.",
                    f"He allowed {hit_text} and {walk_text} but held the damage at zero.",
                ]))

    # --- strikeouts ---
    if notable_k_names:
        k_list = _name_list(notable_k_names)
        if len(notable_k_names) == k:
            # all strikeouts were notable batters
            pieces.append(random.choice([
                f"He struck out {k_list}.",
                f"His strikeouts came against {k_list}.",
                f"He punched out {k_list}.",
            ]))
        else:
            # mix of notable and non-notable
            others = k - len(notable_k_names)
            other_text = f"{number_word(others)} {'other' if others == 1 else 'others'}"
            pieces.append(random.choice([
                f"He struck out {k_list} among his {number_word(k)} punchouts.",
                f"His strikeouts included {k_list}.",
                f"He got {k_list}, plus {other_text}, on strikeouts.",
            ]))
    elif k > 0:
        # strikeouts but no notable batters — just the count, no names
        pieces.append(random.choice([
            f"He struck out {number_word(k)}.",
            f"He collected {stat_phrase(k, 'strikeout')} in the outing.",
        ]))

    # --- heart of order ---
    if heart_faced:
        if heart_retired and len(heart_retired) == len(heart_faced):
            # retired all heart-of-order batters he faced
            heart_list = _name_list(heart_retired)
            pieces.append(random.choice([
                f"He retired {heart_list} cleanly.",
                f"He got through {heart_list} without issue.",
                f"{heart_list} went down without doing any damage.",
                f"He handled {heart_list} in the heart of their order.",
            ]))
        elif heart_retired:
            # retired some but not all
            heart_list = _name_list(heart_retired)
            pieces.append(random.choice([
                f"He got {heart_list} out of the middle of their order.",
                f"{heart_list} couldn't do anything with him.",
            ]))

    # --- LOB when runs were scored (context for rough outings) ---
    if run_events and lob > 0:
        lob_text = "a runner" if lob == 1 else f"{number_word(lob)} runners"
        pieces.append(random.choice([
            f"He also left {lob_text} stranded.",
            f"He stranded {lob_text} in addition to the runs that crossed.",
        ]))

    # --- pulled mid-inning ---
    finished_inning = detail.get("finished_inning", True)
    departure_outs = safe_int(detail.get("departure_outs", 0), 0)
    departure_runners = safe_int(detail.get("departure_runners", 0), 0)
    replaced_by = str(detail.get("replaced_by", "") or "").strip()

    if not finished_inning:
        outs_text = {0: "no outs", 1: "one out", 2: "two outs"}.get(departure_outs, f"{departure_outs} outs")
        replacer = f" with {replaced_by} coming on to finish the inning" if replaced_by else ""
        if departure_runners > 0:
            runner_text = "a runner on" if departure_runners == 1 else f"{number_word(departure_runners)} runners on"
            pieces.append(random.choice([
                f"He was pulled with {outs_text} recorded and {runner_text}{replacer}.",
                f"He did not finish the inning, exiting with {outs_text} and {runner_text}{replacer}.",
                f"The manager pulled him with {outs_text} and {runner_text}, bringing in {replaced_by} to clean up." if replaced_by else f"The manager went to the bullpen with {outs_text} and {runner_text} still on base.",
            ]))
        else:
            pieces.append(random.choice([
                f"He was pulled after recording {outs_text}{replacer}.",
                f"He did not finish the inning, exiting after {outs_text}{replacer}.",
                f"The manager went to the bullpen after {outs_text} from him{', turning to ' + replaced_by if replaced_by else ''}.",
            ]))

    if not pieces:
        # absolute fallback to stat-based prose
        hit_text = stat_phrase(h, "hit")
        walk_text = stat_phrase(bb, "walk")
        run_text = stat_phrase(er, "run")
        if er == 0:
            line = f"He worked {ip_text}, allowing {hit_text} and {walk_text}"
        else:
            line = f"He allowed {run_text} over {ip_text} on {hit_text} and {walk_text}"
        k_part = strikeout_phrase(k)
        return line + (f" {k_part}." if k_part else ".")

    return " ".join(pieces)


def build_context_phrase(context: dict) -> str:
    bits = []
    if context.get("entry_phrase"):
        bits.append(context["entry_phrase"])
    if context.get("entry_outs_text"):
        bits.append(context["entry_outs_text"])
    if context.get("entry_state_text"):
        bits.append(context["entry_state_text"])

    # Inherited runners — append naturally if present and outs were already on
    inherited = safe_int(context.get("inherited_runners", 0), 0)
    entry_outs = safe_int(context.get("entry_outs", 0), 0)
    if inherited > 0 and entry_outs > 0:
        runner_text = "a runner on" if inherited == 1 else f"{number_word(inherited)} runners on"
        bits.append(f"and {runner_text}")

    if not bits:
        return "in relief"

    if len(bits) == 1:
        return bits[0]

    if len(bits) == 2:
        return f"{bits[0]} {bits[1]}"

    return f"{bits[0]} {bits[1]}, {bits[2]}"


# ---------------- RECENT APPEARANCES / TRENDS ----------------

async def get_pitching_stats_for_date(target_date):
    if target_date in pitching_stats_cache:
        return pitching_stats_cache[target_date]

    stats_by_pitcher = {}

    try:
        loop = asyncio.get_event_loop()
        day_games = await loop.run_in_executor(None, _fetch_schedule_sync, target_date.isoformat())

        for game in day_games:
            game_id = game.get("gamePk")
            if not game_id:
                continue

            try:
                feed = await get_feed(game_id)
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

async def get_pitcher_ids_for_date(target_date):
    if target_date in appearance_cache:
        return appearance_cache[target_date]

    pitcher_ids = set()

    try:
        stats_by_pitcher = await get_pitching_stats_for_date(target_date)
        pitcher_ids = set(stats_by_pitcher.keys())
    except Exception as e:
        log(f"Appearance cache load failed for {target_date}: {e}")

    appearance_cache[target_date] = pitcher_ids
    return pitcher_ids


async def get_recent_appearances(pitcher_id: int, game_date_et, limit=5, max_days=21):
    appearances = []
    if pitcher_id is None or game_date_et is None:
        return appearances

    check_date = game_date_et - timedelta(days=1)

    for _ in range(max_days):
        stats_by_pitcher = await get_pitching_stats_for_date(check_date)
        if pitcher_id in stats_by_pitcher:
            appearances.append(stats_by_pitcher[pitcher_id])
            if len(appearances) >= limit:
                break
        check_date -= timedelta(days=1)

    return appearances


async def get_streak_count(pitcher_id: int, game_date_et):
    if pitcher_id is None or game_date_et is None:
        return 0

    yesterday = game_date_et - timedelta(days=1)
    two_days_ago = game_date_et - timedelta(days=2)

    yesterday_ids = await get_pitcher_ids_for_date(yesterday)
    two_days_ids = await get_pitcher_ids_for_date(two_days_ago)

    if pitcher_id in yesterday_ids and pitcher_id in two_days_ids:
        return 3

    if pitcher_id in yesterday_ids:
        return 2

    return 0


async def count_recent_appearances_in_window(pitcher_id: int, game_date_et, days: int = 15) -> int:
    if pitcher_id is None or game_date_et is None:
        return 0

    count = 1
    check_date = game_date_et - timedelta(days=1)
    for _ in range(max(days - 1, 0)):
        if pitcher_id in await get_pitcher_ids_for_date(check_date):
            count += 1
        check_date -= timedelta(days=1)
    return count


async def get_recent_usage_snapshot(pitcher_id: int, game_date_et):
    if pitcher_id is None or game_date_et is None:
        return {"pitched_yesterday": False, "pitched_two_days_ago": False, "apps_last4": 1, "apps_last6": 1}

    yesterday = game_date_et - timedelta(days=1)
    two_days_ago = game_date_et - timedelta(days=2)

    pitched_yesterday = pitcher_id in await get_pitcher_ids_for_date(yesterday)
    pitched_two_days_ago = pitcher_id in await get_pitcher_ids_for_date(two_days_ago)

    apps_last4 = 1
    check_date = yesterday
    for _ in range(3):
        if pitcher_id in await get_pitcher_ids_for_date(check_date):
            apps_last4 += 1
        check_date -= timedelta(days=1)

    apps_last6 = 1
    check_date = yesterday
    for _ in range(5):
        if pitcher_id in await get_pitcher_ids_for_date(check_date):
            apps_last6 += 1
        check_date -= timedelta(days=1)

    return {
        "pitched_yesterday": pitched_yesterday,
        "pitched_two_days_ago": pitched_two_days_ago,
        "apps_last4": apps_last4,
        "apps_last6": apps_last6,
    }


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
    if code in {"ks12last5", "dominant_run", "runs3of5", "multi_inning_pattern"}:
        return 5
    if code in {"dominant_streak"}:
        return 4
    # workload_spike and inherited_runner_specialist use 6-day window but default 3 is fine for IP check
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

    if code in {"ks12last5", "ks8last3"}:
        return random.choice([
            f"He has paired the swing-and-miss with {bb_text} over {ip_text} in that span.",
            f"Over that stretch, he has worked {ip_text} with {bb_text}.",
            f"That run has also come with {bb_text} over {ip_text}.",
        ])

    if code in {"dominant_streak", "dominant_run"}:
        return random.choice([
            f"During that {window}-appearance stretch, he has {k_text} and {bb_text}.",
            f"He has covered {ip_text} during that run, with {k_text} and {bb_text}.",
            f"The stretch has come with {k_text} over {ip_text} and {bb_text}.",
        ])

    if code in {"runs2of3", "runs3of5", "blown_save_watch", "first_rough_after_hot"}:
        return random.choice([
            f"Across that stretch, he has still managed {k_text}, but the run prevention has slipped.",
            f"The recent stretch covers {ip_text}, with {k_text} and {bb_text}, but too many runs have crossed.",
        ])

    if code in {"command_spike", "multi_inning_pattern"}:
        return random.choice([
            f"Over that stretch, he has logged {ip_text} with {k_text} and {bb_text}.",
            f"That span has come with {k_text} and {bb_text} over {ip_text}.",
        ])

    return random.choice([
        f"Over that stretch, he has logged {ip_text} with {k_text} and {bb_text}.",
        f"That span has come with {k_text} and {bb_text} over {ip_text}.",
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
        # Even brief outings can be damaging — check label/er first
        if label == "SHAKY_HOLD":
            return random.choice([
                "He was in a save situation and could not get through even one out cleanly.",
                "One out in the ninth while allowing a run in a save spot is not what you want from this role.",
                "The lead survived, but he made a single out feel like a lot of work.",
            ])
        if label == "BLOWN":
            return random.choice([
                "He could not record an out without letting the lead go.",
                "A brief, damaging appearance that cost the team the lead.",
                "He did not get through even one hitter without giving up the lead.",
            ])
        if s["er"] > 0:
            return random.choice([
                "He was only in for a batter, but the damage was real.",
                "Brief and costly — he allowed a run without even finishing the inning.",
                "He got through one out but not without giving something up.",
            ])
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
        if label == "SHAKY_HOLD":
            return random.choice([
                "He technically kept the lead, but this was a save situation and he let it get away from him.",
                "He was protecting a lead in the ninth and let runs score. That is not the outcome you want.",
                "He escaped without officially blowing the save, but the lead shrank and the damage was real.",
                "The lead survived, but he made a mess of a situation that called for a shutdown inning.",
                "He made it harder than it needed to be in a spot that should have been closed out cleanly.",
            ])
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
        if label == "SAVE" and safe_int(context.get("inherited_runners", 0), 0) > 0:
            inherited = safe_int(context.get("inherited_runners", 0), 0)
            runner_text = "an inherited runner" if inherited == 1 else f"{number_word(inherited)} inherited runners"
            prev_pitcher = str(context.get("relieved_pitcher", "") or "").strip()
            relieved_str = f" relieving {prev_pitcher}" if prev_pitcher else ""
            if outing_grade in {"DOMINANT", "CLEAN"}:
                return random.choice([
                    f"Coming in{relieved_str} to handle {runner_text} and still closing it out is exactly what you want from this role.",
                    f"He walked into a tough spot{relieved_str} with {runner_text} on base and got through it cleanly. That is a meaningful save.",
                    f"Inheriting {runner_text}{relieved_str} and still getting the save says something. He handled the situation.",
                ])
            else:
                return random.choice([
                    f"He came in{relieved_str} with {runner_text} already on base and still got the job done.",
                    f"It was not spotless, but he inherited a problem situation{relieved_str} and came away with the save.",
                    f"Getting the save after inheriting {runner_text}{relieved_str} is a win even if the line was not clean.",
                ])
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
            if safe_int(context.get("inherited_runners", 0), 0) > 0:
                inherited = safe_int(context.get("inherited_runners", 0), 0)
                runner_text = "an inherited runner" if inherited == 1 else f"{number_word(inherited)} inherited runners"
                prev_pitcher = str(context.get("relieved_pitcher", "") or "").strip()
                relieved_str = f" relieving {prev_pitcher}" if prev_pitcher else ""
                return random.choice([
                    f"He came in{relieved_str} with {runner_text} already on base and still closed it out. That is a meaningful save.",
                    f"Inheriting {runner_text}{relieved_str} and earning the save anyway says something about how he handled the situation.",
                    f"He walked into a live mess{relieved_str} — {runner_text} on base — and got the job done.",
                ])
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
        if label == "SAVE":
            if safe_int(context.get("inherited_runners", 0), 0) > 0:
                inherited = safe_int(context.get("inherited_runners", 0), 0)
                runner_text = "an inherited runner" if inherited == 1 else f"{number_word(inherited)} inherited runners"
                prev_pitcher = str(context.get("relieved_pitcher", "") or "").strip()
                relieved_str = f" relieving {prev_pitcher}" if prev_pitcher else ""
                return random.choice([
                    f"He came in{relieved_str} with {runner_text} on base and still locked down the save. That is not easy to do.",
                    f"Getting the save after inheriting {runner_text}{relieved_str} is a meaningful result. He answered when it counted.",
                    f"He was handed a problem{relieved_str} and solved it — {runner_text} on base and he still closed the door.",
                ])
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



def build_summary(name: str, team: str, s: dict, label: str, context: dict, streak_count: int, tracked_info: dict, recent_appearances, usage_note: str = "", velocity_alert: dict = None, detail: dict = None, opp_name: str = "", pitcher_score: int = 0, opp_score: int = 0):
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

    inherited = safe_int(context.get("inherited_runners", 0), 0)
    relieved_pitcher = str(context.get("relieved_pitcher", "") or "").strip()

    # Opponent name phrase
    opp = str(opp_name or "").strip()
    opp_phrase = f"against the {opp}" if opp else ""

    # Combined score + opponent tail phrase for close games (margin <= 2)
    # Only fires for lead/trailing — tie games are excluded since the final score
    # won't match the tied state at entry and produces nonsensical phrases.
    # Also excluded for BLOWN (pitcher lost the lead, "win" would be contradictory)
    # and generic RELIEF (not meaningful enough to add score context).
    margin = safe_int(context.get("entry_margin", 0), 0)
    state_kind = context.get("entry_state_kind", "")
    score_tail = ""
    exclude_labels = {"BLOWN", "SHAKY_HOLD"}
    if (
        (pitcher_score > 0 or opp_score > 0)
        and margin <= 2
        and state_kind == "lead"
        and label not in exclude_labels
    ):
        win_score = max(pitcher_score, opp_score)
        lose_score = min(pitcher_score, opp_score)
        score_num = f"{win_score}-{lose_score}"

        # For SAVE, opponent already in line1 — drop opp from score tail
        opp_in_score_tail = opp and label not in {"SAVE"}

        if state_kind == "lead":
            if opp_in_score_tail:
                score_tail = random.choice([
                    f"in a {score_num} win over the {opp}",
                    f"as his team held on {score_num} against the {opp}",
                    f"to help seal a {score_num} win over the {opp}",
                ])
            else:
                score_tail = random.choice([
                    f"in a {score_num} win",
                    f"as his team held on {score_num}",
                ])

    # opp_in_line1: True for labels where opponent belongs in line1
    # False for HOLD/DOM/CLEAN/TRAFFIC/RELIEF — opponent woven in later
    opp_in_line1 = label in {"SAVE", "BLOWN", "SHAKY_HOLD", "ROUGH"}

    if label == "SAVE":
        if inherited > 0:
            # Skip opponent in line1 for inherited runner saves — sentence is already complex
            runner_text = "a runner" if inherited == 1 else f"{number_word(inherited)} runners"
            relieved_str = f" relieving {relieved_pitcher}" if relieved_pitcher else ""
            line1 = random.choice([
                f"{name} entered {ctx}{relieved_str} to inherit {runner_text} and still closed it out for the save.",
                f"{name} came in{relieved_str} with {runner_text} on base and shut the door for the save.",
                f"{name} was handed a mess{relieved_str} — {runner_text} on base — and still earned the save.",
            ])
        elif early_closer_usage and finished_game:
            line1 = f"{name} was called on {ctx} before the ninth in a high-leverage spot and finished the game for the save."
        elif outs_recorded >= 6:
            opp_str = f" {opp_phrase}" if opp_phrase else ""
            line1 = f"{name} entered {ctx}{opp_str} and covered the final {ip_text} to earn the save."
        elif finished_game and context.get("entry_inning") == 9:
            line1 = random.choice([
                f"{name} entered {ctx} and shut the door {opp_phrase} for the save." if opp_phrase else f"{name} entered {ctx} and shut the door for the save.",
                f"{name} entered {ctx} and locked down the save {opp_phrase}." if opp_phrase else f"{name} entered {ctx} and locked down the save.",
                f"{name} slammed the door {opp_phrase} to earn the save." if opp_phrase else f"{name} entered {ctx} and slammed the door for the save.",
            ])
        else:
            line1 = random.choice([
                f"{name} entered {ctx} and locked down the save {opp_phrase}." if opp_phrase else f"{name} entered {ctx} and locked down the save.",
                f"{name} came on {ctx} and closed it out {opp_phrase}." if opp_phrase else f"{name} came on {ctx} and closed it out.",
                f"{name} entered {ctx} and got the job done for the save {opp_phrase}." if opp_phrase else f"{name} entered {ctx} and got the job done for the save.",
            ])
    elif label == "BLOWN":
        # Always in line1
        opp_str = f" {opp_phrase}" if opp_phrase else ""
        line1 = random.choice([
            f"{name} entered {ctx}{opp_str} but could not hold the lead and was charged with a blown save.",
            f"{name} entered {ctx}{opp_str} and could not keep things together, blowing the save.",
            f"{name} came on {ctx}{opp_str} but the lead slipped away for the blown save.",
        ])
    elif label == "SHAKY_HOLD":
        opp_str = f" {opp_phrase}" if opp_phrase else ""
        line1 = random.choice([
            f"{name} entered {ctx}{opp_str} in a save situation but quickly got into a jam.",
            f"{name} entered {ctx}{opp_str} in a save situation but could not keep the inning clean.",
            f"{name} entered {ctx}{opp_str} in a save situation and struggled to hold things together.",
        ])
    elif label == "HOLD":
        # Opponent deferred to line2/analysis — clean line1
        line1 = random.choice([
            f"{name} entered {ctx} and held the line to earn the hold.",
            f"{name} came on {ctx} and kept things intact for the hold.",
            f"{name} entered {ctx} and bridged the gap with a clean hold.",
        ])
    elif label == "DOM":
        # Opponent deferred
        line1 = random.choice([
            f"{name} entered {ctx} and dominated.",
            f"{name} came on {ctx} and was lights out.",
            f"{name} entered {ctx} and was untouchable.",
        ])
    elif label == "TRAFFIC":
        line1 = f"{name} entered {ctx} and navigated traffic to keep things under control."
    elif label == "ROUGH":
        # In line1
        opp_str = f" {opp_phrase}" if opp_phrase else ""
        line1 = random.choice([
            f"{name} entered {ctx}{opp_str} but was hit hard in a rough outing.",
            f"{name} entered {ctx}{opp_str} but could not get through the inning cleanly.",
            f"{name} entered {ctx}{opp_str} and the {opp} tagged him for a rough inning." if opp else f"{name} entered {ctx} and was tagged for a rough inning.",
        ])
    elif label == "CLEAN":
        # Opponent deferred
        line1 = random.choice([
            f"{name} entered {ctx} and turned in a clean outing.",
            f"{name} came on {ctx} and handled the inning without issue.",
        ])
    else:
        line1 = f"{name} entered {ctx} in relief."

    # Append close-game score+opponent tail to line1 when relevant
    if score_tail:
        line1 = line1.rstrip(".") + f", {score_tail}."

    # line2 — use play-by-play detail when available, fall back to stat prose
    if detail:
        # Pass opponent name for labels where it belongs in line2
        line2_opp = opp if label in {"HOLD", "DOM", "CLEAN", "TRAFFIC"} else ""
        line2 = build_line2_from_detail(s, detail, ip_text, opp=line2_opp)
    else:
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
    apps = recent_appearances[:6]
    if not apps:
        return {}
    last3 = apps[:3]
    last4 = apps[:4]
    last5 = apps[:5]
    last6 = apps[:6]

    # IP floors
    last3_outs = sum(baseball_ip_to_outs(a.get("ip", "0.0")) for a in last3)
    last4_outs = sum(baseball_ip_to_outs(a.get("ip", "0.0")) for a in last4)
    last5_outs = sum(baseball_ip_to_outs(a.get("ip", "0.0")) for a in last5)
    last3_meets_ip_floor = last3_outs >= 9   # 3 innings
    last4_meets_ip_floor = last4_outs >= 12  # 4 innings
    last5_meets_ip_floor = last5_outs >= 15  # 5 innings

    # ER / BB totals
    er_last3 = sum(safe_int(a.get("er", 0), 0) for a in last3)
    er_last5 = sum(safe_int(a.get("er", 0), 0) for a in last5)
    bb_last5 = sum(safe_int(a.get("bb", 0), 0) for a in last5)

    # K totals
    ks_last3 = sum(safe_int(a.get("k", 0), 0) for a in last3)
    ks_last5 = sum(safe_int(a.get("k", 0), 0) for a in last5)

    # Blown saves in last 10 appearances (use last6 as proxy)
    blown_last6 = sum(safe_int(a.get("blownSaves", 0), 0) for a in last6)

    return {
        "last3": last3,
        "last4": last4,
        "last5": last5,
        "last6": last6,
        "last3_meets_ip_floor": last3_meets_ip_floor,
        "last4_meets_ip_floor": last4_meets_ip_floor,
        "last5_meets_ip_floor": last5_meets_ip_floor,
        # --- strikeout trends (raised floors, at most 1 ER across window) ---
        "ks_last3": ks_last3,
        "ks_last5": ks_last5,
        "ks8last3_clean": last3_meets_ip_floor and ks_last3 >= 8 and er_last3 <= 1,
        "ks12last5_clean": last5_meets_ip_floor and ks_last5 >= 12 and er_last5 <= 1,
        # --- dominance: separate streak vs run ---
        "dominant_streak": last4_meets_ip_floor and len(last4) == 4 and all(
            grade_outing(a) in {"DOMINANT", "CLEAN"} for a in last4
        ),
        "dominant_run": last5_meets_ip_floor and len(last5) == 5 and sum(
            1 for a in last5 if grade_outing(a) in {"DOMINANT", "CLEAN"}
        ) >= 4,
        # --- rough trends ---
        "runs2of3": last3_meets_ip_floor and len(last3) == 3 and sum(
            1 for a in last3 if a.get("er", 0) > 0
        ) >= 2,
        "runs3of5": last5_meets_ip_floor and len(last5) == 5 and sum(
            1 for a in last5 if a.get("er", 0) > 0
        ) >= 3,
        # --- blown save watch: 2+ blown saves in last 6 appearances ---
        "blown_save_watch": blown_last6 >= 2,
        # --- command spike: 0 BB in last 3 with IP floor (was issuing walks before) ---
        "command_spike": last3_meets_ip_floor and len(last3) == 3 and all(
            safe_int(a.get("bb", 0), 0) == 0 for a in last3
        ) and bb_last5 >= 2,  # had walks earlier in the window = genuine improvement
        # --- multi-inning pattern: avg 4+ outs/outing over last 5 ---
        "multi_inning_pattern": last5_meets_ip_floor and len(last5) == 5 and (
            last5_outs / 5
        ) >= 4.0,
        # --- role codes (saves/holds exempt from IP floor) ---
        "saves2in7": sum(
            1 for a in last5 if safe_int(a.get("saves", 0), 0) > 0
        ) >= 2,
        "save_plus_hold": (
            sum(1 for a in last5 if safe_int(a.get("saves", 0), 0) > 0) >= 1 and
            sum(1 for a in last5 if safe_int(a.get("holds", 0), 0) > 0) >= 1
        ),
        "holds3": last3_meets_ip_floor and len(last3) == 3 and all(
            safe_int(a.get("holds", 0), 0) > 0 for a in last3
        ),
    }


def build_trend_candidates(current_app: dict, recent_appearances, tracked_info, context: dict, usage_snapshot: dict | None = None):
    if tracked_info:
        return []
    if not recent_appearances:
        return []
    info = recent_window_summary(recent_appearances)
    if not info:
        return []
    candidates = []
    current_save = safe_int(current_app.get("saves", 0), 0) > 0
    current_hold = safe_int(current_app.get("holds", 0), 0) > 0
    prev = recent_appearances[1] if len(recent_appearances) > 1 else None
    prev2 = recent_appearances[2] if len(recent_appearances) > 2 else None
    last3_meets_ip_floor = info.get("last3_meets_ip_floor", False)
    last4_meets_ip_floor = info.get("last4_meets_ip_floor", False)
    last5_meets_ip_floor = info.get("last5_meets_ip_floor", False)

    def add(code, subject, emoji, priority, family, detail=None):
        candidates.append({"code": code, "subject": subject, "emoji": emoji, "priority": priority, "family": family, "detail": detail or {}})

    # --- strikeout trends (raised floors: 12 over 5, 8 over 3; at most 1 ER) ---
    if info.get("ks12last5_clean"):
        add("ks12last5", "Bat-Missing Run", "⚡", 96, "strikeout", {"ks": info.get("ks_last5", 0), "window": 5})
    elif info.get("ks8last3_clean"):
        add("ks8last3", "Strikeout Surge", "⚡", 86, "strikeout", {"ks": info.get("ks_last3", 0), "window": 3})

    # --- dominance: separate streak (4 straight) vs run (4 of 5) ---
    if info.get("dominant_streak"):
        add("dominant_streak", "Dominant Streak", "🔥", 92, "dominance")
    elif info.get("dominant_run"):
        add("dominant_run", "Dominant Run", "🔥", 80, "dominance")

    # --- blown save watch (highest-priority rough signal — wire drop alert) ---
    if info.get("blown_save_watch"):
        add("blown_save_watch", "Blown Save Watch", "🚨", 95, "rough")

    # --- rough trends ---
    if not info.get("blown_save_watch"):
        if info.get("runs3of5"):
            add("runs3of5", "Rough Stretch", "⚠️", 69, "rough")
        elif info.get("runs2of3"):
            add("runs2of3", "Recent Form Trending Down", "⚠️", 64, "rough")
        if prev and prev.get("er", 0) == 0 and prev2 and prev2.get("er", 0) == 0 and current_app.get("er", 0) > 0:
            add("first_rough_after_hot", "First Rough Turn After Hot Stretch", "⚠️", 61, "rough")

    # --- bounce-back: requires 2 clean outings after a rough stretch ---
    if last3_meets_ip_floor and prev and prev2:
        current_clean = current_app.get("er", 0) == 0
        prev_clean = prev.get("er", 0) == 0
        prev2_rough = prev2.get("er", 0) > 0 or safe_int(prev2.get("blownSaves", 0), 0) > 0
        if current_clean and prev_clean and prev2_rough:
            add("bounce_back", "Bounce-Back Stretch", "🔁", 76, "bounce")

    # --- command spike: 0 BB in last 3 after issuing walks earlier ---
    if info.get("command_spike"):
        add("command_spike", "Command Clicking", "🧠", 72, "command")

    # --- multi-inning pattern ---
    if info.get("multi_inning_pattern"):
        add("multi_inning_pattern", "Multi-Inning Workhorse", "📈", 70, "usage")

    # --- workload spike: 4+ appearances in last 6 days (fatigue flag) ---
    apps_last6 = (usage_snapshot or {}).get("apps_last6", 0)
    if apps_last6 >= 4:
        add("workload_spike", "Heavy Workload Alert", "😤", 67, "workload", {"apps_last6": apps_last6})

    # --- inherited runner specialist: track via context inherited_stranded_recent ---
    # NOTE: inherited_stranded_recent requires historical aggregation not yet implemented.
    # context will always return 0 here — this trend cannot fire until that data is built.
    inherited_stranded = context.get("inherited_stranded_recent", 0)
    if inherited_stranded >= 3:
        add("inherited_runner_specialist", "Inherited Runner Stopper", "🛑", 74, "specialist", {"inherited_stranded": inherited_stranded})

    # --- usage / leverage: 2+ late-inning clean appearances in last 7 days ---
    # NOTE: late_inning_clean_recent requires historical aggregation not yet implemented.
    # context will always return 0 here — this trend cannot fire until that data is built.
    late_clean_recent = context.get("late_inning_clean_recent", 0)
    if late_clean_recent >= 2:
        entry = safe_int(context.get("entry_inning"), 0)
        if entry >= 8 and current_app.get("er", 0) == 0:
            add("higher_leverage_usage", "Trusted in the Big Spots", "📈", 78, "usage")
        elif entry >= 7 and current_app.get("er", 0) == 0:
            add("late_inning_clean", "Late-Inning Staple", "📈", 65, "usage")

    # --- role: save_conversion requires 2 saves in 7 days OR save + hold ---
    if info.get("saves2in7"):
        add("saves2in7", "Emerging Closer", "📈", 90, "role")
    elif info.get("save_plus_hold"):
        add("save_plus_hold", "Emerging Late-Inning Option", "📈", 82, "role")
    if current_hold and info.get("holds3"):
        add("holds3", "Three Straight Holds", "📈", 75, "role")

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
    apps_last6 = trend.get("detail", {}).get("apps_last6", 4)  # fallback to 4 for workload copy
    inherited_stranded = trend.get("detail", {}).get("inherited_stranded", 3)  # fallback for copy

    templates = {
        # --- strikeout ---
        "ks12last5": [
            f"{name} has piled up {number_word(info.get('ks_last5', 0))} strikeouts over his last five appearances and the bat-missing has become impossible to ignore.",
            f"Over his last five outings, {name} has racked up {number_word(info.get('ks_last5', 0))} strikeouts — a swing-and-miss rate that puts him among the most electric arms in this bullpen.",
        ],
        "ks8last3": [
            f"{name} has punched out {number_word(info.get('ks_last3', 0))} hitters across his last three outings, and the strikeout stuff has been genuinely sharp.",
            f"Strikeouts are stacking up fast for {name}, who has registered {number_word(info.get('ks_last3', 0))} over his last three appearances.",
        ],
        # --- dominance ---
        "dominant_streak": [
            f"{name} has now turned in four straight dominant performances — the kind of run that changes how managers deploy a reliever.",
            f"Four consecutive dominant outings have {name} on one of the stronger recent stretches in this bullpen.",
        ],
        "dominant_run": [
            f"{name} has been dominant in four of his last five outings, and the consistency is hard to overlook at this point.",
            f"Four dominant appearances over the last five turns have {name} firmly on the radar.",
        ],
        # --- rough / blown save watch ---
        "blown_save_watch": [
            f"{name} has now blown multiple save opportunities recently — that is the kind of trend that should prompt a closer look at the depth chart.",
            f"Multiple blown saves in a short window have put {name}'s closer role in question. Fantasy managers should be paying attention.",
        ],
        "runs3of5": [
            f"{name} has allowed runs in three of his last five outings, and the recent form has gone in the wrong direction.",
        ],
        "runs2of3": [
            f"{name} has been tagged in two of his last three appearances, and the recent results are starting to raise some flags.",
        ],
        "first_rough_after_hot": [
            f"{name} hit his first real bump after a stronger recent stretch.",
        ],
        # --- bounce-back (now requires 2 clean after rough) ---
        "bounce_back": [
            f"After running into trouble, {name} has now strung together two clean outings and the concerning stretch appears to be behind him.",
            f"{name} has answered a rough patch with back-to-back clean appearances — a real bounce-back run.",
        ],
        # --- command spike ---
        "command_spike": [
            f"{name} has gone his last three outings without issuing a walk after showing earlier control issues. The command has clicked in.",
            f"After struggling with his command earlier, {name} has been completely walk-free over his last three turns — a notable shift.",
        ],
        # --- multi-inning pattern ---
        "multi_inning_pattern": [
            f"{name} has averaged more than a full inning per outing over his last five appearances, giving this bullpen genuine length from a reliable arm.",
            f"The recent workload for {name} has crept beyond a typical one-inning role — he has been getting multiple outs consistently and handling it well.",
        ],
        # --- workload spike ---
        "workload_spike": [
            f"{name} has appeared in {apps_last6} of the last six days, and the workload is starting to stack up. Worth monitoring before streaming.",
            f"Heavy recent usage alert — {name} has been called on frequently and fatigue could become a factor if the pattern continues.",
        ],
        # --- inherited runner specialist ---
        "inherited_runner_specialist": [
            f"{name} has stranded multiple inherited runners in recent appearances, quietly becoming one of the more reliable fire-extinguisher options in this pen.",
            f"Managers have been trusting {name} in tight inherited-runner spots and he has delivered, stranding {inherited_stranded} in recent outings.",
        ],
        # --- usage / leverage ---
        "higher_leverage_usage": [
            f"{name} has now made multiple late-game clean appearances and the usage pattern suggests the manager trusts him in spots that actually matter.",
            f"The recent deployment for {name} has consistently landed in the eighth inning or later — and he has been clean each time.",
        ],
        "late_inning_clean": [
            f"{name} has been showing up in late-inning spots consistently and delivering clean work — a pattern worth noting for deeper formats.",
        ],
        # --- role ---
        "saves2in7": [
            f"{name} has picked up multiple saves in the last seven days and is firmly establishing himself as the closer here.",
            f"Back-to-back save opportunities have gone to {name}, and he has converted both. The role appears to be his.",
        ],
        "save_plus_hold": [
            f"{name} has been trusted in both save and hold situations recently, signaling the manager views him as a top leverage arm.",
            f"Save and hold opportunities in the same window point to {name} getting real high-leverage work. He is worth monitoring as a potential closer handcuff.",
        ],
        "holds3": [
            f"{name} has now collected holds in three straight appearances and continues to show up in meaningful spots ahead of the closer.",
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


def within_card_hours(now_et: datetime) -> bool:
    """Returns True if current ET hour is within the card posting window (2 PM – 2 AM ET).
    This window crosses midnight, so hour >= 14 OR hour < 2."""
    hour = now_et.hour
    return hour >= CARD_HOURS_START or hour < CARD_HOURS_END


def within_trend_hours(now_et: datetime) -> bool:
    """Returns True if current ET hour is within the trend/velocity window (2 AM – 2 PM ET)."""
    hour = now_et.hour
    return TREND_HOURS_START <= hour < TREND_HOURS_END


def can_post_trend_now(state, now_et: datetime):
    # Only post trend blurbs between 2 AM and 2 PM ET
    hour = now_et.hour
    if not (TREND_HOURS_START <= hour < TREND_HOURS_END):
        return False

    # Hourly cap — max 2 trend blurbs per hour
    hour_key = now_et.strftime("%Y-%m-%d-%H")
    posts_this_hour = safe_int(state.get("trend_post_count_by_hour", {}).get(hour_key, 0), 0)
    if posts_this_hour >= TREND_MAX_PER_HOUR:
        return False

    # Random interval between blurbs
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


async def build_trend_summary_via_claude(
    name: str,
    team: str,
    trend: dict,
    recent_appearances: list,
    season_stats: dict,
    span_stats: dict,
) -> str | None:
    """
    Call Claude Haiku to generate a trend blurb summary.
    Returns a summary string or None on failure (caller falls back to templates).
    """
    try:
        if not ANTHROPIC_API_KEY:
            return None

        loop = asyncio.get_event_loop()
        code = trend.get("code", "")
        subject = trend.get("subject", "")
        family = trend.get("family", "")
        detail = trend.get("detail", {})

        # Build recent form string (last 5 appearances)
        recent_form_parts = []
        for i, a in enumerate(recent_appearances[:5]):
            label_str = "most recent" if i == 0 else f"{i+1} outings ago"
            er = a.get("er", 0)
            k = safe_int(a.get("k", 0), 0)
            bb = safe_int(a.get("bb", 0), 0)
            ip = a.get("ip", "0.0")
            recent_form_parts.append(f"{label_str}: {ip} IP, {er} ER, {k} K, {bb} BB")
        recent_form = ", ".join(recent_form_parts) if recent_form_parts else "no recent data"

        # Season stats
        season = season_stats or {}
        sv = safe_int(season.get("saves", 0), 0)
        holds = safe_int(season.get("holds", 0), 0)
        season_era = season.get("era") or season.get("earnedRunAverage") or "—"
        season_k = safe_int(season.get("strikeOuts", 0), 0)
        whip = season.get("whip") or "—"
        season_str = f"{sv} SV, {holds} HLD, {season_era} ERA, {whip} WHIP, {season_k} K"

        # Span stats
        span_ip = span_stats.get("ip", "0.0")
        span_k = span_stats.get("k", 0)
        span_bb = span_stats.get("bb", 0)
        span_er = span_stats.get("er", 0)
        span_window = span_stats.get("window", 3)
        span_str = f"{span_ip} IP, {span_er} ER, {span_k} K, {span_bb} BB over last {span_window} appearances"

        # Extra detail for specific trends
        extra_detail = ""
        if code == "workload_spike":
            extra_detail = f"He has appeared in {detail.get('apps_last6', 4)} of the last 6 days."
        elif code == "inherited_runner_specialist":
            extra_detail = f"He has stranded {detail.get('inherited_stranded', 3)} inherited runners recently."

        prompt = f"""PITCHER: {name} | {team}
TREND: {subject} (family: {family})
SPAN STATS: {span_str}
SEASON STATS: {season_str}
RECENT FORM (most recent first): {recent_form}
{('EXTRA CONTEXT: ' + extra_detail) if extra_detail else ''}

Write a 2-3 sentence fantasy baseball trend blurb about this reliever. Be direct and analytical."""

        def _call_claude():
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "anthropic-beta": "prompt-caching-2024-07-31",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "system": [
                        {
                            "type": "text",
                            "text": (
                                "You are writing a short trend blurb for a Discord fantasy baseball bot.\n\n"
                                "Instructions:\n"
                                "- Write 2-3 sentences of flowing prose. No bullet points, no markdown, no headers.\n"
                                "- Be direct and analytical — this is fantasy baseball analysis, not a hype piece.\n"
                                "- CRITICAL: Never use the word 'just' to minimize or downplay any statistic, especially walks. State numbers neutrally.\n"
                                "- CRITICAL: Never invent stats. Only reference numbers explicitly provided in the prompt.\n"
                                "- Do not start with 'In' or 'Tonight' or the pitcher's full name as the very first word.\n"
                                "- Never end with an ellipsis or incomplete thought.\n"
                                "- Vary your sentence openings across different blurbs.\n"
                                "- Target length: ~60-80 words.\n"
                                "- Respond with the blurb text only — no labels, no preamble."
                            ),
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            return "".join(
                block.get("text", "") for block in data.get("content", [])
                if block.get("type") == "text"
            ).strip()

        raw = await loop.run_in_executor(None, _call_claude)
        return raw if raw else None

    except Exception as e:
        log(f"Trend Claude API failed for {name}: {e}")
        return None


async def post_trend_card(channel, meta: dict, trend: dict, recent_appearances):
    team = meta.get("team", "UNK")
    name = meta.get("name", "Unknown Pitcher")
    pitcher_id = meta.get("id") or meta.get("pitcher_id")

    # Always fetch fresh regular-season stats — player_meta_cache may have empty season_stats
    # if the boxscore read happened before get_player_season_stats was called (gameType=R only)
    season_stats = meta.get("season_stats") or {}
    if pitcher_id and not season_stats:
        fetched = await get_player_season_stats(pitcher_id)
        if fetched:
            season_stats = fetched

    subject = f"{trend.get('emoji', '🧠')} {trend.get('subject', 'Bullpen Trend')}"
    code = trend.get("code", "")
    span_stats = summarize_trend_span(recent_appearances, code)

    # Try Claude Haiku first, fall back to templates
    summary = await build_trend_summary_via_claude(name, team, trend, recent_appearances, season_stats, span_stats)
    if summary:
        log(f"Claude trend summary generated for {name}")
    else:
        summary = build_trend_analysis(name, team, trend, recent_appearances, season_stats)

    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(team), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, name, team)
    embed.add_field(name="", value=f"**{subject}**", inline=False)
    embed.add_field(name="Season", value=format_season_line(season_stats), inline=False)
    embed.add_field(name="Summary", value=summary, inline=False)
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
    pitcher_id = meta.get("id") or meta.get("pitcher_id")

    season_stats = meta.get("season_stats") or {}
    if not season_stats and pitcher_id:
        fetched = await get_player_season_stats(pitcher_id)
        if fetched:
            season_stats = fetched

    subject = f"{velocity_alert.get('emoji', '⚠️')} {velocity_alert.get('subject', 'Velocity Alert')}"
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(team), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, name, team)
    embed.add_field(name="", value=f"**{subject}**", inline=False)
    embed.add_field(name="Season", value=format_season_line(season_stats), inline=False)
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




async def gather_trend_candidates_from_recent_games(tracked: dict, processed_pitchers_by_game):
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
        appearance_count_15 = await count_recent_appearances_in_window(pid, game_date_et, days=15)
        if appearance_count_15 < 4:
            continue

        recent = item.get("recent_appearances") or []
        if not recent:
            continue

        # IP floor: lowered to 8 outs for early season — raise back to 12 mid-April
        usage_snapshot = await get_recent_usage_snapshot(pid, game_date_et)
        trend_options = build_trend_candidates(item["current_app"], recent, None, item.get("context", {}), usage_snapshot=usage_snapshot)
        if not trend_options:
            continue

        # Check IP floor against the window for the best candidate
        best = choose_best_trend(trend_options)
        if not best:
            continue

        window = trend_window_for_code(best.get("code", ""))
        window_apps = recent[:window]
        total_outs = sum(baseball_ip_to_outs(a.get("ip", "0.0")) for a in window_apps)
        if total_outs < 8:  # ~2.2 innings minimum — lowered for early season, raise back mid-April
            log(f"Trend suppressed for {p.get('name')} — only {total_outs} outs in window (need 8)")
            continue

        candidates.append({
            "pitcher_id": pid,
            "meta": player_meta_cache.get(pid, {"id": pid, "name": p.get("name"), "team": p.get("team"), "season_stats": p.get("season_stats", {})}),
            "trend": best,
            "recent_appearances": recent,
        })
    candidates.sort(key=lambda x: x["trend"].get("priority", 0), reverse=True)
    return candidates


# ---------------- CORE ----------------

def _fetch_player_season_stats_sync(player_id: int, season: int) -> dict:
    """Fetch regular season pitching stats for a player — sportId=1 only, no spring training."""
    try:
        url = PLAYER_SEASON_STATS_URL.format(player_id, season)
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        for group in data.get("stats", []):
            splits = group.get("splits", [])
            if splits:
                return splits[0].get("stat", {})
    except Exception:
        pass
    return {}


async def get_player_season_stats(player_id: int) -> dict:
    """Get regular season pitching stats with cache."""
    if player_id in season_stats_cache:
        return season_stats_cache[player_id]
    season = datetime.now(ET).year
    loop = asyncio.get_event_loop()
    stats = await loop.run_in_executor(None, _fetch_player_season_stats_sync, player_id, season)
    season_stats_cache[player_id] = stats
    return stats


def _fetch_schedule_sync(date_str: str) -> list:
    """Blocking schedule fetch — run via executor only. Excludes Spring Training (S) and Exhibition (E) games."""
    r = requests.get(f"{SCHEDULE_URL}&date={date_str}", timeout=30)
    r.raise_for_status()
    data = r.json()
    games = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            if game.get("gameType") in ("S", "E"):
                continue
            games.append(game)
    return games


def _fetch_feed_sync(game_id) -> dict:
    """Blocking live feed fetch — run via executor only."""
    r = requests.get(LIVE_URL.format(game_id), timeout=30)
    r.raise_for_status()
    return r.json()


async def get_games() -> list:
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)
    loop = asyncio.get_event_loop()

    # Today-first: once today has any games scheduled, don't fetch yesterday
    try:
        today_games = await loop.run_in_executor(None, _fetch_schedule_sync, today.isoformat())
    except Exception as e:
        log(f"Schedule fetch error for {today}: {e}")
        today_games = []

    if today_games:
        return today_games

    # No games today — fall back to yesterday
    try:
        yesterday_games = await loop.run_in_executor(None, _fetch_schedule_sync, yesterday.isoformat())
        return yesterday_games
    except Exception as e:
        log(f"Schedule fetch error for {yesterday}: {e}")
        return []


async def build_trend_pitcher_list() -> list:
    """Build a processed_pitchers_by_game-style list from yesterday's final games.
    Used by the trend system during the 2 AM–2 PM window when today's games haven't started.
    No recency filter — we want last night's full slate regardless of game start time."""
    yesterday = (datetime.now(ET) - timedelta(days=1)).date()
    loop = asyncio.get_event_loop()
    result = []
    try:
        games = await loop.run_in_executor(None, _fetch_schedule_sync, yesterday.isoformat())
    except Exception as e:
        log(f"Trend source fetch error for {yesterday}: {e}")
        return result

    for g in games:
        if g.get("status", {}).get("detailedState") != "Final":
            continue
        game_id = g.get("gamePk")
        if not game_id:
            continue
        try:
            feed = await get_feed(game_id)
        except Exception:
            continue
        pitchers = get_pitchers(feed)
        game_date_et = parse_game_date_et(g)

        for p in pitchers:
            pitcher_id = p.get("id")
            if pitcher_id is None:
                continue
            context = get_pitcher_entry_context(feed, pitcher_id, p["side"])
            recent_appearances = await get_recent_appearances(pitcher_id, game_date_et, limit=5, max_days=21)
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
            result.append({
                "pitcher": p,
                "current_app": current_app,
                "recent_appearances": recent_for_trend,
                "context": context,
                "game_date_et": game_date_et,
            })
    log(f"Trend source: {len(result)} pitchers from {yesterday} final games")
    return result


async def get_feed(game_id) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_feed_sync, game_id)


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

            # season_stats is populated downstream via get_player_season_stats (gameType=R)
            # Do NOT read seasonStats from the boxscore — it aggregates all game types including Spring Training
            season_stats = {}

            velo = get_fastball_velocity_summary(feed, p.get("person", {}).get("id"))
            player_obj = {
                "id": p.get("person", {}).get("id"),
                "name": _fix_name(p.get("person", {}).get("fullName", "Unknown Pitcher")),
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
                    "id": player_obj["id"],
                    "name": player_obj["name"],
                    "team": normalize_team_abbr(team),
                    "season_stats": season_stats,
                }

    return result


# ---------------- CLAUDE API SUMMARY ----------------

async def build_summary_via_claude(
    name: str,
    team: str,
    opp_name: str,
    label: str,
    s: dict,
    context: dict,
    detail: dict,
    tracked_info: dict,
    recent_appearances: list,
    streak_count: int,
    usage_note: str,
    velocity_alert: dict,
    pitcher_score: int,
    opp_score: int,
    score_tail: str,
    season_stats: dict = None,
) -> dict | None:
    """
    Call the Claude API to generate a headline and summary for a pitcher card.
    Returns {"headline": str, "summary": str} or None on failure.
    """
    try:
        if not ANTHROPIC_API_KEY:
            return None

        loop = asyncio.get_event_loop()

        opp = str(opp_name or "").strip()
        role = infer_role_from_tracked_info(tracked_info)
        entry_ctx = build_context_phrase(context)
        inherited = safe_int(context.get("inherited_runners", 0), 0)
        relieved_pitcher = str(context.get("relieved_pitcher", "") or "").strip()
        ip_text = format_ip_for_summary(s["ip"])

        # Build structured context for the prompt
        stat_line = f"{ip_text}, {s['h']} H, {s['er']} ER, {s['bb']} BB, {s['k']} K"

        # Play-by-play detail
        detail_parts = []
        if detail:
            for ev in detail.get("run_events", []):
                rbi = ev["rbi"]
                detail_parts.append(f"- {ev['batter']} hit a {ev['hit_type']} ({rbi} RBI)")
            for ko in detail.get("notable_ks", []):
                detail_parts.append(f"- Struck out {ko['name']} (bats {ko['slot']}th)")
            if not detail.get("finished_inning", True):
                d_outs = detail.get("departure_outs", 0)
                d_runners = detail.get("departure_runners", 0)
                replaced = detail.get("replaced_by", "")
                d_inning = detail.get("departure_inning", 0)
                d_half = detail.get("departure_half", "")
                inning_str = f" in the {d_half.lower()} of the {ordinal(d_inning)}" if d_inning and d_half else ""
                pull_str = f"Pulled mid-inning{inning_str} with {d_outs} out(s) recorded, {d_runners} runner(s) on"
                if replaced:
                    pull_str += f", replaced by {replaced}"
                detail_parts.append(f"- {pull_str}")
            if detail.get("heart_of_order_retired"):
                detail_parts.append(f"- Retired heart of order: {', '.join(detail['heart_of_order_retired'])}")

        # Recent form — label explicitly so Claude can't misread the sequence
        recent_form_parts = []
        for i, a in enumerate(recent_appearances[:5]):
            er_val = a.get("er", 0)
            label_str = "most recent" if i == 0 else f"{i+1} outings ago"
            recent_form_parts.append(f"{label_str}: {er_val} ER")
        recent_form = ", ".join(recent_form_parts) if recent_form_parts else "no recent data"
        prev_outing_er = recent_appearances[0].get("er", 0) if recent_appearances else 0

        velocity_str = ""
        if velocity_alert:
            velocity_str = (
                f"Fastball: {velocity_alert.get('current_velocity')} MPH this outing "
                f"(baseline {velocity_alert.get('baseline_velocity')} MPH, "
                f"delta {velocity_alert.get('delta'):+.1f} MPH)"
            )

        score_context = ""
        if score_tail:
            score_context = f"Final result: {score_tail}"
        elif pitcher_score > 0 or opp_score > 0:
            if pitcher_score > opp_score:
                score_context = f"Final score: {pitcher_score}-{opp_score} (pitcher's team won)"
            elif opp_score > pitcher_score:
                score_context = f"Final score: {opp_score}-{pitcher_score} (pitcher's team lost)"
            else:
                score_context = f"Final score: tied {pitcher_score}-{opp_score} at time of outing"

        # Season stats
        season = season_stats or {}
        sv = safe_int(season.get("saves", 0), 0)
        holds = safe_int(season.get("holds", 0), 0)
        season_era = season.get("era") or season.get("earnedRunAverage") or "—"
        season_k = safe_int(season.get("strikeOuts", 0), 0)
        season_stats_str = f"{sv} SV, {holds} HLD, {season_era} ERA, {season_k} K (season totals — use only these numbers)"

        prompt = f"""You are writing two things for a Discord baseball bot card about a relief pitcher's outing:
1. A HEADLINE: one punchy sentence (max 12 words), no period at end
2. A SUMMARY: ~100 words of flowing prose, no bullet points or markdown

PITCHER: {name} | {team}
OPPONENT: {opp or 'unknown'}
ROLE: {role}
OUTING LABEL: {label}
ENTRY SITUATION: {entry_ctx}
STAT LINE: {stat_line}
SEASON STATS: {season_stats_str}
PREVIOUS OUTING: {prev_outing_er} ER (use this exact number if referencing his last appearance)
{'INHERITED RUNNERS: ' + str(inherited) + (' (relieved ' + relieved_pitcher + ')' if relieved_pitcher else '') if inherited > 0 else ''}
{'EXTRA INNINGS NOTE: Automatic runner was placed on 2nd base (ghost runner) per MLB extra inning rules. Any run scored by this runner is unearned in the box score but was still allowed by this pitcher.' if context.get('ghost_runner') else ''}
{'GAME OUTCOME: His team LOST this game. The ghost runner scored on his watch. Despite 0 ER in the box score, this was a game-losing appearance — write the summary accordingly, do not call it clean or positive.' if context.get('extra_inning_loss') else ''}
{score_context}

PLAY-BY-PLAY DETAIL:
{chr(10).join(detail_parts) if detail_parts else 'No play-by-play detail available'}

RECENT FORM (last 5 appearances, most recent first):
{recent_form}

{('VELOCITY NOTE: ' + velocity_str) if velocity_str else ''}
{('USAGE NOTE: ' + usage_note) if usage_note else ''}
{'CONSECUTIVE APPEARANCES: ' + str(streak_count) + ' straight days' if streak_count >= 2 else ''}

Respond in this exact format:
HEADLINE: [your one-sentence headline]
SUMMARY: [your summary prose]"""

        def _call_claude():
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "anthropic-beta": "prompt-caching-2024-07-31",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 250,
                    "system": [
                        {
                            "type": "text",
                            "text": (
                                "You are writing two things for a Discord baseball bot card about a relief pitcher's outing:\n"
                                "1. A HEADLINE: one punchy sentence (max 12 words), no period at end\n"
                                "2. A SUMMARY: ~100 words of flowing prose, no bullet points or markdown\n\n"
                                "Instructions:\n"
                                "- CRITICAL: Never invent or assume any numbers not explicitly provided. Do not reference save totals, ERA, strikeout numbers, or any other stats unless they appear in SEASON STATS or STAT LINE. Do not say a pitcher \"gave up an earned run in his previous outing\" unless PREVIOUS OUTING shows a non-zero ER. Do not say he \"inherited X runners\" unless INHERITED RUNNERS is explicitly listed.\n"
                                "- Summary target: ~100 words, under 600 characters total.\n"
                                "- VARY the opening structure — do not always start with \"[Name] entered in the [inning]\".\n"
                                "- Never end with an ellipsis or incomplete thought. Always end with a complete sentence.\n"
                                "- Use play-by-play detail specifically — name batters, describe what happened.\n"
                                "- If he was pulled mid-inning, mention which inning and the situation clearly.\n"
                                "- Weave in recent form naturally if relevant.\n"
                                "- Include velocity note as one sentence if provided.\n"
                                "- SCORE FORMAT: Always write scores winner-first (e.g. '4-2' means the winning team scored 4). Never write a score with the losing team's runs first. Use the FINAL SCORE field exactly as provided — it already states who won and lost.\n"
                                "- Do not start with 'In' or 'Tonight'.\n\n"
                                "Respond in this exact format:\n"
                                "HEADLINE: [your one-sentence headline]\n"
                                "SUMMARY: [your summary prose]"
                            ),
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            return "".join(
                block.get("text", "") for block in data.get("content", [])
                if block.get("type") == "text"
            ).strip()

        raw = await loop.run_in_executor(None, _call_claude)
        if not raw:
            return None

        headline = ""
        summary = ""
        for line in raw.splitlines():
            if line.startswith("HEADLINE:"):
                headline = line[len("HEADLINE:"):].strip()
            elif line.startswith("SUMMARY:"):
                summary = line[len("SUMMARY:"):].strip()

        # Fallback: if format not followed, treat entire response as summary
        if not summary and raw:
            summary = raw.strip()

        if summary:
            log(f"Claude API summary generated for {name}")
            return {"headline": headline, "summary": summary}
        return None

    except Exception as e:
        log(f"Claude API summary failed for {name}: {e}")
        return None


# ---------------- POST ----------------

async def post_card(channel, p: dict, matchup: str, score: str, context: dict, streak_count: int, tracked_info: dict, recent_appearances, usage_note: str = "", velocity_alert: dict = None, feed: dict = None, away_team_name: str = "", home_team_name: str = "", away_score: int = 0, home_score: int = 0):
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

    label = classify(s, context=context)
    detail = get_pitcher_outing_detail(feed, p.get("id"), ip=str(p["stats"].get("inningsPitched", "0.0")), er=safe_int(p["stats"].get("earnedRuns", 0), 0)) if feed else None

    # Fetch verified regular season stats (sportId=1 only — excludes spring training)
    # Falls back to boxscore seasonStats if the API call fails
    pitcher_id_for_stats = p.get("id")
    if pitcher_id_for_stats:
        verified_season_stats = await get_player_season_stats(pitcher_id_for_stats)
        if verified_season_stats:
            p = {**p, "season_stats": verified_season_stats}

    # Reclassify: a hold in the 9th or later with a margin <= 3 and runs allowed
    # is a save situation where the pitcher let damage happen — treat as SHAKY_HOLD
    if (
        label == "HOLD"
        and s["er"] > 0
        and safe_int(context.get("entry_inning"), 0) >= 9
        and safe_int(context.get("entry_margin", 0), 0) <= 3
        and context.get("entry_state_kind") == "lead"
    ):
        label = "SHAKY_HOLD"

    # Determine opponent team name from pitcher's side
    if p["side"] == "home":
        opp_name = away_team_name or ""
        pitcher_score = home_score
        opp_score = away_score
    else:
        opp_name = home_team_name or ""
        pitcher_score = away_score
        opp_score = home_score

    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])

    # Build the template-based summary first (used as fallback)
    template_summary = build_summary(
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
        detail=detail,
        opp_name=opp_name,
        pitcher_score=pitcher_score,
        opp_score=opp_score,
    )

    # Compute score_tail for passing to Claude (same logic as build_summary)
    margin = safe_int(context.get("entry_margin", 0), 0)
    state_kind = context.get("entry_state_kind", "")
    opp = str(opp_name or "").strip()
    score_tail = ""
    if (pitcher_score > 0 or opp_score > 0) and margin <= 2 and state_kind == "lead":
        win_score = max(pitcher_score, opp_score)
        lose_score = min(pitcher_score, opp_score)
        score_num = f"{win_score}-{lose_score}"
        opp_in_tail = opp and label not in {"SAVE"}
        if opp_in_tail:
            score_tail = f"{score_num} win over the {opp}"
        else:
            score_tail = f"{score_num} win"

    # Try Claude API — fall back to template on failure
    claude_result = await build_summary_via_claude(
        name=p["name"],
        team=p["team"],
        opp_name=opp_name,
        label=label,
        s=s,
        context=context,
        detail=detail,
        tracked_info=tracked_info,
        recent_appearances=recent_appearances,
        streak_count=streak_count,
        usage_note=usage_note,
        velocity_alert=velocity_alert,
        pitcher_score=pitcher_score,
        opp_score=opp_score,
        score_tail=score_tail,
        season_stats=p.get("season_stats", {}),
    )

    if claude_result:
        headline_text = claude_result.get("headline", "")
        summary_text = claude_result.get("summary", template_summary)
    else:
        headline_text = ""
        summary_text = template_summary

    # Discord embed field limit is 1024 characters — target 600 in prompt for safety
    if len(summary_text) > 650:
        summary_text = summary_text[:647].rsplit(" ", 1)[0] + "."

    # Layout: impact tag → game line → headline (bold) → summary → pitch count → season
    embed.add_field(name="", value=f"**{impact_tag(label, s)}**", inline=False)
    embed.add_field(name="Game Line", value=format_game_line(s), inline=False)
    if headline_text:
        embed.add_field(name="", value=f"**{headline_text}**", inline=False)
    embed.add_field(name="Summary", value=summary_text, inline=False)
    embed.add_field(name="Pitch Count", value=format_pitch_count(p["stats"]), inline=False)
    embed.add_field(name="Season", value=format_season_line(p.get("season_stats", {})), inline=False)

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
    trend_pitcher_cache: list = []
    trend_pitcher_cache_date: object = None
    loop_lock = asyncio.Lock()

    while True:
        if loop_lock.locked():
            log("Previous loop still posting — skipping this cycle")
            await asyncio.sleep(POLL_MINUTES * 60)
            continue

        async with loop_lock:
            try:
                now_et = datetime.now(ET)
                current_date = now_et.date()
                if current_date != last_refresh_date:
                    tracked = await refresh_tracked_pitchers()
                    last_refresh_date = current_date
                    season_stats_cache.clear()
                    trend_pitcher_cache = []
                    trend_pitcher_cache_date = None
                    log("Season stats cache cleared for new day")
                    cleanup_closer_caches()

                games = await get_games()
                log(f"Checking {len(games)} games")
                processed_pitchers_by_game = []

                # --- collect all postable candidates ---
                candidates = []

                for g in games:
                    if g.get("status", {}).get("detailedState") != "Final":
                        continue

                    game_id = g.get("gamePk")
                    if not game_id:
                        continue

                    # Recency check — skip games that started more than GAME_RECENCY_HOURS ago
                    game_date_str = g.get("gameDate", "")
                    if game_date_str:
                        try:
                            game_start_utc = datetime.fromisoformat(game_date_str.replace("Z", "+00:00"))
                            hours_since_start = (datetime.now(timezone.utc) - game_start_utc).total_seconds() / 3600
                            if hours_since_start > GAME_RECENCY_HOURS:
                                continue
                        except Exception:
                            pass

                    feed = await get_feed(game_id)
                    pitchers = get_pitchers(feed)
                    game_date_et = parse_game_date_et(g)

                    game_teams = feed.get("gameData", {}).get("teams", {})
                    away_abbr = game_teams.get("away", {}).get("abbreviation") or g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation") or "AWAY"
                    home_abbr = game_teams.get("home", {}).get("abbreviation") or g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation") or "HOME"
                    away_team_name = game_teams.get("away", {}).get("teamName") or away_abbr
                    home_team_name = game_teams.get("home", {}).get("teamName") or home_abbr
                    away_score = safe_int(g.get("teams", {}).get("away", {}).get("score", 0), 0)
                    home_score = safe_int(g.get("teams", {}).get("home", {}).get("score", 0), 0)
                    matchup = f"{away_abbr} @ {home_abbr}"
                    score = build_score_line(away_abbr, away_score, home_abbr, home_score)

                    for p in pitchers:
                        pitcher_id = p.get("id")
                        if pitcher_id is None:
                            continue

                        context = get_pitcher_entry_context(feed, pitcher_id, p["side"])
                        recent_appearances = await get_recent_appearances(pitcher_id, game_date_et, limit=5, max_days=21)
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

                        # Compute label for sorting and suppression
                        s_preview = {
                            "ip": str(p["stats"].get("inningsPitched", "0.0")),
                            "h": safe_int(p["stats"].get("hits", 0), 0),
                            "er": safe_int(p["stats"].get("earnedRuns", 0), 0),
                            "bb": safe_int(p["stats"].get("baseOnBalls", 0), 0),
                            "k": safe_int(p["stats"].get("strikeOuts", 0), 0),
                            "saves": safe_int(p["stats"].get("saves", 0), 0),
                            "holds": safe_int(p["stats"].get("holds", 0), 0),
                            "blownSaves": safe_int(p["stats"].get("blownSaves", 0), 0),
                        }
                        label_preview = classify(s_preview, context=context)
                        # Apply SHAKY_HOLD reclassification for sorting purposes
                        if (
                            label_preview == "HOLD"
                            and s_preview["er"] > 0
                            and safe_int(context.get("entry_inning"), 0) >= 9
                            and safe_int(context.get("entry_margin", 0), 0) <= 3
                            and context.get("entry_state_kind") == "lead"
                        ):
                            label_preview = "SHAKY_HOLD"

                        candidates.append({
                            "key": key,
                            "pitcher": p,
                            "game_id": game_id,
                            "pitcher_id": pitcher_id,
                            "context": context,
                            "recent_appearances": recent_appearances,
                            "recent_for_trend": recent_for_trend,
                            "current_app": current_app,
                            "game_date_et": game_date_et,
                            "matchup": matchup,
                            "score": score,
                            "away_team_name": away_team_name,
                            "home_team_name": home_team_name,
                            "away_score": away_score,
                            "home_score": home_score,
                            "tracked_info": tracked_info,
                            "is_tracked": is_tracked,
                            "label": label_preview,
                            "feed": feed,
                        })

                # --- sort by priority ---
                label_order = {
                    "SAVE": 0, "BLOWN": 1, "SHAKY_HOLD": 2, "HOLD": 3,
                    "ROUGH": 4, "DOM": 5, "TRAFFIC": 6, "CLEAN": 7, "RELIEF": 8,
                }
                candidates.sort(key=lambda c: (
                    0 if c["is_tracked"] else 1,          # tracked first
                    label_order.get(c["label"], 9),        # then by label priority
                ))

                # --- card posting window: 2 PM – 2 AM ET only ---
                if not within_card_hours(now_et):
                    if candidates:
                        log(f"Outside card posting hours ({now_et.strftime('%H:%M')} ET) — skipping {len(candidates)} candidate(s)")
                else:
                    # --- apply cap: tracked always through, non-tracked suppressed when at cap ---
                    to_post = []
                    loop_count = 0
                    for c in candidates:
                        if loop_count >= MAX_POSTS_PER_LOOP:
                            if c["is_tracked"]:
                                to_post.append(c)  # tracked always posts
                            else:
                                log(f"Suppressing {c['pitcher']['name']} (cap reached, non-tracked)")
                        else:
                            to_post.append(c)
                            loop_count += 1

                    log(f"Queued {len(to_post)} cards this loop ({len(candidates)} candidates)")

                    # --- post with stagger, write state after each ---
                    for i, c in enumerate(to_post):
                        p = c["pitcher"]
                        pitcher_id = c["pitcher_id"]
                        game_id = c["game_id"]
                        game_date_et = c["game_date_et"]
                        context = c["context"]
                        recent_appearances = c["recent_appearances"]
                        recent_for_trend = c["recent_for_trend"]
                        current_app = c["current_app"]

                        streak_count = await get_streak_count(pitcher_id, game_date_et)
                        usage_note = build_usage_sentence(await get_recent_usage_snapshot(pitcher_id, game_date_et))
                        velocity_alert = build_velocity_alert(current_app, recent_for_trend)

                        log(f"Posting {p['name']} | {p['team']} | {c['matchup']} ({i+1}/{len(to_post)})")
                        await post_card(
                            channel,
                            p,
                            c["matchup"],
                            c["score"],
                            context,
                            streak_count,
                            c["tracked_info"],
                            recent_appearances,
                            usage_note=usage_note,
                            velocity_alert=velocity_alert if c["is_tracked"] else None,
                            feed=c["feed"],
                            away_team_name=c["away_team_name"],
                            home_team_name=c["home_team_name"],
                            away_score=c["away_score"],
                            home_score=c["home_score"],
                        )

                        if (not c["is_tracked"]) and should_post_velocity_alert(state, pitcher_id, game_id, velocity_alert, now_et):
                            log(f"Velocity alert {p['name']} | {p['team']} | {velocity_alert.get('subject')}")
                            velo_season_stats = p.get("season_stats") or {}
                            if not velo_season_stats and pitcher_id:
                                fetched = await get_player_season_stats(pitcher_id)
                                if fetched:
                                    velo_season_stats = fetched
                            await post_velocity_card(
                                channel,
                                {
                                    "name": p["name"],
                                    "team": p["team"],
                                    "season_stats": velo_season_stats,
                                },
                                velocity_alert,
                            )
                            mark_velocity_posted(state, pitcher_id, game_id, velocity_alert, now_et)

                        # Write state after each post to prevent duplicates if loop overlaps
                        posted.add(c["key"])
                        state["posted"] = list(posted)
                        save_state(state)

                        if i < len(to_post) - 1:
                            await asyncio.sleep(POST_STAGGER_SECONDS)

                # --- trend blurbs ---
                if can_post_trend_now(state, now_et):
                    # During the trend window (2 AM–2 PM) today's games haven't started yet.
                    # Fall back to yesterday's final games as the trend data source.
                    # Cache it — yesterday's games are static, no need to refetch every loop.
                    trend_source = processed_pitchers_by_game
                    if not trend_source:
                        if trend_pitcher_cache_date != current_date or not trend_pitcher_cache:
                            trend_pitcher_cache = await build_trend_pitcher_list()
                            trend_pitcher_cache_date = current_date
                            log(f"Trend pitcher cache built: {len(trend_pitcher_cache)} pitchers")
                        else:
                            log(f"Trend pitcher cache hit: {len(trend_pitcher_cache)} pitchers")
                        trend_source = trend_pitcher_cache
                    trend_candidates = await gather_trend_candidates_from_recent_games(tracked, trend_source)
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
        raise RuntimeError("CLOSER_BOT_TOKEN is not set")

    await client.start(TOKEN, reconnect=True)

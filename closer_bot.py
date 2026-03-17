import asyncio
import json
import os
import time as time_module
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo

import discord
import requests

from utils.closer_depth_chart import fetch_closer_depth_chart
from utils.closer_tracker import build_tracked_relief_map, normalize_name

DISCORD_TOKEN = os.getenv("CLOSER_BOT_TOKEN")
CLOSER_WATCH_CHANNEL_ID = int(os.getenv("CLOSER_WATCH_CHANNEL_ID", "0"))
CLOSER_WATCH_POLL_MINUTES = int(os.getenv("CLOSER_WATCH_POLL_MINUTES", "10"))
STATE_DIR = os.getenv("STATE_DIR", "state/closer")
STATE_FILE = os.path.join(STATE_DIR, "closer_alert_state.json")

os.makedirs(STATE_DIR, exist_ok=True)

ET = ZoneInfo("America/New_York")

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
MLB_LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

CLOSER_WATCH_INTERVAL = 60
OUTING_DELAY_SECONDS = 600  # 10 minutes
MIN_LIVE_FEED_GAP = 0.35

FINAL_RECHECK_INTERVAL_SECONDS = 1800  # 30 minutes
FINAL_RECHECK_MAX_AGE_SECONDS = 21600  # 6 hours

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

TEAM_NAME_TO_ABBR = {
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

intents = discord.Intents.default()
client = discord.Client(intents=intents)

tracked_pitchers = build_tracked_relief_map()
last_depth_refresh_date = None
last_seen_pitchers = {}
active_outings = {}
posted_outings = set()

last_live_feed_call_time = 0.0

yesterday_pitchers_cache_date = None
yesterday_pitchers_cache = set()


def log(message: str) -> None:
    print(f"[CLOSER] {message}", flush=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_et() -> datetime:
    return datetime.now(ET)


def iso_utc_now() -> str:
    return now_utc().isoformat()


def parse_iso(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def within_operating_hours() -> bool:
    current = now_et().time()
    return time(11, 30) <= current or current < time(2, 0)


def in_quiet_hours() -> bool:
    return not within_operating_hours()


def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)


def load_state() -> dict:
    ensure_state_dir()
    if not os.path.exists(STATE_FILE):
        return {
            "posted_events": [],
            "posted_outings": [],
            "processed_final_games": {},
        }

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        state = {}

    state.setdefault("posted_events", [])
    state.setdefault("posted_outings", [])
    state.setdefault("processed_final_games", {})
    return state


def save_state(state: dict) -> None:
    ensure_state_dir()

    posted_events = state.get("posted_events", [])
    if len(posted_events) > 5000:
        state["posted_events"] = posted_events[-3000:]

    posted_outings_list = state.get("posted_outings", [])
    if len(posted_outings_list) > 5000:
        state["posted_outings"] = posted_outings_list[-3000:]

    processed_final_games = state.get("processed_final_games", {})
    if len(processed_final_games) > 500:
        items = list(processed_final_games.items())[-300:]
        state["processed_final_games"] = dict(items)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def persist_posted_outings() -> None:
    state = load_state()
    state["posted_outings"] = list(posted_outings)
    save_state(state)


def hydrate_runtime_state() -> None:
    global posted_outings

    state = load_state()
    posted_outings = set(state.get("posted_outings", []))

    if posted_outings:
        log(f"Loaded {len(posted_outings)} posted outings from state")


def get_logo(team_abbr: str) -> str | None:
    if not team_abbr:
        return None

    key = team_abbr.upper()
    if key in ("ATH", "OAK"):
        file_key = "oak"
    elif key == "CWS":
        file_key = "chw"
    else:
        file_key = key.lower()

    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{file_key}.png"


def resolve_team_abbr(game: dict, side: str, team_name: str) -> str:
    abbr = (
        game.get("teams", {})
        .get(side, {})
        .get("team", {})
        .get("abbreviation", "")
        .upper()
    )

    if abbr:
        return abbr

    return TEAM_NAME_TO_ABBR.get(team_name, "")


def format_stat_line(ip: str, h: int, er: int, bb: int, k: int) -> str:
    return f"IP {ip} • H {h} • ER {er} • BB {bb} • K {k}"


def build_score_text(away_abbr: str, away_score: int, home_abbr: str, home_score: int) -> str:
    if away_score > home_score:
        return f"**{away_abbr} {away_score}** - {home_abbr} {home_score}"
    if home_score > away_score:
        return f"{away_abbr} {away_score} - **{home_abbr} {home_score}**"
    return f"{away_abbr} {away_score} - {home_abbr} {home_score}"


def get_games() -> list:
    today_et = now_et().date()
    yesterday_et = today_et - timedelta(days=1)

    games = []

    for d in [today_et, yesterday_et]:
        url = f"{MLB_SCHEDULE_URL}&date={d.isoformat()}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        for date_block in data.get("dates", []):
            games.extend(date_block.get("games", []))

    return games


def fetch_today_games() -> list:
    today_et = now_et().date()
    url = f"{MLB_SCHEDULE_URL}&date={today_et.isoformat()}"

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log(f"Failed to fetch today's schedule: {e}")
        return []

    games = []
    for date_block in data.get("dates", []):
        games.extend(date_block.get("games", []))
    return games


def build_final_stamp(game: dict) -> str:
    status = game.get("status", {}).get("detailedState", "")
    away_score = game.get("teams", {}).get("away", {}).get("score", "")
    home_score = game.get("teams", {}).get("home", {}).get("score", "")
    game_date = game.get("gameDate", "")
    return f"{status}|{away_score}|{home_score}|{game_date}"


def normalize_final_record(record, final_stamp: str) -> dict | None:
    if not record:
        return None

    if isinstance(record, str):
        return {
            "final_stamp": record,
            "status": "done",
            "first_seen_final_utc": None,
            "last_checked_utc": None,
            "retry_count": 0,
        }

    if isinstance(record, dict):
        return {
            "final_stamp": record.get("final_stamp", final_stamp),
            "status": record.get("status", "pending"),
            "first_seen_final_utc": record.get("first_seen_final_utc"),
            "last_checked_utc": record.get("last_checked_utc"),
            "retry_count": record.get("retry_count", 0),
        }

    return None


def build_pending_final_record(existing: dict | None, final_stamp: str) -> dict:
    first_seen = existing.get("first_seen_final_utc") if existing else None
    retry_count = existing.get("retry_count", 0) if existing else 0

    return {
        "final_stamp": final_stamp,
        "status": "pending",
        "first_seen_final_utc": first_seen or iso_utc_now(),
        "last_checked_utc": iso_utc_now(),
        "retry_count": retry_count + 1,
    }


def build_done_final_record(existing: dict | None, final_stamp: str) -> dict:
    first_seen = existing.get("first_seen_final_utc") if existing else None
    retry_count = existing.get("retry_count", 0) if existing else 0

    return {
        "final_stamp": final_stamp,
        "status": "done",
        "first_seen_final_utc": first_seen or iso_utc_now(),
        "last_checked_utc": iso_utc_now(),
        "retry_count": retry_count,
    }


def build_done_no_event_record(existing: dict | None, final_stamp: str) -> dict:
    first_seen = existing.get("first_seen_final_utc") if existing else None
    retry_count = existing.get("retry_count", 0) if existing else 0

    return {
        "final_stamp": final_stamp,
        "status": "done_no_event",
        "first_seen_final_utc": first_seen or iso_utc_now(),
        "last_checked_utc": iso_utc_now(),
        "retry_count": retry_count,
    }


def pending_record_expired(record: dict, current_utc: datetime) -> bool:
    first_seen = parse_iso(record.get("first_seen_final_utc"))
    if first_seen is None:
        return False
    return (current_utc - first_seen).total_seconds() >= FINAL_RECHECK_MAX_AGE_SECONDS


def pending_record_due(record: dict, current_utc: datetime) -> bool:
    if record.get("status") != "pending":
        return False

    last_checked = parse_iso(record.get("last_checked_utc"))
    if last_checked is None:
        return True

    return (current_utc - last_checked).total_seconds() >= FINAL_RECHECK_INTERVAL_SECONDS


def should_skip_final_game(record: dict | None, final_stamp: str, current_utc: datetime) -> tuple[bool, str]:
    if not record:
        return False, ""

    if record.get("final_stamp") != final_stamp:
        return False, ""

    status = record.get("status")

    if status in {"done", "done_no_event"}:
        return True, "already processed"

    if status == "pending":
        if pending_record_expired(record, current_utc):
            return False, ""
        if not pending_record_due(record, current_utc):
            return True, "pending recheck not due"
        return False, ""

    return False, ""


def build_save_embed(
    team: str,
    pitcher: str,
    stats: str,
    score: str,
    team_abbr: str,
    matchup: str,
    summary: str,
) -> discord.Embed:
    color = TEAM_COLORS.get(team_abbr, 0x2ECC71)
    logo = get_logo(team_abbr)

    embed = discord.Embed(
        title=f"🚨 SAVE — {pitcher}",
        description=(
            f"⚾ **{matchup}**\n\n"
            f"**{stats}**\n\n"
            f"**Summary**\n{summary}\n\n"
            f"🏁 **Final Score**\n{score}"
        ),
        color=color,
        timestamp=now_utc(),
    )

    if logo:
        embed.set_author(name=team, icon_url=logo)
        embed.set_thumbnail(url=logo)
    else:
        embed.set_author(name=f"{team} Bullpen Alert")

    return embed


def build_blown_embed(
    team: str,
    pitcher: str,
    stats: str,
    score: str,
    team_abbr: str,
    matchup: str,
    summary: str,
) -> discord.Embed:
    color = TEAM_COLORS.get(team_abbr, 0xE74C3C)
    logo = get_logo(team_abbr)

    embed = discord.Embed(
        title=f"⚠️ BLOWN SAVE — {pitcher}",
        description=(
            f"⚾ **{matchup}**\n\n"
            f"**{stats}**\n\n"
            f"**Summary**\n{summary}\n\n"
            f"🏁 **Final Score**\n{score}"
        ),
        color=color,
        timestamp=now_utc(),
    )

    if logo:
        embed.set_author(name=team, icon_url=logo)
        embed.set_thumbnail(url=logo)
    else:
        embed.set_author(name=f"{team} Bullpen Alert")

    return embed


def build_closer_watch_embed(
    pitcher_name: str,
    team_abbr: str,
    role: str,
    label: str,
    stat_line: dict,
    summary: str,
    fastest_pitch: float | None,
) -> discord.Embed:
    color = TEAM_COLORS.get(team_abbr, 0x2ECC71)
    logo = get_logo(team_abbr)

    embed = discord.Embed(
        title=f"{pitcher_name} — {team_abbr}",
        description=f"**{label}**\n*{role}*",
        color=color,
        timestamp=now_utc(),
    )

    embed.add_field(
        name="Stat Line",
        value=(
            f"{stat_line['ip']} IP • {stat_line['h']} H • {stat_line['er']} ER • "
            f"{stat_line['bb']} BB • {stat_line['k']} K"
        ),
        inline=False,
    )

    embed.add_field(
        name="Pitch Count",
        value=f"{stat_line['pitches']} P • {stat_line['strikes']} S",
        inline=False,
    )

    if fastest_pitch is not None:
        embed.add_field(
            name="Fastest Pitch",
            value=f"{fastest_pitch:.1f} mph",
            inline=False,
        )

    embed.add_field(
        name="Summary",
        value=summary,
        inline=False,
    )

    if logo:
        embed.set_thumbnail(url=logo)

    return embed


async def get_channel() -> discord.abc.Messageable | None:
    channel = client.get_channel(CLOSER_WATCH_CHANNEL_ID)
    if channel is not None:
        return channel

    try:
        return await client.fetch_channel(CLOSER_WATCH_CHANNEL_ID)
    except Exception as e:
        log(f"ERROR: Could not fetch channel {CLOSER_WATCH_CHANNEL_ID}: {e}")
        return None


def refresh_closer_depth_chart() -> None:
    global tracked_pitchers
    global last_depth_refresh_date

    now = now_et()

    if last_depth_refresh_date == now.date():
        return

    if now.time() < time(12, 0):
        return

    log("Refreshing closer depth chart")

    teams = fetch_closer_depth_chart()
    if not teams:
        log("Closer depth chart refresh failed")
        return

    tracked_pitchers = build_tracked_relief_map()
    last_depth_refresh_date = now.date()

    log(f"Tracking {len(tracked_pitchers)} relievers from depth chart")


def is_tracked_pitcher_name(name: str) -> bool:
    if not name:
        return False
    return normalize_name(name) in tracked_pitchers


def fetch_live_feed(game_id: int) -> dict | None:
    global last_live_feed_call_time

    now_ts = time_module.time()
    elapsed = now_ts - last_live_feed_call_time

    if elapsed < MIN_LIVE_FEED_GAP:
        time_module.sleep(MIN_LIVE_FEED_GAP - elapsed)

    url = MLB_LIVE_FEED_URL.format(game_id)

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        last_live_feed_call_time = time_module.time()
        return r.json()
    except Exception as e:
        last_live_feed_call_time = time_module.time()
        log(f"Failed to fetch live feed for game {game_id}: {e}")
        return None


def get_current_pitchers(feed: dict) -> dict:
    result = {"home": None, "away": None}

    try:
        game_data_teams = feed["gameData"]["teams"]
        box_teams = feed["liveData"]["boxscore"]["teams"]

        for side in ["home", "away"]:
            team_abbr = game_data_teams[side]["abbreviation"]
            current_pitcher_id = box_teams[side].get("currentPitcher")

            if not current_pitcher_id:
                continue

            player_key = f"ID{current_pitcher_id}"
            player_block = box_teams[side]["players"].get(player_key, {})
            full_name = player_block.get("person", {}).get("fullName", "")

            result[side] = {
                "id": current_pitcher_id,
                "name": full_name,
                "team": team_abbr,
                "side": side,
            }

    except Exception as e:
        log(f"Error parsing current pitchers: {e}")

    return result


def maybe_start_outing(game_id: int, pitcher_info: dict) -> None:
    if not pitcher_info:
        return

    raw_name = pitcher_info.get("name", "").strip()
    if not raw_name:
        return

    normalized = normalize_name(raw_name)
    tracked_info = tracked_pitchers.get(normalized)

    if not tracked_info:
        return

    outing_key = f"{game_id}_{pitcher_info['id']}"

    if outing_key in active_outings or outing_key in posted_outings:
        return

    active_outings[outing_key] = {
        "game_id": game_id,
        "pitcher_id": pitcher_info["id"],
        "pitcher_name": tracked_info.get("name", raw_name),
        "team": tracked_info.get("team", pitcher_info.get("team")),
        "role": tracked_info.get("role", "Tracked"),
        "side": pitcher_info.get("side"),
        "started_at": now_et().isoformat(),
        "status": "active",
        "pending_since": None,
    }

    log(
        f"Started outing | {tracked_info.get('name', raw_name)} | "
        f"{tracked_info.get('team')} | {tracked_info.get('role')} | game {game_id}"
    )


def scan_single_live_game(game: dict) -> None:
    game_id = game.get("gamePk")
    if not game_id:
        return

    status = game.get("status", {}).get("abstractGameState")
    if status not in ["Live", "Final"]:
        return

    feed = fetch_live_feed(game_id)
    if not feed:
        return

    current_pitchers = get_current_pitchers(feed)

    if game_id not in last_seen_pitchers:
        last_seen_pitchers[game_id] = {"home": None, "away": None}

    for side in ["home", "away"]:
        pitcher = current_pitchers.get(side)
        previous_pitcher = last_seen_pitchers[game_id].get(side)

        pitcher_id = pitcher.get("id") if pitcher else None
        previous_pitcher_id = previous_pitcher.get("id") if previous_pitcher else None

        if pitcher and pitcher_id != previous_pitcher_id:
            maybe_start_outing(game_id, pitcher)

        last_seen_pitchers[game_id][side] = pitcher


def scan_live_games() -> None:
    games = fetch_today_games()
    if not games:
        return

    for game in games:
        scan_single_live_game(game)


def get_pitcher_stat_line(feed: dict, side: str, pitcher_id: int) -> dict:
    try:
        teams = feed["liveData"]["boxscore"]["teams"]
        player_key = f"ID{pitcher_id}"
        player_block = teams[side]["players"].get(player_key, {})
        stats = player_block.get("stats", {}).get("pitching", {})

        return {
            "ip": stats.get("inningsPitched", "0.0"),
            "h": stats.get("hits", 0),
            "er": stats.get("earnedRuns", 0),
            "bb": stats.get("baseOnBalls", 0),
            "k": stats.get("strikeOuts", 0),
            "hr": stats.get("homeRuns", 0),
            "pitches": stats.get("numberOfPitches", 0),
            "strikes": stats.get("strikes", 0),
            "saves": stats.get("saves", 0),
            "holds": stats.get("holds", 0),
            "blownSaves": stats.get("blownSaves", 0),
        }
    except Exception as e:
        log(f"Failed to pull stat line for pitcher {pitcher_id}: {e}")
        return {
            "ip": "0.0",
            "h": 0,
            "er": 0,
            "bb": 0,
            "k": 0,
            "hr": 0,
            "pitches": 0,
            "strikes": 0,
            "saves": 0,
            "holds": 0,
            "blownSaves": 0,
        }


def get_game_context(feed: dict, side: str) -> dict:
    try:
        linescore = feed.get("liveData", {}).get("linescore", {})
        inning = linescore.get("currentInning", 0)
        inning_ordinal = linescore.get("currentInningOrdinal", str(inning))

        home_runs = linescore.get("teams", {}).get("home", {}).get("runs", 0)
        away_runs = linescore.get("teams", {}).get("away", {}).get("runs", 0)

        if side == "home":
            team_runs = home_runs
            opp_runs = away_runs
        else:
            team_runs = away_runs
            opp_runs = home_runs

        diff = team_runs - opp_runs

        if diff > 0:
            game_state = f"protecting a {diff}-run lead"
        elif diff < 0:
            game_state = f"working while trailing by {abs(diff)}"
        else:
            game_state = "with the game tied"

        return {
            "inning_ordinal": inning_ordinal,
            "game_state": game_state,
        }

    except Exception:
        return {
            "inning_ordinal": "",
            "game_state": "in a bullpen spot",
        }


def get_fastest_pitch(feed: dict, pitcher_id: int) -> float | None:
    try:
        all_plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
        fastest = None

        for play in all_plays:
            matchup = play.get("matchup", {})
            pitcher = matchup.get("pitcher", {})
            if pitcher.get("id") != pitcher_id:
                continue

            for event in play.get("playEvents", []):
                pitch_data = event.get("pitchData")
                if not pitch_data:
                    continue

                start_speed = pitch_data.get("startSpeed")
                if start_speed is None:
                    continue

                try:
                    velo = float(start_speed)
                except (TypeError, ValueError):
                    continue

                if fastest is None or velo > fastest:
                    fastest = velo

        return fastest

    except Exception as e:
        log(f"Failed to get fastest pitch for pitcher {pitcher_id}: {e}")
        return None


def refresh_yesterday_pitchers_cache() -> None:
    global yesterday_pitchers_cache_date
    global yesterday_pitchers_cache

    target_date = now_et().date() - timedelta(days=1)
    if yesterday_pitchers_cache_date == target_date:
        return

    pitchers = set()

    try:
        url = f"{MLB_SCHEDULE_URL}&date={target_date.isoformat()}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        for date_block in data.get("dates", []):
            for game in date_block.get("games", []):
                game_pk = game.get("gamePk")
                if not game_pk:
                    continue

                    feed = fetch_live_feed(game_pk)
                if not feed:
                    continue

                box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})

                for side in ["home", "away"]:
                    players = box.get(side, {}).get("players", {})
                    for p in players.values():
                        person = p.get("person", {})
                        pitching_stats = p.get("stats", {}).get("pitching")
                        if not pitching_stats:
                            continue

                        if pitching_stats.get("inningsPitched"):
                            pid = person.get("id")
                            if pid is not None:
                                pitchers.add(pid)

        yesterday_pitchers_cache = pitchers
        yesterday_pitchers_cache_date = target_date
        log(f"Back-to-back cache loaded for {len(pitchers)} pitchers from {target_date.isoformat()}")

    except Exception as e:
        log(f"Failed to build back-to-back cache: {e}")
        yesterday_pitchers_cache = set()
        yesterday_pitchers_cache_date = target_date


def pitched_yesterday(pitcher_id: int) -> bool:
    refresh_yesterday_pitchers_cache()
    return pitcher_id in yesterday_pitchers_cache


def classify_outing(stat_line: dict) -> str:
    if stat_line.get("saves", 0) > 0:
        return "Save"
    if stat_line.get("holds", 0) > 0:
        return "Hold"
    if stat_line.get("blownSaves", 0) > 0:
        return "Blown save"

    er = stat_line["er"]
    hits = stat_line["h"]
    bb = stat_line["bb"]
    k = stat_line["k"]

    baserunners = hits + bb

    if er >= 3:
        return "Rough outing"
    if er >= 1 and baserunners >= 3:
        return "Trouble in relief"
    if er >= 1:
        return "Runs allowed"
    if er == 0 and baserunners == 0 and k >= 2:
        return "Dominant outing"
    if er == 0 and baserunners == 0:
        return "Clean appearance"
    if er == 0 and baserunners >= 2:
        return "Traffic but escaped"
    return "Scoreless outing"


def build_summary(
    outing: dict,
    stat_line: dict,
    context: dict,
    fastest_pitch: float | None = None,
    worked_yesterday: bool = False,
) -> str:
    name = outing["pitcher_name"]
    team = outing["team"]

    ip = stat_line["ip"]
    h = stat_line["h"]
    er = stat_line["er"]
    bb = stat_line["bb"]
    k = stat_line["k"]

    inning = context["inning_ordinal"]
    situation = context["game_state"]

    baserunners = h + bb

    parts = []

    if stat_line.get("saves", 0) > 0:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} and finished the game with a save."
        )
    elif stat_line.get("holds", 0) > 0:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} and recorded a hold before turning the game over to the next reliever."
        )
    elif stat_line.get("blownSaves", 0) > 0:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} but was charged with a blown save."
        )
    elif er == 0 and baserunners == 0 and k >= 2:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} and turned in a dominant outing."
        )
    elif er == 0 and baserunners == 0:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} and delivered a clean scoreless appearance."
        )
    elif er == 0 and baserunners >= 2:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} and worked through traffic to keep the outing scoreless."
        )
    elif er >= 3:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} but the outing unraveled quickly."
        )
    else:
        parts.append(
            f"{name} entered in the {inning} {situation} for {team} and allowed runs in relief."
        )

    if er == 0 and baserunners == 0 and k >= 2:
        parts.append(f"He worked {ip} innings without allowing a baserunner and struck out {k}.")
    elif er == 0 and baserunners == 0:
        parts.append(f"He covered {ip} innings and kept traffic off the bases.")
    elif er == 0 and baserunners >= 2:
        parts.append(f"He allowed {baserunners} baserunners over {ip} innings but escaped the jam.")
    elif er >= 3:
        parts.append(f"He was charged with {er} earned runs over {ip} innings while allowing {h} hits and {bb} walks.")
    else:
        parts.append(f"He finished with {ip} innings pitched, {er} earned runs, {h} hits, {bb} walks and {k} strikeouts.")

    if fastest_pitch is not None:
        parts.append(f"His fastest pitch was {fastest_pitch:.1f} mph.")

    if worked_yesterday:
        parts.append(f"He worked a second straight day for {team}.")

    return " ".join(parts)


async def post_closer_watch_outing(
    outing: dict,
    stat_line: dict,
    label: str,
    summary: str,
    fastest_pitch: float | None,
) -> None:
    channel = await get_channel()
    if channel is None:
        return

    outing_id = f"{outing['game_id']}_{outing['pitcher_id']}"
    if outing_id in posted_outings:
        log(f"Skipping duplicate closer watch outing: {outing_id}")
        return

    embed = build_closer_watch_embed(
        pitcher_name=outing["pitcher_name"],
        team_abbr=outing["team"],
        role=outing["role"],
        label=label,
        stat_line=stat_line,
        summary=summary,
        fastest_pitch=fastest_pitch,
    )

    try:
        await channel.send(embed=embed)
        posted_outings.add(outing_id)
        persist_posted_outings()
        log(f"Posted closer watch outing: {outing['pitcher_name']} | {outing['team']} | {label}")
        await asyncio.sleep(1.5)
    except Exception as e:
        log(f"Discord send error on closer watch post: {e}")


async def finalize_completed_outings() -> None:
    now = now_et()
    to_remove = []

    for outing_key, outing in list(active_outings.items()):
        game_id = outing["game_id"]
        pitcher_id = outing["pitcher_id"]
        side = outing["side"]

        feed = fetch_live_feed(game_id)
        if not feed:
            continue

        current_pitchers = get_current_pitchers(feed)
        current_side_pitcher = current_pitchers.get(side)
        current_side_pitcher_id = current_side_pitcher.get("id") if current_side_pitcher else None

        game_status = feed.get("gameData", {}).get("status", {}).get("abstractGameState", "")

        if game_status == "Live" and current_side_pitcher_id == pitcher_id:
            continue

        if outing["status"] == "active":
            outing["status"] = "pending"
            outing["pending_since"] = now.isoformat()
            log(f"Outing pending finalization | {outing['pitcher_name']} | game {game_id}")
            continue

        if outing["status"] == "pending":
            pending_since = datetime.fromisoformat(outing["pending_since"])
            seconds_waited = (now - pending_since).total_seconds()

            if seconds_waited < OUTING_DELAY_SECONDS:
                continue

            stat_line = get_pitcher_stat_line(feed, side, pitcher_id)
            context = get_game_context(feed, side)
            fastest_pitch = get_fastest_pitch(feed, pitcher_id)
            worked_yesterday = pitched_yesterday(pitcher_id)

            label = classify_outing(stat_line)
            summary = build_summary(
                outing,
                stat_line,
                context,
                fastest_pitch=fastest_pitch,
                worked_yesterday=worked_yesterday,
            )

            await post_closer_watch_outing(
                outing=outing,
                stat_line=stat_line,
                label=label,
                summary=summary,
                fastest_pitch=fastest_pitch,
            )

            to_remove.append(outing_key)

    for outing_key in to_remove:
        active_outings.pop(outing_key, None)


async def process_games() -> None:
    global posted_outings

    state = load_state()
    posted_events = set(state.get("posted_events", []))
    posted_outings.update(state.get("posted_outings", []))
    processed_final_games = state.get("processed_final_games", {})

    channel = await get_channel()
    if channel is None:
        return

    games = get_games()
    current_utc = now_utc()

    total_final_games_seen = 0
    total_new_final_games = 0
    total_saves_found = 0
    total_blown_found = 0
    total_posted = 0

    log(f"Games found: {len(games)}")

    for game in games:
        status = game.get("status", {}).get("detailedState", "")
        if status != "Final":
            continue

        total_final_games_seen += 1

        game_pk = game.get("gamePk")
        if not game_pk:
            continue

        game_pk_str = str(game_pk)
        final_stamp = build_final_stamp(game)
        record = normalize_final_record(processed_final_games.get(game_pk_str), final_stamp)

        skip_game, reason = should_skip_final_game(record, final_stamp, current_utc)
        if skip_game:
            if reason == "already processed":
                log(f"Skipping already processed final game: {game_pk}")
            elif reason == "pending recheck not due":
                log(f"Skipping pending final not due yet: {game_pk}")
            continue

        if record and record.get("status") == "pending":
            log(f"Rechecking pending final game: {game_pk}")
        else:
            total_new_final_games += 1
            log(f"Processing new final game: {game_pk}")

        data = fetch_live_feed(game_pk)
        if not data:
            processed_final_games[game_pk_str] = build_pending_final_record(record, final_stamp)
            continue

        box = data.get("liveData", {}).get("boxscore", {}).get("teams", {})
        if not box:
            log(f"No boxscore found for game {game_pk}")
            processed_final_games[game_pk_str] = build_pending_final_record(record, final_stamp)
            continue

        away_team_box = box.get("away", {})
        home_team_box = box.get("home", {})

        away_team_name = away_team_box.get("team", {}).get("name", "Away Team")
        home_team_name = home_team_box.get("team", {}).get("name", "Home Team")

        away_abbr = resolve_team_abbr(game, "away", away_team_name)
        home_abbr = resolve_team_abbr(game, "home", home_team_name)

        away_score = away_team_box.get("teamStats", {}).get("batting", {}).get("runs", 0)
        home_score = home_team_box.get("teamStats", {}).get("batting", {}).get("runs", 0)

        matchup = f"{away_abbr or 'AWAY'} @ {home_abbr or 'HOME'}"
        score = build_score_text(
            away_abbr or "AWAY",
            away_score,
            home_abbr or "HOME",
            home_score,
        )

        game_saves = 0
        game_blown = 0
        game_posted = 0
        blown_posted_teams = set()

        for side in ["away", "home"]:
            team_box = box.get(side, {})
            team = team_box.get("team", {}).get("name", "Unknown Team")
            team_abbr = away_abbr if side == "away" else home_abbr
            players = team_box.get("players", {})

            for p in players.values():
                pitching_stats = p.get("stats", {}).get("pitching")
                if not pitching_stats:
                    continue

                pitcher = p.get("person", {}).get("fullName", "Unknown Pitcher")
                pitcher_id = p.get("person", {}).get("id", pitcher)
                outing_key = f"{game_pk}_{pitcher_id}"

                ip = pitching_stats.get("inningsPitched", "0.0")
                h = pitching_stats.get("hits", 0)
                er = pitching_stats.get("earnedRuns", 0)
                bb = pitching_stats.get("baseOnBalls", 0)
                k = pitching_stats.get("strikeOuts", 0)

                stat_line = format_stat_line(ip, h, er, bb, k)

                full_stat_line = get_pitcher_stat_line(data, side, pitcher_id)
                context = get_game_context(data, side)
                fastest_pitch = get_fastest_pitch(data, pitcher_id)
                worked_yesterday = pitched_yesterday(pitcher_id)

                summary_outing = {
                    "pitcher_name": pitcher,
                    "team": team_abbr or team,
                    "role": tracked_pitchers.get(normalize_name(pitcher), {}).get("role", "Tracked"),
                }

                summary = build_summary(
                    summary_outing,
                    full_stat_line,
                    context,
                    fastest_pitch=fastest_pitch,
                    worked_yesterday=worked_yesterday,
                )

                if pitching_stats.get("saves", 0) > 0:
                    total_saves_found += 1
                    game_saves += 1
                    event_key = f"save_{game_pk}_{pitcher_id}"

                    if is_tracked_pitcher_name(pitcher) and outing_key in posted_outings:
                        log(f"Skipping tracked save already posted live: {pitcher} | {team}")
                    elif event_key in posted_events:
                        log(f"Skipping duplicate save: {pitcher} | {team}")
                    else:
                        embed = build_save_embed(
                            team=team,
                            pitcher=pitcher,
                            stats=stat_line,
                            score=score,
                            team_abbr=team_abbr,
                            matchup=matchup,
                            summary=summary,
                        )
                        try:
                            await channel.send(embed=embed)
                            posted_events.add(event_key)
                            posted_outings.add(outing_key)
                            total_posted += 1
                            game_posted += 1
                            log(f"SAVE: {pitcher} | {team}")
                            await asyncio.sleep(1.5)
                        except Exception as e:
                            log(f"Discord send error on save: {e}")

                if pitching_stats.get("blownSaves", 0) > 0:
                    total_blown_found += 1
                    game_blown += 1

                    if team in blown_posted_teams:
                        log(f"Skipping extra blown save for team in same game: {team}")
                        continue

                    event_key = f"blown_team_{game_pk}_{team}"

                    if is_tracked_pitcher_name(pitcher) and outing_key in posted_outings:
                        log(f"Skipping tracked blown save already posted live: {pitcher} | {team}")
                    elif event_key in posted_events:
                        log(f"Skipping duplicate blown save team alert: {team}")
                    else:
                        embed = build_blown_embed(
                            team=team,
                            pitcher=pitcher,
                            stats=stat_line,
                            score=score,
                            team_abbr=team_abbr,
                            matchup=matchup,
                            summary=summary,
                        )
                        try:
                            await channel.send(embed=embed)
                            posted_events.add(event_key)
                            posted_outings.add(outing_key)
                            blown_posted_teams.add(team)
                            total_posted += 1
                            game_posted += 1
                            log(f"BLOWN SAVE: {pitcher} | {team}")
                            await asyncio.sleep(1.5)
                        except Exception as e:
                            log(f"Discord send error on blown save: {e}")

        if game_saves > 0 or game_blown > 0:
            processed_final_games[game_pk_str] = build_done_final_record(record, final_stamp)
        else:
            pending_record = build_pending_final_record(record, final_stamp)
            if pending_record_expired(pending_record, current_utc):
                processed_final_games[game_pk_str] = build_done_no_event_record(record, final_stamp)
                log(f"Final game expired with no save/blown save after 6 hours: {game_pk}")
            else:
                processed_final_games[game_pk_str] = pending_record
                log(
                    f"Final game pending recheck | {game_pk} | "
                    f"retry_count={pending_record['retry_count']}"
                )

        log(
            f"Game {game_pk} complete | "
            f"Saves found: {game_saves} | "
            f"Blown saves found: {game_blown} | "
            f"Posted: {game_posted}"
        )

    state["posted_events"] = list(posted_events)
    state["posted_outings"] = list(posted_outings)
    state["processed_final_games"] = processed_final_games
    save_state(state)

    log(
        f"Loop summary | "
        f"Final games seen: {total_final_games_seen} | "
        f"New finals processed: {total_new_final_games} | "
        f"Saves found: {total_saves_found} | "
        f"Blown saves found: {total_blown_found} | "
        f"Posted this loop: {total_posted}"
    )


async def final_game_loop() -> None:
    await client.wait_until_ready()

    log("=== FINAL GAME FALLBACK LOOP STARTED ===")
    log(f"Poll interval: {CLOSER_WATCH_POLL_MINUTES} minutes")
    log(f"Pending recheck interval: {FINAL_RECHECK_INTERVAL_SECONDS // 60} minutes")
    log(f"Pending max age: {FINAL_RECHECK_MAX_AGE_SECONDS // 3600} hours")
    log(f"State file: {STATE_FILE}")

    while not client.is_closed():
        current_et = now_et().strftime("%Y-%m-%d %I:%M:%S %p %Z")
        log(f"Final loop start | ET time: {current_et}")

        try:
            if in_quiet_hours():
                log("Quiet hours active (2:00 AM ET - 11:30 AM ET). Skipping final-game loop.")
            else:
                refresh_closer_depth_chart()
                await process_games()
        except Exception as e:
            log(f"ERROR in final-game loop: {e}")

        log(f"Sleeping {CLOSER_WATCH_POLL_MINUTES} minutes")
        await asyncio.sleep(CLOSER_WATCH_POLL_MINUTES * 60)


async def closer_watch_loop() -> None:
    await client.wait_until_ready()

    log("=== CLOSER WATCH LOOP STARTED ===")
    log(f"Closer watch interval: {CLOSER_WATCH_INTERVAL} seconds")

    while not client.is_closed():
        try:
            if in_quiet_hours():
                await asyncio.sleep(300)
                continue

            refresh_closer_depth_chart()
            scan_live_games()
            await finalize_completed_outings()

        except Exception as e:
            log(f"ERROR in closer watch loop: {e}")

        await asyncio.sleep(CLOSER_WATCH_INTERVAL)


@client.event
async def on_ready():
    log(f"Logged in as {client.user}")

    if not hasattr(client, "runtime_state_hydrated"):
        hydrate_runtime_state()
        client.runtime_state_hydrated = True

    if not hasattr(client, "final_game_task"):
        client.final_game_task = asyncio.create_task(final_game_loop())

    if not hasattr(client, "closer_watch_task"):
        client.closer_watch_task = asyncio.create_task(closer_watch_loop())


if not DISCORD_TOKEN:
    raise RuntimeError("CLOSER_BOT_TOKEN is not set")

if not CLOSER_WATCH_CHANNEL_ID:
    raise RuntimeError("CLOSER_WATCH_CHANNEL_ID is not set")


async def start_closer_bot():
    await client.start(DISCORD_TOKEN)

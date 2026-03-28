import asyncio
import json
import os
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

POLL_MINUTES = 10
RESET_STARTER_STATE = os.getenv("RESET_STARTER_STATE", "").lower() in {"1", "true", "yes"}

MIN_STARTER_SCORE = float(os.getenv("STARTER_MIN_SCORE", "5.0"))
MAX_STARTER_CARDS_PER_GAME = int(os.getenv("STARTER_MAX_CARDS_PER_GAME", "2"))

VELOCITY_MIN_PITCHES = 10
VELOCITY_MIN_FASTBALLS = 3
FASTBALL_PITCH_CODES = {"FF", "FT", "SI", "FC", "FA", "FS"}

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

pitching_stats_cache = {}
player_meta_cache = {}

ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
player_headshot_index = None

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
        whole = safe_int(float(text), 0)
        return f"{whole} innings"
    if text.endswith(".1"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅓ of an inning" if whole == 0 else f"{whole}⅓ innings"
    if text.endswith(".2"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅔ of an inning" if whole == 0 else f"{whole}⅔ innings"
    return f"{text} innings"


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
                        "avg_fastball_velocity": None,
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


def starter_score(stats: dict) -> float:
    ip = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    h = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    qs = 1 if ip >= 6.0 and er <= 3 else 0
    win = safe_int(stats.get("wins", 0), 0)
    return round(ip * 2.1 + k * 0.9 + qs * 3.0 + win * 1.5 - er * 2.0 - h * 0.35 - bb * 0.5, 2)



def classify_starter(p: dict) -> str:
    stats = p.get("stats", {})
    ip = safe_float(stats.get("inningsPitched", "0.0"), 0.0)
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    traffic = hits + bb
    whiffs = safe_int(p.get("whiffs", 0), 0)
    csw = safe_float(p.get("csw_percent", 0.0), 0.0)
    pitch_count = safe_int(p.get("pitch_count", 0), 0)
    strike_pct = (safe_int(p.get("strikes", 0), 0) / pitch_count * 100.0) if pitch_count > 0 else 0.0

    if outs < 9:
        if er >= 3:
            return "ROUGH"
        if bb >= 3 and strike_pct < 60.0:
            return "NO_COMMAND"
        return "SHORT"

    if bb >= 5 or (bb >= 4 and strike_pct < 60.0 and er >= 2):
        return "NO_COMMAND"

    if hits >= 9 or (hits >= 7 and bb <= 1 and er >= 4):
        return "HIT_HARD"

    if er >= 5:
        return "ROUGH"

    if ip >= 6.0 and er == 0 and traffic <= 5 and (k >= 8 or whiffs >= 12 or csw >= 33.0):
        return "DOMINANT"

    if ip >= 7.0 and er == 0 and traffic <= 6:
        return "GEM"

    if ip >= 6.0 and er <= 1 and traffic <= 5 and (k >= 7 or whiffs >= 10 or csw >= 31.0):
        return "DOMINANT"

    if ip >= 6.0 and er <= 3:
        return "QUALITY"

    if (k >= 9 or whiffs >= 12 or csw >= 34.0) and er <= 4:
        return "STRIKEOUT"

    if ip >= 5.0 and er <= 1 and traffic <= 5 and bb <= 2:
        return "SHARP"

    if traffic >= 9 or (hits >= 6 and er <= 2):
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


def format_starter_game_line(stats: dict) -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    return " • ".join([f"{ip} IP", f"{h} H", f"{er} ER", f"{bb} BB", f"{k} K"])


def build_starter_summary_seed(name: str, stats: dict, game_context: dict) -> int:
    seed_text = (
        f"{name}|{stats.get('inningsPitched', '0.0')}|{stats.get('hits', 0)}|"
        f"{stats.get('earnedRuns', 0)}|{stats.get('baseOnBalls', 0)}|"
        f"{stats.get('strikeOuts', 0)}|{game_context.get('score_display', '')}"
    )
    return sum(ord(ch) for ch in seed_text)


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
        whole = safe_int(float(text), 0)
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


def is_bad_starter_label(label: str) -> bool:
    return label in BAD_STARTER_LABELS


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


def choose_seeded(options, seed: int, salt: int = 0):
    if not options:
        return ""
    return options[(seed + salt) % len(options)]


def sentence_has_ip_reference(sentence: str) -> bool:
    text = str(sentence or "").lower()
    return any(token in text for token in [" inning", " innings", "⅓", "⅔", " through ", " over ", " across "])


def build_count_clause(h: int, er: int, bb: int, k: int, include_ip: bool = False, ip_text: str = "") -> str:
    hit_text = stat_phrase(h, "hit")
    er_text = stat_phrase(er, "earned run")
    bb_text = stat_phrase(bb, "walk")
    k_text = stat_phrase(k, "strikeout")

    clauses = [
        f"He gave up {hit_text} and {er_text}, with {bb_text} and {k_text}.",
        f"The final line was {hit_text}, {er_text}, {bb_text}, and {k_text}.",
        f"He finished with {hit_text} allowed, {er_text}, {bb_text}, and {k_text}.",
    ]
    if include_ip and ip_text:
        clauses.extend([
            f"Over {ip_text}, he gave up {hit_text} and {er_text}, with {bb_text} and {k_text}.",
            f"In {ip_text}, he allowed {hit_text} and {er_text}, while issuing {bb_text} and finishing with {k_text}.",
        ])
    return clauses



def build_starter_overview(name: str, label: str, stats: dict, seed: int) -> str:
    ip_text = format_starter_ip_for_summary(str(stats.get("inningsPitched", "0.0")))
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))

    if outs < 3:
        variants = {
            "ROUGH": [
                f"{name} was in trouble almost immediately, and the outing got away from him before it ever settled.",
                f"{name} never really found his footing, and this one unraveled in a hurry.",
                f"{name} barely had time to settle in before the game started moving the wrong way.",
            ],
            "NO_COMMAND": [
                f"{name} never got the count working in his favor, and that put him on the defensive right away.",
                f"{name} was fighting his delivery from the start, and the outing never slowed down for him.",
                f"{name} could not find enough strikes early, and the night turned on him in a hurry.",
            ],
            "HIT_HARD": [
                f"{name} was getting too much of the plate early, and hitters made him pay for it.",
                f"{name} did not have much margin from the jump, and the contact against him was loud right away.",
                f"{name} left too many balls in damage spots, and the game got heavy quickly.",
            ],
            "SHORT": [
                f"{name} was out much earlier than his club needed, so the bullpen had to get involved fast.",
                f"{name} never gave this the feel of a full start, and the workload ended up shifting early.",
                f"{name} only gave his club a brief look before the game had to be handed off.",
            ],
        }
        return choose_seeded(variants.get(label, variants["SHORT"]), seed, 1)

    variants = {
        "GEM": [
            f"{name} had the game under control for most of the night.",
            f"{name} gave his club exactly the kind of tone-setting start it wanted.",
            f"{name} was in command from the start and never let the game get loose.",
            f"{name} made this look comfortable for long stretches.",
        ],
        "DOMINANT": [
            f"{name} grabbed control of this start early and kept it.",
            f"{name} was on top of this outing almost the whole way.",
            f"{name} looked overpowering for much of the night.",
            f"{name} dictated the pace from the mound and never really let hitters breathe.",
        ],
        "QUALITY": [
            f"{name} gave his club a sturdy start and kept the damage in check.",
            f"{name} turned in the kind of outing that keeps a team in good shape all night.",
            f"{name} did the job of a starter here, giving his side real length without much damage.",
            f"{name} gave his club a strong foundation and stayed out of major trouble.",
        ],
        "STRIKEOUT": [
            f"{name} leaned on putaway stuff and kept finding ways to miss bats.",
            f"{name} had enough finish on his stuff to keep piling up strikeouts.",
            f"{name} brought real swing-and-miss to this outing, even if it was not perfectly clean.",
            f"{name} kept hitters uncomfortable with how often he was able to get to a putaway pitch.",
        ],
        "SHARP": [
            f"{name} looked crisp and rarely let the game get noisy.",
            f"{name} worked with a steady rhythm and kept things under control.",
            f"{name} stayed pretty clean throughout and did not give hitters much room to build anything.",
            f"{name} was efficient with his traffic and never let the outing feel chaotic.",
        ],
        "SOLID": [
            f"{name} gave his side a useful start and kept the game from drifting too far while he was out there.",
            f"{name} turned in a steady outing and did enough to keep his club in it.",
            f"{name} was not flashy, but he gave his side a workable start.",
            f"{name} kept this one on the rails long enough to give his club a chance.",
        ],
        "UNEVEN": [
            f"{name} had to work for this one, but he still kept it from fully breaking on him.",
            f"{name} never really cruised, though he did enough to keep the outing from turning ugly.",
            f"{name} had traffic to manage most of the way and spent a lot of the night escaping trouble.",
            f"{name} worked around more stress than the final line alone might suggest.",
        ],
        "SHORT": [
            f"{name} did not make it as deep as his club needed.",
            f"{name} was out earlier than expected, so this never took the shape of a full start.",
            f"{name} gave his club only a short look before the bullpen had to take over.",
            f"{name} left early enough that the game had to be pieced together behind him.",
        ],
        "ROUGH": [
            f"{name} had a rough night and never fully got things back under control.",
            f"{name} spent too much of the outing trying to stop the game from snowballing.",
            f"{name} could not keep the pressure from building, and the outing kept leaning the wrong way.",
            f"{name} never really found a calm pocket in this start.",
        ],
        "NO_COMMAND": [
            f"{name} spent too much of the night behind in the count, and that shaped the whole outing.",
            f"{name} was fighting the zone more than the lineup, which kept every inning from settling down.",
            f"{name} never really got into attack mode because the command kept wavering.",
            f"{name} had stuff to work with, but too many counts drifted out of his control.",
        ],
        "HIT_HARD": [
            f"{name} gave up too much loud contact for the outing to ever feel safe.",
            f"{name} was around the plate, but too much of what he threw got squared up.",
            f"{name} never had much margin because hitters kept finding hard contact against him.",
            f"{name} paid for too many pitches left in spots where hitters could do damage.",
        ],
    }
    return choose_seeded(variants.get(label, variants["SOLID"]), seed, 2)



def build_starter_stat_sentence(stats: dict, seed: int, include_ip: bool = False) -> str:
    h = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    ip_text = format_starter_ip_for_summary(str(stats.get("inningsPitched", "0.0")))
    return choose_seeded(build_count_clause(h, er, bb, k, include_ip=include_ip, ip_text=ip_text), seed, 3)



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
            f"The swing-and-miss still showed up, as he punched out {number_word(k)}.",
            f"He still missed enough bats to finish with {number_word(k)} strikeouts.",
            f"There was at least one real positive underneath it, and that was the strikeout total.",
        ])

    if bb <= 1 and (h >= 5 or er >= 4):
        positives.extend([
            "This was more about the quality of contact than scattered command.",
            f"He was around the plate most of the night, so the bigger issue was what happened once hitters swung.",
            f"Command was not really the main culprit, with only {stat_phrase(bb, 'walk')} issued.",
        ])

    if label == "SHORT" and er <= 2 and k >= 5:
        positives.extend([
            "Even in a short outing, the bat-missing was still there.",
            "There was at least enough life in the stuff to suggest more than the short line shows.",
        ])

    if not positives:
        return ""
    return choose_seeded(positives, seed, 4)



def build_starter_pressure_sentence(stats: dict, label: str, seed: int) -> str:
    h = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    traffic = h + bb
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))

    if label in GOOD_STARTER_LABELS:
        choices = [
            "When runners did get on, he usually found a way to shut the door before the inning changed shape.",
            "The few threats against him never had enough time to become the whole story.",
            "He kept the traffic from turning into anything larger than a brief problem.",
            "He had a couple spots to navigate, but they never got out of hand.",
            "Even when hitters put a little pressure on him, he was able to reset the inning.",
        ]
        if k >= 8:
            choices.extend([
                "When he needed a big pitch, he usually had enough swing-and-miss to get himself out of it.",
                "The putaway stuff kept showing up whenever an inning started to tighten.",
            ])
        elif traffic <= 4:
            choices.extend([
                "There were not many real openings for the lineup, which kept the pressure light most of the way.",
                "Hitters rarely had enough traffic together to make the outing feel unstable.",
            ])
    elif label in BAD_STARTER_LABELS:
        if outs < 3:
            choices = [
                "The trouble started before he had any chance to settle into a rhythm.",
                "There was no clean reset once the first wave of pressure hit him.",
                "The outing got loud before he had any room to steady it.",
            ]
        else:
            choices = [
                "He never found the clean inning that might have calmed the outing down.",
                "Too many hitters kept reaching, which left him with very little margin to work with.",
                "Once traffic started to build, the outing kept moving in the wrong direction.",
                "He spent too much of the night pitching with pressure already on him.",
                "Every bit of traffic felt heavy because he never really found a clean lane through the lineup.",
            ]
        if bb >= 4:
            choices.extend([
                "Falling behind in the count kept making each baserunner feel bigger.",
                "Too many hitter-friendly counts forced him to work from behind all night.",
            ])
        elif h >= 6 and bb <= 1:
            choices.extend([
                "This was less about wildness and more about hitters getting too many good looks.",
                "He was around the zone, but too many swings still turned into damage.",
            ])
        elif traffic >= 10:
            choices.extend([
                "The traffic never really stopped, which kept the outing from ever settling down.",
                "There were simply too many bodies on base for the game to calm down around him.",
            ])
    else:
        choices = [
            "There was enough traffic to keep this from feeling smooth, even if it never fully broke.",
            "He had a few jams to work through, which made the outing feel a little more stressful than the runs alone suggest.",
            "The line held together, but it did not come without a few real escape acts.",
            "There were a couple spots where the outing could have tipped, and he did enough to keep it from happening.",
            "He was not cruising, but he still made the pitches he needed often enough to keep the line usable.",
        ]
        if traffic >= 7:
            choices.extend([
                "He did well to keep the damage from getting bigger, because there were enough runners for this to turn messy.",
                "The amount of traffic made this feel more labor-intensive than the final line might suggest.",
            ])

    return choose_seeded(choices, seed, 5)



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
            "Those innings let his side play from ahead instead of spending the night scrambling.",
            "He handed the game off in good shape and let his club control the rest of the night.",
            "That start gave his side a much cleaner path through the game.",
            "He did the front-end work that let the rest of the night fall into place.",
        ]
        if win_decision:
            choices.extend([
                "He ended up with the win, and the outing put him in position for it.",
                "He was the one who set up the win decision with the way he handled the front half of the game.",
            ])
        elif team_runs <= 2:
            choices.extend([
                "He did it without much offensive cushion, which made each clean inning matter a little more.",
                "He did not have much room to work with, so the run prevention carried extra weight.",
            ])
    elif won and label in SUBPAR_STARTER_LABELS:
        choices = [
            "His lineup gave him enough room to survive the bumps in the outing.",
            "The bats covered for some of the rougher parts of the line.",
            "His side scored enough to move past the shakier parts of the start.",
            "The final result was comfortable enough, even if the outing itself was not.",
        ]
    elif (not won) and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices = [
            "He kept the game within reach, but the support around the outing never really matched it.",
            "The line was good enough to give his club a chance, even if the result went the other way.",
            "He did his part to keep his side hanging around, but the game never fully tilted back toward him.",
            "He left his club with a shot, even if the rest of the night never broke his way.",
        ]
    else:
        choices = [
            "Once the damage landed, his club spent the rest of the night trying to climb back into it.",
            "The outing put his side in chase mode from there.",
            "It left his club playing uphill for most of the night.",
            "The early damage changed the shape of the game after that.",
        ]

    if margin >= 5 and won and label in SUBPAR_STARTER_LABELS:
        choices.extend([
            "The final margin made it look cleaner than the start actually was.",
            "The score line softened the look of the outing more than the outing earned on its own.",
        ])
    elif margin == 1 and won and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices.extend([
            "In a tight game, those innings mattered more than they might look on paper.",
            "Because the margin stayed thin, the value of those innings really showed up.",
        ])

    return choose_seeded(choices, seed, 6)



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
            f"His fastball averaged {velo_text}, down {drop_text} from his previous outing, so the drop stood out.",
            f"The heater checked in at {velo_text}, which was {drop_text} lighter than last time out and worth monitoring.",
            f"One thing that did jump out was the fastball, which sat {velo_text} after being firmer in his previous start.",
        ]
        return choose_seeded(choices, seed, 7)

    bb = safe_int(p.get("stats", {}).get("baseOnBalls", 0), 0)
    hits = safe_int(p.get("stats", {}).get("hits", 0), 0)
    if label in GOOD_STARTER_LABELS:
        choices = [
            f"His fastball averaged {velo_text}, and the life on it showed up whenever he needed a finish pitch.",
            f"The fastball sat {velo_text}, giving him enough carry to stay aggressive in good counts.",
            f"He had plenty on the heater at {velo_text}, and it helped the whole outing play up.",
        ]
    elif is_bad_starter_label(label):
        if bb <= 1 and hits >= 5:
            choices = [
                f"He still averaged {velo_text} on the fastball, so this was more about the contact than a lack of arm strength.",
                f"The heater sat {velo_text}, which points more to bad locations or loud contact than soft stuff.",
            ]
        else:
            choices = [
                f"He still averaged {velo_text} on the fastball, but the outing never really came together around it.",
                f"The raw velocity was there at {velo_text}, though it did not turn into a cleaner line.",
            ]
    else:
        choices = [
            f"His fastball averaged {velo_text}, which helped him hold the outing together when things got a little messy.",
            f"The heater sat {velo_text}, and there were stretches where that gave him enough margin to steady the outing.",
        ]
    return choose_seeded(choices, seed, 8)



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
            f"He controlled a lot of the important counts, finishing with a CSW of {csw_text}.",
            f"The bat-missing and called strikes were both there, and the CSW ended up at {csw_text}.",
            f"He kept hitters reacting to him most of the night, which showed up in the {csw_text} CSW.",
        ]
        if whiffs >= 10:
            choices.extend([
                f"He generated {number_word(whiffs)} swings and misses and turned that into a CSW of {csw_text}.",
                f"With {number_word(whiffs)} whiffs, the swing-and-miss backed up the {csw_text} CSW.",
            ])
        if elite:
            choices.extend([
                f"He was ahead of hitters all night, and the {csw_text} CSW captures how much count control he had.",
                f"The {csw_text} CSW tells the story of how often he was dictating the at-bat.",
            ])
        return choose_seeded(choices, seed, 9)

    choices = [
        f"He never really controlled enough counts, and the CSW finished at {csw_text}.",
        f"There was not enough swing-and-miss or called-strike leverage here, which showed up in the {csw_text} CSW.",
        f"He had trouble getting enough free strikes, and the CSW of {csw_text} reflects that.",
    ]
    if whiffs >= 8:
        choices.extend([
            f"Even with {number_word(whiffs)} swings and misses, the overall count control still landed at only {csw_text}.",
            f"The whiffs were there in spots, but the full shape of the outing still only produced a {csw_text} CSW.",
        ])
    return choose_seeded(choices, seed, 10)



def build_starter_pitch_count_sentence(p: dict, label: str, seed: int) -> str:
    pitches = safe_int(p.get("pitch_count", 0), 0)
    strikes = safe_int(p.get("strikes", 0), 0)
    if pitches <= 0:
        return ""

    stats = p.get("stats", {})
    ip_raw = str(stats.get("inningsPitched", "0.0"))
    ip_text = format_starter_ip_for_summary(ip_raw)
    ip = safe_float(ip_raw, 0.0)
    outs = baseball_ip_to_outs(ip_raw)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    strike_pct = (strikes / pitches * 100.0) if pitches and strikes else 0.0

    choices = []

    if ip >= 6.0 and pitches <= 85:
        choices.extend([
            f"He stayed efficient enough to get through {ip_text} on just {pitches} pitches.",
            f"It was a light workload for that kind of length, with only {pitches} pitches needed.",
            f"He got real length without running the pitch count up, finishing at just {pitches} pitches.",
        ])

    if ip < 5.0 and pitches >= 85:
        choices.extend([
            f"The pitch count got heavy early, and {pitches} pitches disappeared in a hurry.",
            f"He burned through {pitches} pitches without getting the length his club needed.",
            f"The workload climbed too fast, which is why the outing stalled at {pitches} pitches.",
        ])

    if outs < 9 and pitches >= 45:
        choices.extend([
            f"It took {pitches} pitches to record only {number_word(outs)} outs, so the outing never really found breathing room.",
            f"He used up {pitches} pitches in a short look, which tells you how hard each inning was.",
        ])

    if strikes > 0 and strike_pct >= 69.0 and bb <= 1 and (ip >= 5.0 or label in GOOD_STARTER_LABELS | {"SOLID"}):
        choices.extend([
            f"He was in the zone all night, landing {strikes} of {pitches} pitches for strikes.",
            f"He kept the count moving by throwing {strikes} of {pitches} pitches for strikes.",
            f"The strike rate was a real plus, with {strikes} of {pitches} pitches finding the zone.",
        ])

    if strikes > 0 and strike_pct <= 58.0 and (bb >= 3 or label in {"NO_COMMAND", "ROUGH", "HIT_HARD"}):
        choices.extend([
            f"He only landed {strikes} of {pitches} pitches for strikes, and too many counts drifted the wrong way.",
            f"Just {strikes} of {pitches} pitches went for strikes, which kept putting him in bad leverage.",
            f"He did not throw enough quality strikes, and the {strikes}-for-{pitches} split shows it.",
        ])

    if strikes > 0 and bb >= 4 and pitches >= 70:
        choices.extend([
            f"He still threw {strikes} of {pitches} pitches for strikes, but the misses that mattered stretched too many innings out.",
            f"Even with {strikes} strikes, the pitch count kept climbing because the misses came in the wrong spots.",
        ])

    if strikes > 0 and hits >= 7 and bb <= 1 and er >= 3:
        choices.extend([
            f"He was around the zone with {strikes} strikes, but too much of that contact was loud.",
            f"He threw plenty of strikes, but hitters were able to do too much with them.",
        ])

    if not choices:
        return ""
    return choose_seeded(choices, seed, 11)



def build_starter_subject_line(p: dict, label: str, game_context: dict, seed: int) -> str:
    stats = p.get("stats", {})
    name = p.get("name", "This starter")
    k = safe_int(stats.get("strikeOuts", 0), 0)
    er = safe_int(stats.get("earnedRuns", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))
    ip_text = format_starter_ip_for_summary(str(stats.get("inningsPitched", "0.0")))
    team_runs = game_context.get("home_score", 0) if p.get("side") == "home" else game_context.get("away_score", 0)
    opp_runs = game_context.get("away_score", 0) if p.get("side") == "home" else game_context.get("home_score", 0)

    if outs < 3:
        choices = [
            f"⚠️ {name} is knocked out before he can settle in",
            f"⚠️ {name} exits almost as soon as the game starts",
            f"⚠️ {name} cannot make it out of the opening frame",
        ]
    elif label in {"GEM", "DOMINANT"}:
        choices = [
            f"🔥 {name} takes over from the jump",
            f"🔥 {name} controls the game for {ip_text}",
            f"🔥 {name} overpowers hitters in a front-line outing",
        ]
        if k >= 8:
            choices.extend([
                f"🔥 {name} works deep while piling up {number_word(k)} strikeouts",
                f"🔥 {name} puts hitters away all night",
            ])
    elif label == "STRIKEOUT":
        choices = [
            f"🎯 {name} misses bats all night in a high-whiff start",
            f"🎯 {name} punches out {number_word(k)} with real putaway stuff",
            f"🎯 {name} leans on swing-and-miss to carry the outing",
        ]
        if bb >= 3:
            choices.append(f"🎯 {name} piles up strikeouts despite the extra traffic")
    elif label == "QUALITY":
        choices = [
            f"✅ {name} turns in a quality start for his club",
            f"✅ {name} gives his club six strong innings",
            f"✅ {name} keeps the game in good shape all night",
        ]
        if er == 0:
            choices.append(f"✅ {name} keeps the board clean through {ip_text}")
    elif label == "SHARP":
        choices = [
            f"✅ {name} turns in a crisp outing",
            f"✅ {name} gives his club a clean, steady look",
            f"✅ {name} keeps things under control from the mound",
        ]
    elif label == "SOLID":
        choices = [
            f"✅ {name} gives his club a steady start",
            f"✅ {name} does enough to keep his side in it",
            f"✅ {name} turns in a useful outing",
        ]
    elif label == "HIT_HARD":
        choices = [
            f"💥 {name} gets hit hard despite some swing-and-miss",
            f"💥 {name} pays for too many hittable pitches",
            f"💥 {name} cannot avoid the loud contact",
        ]
        if k >= 7:
            choices.append(f"💥 {name} misses bats but still gets punished on contact")
    elif label == "NO_COMMAND":
        choices = [
            f"🧭 {name} never really gets the count working for him",
            f"🧭 {name} fights the zone all night",
            f"🧭 {name} cannot quite harness the outing",
        ]
    elif label == "UNEVEN":
        choices = [
            f"📉 {name} works through traffic in an uneven outing",
            f"📉 {name} never quite settles into a clean rhythm",
            f"📉 {name} labors through a start with some stress on it",
        ]
    else:
        choices = [
            f"⚠️ {name} cannot keep the outing from getting away",
            f"⚠️ {name} runs into trouble and does not recover",
            f"⚠️ {name} has a rough night on the mound",
        ]
        if hits >= 7 and bb <= 1:
            choices.append(f"⚠️ {name} gets tagged even without many walks")
        elif bb >= 4:
            choices.append(f"⚠️ {name} falls behind too often to settle in")
        elif er >= 4:
            choices.append(f"⚠️ {name} cannot stop the damage from building")

    if team_runs > opp_runs and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices.append(f"🏁 {name} helps set up a winning night for his club")
    elif team_runs < opp_runs and label in BAD_STARTER_LABELS | {"UNEVEN"}:
        choices.append(f"📉 {name} leaves his club trying to chase the game")

    subject = choose_seeded(choices, seed, 12).strip().rstrip(".!?:;,-")
    return subject.replace("...", "").strip()



def build_starter_summary(p: dict, label: str, game_context: dict, recent_appearances=None) -> str:
    stats = p["stats"]
    seed = build_starter_summary_seed(p["name"], stats, game_context)

    overview = build_starter_overview(p["name"], label, stats, seed)
    stat_sentence_no_ip = build_starter_stat_sentence(stats, seed, include_ip=False)
    stat_sentence_with_ip = build_starter_stat_sentence(stats, seed, include_ip=True)
    pressure_sentence = build_starter_pressure_sentence(stats, label, seed)
    team_sentence = build_starter_team_context(p, stats, label, game_context, seed)
    velocity_sentence = build_starter_velocity_sentence(p, label, seed, recent_appearances=recent_appearances)
    csw_sentence = build_starter_csw_sentence(p, label, seed)
    pitch_sentence = build_starter_pitch_count_sentence(p, label, seed)
    positive_sentence = build_starter_positive_sentence(stats, label, seed)

    if label in {"DOMINANT", "GEM"}:
        order_options = [
            [overview, csw_sentence, stat_sentence_no_ip, pitch_sentence, pressure_sentence, team_sentence, velocity_sentence],
            [overview, stat_sentence_no_ip, pressure_sentence, pitch_sentence, csw_sentence, team_sentence, velocity_sentence],
            [overview, pitch_sentence, stat_sentence_no_ip, csw_sentence, team_sentence, velocity_sentence, pressure_sentence],
        ]
        subject_probably_has_length = True
    elif label == "STRIKEOUT":
        order_options = [
            [overview, csw_sentence, stat_sentence_with_ip, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, stat_sentence_with_ip, csw_sentence, pitch_sentence, positive_sentence, team_sentence, velocity_sentence],
            [overview, pressure_sentence, stat_sentence_with_ip, csw_sentence, team_sentence, velocity_sentence],
        ]
        subject_probably_has_length = False
    elif is_bad_starter_label(label):
        order_options = [
            [overview, positive_sentence, stat_sentence_no_ip, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, stat_sentence_no_ip, pressure_sentence, positive_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, pitch_sentence, stat_sentence_no_ip, pressure_sentence, positive_sentence, velocity_sentence, team_sentence],
        ]
        subject_probably_has_length = False
    elif label == "UNEVEN":
        order_options = [
            [overview, pressure_sentence, stat_sentence_no_ip, team_sentence, pitch_sentence, velocity_sentence, csw_sentence],
            [overview, stat_sentence_no_ip, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, pitch_sentence, pressure_sentence, stat_sentence_no_ip, team_sentence, velocity_sentence],
        ]
        subject_probably_has_length = False
    else:
        order_options = [
            [overview, stat_sentence_no_ip, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence, csw_sentence],
            [overview, pitch_sentence, stat_sentence_no_ip, pressure_sentence, team_sentence, velocity_sentence],
            [overview, pressure_sentence, stat_sentence_no_ip, team_sentence, pitch_sentence, velocity_sentence],
        ]
        subject_probably_has_length = True

    ordered = [s for s in order_options[seed % len(order_options)] if s]
    final_sentences = []
    ip_reference_used = subject_probably_has_length

    for sentence in ordered:
        if not sentence or sentence in final_sentences:
            continue
        if ip_reference_used and sentence_has_ip_reference(sentence):
            continue
        final_sentences.append(sentence)
        if sentence_has_ip_reference(sentence):
            ip_reference_used = True
        if len(final_sentences) >= 4:
            break

    if len(final_sentences) < 3:
        fillers = [stat_sentence_no_ip, stat_sentence_with_ip, pressure_sentence, csw_sentence, pitch_sentence, team_sentence, velocity_sentence, positive_sentence]
        for sentence in fillers:
            if not sentence or sentence in final_sentences:
                continue
            if ip_reference_used and sentence_has_ip_reference(sentence):
                continue
            final_sentences.append(sentence)
            if sentence_has_ip_reference(sentence):
                ip_reference_used = True
            if len(final_sentences) >= 4:
                break

    return " ".join(final_sentences[:4])


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


async def post_card(channel, p: dict, game_context: dict, score_value: str, recent_appearances=None):
    stats = p["stats"]
    label = classify_starter(p)
    seed = build_starter_summary_seed(p["name"], stats, game_context)

    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])
    embed.add_field(name="", value=f"**{build_starter_subject_line(p, label, game_context, seed)}**", inline=False)
    embed.add_field(name="Summary", value=build_starter_summary(p, label, game_context, recent_appearances=recent_appearances), inline=False)
    embed.add_field(name="Game Line", value=format_starter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_starter_season_line(p.get("season_stats", {})), inline=False)
    embed.add_field(name="⚾ Score", value=score_value, inline=False)
    await channel.send(embed=embed)

async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    state = load_state()
    posted = set(state.get("posted", []))

    if RESET_STARTER_STATE:
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

                    recent_appearances = get_recent_appearances(pid, game_date_et, limit=3, max_days=45)
                    log(f"Posting {p['name']} | {p['team']} | {score_value} | score={score}")
                    await post_card(channel, p, game_context, score_value, recent_appearances=recent_appearances)
                    posted.add(key)
                    posted_this_game += 1

            state["posted"] = list(posted)
            save_state(state)

        except Exception as e:
            log(f"Loop error: {e}")

        await asyncio.sleep(POLL_MINUTES * 60)


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

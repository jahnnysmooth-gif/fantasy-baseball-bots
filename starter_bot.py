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
        if normalize_lookup_name(key).split()[-1:] != [last_name]:
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
        team_matches = [entry for entry in matches if normalize_team_abbr(entry.get("team")) == normalized_team]
        if len(team_matches) == 1:
            return team_matches[0]
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


GOOD_STARTER_LABELS = {"GEM", "DOMINANT", "QUALITY", "STRIKEOUT", "SHARP"}
GOOD_STARTER_LABELS = {"GEM", "DOMINANT", "QUALITY", "STRIKEOUT", "SHARP", "SOLID"}
BAD_STARTER_LABELS = {"ROUGH", "NO_COMMAND", "HIT_HARD", "SHORT"}
SUBPAR_STARTER_LABELS = BAD_STARTER_LABELS | {"UNEVEN"}
POSITIVE_STARTER_LABELS = GOOD_STARTER_LABELS


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


def build_starter_overview(name: str, label: str, stats: dict, seed: int) -> str:
    ip = str(stats.get("inningsPitched", "0.0"))
    ip_text = format_starter_ip_for_summary(ip)
    outs = baseball_ip_to_outs(ip)

    if outs < 3:
        variants = {
            "ROUGH": [
                f"{name} was gone before he could get through the first inning, and the damage started right away.",
                f"{name} only recorded {ip_text}, and this one got sideways before he had time to settle in.",
            ],
            "HIT_HARD": [
                f"{name} only got {ip_text}, with hitters doing damage almost as soon as the game began.",
                f"{name} did not make it out of the first, and the contact against him was loud from the start.",
            ],
            "NO_COMMAND": [
                f"{name} only lasted {ip_text}, and too many missed spots put him in trouble immediately.",
                f"{name} was out after {ip_text}, with the count getting away from him before he could regain control.",
            ],
            "SHORT": [
                f"{name} only recorded {ip_text}, so this never had the feel of a normal start.",
                f"{name} was lifted after {ip_text}, forcing the bullpen into the game almost right away.",
            ],
        }
        choices = variants.get(label, variants["SHORT"])
        return choices[seed % len(choices)]

    variants = {
        "GEM": [
            f"{name} turned in one of the cleanest starts of the day, covering {ip_text} without allowing an earned run.",
            f"{name} set the tone early and stayed in command through {ip_text} of scoreless work.",
        ],
        "DOMINANT": [
            f"{name} took over this start early and stayed on top of it for {ip_text}.",
            f"{name} looked overpowering through {ip_text}, giving the lineup very little air all night.",
        ],
        "QUALITY": [
            f"{name} gave his club a strong {ip_text} and kept the damage to a minimum.",
            f"{name} turned in a quality {ip_text} and rarely let the game get loose on him.",
        ],
        "STRIKEOUT": [
            f"{name} missed bats all through {ip_text}, even if the outing was not completely clean.",
            f"{name} leaned on putaway stuff over {ip_text} and kept finding strikeouts when he needed them.",
        ],
        "SHARP": [
            f"{name} looked sharp through {ip_text} and kept the innings moving in his favor.",
            f"{name} gave a crisp {ip_text} and did not let many innings get noisy.",
        ],
        "SOLID": [
            f"{name} gave his side a useful {ip_text} and kept the game from drifting too far while he was in it.",
            f"{name} turned in a steady {ip_text} and kept things playable for most of the night.",
        ],
        "UNEVEN": [
            f"{name} got through {ip_text}, though the outing had traffic on it from start to finish.",
            f"{name} covered {ip_text}, but there was pressure on the line for much of the night.",
        ],
        "SHORT": [
            f"{name} did not last as long as his team needed, getting through only {ip_text}.",
            f"{name} was out earlier than expected after {ip_text}, and the outing never found much shape.",
        ],
        "ROUGH": [
            f"{name} had a rough night and could not stop the game from leaning the wrong way over {ip_text}.",
            f"{name} never got fully settled over {ip_text}, and the outing kept getting heavier on him.",
        ],
        "NO_COMMAND": [
            f"{name} spent too much of {ip_text} fighting the zone, which kept each inning from breathing.",
            f"{name} did not find enough strikes over {ip_text}, and the extra traffic kept pushing the line around.",
        ],
        "HIT_HARD": [
            f"{name} got hit hard over {ip_text}, and there were too many pitches left in damage spots.",
            f"{name} did not have much margin over {ip_text}, and too much hittable stuff got punished.",
        ],
    }
    choices = variants.get(label, variants["SOLID"])
    return choices[seed % len(choices)]


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
        f"By the time he was done, he had given up {hit_text} and {er_text}, with {bb_text} and {k_text} over {ip_text}.",
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


def build_starter_pressure_sentence(stats: dict, label: str, seed: int) -> str:
    h = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    traffic = h + bb
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))

    if label in GOOD_STARTER_LABELS:
        choices = [
            "When runners did reach, he got the inning back under control before it snowballed.",
            "The few threats against him never had enough time to become the whole game.",
            "He kept the traffic from turning into the kind of inning that flips a start.",
        ]
        if k >= 8:
            choices.append("When the lineup made a push, he still had enough putaway stuff to punch his way out of it.")
        elif traffic <= 4:
            choices.append("There were not many real openings for the lineup, which kept the pressure light most of the way.")
    elif label in BAD_STARTER_LABELS:
        if outs < 3:
            choices = [
                "The trouble was there before he had any chance to settle into the outing.",
                "There was no reset button once the first wave of damage started coming.",
            ]
        else:
            choices = [
                "He never found the clean inning that might have slowed the game down.",
                "Too many hitters kept reaching, which left him with almost no room to work.",
                "Once traffic started to stack up, the outing kept moving in the wrong direction.",
            ]
        if bb >= 4:
            choices.append("He kept pitching from behind, and that made every baserunner feel bigger.")
        elif h >= 6 and bb <= 1:
            choices.append("This was more about too many hittable pitches than scattered command.")
        elif traffic >= 10:
            choices.append("The traffic never really stopped, and that kept the outing from ever calming down.")
    else:
        choices = [
            "There was enough traffic to keep the start from feeling smooth, even if it never fully broke.",
            "He had to work through a few jams, which gave the line more stress than the runs alone suggest.",
            "The outing held together, but there were still a couple innings where he had to earn the escape.",
        ]
        if traffic >= 7:
            choices.append("He did well to keep the damage from getting bigger, because there were enough runners for this to get messy.")
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
            "Those innings let his side play from in front instead of scrambling to catch up.",
            "He handed the rest of the night over in good shape and let his club dictate the pace.",
            "That work gave his side a cleaner path through the rest of the game.",
        ]
        if win_decision:
            choices.append("He wound up with the win, and the outing put him in line for it from the start.")
        elif team_runs <= 2:
            choices.append("He did it without much offensive cushion, which made every clean inning carry more weight.")
    elif won and label in SUBPAR_STARTER_LABELS:
        choices = [
            "His offense gave him enough breathing room to survive the line, even if the start itself stayed shaky.",
            "The bats covered for it, though the outing itself was bumpier than the final margin suggests.",
            "His side scored enough to move past it, even if the start demanded more cleanup than anyone wanted.",
        ]
    elif (not won) and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices = [
            "He kept the game within reach, but the support around the outing never quite matched it.",
            "The line was good enough to keep his club hanging around, even if the result went the other way.",
            "He gave his side a chance, even if the rest of the game never quite tilted back toward him.",
        ]
    else:
        choices = [
            "Once the damage landed, his club spent the rest of the night trying to claw back.",
            "The early hole changed the shape of the game from there.",
            "It left his side playing uphill once the outing slipped.",
        ]

    if margin >= 5 and won and label in SUBPAR_STARTER_LABELS:
        choices.append("The final margin looked comfortable, but the start itself was shakier than that score suggests.")
    elif margin == 1 and won and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices.append("In a tight game, those innings mattered a lot more than they might look on paper.")
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
            f"He stayed efficient enough to cover {ip_text} on just {pitches} pitches.",
            f"It was a clean workload, and he got through {ip_text} in only {pitches} pitches.",
        ])

    if ip < 5.0 and pitches >= 85:
        choices.extend([
            f"The pitch count got heavy early, and {pitches} pitches were all he could squeeze into {ip_text}.",
            f"He needed {pitches} pitches just to get through {ip_text}, which helps explain why this start ended so soon.",
        ])

    if outs < 9 and pitches >= 45:
        choices.extend([
            f"He burned through {pitches} pitches in a short outing, leaving almost no room for the game to settle down.",
            f"It took {pitches} pitches to record just {number_word(outs)} outs, so the outing never gave him much breathing room.",
        ])

    if strikes > 0 and strike_pct >= 69.0 and bb <= 1 and (ip >= 5.0 or label in GOOD_STARTER_LABELS | {"SOLID"}):
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
            f"He still threw {strikes} of {pitches} pitches for strikes, but the misses that mattered stretched too many innings out."
        )

    if strikes > 0 and hits >= 7 and bb <= 1 and er >= 3:
        choices.append(f"He threw {strikes} of {pitches} pitches for strikes, but too much of that contact was loud.")

    if not choices:
        return ""
    return choices[(seed // 19) % len(choices)]


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
            f"⚠️ {name} is knocked out in the opening inning",
            f"⚠️ {name} exits before he can settle in",
        ]
    elif label in {"GEM", "DOMINANT"}:
        choices = [
            f"🔥 {name} controls the game from the jump",
            f"🔥 {name} dominates over {ip_text}",
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
        ]
        if bb >= 3:
            choices.append(f"🎯 {name} piles up strikeouts despite the extra traffic")
    elif label in {"QUALITY", "SHARP", "SOLID"}:
        choices = [
            f"✅ {name} turns in a steady {ip_text}",
            f"✅ {name} gives his club a strong {ip_text}",
        ]
        if k >= 7:
            choices.append(f"✅ {name} pairs length with {number_word(k)} strikeouts")
        elif er == 0:
            choices.append(f"✅ {name} keeps the board clean through {ip_text}")
    elif label == "HIT_HARD":
        choices = [
            f"💥 {name} gets hit hard despite some swing-and-miss",
            f"💥 {name} pays for too many hittable pitches",
        ]
        if k >= 7:
            choices.append(f"💥 {name} misses bats but gets punished on contact")
    elif label == "NO_COMMAND":
        choices = [
            f"🎯 {name} never gets the count working for him",
            f"🎯 {name} fights the zone all night",
        ]
    else:
        choices = [
            f"⚠️ {name} cannot keep the outing from getting away",
            f"⚠️ {name} runs into trouble and does not recover",
        ]
        if hits >= 7 and bb <= 1:
            choices.append(f"⚠️ {name} gets tagged even without many walks")
        elif bb >= 4:
            choices.append(f"⚠️ {name} falls behind too often to settle in")
        elif er >= 4:
            choices.append(f"⚠️ {name} cannot stop the damage from building")

    if team_runs > opp_runs and label in GOOD_STARTER_LABELS | {"SOLID"}:
        choices.append(f"🏁 {name} sets up a winning night for his club")
    elif team_runs < opp_runs and label in BAD_STARTER_LABELS | {"UNEVEN"}:
        choices.append(f"📉 {name} leaves his club chasing the game")

    subject = choices[seed % len(choices)].strip().rstrip(".!?:;,-")
    return subject.replace("...", "").strip()


def build_starter_summary(p: dict, label: str, game_context: dict, recent_appearances=None) -> str:
    stats = p["stats"]
    seed = build_starter_summary_seed(p["name"], stats, game_context)
    overview = build_starter_overview(p["name"], label, stats, seed)
    stat_sentence = build_starter_stat_sentence(stats, seed)
    pressure_sentence = build_starter_pressure_sentence(stats, label, seed)
    team_sentence = build_starter_team_context(p, stats, label, game_context, seed)
    velocity_sentence = build_starter_velocity_sentence(p, label, seed, recent_appearances=recent_appearances)
    csw_sentence = build_starter_csw_sentence(p, label, seed)
    pitch_sentence = build_starter_pitch_count_sentence(p, label, seed)
    positive_sentence = build_starter_positive_sentence(stats, label, seed)

    if is_bad_starter_label(label):
        order_options = [
            [overview, stat_sentence, pressure_sentence, positive_sentence, csw_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, positive_sentence, stat_sentence, pitch_sentence, pressure_sentence, csw_sentence, team_sentence, velocity_sentence],
            [overview, stat_sentence, csw_sentence, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence],
        ]
    else:
        order_options = [
            [overview, stat_sentence, pressure_sentence, csw_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, csw_sentence, stat_sentence, pitch_sentence, pressure_sentence, team_sentence, velocity_sentence],
            [overview, pitch_sentence, stat_sentence, pressure_sentence, csw_sentence, team_sentence, velocity_sentence],
        ]

    ordered = [s for s in order_options[seed % len(order_options)] if s]
    final_sentences = []
    for sentence in ordered:
        if sentence and sentence not in final_sentences:
            final_sentences.append(sentence)
        if len(final_sentences) >= 4:
            break

    if len(final_sentences) < 3:
        fillers = [stat_sentence, pressure_sentence, csw_sentence, pitch_sentence, team_sentence, velocity_sentence, positive_sentence]
        for sentence in fillers:
            if sentence and sentence not in final_sentences:
                final_sentences.append(sentence)
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
    label = classify_starter(stats)
    seed = build_starter_summary_seed(p["name"], stats, game_context)

    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(p["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, p["name"], p["team"])
    embed.add_field(name="", value=f"**{build_starter_subject_line(p, label, game_context, seed)}**", inline=False)
    embed.add_field(name="", value=f"**{starter_impact_tag(label)}**", inline=False)
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

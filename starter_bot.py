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




def build_starter_game_flow(feed: dict, pitcher_id: int, side: str):
    default = {
        "innings_sequence": [],
        "runs_by_inning": {},
        "scoreless_to_start": 0,
        "only_damage_in_one_inning": False,
        "biggest_inning_runs": 0,
        "scored_in_first": False,
        "settled_after_rough": False,
        "late_damage": False,
        "team_runs_while_in": 0,
        "opp_runs_while_in": 0,
        "entry_margin": 0,
        "exit_margin": 0,
        "first_inning": None,
        "last_inning": None,
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

    if side == "home":
        entry_team = prev_home
        entry_opp = prev_away
    else:
        entry_team = prev_away
        entry_opp = prev_home

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

    if side == "home":
        exit_team = prev_home
        exit_opp = prev_away
    else:
        exit_team = prev_away
        exit_opp = prev_home

    inning_runs = [runs_by_inning.get(inning, 0) for inning in innings_sequence]
    scoreless_to_start = 0
    for runs in inning_runs:
        if runs == 0:
            scoreless_to_start += 1
        else:
            break

    run_innings = [runs for runs in inning_runs if runs > 0]
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
    k = safe_int(stats.get("strikeOuts", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)

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
            f"{name} owned the game from the start and never let the lineup settle in.",
            f"{name} gave his club a true stopper outing, controlling the pace all night.",
            f"{name} was on top of this one from the jump and gave hitters almost nothing to work with.",
            f"{name} put the game on his terms early and kept it there for {ip_text}.",
        ],
        "DOMINANT": [
            f"{name} looked overpowering and spent most of the night dictating every at-bat.",
            f"{name} had real finish to his stuff and was clearly the one in control.",
            f"{name} came with swing-and-miss from the outset and never gave the lineup much comfort.",
            f"{name} pitched like the best version of himself and kept the game squarely on his terms.",
        ],
        "QUALITY": [
            f"{name} gave his club a sturdy {ip_text} and limited the damage well.",
            f"{name} turned in the kind of stable start that keeps a team in good shape.",
            f"{name} was not perfect every inning, but the overall work gave his side exactly what it needed.",
            f"{name} put together a strong, useful start and rarely let the game get loose on him.",
        ],
        "STRIKEOUT": [
            f"{name} brought real bat-missing to the mound, even if the outing was not spotless.",
            f"{name} leaned on putaway stuff and kept finding strikeouts when innings got tense.",
            f"{name} had enough swing-and-miss to overpower long stretches of this lineup.",
            f"{name} punched his way through the toughest spots and made the strikeouts the story of the night.",
        ],
        "SHARP": [
            f"{name} looked sharp from the outset and kept most of the night under control.",
            f"{name} gave a crisp outing and did not offer many clean openings to the lineup.",
            f"{name} was steady, efficient, and rarely looked rushed over the course of the start.",
            f"{name} kept the game in order and never let the opposition build much rhythm.",
        ],
        "SOLID": [
            f"{name} gave his side a useful start and kept the game on steady footing while he was in it.",
            f"{name} turned in a workmanlike outing and kept things intact long enough to matter.",
            f"{name} was not overpowering, but he gave his club usable length and a playable game.",
            f"{name} did enough to bridge the game into the middle innings without letting it drift.",
        ],
        "UNEVEN": [
            f"{name} got through {ip_text}, though there was traffic on the bases for much of the night.",
            f"{name} covered enough ground, but the outing never really settled into an easy rhythm.",
            f"{name} had to grind through this one, even if the line never fully collapsed.",
            f"{name} gave his club innings, but there was pressure on him from start to finish.",
        ],
        "SHORT": [
            f"{name} did not last as long as his team needed, getting through only {ip_text}.",
            f"{name} was out earlier than expected, and the outing never found much rhythm.",
            f"{name} came up short on length, which changed the shape of the game pretty quickly.",
            f"{name} could not give his side enough innings, even if the damage stayed somewhat limited.",
        ],
        "ROUGH": [
            f"{name} had a rough night and never really got the start under control.",
            f"{name} spent too much of the outing reacting instead of dictating.",
            f"{name} was chasing the game more than leading it, and the final line reflected that.",
            f"{name} ran into trouble early and never found the reset he needed.",
        ],
        "NO_COMMAND": [
            f"{name} fought the zone most of the night, which kept every inning from calming down.",
            f"{name} did not find enough strikes, and the extra traffic kept pushing the line around.",
            f"{name} was working from behind too often and never had much margin because of it.",
            f"{name} never consistently got ahead in counts, and that made the whole start feel uphill.",
        ],
        "HIT_HARD": [
            f"{name} was around the zone, but too much of the contact against him came with authority.",
            f"{name} paid for too many mistakes in hittable spots, and the lineup did real damage with them.",
            f"{name} did not have much margin, and too many pitches left in bad places got punished.",
            f"{name} got hit harder than the line could absorb, even when he was throwing strikes.",
        ],
    }

    if label == "DOMINANT" and k >= 10:
        variants["DOMINANT"].append(f"{name} paired premium stuff with punchout volume and overpowered hitters for most of {ip_text}.")
    if label == "NO_COMMAND" and bb >= 5:
        variants["NO_COMMAND"].append(f"{name} never got the counts he wanted, and the walks kept every inning from breathing.")
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



def build_starter_pressure_sentence(stats: dict, label: str, seed: int) -> str:
    h = safe_int(stats.get("hits", 0), 0)
    bb = safe_int(stats.get("baseOnBalls", 0), 0)
    k = safe_int(stats.get("strikeOuts", 0), 0)
    traffic = h + bb
    outs = baseball_ip_to_outs(str(stats.get("inningsPitched", "0.0")))

    if label in GOOD_STARTER_LABELS:
        choices = [
            "When traffic showed up, he usually had an answer before the inning could flip on him.",
            "The few threats against him never had enough time to become the whole story.",
            "He kept the bigger moments under control and never let the baserunners change the feel of the game.",
            "Even when hitters reached, he usually got the next pitch or next out he needed.",
            "There were not many clean looks for the lineup, which kept the pressure light for most of the night.",
            "Most of the traffic died quickly, which kept the game from getting dragged into long innings.",
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
                "Once traffic started to stack up, the outing kept moving in the wrong direction.",
                "He spent too much of the night pitching under stress, and it finally caught up with him.",
                "There were too many leverage pitches for this to ever feel stable.",
            ]
        if bb >= 4:
            choices.append("He kept falling behind in counts, and that made every baserunner feel bigger.")
        elif h >= 6 and bb <= 1:
            choices.append("This was more about too much hittable contact than scattered command.")
        elif traffic >= 10:
            choices.append("The traffic never really stopped, and that kept the outing from ever calming down.")
    else:
        choices = [
            "There was enough traffic to keep the start from feeling smooth, even if it never fully broke.",
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
    if p.get("side") == "away":
        team_runs = away_score
        opp_runs = home_score
    else:
        team_runs = home_score
        opp_runs = away_score

    win_decision = safe_int(stats.get("wins", 0), 0) > 0
    won = team_runs > opp_runs
    margin = abs(team_runs - opp_runs)

    if won and label in POSITIVE_STARTER_LABELS:
        choices = [
            "Those innings let his side play from in front instead of scrambling to catch up.",
            "He handed the rest of the night over in good shape and let his club dictate the pace.",
            "That work gave his side a cleaner path through the rest of the game.",
            "He did his part to hand the bullpen a much more manageable finish.",
            "It let his side keep the game on its own terms for most of the night.",
        ]
        if win_decision:
            choices.append("He wound up with the win, and the outing put him in line for it from the start.")
        elif team_runs <= 2:
            choices.append("He did it without much offensive cushion, which made the cleaner innings matter even more.")
        elif margin >= 4:
            choices.append("Once his club gave him room to work, he mostly kept the game pointed in the right direction.")
    elif won and label in SUBPAR_STARTER_LABELS:
        choices = [
            "His offense gave him enough breathing room to survive the line, even if the start itself stayed shaky.",
            "The bats covered for it, though the outing itself was bumpier than the final margin suggests.",
            "His side scored enough to move past it, even if the start demanded more cleanup than anyone wanted.",
            "The final result worked out, but the outing itself was rougher than the scoreboard alone implies.",
        ]
    elif (not won) and label in POSITIVE_STARTER_LABELS:
        choices = [
            "He kept the game close, but the support around the outing never quite matched it.",
            "The line was good enough to keep his club hanging around, even if the result went the other way.",
            "He gave his side a shot, even if the rest of the game never quite tilted back toward him.",
            "He did enough to make the game winnable, even if the ending did not break his way.",
            "It was the kind of start that usually keeps a team alive deep into the game.",
        ]
    else:
        choices = [
            "Once the damage landed, his club spent the rest of the night trying to claw back.",
            "The early hole changed the shape of the game from there.",
            "It left his side playing uphill once the outing slipped.",
            "From there, his club was chasing the game more than controlling it.",
        ]

    if margin >= 5 and won and label in SUBPAR_STARTER_LABELS:
        choices.append("The final margin looked comfortable, but the start itself was shakier than that score suggests.")
    elif margin == 1 and won and label in POSITIVE_STARTER_LABELS:
        choices.append("In a tight game, those innings carried more weight than they might look on paper.")
    return choices[(seed // 7) % len(choices)]

def build_starter_game_flow_sentence(p: dict, label: str, seed: int) -> str:
    flow = p.get("game_flow") or {}
    scoreless_to_start = safe_int(flow.get("scoreless_to_start", 0), 0)
    opp_runs_while_in = safe_int(flow.get("opp_runs_while_in", 0), 0)
    team_runs_while_in = safe_int(flow.get("team_runs_while_in", 0), 0)
    exit_margin = safe_int(flow.get("exit_margin", 0), 0)
    biggest_inning_runs = safe_int(flow.get("biggest_inning_runs", 0), 0)
    first_inning = safe_int(flow.get("first_inning", 0), 0)
    last_inning = safe_int(flow.get("last_inning", 0), 0)

    if flow.get("settled_after_rough"):
        choices = [
            "After a rough first inning, he settled in and gave his club several steadier frames after that.",
            "He had to battle through some early trouble, but the outing looked much cleaner once he found a rhythm.",
            "The start wobbled early before he regrouped and settled into the game.",
            "He did not have much in the opening inning, but he recovered well enough to keep the outing together.",
        ]
        return choices[(seed // 23) % len(choices)]

    if flow.get("only_damage_in_one_inning") and opp_runs_while_in > 0:
        choices = [
            "Almost all of the damage came in one inning, and the rest of the outing was far more controlled.",
            "One rough stretch did most of the damage against him, but he was much steadier outside of it.",
            "The line was hurt by one bad inning more than anything else.",
            "Outside of one inning that got away from him, he mostly kept the game under control.",
        ]
        return choices[(seed // 23) % len(choices)]

    if flow.get("late_damage") and label in POSITIVE_STARTER_LABELS:
        choices = [
            "He cruised through most of the outing before the lineup finally got to him late.",
            "The game was mostly on his terms until some late damage changed the line a bit.",
            "He stayed in control for several innings before things got tougher near the end.",
            "Most of the trouble showed up late after he had been rolling for much of the night.",
        ]
        return choices[(seed // 23) % len(choices)]

    if scoreless_to_start >= 4:
        choices = [
            f"He opened with {number_word(scoreless_to_start)} straight scoreless innings and never let the lineup settle in early.",
            f"He came out sharp and stacked {number_word(scoreless_to_start)} quiet innings before the opposition got anything going.",
            f"He controlled the early part of the game, opening with {number_word(scoreless_to_start)} scoreless frames.",
            f"The lineup had very little going early, as he rolled through {number_word(scoreless_to_start)} scoreless innings to start the night.",
        ]
        return choices[(seed // 23) % len(choices)]

    if team_runs_while_in >= 4 and exit_margin > 0 and label in POSITIVE_STARTER_LABELS:
        choices = [
            "His offense gave him a lead to work with, and he mostly kept the game moving in the right direction.",
            "He pitched with a cushion for much of the night and did a good job protecting it.",
            "With run support behind him, he was able to stay aggressive and attack the zone.",
            "His side gave him enough breathing room to pitch with confidence for most of the outing.",
        ]
        return choices[(seed // 23) % len(choices)]

    if exit_margin == 0 and label in POSITIVE_STARTER_LABELS:
        choices = [
            "He handed the game to the bullpen with the score still tied.",
            "When he left, the game was still hanging in the balance.",
            "He kept things close enough to give the bullpen a real chance to decide it.",
            "He left with the result still very much up for grabs.",
        ]
        return choices[(seed // 23) % len(choices)]

    if exit_margin > 0 and label in POSITIVE_STARTER_LABELS:
        choices = [
            "He handed the bullpen a lead and put his club in a good spot to close it out.",
            "By the time he left, his side still had control of the game.",
            "He left with his team in front, which is exactly what you want from a starter.",
            "He did his part and handed things over with the lead intact.",
        ]
        return choices[(seed // 23) % len(choices)]

    if biggest_inning_runs >= 2 and label in BAD_STARTER_LABELS | {"UNEVEN"}:
        choices = [
            "One crooked inning changed the feel of the whole outing and left him chasing the line after that.",
            "A multi-run inning did most of the damage, and he never really got fully back on top of it.",
            "The start turned once one inning got away from him.",
            "He was hurt most by one inning that snowballed in a hurry.",
        ]
        return choices[(seed // 23) % len(choices)]

    if first_inning and last_inning and last_inning - first_inning >= 4 and label in {"SOLID", "QUALITY"}:
        choices = [
            "He kept the outing moving along long enough to spare the bullpen an early scramble.",
            "He bridged the game deep enough to keep extra stress off the rest of the staff.",
            "He covered enough of the game to leave the bullpen a manageable finish.",
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
            f"He got through his outing on a low pitch count, which helped him work deep into the game.",
        ])

    if ip < 5.0 and pitches >= 85:
        choices.extend([
            f"The pitch count got heavy early, and {pitches} pitches were all he could squeeze out of the outing.",
            f"He needed {pitches} pitches just to get that far, which helps explain why the start ended so soon.",
            f"Too many long at-bats pushed the pitch count up before he could really settle in.",
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

    if ip >= 5.0 and pitches >= 95 and er <= 3:
        choices.append(f"He had to labor for parts of the outing, but still found a way to push through {pitches} pitches.")

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
    flow = p.get("game_flow") or {}
    scoreless_to_start = safe_int(flow.get("scoreless_to_start", 0), 0)
    only_damage_in_one_inning = bool(flow.get("only_damage_in_one_inning"))
    settled_after_rough = bool(flow.get("settled_after_rough"))
    late_damage = bool(flow.get("late_damage"))
    team_runs = game_context.get("home_score", 0) if p.get("side") == "home" else game_context.get("away_score", 0)
    opp_runs = game_context.get("away_score", 0) if p.get("side") == "home" else game_context.get("home_score", 0)

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
            f"🔥 {name} owns the zone from the start",
        ]
    elif label == "DOMINANT":
        choices = [
            f"🔥 {name} dominates over {ip_text}",
            f"🔥 {name} powers through a dominant start",
            f"🔥 {name} overmatches hitters all night",
            f"🔥 {name} takes over with swing-and-miss stuff",
            f"🔥 {name} puts the game on his terms",
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
    elif label in {"QUALITY", "SHARP"}:
        choices = [
            f"✅ {name} turns in a strong night on the mound",
            f"✅ {name} gives his club a steady start",
            f"✅ {name} keeps the game under control",
            f"✅ {name} holds the line with a clean effort",
            f"✅ {name} keeps his club in command of the night",
        ]
        if k >= 7:
            choices.append(f"✅ {name} pairs length with {number_word(k)} strikeouts")
        elif er == 0:
            choices.append(f"✅ {name} keeps the board clean through {ip_text}")
        elif scoreless_to_start >= 4:
            choices.append(f"✅ {name} opens with {number_word(scoreless_to_start)} scoreless innings and never loses the feel for it")
    elif label == "SOLID":
        choices = [
            f"📈 {name} gives his club a useful start",
            f"📈 {name} steadies things on the mound",
            f"📈 {name} turns in a workmanlike outing",
            f"📈 {name} does enough to keep the game in order",
            f"📈 {name} bridges the game into the middle innings",
        ]
        if k >= 7:
            choices.append(f"📈 {name} adds punchouts to a solid night")
        elif only_damage_in_one_inning and er <= 3:
            choices.append(f"📈 {name} keeps things afloat outside of one rough patch")
        elif settled_after_rough:
            choices.append(f"📈 {name} settles in after an early wobble")
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
            f"🎯 {name} never gets the count working for him",
            f"🎯 {name} fights the zone all night",
            f"🎯 {name} cannot get comfortable in the strike zone",
            f"🎯 {name} falls behind too often to settle in",
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

    if late_damage and label in {"QUALITY", "SOLID"}:
        choices.append(f"📉 {name} cruises for a while before some late damage changes the line")

    if team_runs > opp_runs and label in POSITIVE_STARTER_LABELS:
        choices.append(f"🏁 {name} helps set up a winning night for his club")
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
    flow_sentence = build_starter_game_flow_sentence(p, label, seed)
    velocity_sentence = build_starter_velocity_sentence(p, label, seed, recent_appearances=recent_appearances)
    csw_sentence = build_starter_csw_sentence(p, label, seed)
    pitch_sentence = build_starter_pitch_count_sentence(p, label, seed)
    positive_sentence = build_starter_positive_sentence(stats, label, seed)

    if is_bad_starter_label(label):
        order_options = [
            [overview, flow_sentence, stat_sentence, positive_sentence, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence, csw_sentence],
            [overview, pressure_sentence, stat_sentence, positive_sentence, flow_sentence, pitch_sentence, team_sentence, velocity_sentence, csw_sentence],
            [overview, stat_sentence, flow_sentence, positive_sentence, csw_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, stat_sentence, pitch_sentence, flow_sentence, pressure_sentence, positive_sentence, team_sentence, velocity_sentence],
        ]
    elif label == "STRIKEOUT":
        order_options = [
            [overview, csw_sentence, stat_sentence, flow_sentence, pressure_sentence, pitch_sentence, team_sentence, velocity_sentence],
            [overview, flow_sentence, csw_sentence, stat_sentence, team_sentence, pitch_sentence, velocity_sentence, pressure_sentence],
            [overview, stat_sentence, csw_sentence, flow_sentence, team_sentence, velocity_sentence, pressure_sentence, pitch_sentence],
        ]
    elif label in {"GEM", "DOMINANT"}:
        order_options = [
            [overview, flow_sentence, csw_sentence, pressure_sentence, stat_sentence, team_sentence, velocity_sentence, pitch_sentence],
            [overview, pressure_sentence, stat_sentence, flow_sentence, csw_sentence, team_sentence, pitch_sentence, velocity_sentence],
            [overview, csw_sentence, stat_sentence, flow_sentence, team_sentence, pressure_sentence, velocity_sentence, pitch_sentence],
        ]
    else:
        order_options = [
            [overview, flow_sentence, stat_sentence, pressure_sentence, team_sentence, pitch_sentence, csw_sentence, velocity_sentence],
            [overview, pitch_sentence, flow_sentence, stat_sentence, pressure_sentence, team_sentence, csw_sentence, velocity_sentence],
            [overview, pressure_sentence, stat_sentence, flow_sentence, csw_sentence, team_sentence, velocity_sentence, pitch_sentence],
            [overview, stat_sentence, team_sentence, flow_sentence, pressure_sentence, pitch_sentence, velocity_sentence, csw_sentence],
        ]

    ordered = [s for s in order_options[seed % len(order_options)] if s]
    final_sentences = []
    for sentence in ordered:
        if sentence and sentence not in final_sentences:
            final_sentences.append(sentence)
        if len(final_sentences) >= 4:
            break

    if len(final_sentences) < 3:
        fillers = [flow_sentence, stat_sentence, pressure_sentence, team_sentence, csw_sentence, pitch_sentence, velocity_sentence, positive_sentence]
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
    embed.add_field(name="", value=build_starter_summary(p, label, game_context, recent_appearances=recent_appearances), inline=False)
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

import asyncio
import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
import requests

# ---------------- CONFIG ----------------

TOKEN = os.getenv("ANALYTIC_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("HITTER_WATCH_CHANNEL_ID", "0"))

STATE_DIR = Path("state/hitter")
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state.json"

ET = ZoneInfo("America/New_York")
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = int(os.getenv("HITTER_POLL_MINUTES", "10"))
RESET_HITTER_STATE = os.getenv("RESET_HITTER_STATE", "").lower() in {"1", "true", "yes"}
MIN_HITTER_SCORE = float(os.getenv("HITTER_MIN_SCORE", "5.0"))
MAX_CARDS_PER_GAME = int(os.getenv("HITTER_MAX_CARDS_PER_GAME", "4"))
REQUEST_TIMEOUT = float(os.getenv("HITTER_REQUEST_TIMEOUT", "30"))
ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")

intents = discord.Intents.default()
client: discord.Client | None = None
background_task: asyncio.Task | None = None
player_headshot_index: dict | None = None
hitter_stats_cache: dict = {}


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

TEAM_NAME_MAP = {
    "ARI": "Diamondbacks",
    "ATH": "Athletics",
    "ATL": "Braves",
    "BAL": "Orioles",
    "BOS": "Red Sox",
    "CHC": "Cubs",
    "CWS": "White Sox",
    "CIN": "Reds",
    "CLE": "Guardians",
    "COL": "Rockies",
    "DET": "Tigers",
    "HOU": "Astros",
    "KC": "Royals",
    "LAA": "Angels",
    "LAD": "Dodgers",
    "MIA": "Marlins",
    "MIL": "Brewers",
    "MIN": "Twins",
    "NYM": "Mets",
    "NYY": "Yankees",
    "PHI": "Phillies",
    "PIT": "Pirates",
    "SD": "Padres",
    "SF": "Giants",
    "SEA": "Mariners",
    "STL": "Cardinals",
    "TB": "Rays",
    "TEX": "Rangers",
    "TOR": "Blue Jays",
    "WSH": "Nationals",
}


# ---------------- LOGGING / HELPERS ----------------

def log(msg: str) -> None:
    print(f"[HITTER] {msg}", flush=True)


def safe_int(value, default: int = 0) -> int:
    try:
        if value in (None, "", "-"):
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def normalize_team_abbr(team: str | None) -> str:
    key = str(team or "").strip().upper()
    aliases = {
        "AZ": "ARI",
        "CHW": "CWS",
        "WAS": "WSH",
        "WSN": "WSH",
        "TBR": "TB",
        "KCR": "KC",
        "SDP": "SD",
        "SFG": "SF",
        "OAK": "ATH",
    }
    return aliases.get(key, key)


def team_name_from_abbr(team: str | None) -> str:
    normalized = normalize_team_abbr(team)
    return TEAM_NAME_MAP.get(normalized, normalized or "opponent")


def get_logo(team: str | None) -> str:
    normalized = normalize_team_abbr(team)
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
    key = logo_key_map.get(normalized, normalized.lower())
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"


def normalize_lookup_name(name: str) -> str:
    cleaned = (name or "").lower()
    for ch in [".", ",", "'", "`", "-", "_", "(", ")", "[", "]"]:
        cleaned = cleaned.replace(ch, " ")
    return " ".join(cleaned.split())


# ---------------- HEADSHOTS ----------------

def load_player_headshot_index() -> dict:
    global player_headshot_index
    if player_headshot_index is not None:
        return player_headshot_index

    player_headshot_index = {}
    path = Path(ESPN_PLAYER_IDS_PATH)
    if not path.exists():
        log(f"Player ID file not found: {ESPN_PLAYER_IDS_PATH}")
        return player_headshot_index

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"Could not load player ID file: {exc}")
        return player_headshot_index

    if not isinstance(raw, dict):
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
            payload = {
                "name": raw_name,
                "team": normalize_team_abbr(entry.get("team")),
                "headshot_url": headshot_url,
            }
            player_headshot_index.setdefault(raw_name, []).append(payload)
            normalized = normalize_lookup_name(raw_name)
            if normalized:
                player_headshot_index.setdefault(normalized, []).append(payload)

    log(f"Loaded player headshot index from {ESPN_PLAYER_IDS_PATH}")
    return player_headshot_index


def choose_headshot_entry(entries, team: str | None = None):
    if not entries:
        return None
    normalized_team = normalize_team_abbr(team) if team else None
    if normalized_team:
        for entry in entries:
            if normalize_team_abbr(entry.get("team")) == normalized_team:
                return entry
    return entries[0]


def get_player_headshot(name: str, team: str | None = None) -> str | None:
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

    last = normalized.split()[-1] if normalized else ""
    if not last:
        return None

    for key, entries in index.items():
        key_norm = normalize_lookup_name(key)
        if key_norm.split() and key_norm.split()[-1] == last:
            picked = choose_headshot_entry(entries, team)
            if picked:
                return picked.get("headshot_url")
    return None


def apply_player_card_chrome(embed: discord.Embed, name: str, team: str) -> None:
    display_team = normalize_team_abbr(team) or "UNK"
    logo_url = get_logo(display_team)
    try:
        embed.set_author(name=f"{name} | {display_team}", icon_url=logo_url)
    except Exception:
        embed.set_author(name=f"{name} | {display_team}")

    headshot = get_player_headshot(name, team)
    if headshot:
        try:
            embed.set_thumbnail(url=headshot)
            return
        except Exception:
            pass
    try:
        embed.set_thumbnail(url=logo_url)
    except Exception:
        pass


# ---------------- STATE ----------------

def load_state() -> dict:
    base = {"posted": []}
    if RESET_HITTER_STATE:
        return base
    if not STATE_FILE.exists():
        return base
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            base.update(data)
    except Exception:
        pass
    return base


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps({"posted": state.get("posted", [])}, indent=2), encoding="utf-8")


# ---------------- MLB DATA ----------------

def get_games() -> list[dict]:
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)
    games: list[dict] = []

    for target_date in [today, yesterday]:
        try:
            response = requests.get(f"{SCHEDULE_URL}&date={target_date.isoformat()}", timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            for date_block in payload.get("dates", []):
                games.extend(date_block.get("games", []))
        except Exception as exc:
            log(f"Schedule fetch error for {target_date}: {exc}")
    return games


def get_feed(game_id: int) -> dict:
    response = requests.get(LIVE_URL.format(game_id), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def get_hitting_stats_for_date(target_date):
    if target_date in hitter_stats_cache:
        return hitter_stats_cache[target_date]

    stats_by_hitter: dict[int, dict] = {}
    try:
        response = requests.get(f"{SCHEDULE_URL}&date={target_date.isoformat()}", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        games = []
        for date_block in payload.get("dates", []):
            games.extend(date_block.get("games", []))

        for game in games:
            game_id = game.get("gamePk")
            if not game_id:
                continue
            try:
                feed = get_feed(int(game_id))
            except Exception:
                continue

            box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
            game_teams = feed.get("gameData", {}).get("teams", {})
            for side in ["home", "away"]:
                team = normalize_team_abbr(game_teams.get(side, {}).get("abbreviation"))
                if not team:
                    team = normalize_team_abbr(box.get(side, {}).get("team", {}).get("abbreviation", "UNK"))
                players = box.get(side, {}).get("players", {})
                for player in players.values():
                    stats = player.get("stats", {}).get("batting")
                    if not stats:
                        continue
                    hitter_id = player.get("person", {}).get("id")
                    if hitter_id is None:
                        continue
                    ab = safe_int(stats.get("atBats", 0), 0)
                    pa = ab + safe_int(stats.get("baseOnBalls", 0), 0) + safe_int(stats.get("hitByPitch", 0), 0) + safe_int(stats.get("sacFlies", 0), 0) + safe_int(stats.get("sacBunts", 0), 0)
                    if pa <= 0:
                        continue
                    stats_by_hitter[int(hitter_id)] = {
                        "ab": ab,
                        "h": safe_int(stats.get("hits", 0), 0),
                        "hr": safe_int(stats.get("homeRuns", 0), 0),
                        "rbi": safe_int(stats.get("rbi", 0), 0),
                        "r": safe_int(stats.get("runs", 0), 0),
                        "bb": safe_int(stats.get("baseOnBalls", 0), 0),
                        "k": safe_int(stats.get("strikeOuts", 0), 0),
                        "sb": safe_int(stats.get("stolenBases", 0), 0),
                        "2b": safe_int(stats.get("doubles", 0), 0),
                        "3b": safe_int(stats.get("triples", 0), 0),
                        "team": team,
                    }
    except Exception as exc:
        log(f"Hitting stats cache load failed for {target_date}: {exc}")

    hitter_stats_cache[target_date] = stats_by_hitter
    return stats_by_hitter


def get_recent_hitter_games(hitter_id: int, game_date_et, limit: int = 7, max_days: int = 30) -> list[dict]:
    recent_games: list[dict] = []
    if hitter_id is None or game_date_et is None:
        return recent_games

    check_date = game_date_et - timedelta(days=1)
    for _ in range(max_days):
        stats_by_hitter = get_hitting_stats_for_date(check_date)
        if hitter_id in stats_by_hitter:
            recent_games.append(stats_by_hitter[hitter_id])
            if len(recent_games) >= limit:
                break
        check_date -= timedelta(days=1)
    return recent_games


def _number_word(value: int) -> str:
    words = {0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten"}
    return words.get(int(value), str(value))


def _small_count_phrase(value: int, noun: str, plural: str | None = None, include_article: bool = False) -> str:
    plural = plural or f"{noun}s"
    value = int(value)
    if value == 1:
        return f"a {noun}" if include_article else noun
    if 2 <= value <= 10:
        return f"{_number_word(value)} {plural}"
    return f"{value} {plural}"


def _count_only_phrase(value: int) -> str:
    value = int(value)
    if value == 1:
        return "a"
    if 2 <= value <= 10:
        return _number_word(value)
    return str(value)


def build_hitter_game_context(feed: dict, hitter: dict) -> dict:
    hitter_id = hitter.get("id")
    team = normalize_team_abbr(hitter.get("team"))
    side = hitter.get("side")
    context = {
        "go_ahead_hit": False,
        "go_ahead_homer": False,
        "game_tying_hit": False,
        "walkoff": False,
        "insurance_hit": False,
        "late_rbi_hit": False,
        "multi_rbi_hit": False,
        "hardest_ev": None,
        "balls_100": 0,
        "homers": [],
        "extra_base_hits": [],
    }
    if hitter_id is None:
        return context

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in plays:
        matchup = play.get("matchup", {}) or {}
        batter = matchup.get("batter", {}) or {}
        if batter.get("id") != hitter_id:
            continue

        result = play.get("result", {}) or {}
        event = str(result.get("event") or "").lower()
        event_type = str(result.get("eventType") or "").lower()
        rbi = safe_int(result.get("rbi", 0), 0)
        about = play.get("about", {}) or {}
        inning = safe_int(about.get("inning", 0), 0)
        is_scoring_play = bool(about.get("isScoringPlay"))
        half = str(about.get("halfInning") or "").lower()

        if side == "away":
            before_team = safe_int(result.get("awayScore", 0), 0) - rbi
            before_opp = safe_int(result.get("homeScore", 0), 0)
            after_team = safe_int(result.get("awayScore", 0), 0)
            after_opp = safe_int(result.get("homeScore", 0), 0)
        else:
            before_team = safe_int(result.get("homeScore", 0), 0) - rbi
            before_opp = safe_int(result.get("awayScore", 0), 0)
            after_team = safe_int(result.get("homeScore", 0), 0)
            after_opp = safe_int(result.get("awayScore", 0), 0)

        if is_scoring_play and rbi > 0:
            if before_team <= before_opp and after_team > after_opp:
                context["go_ahead_hit"] = True
                if "home run" in event or event_type == "home_run":
                    context["go_ahead_homer"] = True
            elif before_team < before_opp and after_team == after_opp:
                context["game_tying_hit"] = True
            elif inning >= 7 and after_team > before_team and after_team > after_opp and (after_team - before_opp) >= 2:
                context["insurance_hit"] = True
            elif inning >= 7:
                context["late_rbi_hit"] = True

        if rbi >= 2:
            context["multi_rbi_hit"] = True

        if inning >= 9 and is_scoring_play and after_team > after_opp:
            if (side == "home" and half == "bottom") or (side == "away" and half == "top"):
                context["walkoff"] = side == "home" and half == "bottom"

        for event_obj in play.get("playEvents", []) or []:
            hit_data = event_obj.get("hitData") if isinstance(event_obj, dict) else None
            if not isinstance(hit_data, dict):
                continue
            launch_speed = hit_data.get("launchSpeed")
            if launch_speed not in (None, ""):
                ev = float(launch_speed)
                if context["hardest_ev"] is None or ev > context["hardest_ev"]:
                    context["hardest_ev"] = ev
                if ev >= 100:
                    context["balls_100"] += 1

        if "home run" in event or event_type == "home_run":
            context["homers"].append({"inning": inning, "rbi": rbi})
        if event_type in {"double", "triple"}:
            context["extra_base_hits"].append({"type": event_type, "inning": inning, "rbi": rbi})

    if context["hardest_ev"] is not None:
        context["hardest_ev"] = round(float(context["hardest_ev"]), 1)
    return context


def build_recent_form_blurb(recent_games: list[dict], stats: dict) -> str:
    if not recent_games:
        return ""
    total_hits = sum(g.get("h", 0) for g in recent_games[:5]) + safe_int(stats.get("hits", 0), 0)
    total_hr = sum(g.get("hr", 0) for g in recent_games[:5]) + safe_int(stats.get("homeRuns", 0), 0)
    total_rbi = sum(g.get("rbi", 0) for g in recent_games[:5]) + safe_int(stats.get("rbi", 0), 0)
    games = len(recent_games[:5]) + 1

    streak_hits = 0
    for game in recent_games:
        if game.get("h", 0) > 0:
            streak_hits += 1
        else:
            break
    hit_streak = streak_hits + (1 if safe_int(stats.get("hits", 0), 0) > 0 else 0)

    hitless_tail = 0
    for game in recent_games:
        if game.get("h", 0) == 0:
            hitless_tail += 1
        else:
            break

    if hitless_tail >= 3 and safe_int(stats.get("hits", 0), 0) >= 2:
        return random.choice([
            "It also looked like a possible step out of a recent skid.",
            "After a quiet stretch, this looked more like the hitter they needed.",
            "It felt like the sort of night that can pull a hitter out of a slump.",
        ])
    if hit_streak >= 5:
        return random.choice([
            f"He has now pushed his hitting streak to {hit_streak} games.",
            f"That keeps a hot stretch going, with hits now in {hit_streak} straight games.",
            f"He has kept the heater going with hits in {hit_streak} straight games.",
        ])
    if games >= 5 and total_hits >= 8:
        return random.choice([
            f"He has piled up {total_hits} hits over his last {games} games.",
            f"That gives him {total_hits} hits across his last {games} games and keeps the recent run rolling.",
        ])
    if games >= 5 and total_hr >= 3:
        return random.choice([
            f"He has now left the yard {total_hr} times over his last {games} games.",
            f"The power has been carrying over lately, with {total_hr} homers across his last {games} games.",
        ])
    if games >= 5 and total_rbi >= 8:
        return random.choice([
            f"He has also driven in {total_rbi} runs over his last {games} games.",
            f"The recent production keeps building, with {total_rbi} RBI across his last {games} games.",
        ])
    return ""


def get_hitters(feed: dict) -> list[dict]:
    results: list[dict] = []
    box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    game_teams = feed.get("gameData", {}).get("teams", {})

    for side in ["home", "away"]:
        team = normalize_team_abbr(game_teams.get(side, {}).get("abbreviation"))
        if not team:
            team = normalize_team_abbr(box.get(side, {}).get("team", {}).get("abbreviation", "UNK"))

        players = box.get(side, {}).get("players", {})
        for player in players.values():
            stats = player.get("stats", {}).get("batting")
            if not stats:
                continue

            ab = safe_int(stats.get("atBats", 0), 0)
            pa = (
                ab
                + safe_int(stats.get("baseOnBalls", 0), 0)
                + safe_int(stats.get("hitByPitch", 0), 0)
                + safe_int(stats.get("sacFlies", 0), 0)
                + safe_int(stats.get("sacBunts", 0), 0)
            )
            if pa <= 0:
                continue

            season_stats_block = player.get("seasonStats", {})
            if isinstance(season_stats_block, dict) and "batting" in season_stats_block:
                season_stats = season_stats_block.get("batting", {})
            elif isinstance(season_stats_block, dict):
                season_stats = season_stats_block
            else:
                season_stats = {}

            results.append(
                {
                    "id": player.get("person", {}).get("id"),
                    "name": player.get("person", {}).get("fullName", "Unknown Hitter"),
                    "team": team,
                    "side": side,
                    "position": player.get("position", {}).get("abbreviation", ""),
                    "stats": stats,
                    "season_stats": season_stats,
                }
            )
    return results


# ---------------- HITTER SCORING ----------------

def hitter_total_bases(stats: dict) -> int:
    hits = safe_int(stats.get("hits", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    singles = max(hits - doubles - triples - homers, 0)
    return singles + doubles * 2 + triples * 3 + homers * 4


def score_hitter(stats: dict) -> float:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    total_bases = hitter_total_bases(stats)
    return round(
        total_bases * 1.55
        + rbi * 1.35
        + runs * 1.1
        + walks * 0.6
        + steals * 1.25
        + homers * 1.5
        + hits * 0.25
        - strikeouts * 0.25,
        2,
    )


def classify_hitter(stats: dict) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    xbh = homers + doubles + triples

    if homers >= 2:
        return "power_show"
    if hits >= 4:
        return "hit_parade"
    if hits >= 3 and xbh >= 2:
        return "loud_three_hit"
    if homers >= 1 and rbi >= 3:
        return "impact_power"
    if steals >= 2:
        return "speed_pressure"
    if rbi >= 4:
        return "run_producer"
    if hits >= 3:
        return "steady_attack"
    return "solid_night"


# ---------------- CARD TEXT ----------------

def build_hitter_subject(name: str, stats: dict, label: str, context: dict, recent_games: list[dict]) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    total_bases = hitter_total_bases(stats)

    streak_hits = 0
    for game in recent_games:
        if game.get("h", 0) > 0:
            streak_hits += 1
        else:
            break
    hit_streak = streak_hits + (1 if hits > 0 else 0)
    slump_breaker = False
    hitless_tail = 0
    for game in recent_games:
        if game.get("h", 0) == 0:
            hitless_tail += 1
        else:
            break
    if hitless_tail >= 3 and hits >= 2:
        slump_breaker = True

    if context.get("walkoff"):
        options = ["Walk-Off Hero", "Won It With One Swing", "Delivered The Walk-Off", "Ended It Himself"]
    elif context.get("go_ahead_homer"):
        options = ["Crushed The Go-Ahead Homer", "Supplied The Lead-Changing Swing", "Turned The Game With One Swing", "Changed The Night With A Homer"]
    elif context.get("go_ahead_hit"):
        options = ["Delivered The Decisive Hit", "Came Through With The Go-Ahead Swing", "Put His Club In Front", "Produced The Biggest Hit Of The Night"]
    elif slump_breaker:
        options = ["Looked Like A Slump Breaker", "A Needed Breakthrough Night", "May Have Turned A Corner", "Finally Broke Through At The Plate"]
    elif hit_streak >= 5:
        options = ["Kept The Hot Streak Rolling", "Stayed Red Hot At The Plate", "Another Big Night In A Strong Run", "The Heater Stayed Alive"]
    elif homers >= 2:
        options = ["A Two-Homer Statement", "Left The Yard Twice", "Two Big Flies, Big Impact", "Power That Changed The Game"]
    elif homers >= 1 and rbi >= 3:
        options = ["Power With Real Damage Behind It", "Big Swing Night", "Middle-Of-The-Order Damage", "Thunder In The Right Spots"]
    elif hits >= 4:
        options = ["A Four-Hit Showcase", "Everything He Touched Fell In", "A Barrel-Heavy Night", "Hit After Hit All Night"]
    elif hits >= 3 and total_bases >= 6:
        options = ["Loud Contact From Start To Finish", "Damage In Bunches", "A Three-Hit Night With Thump", "The Bat Never Quieted Down"]
    elif steals >= 2:
        options = ["Pressure On Every Basepath", "Created Chaos On The Bases", "Speed That Changed The Game", "A Dynamic Night On The Move"]
    elif rbi >= 4:
        options = ["Drove In The Difference", "A Huge Night In Run-Scoring Spots", "Kept Cashing In Chances", "Production When It Mattered"]
    else:
        options = ["A Strong Offensive Night", "A Real Lift For The Offense", "A Productive Night At The Plate", "Quality Damage In The Box"]

    seed = len(name) + hits + homers * 3 + rbi * 2 + steals + total_bases
    return options[seed % len(options)]


def format_hitter_game_line(stats: dict) -> str:
    ab = safe_int(stats.get("atBats", 0), 0)
    hits = safe_int(stats.get("hits", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)

    parts = [f"{hits}-{ab}", f"{runs} R", f"{rbi} RBI"]
    if homers:
        parts.append(f"{homers} HR")
    if doubles:
        parts.append(f"{doubles} 2B")
    if triples:
        parts.append(f"{triples} 3B")
    if walks:
        parts.append(f"{walks} BB")
    if steals:
        parts.append(f"{steals} SB")
    if strikeouts:
        parts.append(f"{strikeouts} K")
    return " • ".join(parts)


def format_hitter_season_line(season_stats: dict) -> str:
    season = season_stats or {}
    avg = season.get("avg") or season.get("battingAverage") or ".000"
    obp = season.get("obp") or season.get("onBasePercentage") or ".000"
    slg = season.get("slg") or season.get("sluggingPercentage") or ".000"
    ops = season.get("ops") or season.get("onBasePlusSlugging")
    hr = safe_int(season.get("homeRuns", 0), 0)
    rbi = safe_int(season.get("rbi", 0), 0)
    sb = safe_int(season.get("stolenBases", 0), 0)

    parts = [f"AVG {avg}", f"OBP {obp}", f"SLG {slg}"]
    if ops not in (None, ""):
        parts.append(f"OPS {ops}")
    if hr:
        parts.append(f"{hr} HR")
    if rbi:
        parts.append(f"{rbi} RBI")
    if sb:
        parts.append(f"{sb} SB")
    return " • ".join(parts)


def build_hitter_summary(name: str, team: str, stats: dict, label: str, opponent: str, team_won: bool, context: dict, recent_games: list[dict]) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    total_bases = hitter_total_bases(stats)
    opponent_text = opponent or "the opposition"

    opening_pool = [
        f"Against the {opponent_text}, {name} supplied some of the loudest contact in the lineup.",
        f"{name} gave {team} a real offensive lift against the {opponent_text}.",
        f"The biggest swings for {team} came off {name}'s bat against the {opponent_text}.",
        f"{name} stayed in the middle of everything offensively against the {opponent_text}.",
    ]

    if context.get("go_ahead_homer"):
        opening_pool = [
            f"{name} turned this game with a go-ahead homer against the {opponent_text}.",
            f"Against the {opponent_text}, {name} delivered the swing that put his club in front for good.",
            f"{name} came up with the lead-changing homer against the {opponent_text} and never let that moment fade.",
        ]
    elif context.get("go_ahead_hit"):
        opening_pool = [
            f"{name} came through with the go-ahead hit against the {opponent_text}.",
            f"Against the {opponent_text}, {name} delivered the hit that changed the shape of the game.",
            f"The biggest swing of the night belonged to {name}, who put his club in front against the {opponent_text}.",
        ]
    elif context.get("game_tying_hit"):
        opening_pool = [
            f"{name} helped swing the game back by delivering the tying hit against the {opponent_text}.",
            f"Against the {opponent_text}, {name} supplied the swing that pulled his club back even.",
            f"{name} kept this one alive with the hit that drew his club level against the {opponent_text}.",
        ]
    elif homers >= 2:
        opening_pool = [
            f"Against the {opponent_text}, {name} brought real thunder to the middle of the order.",
            f"{name} gave the offense a huge jolt against the {opponent_text} with one loud swing after another.",
            f"This turned into a power show for {name} against the {opponent_text}.",
        ]

    opening = opening_pool[(len(name) + hits + rbi + homers + total_bases) % len(opening_pool)]

    pieces = [f"He finished {hits}-for-{ab}"]
    if homers:
        pieces.append(f"left the yard {_count_only_phrase(homers)} time{'s' if homers != 1 else ''}")
    elif doubles or triples:
        xbh_bits = []
        if doubles:
            xbh_bits.append(_small_count_phrase(doubles, "double"))
        if triples:
            xbh_bits.append(_small_count_phrase(triples, "triple"))
        if len(xbh_bits) == 1:
            pieces.append(f"ripped {xbh_bits[0]}")
        else:
            pieces.append("ripped " + " and ".join(xbh_bits))
    if rbi:
        pieces.append(f"drove in {_count_only_phrase(rbi)}")
    if runs:
        pieces.append(f"scored {_small_count_phrase(runs, 'run', include_article=True)}")
    if walks:
        pieces.append(f"worked {_small_count_phrase(walks, 'walk', include_article=True)}")
    if steals:
        pieces.append(f"stole {_small_count_phrase(steals, 'base', include_article=True)}")

    if len(pieces) == 1:
        middle = pieces[0] + "."
    else:
        middle = ", ".join(pieces[:-1]) + ", and " + pieces[-1] + "."

    context_pool: list[str] = []
    if context.get("walkoff"):
        context_pool.append("He ended it with the walk-off swing.")
    if context.get("go_ahead_homer"):
        context_pool.append("That homer put his club in front for good.")
    elif context.get("go_ahead_hit"):
        context_pool.append("That swing turned into the deciding hit of the night.")
    if context.get("game_tying_hit"):
        context_pool.append("He also had the hit that pulled his club back even.")
    if context.get("insurance_hit"):
        context_pool.append("He also helped create breathing room once his club got in front.")
    if context.get("late_rbi_hit") and not context.get("go_ahead_hit"):
        context_pool.append("His damage also came in a big late spot.")
    if context.get("multi_rbi_hit") and rbi >= 3 and not context.get("go_ahead_hit"):
        context_pool.append("A lot of the run-producing work traced back to him in this one.")
    if context.get("hardest_ev"):
        ev_sentence = f"His hardest-hit ball came off the bat at {context['hardest_ev']:.1f} mph."
        if context.get("balls_100", 0) >= 2:
            ev_sentence = f"He also produced {context['balls_100']} batted balls at 100-plus mph, topped by a {context['hardest_ev']:.1f} mph shot."
        context_pool.append(ev_sentence)

    recent_blurb = build_recent_form_blurb(recent_games, stats)
    if recent_blurb:
        context_pool.append(recent_blurb)

    if not context_pool:
        if team_won and total_bases >= 6:
            context_pool.append("He carried real weight in the result with the quality of contact he produced.")
        elif team_won:
            context_pool.append("He played a meaningful part in the way this one unfolded.")
        elif homers >= 2:
            context_pool.append("The final result did not take much away from how loud this line was.")
        else:
            context_pool.append("There was real value in the line even if the box score does not tell the whole story.")

    closing = context_pool[(hits + rbi + runs + homers + walks + steals + len(context_pool)) % len(context_pool)]
    return " ".join([opening, middle, closing]).strip()


# ---------------- EMBED POSTING ----------------

async def post_card(channel: discord.abc.Messageable, hitter: dict, opponent: str, team_won: bool, feed: dict, game_date_et) -> None:
    stats = hitter["stats"]
    label = classify_hitter(stats)
    recent_games = get_recent_hitter_games(hitter.get("id"), game_date_et)
    game_context = build_hitter_game_context(feed, hitter)
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(hitter["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, hitter["name"], hitter["team"])
    embed.add_field(name="", value=f"**{build_hitter_subject(hitter['name'], stats, label, game_context, recent_games)}**", inline=False)
    embed.add_field(
        name="Summary",
        value=build_hitter_summary(hitter["name"], hitter["team"], stats, label, opponent, team_won, game_context, recent_games),
        inline=False,
    )
    embed.add_field(name="Game Line", value=format_hitter_game_line(stats), inline=False)
    embed.add_field(name="Season", value=format_hitter_season_line(hitter.get("season_stats", {})), inline=False)
    await channel.send(embed=embed)


# ---------------- LOOP ----------------

async def hitter_loop() -> None:
    assert client is not None
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    state = load_state()
    posted = set(state.get("posted", []))
    if RESET_HITTER_STATE:
        log("RESET_HITTER_STATE enabled — posted state cleared for this run")
        posted = set()

    while True:
        try:
            games = get_games()
            log(f"Checking {len(games)} games")

            for game in games:
                if game.get("status", {}).get("detailedState") != "Final":
                    continue

                game_id = game.get("gamePk")
                if not game_id:
                    continue

                try:
                    feed = get_feed(game_id)
                except Exception as exc:
                    log(f"Feed fetch error for {game_id}: {exc}")
                    continue

                hitters = get_hitters(feed)
                if not hitters:
                    continue

                game_teams = feed.get("gameData", {}).get("teams", {})
                away_abbr = normalize_team_abbr(
                    game_teams.get("away", {}).get("abbreviation")
                    or game.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation")
                    or "AWAY"
                )
                home_abbr = normalize_team_abbr(
                    game_teams.get("home", {}).get("abbreviation")
                    or game.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation")
                    or "HOME"
                )
                away_score = safe_int(game.get("teams", {}).get("away", {}).get("score", 0), 0)
                home_score = safe_int(game.get("teams", {}).get("home", {}).get("score", 0), 0)
                matchup = f"{away_abbr} @ {home_abbr}"
                away_name = team_name_from_abbr(away_abbr)
                home_name = team_name_from_abbr(home_abbr)

                ranked: list[tuple[float, dict]] = []
                for hitter in hitters:
                    score_value = score_hitter(hitter["stats"])
                    if score_value < MIN_HITTER_SCORE:
                        continue
                    ranked.append((score_value, hitter))

                ranked.sort(
                    key=lambda item: (
                        item[0],
                        hitter_total_bases(item[1]["stats"]),
                        safe_int(item[1]["stats"].get("rbi", 0), 0),
                        safe_int(item[1]["stats"].get("hits", 0), 0),
                    ),
                    reverse=True,
                )

                posted_this_game = 0
                for score_value, hitter in ranked:
                    if posted_this_game >= MAX_CARDS_PER_GAME:
                        break
                    hitter_id = hitter.get("id")
                    if hitter_id is None:
                        continue

                    post_key = f"{game_id}_{hitter_id}"
                    if post_key in posted:
                        continue

                    player_team = normalize_team_abbr(hitter.get("team"))
                    opponent = home_name if player_team == away_abbr else away_name
                    team_won = (player_team == away_abbr and away_score > home_score) or (
                        player_team == home_abbr and home_score > away_score
                    )

                    log(f"Posting {hitter['name']} | {hitter['team']} | {matchup} | score={score_value}")
                    await post_card(channel, hitter, opponent, team_won, feed, parse_game_date_et(game))
                    posted.add(post_key)
                    posted_this_game += 1

            state["posted"] = sorted(posted)
            save_state(state)
        except Exception as exc:
            log(f"Loop error: {exc}")

        await asyncio.sleep(POLL_MINUTES * 60)


# ---------------- DISCORD LIFECYCLE ----------------

async def on_ready() -> None:
    global background_task
    assert client is not None
    log(f"Logged in as {client.user}")
    if background_task is None or background_task.done():
        background_task = asyncio.create_task(hitter_loop())
        log("Hitter background task created")


async def start_hitter_bot() -> None:
    global client, background_task
    if not TOKEN:
        raise RuntimeError("ANALYTIC_BOT_TOKEN is not set")
    if CHANNEL_ID <= 0:
        raise RuntimeError("HITTER_WATCH_CHANNEL_ID is not set")

    background_task = None
    client = discord.Client(intents=intents)
    client.event(on_ready)

    # Let main.py own the restart loop. reconnect=False avoids the discord.py
    # resume path that has been crashing with self.ws=None after connect timeouts.
    await client.start(TOKEN, reconnect=False)

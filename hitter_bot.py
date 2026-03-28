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

def build_hitter_subject(name: str, stats: dict, label: str) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    xbh = homers + doubles + triples

    subject_sets = {
        "power_show": [
            "Power Show At The Plate",
            "The Bat Carried Real Weight",
            "A Loud Night In The Box",
            "Impact Power From Start To Finish",
        ],
        "hit_parade": [
            "Everything Was Falling In",
            "A Hit Parade At The Plate",
            "Locked In From The Jump",
            "Barrels All Night Long",
        ],
        "loud_three_hit": [
            "Damage In Bunches",
            "A Loud Three-Hit Night",
            "Plenty Of Thump In The Bat",
            "Consistent Damage All Night",
        ],
        "impact_power": [
            "Big Swing After Big Swing",
            "Middle-Order Damage",
            "A Real Run-Producing Night",
            "The Power Showed Up",
        ],
        "speed_pressure": [
            "Pressure On Every Basepath",
            "Dynamic Offense All Night",
            "He Created Trouble Constantly",
            "Impact With Bat And Legs",
        ],
        "run_producer": [
            "He Cashed In Chances",
            "Timely Damage At The Plate",
            "A Big Night In Run-Scoring Spots",
            "Production When It Mattered",
        ],
        "steady_attack": [
            "A Steady Night At The Plate",
            "The Offense Kept Running Through Him",
            "Consistent Pressure In The Box",
            "A Strong All-Around Night",
        ],
        "solid_night": [
            "A Strong Offensive Night",
            "Useful Production At The Plate",
            "A Quality Box-Score Night",
            "He Helped Drive The Offense",
        ],
    }

    seed = len(name) + hits + homers * 3 + rbi * 2 + steals + xbh
    options = subject_sets.get(label, subject_sets["solid_night"])
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


def build_hitter_summary(name: str, team: str, stats: dict, label: str, opponent: str, team_won: bool) -> str:
    hits = safe_int(stats.get("hits", 0), 0)
    ab = safe_int(stats.get("atBats", 0), 0)
    runs = safe_int(stats.get("runs", 0), 0)
    rbi = safe_int(stats.get("rbi", 0), 0)
    homers = safe_int(stats.get("homeRuns", 0), 0)
    doubles = safe_int(stats.get("doubles", 0), 0)
    triples = safe_int(stats.get("triples", 0), 0)
    walks = safe_int(stats.get("baseOnBalls", 0), 0)
    steals = safe_int(stats.get("stolenBases", 0), 0)
    strikeouts = safe_int(stats.get("strikeOuts", 0), 0)
    total_bases = hitter_total_bases(stats)
    opponent_text = opponent or "the opposition"

    opener_sets = {
        "power_show": [
            f"{name} brought the thunder against the {opponent_text}.",
            f"Against the {opponent_text}, {name} supplied the loudest swings in the lineup.",
            f"The offense leaned heavily on {name}'s power against the {opponent_text}.",
            f"It was a power-driven night for {name} against the {opponent_text}.",
        ],
        "hit_parade": [
            f"{name} was everywhere offensively against the {opponent_text}.",
            f"Few hitters were busier than {name} against the {opponent_text}.",
            f"The at-bats kept adding up for {name} against the {opponent_text}.",
            f"{name} kept the offense moving all night against the {opponent_text}.",
        ],
        "loud_three_hit": [
            f"{name} kept doing damage against the {opponent_text}.",
            f"Against the {opponent_text}, {name} turned a strong hit total into real impact.",
            f"There was plenty of thump in {name}'s night against the {opponent_text}.",
            f"{name} stayed on the barrel against the {opponent_text}.",
        ],
        "impact_power": [
            f"{name} gave {team} a serious jolt against the {opponent_text}.",
            f"The biggest swings for {team} came off {name}'s bat against the {opponent_text}.",
            f"Against the {opponent_text}, {name} made his damage count.",
            f"{name} packed real force into his night against the {opponent_text}.",
        ],
        "speed_pressure": [
            f"{name} put the {opponent_text} under pressure in more than one way.",
            f"Against the {opponent_text}, {name} kept creating action once he got involved.",
            f"The game sped up whenever {name} got moving against the {opponent_text}.",
            f"{name} gave the {opponent_text} trouble all night with both his bat and legs.",
        ],
        "run_producer": [
            f"{name} came through in the biggest run-scoring spots against the {opponent_text}.",
            f"Against the {opponent_text}, {name} made his RBI chances matter.",
            f"A lot of the damage on the scoreboard traced back to {name} against the {opponent_text}.",
            f"{name} kept cashing in chances against the {opponent_text}.",
        ],
        "steady_attack": [
            f"{name} stayed in the middle of the action against the {opponent_text}.",
            f"Against the {opponent_text}, {name} kept adding useful offense.",
            f"{name} put together a steady line against the {opponent_text}.",
            f"The offense kept getting a boost from {name} against the {opponent_text}.",
        ],
        "solid_night": [
            f"{name} turned in a useful offensive night against the {opponent_text}.",
            f"Against the {opponent_text}, {name} gave {team} a helpful line from the plate.",
            f"There was quiet value in {name}'s night against the {opponent_text}.",
            f"{name} gave the offense a lift against the {opponent_text}.",
        ],
    }

    seed = len(name) + hits + runs + rbi + homers + doubles + triples + walks + steals + strikeouts
    opening_options = opener_sets.get(label, opener_sets["solid_night"])
    opening = opening_options[seed % len(opening_options)]

    action_bits: list[str] = []
    if hits:
        action_bits.append(f"He finished {hits}-for-{ab}")
    if homers:
        action_bits.append(f"left the yard {homers} time{'s' if homers != 1 else ''}")
    elif doubles or triples:
        xbh_bits: list[str] = []
        if doubles:
            xbh_bits.append(f"{doubles} double{'s' if doubles != 1 else ''}")
        if triples:
            xbh_bits.append(f"{triples} triple{'s' if triples != 1 else ''}")
        action_bits.append("ripped " + " and ".join(xbh_bits))
    if rbi:
        action_bits.append(f"drove in {rbi}")
    if runs:
        action_bits.append(f"scored {runs} run{'s' if runs != 1 else ''}")
    if walks:
        action_bits.append(f"worked {walks} walk{'s' if walks != 1 else ''}")
    if steals:
        action_bits.append(f"stole {steals} base{'s' if steals != 1 else ''}")

    middle = ""
    if action_bits:
        if len(action_bits) == 1:
            middle = action_bits[0] + "."
        else:
            middle = ", ".join(action_bits[:-1]) + ", and " + action_bits[-1] + "."

    closer_pool: list[str] = []
    if team_won:
        if homers >= 2:
            closer_pool.extend(
                [
                    "His power had a lot to do with the way this one swung.",
                    "That kind of thunder usually ends up deciding the game.",
                    "He carried real weight in the result with those swings.",
                ]
            )
        elif rbi >= 3:
            closer_pool.extend(
                [
                    "He did a lot of the run-producing work in this one.",
                    "That line came with real scoreboard impact.",
                    "He kept showing up when the offense needed a big swing.",
                ]
            )
        elif steals >= 2:
            closer_pool.extend(
                [
                    "His pressure on the bases helped shape the game.",
                    "He kept forcing the defense to rush everything once he got aboard.",
                ]
            )
        elif total_bases >= 6:
            closer_pool.extend(
                [
                    "He supplied a lot of the offense's loud contact in this one.",
                    "The quality of contact kept showing up at the right time.",
                ]
            )
        else:
            closer_pool.extend(
                [
                    "He played a meaningful part in the way this one unfolded.",
                    "There was more impact here than the basic line alone suggests.",
                ]
            )
    else:
        if homers >= 2:
            closer_pool.extend(
                [
                    "Even without the win, he was still one of the biggest offensive stories in the game.",
                    "The final result did not take much away from how loud this line was.",
                ]
            )
        elif hits >= 3 or rbi >= 2:
            closer_pool.extend(
                [
                    "He still did plenty of damage even though the result went the other way.",
                    "There was real production here despite the final outcome.",
                ]
            )
        elif strikeouts >= 3 and hits <= 1:
            closer_pool.extend(
                [
                    "The strikeouts kept it from being a cleaner night overall.",
                    "There was still some swing-and-miss attached to the line.",
                ]
            )
        else:
            closer_pool.extend(
                [
                    "He was one of the few bats to give the lineup a lift in this one.",
                    "It was still a useful offensive line even without the win.",
                ]
            )

    if rbi >= 2 and homers == 0 and hits >= 2:
        closer_pool.append("He kept finding a way to matter once traffic built on the bases.")
    if total_bases >= 8:
        closer_pool.append("He piled up extra-base damage all night long.")

    closer = closer_pool[seed % len(closer_pool)] if closer_pool else ""
    return " ".join(part for part in [opening, middle, closer] if part)


# ---------------- EMBED POSTING ----------------

async def post_card(channel: discord.abc.Messageable, hitter: dict, opponent: str, team_won: bool) -> None:
    stats = hitter["stats"]
    label = classify_hitter(stats)
    embed = discord.Embed(
        color=TEAM_COLORS.get(normalize_team_abbr(hitter["team"]), 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )
    apply_player_card_chrome(embed, hitter["name"], hitter["team"])
    embed.add_field(name="", value=f"**{build_hitter_subject(hitter['name'], stats, label)}**", inline=False)
    embed.add_field(name="Game Line", value=format_hitter_game_line(stats), inline=False)
    embed.add_field(
        name="Summary",
        value=build_hitter_summary(hitter["name"], hitter["team"], stats, label, opponent, team_won),
        inline=False,
    )
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
                    await post_card(channel, hitter, opponent, team_won)
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

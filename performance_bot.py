from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import discord
import requests
from dotenv import load_dotenv

from performance_rules import should_post_performance
from performance_context import (
    get_hitter_performance_context,
    get_starter_performance_context,
)
from statcast_client import (
    fetch_hitter_statcast_context,
    fetch_starter_velocity_context,
)

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    load_dotenv()

TOKEN = os.getenv("NEWS_BOT_TOKEN", "").strip()
NEWS_CHANNEL_ID = int(os.getenv("NEWS_CHANNEL_ID", "0") or "0")

MLB_STATS_API_BASE = os.getenv("MLB_STATS_API_BASE", "https://statsapi.mlb.com/api/v1").rstrip("/")
PERFORMANCE_POLL_MINUTES = int(os.getenv("PERFORMANCE_POLL_MINUTES", "10"))
PERFORMANCE_SCAN_INTERVAL = int(
    os.getenv("PERFORMANCE_SCAN_INTERVAL", str(PERFORMANCE_POLL_MINUTES * 60))
)
STARTUP_DELAY_SECONDS = int(os.getenv("STARTUP_DELAY_SECONDS", "5"))
BACKFILL_DAYS = int(os.getenv("PERFORMANCE_BACKFILL_DAYS", "1"))
PERFORMANCE_ONLY_FINAL = os.getenv("PERFORMANCE_ONLY_FINAL", "true").strip().lower() in {
    "1", "true", "yes", "y", "on"
}
REQUEST_TIMEOUT = float(os.getenv("MLB_STATS_TIMEOUT", "10"))
DEBUG_SCHEDULE = os.getenv("DEBUG_SCHEDULE", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
DEBUG_RECAP = os.getenv("DEBUG_RECAP", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
BYPASS_POSTED_IDS = os.getenv("BYPASS_POSTED_IDS", "false").lower() == "true"

STATE_DIR = BASE_DIR / "state" / "performance_bot"
STATE_DIR.mkdir(parents=True, exist_ok=True)
POSTED_PERFORMANCE_IDS_FILE = STATE_DIR / "posted_performance_ids.json"
TOP_300_PLAYERS_FILE = BASE_DIR / "top_300_players.json"
SHARED_DIR = BASE_DIR.parent / "shared" / "player_ids"
ESPN_PLAYER_IDS_FILE = SHARED_DIR / "espn_player_ids.json"
ESPN_PLAYER_IDS_FALLBACK = BASE_DIR / "espn_player_ids.json"

SESSION = requests.Session()

TEAM_COLORS = {
    "ARI": 0xA71930, "ATH": 0x003831, "SAC": 0x003831, "ATL": 0xCE1141, "BAL": 0xDF4601,
    "BOS": 0xBD3039, "CHC": 0x0E3386, "CWS": 0x27251F, "CIN": 0xC6011F, "CLE": 0xE31937,
    "COL": 0x33006F, "DET": 0x0C2340, "HOU": 0xEB6E1F, "KC": 0x004687, "LAA": 0xBA0021,
    "LAD": 0x005A9C, "MIA": 0x00A3E0, "MIL": 0x12284B, "MIN": 0x002B5C, "NYM": 0xFF5910,
    "NYY": 0x0C2340, "PHI": 0xE81828, "PIT": 0xFDB827, "SD": 0x2F241D, "SF": 0xFD5A1E,
    "SEA": 0x005C5C, "STL": 0xC41E3A, "TB": 0x092C5C, "TEX": 0x003278, "TOR": 0x134A8E,
    "WSH": 0xAB0003,
}
ESPN_TEAM_SLUGS = {
    "ARI": "ari", "ATH": "oak", "SAC": "oak", "ATL": "atl", "BAL": "bal", "BOS": "bos",
    "CHC": "chc", "CWS": "chw", "CIN": "cin", "CLE": "cle", "COL": "col", "DET": "det",
    "HOU": "hou", "KC": "kc", "LAA": "laa", "LAD": "lad", "MIA": "mia", "MIL": "mil",
    "MIN": "min", "NYM": "nym", "NYY": "nyy", "PHI": "phi", "PIT": "pit", "SD": "sd",
    "SF": "sf", "SEA": "sea", "STL": "stl", "TB": "tb", "TEX": "tex", "TOR": "tor", "WSH": "wsh",
}
TEAM_ABBR = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC", 113: "CIN", 114: "CLE",
    115: "COL", 116: "DET", 117: "HOU", 118: "KC", 119: "LAD", 120: "WSH", 121: "NYM",
    133: "ATH", 134: "PIT", 135: "SD", 136: "SEA", 137: "SF", 138: "STL", 139: "TB",
    140: "TEX", 141: "TOR", 142: "MIN", 143: "PHI", 144: "ATL", 145: "CWS", 146: "MIA",
    147: "NYY", 158: "MIL", 159: "SAC",
}


def get_logo(team_abbr: str) -> str:
    slug = ESPN_TEAM_SLUGS.get(team_abbr, team_abbr.lower())
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{slug}.png"


def team_color(team_abbr: str) -> int:
    return TEAM_COLORS.get(team_abbr, 0x2ECC71)


def team_from_id(team_id: Optional[int]) -> str:
    try:
        return TEAM_ABBR.get(int(team_id), "UNK")
    except Exception:
        return "UNK"


intents = discord.Intents.default()
client = discord.Client(intents=intents)
posted_performance_ids: Set[str] = set()
top_300_players: Dict[str, Dict[str, Any]] = {}
espn_players_by_mlbam: Dict[int, Dict[str, Any]] = {}
espn_players_by_name: Dict[str, Dict[str, Any]] = {}


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state() -> None:
    global posted_performance_ids
    posted_performance_ids = {str(x) for x in _load_json(POSTED_PERFORMANCE_IDS_FILE, [])}


def save_posted_performance_id(perf_id: str) -> None:
    posted_performance_ids.add(perf_id)
    _save_json(POSTED_PERFORMANCE_IDS_FILE, sorted(posted_performance_ids))


def load_top_300_players() -> None:
    global top_300_players
    payload = _load_json(TOP_300_PLAYERS_FILE, {})
    cleaned: Dict[str, Dict[str, Any]] = {}
    if isinstance(payload, dict):
        for raw_name, meta in payload.items():
            if not isinstance(raw_name, str):
                continue
            name = raw_name.strip()
            if not name:
                continue
            if isinstance(meta, dict):
                cleaned[name] = {
                    "rank": int(meta.get("rank") or 0),
                    "team": str(meta.get("team") or "").strip(),
                    "pos": str(meta.get("pos") or "").strip(),
                }
            elif isinstance(meta, int):
                cleaned[name] = {"rank": int(meta), "team": "", "pos": ""}
    top_300_players = cleaned


def get_top_300_meta(player_name: str) -> Dict[str, Any]:
    return top_300_players.get((player_name or "").strip(), {})



def _espn_file_path() -> Path:
    if ESPN_PLAYER_IDS_FILE.exists():
        return ESPN_PLAYER_IDS_FILE
    return ESPN_PLAYER_IDS_FALLBACK


def load_espn_player_ids() -> None:
    global espn_players_by_mlbam, espn_players_by_name
    payload = _load_json(_espn_file_path(), {})
    by_mlbam: Dict[int, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}

    def _store(name: str, item: Dict[str, Any]) -> None:
        clean_name = (name or "").strip()
        if not clean_name or not isinstance(item, dict):
            return
        record = {
            "espn_id": item.get("espn_id"),
            "headshot_url": str(item.get("headshot_url") or "").strip(),
            "mlbam_id": item.get("mlbam_id"),
            "pos": str(item.get("pos") or "").strip(),
        }
        mlbam_id = record.get("mlbam_id")
        try:
            if mlbam_id is not None and str(mlbam_id).strip() != "":
                by_mlbam[int(mlbam_id)] = record
        except Exception:
            pass
        if clean_name not in by_name:
            by_name[clean_name] = record

    if isinstance(payload, dict):
        for raw_name, meta in payload.items():
            if isinstance(meta, list):
                for item in meta:
                    _store(raw_name, item)
            elif isinstance(meta, dict):
                _store(raw_name, meta)

    espn_players_by_mlbam = by_mlbam
    espn_players_by_name = by_name


def get_player_headshot(player_name: str, mlbam_id: Optional[int] = None) -> str:
    try:
        if mlbam_id is not None:
            record = espn_players_by_mlbam.get(int(mlbam_id))
            if record and record.get("headshot_url"):
                return str(record["headshot_url"])
    except Exception:
        pass
    record = espn_players_by_name.get((player_name or "").strip())
    if record and record.get("headshot_url"):
        return str(record["headshot_url"])
    return ""



def _today_key(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(UTC)
    return dt.date().isoformat()


def _get(url: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        resp = SESSION.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[HTTP ERROR] {url} | {exc}")
        return None


def _schedule_url() -> str:
    return f"{MLB_STATS_API_BASE}/schedule"


def _live_feed_url(game_pk: int) -> str:
    return f"{MLB_STATS_API_BASE}.1/game/{game_pk}/feed/live"


def fetch_game_pks_for_date(target_date: str) -> List[int]:
    payload = _get(
        _schedule_url(),
        params={"sportId": 1, "date": target_date, "hydrate": "team,linescore,probablePitcher"},
    ) or {}

    pks: List[int] = []
    all_games: List[str] = []

    for d in payload.get("dates") or []:
        for game in d.get("games") or []:
            status = ((game.get("status") or {}).get("abstractGameState") or "").strip()
            detailed = ((game.get("status") or {}).get("detailedState") or "").strip()
            game_pk = game.get("gamePk")
            teams = game.get("teams") or {}
            away = (((teams.get("away") or {}).get("team") or {}).get("abbreviation") or "AWAY").strip()
            home = (((teams.get("home") or {}).get("team") or {}).get("abbreviation") or "HOME").strip()

            all_games.append(f"{game_pk} | {away} @ {home} | abstract={status} | detailed={detailed}")

            if not game_pk:
                continue

            if PERFORMANCE_ONLY_FINAL:
                if status == "Final" or detailed == "Final":
                    pks.append(int(game_pk))
            else:
                if status in {"Live", "Final"} or detailed in {"In Progress", "Game Over", "Final"}:
                    pks.append(int(game_pk))

    if DEBUG_SCHEDULE:
        print(f"[SCHEDULE DEBUG] date={target_date} PERFORMANCE_ONLY_FINAL={PERFORMANCE_ONLY_FINAL}")
        print(f"[SCHEDULE DEBUG] total_games_returned={len(all_games)}")
        for row in all_games:
            print(f"[SCHEDULE GAME] {row}")
        print(f"[SCHEDULE DEBUG] selected_game_pks={sorted(set(pks))}")

    return sorted(set(pks))


def fetch_today_game_pks() -> List[int]:
    return fetch_game_pks_for_date(date.today().isoformat())


def fetch_startup_backfill_game_pks(days_back: int) -> List[int]:
    game_pks: List[int] = []
    if days_back <= 0:
        return game_pks
    today = date.today()
    for offset in range(days_back, 0, -1):
        target_date = (today.fromordinal(today.toordinal() - offset)).isoformat()
        game_pks.extend(fetch_game_pks_for_date(target_date))
    return sorted(set(game_pks))


def fetch_live_feed(game_pk: int) -> Optional[dict]:
    return _get(_live_feed_url(game_pk))


def _all_plays(feed: Dict[str, Any]) -> List[Dict[str, Any]]:
    return (feed.get("liveData") or {}).get("plays", {}).get("allPlays") or []


def _boxscore(feed: Dict[str, Any]) -> Dict[str, Any]:
    return (feed.get("liveData") or {}).get("boxscore") or {}


def _season_from_feed(feed: Dict[str, Any]) -> int:
    try:
        return int(((feed.get("gameData") or {}).get("datetime") or {}).get("officialDate", "")[:4])
    except Exception:
        return date.today().year


def _game_label(feed: Dict[str, Any]) -> str:
    gd = feed.get("gameData") or {}
    teams = gd.get("teams") or {}
    away = ((teams.get("away") or {}).get("abbreviation") or "AWAY").strip()
    home = ((teams.get("home") or {}).get("abbreviation") or "HOME").strip()
    return f"{away} @ {home}"


def _player_name(parts: Dict[str, Any]) -> str:
    return str(parts.get("fullName") or parts.get("name") or "").strip()


def _resulting_scores(play: Dict[str, Any]) -> Tuple[int, int]:
    result = play.get("result") or {}
    return int(result.get("awayScore") or 0), int(result.get("homeScore") or 0)


_NUMBER_WORDS = {
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


def _num_word(value: int) -> str:
    try:
        ivalue = int(value)
    except Exception:
        return str(value)
    return _NUMBER_WORDS.get(ivalue, str(ivalue))


def _ip_words(ip: Any) -> str:
    raw = str(ip if ip not in (None, "", "None", "null") else "0.0").strip()
    if "." not in raw:
        try:
            return _num_word(int(float(raw)))
        except Exception:
            return raw
    try:
        whole_s, frac_s = raw.split(".", 1)
        whole = int(whole_s)
        frac = int(frac_s)
    except Exception:
        return raw
    whole_word = _num_word(whole)
    if frac == 0:
        return whole_word
    if frac == 1:
        return f"{whole_word} and one-third"
    if frac == 2:
        return f"{whole_word} and two-thirds"
    return raw



_ORDINAL_INNING_WORDS = {
    1: "first",
    2: "second",
    3: "third",
    4: "fourth",
    5: "fifth",
    6: "sixth",
    7: "seventh",
    8: "eighth",
    9: "ninth",
    10: "tenth",
    11: "eleventh",
    12: "twelfth",
}

def _inning_word(value: int) -> str:
    try:
        ivalue = int(value)
    except Exception:
        return str(value)
    return _ORDINAL_INNING_WORDS.get(ivalue, f"{ivalue}th")




def _stable_choice(seed: str, options: List[str]) -> str:
    if not options:
        return ""
    idx = sum(ord(ch) for ch in str(seed or "")) % len(options)
    return options[idx]


def _plural(word: str, count: int) -> str:
    return word if int(count) == 1 else f"{word}s"


def _extra_hard_hit_result_text(line: Dict[str, Any], extra_count: int) -> str:
    if extra_count <= 0:
        return ""
    singles = max(int(line.get("h") or 0) - int(line.get("2b") or 0) - int(line.get("3b") or 0) - int(line.get("hr") or 0), 0)
    doubles = int(line.get("2b") or 0)
    triples = int(line.get("3b") or 0)
    pieces: List[str] = []
    if singles:
        pieces.extend(["single"] * singles)
    if doubles:
        pieces.extend(["double"] * doubles)
    if triples:
        pieces.extend(["triple"] * triples)
    if extra_count == 1:
        if len(pieces) == 1:
            return f"one more {pieces[0]}"
        return ""
    if pieces and len(set(pieces)) == 1:
        return f"{_num_word(extra_count)} more {pieces[0]}{'s' if extra_count != 1 else ''}"
    return ""


def _score_line(feed: Dict[str, Any]) -> str:
    linescore = (feed.get("liveData") or {}).get("linescore") or {}
    teams = linescore.get("teams") or {}
    away_runs = (teams.get("away") or {}).get("runs")
    home_runs = (teams.get("home") or {}).get("runs")
    gd = feed.get("gameData") or {}
    away = ((gd.get("teams") or {}).get("away") or {}).get("abbreviation") or "AWAY"
    home = ((gd.get("teams") or {}).get("home") or {}).get("abbreviation") or "HOME"
    if away_runs is None or home_runs is None:
        return f"{away} @ {home}"
    if int(home_runs) > int(away_runs):
        return f"{home} {home_runs} - {away_runs} {away}"
    return f"{away} {away_runs} - {home_runs} {home}"


def _recent_hitting_games(player_id: int, season: int, limit: int = 5) -> List[Dict[str, Any]]:
    payload = _get(
        f"{MLB_STATS_API_BASE}/people/{player_id}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": season},
    ) or {}
    stats = payload.get("stats") or []
    splits = (stats[0].get("splits") or []) if stats else []
    recent: List[Dict[str, Any]] = []
    for split in reversed(splits[-limit:]):
        stat = split.get("stat") or {}
        recent.append({
            "date": split.get("date"),
            "h": stat.get("hits"),
            "ab": stat.get("atBats"),
            "hr": stat.get("homeRuns"),
            "rbi": stat.get("rbi"),
            "2b": stat.get("doubles"),
            "3b": stat.get("triples"),
            "bb": stat.get("baseOnBalls"),
            "r": stat.get("runs"),
            "k": stat.get("strikeOuts"),
        })
    recent.reverse()
    return recent


def _recent_pitching_starts(player_id: int, season: int, limit: int = 3) -> List[Dict[str, Any]]:
    payload = _get(
        f"{MLB_STATS_API_BASE}/people/{player_id}/stats",
        params={"stats": "gameLog", "group": "pitching", "season": season},
    ) or {}
    stats = payload.get("stats") or []
    splits = (stats[0].get("splits") or []) if stats else []
    starts = []
    for split in reversed(splits):
        stat = split.get("stat") or {}
        gs = int(stat.get("gamesStarted") or 0)
        if gs <= 0:
            continue
        starts.append({
            "date": split.get("date"),
            "ip": stat.get("inningsPitched"),
            "er": stat.get("earnedRuns"),
            "k": stat.get("strikeOuts"),
            "fastball_avg": None,
        })
        if len(starts) >= limit:
            break
    starts.reverse()
    return starts



def _clean_hitter_impact_description(player_name: str, description: str) -> str:
    text = (description or "").strip()
    if not text:
        return ""
    lower = text.lower()
    if "grand slam" in lower:
        return "Delivered a grand slam."
    if "walk-off" in lower:
        return "Delivered a walk-off hit."
    safe_name = re.escape(player_name)
    text = re.sub(safe_name, "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\s+", " ", text).strip(" .")
    text = text[:1].upper() + text[1:] if text else ""
    if text and not text.endswith("."):
        text += "."
    return text


def get_hitter_game_impact(feed: Dict[str, Any], player_id: int, player_name: str, team_side: str) -> Dict[str, Any]:
    plays = _all_plays(feed)
    away_score = 0
    home_score = 0
    best: Dict[str, Any] = {}
    for i, play in enumerate(plays):
        matchup = play.get("matchup") or {}
        batter = matchup.get("batter") or {}
        result = play.get("result") or {}
        about = play.get("about") or {}
        new_away, new_home = _resulting_scores(play)
        lead_before = away_score - home_score
        lead_after = new_away - new_home
        if int(batter.get("id") or 0) == int(player_id):
            desc = str(result.get("description") or "").strip()
            clean_desc = _clean_hitter_impact_description(player_name, desc)
            is_scoring = bool(about.get("isScoringPlay"))
            rbi = int(result.get("rbi") or 0)
            inning_no = int(about.get("inning") or 0)
            lower_desc = desc.lower()
            inning_text = f"in the {_inning_word(inning_no)} inning" if inning_no else ""
            if i == len(plays) - 1 and team_side == "home" and inning_no >= 9 and lead_before == 0 and lead_after > 0:
                return {"walk_off": True, "description": "Delivered a walk-off hit."}
            if "grand slam" in lower_desc:
                best = {"grand_slam": True, "description": "Delivered a grand slam."}
            elif lead_before == 0 and lead_after != 0 and is_scoring:
                if "home run" in lower_desc or "homers" in lower_desc or "homered" in lower_desc:
                    best = {"go_ahead_hit": True, "description": f"His homer put his club in front for good {inning_text}.".replace("  ", " ").replace(" .", ".")}
                else:
                    best = {"go_ahead_hit": True, "description": clean_desc or "Delivered the go-ahead hit."}
            elif lead_before != 0 and lead_after == 0 and is_scoring and inning_no >= 6 and not best:
                best = {"game_tying_hit": True, "description": clean_desc or "Delivered the game-tying hit."}
            elif rbi > 0 and inning_no >= 7 and not best:
                best = {"late_inning_rbi_hit": True, "description": clean_desc or "Came through with a late RBI hit."}
            elif ("home run" in lower_desc or "homers" in lower_desc or "homered" in lower_desc) and not best:
                best = {"homer_context": True, "description": f"The homer came {inning_text}.".replace("  ", " ").replace(" .", ".")}
        away_score, home_score = new_away, new_home
    return best


def get_starter_game_impact(feed: Dict[str, Any], pitcher_id: int) -> Dict[str, Any]:
    plays = _all_plays(feed)
    saw_bases_loaded_escape = False
    saw_traffic = False
    for play in plays:
        matchup = play.get("matchup") or {}
        pitcher = matchup.get("pitcher") or {}
        if int(pitcher.get("id") or 0) != int(pitcher_id):
            continue
        desc = str((play.get("result") or {}).get("description") or "").lower()
        if "bases loaded" in desc:
            saw_bases_loaded_escape = True
        if "runners at" in desc or "men on" in desc or "bases loaded" in desc:
            saw_traffic = True
    if saw_bases_loaded_escape:
        return {"bases_loaded_escape": True, "description": "Worked out of a bases-loaded jam."}
    if saw_traffic:
        return {"pitched_through_traffic": True, "description": "Pitched through traffic in a key spot."}
    return {}


def _player_line_from_boxscore(player_entry: Dict[str, Any]) -> Dict[str, Any]:
    stats = player_entry.get("stats") or {}
    batting = stats.get("batting") or {}
    pitching = stats.get("pitching") or {}
    position = ((player_entry.get("position") or {}).get("abbreviation") or "").strip()
    person = player_entry.get("person") or {}
    line: Dict[str, Any] = {"name": _player_name(person), "mlbam_id": person.get("id"), "position": position}
    if batting:
        line.update({
            "appearance_type": "hitter",
            "ab": batting.get("atBats"),
            "h": batting.get("hits"),
            "2b": batting.get("doubles"),
            "3b": batting.get("triples"),
            "hr": batting.get("homeRuns"),
            "rbi": batting.get("rbi"),
            "bb": batting.get("baseOnBalls"),
            "sb": batting.get("stolenBases"),
            "r": batting.get("runs"),
            "k": batting.get("strikeOuts"),
        })
    if pitching:
        line.update({
            "ip": pitching.get("inningsPitched"),
            "er": pitching.get("earnedRuns"),
            "h_allowed": pitching.get("hits"),
            "bb_allowed": pitching.get("baseOnBalls"),
            "k": pitching.get("strikeOuts"),
            "pitches": pitching.get("numberOfPitches"),
            "games_started": pitching.get("gamesStarted"),
        })
    return line


def _extract_hitters(feed: Dict[str, Any]) -> List[Dict[str, Any]]:
    box = _boxscore(feed)
    out: List[Dict[str, Any]] = []
    for team_side in ("away", "home"):
        team = (box.get("teams") or {}).get(team_side) or {}
        players = team.get("players") or {}
        for _, entry in players.items():
            line = _player_line_from_boxscore(entry)
            if "ab" not in line and "h" not in line and "hr" not in line:
                continue
            if int(line.get("ab") or 0) <= 0 and int(line.get("bb") or 0) <= 0 and int(line.get("sb") or 0) <= 0:
                continue
            line["team_side"] = team_side
            line["team_id"] = ((team.get("team") or {}).get("id"))
            out.append(line)
    return out


def _extract_team_starting_pitcher_ids(team_box: Dict[str, Any]) -> Set[int]:
    pitcher_ids = team_box.get("pitchers") or []
    if not pitcher_ids:
        return set()
    try:
        return {int(pitcher_ids[0])}
    except Exception:
        return set()


def _extract_starters(feed: Dict[str, Any]) -> List[Dict[str, Any]]:
    box = _boxscore(feed)
    out: List[Dict[str, Any]] = []
    for team_side in ("away", "home"):
        team = (box.get("teams") or {}).get(team_side) or {}
        starter_ids = _extract_team_starting_pitcher_ids(team)
        players = team.get("players") or {}
        for _, entry in players.items():
            person = entry.get("person") or {}
            player_id = int(person.get("id") or 0)
            if player_id not in starter_ids:
                continue
            line = _player_line_from_boxscore(entry)
            if "ip" not in line:
                continue
            line["appearance_type"] = "starter"
            line["is_starter"] = True
            line["role"] = "SP"
            line["team_side"] = team_side
            line["team_id"] = ((team.get("team") or {}).get("id"))
            out.append(line)
    return out


def _trim(text: Optional[str], max_len: int = 520) -> str:
    text = (text or "").strip()
    if not text:
        return "—"
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _display_position(line: Dict[str, Any]) -> str:
    appearance = str(line.get("appearance_type") or "").lower()
    if appearance == "starter" or line.get("role") == "SP":
        return "SP"
    pos = str(line.get("position") or "").strip().upper()
    return pos or "POS"


def _build_hitter_statline(line: Dict[str, Any]) -> str:
    ab = int(line.get("ab") or 0)
    hits = int(line.get("h") or 0)
    doubles = int(line.get("2b") or 0)
    triples = int(line.get("3b") or 0)
    hr = int(line.get("hr") or 0)
    rbi = int(line.get("rbi") or 0)
    bb = int(line.get("bb") or 0)
    sb = int(line.get("sb") or 0)
    runs = int(line.get("r") or 0)
    parts = [f"{hits}-for-{ab}"]
    if doubles:
        parts.append(f"{doubles} 2B")
    if triples:
        parts.append(f"{triples} 3B")
    if hr:
        parts.append(f"{hr} HR")
    if rbi:
        parts.append(f"{rbi} RBI")
    if runs:
        parts.append(f"{runs} R")
    if bb:
        parts.append(f"{bb} BB")
    if sb:
        parts.append(f"{sb} SB")
    return ", ".join(parts)


def _build_starter_statline(line: Dict[str, Any]) -> str:
    ip = line.get("ip") or "0.0"
    er = int(line.get("er") or 0)
    k = int(line.get("k") or 0)
    bb = int(line.get("bb_allowed") or 0)
    hits = int(line.get("h_allowed") or 0)
    return ", ".join([f"{ip} IP", f"{er} ER", f"{hits} H", f"{bb} BB", f"{k} K"])


def _sentence(text: str) -> str:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return ""
    text = text[:1].upper() + text[1:]
    if not text.endswith("."):
        text += "."
    return text


def _unique_sentences(items: List[str], max_count: int = 5) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in items:
        s = _sentence(item)
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
        if len(out) >= max_count:
            break
    return out


def _is_generic_homer_impact(text: Optional[str]) -> bool:
    if not text:
        return False
    t = re.sub(r"\s+", " ", str(text).strip().lower())
    return t in {
        "delivered a home run.",
        "delivered a home run",
        "he also delivered a home run.",
        "he also delivered a home run",
    }



def _hitter_fallback_details(name: str, line: Dict[str, Any]) -> List[str]:
    hits = int(line.get("h") or 0)
    ab = int(line.get("ab") or 0)
    hr = int(line.get("hr") or 0)
    rbi = int(line.get("rbi") or 0)
    runs = int(line.get("r") or 0)
    bb = int(line.get("bb") or 0)
    sb = int(line.get("sb") or 0)
    doubles = int(line.get("2b") or 0)
    triples = int(line.get("3b") or 0)
    k = int(line.get("k") or 0)
    xbh = doubles + triples + hr
    details: List[str] = []

    if hr >= 2 and rbi >= 3:
        details.append(f"He drove in {_num_word(rbi)} runs while delivering the biggest swings of the night")
    elif hr >= 1 and rbi >= 4:
        details.append(f"One swing ended up carrying a {_num_word(rbi)} rbi fantasy punch")
    elif hr >= 1 and rbi >= 2:
        details.append(f"The homer did real damage, bringing home {_num_word(rbi)} runs")
    elif rbi >= 4:
        details.append(f"He turned his opportunities into {_num_word(rbi)} rbi without wasting many chances")

    if xbh >= 2 and hits >= 3:
        details.append(f"He mixed {_num_word(hits)} hits with multiple balls driven for extra bases")
    elif xbh >= 2:
        details.append("The extra-base damage gave the line a lot more weight")
    elif hits >= 3 and ab > 0:
        details.append(f"He stacked {_num_word(hits)} hits in {_num_word(ab)} trips to the plate")
    elif hits >= 2 and bb >= 2:
        details.append("He kept reaching and forced the pitching staff to keep dealing with him")

    if runs >= 2:
        details.append(f"He also crossed the plate {_num_word(runs)} times")

    if bb >= 2:
        details.append(f"He layered {_num_word(bb)} walks onto the rest of the production")
    elif bb == 1 and hits <= 1 and hr == 0:
        details.append("He at least found one more trip on base with a walk")

    if sb >= 2:
        details.append(f"He added {_num_word(sb)} stolen bases on top of the work with the bat")
    elif sb == 1 and hits + bb >= 2:
        details.append("He also swiped a bag once he got aboard")

    if hits == 0 and ab >= 4 and k >= 2:
        details.append(f"He also struck out {_num_word(k)} times in another frustrating box score")
    elif hits == 1 and k >= 3:
        details.append(f"The line still came with {_num_word(k)} strikeouts, which keeps the swing-and-miss angle in view")

    return details




def _starter_fallback_details(name: str, line: Dict[str, Any], bits: Dict[str, Any]) -> List[str]:
    pitches = int(line.get("pitches") or 0)
    k = int(line.get("k") or 0)
    er = int(line.get("er") or 0)
    hits = int(line.get("h_allowed") or 0)
    bb = int(line.get("bb_allowed") or 0)
    ip_words = _ip_words(line.get("ip") or "0.0")
    details: List[str] = []
    seed = f"{name}|{line.get('ip')}|{er}|{hits}|{bb}|{k}"

    if er <= 2:
        if pitches and pitches <= 60:
            details.append(f"He needed only {pitches} pitches to get through {ip_words} innings")
        elif pitches and pitches <= 75:
            details.append(f"He moved through the outing quickly and finished {ip_words} innings on just {pitches} pitches")
        elif pitches and pitches >= 100:
            details.append(f"He still had to grind through a heavy {pitches} pitch workload")
        elif pitches and pitches >= 90:
            details.append(f"He still needed {pitches} pitches to bring the outing home")
    elif er >= 4:
        rough_options: List[str] = []
        if hits >= 7 and bb <= 1:
            rough_options.extend([
                "Balls kept finding grass, and he never got the line back under control",
                "Hard contact kept stacking up, and he never got the inning to slow down",
                "Hitters kept squaring him up, and the line got away from him in a hurry",
            ])
        if bb >= 3:
            rough_options.extend([
                "Traffic kept building, and he never really found a clean way to stop it",
                "He spent too much of the outing pitching with runners aboard",
                "The command wobbled enough to keep him working under pressure all night",
            ])
        if k <= 2:
            rough_options.extend([
                "He never had a put away pitch when he needed one",
                "There was not enough swing and miss in the mix to stop the damage",
                "He could not miss enough bats to break the momentum against him",
            ])
        rough_options.extend([
            "He kept pitching on the defensive, and the outing unraveled before he could settle in",
            "The outing never found much rhythm, and the trouble kept following him",
        ])
        details.append(_stable_choice(seed + "|roughdetail", rough_options))
    else:
        if pitches and pitches <= 70:
            details.append(f"He worked at a decent pace and got through {ip_words} innings on {pitches} pitches")
        elif pitches and pitches >= 90:
            details.append(f"He had to work for it, needing {pitches} pitches to finish {ip_words} innings")
        elif hits >= 6 and bb == 0:
            details.append("The line was workable, but there was more traffic than the final damage suggested")
        elif bb >= 3:
            details.append("He worked around some traffic, but the free passes made the outing feel less settled than the line suggests")

    if k >= 8 and er <= 2:
        details.append("The strikeout total gave the outing real fantasy weight")
    elif k >= 5 and er >= 3:
        details.append(f"He still missed enough bats to finish with {_num_word(k)} strikeouts")
    elif k <= 3 and er >= 4:
        details.append(_stable_choice(seed + "|kdetail", [
            "He never found enough swing and miss to slow the damage",
            "He could not generate enough empty swings to change the tone of the outing",
            "There was not enough swing and miss to keep the line from getting away from him",
        ]))

    if hits >= 9:
        details.append(f"Opponents kept lining balls around the yard and turned it into {hits} hits")
    elif hits <= 2 and er <= 1:
        details.append("He allowed very little clean contact from start to finish")
    elif hits <= 4 and er <= 2 and k >= 6:
        details.append("He paired the run prevention with a pretty clean contact profile")
    elif hits >= 6 and er <= 3 and bb == 0:
        details.append("The line was workable, but there was more traffic than the final damage suggested")

    if bb >= 4:
        details.append(f"The command drifted enough to lead to {_num_word(bb)} walks")

    fb_avg = bits.get("fastball_avg")
    fb_max = bits.get("fastball_max")
    delta = bits.get("fastball_delta")
    if fb_avg:
        velo = f"His fastball averaged {fb_avg:.1f} mph"
        if fb_max:
            velo += f" and topped out at {fb_max:.1f}"
        if delta is not None:
            if delta > 0:
                velo += f", up {abs(delta):.1f} mph from recent starts"
            elif delta < 0:
                velo += f", down {abs(delta):.1f} mph from recent starts"
            else:
                velo += ", right in line with recent starts"
        details.append(velo)
    return details




def _build_hitter_recap(player: Dict[str, Any], line: Dict[str, Any], context: Dict[str, Any]) -> str:
    name = player["name"]
    bits = context.get("summary_bits") or {}
    hits = int(line.get("h") or 0)
    ab = int(line.get("ab") or 0)
    hr = int(line.get("hr") or 0)
    rbi = int(line.get("rbi") or 0)
    bb = int(line.get("bb") or 0)
    doubles = int(line.get("2b") or 0)
    triples = int(line.get("3b") or 0)
    xbh = doubles + triples + hr
    sb = int(line.get("sb") or 0)
    k = int(line.get("k") or 0)

    if hr >= 2:
        first = f"{name} left the yard {_num_word(hr)} times and authored one of the louder lines of the night"
    elif hr >= 1 and hits >= 3:
        first = f"{name} homered in the middle of a {_num_word(hits)} hit night and kept pressing the action"
    elif hits >= 4:
        first = f"{name} piled up {_num_word(hits)} hits and kept the offense moving every time he came up"
    elif hits >= 3 and xbh >= 2:
        first = f"{name} paired volume with impact, collecting {_num_word(hits)} hits and more than one extra base knock"
    elif hits >= 3:
        first = f"{name} strung together a {_num_word(hits)} hit night and kept putting pressure on the staff"
    elif xbh >= 2:
        first = f"{name} built his line on extra base damage"
    elif hr >= 1 and rbi >= 3:
        first = f"{name} flipped the game with a homer and a {_num_word(rbi)} rbi night"
    elif hr >= 1 and bb >= 2:
        first = f"{name} went deep and kept reaching base around the power"
    elif hr >= 1:
        first = f"{name} came up with the big swing of the night and left the yard"
    elif rbi >= 4:
        first = f"{name} drove in {_num_word(rbi)} runs and turned his chances into a major fantasy line"
    elif sb >= 2 and hits + bb >= 2:
        first = f"{name} created offense with both contact and speed"
    elif hits == 0 and ab >= 4:
        first = f"{name} went 0-for-{ab} and the rough stretch stayed in place"
    elif hits == 1 and k >= 3:
        first = f"{name} managed just one hit in {_num_word(ab)} at bats while striking out {_num_word(k)} times"
    else:
        first = f"{name} turned in a fantasy relevant offensive line"

    candidates: List[str] = [first]
    evaluation_note = bits.get("evaluation_note")
    impact_note = bits.get("impact_note")
    max_ev = bits.get("max_exit_velocity")
    balls_hit_100 = bits.get("balls_hit_100_plus")
    slump_note = bits.get("slump_note")
    streak_note = bits.get("streak_note")

    if evaluation_note and "top 300" not in str(evaluation_note).lower():
        candidates.append(evaluation_note)
    if impact_note and not _is_generic_homer_impact(impact_note):
        candidates.append(impact_note)
    if slump_note:
        candidates.append(slump_note)
    elif streak_note:
        candidates.append(streak_note)

    if max_ev and max_ev >= 100:
        ev_text = f"The homer left the bat at {max_ev:.1f} mph" if hr >= 1 else f"His loudest contact came off the bat at {max_ev:.1f} mph"
        if balls_hit_100 and balls_hit_100 > 1:
            if hr >= 1:
                extra_count = max(balls_hit_100 - 1, 0)
                extra_text = _extra_hard_hit_result_text(line, extra_count)
                if extra_text:
                    ev_text += f", and he added {extra_text} at 100+ mph"
            else:
                ev_text += f", and he produced {_num_word(balls_hit_100)} batted balls at 100+ mph"
        candidates.append(ev_text)

    for fallback in _hitter_fallback_details(name, line):
        candidates.append(fallback)

    return " ".join(_unique_sentences(candidates, max_count=5))




def _build_starter_recap(player: Dict[str, Any], line: Dict[str, Any], context: Dict[str, Any]) -> str:
    name = player["name"]
    bits = context.get("summary_bits") or {}
    ip = line.get("ip") or "0.0"
    er = int(line.get("er") or 0)
    k = int(line.get("k") or 0)
    hits = int(line.get("h_allowed") or 0)
    bb = int(line.get("bb_allowed") or 0)
    pitches = int(line.get("pitches") or 0)
    ip_words = _ip_words(ip)
    seed = f"{name}|{ip}|{er}|{hits}|{bb}|{k}"

    if er == 0 and k >= 8:
        first = f"{name} dominated for {ip_words} scoreless innings and punched out {_num_word(k)}"
    elif er == 0:
        first = f"{name} spun {ip_words} scoreless innings and never let the game get loose on him"
    elif er <= 2 and k >= 7:
        first = f"{name} worked {ip_words} innings, gave up only {_num_word(er)} {_plural('run', er)}, and struck out {_num_word(k)}"
    elif er <= 2 and bb >= 3:
        first = f"{name} bent at times but still kept the line to {_num_word(er)} {_plural('run', er)} over {ip_words} innings"
    elif er <= 2:
        first = f"{name} limited the damage to {_num_word(er)} {_plural('run', er)} over {ip_words} innings"
    elif er >= 6:
        first = f"{name} got hit hard for {_num_word(er)} runs across {ip_words} innings"
    elif er >= 4 and hits >= 7 and bb <= 1:
        first = _stable_choice(seed + "|roughfirst", [
            f"{name} gave up {_num_word(er)} runs over {ip_words} innings as the hard contact piled up on him",
            f"{name} surrendered {_num_word(er)} runs in {ip_words} innings while hitters kept squaring him up",
            f"{name} was tagged for {_num_word(er)} runs across {ip_words} innings and never slowed the contact down",
        ])
    elif er >= 4 and bb >= 3:
        first = _stable_choice(seed + "|trafficfirst", [
            f"{name} pitched in trouble most of the night and gave up {_num_word(er)} runs over {ip_words} innings",
            f"{name} worked through constant traffic and wound up allowing {_num_word(er)} runs in {ip_words} innings",
            f"{name} kept pitching with runners aboard and the line finished at {_num_word(er)} runs over {ip_words} innings",
        ])
    elif er >= 4 and k <= 2:
        first = _stable_choice(seed + "|noswingfirst", [
            f"{name} ran into a rough turn over {ip_words} innings and never found a put away pitch",
            f"{name} could not miss enough bats in a rough {ip_words} inning outing that ended with {_num_word(er)} runs allowed",
            f"{name} labored through {ip_words} innings in a rough turn and never found much swing and miss",
        ])
    elif er >= 4:
        first = _stable_choice(seed + "|roughgeneral", [
            f"{name} gave up {_num_word(er)} runs over {ip_words} innings in a rough turn through the rotation",
            f"{name} wound up with {_num_word(er)} runs against him over {ip_words} innings in a shaky outing",
            f"{name} was tagged for {_num_word(er)} runs across {ip_words} innings in a start that never fully settled",
        ])
    else:
        first = f"{name} covered {ip_words} innings and allowed {_num_word(er)} {_plural('run', er)}"

    if er > 0 and k >= 8 and "struck out" not in first:
        first += f" while still striking out {_num_word(k)}"
    elif er > 0 and k >= 5 and "strike" not in first:
        first += f" with {_num_word(k)} strikeouts"

    if pitches and er >= 5:
        first += f" on {pitches} pitches"

    candidates: List[str] = [first]
    evaluation_note = bits.get("evaluation_note")
    impact_note = bits.get("impact_note")
    form_note = bits.get("form_note")
    pitch_note = bits.get("pitch_note")

    if evaluation_note:
        candidates.append(evaluation_note)
    for extra in _starter_fallback_details(name, line, bits):
        candidates.append(extra)
    if impact_note:
        candidates.append(impact_note)
    if form_note:
        candidates.append(form_note)
    if pitch_note:
        candidates.append(pitch_note)

    return " ".join(_unique_sentences(candidates, max_count=5))




def _debug_hitter_context(player: Dict[str, Any], line: Dict[str, Any], context: Dict[str, Any], statcast: Dict[str, Any], impact: Dict[str, Any], recent_games: List[Dict[str, Any]]) -> None:
    if not DEBUG_RECAP:
        return
    bits = context.get("summary_bits") or {}
    print(f"[RECAP DEBUG][HITTER] {player['name']}")
    print(f"  line={_build_hitter_statline(line)}")
    print(f"  statcast={statcast}")
    print(f"  impact={impact}")
    print(f"  recent_games_count={len(recent_games)}")
    print(f"  summary_bits={bits}")
    print(f"  final_recap={_build_hitter_recap(player, line, context)}")


def _debug_starter_context(player: Dict[str, Any], line: Dict[str, Any], context: Dict[str, Any], velo: Dict[str, Any], impact: Dict[str, Any], previous_starts: List[Dict[str, Any]]) -> None:
    if not DEBUG_RECAP:
        return
    bits = context.get("summary_bits") or {}
    print(f"[RECAP DEBUG][STARTER] {player['name']}")
    print(f"  line={_build_starter_statline(line)} | pitches={line.get('pitches')}")
    print(f"  velocity_data={velo}")
    print(f"  impact={impact}")
    print(f"  previous_starts_count={len(previous_starts)}")
    print(f"  summary_bits={bits}")
    print(f"  final_recap={_build_starter_recap(player, line, context)}")


def build_hitter_embed(player: Dict[str, Any], line: Dict[str, Any], context: Dict[str, Any], feed: Dict[str, Any]) -> discord.Embed:
    name = player["name"]
    team = team_from_id(line.get("team_id"))
    pos = _display_position(line)
    score_line = _score_line(feed)
    embed = discord.Embed(color=team_color(team))
    try:
        embed.set_author(name=f"{name} | {pos} | {team}", icon_url=get_logo(team))
    except Exception:
        embed.title = f"{name} | {pos} | {team}"
    headshot_url = get_player_headshot(name, line.get("mlbam_id"))
    if headshot_url:
        try:
            embed.set_thumbnail(url=headshot_url)
        except Exception:
            pass
    embed.add_field(name="Stat Line", value=_trim(_build_hitter_statline(line), 160), inline=False)
    embed.add_field(name="Score", value=score_line, inline=False)
    embed.add_field(name="Recap", value=_trim(_build_hitter_recap(player, line, context), 520), inline=False)
    embed.set_footer(text="Posted")
    embed.timestamp = datetime.now(UTC)
    return embed


def build_starter_embed(player: Dict[str, Any], line: Dict[str, Any], context: Dict[str, Any], feed: Dict[str, Any]) -> discord.Embed:
    name = player["name"]
    team = team_from_id(line.get("team_id"))
    pos = _display_position(line)
    score_line = _score_line(feed)
    embed = discord.Embed(color=team_color(team))
    try:
        embed.set_author(name=f"{name} | {pos} | {team}", icon_url=get_logo(team))
    except Exception:
        embed.title = f"{name} | {pos} | {team}"
    headshot_url = get_player_headshot(name, line.get("mlbam_id"))
    if headshot_url:
        try:
            embed.set_thumbnail(url=headshot_url)
        except Exception:
            pass
    embed.add_field(name="Stat Line", value=_trim(_build_starter_statline(line), 160), inline=False)
    embed.add_field(name="Score", value=score_line, inline=False)
    embed.add_field(name="Recap", value=_trim(_build_starter_recap(player, line, context), 520), inline=False)
    embed.set_footer(text="Posted")
    embed.timestamp = datetime.now(UTC)
    return embed


async def send_output_embed(embed: discord.Embed) -> bool:
    channel = client.get_channel(NEWS_CHANNEL_ID)
    if not channel:
        print(f"[ERROR] Output channel not found or no access: {NEWS_CHANNEL_ID}")
        return False
    try:
        await channel.send(embed=embed)
    except Exception as exc:
        print(f"[ERROR] Failed to send embed: {exc}")
        return False
    return True


async def process_game_performances(game_pk: int) -> None:
    feed = await asyncio.to_thread(fetch_live_feed, game_pk)
    if not feed:
        return
    season = _season_from_feed(feed)
    game_label = _game_label(feed)
    game_date = ((feed.get("gameData") or {}).get("datetime") or {}).get("officialDate") or _today_key()
    print(f"[PERF GAME] gamePk={game_pk} | {game_label}")

    hitters = _extract_hitters(feed)
    for line in hitters:
        player = {"name": line["name"], "mlbam_id": line.get("mlbam_id"), "position": line.get("position"), "role": None}
        player_id = int(line.get("mlbam_id") or 0)
        perf_id = f"{game_date}|{game_pk}|hitter|{player_id}"
        if perf_id in posted_performance_ids:
            continue

        recent_games = await asyncio.to_thread(_recent_hitting_games, player_id, season, 5)

        top_meta = get_top_300_meta(player["name"])
        top_rank = top_meta.get("rank") or None
        decision = should_post_performance(player, line, recent_games=recent_games, top_rank=top_rank)
        if not decision.get("post"):
            continue

        statcast = await asyncio.to_thread(fetch_hitter_statcast_context, player_id, game_date, game_pk)
        impact = get_hitter_game_impact(feed, player_id, player["name"], line.get("team_side") or "away")
        player["top_rank"] = top_rank
        context = get_hitter_performance_context(
            player,
            line,
            statcast=statcast,
            recent_games=recent_games,
            game_impact=impact,
            top_rank=top_rank,
            decision=decision,
        )
        _debug_hitter_context(player, line, context, statcast, impact, recent_games)

        embed = build_hitter_embed(player, line, context, feed)
        sent_ok = await send_output_embed(embed)
        if not sent_ok:
            continue
        save_posted_performance_id(perf_id)
        print(f"[PERF POSTED] hitter={player['name']} gamePk={game_pk} | reason={decision.get('reason')} | rank={top_rank}")

    starters = _extract_starters(feed)
    for line in starters:
        player = {"name": line["name"], "mlbam_id": line.get("mlbam_id"), "position": "P", "role": "SP"}
        decision = should_post_performance(player, line)
        if not decision.get("post"):
            continue
        player_id = int(line.get("mlbam_id") or 0)
        perf_id = f"{game_date}|{game_pk}|starter|{player_id}"
        if perf_id in posted_performance_ids:
            continue

        previous_starts = await asyncio.to_thread(_recent_pitching_starts, player_id, season, 3)

        velo = await asyncio.to_thread(fetch_starter_velocity_context, player_id, game_date, 3, game_pk)
        impact = get_starter_game_impact(feed, player_id)
        statcast_previous = []
        if isinstance(velo, dict):
            statcast_previous = velo.get("previous_starts") or []

        merged_previous_starts = []
        for idx, start in enumerate(previous_starts):
            merged = dict(start)
            if idx < len(statcast_previous):
                if statcast_previous[idx].get("fastball_avg") is not None:
                    merged["fastball_avg"] = statcast_previous[idx].get("fastball_avg")
                if statcast_previous[idx].get("fastball_max") is not None:
                    merged["fastball_max"] = statcast_previous[idx].get("fastball_max")
            merged_previous_starts.append(merged)

        context = get_starter_performance_context(
            player,
            line,
            velocity_data=velo,
            previous_starts=merged_previous_starts,
            game_impact=impact,
            decision=decision,
        )
        _debug_starter_context(player, line, context, velo, impact, merged_previous_starts)

        embed = build_starter_embed(player, line, context, feed)
        sent_ok = await send_output_embed(embed)
        if not sent_ok:
            continue
        save_posted_performance_id(perf_id)
        print(f"[PERF POSTED] starter={player['name']} gamePk={game_pk} | reason={decision.get('reason')}")


async def run_startup_backfill() -> None:
    if BACKFILL_DAYS <= 0:
        return
    try:
        backfill_game_pks = await asyncio.to_thread(fetch_startup_backfill_game_pks, BACKFILL_DAYS)
        print(f"[PERF BACKFILL] days={BACKFILL_DAYS} games={len(backfill_game_pks)}")
        for game_pk in backfill_game_pks:
            await process_game_performances(game_pk)
    except Exception as exc:
        print(f"[PERF BACKFILL ERROR] {exc}")


async def performance_scan_loop() -> None:
    await client.wait_until_ready()
    await run_startup_backfill()
    while True:
        try:
            game_pks = await asyncio.to_thread(fetch_today_game_pks)
            print(f"[PERF SCAN] games={len(game_pks)}")
            for game_pk in game_pks:
                await process_game_performances(game_pk)
        except Exception as exc:
            print(f"[PERF LOOP ERROR] {exc}")
        await asyncio.sleep(PERFORMANCE_SCAN_INTERVAL)


@client.event
async def on_ready() -> None:
    print(f"[PERFORMANCE BOT] Logged in as {client.user}")
    print(f"[PERFORMANCE BOT] Output channel = {NEWS_CHANNEL_ID}")
    print(f"[PERFORMANCE BOT] Posted performance ids loaded = {len(posted_performance_ids)}")
    print(f"[PERFORMANCE BOT] State file = {POSTED_PERFORMANCE_IDS_FILE}")
    print(f"[PERFORMANCE BOT] ENV file = {ENV_FILE}")
    print(f"[PERFORMANCE BOT] PERFORMANCE_ONLY_FINAL = {PERFORMANCE_ONLY_FINAL}")
    print(f"[PERFORMANCE BOT] PERFORMANCE_SCAN_INTERVAL = {PERFORMANCE_SCAN_INTERVAL}")
    print(f"[PERFORMANCE BOT] PERFORMANCE_BACKFILL_DAYS = {BACKFILL_DAYS}")
    print(f"[PERFORMANCE BOT] DEBUG_SCHEDULE = {DEBUG_SCHEDULE}")
    print(f"[PERFORMANCE BOT] DEBUG_RECAP = {DEBUG_RECAP}")
    print(f"[PERFORMANCE BOT] Top 300 file = {TOP_300_PLAYERS_FILE}")
    print(f"[PERFORMANCE BOT] Top 300 players loaded = {len(top_300_players)}")
    print(f"[PERFORMANCE BOT] ESPN player ids file = {_espn_file_path()}")
    print(f"[PERFORMANCE BOT] ESPN player ids loaded = {len(espn_players_by_mlbam)}")
    asyncio.create_task(performance_scan_loop())


async def main() -> None:
    if not TOKEN:
        raise RuntimeError("NEWS_BOT_TOKEN is missing")
    if not NEWS_CHANNEL_ID:
        raise RuntimeError("NEWS_CHANNEL_ID is missing")
    load_state()
    load_top_300_players()
    load_espn_player_ids()
    print("[PERFORMANCE BOT] Starting...")
    await asyncio.sleep(STARTUP_DELAY_SECONDS)
    await client.start(TOKEN)


async def start_performance_bot() -> None:
    await main()


if __name__ == "__main__":
    asyncio.run(main())

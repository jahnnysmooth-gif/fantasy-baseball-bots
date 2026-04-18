#!/usr/bin/env python3
"""MiLB Prospect Watch Bot.

Polls completed AA and AAA games, finds qualifying performances from players
on the top_prospects.json list, and posts a Discord embed card for each one.
Runs as a long-lived Discord bot process — same pattern as hitter_bot / starter_bot.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import anthropic
import discord
import requests

import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.team_data import get_logo as _get_team_logo_espn
_sys.path.pop(0)

# ── CONFIG ───────────────────────────────────────────────────────────────────

TOKEN      = os.getenv("ANALYTIC_BOT_TOKEN", "")
CHANNEL_ID = int(os.getenv("PROSPECT_WATCH_CHANNEL_ID", "0"))
ANTHROPIC_API_KEY   = os.getenv("PROSPECT_BOT_SUMMARY", "")
PROSPECTS_FILE      = os.getenv("PROSPECT_WATCH_PROSPECTS_FILE", "prospects/top_prospects.json")
ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
STATE_FILE  = os.getenv("PROSPECT_WATCH_STATE_FILE", "state/prospect_watch/state.json")
RESET_STATE = os.getenv("RESET_PROSPECT_STATE", "").lower() in {"1", "true", "yes"}

POLL_MINUTES        = int(os.getenv("PROSPECT_WATCH_POLL_MINUTES", "15"))
MIN_HITTER_SCORE    = float(os.getenv("PROSPECT_WATCH_MIN_HITTER_SCORE", "5.0"))
MIN_PITCHER_SCORE   = float(os.getenv("PROSPECT_WATCH_MIN_PITCHER_SCORE", "12.0"))
MIN_HITTER_PA       = int(os.getenv("PROSPECT_WATCH_MIN_PA", "2"))
MIN_PITCHER_IP      = float(os.getenv("PROSPECT_WATCH_MIN_IP", "3.0"))
MAX_POSTS_PER_SCAN  = int(os.getenv("PROSPECT_WATCH_MAX_POSTS", "15"))
SLEEP_START_HOUR_ET = int(os.getenv("PROSPECT_WATCH_SLEEP_START", "3"))
SLEEP_END_HOUR_ET   = int(os.getenv("PROSPECT_WATCH_SLEEP_END", "11"))
REQUEST_TIMEOUT     = int(os.getenv("PROSPECT_WATCH_TIMEOUT", "30"))
POST_DELAY_SECONDS  = float(os.getenv("PROSPECT_WATCH_POST_DELAY", "1.5"))
CLAUDE_MODEL        = "claude-sonnet-4-6"

ET       = ZoneInfo("America/New_York")
BASE_URL = "https://statsapi.mlb.com/api/v1"

AAA_SPORT_ID = 11
AA_SPORT_ID  = 12
A_PLUS_SPORT_ID = 13
A_SPORT_ID   = 14
ROK_SPORT_ID = 16
STATUS_FINAL_CODES = {"F", "O", "R"}
POSITION_PLAYER_PITCHING_CODES = {"C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "OF", "DH"}

LEVEL_CONFIG = {
    AAA_SPORT_ID:   {"label": "AAA", "color": 0x2563EB},
    AA_SPORT_ID:    {"label": "AA",  "color": 0x7C3AED},
    A_PLUS_SPORT_ID:{"label": "A+",  "color": 0x16A34A},
    A_SPORT_ID:     {"label": "A",   "color": 0xCA8A04},
    ROK_SPORT_ID:   {"label": "ROK", "color": 0xDC2626},
}

TEAM_ABBREV_TO_ID = {
    "ARI": 109, "ATL": 144, "BAL": 110, "BOS": 111, "CHC": 112,
    "CWS": 145, "CIN": 113, "CLE": 114, "COL": 115, "DET": 116,
    "HOU": 117, "KC":  118, "LAA": 108, "LAD": 119, "MIA": 146,
    "MIL": 158, "MIN": 142, "NYM": 121, "NYY": 147, "OAK": 133,
    "PHI": 143, "PIT": 134, "SD":  135, "SF":  137, "SEA": 136,
    "STL": 138, "TB":  139, "TEX": 140, "TOR": 141, "WSH": 120,
}

# ── GLOBALS ──────────────────────────────────────────────────────────────────

client: discord.Client | None = None
background_task: asyncio.Task | None = None

_prospect_by_norm_name: dict[str, dict] = {}
_prospect_by_mlb_id: dict[int, dict] = {}
_team_org_cache: dict[int, str] = {}
_rehab_cache: dict[int, bool] = {}
_headshot_index: dict[str, list] = {}
_headshot_loaded = False


# ── LOGGING ──────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[PROSPECT] {msg}", flush=True)


def log_exception(ctx: str) -> None:
    log(ctx)
    traceback.print_exc()


# ── NAME NORMALIZATION ────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Lowercase, strip accents, remove periods/apostrophes, collapse spaces."""
    if not name:
        return ""
    nfd = unicodedata.normalize("NFD", name)
    ascii_str = nfd.encode("ascii", "ignore").decode("ascii")
    cleaned = ascii_str.lower().replace(".", "").replace("'", "").replace("-", " ")
    return " ".join(cleaned.split())


# ── PROSPECT LIST ─────────────────────────────────────────────────────────────

def load_prospects() -> None:
    global _prospect_by_norm_name, _prospect_by_mlb_id
    path = Path(PROSPECTS_FILE)
    if not path.exists():
        log(f"Prospects file not found: {PROSPECTS_FILE}")
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"Failed to load prospects: {exc}")
        return

    _prospect_by_norm_name = {}
    _prospect_by_mlb_id = {}
    for p in data:
        norm = normalize_name(p.get("name", ""))
        if norm:
            _prospect_by_norm_name[norm] = p
        mlb_id = p.get("mlb_id")
        if mlb_id:
            _prospect_by_mlb_id[int(mlb_id)] = p

    log(f"Loaded {len(_prospect_by_norm_name)} prospects")


def find_prospect(player_id: int, name: str) -> dict | None:
    """Return prospect dict if this player is on the list, else None."""
    if player_id and player_id in _prospect_by_mlb_id:
        return _prospect_by_mlb_id[player_id]
    norm = normalize_name(name)
    if norm in _prospect_by_norm_name:
        return _prospect_by_norm_name[norm]
    # Try stripping generational suffixes (Jr., Sr., II, III, IV)
    parts = norm.split()
    if parts and parts[-1] in {"jr", "sr", "ii", "iii", "iv"}:
        without_suffix = " ".join(parts[:-1])
        if without_suffix in _prospect_by_norm_name:
            return _prospect_by_norm_name[without_suffix]
    return None


# ── SAFE CONVERSIONS ──────────────────────────────────────────────────────────

def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", "-"):
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", "-"):
            return default
        return float(value)
    except Exception:
        return default


def parse_ip(ip_value: Any) -> float:
    if ip_value in (None, ""):
        return 0.0
    if isinstance(ip_value, (int, float)):
        return float(ip_value)
    text = str(ip_value)
    if "." not in text:
        try:
            return float(text)
        except ValueError:
            return 0.0
    whole_str, frac_str = text.split(".", 1)
    try:
        whole = int(whole_str)
        frac = int(frac_str[:1]) if frac_str else 0
    except ValueError:
        return 0.0
    return whole + {0: 0.0, 1: 1 / 3, 2: 2 / 3}.get(frac, 0.0)


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _get_json(url: str, params: dict | None = None) -> dict:
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("Unreachable retry loop")


def fetch_schedule(sport_id: int, date_str: str) -> list[dict]:
    data = _get_json(f"{BASE_URL}/schedule", params={
        "sportId": sport_id,
        "date": date_str,
        "hydrate": "team,linescore",
    })
    games: list[dict] = []
    for block in data.get("dates", []):
        games.extend(block.get("games", []))
    return games


def fetch_boxscore(game_pk: int) -> dict:
    return _get_json(f"{BASE_URL}/game/{game_pk}/boxscore")


def fetch_live_feed(game_pk: int) -> dict:
    return _get_json(f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live")


def extract_player_plays(feed: dict, player_id: int, perf_type: str) -> str:
    """Return a compact situational summary of the player's key moments from the live feed."""
    try:
        all_plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
        moments: list[str] = []

        for play in all_plays:
            matchup = play.get("matchup", {})
            result  = play.get("result", {})
            about   = play.get("about", {})

            if perf_type == "hitter":
                batter_id = safe_int((matchup.get("batter") or {}).get("id"))
                if batter_id != player_id:
                    continue
                event = result.get("event", "")
                desc  = result.get("description", "")
                inning = about.get("inning", "?")
                half   = "top" if about.get("isTopInning") else "bottom"
                rbi    = safe_int(result.get("rbi"))
                runners_text = ""
                runners = play.get("runners", [])
                on_base = [r.get("movement", {}).get("originBase") for r in runners
                           if r.get("movement", {}).get("originBase")]
                if on_base:
                    runners_text = f", runners on {', '.join(on_base)}"
                if event in ("Home Run", "Triple", "Double", "Single", "Walk",
                             "Stolen Base", "Hit By Pitch"):
                    rbi_text = f", {rbi} RBI" if rbi else ""
                    moments.append(
                        f"{half} {inning}: {event}{rbi_text}{runners_text}"
                    )

            else:
                pitcher_id = safe_int((matchup.get("pitcher") or {}).get("id"))
                if pitcher_id != player_id:
                    continue
                event  = result.get("event", "")
                inning = about.get("inning", "?")
                half   = "top" if about.get("isTopInning") else "bottom"
                if event in ("Strikeout", "Home Run", "Walk"):
                    moments.append(f"{half} {inning}: {event}")

        if not moments:
            return ""
        # Cap to avoid blowing out the prompt
        return "; ".join(moments[:12])
    except Exception as exc:
        log(f"extract_player_plays failed for player {player_id}: {exc}")
        return ""


def fetch_season_stats(player_id: int, group: str, sport_id: int) -> dict | None:
    season = date.today().year
    # Try the player's current level first, then fall back to all MiLB levels combined
    for sid_param in [str(sport_id), "11,12,13,14,16"]:
        try:
            data = _get_json(f"{BASE_URL}/people/{player_id}/stats", params={
                "stats": "season",
                "group": group,
                "season": season,
                "sportId": sid_param,
            })
            splits = (data.get("stats") or [{}])[0].get("splits", [])
            if splits:
                return splits[0].get("stat")
        except Exception as exc:
            log(f"Season stats fetch failed for player {player_id} sportId={sid_param}: {exc}")
    return None


def fetch_recent_stats(player_id: int, group: str, sport_id: int, games: int = 7) -> dict | None:
    """Return lastXGames stats for hitters (7 games) or pitchers (3 starts)."""
    limit = games if group == "hitting" else 3
    for sid_param in [str(sport_id), "11,12,13,14,16"]:
        try:
            data = _get_json(f"{BASE_URL}/people/{player_id}/stats", params={
                "stats": "lastXGames",
                "group": group,
                "season": date.today().year,
                "sportId": sid_param,
                "limit": limit,
            })
            splits = (data.get("stats") or [{}])[0].get("splits", [])
            if splits:
                return splits[0].get("stat")
        except Exception as exc:
            log(f"Recent stats fetch failed for player {player_id} sportId={sid_param}: {exc}")
    return None


def format_recent_hitter_line(stat: dict, games: int = 7) -> str:
    if not stat:
        return ""
    avg  = stat.get("avg", ".000")
    hr   = safe_int(stat.get("homeRuns"))
    rbi  = safe_int(stat.get("rbi"))
    sb   = safe_int(stat.get("stolenBases"))
    ops  = stat.get("ops", ".000")
    parts = [f"{avg} AVG", f"{hr} HR", f"{rbi} RBI", f"{ops} OPS"]
    if sb:
        parts.append(f"{sb} SB")
    return f"Last {games}G: " + " • ".join(parts)


def format_recent_pitcher_line(stat: dict) -> str:
    if not stat:
        return ""
    era  = stat.get("era", "-.--")
    whip = stat.get("whip", "-.--")
    k    = safe_int(stat.get("strikeOuts"))
    ip   = stat.get("inningsPitched", "0.0")
    return f"Last 3 starts: {era} ERA • {whip} WHIP • {k} K • {ip} IP"


def is_rehab_assignment(player_id: int) -> bool:
    """Return True if the player's current team is an MLB roster (sportId=1)."""
    if player_id in _rehab_cache:
        return _rehab_cache[player_id]
    try:
        data = _get_json(f"{BASE_URL}/people/{player_id}", params={"hydrate": "currentTeam"})
        people = data.get("people", [])
        if people:
            sport = (people[0].get("currentTeam") or {}).get("sport") or {}
            result = sport.get("id") == 1
            _rehab_cache[player_id] = result
            return result
    except Exception as exc:
        log(f"Rehab check failed for player {player_id}: {exc}")
    _rehab_cache[player_id] = False
    return False


def fetch_team_org_abbrev(team_id: int, fallback: str) -> str:
    if team_id in _team_org_cache:
        return _team_org_cache[team_id]
    try:
        data = _get_json(f"{BASE_URL}/teams/{team_id}")
        teams = data.get("teams", [])
        if teams:
            team = teams[0]
            parent_org_id = team.get("parentOrgId")
            if parent_org_id:
                parent = _get_json(f"{BASE_URL}/teams/{parent_org_id}")
                parent_teams = parent.get("teams", [])
                if parent_teams:
                    abbrev = (parent_teams[0].get("abbreviation")
                              or parent_teams[0].get("teamCode")
                              or fallback)
                    _team_org_cache[team_id] = abbrev.upper()
                    return _team_org_cache[team_id]
            abbrev = team.get("abbreviation") or team.get("teamCode") or fallback
            _team_org_cache[team_id] = str(abbrev).upper()
            return _team_org_cache[team_id]
    except Exception as exc:
        log(f"Team org lookup failed for team_id={team_id}: {exc}")
    _team_org_cache[team_id] = fallback.upper()
    return _team_org_cache[team_id]


# ── SCORING ───────────────────────────────────────────────────────────────────

def score_hitter(h: int, doubles: int, triples: int, hr: int,
                 rbi: int, r: int, bb: int, hbp: int, sb: int, so: int) -> float:
    singles = max(0, h - doubles - triples - hr)
    score = (
        singles * 1.0 + doubles * 2.0 + triples * 3.0 + hr * 4.0
        + rbi * 1.0 + r * 1.0 + sb * 2.0 + bb * 0.5 + hbp * 0.5
        - so * 0.25
    )
    if h >= 2:
        score += 0.5
    if (doubles + triples + hr) >= 2:
        score += 0.5
    if hr >= 2:
        score += 1.5
    if rbi >= 3:
        score += 0.5
    if sb >= 2:
        score += 1.0
    return round(score, 3)


def score_pitcher(ip: float, k: int, er: int, bb: int, h: int,
                  hr_allowed: int, win: int, save: int) -> float:
    score = (
        ip * 3.0 + k * 1.5 - er * 2.0 - bb * 1.0
        - h * 0.5 - hr_allowed * 1.0 + win * 2.0 + save * 2.0
    )
    if er == 0 and ip >= 5.0:
        score += 1.0
    if k >= 10:
        score += 1.0
    if ip >= 6.0:
        score += 1.0
    return round(score, 3)


# ── PERFORMANCE EXTRACTION ────────────────────────────────────────────────────

def extract_performances(boxscore: dict, level_label: str, sport_id: int) -> list[dict]:
    results: list[dict] = []
    teams = boxscore.get("teams") or {}

    for side in ("home", "away"):
        team_block = teams.get(side) or {}
        team_meta  = team_block.get("team") or {}
        team_id    = safe_int(team_meta.get("id"))
        fallback   = str(
            team_meta.get("abbreviation") or team_meta.get("teamCode")
            or team_meta.get("name") or "UNK"
        ).upper()
        org = fetch_team_org_abbrev(team_id, fallback) if team_id else fallback

        for player_key, player in (team_block.get("players") or {}).items():
            person    = player.get("person") or {}
            player_id = safe_int(person.get("id") or player_key.replace("ID", ""))
            name      = person.get("fullName") or person.get("lastName") or f"Player {player_id}"

            prospect = find_prospect(player_id, name)
            if not prospect:
                continue
            if is_rehab_assignment(player_id):
                log(f"Skipping {name} — rehab assignment")
                continue

            stats         = player.get("stats") or {}
            position      = player.get("position") or {}
            position_code = str(position.get("abbreviation") or "")
            batting        = stats.get("batting") or {}
            pitching       = stats.get("pitching") or {}

            if batting:
                h       = safe_int(batting.get("hits"))
                doubles = safe_int(batting.get("doubles"))
                triples = safe_int(batting.get("triples"))
                hr      = safe_int(batting.get("homeRuns"))
                rbi     = safe_int(batting.get("rbi"))
                r       = safe_int(batting.get("runs"))
                bb      = safe_int(batting.get("baseOnBalls"))
                hbp     = safe_int(batting.get("hitByPitch"))
                sb      = safe_int(batting.get("stolenBases"))
                so      = safe_int(batting.get("strikeOuts"))
                ab      = safe_int(batting.get("atBats"))
                pa      = safe_int(
                    batting.get("plateAppearances")
                    or batting.get("pa")
                    or batting.get("battersFaced")
                )
                if pa < MIN_HITTER_PA:
                    continue
                scr = score_hitter(h, doubles, triples, hr, rbi, r, bb, hbp, sb, so)
                if scr < MIN_HITTER_SCORE:
                    continue
                results.append({
                    "type": "hitter",
                    "player_id": player_id,
                    "name": name,
                    "org": org,
                    "level": level_label,
                    "sport_id": sport_id,
                    "prospect": prospect,
                    "score": scr,
                    "stats": {
                        "h": h, "ab": ab, "pa": pa,
                        "2b": doubles, "3b": triples, "hr": hr,
                        "rbi": rbi, "r": r, "bb": bb,
                        "hbp": hbp, "sb": sb, "so": so,
                    },
                })

            if pitching and position_code not in POSITION_PLAYER_PITCHING_CODES:
                ip          = parse_ip(pitching.get("inningsPitched"))
                ip_display  = str(pitching.get("inningsPitched") or "0.0")
                k           = safe_int(pitching.get("strikeOuts"))
                er          = safe_int(pitching.get("earnedRuns"))
                bb          = safe_int(pitching.get("baseOnBalls"))
                h           = safe_int(pitching.get("hits"))
                hr_allowed  = safe_int(pitching.get("homeRuns"))
                win         = 1 if safe_int(pitching.get("wins")) > 0 else 0
                save        = 1 if safe_int(pitching.get("saves")) > 0 else 0
                if ip < MIN_PITCHER_IP:
                    continue
                scr = score_pitcher(ip, k, er, bb, h, hr_allowed, win, save)
                if scr < MIN_PITCHER_SCORE:
                    continue
                results.append({
                    "type": "pitcher",
                    "player_id": player_id,
                    "name": name,
                    "org": org,
                    "level": level_label,
                    "sport_id": sport_id,
                    "prospect": prospect,
                    "score": scr,
                    "stats": {
                        "ip": ip, "ip_display": ip_display,
                        "k": k, "er": er, "bb": bb,
                        "h": h, "hr_allowed": hr_allowed,
                        "win": win, "save": save,
                    },
                })

    return results


# ── HEADSHOT ──────────────────────────────────────────────────────────────────

def load_headshot_index() -> None:
    global _headshot_index, _headshot_loaded
    if _headshot_loaded:
        return
    _headshot_loaded = True
    path = Path(ESPN_PLAYER_IDS_PATH)
    if not path.exists():
        log(f"Headshot index not found: {ESPN_PLAYER_IDS_PATH}")
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"Could not load headshot index: {exc}")
        return
    if not isinstance(raw, dict):
        return

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
            norm = normalize_name(raw_name)
            _headshot_index.setdefault(norm, []).append({"headshot_url": headshot_url})

    log(f"Loaded headshot index: {len(_headshot_index)} entries")


def get_headshot(name: str) -> str | None:
    norm = normalize_name(name)
    entries = _headshot_index.get(norm, [])
    if entries:
        return entries[0].get("headshot_url")
    # Last-name fallback
    parts = norm.split()
    if not parts:
        return None
    last = parts[-1]
    for key, ents in _headshot_index.items():
        key_parts = key.split()
        if key_parts and key_parts[-1] == last and ents:
            return ents[0].get("headshot_url")
    return None


def get_team_logo(org: str) -> str | None:
    try:
        return _get_team_logo_espn(org)
    except Exception:
        return None


# ── FORMAT HELPERS ────────────────────────────────────────────────────────────

def format_hitter_game_line(stats: dict) -> str:
    parts = [f"{stats['h']}-for-{stats['ab']}"]
    if stats["hr"]:
        parts.append(f"{stats['hr']} HR")
    if stats["2b"]:
        parts.append(f"{stats['2b']} 2B")
    if stats["3b"]:
        parts.append(f"{stats['3b']} 3B")
    if stats["rbi"]:
        parts.append(f"{stats['rbi']} RBI")
    if stats["bb"]:
        parts.append(f"{stats['bb']} BB")
    if stats["sb"]:
        parts.append(f"{stats['sb']} SB")
    if stats["r"]:
        parts.append(f"{stats['r']} R")
    return ", ".join(parts)


def format_pitcher_game_line(stats: dict) -> str:
    parts = [f"{stats['ip_display']} IP", f"{stats['k']} K", f"{stats['er']} ER"]
    if stats["bb"]:
        parts.append(f"{stats['bb']} BB")
    if stats["h"]:
        parts.append(f"{stats['h']} H")
    if stats["win"]:
        parts.append("W")
    if stats["save"]:
        parts.append("SV")
    return ", ".join(parts)


def format_hitter_season_line(season: dict) -> str | None:
    if not season:
        return None
    avg  = season.get("avg", "-.---")
    hr   = safe_int(season.get("homeRuns"))
    rbi  = safe_int(season.get("rbi"))
    ops  = season.get("ops", "-.---")
    sb   = safe_int(season.get("stolenBases"))
    parts = [f"{avg} AVG", f"{hr} HR", f"{rbi} RBI", f"{ops} OPS"]
    if sb:
        parts.append(f"{sb} SB")
    return " • ".join(parts)


def format_pitcher_season_line(season: dict) -> str | None:
    if not season:
        return None
    era  = season.get("era", "-.--")
    w    = safe_int(season.get("wins"))
    l    = safe_int(season.get("losses"))
    k    = safe_int(season.get("strikeOuts"))
    ip   = season.get("inningsPitched", "0.0")
    whip = season.get("whip", "-.--")
    return f"{w}-{l}, {era} ERA, {whip} WHIP, {k} K, {ip} IP"


def game_score_string(game: dict) -> str:
    teams = game.get("teams") or {}
    home  = teams.get("home") or {}
    away  = teams.get("away") or {}
    home_abbr = str((home.get("team") or {}).get("abbreviation") or "HOME").upper()
    away_abbr = str((away.get("team") or {}).get("abbreviation") or "AWAY").upper()
    # Scores live in linescore sub-object when hydrate=linescore is used
    linescore = game.get("linescore") or {}
    ls_teams  = linescore.get("teams") or {}
    home_runs = safe_int((ls_teams.get("home") or {}).get("runs"))
    away_runs = safe_int((ls_teams.get("away") or {}).get("runs"))
    if home_runs > away_runs:
        return f"{home_abbr} {home_runs}, {away_abbr} {away_runs}"
    if away_runs > home_runs:
        return f"{away_abbr} {away_runs}, {home_abbr} {home_runs}"
    return f"{home_abbr} {home_runs}, {away_abbr} {away_runs}"


# ── CLAUDE BLURB ──────────────────────────────────────────────────────────────

def _generate_blurb_sync(perf: dict, score_str: str, play_context: str, recent_line: str) -> tuple[str, str]:
    """Returns (headline, blurb). Both empty strings if no API key or on error."""
    if not ANTHROPIC_API_KEY:
        return "", ""
    try:
        ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        name  = perf["name"]
        rank  = perf["prospect"].get("rank", "?")
        org   = perf["org"]
        level = perf["level"]

        banned = (
            "Do not use em dashes, the phrase 'down the stretch', "
            "'late in the season', 'playoff push', or any language implying late-season context. "
            "It is early April. "
            "When referencing his rank, always make clear it is his overall MLB prospect ranking, "
            "not his team rank (e.g. 'the #49 overall prospect in baseball', not 'the 49th-ranked prospect in the Dodgers system')."
        )

        play_section = (
            f"Key situational moments from the game: {play_context}\n"
            if play_context else ""
        )
        recent_section = (
            f"Recent form: {recent_line}\n"
            if recent_line else ""
        )

        if perf["type"] == "hitter":
            line = format_hitter_game_line(perf["stats"])
            prompt = (
                f"You are writing for a fantasy baseball Discord. "
                f"Respond with exactly two parts, each separated by a blank line.\n\n"
                f"{play_section}"
                f"{recent_section}"
                f"Part 1: A punchy 6-10 word headline for {name} (#{rank} overall MLB prospect, {org} {level}) "
                f"who just went {line} (final: {score_str}). No quotes, no colons. {banned}\n\n"
                f"Part 2: 2 to 3 sentences in beat-writer voice. Use the situational details and recent "
                f"form above to add context — is this part of a hot streak, a bounce-back, or a breakout? "
                f"Reference the inning or game situation where relevant. "
                f"Naturally reference his prospect pedigree or upside. {banned}"
            )
        else:
            line = format_pitcher_game_line(perf["stats"])
            prompt = (
                f"You are writing for a fantasy baseball Discord. "
                f"Respond with exactly two parts, each separated by a blank line.\n\n"
                f"{play_section}"
                f"{recent_section}"
                f"Part 1: A punchy 6-10 word headline for {name} (#{rank} overall MLB prospect, {org} {level}) "
                f"who just threw {line} (final: {score_str}). No quotes, no colons. {banned}\n\n"
                f"Part 2: 2 to 3 sentences in beat-writer voice. Use the situational details and recent "
                f"form above — is he on a run of dominant starts, or is this a statement outing? "
                f"Reference key moments or his strikeout sequences where relevant. "
                f"Naturally reference his prospect pedigree or stuff. {banned}"
            )

        msg = ai.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=250,
            messages=[{"role": "user", "content": prompt}],
        )
        text  = msg.content[0].text.strip()
        parts = text.split("\n\n", 1)
        headline = parts[0].strip() if len(parts) > 0 else ""
        blurb    = parts[1].strip() if len(parts) > 1 else ""
        return headline, blurb
    except Exception as exc:
        log(f"Claude blurb failed for {perf['name']}: {exc}")
        return "", ""


async def generate_blurb(perf: dict, score_str: str, play_context: str, recent_line: str) -> tuple[str, str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _generate_blurb_sync, perf, score_str, play_context, recent_line)


# ── EMBED BUILDER ─────────────────────────────────────────────────────────────

def build_embed(perf: dict, score_str: str, headline: str, blurb: str,
                season_stats: dict | None) -> discord.Embed:
    prospect = perf["prospect"]
    rank     = prospect.get("rank", "?")
    name     = perf["name"]
    org      = perf["org"]
    level    = perf["level"]
    sport_id = perf["sport_id"]
    color    = LEVEL_CONFIG[sport_id]["color"]

    if perf["type"] == "hitter":
        game_line   = format_hitter_game_line(perf["stats"])
        season_line = format_hitter_season_line(season_stats)
    else:
        game_line   = format_pitcher_game_line(perf["stats"])
        season_line = format_pitcher_season_line(season_stats)

    embed = discord.Embed(color=color, timestamp=datetime.now(timezone.utc))

    logo_url = get_team_logo(org)
    try:
        embed.set_author(name=f"{name} | {org} {level}", icon_url=logo_url)
    except Exception:
        embed.set_author(name=f"{name} | {org} {level}")

    player_id = perf.get("player_id")
    mlb_headshot = (
        f"https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"d_people:generic:headshot:67:current.png/w_213,q_auto:best/"
        f"v1/people/{player_id}/headshot/67/current"
        if player_id else None
    )
    headshot = mlb_headshot or get_headshot(name) or logo_url
    try:
        embed.set_thumbnail(url=headshot)
    except Exception:
        pass

    display_headline = headline if headline else game_line
    stats = perf.get("stats", {})
    if perf["type"] == "hitter":
        if stats.get("hr", 0):
            emoji = "💣"
        elif stats.get("sb", 0) and stats.get("h", 0) >= 2:
            emoji = "⚡"
        elif stats.get("sb", 0):
            emoji = "🏃"
        elif stats.get("rbi", 0) >= 3:
            emoji = "🔥"
        elif stats.get("h", 0) >= 3:
            emoji = "🌡️"
        else:
            emoji = "✅"
    else:
        if stats.get("er", 1) == 0 and float(stats.get("ip", 0)) >= 6:
            emoji = "🔒"
        elif stats.get("so", 0) >= 8:
            emoji = "🔥"
        elif stats.get("so", 0) >= 6:
            emoji = "⚡"
        else:
            emoji = "✅"
    embed.add_field(name="", value=f"**{emoji} {display_headline}**", inline=False)

    if blurb:
        embed.add_field(name="Summary", value=blurb, inline=False)

    embed.add_field(name="Game Line", value=game_line, inline=False)

    if season_line:
        embed.add_field(name="Season", value=season_line, inline=False)


    now_et = datetime.now(ET)
    embed.set_footer(
        text=f"#{rank} Prospect • {org} {level} • {now_et.strftime('%-I:%M %p ET')}"
    )
    return embed


# ── STATE ─────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if RESET_STATE:
        return {"posted": {}}
    path = Path(STATE_FILE)
    if not path.exists():
        return {"posted": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {"posted": {}}


def save_state(state: dict) -> None:
    path = Path(STATE_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def is_posted(state: dict, date_str: str, game_pk: int, player_id: int) -> bool:
    key = f"{game_pk}_{player_id}"
    return bool(state.get("posted", {}).get(date_str, {}).get(key))


def mark_posted(state: dict, date_str: str, game_pk: int, player_id: int) -> None:
    state.setdefault("posted", {}).setdefault(date_str, {})[f"{game_pk}_{player_id}"] = True


def prune_state(state: dict) -> None:
    cutoff = (date.today() - timedelta(days=3)).isoformat()
    stale  = [k for k in state.get("posted", {}) if k < cutoff]
    for k in stale:
        del state["posted"][k]


# ── SLEEP HELPERS ─────────────────────────────────────────────────────────────

def now_et() -> datetime:
    return datetime.now(ET)


def is_sleep_window() -> bool:
    h = now_et().hour
    return SLEEP_START_HOUR_ET <= h < SLEEP_END_HOUR_ET


def seconds_until_wake() -> int:
    now  = now_et()
    wake = now.replace(hour=SLEEP_END_HOUR_ET, minute=0, second=0, microsecond=0)
    if now >= wake:
        wake = (now + timedelta(days=1)).replace(
            hour=SLEEP_END_HOUR_ET, minute=0, second=0, microsecond=0
        )
    return max(1, int((wake - now).total_seconds()))


# ── MAIN POLL LOOP ────────────────────────────────────────────────────────────

async def scan_and_post(state: dict, channel, date_str: str | None = None) -> int:
    if date_str is None:
        date_str = date.today().isoformat()
    cards: list[tuple[dict, int, dict]] = []  # (game, game_pk, perf)

    for sport_id, cfg in LEVEL_CONFIG.items():
        level_label = cfg["label"]
        try:
            games = fetch_schedule(sport_id, date_str)
        except Exception as exc:
            log(f"Schedule fetch failed [{level_label}]: {exc}")
            continue

        final_games = [
            g for g in games
            if (
                (g.get("status") or {}).get("codedGameState") in STATUS_FINAL_CODES
                or (g.get("status") or {}).get("abstractGameState", "").lower() == "final"
                or "final" in (g.get("status") or {}).get("detailedState", "").lower()
            )
        ]
        log(f"[{level_label}] {len(final_games)} completed games for {date_str}")

        for game in final_games:
            game_pk = safe_int(game.get("gamePk"))
            if not game_pk:
                continue
            try:
                boxscore = fetch_boxscore(game_pk)
            except Exception as exc:
                log(f"Boxscore fetch failed game={game_pk}: {exc}")
                continue

            for perf in extract_performances(boxscore, level_label, sport_id):
                if not is_posted(state, date_str, game_pk, perf["player_id"]):
                    cards.append((game, game_pk, perf))

    # Best performances first; cap total posts per scan
    cards.sort(key=lambda x: x[2]["score"], reverse=True)
    cards = cards[:MAX_POSTS_PER_SCAN]
    log(f"Cards to post: {len(cards)}")

    # Cache live feeds per game_pk so multiple prospects from same game share one fetch
    feed_cache: dict[int, dict] = {}

    posted_count = 0
    for game, game_pk, perf in cards:
        try:
            score_str    = game_score_string(game)
            group        = "hitting" if perf["type"] == "hitter" else "pitching"
            season_stats = fetch_season_stats(perf["player_id"], group, perf["sport_id"])

            if game_pk not in feed_cache:
                try:
                    feed_cache[game_pk] = fetch_live_feed(game_pk)
                except Exception as exc:
                    log(f"Live feed fetch failed game={game_pk}: {exc}")
                    feed_cache[game_pk] = {}
            play_context = extract_player_plays(
                feed_cache[game_pk], perf["player_id"], perf["type"]
            )

            recent_stat = fetch_recent_stats(perf["player_id"], group, perf["sport_id"])
            if perf["type"] == "hitter":
                recent_line = format_recent_hitter_line(recent_stat)
            else:
                recent_line = format_recent_pitcher_line(recent_stat)

            headline, blurb = await generate_blurb(perf, score_str, play_context, recent_line)
            embed = build_embed(perf, score_str, headline, blurb, season_stats)

            await channel.send(embed=embed)
            mark_posted(state, date_str, game_pk, perf["player_id"])
            save_state(state)
            posted_count += 1
            log(
                f"Posted: {perf['name']} | {perf['org']} {perf['level']} "
                f"| score={perf['score']} | rank=#{perf['prospect'].get('rank')}"
            )
            await asyncio.sleep(POST_DELAY_SECONDS)
        except Exception as exc:
            log_exception(f"Failed to post card for {perf['name']}: {exc}")

    return posted_count


async def prospect_loop() -> None:
    assert client is not None
    await client.wait_until_ready()

    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        log(f"Channel {CHANNEL_ID} not found — aborting loop")
        return

    state = load_state()
    log("Prospect poll loop started")

    # On first boot, scan yesterday's completed games for immediate feedback
    yesterday_str = (date.today() - timedelta(days=1)).isoformat()
    log(f"Running startup scan for {yesterday_str}")
    try:
        _rehab_cache.clear()
        count = await scan_and_post(state, channel, date_str=yesterday_str)
        log(f"Startup scan complete — {count} card(s) posted from {yesterday_str}")
    except Exception as exc:
        log_exception(f"Startup scan error: {exc}")

    while not client.is_closed():
        if is_sleep_window():
            secs = seconds_until_wake()
            log(f"Sleep window — waking in {secs // 3600}h {(secs % 3600) // 60}m")
            await asyncio.sleep(secs)
            state = load_state()
            continue

        try:
            _rehab_cache.clear()
            prune_state(state)
            count = await scan_and_post(state, channel)
            log(f"Scan complete — {count} card(s) posted")
        except Exception as exc:
            log_exception(f"Scan error: {exc}")

        await asyncio.sleep(POLL_MINUTES * 60)


# ── DISCORD BOT ───────────────────────────────────────────────────────────────

intents = discord.Intents.default()


async def on_ready() -> None:
    global background_task
    assert client is not None
    log(f"Logged in as {client.user}")
    await client.change_presence(status=discord.Status.invisible)
    if background_task is None or background_task.done():
        background_task = asyncio.create_task(prospect_loop())
        log("Prospect background task started")


async def start_prospect_watch_bot() -> None:
    global client, background_task
    if not TOKEN:
        raise RuntimeError("ANALYTIC_BOT_TOKEN is required")
    if CHANNEL_ID <= 0:
        raise RuntimeError("PROSPECT_WATCH_CHANNEL_ID is required")

    load_prospects()
    load_headshot_index()

    background_task = None
    client = discord.Client(intents=intents)
    client.event(on_ready)
    await client.start(TOKEN, reconnect=False)


if __name__ == "__main__":
    asyncio.run(start_prospect_watch_bot())

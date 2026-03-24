#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams?limit=1000"
TEAM_URL = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}"
ROSTER_URL = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/roster"
HEADSHOT_URL = "https://a.espncdn.com/i/headshots/mlb/players/full/{espn_id}.png"
TIMEOUT = 20
SLEEP = 0.15

# Shared output location by default.
DEFAULT_OUTPUT = Path("../shared/player_ids/espn_player_ids.json")

# A few common team display aliases that show up across bots.
TEAM_ABBR_ALIASES = {
    "WSH": "WSH",
    "WAS": "WSH",
    "ATH": "ATH",
    "OAK": "ATH",
    "SAC": "ATH",
    "CWS": "CWS",
    "CHW": "CWS",
    "CHC": "CHC",
    "NYY": "NYY",
    "NYM": "NYM",
    "LAD": "LAD",
    "LAA": "LAA",
    "SD": "SD",
    "SDP": "SD",
    "SF": "SF",
    "SFG": "SF",
    "TB": "TB",
    "TBR": "TB",
    "KC": "KC",
    "KCR": "KC",
}

_SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv)\.?$", re.IGNORECASE)
_PUNCT_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    s = name.lower().replace("’", "'").replace("`", "'")
    s = s.replace("-", " ")
    s = _SUFFIX_RE.sub(lambda m: f" {m.group(1).lower()}", s)
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def normalize_team(team: Optional[str]) -> Optional[str]:
    if not team:
        return None
    team = team.strip().upper()
    return TEAM_ABBR_ALIASES.get(team, team)


def request_json(session: requests.Session, url: str) -> Dict[str, Any]:
    resp = session.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def team_entries(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    sports = payload.get("sports") or []
    for sport in sports:
        leagues = sport.get("leagues") or []
        for league in leagues:
            for item in league.get("teams") or []:
                team = item.get("team") or item
                if team:
                    yield team


def flatten_athletes(node: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(node, dict):
        if "items" in node and isinstance(node["items"], list):
            for item in node["items"]:
                if isinstance(item, dict):
                    athlete = item.get("athlete") or item
                    if isinstance(athlete, dict):
                        yield athlete
        if "athletes" in node and isinstance(node["athletes"], list):
            for item in node["athletes"]:
                yield from flatten_athletes(item)
        if all(k in node for k in ("id", "displayName")):
            yield node
        for v in node.values():
            if isinstance(v, (dict, list)):
                yield from flatten_athletes(v)
    elif isinstance(node, list):
        for item in node:
            yield from flatten_athletes(item)


def parse_team_info(team: Dict[str, Any]) -> Dict[str, str]:
    abbr = normalize_team(team.get("abbreviation") or team.get("shortDisplayName") or team.get("name") or "")
    return {
        "team_id": str(team.get("id") or "").strip(),
        "team": abbr or "",
        "display_name": team.get("displayName") or team.get("shortDisplayName") or team.get("name") or abbr or "",
    }


def parse_position(athlete: Dict[str, Any]) -> Optional[str]:
    pos = athlete.get("position") or {}
    if isinstance(pos, dict):
        return pos.get("abbreviation") or pos.get("name")
    return None


def add_aliases(store: Dict[str, Any], canonical_name: str, entry: Dict[str, Any]) -> None:
    aliases = {canonical_name}

    # stripped punctuation / suffix variants
    lower_name = canonical_name
    aliases.add(lower_name.replace(" Jr.", " Jr").replace(" Sr.", " Sr"))
    aliases.add(lower_name.replace(" Jr", " Jr.").replace(" Sr", " Sr."))
    aliases.add(lower_name.replace(" II", " II.").replace(" III", " III."))

    # Oneil / O'Neil style normalization variants
    normalized = normalize_name(canonical_name)
    aliases.add(normalized.title())
    aliases.add(canonical_name.replace("O'", "O").replace("D'", "D"))

    for alias in {a.strip() for a in aliases if a and a.strip()}:
        existing = store.get(alias)
        if existing is None:
            store[alias] = entry.copy()
        else:
            # preserve duplicate-name collisions as a list keyed by team
            if isinstance(existing, list):
                if not any(x.get("team") == entry.get("team") and x.get("espn_id") == entry.get("espn_id") for x in existing):
                    existing.append(entry.copy())
            else:
                if existing.get("team") == entry.get("team") and existing.get("espn_id") == entry.get("espn_id"):
                    continue
                store[alias] = [existing, entry.copy()]


def build_mapping(session: requests.Session) -> Dict[str, Any]:
    teams_payload = request_json(session, TEAMS_URL)
    teams = [parse_team_info(t) for t in team_entries(teams_payload)]

    out: Dict[str, Any] = {}
    seen = set()

    for t in teams:
        team_id = t["team_id"]
        if not team_id:
            continue
        payload = None
        errors = []
        for url in (ROSTER_URL.format(team_id=team_id), TEAM_URL.format(team_id=team_id) + "?enable=roster"):
            try:
                payload = request_json(session, url)
                break
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
                payload = None
        if payload is None:
            print(f"[WARN] Skipping team {t['display_name']} ({team_id}) | {' | '.join(errors)}", file=sys.stderr)
            continue

        for athlete in flatten_athletes(payload):
            espn_id = str(athlete.get("id") or "").strip()
            full_name = (athlete.get("displayName") or athlete.get("fullName") or "").strip()
            if not espn_id or not full_name:
                continue
            key = (espn_id, t["team"])
            if key in seen:
                continue
            seen.add(key)
            entry = {
                "espn_id": int(espn_id),
                "headshot_url": HEADSHOT_URL.format(espn_id=espn_id),
                "team": t["team"],
            }
            pos = parse_position(athlete)
            if pos:
                entry["pos"] = pos
            add_aliases(out, full_name, entry)
        time.sleep(SLEEP)

    return out


def main() -> int:
    if len(sys.argv) > 1:
        output_path = Path(sys.argv[1]).expanduser()
    else:
        output_path = DEFAULT_OUTPUT

    if not output_path.is_absolute():
        output_path = (Path.cwd() / output_path).resolve()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    })

    mapping = build_mapping(session)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(mapping, indent=2, sort_keys=True), encoding="utf-8")

    single = sum(1 for v in mapping.values() if isinstance(v, dict))
    multi = sum(1 for v in mapping.values() if isinstance(v, list))
    print(f"Wrote {len(mapping)} keys to {output_path}")
    print(f"Single-entry keys: {single}")
    print(f"Duplicate-name keys: {multi}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

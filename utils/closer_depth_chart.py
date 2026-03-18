import json
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

STATE_FILE = "state/closers/closer_depth_chart.json"
URL = "https://closermonkey.com/2015/05/04/updated-closer-depth-chart/"

TEAM_ABBRS = {
    "ARI", "ATL", "BAL", "BOS", "CHC", "CWS", "CHW", "CIN", "CLE", "COL", "DET",
    "HOU", "KC", "LAA", "LAD", "MIA", "MIL", "MIN", "NYM", "NYY", "ATH",
    "PHI", "PIT", "SD", "SF", "SEA", "STL", "TB", "TEX", "TOR", "WSH", "WAS"
}

EXPECTED_TEAMS = {
    "ARI", "ATH", "ATL", "BAL", "BOS", "CHC", "CWS", "CIN", "CLE", "COL",
    "DET", "HOU", "KC", "LAA", "LAD", "MIA", "MIL", "MIN", "NYM", "NYY",
    "PHI", "PIT", "SD", "SF", "SEA", "STL", "TB", "TEX", "TOR", "WSH"
}


def _normalize_team(team: str) -> str:
    team = team.strip().upper()
    if team == "WAS":
        return "WSH"
    if team == "CHW":
        return "CWS"
    return team


def _clean_name(name: str) -> str:
    name = name.replace("*", "")
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def fetch_closer_depth_chart():
    print("[CLOSER WATCH] Fetching Closer Monkey depth chart", flush=True)

    try:
        r = requests.get(URL, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[CLOSER WATCH] Failed to load site: {e}", flush=True)
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    strings = [s.strip() for s in soup.stripped_strings if s.strip()]

    teams = {}

    start_idx = None
    end_idx = None

    for i, s in enumerate(strings):
        if "MLB Bullpen Depth Charts" in s or "Updated MLB Closer Depth Chart" in s:
            start_idx = i
            break

    if start_idx is None:
        print("[CLOSER WATCH] Could not find chart header", flush=True)
        return {}

    for i in range(start_idx, len(strings)):
        if strings[i].startswith("* = closer-by-committee"):
            end_idx = i
            break

    if end_idx is None:
        print("[CLOSER WATCH] Could not find chart footer", flush=True)
        return {}

    chart_strings = strings[start_idx:end_idx]

    first_team_idx = None
    for i, s in enumerate(chart_strings):
        token = s.upper()
        if token in TEAM_ABBRS:
            first_team_idx = i
            break

    if first_team_idx is None:
        print("[CLOSER WATCH] Could not find first team token", flush=True)
        return {}

    chart_strings = chart_strings[first_team_idx:]

    i = 0
    while i < len(chart_strings):
        token = chart_strings[i].upper()

        if token not in TEAM_ABBRS:
            i += 1
            continue

        team = _normalize_team(token)

        if i + 4 >= len(chart_strings):
            break

        closer = _clean_name(chart_strings[i + 1])
        next_man = _clean_name(chart_strings[i + 2])
        second = _clean_name(chart_strings[i + 3])
        updated = chart_strings[i + 4]

        if not re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2}", updated):
            print(
                f"[CLOSER WATCH] Bad date slot for {team}: "
                f"{closer} | {next_man} | {second} | {updated}",
                flush=True,
            )
            i += 1
            continue

        teams[team] = {
            "closer": closer,
            "next": next_man,
            "second": second,
        }

        i += 5

    if len(teams) != 30:
        missing = sorted(EXPECTED_TEAMS - set(teams.keys()))
        print(
            f"[CLOSER WATCH] Parsed {len(teams)} teams instead of 30. Missing: {missing}. Refusing to save.",
            flush=True,
        )
        return {}

    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    payload = {
        "last_update_utc": datetime.utcnow().isoformat(),
        "source": URL,
        "teams": teams,
    }

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"[CLOSER WATCH] Stored {len(teams)} bullpens in {STATE_FILE}", flush=True)
    return teams


def load_depth_chart():
    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("teams", {})
    except Exception as e:
        print(f"[CLOSER WATCH] Failed to load saved depth chart: {e}", flush=True)
        return {}


if __name__ == "__main__":
    teams = fetch_closer_depth_chart()

    if teams:
        print("[CLOSER WATCH] Sample entries:", flush=True)
        for i, (team, roles) in enumerate(sorted(teams.items())):
            print(f"{team}: {roles}", flush=True)
            if i >= 4:
                break

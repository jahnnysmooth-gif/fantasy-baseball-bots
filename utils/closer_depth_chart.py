import json
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

STATE_FILE = "state/closers/closer_depth_chart.json"
URL = "https://closermonkey.com"

TEAM_NAME_TO_ABBR = {
    "Arizona Diamondbacks": "ARI",
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
    "Athletics": "ATH",
    "Oakland Athletics": "ATH",
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

VALID_TEAM_ABBRS = set(TEAM_NAME_TO_ABBR.values())


def _clean_cell_text(td):
    return " ".join(td.stripped_strings).strip()


def _resolve_team(raw_team: str) -> str:
    raw_team = raw_team.strip()
    if raw_team in TEAM_NAME_TO_ABBR:
        return TEAM_NAME_TO_ABBR[raw_team]
    if raw_team.upper() in VALID_TEAM_ABBRS:
        return raw_team.upper()
    return ""


def fetch_closer_depth_chart():
    print("[CLOSER WATCH] Fetching Closer Monkey depth chart", flush=True)

    try:
        r = requests.get(URL, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[CLOSER WATCH] Failed to load site: {e}", flush=True)
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    teams = {}

    # Closer Monkey has multiple tables/rows on the page.
    # We only want rows whose first column resolves to a real MLB team.
    rows = soup.select("table tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        raw_team = _clean_cell_text(cols[0])
        team = _resolve_team(raw_team)

        if not team:
            continue

        closer = _clean_cell_text(cols[1])
        next_man = _clean_cell_text(cols[2])
        second = _clean_cell_text(cols[3])

        # Ignore junk rows
        if not closer and not next_man and not second:
            continue

        teams[team] = {
            "closer": closer,
            "next": next_man,
            "second": second,
        }

    # Hard guard: if we somehow parsed nonsense again, do not save it
    if len(teams) < 25 or len(teams) > 35:
        print(f"[CLOSER WATCH] Parsed suspicious bullpen count: {len(teams)}. Refusing to save.", flush=True)
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
        for i, (team, roles) in enumerate(teams.items()):
            print(f"{team}: {roles}", flush=True)
            if i >= 4:
                break

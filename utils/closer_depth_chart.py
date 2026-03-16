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
    "The Athletics": "ATH",
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

VALID_TEAM_ABBRS = {
    "ARI", "ATL", "BAL", "BOS", "CHC", "CWS", "CIN", "CLE", "COL", "DET",
    "HOU", "KC", "LAA", "LAD", "MIA", "MIL", "MIN", "NYM", "NYY", "ATH",
    "PHI", "PIT", "SD", "SF", "SEA", "STL", "TB", "TEX", "TOR", "WSH", "WAS"
}


def _clean_cell_text(td):
    return " ".join(td.stripped_strings).strip()


def _resolve_team(raw_team: str) -> str:
    raw_team = raw_team.strip()

    if raw_team in TEAM_NAME_TO_ABBR:
        return TEAM_NAME_TO_ABBR[raw_team]

    upper = raw_team.upper()
    if upper == "WAS":
        return "WSH"
    if upper in VALID_TEAM_ABBRS:
        return upper

    return ""


def _parse_team_block(cols, start_index):
    """
    Parse one 4-column team block:
    team | closer | next | second
    Returns (team_abbr, data_dict) or (None, None)
    """
    if len(cols) < start_index + 4:
        return None, None

    raw_team = _clean_cell_text(cols[start_index])
    team = _resolve_team(raw_team)
    if not team:
        return None, None

    closer = _clean_cell_text(cols[start_index + 1])
    next_man = _clean_cell_text(cols[start_index + 2])
    second = _clean_cell_text(cols[start_index + 3])

    if not closer and not next_man and not second:
        return None, None

    return team, {
        "closer": closer,
        "next": next_man,
        "second": second,
    }


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

    rows = soup.select("table tr")

    for row in rows:
        cols = row.find_all("td")

        # We expect either:
        # 4 columns  -> one team block
        # 8+ columns -> two team blocks on same row
        if len(cols) < 4:
            continue

        # left side
        team_left, data_left = _parse_team_block(cols, 0)
        if team_left and data_left:
            teams[team_left] = data_left

        # right side
        if len(cols) >= 8:
            team_right, data_right = _parse_team_block(cols, 4)
            if team_right and data_right:
                teams[team_right] = data_right

    if len(teams) < 25 or len(teams) > 35:
        print(
            f"[CLOSER WATCH] Parsed suspicious bullpen count: {len(teams)}. Refusing to save.",
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

import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime

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


def fetch_closer_depth_chart():
    print("[CLOSER WATCH] Fetching Closer Monkey depth chart")

    try:
        r = requests.get(URL, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[CLOSER WATCH] Failed to load site: {e}")
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    teams = {}

    rows = soup.select("table tbody tr")

    if not rows:
        print("[CLOSER WATCH] No table rows found on page")
        return {}

    for row in rows:
        cols = row.find_all("td")

        if len(cols) < 4:
            continue

        raw_team = cols[0].get_text(" ", strip=True)
        team = TEAM_NAME_TO_ABBR.get(raw_team, raw_team)

        closer = cols[1].get_text(" ", strip=True)
        next_man = cols[2].get_text(" ", strip=True)
        second = cols[3].get_text(" ", strip=True)

        if not team:
            continue

        teams[team] = {
            "closer": closer,
            "next": next_man,
            "second": second,
        }

    if not teams:
        print("[CLOSER WATCH] Parsed 0 bullpens")
        return {}

    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    payload = {
        "last_update_utc": datetime.utcnow().isoformat(),
        "source": URL,
        "teams": teams,
    }

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"[CLOSER WATCH] Stored {len(teams)} bullpens in {STATE_FILE}")
    return teams


def load_depth_chart():
    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("teams", {})
    except Exception as e:
        print(f"[CLOSER WATCH] Failed to load saved depth chart: {e}")
        return {}


if __name__ == "__main__":
    teams = fetch_closer_depth_chart()

    if teams:
        print("[CLOSER WATCH] Sample entries:")
        for i, (team, roles) in enumerate(teams.items()):
            print(f"{team}: {roles}")
            if i >= 4:
                break

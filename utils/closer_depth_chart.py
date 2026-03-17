import json
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

STATE_FILE = "state/closers/closer_depth_chart.json"
URL = "https://www.fangraphs.com/roster-resource/closer-depth-chart"

TEAM_ABBRS = {
    "ARI","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET",
    "HOU","KC","LAA","LAD","MIA","MIL","MIN","NYM","NYY","ATH",
    "PHI","PIT","SD","SF","SEA","STL","TB","TEX","TOR","WSH"
}


def fetch_closer_depth_chart():
    print("[CLOSER WATCH] Fetching FanGraphs closer depth chart", flush=True)

    try:
        r = requests.get(
            URL,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[CLOSER WATCH] Failed to load FanGraphs page: {e}", flush=True)
        return {}

    soup = BeautifulSoup(r.text, "html.parser")

    teams = {}

    tables = soup.find_all("table")

    for table in tables:

        rows = table.find_all("tr")

        if not rows:
            continue

        team = None
        closer = None
        setup = []

        for row in rows:

            cols = [c.get_text(strip=True) for c in row.find_all(["td","th"])]

            if not cols:
                continue

            # Detect team header
            if len(cols) == 1 and cols[0].upper() in TEAM_ABBRS:
                team = cols[0].upper()
                closer = None
                setup = []
                continue

            if not team:
                continue

            if len(cols) < 2:
                continue

            role = cols[0]
            name = cols[1]

            if role in ["Closer","Co-Closer"] and not closer:
                closer = name

            elif role == "Setup Man":
                setup.append(name)

        if team and closer:
            teams[team] = {
                "closer": closer,
                "next": setup[0] if len(setup) > 0 else "",
                "second": setup[1] if len(setup) > 1 else "",
            }

    print(f"[CLOSER WATCH] Parsed {len(teams)} bullpens", flush=True)

    if len(teams) < 28:
        print("[CLOSER WATCH] Suspicious team count, refusing to save", flush=True)
        return {}

    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    payload = {
        "last_update_utc": datetime.utcnow().isoformat(),
        "source": URL,
        "teams": teams,
    }

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"[CLOSER WATCH] Stored {len(teams)} bullpens in {STATE_FILE}", flush=True)

    return teams


def load_depth_chart():

    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE,"r") as f:
            data = json.load(f)
            return data.get("teams",{})
    except Exception as e:
        print(f"[CLOSER WATCH] Failed to load depth chart: {e}", flush=True)
        return {}


if __name__ == "__main__":

    teams = fetch_closer_depth_chart()

    if teams:
        print("[CLOSER WATCH] Sample:", flush=True)

        for i,(team,data) in enumerate(sorted(teams.items())):
            print(team,data)

            if i >= 4:
                break

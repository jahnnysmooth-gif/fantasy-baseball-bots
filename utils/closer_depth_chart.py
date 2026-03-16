import json
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

STATE_FILE = "state/closers/closer_depth_chart.json"
BASE_URL = "https://closermonkey.com"

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

TEAM_PAGE_PATHS = {
    "/test/archives/al-east/bal/": "BAL",
    "/test/archives/al-east/bos/": "BOS",
    "/test/archives/al-east/nyy/": "NYY",
    "/test/archives/al-east/tb/": "TB",
    "/test/archives/al-east/tor/": "TOR",
    "/test/archives/al-central/chw/": "CWS",
    "/test/archives/al-central/cle/": "CLE",
    "/test/archives/al-central/det/": "DET",
    "/test/archives/al-central/kc/": "KC",
    "/test/archives/al-central/min/": "MIN",
    "/test/archives/al-west/hou/": "HOU",
    "/test/archives/al-west/laa/": "LAA",
    "/test/archives/al-west/sea/": "SEA",
    "/test/archives/al-west/tex/": "TEX",
    "/test/archives/al-west/oak/": "ATH",
    "/test/archives/nl-east/atl/": "ATL",
    "/test/archives/nl-east/mia/": "MIA",
    "/test/archives/nl-east/nym/": "NYM",
    "/test/archives/nl-east/phi/": "PHI",
    "/test/archives/nl-east/was/": "WSH",
    "/test/archives/nl-central/chc/": "CHC",
    "/test/archives/nl-central/cin/": "CIN",
    "/test/archives/nl-central/mil/": "MIL",
    "/test/archives/nl-central/pit/": "PIT",
    "/test/archives/nl-central/stl/": "STL",
    "/test/archives/nl-west/ari/": "ARI",
    "/test/archives/nl-west/col/": "COL",
    "/test/archives/nl-west/lad/": "LAD",
    "/test/archives/nl-west/sd/": "SD",
    "/test/archives/nl-west/sf/": "SF",
}


def _clean_name(name: str) -> str:
    name = name.replace("*", "")
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def _extract_team_links(home_html: str) -> dict:
    soup = BeautifulSoup(home_html, "html.parser")
    links = {}

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href in TEAM_PAGE_PATHS:
            links[TEAM_PAGE_PATHS[href]] = href

    return links


def _parse_team_page(team_abbr: str, html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Look for the line after the header:
    # Closer 1st in line 2nd in line 2027 Closer Updated
    # BAL Ryan Helsley Tyler Wells Keegan Akin TBD 3/15/26
    pattern = re.compile(
        rf"{team_abbr}\s+(.*?)\s+(.*?)\s+(.*?)\s+(?:TBD\s+)?\d{{1,2}}/\d{{1,2}}/\d{{2}}",
        re.DOTALL,
    )

    m = pattern.search(text)
    if not m:
        return None

    return {
        "closer": _clean_name(m.group(1)),
        "next": _clean_name(m.group(2)),
        "second": _clean_name(m.group(3)),
    }


def fetch_closer_depth_chart():
    print("[CLOSER WATCH] Fetching Closer Monkey depth chart", flush=True)

    try:
        r = requests.get(BASE_URL, timeout=20)
        r.raise_for_status()
        home_html = r.text
    except requests.RequestException as e:
        print(f"[CLOSER WATCH] Failed to load homepage: {e}", flush=True)
        return {}

    team_links = _extract_team_links(home_html)
    if len(team_links) < 25:
        print(f"[CLOSER WATCH] Found only {len(team_links)} team links. Refusing to save.", flush=True)
        return {}

    teams = {}

    for team_abbr, path in team_links.items():
        try:
            r = requests.get(f"{BASE_URL}{path}", timeout=20)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"[CLOSER WATCH] Failed to load {team_abbr} page: {e}", flush=True)
            continue

        parsed = _parse_team_page(team_abbr, r.text)
        if not parsed:
            print(f"[CLOSER WATCH] Failed to parse team page for {team_abbr}", flush=True)
            continue

        teams[team_abbr] = parsed

    if len(teams) < 25 or len(teams) > 35:
        print(f"[CLOSER WATCH] Parsed suspicious bullpen count: {len(teams)}. Refusing to save.", flush=True)
        return {}

    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    payload = {
        "last_update_utc": datetime.utcnow().isoformat(),
        "source": BASE_URL,
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

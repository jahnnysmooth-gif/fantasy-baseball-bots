import json
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

STATE_FILE = "state/closers/closer_depth_chart.json"
URL = "https://closermonkey.com"

TEAM_ABBRS = {
    "ARI", "ATL", "BAL", "BOS", "CHC", "CWS", "CIN", "CLE", "COL", "DET",
    "HOU", "KC", "LAA", "LAD", "MIA", "MIL", "MIN", "NYM", "NYY", "ATH",
    "PHI", "PIT", "SD", "SF", "SEA", "STL", "TB", "TEX", "TOR", "WSH", "WAS"
}


def _normalize_team(team: str) -> str:
    team = team.strip().upper()
    if team == "WAS":
        return "WSH"
    return team


def _clean_player_name(name: str) -> str:
    name = name.strip()
    # remove committee asterisks like *Jax*
    name = name.replace("*", "")
    # collapse whitespace
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def _extract_chart_lines(text: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]

    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if "Updated MLB Closer Depth Chart" in line:
            start_idx = i
            break

    if start_idx is None:
        return []

    for i in range(start_idx, len(lines)):
        if lines[i].startswith("* = closer-by-committee"):
            end_idx = i
            break

    if end_idx is None:
        return []

    return lines[start_idx:end_idx]


def _parse_depth_chart_from_text(text: str) -> dict:
    lines = _extract_chart_lines(text)
    teams = {}

    if not lines:
        return teams

    # We only want the actual chart rows, which begin with a team abbr
    chart_rows = []
    for line in lines:
        if re.match(r"^(ARI|ATL|BAL|BOS|CHC|CWS|CIN|CLE|COL|DET|HOU|KC|LAA|LAD|MIA|MIL|MIN|NYM|NYY|ATH|PHI|PIT|SD|SF|SEA|STL|TB|TEX|TOR|WSH|WAS)\b", line):
            chart_rows.append(line)

    # Each row is two side-by-side team blocks:
    # BAL Helsley Wells Akin 3/13/26 ATL Iglesias R Suárez Lee 3/13/26
    pattern = re.compile(
        r"^(?P<t1>ARI|ATL|BAL|BOS|CHC|CWS|CIN|CLE|COL|DET|HOU|KC|LAA|LAD|MIA|MIL|MIN|NYM|NYY|ATH|PHI|PIT|SD|SF|SEA|STL|TB|TEX|TOR|WSH|WAS)\s+"
        r"(?P<c1>.+?)\s+"
        r"(?P<n1>.+?)\s+"
        r"(?P<s1>.+?)\s+"
        r"(?P<d1>\d{1,2}/\d{1,2}/\d{2})\s+"
        r"(?P<t2>ARI|ATL|BAL|BOS|CHC|CWS|CIN|CLE|COL|DET|HOU|KC|LAA|LAD|MIA|MIL|MIN|NYM|NYY|ATH|PHI|PIT|SD|SF|SEA|STL|TB|TEX|TOR|WSH|WAS)\s+"
        r"(?P<c2>.+?)\s+"
        r"(?P<n2>.+?)\s+"
        r"(?P<s2>.+?)\s+"
        r"(?P<d2>\d{1,2}/\d{1,2}/\d{2})$"
    )

    for row in chart_rows:
        m = pattern.match(row)
        if not m:
            print(f"[CLOSER WATCH] Could not parse row: {row}", flush=True)
            continue

        t1 = _normalize_team(m.group("t1"))
        t2 = _normalize_team(m.group("t2"))

        teams[t1] = {
            "closer": _clean_player_name(m.group("c1")),
            "next": _clean_player_name(m.group("n1")),
            "second": _clean_player_name(m.group("s1")),
        }

        teams[t2] = {
            "closer": _clean_player_name(m.group("c2")),
            "next": _clean_player_name(m.group("n2")),
            "second": _clean_player_name(m.group("s2")),
        }

    return teams


def fetch_closer_depth_chart():
    print("[CLOSER WATCH] Fetching Closer Monkey depth chart", flush=True)

    try:
        r = requests.get(URL, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[CLOSER WATCH] Failed to load site: {e}", flush=True)
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    teams = _parse_depth_chart_from_text(page_text)

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

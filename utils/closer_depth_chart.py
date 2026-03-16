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
    text = soup.get_text(" ", strip=True)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)

    # Find the chart section
    start_marker = "Updated MLB Closer Depth Chart"
    end_marker = "* = closer-by-committee"

    start_idx = text.find(start_marker)
    end_idx = text.find(end_marker)

    if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
        print("[CLOSER WATCH] Could not locate depth chart text block", flush=True)
        return {}

    chart_text = text[start_idx:end_idx]

    # Break into tokens
    tokens = chart_text.split()

    # Keep only from first team abbreviation onward
    first_team_idx = None
    for i, tok in enumerate(tokens):
        if tok.upper() in TEAM_ABBRS:
            first_team_idx = i
            break

    if first_team_idx is None:
        print("[CLOSER WATCH] No team abbreviations found in chart text", flush=True)
        return {}

    tokens = tokens[first_team_idx:]

    teams = {}
    i = 0

    while i < len(tokens):
        tok = tokens[i].upper()

        if tok not in TEAM_ABBRS:
            i += 1
            continue

        team = _normalize_team(tok)
        i += 1

        fields = []
        current = []

        # We want: closer, next, second, date
        while i < len(tokens) and len(fields) < 4:
            t = tokens[i]

            # stop if another team starts unexpectedly
            if t.upper() in TEAM_ABBRS and len(fields) < 3:
                break

            # date ends a team block
            if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2}", t):
                if current:
                    fields.append(" ".join(current).strip())
                    current = []
                fields.append(t)
                i += 1
                break

            # separator "/" splits closer / next / second
            if t == "/":
                fields.append(" ".join(current).strip())
                current = []
            else:
                current.append(t)

            i += 1

        if current:
            fields.append(" ".join(current).strip())

        # Need at least closer / next / second
        if len(fields) >= 3:
            closer = _clean_player_name(fields[0])
            next_man = _clean_player_name(fields[1])
            second = _clean_player_name(fields[2])

            teams[team] = {
                "closer": closer,
                "next": next_man,
                "second": second,
            }

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

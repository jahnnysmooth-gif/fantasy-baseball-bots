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
    "PHI", "PIT", "SD", "SF", "SEA", "STL", "TB", "TEX", "TOR", "WSH", "WAS",
}

DATE_RE = r"\d{1,2}/\d{1,2}/\d{2}"
TEAM_RE = r"(?:ARI|ATL|BAL|BOS|CHC|CWS|CIN|CLE|COL|DET|HOU|KC|LAA|LAD|MIA|MIL|MIN|NYM|NYY|ATH|PHI|PIT|SD|SF|SEA|STL|TB|TEX|TOR|WSH|WAS)"


def _normalize_team(team: str) -> str:
    team = team.strip().upper()
    if team == "WAS":
        return "WSH"
    return team


def _clean_name(name: str) -> str:
    name = name.replace("*", "")
    name = re.sub(r"\s+", " ", name)
    return name.strip(" -–—\u00a0")


def _tokenize_payload(payload: str) -> list[str]:
    payload = payload.replace("\xa0", " ")
    payload = re.sub(r"\s+", " ", payload).strip()
    if not payload:
        return []
    return payload.split()


def _consume_name(tokens: list[str], i: int, remaining_names: int) -> tuple[str, int]:
    remaining_tokens = len(tokens) - i
    if remaining_tokens <= 0:
        return "", i

    tok = tokens[i]

    # single-token fallback when counts line up exactly
    if remaining_tokens == remaining_names:
        return tok, i + 1

    # initial + surname, e.g. R. Suarez or R Suarez
    if remaining_tokens >= 2:
        if re.fullmatch(r"[A-Za-z]\.?", tok):
            return f"{tok} {tokens[i + 1]}", i + 2

    # suffix handling, e.g. Leiter Jr.
    if remaining_tokens >= 2 and tokens[i + 1] in {"Jr.", "Sr.", "II", "III", "IV"}:
        return f"{tok} {tokens[i + 1]}", i + 2

    # if we have one extra token beyond remaining names, use 2-token name here
    if remaining_tokens == remaining_names + 1:
        return f"{tok} {tokens[i + 1]}", i + 2

    # otherwise default to single token
    return tok, i + 1


def _parse_payload(payload: str) -> tuple[str, str, str] | None:
    payload = _clean_name(payload)
    if not payload:
        return None

    # Committee-marked rows often wrap names in stars
    starred = [_clean_name(x) for x in re.findall(r"\*([^*]+)\*", payload) if _clean_name(x)]
    if len(starred) == 3:
        return starred[0], starred[1], starred[2]

    tokens = _tokenize_payload(payload)
    if len(tokens) < 3:
        return None

    names = []
    i = 0
    for remaining_names in (3, 2, 1):
        name, i = _consume_name(tokens, i, remaining_names)
        if not name:
            return None
        names.append(_clean_name(name))

    # if there are leftover tokens, append them to the last name
    if i < len(tokens):
        names[-1] = _clean_name(f"{names[-1]} {' '.join(tokens[i:])}")

    if len(names) != 3 or not all(names):
        return None

    return names[0], names[1], names[2]


def _extract_chart_text(soup: BeautifulSoup) -> str:
    strings = [s.strip() for s in soup.stripped_strings if s.strip()]

    start_idx = None
    end_idx = None

    for i, s in enumerate(strings):
        if "Updated MLB Closer Depth Chart" in s:
            start_idx = i
            break

    if start_idx is None:
        print("[CLOSER WATCH] Could not find chart header", flush=True)
        return ""

    for i in range(start_idx, len(strings)):
        if strings[i].startswith("* = closer-by-committee"):
            end_idx = i
            break

    if end_idx is None:
        print("[CLOSER WATCH] Could not find chart footer", flush=True)
        return ""

    chart_strings = strings[start_idx + 1:end_idx]

    # remove repeated header labels if present
    filtered = []
    skip_labels = {"Closer", "1st in line", "2nd in line", "Updated"}
    for s in chart_strings:
        if s in skip_labels:
            continue
        filtered.append(s)

    text = " ".join(filtered)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_chart_text(chart_text: str) -> dict:
    teams = {}

    if not chart_text:
        return teams

    # pull blocks like:
    # ATL Iglesias R. Suarez Lee 3/13/26
    # TB *Jax*Cleavinger*Baker 3/13/26
    pattern = re.compile(
        rf"\b(?P<team>{TEAM_RE})\b\s+"
        rf"(?P<payload>.*?)\s+"
        rf"(?P<date>{DATE_RE})"
        rf"(?=\s+\b(?:{TEAM_RE})\b\s+|$)"
    )

    for m in pattern.finditer(chart_text):
        team = _normalize_team(m.group("team"))
        payload = m.group("payload").strip()
        updated = m.group("date").strip()

        if team not in TEAM_ABBRS:
            continue

        if not re.fullmatch(DATE_RE, updated):
            print(f"[CLOSER WATCH] Bad date slot for {team}: {updated}", flush=True)
            continue

        parsed = _parse_payload(payload)
        if not parsed:
            print(f"[CLOSER WATCH] Could not parse payload for {team}: {payload}", flush=True)
            continue

        closer, next_man, second = parsed
        teams[team] = {
            "closer": closer,
            "next": next_man,
            "second": second,
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
    chart_text = _extract_chart_text(soup)

    if not chart_text:
        return {}

    teams = _parse_chart_text(chart_text)

    if len(teams) != 30:
        expected = {_normalize_team(t) for t in TEAM_ABBRS if t != "WAS"}
        missing = sorted(expected - set(teams.keys()))
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

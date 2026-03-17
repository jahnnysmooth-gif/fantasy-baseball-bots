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

DATE_RE = r"\d{1,2}/\d{1,2}/\d{2}"


def _normalize_team(team: str) -> str:
    team = team.strip().upper()
    if team == "WAS":
        return "WSH"
    return team


def _clean_name(name: str) -> str:
    name = name.replace("*", "")
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def _extract_chart_lines(soup: BeautifulSoup) -> list[str]:
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if "Updated MLB Closer Depth Chart" in line:
            start_idx = i
            break

    if start_idx is None:
        print("[CLOSER WATCH] Could not find chart header", flush=True)
        return []

    for i in range(start_idx, len(lines)):
        if line := lines[i]:
            if line.startswith("* = closer-by-committee"):
                end_idx = i
                break

    if end_idx is None:
        print("[CLOSER WATCH] Could not find chart footer", flush=True)
        return []

    chart_lines = lines[start_idx + 1:end_idx]

    # drop repeated header row if present
    cleaned = []
    for line in chart_lines:
        if "Closer 1st in line 2nd in line Updated" in line:
            continue
        cleaned.append(line)

    return cleaned


def _parse_team_block(block: str):
    """
    Parse a single team block of the form:
      TB *Jax*Cleavinger*Baker 3/13/26
      ATH *Leiter Jr.*Harris*Sterner 3/13/26
      ATL Iglesias R Suárez Lee 3/13/26
      WAS *Beeter*Henry*C Pérez 3/13/26
    """
    block = re.sub(r"\s+", " ", block).strip()

    m = re.match(rf"^([A-Z]{{2,3}})\s+(.+?)\s+({DATE_RE})$", block)
    if not m:
        return None

    team = _normalize_team(m.group(1))
    payload = m.group(2).strip()

    if team not in TEAM_ABBRS:
        return None

    # Split payload into three names.
    # Strategy:
    # 1) If there are committee markers, names are often wrapped with *...*
    # 2) Otherwise fall back to space splitting with support for common multi-token names.

    star_names = re.findall(r"\*([^*]+)\*", payload)
    if len(star_names) == 3:
        closer, next_man, second = [_clean_name(x) for x in star_names]
        return team, closer, next_man, second

    if len(star_names) == 2:
        # one non-starred name remains outside the starred names
        temp = payload
        for s in star_names:
            temp = temp.replace(f"*{s}*", " ").strip()
        non_star = _clean_name(temp)
        names = [_clean_name(star_names[0]), non_star, _clean_name(star_names[1])]
        if len([n for n in names if n]) == 3:
            return team, names[0], names[1], names[2]

    if len(star_names) == 1:
        temp = payload.replace(f"*{star_names[0]}*", " ").strip()
        rest_tokens = temp.split()
        if len(rest_tokens) >= 2:
            closer = _clean_name(star_names[0])
            next_man = _clean_name(rest_tokens[0])
            second = _clean_name(" ".join(rest_tokens[1:]))
            return team, closer, next_man, second

    # Fallback for standard non-star rows.
    tokens = payload.split()

    # Handle common two-token player formats in this chart:
    # - single initial + surname: "R Suárez", "B King", "C Pérez", "E Díaz", "T Scott", "R Walker"
    # - suffix name: "Leiter Jr."
    # We parse from left to right into exactly 3 names.

    names = []
    i = 0
    while i < len(tokens) and len(names) < 3:
        tok = tokens[i]

        # suffix handling
        if i + 1 < len(tokens) and tokens[i + 1] in {"Jr.", "Sr.", "II", "III", "IV"}:
            names.append(f"{tok} {tokens[i + 1]}")
            i += 2
            continue

        # initial + surname handling
        if len(tok) == 1 and i + 1 < len(tokens):
            names.append(f"{tok} {tokens[i + 1]}")
            i += 2
            continue

        names.append(tok)
        i += 1

    if len(names) != 3:
        return None

    closer, next_man, second = [_clean_name(x) for x in names]
    return team, closer, next_man, second


def _parse_chart_lines(chart_lines: list[str]) -> dict:
    teams = {}

    # Each chart line usually contains two team blocks:
    # BAL ... 3/13/26 ATL ... 3/13/26
    pair_pattern = re.compile(
        rf"([A-Z]{{2,3}} .+? {DATE_RE})(?=[A-Z]{{2,3}} .+? {DATE_RE}|$)"
    )

    for line in chart_lines:
        line = re.sub(r"\s+", " ", line).strip()
        blocks = [b.strip() for b in pair_pattern.findall(line) if b.strip()]

        # fallback: if regex misses, try manual split by dates
        if not blocks:
            date_matches = list(re.finditer(DATE_RE, line))
            if date_matches:
                start = 0
                for dm in date_matches:
                    end = dm.end()
                    block = line[start:end].strip()
                    if block:
                        blocks.append(block)
                    start = end

        for block in blocks:
            parsed = _parse_team_block(block)
            if not parsed:
                print(f"[CLOSER WATCH] Could not parse block: {block}", flush=True)
                continue

            team, closer, next_man, second = parsed
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
    chart_lines = _extract_chart_lines(soup)

    if not chart_lines:
        return {}

    teams = _parse_chart_lines(chart_lines)

    if len(teams) != 30:
        missing = sorted({_normalize_team(t) for t in TEAM_ABBRS if t != "WAS"} - set(teams.keys()))
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

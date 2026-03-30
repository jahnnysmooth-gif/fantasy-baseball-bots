import json
import os
import re
import unicodedata

STATE_FILE = "state/closers/closer_depth_chart.json"

ROLE_CONFIG = [
    ("closer", "Closer", 5),
    ("co_closer", "Co-closer", 4),
    ("committee", "Committee", 3),
    ("setup", "Setup", 2),
    ("leverage_arm", "Leverage arm", 1),
]

TEAM_ALIASES = {
    "AZ": "ARI",
    "WSN": "WSH",
    "WAS": "WSH",
    "CHW": "CWS",
    "TBR": "TB",
    "KCR": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "OAK": "ATH",
}


def normalize_name(name: str) -> str:
    if not name:
        return ""

    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = " ".join(name.strip().split())
    return name


def _normalize_team(team: str) -> str:
    text = str(team or "").strip().upper()
    return TEAM_ALIASES.get(text, text)


def load_depth_chart():
    if not os.path.exists(STATE_FILE):
        return {}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("teams", {})


def build_tracked_relief_map():
    teams = load_depth_chart()
    tracked = {}

    for raw_team, roles in teams.items():
        team = _normalize_team(raw_team)
        role_dict = roles if isinstance(roles, dict) else {}

        for key, label, priority in ROLE_CONFIG:
            names = role_dict.get(key, []) or []
            if not isinstance(names, list):
                names = [names]

            for raw_name in names:
                name = str(raw_name or "").strip()
                if not name:
                    continue

                norm = normalize_name(name)
                if not norm:
                    continue

                existing = tracked.get(norm)
                if existing and existing.get("priority", 0) > priority:
                    continue

                tracked[norm] = {
                    "team": team,
                    "role": label,
                    "name": name,
                    "priority": priority,
                }

    for info in tracked.values():
        info.pop("priority", None)

    return tracked


if __name__ == "__main__":
    tracked = build_tracked_relief_map()
    print(f"Built tracked reliever map for {len(tracked)} pitchers")
    for i, (k, v) in enumerate(tracked.items()):
        print(k, "->", v)
        if i >= 9:
            break

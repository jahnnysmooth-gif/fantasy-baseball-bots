import json
import os
import unicodedata

STATE_FILE = "state/closers/closer_depth_chart.json"


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    return " ".join(name.lower().strip().split())


def load_depth_chart():
    if not os.path.exists(STATE_FILE):
        return {}

    with open(STATE_FILE, "r") as f:
        data = json.load(f)

    return data.get("teams", {})


def build_tracked_relief_map():
    teams = load_depth_chart()
    tracked = {}

    for team, roles in teams.items():
        closer = roles.get("closer", "").strip()
        next_man = roles.get("next", "").strip()
        second = roles.get("second", "").strip()

        if closer:
            tracked[normalize_name(closer)] = {
                "team": team,
                "role": "Closer",
                "name": closer
            }

        if next_man:
            tracked[normalize_name(next_man)] = {
                "team": team,
                "role": "Next in line",
                "name": next_man
            }

        if second:
            tracked[normalize_name(second)] = {
                "team": team,
                "role": "Second in line",
                "name": second
            }

    return tracked


if __name__ == "__main__":
    tracked = build_tracked_relief_map()
    print(f"Built tracked reliever map for {len(tracked)} pitchers")
    for i, (k, v) in enumerate(tracked.items()):
        print(k, "->", v)
        if i >= 9:
            break

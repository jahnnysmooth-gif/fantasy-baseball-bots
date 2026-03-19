import json
import os
from datetime import datetime

STATE_FILE = "state/closers/closer_depth_chart.json"
DEFAULT_OVERRIDE_FILENAME = "depth_chart_override.json"

TEAM_ALIASES = {
    "WSN": "WSH",
    "WAS": "WSH",
    "WSH": "WSH",
    "CHW": "CWS",
    "CWS": "CWS",
    "TBR": "TB",
    "TB": "TB",
}

VALID_ROLE_KEYS = {"closer", "co_closer", "committee", "setup", "leverage_arm"}


def _normalize_team(team: str) -> str:
    text = str(team or "").strip().upper()
    return TEAM_ALIASES.get(text, text)


def _clean_name(name: str) -> str:
    return " ".join(str(name or "").replace("*", "").split())


def _normalize_roles(raw_roles: dict) -> dict:
    roles = {key: [] for key in VALID_ROLE_KEYS}
    if not isinstance(raw_roles, dict):
        return roles

    for key, values in raw_roles.items():
        role_key = str(key or "").strip().lower()
        if role_key not in VALID_ROLE_KEYS:
            continue

        if isinstance(values, list):
            cleaned = [_clean_name(v) for v in values if _clean_name(v)]
        elif values in (None, ""):
            cleaned = []
        else:
            single = _clean_name(values)
            cleaned = [single] if single else []

        roles[role_key] = cleaned

    return roles


def normalize_override_payload(payload: dict) -> dict:
    teams = {}
    if not isinstance(payload, dict):
        return teams

    for raw_team, raw_roles in payload.items():
        team = _normalize_team(raw_team)
        if not team:
            continue
        teams[team] = _normalize_roles(raw_roles)

    return teams


def save_depth_chart(teams: dict, source: str, message_id: int | None = None, attachment_name: str | None = None):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    payload = {
        "last_update_utc": datetime.utcnow().isoformat(),
        "source": source,
        "message_id": message_id,
        "attachment_name": attachment_name,
        "teams": teams,
    }
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


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


async def fetch_closer_depth_chart(client, channel_id: int, preferred_filename: str = DEFAULT_OVERRIDE_FILENAME, history_limit: int = 50):
    print(f"[CLOSER WATCH] Checking Discord override channel {channel_id}", flush=True)

    try:
        channel = await client.fetch_channel(channel_id)
    except Exception as e:
        print(f"[CLOSER WATCH] Failed to fetch override channel: {e}", flush=True)
        return load_depth_chart()

    try:
        async for message in channel.history(limit=history_limit):
            for attachment in message.attachments:
                filename = str(getattr(attachment, "filename", "") or "")
                lower_name = filename.lower()
                if preferred_filename and lower_name != preferred_filename.lower() and not lower_name.endswith(".json"):
                    continue

                try:
                    raw_bytes = await attachment.read()
                    payload = json.loads(raw_bytes.decode("utf-8"))
                    teams = normalize_override_payload(payload)
                    if not teams:
                        continue

                    save_depth_chart(
                        teams,
                        source=f"discord:{channel_id}",
                        message_id=getattr(message, "id", None),
                        attachment_name=filename,
                    )
                    print(f"[CLOSER WATCH] Loaded {len(teams)} bullpens from Discord override", flush=True)
                    return teams
                except Exception as e:
                    print(f"[CLOSER WATCH] Failed to parse attachment {filename}: {e}", flush=True)
                    continue
    except Exception as e:
        print(f"[CLOSER WATCH] Failed reading override history: {e}", flush=True)

    cached = load_depth_chart()
    if cached:
        print("[CLOSER WATCH] Using cached saved depth chart", flush=True)
    else:
        print("[CLOSER WATCH] No Discord override found and no cached depth chart available", flush=True)
    return cached

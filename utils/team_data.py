"""Shared team data and helpers used across multiple bots."""

TEAM_COLORS = {
    "ARI": 0xA71930, "ATH": 0x003831, "ATL": 0xCE1141, "BAL": 0xDF4601,
    "BOS": 0xBD3039, "CHC": 0x0E3386, "CWS": 0x27251F, "CIN": 0xC6011F,
    "CLE": 0xE31937, "COL": 0x33006F, "DET": 0x0C2340, "HOU": 0xEB6E1F,
    "KC": 0x004687, "LAA": 0xBA0021, "LAD": 0x005A9C, "MIA": 0x00A3E0,
    "MIL": 0x12284B, "MIN": 0x002B5C, "NYM": 0x002D72, "NYY": 0x0C2340,
    "PHI": 0xE81828, "PIT": 0xFDB827, "SD": 0x2F241D, "SF": 0xFD5A1E,
    "SEA": 0x005C5C, "STL": 0xC41E3A, "TB": 0x092C5C, "TEX": 0x003278,
    "TOR": 0x134A8E, "WSH": 0xAB0003,
}

TEAM_NAME_MAP = {
    "ARI": "Diamondbacks", "ATH": "Athletics", "ATL": "Braves", "BAL": "Orioles",
    "BOS": "Red Sox", "CHC": "Cubs", "CWS": "White Sox", "CIN": "Reds",
    "CLE": "Guardians", "COL": "Rockies", "DET": "Tigers", "HOU": "Astros",
    "KC": "Royals", "LAA": "Angels", "LAD": "Dodgers", "MIA": "Marlins",
    "MIL": "Brewers", "MIN": "Twins", "NYM": "Mets", "NYY": "Yankees",
    "PHI": "Phillies", "PIT": "Pirates", "SD": "Padres", "SF": "Giants",
    "SEA": "Mariners", "STL": "Cardinals", "TB": "Rays", "TEX": "Rangers",
    "TOR": "Blue Jays", "WSH": "Nationals",
}

_ABBR_ALIASES = {
    "AZ": "ARI", "ARI": "ARI", "CHW": "CWS", "CWS": "CWS",
    "WAS": "WSH", "WSN": "WSH", "WSH": "WSH", "TBR": "TB", "TB": "TB",
    "KCR": "KC", "KC": "KC", "SDP": "SD", "SD": "SD",
    "SFG": "SF", "SF": "SF", "OAK": "ATH", "ATH": "ATH",
}

_LOGO_KEY_MAP = {
    "CWS": "chw", "ATH": "oak", "ARI": "ari",
    "WSH": "wsh", "TB": "tb", "KC": "kc", "SD": "sd", "SF": "sf",
}


def normalize_team_abbr(team: str) -> str:
    key = str(team or "").strip().upper()
    return _ABBR_ALIASES.get(key, key)


def get_logo(team: str) -> str:
    normalized = normalize_team_abbr(team)
    key = _LOGO_KEY_MAP.get(normalized, normalized.lower())
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"


def normalize_lookup_name(name: str) -> str:
    if not name:
        return ""
    cleaned = name.lower()
    for ch in [".", ",", "'", "`", "-", "_", "(", ")", "[", "]"]:
        cleaned = cleaned.replace(ch, " ")
    return " ".join(cleaned.split())


import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import discord
from discord.ext import commands, tasks

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    from playwright.async_api import async_playwright
except Exception:
    async_playwright = None


BASE_DIR = Path(__file__).resolve().parent
if load_dotenv:
    load_dotenv(BASE_DIR / ".env")

try:
    import news_config  # type: ignore
except Exception:
    news_config = None


def cfg(name: str, default: Any = None) -> Any:
    if news_config and hasattr(news_config, name):
        value = getattr(news_config, name)
        if value is not None:
            return value
    return os.getenv(name, default)


NEWS_BOT_TOKEN = str(cfg("NEWS_BOT_TOKEN", "") or "").strip()
NEWS_CHANNEL_ID = int(str(cfg("NEWS_CHANNEL_ID", "0") or "0"))
POLL_MINUTES = int(str(cfg("POLL_MINUTES", "5") or "5"))
MAX_POSTS_PER_RUN = int(str(cfg("MAX_POSTS_PER_RUN", "50") or "50"))
ESPN_URL = str(cfg("ESPN_URL", "https://fantasy.espn.com/baseball/playernews") or "").strip()
HEADLESS = str(cfg("HEADLESS", "true") or "true").lower() not in {"0", "false", "no"}
RESET_STATE_ON_START = str(cfg("RESET_STATE_ON_START", "false") or "false").lower() in {"1", "true", "yes"}

STATE_DIR = BASE_DIR / "state" / "espn_news"
POSTED_IDS_FILE = STATE_DIR / "posted_ids.json"
RECENT_HASHES_FILE = STATE_DIR / "recent_hashes.json"
PLAYER_LAST_POSTS_FILE = STATE_DIR / "player_last_posts.json"
SCRAPE_DEBUG_FILE = STATE_DIR / "last_scrape_candidates.txt"

PLAYER_ID_PATH = BASE_DIR / "shared" / "player_ids" / "espn_player_ids.json"
MLB_FALLBACK_LOGO = "https://a.espncdn.com/i/teamlogos/leagues/500/mlb.png"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

TIMESTAMP_RE = re.compile(
    r"(?:SUN|MON|TUE|WED|THU|FRI|SAT),\s+[A-Z]{3}\s+\d{1,2},\s+\d{1,2}:\d{2}\s+[AP]\.?M\.?",
    re.I,
)

POS_SUFFIX_RE = re.compile(r"(?:C|1B|2B|3B|SS|OF|LF|CF|RF|DH|SP|RP|P)$", re.I)

NAV_BAD_PATTERNS = [
    "hsb.accessibility.skipcontent",
    "where to watch",
    "fantasy where to watch",
    "espn nfl nba ncaam",
    "soccer more sports watch fantasy",
    "menu espn",
    "search scores",
    "fantasy baseball support",
    "reset draft",
    "member services",
    "privacy policy",
    "terms of use",
]

PERFORMANCE_PATTERNS = [
    r"\bwent \d+-for-\d+\b",
    r"\bhas allowed\b",
    r"\bhas slashed\b",
    r"\bis batting\b",
    r"\bthrough \d+ appearances\b",
    r"\bover \d+(\.\d+)? innings\b",
    r"\bposted a \d+:\d+ k:bb\b",
    r"\bcollected \d+ hits?\b",
    r"\bera\b",
    r"\bops\b",
]

TEAM_WORD_TO_ABBR = {
    "reds": "CIN", "angels": "LAA", "brewers": "MIL", "astros": "HOU",
    "nationals": "WSH", "rockies": "COL", "yankees": "NYY", "dodgers": "LAD",
    "mets": "NYM", "orioles": "BAL", "guardians": "CLE", "twins": "MIN",
    "mariners": "SEA", "phillies": "PHI", "padres": "SD", "pirates": "PIT",
    "cubs": "CHC", "cardinals": "STL", "diamondbacks": "ARI", "blue jays": "TOR",
    "tigers": "DET", "rangers": "TEX", "giants": "SF", "athletics": "ATH",
    "royals": "KC", "marlins": "MIA", "rays": "TB", "red sox": "BOS",
    "white sox": "CWS", "braves": "ATL"
}

TEAM_META = {
    "ARI": {"slug": "ari", "color": 0xA71930},
    "ATH": {"slug": "oak", "color": 0x003831},
    "ATL": {"slug": "atl", "color": 0xCE1141},
    "BAL": {"slug": "bal", "color": 0xDF4601},
    "BOS": {"slug": "bos", "color": 0xBD3039},
    "CHC": {"slug": "chc", "color": 0x0E3386},
    "CWS": {"slug": "chw", "color": 0x27251F},
    "CIN": {"slug": "cin", "color": 0xC6011F},
    "CLE": {"slug": "cle", "color": 0xE31937},
    "COL": {"slug": "col", "color": 0x33006F},
    "DET": {"slug": "det", "color": 0x0C2340},
    "HOU": {"slug": "hou", "color": 0xEB6E1F},
    "KC": {"slug": "kc", "color": 0x004687},
    "LAA": {"slug": "laa", "color": 0xBA0021},
    "LAD": {"slug": "lad", "color": 0x005A9C},
    "MIA": {"slug": "mia", "color": 0x00A3E0},
    "MIL": {"slug": "mil", "color": 0x12284B},
    "MIN": {"slug": "min", "color": 0x002B5C},
    "NYM": {"slug": "nym", "color": 0x002D72},
    "NYY": {"slug": "nyy", "color": 0x132448},
    "PHI": {"slug": "phi", "color": 0xE81828},
    "PIT": {"slug": "pit", "color": 0xFDB827},
    "SD": {"slug": "sd", "color": 0x2F241D},
    "SF": {"slug": "sf", "color": 0xFD5A1E},
    "SEA": {"slug": "sea", "color": 0x0C2C56},
    "STL": {"slug": "stl", "color": 0xC41E3A},
    "TB": {"slug": "tb", "color": 0x092C5C},
    "TEX": {"slug": "tex", "color": 0x003278},
    "TOR": {"slug": "tor", "color": 0x134A8E},
    "WSH": {"slug": "wsh", "color": 0xAB0003},
}


def log(msg: str) -> None:
    print(f"[ESPN NEWS BOT] {msg}", flush=True)


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def load_json_file(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return default
        return json.loads(raw)
    except Exception as exc:
        log(f"Failed reading {path}: {exc}")
        return default


def save_json_file(path: Path, data: Any) -> None:
    ensure_state_dir()
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def write_text_file(path: Path, text: str) -> None:
    ensure_state_dir()
    path.write_text(text, encoding="utf-8")


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_text(text: str) -> str:
    text = clean_spaces(text).lower()
    text = text.replace("’", "'").replace("“", '"').replace("”", '"')
    text = re.sub(r"[^a-z0-9\s':,()./-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


KNOWN_NAME_FIXES = {
    "Matt Thai": "Matt Thaiss",
    "Matt Thai Red": "Matt Thaiss",
}

def normalize_person_name(text: str) -> str:
    text = clean_spaces(text).lower()
    text = re.sub(r"[^a-z0-9\s.-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def looks_like_nav_or_shell(text: str) -> bool:
    norm = normalize_text(text)
    return not norm or any(p in norm for p in NAV_BAD_PATTERNS)


def strip_reporter_tail(text: str) -> str:
    text = clean_spaces(text)
    patterns = [
        r",\s*[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+)+\s+of\s+[^.]+(?:\.)?$",
        r",\s*[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+)+\s+reports?\.$",
        r",\s*according to [^.]+(?:\.)?$",
        r",\s*[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+)+\s+told\s+MLB\.com\.?$",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text).strip(" ,")
    return clean_spaces(text)


def is_performance_post(text: str) -> bool:
    norm = normalize_text(text)
    return any(re.search(p, norm, re.I) for p in PERFORMANCE_PATTERNS)


def is_obvious_performance_only(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    performance_terms = [
        " batting order ", " slotted fifth ", " in the batting order ", " two games since he returned ",
        " diminished velocity ", " grapefruit league game ", " spring training game ",
        " exhibition against ", " exhibition game ", " world baseball classic ",
        " in the two games since ", " velocity during ",
    ]
    health_or_roster_terms = [
        " injured list", " rehab assignment", " opening day roster", " roster spot",
        " optioned", " reassigned", " signed", " dfa", " designated for assignment",
        " expected to begin the regular season", " oblique", " shoulder", " arm", " hamstring",
        " illness", " mri", " healing well", " relief appearance",
    ]
    if any(term in text for term in health_or_roster_terms):
        return False
    return any(term in text for term in performance_terms)


def classify_item(update_text: str, details_text: str = "") -> str:
    norm = f" {normalize_text(update_text + ' ' + details_text)} "

    lineup_terms = [
        " opening day roster ", " roster spot ", " expected to make the opening day roster ",
        " expected to earn a roster spot ", " will not be a part of the opening day roster ",
        " secured a spot ", " will make the ", " earned a roster spot ",
        " expected to make the roster ", " part of the opening day roster ",
        " included on the opening day roster ", " included on cleveland's opening day roster ",
        " expected to be included on the opening day roster ", " make the club's opening day roster ",
        " expected to be part of the opening day roster ", " expected to make cleveland's opening day roster ",
        " expected to make the guardians' opening day roster ",
    ]
    direct_transaction_terms = [
        " was optioned ", " was reassigned ", " was designated for assignment ",
        " optioned to ", " reassigned to ", " designated for assignment ",
        " was claimed ", " was released ", " was waived ", " was assigned to ",
        " signed a ", " agreed to a ", " was traded ", " was acquired ",
    ]
    broad_transaction_terms = [
        " optioned ", " reassigned ", " designated for assignment ", " dfa ", " signed ",
        " agreed to a ", " claimed ", " released ", " waived ", " selected the contract ",
        " assigned to ", " traded ", " acquired ",
    ]
    role_terms = [
        " will play third base ", " playing time at third base ", " see most of his playing time at ",
        " regular playing time at ", " regular work at ", " primary ", " expected to split time ",
        " expected to see time at ", " expected to work at ", " fill the vacancy ", " more starts ",
        " rotation ", " starter ", " long reliever ", " closer ", " setup role ",
        " bullpen role ", " fifth starter ", " will start ",
        " batting order ", " slotted ", " hit fifth ", " hit cleanup ", " bat leadoff ",
    ]
    injury_terms = [
        " injured list ", " il ", " rehab assignment ", " rehab ", " fracture ", " x-rays ",
        " expected to begin the regular season on the injured list ", " mri ", " day to day ",
        " scratched from ", " healing well ", " relief appearance ", " illness ",
        " strain ", " sprain ", " oblique ",
    ]

    if any(term in norm for term in lineup_terms):
        if any(term in norm for term in direct_transaction_terms):
            return "transaction"
        return "lineup"

    if any(term in norm for term in role_terms):
        return "role"

    if any(term in norm for term in injury_terms):
        return "injury"

    if any(term in norm for term in broad_transaction_terms):
        return "transaction"

    return "general"



def _player_name_forms(player_name: str) -> List[str]:
    player_name = clean_spaces(player_name)
    if not player_name:
        return []
    forms = [player_name.lower()]
    parts = player_name.split()
    if parts:
        forms.append(parts[-1].lower())
    return list(dict.fromkeys(forms))


def _subject_direct_movement(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    for form in _player_name_forms(player_name):
        patterns = [
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?optioned\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?reassigned\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?designated for assignment\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?claimed\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?released\b",
            rf"\b{re.escape(form)}\b\s+(?:was\s+)?waived\b",
            rf"\b{re.escape(form)}\b\s+(?:agreed|signed)\b",
            rf"\boptioned\b.*?\b{re.escape(form)}\b",
            rf"\breassigned\b.*?\b{re.escape(form)}\b",
            rf"\bdesignated\b.*?\b{re.escape(form)}\b\s+for assignment\b",
            rf"\bclaimed\b.*?\b{re.escape(form)}\b",
            rf"\breleased\b.*?\b{re.escape(form)}\b",
            rf"\bwaived\b.*?\b{re.escape(form)}\b",
            rf"\bsigned\b.*?\b{re.escape(form)}\b",
            rf"\bacquired\b.*?\b{re.escape(form)}\b",
            rf"\btraded\b.*?\b{re.escape(form)}\b",
        ]
        if any(re.search(p, text) for p in patterns):
            return True
    return False


def _subject_roster_outcome(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    roster_phrases = [
        "opening day roster", "roster spot", "expected to make the roster",
        "expected to earn a roster spot", "included on the opening day roster",
        "expected to be included on the opening day roster", "expected to be part of the opening day roster",
        "earned a roster spot", "will make the", "secured a spot",
        "make the club's opening day roster", "will be included on",
        "expected to make the opening day roster",
    ]
    return any(p in text for p in roster_phrases)


def _subject_role_usage(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    role_phrases = [
        "see most of his playing time at", "will play third base", "playing time at third base",
        "regular playing time at", "regular work at", "fill the vacancy", "more starts",
        "expected to see time at", "expected to work at", "split time",
        "plans to deploy", "out of the bullpen", "in the bullpen", "opening day rotation",
        "slot in to the dodgers' opening day rotation", "will open the season in the cardinals' rotation",
        "in line for a starting role", "starting role to begin the season",
        "line for regular work", "projected to start", "projected by mlb.com to start",
        "bench role", "platoon role", "playing time at third", "at third base this season",
    ]
    return any(p in text for p in role_phrases)


def _subject_injury_availability(player_name: str, update_text: str) -> bool:
    text = normalize_text(update_text)
    injury_phrases = [
        "injured list", "rehab assignment", "expected to begin the regular season on the injured list",
        "mri", "x-rays", "fracture", "illness", "day to day", "healing well",
        "relief appearance", "strain", "sprain", "oblique", "shoulder", "hamstring",
        "thumb", "wrist", "elbow", "knee", "back", "groin", "forearm", "blister",
        "soreness",
    ]
    deployment_only_phrases = [
        "plans to deploy", "out of the bullpen", "in the bullpen", "opening day rotation",
        "fill the vacancy", "more starts", "projected to start", "projected by mlb.com to start",
        "bench role", "platoon role", "playing time at third base", "at third base this season",
    ]
    if any(p in text for p in deployment_only_phrases) and not any(p in text for p in injury_phrases):
        return False
    return any(p in text for p in injury_phrases)


def refine_category_for_subject(player_name: str, update_text: str, category: str) -> str:
    if _subject_direct_movement(player_name, update_text):
        return "transaction"
    if _subject_roster_outcome(player_name, update_text):
        return "lineup"
    if _subject_injury_availability(player_name, update_text):
        return "injury"
    if _subject_role_usage(player_name, update_text):
        return "role"
    return category


def is_hitter_box_score_recap(update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)

    # Injury/availability supersedes the stat-line filter.
    injury_override_terms = [
        " injured list", " rehab assignment", " expected to begin the regular season on the injured list",
        " mri", " x-rays", " fracture", " illness", " day to day", " healing well",
        " strain", " sprain", " oblique", " shoulder", " hamstring", " thumb", " wrist",
        " elbow", " knee", " back", " groin", " forearm", " blister", " soreness",
    ]
    if any(term in text for term in injury_override_terms):
        return False

    spring_context_terms = [
        "grapefruit league", "cactus league", "spring training", "exhibition",
    ]
    if not any(term in text for term in spring_context_terms):
        return False

    hitter_patterns = [
        r"\bwent\s+\d+-for-\d+\b",
        r"\bwith\s+\d+\s+strikeouts?\b",
        r"\bwith\s+an?\s+double\b",
        r"\bwith\s+an?\s+triple\b",
        r"\bwith\s+an?\s+home run\b",
        r"\bwith\s+two\s+home runs\b",
        r"\bwith\s+an?\s+rbi\b",
        r"\bwith\s+\d+\s+rbi\b",
        r"\bhit a home run\b",
        r"\bhomered\b",
        r"\bdoubled\b",
        r"\btripled\b",
        r"\bsingled\b",
        r"\bdrove in\b",
        r"\bstole a base\b",
    ]
    return any(re.search(pattern, text) for pattern in hitter_patterns)

def should_skip_low_priority(category: str, update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)
    update_norm = normalize_text(update_text)

    clear_roster_terms = [
        " opening day roster", " roster spot", " expected to make the roster",
        " expected to earn a roster spot", " included on the opening day roster",
        " expected to be included on the opening day roster", " expected to be part of the opening day roster",
        " will not be a part of the opening day roster", " earned a roster spot",
    ]
    clear_movement_terms = [
        " was optioned", " was reassigned", " was designated for assignment",
        " optioned to", " reassigned to", " designated for assignment",
        " signed a", " agreed to a", " was claimed", " was released", " was waived",
        " was traded", " was acquired",
    ]
    clear_injury_terms = [
        " injured list", " rehab assignment", " expected to begin the regular season on the injured list",
        " mri", " x-rays", " fracture", " illness", " day to day", " healing well",
        " relief appearance", " strain", " sprain", " oblique",
    ]
    clear_role_terms = [
        " plans to deploy", " out of the bullpen", " projected to start", " projected by mlb.com to start",
        " see most of his playing time at", " will play third base", " bench role", " platoon role",
        " playing time at third base", " at third base this season",
    ]

    if any(term in text for term in clear_roster_terms):
        return False
    if any(term in text for term in clear_movement_terms):
        return False
    if any(term in text for term in clear_injury_terms):
        return False
    if any(term in text for term in clear_role_terms):
        return False

    # Clean hitter recap filter with injury override baked in.
    if is_hitter_box_score_recap(update_text, details_text):
        return True

    # Hard skip obvious spring / exhibition / box-score blurbs.
    hard_box_score_terms = [
        " took the loss ", " took a loss ", " scoreless inning", " scoreless innings",
        " allowing one earned run ", " allowing two earned runs ", " allowing three earned runs ",
        " allowed one run ", " allowed two runs ", " allowed three runs ", " allowed four runs ",
        " on one hit ", " on two hits ", " on three hits ", " on four hits ",
        " including one home run ", " including two home runs ",
        " struck out ", " strikeouts ", " k:bb ", " exhibition against ", " exhibition game ", " exhibition defeat ",
        " grapefruit league ", " spring training game ", " through three appearances ",
        " over three innings ", " over two innings ", " over four innings ",
        " one earned run on two hits", " one home run",
    ]
    if any(term in update_norm for term in hard_box_score_terms):
        return True

    if is_performance_post(update_text) or is_obvious_performance_only(update_text, details_text):
        return True

    ripple_terms = [
        " because strider ", " due to strider ",
        " diminished velocity during sunday's grapefruit league game ",
    ]
    if any(term in text for term in ripple_terms):
        return True

    if category in {"transaction", "injury", "lineup", "role"}:
        return False

    return True


def should_skip_rp_blurb(player_name: str, team_hint: Optional[str], category: str, update_text: str, details_text: str = "") -> bool:
    text = normalize_text(update_text + " " + details_text)

    # Injury-related reliever blurbs should still post.
    injury_terms = [
        "injured list", "rehab assignment", "expected to begin the regular season on the injured list",
        "mri", "x-rays", "fracture", "illness", "day to day", "healing well",
        "relief appearance", "strain", "sprain", "oblique", "shoulder", "hamstring",
        "thumb", "wrist", "elbow", "knee", "back", "groin", "forearm", "blister",
        "soreness",
    ]
    if any(term in text for term in injury_terms):
        return False

    # Allow closer promotions / demotions.
    closer_change_terms = [
        "closer", "save chances", "ninth inning", "will handle saves", "in line for saves",
        "moving into the closer role", "lost the closer role", "removed from the closer role",
        "demoted from the closer role", "expected to close", "favorite for saves",
    ]
    if any(term in text for term in closer_change_terms):
        return False

    # If this looks like a reliever/bullpen note, skip it.
    rp_terms = [
        "out of the bullpen", "in the bullpen", "reliever", "relief role",
        "setup role", "bullpen role", "middle relief", "seventh inning", "eighth inning",
        "late-inning", "high-leverage", "leverage role",
    ]
    pos_terms = [" rp ", " relief pitcher ", " reliever "]

    if any(term in text for term in rp_terms) or any(term in text for term in pos_terms):
        return True

    return False

def summarize_fantasy_impact(category: str, update_text: str, details_text: str) -> str:
    if category == "injury":
        return "Monitor the timetable and role impact before making a big roster move."
    if category == "transaction":
        return "This is mainly a role or depth move unless it changes playing time in your format."
    if category == "lineup":
        return "This matters most in deeper formats where early-season role and roster spots count."
    if category == "role":
        return "Keep an eye on how this affects usage and short-term opportunity."
    return "Watch for the next update before reacting."


def canonical_story_key(player_name: str, update_text: str, details_text: str) -> str:
    return sha1_text(f"{player_name.lower()}|{normalize_text(strip_reporter_tail(update_text))}|{normalize_text(details_text)}")


def exact_item_key(player_name: str, category: str, update_text: str, details_text: str) -> str:
    return sha1_text(f"{player_name.lower()}|{category}|{normalize_text(update_text)}|{normalize_text(details_text)}")


def load_player_ids_map() -> Dict[str, Any]:
    if not PLAYER_ID_PATH.exists():
        log(f"Player id map missing at {PLAYER_ID_PATH}")
        return {}
    try:
        raw = json.loads(PLAYER_ID_PATH.read_text(encoding="utf-8"))
        log(f"Loaded ESPN player id map from {PLAYER_ID_PATH}")
        return raw
    except Exception as exc:
        log(f"Failed loading player id map {PLAYER_ID_PATH}: {exc}")
        return {}


def build_last_name_index(player_ids_map: Dict[str, Any]) -> Dict[str, List[str]]:
    index: Dict[str, List[str]] = {}
    for full_name in player_ids_map:
        parts = clean_spaces(full_name).split()
        if parts:
            index.setdefault(parts[-1].lower(), []).append(full_name)
    return index


def build_name_prefixes(player_ids_map: Dict[str, Any]) -> List[str]:
    return sorted(player_ids_map.keys(), key=len, reverse=True)


def resolve_player_name(raw_name: str, player_ids_map: Dict[str, Any], last_name_index: Dict[str, List[str]]) -> str:
    raw_name = clean_spaces(raw_name)
    raw_name = KNOWN_NAME_FIXES.get(raw_name, raw_name)

    if raw_name in player_ids_map:
        return raw_name

    normalized_target = normalize_person_name(raw_name)
    for candidate in player_ids_map.keys():
        if normalize_person_name(candidate) == normalized_target:
            return candidate

    parts = raw_name.split()
    if parts:
        last = parts[-1].lower()
        matches = last_name_index.get(last, [])
        if len(matches) == 1:
            return matches[0]
    return raw_name


def is_suspicious_row_mismatch(player_name: str, team_hint: Optional[str], update_text: str) -> bool:
    text = normalize_text(update_text)
    if player_name == "Kody Funderburk" and "derek shelton" in text:
        return True
    if team_hint == "MIN" and "derek shelton" in text:
        return True
    if player_name == "JP Sears" and "el paso" in text:
        return True
    if player_name == "Jose Suarez" and "strider" in text:
        return True
    if re.search(r"(Red|White|Pirates|Padres|Giants|Mariners|Diamondbacks|Twins|Yankees|Dodgers|Mets|Orioles|Cubs|Cardinals|Athletics|Rays|Royals|Rockies|Brewers|Braves|Nationals|Tigers|Rangers|Astros|Phillies|Marlins|Angels)(?:RP|SP|DH|OF|P)?$", player_name):
        return True
    return False


def resolve_player_card_assets(
    player_name: str,
    hinted_team: Optional[str],
    player_ids_map: Dict[str, Any],
    last_name_index: Dict[str, List[str]],
) -> Dict[str, Any]:
    resolved_name = resolve_player_name(player_name, player_ids_map, last_name_index)
    entry = player_ids_map.get(resolved_name) if isinstance(player_ids_map.get(resolved_name), dict) else None
    team_abbr = str(hinted_team or "").upper()
    headshot_url = None

    if entry:
        team_abbr = str(entry.get("team") or team_abbr or "").upper()
        headshot_url = entry.get("headshot_url")

    if team_abbr in TEAM_META:
        team_meta = TEAM_META[team_abbr]
        team_logo_url = f"https://a.espncdn.com/i/teamlogos/mlb/500/{team_meta['slug']}.png"
        color = team_meta["color"]
    else:
        team_logo_url = MLB_FALLBACK_LOGO
        color = 0x1D4ED8

    return {
        "resolved_name": resolved_name,
        "team_abbr": team_abbr or "MLB",
        "team_logo_url": team_logo_url,
        "color": color,
        "headshot_url": headshot_url or team_logo_url,
    }


@dataclass
class NewsItem:
    source: str
    source_id: str
    player_name: str
    update_text: str
    details_text: str
    category: str
    published_label: Optional[str] = None
    status: str = "new"
    team_hint: Optional[str] = None

    def exact_hash(self) -> str:
        return exact_item_key(self.player_name, self.category, self.update_text, self.details_text)

    def story_hash(self) -> str:
        return canonical_story_key(self.player_name, self.update_text, self.details_text)


class ESPNSource:
    def __init__(self, player_ids_map: Dict[str, Any]):
        self.player_ids_map = player_ids_map
        self.name_prefixes = build_name_prefixes(player_ids_map)

    def _extract_left_name(self, left: str) -> Optional[str]:
        for candidate in self.name_prefixes:
            if left.startswith(candidate + " "):
                return candidate

        left_lower = " " + left.lower() + " "
        earliest = None
        for team_word in sorted(TEAM_WORD_TO_ABBR.keys(), key=len, reverse=True):
            token = " " + team_word + " "
            pos = left_lower.find(token)
            if pos != -1:
                real_pos = max(0, pos - 1)
                if earliest is None or real_pos < earliest:
                    earliest = real_pos

        candidate = None
        if earliest is not None:
            candidate = clean_spaces(left[:earliest])

        if not candidate:
            m = re.match(r"^([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,2})\b", left)
            if m:
                candidate = clean_spaces(m.group(1))

        if not candidate:
            return None

        # Trim flattened team residue like "MarinersOF", "PadresRP", "White", "Red"
        team_noise_patterns = [
            r"\s+(?:Red|White|Blue)$",
            r"(?:Pirates|Padres|Giants|Mariners|Diamondbacks|Twins|Yankees|Dodgers|Mets|Orioles|Cubs|Cardinals|Athletics|Rays|Royals|Rockies|Brewers|Braves|Nationals|Tigers|Rangers|Astros|Phillies|Marlins|Angels)(?:RP|SP|DH|OF|P)$",
        ]
        for pattern in team_noise_patterns:
            candidate = re.sub(pattern, "", candidate)
        candidate = POS_SUFFIX_RE.sub("", candidate).strip()
        candidate = clean_spaces(candidate)
        return candidate or None

    def _extract_team_hint(self, left: str) -> Optional[str]:
        left_lower = left.lower()

        # First, normal spaced team match
        for team_word, abbr in sorted(TEAM_WORD_TO_ABBR.items(), key=lambda x: len(x[0]), reverse=True):
            if f" {team_word} " in f" {left_lower} ":
                return abbr

        # Fallback for flattened team+pos strings like MarinersOF, PadresRP, DiamondbacksSP
        normalized_left = re.sub(r"\s+", "", left_lower)
        for team_word, abbr in sorted(TEAM_WORD_TO_ABBR.items(), key=lambda x: len(x[0]), reverse=True):
            squashed_team = team_word.replace(" ", "")
            if squashed_team in normalized_left:
                return abbr

        return None

    def _parse_row_text(self, text: str) -> Optional[NewsItem]:
        text = clean_spaces(text)
        if "News Archive" not in text:
            return None

        parts = text.split("News Archive", 1)
        if len(parts) != 2:
            return None

        left = clean_spaces(parts[0])
        right = clean_spaces(parts[1])

        full_name = self._extract_left_name(left)
        if not full_name:
            return None

        full_name = KNOWN_NAME_FIXES.get(full_name, full_name)
        team_hint = self._extract_team_hint(left)

        ts_match = TIMESTAMP_RE.search(right)
        if not ts_match:
            return None

        body = clean_spaces(right[ts_match.end():])
        if "Spin:" in body:
            update_text, details_text = body.split("Spin:", 1)
        else:
            update_text, details_text = body, ""

        update_text = strip_reporter_tail(clean_spaces(update_text))
        details_text = clean_spaces(details_text)

        if not update_text:
            return None

        category = classify_item(update_text, details_text)
        category = refine_category_for_subject(full_name, update_text, category)
        source_id = sha1_text(text[:700])

        return NewsItem(
            source="espn",
            source_id=source_id,
            player_name=full_name,
            update_text=update_text,
            details_text=details_text,
            category=category,
            published_label=None,
            team_hint=team_hint,
        )

    async def fetch_items(self) -> List[NewsItem]:
        if async_playwright is None:
            raise RuntimeError("Playwright is not installed. Run: python3 -m pip install playwright && python3 -m playwright install")

        log(f"Opening {ESPN_URL}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(user_agent=USER_AGENT, viewport={"width": 1440, "height": 2600})
            page = await context.new_page()
            await page.goto(ESPN_URL, wait_until="domcontentloaded", timeout=60000)

            for _ in range(6):
                await page.wait_for_timeout(1500)
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2500)

            row_candidates = await page.evaluate(
                """
                () => {
                  const nodes = Array.from(document.querySelectorAll("article, section, li, div, tr"));
                  const out = [];
                  for (const el of nodes) {
                    const text = (el.innerText || "").replace(/\\s+/g, " ").trim();
                    if (!text) continue;
                    if (text.length < 80 || text.length > 4000) continue;
                    if (!text.includes("News Archive")) continue;
                    out.push(text);
                  }
                  return out;
                }
                """
            )

            await context.close()
            await browser.close()

        unique_rows: List[str] = []
        seen: Set[str] = set()
        for text in row_candidates:
            text = clean_spaces(text)
            if text and text not in seen and not looks_like_nav_or_shell(text):
                seen.add(text)
                unique_rows.append(text)

        debug_lines = ["ROW CANDIDATES", "=" * 60]
        for i, text in enumerate(unique_rows[:300], start=1):
            debug_lines.append(f"[{i}] {text}")
            debug_lines.append("")
        write_text_file(SCRAPE_DEBUG_FILE, "\n".join(debug_lines))

        items: List[NewsItem] = []
        for text in unique_rows:
            item = self._parse_row_text(text)
            if item:
                items.append(item)

        deduped: List[NewsItem] = []
        seen_hashes: Set[str] = set()
        for item in items:
            h = item.exact_hash()
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            deduped.append(item)

        log(f"Row candidates={len(unique_rows)} | parsed_items={len(deduped)}")
        return deduped[:100]


class BotState:
    def __init__(self) -> None:
        ensure_state_dir()
        if RESET_STATE_ON_START:
            self.reset()
        self.posted_ids: List[str] = load_json_file(POSTED_IDS_FILE, [])
        self.recent_hashes: Dict[str, str] = load_json_file(RECENT_HASHES_FILE, {})
        self.player_last_posts: Dict[str, Dict[str, Any]] = load_json_file(PLAYER_LAST_POSTS_FILE, {})

    def reset(self) -> None:
        save_json_file(POSTED_IDS_FILE, [])
        save_json_file(RECENT_HASHES_FILE, {})
        save_json_file(PLAYER_LAST_POSTS_FILE, {})
        log("RESET_STATE_ON_START enabled — cleared ESPN state")

    def save(self) -> None:
        save_json_file(POSTED_IDS_FILE, self.posted_ids[-5000:])
        save_json_file(RECENT_HASHES_FILE, self.recent_hashes)
        save_json_file(PLAYER_LAST_POSTS_FILE, self.player_last_posts)

    def seen_exact(self, item: NewsItem) -> bool:
        return item.exact_hash() in self.recent_hashes

    def should_mark_update(self, item: NewsItem) -> bool:
        previous = self.player_last_posts.get(item.player_name.lower())
        if not previous:
            return False
        if previous.get("exact_hash") == item.exact_hash():
            return False
        return previous.get("story_hash") != item.story_hash()

    def record_post(self, item: NewsItem) -> None:
        self.posted_ids.append(item.source_id)
        self.recent_hashes[item.exact_hash()] = datetime.now(timezone.utc).isoformat()
        self.player_last_posts[item.player_name.lower()] = {
            "exact_hash": item.exact_hash(),
            "story_hash": item.story_hash(),
            "update_text": item.update_text,
            "details_text": item.details_text,
            "category": item.category,
            "status": item.status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


class ESPNNewsBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)

        self.state = BotState()
        self.player_ids_map = load_player_ids_map()
        self.last_name_index = build_last_name_index(self.player_ids_map)
        self.source = ESPNSource(self.player_ids_map)
        self.current_run_seen: Set[str] = set()

    async def setup_hook(self) -> None:
        self.poll_loop.start()

    async def on_ready(self) -> None:
        log(f"Logged in as {self.user}")
        log(f"Target channel id: {NEWS_CHANNEL_ID}")
        log("Poll loop started")

    def build_embed(self, item: NewsItem) -> discord.Embed:
        assets = resolve_player_card_assets(item.player_name, item.team_hint, self.player_ids_map, self.last_name_index)
        display_name = assets["resolved_name"]

        lines = [f"**Update:** {item.update_text}"]
        if item.details_text:
            details = item.details_text
            if len(details) > 500:
                details = details[:497].rstrip() + "..."
            lines.append(f"**Details:** {details}")
        lines.append(f"**Fantasy impact:** {summarize_fantasy_impact(item.category, item.update_text, item.details_text)}")

        embed = discord.Embed(description="\n\n".join(lines), color=assets["color"])
        embed.set_author(name=f"{display_name} | {assets['team_abbr']}", icon_url=assets["team_logo_url"])
        embed.set_thumbnail(url=assets["headshot_url"])
        embed.set_footer(text=f"{item.category.title()} | ESPN Fantasy | {item.status.title()}")
        embed.timestamp = datetime.now(timezone.utc)
        return embed

    async def post_item(self, channel: discord.abc.Messageable, item: NewsItem) -> None:
        await channel.send(embed=self.build_embed(item))

    @tasks.loop(minutes=POLL_MINUTES)
    async def poll_loop(self) -> None:
        await self.run_poll_cycle("loop")

    @poll_loop.before_loop
    async def before_poll_loop(self) -> None:
        await self.wait_until_ready()

    async def run_poll_cycle(self, trigger: str = "manual") -> None:
        channel = self.get_channel(NEWS_CHANNEL_ID)
        if channel is None:
            log(f"Channel not found: {NEWS_CHANNEL_ID}")
            return

        log(f"Starting poll cycle | trigger={trigger}")
        self.current_run_seen = set()

        try:
            items = await self.source.fetch_items()
        except Exception as exc:
            log(f"Source fetch failed: {exc}")
            import traceback
            traceback.print_exc()
            return

        log(f"Extracted {len(items)} items")
        posted = 0

        for item in items:
            if posted >= MAX_POSTS_PER_RUN:
                log(f"Reached MAX_POSTS_PER_RUN={MAX_POSTS_PER_RUN}")
                break

            if is_suspicious_row_mismatch(item.player_name, item.team_hint, item.update_text):
                log(f"Skipping suspicious row mismatch: {item.player_name} | {item.update_text[:120]}")
                continue

            if should_skip_rp_blurb(item.player_name, item.team_hint, item.category, item.update_text, item.details_text):
                log(f"Skipping RP blurb: {item.player_name} | {item.category}")
                continue

            if should_skip_low_priority(item.category, item.update_text, item.details_text):
                log(f"Skipping low-priority item: {item.player_name} | {item.category}")
                continue

            exact_hash = item.exact_hash()
            if exact_hash in self.current_run_seen:
                log(f"Skipping same-run exact duplicate: {item.player_name} | {item.update_text[:120]}")
                continue

            if self.state.seen_exact(item):
                log(f"Skipping posted exact duplicate: {item.player_name} | {item.update_text[:120]}")
                continue

            item.status = "update" if self.state.should_mark_update(item) else "new"
            await self.post_item(channel, item)
            self.current_run_seen.add(exact_hash)
            self.state.record_post(item)
            posted += 1
            log(f"Posted {item.player_name} | {item.category} | {item.status} | {item.update_text[:140]}")

        self.state.save()
        log(f"Poll cycle complete | posted={posted} | found={len(items)}")


def validate_config() -> None:
    if not NEWS_BOT_TOKEN:
        raise RuntimeError("Missing NEWS_BOT_TOKEN environment variable or news_config.NEWS_BOT_TOKEN")
    if not NEWS_CHANNEL_ID:
        raise RuntimeError("Missing NEWS_CHANNEL_ID environment variable or news_config.NEWS_CHANNEL_ID")


async def start_news_bot() -> None:
    validate_config()
    bot = ESPNNewsBot()
    await bot.start(NEWS_BOT_TOKEN)


def main() -> None:
    validate_config()
    bot = ESPNNewsBot()
    bot.run(NEWS_BOT_TOKEN)


if __name__ == "__main__":
    main()

import asyncio
import json
import os
import re
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import discord
import requests
from bs4 import BeautifulSoup

os.makedirs("state", exist_ok=True)
os.makedirs("state/player_profiles", exist_ok=True)

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = "state/news_posted_ids.json"
DUPES_FILE = "state/news_recent_fingerprints.json"
THREAD_MAP_FILE = BASE_DIR / "state/player_profiles/player_threads.json"

DISCORD_TOKEN = os.getenv("NEWS_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
NEWS_CHANNEL_ID = int(os.getenv("NEWS_CHANNEL_ID", "0"))
NEWS_POLL_SECONDS = int(os.getenv("NEWS_POLL_SECONDS", "180"))
MAX_NEWS_ITEMS = int(os.getenv("NEWS_MAX_ITEMS", "25"))

DUPLICATE_WINDOW_SECONDS = int(
    os.getenv("NEWS_DUPLICATE_WINDOW_SECONDS", str(12 * 60 * 60))
)

NBC_PLAYER_NEWS_URL = "https://www.nbcsports.com/fantasy/baseball/player-news"
NBC_BASE_URL = "https://www.nbcsports.com"

TEAM_LOGOS = {
    "ARI": "https://a.espncdn.com/i/teamlogos/mlb/500/ari.png",
    "ATL": "https://a.espncdn.com/i/teamlogos/mlb/500/atl.png",
    "BAL": "https://a.espncdn.com/i/teamlogos/mlb/500/bal.png",
    "BOS": "https://a.espncdn.com/i/teamlogos/mlb/500/bos.png",
    "CHC": "https://a.espncdn.com/i/teamlogos/mlb/500/chc.png",
    "CWS": "https://a.espncdn.com/i/teamlogos/mlb/500/chw.png",
    "CIN": "https://a.espncdn.com/i/teamlogos/mlb/500/cin.png",
    "CLE": "https://a.espncdn.com/i/teamlogos/mlb/500/cle.png",
    "COL": "https://a.espncdn.com/i/teamlogos/mlb/500/col.png",
    "DET": "https://a.espncdn.com/i/teamlogos/mlb/500/det.png",
    "HOU": "https://a.espncdn.com/i/teamlogos/mlb/500/hou.png",
    "KC": "https://a.espncdn.com/i/teamlogos/mlb/500/kc.png",
    "LAA": "https://a.espncdn.com/i/teamlogos/mlb/500/laa.png",
    "LAD": "https://a.espncdn.com/i/teamlogos/mlb/500/lad.png",
    "MIA": "https://a.espncdn.com/i/teamlogos/mlb/500/mia.png",
    "MIL": "https://a.espncdn.com/i/teamlogos/mlb/500/mil.png",
    "MIN": "https://a.espncdn.com/i/teamlogos/mlb/500/min.png",
    "NYM": "https://a.espncdn.com/i/teamlogos/mlb/500/nym.png",
    "NYY": "https://a.espncdn.com/i/teamlogos/mlb/500/nyy.png",
    "ATH": "https://a.espncdn.com/i/teamlogos/mlb/500/oak.png",
    "PHI": "https://a.espncdn.com/i/teamlogos/mlb/500/phi.png",
    "PIT": "https://a.espncdn.com/i/teamlogos/mlb/500/pit.png",
    "SD": "https://a.espncdn.com/i/teamlogos/mlb/500/sd.png",
    "SF": "https://a.espncdn.com/i/teamlogos/mlb/500/sf.png",
    "SEA": "https://a.espncdn.com/i/teamlogos/mlb/500/sea.png",
    "STL": "https://a.espncdn.com/i/teamlogos/mlb/500/stl.png",
    "TB": "https://a.espncdn.com/i/teamlogos/mlb/500/tb.png",
    "TEX": "https://a.espncdn.com/i/teamlogos/mlb/500/tex.png",
    "TOR": "https://a.espncdn.com/i/teamlogos/mlb/500/tor.png",
    "WSH": "https://a.espncdn.com/i/teamlogos/mlb/500/wsh.png",
    "MLB": "https://a.espncdn.com/i/teamlogos/mlb/500/mlb.png",
}

TEAM_COLORS = {
    "ARI": 0xA71930,
    "ATL": 0xCE1141,
    "BAL": 0xDF4601,
    "BOS": 0xBD3039,
    "CHC": 0x0E3386,
    "CWS": 0x27251F,
    "CIN": 0xC6011F,
    "CLE": 0xE31937,
    "COL": 0x33006F,
    "DET": 0x0C2340,
    "HOU": 0xEB6E1F,
    "KC": 0x004687,
    "LAA": 0xBA0021,
    "LAD": 0x005A9C,
    "MIA": 0x00A3E0,
    "MIL": 0x12284B,
    "MIN": 0x002B5C,
    "NYM": 0x002D72,
    "NYY": 0x0C2340,
    "ATH": 0x003831,
    "PHI": 0xE81828,
    "PIT": 0xFDB827,
    "SD": 0x2F241D,
    "SF": 0xFD5A1E,
    "SEA": 0x005C5C,
    "STL": 0xC41E3A,
    "TB": 0x092C5C,
    "TEX": 0x003278,
    "TOR": 0x134A8E,
    "WSH": 0xAB0003,
    "MLB": 0x2B2D31,
}

KNOWN_TAGS = {"Headline", "Injury", "Recap", "Transaction"}

TEAM_CODES = {
    "ARI", "ATL", "BAL", "BOS", "CHC", "CWS", "CIN", "CLE", "COL", "DET",
    "HOU", "KC", "LAA", "LAD", "MIA", "MIL", "MIN", "NYM", "NYY", "ATH",
    "PHI", "PIT", "SD", "SF", "SEA", "STL", "TB", "TEX", "TOR", "WSH"
}

WHITESPACE_RE = re.compile(r"\s+")
URL_RE = re.compile(r"https?://\S+")
HANDLE_RE = re.compile(r"@\w+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]")
MULTISPACE_RE = re.compile(r"\s+")
TIMESTAMP_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+[AP]M$"
)
PLAYER_SLUG_RE = re.compile(r"/player/[^/]+/([a-z0-9-]+)", re.IGNORECASE)

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        )
    }
)


def strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


def normalize_for_match(text: str) -> str:
    text = strip_accents(text or "").lower()
    text = text.replace("&amp;", "&")
    text = URL_RE.sub(" ", text)
    text = NON_ALNUM_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return f" {text} " if text else " "


def normalize_text(text: str) -> str:
    text = text or ""
    text = URL_RE.sub("", text)
    text = text.replace("&amp;", "&")
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def load_json_file(path: str, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_posted_ids() -> Set[str]:
    try:
        if not os.path.exists(STATE_FILE):
            save_json_file(STATE_FILE, [])
            return set()

        data = load_json_file(STATE_FILE, [])
        if isinstance(data, list):
            return set(str(x) for x in data)
        return set()
    except Exception as e:
        print(f"[NEWS] Failed loading state file: {e}")
        return set()


def save_posted_ids(posted_ids: Set[str]) -> None:
    try:
        save_json_file(STATE_FILE, sorted(posted_ids))
    except Exception as e:
        print(f"[NEWS] Failed saving state file: {e}")


def load_recent_fingerprints() -> Dict[str, float]:
    try:
        data = load_json_file(DUPES_FILE, {})
        cleaned: Dict[str, float] = {}
        if isinstance(data, dict):
            for key, value in data.items():
                try:
                    cleaned[str(key)] = float(value)
                except Exception:
                    continue
        return cleaned
    except Exception as e:
        print(f"[NEWS] Failed loading duplicate fingerprint file: {e}")
        return {}


def save_recent_fingerprints(fingerprints: Dict[str, float]) -> None:
    try:
        save_json_file(DUPES_FILE, fingerprints)
    except Exception as e:
        print(f"[NEWS] Failed saving duplicate fingerprint file: {e}")


def prune_recent_fingerprints(fingerprints: Dict[str, float]) -> Dict[str, float]:
    cutoff = time.time() - DUPLICATE_WINDOW_SECONDS
    return {k: v for k, v in fingerprints.items() if v >= cutoff}


def load_player_threads() -> Dict[str, int]:
    try:
        if not THREAD_MAP_FILE.exists():
            return {}

        with open(THREAD_MAP_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)

        cleaned: Dict[str, int] = {}
        if isinstance(raw, dict):
            for player_name, thread_id in raw.items():
                try:
                    cleaned[str(player_name).strip()] = int(thread_id)
                except Exception:
                    continue
        return cleaned
    except Exception as e:
        print(f"[NEWS] Failed loading player thread map: {e}")
        return {}


def build_player_match_index(player_threads: Dict[str, int]) -> List[Tuple[str, str, int]]:
    indexed = []
    for player_name, thread_id in player_threads.items():
        norm = normalize_for_match(player_name).strip()
        if norm:
            indexed.append((norm, player_name, thread_id))

    indexed.sort(key=lambda item: len(item[0]), reverse=True)
    return indexed


def detect_player(title: str, summary: str, player_index: List[Tuple[str, str, int]]) -> Tuple[Optional[str], Optional[int]]:
    haystack = normalize_for_match(f"{title} {summary}")

    for normalized_name, original_name, thread_id in player_index:
        needle = f" {normalized_name} "
        if needle in haystack:
            return original_name, thread_id

    return None, None


def extract_team_from_thread_name(thread_name: str) -> Optional[str]:
    if not thread_name:
        return None

    for sep in [" — ", " - ", " | ", "("]:
        if sep in thread_name:
            part = thread_name.split(sep, 1)[-1].strip(" )")
            maybe = part.upper().strip()
            if len(maybe) in (2, 3, 4):
                return maybe
    return None


async def resolve_player_thread(client: discord.Client, thread_id: Optional[int]):
    if not thread_id:
        return None

    thread = client.get_channel(thread_id)
    if thread is not None:
        return thread

    try:
        return await client.fetch_channel(thread_id)
    except Exception:
        return None


async def infer_team(
    client: discord.Client,
    source_team: Optional[str],
    player_thread_id: Optional[int],
) -> str:
    if source_team and source_team != "MLB":
        return source_team

    thread = await resolve_player_thread(client, player_thread_id)
    if thread is not None:
        inferred = extract_team_from_thread_name(getattr(thread, "name", ""))
        if inferred:
            return inferred

    return "MLB"


def build_story_fingerprint(title: str, summary: str, tag: str, team: str) -> str:
    text = f"{title} {summary}".lower()
    text = strip_accents(text)
    text = URL_RE.sub(" ", text)
    text = HANDLE_RE.sub(" ", text)

    replacements = {
        "according to": " ",
        "reports": " ",
        "reportedly": " ",
        "per source": " ",
        "source says": " ",
        "source said": " ",
        "manager said": " ",
        "told reporters": " ",
        "said": " ",
        "expected to": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    text = NON_ALNUM_RE.sub(" ", text)
    words = [w for w in MULTISPACE_RE.sub(" ", text).strip().split(" ") if w]

    stop_words = {
        "the", "a", "an", "and", "or", "to", "of", "for", "in", "on", "with",
        "at", "from", "by", "is", "are", "was", "were", "be", "as", "that",
        "this", "it", "his", "her", "their", "they", "he", "she", "will",
        "would", "could", "should", "has", "have", "had", "after", "before",
        "today", "tonight", "tomorrow", "yesterday"
    }
    words = [w for w in words if w not in stop_words]

    base = " ".join(words[:18]).strip()
    return f"{tag}|{team}|{base}"


def is_recent_duplicate(fingerprints: Dict[str, float], fingerprint: str) -> bool:
    ts = fingerprints.get(fingerprint)
    if ts is None:
        return False
    return (time.time() - ts) <= DUPLICATE_WINDOW_SECONDS


def remember_fingerprint(fingerprints: Dict[str, float], fingerprint: str) -> None:
    fingerprints[fingerprint] = time.time()


def absolute_url(url: Optional[str]) -> str:
    if not url:
        return NBC_PLAYER_NEWS_URL
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return NBC_BASE_URL + url
    return NBC_BASE_URL + "/" + url.lstrip("/")


def get_tag_color(tag: str, team: str) -> int:
    lowered = (tag or "").lower()
    if lowered == "injury":
        return 0xE74C3C
    if lowered == "transaction":
        return 0x3498DB
    if lowered == "recap":
        return 0x2ECC71
    if lowered == "headline":
        return 0xF1C40F
    return TEAM_COLORS.get(team, TEAM_COLORS["MLB"])


def build_embed(
    team: str,
    tag: str,
    title: str,
    summary: str,
    link: str,
    player_name: Optional[str] = None,
    player_thread_url: Optional[str] = None,
    timestamp_text: Optional[str] = None,
    position_text: Optional[str] = None,
) -> discord.Embed:
    clean_title = title.strip() or "New update"
    clean_summary = summary.strip()

    if len(clean_title) > 256:
        clean_title = clean_title[:253] + "..."

    embed = discord.Embed(
        title=clean_title,
        description=clean_summary[:3500] if clean_summary else None,
        color=get_tag_color(tag, team),
        url=link,
    )

    logo = TEAM_LOGOS.get(team)
    if logo:
        embed.set_thumbnail(url=logo)

    embed.add_field(name="Tag", value=tag or "News", inline=True)
    embed.add_field(name="Source", value=f"[NBC Sports Rotoworld]({link})", inline=True)

    if position_text:
        embed.add_field(name="Position", value=position_text[:100], inline=True)

    if player_name and player_thread_url:
        embed.add_field(
            name="Player Profile",
            value=f"[Open {player_name} thread]({player_thread_url})",
            inline=False,
        )

    footer_parts = [team if team else "MLB", "Fantasy Baseball Geek"]
    if timestamp_text:
        footer_parts.append(timestamp_text)

    embed.set_footer(text=" • ".join(footer_parts))
    return embed


def is_valid_player_name(text: str) -> bool:
    if not text:
        return False
    text = text.strip()
    if len(text) < 3 or len(text) > 60:
        return False
    if text in KNOWN_TAGS:
        return False
    if text.lower().startswith("personalize your rotoworld"):
        return False
    if text.lower() in {"related", "up next", "my favorites", "all news"}:
        return False
    words = text.split()
    return len(words) <= 5


def extract_team_and_position(line: str) -> Tuple[Optional[str], Optional[str]]:
    if not line:
        return None, None

    cleaned = normalize_text(line)
    parts = cleaned.split()

    team = None
    for part in parts:
        upper = part.upper()
        if upper in TEAM_CODES:
            team = upper
            break

    position = None
    if team and cleaned.startswith(team):
        position = cleaned[len(team):].strip()
        position = re.sub(r"#\d+\b", "", position).strip() or None

    return team, position


def slug_to_name(slug: str) -> str:
    parts = [p for p in slug.split("-") if p]
    return " ".join(p.capitalize() for p in parts)


def extract_candidate_links(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        text = normalize_text(a.get_text(" ", strip=True))

        if not href:
            continue

        match = PLAYER_SLUG_RE.search(href)
        if match:
            slug = match.group(1)
            player_name = slug_to_name(slug)
            if is_valid_player_name(player_name):
                out.setdefault(player_name, absolute_url(href))

        if is_valid_player_name(text) and "/player/" in href:
            out.setdefault(text, absolute_url(href))

    return out


def parse_rotoworld_items_from_text(raw_text: str, player_links: Dict[str, str]) -> List[dict]:
    text = raw_text.replace("\r", "\n")
    lines = [normalize_text(x) for x in text.split("\n")]
    lines = [x for x in lines if x]

    start_idx = None
    for i, line in enumerate(lines):
        if line == "Rotoworld":
            start_idx = i
            break

    if start_idx is None:
        return []

    lines = lines[start_idx:]

    items: List[dict] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if line in KNOWN_TAGS and items:
            items[-1]["tag"] = line
            i += 1
            continue

        if not is_valid_player_name(line):
            i += 1
            continue

        player_name = line

        if i + 1 >= len(lines):
            i += 1
            continue

        team = None
        position = None
        title = None
        summary = None
        tag = "News"
        timestamp_text = None

        next_line = lines[i + 1]
        team, position = extract_team_and_position(next_line)

        if not team:
            i += 1
            continue

        j = i + 2

        while j < len(lines):
            probe = lines[j]
            low = probe.lower()

            if low.startswith("personalize your rotoworld feed"):
                j += 1
                continue

            if probe == "Related":
                break

            if probe in KNOWN_TAGS:
                break

            if not title:
                title = probe
                j += 1
                continue

            if not summary:
                summary = probe
                j += 1
                continue

            if TIMESTAMP_RE.match(probe):
                timestamp_text = probe
                j += 1
                continue

            if is_valid_player_name(probe):
                next_probe = lines[j + 1] if j + 1 < len(lines) else ""
                maybe_team, _ = extract_team_and_position(next_probe)
                if maybe_team:
                    break

            j += 1

        scan_limit = min(j + 18, len(lines))
        for k in range(i + 2, scan_limit):
            probe = lines[k]
            if probe in KNOWN_TAGS:
                tag = probe
            elif TIMESTAMP_RE.match(probe):
                timestamp_text = probe
            elif probe == "Link copied to clipboard!":
                continue

        if title and summary:
            link = player_links.get(player_name, NBC_PLAYER_NEWS_URL)
            uid = f"{player_name}|{team}|{tag}|{title}|{summary[:80]}"

            items.append(
                {
                    "id": uid,
                    "player_name": player_name,
                    "team": team or "MLB",
                    "position": position,
                    "title": title,
                    "summary": summary,
                    "tag": tag,
                    "link": link,
                    "timestamp_text": timestamp_text,
                }
            )

        i = j if j > i else i + 1

    seen: Set[str] = set()
    deduped: List[dict] = []
    for item in items:
        if item["id"] in seen:
            continue
        seen.add(item["id"])
        deduped.append(item)

    return deduped[:MAX_NEWS_ITEMS]


def fetch_nbc_player_news() -> List[dict]:
    r = SESSION.get(NBC_PLAYER_NEWS_URL, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    raw_text = soup.get_text("\n", strip=True)
    player_links = extract_candidate_links(soup)
    items = parse_rotoworld_items_from_text(raw_text, player_links)

    if not items:
        raise RuntimeError("Could not parse any Rotoworld player news items from NBC page")

    return items


async def parse_nbc_news():
    return await asyncio.to_thread(fetch_nbc_player_news)


class NewsBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.bg_task: Optional[asyncio.Task] = None
        self.posted_ids: Set[str] = load_posted_ids()
        self.recent_fingerprints: Dict[str, float] = prune_recent_fingerprints(
            load_recent_fingerprints()
        )
        self.started_loop = False
        self.player_threads: Dict[str, int] = {}
        self.player_index: List[Tuple[str, str, int]] = []

    def refresh_player_threads(self):
        self.player_threads = load_player_threads()
        self.player_index = build_player_match_index(self.player_threads)

    async def on_ready(self):
        print(f"[NEWS] Logged in as {self.user}")
        print("[NEWS] NBC Rotoworld news bot started")

        if self.started_loop:
            return

        self.started_loop = True
        self.refresh_player_threads()

        print(f"[NEWS] Loaded {len(self.player_threads)} player thread mappings")
        print(f"[NEWS] Poll interval: {NEWS_POLL_SECONDS} seconds")
        print(f"[NEWS] State file: {STATE_FILE}")
        print(f"[NEWS] Duplicate file: {DUPES_FILE}")
        print(f"[NEWS] Source page: {NBC_PLAYER_NEWS_URL}")

        self.bg_task = asyncio.create_task(self.news_loop())

    async def news_loop(self):
        await self.wait_until_ready()

        while not self.is_closed():
            try:
                self.refresh_player_threads()
                self.recent_fingerprints = prune_recent_fingerprints(self.recent_fingerprints)
                await self.check_news_page()
            except Exception as e:
                print(f"[NEWS] Loop error: {e}")

            await asyncio.sleep(NEWS_POLL_SECONDS)

    async def check_news_page(self):
        if NEWS_CHANNEL_ID == 0:
            print("[NEWS] NEWS_CHANNEL_ID is not set")
            return

        channel = self.get_channel(NEWS_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.fetch_channel(NEWS_CHANNEL_ID)
            except Exception as e:
                print(f"[NEWS] Could not fetch news channel {NEWS_CHANNEL_ID}: {e}")
                return

        print("[NEWS] Checking NBC Rotoworld player news page")

        new_posts = 0
        dupes_skipped = 0

        items = await parse_nbc_news()

        print(f"[NEWS] Parsed items: {len(items)}")
        print(f"[NEWS] Posted ID count: {len(self.posted_ids)}")
        if items:
            first = items[0]
            print(f"[NEWS] First item player: {first.get('player_name')}")
            print(f"[NEWS] First item tag: {first.get('tag')}")
            print(f"[NEWS] First item title: {first.get('title')}")

        for item in reversed(items):
            uid = item["id"]
            if uid in self.posted_ids:
                print(f"[NEWS] Already posted uid: {uid[:120]}")
                continue

            title = item["title"]
            summary = item["summary"]
            tag = item["tag"]
            link = item["link"]
            source_team = item["team"]
            position_text = item.get("position")
            timestamp_text = item.get("timestamp_text")
            scraped_player_name = item.get("player_name")

            player_name = None
            player_thread_id = None

            if scraped_player_name and scraped_player_name in self.player_threads:
                player_name = scraped_player_name
                player_thread_id = self.player_threads.get(scraped_player_name)
            else:
                player_name, player_thread_id = detect_player(
                    title=title,
                    summary=f"{scraped_player_name or ''} {summary}",
                    player_index=self.player_index,
                )

            player_thread = await resolve_player_thread(self, player_thread_id)
            player_thread_url = getattr(player_thread, "jump_url", None)

            team = await infer_team(
                client=self,
                source_team=source_team,
                player_thread_id=player_thread_id,
            )

            fingerprint = build_story_fingerprint(
                title=f"{scraped_player_name or ''} {title}",
                summary=summary,
                tag=tag,
                team=team,
            )

            if is_recent_duplicate(self.recent_fingerprints, fingerprint):
                dupes_skipped += 1
                self.posted_ids.add(uid)
                print(f"[NEWS] Duplicate skipped [{team}] [{tag}] {title[:100]}")
                continue

            embed = build_embed(
                team=team,
                tag=tag,
                title=f"{scraped_player_name} — {title}" if scraped_player_name else title,
                summary=summary,
                link=link,
                player_name=player_name,
                player_thread_url=player_thread_url,
                timestamp_text=timestamp_text,
                position_text=position_text,
            )

            try:
                await channel.send(embed=embed)
            except Exception as e:
                print(f"[NEWS] Failed sending to NEWS_CHANNEL_ID {NEWS_CHANNEL_ID}: {e}")
                continue

            if player_thread is not None:
                try:
                    await player_thread.send(embed=embed)
                    print(f"[NEWS] Also posted to thread for {player_name}")
                except Exception as e:
                    print(f"[NEWS] Failed posting to player thread for {player_name}: {e}")

            self.posted_ids.add(uid)
            remember_fingerprint(self.recent_fingerprints, fingerprint)
            new_posts += 1

            print(
                f"[NEWS] Posted [{team}] [{tag}] "
                f"{(scraped_player_name or 'Unknown')} | {title[:120]}"
                + (f" | player={player_name}" if player_name else "")
            )

        save_posted_ids(self.posted_ids)
        save_recent_fingerprints(self.recent_fingerprints)

        print(f"[NEWS] Done. New posts: {new_posts} | Duplicates skipped: {dupes_skipped}")


async def start_news_bot():
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN (or NEWS_BOT_TOKEN) is not set")

    if NEWS_CHANNEL_ID == 0:
        raise RuntimeError("NEWS_CHANNEL_ID is not set")

    bot = NewsBot()
    await bot.start(DISCORD_TOKEN)

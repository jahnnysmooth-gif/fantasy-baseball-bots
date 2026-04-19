#!/usr/bin/env python3
"""Prospect News Bot.

Polls the MLB Transactions API for call-ups, options, IL moves, contract
selections, and other roster events, filtered to players on top_prospects.json.
Call-up and contract-selection cards include a Claude-generated blurb.

Runs as a long-lived Discord bot, integrated into main.py.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import anthropic
import discord
import requests

import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.team_data import get_logo as _get_team_logo
_sys.path.pop(0)

# ── CONFIG ────────────────────────────────────────────────────────────────────

TOKEN             = os.getenv("PROSPECT_BOT_TOKEN", "")
CHANNEL_ID        = int(os.getenv("PROSPECT_NEWS_CHANNEL_ID", "0"))
ANTHROPIC_API_KEY = os.getenv("PROSPECT_BOT_SUMMARY", "")
PROSPECTS_FILE    = os.getenv("PROSPECT_NEWS_PROSPECTS_FILE", "prospects/top_prospects.json")
ESPN_PLAYER_IDS_PATH = os.getenv("ESPN_PLAYER_IDS_PATH", "shared/player_ids/espn_player_ids.json")
STATE_FILE        = os.getenv("PROSPECT_NEWS_STATE_FILE", "state/prospect_news/state.json")
RESET_STATE       = os.getenv("RESET_PROSPECT_NEWS_STATE", "").lower() in {"1", "true", "yes"}

TX_POLL_MINUTES    = int(os.getenv("PROSPECT_NEWS_TX_POLL_MINUTES", "20"))
SLEEP_START_HOUR   = int(os.getenv("PROSPECT_NEWS_SLEEP_START", "3"))
SLEEP_END_HOUR     = int(os.getenv("PROSPECT_NEWS_SLEEP_END", "9"))
REQUEST_TIMEOUT    = int(os.getenv("PROSPECT_NEWS_TIMEOUT", "30"))
POST_DELAY_SECONDS = float(os.getenv("PROSPECT_NEWS_POST_DELAY", "1.5"))
CLAUDE_MODEL       = "claude-sonnet-4-6"

ET       = ZoneInfo("America/New_York")
BASE_URL = "https://statsapi.mlb.com/api/v1"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

# ── TRANSACTION TYPE CONFIG ───────────────────────────────────────────────────

TX_TYPE_CONFIG: Dict[str, Dict[str, Any]] = {
    "CU":  {"label": "Called Up",         "emoji": "📈", "color": 0x16A34A},
    "SE":  {"label": "Contract Selected", "emoji": "📋", "color": 0x2563EB},
    "OPT": {"label": "Optioned",          "emoji": "⬇️",  "color": 0xCA8A04},
    "ASG": {"label": "Assigned",          "emoji": "↩️",  "color": 0x9CA3AF},
    "SC":  {"label": "IL / Status",       "emoji": "🤕", "color": 0xDC2626},
    "DES": {"label": "DFA",               "emoji": "⚠️",  "color": 0x7C3AED},
    "TR":  {"label": "Traded",            "emoji": "🔁", "color": 0x0F766E},
    "CLW": {"label": "Claimed",           "emoji": "🔔", "color": 0x0891B2},
    "OUT": {"label": "Outrighted",        "emoji": "🔄", "color": 0x6B7280},
}

TEAM_META: Dict[str, Dict[str, Any]] = {
    "ARI": {"slug": "ari", "color": 0xA71930}, "ATH": {"slug": "oak", "color": 0x003831},
    "ATL": {"slug": "atl", "color": 0xCE1141}, "BAL": {"slug": "bal", "color": 0xDF4601},
    "BOS": {"slug": "bos", "color": 0xBD3039}, "CHC": {"slug": "chc", "color": 0x0E3386},
    "CWS": {"slug": "chw", "color": 0x27251F}, "CIN": {"slug": "cin", "color": 0xC6011F},
    "CLE": {"slug": "cle", "color": 0xE31937}, "COL": {"slug": "col", "color": 0x33006F},
    "DET": {"slug": "det", "color": 0x0C2340}, "HOU": {"slug": "hou", "color": 0xEB6E1F},
    "KC":  {"slug": "kc",  "color": 0x004687}, "LAA": {"slug": "laa", "color": 0xBA0021},
    "LAD": {"slug": "lad", "color": 0x005A9C}, "MIA": {"slug": "mia", "color": 0x00A3E0},
    "MIL": {"slug": "mil", "color": 0x12284B}, "MIN": {"slug": "min", "color": 0x002B5C},
    "NYM": {"slug": "nym", "color": 0x002D72}, "NYY": {"slug": "nyy", "color": 0x132448},
    "PHI": {"slug": "phi", "color": 0xE81828}, "PIT": {"slug": "pit", "color": 0xFDB827},
    "SD":  {"slug": "sd",  "color": 0x2F241D}, "SF":  {"slug": "sf",  "color": 0xFD5A1E},
    "SEA": {"slug": "sea", "color": 0x0C2C56}, "STL": {"slug": "stl", "color": 0xC41E3A},
    "TB":  {"slug": "tb",  "color": 0x092C5C}, "TEX": {"slug": "tex", "color": 0x003278},
    "TOR": {"slug": "tor", "color": 0x134A8E}, "WSH": {"slug": "wsh", "color": 0xAB0003},
}

# ── GLOBALS ───────────────────────────────────────────────────────────────────

client: discord.Client | None = None
_bg_task: asyncio.Task | None = None

_prospect_by_norm_name: Dict[str, dict] = {}
_prospect_by_mlb_id: Dict[int, dict] = {}
_headshot_index: Dict[str, list] = {}
_headshot_loaded = False
_state: Dict[str, Any] = {"tx_ids": []}


# ── LOGGING ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[PROSPECT NEWS] {msg}", flush=True)


def log_exception(ctx: str) -> None:
    log(ctx)
    traceback.print_exc()


# ── UTILS ─────────────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfd = unicodedata.normalize("NFD", name)
    ascii_str = nfd.encode("ascii", "ignore").decode("ascii")
    cleaned = ascii_str.lower().replace(".", "").replace("'", "").replace("-", " ")
    return " ".join(cleaned.split())


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", "-"):
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


# ── PROSPECT LIST ─────────────────────────────────────────────────────────────

def load_prospects() -> None:
    global _prospect_by_norm_name, _prospect_by_mlb_id
    path = Path(PROSPECTS_FILE)
    if not path.exists():
        log(f"Prospects file not found: {PROSPECTS_FILE}")
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"Failed to load prospects: {exc}")
        return
    _prospect_by_norm_name = {}
    _prospect_by_mlb_id = {}
    for p in data:
        norm = normalize_name(p.get("name", ""))
        if norm:
            _prospect_by_norm_name[norm] = p
        mlb_id = p.get("mlb_id")
        if mlb_id:
            _prospect_by_mlb_id[int(mlb_id)] = p
    log(f"Loaded {len(_prospect_by_norm_name)} prospects")


def find_prospect(player_id: Optional[int], name: str) -> Optional[dict]:
    if player_id and player_id in _prospect_by_mlb_id:
        return _prospect_by_mlb_id[player_id]
    norm = normalize_name(name)
    if norm in _prospect_by_norm_name:
        return _prospect_by_norm_name[norm]
    parts = norm.split()
    if parts and parts[-1] in {"jr", "sr", "ii", "iii", "iv"}:
        without_suffix = " ".join(parts[:-1])
        if without_suffix in _prospect_by_norm_name:
            return _prospect_by_norm_name[without_suffix]
    return None


# ── HEADSHOT ──────────────────────────────────────────────────────────────────

def load_headshot_index() -> None:
    global _headshot_index, _headshot_loaded
    if _headshot_loaded:
        return
    _headshot_loaded = True
    path = Path(ESPN_PLAYER_IDS_PATH)
    if not path.exists():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    for raw_name, raw_value in raw.items():
        entries = raw_value if isinstance(raw_value, list) else [raw_value]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            headshot_url = entry.get("headshot_url")
            espn_id = entry.get("espn_id")
            if not headshot_url and espn_id:
                headshot_url = f"https://a.espncdn.com/i/headshots/mlb/players/full/{espn_id}.png"
            if not headshot_url:
                continue
            _headshot_index.setdefault(normalize_name(raw_name), []).append(
                {"headshot_url": headshot_url}
            )
    log(f"Loaded headshot index: {len(_headshot_index)} entries")


def get_headshot(player_id: Optional[int], name: str) -> Optional[str]:
    if player_id:
        return (
            f"https://img.mlbstatic.com/mlb-photos/image/upload/"
            f"d_people:generic:headshot:67:current.png/w_213,q_auto:best/"
            f"v1/people/{player_id}/headshot/67/current"
        )
    entries = _headshot_index.get(normalize_name(name), [])
    return entries[0].get("headshot_url") if entries else None


def get_team_logo(org: str) -> Optional[str]:
    try:
        return _get_team_logo(org)
    except Exception:
        meta = TEAM_META.get(org)
        return f"https://a.espncdn.com/i/teamlogos/mlb/500/{meta['slug']}.png" if meta else None


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _get_json(url: str, params: Optional[dict] = None) -> dict:
    for attempt in range(3):
        try:
            r = requests.get(
                url, params=params, timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("unreachable")


# ── STATE ─────────────────────────────────────────────────────────────────────

def _load_state_from_disk() -> Dict[str, Any]:
    if RESET_STATE:
        return {"tx_ids": []}
    path = Path(STATE_FILE)
    if not path.exists():
        return {"tx_ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"tx_ids": []}
    except Exception:
        return {"tx_ids": []}


def save_state() -> None:
    path = Path(STATE_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_state, indent=2), encoding="utf-8")


def _record_tx(tx_id: int) -> None:
    ids: Set[int] = set(_state.get("tx_ids", []))
    ids.add(tx_id)
    _state["tx_ids"] = list(ids)[-5000:]
    save_state()


# ── MLB TRANSACTIONS ──────────────────────────────────────────────────────────

@dataclass
class Transaction:
    tx_id: int
    type_code: str
    description: str
    player_id: int
    player_name: str
    from_team: str
    to_team: str
    date_str: str
    prospect: dict


def _abbrev(team_block: dict) -> str:
    if not team_block:
        return ""
    return str(team_block.get("abbreviation") or team_block.get("teamCode") or "").upper()


def fetch_transactions(start_date: str, end_date: str) -> List[Transaction]:
    try:
        data = _get_json(f"{BASE_URL}/transactions", params={
            "startDate": start_date,
            "endDate":   end_date,
            "sportId":   1,
        })
    except Exception as exc:
        log(f"Transactions API failed: {exc}")
        return []

    results: List[Transaction] = []
    for tx in data.get("transactions", []):
        type_code = str(tx.get("typeCode") or "").upper()
        if type_code not in TX_TYPE_CONFIG:
            continue
        person = tx.get("person") or {}
        player_id = safe_int(person.get("id"))
        player_name = str(person.get("fullName") or "")
        if not player_name:
            continue
        prospect = find_prospect(player_id or None, player_name)
        if not prospect:
            continue
        results.append(Transaction(
            tx_id=safe_int(tx.get("id")),
            type_code=type_code,
            description=str(tx.get("description") or ""),
            player_id=player_id,
            player_name=player_name,
            from_team=_abbrev(tx.get("fromTeam") or {}),
            to_team=_abbrev(tx.get("toTeam") or {}),
            date_str=str(tx.get("date") or start_date),
            prospect=prospect,
        ))
    return results


# ── CLAUDE BLURB ─────────────────────────────────────────────────────────────

def _blurb_sync(tx: Transaction) -> tuple[str, str]:
    if not ANTHROPIC_API_KEY:
        return "", ""
    try:
        ai     = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        name   = tx.player_name
        rank   = tx.prospect.get("rank", "?")
        org    = tx.prospect.get("team") or tx.to_team or tx.from_team or "MLB"
        pos    = tx.prospect.get("position", "")
        action = TX_TYPE_CONFIG[tx.type_code]["label"].lower()

        banned = (
            "Do not use em dashes. "
            "Do not say 'down the stretch', 'playoff push', or any late-season language. "
            "When referencing his rank, always specify it is his overall MLB prospect ranking, "
            "not a team rank (e.g. 'the #5 overall prospect in baseball')."
        )
        desc_section = f"Transaction note: {tx.description}\n\n" if tx.description else ""

        prompt = (
            f"You are writing for a fantasy baseball Discord.\n\n"
            f"{desc_section}"
            f"Write exactly two parts separated by a blank line.\n\n"
            f"Part 1: A punchy 6-10 word headline for {name} ({pos}, #{rank} overall MLB prospect, {org}) "
            f"who was just {action}. No quotes, no colons. {banned}\n\n"
            f"Part 2: 2-3 sentences in beat-writer voice. Why does this matter for fantasy? "
            f"Mention his prospect pedigree and what owners should watch for. {banned}"
        )
        msg = ai.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=250,
            messages=[{"role": "user", "content": prompt}],
        )
        text  = msg.content[0].text.strip()
        parts = text.split("\n\n", 1)
        headline = parts[0].strip() if parts else ""
        blurb    = parts[1].strip() if len(parts) > 1 else ""
        return headline, blurb
    except Exception as exc:
        log(f"Claude blurb failed for {tx.player_name}: {exc}")
        return "", ""


async def generate_blurb(tx: Transaction) -> tuple[str, str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _blurb_sync, tx)


# ── EMBED ─────────────────────────────────────────────────────────────────────

def build_tx_embed(tx: Transaction, headline: str, blurb: str) -> discord.Embed:
    cfg_entry = TX_TYPE_CONFIG[tx.type_code]
    rank = tx.prospect.get("rank", "?")
    org  = tx.prospect.get("team") or tx.to_team or ""
    pos  = tx.prospect.get("position", "")

    embed = discord.Embed(color=cfg_entry["color"], timestamp=datetime.now(timezone.utc))

    logo = get_team_logo(org)
    try:
        embed.set_author(name=f"{tx.player_name} | {org}", icon_url=logo)
    except Exception:
        embed.set_author(name=f"{tx.player_name} | {org}")

    headshot = get_headshot(tx.player_id or None, tx.player_name)
    if headshot:
        try:
            embed.set_thumbnail(url=headshot)
        except Exception:
            pass

    display = headline or tx.description or cfg_entry["label"]
    embed.add_field(name="", value=f"**{cfg_entry['emoji']} {display}**", inline=False)

    if blurb:
        embed.add_field(name="Fantasy Take", value=blurb, inline=False)

    if tx.description and tx.description != display:
        embed.add_field(name="Transaction", value=tx.description, inline=False)

    team_str = ""
    if tx.from_team and tx.to_team and tx.from_team != tx.to_team:
        team_str = f" • {tx.from_team} → {tx.to_team}"
    elif tx.to_team:
        team_str = f" • {tx.to_team}"
    elif tx.from_team:
        team_str = f" • {tx.from_team}"

    embed.set_footer(text=f"#{rank} Prospect • {pos}{team_str} • {cfg_entry['label']}")
    return embed


# ── SLEEP HELPERS ─────────────────────────────────────────────────────────────

def now_et() -> datetime:
    return datetime.now(ET)


def is_sleep_window() -> bool:
    h = now_et().hour
    return SLEEP_START_HOUR <= h < SLEEP_END_HOUR


def seconds_until_wake() -> int:
    now  = now_et()
    wake = now.replace(hour=SLEEP_END_HOUR, minute=0, second=0, microsecond=0)
    if now >= wake:
        wake = (now + timedelta(days=1)).replace(
            hour=SLEEP_END_HOUR, minute=0, second=0, microsecond=0
        )
    return max(1, int((wake - now).total_seconds()))


# ── POLL LOOP ─────────────────────────────────────────────────────────────────

async def transactions_loop(channel: discord.abc.Messageable) -> None:
    posted_ids: Set[int] = set(_state.get("tx_ids", []))
    log("Transactions loop started")

    # On first boot, look back 2 days to catch recent moves
    start_dt = date.today() - timedelta(days=2)

    while True:
        if is_sleep_window():
            secs = seconds_until_wake()
            log(f"Sleep window — waking in {secs // 3600}h {(secs % 3600) // 60}m")
            await asyncio.sleep(secs)
            posted_ids = set(_state.get("tx_ids", []))
            start_dt = date.today() - timedelta(days=1)
            continue

        try:
            start_str = start_dt.isoformat()
            end_str   = date.today().isoformat()
            transactions = fetch_transactions(start_str, end_str)
            log(f"Fetched {len(transactions)} prospect transactions ({start_str} → {end_str})")

            posted = 0
            for tx in transactions:
                if tx.tx_id and tx.tx_id in posted_ids:
                    continue

                headline, blurb = "", ""
                if tx.type_code in {"CU", "SE"}:
                    headline, blurb = await generate_blurb(tx)

                embed = build_tx_embed(tx, headline, blurb)
                await channel.send(embed=embed)
                if tx.tx_id:
                    posted_ids.add(tx.tx_id)
                    _record_tx(tx.tx_id)
                posted += 1
                log(
                    f"TX posted: {tx.player_name} | {TX_TYPE_CONFIG[tx.type_code]['label']} "
                    f"| rank=#{tx.prospect.get('rank')}"
                )
                await asyncio.sleep(POST_DELAY_SECONDS)

            if posted:
                log(f"Scan complete — {posted} posted")

            # After first boot, only look back 1 day
            start_dt = date.today() - timedelta(days=1)

        except Exception as exc:
            log_exception(f"Transactions loop error: {exc}")

        await asyncio.sleep(TX_POLL_MINUTES * 60)


# ── DISCORD BOT ───────────────────────────────────────────────────────────────

intents = discord.Intents.default()


async def on_ready() -> None:
    global _bg_task
    assert client is not None
    log(f"Logged in as {client.user}")
    try:
        await client.change_presence(status=discord.Status.invisible)
        log(f"Looking for channel {CHANNEL_ID}")
        channel = client.get_channel(CHANNEL_ID)
        if channel is None:
            log(f"Channel {CHANNEL_ID} not found — aborting")
            return
        log(f"Found channel: {channel}")
        if _bg_task is None or _bg_task.done():
            _bg_task = asyncio.create_task(transactions_loop(channel))
    except Exception as exc:
        log_exception(f"on_ready error: {exc}")


async def start_prospect_news_bot() -> None:
    global client, _bg_task, _state
    if not TOKEN:
        raise RuntimeError("PROSPECT_NEWS_BOT_TOKEN is required")
    if CHANNEL_ID <= 0:
        raise RuntimeError("PROSPECT_NEWS_CHANNEL_ID is required")

    load_prospects()
    load_headshot_index()
    _state.update(_load_state_from_disk())

    _bg_task = None
    client = discord.Client(intents=intents)
    client.event(on_ready)
    await client.start(TOKEN, reconnect=False)


if __name__ == "__main__":
    asyncio.run(start_prospect_news_bot())

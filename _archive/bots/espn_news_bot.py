import asyncio
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands, tasks
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# =========================
# Configuration
# =========================
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
ESPN_PLAYER_NEWS_URL = os.getenv("ESPN_PLAYER_NEWS_URL", "https://fantasy.espn.com/baseball/playernews")
POLL_MINUTES = int(os.getenv("POLL_MINUTES", "5"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
STATE_DIR = Path(os.getenv("STATE_DIR", "state/espn_news"))
POST_EVERYTHING = os.getenv("POST_EVERYTHING", "false").lower() == "true"
MAX_ITEMS_PER_RUN = int(os.getenv("MAX_ITEMS_PER_RUN", "8"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# This should be filled in after you inspect the live DOM once.
# The bot tries several generic patterns so it can still work as a starter.
CARD_SELECTORS = [
    "article",
    "section article",
    "[data-testid*='news']",
    "[class*='news']",
    "[class*='News']",
    "li",
    "div",
]

TITLE_SELECTORS = [
    "h1",
    "h2",
    "h3",
    "h4",
    "a",
    "[class*='headline']",
    "[class*='title']",
]

BODY_SELECTORS = [
    "p",
    "div",
    "span",
    "[class*='description']",
    "[class*='content']",
    "[class*='summary']",
]

LINK_SELECTORS = [
    "a",
]

TEAM_POS_PATTERN = re.compile(r"\b([A-Z]{2,3})\s*[|·•-]\s*([A-Z]{1,3})\b")
PLAYER_NAME_PATTERN = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z'\-\.]+){1,3})\b")


# =========================
# Logging
# =========================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[ESPN BOT] %(message)s",
)
logger = logging.getLogger("espn_news_bot")


# =========================
# Models
# =========================
@dataclass
class NewsItem:
    source: str
    source_id: str
    player_name: str
    team: str
    position: str
    headline: str
    news: str
    spin: str
    published_at: str
    player_url: str
    news_url: str
    category: str
    importance: str
    raw_text: str


# =========================
# State manager
# =========================
class StateManager:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.posted_ids_path = self.state_dir / "posted_ids.json"
        self.recent_hashes_path = self.state_dir / "recent_hashes.json"
        self.source_health_path = self.state_dir / "source_health.json"
        self.player_cache_path = self.state_dir / "player_cache.json"

    def _load_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"Failed to read {path}: {exc}")
            return default

    def _save_json(self, path: Path, data: Any) -> None:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def load_posted_ids(self) -> set[str]:
        return set(self._load_json(self.posted_ids_path, []))

    def save_posted_ids(self, posted_ids: set[str]) -> None:
        self._save_json(self.posted_ids_path, sorted(posted_ids))

    def load_recent_hashes(self) -> dict[str, str]:
        return self._load_json(self.recent_hashes_path, {})

    def save_recent_hashes(self, recent_hashes: dict[str, str]) -> None:
        self._save_json(self.recent_hashes_path, recent_hashes)

    def save_source_health(self, payload: dict[str, Any]) -> None:
        self._save_json(self.source_health_path, payload)


# =========================
# Text helpers
# =========================
def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value).strip()
    value = value.replace("\u2019", "'")
    return value


def make_hash(*parts: str) -> str:
    joined = "||".join(clean_text(p).lower() for p in parts if p)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:24]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def infer_player_name(text: str) -> str:
    match = PLAYER_NAME_PATTERN.search(text)
    return match.group(1) if match else "Unknown Player"


def infer_team_and_position(text: str) -> tuple[str, str]:
    match = TEAM_POS_PATTERN.search(text)
    if match:
        return match.group(1), match.group(2)
    return "MLB", ""


def classify_item(text: str) -> tuple[str, str]:
    lower = text.lower()

    injury_terms = [
        "injury", "injured", "il", "finger", "hamstring", "shoulder", "elbow",
        "back", "knee", "wrist", "out of the game", "exits", "left the game",
        "day-to-day", "mri", "rehab", "activated", "returns from injury",
    ]
    lineup_terms = [
        "lineup", "batting", "starting", "not in the lineup", "rest day", "leading off",
    ]
    transaction_terms = [
        "optioned", "recalled", "promoted", "sent down", "demoted", "claimed",
        "traded", "signed", "released", "designated for assignment", "dfa",
    ]
    role_terms = [
        "closer", "save chance", "rotation", "bullpen", "starter", "reliever",
    ]
    prospect_terms = [
        "prospect", "debut", "called up", "promotion",
    ]

    if any(term in lower for term in injury_terms):
        return "injury", "high"
    if any(term in lower for term in transaction_terms):
        return "transaction", "high"
    if any(term in lower for term in role_terms):
        return "role", "high"
    if any(term in lower for term in lineup_terms):
        return "lineup", "medium"
    if any(term in lower for term in prospect_terms):
        return "prospect", "medium"
    return "general", "low"


def should_post(item: NewsItem) -> bool:
    if POST_EVERYTHING:
        return True
    return item.importance in {"high", "medium"}


def rewrite_update(item: NewsItem) -> tuple[str, str]:
    base = clean_text(item.news or item.headline or item.raw_text)
    spin = clean_text(item.spin)

    if not base:
        base = clean_text(item.raw_text)

    update = base
    if update and not update.endswith("."):
        update += "."

    if spin:
        fantasy_impact = spin
    else:
        if item.category == "injury":
            fantasy_impact = "Fantasy managers should watch for follow-up details before making a firm move."
        elif item.category == "transaction":
            fantasy_impact = "This could change playing time or role quickly, so it is worth tracking closely."
        elif item.category == "role":
            fantasy_impact = "This may affect saves, innings, or rotation value depending on how the team uses him next."
        elif item.category == "lineup":
            fantasy_impact = "This is more relevant in daily formats and lineup-lock leagues."
        else:
            fantasy_impact = "Useful context, but the fantasy impact looks limited unless more details follow."

    if fantasy_impact and not fantasy_impact.endswith("."):
        fantasy_impact += "."

    return update, fantasy_impact


# =========================
# ESPN source
# =========================
class ESPNSource:
    def __init__(self, url: str) -> None:
        self.url = url

    async def fetch_items(self) -> List[NewsItem]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            page = await browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                )
            )

            try:
                logger.info(f"Opening {self.url}")
                await page.goto(self.url, wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(4000)
                await self._best_effort_wait_for_content(page)
                items = await self._extract_items(page)
                logger.info(f"Extracted {len(items)} items from ESPN page")
                return items
            finally:
                await browser.close()

    async def _best_effort_wait_for_content(self, page) -> None:
        selectors = [
            "article",
            "a",
            "p",
            "[class*='news']",
            "[class*='News']",
            "[data-testid*='news']",
        ]
        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=3000)
                return
            except PlaywrightTimeoutError:
                continue

    async def _extract_items(self, page) -> List[NewsItem]:
        extracted: List[NewsItem] = []
        seen_hashes: set[str] = set()

        for card_selector in CARD_SELECTORS:
            try:
                cards = await page.locator(card_selector).all()
            except Exception:
                cards = []

            if not cards:
                continue

            for card in cards[:250]:
                try:
                    text = clean_text(await card.inner_text())
                except Exception:
                    continue

                if len(text) < 40:
                    continue
                if "support" in text.lower() and "issues joining a league" in text.lower():
                    continue

                item = await self._parse_card(card, text)
                if not item:
                    continue
                if item.source_id in seen_hashes:
                    continue
                seen_hashes.add(item.source_id)
                extracted.append(item)

            if extracted:
                break

        extracted.sort(key=lambda x: x.published_at, reverse=True)
        return extracted[:MAX_ITEMS_PER_RUN]

    async def _parse_card(self, card, card_text: str) -> Optional[NewsItem]:
        title = ""
        for selector in TITLE_SELECTORS:
            try:
                locator = card.locator(selector).first
                if await locator.count() > 0:
                    candidate = clean_text(await locator.inner_text())
                    if 4 <= len(candidate) <= 180:
                        title = candidate
                        break
            except Exception:
                continue

        paragraphs: List[str] = []
        for selector in BODY_SELECTORS:
            try:
                loc = card.locator(selector)
                count = await loc.count()
                for idx in range(min(count, 5)):
                    candidate = clean_text(await loc.nth(idx).inner_text())
                    if candidate and candidate not in paragraphs and len(candidate) > 20:
                        paragraphs.append(candidate)
            except Exception:
                continue
            if paragraphs:
                break

        href = ""
        for selector in LINK_SELECTORS:
            try:
                loc = card.locator(selector).first
                if await loc.count() > 0:
                    href = clean_text(await loc.get_attribute("href"))
                    if href:
                        break
            except Exception:
                continue

        combined = clean_text(" ".join([title] + paragraphs + [card_text]))
        if len(combined) < 60:
            return None

        player_name = infer_player_name(combined)
        team, position = infer_team_and_position(combined)
        category, importance = classify_item(combined)

        news = paragraphs[0] if paragraphs else card_text[:280]
        spin = paragraphs[1] if len(paragraphs) > 1 else ""

        source_id = make_hash(player_name, title, news, spin)
        resolved_href = self._resolve_url(href)

        return NewsItem(
            source="espn",
            source_id=source_id,
            player_name=player_name,
            team=team,
            position=position,
            headline=title or news[:120],
            news=news,
            spin=spin,
            published_at=now_iso(),
            player_url=resolved_href,
            news_url=resolved_href,
            category=category,
            importance=importance,
            raw_text=combined,
        )

    def _resolve_url(self, href: str) -> str:
        if not href:
            return self.url
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return f"https://fantasy.espn.com{href}"
        return self.url


# =========================
# Discord embed builder
# =========================
def build_embed(item: NewsItem) -> discord.Embed:
    update, fantasy_impact = rewrite_update(item)

    author_bits = ["ESPN Fantasy"]
    if item.team:
        author_bits.append(item.team)
    if item.position:
        author_bits.append(item.position)

    embed = discord.Embed(
        title=item.player_name,
        url=item.news_url or ESPN_PLAYER_NEWS_URL,
        description=(
            f"**Update:** {update}\n\n"
            f"**Fantasy impact:** {fantasy_impact}"
        ),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(name=" | ".join(author_bits))
    embed.set_footer(text=f"{item.category.title()} | ESPN Fantasy")

    if item.headline and item.headline.lower() != item.player_name.lower():
        embed.add_field(name="Headline", value=item.headline[:1024], inline=False)

    return embed


# =========================
# Bot
# =========================
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
state = StateManager(STATE_DIR)
source = ESPNSource(ESPN_PLAYER_NEWS_URL)


@bot.event
async def on_ready() -> None:
    logger.info(f"Logged in as {bot.user}")
    if not poll_espn_news.is_running():
        poll_espn_news.start()
        logger.info("ESPN poll task started")


@tasks.loop(minutes=POLL_MINUTES)
async def poll_espn_news() -> None:
    await run_poll_cycle()


@poll_espn_news.before_loop
async def before_poll() -> None:
    await bot.wait_until_ready()


async def run_poll_cycle() -> None:
    logger.info("Starting poll cycle")

    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        logger.error("DISCORD_CHANNEL_ID is missing or invalid")
        return

    posted_ids = state.load_posted_ids()
    recent_hashes = state.load_recent_hashes()

    try:
        items = await source.fetch_items()
        state.save_source_health({
            "last_success": now_iso(),
            "items_found": len(items),
            "url": ESPN_PLAYER_NEWS_URL,
        })
    except Exception as exc:
        logger.exception(f"Source fetch failed: {exc}")
        state.save_source_health({
            "last_failure": now_iso(),
            "error": str(exc),
            "url": ESPN_PLAYER_NEWS_URL,
        })
        return

    posted_count = 0

    for item in items:
        recent_text_hash = make_hash(item.player_name, item.headline, item.news)
        if item.source_id in posted_ids:
            logger.info(f"Skipping already posted item: {item.player_name} | {item.headline}")
            continue
        if recent_hashes.get(recent_text_hash) == item.player_name:
            logger.info(f"Skipping recent duplicate: {item.player_name} | {item.headline}")
            continue
        if not should_post(item):
            logger.info(f"Skipping low-priority item: {item.player_name} | {item.category}")
            continue

        embed = build_embed(item)
        await channel.send(embed=embed)
        logger.info(f"Posted {item.player_name} | {item.category} | {item.headline}")

        posted_ids.add(item.source_id)
        recent_hashes[recent_text_hash] = item.player_name
        posted_count += 1

    # keep recent hash file from growing forever
    if len(recent_hashes) > 2000:
        trimmed = list(recent_hashes.items())[-1000:]
        recent_hashes = dict(trimmed)

    state.save_posted_ids(posted_ids)
    state.save_recent_hashes(recent_hashes)
    logger.info(f"Poll cycle complete | posted={posted_count} | found={len(items)}")


@bot.command(name="espncheck")
async def espncheck(ctx: commands.Context) -> None:
    await ctx.send("Running ESPN news check...")
    await run_poll_cycle()
    await ctx.send("Done.")


@bot.command(name="espnpreview")
async def espnpreview(ctx: commands.Context) -> None:
    try:
        items = await source.fetch_items()
    except Exception as exc:
        await ctx.send(f"Preview failed: {exc}")
        return

    if not items:
        await ctx.send("No items found.")
        return

    preview_item = items[0]
    embed = build_embed(preview_item)
    await ctx.send(embed=embed)


def validate_config() -> None:
    if not DISCORD_TOKEN:
        raise RuntimeError("Missing DISCORD_TOKEN environment variable")
    if not DISCORD_CHANNEL_ID:
        raise RuntimeError("Missing DISCORD_CHANNEL_ID environment variable")


if __name__ == "__main__":
    validate_config()
    bot.run(DISCORD_TOKEN)

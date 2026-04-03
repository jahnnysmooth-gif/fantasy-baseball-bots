import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import discord
from discord.ext import commands

print("[BEAT WRITER] Module import started", flush=True)

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


BASE_DIR = Path(__file__).resolve().parent
if load_dotenv:
    load_dotenv(BASE_DIR / ".env")

try:
    import beat_writer_config  # type: ignore
    print("[BEAT WRITER] beat_writer_config imported", flush=True)
except Exception:
    beat_writer_config = None
    print("[BEAT WRITER] beat_writer_config not found", flush=True)


def cfg(name: str, default: Any = None) -> Any:
    if beat_writer_config and hasattr(beat_writer_config, name):
        value = getattr(beat_writer_config, name)
        if value is not None:
            return value
    return os.getenv(name, default)


print(f"[BEAT WRITER] Reading env vars...", flush=True)
print(f"[BEAT WRITER] PETER_GAMMONS_BOT_TOKEN from env: {os.getenv('PETER_GAMMONS_BOT_TOKEN', 'NOT FOUND')[:20]}...", flush=True)
print(f"[BEAT WRITER] TWEETSHIFT_CHANNEL_ID from env: {os.getenv('TWEETSHIFT_CHANNEL_ID', 'NOT FOUND')}", flush=True)
print(f"[BEAT WRITER] ON_THE_BEAT_CHANNEL_ID from env: {os.getenv('ON_THE_BEAT_CHANNEL_ID', 'NOT FOUND')}", flush=True)

PETER_GAMMONS_BOT_TOKEN = str(cfg("PETER_GAMMONS_BOT_TOKEN", "") or "").strip()
TWEETSHIFT_CHANNEL_ID = int(str(cfg("TWEETSHIFT_CHANNEL_ID", "0") or "0"))
ON_THE_BEAT_CHANNEL_ID = int(str(cfg("ON_THE_BEAT_CHANNEL_ID", "0") or "0"))

print(f"[BEAT WRITER] After cfg() - PETER_GAMMONS_BOT_TOKEN={'SET' if PETER_GAMMONS_BOT_TOKEN else 'NOT SET'}", flush=True)
print(f"[BEAT WRITER] After cfg() - TWEETSHIFT_CHANNEL_ID={TWEETSHIFT_CHANNEL_ID}", flush=True)
print(f"[BEAT WRITER] After cfg() - ON_THE_BEAT_CHANNEL_ID={ON_THE_BEAT_CHANNEL_ID}", flush=True)

STATE_DIR = BASE_DIR / "state" / "beat_writer"
POSTED_HASHES_FILE = STATE_DIR / "posted_hashes.json"
PLAYER_CONTENT_FILE = STATE_DIR / "player_content_hashes.json"

# Fallback logo
BOARD_REGS_LOGO = BASE_DIR / "our_logo_1.png"

KEYWORDS = [
    # Injury related
    "injury", "injured", "il", "placed on", "10-day il", "15-day il", "60-day il",
    "dtd", "day-to-day", "reaggravated", "strain", "sprain", "surgery", "rehab",
    
    # Roster moves
    "activated", "optioned", "recalled", "designated for assignment", "dfa", 
    "claimed", "waived", "released", "signed", "outrighted",
    
    # Lineup/playing time
    "scratched", "removed from", "exited", "left the game",
    
    # Specific injury terms
    "oblique", "hamstring", "shoulder", "elbow", "knee", "back", "concussion",
    "broken", "fracture", "torn", "ucl", "tommy john"
]

TEAM_COLORS = {
    "ari": 0xA71930, "diamondbacks": 0xA71930,
    "atl": 0xCE1141, "braves": 0xCE1141,
    "bal": 0xDF4601, "orioles": 0xDF4601,
    "bos": 0xBD3039, "red sox": 0xBD3039,
    "chc": 0x0E3386, "cubs": 0x0E3386,
    "cws": 0x27251F, "white sox": 0x27251F,
    "cin": 0xC6011F, "reds": 0xC6011F,
    "cle": 0xE31937, "guardians": 0xE31937,
    "col": 0x33006F, "rockies": 0x33006F,
    "det": 0x0C2340, "tigers": 0x0C2340,
    "hou": 0xEB6E1F, "astros": 0xEB6E1F,
    "kc": 0x004687, "royals": 0x004687,
    "laa": 0xBA0021, "angels": 0xBA0021,
    "lad": 0x005A9C, "dodgers": 0x005A9C,
    "mia": 0x00A3E0, "marlins": 0x00A3E0,
    "mil": 0x12284B, "brewers": 0x12284B,
    "min": 0x002B5C, "twins": 0x002B5C,
    "nym": 0x002D72, "mets": 0x002D72,
    "nyy": 0x132448, "yankees": 0x132448,
    "oak": 0x003831, "athletics": 0x003831,
    "phi": 0xE81828, "phillies": 0xE81828,
    "pit": 0xFDB827, "pirates": 0xFDB827,
    "sd": 0x2F241D, "padres": 0x2F241D,
    "sf": 0xFD5A1E, "giants": 0xFD5A1E,
    "sea": 0x0C2C56, "mariners": 0x0C2C56,
    "stl": 0xC41E3A, "cardinals": 0xC41E3A,
    "tb": 0x092C5C, "rays": 0x092C5C,
    "tex": 0x003278, "rangers": 0x003278,
    "tor": 0x134A8E, "blue jays": 0x134A8E,
    "wsh": 0xAB0003, "nationals": 0xAB0003,
}

TEAM_WORD_TO_ABBR = {
    "reds": "CIN", "angels": "LAA", "brewers": "MIL", "astros": "HOU",
    "nationals": "WSH", "rockies": "COL", "yankees": "NYY", "dodgers": "LAD",
    "mets": "NYM", "orioles": "BAL", "guardians": "CLE", "twins": "MIN",
    "mariners": "SEA", "phillies": "PHI", "padres": "SD", "pirates": "PIT",
    "cubs": "CHC", "cardinals": "STL", "diamondbacks": "ARI", "blue jays": "TOR",
    "tigers": "DET", "rangers": "TEX", "giants": "SF", "athletics": "OAK",
    "royals": "KC", "marlins": "MIA", "rays": "TB", "red sox": "BOS",
    "white sox": "CWS", "braves": "ATL"
}

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
    "OAK": "https://a.espncdn.com/i/teamlogos/mlb/500/oak.png",
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
}


def generate_headline(content: str, author: str) -> str:
    """Generate a short headline from tweet content"""
    normalized = normalize_text(content)
    
    # Extract player name (first capitalized name)
    player_match = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', content)
    player_name = player_match.group(1) if player_match else None
    
    # IL placements (check first - highest priority)
    if "to il" in normalized or "on il" in normalized or "to the il" in normalized or "on the il" in normalized:
        if player_name:
            if "15-day" in normalized:
                return f"🏥 {player_name} → 15-Day IL"
            elif "60-day" in normalized:
                return f"🏥 {player_name} → 60-Day IL"
            elif "10-day" in normalized:
                return f"🏥 {player_name} → 10-Day IL"
            else:
                return f"🏥 {player_name} → IL"
    
    # Activations/Reinstatements
    if "activated from" in normalized or "reinstated from" in normalized:
        if player_name:
            return f"✅ {player_name} Activated"
    
    # Recalls (only if not going TO IL)
    if "recalled from" in normalized and "to il" not in normalized and "on il" not in normalized:
        if player_name:
            return f"📈 {player_name} Recalled"
    
    # Roster moves
    if "optioned to" in normalized:
        if player_name:
            return f"📉 {player_name} Optioned"
    
    if "released" in normalized or "released by" in normalized:
        if player_name:
            return f"🚪 {player_name} Released"
    
    if "claimed" in normalized and ("waiver" in normalized or "from" in normalized):
        if player_name:
            return f"📥 {player_name} Claimed"
    
    if "dealt to" in normalized or "traded to" in normalized:
        if player_name:
            return f"🔄 {player_name} Traded"
    
    # In-game injuries
    if "left the game" in normalized or "exited" in normalized or "removed from" in normalized:
        if player_name:
            return f"⚠️ {player_name} Left Game"
    
    if "scratched" in normalized and "lineup" in normalized:
        if player_name:
            return f"❌ {player_name} Scratched"
    
    # X-rays/Medical
    if "x-rays" in normalized or "x rays" in normalized:
        if player_name:
            if "negative" in normalized:
                return f"✅ {player_name} X-Rays Negative"
            else:
                return f"🏥 {player_name} X-Rays"
    
    # Rehab assignment
    if "rehab assignment" in normalized or ("rehab" in normalized and "assignment" in normalized):
        if player_name:
            return f"🔄 {player_name} Rehab Update"
    
    # Default
    return "📰 Injury/Roster Update"


def log(msg: str) -> None:
    print(f"[BEAT WRITER BOT] {msg}", flush=True)


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


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip().lower()


@dataclass
class TweetData:
    author: str
    content: str
    timestamp: datetime
    message_id: int
    tweet_url: Optional[str] = None
    team_abbr: Optional[str] = None
    
    def content_hash(self) -> str:
        return sha1_text(normalize_text(self.content))
    
    def extract_player_name(self) -> Optional[str]:
        """Extract player name from tweet content (simple heuristic)"""
        # Look for capitalized two-word names
        pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b'
        matches = re.findall(pattern, self.content)
        if matches:
            return matches[0]
        return None
    
    def extract_team_abbr(self) -> Optional[str]:
        """Extract team abbreviation from tweet content"""
        content_lower = normalize_text(self.content)
        
        # Skip x-rays false positive for Rays
        if "x-rays" in content_lower or "x rays" in content_lower:
            content_lower = content_lower.replace("x-rays", "xrays").replace("x rays", "xrays")
        
        # Direct hashtag matches: #Rays, #Dodgers, etc.
        for team_word, abbr in TEAM_WORD_TO_ABBR.items():
            if f"#{team_word}" in content_lower or f" {team_word} " in content_lower or content_lower.startswith(team_word + " ") or content_lower.endswith(" " + team_word):
                return abbr
        
        return None


class BotState:
    def __init__(self) -> None:
        ensure_state_dir()
        self.posted_hashes: List[str] = load_json_file(POSTED_HASHES_FILE, [])
        self.player_content: Dict[str, Dict[str, Any]] = load_json_file(PLAYER_CONTENT_FILE, {})

    def save(self) -> None:
        save_json_file(POSTED_HASHES_FILE, self.posted_hashes[-5000:])
        save_json_file(PLAYER_CONTENT_FILE, self.player_content)

    def already_posted(self, content_hash: str) -> bool:
        return content_hash in self.posted_hashes

    def is_duplicate_subject(self, player_name: str, content_hash: str) -> bool:
        """Check if we've already posted similar content about this player"""
        if not player_name:
            return False
        
        player_key = normalize_text(player_name)
        if player_key not in self.player_content:
            return False
        
        last_hash = self.player_content[player_key].get("last_hash")
        return last_hash == content_hash

    def record_post(self, tweet: TweetData) -> None:
        content_hash = tweet.content_hash()
        self.posted_hashes.append(content_hash)
        
        player_name = tweet.extract_player_name()
        if player_name:
            player_key = normalize_text(player_name)
            self.player_content[player_key] = {
                "last_hash": content_hash,
                "last_content": tweet.content[:200],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }


class BeatWriterBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        
        self.state = BotState()

    async def on_ready(self) -> None:
        log(f"Logged in as {self.user}")
        log(f"Listening to channel: {TWEETSHIFT_CHANNEL_ID}")
        log(f"Posting to channel: {ON_THE_BEAT_CHANNEL_ID}")
        
        # Scan last 12 hours on startup
        await self.scan_recent_messages()

    async def on_message(self, message: discord.Message) -> None:
        # Debug: log all messages from tweetshift channel
        if message.channel.id == TWEETSHIFT_CHANNEL_ID:
            log(f"Message received from tweetshift channel (ID: {message.id})")
        
        # Ignore own messages
        if message.author == self.user:
            return
        
        # Only listen to tweetshift channel
        if message.channel.id != TWEETSHIFT_CHANNEL_ID:
            return
        
        # Parse tweetshift format
        tweet = self.parse_tweetshift_message(message)
        if not tweet:
            log(f"Failed to parse message {message.id}")
            return
        
        # Filter by keywords
        if not self.contains_keywords(tweet.content):
            log(f"Skipping (no keywords): {tweet.author} - {tweet.content[:80]}")
            return
        
        # Check if already posted
        content_hash = tweet.content_hash()
        if self.state.already_posted(content_hash):
            log(f"Skipping (exact duplicate): {tweet.author} - {tweet.content[:80]}")
            return
        
        # Check if duplicate subject for same player
        player_name = tweet.extract_player_name()
        if self.state.is_duplicate_subject(player_name, content_hash):
            log(f"Skipping (duplicate subject for {player_name}): {tweet.content[:80]}")
            return
        
        # Post to public channel
        await self.post_tweet(tweet)
        self.state.record_post(tweet)
        self.state.save()
        
        log(f"Posted: {tweet.author} - {tweet.content[:100]}")

    async def scan_recent_messages(self) -> None:
        """Scan last 18 hours of tweetshift channel on startup"""
        channel = self.get_channel(TWEETSHIFT_CHANNEL_ID)
        if channel is None:
            log(f"Tweetshift channel not found: {TWEETSHIFT_CHANNEL_ID}")
            return
        
        cutoff = datetime.now(timezone.utc).timestamp() - (18 * 60 * 60)
        log("Scanning last 18 hours of tweetshift messages...")
        
        processed = 0
        posted = 0
        
        async for message in channel.history(limit=500):
            if message.created_at.timestamp() < cutoff:
                break
            
            if message.author == self.user:
                continue
            
            tweet = self.parse_tweetshift_message(message)
            if not tweet:
                continue
            
            processed += 1
            
            if not self.contains_keywords(tweet.content):
                continue
            
            content_hash = tweet.content_hash()
            if self.state.already_posted(content_hash):
                continue
            
            player_name = tweet.extract_player_name()
            if self.state.is_duplicate_subject(player_name, content_hash):
                continue
            
            await self.post_tweet(tweet)
            self.state.record_post(tweet)
            posted += 1
            
            log(f"Posted (backfill): {tweet.author} - {tweet.content[:100]}")
            
            # Small delay to avoid rate limits
            import asyncio
            await asyncio.sleep(1)
        
        self.state.save()
        log(f"Backfill complete: processed={processed}, posted={posted}")


    def parse_tweetshift_message(self, message: discord.Message) -> Optional[TweetData]:
        """Parse tweetshift format - reads from embeds OR plain text"""
        
        # Try embed format first (preferred)
        if message.embeds:
            embed = message.embeds[0]
            
            # Extract author from embed.author.name
            author = None
            if embed.author and embed.author.name:
                author_text = embed.author.name
                handle_match = re.search(r'\(@([^)]+)\)', author_text)
                if handle_match:
                    author = handle_match.group(1)
                else:
                    author = author_text
            
            # Fallback: try to extract author from title or footer
            if not author:
                if embed.title:
                    handle_match = re.search(r'@([A-Za-z0-9_]+)', embed.title)
                    if handle_match:
                        author = handle_match.group(1)
                
                if not author and embed.footer and embed.footer.text:
                    handle_match = re.search(r'@([A-Za-z0-9_]+)', embed.footer.text)
                    if handle_match:
                        author = handle_match.group(1)
            
            # Get tweet content from embed description
            tweet_content = embed.description
            if not tweet_content:
                log(f"Message {message.id} embed has no description - skipping")
                return None
            
            # Unescape markdown characters
            tweet_content = tweet_content.replace('\\#', '#').replace('\\-', '-').replace('\\.', '.')
            tweet_content = tweet_content.replace('\\*', '*').replace('\\(', '(').replace('\\)', ')')
            
            # Extract tweet URL from embed.url
            tweet_url = embed.url if embed.url else None
            
            # Use generic author if none found
            if not author:
                author = "BeatWriter"
            
        else:
            # Plain text format - extract author and content from message.content
            content = message.content.strip()
            if not content:
                log(f"Message {message.id} has no content - skipping")
                return None
            
            # Try to extract author from text
            # Common formats: "Author - tweet text" or just "tweet text"
            author_match = re.match(r'^([A-Za-z0-9_]+)\s*-\s*(.+)', content, re.DOTALL)
            if author_match:
                author = author_match.group(1)
                tweet_content = author_match.group(2).strip()
            else:
                # No clear author, use generic and use full content
                author = "BeatWriter"
                tweet_content = content
            
            tweet_url = None
        
        # Create tweet data and extract team
        tweet_data = TweetData(
            author=author,
            content=tweet_content,
            timestamp=message.created_at,
            message_id=message.id,
            tweet_url=tweet_url
        )
        
        # Extract team abbreviation
        tweet_data.team_abbr = tweet_data.extract_team_abbr()
        
        return tweet_data

    def contains_keywords(self, text: str) -> bool:
        """Check if text contains any relevant keywords"""
        normalized = normalize_text(text)
        
        # Hard skip patterns - instant reject
        hard_skip = [
            "the five", "probables for", "probable for",
            "walkout video", "channeling his inner",
            "cold reality", "temperatures in",
            "trust is a two-way", "compile 16 hits", "pile 16 hits",
            "world series", "curse-breaking", "10 yrs after winning",
            "mailbag", "submit any", "hard hit % leaders",
            "throwback", "merch being sold", "will wear no.",
            "era begins tomorrow", "planning to run another",
            "expecting among many", "aggressiveness of some teams",
            # Game action
            "didn't realize", "handed him the ball", "will replace",
            "will make his debut", "gave up a run", "got it back",
            "hit his first", "hits the first", "first career",
            "ugliest inning", "not to pile on", "tough first",
            "hoped to stretch", "played wild games", "beginning the season",
            "equivalent of the backup", "dealing with the stomach bug",
            "resorted to using", "talked before the game",
            # Non-injury "back" references
            "looking back at", "back at", "back in", "are back at",
            "want it bad", "want it back"
        ]
        
        for pattern in hard_skip:
            if pattern in normalized:
                return False
        
        # MUST contain one of these specific phrases
        required_phrases = [
            # IL transactions
            "to il", "to the il", "on il", "on the il", 
            "placed on", "activated from", "reinstated from",
            "10-day il", "15-day il", "60-day il",
            
            # Roster moves
            "optioned to", "recalled from", 
            "designated for assignment", "designate", "dfa",
            "claimed off", "released by", "dealt to", "traded to",
            
            # In-game injuries
            "left the game", "left game", "exited the game", "exited with",
            "removed from the game", "out of the game",
            "helped off", "limped off", "carried off",
            
            # Injury updates
            "x-rays", "x rays", "mri", "ct scan",
            "placed on injured", "to injured list",
            "scratched from lineup", "scratched from the lineup",
            "day-to-day with", "dtd with",
            "suffered a", "reaggravated", "underwent surgery",
            "scheduled for surgery", "rehab assignment",
            
            # Specific injury diagnosis
            "oblique strain", "hamstring strain", "shoulder injury",
            "elbow injury", "knee injury", "ankle injury",
            "hit by a pitch on", "fouled a ball off"
        ]
        
        return any(phrase in normalized for phrase in required_phrases)

    def extract_team_color(self, content: str) -> int:
        """Try to extract team from content and return color"""
        normalized = normalize_text(content)
        for team, color in TEAM_COLORS.items():
            if team in normalized:
                return color
        return 0x1DA1F2  # Twitter blue default

    async def post_tweet(self, tweet: TweetData) -> None:
        channel = self.get_channel(ON_THE_BEAT_CHANNEL_ID)
        if channel is None:
            log(f"Public channel not found: {ON_THE_BEAT_CHANNEL_ID}")
            return
        
        # Get team color
        color = self.extract_team_color(tweet.content)
        
        # Generate headline
        headline = generate_headline(tweet.content, tweet.author)
        
        # Build embed
        embed = discord.Embed(
            title=headline,
            description=tweet.content,
            color=color,
            timestamp=tweet.timestamp
        )
        embed.set_author(name="On The Beat | Board Regs Fantasy Baseball")
        
        # Add team logo if detected, otherwise use Board Regs logo
        if tweet.team_abbr and tweet.team_abbr in TEAM_LOGOS:
            embed.set_thumbnail(url=TEAM_LOGOS[tweet.team_abbr])
        elif BOARD_REGS_LOGO.exists():
            # Upload local logo file
            file = discord.File(BOARD_REGS_LOGO, filename="board_regs_logo.png")
            embed.set_thumbnail(url="attachment://board_regs_logo.png")
            
            # Add source link
            if tweet.tweet_url:
                embed.add_field(name="", value=f"[Source: @{tweet.author}]({tweet.tweet_url})", inline=False)
            else:
                embed.set_footer(text=f"Source: @{tweet.author}")
            
            await channel.send(file=file, embed=embed)
            return
        
        # Add source link
        if tweet.tweet_url:
            embed.add_field(name="", value=f"[Source: @{tweet.author}]({tweet.tweet_url})", inline=False)
        else:
            embed.set_footer(text=f"Source: @{tweet.author}")
        
        await channel.send(embed=embed)


def validate_config() -> None:
    if not PETER_GAMMONS_BOT_TOKEN:
        raise RuntimeError("Missing PETER_GAMMONS_BOT_TOKEN")
    if not TWEETSHIFT_CHANNEL_ID:
        raise RuntimeError("Missing TWEETSHIFT_CHANNEL_ID")
    if not ON_THE_BEAT_CHANNEL_ID:
        raise RuntimeError("Missing ON_THE_BEAT_CHANNEL_ID")


async def start_beat_writer_bot() -> None:
    """Entry point for main.py integration"""
    validate_config()
    bot = BeatWriterBot()
    await bot.start(PETER_GAMMONS_BOT_TOKEN)


def main() -> None:
    """Standalone entry point for local testing"""
    import asyncio
    validate_config()
    bot = BeatWriterBot()
    bot.run(PETER_GAMMONS_BOT_TOKEN)


if __name__ == "__main__":
    main()

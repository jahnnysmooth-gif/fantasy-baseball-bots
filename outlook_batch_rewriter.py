import os
import json
import asyncio
from pathlib import Path
from datetime import datetime

import discord
from discord.ext import commands
import aiohttp

# Import functions from your player_profiles_bot
from player_profiles_bot import (
    FORUM_CHANNEL_ID,
    resolve_bundle_from_thread,
    log_profiles,
)

# =========================
# CONFIG
# =========================
TOKEN = os.getenv("PLAYER_PROFILES_TOKEN")

ANTHROPIC_API_KEY = os.getenv("PROFILES_REWRITE_KEY")
if not ANTHROPIC_API_KEY:
    raise ValueError("PROFILES_REWRITE_KEY environment variable is not set")

# Batch processing settings
BATCH_SIZE = 10  # Process 10 threads, then save progress
DELAY_BETWEEN_REQUESTS = 2  # Seconds between API calls
DELAY_BETWEEN_POSTS = 3  # Seconds between Discord edits

# Output directory for logs
OUTPUT_DIR = Path("outlook_batch_results")
OUTPUT_DIR.mkdir(exist_ok=True)

# Progress tracking
PROGRESS_FILE = OUTPUT_DIR / "batch_progress.json"

# =========================
# LOGGING
# =========================
def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}][BATCH] {msg}", flush=True)

# =========================
# DISCORD BOT SETUP
# =========================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# PROGRESS TRACKING
# =========================
def load_progress():
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "processed_threads": [],
        "failed_threads": [],
        "total_processed": 0,
        "total_failed": 0,
        "started_at": None,
        "last_updated": None,
    }


def save_progress(progress):
    """Save progress to file."""
    progress["last_updated"] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)
    log(f"Progress saved: {progress['total_processed']} processed, {progress['total_failed']} failed")


# =========================
# CLAUDE API INTEGRATION
# =========================
async def generate_outlook_with_claude(player_bundle: dict) -> str:
    """
    Generate a fantasy outlook using Claude API.
    
    Args:
        player_bundle: Dict containing profile, curr_metrics, prev_metrics
    
    Returns:
        Generated outlook text
    """
    profile = player_bundle["profile"]
    is_pitcher = player_bundle["is_pitcher"]
    curr_metrics = player_bundle.get("curr_metrics", {})
    prev_metrics = player_bundle.get("prev_metrics", {})
    
    # Extract relevant stats
    player_name = profile.get("full_name", "Unknown")
    team = profile.get("team", "Unknown")
    position = profile.get("position", "Unknown")
    age = profile.get("age", "Unknown")
    
    # Build context for Claude
    if is_pitcher:
        p = profile.get("pitching_stats") or {}
        prev_profile = player_bundle.get("previous_profile") or {}
        prev_p = prev_profile.get("pitching_stats") or {}
        
        stats_context = f"""
Pitcher: {player_name} ({team} - {position}, Age {age})

2025 Stats:
- IP: {p.get('inningsPitched', 'N/A')}
- ERA: {p.get('era', 'N/A')}
- WHIP: {p.get('whip', 'N/A')}
- K/9: {p.get('strikeoutsPer9Inn', 'N/A')}
- BB/9: {p.get('walksPer9Inn', 'N/A')}
- Wins: {p.get('wins', 'N/A')}
- Saves: {p.get('saves', 'N/A')}

2024 Stats (for comparison):
- IP: {prev_p.get('inningsPitched', 'N/A')}
- ERA: {prev_p.get('era', 'N/A')}
- WHIP: {prev_p.get('whip', 'N/A')}
- K/9: {prev_p.get('strikeoutsPer9Inn', 'N/A')}

Statcast Metrics 2025:
- Avg Exit Velo Against: {curr_metrics.get('avg_ev', 'N/A')} mph
- Barrel% Against: {curr_metrics.get('barrel_pct', 'N/A')}%
- Hard Hit%: {curr_metrics.get('hard_hit_pct', 'N/A')}%
- xERA: {curr_metrics.get('xera', 'N/A')}
- xwOBA: {curr_metrics.get('xwoba', 'N/A')}
- K%: {curr_metrics.get('k_pct', 'N/A')}%
- BB%: {curr_metrics.get('bb_pct', 'N/A')}%
"""
    else:
        h = profile.get("hitting_stats") or {}
        prev_profile = player_bundle.get("previous_profile") or {}
        prev_h = prev_profile.get("hitting_stats") or {}
        
        stats_context = f"""
Hitter: {player_name} ({team} - {position}, Age {age})

2025 Stats:
- AVG: {h.get('avg', 'N/A')}
- HR: {h.get('homeRuns', 'N/A')}
- RBI: {h.get('rbi', 'N/A')}
- R: {h.get('runs', 'N/A')}
- SB: {h.get('stolenBases', 'N/A')}
- OPS: {h.get('ops', 'N/A')}

2024 Stats (for comparison):
- AVG: {prev_h.get('avg', 'N/A')}
- HR: {prev_h.get('homeRuns', 'N/A')}
- RBI: {prev_h.get('rbi', 'N/A')}
- OPS: {prev_h.get('ops', 'N/A')}

Statcast Metrics 2025:
- Avg Exit Velocity: {curr_metrics.get('avg_ev', 'N/A')} mph
- Barrel%: {curr_metrics.get('barrel_pct', 'N/A')}%
- Hard Hit%: {curr_metrics.get('hard_hit_pct', 'N/A')}%
- K%: {curr_metrics.get('k_pct', 'N/A')}%
- BB%: {curr_metrics.get('bb_pct', 'N/A')}%
- xBA: {curr_metrics.get('xba', 'N/A')}
- xSLG: {curr_metrics.get('xslg', 'N/A')}
- xwOBA: {curr_metrics.get('xwoba', 'N/A')}
"""
    
    # System prompt for Claude
    system_prompt = """You are a fantasy baseball analyst writing player outlooks for 2026. 

Your outlook should:
- Be 4-6 sentences in a single paragraph
- Assess the player's fantasy value going into 2026 based on their 2025 performance
- Reference specific stats and underlying metrics (Statcast data) to support your analysis
- Compare 2025 to 2024 to identify trends (improvement, decline, consistency)
- Be balanced - acknowledge both strengths and concerns
- Focus on fantasy-relevant categories (HR, RBI, R, SB, AVG for hitters; W, SV, K, ERA, WHIP for pitchers)
- Write in a confident, analytical tone (not overly cautious or hedging)
- Avoid clichés like "potential sleeper" or "must-draft" - be specific about value

Do NOT include any preamble, title, or labels. Just write the outlook paragraph."""

    user_prompt = f"""Based on the following player data, write a fantasy baseball outlook for 2026:

{stats_context}

Write the outlook now:"""

    # Call Claude API
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01",
                "x-api-key": ANTHROPIC_API_KEY,
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 500,
                "system": [
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"}
                    }
                ],
                "messages": [
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ]
            }
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"Claude API error {response.status}: {error_text}")
            
            data = await response.json()
            
            # Extract text from response
            outlook_text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    outlook_text += block.get("text", "")
            
            return outlook_text.strip()


# =========================
# OUTLOOK MESSAGE FINDING & EDITING
# =========================
async def find_outlook_message(thread: discord.Thread) -> discord.Message | None:
    """Find the message containing the outlook in a thread."""
    async for message in thread.history(limit=50):
        if message.author.bot and message.embeds:
            for embed in message.embeds:
                # Look for "2026 Outlook" in description
                if embed.description and "2026 outlook" in embed.description.lower():
                    return message
    return None


async def update_outlook_in_message(message: discord.Message, new_outlook: str) -> bool:
    """Update the outlook in an existing embed message."""
    if not message.embeds:
        return False
    
    embed = message.embeds[0]
    
    # Find and replace the outlook section in the description
    if not embed.description:
        return False
    
    lines = embed.description.split('\n')
    new_lines = []
    in_outlook = False
    outlook_replaced = False
    
    for line in lines:
        if "2026 outlook" in line.lower():
            in_outlook = True
            new_lines.append(line)  # Keep the header
            new_lines.append(new_outlook)  # Add new outlook
            outlook_replaced = True
            continue
        
        if in_outlook:
            # Skip old outlook lines until we hit the next section
            if line.startswith("MLB_ID:") or line.startswith("**") or line == "":
                in_outlook = False
                new_lines.append(line)
            continue
        
        new_lines.append(line)
    
    if not outlook_replaced:
        return False
    
    # Update the embed
    embed.description = '\n'.join(new_lines)
    
    try:
        await message.edit(embed=embed)
        return True
    except Exception as e:
        log(f"Failed to edit message: {e}")
        return False


# =========================
# BATCH PROCESSING
# =========================
async def get_all_threads(forum_channel):
    """Get all threads from the forum (active and archived)."""
    threads = []
    
    # Get active threads
    for thread in forum_channel.threads:
        threads.append(thread)
    
    # Get archived threads
    async for thread in forum_channel.archived_threads(limit=None):
        threads.append(thread)
    
    log(f"Found {len(threads)} total threads")
    return threads


async def process_batch():
    """Process all threads in batches."""
    log("Starting batch outlook rewrite...")
    
    forum_channel = bot.get_channel(FORUM_CHANNEL_ID)
    if not forum_channel:
        log("ERROR: Could not find forum channel")
        return
    
    # Load progress
    progress = load_progress()
    if progress["started_at"] is None:
        progress["started_at"] = datetime.now().isoformat()
    
    # Get all threads
    all_threads = await get_all_threads(forum_channel)
    
    # Filter out already processed threads
    processed_ids = set(progress["processed_threads"])
    failed_ids = set(progress["failed_threads"])
    remaining_threads = [t for t in all_threads if t.id not in processed_ids and t.id not in failed_ids]
    
    log(f"Total threads: {len(all_threads)}")
    log(f"Already processed: {len(processed_ids)}")
    log(f"Previously failed: {len(failed_ids)}")
    log(f"Remaining to process: {len(remaining_threads)}")
    
    if not remaining_threads:
        log("✅ All threads already processed!")
        return
    
    # Process threads
    for i, thread in enumerate(remaining_threads, 1):
        try:
            log(f"\n[{i}/{len(remaining_threads)}] Processing: {thread.name}")
            
            # Find the outlook message
            log("  🔍 Finding outlook message...")
            outlook_message = await find_outlook_message(thread)
            
            if not outlook_message:
                log("  ⚠️  No outlook message found, skipping")
                progress["failed_threads"].append(thread.id)
                progress["total_failed"] += 1
                continue
            
            # Resolve player bundle
            log("  📊 Resolving player bundle...")
            bundle = await resolve_bundle_from_thread(thread)
            
            if not bundle:
                log("  ⚠️  Could not resolve bundle, skipping")
                progress["failed_threads"].append(thread.id)
                progress["total_failed"] += 1
                continue
            
            # Generate new outlook
            log("  🤖 Generating new outlook with Claude...")
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)  # Rate limiting
            new_outlook = await generate_outlook_with_claude(bundle)
            
            # Update the message
            log("  ✏️  Updating Discord message...")
            await asyncio.sleep(DELAY_BETWEEN_POSTS)  # Rate limiting
            success = await update_outlook_in_message(outlook_message, new_outlook)
            
            if success:
                log(f"  ✅ Updated ({len(new_outlook)} chars)")
                progress["processed_threads"].append(thread.id)
                progress["total_processed"] += 1
            else:
                log("  ❌ Failed to update message")
                progress["failed_threads"].append(thread.id)
                progress["total_failed"] += 1
            
            # Save progress every BATCH_SIZE threads
            if (i % BATCH_SIZE) == 0:
                save_progress(progress)
                log(f"\n📊 Checkpoint: {progress['total_processed']}/{len(all_threads)} completed\n")
            
        except Exception as e:
            log(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            progress["failed_threads"].append(thread.id)
            progress["total_failed"] += 1
            continue
    
    # Final save
    save_progress(progress)
    
    # Summary
    log("\n" + "="*60)
    log("BATCH COMPLETE")
    log("="*60)
    log(f"✅ Successfully processed: {progress['total_processed']}")
    log(f"❌ Failed: {progress['total_failed']}")
    log(f"📄 Progress file: {PROGRESS_FILE}")
    log("="*60)


@bot.event
async def on_ready():
    log(f"Logged in as {bot.user}")
    
    try:
        await process_batch()
    except Exception as e:
        log(f"Fatal error during batch processing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        log("Shutting down...")
        await bot.close()


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("PLAYER_PROFILES_TOKEN environment variable is not set")
    
    bot.run(TOKEN)

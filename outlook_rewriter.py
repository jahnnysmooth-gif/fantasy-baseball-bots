import os
import json
import asyncio
from pathlib import Path

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

# Sample size for initial testing
SAMPLE_SIZE = 5

# Output directory for results
OUTPUT_DIR = Path("outlook_samples")
OUTPUT_DIR.mkdir(exist_ok=True)

# =========================
# LOGGING
# =========================
def log(msg: str) -> None:
    print(f"[REWRITER] {msg}", flush=True)

# =========================
# DISCORD BOT SETUP
# =========================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# CLAUDE API INTEGRATION
# =========================
async def generate_outlook_with_claude(player_bundle: dict) -> str:
    """
    Generate a fantasy outlook using Claude API.
    
    Args:
        player_bundle: Dict containing profile, stats, and metrics
    
    Returns:
        Generated outlook text
    """
    profile = player_bundle["profile"]
    is_pitcher = player_bundle["is_pitcher"]
    
    # Extract relevant stats
    player_name = profile.get("fullName", "Unknown")
    team = profile.get("team", "Unknown")
    position = profile.get("position", "Unknown")
    age = profile.get("age", "Unknown")
    
    # Build stats summary
    stats_2025 = player_bundle.get("stats_2025", {})
    stats_2024 = player_bundle.get("stats_2024", {})
    
    # Statcast metrics
    batting_ev = player_bundle.get("batting_ev_2025", {})
    batting_x = player_bundle.get("batting_x_2025", {})
    pitching_ev = player_bundle.get("pitching_ev_2025", {})
    pitching_x = player_bundle.get("pitching_x_2025", {})
    
    # Build context for Claude
    if is_pitcher:
        stats_context = f"""
Pitcher: {player_name} ({team} - {position}, Age {age})

2025 Stats:
- IP: {stats_2025.get('ip', 'N/A')}
- ERA: {stats_2025.get('era', 'N/A')}
- WHIP: {stats_2025.get('whip', 'N/A')}
- K/9: {stats_2025.get('k9', 'N/A')}
- BB/9: {stats_2025.get('bb9', 'N/A')}
- Wins: {stats_2025.get('w', 'N/A')}
- Saves: {stats_2025.get('sv', 'N/A')}

2024 Stats (for comparison):
- IP: {stats_2024.get('ip', 'N/A')}
- ERA: {stats_2024.get('era', 'N/A')}
- WHIP: {stats_2024.get('whip', 'N/A')}
- K/9: {stats_2024.get('k9', 'N/A')}

Statcast Metrics 2025:
- Avg Exit Velo Against: {pitching_ev.get('avg_hit_speed', 'N/A')} mph
- Barrel% Against: {pitching_ev.get('barrel_batted_rate', 'N/A')}%
- Hard Hit%: {pitching_ev.get('hard_hit_percent', 'N/A')}%
- xERA: {pitching_x.get('est_era', 'N/A')}
- xwOBA: {pitching_x.get('est_woba', 'N/A')}
"""
    else:
        stats_context = f"""
Hitter: {player_name} ({team} - {position}, Age {age})

2025 Stats:
- AVG: {stats_2025.get('avg', 'N/A')}
- HR: {stats_2025.get('hr', 'N/A')}
- RBI: {stats_2025.get('rbi', 'N/A')}
- R: {stats_2025.get('r', 'N/A')}
- SB: {stats_2025.get('sb', 'N/A')}
- OPS: {stats_2025.get('ops', 'N/A')}

2024 Stats (for comparison):
- AVG: {stats_2024.get('avg', 'N/A')}
- HR: {stats_2024.get('hr', 'N/A')}
- RBI: {stats_2024.get('rbi', 'N/A')}
- OPS: {stats_2024.get('ops', 'N/A')}

Statcast Metrics 2025:
- Avg Exit Velocity: {batting_ev.get('avg_hit_speed', 'N/A')} mph
- Barrel%: {batting_ev.get('barrel_batted_rate', 'N/A')}%
- Hard Hit%: {batting_ev.get('hard_hit_percent', 'N/A')}%
- K%: {batting_ev.get('k_percent', 'N/A')}%
- BB%: {batting_ev.get('bb_percent', 'N/A')}%
- xBA: {batting_x.get('est_ba', 'N/A')}
- xSLG: {batting_x.get('est_slg', 'N/A')}
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
# SAMPLE GENERATION
# =========================
async def get_sample_threads(forum_channel, limit: int = SAMPLE_SIZE):
    """Get a sample of threads from the forum."""
    threads = []
    
    async for thread in forum_channel.archived_threads(limit=100):
        threads.append(thread)
        if len(threads) >= limit:
            break
    
    # If we don't have enough archived threads, get active ones
    if len(threads) < limit:
        for thread in forum_channel.threads:
            if len(threads) >= limit:
                break
            threads.append(thread)
    
    return threads[:limit]


async def extract_current_outlook(thread: discord.Thread) -> str | None:
    """Extract the current outlook from a thread."""
    async for message in thread.history(limit=50):
        if message.author.bot and message.embeds:
            for embed in message.embeds:
                # Look for "2026 Outlook" field
                for field in embed.fields:
                    if "outlook" in field.name.lower():
                        return field.value
                
                # Sometimes it's in the description
                if embed.description and "outlook" in embed.description.lower():
                    # Try to extract just the outlook portion
                    lines = embed.description.split('\n')
                    outlook_started = False
                    outlook_lines = []
                    
                    for line in lines:
                        if "2026 outlook" in line.lower():
                            outlook_started = True
                            continue
                        if outlook_started:
                            if line.startswith("MLB_ID:"):
                                break
                            outlook_lines.append(line)
                    
                    if outlook_lines:
                        return '\n'.join(outlook_lines).strip()
    
    return None


async def generate_samples():
    """Generate sample outlooks for review."""
    log("Starting sample generation...")
    
    forum_channel = bot.get_channel(FORUM_CHANNEL_ID)
    if not forum_channel:
        log("ERROR: Could not find forum channel")
        return
    
    log(f"Getting {SAMPLE_SIZE} sample threads...")
    sample_threads = await get_sample_threads(forum_channel, SAMPLE_SIZE)
    
    if not sample_threads:
        log("ERROR: No threads found")
        return
    
    log(f"Found {len(sample_threads)} threads")
    
    results = []
    
    for i, thread in enumerate(sample_threads, 1):
        try:
            log(f"Processing {i}/{len(sample_threads)}: {thread.name}")
            
            # Extract current outlook
            current_outlook = await extract_current_outlook(thread)
            
            if not current_outlook:
                log(f"  ⚠️  No outlook found, skipping")
                continue
            
            log(f"  📊 Resolving player bundle...")
            bundle = await resolve_bundle_from_thread(thread)
            
            if not bundle:
                log(f"  ⚠️  Could not resolve bundle, skipping")
                continue
            
            log(f"  🤖 Generating new outlook with Claude...")
            new_outlook = await generate_outlook_with_claude(bundle)
            
            results.append({
                "thread_name": thread.name,
                "thread_id": thread.id,
                "player_id": bundle["profile"]["id"],
                "current_outlook": current_outlook,
                "new_outlook": new_outlook
            })
            
            log(f"  ✅ Generated outlook ({len(new_outlook)} chars)")
            
        except Exception as e:
            log(f"  ❌ Error processing {thread.name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Save results
    if results:
        output_file = OUTPUT_DIR / f"samples_{len(results)}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        log(f"\n✅ Generated {len(results)} samples")
        log(f"📄 Saved to: {output_file}")
        
        # Print preview
        log("\n" + "="*60)
        log("SAMPLE PREVIEW")
        log("="*60)
        
        for result in results[:3]:  # Show first 3
            log(f"\n🔵 {result['thread_name']}")
            log(f"\n📝 CURRENT OUTLOOK:")
            log(result['current_outlook'])
            log(f"\n✨ NEW OUTLOOK:")
            log(result['new_outlook'])
            log("\n" + "-"*60)
        
        log(f"\n💡 Review the samples in: {output_file}")
        log("If you like them, I'll create the full batch script.")
    else:
        log("❌ No results generated")


@bot.event
async def on_ready():
    log(f"Logged in as {bot.user}")
    
    try:
        await generate_samples()
    except Exception as e:
        log(f"Error during sample generation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        log("Shutting down...")
        await bot.close()


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("PLAYER_PROFILES_TOKEN environment variable is not set")
    
    bot.run(TOKEN)

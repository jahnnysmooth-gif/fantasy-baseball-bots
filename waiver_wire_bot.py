import os
import discord
import asyncio
import aiohttp
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import json
import re
from anthropic import Anthropic

# Environment variables
DISCORD_TOKEN = os.getenv('ANALYTIC_BOT_TOKEN')
CHANNEL_ID = int(os.getenv('WAIVER_WIRE_CHANNEL_ID'))
CLAUDE_API_KEY = os.getenv('WAIVER_WIRE_KEY')

# Configuration
STATE_FILE = 'state/waiver_wire_state.json'
OWNERSHIP_THRESHOLD = 5.0  # ±5% to qualify as trending
TOP_N = 5  # Top 5 adds/drops (keeps embed within Discord 1024-char field limit)

# Discord client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Claude client
anthropic_client = Anthropic(api_key=CLAUDE_API_KEY)

# Scheduler
scheduler = AsyncIOScheduler(timezone='America/New_York')


def load_state():
    """Load previous state from file"""
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {'last_post_time': None, 'last_ownership_data': {}}


def save_state(state):
    """Save state to file"""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


async def fetch_espn_ownership():
    """
    Fetch player ownership data from ESPN's public kona_player_info API.
    No auth required. Returns dict:
    {player_name: {'ownership': float, 'change': float, 'position': str, 'espn_id': int}}
    """
    print("[ESPN API] Fetching player ownership data...")
    ownership_data = {}

    # ESPN position slot ID -> readable position string
    POSITION_MAP = {
        1: 'C', 2: '1B', 3: '2B', 4: '3B', 5: 'SS',
        6: '2B/SS', 7: '1B/3B', 8: 'OF', 9: 'DH',
        10: 'SP', 11: 'RP', 12: 'P',
        13: 'UTIL', 14: 'SP', 15: 'RP', 16: 'BN', 17: 'IL'
    }

    url = (
        'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb'
        '/seasons/2026/segments/0/leaguedefaults/3?view=kona_player_info'
    )
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    print(f"[ESPN API] Bad status: {response.status}")
                    return ownership_data

                data = await response.json(content_type=None)
                players = data.get('players', [])
                print(f"[ESPN API] Retrieved {len(players)} players from ESPN")

                for entry in players:
                    try:
                        player = entry.get('player', {})
                        ownership = player.get('ownership', {})

                        full_name = player.get('fullName', '').strip()
                        if not full_name:
                            continue

                        pct_owned = ownership.get('percentOwned', 0.0)
                        pct_change = ownership.get('percentChange', 0.0)

                        # Skip completely unowned and zero-change players
                        if pct_owned == 0.0 and pct_change == 0.0:
                            continue

                        # Determine position from eligibleSlots
                        eligible_slots = player.get('eligibleSlots', [])
                        position = 'NA'
                        for slot_id in eligible_slots:
                            pos = POSITION_MAP.get(slot_id)
                            if pos and pos not in ('UTIL', 'BN', 'IL'):
                                position = pos
                                break

                        ownership_data[full_name] = {
                            'ownership': round(pct_owned, 2),
                            'change': round(pct_change, 2),
                            'position': position,
                            'espn_id': player.get('id'),
                            'injury_status': player.get('injuryStatus', 'ACTIVE'),
                        }

                    except Exception as e:
                        print(f"[ESPN API] Error parsing player entry: {e}")
                        continue

        except Exception as e:
            print(f"[ESPN API] Request failed: {e}")
            import traceback
            traceback.print_exc()

    print(f"[ESPN API] Parsed {len(ownership_data)} owned/trending players")
    return ownership_data


def normalize_player_name(name):
    """
    Normalize player names for comparison across platforms
    Handles formats like:
    - "Shohei Ohtani"
    - "Ohtani, Shohei"
    - "S. Ohtani"
    - "Ohtani, S."
    """
    # Remove suffixes
    name = re.sub(r'\s+(Jr\.|Sr\.|III|IV|II)$', '', name, flags=re.IGNORECASE)
    
    # Handle "Last, First" format
    if ',' in name:
        parts = name.split(',')
        if len(parts) == 2:
            last, first = parts[0].strip(), parts[1].strip()
            name = f"{first} {last}"
    
    # Remove middle initials
    name = re.sub(r'\s+[A-Z]\.\s+', ' ', name)
    
    # Clean up whitespace
    name = ' '.join(name.split())
    
    return name.strip()


def merge_ownership_data(espn_data, previous_data):
    """
    Build merged data dict from ESPN API data.
    Falls back to diff against previous snapshot if percentChange is missing.
    """
    merged = {}

    for player, info in espn_data.items():
        prev_own = previous_data.get(player, {}).get('espn_ownership', 0)
        espn_own = info.get('ownership', 0)
        espn_change = info.get('change', espn_own - prev_own)

        merged[player] = {
            'espn_ownership': espn_own,
            'espn_change': espn_change,
            'avg_change': espn_change,
            'position': info.get('position', 'NA'),
            'injury_status': info.get('injury_status', 'ACTIVE'),
            'espn_id': info.get('espn_id'),
        }

    return merged


def filter_trending_players(merged_data, threshold=OWNERSHIP_THRESHOLD):
    """
    Filter players with significant ownership changes.
    Falls back to top/bottom owned if change data is flat (early season).
    """
    adds = []
    drops = []

    for player, data in merged_data.items():
        avg_change = data['avg_change']
        player_info = {'name': player, **data}
        if avg_change >= threshold:
            adds.append(player_info)
        elif avg_change <= -threshold:
            drops.append(player_info)

    adds.sort(key=lambda x: x['avg_change'], reverse=True)
    drops.sort(key=lambda x: x['avg_change'])

    # Fallback: if change data is flat, show top/bottom by ownership %
    if not adds:
        print("[Filter] No adds above threshold — falling back to top owned players")
        all_players = [{'name': p, **d} for p, d in merged_data.items()]
        all_players.sort(key=lambda x: x['espn_ownership'], reverse=True)
        adds = all_players[:TOP_N]

    if not drops:
        print("[Filter] No drops below threshold — falling back to lowest owned players")
        all_players = [{'name': p, **d} for p, d in merged_data.items()]
        all_players.sort(key=lambda x: x['espn_ownership'])
        drops = all_players[:TOP_N]

    return adds[:TOP_N], drops[:TOP_N]


async def fetch_recent_stats(player_names):
    """
    Fetch recent stats (last 7 days) for players from MLB Stats API
    Returns dict: {player_name: {stats}}
    """
    print(f"[MLB Stats] Fetching stats for {len(player_names)} players...")
    stats = {}
    
    # MLB Stats API endpoint
    base_url = "https://statsapi.mlb.com/api/v1"
    
    async with aiohttp.ClientSession() as session:
        for player in player_names:
            try:
                # Search for player ID
                search_url = f"{base_url}/people/search?names={player}"
                async with session.get(search_url, timeout=10) as response:
                    if response.status != 200:
                        continue
                    data = await response.json()
                    
                    if not data.get('people'):
                        continue
                    
                    player_id = data['people'][0]['id']
                    
                    # Get recent stats (last 7 days)
                    end_date = datetime.now().strftime('%Y-%m-%d')
                    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                    
                    stats_url = f"{base_url}/people/{player_id}/stats?stats=statsSingleSeason&season=2026&startDate={start_date}&endDate={end_date}"
                    async with session.get(stats_url, timeout=10) as stats_response:
                        if stats_response.status != 200:
                            continue
                        stats_data = await stats_response.json()
                        
                        # Extract relevant stats (batting or pitching)
                        if stats_data.get('stats'):
                            stats[player] = stats_data['stats'][0].get('splits', [{}])[0].get('stat', {})
                
            except Exception as e:
                print(f"[MLB Stats] Error fetching stats for {player}: {e}")
                continue
    
    print(f"[MLB Stats] Retrieved stats for {len(stats)} players")
    return stats


async def fetch_recent_news(player_names):
    """
    Fetch recent news for players (call-ups, injuries, etc.)
    Returns dict: {player_name: 'news headline'}
    """
    print(f"[News Fetcher] Fetching news for {len(player_names)} players...")
    news = {}
    
    # ESPN News API or similar
    # This is a simplified template - actual implementation depends on available news APIs
    async with aiohttp.ClientSession() as session:
        for player in player_names:
            try:
                # Example: ESPN news search
                # Actual endpoint may vary
                search_query = player.replace(' ', '+')
                url = f"https://www.espn.com/apis/fantasy/v2/news?player={search_query}"
                
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('headlines'):
                            news[player] = data['headlines'][0]['headline']
            except:
                # If ESPN doesn't work, try a simple search or skip
                continue
    
    print(f"[News Fetcher] Retrieved news for {len(news)} players")
    return news


async def generate_claude_analysis(adds, drops, stats, news):
    """
    Send data to Claude Sonnet for spicy analysis
    Returns dict with intro, commentary, buy_low, dont_chase
    """
    print("[Claude] Generating analysis...")
    
    # Build context for Claude
    adds_context = []
    for player in adds[:5]:  # Top 5 for detailed analysis
        player_stats = stats.get(player['name'], {})
        player_news = news.get(player['name'], 'No recent news')
        
        adds_context.append({
            'name': player['name'],
            'position': player['position'],
            'espn_owned': player['espn_ownership'],
            'espn_change': player['espn_change'],
            'stats': player_stats,
            'news': player_news
        })
    
    drops_context = []
    for player in drops[:5]:  # Top 5 for detailed analysis
        player_stats = stats.get(player['name'], {})
        player_news = news.get(player['name'], 'No recent news')
        
        drops_context.append({
            'name': player['name'],
            'position': player['position'],
            'espn_owned': player['espn_ownership'],
            'espn_change': player['espn_change'],
            'stats': player_stats,
            'news': player_news
        })
    
    # Build prompt for Claude
    prompt = f"""You're analyzing today's fantasy baseball waiver wire trends. Write SPICY, opinionated analysis.

TODAY'S HOTTEST ADDS:
{json.dumps(adds_context, indent=2)}

TODAY'S BIGGEST DROPS:
{json.dumps(drops_context, indent=2)}

Write a JSON response with:
{{
  "intro": "1-2 sentence market overview with attitude (e.g., 'The waiver wire is drunk on rookie hype...')",
  "add_comments": {{"player_name": "brief spicy reason (10-15 words)", ...}},
  "drop_comments": {{"player_name": "brief spicy reason (10-15 words)", ...}},
  "spicy_take": "2-3 sentence hot take about the overall market",
  "buy_low_player": "Player name from drops list",
  "buy_low_reason": "Why this is a buy-low (2 sentences, include stats if relevant)",
  "dont_chase_player": "Player name from adds list",
  "dont_chase_reason": "Why not to chase this player (2 sentences)"
}}

Be controversial. Be confident. Use stats to back up hot takes. Don't be generic."""

    try:
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        response_text = message.content[0].text
        
        # Parse JSON response
        # Claude might wrap in ```json, so clean it
        response_text = re.sub(r'```json\s*|\s*```', '', response_text)
        analysis = json.loads(response_text)
        
        print("[Claude] Analysis generated successfully")
        return analysis
        
    except Exception as e:
        print(f"[Claude] Error generating analysis: {e}")
        # Return fallback structure
        return {
            "intro": "Today's waiver wire is seeing some interesting movement.",
            "add_comments": {},
            "drop_comments": {},
            "spicy_take": "Fantasy managers are making moves - some smart, some not.",
            "buy_low_player": drops[0]['name'] if drops else "N/A",
            "buy_low_reason": "Consider picking up if available.",
            "dont_chase_player": adds[0]['name'] if adds else "N/A",
            "dont_chase_reason": "Wait and see before committing."
        }


def build_discord_embed(adds, drops, analysis, stats, news):
    """
    Build the Discord embed for posting
    """
    embed = discord.Embed(
        title="📊 FANTASY BASEBALL WAIVER WIRE",
        description=f"{datetime.now(ZoneInfo('America/New_York')).strftime('%A, %B %d, %Y')}\n\n{analysis['intro']}",
        color=0x1DB954,  # Green
        timestamp=datetime.now(ZoneInfo('UTC'))
    )
    
    # HOTTEST ADDS section
    adds_text = ""
    for i, player in enumerate(adds, 1):
        emoji = "🔥" if i <= 3 else "📈"
        name = player['name']
        pos = player['position']
        
        espn_own = f"{player['espn_ownership']:.1f}%"
        espn_change = f"⬆️ +{player['espn_change']:.1f}%" if player['espn_change'] > 0 else f"⬇️ {player['espn_change']:.1f}%"

        adds_text += f"{emoji} **{name}** - {pos}\n"
        adds_text += f"   ESPN: {espn_own} owned ({espn_change})\n"
        
        # Add stats if available
        player_stats = stats.get(name, {})
        if player_stats:
            stats_line = format_stats_line(player_stats, pos)
            if stats_line:
                adds_text += f"   {stats_line}\n"
        
        # Add news/comment
        comment = analysis['add_comments'].get(name, news.get(name, ''))
        if comment:
            adds_text += f"   📰 {comment}\n"
        
        adds_text += "\n"
    
    embed.add_field(
        name="🔥 HOTTEST ADDS (Last 24 Hours)",
        value=(adds_text[:1020] + "...") if len(adds_text) > 1024 else (adds_text or "No significant adds"),
        inline=False
    )
    
    # Add separator
    embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━━━━", inline=False)
    
    # BIGGEST DROPS section
    drops_text = ""
    for i, player in enumerate(drops, 1):
        emoji = "❄️" if i <= 3 else "📉"
        name = player['name']
        pos = player['position']
        
        espn_own = f"{player['espn_ownership']:.1f}%"
        espn_change = f"⬇️ {player['espn_change']:.1f}%"

        drops_text += f"{emoji} **{name}** - {pos}\n"
        drops_text += f"   ESPN: {espn_own} owned ({espn_change})\n"
        
        # Add stats if available
        player_stats = stats.get(name, {})
        if player_stats:
            stats_line = format_stats_line(player_stats, pos)
            if stats_line:
                drops_text += f"   {stats_line}\n"
        
        # Add news/comment
        comment = analysis['drop_comments'].get(name, news.get(name, ''))
        if comment:
            drops_text += f"   📰 {comment}\n"
        
        drops_text += "\n"
    
    embed.add_field(
        name="❄️ BIGGEST DROPS (Last 24 Hours)",
        value=(drops_text[:1020] + "...") if len(drops_text) > 1024 else (drops_text or "No significant drops"),
        inline=False
    )
    
    # Add separator
    embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━━━━", inline=False)
    
    # CLAUDE'S SPICY TAKE
    take_text = f"{analysis['spicy_take']}\n\n"
    take_text += f"💎 **BUY-LOW ALERT: {analysis['buy_low_player']}**\n"
    take_text += f"{analysis['buy_low_reason']}\n\n"
    take_text += f"🗑️ **DON'T CHASE: {analysis['dont_chase_player']}**\n"
    take_text += f"{analysis['dont_chase_reason']}"
    
    embed.add_field(
        name="🎯 CLAUDE'S SPICY TAKE",
        value=(take_text[:1020] + "...") if len(take_text) > 1024 else take_text,
        inline=False
    )
    
    # Footer
    embed.set_footer(
        text="Data: ESPN Fantasy • Updated daily at 7:00 AM ET"
    )
    
    return embed


def format_stats_line(stats, position):
    """Format stats based on position (hitter vs pitcher)"""
    if not stats:
        return ""
    
    if position in ['SP', 'RP', 'P']:
        # Pitcher stats - cast to float/int since MLB API returns strings
        era = float(stats.get('era', 0) or 0)
        ip = stats.get('inningsPitched', '0')
        k = int(stats.get('strikeOuts', 0) or 0)
        whip = float(stats.get('whip', 0) or 0)
        return f"Last 7: {ip} IP, {k} K, {era:.2f} ERA, {whip:.2f} WHIP"
    else:
        # Hitter stats - cast to float/int since MLB API returns strings
        avg = stats.get('avg', '.000') or '.000'
        hr = int(stats.get('homeRuns', 0) or 0)
        rbi = int(stats.get('rbi', 0) or 0)
        sb = int(stats.get('stolenBases', 0) or 0)
        return f"Last 7: {avg} AVG, {hr} HR, {rbi} RBI, {sb} SB"


async def post_daily_report():
    """Main function to generate and post daily waiver wire report"""
    print(f"\n[Waiver Wire Bot] Starting daily report generation at {datetime.now(ZoneInfo('America/New_York'))}")
    
    try:
        # Load previous state
        state = load_state()
        previous_ownership = state.get('last_ownership_data', {})
        
        # Step 1: Fetch ownership data from ESPN API
        espn_data = await fetch_espn_ownership()

        if not espn_data:
            print("[Waiver Wire Bot] ESPN fetch returned no data - skipping post")
            return

        # Step 2: Merge and calculate changes
        merged_data = merge_ownership_data(espn_data, previous_ownership)
        
        # Step 3: Filter trending players
        adds, drops = filter_trending_players(merged_data)
        
        # Step 4: Fetch recent stats and news
        all_player_names = [p['name'] for p in adds + drops]
        stats = await fetch_recent_stats(all_player_names)
        news = await fetch_recent_news(all_player_names)
        
        # Step 5: Get Claude's analysis
        analysis = await generate_claude_analysis(adds, drops, stats, news)
        
        # Step 6: Build Discord embed
        embed = build_discord_embed(adds, drops, analysis, stats, news)
        
        # Step 7: Post to Discord
        channel = client.get_channel(CHANNEL_ID)
        if not channel:
            print(f"[Waiver Wire Bot] Channel {CHANNEL_ID} not found")
            return
        
        message = await channel.send(embed=embed)
        
        # Step 8: Add reaction emojis
        reactions = ['🔥', '🧊', '💎', '🗑️']
        for emoji in reactions:
            try:
                await message.add_reaction(emoji)
            except:
                pass
        
        # Step 9: Save state
        state['last_post_time'] = datetime.now(ZoneInfo('UTC')).isoformat()
        state['last_ownership_data'] = merged_data
        save_state(state)
        
        print(f"[Waiver Wire Bot] Daily report posted successfully at {datetime.now(ZoneInfo('America/New_York'))}")
        
    except Exception as e:
        print(f"[Waiver Wire Bot] Error in daily report: {e}")
        import traceback
        traceback.print_exc()


@client.event
async def on_ready():
    print(f'[Waiver Wire Bot] Logged in as {client.user}')
    
    # TEST MODE: Run immediately on startup (comment out after testing)
    TEST_MODE = os.getenv('WAIVER_WIRE_TEST_MODE', 'false').lower() == 'true'
    if TEST_MODE:
        print("[Waiver Wire Bot] TEST MODE: Running report immediately")
        await post_daily_report()
    
    # Schedule daily post at 7:00 AM ET
    scheduler.add_job(
        post_daily_report,
        'cron',
        hour=7,
        minute=0,
        timezone='America/New_York',
        id='daily_waiver_report'
    )
    
    print("[Waiver Wire Bot] Scheduled daily post for 7:00 AM ET")
    scheduler.start()


async def start_waiver_wire_bot():
    """Entry point for main.py to call - matches the pattern of other bots"""
    await client.start(DISCORD_TOKEN)


if __name__ == '__main__':
    import asyncio
    asyncio.run(start_waiver_wire_bot())

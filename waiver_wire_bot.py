import os
import discord
import asyncio
import aiohttp
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
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
TOP_N = 10  # Top 10 adds/drops

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


async def scrape_yahoo_ownership():
    """
    Scrape Yahoo's public player pages for ownership percentages
    Returns dict: {player_name: {'ownership': float, 'change': float, 'position': str}}
    """
    print("[Yahoo Scraper] Starting Yahoo ownership scrape...")
    ownership_data = {}
    
    # Yahoo's player list URLs (batters and pitchers)
    urls = [
        'https://baseball.fantasysports.yahoo.com/b1/playersearch?status=ALL&pos=B&sort=OR&sdir=1',  # Batters
        'https://baseball.fantasysports.yahoo.com/b1/playersearch?status=ALL&pos=P&sort=OR&sdir=1'   # Pitchers
    ]
    
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url, timeout=30) as response:
                    if response.status != 200:
                        print(f"[Yahoo Scraper] Failed to fetch {url}: {response.status}")
                        continue
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Parse player rows (structure may vary - this is a template)
                    # Yahoo typically shows ownership % in player tables
                    # This is a simplified version - may need adjustment based on actual HTML
                    player_rows = soup.find_all('tr', class_=re.compile('player'))
                    
                    for row in player_rows:
                        try:
                            # Extract player name
                            name_elem = row.find('a', class_=re.compile('name|player'))
                            if not name_elem:
                                continue
                            player_name = normalize_player_name(name_elem.get_text(strip=True))
                            
                            # Extract position
                            pos_elem = row.find('span', class_=re.compile('pos'))
                            position = pos_elem.get_text(strip=True) if pos_elem else 'NA'
                            
                            # Extract ownership %
                            own_elem = row.find('td', class_=re.compile('own|percent'))
                            if not own_elem:
                                continue
                            own_text = own_elem.get_text(strip=True)
                            ownership_match = re.search(r'(\d+(?:\.\d+)?)%', own_text)
                            if not ownership_match:
                                continue
                            ownership = float(ownership_match.group(1))
                            
                            # Extract change (if available - usually shown as +5% or -3%)
                            change = 0.0
                            change_match = re.search(r'([+-]?\d+(?:\.\d+)?)%', own_text)
                            if change_match and change_match.group(0) != ownership_match.group(0):
                                change = float(change_match.group(1))
                            
                            ownership_data[player_name] = {
                                'ownership': ownership,
                                'change': change,
                                'position': position,
                                'source': 'yahoo'
                            }
                        except Exception as e:
                            print(f"[Yahoo Scraper] Error parsing player row: {e}")
                            continue
                            
            except Exception as e:
                print(f"[Yahoo Scraper] Error scraping {url}: {e}")
                continue
    
    print(f"[Yahoo Scraper] Scraped {len(ownership_data)} players")
    return ownership_data


async def scrape_espn_ownership():
    """
    Scrape ESPN's public ownership data
    Returns dict: {player_name: {'ownership': float, 'change': float, 'position': str}}
    """
    print("[ESPN Scraper] Starting ESPN ownership scrape...")
    ownership_data = {}
    
    # ESPN's player rater or availability pages
    # Note: ESPN's structure varies - this is a template
    url = 'https://www.espn.com/fantasy/baseball/players/projections'
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    print(f"[ESPN Scraper] Failed to fetch ESPN data: {response.status}")
                    return ownership_data
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Parse player data (structure depends on ESPN's HTML)
                # This is a simplified template
                player_rows = soup.find_all('tr', class_=re.compile('player'))
                
                for row in player_rows:
                    try:
                        # Extract player name
                        name_elem = row.find('a', class_=re.compile('player|name'))
                        if not name_elem:
                            continue
                        player_name = normalize_player_name(name_elem.get_text(strip=True))
                        
                        # Extract position
                        pos_elem = row.find('span', class_=re.compile('position'))
                        position = pos_elem.get_text(strip=True) if pos_elem else 'NA'
                        
                        # Extract ownership %
                        own_elem = row.find('td', class_=re.compile('own|roster'))
                        if not own_elem:
                            continue
                        own_text = own_elem.get_text(strip=True)
                        ownership_match = re.search(r'(\d+(?:\.\d+)?)%', own_text)
                        if not ownership_match:
                            continue
                        ownership = float(ownership_match.group(1))
                        
                        # ESPN may not show weekly change directly
                        # We'll calculate it by comparing to previous scrape
                        
                        ownership_data[player_name] = {
                            'ownership': ownership,
                            'change': 0.0,  # Will calculate from historical data
                            'position': position,
                            'source': 'espn'
                        }
                    except Exception as e:
                        print(f"[ESPN Scraper] Error parsing player row: {e}")
                        continue
                        
        except Exception as e:
            print(f"[ESPN Scraper] Error scraping ESPN: {e}")
    
    print(f"[ESPN Scraper] Scraped {len(ownership_data)} players")
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


def merge_ownership_data(yahoo_data, espn_data, previous_data):
    """
    Merge Yahoo and ESPN data, calculate changes from previous scrape
    Returns dict with combined data showing both platform ownership
    """
    merged = {}
    all_players = set(yahoo_data.keys()) | set(espn_data.keys())
    
    for player in all_players:
        yahoo_info = yahoo_data.get(player, {})
        espn_info = espn_data.get(player, {})
        
        # Calculate change from previous data if we don't have it from scraping
        prev_yahoo = previous_data.get(player, {}).get('yahoo_ownership', 0)
        prev_espn = previous_data.get(player, {}).get('espn_ownership', 0)
        
        yahoo_own = yahoo_info.get('ownership', 0)
        espn_own = espn_info.get('ownership', 0)
        
        yahoo_change = yahoo_info.get('change', yahoo_own - prev_yahoo)
        espn_change = espn_info.get('change', espn_own - prev_espn)
        
        # Average change across platforms (for ranking)
        avg_change = (yahoo_change + espn_change) / 2 if yahoo_change and espn_change else (yahoo_change or espn_change)
        
        merged[player] = {
            'yahoo_ownership': yahoo_own,
            'espn_ownership': espn_own,
            'yahoo_change': yahoo_change,
            'espn_change': espn_change,
            'avg_change': avg_change,
            'position': yahoo_info.get('position') or espn_info.get('position', 'NA')
        }
    
    return merged


def filter_trending_players(merged_data, threshold=OWNERSHIP_THRESHOLD):
    """
    Filter players with significant ownership changes
    Returns separate lists for adds (positive) and drops (negative)
    """
    adds = []
    drops = []
    
    for player, data in merged_data.items():
        avg_change = data['avg_change']
        
        if abs(avg_change) >= threshold:
            player_info = {
                'name': player,
                **data
            }
            
            if avg_change > 0:
                adds.append(player_info)
            else:
                drops.append(player_info)
    
    # Sort by absolute change
    adds.sort(key=lambda x: x['avg_change'], reverse=True)
    drops.sort(key=lambda x: x['avg_change'])  # Most negative first
    
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
            'yahoo_own': player['yahoo_ownership'],
            'yahoo_change': player['yahoo_change'],
            'espn_own': player['espn_ownership'],
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
            'yahoo_own': player['yahoo_ownership'],
            'yahoo_change': player['yahoo_change'],
            'espn_own': player['espn_ownership'],
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
        
        yahoo_own = f"{player['yahoo_ownership']:.0f}%"
        yahoo_change = f"⬆️ +{player['yahoo_change']:.0f}%" if player['yahoo_change'] > 0 else f"{player['yahoo_change']:.0f}%"
        espn_own = f"{player['espn_ownership']:.0f}%"
        espn_change = f"⬆️ +{player['espn_change']:.0f}%" if player['espn_change'] > 0 else f"{player['espn_change']:.0f}%"
        
        adds_text += f"{emoji} **{name}** - {pos}\n"
        adds_text += f"   Yahoo: {yahoo_own} ({yahoo_change}) • ESPN: {espn_own} ({espn_change})\n"
        
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
        value=adds_text or "No significant adds",
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
        
        yahoo_own = f"{player['yahoo_ownership']:.0f}%"
        yahoo_change = f"⬇️ {player['yahoo_change']:.0f}%"
        espn_own = f"{player['espn_ownership']:.0f}%"
        espn_change = f"⬇️ {player['espn_change']:.0f}%"
        
        drops_text += f"{emoji} **{name}** - {pos}\n"
        drops_text += f"   Yahoo: {yahoo_own} ({yahoo_change}) • ESPN: {espn_own} ({espn_change})\n"
        
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
        value=drops_text or "No significant drops",
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
        value=take_text,
        inline=False
    )
    
    # Footer
    embed.set_footer(
        text="Data: Yahoo Sports • ESPN Fantasy\nUpdated daily at 7:00 AM ET"
    )
    
    return embed


def format_stats_line(stats, position):
    """Format stats based on position (hitter vs pitcher)"""
    if not stats:
        return ""
    
    if position in ['SP', 'RP', 'P']:
        # Pitcher stats
        era = stats.get('era', 0)
        ip = stats.get('inningsPitched', '0')
        k = stats.get('strikeOuts', 0)
        whip = stats.get('whip', 0)
        return f"Last 7: {ip} IP, {k} K, {era:.2f} ERA, {whip:.2f} WHIP"
    else:
        # Hitter stats
        avg = stats.get('avg', '.000')
        hr = stats.get('homeRuns', 0)
        rbi = stats.get('rbi', 0)
        sb = stats.get('stolenBases', 0)
        return f"Last 7: {avg} AVG, {hr} HR, {rbi} RBI, {sb} SB"


async def post_daily_report():
    """Main function to generate and post daily waiver wire report"""
    print(f"\n[Waiver Wire Bot] Starting daily report generation at {datetime.now(ZoneInfo('America/New_York'))}")
    
    try:
        # Load previous state
        state = load_state()
        previous_ownership = state.get('last_ownership_data', {})
        
        # Step 1: Scrape ownership data
        yahoo_data = await scrape_yahoo_ownership()
        espn_data = await scrape_espn_ownership()
        
        # If both scrapers fail, skip post
        if not yahoo_data and not espn_data:
            print("[Waiver Wire Bot] Both scrapers failed - skipping post")
            return
        
        # Step 2: Merge and calculate changes
        merged_data = merge_ownership_data(yahoo_data, espn_data, previous_ownership)
        
        # Step 3: Filter trending players
        adds, drops = filter_trending_players(merged_data)
        
        if not adds and not drops:
            print("[Waiver Wire Bot] No trending players found - skipping post")
            return
        
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

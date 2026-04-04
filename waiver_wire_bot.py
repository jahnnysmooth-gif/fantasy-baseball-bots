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
OWNERSHIP_THRESHOLD = 5.0   # min % change to qualify as trending
SPIKE_THRESHOLD = 10.0      # ownership spike that overrides the 50% ownership cap
MAX_OWNERSHIP = 50.0        # skip players owned in >50% of leagues (unless spiking)
TOP_N = 5                   # top 5 must-adds

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
        return {'last_post_time': None, 'last_ownership_data': {}, 'recommendation_history': []}


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
        'X-Fantasy-Filter': '{"players": {"limit": 300, "sortPercentChange": {"sortPriority": 1, "sortAsc": false}, "filterActive": {"value": true}}}',
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    print(f"[ESPN API] Bad status: {response.status} — retrying without filter header")
                    # Retry without X-Fantasy-Filter in case the endpoint rejects it
                    fallback_headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
                    async with session.get(url, headers=fallback_headers, timeout=30) as r2:
                        if r2.status != 200:
                            print(f"[ESPN API] Fallback also failed: {r2.status}")
                            return ownership_data
                        data = await r2.json(content_type=None)
                else:
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

                        # Skip players with absolutely zero ownership (not in any league)
                        if pct_owned == 0.0:
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
    Returns adds only — players worth picking up on the waiver wire.
    Rules:
    - Skip players owned in >50% of leagues UNLESS they have a big ownership spike
    - Sort by ownership change descending, fallback to top owned if change data is flat
    """
    adds = []

    for player, data in merged_data.items():
        own = data['espn_ownership']
        change = data['avg_change']

        # Must be actively being added (positive ownership change)
        if change <= 0:
            continue

        # Skip heavily owned players unless they are spiking hard
        if own > MAX_OWNERSHIP and change < SPIKE_THRESHOLD:
            continue

        adds.append({'name': player, **data})

    adds.sort(key=lambda x: x['avg_change'], reverse=True)
    print(f"[Filter] Found {len(adds)} players with positive ownership change")
    return adds[:TOP_N]


async def fetch_player_id(session, player_name):
    """Look up MLB player ID by name. Returns int or None."""
    base_url = "https://statsapi.mlb.com/api/v1"
    try:
        url = f"{base_url}/people/search?names={player_name}"
        async with session.get(url, timeout=10) as r:
            if r.status != 200:
                return None
            data = await r.json()
            people = data.get('people', [])
            return people[0]['id'] if people else None
    except Exception:
        return None


async def fetch_splits(session, player_id, days):
    """Fetch hitting or pitching stats for the last N days."""
    base_url = "https://statsapi.mlb.com/api/v1"
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    url = (f"{base_url}/people/{player_id}/stats"
           f"?stats=statsSingleSeason&season=2026"
           f"&startDate={start_date}&endDate={end_date}")
    try:
        async with session.get(url, timeout=10) as r:
            if r.status != 200:
                return {}
            data = await r.json()
            stats_list = data.get('stats', [])
            if stats_list:
                splits = stats_list[0].get('splits', [])
                return splits[0].get('stat', {}) if splits else {}
    except Exception:
        pass
    return {}


async def fetch_savant_metrics(session, player_id):
    """
    Fetch expected stats from Baseball Savant for a player.
    Returns dict with xba, xslg, xwoba, barrel_rate, hard_hit_pct, k_pct, bb_pct.
    """
    try:
        url = (
            "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
            f"?type=batter&year=2026&position=&team=&min=5&csv=true"
        )
        async with session.get(url, timeout=15) as r:
            if r.status != 200:
                return {}
            text = await r.text()
            lines = text.strip().split("\n")
            if len(lines) < 2:
                return {}
            headers = [h.strip().strip('"') for h in lines[0].split(",")]
            for line in lines[1:]:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) < len(headers):
                    continue
                row = dict(zip(headers, parts))
                if str(row.get('player_id', '')) == str(player_id):
                    return {
                        'xba':          row.get('est_ba', 'N/A'),
                        'xslg':         row.get('est_slg', 'N/A'),
                        'xwoba':        row.get('est_woba', 'N/A'),
                        'barrel_rate':  row.get('barrel_batted_rate', 'N/A'),
                        'hard_hit_pct': row.get('hard_hit_percent', 'N/A'),
                        'k_pct':        row.get('strikeout', 'N/A'),
                        'bb_pct':       row.get('walk', 'N/A'),
                    }
    except Exception as e:
        print(f"[Savant] Error fetching metrics: {e}")
    return {}


async def fetch_recent_stats(player_names):
    """
    Fetch last-7, last-14 splits + Savant underlying metrics for each player.
    Returns dict: {player_name: {'last7': {}, 'last14': {}, 'savant': {}}}
    """
    print(f"[MLB Stats] Fetching stats for {len(player_names)} players...")
    stats = {}

    # Pre-fetch Savant leaderboard once (covers all players)
    savant_cache = {}
    async with aiohttp.ClientSession() as session:
        try:
            url = (
                "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
                "?type=batter&year=2026&position=&team=&min=5&csv=true"
            )
            async with session.get(url, timeout=15) as r:
                if r.status == 200:
                    text = await r.text()
                    lines = text.strip().split("\n")
                    if len(lines) >= 2:
                        headers = [h.strip().strip('"') for h in lines[0].split(",")]
                        for line in lines[1:]:
                            parts = [p.strip().strip('"') for p in line.split(",")]
                            if len(parts) < len(headers):
                                continue
                            row = dict(zip(headers, parts))
                            pid = row.get('player_id', '')
                            if pid:
                                savant_cache[pid] = {
                                    'xba':          row.get('est_ba', 'N/A'),
                                    'xslg':         row.get('est_slg', 'N/A'),
                                    'xwoba':        row.get('est_woba', 'N/A'),
                                    'barrel_rate':  row.get('barrel_batted_rate', 'N/A'),
                                    'hard_hit_pct': row.get('hard_hit_percent', 'N/A'),
                                    'k_pct':        row.get('strikeout', 'N/A'),
                                    'bb_pct':       row.get('walk', 'N/A'),
                                }
            print(f"[Savant] Loaded {len(savant_cache)} players from leaderboard")
        except Exception as e:
            print(f"[Savant] Leaderboard fetch failed: {e}")

        for player in player_names:
            try:
                player_id = await fetch_player_id(session, player)
                if not player_id:
                    continue

                last7  = await fetch_splits(session, player_id, 7)
                last14 = await fetch_splits(session, player_id, 14)
                savant = savant_cache.get(str(player_id), {})

                stats[player] = {
                    'last7':   last7,
                    'last14':  last14,
                    'savant':  savant,
                    'mlb_id':  player_id,
                }

            except Exception as e:
                print(f"[MLB Stats] Error for {player}: {e}")
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


async def find_breakout_candidates(merged_data, stats, recent_recommendations):
    """
    From the pool of <50% owned players, find 3 with the best underlying metrics
    who have NOT been recommended in the last 10 days.
    Returns list of up to 3 dicts with player info + stats + savant.
    """
    print("[Breakout] Finding breakout candidates...")

    cutoff = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    recently_recommended = {
        r['name'] for r in recent_recommendations
        if r.get('date', '0000-00-00') >= cutoff
    }

    candidates = []
    for name, data in merged_data.items():
        own = data['espn_ownership']

        # Must be owned somewhere meaningful but not a mainstream pickup
        if own < 1.0 or own >= MAX_OWNERSHIP:
            continue
        if name in recently_recommended:
            continue
        if name not in stats:
            continue

        player_stats = stats[name]
        savant = player_stats.get('savant', {})
        last7  = player_stats.get('last7', {})
        last14 = player_stats.get('last14', {})

        # Must have actually played — require at least 1 PA or 1 IP in last 14 days
        pa14 = int(last14.get('plateAppearances', 0) or last14.get('atBats', 0) or 0)
        ip14 = float(last14.get('inningsPitched', 0) or 0)
        if pa14 < 5 and ip14 < 1.0:
            continue

        # Score the player — higher = better breakout candidate
        score = 0.0

        # xwOBA above .320 is solid
        try:
            xwoba = float(savant.get('xwoba', 0) or 0)
            if xwoba >= 0.370:
                score += 3
            elif xwoba >= 0.340:
                score += 2
            elif xwoba >= 0.310:
                score += 1
        except (ValueError, TypeError):
            pass

        # Barrel rate above 8% is meaningful
        try:
            barrel = float(savant.get('barrel_rate', 0) or 0)
            if barrel >= 12:
                score += 3
            elif barrel >= 8:
                score += 2
            elif barrel >= 5:
                score += 1
        except (ValueError, TypeError):
            pass

        # Hard hit % above 40% is good
        try:
            hard_hit = float(savant.get('hard_hit_pct', 0) or 0)
            if hard_hit >= 48:
                score += 2
            elif hard_hit >= 40:
                score += 1
        except (ValueError, TypeError):
            pass

        # Recent trend: improving from last14 to last7 avg
        try:
            avg7  = float(last7.get('avg', 0) or 0)
            avg14 = float(last14.get('avg', 0) or 0)
            if avg7 > avg14 + 0.030:
                score += 2  # heating up
            elif avg7 > avg14:
                score += 1
        except (ValueError, TypeError):
            pass

        # Pitcher: K rate improving
        try:
            k7  = int(last7.get('strikeOuts', 0) or 0)
            ip7 = float(last7.get('inningsPitched', 0) or 0)
            if ip7 > 0 and (k7 / ip7) >= 1.0:
                score += 2
        except (ValueError, TypeError):
            pass

        if score >= 3:  # require meaningful evidence, not just one weak signal
            candidates.append({
                'name': name,
                'score': score,
                'espn_ownership': data['espn_ownership'],
                'espn_change': data['espn_change'],
                'position': data['position'],
                'last7': last7,
                'last14': last14,
                'savant': savant,
            })

    candidates.sort(key=lambda x: x['score'], reverse=True)
    top3 = candidates[:3]
    print(f"[Breakout] Selected {len(top3)} candidates from {len(candidates)} scored players")
    return top3


def check_previous_picks_on_fire(recent_recommendations, stats, merged_data):
    """
    Check if any player recommended in the last 10 days is now on a hot streak.
    Returns list of dicts for players that qualify.
    """
    cutoff = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    on_fire = []

    for rec in recent_recommendations:
        if rec.get('date', '0000-00-00') < cutoff:
            continue
        name = rec['name']
        player_stats = stats.get(name, {})
        last7 = player_stats.get('last7', {})
        current_own = merged_data.get(name, {}).get('espn_ownership', 0)

        # Hot streak signals
        try:
            avg7 = float(last7.get('avg', 0) or 0)
            hr7  = int(last7.get('homeRuns', 0) or 0)
            k7   = int(last7.get('strikeOuts', 0) or 0)
            ip7  = float(last7.get('inningsPitched', 0) or 0)
            own_gain = current_own - rec.get('ownership_at_rec', current_own)

            is_hot = (
                avg7 >= 0.320 or
                hr7 >= 2 or
                (ip7 > 0 and k7 / ip7 >= 1.2) or
                own_gain >= 10
            )

            if is_hot:
                on_fire.append({
                    'name': name,
                    'rec_date': rec['date'],
                    'last7': last7,
                    'current_ownership': current_own,
                    'ownership_gain': own_gain,
                })
        except (ValueError, TypeError):
            continue

    return on_fire


async def generate_claude_analysis(adds, breakout_candidates, on_fire_picks, stats, news):
    """
    Claude analyzes top adds + generates breakout write-ups + calls out hot prev picks.
    """
    print("[Claude] Generating analysis...")

    adds_context = []
    for player in adds:
        player_stats = stats.get(player['name'], {})
        adds_context.append({
            'name': player['name'],
            'position': player['position'],
            'espn_owned': player['espn_ownership'],
            'espn_change': player['espn_change'],
            'last7': player_stats.get('last7', {}),
            'news': news.get(player['name'], ''),
        })

    breakout_context = []
    for player in breakout_candidates:
        breakout_context.append({
            'name': player['name'],
            'position': player['position'],
            'espn_owned': player['espn_ownership'],
            'last7': player['last7'],
            'last14': player['last14'],
            'savant': player['savant'],
        })

    on_fire_context = [
        {
            'name': p['name'],
            'rec_date': p['rec_date'],
            'last7': p['last7'],
            'current_ownership': p['current_ownership'],
            'ownership_gain': p['ownership_gain'],
        }
        for p in on_fire_picks
    ]

    prompt = f"""You are a sharp, opinionated fantasy baseball analyst. Today is {datetime.now(ZoneInfo('America/New_York')).strftime('%B %d, %Y')}.

TOP WAIVER WIRE ADDS (sorted by ownership spike):
{json.dumps(adds_context, indent=2)}

BREAKOUT CANDIDATES (under 50% owned, strong underlying metrics):
{json.dumps(breakout_context, indent=2)}

PREVIOUSLY RECOMMENDED PLAYERS NOW ON A HOT STREAK:
{json.dumps(on_fire_context, indent=2)}

Respond ONLY with a valid JSON object (no markdown, no explanation):
{{
  "intro": "1-2 punchy sentences on today's waiver wire landscape",
  "add_comments": {{"player_name": "10-15 word take on why to add or avoid", ...}},
  "breakout_writeups": [
    {{
      "name": "player name",
      "headline": "5-8 word punchy header",
      "why": "2-3 sentences referencing actual metrics — xwOBA, barrel rate, trend from last14 to last7. Be specific and confident."
    }}
  ],
  "on_fire_callouts": [
    {{
      "name": "player name",
      "callout": "1-2 sentences celebrating the hot streak, referencing stats"
    }}
  ],
  "spicy_take": "2-3 sentence bold overall take — name names, use numbers"
}}

Be controversial. Be specific. Reference the actual stats provided."""

    try:
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1800,
            messages=[{"role": "user", "content": prompt}]
        )
        response_text = re.sub(r'```json\s*|\s*```', '', message.content[0].text)
        analysis = json.loads(response_text)
        print("[Claude] Analysis generated successfully")
        return analysis

    except Exception as e:
        print(f"[Claude] Error generating analysis: {e}")
        return {
            "intro": "Today's waiver wire has some interesting names worth a look.",
            "add_comments": {},
            "breakout_writeups": [],
            "on_fire_callouts": [],
            "spicy_take": "Do your homework before burning FAAB.",
        }


def build_discord_embed(adds, breakout_candidates, on_fire_picks, analysis, stats, news):
    """Build the Discord embed with top adds, breakout candidates, and hot prev picks."""
    embed = discord.Embed(
        title="🌶️ SHANDLER'S SPICY SUMMARY",
        description=f"{datetime.now(ZoneInfo('America/New_York')).strftime('%A, %B %d, %Y')}\n\n{analysis.get('intro', '')}",
        color=0x1DB954,
        timestamp=datetime.now(ZoneInfo('UTC'))
    )

    # ── SECTION 1: TOP ADDS ──
    adds_text = ""
    for i, player in enumerate(adds, 1):
        emoji = "🔥" if i <= 3 else "📈"
        name = player['name']
        pos = player['position']
        espn_own = f"{player['espn_ownership']:.1f}%"
        espn_change = f"⬆️ +{player['espn_change']:.1f}%" if player['espn_change'] > 0 else f"⬇️ {player['espn_change']:.1f}%"

        adds_text += f"{emoji} **{name}** - {pos}\n"
        adds_text += f"   ESPN: {espn_own} owned ({espn_change})\n"

        player_stats = stats.get(name, {})
        stats_line = format_stats_line(player_stats.get('last7', {}), pos)
        if stats_line:
            adds_text += f"   {stats_line}\n"

        comment = analysis.get('add_comments', {}).get(name, news.get(name, ''))
        if comment:
            adds_text += f"   📰 {comment}\n"
        adds_text += "\n"

    embed.add_field(
        name="🔥 TOP WAIVER WIRE ADDS",
        value=(adds_text[:1020] + "...") if len(adds_text) > 1024 else (adds_text or "No significant adds"),
        inline=False
    )

    embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━━━━", inline=False)

    # ── SECTION 2: BREAKOUT CANDIDATES ──
    breakout_writeups = analysis.get('breakout_writeups', [])
    breakout_text = ""
    for i, player in enumerate(breakout_candidates, 1):
        name = player['name']
        pos = player['position']
        own = f"{player['espn_ownership']:.1f}%"
        savant = player.get('savant', {})

        # Find Claude's writeup for this player
        writeup = next((w for w in breakout_writeups if w.get('name', '').lower() == name.lower()), {})
        headline = writeup.get('headline', 'Under-the-radar pick')
        why = writeup.get('why', '')

        breakout_text += f"💎 **{name}** - {pos} ({own} owned)\n"
        breakout_text += f"   *{headline}*\n"

        # Savant metrics line
        metrics = []
        if savant.get('xwoba') and savant['xwoba'] != 'N/A':
            metrics.append(f"xwOBA: {savant['xwoba']}")
        if savant.get('barrel_rate') and savant['barrel_rate'] != 'N/A':
            metrics.append(f"Barrel%: {savant['barrel_rate']}")
        if savant.get('hard_hit_pct') and savant['hard_hit_pct'] != 'N/A':
            metrics.append(f"HH%: {savant['hard_hit_pct']}")
        if metrics:
            breakout_text += f"   📊 {' • '.join(metrics)}\n"

        if why:
            breakout_text += f"   {why}\n"
        breakout_text += "\n"

    if breakout_text:
        embed.add_field(
            name="🚀 BREAKOUT CANDIDATES",
            value=(breakout_text[:1020] + "...") if len(breakout_text) > 1024 else breakout_text,
            inline=False
        )
        embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━━━━", inline=False)

    # ── SECTION 3: PREVIOUS PICKS ON FIRE ──
    on_fire_callouts = analysis.get('on_fire_callouts', [])
    if on_fire_picks:
        fire_text = ""
        for player in on_fire_picks:
            name = player['name']
            last7 = player.get('last7', {})
            stats_line = format_stats_line(last7, 'OF')  # generic hitter display
            callout_data = next((c for c in on_fire_callouts if c.get('name', '').lower() == name.lower()), {})
            callout = callout_data.get('callout', f"Still cooking — {player['current_ownership']:.1f}% owned and climbing.")

            fire_text += f"🔥 **{name}** (rec'd {player['rec_date']})\n"
            if stats_line:
                fire_text += f"   {stats_line}\n"
            fire_text += f"   {callout}\n\n"

        embed.add_field(
            name="✅ PREVIOUS PICKS PAYING OFF",
            value=(fire_text[:1020] + "...") if len(fire_text) > 1024 else fire_text,
            inline=False
        )
        embed.add_field(name="\u200b", value="━━━━━━━━━━━━━━━━━━━", inline=False)

    # ── SECTION 4: CLAUDE'S SPICY TAKE ──
    take_text = analysis.get('spicy_take', 'No take today.')
    embed.add_field(
        name="🌶️ SHANDLER'S SPICY TAKE",
        value=(take_text[:1020] + "...") if len(take_text) > 1024 else take_text,
        inline=False
    )

    embed.set_footer(text="Data: ESPN Fantasy + Baseball Savant • Updated daily at 7:00 AM ET")
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
        
        # Step 3: Filter trending players (adds only, top 5)
        adds = filter_trending_players(merged_data)

        # Step 4: Fetch stats for all adds + broader pool for breakout candidates
        all_names = [p['name'] for p in adds]

        # Also pull a broader pool for breakout analysis (low-owned players)
        breakout_pool_names = [
            name for name, data in merged_data.items()
            if data['espn_ownership'] < MAX_OWNERSHIP and name not in all_names
        ][:40]  # cap to avoid too many API calls

        stats = await fetch_recent_stats(all_names + breakout_pool_names)
        news = await fetch_recent_news(all_names)

        # Step 5: Find breakout candidates and check previous picks
        recent_recommendations = state.get('recommendation_history', [])
        breakout_candidates = await find_breakout_candidates(merged_data, stats, recent_recommendations)
        on_fire_picks = check_previous_picks_on_fire(recent_recommendations, stats, merged_data)

        # Step 6: Claude analysis
        analysis = await generate_claude_analysis(adds, breakout_candidates, on_fire_picks, stats, news)

        # Step 7: Build Discord embed
        embed = build_discord_embed(adds, breakout_candidates, on_fire_picks, analysis, stats, news)

        # Step 8: Post to Discord
        channel = client.get_channel(CHANNEL_ID)
        if not channel:
            print(f"[Waiver Wire Bot] Channel {CHANNEL_ID} not found")
            return

        message = await channel.send(embed=embed)

        # Step 9: Add reaction emojis
        reactions = ['🔥', '🧊', '💎', '🚀']
        for emoji in reactions:
            try:
                await message.add_reaction(emoji)
            except:
                pass

        # Step 10: Save state — log new breakout recommendations
        today = datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
        history = state.get('recommendation_history', [])
        for player in breakout_candidates:
            history.append({
                'name': player['name'],
                'date': today,
                'ownership_at_rec': player['espn_ownership'],
            })

        # Prune history older than 30 days
        cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        history = [r for r in history if r.get('date', '0000-00-00') >= cutoff]

        state['last_post_time'] = datetime.now(ZoneInfo('UTC')).isoformat()
        state['last_ownership_data'] = merged_data
        state['recommendation_history'] = history
        save_state(state)
        
        print(f"[Waiver Wire Bot] Daily report posted successfully at {datetime.now(ZoneInfo('America/New_York'))}")
        
    except Exception as e:
        print(f"[Waiver Wire Bot] Error in daily report: {e}")
        import traceback
        traceback.print_exc()


@client.event
async def on_ready():
    print(f'[Waiver Wire Bot] Logged in as {client.user}')

    # Kill switch — set WAIVER_WIRE_ENABLED=false to disable posting entirely
    if os.getenv('WAIVER_WIRE_ENABLED', 'true').lower() == 'false':
        print("[Waiver Wire Bot] DISABLED via WAIVER_WIRE_ENABLED=false — not scheduling any posts")
        return

    # TEST MODE: run immediately on startup
    if os.getenv('WAIVER_WIRE_TEST_MODE', 'false').lower() == 'true':
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

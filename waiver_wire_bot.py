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
OWNERSHIP_THRESHOLD = 5.0
SPIKE_THRESHOLD = 10.0
MAX_OWNERSHIP = 50.0
TOP_N = 5

# Discord client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Claude client
anthropic_client = Anthropic(api_key=CLAUDE_API_KEY)

# Scheduler
scheduler = AsyncIOScheduler(timezone='America/New_York')


def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {'last_post_time': None, 'last_ownership_data': {}, 'recommendation_history': []}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


async def fetch_espn_ownership():
    """Fetch player ownership + eligibility from ESPN kona_player_info."""
    print("[ESPN API] Fetching player ownership data...")
    ownership_data = {}

    POSITION_MAP = {
        1: 'SP', 2: 'C', 3: '1B', 4: '2B', 5: '3B',
        6: 'SS', 7: 'OF', 8: 'DH', 9: 'RP',
    }

    TEAM_MAP = {
        1:'BAL',2:'BOS',3:'LAA',4:'CHW',5:'CLE',6:'DET',7:'KC',8:'MIL',
        9:'MIN',10:'NYY',11:'OAK',12:'SEA',13:'TEX',14:'TOR',15:'ATL',
        16:'CHC',17:'CIN',18:'HOU',19:'LAD',20:'WSH',21:'NYM',22:'PHI',
        23:'PIT',24:'STL',25:'SD',26:'SF',27:'COL',28:'MIA',29:'ARI',30:'TB',
    }

    # eligibleSlots that map to actual positions (not UTIL/BN/IL)
    SLOT_TO_POS = {
        0:'C', 1:'1B', 2:'2B', 3:'3B', 4:'SS',
        5:'OF', 6:'2B/SS', 7:'1B/3B', 9:'DH',
        14:'SP', 15:'RP',
    }

    url = (
        'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb'
        '/seasons/2026/segments/0/leaguedefaults/3?view=kona_player_info'
    )
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
        'x-fantasy-filter': '{"players":{"limit":300,"sortPercOwned":{"sortPriority":1,"sortAsc":false},"filterActive":{"value":true}}}',
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                print(f"[ESPN API] Response status: {response.status}")
                if response.status != 200:
                    body = await response.text()
                    print(f"[ESPN API] Error body: {body[:300]}")
                    return ownership_data

                data = await response.json(content_type=None)
                players = data.get('players', [])
                print(f"[ESPN API] Retrieved {len(players)} players from ESPN")

                sample = sorted(
                    [e for e in players if e.get('player', {}).get('ownership', {}).get('percentChange', 0) != 0],
                    key=lambda e: e.get('player', {}).get('ownership', {}).get('percentChange', 0),
                    reverse=True
                )[:5]
                for e in sample:
                    p = e.get('player', {})
                    own = p.get('ownership', {})
                    print(f"[ESPN API] Sample: {p.get('fullName')} — {own.get('percentOwned'):.1f}% owned, {own.get('percentChange'):+.1f}% change")

                with_ownership = sum(1 for e in players if e.get('player', {}).get('ownership', {}).get('percentOwned', 0) > 0)
                with_change = sum(1 for e in players if e.get('player', {}).get('ownership', {}).get('percentChange', 0) != 0)
                print(f"[ESPN API] Players with >0% owned: {with_ownership}, with non-zero change: {with_change}")

                for entry in players:
                    try:
                        player = entry.get('player', {})
                        ownership = player.get('ownership', {})

                        full_name = player.get('fullName', '').strip()
                        if not full_name:
                            continue

                        pct_owned = ownership.get('percentOwned', 0.0)
                        pct_change = ownership.get('percentChange', 0.0)

                        if pct_owned == 0.0:
                            continue

                        default_pos_id = player.get('defaultPositionId', 0)
                        position = POSITION_MAP.get(default_pos_id, 'NA')

                        # Build multi-position eligibility string
                        eligible_slots = player.get('eligibleSlots', [])
                        eligible_positions = []
                        for slot in eligible_slots:
                            pos = SLOT_TO_POS.get(slot)
                            if pos and pos not in eligible_positions:
                                eligible_positions.append(pos)
                        multi_pos = '/'.join(eligible_positions) if len(eligible_positions) > 1 else ''

                        pro_team_id = player.get('proTeamId', 0)
                        team = TEAM_MAP.get(pro_team_id, '')

                        # Injury status — flag but don't filter (early season, be permissive)
                        injury_status = player.get('injuryStatus', 'ACTIVE')
                        # Only hard-filter IL60
                        if injury_status in ('SIXTY_DAY_DL',):
                            continue

                        # Extract season stats from ESPN payload directly (stat key map)
                        # statSplitTypeId=0 = full season, seasonId=2026
                        espn_stats = {}
                        for stat_entry in player.get('stats', []):
                            if (stat_entry.get('seasonId') == 2026 and
                                    stat_entry.get('statSplitTypeId') == 0 and
                                    stat_entry.get('statSourceId') == 0):
                                espn_stats = stat_entry.get('stats', {})
                                break

                        # Games played (pitchers use GS, hitters use AB proxy)
                        games_played = entry.get('player', {}).get('gamesPlayedByPosition', {})
                        total_games = sum(games_played.values()) if games_played else 0

                        ownership_data[full_name] = {
                            'ownership': round(pct_owned, 2),
                            'change': round(pct_change, 2),
                            'position': position,
                            'multi_pos': multi_pos,
                            'team': team,
                            'espn_id': player.get('id'),
                            'injury_status': injury_status,
                            'espn_stats': espn_stats,
                            'games_played': total_games,
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
    name = re.sub(r'\s+(Jr\.|Sr\.|III|IV|II)$', '', name, flags=re.IGNORECASE)
    if ',' in name:
        parts = name.split(',')
        if len(parts) == 2:
            last, first = parts[0].strip(), parts[1].strip()
            name = f"{first} {last}"
    name = re.sub(r'\s+[A-Z]\.\s+', ' ', name)
    name = ' '.join(name.split())
    return name.strip()


def merge_ownership_data(espn_data, previous_data):
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
            'multi_pos': info.get('multi_pos', ''),
            'team': info.get('team', ''),
            'injury_status': info.get('injury_status', 'ACTIVE'),
            'espn_id': info.get('espn_id'),
            'espn_stats': info.get('espn_stats', {}),
            'games_played': info.get('games_played', 0),
        }

    return merged


def filter_trending_players(merged_data, threshold=OWNERSHIP_THRESHOLD):
    """Top 5 adds — positive ownership change, under 50% owned unless spiking."""
    adds = []

    for player, data in merged_data.items():
        own = data['espn_ownership']
        change = data['avg_change']

        if change <= 0:
            continue

        if own > MAX_OWNERSHIP and change < SPIKE_THRESHOLD:
            continue

        # Soft flag IL10/DTD but don't exclude — Claude can mention it
        adds.append({'name': player, **data})

    adds.sort(key=lambda x: x['avg_change'], reverse=True)
    print(f"[Filter] Found {len(adds)} players with positive ownership change")
    return adds[:TOP_N]


async def fetch_schedule(session, team_abbrev, days=7):
    """
    Fetch upcoming starts/games for a team from MLB Stats API.
    Returns list of {date, opponent, home} for next N days.
    """
    TEAM_ID_MAP = {
        'BAL':110,'BOS':111,'LAA':108,'CHW':145,'CLE':114,'DET':116,'KC':118,
        'MIL':158,'MIN':142,'NYY':147,'OAK':133,'SEA':136,'TEX':140,'TOR':141,
        'ATL':144,'CHC':112,'CIN':113,'HOU':117,'LAD':119,'WSH':120,'NYM':121,
        'PHI':143,'PIT':134,'STL':138,'SD':135,'SF':137,'COL':115,'MIA':146,
        'ARI':109,'TB':139,
    }
    team_id = TEAM_ID_MAP.get(team_abbrev)
    if not team_id:
        return []

    start = datetime.now().strftime('%Y-%m-%d')
    end = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
    url = f"https://statsapi.mlb.com/api/v1/schedule?teamId={team_id}&startDate={start}&endDate={end}&sportId=1"

    try:
        async with session.get(url, timeout=10) as r:
            if r.status != 200:
                return []
            data = await r.json()
            games = []
            for date_entry in data.get('dates', []):
                for game in date_entry.get('games', []):
                    home_team = game.get('teams', {}).get('home', {}).get('team', {}).get('abbreviation', '')
                    away_team = game.get('teams', {}).get('away', {}).get('team', {}).get('abbreviation', '')
                    is_home = home_team == team_abbrev
                    opponent = away_team if is_home else home_team
                    games.append({
                        'date': date_entry['date'],
                        'opponent': opponent,
                        'home': is_home,
                    })
            return games
    except Exception:
        return []


async def fetch_player_id(session, player_name):
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


async def fetch_savant_pitcher_metrics(session):
    """Fetch pitcher Savant leaderboard — K%, BB%, GB%."""
    try:
        url = (
            "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
            "?type=pitcher&year=2026&position=&team=&min=5&csv=true"
        )
        async with session.get(url, timeout=15) as r:
            if r.status != 200:
                return {}
            text = await r.text()
            lines = text.strip().split("\n")
            if len(lines) < 2:
                return {}
            headers = [h.strip().strip('"') for h in lines[0].split(",")]
            cache = {}
            for line in lines[1:]:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) < len(headers):
                    continue
                row = dict(zip(headers, parts))
                pid = row.get('player_id', '')
                if pid:
                    cache[pid] = {
                        'xera':    row.get('est_woba', 'N/A'),
                        'k_pct':   row.get('strikeout', 'N/A'),
                        'bb_pct':  row.get('walk', 'N/A'),
                        'gb_pct':  row.get('groundballs_percent', 'N/A'),
                    }
            return cache
    except Exception as e:
        print(f"[Savant Pitcher] Fetch failed: {e}")
        return {}


async def fetch_recent_stats(player_names, merged_data=None):
    """
    Fetch last-7 + last-14 splits, Savant hitter + pitcher metrics,
    and upcoming schedule for each player.
    """
    print(f"[MLB Stats] Fetching stats for {len(player_names)} players...")
    stats = {}

    async with aiohttp.ClientSession() as session:
        # Pre-fetch Savant hitter leaderboard
        hitter_savant = {}
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
                                hitter_savant[pid] = {
                                    'xba':          row.get('est_ba', 'N/A'),
                                    'xslg':         row.get('est_slg', 'N/A'),
                                    'xwoba':        row.get('est_woba', 'N/A'),
                                    'barrel_rate':  row.get('barrel_batted_rate', 'N/A'),
                                    'hard_hit_pct': row.get('hard_hit_percent', 'N/A'),
                                    'k_pct':        row.get('strikeout', 'N/A'),
                                    'bb_pct':       row.get('walk', 'N/A'),
                                }
            print(f"[Savant] Loaded {len(hitter_savant)} hitters from leaderboard")
        except Exception as e:
            print(f"[Savant] Hitter fetch failed: {e}")

        # Pre-fetch Savant pitcher leaderboard
        pitcher_savant = await fetch_savant_pitcher_metrics(session)
        print(f"[Savant] Loaded {len(pitcher_savant)} pitchers from leaderboard")

        for player in player_names:
            try:
                player_id = await fetch_player_id(session, player)
                if not player_id:
                    continue

                last7  = await fetch_splits(session, player_id, 7)
                last14 = await fetch_splits(session, player_id, 14)

                # Determine if pitcher or hitter for Savant lookup
                position = (merged_data or {}).get(player, {}).get('position', 'OF')
                is_pitcher = position in ('SP', 'RP')

                if is_pitcher:
                    savant = pitcher_savant.get(str(player_id), {})
                else:
                    savant = hitter_savant.get(str(player_id), {})

                # Fetch upcoming schedule
                team = (merged_data or {}).get(player, {}).get('team', '')
                schedule = await fetch_schedule(session, team) if team else []

                # Count starts in next 7 days for pitchers
                starts_next_7 = len(schedule) if is_pitcher else 0

                stats[player] = {
                    'last7':          last7,
                    'last14':         last14,
                    'savant':         savant,
                    'mlb_id':         player_id,
                    'schedule':       schedule,
                    'starts_next_7':  starts_next_7,
                    'is_pitcher':     is_pitcher,
                }

            except Exception as e:
                print(f"[MLB Stats] Error for {player}: {e}")
                continue

    print(f"[MLB Stats] Retrieved stats for {len(stats)} players")
    return stats


async def fetch_recent_news(player_names):
    print(f"[News Fetcher] Fetching news for {len(player_names)} players...")
    news = {}
    async with aiohttp.ClientSession() as session:
        for player in player_names:
            try:
                search_query = player.replace(' ', '+')
                url = f"https://www.espn.com/apis/fantasy/v2/news?player={search_query}"
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('headlines'):
                            news[player] = data['headlines'][0]['headline']
            except:
                continue
    print(f"[News Fetcher] Retrieved news for {len(news)} players")
    return news


async def find_breakout_candidates(merged_data, stats, recent_recommendations):
    """
    Under 35% owned, at least 5 PA or 1 IP in last 14 days,
    not recommended in last 10 days, score >= 2.
    Pitchers and hitters scored separately.
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

        if own < 1.0 or own > 35.0:
            continue
        if name in recently_recommended:
            continue
        if name not in stats:
            continue

        player_stats = stats[name]
        savant = player_stats.get('savant', {})
        last7  = player_stats.get('last7', {})
        last14 = player_stats.get('last14', {})
        is_pitcher = player_stats.get('is_pitcher', False)

        pa14 = int(last14.get('plateAppearances', 0) or last14.get('atBats', 0) or 0)
        ip14 = float(last14.get('inningsPitched', 0) or 0)
        if pa14 < 5 and ip14 < 1.0:
            continue

        score = 0.0

        if is_pitcher:
            # Pitcher scoring: K%, BB%, GB%, K/IP trend
            try:
                k_pct = float(savant.get('k_pct', 0) or 0)
                if k_pct >= 28:
                    score += 3
                elif k_pct >= 22:
                    score += 2
                elif k_pct >= 18:
                    score += 1
            except (ValueError, TypeError):
                pass

            try:
                bb_pct = float(savant.get('bb_pct', 0) or 0)
                if bb_pct <= 7:
                    score += 2
                elif bb_pct <= 10:
                    score += 1
            except (ValueError, TypeError):
                pass

            try:
                k7  = int(last7.get('strikeOuts', 0) or 0)
                ip7 = float(last7.get('inningsPitched', 0) or 0)
                if ip7 > 0 and (k7 / ip7) >= 1.2:
                    score += 3
                elif ip7 > 0 and (k7 / ip7) >= 1.0:
                    score += 2
            except (ValueError, TypeError):
                pass

            # Upcoming starts bonus
            if player_stats.get('starts_next_7', 0) >= 2:
                score += 1

        else:
            # Hitter scoring: xwOBA, barrel rate, hard hit%, trend
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

            try:
                hard_hit = float(savant.get('hard_hit_pct', 0) or 0)
                if hard_hit >= 48:
                    score += 2
                elif hard_hit >= 40:
                    score += 1
            except (ValueError, TypeError):
                pass

            try:
                avg7  = float(last7.get('avg', 0) or 0)
                avg14 = float(last14.get('avg', 0) or 0)
                if avg7 > avg14 + 0.030:
                    score += 2
                elif avg7 > avg14:
                    score += 1
            except (ValueError, TypeError):
                pass

        if score >= 2:
            candidates.append({
                'name':           name,
                'score':          score,
                'espn_ownership': data['espn_ownership'],
                'espn_change':    data['espn_change'],
                'position':       data['position'],
                'multi_pos':      data.get('multi_pos', ''),
                'team':           data.get('team', ''),
                'injury_status':  data.get('injury_status', 'ACTIVE'),
                'games_played':   data.get('games_played', 0),
                'last7':          last7,
                'last14':         last14,
                'savant':         savant,
                'schedule':       player_stats.get('schedule', []),
                'starts_next_7':  player_stats.get('starts_next_7', 0),
                'is_pitcher':     is_pitcher,
            })

    candidates.sort(key=lambda x: x['score'], reverse=True)
    top3 = candidates[:3]
    print(f"[Breakout] Selected {len(top3)} candidates from {len(candidates)} scored players")
    return top3


async def generate_claude_analysis(adds, breakout_candidates, stats, news):
    """Claude analyzes adds + breakout candidates."""
    print("[Claude] Generating analysis...")

    adds_context = []
    for player in adds:
        player_stats = stats.get(player['name'], {})
        schedule = player_stats.get('schedule', [])
        starts_next_7 = player_stats.get('starts_next_7', 0)
        pos = player['position']
        is_pitcher = pos in ('SP', 'RP')

        entry = {
            'name':         player['name'],
            'position':     pos,
            'team':         player.get('team', ''),
            'multi_pos':    player.get('multi_pos', ''),
            'espn_owned':   player['espn_ownership'],
            'espn_change':  player['espn_change'],
            'injury_status': player.get('injury_status', 'ACTIVE'),
            'last7':        player_stats.get('last7', {}),
            'news':         news.get(player['name'], ''),
        }
        if is_pitcher:
            entry['starts_next_7'] = starts_next_7
            entry['upcoming_opponents'] = [g['opponent'] for g in schedule[:2]]
        adds_context.append(entry)

    breakout_context = []
    for player in breakout_candidates:
        player_stats = stats.get(player['name'], {})
        pos = player['position']
        is_pitcher = pos in ('SP', 'RP')

        entry = {
            'name':          player['name'],
            'position':      pos,
            'team':          player.get('team', ''),
            'multi_pos':     player.get('multi_pos', ''),
            'espn_owned':    player['espn_ownership'],
            'games_played':  player.get('games_played', 0),
            'injury_status': player.get('injury_status', 'ACTIVE'),
            'last7':         player['last7'],
            'last14':        player['last14'],
            'savant':        player['savant'],
        }
        if is_pitcher:
            entry['starts_next_7'] = player.get('starts_next_7', 0)
            entry['upcoming_opponents'] = [g['opponent'] for g in player.get('schedule', [])[:2]]
        breakout_context.append(entry)

    prompt = f"""You are a sharp, opinionated fantasy baseball analyst. Today is {datetime.now(ZoneInfo('America/New_York')).strftime('%B %d, %Y')}. It is 10 days into the 2026 MLB season — small samples, but real signals exist.

TOP WAIVER WIRE ADDS (sorted by 7-day ownership spike):
{json.dumps(adds_context, indent=2)}

BREAKOUT CANDIDATES (under 35% owned, strong underlying metrics):
{json.dumps(breakout_context, indent=2)}

Respond ONLY with a valid JSON object (no markdown, no explanation):
{{
  "intro": "1-2 punchy sentences on today's waiver wire landscape. Appears below the player list as editorial context.",
  "add_comments": {{
    "player_name": "2 sentences max. First: stat-backed take on why to add or avoid. Second: one piece of context that changes the picture — upcoming starts, opponent quality, injury flag, multi-position value, or workload concern. Skip second sentence if it would be filler. No ellipses.",
    "...": "..."
  }},
  "breakout_writeups": [
    {{
      "name": "player name",
      "headline": "5-8 word punchy header",
      "league_fit": "Roto" or "H2H" or "Both",
      "why": "2-3 sentences. Lead with most compelling metric. Add schedule/role/trend context. Third sentence only if it sharpens the argument. No ellipses.",
      "faab_range": "e.g. 5-10% or $3-5 or Low priority"
    }}
  ],
  "spicy_take": "2-3 sentences. Bold overall market take — name names, cite numbers. Third sentence only if it materially sharpens the argument. No ellipses."
}}

Rules: Be controversial. Be specific. Reference actual stats. No filler. No ellipses anywhere."""

    try:
        message = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
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
            "spicy_take": "Do your homework before burning FAAB.",
        }


def format_stats_line(stats, position):
    if not stats:
        return ""
    if position in ['SP', 'RP']:
        era  = float(stats.get('era', 0) or 0)
        ip   = stats.get('inningsPitched', '0')
        k    = int(stats.get('strikeOuts', 0) or 0)
        whip = float(stats.get('whip', 0) or 0)
        return f"Last 7: {ip} IP, {k} K, {era:.2f} ERA, {whip:.2f} WHIP"
    else:
        avg = stats.get('avg', '.000') or '.000'
        hr  = int(stats.get('homeRuns', 0) or 0)
        rbi = int(stats.get('rbi', 0) or 0)
        sb  = int(stats.get('stolenBases', 0) or 0)
        return f"Last 7: {avg} AVG, {hr} HR, {rbi} RBI, {sb} SB"


def safe_truncate(text, limit=1024):
    """Trim to last complete sentence within Discord field limit."""
    if len(text) <= limit:
        return text
    truncated = text[:limit]
    last_period = max(truncated.rfind('. '), truncated.rfind('.\n'))
    return truncated[:last_period + 1] if last_period > 0 else truncated


def build_discord_embed(adds, breakout_candidates, analysis, stats, news):
    embed = discord.Embed(
        title="🌶️ SHANDLER'S SPICY SUMMARY",
        description=f"⚡ **The Wire Tap | Board Regs Fantasy Baseball**\n{datetime.now(ZoneInfo('America/New_York')).strftime('%A, %B %d, %Y')}",
        color=0x1DB954,
        timestamp=datetime.now(ZoneInfo('UTC'))
    )

    # ── SECTION 1: TOP ADDS ──
    adds_text = ""
    for i, player in enumerate(adds, 1):
        emoji = "🔥" if i <= 3 else "📈"
        name = player['name']
        pos = player['position']
        team = player.get('team', '')
        multi_pos = player.get('multi_pos', '')
        espn_own = f"{player['espn_ownership']:.1f}%"
        espn_change = f"⬆️ +{player['espn_change']:.1f}%" if player['espn_change'] > 0 else f"⬇️ {player['espn_change']:.1f}%"
        injury = player.get('injury_status', 'ACTIVE')

        name_line = f"**{name} | {pos}"
        if team:
            name_line += f" | {team}"
        name_line += "**"
        if injury not in ('ACTIVE', ''):
            name_line += f" ⚠️ {injury.replace('_', ' ').title()}"

        adds_text += f"{emoji} {name_line}\n"
        adds_text += f"   ESPN: {espn_own} owned ({espn_change})"

        # Multi-position flag
        if multi_pos:
            adds_text += f" [{multi_pos}]"
        adds_text += "\n"

        player_stats = stats.get(name, {})
        stats_line = format_stats_line(player_stats.get('last7', {}), pos)
        if stats_line:
            adds_text += f"   {stats_line}\n"

        comment = analysis.get('add_comments', {}).get(name, news.get(name, ''))
        if comment:
            adds_text += f"   📰 {comment}\n"
        adds_text += "\n"

    intro = analysis.get('intro', '')
    if intro:
        adds_text += f"\n*{intro}*"

    embed.add_field(
        name="🎯 TOP WAIVER WIRE ADDS",
        value=safe_truncate(adds_text) or "No significant adds",
        inline=False
    )


    # ── SECTION 2: BREAKOUT CANDIDATES ──
    breakout_writeups = analysis.get('breakout_writeups', [])
    breakout_text = ""
    for player in breakout_candidates:
        name = player['name']
        pos = player['position']
        team = player.get('team', '')
        multi_pos = player.get('multi_pos', '')
        own = f"{player['espn_ownership']:.1f}%"
        savant = player.get('savant', {})
        injury = player.get('injury_status', 'ACTIVE')

        writeup = next((w for w in breakout_writeups if w.get('name', '').lower() == name.lower()), {})
        headline = writeup.get('headline', 'Under-the-radar pick')
        why = writeup.get('why', '')
        league_fit = writeup.get('league_fit', '')
        faab = writeup.get('faab_range', '')

        name_line = f"**{name} | {pos}"
        if team:
            name_line += f" | {team}"
        name_line += f"** ({own} owned)"
        if injury not in ('ACTIVE', ''):
            name_line += f" ⚠️"
        if multi_pos:
            name_line += f" [{multi_pos}]"

        breakout_text += f"💎 {name_line}\n"
        breakout_text += f"   *{headline}*"
        if league_fit:
            fit_emoji = "📊" if league_fit == "Roto" else "⚔️" if league_fit == "H2H" else "✅"
            breakout_text += f" — {fit_emoji} {league_fit}"
        breakout_text += "\n"

        # Savant metrics
        metrics = []
        if savant.get('xwoba') and savant['xwoba'] != 'N/A':
            metrics.append(f"xwOBA: {savant['xwoba']}")
        if savant.get('barrel_rate') and savant['barrel_rate'] != 'N/A':
            metrics.append(f"Barrel%: {savant['barrel_rate']}")
        if savant.get('hard_hit_pct') and savant['hard_hit_pct'] != 'N/A':
            metrics.append(f"HH%: {savant['hard_hit_pct']}")
        if savant.get('k_pct') and savant['k_pct'] != 'N/A' and pos in ('SP', 'RP'):
            metrics.append(f"K%: {savant['k_pct']}")
        if metrics:
            breakout_text += f"   📊 {' • '.join(metrics)}\n"

        # Schedule for pitchers
        starts = player.get('starts_next_7', 0)
        schedule = player.get('schedule', [])
        if pos in ('SP', 'RP') and starts > 0:
            opponents = ', '.join([f"vs {g['opponent']}" if g['home'] else f"@ {g['opponent']}" for g in schedule[:starts]])
            breakout_text += f"   📅 {starts} start{'s' if starts > 1 else ''} this week: {opponents}\n"

        if why:
            breakout_text += f"   {why}\n"
        if faab:
            breakout_text += f"   💰 FAAB: {faab}\n"
        breakout_text += "\n"

    if breakout_text:
        embed.add_field(
            name="🚀 BREAKOUT CANDIDATES",
            value=safe_truncate(breakout_text),
            inline=False
        )
    else:
        embed.add_field(
            name="🚀 BREAKOUT CANDIDATES",
            value="*No candidates cleared the bar today — the Savant data is thin this early. Check back as the sample builds.*",
            inline=False
        )


    # ── SECTION 3: SPICY TAKE ──
    take_text = analysis.get('spicy_take', 'No take today.')
    embed.add_field(
        name="🌶️ SHANDLER'S SPICY TAKE",
        value=safe_truncate(take_text),
        inline=False
    )

    embed.set_footer(text=f"Updated daily at 7:00 AM ET • {datetime.now(ZoneInfo('America/New_York')).strftime('%B %d, %Y')}")
    return embed


async def post_daily_report():
    print(f"\n[Waiver Wire Bot] Starting daily report generation at {datetime.now(ZoneInfo('America/New_York'))}")

    try:
        state = load_state()
        previous_ownership = state.get('last_ownership_data', {})

        # Step 1: ESPN ownership data
        espn_data = await fetch_espn_ownership()
        if not espn_data:
            print("[Waiver Wire Bot] ESPN fetch returned no data - skipping post")
            return

        # Step 2: Merge
        merged_data = merge_ownership_data(espn_data, previous_ownership)

        # Step 3: Top adds
        adds = filter_trending_players(merged_data)

        # Step 4: Stats — adds + breakout pool
        all_names = [p['name'] for p in adds]
        breakout_pool_names = [
            name for name, data in merged_data.items()
            if data['espn_ownership'] <= 35.0 and name not in all_names
        ][:50]

        stats = await fetch_recent_stats(all_names + breakout_pool_names, merged_data)
        news = await fetch_recent_news(all_names)

        # Step 5: Breakout candidates
        recent_recommendations = state.get('recommendation_history', [])
        breakout_candidates = await find_breakout_candidates(merged_data, stats, recent_recommendations)

        # Step 6: Claude
        analysis = await generate_claude_analysis(adds, breakout_candidates, stats, news)

        # Step 7: Build embed
        embed = build_discord_embed(adds, breakout_candidates, analysis, stats, news)

        # Step 8: Post
        channel = client.get_channel(CHANNEL_ID)
        if not channel:
            print(f"[Waiver Wire Bot] Channel {CHANNEL_ID} not found")
            return

        message = await channel.send(embed=embed)

        reactions = ['🔥', '💎', '🚀', '🌶️']
        for emoji in reactions:
            try:
                await message.add_reaction(emoji)
            except:
                pass

        # Step 9: Save state
        today = datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
        history = state.get('recommendation_history', [])
        for player in breakout_candidates:
            history.append({
                'name': player['name'],
                'date': today,
                'ownership_at_rec': player['espn_ownership'],
            })
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

    if os.getenv('WAIVER_WIRE_ENABLED', 'true').lower() == 'false':
        print("[Waiver Wire Bot] DISABLED via WAIVER_WIRE_ENABLED=false — not scheduling any posts")
        return

    if os.getenv('WAIVER_WIRE_TEST_MODE', 'false').lower() == 'true':
        print("[Waiver Wire Bot] TEST MODE: Running report immediately")
        await post_daily_report()

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
    await client.start(DISCORD_TOKEN)


if __name__ == '__main__':
    asyncio.run(start_waiver_wire_bot())

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
SPIKE_THRESHOLD = 10.0
MAX_OWNERSHIP = 50.0
TOP_N = 5  # top 5 per category

# Discord client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Claude client
anthropic_client = Anthropic(api_key=CLAUDE_API_KEY)

# Scheduler
scheduler = AsyncIOScheduler(timezone='America/New_York')

PITCHERS = ('SP', 'RP')
HITTERS  = ('C', '1B', '2B', '3B', 'SS', 'OF', 'DH')


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


def safe_truncate(text, limit=1024):
    """Trim to last complete sentence within Discord field limit."""
    if len(text) <= limit:
        return text
    truncated = text[:limit]
    last = max(truncated.rfind('. '), truncated.rfind('.\n'))
    return truncated[:last + 1] if last > 0 else truncated


async def fetch_espn_ownership():
    """Fetch player ownership data from ESPN's kona_player_info API."""
    print("[ESPN API] Fetching player ownership data...")
    ownership_data = {}

    POSITION_MAP = {
        1: 'SP', 2: 'C', 3: '1B', 4: '2B', 5: '3B',
        6: 'SS', 7: 'OF', 8: 'DH', 9: 'RP',
    }

    TEAM_MAP = {
        1:'BAL', 2:'BOS', 3:'LAA', 4:'CHW', 5:'CLE', 6:'DET', 7:'KC',  8:'MIL',
        9:'MIN', 10:'NYY', 11:'OAK', 12:'SEA', 13:'TEX', 14:'TOR', 15:'ATL',
        16:'CHC', 17:'CIN', 18:'HOU', 19:'LAD', 20:'WSH', 21:'NYM', 22:'PHI',
        23:'PIT', 24:'STL', 25:'SD',  26:'SF',  27:'COL', 28:'MIA', 29:'ARI', 30:'TB',
    }

    # Roster slot ID → eligible position (excludes UTIL/BN/IL)
    SLOT_TO_POS = {
        0:'C', 1:'1B', 2:'2B', 3:'3B', 4:'SS',
        5:'OF', 6:'2B/SS', 7:'1B/3B', 9:'DH', 10:'SP', 11:'RP',
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

                # Debug: top movers
                sample = sorted(
                    [e for e in players if e.get('player', {}).get('ownership', {}).get('percentChange', 0) > 0],
                    key=lambda e: e['player']['ownership']['percentChange'],
                    reverse=True
                )[:5]
                for e in sample:
                    p = e['player']
                    own = p['ownership']
                    print(f"[ESPN API] Sample: {p.get('fullName')} — {own.get('percentOwned'):.1f}% owned, {own.get('percentChange'):+.1f}% change")

                for entry in players:
                    try:
                        player   = entry.get('player', {})
                        ownership = player.get('ownership', {})
                        full_name = player.get('fullName', '').strip()
                        if not full_name:
                            continue

                        pct_owned  = ownership.get('percentOwned', 0.0)
                        pct_change = ownership.get('percentChange', 0.0)
                        if pct_owned == 0.0:
                            continue

                        # IL60 only hard filter
                        injury_status = player.get('injuryStatus', 'ACTIVE')
                        if injury_status == 'SIXTY_DAY_DL':
                            continue

                        default_pos_id = player.get('defaultPositionId', 0)
                        position = POSITION_MAP.get(default_pos_id, 'NA')

                        # Two-way player fix: if eligible at hitter AND pitcher slots,
                        # prefer hitter designation (more fantasy-relevant)
                        hitter_slots = {0,1,2,3,4,5,6,7,9}  # C,1B,2B,3B,SS,OF,2B/SS,1B/3B,DH
                        pitcher_slots = {10, 11}
                        slot_set = set(player.get('eligibleSlots', []))
                        if (slot_set & hitter_slots) and (slot_set & pitcher_slots):
                            # Has both — find primary hitter position
                            for slot in player.get('eligibleSlots', []):
                                if slot in hitter_slots:
                                    hitter_pos = SLOT_TO_POS.get(slot)
                                    if hitter_pos:
                                        position = hitter_pos
                                        break

                        # Multi-position eligibility (deduplicated, excluding default)
                        eligible_slots = player.get('eligibleSlots', [])
                        seen = set()
                        eligible_positions = []
                        for slot in eligible_slots:
                            pos_str = SLOT_TO_POS.get(slot)
                            if pos_str and pos_str not in seen and pos_str != position:
                                seen.add(pos_str)
                                eligible_positions.append(pos_str)
                        multi_pos = '/'.join(eligible_positions) if eligible_positions else ''

                        team = TEAM_MAP.get(player.get('proTeamId', 0), '')

                        games_played = sum(entry.get('player', {}).get('gamesPlayedByPosition', {}).values())

                        ownership_data[full_name] = {
                            'ownership':      round(pct_owned, 2),
                            'change':         round(pct_change, 2),
                            'position':       position,
                            'multi_pos':      multi_pos,
                            'team':           team,
                            'injury_status':  injury_status,
                            'games_played':   games_played,
                            'espn_id':        player.get('id'),
                        }

                    except Exception as e:
                        print(f"[ESPN API] Error parsing entry: {e}")
                        continue

        except Exception as e:
            print(f"[ESPN API] Request failed: {e}")
            import traceback; traceback.print_exc()

    print(f"[ESPN API] Parsed {len(ownership_data)} players")
    return ownership_data


def merge_ownership_data(espn_data, previous_data):
    merged = {}
    for player, info in espn_data.items():
        prev_own   = previous_data.get(player, {}).get('espn_ownership', 0)
        espn_own   = info['ownership']
        espn_change = info.get('change', espn_own - prev_own)

        merged[player] = {
            'espn_ownership': espn_own,
            'espn_change':    espn_change,
            'avg_change':     espn_change,
            'position':       info.get('position', 'NA'),
            'multi_pos':      info.get('multi_pos', ''),
            'team':           info.get('team', ''),
            'injury_status':  info.get('injury_status', 'ACTIVE'),
            'games_played':   info.get('games_played', 0),
            'espn_id':        info.get('espn_id'),
        }
    return merged


def filter_adds(merged_data, position_group):
    """
    Return top 5 adds for either pitchers or hitters.
    Must have positive ownership change and be under MAX_OWNERSHIP
    unless spiking over SPIKE_THRESHOLD.
    """
    adds = []
    for player, data in merged_data.items():
        pos    = data['position']
        own    = data['espn_ownership']
        change = data['avg_change']

        if pos not in position_group:
            continue
        if change <= 0:
            continue
        if own > MAX_OWNERSHIP and change < SPIKE_THRESHOLD:
            continue

        adds.append({'name': player, **data})

    adds.sort(key=lambda x: x['avg_change'], reverse=True)
    return adds[:TOP_N]


async def fetch_player_id(session, player_name):
    try:
        url = f"https://statsapi.mlb.com/api/v1/people/search?names={player_name}"
        async with session.get(url, timeout=10) as r:
            if r.status != 200:
                return None
            data = await r.json()
            people = data.get('people', [])
            return people[0]['id'] if people else None
    except Exception:
        return None


async def fetch_splits(session, player_id, days):
    end_date   = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    url = (f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
           f"?stats=statsSingleSeason&season=2026&startDate={start_date}&endDate={end_date}")
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


async def fetch_schedule(session, team_abbrev, days=7):
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
    end   = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
    url   = f"https://statsapi.mlb.com/api/v1/schedule?teamId={team_id}&startDate={start}&endDate={end}&sportId=1"
    try:
        async with session.get(url, timeout=10) as r:
            if r.status != 200:
                return []
            data = await r.json()
            games = []
            for date_entry in data.get('dates', []):
                for game in date_entry.get('games', []):
                    home = game.get('teams', {}).get('home', {}).get('team', {}).get('abbreviation', '')
                    away = game.get('teams', {}).get('away', {}).get('team', {}).get('abbreviation', '')
                    is_home = home == team_abbrev
                    games.append({'date': date_entry['date'], 'opponent': away if is_home else home, 'home': is_home})
            return games
    except Exception:
        return []


async def fetch_recent_stats(player_names, merged_data=None):
    """Fetch last-7 + last-14 splits, Savant metrics, and schedule for each player."""
    print(f"[MLB Stats] Fetching stats for {len(player_names)} players...")
    stats = {}

    async with aiohttp.ClientSession() as session:
        # Hitter Savant leaderboard
        hitter_savant = {}
        try:
            url = "https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=batter&year=2026&position=&team=&min=5&csv=true"
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
            print(f"[Savant] {len(hitter_savant)} hitters loaded")
        except Exception as e:
            print(f"[Savant] Hitter fetch failed: {e}")

        # Pitcher Savant leaderboard
        pitcher_savant = {}
        try:
            url = "https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=pitcher&year=2026&position=&team=&min=5&csv=true"
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
                                pitcher_savant[pid] = {
                                    'xera':   row.get('est_woba', 'N/A'),
                                    'k_pct':  row.get('strikeout', 'N/A'),
                                    'bb_pct': row.get('walk', 'N/A'),
                                    'gb_pct': row.get('groundballs_percent', 'N/A'),
                                }
            print(f"[Savant] {len(pitcher_savant)} pitchers loaded")
        except Exception as e:
            print(f"[Savant] Pitcher fetch failed: {e}")

        for player in player_names:
            try:
                player_id = await fetch_player_id(session, player)
                if not player_id:
                    continue

                last7  = await fetch_splits(session, player_id, 7)
                last14 = await fetch_splits(session, player_id, 14)

                pos        = (merged_data or {}).get(player, {}).get('position', 'OF')
                is_pitcher = pos in PITCHERS
                savant     = pitcher_savant.get(str(player_id), {}) if is_pitcher else hitter_savant.get(str(player_id), {})

                team        = (merged_data or {}).get(player, {}).get('team', '')
                schedule    = await fetch_schedule(session, team) if team else []
                starts_next_7 = len(schedule) if is_pitcher else 0

                stats[player] = {
                    'last7':         last7,
                    'last14':        last14,
                    'savant':        savant,
                    'mlb_id':        player_id,
                    'schedule':      schedule,
                    'starts_next_7': starts_next_7,
                    'is_pitcher':    is_pitcher,
                }

            except Exception as e:
                print(f"[MLB Stats] Error for {player}: {e}")
                continue

    print(f"[MLB Stats] Retrieved stats for {len(stats)} players")
    return stats


async def fetch_recent_news(player_names):
    news = {}
    async with aiohttp.ClientSession() as session:
        for player in player_names:
            try:
                url = f"https://www.espn.com/apis/fantasy/v2/news?player={player.replace(' ', '+')}"
                async with session.get(url, timeout=10) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data.get('headlines'):
                            news[player] = data['headlines'][0]['headline']
            except:
                continue
    return news


async def find_breakout_candidates(merged_data, stats, recent_recommendations):
    """2 hitters + 2 pitchers under 35% owned with strong underlying metrics."""
    print("[Breakout] Finding breakout candidates...")

    cutoff = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    recently_recommended = {
        r['name'] for r in recent_recommendations
        if r.get('date', '0000-00-00') >= cutoff
    }

    hitter_candidates  = []
    pitcher_candidates = []

    for name, data in merged_data.items():
        own = data['espn_ownership']
        pos = data['position']

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
        is_pitcher = pos in PITCHERS

        pa14 = int(last14.get('plateAppearances', 0) or last14.get('atBats', 0) or 0)
        ip14 = float(last14.get('inningsPitched', 0) or 0)
        if pa14 < 5 and ip14 < 1.0:
            continue

        score = 0.0

        if is_pitcher:
            try:
                k_pct = float(savant.get('k_pct', 0) or 0)
                if k_pct >= 28: score += 3
                elif k_pct >= 22: score += 2
                elif k_pct >= 18: score += 1
            except: pass
            try:
                bb_pct = float(savant.get('bb_pct', 0) or 0)
                if bb_pct <= 7: score += 2
                elif bb_pct <= 10: score += 1
            except: pass
            try:
                k7  = int(last7.get('strikeOuts', 0) or 0)
                ip7 = float(last7.get('inningsPitched', 0) or 0)
                if ip7 > 0 and (k7 / ip7) >= 1.2: score += 3
                elif ip7 > 0 and (k7 / ip7) >= 1.0: score += 2
            except: pass
            if player_stats.get('starts_next_7', 0) >= 2:
                score += 1
        else:
            try:
                xwoba = float(savant.get('xwoba', 0) or 0)
                if xwoba >= 0.370: score += 3
                elif xwoba >= 0.340: score += 2
                elif xwoba >= 0.310: score += 1
            except: pass
            try:
                barrel = float(savant.get('barrel_rate', 0) or 0)
                if barrel >= 12: score += 3
                elif barrel >= 8: score += 2
                elif barrel >= 5: score += 1
            except: pass
            try:
                hard_hit = float(savant.get('hard_hit_pct', 0) or 0)
                if hard_hit >= 48: score += 2
                elif hard_hit >= 40: score += 1
            except: pass
            try:
                avg7  = float(last7.get('avg', 0) or 0)
                avg14 = float(last14.get('avg', 0) or 0)
                if avg7 > avg14 + 0.030: score += 2
                elif avg7 > avg14: score += 1
            except: pass

        if score < 2:
            continue

        candidate = {
            'name':           name,
            'score':          score,
            'espn_ownership': own,
            'espn_change':    data['espn_change'],
            'position':       pos,
            'multi_pos':      data.get('multi_pos', ''),
            'team':           data.get('team', ''),
            'injury_status':  data.get('injury_status', 'ACTIVE'),
            'games_played':   data.get('games_played', 0),
            'last7':          last7,
            'last14':         last14,
            'savant':         savant,
            'starts_next_7':  player_stats.get('starts_next_7', 0),
            'is_pitcher':     is_pitcher,
        }

        if is_pitcher:
            pitcher_candidates.append(candidate)
        else:
            hitter_candidates.append(candidate)

    hitter_candidates.sort(key=lambda x: x['score'], reverse=True)
    pitcher_candidates.sort(key=lambda x: x['score'], reverse=True)

    top2_hitters  = hitter_candidates[:2]
    top2_pitchers = pitcher_candidates[:2]

    print(f"[Breakout] {len(top2_pitchers)} pitchers, {len(top2_hitters)} hitters selected")
    return top2_pitchers, top2_hitters


async def generate_claude_analysis(pitcher_adds, hitter_adds, breakout_pitchers, breakout_hitters, stats, news):
    """Single Claude call covering all three embeds."""
    print("[Claude] Generating analysis...")

    def build_add_context(players):
        out = []
        for p in players:
            ps = stats.get(p['name'], {})
            entry = {
                'name':           p['name'],
                'position':       p['position'],
                'team':           p.get('team', ''),
                'multi_pos':      p.get('multi_pos', ''),
                'espn_owned':     p['espn_ownership'],
                'espn_change':    p['espn_change'],
                'injury_status':  p.get('injury_status', 'ACTIVE'),
                'last7':          ps.get('last7', {}),
                'news':           news.get(p['name'], ''),
            }
            if p['position'] in PITCHERS:
                entry['starts_next_7'] = ps.get('starts_next_7', 0)
                entry['upcoming_opponents'] = [g['opponent'] for g in ps.get('schedule', [])[:2]]
            out.append(entry)
        return out

    def build_breakout_context(players):
        out = []
        for p in players:
            entry = {
                'name':          p['name'],
                'position':      p['position'],
                'team':          p.get('team', ''),
                'multi_pos':     p.get('multi_pos', ''),
                'espn_owned':    p['espn_ownership'],
                'games_played':  p.get('games_played', 0),
                'injury_status': p.get('injury_status', 'ACTIVE'),
                'last7':         p['last7'],
                'last14':        p['last14'],
                'savant':        p['savant'],
            }
            if p['is_pitcher']:
                entry['starts_next_7'] = p.get('starts_next_7', 0)
            out.append(entry)
        return out

    prompt = f"""You are a sharp, opinionated fantasy baseball analyst. Today is {datetime.now(ZoneInfo('America/New_York')).strftime('%B %d, %Y')}. It is 10 days into the 2026 MLB season.

TOP PITCHER ADDS:
{json.dumps(build_add_context(pitcher_adds), indent=2)}

TOP HITTER ADDS:
{json.dumps(build_add_context(hitter_adds), indent=2)}

BREAKOUT PITCHER CANDIDATES (under 35% owned):
{json.dumps(build_breakout_context(breakout_pitchers), indent=2)}

BREAKOUT HITTER CANDIDATES (under 35% owned):
{json.dumps(build_breakout_context(breakout_hitters), indent=2)}

Respond ONLY with a valid JSON object. No markdown, no explanation:
{{
  "pitcher_add_comments": {{
    "player_name": "2 sentences max. First: specific stat-backed take. Second: one key context — starts, health, matchup, regression risk. Skip second if filler. No ellipses."
  }},
  "hitter_add_comments": {{
    "player_name": "2 sentences max. Same rules as pitchers."
  }},
  "breakout_writeups": [
    {{
      "name": "player name",
      "headline": "5-8 word punchy header",
      "league_fit": "Roto" or "H2H" or "Both",
      "why": "2-3 sentences. Lead with the best metric. Add role/trend context. Third sentence only if it materially adds — not filler. No ellipses.",
    }}
  ],
  "pitcher_intro": "One punchy sentence on the overall pitcher waiver landscape today.",
  "hitter_intro": "One punchy sentence on the overall hitter waiver landscape today.",
  "breakout_intro": "One punchy sentence setting up the breakout candidates."
}}

Be opinionated. Reference actual stats. No filler. No ellipses."""

    try:
        message = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}]
        )
        response_text = re.sub(r'```json\s*|\s*```', '', message.content[0].text)
        analysis = json.loads(response_text)
        print("[Claude] Analysis generated successfully")
        return analysis
    except Exception as e:
        print(f"[Claude] Error: {e}")
        return {
            "pitcher_add_comments": {}, "hitter_add_comments": {},
            "breakout_writeups": [],
            "pitcher_intro": "", "hitter_intro": "", "breakout_intro": "",
        }


def format_stats_line(stats, position):
    if not stats:
        return ""
    if position in PITCHERS:
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


def build_header():
    """Shared header line for all three embeds."""
    return f"⚡ **The Wire Tap | Board Regs Fantasy Baseball**\n{datetime.now(ZoneInfo('America/New_York')).strftime('%A, %B %d, %Y')}"


def build_adds_embed(players, analysis, stats, news, is_pitcher):
    """Build a top-5 adds embed for either pitchers or hitters."""
    comments_key = "pitcher_add_comments" if is_pitcher else "hitter_add_comments"
    intro_key    = "pitcher_intro" if is_pitcher else "hitter_intro"
    title        = "🔥 TOP PITCHER ADDS" if is_pitcher else "🔥 TOP HITTER ADDS"
    color        = 0xE74C3C if is_pitcher else 0x2ECC71  # red for pitchers, green for hitters

    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=datetime.now(ZoneInfo('UTC'))
    )

    field_text = ""
    for i, player in enumerate(players, 1):
        emoji  = "🔥" if i <= 3 else "📈"
        name   = player['name']
        pos    = player['position']
        team   = player.get('team', '')
        multi  = player.get('multi_pos', '')
        injury = player.get('injury_status', 'ACTIVE')
        own    = f"{player['espn_ownership']:.1f}%"
        change = f"⬆️ +{player['espn_change']:.1f}%" if player['espn_change'] > 0 else f"⬇️ {player['espn_change']:.1f}%"

        name_line = f"**{name} | {pos} | {team}**" if team else f"**{name} | {pos}**"
        if injury not in ('ACTIVE', ''):
            name_line += f" ⚠️"
        if multi:
            name_line += f" [{multi}]"

        field_text += f"{emoji} {name_line}\n"
        field_text += f"   ESPN: {own} owned ({change})\n"

        ps = stats.get(name, {})
        stats_line = format_stats_line(ps.get('last7', {}), pos)
        if stats_line:
            field_text += f"   {stats_line}\n"

        comment = analysis.get(comments_key, {}).get(name, news.get(name, ''))
        if comment:
            field_text += f"   📰 {comment}\n"

        field_text += "\n"

    embed.add_field(
        name="\u200b",
        value=safe_truncate(field_text) or "No significant adds today.",
        inline=False
    )

    embed.set_footer(text=f"Updated daily at 7:00 AM ET • {datetime.now(ZoneInfo('America/New_York')).strftime('%B %d, %Y')}")
    return embed


def build_breakout_embed(breakout_pitchers, breakout_hitters, analysis):
    """Build the breakout candidates embed with 2 pitchers + 2 hitters."""
    embed = discord.Embed(
        title="🚀 BREAKOUT CANDIDATES",
        color=0x9B59B6,  # purple
        timestamp=datetime.now(ZoneInfo('UTC'))
    )

    writeups = analysis.get('breakout_writeups', [])

    def render_candidate(player):
        name   = player['name']
        pos    = player['position']
        team   = player.get('team', '')
        multi  = player.get('multi_pos', '')
        own    = f"{player['espn_ownership']:.1f}%"
        injury = player.get('injury_status', 'ACTIVE')
        savant = player.get('savant', {})

        wu = next((w for w in writeups if w.get('name', '').lower() == name.lower()), {})
        headline   = wu.get('headline', 'Under-the-radar pick')
        league_fit = wu.get('league_fit', '')
        why        = wu.get('why', '')

        name_line = f"**{name} | {pos} | {team}**" if team else f"**{name} | {pos}**"
        name_line += f" ({own} owned)"
        if injury not in ('ACTIVE', ''):
            name_line += " ⚠️"
        if multi:
            name_line += f" [{multi}]"

        text = f"💎 {name_line}\n"
        text += f"   *{headline}*"
        if league_fit:
            text += f" [{league_fit}]"
        text += "\n"

        # Savant metrics
        metrics = []
        if savant.get('xwoba') and savant['xwoba'] != 'N/A':
            metrics.append(f"xwOBA: {savant['xwoba']}")
        if savant.get('barrel_rate') and savant['barrel_rate'] != 'N/A':
            metrics.append(f"Barrel%: {savant['barrel_rate']}")
        if savant.get('hard_hit_pct') and savant['hard_hit_pct'] != 'N/A':
            metrics.append(f"HH%: {savant['hard_hit_pct']}")
        if savant.get('k_pct') and savant['k_pct'] != 'N/A' and pos in PITCHERS:
            metrics.append(f"K%: {savant['k_pct']}")
        if savant.get('xera') and savant['xera'] != 'N/A' and pos in PITCHERS:
            metrics.append(f"xERA: {savant['xera']}")
        if metrics:
            text += f"   📊 {' • '.join(metrics)}\n"

        if why:
            text += f"   {why}\n"
        text += "\n"
        return text

    # Pitchers first
    if breakout_pitchers:
        pitcher_text = "".join(render_candidate(p) for p in breakout_pitchers)
        embed.add_field(
            name="⚾ PITCHERS",
            value=safe_truncate(pitcher_text),
            inline=False
        )
    
    # Then hitters
    if breakout_hitters:
        hitter_text = "".join(render_candidate(p) for p in breakout_hitters)
        embed.add_field(
            name="🏃 HITTERS",
            value=safe_truncate(hitter_text),
            inline=False
        )

    if not breakout_pitchers and not breakout_hitters:
        embed.add_field(
            name="\u200b",
            value="*No candidates cleared the bar today — Savant data is thin this early. Check back as the sample builds.*",
            inline=False
        )

    embed.set_footer(text=f"Updated daily at 7:00 AM ET • {datetime.now(ZoneInfo('America/New_York')).strftime('%B %d, %Y')}")
    return embed


async def post_daily_report():
    print(f"\n[Waiver Wire Bot] Starting daily report at {datetime.now(ZoneInfo('America/New_York'))}")

    try:
        state = load_state()
        previous_ownership = state.get('last_ownership_data', {})

        # 1. ESPN ownership
        espn_data = await fetch_espn_ownership()
        if not espn_data:
            print("[Waiver Wire Bot] ESPN fetch returned no data — skipping post")
            return

        # 2. Merge
        merged_data = merge_ownership_data(espn_data, previous_ownership)

        # 3. Filter adds by type
        pitcher_adds = filter_adds(merged_data, PITCHERS)
        hitter_adds  = filter_adds(merged_data, HITTERS)
        print(f"[Filter] {len(pitcher_adds)} pitcher adds, {len(hitter_adds)} hitter adds")

        # 4. Collect all names needing stats
        add_names = list({p['name'] for p in pitcher_adds + hitter_adds})
        breakout_pool = [
            name for name, data in merged_data.items()
            if data['espn_ownership'] <= 35.0 and name not in add_names
        ][:60]

        stats = await fetch_recent_stats(add_names + breakout_pool, merged_data)
        news  = await fetch_recent_news(add_names)

        # 5. Breakout candidates
        recent_recs = state.get('recommendation_history', [])
        breakout_pitchers, breakout_hitters = await find_breakout_candidates(merged_data, stats, recent_recs)

        # 6. Claude analysis
        analysis = await generate_claude_analysis(
            pitcher_adds, hitter_adds,
            breakout_pitchers, breakout_hitters,
            stats, news
        )

        # 7. Build embeds
        pitcher_embed  = build_adds_embed(pitcher_adds,  analysis, stats, news, is_pitcher=True)
        hitter_embed   = build_adds_embed(hitter_adds,   analysis, stats, news, is_pitcher=False)
        breakout_embed = build_breakout_embed(breakout_pitchers, breakout_hitters, analysis)

        # 8. Post — pitchers first, then hitters, then breakouts
        channel = client.get_channel(CHANNEL_ID)
        if not channel:
            print(f"[Waiver Wire Bot] Channel {CHANNEL_ID} not found")
            return

        reactions = ['🔥', '💎', '🚀', '🌶️']

        for embed in (pitcher_embed, hitter_embed, breakout_embed):
            try:
                msg = await channel.send(embed=embed)
                for emoji in reactions:
                    try:
                        await msg.add_reaction(emoji)
                    except:
                        pass
                await asyncio.sleep(1)  # small stagger between embeds
            except Exception as e:
                print(f"[Waiver Wire Bot] Failed to post embed: {e}")
                continue

        # 9. Save state
        today   = datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')
        history = state.get('recommendation_history', [])
        for player in breakout_pitchers + breakout_hitters:
            history.append({
                'name':             player['name'],
                'date':             today,
                'ownership_at_rec': player['espn_ownership'],
            })
        cutoff  = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        history = [r for r in history if r.get('date', '0000-00-00') >= cutoff]

        state['last_post_time']        = datetime.now(ZoneInfo('UTC')).isoformat()
        state['last_ownership_data']   = merged_data
        state['recommendation_history'] = history
        save_state(state)

        print(f"[Waiver Wire Bot] All embeds posted successfully at {datetime.now(ZoneInfo('America/New_York'))}")

    except Exception as e:
        print(f"[Waiver Wire Bot] Error in daily report: {e}")
        import traceback; traceback.print_exc()


@client.event
async def on_ready():
    print(f'[Waiver Wire Bot] Logged in as {client.user}')

    if os.getenv('WAIVER_WIRE_ENABLED', 'true').lower() == 'false':
        print("[Waiver Wire Bot] DISABLED via WAIVER_WIRE_ENABLED=false")
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

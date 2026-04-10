import os
import re
import json
import csv
import io
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
import discord
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from anthropic import Anthropic

# Environment variables
DISCORD_TOKEN = os.getenv('ANALYTIC_BOT_TOKEN')
CHANNEL_ID = int(os.getenv('STREAMING_CHANNEL_ID', '0'))
CLAUDE_API_KEY = os.getenv('STREAMING_BOT_SUMMARY')

# Configuration
STATE_FILE = 'state/probable_starters_state.json'
TIMEZONE = 'America/New_York'
MAX_OWNERSHIP = float(os.getenv('PROBABLE_STARTERS_MAX_OWNERSHIP', '60'))
MAX_STARTERS_PER_RUN = int(os.getenv('PROBABLE_STARTERS_MAX_POSTS', '25'))
MIN_START_SCORE_TO_POST = int(os.getenv('PROBABLE_STARTERS_MIN_SCORE', '55'))
SCHED_HOUR = int(os.getenv('PROBABLE_STARTERS_SCHED_HOUR', '7'))
SCHED_MINUTE = int(os.getenv('PROBABLE_STARTERS_SCHED_MINUTE', '10'))

# Discord client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Claude client
anthropic_client = Anthropic(api_key=CLAUDE_API_KEY) if CLAUDE_API_KEY else None

# Scheduler
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

TEAM_ABBR_BY_ID = {
    109: 'ARI', 110: 'BAL', 111: 'BOS', 112: 'CHC', 113: 'CIN', 114: 'CLE',
    115: 'COL', 116: 'DET', 117: 'HOU', 118: 'KC', 119: 'LAD', 120: 'WSH',
    121: 'NYM', 133: 'ATH', 134: 'PIT', 135: 'SD', 136: 'SEA', 137: 'SF',
    138: 'STL', 139: 'TB', 140: 'TEX', 141: 'TOR', 142: 'MIN', 143: 'PHI',
    144: 'ATL', 145: 'CHW', 146: 'MIA', 147: 'NYY', 158: 'MIL', 108: 'LAA',
}

TEAM_NAME_TO_ABBR = {
    'diamondbacks': 'ARI', 'orioles': 'BAL', 'red sox': 'BOS', 'cubs': 'CHC',
    'reds': 'CIN', 'guardians': 'CLE', 'rockies': 'COL', 'tigers': 'DET',
    'astros': 'HOU', 'royals': 'KC', 'dodgers': 'LAD', 'nationals': 'WSH',
    'mets': 'NYM', 'athletics': 'ATH', 'pirates': 'PIT', 'padres': 'SD',
    'mariners': 'SEA', 'giants': 'SF', 'cardinals': 'STL', 'rays': 'TB',
    'rangers': 'TEX', 'blue jays': 'TOR', 'twins': 'MIN', 'phillies': 'PHI',
    'braves': 'ATL', 'white sox': 'CHW', 'marlins': 'MIA', 'yankees': 'NYY',
    'brewers': 'MIL', 'angels': 'LAA',
}

PARK_FACTORS = {
    'ARI': {'run': 101, 'hr': 106}, 'ATL': {'run': 100, 'hr': 104}, 'BAL': {'run': 95, 'hr': 92},
    'BOS': {'run': 104, 'hr': 96}, 'CHC': {'run': 103, 'hr': 109}, 'CHW': {'run': 101, 'hr': 108},
    'CIN': {'run': 105, 'hr': 112}, 'CLE': {'run': 97, 'hr': 96}, 'COL': {'run': 119, 'hr': 116},
    'DET': {'run': 97, 'hr': 94}, 'HOU': {'run': 99, 'hr': 101}, 'KC': {'run': 102, 'hr': 97},
    'LAA': {'run': 99, 'hr': 101}, 'LAD': {'run': 99, 'hr': 99}, 'MIA': {'run': 95, 'hr': 92},
    'MIL': {'run': 101, 'hr': 103}, 'MIN': {'run': 100, 'hr': 103}, 'NYM': {'run': 97, 'hr': 95},
    'NYY': {'run': 102, 'hr': 114}, 'ATH': {'run': 100, 'hr': 101}, 'PHI': {'run': 103, 'hr': 109},
    'PIT': {'run': 97, 'hr': 93}, 'SD': {'run': 96, 'hr': 93}, 'SEA': {'run': 94, 'hr': 92},
    'SF': {'run': 94, 'hr': 90}, 'STL': {'run': 97, 'hr': 95}, 'TB': {'run': 99, 'hr': 98},
    'TEX': {'run': 103, 'hr': 110}, 'TOR': {'run': 101, 'hr': 104}, 'WSH': {'run': 100, 'hr': 101},
}


TEAM_COLORS = {
    'ARI': 0xA71930,
    'ATL': 0xCE1141,
    'BAL': 0xDF4601,
    'BOS': 0xBD3039,
    'CHC': 0x0E3386,
    'CHW': 0x27251F,
    'CIN': 0xC6011F,
    'CLE': 0xE31937,
    'COL': 0x33006F,
    'DET': 0x0C2340,
    'HOU': 0xEB6E1F,
    'KC': 0x004687,
    'LAA': 0xBA0021,
    'LAD': 0x005A9C,
    'MIA': 0x00A3E0,
    'MIL': 0x12284B,
    'MIN': 0x002B5C,
    'NYM': 0x002D72,
    'NYY': 0x0C2340,
    'ATH': 0x003831,
    'PHI': 0xE81828,
    'PIT': 0xFDB827,
    'SD': 0x2F241D,
    'SEA': 0x005C5C,
    'SF': 0xFD5A1E,
    'STL': 0xC41E3A,
    'TB': 0x092C5C,
    'TEX': 0x003278,
    'TOR': 0x134A8E,
    'WSH': 0xAB0003,
}

TEAM_LOGO_SLUGS = {
    'ARI': 'ari', 'ATL': 'atl', 'BAL': 'bal', 'BOS': 'bos', 'CHC': 'chc', 'CHW': 'cws',
    'CIN': 'cin', 'CLE': 'cle', 'COL': 'col', 'DET': 'det', 'HOU': 'hou', 'KC': 'kc',
    'LAA': 'laa', 'LAD': 'lad', 'MIA': 'mia', 'MIL': 'mil', 'MIN': 'min', 'NYM': 'nym',
    'NYY': 'nyy', 'ATH': 'oak', 'PHI': 'phi', 'PIT': 'pit', 'SD': 'sd', 'SEA': 'sea',
    'SF': 'sf', 'STL': 'stl', 'TB': 'tb', 'TEX': 'tex', 'TOR': 'tor', 'WSH': 'wsh',
}

def dart_rating(score):
    try:
        score = int(score or 0)
    except Exception:
        return '🎯'

    if score >= 81:
        count = 5
    elif score >= 75:
        count = 4
    elif score >= 68:
        count = 3
    elif score >= 60:
        count = 2
    elif score >= 55:
        count = 1
    else:
        count = 1
    return '🎯' * count


def team_logo_url(team_abbr):
    slug = TEAM_LOGO_SLUGS.get(team_abbr)
    if not slug:
        return None
    return f'https://a.espncdn.com/i/teamlogos/mlb/500/{slug}.png'



def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {'last_post_date': None, 'posted_gamepks': []}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def safe_truncate(text, limit=1024):
    if not text:
        return ''
    if len(text) <= limit:
        return text
    trimmed = text[:limit]
    last_stop = max(trimmed.rfind('. '), trimmed.rfind('.\n'))
    return trimmed[:last_stop + 1] if last_stop > 0 else trimmed


def fmt_avg(value, default='—'):
    try:
        return f"{float(value):.3f}".lstrip('0')
    except Exception:
        return default


def fmt_num(value, digits=2, default='—'):
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return default


def fmt_pct(value, digits=1, default='—'):
    try:
        return f"{float(value):.{digits}f}%"
    except Exception:
        return default


def innings_to_float(ip):
    try:
        if isinstance(ip, (int, float)):
            return float(ip)
        s = str(ip)
        if '.' not in s:
            return float(s)
        whole, frac = s.split('.', 1)
        whole = int(whole)
        frac = int(frac)
        if frac == 1:
            return whole + (1 / 3)
        if frac == 2:
            return whole + (2 / 3)
        return float(s)
    except Exception:
        return 0.0


def classify_park(park_abbr):
    pf = PARK_FACTORS.get(park_abbr, {'run': 100, 'hr': 100})
    run_pf = pf['run']
    hr_pf = pf['hr']
    if run_pf >= 108 or hr_pf >= 110:
        return 'very hitter-friendly'
    if run_pf >= 103 or hr_pf >= 105:
        return 'slightly hitter-friendly'
    if run_pf <= 94 or hr_pf <= 92:
        return 'very pitcher-friendly'
    if run_pf <= 97 or hr_pf <= 96:
        return 'slightly pitcher-friendly'
    return 'neutral'


def build_start_score(starter):
    score = 50

    pitcher = starter.get('pitcher_metrics', {})
    opp = starter.get('opponent_metrics', {})
    recent = starter.get('recent_form', {})
    park = starter.get('park_factor', {})

    try:
        k_pct = float(pitcher.get('k_pct', 0) or 0)
        if k_pct >= 28:
            score += 10
        elif k_pct >= 24:
            score += 7
        elif k_pct >= 20:
            score += 4
    except Exception:
        pass

    try:
        bb_pct = float(pitcher.get('bb_pct', 0) or 0)
        if bb_pct <= 6:
            score += 5
        elif bb_pct <= 8:
            score += 3
        elif bb_pct >= 10:
            score -= 5
    except Exception:
        pass

    try:
        xera = float(pitcher.get('xera', 0) or 0)
        if xera <= 3.30:
            score += 8
        elif xera <= 3.80:
            score += 5
        elif xera >= 4.60:
            score -= 8
    except Exception:
        pass

    try:
        hard_hit = float(pitcher.get('hard_hit_pct', 0) or 0)
        if hard_hit <= 35:
            score += 4
        elif hard_hit >= 43:
            score -= 4
    except Exception:
        pass

    try:
        era3 = float(recent.get('era_last3', 0) or 0)
        if era3 <= 3.25:
            score += 6
        elif era3 >= 5.00:
            score -= 6
    except Exception:
        pass

    try:
        whip3 = float(recent.get('whip_last3', 0) or 0)
        if whip3 <= 1.10:
            score += 4
        elif whip3 >= 1.40:
            score -= 4
    except Exception:
        pass

    try:
        k9_3 = float(recent.get('k9_last3', 0) or 0)
        if k9_3 >= 10:
            score += 5
        elif k9_3 <= 7:
            score -= 3
    except Exception:
        pass

    try:
        opp_woba = float(opp.get('woba_last14', 0) or 0)
        if opp_woba <= 0.295:
            score += 8
        elif opp_woba <= 0.310:
            score += 5
        elif opp_woba >= 0.335:
            score -= 8
    except Exception:
        pass

    try:
        opp_k = float(opp.get('k_pct_last14', 0) or 0)
        if opp_k >= 24:
            score += 7
        elif opp_k >= 22:
            score += 4
        elif opp_k <= 19:
            score -= 4
    except Exception:
        pass

    try:
        park_run = float(park.get('run', 100) or 100)
        park_hr = float(park.get('hr', 100) or 100)
        if park_run <= 97:
            score += 3
        elif park_run >= 103:
            score -= 3
        if park_hr <= 96:
            score += 2
        elif park_hr >= 105:
            score -= 3
    except Exception:
        pass

    return max(20, min(85, int(round(score))))


def start_tier(score):
    if score >= 76:
        return 'strong stream'
    if score >= 68:
        return 'viable stream'
    if score >= 60:
        return 'deep-league stream'
    return 'risky stream'

def first_non_empty(row, *keys):
    for key in keys:
        value = row.get(key)
        if value not in (None, '', 'null', 'None', 'N/A', 'NA', '--', '—'):
            return value
    return None


def normalize_header_key(key):
    return re.sub(r'[^a-z0-9]+', '', (key or '').strip().lower())


def value_from_candidates(row, normalized_map, *candidates):
    for candidate in candidates:
        value = row.get(candidate)
        if value not in (None, '', 'null', 'None', 'N/A', 'NA', '--', '—'):
            return value
        norm = normalize_header_key(candidate)
        actual = normalized_map.get(norm)
        if actual:
            value = row.get(actual)
            if value not in (None, '', 'null', 'None', 'N/A', 'NA', '--', '—'):
                return value
    return None


async def fetch_json(session, url, headers=None, timeout=20):
    async with session.get(url, headers=headers, timeout=timeout) as response:
        if response.status != 200:
            text = await response.text()
            raise RuntimeError(f"GET {url} failed with {response.status}: {text[:180]}")
        return await response.json(content_type=None)


async def fetch_text(session, url, headers=None, timeout=20):
    async with session.get(url, headers=headers, timeout=timeout) as response:
        if response.status != 200:
            text = await response.text()
            raise RuntimeError(f"GET {url} failed with {response.status}: {text[:180]}")
        return await response.text()


async def fetch_espn_pitcher_ownership(session):
    print('[Probable Starters] Fetching ESPN ownership...')
    url = (
        'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb'
        '/seasons/2026/segments/0/leaguedefaults/3?view=kona_player_info'
    )
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
        'x-fantasy-filter': json.dumps({
            'players': {
                'limit': 500,
                'sortPercOwned': {'sortPriority': 1, 'sortAsc': False},
                'filterActive': {'value': True},
            }
        }),
    }
    data = await fetch_json(session, url, headers=headers, timeout=30)
    players = data.get('players', [])
    ownership = {}
    for entry in players:
        player = entry.get('player', {})
        if player.get('defaultPositionId') not in (1, 9):
            continue
        full_name = (player.get('fullName') or '').strip()
        if not full_name:
            continue
        own = player.get('ownership', {}).get('percentOwned', 0.0) or 0.0
        change = player.get('ownership', {}).get('percentChange', 0.0) or 0.0
        ownership[full_name.lower()] = {
            'name': full_name,
            'ownership': round(float(own), 1),
            'change': round(float(change), 1),
            'espn_id': player.get('id'),
        }
    print(f'[Probable Starters] Loaded {len(ownership)} ESPN pitchers')
    return ownership


async def fetch_probable_starters(session, target_date):
    print(f'[Probable Starters] Fetching probable starters for {target_date}...')
    url = (
        'https://statsapi.mlb.com/api/v1/schedule'
        f'?sportId=1&date={target_date}&hydrate=probablePitcher,team,venue,game(content(summary)),linescore'
    )
    data = await fetch_json(session, url, timeout=30)
    starters = []

    for date_entry in data.get('dates', []):
        for game in date_entry.get('games', []):
            game_pk = game.get('gamePk')
            venue = game.get('venue', {})
            home_team = game.get('teams', {}).get('home', {}).get('team', {})
            away_team = game.get('teams', {}).get('away', {}).get('team', {})
            home_probable = game.get('teams', {}).get('home', {}).get('probablePitcher', {})
            away_probable = game.get('teams', {}).get('away', {}).get('probablePitcher', {})

            game_time = game.get('gameDate')

            def push(probable, team, opponent, is_home):
                if not probable or not probable.get('id'):
                    return
                starters.append({
                    'game_pk': game_pk,
                    'game_time': game_time,
                    'venue_name': venue.get('name', ''),
                    'park_team': TEAM_ABBR_BY_ID.get(venue.get('id')),
                    'pitcher_id': probable.get('id'),
                    'pitcher_name': probable.get('fullName', ''),
                    'team_id': team.get('id'),
                    'team_abbr': TEAM_ABBR_BY_ID.get(team.get('id'), team.get('abbreviation', '')),
                    'opponent_id': opponent.get('id'),
                    'opponent_abbr': TEAM_ABBR_BY_ID.get(opponent.get('id'), opponent.get('abbreviation', '')),
                    'is_home': is_home,
                })

            push(home_probable, home_team, away_team, True)
            push(away_probable, away_team, home_team, False)

    print(f'[Probable Starters] Found {len(starters)} probable starters before ownership filter')
    return starters


async def fetch_pitcher_game_log(session, pitcher_id):
    current_year = datetime.now(ZoneInfo(TIMEZONE)).year
    url = f'https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats?stats=gameLog&group=pitching&season={current_year}'
    data = await fetch_json(session, url, timeout=20)
    stats = data.get('stats', [])
    if not stats or not stats[0].get('splits'):
        return []
    return stats[0].get('splits', [])


async def fetch_player_lastname_map(session, team_id):
    url = f'https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active'
    data = await fetch_json(session, url, timeout=20)
    hitters = []
    for row in data.get('roster', []):
        person = row.get('person', {})
        pos = row.get('position', {}).get('abbreviation', '')
        if pos in ('P', 'TWP'):
            continue
        full_name = person.get('fullName', '')
        if full_name:
            hitters.append({'id': person.get('id'), 'name': full_name})
    return hitters


async def fetch_team_recent_offense(session, team_id):
    current_year = datetime.now(ZoneInfo(TIMEZONE)).year
    season_url = f'https://statsapi.mlb.com/api/v1/teams/{team_id}/stats?stats=season&group=hitting&season={current_year}'
    season_data = await fetch_json(session, season_url, timeout=20)
    season_stat = {}
    if season_data.get('stats') and season_data['stats'][0].get('splits'):
        season_stat = season_data['stats'][0]['splits'][0].get('stat', {})

    end_date = datetime.now(ZoneInfo(TIMEZONE)).date()
    start_date = end_date - timedelta(days=14)
    logs_url = (
        'https://statsapi.mlb.com/api/v1/schedule'
        f'?sportId=1&teamId={team_id}&startDate={start_date}&endDate={end_date}&hydrate=linescore,team,probablePitcher'
    )
    logs_data = await fetch_json(session, logs_url, timeout=25)

    recent = {'runs': 0, 'games': 0, 'wins': 0}
    for date_entry in logs_data.get('dates', []):
        for game in date_entry.get('games', []):
            home = game.get('teams', {}).get('home', {})
            away = game.get('teams', {}).get('away', {})
            is_home = home.get('team', {}).get('id') == team_id
            me = home if is_home else away
            opp = away if is_home else home
            if me.get('score') is None:
                continue
            recent['runs'] += int(me.get('score', 0) or 0)
            recent['games'] += 1
            if (me.get('score', 0) or 0) > (opp.get('score', 0) or 0):
                recent['wins'] += 1

    recent['runs_per_game'] = round(recent['runs'] / recent['games'], 2) if recent['games'] else None
    return season_stat, recent


async def fetch_hitter_recent_form(session, hitter_id):
    current_year = datetime.now(ZoneInfo(TIMEZONE)).year
    end_date = datetime.now(ZoneInfo(TIMEZONE)).date()
    start_date = end_date - timedelta(days=7)
    url = (
        f'https://statsapi.mlb.com/api/v1/people/{hitter_id}/stats?'
        f'stats=byDateRange&group=hitting&season={current_year}&startDate={start_date}&endDate={end_date}'
    )
    data = await fetch_json(session, url, timeout=20)
    stats = data.get('stats', [])
    if not stats or not stats[0].get('splits'):
        return None
    stat = stats[0]['splits'][0].get('stat', {})
    return {
        'avg': stat.get('avg'),
        'ops': stat.get('ops'),
        'homeRuns': stat.get('homeRuns', 0),
        'strikeOuts': stat.get('strikeOuts', 0),
        'plateAppearances': stat.get('plateAppearances', 0),
    }



async def fetch_savant_pitcher_metrics(session):
    expected_url = 'https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=pitcher&year=2026&position=&team=&min=1&csv=true'
    custom_url = (
        'https://baseballsavant.mlb.com/leaderboard/custom'
        '?type=pitcher&year=2026&min=1&csv=true&chart=false&chartType=beeswarm&filter=&r=no'
        '&selections=player_id,pa,hard_hit_percent,barrel_batted_rate,avg_best_speed'
        '&sort=hard_hit_percent&sortDir=desc&x=pa&y=pa'
    )

    rows = {}

    # Expected stats endpoint: xERA / xBA / xSLG / xwOBA
    expected_text = await fetch_text(session, expected_url, timeout=30)
    expected_text = expected_text.lstrip('\ufeff')
    expected_reader = csv.DictReader(io.StringIO(expected_text))
    expected_headers = expected_reader.fieldnames or []

    if not expected_headers:
        print('[Probable Starters] Savant expected-stats CSV returned no headers')
        return rows

    print(f"[Probable Starters] Savant expected headers: {expected_headers}")
    print('[Probable Starters] Expected Stats CSV only includes x-stats; K/BB are filled from MLB season pitching stats.')

    expected_normalized = {normalize_header_key(h): h for h in expected_headers if h}

    for row in expected_reader:
        if not row:
            continue

        pid = first_non_empty(
            row,
            'player_id',
            'pitcher_id',
            'mlb_id',
            'playerid',
            expected_normalized.get('playerid', ''),
            expected_normalized.get('player_id', ''),
        )
        if not pid:
            continue

        try:
            pid_int = int(float(pid))
        except Exception:
            continue

        player_name = first_non_empty(
            row,
            'player_name',
            'last_name, first_name',
            'name',
            expected_normalized.get('playername', ''),
            expected_normalized.get('lastnamefirstname', ''),
        )

        rows[pid_int] = {
            'pitcher_name': player_name,
            'xba': value_from_candidates(row, expected_normalized, 'est_ba', 'xba', 'xbaagainst', 'expectedba'),
            'xslg': value_from_candidates(row, expected_normalized, 'est_slg', 'xslg', 'xslgagainst', 'expectedslg'),
            'xwoba': value_from_candidates(row, expected_normalized, 'est_woba', 'xwoba', 'expectedwoba'),
            'xera': value_from_candidates(row, expected_normalized, 'xera', 'est_xera', 'expectedera', 'era_estimator'),
            'k_pct': None,
            'bb_pct': None,
            'hard_hit_pct': None,
            'barrel_pct': None,
            'avg_ev': None,
            'gb_pct': None,
        }

    # Custom leaderboard endpoint: contact-quality stats
    try:
        custom_text = await fetch_text(session, custom_url, timeout=30)
        custom_text = custom_text.lstrip('\ufeff')
        custom_reader = csv.DictReader(io.StringIO(custom_text))
        custom_headers = custom_reader.fieldnames or []
        if custom_headers:
            print(f"[Probable Starters] Savant custom headers: {custom_headers}")
            custom_normalized = {normalize_header_key(h): h for h in custom_headers if h}

            for row in custom_reader:
                if not row:
                    continue

                pid = first_non_empty(
                    row,
                    'player_id',
                    'pitcher_id',
                    'mlb_id',
                    'playerid',
                    custom_normalized.get('playerid', ''),
                    custom_normalized.get('player_id', ''),
                )
                if not pid:
                    continue

                try:
                    pid_int = int(float(pid))
                except Exception:
                    continue

                existing = rows.setdefault(pid_int, {
                    'pitcher_name': first_non_empty(
                        row,
                        'player_name',
                        'last_name, first_name',
                        'name',
                        custom_normalized.get('playername', ''),
                        custom_normalized.get('lastnamefirstname', ''),
                    ),
                    'xba': None,
                    'xslg': None,
                    'xwoba': None,
                    'xera': None,
                    'k_pct': None,
                    'bb_pct': None,
                    'hard_hit_pct': None,
                    'barrel_pct': None,
                    'avg_ev': None,
                    'gb_pct': None,
                })

                existing['hard_hit_pct'] = value_from_candidates(
                    row, custom_normalized,
                    'hard_hit_percent', 'hard hit %', 'hardhitpercent', 'hardhitpct', 'hard_hit_pct'
                ) or existing.get('hard_hit_pct')

                existing['barrel_pct'] = value_from_candidates(
                    row, custom_normalized,
                    'barrel_batted_rate', 'barrel%', 'barrel %', 'barrel_percent', 'barrel_pct', 'barrelpct'
                ) or existing.get('barrel_pct')

                existing['avg_ev'] = value_from_candidates(
                    row, custom_normalized,
                    'avg_best_speed', 'avg ev (mph)', 'avg_ev', 'avg_hit_speed', 'average_exit_velocity', 'avg exit velocity'
                ) or existing.get('avg_ev')

                if existing.get('pitcher_name', '').lower() in ('roupp, landen', 'landen roupp'):
                    print(f"[Probable Starters] Savant custom parsed metrics for {existing.get('pitcher_name')}: "
                          f"hard_hit={existing.get('hard_hit_pct')}, barrel={existing.get('barrel_pct')}, avg_ev={existing.get('avg_ev')}")
        else:
            print('[Probable Starters] Savant custom leaderboard returned no headers')
    except Exception as e:
        print(f'[Probable Starters] Savant custom leaderboard fetch failed: {e}')

    print(f'[Probable Starters] Loaded Savant metrics for {len(rows)} pitchers')
    return rows


async def fetch_pitcher_season_stats(session, pitcher_id):
    current_year = datetime.now(ZoneInfo(TIMEZONE)).year
    url = f'https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats?stats=season&group=pitching&season={current_year}'
    data = await fetch_json(session, url, timeout=20)
    stats = data.get('stats', [])
    if not stats or not stats[0].get('splits'):
        return {}
    stat = stats[0]['splits'][0].get('stat', {}) or {}

    batters_faced = stat.get('battersFaced')
    strikeouts = stat.get('strikeOuts')
    walks = stat.get('baseOnBalls')
    ground_outs = stat.get('groundOuts')
    air_outs = stat.get('airOuts')

    out = {
        'batters_faced': batters_faced,
        'strike_outs': strikeouts,
        'walks': walks,
    }

    try:
        bf = float(batters_faced or 0)
        so = float(strikeouts or 0)
        if bf > 0:
            out['k_pct'] = round((so / bf) * 100, 1)
    except Exception:
        pass

    try:
        bf = float(batters_faced or 0)
        bb = float(walks or 0)
        if bf > 0:
            out['bb_pct'] = round((bb / bf) * 100, 1)
    except Exception:
        pass

    try:
        go = float(ground_outs or 0)
        ao = float(air_outs or 0)
        if (go + ao) > 0:
            out['gb_pct'] = round((go / (go + ao)) * 100, 1)
    except Exception:
        pass

    return out


async def build_recent_form(session, pitcher_id):
    game_log = await fetch_pitcher_game_log(session, pitcher_id)
    last_three = game_log[-3:] if len(game_log) >= 3 else game_log
    if not last_three:
        return {}

    ip_total = 0.0
    er_total = 0
    h_total = 0
    bb_total = 0
    k_total = 0
    pitches_total = 0
    opps = []
    dates = []

    for split in last_three:
        stat = split.get('stat', {})
        ip = innings_to_float(stat.get('inningsPitched', 0))
        ip_total += ip
        er_total += int(stat.get('earnedRuns', 0) or 0)
        h_total += int(stat.get('hits', 0) or 0)
        bb_total += int(stat.get('baseOnBalls', 0) or 0)
        k_total += int(stat.get('strikeOuts', 0) or 0)
        pitches_total += int(stat.get('numberOfPitches', 0) or 0)
        opp_name = (((split.get('opponent') or {}).get('name')) or '')
        dates.append(split.get('date', ''))
        for key, val in TEAM_NAME_TO_ABBR.items():
            if key in opp_name.lower():
                opp_name = val
                break
        opps.append(opp_name)

    era = (er_total * 9 / ip_total) if ip_total > 0 else None
    whip = ((h_total + bb_total) / ip_total) if ip_total > 0 else None
    k9 = (k_total * 9 / ip_total) if ip_total > 0 else None
    ppi = (pitches_total / ip_total) if ip_total > 0 else None

    return {
        'starts': len(last_three),
        'opponents': opps,
        'dates': dates,
        'ip_last3': round(ip_total, 1),
        'k_last3': k_total,
        'bb_last3': bb_total,
        'era_last3': round(era, 2) if era is not None else None,
        'whip_last3': round(whip, 2) if whip is not None else None,
        'k9_last3': round(k9, 1) if k9 is not None else None,
        'pitches_per_inning': round(ppi, 1) if ppi is not None else None,
        'pitch_count_avg': round(pitches_total / len(last_three), 1),
    }


async def build_hot_cold_hitters(session, opponent_id):
    hitters = await fetch_player_lastname_map(session, opponent_id)
    if not hitters:
        return {'hot': [], 'cold': []}

    async def load(h):
        form = await fetch_hitter_recent_form(session, h['id'])
        if not form:
            return None
        pa = int(form.get('plateAppearances', 0) or 0)
        if pa < 8:
            return None
        ops_val = None
        try:
            ops_val = float(form.get('ops', 0) or 0)
        except Exception:
            ops_val = 0.0
        return {
            'name': h['name'],
            'avg': form.get('avg'),
            'ops': form.get('ops'),
            'hr': int(form.get('homeRuns', 0) or 0),
            'k': int(form.get('strikeOuts', 0) or 0),
            'pa': pa,
            'ops_value': ops_val,
        }

    loaded = [x for x in await asyncio.gather(*(load(h) for h in hitters)) if x]
    hot = sorted(loaded, key=lambda x: x['ops_value'], reverse=True)[:3]
    cold = sorted(loaded, key=lambda x: x['ops_value'])[:3]
    return {'hot': hot, 'cold': cold}


async def enrich_starter(session, starter, ownership_map, savant_map):
    own = ownership_map.get(starter['pitcher_name'].lower())
    if not own:
        return None
    if own['ownership'] > MAX_OWNERSHIP:
        return None

    espn_id = own.get('espn_id')
    headshot_url = None
    if espn_id:
        headshot_url = f"https://a.espncdn.com/i/headshots/mlb/players/full/{espn_id}.png"

    season_stat, recent_offense = await fetch_team_recent_offense(session, starter['opponent_id'])
    recent_form = await build_recent_form(session, starter['pitcher_id'])
    season_pitching = await fetch_pitcher_season_stats(session, starter['pitcher_id'])
    hot_cold = await build_hot_cold_hitters(session, starter['opponent_id'])

    savant = dict(savant_map.get(starter['pitcher_id'], {}) or {})
    if season_pitching.get('k_pct') is not None:
        savant['k_pct'] = season_pitching.get('k_pct')
    if season_pitching.get('bb_pct') is not None:
        savant['bb_pct'] = season_pitching.get('bb_pct')
    if season_pitching.get('gb_pct') is not None and savant.get('gb_pct') in (None, '', '—'):
        savant['gb_pct'] = season_pitching.get('gb_pct')
    park_abbr = starter['team_abbr'] if starter['is_home'] else starter['opponent_abbr']
    park_factor = PARK_FACTORS.get(park_abbr, {'run': 100, 'hr': 100})

    enriched = {
        **starter,
        'ownership': own['ownership'],
        'ownership_change': own['change'],
        'espn_id': espn_id,
        'headshot_url': headshot_url,
        'team_color': TEAM_COLORS.get(starter['team_abbr'], 0x1D428A),
        'pitcher_metrics': savant,
        'pitcher_season_stats': season_pitching,
        'recent_form': recent_form,
        'opponent_metrics': {
            'avg': season_stat.get('avg'),
            'obp': season_stat.get('obp'),
            'slg': season_stat.get('slg'),
            'ops': season_stat.get('ops'),
            'home_runs': season_stat.get('homeRuns'),
            'strike_outs': season_stat.get('strikeOuts'),
            'plate_appearances': season_stat.get('plateAppearances'),
            'runs_per_game_last14': recent_offense.get('runs_per_game'),
        },
        'opponent_recent': recent_offense,
        'hot_hitters': hot_cold.get('hot', []),
        'cold_hitters': hot_cold.get('cold', []),
        'park_factor': park_factor,
        'park_label': classify_park(park_abbr),
    }

    pa = season_stat.get('plateAppearances', 0) or 0
    so = season_stat.get('strikeOuts', 0) or 0
    try:
        enriched['opponent_metrics']['k_pct_season'] = round((float(so) / float(pa)) * 100, 1) if float(pa) > 0 else None
    except Exception:
        enriched['opponent_metrics']['k_pct_season'] = None

    # Rough last-14 opponent quality proxy from runs/game
    rpg = recent_offense.get('runs_per_game')
    if rpg is not None:
        # convert simple recent scoring proxy into a rough descriptive layer and pseudo-wOBA band
        if rpg >= 5.3:
            enriched['opponent_metrics']['woba_last14'] = 0.340
            enriched['opponent_metrics']['k_pct_last14'] = 20.0
        elif rpg >= 4.7:
            enriched['opponent_metrics']['woba_last14'] = 0.325
            enriched['opponent_metrics']['k_pct_last14'] = 21.5
        elif rpg >= 4.1:
            enriched['opponent_metrics']['woba_last14'] = 0.312
            enriched['opponent_metrics']['k_pct_last14'] = 22.5
        elif rpg >= 3.5:
            enriched['opponent_metrics']['woba_last14'] = 0.302
            enriched['opponent_metrics']['k_pct_last14'] = 23.5
        else:
            enriched['opponent_metrics']['woba_last14'] = 0.292
            enriched['opponent_metrics']['k_pct_last14'] = 24.5

    enriched['start_score'] = build_start_score(enriched)
    enriched['start_tier'] = start_tier(enriched['start_score'])
    return enriched


async def generate_summaries(starters):
    if not starters:
        return {}

    if not anthropic_client:
        print('[Probable Starters] No Claude key found; using fallback summaries')
        return {s['pitcher_name']: fallback_summary(s) for s in starters}

    prompt_rows = []
    for s in starters:
        prompt_rows.append({
            'pitcher': s['pitcher_name'],
            'team': s['team_abbr'],
            'opp': s['opponent_abbr'],
            'home': s['is_home'],
            'owned': s['ownership'],
            'score': s['start_score'],
            'tier': s['start_tier'],
            'recent_form': s['recent_form'],
            'pitcher_metrics': {
                'xERA': fmt_num(s['pitcher_metrics'].get('xera')),
                'K%': fmt_pct(s['pitcher_metrics'].get('k_pct')),
                'BB%': fmt_pct(s['pitcher_metrics'].get('bb_pct')),
                'HardHit%': fmt_pct(s['pitcher_metrics'].get('hard_hit_pct')),
                'Barrel%': fmt_pct(s['pitcher_metrics'].get('barrel_pct')),
                'xBA': fmt_avg(s['pitcher_metrics'].get('xba')),
                'xSLG': fmt_avg(s['pitcher_metrics'].get('xslg')),
            },
            'opp_recent': s['opponent_recent'],
            'opp_metrics': {
                'AVG': s['opponent_metrics'].get('avg'),
                'OBP': s['opponent_metrics'].get('obp'),
                'SLG': s['opponent_metrics'].get('slg'),
                'OPS': s['opponent_metrics'].get('ops'),
                'K%': s['opponent_metrics'].get('k_pct_season'),
                'RPG_last14': s['opponent_metrics'].get('runs_per_game_last14'),
            },
            'hot_hitters': [{'name': h['name'], 'ops': h['ops'], 'avg': h['avg'], 'hr': h['hr']} for h in s['hot_hitters']],
            'cold_hitters': [{'name': h['name'], 'ops': h['ops'], 'avg': h['avg'], 'hr': h['hr']} for h in s['cold_hitters']],
            'park': {'label': s['park_label'], 'run': s['park_factor']['run'], 'hr': s['park_factor']['hr']},
        })

    prompt = f"""You are writing concise fantasy baseball probable starter blurbs for a Discord bot.

Write one sharp paragraph for each starter below. Focus on whether the pitcher is usable as a stream today. Mention the matchup, recent form, strikeout path, risk level, and one or two specific supporting stats. Keep each blurb to 3 sentences max. Avoid rigid repetitive phrasing.

Do not mention roster percentage or ownership unless availability is truly part of the story. Most blurbs should not mention ownership at all.

Vary the structure from pitcher to pitcher. Some blurbs should open with the matchup, some with recent form, some with strikeout upside, some with park context, and some with risk factors. Avoid repetitive wording like "lines up as" or "carries a score into the matchup."

Return ONLY valid JSON in this shape:
{{
  \"summaries\": [
    {{\"pitcher\": \"Name\", \"summary\": \"text\"}}
  ]
}}

Starters:
{json.dumps(prompt_rows, indent=2)}
"""

    try:
        message = anthropic_client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=2500,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = message.content[0].text.strip()
        text = re.sub(r'^```json\s*|```$', '', text, flags=re.MULTILINE).strip()
        data = json.loads(text)
        out = {}
        for row in data.get('summaries', []):
            name = row.get('pitcher')
            summary = row.get('summary', '')
            if name and summary:
                out[name] = summary
        for s in starters:
            out.setdefault(s['pitcher_name'], fallback_summary(s))
        return out
    except Exception as e:
        print(f'[Probable Starters] Claude summary generation failed: {e}')
        return {s['pitcher_name']: fallback_summary(s) for s in starters}


def fallback_summary(starter):
    recent = starter.get('recent_form', {})
    opp = starter.get('opponent_metrics', {})
    pitcher = starter.get('pitcher_metrics', {})
    team = starter.get('team_abbr')
    opp_team = starter.get('opponent_abbr')
    score = starter.get('start_score')
    tier = starter.get('start_tier')
    xera = fmt_num(pitcher.get('xera'))
    k_pct = fmt_pct(pitcher.get('k_pct'))
    era3 = recent.get('era_last3', '—')
    k9 = recent.get('k9_last3', '—')
    rpg = opp.get('runs_per_game_last14')
    park_label = starter.get('park_label', 'neutral')
    return (
        f"{starter['pitcher_name']} lines up as a {tier} against {opp_team}, carrying a {score}/100 stream score into the matchup. "
        f"The recent form has been {'steady' if isinstance(era3, (int, float)) and era3 <= 4 else 'shaky'}, with a {era3} ERA and {k9} K/9 across his last three starts, while the underlying profile shows a {xera} xERA and {k_pct} strikeout rate. "
        f"{opp_team} has scored {rpg if rpg is not None else '—'} runs per game over the last two weeks, and the park plays {park_label}, so this is more {'usable' if score >= 68 else 'speculative'} than automatic."
    )


def build_header_embed(starters, target_date):
    embed = discord.Embed(
        title='⚾ Probable Starters',
        description=(
            f"Streaming board for **{target_date}**. "
            f"Only starters at **{MAX_OWNERSHIP:.0f}% ESPN rostered or lower** are included."
        ),
        color=0x1D428A,
        timestamp=datetime.now(ZoneInfo('UTC')),
    )
    embed.add_field(
        name='Slate snapshot',
        value=(
            f"**Qualified arms:** {len(starters)}\n"
            f"**Top score on board:** {max((s['start_score'] for s in starters), default='—')}\n"
            f"**Strong stream tier:** {sum(1 for s in starters if s['start_score'] >= 76)}"
        ),
        inline=False,
    )
    embed.add_field(
        name='General interpretation',
        value=(
            '**76+** = elite / strong stream.\n'
            '**68-75** = solid stream.\n'
            '**60-67** = usable but risky.\n'
            '**Below 60** = more speculative / desperation only.'
        ),
        inline=False,
    )
    embed.set_footer(text='Probable starters bot • MLB + ESPN + Savant blend.')
    return embed


def build_starter_embed(starter, summary):
    team = starter['team_abbr']
    opp = starter['opponent_abbr']
    recent = starter.get('recent_form', {})
    metrics = starter.get('pitcher_metrics', {})
    opp_metrics = starter.get('opponent_metrics', {})
    hot = starter.get('hot_hitters', [])
    cold = starter.get('cold_hitters', [])
    at_vs = 'vs' if starter['is_home'] else '@'
    color = starter.get('team_color') or (0x2ECC71 if starter['start_score'] >= 76 else 0xF1C40F if starter['start_score'] >= 60 else 0xE74C3C)
    tier_emoji = dart_rating(starter.get('start_score'))

    embed = discord.Embed(
        description=f"**{team}** | **ESPN:** **{starter['ownership']:.1f}% owned** | **{starter['start_score']}/100** {tier_emoji}",
        color=color,
        timestamp=datetime.now(ZoneInfo('UTC')),
    )

    logo_url = team_logo_url(team)
    embed.set_author(name=f"{starter['pitcher_name']} {at_vs} {opp}", icon_url=logo_url or discord.Embed.Empty)

    headshot_url = starter.get('headshot_url')
    if headshot_url:
        embed.set_thumbnail(url=headshot_url)

    print(f"[Probable Starters] Metrics for {starter['pitcher_name']}: {metrics}")

    embed.add_field(
        name='Recent form',
        value=(
            f"**Last 3:** {recent.get('ip_last3', '—')} IP, {recent.get('k_last3', '—')} K, {recent.get('bb_last3', '—')} BB\n"
            f"**ERA/WHIP:** {recent.get('era_last3', '—')} / {recent.get('whip_last3', '—')}\n"
            f"**K/9:** {recent.get('k9_last3', '—')} | **Pitch avg:** {recent.get('pitch_count_avg', '—')}"
        ),
        inline=False,
    )

    metric_lines = [
        f"**xERA:** {fmt_num(metrics.get('xera'))} | **K%:** {fmt_pct(metrics.get('k_pct'))} | **BB%:** {fmt_pct(metrics.get('bb_pct'))}",
        f"**xBA/xSLG allowed:** {fmt_avg(metrics.get('xba'))} / {fmt_avg(metrics.get('xslg'))}",
    ]
    contact_bits = []
    if metrics.get('hard_hit_pct') not in (None, '', '—'):
        contact_bits.append(f"**HardHit%:** {fmt_pct(metrics.get('hard_hit_pct'))}")
    if metrics.get('barrel_pct') not in (None, '', '—'):
        contact_bits.append(f"**Barrel%:** {fmt_pct(metrics.get('barrel_pct'))}")
    if metrics.get('avg_ev') not in (None, '', '—'):
        contact_bits.append(f"**Avg EV:** {fmt_num(metrics.get('avg_ev'))}")
    if metrics.get('gb_pct') not in (None, '', '—'):
        contact_bits.append(f"**GB%:** {fmt_pct(metrics.get('gb_pct'))}")
    if contact_bits:
        metric_lines.append(' | '.join(contact_bits))

    embed.add_field(
        name='Underlying metrics',
        value='\n'.join(metric_lines),
        inline=False,
    )

    embed.add_field(
        name=f'Opponent: {opp}',
        value=(
            f"**Season line:** {opp_metrics.get('avg', '—')} AVG / {opp_metrics.get('obp', '—')} OBP / {opp_metrics.get('slg', '—')} SLG\n"
            f"**Season K%:** {opp_metrics.get('k_pct_season', '—')} | **Last 14 RPG:** {opp_metrics.get('runs_per_game_last14', '—')}\n"
            f"**Park:** {starter['park_label']} (Run {starter['park_factor']['run']}, HR {starter['park_factor']['hr']})"
        ),
        inline=False,
    )

    hot_names = ' | '.join(h['name'] for h in hot[:3]) or '—'
    cold_names = ' | '.join(h['name'] for h in cold[:3]) or '—'
    embed.add_field(name='Who’s hot', value=safe_truncate(hot_names, 256), inline=False)
    embed.add_field(name='Who’s cold', value=safe_truncate(cold_names, 256), inline=False)
    embed.add_field(name='Summary', value=safe_truncate(summary, 1024), inline=False)

    game_time = starter.get('game_time')
    if game_time:
        try:
            dt = datetime.fromisoformat(game_time.replace('Z', '+00:00')).astimezone(ZoneInfo(TIMEZONE))
            time_text = dt.strftime('%I:%M %p ET').lstrip('0')
        except Exception:
            time_text = game_time
    else:
        time_text = 'TBD'

    embed.set_footer(text=f"{team} {at_vs} {opp} • {time_text} • {starter.get('venue_name', '')}")
    return embed


async def post_probable_starters_report():
    now_et = datetime.now(ZoneInfo(TIMEZONE))
    today = now_et.strftime('%Y-%m-%d')
    print(f'\n[Probable Starters] Starting report for {today} at {now_et}')

    if not DISCORD_TOKEN or not CHANNEL_ID:
        print('[Probable Starters] Missing ANALYTIC_BOT_TOKEN or STREAMING_CHANNEL_ID')
        return

    try:
        state = load_state()

        async with aiohttp.ClientSession() as session:
            ownership_map, probable, savant_map = await asyncio.gather(
                fetch_espn_pitcher_ownership(session),
                fetch_probable_starters(session, today),
                fetch_savant_pitcher_metrics(session),
            )

            enriched = [
                x for x in await asyncio.gather(*(enrich_starter(session, s, ownership_map, savant_map) for s in probable)) if x
            ]

        if not enriched:
            print('[Probable Starters] No qualified probable starters found')
            return

        enriched = [x for x in enriched if (x.get('start_score') or 0) >= MIN_START_SCORE_TO_POST]
        enriched.sort(key=lambda x: (x['start_score'], -x['ownership']), reverse=True)
        selected = enriched[:MAX_STARTERS_PER_RUN]

        summaries = await generate_summaries(selected)

        channel = client.get_channel(CHANNEL_ID)
        if channel is None:
            channel = await client.fetch_channel(CHANNEL_ID)

        await channel.send(embed=build_header_embed(selected, today))
        for starter in selected:
            await channel.send(embed=build_starter_embed(starter, summaries.get(starter['pitcher_name'], fallback_summary(starter))))
            await asyncio.sleep(1.0)

        state['last_post_date'] = today
        state['posted_gamepks'] = list({s['game_pk'] for s in selected})
        save_state(state)
        print(f"[Probable Starters] Posted {len(selected)} starter embeds")

    except Exception as e:
        print(f'[Probable Starters] Error in report: {e}')
        import traceback
        traceback.print_exc()


@client.event
async def on_ready():
    print(f'[Probable Starters] Logged in as {client.user}')

    if os.getenv('PROBABLE_STARTERS_ENABLED', 'true').lower() == 'false':
        print('[Probable Starters] DISABLED via PROBABLE_STARTERS_ENABLED=false')
        return

    if os.getenv('PROBABLE_STARTERS_TEST_MODE', 'false').lower() == 'true':
        print('[Probable Starters] TEST MODE: running report immediately')
        await post_probable_starters_report()

    if not scheduler.get_job('probable_starters_daily_report'):
        scheduler.add_job(
            post_probable_starters_report,
            'cron',
            hour=SCHED_HOUR,
            minute=SCHED_MINUTE,
            timezone=TIMEZONE,
            id='probable_starters_daily_report',
        )
        print(f'[Probable Starters] Scheduled daily post for {SCHED_HOUR}:{SCHED_MINUTE:02d} ET')

    if not scheduler.running:
        scheduler.start()


async def start_probable_starters_bot():
    await client.start(DISCORD_TOKEN)


if __name__ == '__main__':
    asyncio.run(start_probable_starters_bot())

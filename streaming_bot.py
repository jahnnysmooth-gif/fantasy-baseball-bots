"""
Streaming Bot v2.0 - Complete Fantasy Baseball Streamer Scout
Full-featured day-one deployment with Statcast, weather, lineup analysis, and AI summaries
"""

import discord
from discord.ext import commands, tasks
import os
import aiohttp
import asyncio
from datetime import datetime, timedelta
import pytz
import json
import math
from anthropic import AsyncAnthropic
import statsapi
from pybaseball import statcast_pitcher, pitching_stats
import pandas as pd
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Environment variables
DISCORD_TOKEN = os.getenv('STREAMING_BOT_TOKEN')
STREAMING_CHANNEL_ID = int(os.getenv('STREAMING_CHANNEL_ID', '0'))
ANTHROPIC_API_KEY = os.getenv('STREAMING_BOT_SUMMARY')
ESPN_PLAYER_IDS_PATH = os.getenv('ESPN_PLAYER_IDS_PATH', '/home/claude/espn_player_ids.json')
OWNERSHIP_THRESHOLD = float(os.getenv('OWNERSHIP_THRESHOLD', '60.0'))

# Initialize Anthropic client
anthropic = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# Shared aiohttp session
http_session = None

# ESPN player ID mapping
espn_player_map = {}

# Statcast cache (daily refresh)
statcast_cache = {}
statcast_cache_date = None

# Park factors with coordinates
PARK_DATA = {
    'Yankee Stadium': {'lat': 40.8296, 'lng': -73.9262, 'runs': 105, 'hr': 109, 'type': 'hitter-friendly'},
    'Fenway Park': {'lat': 42.3467, 'lng': -71.0972, 'runs': 104, 'hr': 102, 'type': 'slightly hitter-friendly'},
    'Coors Field': {'lat': 39.7559, 'lng': -104.9942, 'runs': 115, 'hr': 110, 'type': 'extreme hitter park'},
    'Oracle Park': {'lat': 37.7786, 'lng': -122.3893, 'runs': 93, 'hr': 88, 'type': 'extreme pitcher park'},
    'Dodger Stadium': {'lat': 34.0739, 'lng': -118.2400, 'runs': 95, 'hr': 94, 'type': 'pitcher-friendly'},
    'Wrigley Field': {'lat': 41.9484, 'lng': -87.6553, 'runs': 101, 'hr': 100, 'type': 'neutral'},
    'Great American Ball Park': {'lat': 39.0974, 'lng': -84.5067, 'runs': 108, 'hr': 112, 'type': 'hitter-friendly'},
    'Citizens Bank Park': {'lat': 39.9061, 'lng': -75.1665, 'runs': 103, 'hr': 107, 'type': 'slightly hitter-friendly'},
    'Camden Yards': {'lat': 39.2839, 'lng': -76.6218, 'runs': 103, 'hr': 106, 'type': 'slightly hitter-friendly'},
    'Truist Park': {'lat': 33.8907, 'lng': -84.4677, 'runs': 100, 'hr': 99, 'type': 'neutral'},
    'Petco Park': {'lat': 32.7073, 'lng': -117.1566, 'runs': 95, 'hr': 92, 'type': 'pitcher-friendly'},
    'T-Mobile Park': {'lat': 47.5914, 'lng': -122.3325, 'runs': 95, 'hr': 93, 'type': 'pitcher-friendly'},
    'Minute Maid Park': {'lat': 29.7573, 'lng': -95.3555, 'runs': 102, 'hr': 103, 'type': 'slightly hitter-friendly'},
    'Progressive Field': {'lat': 41.4962, 'lng': -81.6852, 'runs': 98, 'hr': 99, 'type': 'slightly pitcher-friendly'},
    'Guaranteed Rate Field': {'lat': 41.8299, 'lng': -87.6338, 'runs': 98, 'hr': 100, 'type': 'slightly pitcher-friendly'},
    'Busch Stadium': {'lat': 38.6226, 'lng': -90.1928, 'runs': 97, 'hr': 95, 'type': 'pitcher-friendly'},
    'Kauffman Stadium': {'lat': 39.0517, 'lng': -94.4803, 'runs': 96, 'hr': 94, 'type': 'pitcher-friendly'},
    'Comerica Park': {'lat': 42.3391, 'lng': -83.0485, 'runs': 96, 'hr': 93, 'type': 'pitcher-friendly'},
    'PNC Park': {'lat': 40.4469, 'lng': -80.0057, 'runs': 96, 'hr': 95, 'type': 'pitcher-friendly'},
    'Angel Stadium': {'lat': 33.8003, 'lng': -117.8827, 'runs': 97, 'hr': 96, 'type': 'pitcher-friendly'},
    'Oakland Coliseum': {'lat': 37.7516, 'lng': -122.2005, 'runs': 94, 'hr': 91, 'type': 'pitcher-friendly'},
    'Tropicana Field': {'lat': 27.7682, 'lng': -82.6534, 'runs': 96, 'hr': 95, 'type': 'pitcher-friendly'},
    'loanDepot park': {'lat': 25.7781, 'lng': -80.2197, 'runs': 94, 'hr': 90, 'type': 'pitcher-friendly'},
    'Chase Field': {'lat': 33.4453, 'lng': -112.0667, 'runs': 102, 'hr': 104, 'type': 'slightly hitter-friendly'},
    'Target Field': {'lat': 44.9817, 'lng': -93.2776, 'runs': 98, 'hr': 97, 'type': 'slightly pitcher-friendly'},
    'Globe Life Field': {'lat': 32.7473, 'lng': -97.0833, 'runs': 100, 'hr': 101, 'type': 'neutral'},
    'Rogers Centre': {'lat': 43.6414, 'lng': -79.3894, 'runs': 102, 'hr': 105, 'type': 'slightly hitter-friendly'},
    'Nationals Park': {'lat': 38.8730, 'lng': -77.0074, 'runs': 99, 'hr': 98, 'type': 'neutral'},
    'American Family Field': {'lat': 43.0280, 'lng': -87.9712, 'runs': 99, 'hr': 100, 'type': 'neutral'},
}


def load_espn_player_ids():
    """Load ESPN player ID mapping"""
    global espn_player_map
    try:
        if os.path.exists(ESPN_PLAYER_IDS_PATH):
            with open(ESPN_PLAYER_IDS_PATH, 'r') as f:
                espn_player_map = json.load(f)
            print(f"Loaded {len(espn_player_map)} ESPN player IDs")
    except Exception as e:
        print(f"Error loading ESPN player IDs: {e}")


async def refresh_statcast_cache():
    """Refresh Statcast data cache daily"""
    global statcast_cache, statcast_cache_date
    
    try:
        today = datetime.now(pytz.timezone('America/New_York')).date()
        
        if statcast_cache_date == today and statcast_cache:
            return  # Already cached today
        
        print("Refreshing Statcast cache...")
        
        # Get season pitching stats with Statcast metrics
        season_stats = pitching_stats(2026, qual=1)
        
        if season_stats is not None and not season_stats.empty:
            # Cache by player name
            for _, row in season_stats.iterrows():
                pitcher_name = row.get('Name', '').strip()
                if pitcher_name:
                    statcast_cache[pitcher_name] = {
                        'xera': row.get('xERA', 0),
                        'fip': row.get('FIP', 0),
                        'xfip': row.get('xFIP', 0),
                        'k_pct': row.get('K%', 0),
                        'bb_pct': row.get('BB%', 0),
                        'swstr_pct': row.get('SwStr%', 0),
                        'csw_pct': row.get('CSW%', 0),
                        'hard_hit_pct': row.get('HardHit%', 0),
                        'barrel_pct': row.get('Barrel%', 0),
                        'avg_ev': row.get('avgEV', 0),
                        'whiff_pct': row.get('Whiff%', 0)
                    }
            
            statcast_cache_date = today
            print(f"Cached Statcast data for {len(statcast_cache)} pitchers")
        
    except Exception as e:
        print(f"Error refreshing Statcast cache: {e}")


async def get_statcast_metrics(pitcher_name):
    """Get Statcast metrics from cache"""
    return statcast_cache.get(pitcher_name, {})


async def get_espn_ownership(player_name, mlb_id):
    """Get ESPN ownership percentage"""
    try:
        # Normalize name (remove accents for matching)
        import unicodedata
        
        def normalize_name(name):
            # Remove accents
            name = unicodedata.normalize('NFD', name)
            name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
            return name.strip()
        
        normalized_search = normalize_name(player_name)
        
        # Try exact match first
        player_data = espn_player_map.get(player_name)
        
        if not player_data:
            # Try normalized match
            for key, data in espn_player_map.items():
                if normalize_name(key) == normalized_search:
                    player_data = data
                    print(f"[STREAMING] 🔄 Matched '{player_name}' to '{key}'")
                    break
        
        if not player_data:
            print(f"[STREAMING] ⚠️  No ESPN ID for {player_name} (MLB ID: {mlb_id})")
            return None
        
        espn_id = player_data.get('espn_id')
        if not espn_id:
            print(f"[STREAMING] ⚠️  No espn_id field for {player_name}")
            return None
        
        url = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/2026/players'
        params = {'scoringPeriodId': 0, 'view': 'kona_player_info'}
        
        async with http_session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                for player in data:
                    if player.get('id') == int(espn_id):
                        ownership = round(player.get('ownership', {}).get('percentOwned', 0), 1)
                        print(f"[STREAMING] ✅ {player_name}: {ownership}% owned")
                        return ownership
                
                print(f"[STREAMING] ⚠️  ESPN ID {espn_id} not in API response for {player_name}")
                return None
            else:
                print(f"[STREAMING] ❌ ESPN API status {resp.status}")
                return None
        
        return None
        
    except Exception as e:
        print(f"[STREAMING] ❌ Ownership error for {player_name}: {e}")
        return None


async def get_weather(lat, lng, game_datetime):
    """Get weather forecast from Open-Meteo"""
    try:
        # Parse game datetime
        if isinstance(game_datetime, str):
            game_dt = datetime.fromisoformat(game_datetime.replace('Z', '+00:00'))
        else:
            game_dt = game_datetime
        
        # Format for API
        date_str = game_dt.strftime('%Y-%m-%d')
        hour = game_dt.hour
        
        url = 'https://api.open-meteo.com/v1/forecast'
        params = {
            'latitude': lat,
            'longitude': lng,
            'hourly': 'temperature_2m,windspeed_10m,winddirection_10m,precipitation_probability',
            'start_date': date_str,
            'end_date': date_str,
            'temperature_unit': 'fahrenheit',
            'windspeed_unit': 'mph',
            'timezone': 'America/New_York'
        }
        
        async with http_session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                hourly = data.get('hourly', {})
                
                # Get closest hour
                temp = hourly['temperature_2m'][hour] if hour < len(hourly['temperature_2m']) else hourly['temperature_2m'][0]
                wind_speed = hourly['windspeed_10m'][hour] if hour < len(hourly['windspeed_10m']) else hourly['windspeed_10m'][0]
                wind_dir = hourly['winddirection_10m'][hour] if hour < len(hourly['winddirection_10m']) else hourly['winddirection_10m'][0]
                rain_prob = hourly['precipitation_probability'][hour] if hour < len(hourly['precipitation_probability']) else 0
                
                return {
                    'temp_f': round(temp, 1),
                    'wind_speed': round(wind_speed, 1),
                    'wind_direction': wind_dir,
                    'rain_prob': rain_prob,
                    'wind_desc': get_wind_description(wind_dir, wind_speed)
                }
        
        return None
        
    except Exception as e:
        print(f"Error fetching weather: {e}")
        return None


def get_wind_description(direction, speed):
    """Convert wind direction to description"""
    if speed < 5:
        return 'calm'
    
    # Convert degrees to direction
    dirs = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 
            'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
    idx = round(direction / 22.5) % 16
    
    # Interpret for baseball context
    if dirs[idx] in ['S', 'SSW', 'SW']:
        return f'{speed} mph out to CF/LF (hitter boost)'
    elif dirs[idx] in ['N', 'NNW', 'NW']:
        return f'{speed} mph in from CF (pitcher boost)'
    else:
        return f'{speed} mph {dirs[idx].lower()}'


async def get_probable_starters(date_str=None):
    """Get probable starters"""
    try:
        if date_str is None:
            et_tz = pytz.timezone('America/New_York')
            date_str = datetime.now(et_tz).strftime('%Y-%m-%d')
        
        schedule = statsapi.schedule(date=date_str)
        probable_starters = []
        
        for game in schedule:
            game_pk = game.get('game_id')
            
            try:
                game_data = statsapi.get('game', {'gamePk': game_pk})
                
                away_probable = game_data.get('gameData', {}).get('probablePitchers', {}).get('away')
                home_probable = game_data.get('gameData', {}).get('probablePitchers', {}).get('home')
                venue_name = game_data.get('gameData', {}).get('venue', {}).get('name', 'Unknown')
                
                for pitcher_data, team_type in [(away_probable, 'away'), (home_probable, 'home')]:
                    if pitcher_data:
                        pitcher_id = pitcher_data.get('id')
                        pitcher_name = pitcher_data.get('fullName')
                        
                        # Get pitcher details for handedness
                        person_data = statsapi.get('person', {'personId': pitcher_id})
                        hand_code = person_data.get('people', [{}])[0].get('pitchHand', {}).get('code', 'R')
                        pitcher_hand = 'LHP' if hand_code == 'L' else 'RHP'
                        
                        if team_type == 'away':
                            team = game.get('away_name')
                            opponent = game.get('home_name')
                            opponent_id = game_data.get('gameData', {}).get('teams', {}).get('home', {}).get('id')
                        else:
                            team = game.get('home_name')
                            opponent = game.get('away_name')
                            opponent_id = game_data.get('gameData', {}).get('teams', {}).get('away', {}).get('id')
                        
                        probable_starters.append({
                            'pitcher_id': pitcher_id,
                            'pitcher_name': pitcher_name,
                            'pitcher_hand': pitcher_hand,
                            'team': team,
                            'opponent': opponent,
                            'opponent_id': opponent_id,
                            'venue': venue_name,
                            'game_time': game.get('game_datetime'),
                            'is_home': team_type == 'home',
                            'game_pk': game_pk
                        })
                        
            except Exception as e:
                print(f"Error fetching game {game_pk}: {e}")
                continue
        
        return probable_starters
        
    except Exception as e:
        print(f"Error fetching probable starters: {e}")
        return []


async def get_pitcher_stats(pitcher_id, pitcher_name):
    """Get comprehensive pitcher statistics"""
    try:
        season_stats = statsapi.player_stat_data(pitcher_id, group='pitching', type='season')
        game_log = statsapi.player_stat_data(pitcher_id, group='pitching', type='gameLog')
        splits_data = statsapi.player_stat_data(pitcher_id, group='pitching', type='homeAndAway')
        
        stats = {
            'era': 0.0,
            'whip': 0.0,
            'k_per_9': 0.0,
            'bb_per_9': 0.0,
            'k_pct': 0.0,
            'bb_pct': 0.0,
            'k_bb_pct': 0.0,
            'ip': 0.0,
            'games': 0,
            'recent_starts': [],
            'home_era': 0.0,
            'away_era': 0.0,
            'ip_per_game': 0.0
        }
        
        # Season stats
        if season_stats and 'stats' in season_stats:
            for stat_group in season_stats['stats']:
                splits = stat_group.get('splits', [])
                if splits:
                    season_split = splits[0].get('stat', {})
                    
                    stats['era'] = float(season_split.get('era', 0))
                    stats['whip'] = float(season_split.get('whip', 0))
                    stats['k_per_9'] = float(season_split.get('strikeoutsPerNine', 0))
                    stats['bb_per_9'] = float(season_split.get('walksPerNine', 0))
                    stats['ip'] = float(season_split.get('inningsPitched', 0))
                    stats['games'] = int(season_split.get('gamesPlayed', 0))
                    
                    if stats['games'] > 0:
                        stats['ip_per_game'] = stats['ip'] / stats['games']
                    
                    bf = int(season_split.get('battersFaced', 0))
                    if bf > 0:
                        k = int(season_split.get('strikeOuts', 0))
                        bb = int(season_split.get('baseOnBalls', 0))
                        stats['k_pct'] = (k / bf) * 100
                        stats['bb_pct'] = (bb / bf) * 100
                        stats['k_bb_pct'] = stats['k_pct'] - stats['bb_pct']
        
        # Recent game logs
        if game_log and 'stats' in game_log:
            for stat_group in game_log['stats']:
                splits = stat_group.get('splits', [])[:3]
                for game in splits:
                    game_stat = game.get('stat', {})
                    stats['recent_starts'].append({
                        'date': game.get('date'),
                        'opponent': game.get('opponent', {}).get('name', 'Unknown'),
                        'ip': game_stat.get('inningsPitched', '0.0'),
                        'k': game_stat.get('strikeOuts', 0),
                        'bb': game_stat.get('baseOnBalls', 0),
                        'er': game_stat.get('earnedRuns', 0),
                        'h': game_stat.get('hits', 0),
                        'hr': game_stat.get('homeRuns', 0)
                    })
        
        # Home/away splits
        if splits_data and 'stats' in splits_data:
            for stat_group in splits_data['stats']:
                for split in stat_group.get('splits', []):
                    split_type = split.get('split', {}).get('code')
                    split_stat = split.get('stat', {})
                    if split_type == 'h':
                        stats['home_era'] = float(split_stat.get('era', 0))
                    elif split_type == 'a':
                        stats['away_era'] = float(split_stat.get('era', 0))
        
        return stats
        
    except Exception as e:
        print(f"Error fetching pitcher stats for {pitcher_name}: {e}")
        return None


async def get_projected_lineup(game_pk, opponent_id):
    """Get projected lineup for opponent"""
    try:
        game_data = statsapi.get('game', {'gamePk': game_pk})
        
        # Try to get actual lineup from boxscore
        boxscore = game_data.get('liveData', {}).get('boxscore', {})
        
        # Determine which team (away or home)
        away_id = game_data.get('gameData', {}).get('teams', {}).get('away', {}).get('id')
        
        team_key = 'away' if away_id == opponent_id else 'home'
        batting_order = boxscore.get('teams', {}).get(team_key, {}).get('battingOrder', [])
        
        lineup_hitters = []
        
        for player_id in batting_order[:9]:  # Top 9 hitters
            try:
                # Get hitter season stats
                hitter_stats = statsapi.player_stat_data(player_id, group='hitting', type='season')
                
                if hitter_stats and 'stats' in hitter_stats:
                    for stat_group in hitter_stats['stats']:
                        splits = stat_group.get('splits', [])
                        if splits:
                            stat = splits[0].get('stat', {})
                            
                            lineup_hitters.append({
                                'player_id': player_id,
                                'avg': float(stat.get('avg', 0)),
                                'ops': float(stat.get('ops', 0)),
                                'hr': int(stat.get('homeRuns', 0)),
                                'k_pct': (int(stat.get('strikeOuts', 0)) / max(int(stat.get('atBats', 1)), 1)) * 100
                            })
            except:
                continue
        
        return lineup_hitters
        
    except Exception as e:
        print(f"Error fetching projected lineup: {e}")
        return []


async def get_team_vs_handedness(team_id, hand):
    """Get team offensive stats vs LHP/RHP"""
    try:
        # Use MLB Stats API team stats with splits
        team_stats = statsapi.get('team_stats', {
            'teamId': team_id,
            'stats': 'season',
            'group': 'hitting'
        })
        
        # Parse splits vs LHP/RHP if available
        # For now, return league average estimates
        stats = {
            'wrc_plus': 100,
            'ops': 0.700,
            'iso': 0.150,
            'k_pct': 22.0,
            'bb_pct': 8.5,
            'avg': 0.245
        }
        
        return stats
        
    except Exception as e:
        return {
            'wrc_plus': 100,
            'ops': 0.700,
            'iso': 0.150,
            'k_pct': 22.0,
            'bb_pct': 8.5,
            'avg': 0.245
        }


async def calculate_start_score(pitcher_data, pitcher_stats, statcast_metrics, opponent_stats, lineup, park_data, weather):
    """Calculate comprehensive start score"""
    
    score = 0.0
    breakdown = {}
    
    # 1. Pitcher Skill Bucket (30 points) - now using Statcast metrics
    skill_score = 0.0
    
    # xERA primary (0-10 points)
    xera = statcast_metrics.get('xera', pitcher_stats.get('era', 5.00))
    if xera <= 2.50:
        skill_score += 10
    elif xera <= 3.50:
        skill_score += 8
    elif xera <= 4.00:
        skill_score += 6
    elif xera <= 4.50:
        skill_score += 4
    else:
        skill_score += max(0, 10 - (xera - 2.5) * 2)
    
    # K-BB% (0-8 points)
    k_bb_pct = statcast_metrics.get('k_pct', pitcher_stats.get('k_pct', 0)) - statcast_metrics.get('bb_pct', pitcher_stats.get('bb_pct', 0))
    if k_bb_pct >= 20:
        skill_score += 8
    elif k_bb_pct >= 15:
        skill_score += 6
    elif k_bb_pct >= 10:
        skill_score += 4
    else:
        skill_score += max(0, k_bb_pct / 5)
    
    # SwStr% + CSW% (0-7 points)
    swstr = statcast_metrics.get('swstr_pct', 0)
    csw = statcast_metrics.get('csw_pct', 0)
    
    if swstr >= 13 or csw >= 30:
        skill_score += 7
    elif swstr >= 11 or csw >= 28:
        skill_score += 5
    elif swstr >= 9 or csw >= 26:
        skill_score += 3
    else:
        skill_score += max(0, swstr / 2)
    
    # Hard-hit% + Barrel% (0-5 points)
    hard_hit = statcast_metrics.get('hard_hit_pct', 35)
    barrel = statcast_metrics.get('barrel_pct', 8)
    
    if hard_hit <= 30 and barrel <= 5:
        skill_score += 5
    elif hard_hit <= 35 and barrel <= 7:
        skill_score += 3
    elif hard_hit <= 40 and barrel <= 9:
        skill_score += 1
    
    breakdown['skill'] = round(skill_score, 1)
    score += skill_score
    
    # 2. Recent Form Bucket (20 points)
    form_score = 0.0
    recent_starts = pitcher_stats.get('recent_starts', [])
    
    if recent_starts:
        total_ip = sum(float(start.get('ip', 0)) for start in recent_starts)
        total_k = sum(int(start.get('k', 0)) for start in recent_starts)
        total_er = sum(int(start.get('er', 0)) for start in recent_starts)
        total_bb = sum(int(start.get('bb', 0)) for start in recent_starts)
        
        if total_ip > 0:
            recent_era = (total_er * 9) / total_ip
            recent_k_per_9 = (total_k * 9) / total_ip
            recent_bb_per_9 = (total_bb * 9) / total_ip
            
            # Recent ERA (0-10)
            if recent_era <= 2.50:
                form_score += 10
            elif recent_era <= 3.50:
                form_score += 8
            elif recent_era <= 4.50:
                form_score += 6
            else:
                form_score += max(0, 10 - (recent_era - 2.5) * 2)
            
            # K rate (0-5)
            if recent_k_per_9 >= 10:
                form_score += 5
            elif recent_k_per_9 >= 8:
                form_score += 4
            else:
                form_score += max(0, recent_k_per_9 / 2.5)
            
            # Walk control (0-5)
            if recent_bb_per_9 <= 2.0:
                form_score += 5
            elif recent_bb_per_9 <= 3.0:
                form_score += 3
            else:
                form_score += max(0, 5 - recent_bb_per_9)
    else:
        form_score = 10
    
    breakdown['form'] = round(form_score, 1)
    score += form_score
    
    # 3. Opponent Matchup Bucket (25 points) - expanded with lineup analysis
    matchup_score = 0.0
    
    # Team stats vs handedness (0-10)
    wrc_plus = opponent_stats.get('wrc_plus', 100)
    if wrc_plus <= 85:
        matchup_score += 10
    elif wrc_plus <= 95:
        matchup_score += 8
    elif wrc_plus <= 105:
        matchup_score += 6
    elif wrc_plus <= 115:
        matchup_score += 4
    else:
        matchup_score += max(0, 10 - (wrc_plus - 85) / 5)
    
    # Team K% (0-5)
    k_pct = opponent_stats.get('k_pct', 22.0)
    if k_pct >= 25:
        matchup_score += 5
    elif k_pct >= 23:
        matchup_score += 4
    elif k_pct >= 21:
        matchup_score += 3
    else:
        matchup_score += max(0, k_pct / 6)
    
    # Lineup danger map (0-10)
    if lineup:
        elite_hitters = sum(1 for h in lineup if h.get('ops', 0) > 0.850)
        high_k_hitters = sum(1 for h in lineup if h.get('k_pct', 0) > 25)
        
        # Fewer elite hitters = higher score
        if elite_hitters <= 2:
            matchup_score += 5
        elif elite_hitters <= 4:
            matchup_score += 3
        elif elite_hitters <= 6:
            matchup_score += 1
        
        # More K-prone hitters = higher score
        if high_k_hitters >= 4:
            matchup_score += 5
        elif high_k_hitters >= 2:
            matchup_score += 3
        elif high_k_hitters >= 1:
            matchup_score += 1
    else:
        matchup_score += 5  # Neutral if no lineup data
    
    breakdown['matchup'] = round(matchup_score, 1)
    score += matchup_score
    
    # 4. Park/Weather Bucket (15 points) - expanded
    park_score = 0.0
    
    # Base park factor (0-10)
    runs_factor = park_data.get('runs', 100)
    if runs_factor <= 94:
        park_score += 10
    elif runs_factor <= 97:
        park_score += 8
    elif runs_factor <= 103:
        park_score += 6
    elif runs_factor <= 106:
        park_score += 4
    else:
        park_score += max(0, 10 - (runs_factor - 94) / 2)
    
    # Weather adjustment (0-5)
    if weather:
        temp = weather.get('temp_f', 70)
        wind_speed = weather.get('wind_speed', 0)
        
        # Temperature
        if temp < 60:
            park_score += 2  # Cold favors pitchers
        elif temp > 85:
            park_score -= 1  # Hot favors hitters
        
        # Wind (simplified - would need direction relative to field)
        if wind_speed > 15:
            park_score -= 1  # High wind = unpredictable
        elif wind_speed < 5:
            park_score += 1  # Calm = pitcher control
    
    park_score = max(0, min(park_score, 15))
    breakdown['park'] = round(park_score, 1)
    score += park_score
    
    # 5. Context Bucket (10 points)
    context_score = 0.0
    
    # Home/away (0-6)
    if pitcher_data.get('is_home'):
        # Check if home ERA significantly better
        if pitcher_stats.get('home_era', 0) > 0 and pitcher_stats.get('away_era', 0) > 0:
            if pitcher_stats['home_era'] < pitcher_stats['away_era'] - 0.50:
                context_score += 6
            else:
                context_score += 4
        else:
            context_score += 5  # Default home advantage
    else:
        context_score += 3
    
    # IP depth (0-4)
    if pitcher_stats.get('ip_per_game', 0) >= 6.0:
        context_score += 4
    elif pitcher_stats.get('ip_per_game', 0) >= 5.5:
        context_score += 2
    
    breakdown['context'] = round(context_score, 1)
    score += context_score
    
    final_score = min(100, round(score, 1))
    return final_score, breakdown


def get_streaming_tier(score):
    """Convert score to streaming tier"""
    if score >= 85:
        return 'Must-Stream', '🔥'
    elif score >= 75:
        return 'Strong Stream', '✅'
    elif score >= 65:
        return 'Viable Stream', '⚡'
    elif score >= 55:
        return 'Deep League Only', '🤔'
    else:
        return 'Avoid', '❌'


def get_league_fit(score):
    """Get league depth recommendation"""
    if score >= 85:
        return 'Priority in 10-team leagues'
    elif score >= 75:
        return 'Must-consider in 12-team leagues'
    elif score >= 65:
        return 'Viable in 15-team leagues'
    elif score >= 55:
        return 'Deep-league dart only'
    else:
        return 'Skip in all formats'


async def generate_ai_summary(pitcher_data, pitcher_stats, statcast_metrics, opponent_stats, lineup, park_data, weather, score, breakdown):
    """Generate AI summary with full context"""
    
    try:
        # Build rich context
        recent_starts_text = ""
        if pitcher_stats.get('recent_starts'):
            for i, start in enumerate(pitcher_stats['recent_starts'][:3], 1):
                recent_starts_text += f"\nStart {i}: {start['ip']} IP, {start['k']} K, {start['bb']} BB, {start['er']} ER vs {start['opponent']}"
        
        statcast_text = ""
        if statcast_metrics:
            statcast_text = f"""
Statcast Profile:
- xERA: {statcast_metrics.get('xera', 0):.2f}
- SwStr%: {statcast_metrics.get('swstr_pct', 0):.1f}%
- CSW%: {statcast_metrics.get('csw_pct', 0):.1f}%
- Hard-Hit%: {statcast_metrics.get('hard_hit_pct', 0):.1f}%
- Barrel%: {statcast_metrics.get('barrel_pct', 0):.1f}%
"""
        
        lineup_text = ""
        if lineup:
            elite = [h for h in lineup if h.get('ops', 0) > 0.850]
            k_prone = [h for h in lineup if h.get('k_pct', 0) > 25]
            lineup_text = f"\nLineup: {len(elite)} elite bats (.850+ OPS), {len(k_prone)} K-prone hitters (25%+ K rate)"
        
        weather_text = ""
        if weather:
            weather_text = f"\nWeather: {weather.get('temp_f', 0)}°F, {weather.get('wind_desc', 'calm')}"
        
        prompt = f"""You are an expert fantasy baseball analyst writing a streaming recommendation.

Pitcher: {pitcher_data['pitcher_name']} ({pitcher_data['pitcher_hand']}, {pitcher_data['team']})
Opponent: {pitcher_data['opponent']} at {pitcher_data['venue']}
Start Score: {score}/100

Season Stats:
- ERA: {pitcher_stats.get('era', 0):.2f} | WHIP: {pitcher_stats.get('whip', 0):.2f}
- K/9: {pitcher_stats.get('k_per_9', 0):.1f} | BB/9: {pitcher_stats.get('bb_per_9', 0):.1f}
- K-BB%: {pitcher_stats.get('k_bb_pct', 0):.1f}%
{statcast_text}
Recent Form (last 3 starts):{recent_starts_text}

Opponent vs {pitcher_data['pitcher_hand']}:
- wRC+: {opponent_stats.get('wrc_plus', 100)}
- K%: {opponent_stats.get('k_pct', 0):.1f}%
- OPS: {opponent_stats.get('ops', 0):.3f}
{lineup_text}

Park: {park_data.get('type', 'neutral')}
{weather_text}

Score Breakdown:
- Skill: {breakdown.get('skill', 0)}/30
- Form: {breakdown.get('form', 0)}/20  
- Matchup: {breakdown.get('matchup', 0)}/25
- Park/Weather: {breakdown.get('park', 0)}/15

Write a 4-6 sentence fantasy streaming analysis in beat-writer prose style. Be specific about skills, matchup details, and category outcomes. Include one concrete risk factor. No generic language."""

        response = await anthropic.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
            system="You are an expert fantasy baseball analyst. Write specific, actionable streaming recommendations in beat-writer prose. Always include a risk callout."
        )
        
        return response.content[0].text.strip()
        
    except Exception as e:
        print(f"Error generating AI summary: {e}")
        
        # Fallback
        tier, _ = get_streaming_tier(score)
        risk = "ratio risk moderate" if score < 70 else "early hook possible"
        return f"{pitcher_data['pitcher_name']} profiles as a {tier.lower()} against {pitcher_data['opponent']}. The matchup shows {breakdown.get('matchup', 0):.0f} points of support, and the park context is {park_data.get('type', 'neutral')}. Risk factor: {risk}."


async def post_streaming_board(date_str=None):
    """Main posting function"""
    try:
        # Refresh Statcast cache
        await refresh_statcast_cache()
        
        # Get probable starters (use provided date or default to today)
        starters = await get_probable_starters(date_str)
        
        if not starters:
            print("No probable starters found")
            return
        
        viable_streamers = []
        
        for starter in starters:
            # Get ownership
            ownership = await get_espn_ownership(starter['pitcher_name'], starter['pitcher_id'])
            
            if ownership is None:
                print(f"[STREAMING] ⏭️  Skipping {starter['pitcher_name']} - ownership unknown")
                continue
            
            if ownership > OWNERSHIP_THRESHOLD:
                print(f"[STREAMING] ⏭️  Skipping {starter['pitcher_name']} - {ownership}% over threshold ({OWNERSHIP_THRESHOLD}%)")
                continue
            
            print(f"[STREAMING] ✅ Processing {starter['pitcher_name']} - {ownership}% owned")
            
            # Get all data
            pitcher_stats = await get_pitcher_stats(starter['pitcher_id'], starter['pitcher_name'])
            if not pitcher_stats:
                print(f"[STREAMING] ⚠️  Skipping {starter['pitcher_name']} - stats unavailable")
                continue
            
            statcast_metrics = await get_statcast_metrics(starter['pitcher_name'])
            opponent_stats = await get_team_vs_handedness(starter['opponent_id'], starter['pitcher_hand'])
            lineup = await get_projected_lineup(starter['game_pk'], starter['opponent_id'])
            
            # Get park data
            park_data = None
            for park_name, data in PARK_DATA.items():
                if park_name.lower() in starter['venue'].lower():
                    park_data = data
                    break
            
            if not park_data:
                park_data = {'lat': 0, 'lng': 0, 'runs': 100, 'hr': 100, 'type': 'neutral'}
            
            # Get weather
            weather = None
            if park_data.get('lat') and park_data.get('lng'):
                weather = await get_weather(park_data['lat'], park_data['lng'], starter['game_time'])
            
            # Calculate score
            score, breakdown = await calculate_start_score(
                starter, pitcher_stats, statcast_metrics, 
                opponent_stats, lineup, park_data, weather
            )
            
            # Generate summary
            summary = await generate_ai_summary(
                starter, pitcher_stats, statcast_metrics,
                opponent_stats, lineup, park_data, weather,
                score, breakdown
            )
            
            viable_streamers.append({
                'data': starter,
                'stats': pitcher_stats,
                'statcast': statcast_metrics,
                'opponent_stats': opponent_stats,
                'lineup': lineup,
                'park': park_data,
                'weather': weather,
                'score': score,
                'breakdown': breakdown,
                'summary': summary,
                'ownership': ownership
            })
        
        # Sort and post
        viable_streamers.sort(key=lambda x: x['score'], reverse=True)
        
        channel = bot.get_channel(STREAMING_CHANNEL_ID)
        if not channel:
            print(f"Channel {STREAMING_CHANNEL_ID} not found")
            return
        
        # Header
        et_tz = pytz.timezone('America/New_York')
        if date_str:
            display_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%A, %B %d, %Y')
        else:
            display_date = datetime.now(et_tz).strftime('%A, %B %d, %Y')
        
        header = discord.Embed(
            title=f"📊 Streaming Scout: {display_date}",
            description=f"{len(viable_streamers)} pitchers under {OWNERSHIP_THRESHOLD}% rostered",
            color=0x1E88E5
        )
        await channel.send(embed=header)
        
        # Post top 10
        for streamer in viable_streamers[:10]:
            await post_streamer_card(channel, streamer)
            await asyncio.sleep(2)
        
        print(f"Posted {min(len(viable_streamers), 10)} streaming recommendations")
        
    except Exception as e:
        print(f"Error in post_streaming_board: {e}")


async def post_streamer_card(channel, streamer):
    """Post individual card"""
    try:
        data = streamer['data']
        stats = streamer['stats']
        statcast = streamer['statcast']
        score = streamer['score']
        breakdown = streamer['breakdown']
        summary = streamer['summary']
        ownership = streamer['ownership']
        park = streamer['park']
        weather = streamer['weather']
        lineup = streamer['lineup']
        
        tier, emoji = get_streaming_tier(score)
        league_fit = get_league_fit(score)
        
        embed = discord.Embed(
            title=f"{data['pitcher_name']} ({data['pitcher_hand']}) vs {data['opponent']}",
            description=f"{emoji} **{tier}** • Start Score: {score}/100",
            color=get_tier_color(tier)
        )
        
        # Ownership + venue
        venue_line = f"{data['venue']}\n{park['type']}"
        if weather:
            venue_line += f"\n{weather.get('temp_f', 0)}°F, {weather.get('wind_desc', 'calm')}"
        
        embed.add_field(name="📈 Ownership", value=f"{ownership}% ESPN", inline=True)
        embed.add_field(name="🎯 Venue", value=venue_line, inline=True)
        embed.add_field(name="🎯 League Fit", value=league_fit, inline=True)
        
        # Stats
        stats_line = f"{stats.get('era', 0):.2f} ERA • {stats.get('whip', 0):.2f} WHIP\n{stats.get('k_per_9', 0):.1f} K/9 • {stats.get('k_bb_pct', 0):.1f}% K-BB"
        embed.add_field(name="📊 Season Line", value=stats_line, inline=False)
        
        # Statcast
        if statcast:
            statcast_line = f"xERA: {statcast.get('xera', 0):.2f} • SwStr: {statcast.get('swstr_pct', 0):.1f}%\nHard-Hit: {statcast.get('hard_hit_pct', 0):.1f}% • Barrel: {statcast.get('barrel_pct', 0):.1f}%"
            embed.add_field(name="⚡ Statcast Profile", value=statcast_line, inline=False)
        
        # Recent form
        if stats.get('recent_starts'):
            recent = stats['recent_starts']
            total_ip = sum(float(s.get('ip', 0)) for s in recent)
            total_k = sum(int(s.get('k', 0)) for s in recent)
            total_er = sum(int(s.get('er', 0)) for s in recent)
            
            if total_ip > 0:
                recent_era = (total_er * 9) / total_ip
                embed.add_field(
                    name="🔥 Last 3 Starts",
                    value=f"{total_ip:.1f} IP • {total_k} K • {recent_era:.2f} ERA",
                    inline=False
                )
        
        # Lineup danger
        if lineup:
            elite = [h for h in lineup if h.get('ops', 0) > 0.850]
            k_prone = [h for h in lineup if h.get('k_pct', 0) > 25]
            embed.add_field(
                name="👥 Opposing Lineup",
                value=f"{len(elite)} elite bats (.850+ OPS)\n{len(k_prone)} K-prone hitters (25%+ K)",
                inline=False
            )
        
        # Score breakdown
        embed.add_field(
            name="📈 Score Breakdown",
            value=f"Skill: {breakdown.get('skill', 0)}/30 • Form: {breakdown.get('form', 0)}/20\nMatchup: {breakdown.get('matchup', 0)}/25 • Park: {breakdown.get('park', 0)}/15",
            inline=False
        )
        
        # Summary
        embed.add_field(name="💭 Scout's Take", value=summary, inline=False)
        
        await channel.send(embed=embed)
        
    except Exception as e:
        print(f"Error posting card: {e}")


def get_tier_color(tier):
    """Get color for tier"""
    colors = {
        'Must-Stream': 0xFF4444,
        'Strong Stream': 0x44FF44,
        'Viable Stream': 0xFFAA44,
        'Deep League Only': 0xAAAA44,
        'Avoid': 0x888888
    }
    return colors.get(tier, 0x1E88E5)


@tasks.loop(hours=24)
async def daily_streaming_board():
    """Daily 8 AM ET posting"""
    et_tz = pytz.timezone('America/New_York')
    now = datetime.now(et_tz)
    
    if now.hour == 8:
        await post_streaming_board()


@bot.event
async def on_ready():
    global http_session
    http_session = aiohttp.ClientSession()
    
    load_espn_player_ids()
    await bot.change_presence(status=discord.Status.invisible)
    
    print(f'{bot.user} is now running!')
    print(f'Streaming channel: {STREAMING_CHANNEL_ID}')
    
    if not daily_streaming_board.is_running():
        daily_streaming_board.start()


@bot.command(name='stream')
async def manual_stream(ctx):
    """Manual trigger"""
    await ctx.send("Generating today's streaming board...")
    await post_streaming_board()


@bot.command(name='tomorrow')
async def tomorrow_stream(ctx):
    """Get tomorrow's streaming board"""
    et_tz = pytz.timezone('America/New_York')
    tomorrow = (datetime.now(et_tz) + timedelta(days=1)).strftime('%Y-%m-%d')
    await ctx.send(f"Generating streaming board for {tomorrow}...")
    await post_streaming_board(date_str=tomorrow)


@bot.event
async def on_close():
    if http_session:
        await http_session.close()


if __name__ == '__main__':
    if not DISCORD_TOKEN:
        print("Error: STREAMING_BOT_TOKEN not set")
    elif STREAMING_CHANNEL_ID == 0:
        print("Error: STREAMING_CHANNEL_ID not set")
    else:
        bot.run(DISCORD_TOKEN)


async def start_streaming_bot():
    """Entry point for main.py integration"""
    global http_session
    http_session = aiohttp.ClientSession()
    
    load_espn_player_ids()
    
    print(f'Starting streaming bot...')
    print(f'Streaming channel: {STREAMING_CHANNEL_ID}')
    
    await bot.start(DISCORD_TOKEN)

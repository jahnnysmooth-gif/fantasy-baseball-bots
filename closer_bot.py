import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord
import requests

DISCORD_TOKEN = os.getenv("CLOSER_BOT_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
POLL_MINUTES = int(os.getenv("POLL_MINUTES", "10"))
STATE_DIR = os.getenv("STATE_DIR", "state/closer")
STATE_FILE = os.path.join(STATE_DIR, "closer_alert_state.json")

os.makedirs(STATE_DIR, exist_ok=True)

ET = ZoneInfo("America/New_York")

TEAM_COLORS = {
    "ARI": 0xA71930,
    "ATH": 0x003831,
    "ATL": 0xCE1141,
    "BAL": 0xDF4601,
    "BOS": 0xBD3039,
    "CHC": 0x0E3386,
    "CWS": 0x27251F,
    "CIN": 0xC6011F,
    "CLE": 0xE31937,
    "COL": 0x33006F,
    "DET": 0x0C2340,
    "HOU": 0xEB6E1F,
    "KC": 0x004687,
    "LAA": 0xBA0021,
    "LAD": 0x005A9C,
    "MIA": 0x00A3E0,
    "MIL": 0x12284B,
    "MIN": 0x002B5C,
    "NYM": 0xFF5910,
    "NYY": 0x0C2340,
    "PHI": 0xE81828,
    "PIT": 0xFDB827,
    "SD": 0x2F241D,
    "SF": 0xFD5A1E,
    "SEA": 0x005C5C,
    "STL": 0xC41E3A,
    "TB": 0x092C5C,
    "TEX": 0x003278,
    "TOR": 0x134A8E,
    "WSH": 0xAB0003,
}

TEAM_NAME_TO_ABBR = {
    "Arizona Diamondbacks": "ARI",
    "Athletics": "ATH",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

intents = discord.Intents.default()
client = discord.Client(intents=intents)


def log(message: str) -> None:
    print(f"[CLOSER] {message}", flush=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_et() -> datetime:
    return datetime.now(ET)


def in_quiet_hours() -> bool:
    hour = now_et().hour
    return 2 <= hour < 13


def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)


def load_state() -> dict:
    ensure_state_dir()
    if not os.path.exists(STATE_FILE):
        return {
            "posted_events": [],
            "processed_final_games": {},
        }

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        state = {}

    state.setdefault("posted_events", [])
    state.setdefault("processed_final_games", {})
    return state


def save_state(state: dict) -> None:
    ensure_state_dir()

    posted_events = state.get("posted_events", [])
    if len(posted_events) > 5000:
        state["posted_events"] = posted_events[-3000:]

    processed_final_games = state.get("processed_final_games", {})
    if len(processed_final_games) > 500:
        items = list(processed_final_games.items())[-300:]
        state["processed_final_games"] = dict(items)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_logo(team_abbr: str) -> str | None:
    if not team_abbr:
        return None

    key = team_abbr.upper()
    if key in ("ATH", "OAK"):
        file_key = "oak"
    elif key == "CWS":
        file_key = "chw"
    else:
        file_key = key.lower()

    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{file_key}.png"


def resolve_team_abbr(game: dict, side: str, team_name: str) -> str:
    abbr = (
        game.get("teams", {})
        .get(side, {})
        .get("team", {})
        .get("abbreviation", "")
        .upper()
    )

    if abbr:
        return abbr

    return TEAM_NAME_TO_ABBR.get(team_name, "")


def format_stat_line(ip: str, h: int, er: int, bb: int, k: int) -> str:
    return f"IP {ip} • H {h} • ER {er} • BB {bb} • K {k}"


def build_score_text(away_abbr: str, away_score: int, home_abbr: str, home_score: int) -> str:
    if away_score > home_score:
        return f"**{away_abbr} {away_score}** - {home_abbr} {home_score}"
    if home_score > away_score:
        return f"{away_abbr} {away_score} - **{home_abbr} {home_score}**"
    return f"{away_abbr} {away_score} - {home_abbr} {home_score}"


def get_games() -> list:
    today_et = now_et().date()
    yesterday_et = today_et - timedelta(days=1)

    games = []

    for d in [today_et, yesterday_et]:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={d.isoformat()}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        for date_block in data.get("dates", []):
            games.extend(date_block.get("games", []))

    return games


def build_final_stamp(game: dict) -> str:
    status = game.get("status", {}).get("detailedState", "")
    away_score = game.get("teams", {}).get("away", {}).get("score", "")
    home_score = game.get("teams", {}).get("home", {}).get("score", "")
    game_date = game.get("gameDate", "")
    return f"{status}|{away_score}|{home_score}|{game_date}"


def build_save_embed(
    team: str,
    pitcher: str,
    stats: str,
    score: str,
    team_abbr: str,
    matchup: str,
) -> discord.Embed:
    color = TEAM_COLORS.get(team_abbr, 0x2ECC71)
    logo = get_logo(team_abbr)

    embed = discord.Embed(
        title=f"🚨 SAVE — {pitcher}",
        description=(
            f"⚾ **{matchup}**\n\n"
            f"**{stats}**\n\n"
            f"🏁 **Final Score**\n{score}"
        ),
        color=color,
        timestamp=now_utc(),
    )

    if logo:
        embed.set_author(name=team, icon_url=logo)
        embed.set_thumbnail(url=logo)
    else:
        embed.set_author(name=f"{team} Bullpen Alert")

    return embed


def build_blown_embed(
    team: str,
    pitcher: str,
    stats: str,
    score: str,
    team_abbr: str,
    matchup: str,
) -> discord.Embed:
    color = TEAM_COLORS.get(team_abbr, 0xE74C3C)
    logo = get_logo(team_abbr)

    embed = discord.Embed(
        title=f"⚠️ BLOWN SAVE — {pitcher}",
        description=(
            f"⚾ **{matchup}**\n\n"
            f"**{stats}**\n\n"
            f"🏁 **Final Score**\n{score}"
        ),
        color=color,
        timestamp=now_utc(),
    )

    if logo:
        embed.set_author(name=team, icon_url=logo)
        embed.set_thumbnail(url=logo)
    else:
        embed.set_author(name=f"{team} Bullpen Alert")

    return embed


async def get_channel() -> discord.abc.Messageable | None:
    channel = client.get_channel(DISCORD_CHANNEL_ID)
    if channel is not None:
        return channel

    try:
        return await client.fetch_channel(DISCORD_CHANNEL_ID)
    except Exception as e:
        log(f"ERROR: Could not fetch channel {DISCORD_CHANNEL_ID}: {e}")
        return None


async def process_games() -> None:
    state = load_state()
    posted_events = set(state.get("posted_events", []))
    processed_final_games = state.get("processed_final_games", {})

    channel = await get_channel()
    if channel is None:
        return

    games = get_games()
    total_final_games_seen = 0
    total_new_final_games = 0
    total_saves_found = 0
    total_blown_found = 0
    total_posted = 0

    log(f"Games found: {len(games)}")

    for game in games:
        status = game.get("status", {}).get("detailedState", "")
        if status != "Final":
            continue

        total_final_games_seen += 1

        game_pk = game.get("gamePk")
        if not game_pk:
            continue

        game_pk_str = str(game_pk)
        final_stamp = build_final_stamp(game)

        if processed_final_games.get(game_pk_str) == final_stamp:
            log(f"Skipping already processed final game: {game_pk}")
            continue

        total_new_final_games += 1
        log(f"Processing new final game: {game_pk}")

        url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        box = data.get("liveData", {}).get("boxscore", {}).get("teams", {})
        if not box:
            log(f"No boxscore found for game {game_pk}")
            processed_final_games[game_pk_str] = final_stamp
            continue

        away_team_box = box.get("away", {})
        home_team_box = box.get("home", {})

        away_team_name = away_team_box.get("team", {}).get("name", "Away Team")
        home_team_name = home_team_box.get("team", {}).get("name", "Home Team")

        away_abbr = resolve_team_abbr(game, "away", away_team_name)
        home_abbr = resolve_team_abbr(game, "home", home_team_name)

        away_score = away_team_box.get("teamStats", {}).get("batting", {}).get("runs", 0)
        home_score = home_team_box.get("teamStats", {}).get("batting", {}).get("runs", 0)

        matchup = f"{away_abbr or 'AWAY'} @ {home_abbr or 'HOME'}"
        score = build_score_text(
            away_abbr or "AWAY",
            away_score,
            home_abbr or "HOME",
            home_score,
        )

        game_saves = 0
        game_blown = 0
        game_posted = 0
        blown_posted_teams = set()

        for side in ["away", "home"]:
            team_box = box.get(side, {})
            team = team_box.get("team", {}).get("name", "Unknown Team")
            team_abbr = away_abbr if side == "away" else home_abbr
            players = team_box.get("players", {})

            for p in players.values():
                pitching_stats = p.get("stats", {}).get("pitching")
                if not pitching_stats:
                    continue

                pitcher = p.get("person", {}).get("fullName", "Unknown Pitcher")
                pitcher_id = p.get("person", {}).get("id", pitcher)

                ip = pitching_stats.get("inningsPitched", "0.0")
                h = pitching_stats.get("hits", 0)
                er = pitching_stats.get("earnedRuns", 0)
                bb = pitching_stats.get("baseOnBalls", 0)
                k = pitching_stats.get("strikeOuts", 0)

                stat_line = format_stat_line(ip, h, er, bb, k)

                if pitching_stats.get("saves", 0) > 0:
                    total_saves_found += 1
                    game_saves += 1
                    event_key = f"save_{game_pk}_{pitcher_id}"

                    if event_key in posted_events:
                        log(f"Skipping duplicate save: {pitcher} | {team}")
                    else:
                        embed = build_save_embed(
                            team=team,
                            pitcher=pitcher,
                            stats=stat_line,
                            score=score,
                            team_abbr=team_abbr,
                            matchup=matchup,
                        )
                        try:
                            await channel.send(embed=embed)
                            posted_events.add(event_key)
                            total_posted += 1
                            game_posted += 1
                            log(f"SAVE: {pitcher} | {team}")
                            await asyncio.sleep(1.5)
                        except Exception as e:
                            log(f"Discord send error on save: {e}")

                if pitching_stats.get("blownSaves", 0) > 0:
                    total_blown_found += 1
                    game_blown += 1

                    if team in blown_posted_teams:
                        log(f"Skipping extra blown save for team in same game: {team}")
                        continue

                    event_key = f"blown_team_{game_pk}_{team}"

                    if event_key in posted_events:
                        log(f"Skipping duplicate blown save team alert: {team}")
                    else:
                        embed = build_blown_embed(
                            team=team,
                            pitcher=pitcher,
                            stats=stat_line,
                            score=score,
                            team_abbr=team_abbr,
                            matchup=matchup,
                        )
                        try:
                            await channel.send(embed=embed)
                            posted_events.add(event_key)
                            blown_posted_teams.add(team)
                            total_posted += 1
                            game_posted += 1
                            log(f"BLOWN SAVE: {pitcher} | {team}")
                            await asyncio.sleep(1.5)
                        except Exception as e:
                            log(f"Discord send error on blown save: {e}")

        processed_final_games[game_pk_str] = final_stamp

        log(
            f"Game {game_pk} complete | "
            f"Saves found: {game_saves} | "
            f"Blown saves found: {game_blown} | "
            f"Posted: {game_posted}"
        )

    state["posted_events"] = list(posted_events)
    state["processed_final_games"] = processed_final_games
    save_state(state)

    log(
        f"Loop summary | "
        f"Final games seen: {total_final_games_seen} | "
        f"New finals processed: {total_new_final_games} | "
        f"Saves found: {total_saves_found} | "
        f"Blown saves found: {total_blown_found} | "
        f"Posted this loop: {total_posted}"
    )


async def polling_loop() -> None:
    await client.wait_until_ready()

    log("=== CLOSER ALERT BOT STARTED ===")
    log(f"Poll interval: {POLL_MINUTES} minutes")
    log(f"State file: {STATE_FILE}")

    while not client.is_closed():
        current_et = now_et().strftime("%Y-%m-%d %I:%M:%S %p %Z")
        log(f"Loop start | ET time: {current_et}")

        try:
            if in_quiet_hours():
                log("Quiet hours active (2:00 AM ET - 1:00 PM ET). Skipping this loop.")
            else:
                await process_games()
        except Exception as e:
            log(f"ERROR: {e}")

        log(f"Sleeping {POLL_MINUTES} minutes")
        await asyncio.sleep(POLL_MINUTES * 60)


@client.event
async def on_ready():
    log(f"Logged in as {client.user}")
    if not hasattr(client, "polling_task"):
        client.polling_task = asyncio.create_task(polling_loop())


if not DISCORD_TOKEN:
    raise RuntimeError("CLOSER_BOT_TOKEN is not set")

if not DISCORD_CHANNEL_ID:
    raise RuntimeError("DISCORD_CHANNEL_ID is not set")


async def start_closer_bot():
    await client.start(DISCORD_TOKEN)

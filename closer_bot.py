import asyncio
import json
import os
import random
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo

import discord
import requests

from utils.closer_depth_chart import fetch_closer_depth_chart
from utils.closer_tracker import build_tracked_relief_map, normalize_name

# ---------------- CONFIG ----------------

DISCORD_TOKEN = os.getenv("CLOSER_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CLOSER_WATCH_CHANNEL_ID", "0"))

STATE_DIR = "state/closer"
STATE_FILE = os.path.join(STATE_DIR, "state.json")

os.makedirs(STATE_DIR, exist_ok=True)

ET = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = 10

# ---------------- TEAM STYLING ----------------

TEAM_COLORS = {
    "SEA": 0x005C5C,
    "BOS": 0xBD3039,
    "NYY": 0x0C2340,
    "NYM": 0xFF5910,
    "LAD": 0x005A9C,
    "HOU": 0xEB6E1F,
    # fallback handled below
}

def get_logo(team):
    key = team.lower()
    if team == "CWS":
        key = "chw"
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"

# ---------------- DISCORD ----------------

intents = discord.Intents.default()
client = discord.Client(intents=intents)

# ---------------- STATE ----------------

def load_state():
    if not os.path.exists(STATE_FILE):
        return {"posted": []}
    return json.load(open(STATE_FILE))

def save_state(state):
    json.dump(state, open(STATE_FILE, "w"), indent=2)

# ---------------- HELPERS ----------------

def now_et():
    return datetime.now(ET)

def format_ip(ip):
    if ip.endswith(".0"):
        return f"{int(float(ip))} IP"
    if ip.endswith(".1"):
        return "1 out"
    if ip.endswith(".2"):
        return "2 outs"
    return ip

def format_game_line(s):
    return f"{format_ip(s['ip'])} • {s['h']} H • {s['er']} ER • {s['bb']} BB • {s['k']} K"

def format_season_line(s):
    parts = [f"{s.get('wins',0)}-{s.get('losses',0)}"]

    if s.get("saves", 0) > 0:
        parts.append(f"{s['saves']} SV")
    if s.get("holds", 0) > 0:
        parts.append(f"{s['holds']} HLD")

    parts += [
        f"{s.get('era','0.00')} ERA",
        f"{s.get('strikeOuts',0)} K",
        f"{s.get('whip','0.00')} WHIP",
        f"{s.get('k9','0.0')} K/9",
    ]
    return " • ".join(parts)

def classify(stat):
    if stat.get("saves"): return "SAVE"
    if stat.get("blownSaves"): return "BLOWN"
    if stat.get("holds"): return "HOLD"
    if stat["er"] == 0 and stat["h"] == 0 and stat["bb"] == 0:
        return "DOM"
    if stat["er"] == 0:
        return "CLEAN"
    if stat["er"] >= 3:
        return "ROUGH"
    return "TROUBLE"

def impact_tag(label):
    return {
        "SAVE": "🔒 Locked it down",
        "BLOWN": "💥 Lead blown",
        "HOLD": "🧱 Held the line",
        "DOM": "🔥 Dominant",
        "CLEAN": "🧊 Clean inning",
        "TROUBLE": "⚠️ Trouble outing",
        "ROUGH": "💀 Rough outing",
    }.get(label, "")

def build_summary(name, team, stat, label):
    ip = stat["ip"]
    h, er, bb, k = stat["h"], stat["er"], stat["bb"], stat["k"]

    if label == "SAVE":
        return f"{name} closed it out for {team}, locking down the save. He worked {format_ip(ip).lower()} and struck out {k}."

    if label == "BLOWN":
        return f"{name} couldn't hold the lead for {team} as things unraveled quickly. He allowed {er} runs while getting just {format_ip(ip).lower()}."

    if label == "DOM":
        return f"{name} dominated out of the bullpen for {team}, not allowing a baserunner while striking out {k}."

    if label == "CLEAN":
        return f"{name} delivered a clean outing for {team}, working {format_ip(ip).lower()} without allowing a run."

    if label == "ROUGH":
        return f"{name} was hit hard out of the bullpen for {team}, allowing {er} runs."

    return f"{name} worked in relief for {team}, allowing {er} runs over {format_ip(ip).lower()}."

# ---------------- CORE ----------------

def get_games():
    today = now_et().date()
    yesterday = today - timedelta(days=1)

    games = []

    for d in [today, yesterday]:
        url = f"{SCHEDULE_URL}&date={d.isoformat()}"
        try:
            data = requests.get(url, timeout=30).json()
            for date_block in data.get("dates", []):
                games.extend(date_block.get("games", []))
        except Exception as e:
            print(f"Error fetching schedule for {d}: {e}")

    return games

def get_feed(game_id):
    return requests.get(LIVE_URL.format(game_id)).json()

def get_pitchers(feed):
    box = feed["liveData"]["boxscore"]["teams"]
    result = []

    for side in ["home", "away"]:
        team = box[side]["team"]["abbreviation"]
        players = box[side]["players"]

        for p in players.values():
            stats = p.get("stats", {}).get("pitching")
            if not stats:
                continue

            if not stats.get("inningsPitched"):
                continue

            result.append({
                "id": p["person"]["id"],
                "name": p["person"]["fullName"],
                "team": team,
                "stats": stats
            })
    return result

# ---------------- POST ----------------

async def post_card(channel, p, matchup, score):
    stat = {
        "ip": p["stats"]["inningsPitched"],
        "h": p["stats"]["hits"],
        "er": p["stats"]["earnedRuns"],
        "bb": p["stats"]["baseOnBalls"],
        "k": p["stats"]["strikeOuts"],
        "saves": p["stats"].get("saves",0),
        "holds": p["stats"].get("holds",0),
        "blownSaves": p["stats"].get("blownSaves",0),
    }

    label = classify(stat)

    title_prefix = ""
    if label == "SAVE":
        title_prefix = "🚨 SAVE — "
    elif label == "BLOWN":
        title_prefix = "⚠️ BLOWN SAVE — "

    title = f"**{title_prefix}{p['name']} ({p['team']})**"

    embed = discord.Embed(
        title=title,
        color=TEAM_COLORS.get(p["team"], 0x2ECC71),
        timestamp=datetime.now(timezone.utc)
    )

    embed.set_thumbnail(url=get_logo(p["team"]))

    embed.add_field(name="", value=f"**{impact_tag(label)}**", inline=False)
    embed.add_field(name="⚾ Matchup", value=matchup, inline=False)
    embed.add_field(name="Game Line", value=format_game_line(stat), inline=False)
    embed.add_field(name="Season", value="Coming soon", inline=False)
    embed.add_field(name="Summary", value=build_summary(p["name"], p["team"], stat, label), inline=False)
    embed.add_field(name="Final Score", value=score, inline=False)

    await channel.send(embed=embed)

# ---------------- LOOP ----------------

async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    state = load_state()
    posted = set(state["posted"])

    tracked = build_tracked_relief_map()

    while True:
        games = get_games()

        for g in games:
            if g["status"]["detailedState"] != "Final":
                continue

            game_id = g["gamePk"]
            feed = get_feed(game_id)

            pitchers = get_pitchers(feed)

            matchup = f"{g['teams']['away']['team']['abbreviation']} @ {g['teams']['home']['team']['abbreviation']}"
            score = f"{g['teams']['away']['score']} - {g['teams']['home']['score']}"

            save_ids = set()

            for p in pitchers:
                if p["stats"].get("saves",0):
                    key = f"{game_id}_{p['id']}"
                    if key not in posted:
                        await post_card(channel, p, matchup, score)
                        posted.add(key)
                        save_ids.add(key)

            for p in pitchers:
                norm = normalize_name(p["name"])
                key = f"{game_id}_{p['id']}"

                if key in posted:
                    continue

                if norm in tracked:
                    await post_card(channel, p, matchup, score)
                    posted.add(key)

        state["posted"] = list(posted)
        save_state(state)

        await asyncio.sleep(POLL_MINUTES * 60)

# ---------------- START ----------------

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    client.loop.create_task(loop())

client.run(DISCORD_TOKEN)

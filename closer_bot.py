import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import discord
import requests

from utils.closer_tracker import build_tracked_relief_map, normalize_name

# ---------------- CONFIG ----------------

TOKEN = os.getenv("CLOSER_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CLOSER_WATCH_CHANNEL_ID", "0"))

STATE_FILE = "state/closer/state.json"
os.makedirs("state/closer", exist_ok=True)

ET = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
LIVE_URL = "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live"

POLL_MINUTES = 10

# ---------------- TEAM STYLE ----------------

TEAM_COLORS = {
    "ARI": 0xA71930, "ATL": 0xCE1141, "BAL": 0xDF4601, "BOS": 0xBD3039,
    "CHC": 0x0E3386, "CWS": 0x27251F, "CIN": 0xC6011F, "CLE": 0xE31937,
    "COL": 0x33006F, "DET": 0x0C2340, "HOU": 0xEB6E1F, "KC": 0x004687,
    "LAA": 0xBA0021, "LAD": 0x005A9C, "MIA": 0x00A3E0, "MIL": 0x12284B,
    "MIN": 0x002B5C, "NYM": 0xFF5910, "NYY": 0x0C2340, "PHI": 0xE81828,
    "PIT": 0xFDB827, "SD": 0x2F241D, "SF": 0xFD5A1E, "SEA": 0x005C5C,
    "STL": 0xC41E3A, "TB": 0x092C5C, "TEX": 0x003278, "TOR": 0x134A8E,
    "WSH": 0xAB0003
}

def get_logo(team):
    key = team.lower()
    if team == "CWS":
        key = "chw"
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"

# ---------------- STATE ----------------

def load_state():
    if not os.path.exists(STATE_FILE):
        return {"posted": []}
    return json.load(open(STATE_FILE))

def save_state(state):
    json.dump(state, open(STATE_FILE, "w"), indent=2)

# ---------------- FORMATTING ----------------

def format_ip(ip):
    if ip.endswith(".0"):
        val = int(float(ip))
        return "an inning" if val == 1 else f"{val} innings"
    if ip.endswith(".1"):
        return "one out"
    if ip.endswith(".2"):
        return "two outs"
    return ip

def format_line(s):
    return f"{s['ip']} • {s['h']} H • {s['er']} ER • {s['bb']} BB • {s['k']} K"

# ---------------- CLASSIFICATION ----------------

def classify(s):
    if s.get("saves"): return "SAVE"
    if s.get("blownSaves"): return "BLOWN"
    if s.get("holds"): return "HOLD"
    if s["er"] == 0 and s["h"] == 0 and s["bb"] == 0: return "DOM"
    if s["er"] == 0 and (s["h"] + s["bb"]) >= 2: return "TRAFFIC"
    if s["er"] >= 3: return "ROUGH"
    return "CLEAN"

# ---------------- ELITE SUMMARY ----------------

def build_summary(name, team, s, label):
    ip = format_ip(s["ip"])
    er = s["er"]
    h = s["h"]
    bb = s["bb"]
    k = s["k"]

    if label == "SAVE":
        line1 = f"{name} entered in the 9th and shut the door for {team}, locking down the save."
    elif label == "BLOWN":
        line1 = f"{name} entered in the 9th but couldn’t hold the lead for {team}."
    elif label == "HOLD":
        line1 = f"{name} came on in a key spot and protected the lead for {team}."
    elif label == "DOM":
        line1 = f"{name} came on and dominated for {team}."
    elif label == "TRAFFIC":
        line1 = f"{name} worked out of trouble for {team}."
    elif label == "ROUGH":
        line1 = f"{name} was hit hard in relief for {team}."
    else:
        line1 = f"{name} worked a scoreless outing for {team}."

    if er == 0 and h == 0 and bb == 0:
        line2 = f"He retired all hitters he faced over {ip}" + (f" with {k} strikeouts." if k >= 2 else ".")
    elif er == 0:
        line2 = f"He worked {ip}, allowing {h} hits and {bb} walks" + (f" while striking out {k}." if k >= 2 else ".")
    else:
        line2 = f"He allowed {er} runs over {ip} on {h} hits and {bb} walks" + (f", striking out {k}." if k >= 2 else ".")

    return f"{line1} {line2}"

# ---------------- CORE ----------------

def get_games():
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)

    games = []

    for d in [today, yesterday]:
        try:
            data = requests.get(f"{SCHEDULE_URL}&date={d}", timeout=30).json()
            for date_block in data.get("dates", []):
                games.extend(date_block.get("games", []))
        except:
            continue

    return games

def get_feed(game_id):
    return requests.get(LIVE_URL.format(game_id), timeout=30).json()

def get_pitchers(feed):
    result = []
    box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})

    for side in ["home", "away"]:
        team = box.get(side, {}).get("team", {}).get("abbreviation")
        if not team:
            team = feed.get("gameData", {}).get("teams", {}).get(side, {}).get("abbreviation", "UNK")

        players = box.get(side, {}).get("players", {})

        for p in players.values():
            stats = p.get("stats", {}).get("pitching")
            if not stats or not stats.get("inningsPitched"):
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
    s = {
        "ip": p["stats"]["inningsPitched"],
        "h": p["stats"]["hits"],
        "er": p["stats"]["earnedRuns"],
        "bb": p["stats"]["baseOnBalls"],
        "k": p["stats"]["strikeOuts"],
        "saves": p["stats"].get("saves", 0),
        "holds": p["stats"].get("holds", 0),
        "blownSaves": p["stats"].get("blownSaves", 0),
    }

    label = classify(s)

    prefix = ""
    if label == "SAVE": prefix = "🚨 SAVE — "
    if label == "BLOWN": prefix = "⚠️ BLOWN SAVE — "

    embed = discord.Embed(
        title=f"**{prefix}{p['name']} ({p['team']})**",
        color=TEAM_COLORS.get(p["team"], 0x2ECC71),
        timestamp=datetime.now(timezone.utc)
    )

    embed.set_thumbnail(url=get_logo(p["team"]))

    embed.add_field(name="⚾ Matchup", value=matchup, inline=False)
    embed.add_field(name="Game Line", value=format_line(s), inline=False)
    embed.add_field(name="Summary", value=build_summary(p["name"], p["team"], s, label), inline=False)
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
        try:
            games = get_games()

            for g in games:
                if g["status"]["detailedState"] != "Final":
                    continue

                game_id = g["gamePk"]
                feed = get_feed(game_id)
                pitchers = get_pitchers(feed)

                away = g['teams']['away']['team']
                home = g['teams']['home']['team']

                away_abbr = away.get("abbreviation") or away.get("name", "AWAY")[:3].upper()
                home_abbr = home.get("abbreviation") or home.get("name", "HOME")[:3].upper()

                matchup = f"{away_abbr} @ {home_abbr}"
                score = f"{g['teams']['away']['score']} - {g['teams']['home']['score']}"

                for p in pitchers:
                    key = f"{game_id}_{p['id']}"
                    if key in posted:
                        continue

                    norm = normalize_name(p["name"])
                    is_save = p["stats"].get("saves", 0) > 0
                    is_tracked = norm in tracked

                    if is_save or is_tracked:
                        await post_card(channel, p, matchup, score)
                        posted.add(key)

            state["posted"] = list(posted)
            save_state(state)

        except Exception as e:
            print("Loop error:", e)

        await asyncio.sleep(POLL_MINUTES * 60)

# ---------------- START ----------------

intents = discord.Intents.default()
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    asyncio.create_task(loop())

client.run(TOKEN)

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
RESET_CLOSER_STATE = os.getenv("RESET_CLOSER_STATE", "").lower() in {"1", "true", "yes"}

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


def get_logo(team: str) -> str:
    key = team.lower()
    if team == "CWS":
        key = "chw"
    elif team == "ATH":
        key = "oak"
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{key}.png"


# ---------------- STATE ----------------

def load_state():
    if RESET_CLOSER_STATE:
        return {"posted": []}

    if not os.path.exists(STATE_FILE):
        return {"posted": []}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"posted": []}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# ---------------- HELPERS ----------------

def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def safe_int(value, default=0):
    try:
        if value in (None, "", "-"):
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value, default=0.0):
    try:
        if value in (None, "", "-"):
            return default
        return float(value)
    except Exception:
        return default


def baseball_ip_to_outs(ip: str) -> int:
    """
    Baseball IP format:
      1.0 = 3 outs
      1.1 = 4 outs
      1.2 = 5 outs
    """
    if not ip:
        return 0

    text = str(ip).strip()
    if "." not in text:
        whole = safe_int(text, 0)
        return whole * 3

    whole_str, frac_str = text.split(".", 1)
    whole = safe_int(whole_str, 0)
    frac = safe_int(frac_str, 0)
    frac = min(frac, 2)
    return whole * 3 + frac


def format_ip_for_line(ip: str) -> str:
    text = str(ip).strip()
    if text.endswith(".0"):
        return f"{safe_int(float(text), 0)} IP"
    if text.endswith(".1"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅓ IP" if whole == 0 else f"{whole}⅓ IP"
    if text.endswith(".2"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅔ IP" if whole == 0 else f"{whole}⅔ IP"
    return f"{text} IP"


def format_ip_for_summary(ip: str) -> str:
    outs = baseball_ip_to_outs(ip)

    if outs == 1:
        return "one out"
    if outs == 2:
        return "two outs"
    if outs == 3:
        return "an inning"
    if outs == 6:
        return "two innings"

    text = str(ip).strip()
    if text.endswith(".0"):
        whole = safe_int(float(text), 0)
        return f"{whole} innings"

    if text.endswith(".1"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅓ of an inning" if whole == 0 else f"{whole}⅓ innings"

    if text.endswith(".2"):
        whole = safe_int(text.split(".")[0], 0)
        return "⅔ of an inning" if whole == 0 else f"{whole}⅔ innings"

    return f"{text} innings"


def format_game_line(s):
    return f"{format_ip_for_line(s['ip'])} • {s['h']} H • {s['er']} ER • {s['bb']} BB • {s['k']} K"


def format_pitch_count(s):
    pitches = safe_int(s.get("pitches", 0))
    strikes = safe_int(s.get("strikes", 0))
    if pitches <= 0:
        return "N/A"
    if strikes <= 0:
        return f"{pitches} pitches"
    return f"{pitches} pitches • {strikes} strikes"


def format_season_line(season):
    wins = safe_int(season.get("wins", 0))
    losses = safe_int(season.get("losses", 0))
    saves = safe_int(season.get("saves", 0))
    holds = safe_int(season.get("holds", 0))
    strikeouts = safe_int(season.get("strikeOuts", 0))

    era = season.get("era")
    if era in (None, "", "-"):
        era = season.get("earnedRunAverage", "0.00")
    try:
        era = f"{float(era):.2f}"
    except Exception:
        era = "0.00"

    season_ip = season.get("inningsPitched", "0.0")
    season_outs = baseball_ip_to_outs(str(season_ip))
    hits = safe_int(season.get("hits", 0))
    walks = safe_int(season.get("baseOnBalls", 0))

    whip = season.get("whip")
    if whip in (None, "", "-"):
        if season_outs > 0:
            ip_float = season_outs / 3.0
            whip = f"{((hits + walks) / ip_float):.2f}"
        else:
            whip = "0.00"
    else:
        try:
            whip = f"{float(whip):.2f}"
        except Exception:
            whip = "0.00"

    k9 = season.get("strikeoutsPer9Inn")
    if k9 in (None, "", "-"):
        if season_outs > 0:
            ip_float = season_outs / 3.0
            k9 = f"{(strikeouts * 9 / ip_float):.1f}"
        else:
            k9 = "0.0"
    else:
        try:
            k9 = f"{float(k9):.1f}"
        except Exception:
            k9 = "0.0"

    parts = [f"{wins}-{losses}"]

    if saves > 0:
        parts.append(f"{saves} SV")
    if holds > 0:
        parts.append(f"{holds} HLD")

    parts.extend([
        f"{era} ERA",
        f"{strikeouts} K",
        f"{whip} WHIP",
        f"{k9} K/9",
    ])

    return " • ".join(parts)


def build_score_line(away_abbr, away_score, home_abbr, home_score):
    if away_score > home_score:
        return f"**{away_abbr} {away_score}** - {home_abbr} {home_score}"
    if home_score > away_score:
        return f"{away_abbr} {away_score} - **{home_abbr} {home_score}**"
    return f"{away_abbr} {away_score} - {home_abbr} {home_score}"


# ---------------- CLASSIFICATION ----------------

def classify(s):
    if s.get("saves"):
        return "SAVE"
    if s.get("blownSaves"):
        return "BLOWN"
    if s.get("holds"):
        return "HOLD"
    if s["er"] == 0 and s["h"] == 0 and s["bb"] == 0:
        return "DOM"
    if s["er"] == 0 and (s["h"] + s["bb"]) >= 2:
        return "TRAFFIC"
    if s["er"] >= 3:
        return "ROUGH"
    return "CLEAN"


def impact_tag(label):
    return {
        "SAVE": "🔒 Locked it down",
        "BLOWN": "💥 Lead blown",
        "HOLD": "🧱 Held the line",
        "DOM": "🔥 Dominant outing",
        "TRAFFIC": "⚠️ Escaped trouble",
        "ROUGH": "💀 Rough outing",
        "CLEAN": "🧊 Clean inning",
    }.get(label, "⚾ Relief outing")


# ---------------- ENTRY CONTEXT ----------------

def get_pitcher_entry_context(feed, pitcher_id: int, pitcher_side: str):
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    if not plays:
        return {
            "entry_phrase": "",
            "entry_outs_text": "",
            "entry_state_text": "",
            "entry_inning": None,
            "finished_game": False,
        }

    pitcher_indices = []
    for idx, play in enumerate(plays):
        matchup = play.get("matchup", {})
        pitcher = matchup.get("pitcher", {})
        if pitcher.get("id") == pitcher_id:
            pitcher_indices.append(idx)

    if not pitcher_indices:
        return {
            "entry_phrase": "",
            "entry_outs_text": "",
            "entry_state_text": "",
            "entry_inning": None,
            "finished_game": False,
        }

    first_idx = pitcher_indices[0]
    last_idx = pitcher_indices[-1]
    first_play = plays[first_idx]

    about = first_play.get("about", {})
    inning = about.get("inning")
    half = about.get("halfInning", "")
    outs = safe_int(first_play.get("count", {}).get("outs"), 0)

    entry_phrase = ""
    if inning is not None and half:
        entry_phrase = f"the {half.lower()} of the {ordinal(inning)}"

    if outs == 0:
        entry_outs_text = "with nobody out"
    elif outs == 1:
        entry_outs_text = "with one out"
    else:
        entry_outs_text = "with two outs"

    # score before the first play by this pitcher
    if first_idx > 0:
        prev_result = plays[first_idx - 1].get("result", {})
        prev_away = safe_int(prev_result.get("awayScore"), 0)
        prev_home = safe_int(prev_result.get("homeScore"), 0)
    else:
        prev_away = 0
        prev_home = 0

    if pitcher_side == "home":
        team_score = prev_home
        opp_score = prev_away
    else:
        team_score = prev_away
        opp_score = prev_home

    diff = team_score - opp_score
    if diff > 0:
        if diff == 1:
            state_text = "protecting a one-run lead"
        elif diff == 2:
            state_text = "protecting a two-run lead"
        elif diff == 3:
            state_text = "protecting a three-run lead"
        else:
            state_text = f"protecting a {diff}-run lead"
    elif diff < 0:
        deficit = abs(diff)
        if deficit == 1:
            state_text = "with his club trailing by a run"
        elif deficit == 2:
            state_text = "with his club trailing by two"
        else:
            state_text = f"with his club trailing by {deficit}"
    else:
        state_text = "with the game tied"

    return {
        "entry_phrase": entry_phrase,
        "entry_outs_text": entry_outs_text,
        "entry_state_text": state_text,
        "entry_inning": inning,
        "finished_game": (last_idx == len(plays) - 1),
    }


# ---------------- ELITE SUMMARY ----------------

def build_summary(name, team, role, s, label, context):
    ip_text = format_ip_for_summary(s["ip"])
    outs_recorded = baseball_ip_to_outs(s["ip"])
    er = s["er"]
    h = s["h"]
    bb = s["bb"]
    k = s["k"]

    entry_phrase = context.get("entry_phrase", "")
    entry_outs_text = context.get("entry_outs_text", "")
    entry_state_text = context.get("entry_state_text", "")
    finished_game = context.get("finished_game", False)

    context_bits = []
    if entry_phrase:
        context_bits.append(f"in {entry_phrase}")
    if entry_outs_text:
        context_bits.append(entry_outs_text)
    if entry_state_text:
        context_bits.append(entry_state_text)

    if context_bits:
        opening_context = " ".join([context_bits[0], *[f", {bit}" for bit in context_bits[1:]]]).replace(" ,", ",")
    else:
        opening_context = "in relief"

    role_text = ""
    if role and role != "Tracked":
        role_text = f" in his {role.lower()} role"

    if label == "SAVE":
        if outs_recorded >= 6:
            line1 = f"{name} entered {opening_context} for {team}{role_text} and covered the final {ip_text} to earn the save."
        elif finished_game and context.get("entry_inning") == 9:
            line1 = f"{name} entered {opening_context} for {team}{role_text} and shut the door for the save."
        else:
            line1 = f"{name} entered {opening_context} for {team}{role_text} and locked down the save."

    elif label == "BLOWN":
        line1 = f"{name} entered {opening_context} for {team}{role_text} but couldn’t hold the lead and was charged with a blown save."

    elif label == "HOLD":
        line1 = f"{name} entered {opening_context} for {team}{role_text} and protected the lead to earn the hold."

    elif label == "DOM":
        line1 = f"{name} entered {opening_context} for {team}{role_text} and dominated."

    elif label == "TRAFFIC":
        line1 = f"{name} entered {opening_context} for {team}{role_text} and worked through traffic to keep the game under control."

    elif label == "ROUGH":
        line1 = f"{name} entered {opening_context} for {team}{role_text} but was hit hard in a rough outing."

    else:
        line1 = f"{name} entered {opening_context} for {team}{role_text} and turned in a scoreless outing."

    if er == 0 and h == 0 and bb == 0:
        if k >= 2:
            line2 = f"He retired all hitters he faced over {ip_text} and struck out {k}."
        else:
            line2 = f"He retired all hitters he faced over {ip_text}."
    elif er == 0:
        if k >= 2:
            line2 = f"He worked {ip_text}, allowing {h} hits and {bb} walks while striking out {k}."
        else:
            line2 = f"He worked {ip_text}, allowing {h} hits and {bb} walks."
    else:
        if k >= 2:
            line2 = f"He allowed {er} runs over {ip_text} on {h} hits and {bb} walks, striking out {k}."
        else:
            line2 = f"He allowed {er} runs over {ip_text} on {h} hits and {bb} walks."

    return f"{line1} {line2}"


# ---------------- CORE ----------------

def get_games():
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)

    games = []
    for d in [today, yesterday]:
        try:
            r = requests.get(f"{SCHEDULE_URL}&date={d}", timeout=30)
            r.raise_for_status()
            data = r.json()
            for date_block in data.get("dates", []):
                games.extend(date_block.get("games", []))
        except Exception:
            continue

    return games


def get_feed(game_id):
    r = requests.get(LIVE_URL.format(game_id), timeout=30)
    r.raise_for_status()
    return r.json()


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

            season_stats_block = p.get("seasonStats", {})
            if isinstance(season_stats_block, dict) and "pitching" in season_stats_block:
                season_stats = season_stats_block.get("pitching", {})
            elif isinstance(season_stats_block, dict):
                season_stats = season_stats_block
            else:
                season_stats = {}

            result.append({
                "id": p.get("person", {}).get("id"),
                "name": p.get("person", {}).get("fullName", "Unknown Pitcher"),
                "team": team,
                "side": side,
                "stats": stats,
                "season_stats": season_stats,
            })

    return result


# ---------------- POST ----------------

async def post_card(channel, p, matchup, score, tracked_info, context):
    stats = p["stats"]

    s = {
        "ip": str(stats.get("inningsPitched", "0.0")),
        "h": safe_int(stats.get("hits"), 0),
        "er": safe_int(stats.get("earnedRuns"), 0),
        "bb": safe_int(stats.get("baseOnBalls"), 0),
        "k": safe_int(stats.get("strikeOuts"), 0),
        "pitches": safe_int(stats.get("numberOfPitches"), 0),
        "strikes": safe_int(stats.get("strikes"), 0),
        "saves": safe_int(stats.get("saves"), 0),
        "holds": safe_int(stats.get("holds"), 0),
        "blownSaves": safe_int(stats.get("blownSaves"), 0),
    }

    label = classify(s)
    role = tracked_info.get("role", "Tracked") if tracked_info else ""

    prefix = ""
    if label == "SAVE":
        prefix = "🚨 SAVE — "
    elif label == "BLOWN":
        prefix = "⚠️ BLOWN SAVE — "

    embed = discord.Embed(
        title=f"{prefix}{p['name']} ({p['team']})",
        color=TEAM_COLORS.get(p["team"], 0x2ECC71),
        timestamp=datetime.now(timezone.utc),
    )

    try:
        embed.set_thumbnail(url=get_logo(p["team"]))
    except Exception:
        pass

    embed.add_field(name="", value=f"**{impact_tag(label)}**", inline=False)
    embed.add_field(name="⚾ Matchup", value=matchup, inline=False)

    if tracked_info:
        embed.add_field(name="Role", value=tracked_info.get("role", "Tracked"), inline=False)

    embed.add_field(name="Game Line", value=format_game_line(s), inline=False)
    embed.add_field(name="Pitch Count", value=format_pitch_count(s), inline=False)
    embed.add_field(name="Season", value=format_season_line(p.get("season_stats", {})), inline=False)
    embed.add_field(
        name="Summary",
        value=build_summary(p["name"], p["team"], role, s, label, context),
        inline=False,
    )
    embed.add_field(name="Final Score", value=score, inline=False)

    await channel.send(embed=embed)


# ---------------- LOOP ----------------

async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(CHANNEL_ID)

    state = load_state()
    posted = set(state.get("posted", []))
    tracked = build_tracked_relief_map()

    if RESET_CLOSER_STATE:
        print("[CLOSER] RESET_CLOSER_STATE enabled — posted state cleared for this run.", flush=True)

    while True:
        try:
            games = get_games()
            print(f"[CLOSER] Checking {len(games)} games", flush=True)

            for g in games:
                if g.get("status", {}).get("detailedState") != "Final":
                    continue

                game_id = g.get("gamePk")
                if not game_id:
                    continue

                feed = get_feed(game_id)
                pitchers = get_pitchers(feed)

                away = g.get("teams", {}).get("away", {}).get("team", {})
                home = g.get("teams", {}).get("home", {}).get("team", {})

                away_abbr = away.get("abbreviation") or away.get("name", "AWAY")[:3].upper()
                home_abbr = home.get("abbreviation") or home.get("name", "HOME")[:3].upper()

                away_score = safe_int(g.get("teams", {}).get("away", {}).get("score"), 0)
                home_score = safe_int(g.get("teams", {}).get("home", {}).get("score"), 0)

                matchup = f"{away_abbr} @ {home_abbr}"
                score = build_score_line(away_abbr, away_score, home_abbr, home_score)

                for p in pitchers:
                    pitcher_id = p.get("id")
                    if pitcher_id is None:
                        continue

                    key = f"{game_id}_{pitcher_id}"
                    if key in posted:
                        continue

                    norm = normalize_name(p["name"])
                    tracked_info = tracked.get(norm)
                    is_save = safe_int(p["stats"].get("saves"), 0) > 0
                    is_tracked = tracked_info is not None

                    if not (is_save or is_tracked):
                        continue

                    context = get_pitcher_entry_context(feed, pitcher_id, p["side"])

                    print(f"[POST] {p['name']} | {p['team']} | {matchup}", flush=True)
                    await post_card(channel, p, matchup, score, tracked_info, context)
                    posted.add(key)

            state["posted"] = list(posted)
            save_state(state)

        except Exception as e:
            print("[CLOSER] Loop error:", e, flush=True)

        await asyncio.sleep(POLL_MINUTES * 60)


# ---------------- START ----------------

intents = discord.Intents.default()
client = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f"Logged in as {client.user}", flush=True)
    asyncio.create_task(loop())


client.run(TOKEN)

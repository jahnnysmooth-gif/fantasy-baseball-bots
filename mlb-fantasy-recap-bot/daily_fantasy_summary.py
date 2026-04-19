import os
import json as json_lib
import time
import random
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

LOGO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "our_logo_1.png")

NY_TZ = ZoneInfo("America/New_York")

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

DAY_COLORS = {
    0: 0x2ECC71,   # Monday    — green
    1: 0xF1C40F,   # Tuesday   — yellow
    2: 0xE67E22,   # Wednesday — orange
    3: 0x9B59B6,   # Thursday  — purple
    4: 0xFF69B4,   # Friday    — pink
    5: 0x3498DB,   # Saturday  — blue
    6: 0xE74C3C,   # Sunday    — red
}

FIELD_LIMIT = 1024
TOTAL_EMBED_TEXT_LIMIT = 6000


def get_target_date():
    now_ny = datetime.now(NY_TZ)
    return (now_ny - timedelta(days=1)).date().isoformat()


def get_schedule_for_date(date_str):
    r = requests.get(
        SCHEDULE_URL,
        params={"sportId": 1, "date": date_str},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    games = []
    for date_block in data.get("dates", []):
        games.extend(date_block.get("games", []))
    return games


def get_live_feed(game_pk):
    r = requests.get(LIVE_FEED_URL.format(game_pk=game_pk), timeout=30)
    r.raise_for_status()
    return r.json()


def safe_int(value):
    try:
        return int(value or 0)
    except Exception:
        return 0


def safe_float(value):
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def innings_to_outs(ip):
    try:
        ip_str = str(ip or "0")
        if "." in ip_str:
            whole, frac = ip_str.split(".", 1)
            return int(whole) * 3 + int(frac)
        return int(ip_str) * 3
    except Exception:
        return 0


def collect_statcast_notes(feed):
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])

    hardest_hits = []
    fastest_pitches = []

    for play in plays:
        matchup = play.get("matchup", {}) or {}
        batter = matchup.get("batter", {}) or {}
        pitcher = matchup.get("pitcher", {}) or {}

        batter_name = batter.get("fullName", "Unknown Player")
        pitcher_name = pitcher.get("fullName", "Unknown Player")

        play_events = play.get("playEvents", []) or []
        last_event = play_events[-1] if play_events else {}
        play_hit_data = play.get("hitData", {}) or last_event.get("hitData", {}) or {}

        launch_speed = safe_float(play_hit_data.get("launchSpeed"))
        result = play.get("result", {}) or {}
        hit_type = (result.get("event") or "").strip()

        HITS = {"Single", "Double", "Triple", "Home Run"}
        if launch_speed >= 100 and hit_type in HITS:
            hardest_hits.append(
                {
                    "name": batter_name,
                    "ev": launch_speed,
                    "hit_type": hit_type,
                }
            )

        for event in play_events:
            details = event.get("details", {}) or {}
            pitch_data = event.get("pitchData", {}) or {}
            start_speed = safe_float(pitch_data.get("startSpeed"))
            pitch_type = details.get("type", {}) or {}
            pitch_name = pitch_type.get("description") or details.get("description") or "Pitch"

            if start_speed > 0:
                fastest_pitches.append(
                    {
                        "name": pitcher_name,
                        "velo": start_speed,
                        "pitch_type": pitch_name,
                    }
                )

    hardest_hits.sort(key=lambda x: x["ev"], reverse=True)

    fastest_pitches.sort(
        key=lambda x: x["velo"],
        reverse=True,
    )

    unique_hardest_hits = []
    seen_hard_hits = set()
    for item in hardest_hits:
        key = (item["name"], round(item["ev"], 1), item["hit_type"])
        if key not in seen_hard_hits:
            seen_hard_hits.add(key)
            unique_hardest_hits.append(item)

    unique_fastest_pitches = []
    seen_fast_pitches = set()
    for item in fastest_pitches:
        key = (item["name"], round(item["velo"], 1), item["pitch_type"])
        if key not in seen_fast_pitches:
            seen_fast_pitches.add(key)
            unique_fastest_pitches.append(item)

    return {
        "hardest_hits": unique_hardest_hits[:3],
        "fastest_pitches": unique_fastest_pitches[:3],
    }


def collect_game_notes(feed):
    boxscore = feed.get("liveData", {}).get("boxscore", {})
    teams = boxscore.get("teams", {})

    game_data = feed.get("gameData", {}) or {}
    gd_teams = game_data.get("teams", {}) or {}

    away_team_name = (
    gd_teams.get("away", {}).get("abbreviation")
    or teams.get("away", {}).get("team", {}).get("abbreviation")
    or teams.get("away", {}).get("team", {}).get("name")
    or "AWAY"
    )
    home_team_name = (
    gd_teams.get("home", {}).get("abbreviation")
    or teams.get("home", {}).get("team", {}).get("abbreviation")
    or teams.get("home", {}).get("team", {}).get("name")
    or "HOME"
    )

    away_players = teams.get("away", {}).get("players", {})

    home_players = teams.get("home", {}).get("players", {})
    hitters = []
    multi_hr = []
    multi_sb = []
    saves = []
    blown_saves = []
    dominant_relief = []
    holds = []
    pitchers = []

    all_players = []

    for player in away_players.values():
        all_players.append((player, away_team_name))

    for player in home_players.values():
        all_players.append((player, home_team_name))

    for player, team in all_players:
        person = player.get("person", {})
        name = person.get("fullName", "Unknown Player")

        stats = player.get("stats", {})
        batting = stats.get("batting", {})
        pitching = stats.get("pitching", {})

        hits = safe_int(batting.get("hits"))
        hr = safe_int(batting.get("homeRuns"))
        rbi = safe_int(batting.get("rbi"))
        runs = safe_int(batting.get("runs"))
        sb = safe_int(batting.get("stolenBases"))

        score = (hr * 10) + (rbi * 3) + (runs * 2) + (sb * 5) + hits

        if hits > 0 or hr > 0 or rbi > 0 or runs > 0 or sb > 0:
            hitters.append(
                {
                    "name": name,
                    "team": team,
                    "hr": hr,
                    "rbi": rbi,
                    "runs": runs,
                    "sb": sb,
                    "hits": hits,
                    "score": score,
                }
            )

        if hr >= 2:
            multi_hr.append(
                {
                    "name": name,
                    "team": team,
                    "hr": hr,
                    "rbi": rbi,
                    "runs": runs,
                    "hits": hits,
                }
            )

        if sb >= 2:
            multi_sb.append(
                {
                    "name": name,
                    "team": team,
                    "sb": sb,
                    "runs": runs,
                    "hits": hits,
                }
            )

        innings_pitched = pitching.get("inningsPitched")
        strikeouts = safe_int(pitching.get("strikeOuts"))
        earned_runs = safe_int(pitching.get("earnedRuns"))
        wins = safe_int(pitching.get("wins"))
        losses = safe_int(pitching.get("losses"))
        hits_allowed = safe_int(pitching.get("hits"))
        walks = safe_int(pitching.get("baseOnBalls"))
        games_started = safe_int(pitching.get("gamesStarted"))
        saves_stat = safe_int(pitching.get("saves"))
        holds_stat = safe_int(pitching.get("holds"))
        outs_recorded = innings_to_outs(innings_pitched)

        if saves_stat >= 1 and strikeouts >= 2:
            saves.append(
                {
                    "name": name,
                    "team": team,
                    "k": strikeouts,
                    "ip": innings_pitched,
                }
            )

        if holds_stat >= 1 and strikeouts >= 2 and walks <= 1:
            holds.append(
                {
                    "name": name,
                    "team": team,
                    "ip": innings_pitched,
                    "k": strikeouts,
                    "h": hits_allowed,
                    "bb": walks,
                }
            )

        if (
            games_started == 0
            and outs_recorded >= 3
            and strikeouts >= 3
            and earned_runs == 0
        ):
            dominant_relief.append(
                {
                    "name": name,
                    "team": team,
                    "ip": innings_pitched,
                    "k": strikeouts,
                    "h": hits_allowed,
                    "bb": walks,
                }
            )

        if innings_pitched:
            pitcher_score = (
                safe_float(innings_pitched) * 3
                + strikeouts * 2
                - earned_runs * 3
                - walks
                - hits_allowed * 0.5
                + wins * 2
                - losses * 2
            )

            pitchers.append(
                {
                    "name": name,
                    "team": team,
                    "ip": innings_pitched,
                    "k": strikeouts,
                    "er": earned_runs,
                    "h": hits_allowed,
                    "bb": walks,
                    "score": pitcher_score,
                }
            )

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    blown_save_pitchers = set()

    for play in plays:
        about = play.get("about", {}) or {}
        inning = safe_int(about.get("inning"))
        result = play.get("result", {}) or {}
        event = (result.get("event") or "").lower()

        if inning < 7:
            continue

        if "blown save" not in event:
            continue

        matchup = play.get("matchup", {}) or {}
        pitcher = matchup.get("pitcher", {}) or {}
        pitcher_name = pitcher.get("fullName")

        if pitcher_name:
            blown_save_pitchers.add(pitcher_name)

    for player, team in all_players:
        person = player.get("person", {})
        name = person.get("fullName", "Unknown Player")
        pitching = player.get("stats", {}).get("pitching", {})
        player_blown_saves = safe_int(pitching.get("blownSaves"))

        if name in blown_save_pitchers and player_blown_saves >= 1:
            blown_saves.append(
                {
                    "name": name,
                    "team": team,
                    "ip": pitching.get("inningsPitched", "0.0"),
                    "er": safe_int(pitching.get("earnedRuns")),
                    "h": safe_int(pitching.get("hits")),
                    "bb": safe_int(pitching.get("baseOnBalls")),
                    "k": safe_int(pitching.get("strikeOuts")),
                }
            )

    statcast_notes = collect_statcast_notes(feed)

    dominant_relief.sort(
        key=lambda x: (x["k"], innings_to_outs(x["ip"]), -x["bb"], -x["h"]),
        reverse=True,
    )

    holds.sort(
        key=lambda x: (x["k"], innings_to_outs(x["ip"]), -x["h"]),
        reverse=True,
    )

    return {
        "hitters": hitters,
        "multi_hr": multi_hr,
        "multi_sb": multi_sb,
        "saves": saves,
        "blown_saves": blown_saves,
        "dominant_relief": dominant_relief[:5],
        "holds": holds[:5],
        "pitchers": pitchers,
        "hardest_hits": statcast_notes["hardest_hits"],
        "fastest_pitches": statcast_notes["fastest_pitches"],
    }


def build_summary_data(date_str):
    games = get_schedule_for_date(date_str)

    all_hitters = []
    all_multi_hr = []
    all_multi_sb = []
    all_saves = []
    all_blown_saves = []
    all_dominant_relief = []
    all_holds = []
    all_pitchers = []
    all_hardest_hits = []
    all_fastest_pitches = []

    for game in games:
        game_pk = game.get("gamePk")
        status = game.get("status", {}).get("detailedState", "")

        if status not in {"Final", "Game Over", "Completed Early"}:
            continue

        try:
            feed = get_live_feed(game_pk)
            notes = collect_game_notes(feed)

            all_hitters.extend(notes["hitters"])
            all_multi_hr.extend(notes["multi_hr"])
            all_multi_sb.extend(notes["multi_sb"])
            all_saves.extend(notes["saves"])
            all_blown_saves.extend(notes["blown_saves"])
            all_dominant_relief.extend(notes["dominant_relief"])
            all_holds.extend(notes["holds"])
            all_pitchers.extend(notes["pitchers"])
            all_hardest_hits.extend(notes["hardest_hits"])
            all_fastest_pitches.extend(notes["fastest_pitches"])

        except Exception as e:
            print(f"Error processing game {game_pk}: {e}")

    all_hitters.sort(
        key=lambda x: (x["score"], x["hr"], x["rbi"], x["sb"], x["runs"], x["hits"]),
        reverse=True,
    )

    all_multi_hr.sort(
        key=lambda x: (x["hr"], x["rbi"], x["runs"], x["hits"]),
        reverse=True,
    )

    all_multi_sb.sort(
        key=lambda x: (x["sb"], x["runs"], x["hits"]),
        reverse=True,
    )

    all_pitchers.sort(
        key=lambda x: (x["score"], safe_float(x["ip"]), x["k"], -x["er"]),
        reverse=True,
    )

    all_dominant_relief.sort(
        key=lambda x: (x["k"], innings_to_outs(x["ip"]), -x["bb"], -x["h"]),
        reverse=True,
    )

    all_holds.sort(
        key=lambda x: (x["k"], innings_to_outs(x["ip"]), -x["h"]),
        reverse=True,
    )

    all_hardest_hits.sort(key=lambda x: x["ev"], reverse=True)

    all_fastest_pitches.sort(
        key=lambda x: x["velo"],
        reverse=True,
    )

    unique_hardest_hits = []
    seen_hard_hits = set()
    for item in all_hardest_hits:
        key = (item["name"], round(item["ev"], 1), item["hit_type"])
        if key not in seen_hard_hits:
            seen_hard_hits.add(key)
            unique_hardest_hits.append(item)

    unique_fastest_pitches = []
    seen_fast_pitches = set()
    for item in all_fastest_pitches:
        key = (item["name"], round(item["velo"], 1), item["pitch_type"])
        if key not in seen_fast_pitches:
            seen_fast_pitches.add(key)
            unique_fastest_pitches.append(item)

    return {
        "date": date_str,
        "top_hitters": all_hitters[:5],
        "multi_hr": all_multi_hr,
        "multi_sb": all_multi_sb,
        "saves": all_saves,
        "blown_saves": all_blown_saves,
        "dominant_relief": all_dominant_relief[:5],
        "holds": all_holds[:5],
        "best_pitchers": all_pitchers[:3],
        "hardest_hits": unique_hardest_hits[:3],
        "fastest_pitches": unique_fastest_pitches[:3],
    }


def trim_field_text(text, limit=FIELD_LIMIT):
    if not text:
        return "—"

    if len(text) <= limit:
        return text

    cutoff = text[: limit - 12]
    last_newline = cutoff.rfind("\n")

    if last_newline > 0:
        cutoff = cutoff[:last_newline]

    cutoff = cutoff.rstrip()
    return cutoff + "\n• ...and more"


def fmt_player_of_the_day(items):
    if not items:
        return "No standout hitter performance found."

    p = items[0]

    parts = []
    if p["hr"] > 0:
        parts.append(f'{p["hr"]} HR')
    if p["rbi"] > 0:
        parts.append(f'{p["rbi"]} RBI')
    if p["runs"] > 0:
        parts.append(f'{p["runs"]} R')
    if p["sb"] > 0:
        parts.append(f'{p["sb"]} SB')
    if p["hits"] > 0:
        parts.append(f'{p["hits"]} H')

    line = f'• **{p["name"]}** | {p["team"]}: {", ".join(parts)}'
    return trim_field_text(line)


def fmt_top_hitters(items):
    if not items:
        return "No notable hitter lines found."

    lines = []
    for p in items:
        parts = []
        if p["hr"] > 0:
            parts.append(f'{p["hr"]} HR')
        if p["rbi"] > 0:
            parts.append(f'{p["rbi"]} RBI')
        if p["runs"] > 0:
            parts.append(f'{p["runs"]} R')
        if p["sb"] > 0:
            parts.append(f'{p["sb"]} SB')
        if p["hits"] > 0:
            parts.append(f'{p["hits"]} H')

        lines.append(f'• **{p["name"]}** | {p["team"]}: {", ".join(parts)}')

    return trim_field_text("\n".join(lines))


def fmt_multi_hr(items):
    if not items:
        return "No multi-HR games."

    lines = []
    for p in items:
        parts = [f'{p["hr"]} HR']
        if p["rbi"] > 0:
            parts.append(f'{p["rbi"]} RBI')
        if p["runs"] > 0:
            parts.append(f'{p["runs"]} R')
        if p["hits"] > 0:
            parts.append(f'{p["hits"]} H')

        lines.append(f'• **{p["name"]}** | {p["team"]}: {", ".join(parts)}')

    return trim_field_text("\n".join(lines))


def fmt_multi_sb(items):
    if not items:
        return "No multi-SB games."

    lines = []
    for p in items:
        parts = [f'{p["sb"]} SB']
        if p["runs"] > 0:
            parts.append(f'{p["runs"]} R')
        if p["hits"] > 0:
            parts.append(f'{p["hits"]} H')

        lines.append(f'• **{p["name"]}** | {p["team"]}: {", ".join(parts)}')

    return trim_field_text("\n".join(lines))


def fmt_hardest_hits(items):
    if not items:
        return "No hard-hit ball data found."

    lines = []
    for item in items:
        lines.append(f'• **{item["name"]}**: {item["ev"]:.1f} mph EV — {item["hit_type"]}')

    return trim_field_text("\n".join(lines))


def fmt_pitcher_of_the_day(items):
    if not items:
        return "No standout pitching performance found."

    p = items[0]
    line = (
        f'• **{p["name"]}** | {p["team"]}: '
        f'{p["ip"]} IP, {p["k"]} K, {p["er"]} ER, {p["h"]} H, {p["bb"]} BB'
    )
    return trim_field_text(line)


def fmt_best_pitchers(items):
    if not items:
        return "No notable pitching lines."

    lines = []
    for p in items:
        lines.append(
            f'• **{p["name"]}** | {p["team"]}: '
            f'{p["ip"]} IP, {p["k"]} K, {p["er"]} ER, {p["h"]} H, {p["bb"]} BB'
        )
    return trim_field_text("\n".join(lines))


def fmt_saves(items):
    if not items:
        return "No impact saves."

    lines = []
    for p in items:
        lines.append(f'• **{p["name"]}** | {p["team"]}: {p["ip"]} IP, {p["k"]} K')

    return trim_field_text("\n".join(lines))


def fmt_holds(items):
    if not items:
        return "No holds with 2+ strikeouts and 0 walks."

    lines = []
    for p in items:
        lines.append(
            f'• **{p["name"]}** | {p["team"]}: '
            f'{p["ip"]} IP, {p["k"]} K, {p["h"]} H, {p["bb"]} BB'
        )

    return trim_field_text("\n".join(lines))


def fmt_dominant_relief(items):
    if not items:
        return "No dominant relief outings."

    lines = []
    for p in items:
        lines.append(
            f'• **{p["name"]}** | {p["team"]}: '
            f'{p["ip"]} IP, {p["k"]} K, {p["h"]} H, {p["bb"]} BB'
        )

    return trim_field_text("\n".join(lines))


def fmt_fastest_pitches(items):
    if not items:
        return "No pitch velocity data found."

    lines = []
    for item in items:
        lines.append(
            f'• **{item["name"]}**: {item["velo"]:.1f} mph ({item["pitch_type"]})'
        )

    return trim_field_text("\n".join(lines))


def fmt_blown_saves(items):
    if not items:
        return "No blown saves in the 7th inning or later."

    lines = []
    for p in items:
        lines.append(
            f'• **{p["name"]}** | {p["team"]}: '
            f'{p["ip"]} IP, {p["er"]} ER, {p["h"]} H, {p["bb"]} BB, {p["k"]} K'
        )

    return trim_field_text("\n".join(lines))


def estimate_embed_size(embed):
    total = 0
    total += len(embed.get("title", ""))
    total += len(embed.get("description", ""))
    total += len(embed.get("footer", {}).get("text", ""))

    for field in embed.get("fields", []):
        total += len(field.get("name", ""))
        total += len(field.get("value", ""))

    return total


def build_embeds(summary_data):
    date_obj = datetime.strptime(summary_data["date"], "%Y-%m-%d")
    pretty_date = date_obj.strftime("%A, %B %d, %Y").replace(" 0", " ")
    timestamp = datetime.now(timezone.utc).isoformat()
    embed_color = DAY_COLORS[date_obj.weekday()]

    thumbnail = {"url": "attachment://our_logo_1.png"}

    hitting_embed = {
        "title": "⚾ Fantasy Baseball Daily Hitting Recap",
        "description": f"**{pretty_date}**",
        "color": embed_color,
        "thumbnail": thumbnail,
        "fields": [
            {
                "name": "🏆 Fantasy Player of the Day",
                "value": fmt_player_of_the_day(summary_data["top_hitters"]),
                "inline": False,
            },
            {
                "name": "🔥 Top Hitters",
                "value": fmt_top_hitters(summary_data["top_hitters"]),
                "inline": False,
            },
            {
                "name": "💣 Multi-HR Games",
                "value": fmt_multi_hr(summary_data["multi_hr"]),
                "inline": False,
            },
            {
                "name": "💨 Multi-SB Games",
                "value": fmt_multi_sb(summary_data["multi_sb"]),
                "inline": False,
            },
            {
                "name": "🚀 Hardest-Hit Balls",
                "value": fmt_hardest_hits(summary_data["hardest_hits"]),
                "inline": False,
            },
        ],
        "footer": {"text": "Board Regs Fantasy Baseball Daily Recap"},
        "timestamp": timestamp,
    }

    pitching_embed = {
        "title": "🎯 Fantasy Baseball Daily Pitching Recap",
        "description": f"**{pretty_date}**",
        "color": embed_color,
        "thumbnail": thumbnail,
        "fields": [
            {
                "name": "🎯 Pitcher of the Day",
                "value": fmt_pitcher_of_the_day(summary_data["best_pitchers"]),
                "inline": False,
            },
            {
                "name": "🔥 Best Pitching Performances",
                "value": fmt_best_pitchers(summary_data["best_pitchers"]),
                "inline": False,
            },
            {
                "name": "💪 Dominant Relief Outings",
                "value": fmt_dominant_relief(summary_data["dominant_relief"]),
                "inline": False,
            },
            {
                "name": "⚡ Fastest Pitches Thrown",
                "value": fmt_fastest_pitches(summary_data["fastest_pitches"]),
                "inline": False,
            },
        ],
        "footer": {"text": "Board Regs Fantasy Baseball Daily Recap"},
        "timestamp": timestamp,
    }

    embeds = [hitting_embed, pitching_embed]

    for embed in embeds:
        size = estimate_embed_size(embed)
        if size > TOTAL_EMBED_TEXT_LIMIT:
            print(f"Warning: embed '{embed.get('title')}' estimated size is {size}, over limit.")

    return embeds


def get_retry_after_seconds(response):
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except Exception:
            pass

    try:
        data = response.json()
        return float(data.get("retry_after", 60))
    except Exception:
        return 60.0


def post_to_discord(embeds, max_retries=6):
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        raise RuntimeError("DISCORD_WEBHOOK_URL environment variable is not set")

    payload = {"embeds": embeds}
    has_logo = os.path.isfile(LOGO_PATH)

    for attempt in range(max_retries):
        if has_logo:
            with open(LOGO_PATH, "rb") as f:
                r = requests.post(
                    webhook_url,
                    data={"payload_json": json_lib.dumps(payload)},
                    files={"files[0]": ("our_logo_1.png", f, "image/png")},
                    timeout=30,
                )
        else:
            r = requests.post(webhook_url, json=payload, timeout=30)

        if r.status_code in (200, 204):
            print(f"Posted {len(embeds)} embeds successfully.")
            return True

        if r.status_code == 429:
            retry_after = get_retry_after_seconds(r)
            buffer_seconds = 10 + (attempt * 15)
            jitter = random.randint(0, 10)
            wait_time = retry_after + buffer_seconds + jitter

            print("429 raw Retry-After header:", r.headers.get("Retry-After"))
            print("429 body preview:", r.text[:300])
            print(
                f"Rate limited by Discord/Cloudflare. Waiting {wait_time:.1f} seconds "
                f"(attempt {attempt + 1}/{max_retries})"
            )

            time.sleep(wait_time)
            continue

        print(f"Discord post failed: {r.status_code} - {r.text[:1000]}")
        return False

    print("Webhook stayed unavailable after all retries.")
    return False


def main():
    time.sleep(random.randint(5, 30))

    target_date = get_target_date()
    summary_data = build_summary_data(target_date)

    print(f"Fantasy recap date: {target_date}")
    print(f"Top hitters found: {len(summary_data['top_hitters'])}")
    print(f"Multi-HR games found: {len(summary_data['multi_hr'])}")
    print(f"Multi-SB games found: {len(summary_data['multi_sb'])}")
    print(f"Pitching lines found: {len(summary_data['best_pitchers'])}")
    print(f"Impact saves found: {len(summary_data['saves'])}")
    print(f"Holds watch entries found: {len(summary_data['holds'])}")
    print(f"Dominant relief outings found: {len(summary_data['dominant_relief'])}")
    print(f"Fastest pitches found: {len(summary_data['fastest_pitches'])}")
    print(f"Blown saves found: {len(summary_data['blown_saves'])}")

    embeds = build_embeds(summary_data)
    post_to_discord(embeds)


if __name__ == "__main__":
    main()

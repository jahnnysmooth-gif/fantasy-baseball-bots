from __future__ import annotations

from typing import Any, Dict, List, Optional


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _to_ip_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    text = str(value).strip()
    if "." not in text:
        try:
            return float(int(text))
        except Exception:
            return 0.0
    left, right = text.split(".", 1)
    try:
        whole = int(left)
    except Exception:
        return 0.0
    if right == "0":
        return float(whole)
    if right == "1":
        return whole + (1.0 / 3.0)
    if right == "2":
        return whole + (2.0 / 3.0)
    try:
        return float(text)
    except Exception:
        return 0.0


def _has_any(text: str, candidates: List[str]) -> bool:
    lowered = (text or "").strip().lower()
    return any(c.lower() in lowered for c in candidates)


def _position_text(player: Optional[Dict[str, Any]], line: Optional[Dict[str, Any]]) -> str:
    pieces: List[str] = []
    for source in (player or {}, line or {}):
        for key in ("role", "position", "primary_position", "appearance_type"):
            value = source.get(key)
            if value:
                pieces.append(str(value).lower())
    return " | ".join(pieces)


def is_starter_appearance(player: Optional[Dict[str, Any]], line: Dict[str, Any]) -> bool:
    if _to_bool(line.get("is_starter")):
        return True
    if _to_bool(line.get("started_game")):
        return True
    if _to_int(line.get("games_started"), 0) > 0:
        return True
    text = _position_text(player, line)
    return _has_any(text, ["starter", "sp"])


def is_pitcher(player: Optional[Dict[str, Any]], line: Dict[str, Any]) -> bool:
    # Do not treat plain "k" as pitcher-only, because hitter lines also include strikeouts.
    # Lean on pitcher-specific stat fields and role/position text instead.
    pitcher_keys = (
        "ip",
        "er",
        "h_allowed",
        "bb_allowed",
        "pitches",
        "strikes",
        "outs_recorded",
        "batters_faced",
        "earned_runs",
        "runs_allowed",
    )
    if any(k in line for k in pitcher_keys):
        return True
    return _has_any(_position_text(player, line), ["pitcher", "sp", "rp", "reliever", "starter", "bullpen", "closer"])


def is_relief_appearance(player: Optional[Dict[str, Any]], line: Dict[str, Any]) -> bool:
    if not is_pitcher(player, line):
        return False
    return not is_starter_appearance(player, line)


def is_hitter_appearance(player: Optional[Dict[str, Any]], line: Dict[str, Any]) -> bool:
    if is_pitcher(player, line):
        return False
    if any(k in line for k in ("ab", "h", "2b", "3b", "hr", "rbi", "sb", "bb", "r", "k")):
        return True
    return _has_any(
        _position_text(player, line),
        ["hitter", "dh", "1b", "2b", "3b", "ss", "lf", "cf", "rf", "of", "c", "catcher"],
    )


def _recent_hitting_summary(recent_games: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    recent_games = recent_games or []
    ab = sum(_to_int(g.get("ab")) for g in recent_games)
    hits = sum(_to_int(g.get("h")) for g in recent_games)
    hr = sum(_to_int(g.get("hr")) for g in recent_games)
    doubles = sum(_to_int(g.get("2b")) for g in recent_games)
    triples = sum(_to_int(g.get("3b")) for g in recent_games)
    xbh = doubles + triples + hr
    rbi = sum(_to_int(g.get("rbi")) for g in recent_games)
    strikeouts = sum(_to_int(g.get("k")) for g in recent_games)
    hitless_tail = 0
    for game in reversed(recent_games):
        if _to_int(game.get("h")) == 0:
            hitless_tail += 1
        else:
            break
    avg = round(hits / ab, 3) if ab > 0 else None
    return {
        "games": len(recent_games),
        "ab": ab,
        "hits": hits,
        "hr": hr,
        "xbh": xbh,
        "rbi": rbi,
        "strikeouts": strikeouts,
        "hitless_tail": hitless_tail,
        "avg": avg,
    }


def _hitter_slump_flags(recent_games: Optional[List[Dict[str, Any]]], top_rank: Optional[int]) -> List[str]:
    if not top_rank:
        return []
    summary = _recent_hitting_summary(recent_games)
    flags: List[str] = []
    ab = summary["ab"]
    hits = summary["hits"]
    avg = summary["avg"]
    xbh = summary["xbh"]
    strikeouts = summary["strikeouts"]
    if ab >= 12 and hits <= 2:
        flags.append("two_or_fewer_hits_last_12_plus_ab")
    if avg is not None and ab >= 10 and avg <= 0.200:
        flags.append("recent_avg_under_200")
    if ab >= 12 and xbh == 0 and hits <= 3:
        flags.append("no_xbh_recent_skid")
    if summary["hitless_tail"] >= 2:
        flags.append("multi_game_hitless_skid")
    if ab >= 12 and strikeouts >= 6:
        flags.append("swing_and_miss_skid")
    return flags


def _strong_hitter_performance(line: Dict[str, Any]) -> List[str]:
    hits = _to_int(line.get("h"))
    ab = _to_int(line.get("ab"))
    doubles = _to_int(line.get("2b"))
    triples = _to_int(line.get("3b"))
    hr = _to_int(line.get("hr"))
    rbi = _to_int(line.get("rbi"))
    bb = _to_int(line.get("bb"))
    sb = _to_int(line.get("sb"))
    runs = _to_int(line.get("r"))
    xbh = doubles + triples + hr
    tags: List[str] = []
    if hr >= 2:
        tags.append("multi_homer_game")
    if hits >= 4:
        tags.append("four_hit_game")
    if hits >= 3:
        tags.append("three_hit_game")
    if xbh >= 2:
        tags.append("multiple_extra_base_hits")
    if rbi >= 4:
        tags.append("big_rbi_game")
    if hr >= 1 and (hits >= 2 or rbi >= 2 or xbh >= 2 or runs >= 2 or bb >= 1 or sb >= 1):
        tags.append("supported_homer")
    if sb >= 2 and hits >= 1:
        tags.append("impact_speed_game")
    if hits >= 2 and rbi >= 3:
        tags.append("multi_hit_run_production")
    if ab >= 5 and hits >= 3:
        tags.append("high_volume_hit_game")
    return tags


def is_postworthy_hitter_performance(
    player: Optional[Dict[str, Any]],
    line: Dict[str, Any],
    *,
    recent_games: Optional[List[Dict[str, Any]]] = None,
    top_rank: Optional[int] = None,
) -> Dict[str, Any]:
    ab = _to_int(line.get("ab"))
    hits = _to_int(line.get("h"))
    doubles = _to_int(line.get("2b"))
    triples = _to_int(line.get("3b"))
    hr = _to_int(line.get("hr"))
    rbi = _to_int(line.get("rbi"))
    bb = _to_int(line.get("bb"))
    sb = _to_int(line.get("sb"))
    k = _to_int(line.get("k"))
    xbh = doubles + triples + hr

    strong_tags = _strong_hitter_performance(line)
    if strong_tags:
        return {
            "post": True,
            "category": "HITTER",
            "reason": "strong_hitter_performance",
            "score": max(8, len(strong_tags) * 2),
            "details": strong_tags,
            "blurb_type": "performance_hitter",
            "top_rank": top_rank,
            "slump_flags": [],
        }

    slump_flags = _hitter_slump_flags(recent_games, top_rank)
    is_slumping = bool(slump_flags)

    # Solo homer only if it means something in a slump.
    if hr >= 1 and is_slumping:
        return {
            "post": True,
            "category": "HITTER",
            "reason": "slump_homer_signal",
            "score": 7,
            "details": ["solo_homer_in_slump"] + slump_flags,
            "blurb_type": "performance_hitter",
            "top_rank": top_rank,
            "slump_flags": slump_flags,
        }

    # Cold streak continuation for Top 300 hitters.
    if is_slumping:
        if ab >= 4 and hits == 0:
            return {
                "post": True,
                "category": "HITTER",
                "reason": "slump_continues_hitless",
                "score": 6,
                "details": ["hitless_slump_game"] + slump_flags,
                "blurb_type": "performance_hitter",
                "top_rank": top_rank,
                "slump_flags": slump_flags,
            }
        if ab >= 4 and hits == 1 and hr == 0 and xbh == 0 and k >= 3:
            return {
                "post": True,
                "category": "HITTER",
                "reason": "slump_continues_strikeouts",
                "score": 6,
                "details": ["thin_line_with_strikeouts"] + slump_flags,
                "blurb_type": "performance_hitter",
                "top_rank": top_rank,
                "slump_flags": slump_flags,
            }

    # Skip ordinary lines.
    if hits == 0 and hr == 0 and sb == 0 and rbi == 0 and bb == 0:
        return {
            "post": False,
            "category": "HITTER",
            "reason": "empty_offensive_line",
            "score": 0,
            "details": [],
            "blurb_type": None,
            "top_rank": top_rank,
            "slump_flags": slump_flags,
        }

    return {
        "post": False,
        "category": "HITTER",
        "reason": "ordinary_hitter_line",
        "score": 0,
        "details": strong_tags,
        "blurb_type": None,
        "top_rank": top_rank,
        "slump_flags": slump_flags,
    }


def is_postworthy_starter_performance(player: Optional[Dict[str, Any]], line: Dict[str, Any]) -> Dict[str, Any]:
    ip = _to_ip_float(line.get("ip"))
    er = _to_int(line.get("er"))
    k = _to_int(line.get("k"))
    details: List[str] = ["starter_full_coverage"]

    if ip <= 0.0:
        return {
            "post": False,
            "category": "STARTER",
            "reason": "no_recorded_outs",
            "score": 0,
            "details": [],
            "blurb_type": None,
        }

    if er == 0 and ip >= 6.0:
        details.append("dominant_scoreless_start")
    elif er <= 2 and ip >= 6.0:
        details.append("quality_run_prevention")
    elif er >= 5:
        details.append("rough_start")
    elif ip < 5.0:
        details.append("short_start")
    if k >= 8:
        details.append("bat_misser")

    return {
        "post": True,
        "category": "STARTER",
        "reason": "starter_full_coverage",
        "score": 5 + len(details),
        "details": details,
        "blurb_type": "performance_starter",
    }


def should_post_performance(
    player: Optional[Dict[str, Any]],
    line: Dict[str, Any],
    *,
    recent_games: Optional[List[Dict[str, Any]]] = None,
    top_rank: Optional[int] = None,
) -> Dict[str, Any]:
    if is_relief_appearance(player, line):
        return {
            "post": False,
            "category": "RELIEVER",
            "reason": "reliever_performance_blocked",
            "score": 0,
            "details": ["performance_blocked_for_relievers"],
            "blurb_type": None,
        }
    if is_starter_appearance(player, line):
        return is_postworthy_starter_performance(player, line)
    if is_hitter_appearance(player, line):
        return is_postworthy_hitter_performance(player, line, recent_games=recent_games, top_rank=top_rank)
    return {
        "post": False,
        "category": "UNKNOWN",
        "reason": "unknown_appearance_type",
        "score": 0,
        "details": [],
        "blurb_type": None,
    }

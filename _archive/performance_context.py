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


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _clean_sentence(text: Optional[str]) -> str:
    text = str(text or "").strip()
    if not text:
        return ""
    text = " ".join(text.split())
    text = text[:1].upper() + text[1:]
    if not text.endswith("."):
        text += "."
    return text


def _recent_hitting_summary(recent_games: List[Dict[str, Any]]) -> Dict[str, Any]:
    ab = sum(_to_int(g.get("ab")) for g in recent_games)
    hits = sum(_to_int(g.get("h")) for g in recent_games)
    hr = sum(_to_int(g.get("hr")) for g in recent_games)
    doubles = sum(_to_int(g.get("2b")) for g in recent_games)
    triples = sum(_to_int(g.get("3b")) for g in recent_games)
    xbh = doubles + triples + hr
    rbi = sum(_to_int(g.get("rbi")) for g in recent_games)
    walks = sum(_to_int(g.get("bb")) for g in recent_games)
    strikeouts = sum(_to_int(g.get("k")) for g in recent_games)
    avg = round(hits / ab, 3) if ab > 0 else None
    return {
        "games": len(recent_games),
        "ab": ab,
        "hits": hits,
        "hr": hr,
        "xbh": xbh,
        "rbi": rbi,
        "bb": walks,
        "k": strikeouts,
        "avg": avg,
    }


def _clean_hitter_impact(game_impact: Dict[str, Any]) -> str:
    raw = str((game_impact or {}).get("description") or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if game_impact.get("walk_off"):
        return "He delivered the walk-off swing."
    if game_impact.get("go_ahead_hit"):
        if "home run" in lowered or "homered" in lowered or "homers" in lowered:
            return "His homer put his club in front for good."
        return "He came through with the go-ahead hit."
    if game_impact.get("game_tying_hit"):
        if "home run" in lowered or "homered" in lowered or "homers" in lowered:
            return "His homer pulled the game even."
        return "He came through with the game-tying hit."
    if game_impact.get("late_inning_rbi_hit"):
        return "He also came through with a late RBI hit."
    if game_impact.get("grand_slam"):
        return "He punctuated the night with a grand slam."
    return ""


def _build_hitter_streak_note(recent_games: List[Dict[str, Any]], today_line: Dict[str, Any]) -> str:
    if not recent_games:
        return ""
    summary = _recent_hitting_summary(recent_games)
    total_hits = summary["hits"] + _to_int(today_line.get("h"))
    total_hr = summary["hr"] + _to_int(today_line.get("hr"))
    total_rbi = summary["rbi"] + _to_int(today_line.get("rbi"))
    games = summary["games"] + 1
    if games >= 5 and total_hits >= 8:
        return f"He has kept the barrel rolling with {total_hits} hits over his last {games} games."
    if games >= 5 and total_hr >= 3:
        return f"He has stayed hot enough to leave the yard {total_hr} times over his last {games} games."
    if games >= 5 and total_rbi >= 8:
        return f"He has kept the production coming with {total_rbi} RBI over his last {games} games."
    return ""


def _build_hitter_slump_note(recent_games: List[Dict[str, Any]], today_line: Dict[str, Any]) -> str:
    if not recent_games:
        return ""
    summary = _recent_hitting_summary(recent_games)
    if summary["ab"] <= 0:
        return ""
    today_ab = _to_int(today_line.get("ab"))
    today_hits = _to_int(today_line.get("h"))
    today_hr = _to_int(today_line.get("hr"))
    today_k = _to_int(today_line.get("k"))
    games = summary["games"]
    total_ab = summary["ab"] + today_ab
    total_hits = summary["hits"] + today_hits
    total_k = summary["k"] + today_k
    if today_hr >= 1:
        return f"He badly needed that swing after going {summary['hits']}-for-{summary['ab']} over his previous {games} games."
    if today_ab >= 4 and today_hits == 0 and today_k >= 2:
        return f"The rough patch continued, leaving him at {total_hits}-for-{total_ab} with {total_k} strikeouts over his last {games + 1} games."
    if today_ab >= 4 and today_hits == 0:
        return f"The slump stayed in place, leaving him at {total_hits}-for-{total_ab} across his last {games + 1} games."
    if today_ab >= 4 and today_hits == 1:
        return f"He is still just {total_hits}-for-{total_ab} over his last {games + 1} games."
    return ""


def _build_hitter_evaluation(line: Dict[str, Any], decision: Dict[str, Any]) -> str:
    reason = str((decision or {}).get("reason") or "")
    hr = _to_int(line.get("hr"))
    hits = _to_int(line.get("h"))
    rbi = _to_int(line.get("rbi"))
    sb = _to_int(line.get("sb"))
    k = _to_int(line.get("k"))
    xbh = _to_int(line.get("2b")) + _to_int(line.get("3b")) + hr
    if reason == "slump_homer_signal":
        return "It was the kind of swing that can matter more than the rest of the line."
    if reason == "slump_continues_hitless":
        return "For a fantasy-relevant bat, another empty box score still tells part of the story."
    if reason == "slump_continues_strikeouts":
        return "The swing-and-miss stayed attached to the line, which matters for fantasy players tracking the skid."
    if hr >= 2:
        return "That was one of the bigger power lines anywhere on the slate."
    if hits >= 4:
        return "He put together the kind of night where every trip to the plate felt productive."
    if hits >= 3 and rbi >= 3:
        return "It was a full fantasy line with both volume and impact."
    if hits >= 3 and xbh >= 2:
        return "The contact quality and the hit total both showed up in the same box score."
    if hits >= 3:
        return "He kept finding his way into the middle of the action."
    if hr >= 1 and rbi >= 3:
        return "The power showed up with real run-producing weight behind it."
    if xbh >= 2:
        return "The extra-base damage gave the whole line more shape."
    if sb >= 2:
        return "He added real category juice with his legs too."
    if k >= 3 and hits <= 1:
        return "The strikeout total kept some swing-and-miss concern attached to the night."
    return ""


def _pitch_efficiency_note(line: Dict[str, Any]) -> str:
    pitches = _to_int(line.get("pitches"))
    er = _to_int(line.get("er"))
    ip = str(line.get("ip") or "0.0")
    if pitches <= 0:
        return ""
    try:
        ip_float = float(ip.replace(".1", ".33").replace(".2", ".67"))
    except Exception:
        ip_float = 0.0
    if er >= 4:
        if pitches >= 90:
            return f"He still had to labor through {pitches} pitches before the outing finally ended."
        return ""
    if ip_float >= 6.0 and pitches <= 85:
        return f"He covered {ip} innings on just {pitches} pitches."
    if ip_float >= 5.0 and pitches <= 70:
        return f"He moved through the outing quickly, finishing {ip} innings in only {pitches} pitches."
    if pitches >= 100:
        return f"He had to grind through {pitches} pitches to finish the outing."
    if pitches >= 90:
        return f"He still needed {pitches} pitches to get through the outing."
    return ""


def _starter_form_note(previous_starts: List[Dict[str, Any]], today_line: Dict[str, Any]) -> str:
    if not previous_starts:
        return ""
    combined = list(previous_starts) + [today_line]
    last_three = combined[-3:]
    total_er = sum(_to_int(s.get("er")) for s in last_three)
    total_k = sum(_to_int(s.get("k")) for s in last_three)
    if len(last_three) == 3 and total_er <= 3:
        return f"He has now allowed only {total_er} earned runs across his last three starts."
    if len(last_three) == 3 and total_er >= 12:
        return f"He has been hit hard lately, allowing {total_er} earned runs over his last three starts."
    if len(last_three) == 3 and total_k >= 20:
        return f"He has also piled up {total_k} strikeouts over his last three starts."
    return ""


def _starter_evaluation_note(line: Dict[str, Any]) -> str:
    er = _to_int(line.get("er"))
    k = _to_int(line.get("k"))
    bb = _to_int(line.get("bb_allowed"))
    hits = _to_int(line.get("h_allowed"))
    ip = str(line.get("ip") or "0.0")
    try:
        ip_float = float(ip.replace(".1", ".33").replace(".2", ".67"))
    except Exception:
        ip_float = 0.0

    if er == 0 and ip_float >= 6.0 and k >= 8:
        return "It was a dominant blend of length, run prevention, and swing-and-miss stuff."
    if er == 0 and ip_float >= 5.0:
        return "He gave his club a clean scoreless turn."
    if er <= 2 and ip_float >= 6.0 and k >= 7:
        return "That is the kind of starter line that plays in any fantasy format."
    if er <= 2 and bb >= 4:
        return "The run prevention held even though the command wandered at times."
    if er <= 2 and hits <= 3:
        return "He did not give hitters much to square up all night."
    if er >= 6:
        return "It was the sort of blowup fantasy players need to flag right away."
    if er >= 4 and bb >= 3:
        return "He pitched in traffic most of the way and never really found a clean rhythm."
    if er >= 4:
        return "The outing slipped away before he could reset it."
    if k >= 8:
        return "Even with some traffic, the strikeout total still carried weight."
    if bb >= 4:
        return "The walks shaped the line almost as much as the contact."
    return ""


def get_hitter_performance_context(
    player: Dict[str, Any],
    line: Dict[str, Any],
    *,
    statcast: Optional[Dict[str, Any]] = None,
    recent_games: Optional[List[Dict[str, Any]]] = None,
    game_impact: Optional[Dict[str, Any]] = None,
    top_rank: Optional[int] = None,
    decision: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    statcast = statcast or {}
    recent_games = recent_games or []
    game_impact = game_impact or {}
    decision = decision or {}

    max_ev = _to_float(statcast.get("max_exit_velocity"))
    balls_hit_100 = _to_int(statcast.get("balls_hit_100_plus"), 0) or None
    impact_note = _clean_hitter_impact(game_impact)
    streak_note = _build_hitter_streak_note(recent_games, line)
    slump_note = _build_hitter_slump_note(recent_games, line) if decision.get("slump_flags") else ""
    evaluation_note = _build_hitter_evaluation(line, decision)

    summary_bits: Dict[str, Any] = {}
    if top_rank:
        summary_bits["top_rank"] = top_rank
    if max_ev is not None:
        summary_bits["max_exit_velocity"] = round(max_ev, 1)
    if balls_hit_100:
        summary_bits["balls_hit_100_plus"] = balls_hit_100
    if impact_note:
        summary_bits["impact_note"] = impact_note
    if streak_note:
        summary_bits["streak_note"] = streak_note
    if slump_note:
        summary_bits["slump_note"] = slump_note
    if evaluation_note:
        summary_bits["evaluation_note"] = evaluation_note

    priority_candidates = []
    if evaluation_note:
        priority_candidates.append(evaluation_note)
    if impact_note:
        priority_candidates.append(impact_note)
    if slump_note:
        priority_candidates.append(slump_note)
    if streak_note:
        priority_candidates.append(streak_note)

    return {
        "priority_note": _clean_sentence(priority_candidates[0]) if priority_candidates else "",
        "summary_bits": summary_bits,
    }


def get_starter_performance_context(
    player: Dict[str, Any],
    line: Dict[str, Any],
    *,
    velocity_data: Optional[Dict[str, Any]] = None,
    previous_starts: Optional[List[Dict[str, Any]]] = None,
    game_impact: Optional[Dict[str, Any]] = None,
    decision: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    velocity_data = velocity_data or {}
    previous_starts = previous_starts or []
    game_impact = game_impact or {}

    fb_avg = _to_float(velocity_data.get("fastball_avg"))
    fb_max = _to_float(velocity_data.get("fastball_max"))
    prev_velos: List[float] = []
    for start in previous_starts:
        prev = _to_float(start.get("fastball_avg"))
        if prev is not None:
            prev_velos.append(prev)
    fastball_delta: Optional[float] = None
    if fb_avg is not None and prev_velos:
        fastball_delta = round(fb_avg - (sum(prev_velos) / len(prev_velos)), 1)

    impact_note = _clean_sentence((game_impact or {}).get("description") or "")
    pitch_note = _pitch_efficiency_note(line)
    form_note = _starter_form_note(previous_starts, line)
    evaluation_note = _starter_evaluation_note(line)

    summary_bits: Dict[str, Any] = {"pitches": _to_int(line.get("pitches"))}
    if evaluation_note:
        summary_bits["evaluation_note"] = evaluation_note
    if pitch_note:
        summary_bits["pitch_note"] = pitch_note
    if impact_note:
        summary_bits["impact_note"] = impact_note
    if form_note:
        summary_bits["form_note"] = form_note
    if fb_avg is not None:
        summary_bits["fastball_avg"] = round(fb_avg, 1)
    if fb_max is not None:
        summary_bits["fastball_max"] = round(fb_max, 1)
    if fastball_delta is not None:
        summary_bits["fastball_delta"] = fastball_delta

    priority_candidates = []
    if evaluation_note:
        priority_candidates.append(evaluation_note)
    if impact_note:
        priority_candidates.append(impact_note)
    if form_note:
        priority_candidates.append(form_note)
    if pitch_note:
        priority_candidates.append(pitch_note)

    return {
        "priority_note": _clean_sentence(priority_candidates[0]) if priority_candidates else "",
        "summary_bits": summary_bits,
    }

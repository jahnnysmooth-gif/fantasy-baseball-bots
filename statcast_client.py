"""
statcast_client_v2.py

Improved Statcast / Baseball Savant helper layer for the performance bot.

What this version improves
--------------------------
- broader date-window fetches for both hitters and starters
- less brittle current-game detection
- safer fallback to most recent available Statcast date near the target date
- debug logging so you can see why context is or is not returning

Environment variables
---------------------
Optional:
- STATCAST_WINDOW_DAYS=14
- STATCAST_DEBUG=true
- STATCAST_CURRENT_GAME_FALLBACK_DAYS=2
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None

try:
    from pybaseball import statcast_batter, statcast_pitcher  # type: ignore
except Exception:
    statcast_batter = None
    statcast_pitcher = None


STATCAST_WINDOW_DAYS = int(os.getenv("STATCAST_WINDOW_DAYS", "14"))
STATCAST_DEBUG = os.getenv("STATCAST_DEBUG", "false").strip().lower() in {"1", "true", "yes", "y", "on"}
STATCAST_CURRENT_GAME_FALLBACK_DAYS = int(os.getenv("STATCAST_CURRENT_GAME_FALLBACK_DAYS", "2"))

STATCAST_FASTBALL_TYPES = {
    "FF",  # four-seam
    "SI",  # sinker
    "FC",  # cutter
    "FT",  # two-seam / historical
}


# ============================================================
# Generic helpers
# ============================================================
def _debug(msg: str) -> None:
    if STATCAST_DEBUG:
        print(f"[STATCAST DEBUG] {msg}")


def _normalize_date(value: Any) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value).strip()
    if len(text) >= 10:
        return text[:10]
    return text


def _has_pybaseball() -> bool:
    return statcast_batter is not None and statcast_pitcher is not None and pd is not None


def _get_column(df, *names: str):
    if df is None:
        return None
    for name in names:
        if name in df.columns:
            return name
    return None


def _safe_game_dates(df) -> List[str]:
    if df is None or len(df.index) == 0:
        return []
    game_date_col = _get_column(df, "game_date")
    if not game_date_col:
        return []
    try:
        return sorted({str(x)[:10] for x in df[game_date_col].dropna().tolist()})
    except Exception:
        return []


def _filter_by_player_id(df, player_id: int):
    if df is None or len(df.index) == 0:
        return df

    batter_col = _get_column(df, "batter")
    pitcher_col = _get_column(df, "pitcher")

    if batter_col and batter_col in df.columns:
        try:
            out = df[df[batter_col].astype(str) == str(int(player_id))]
            if len(out.index) > 0:
                return out
        except Exception:
            pass

    if pitcher_col and pitcher_col in df.columns:
        try:
            out = df[df[pitcher_col].astype(str) == str(int(player_id))]
            if len(out.index) > 0:
                return out
        except Exception:
            pass

    return df.iloc[0:0]


def _filter_by_game_date(df, game_date: str):
    if df is None or len(df.index) == 0:
        return df.iloc[0:0] if df is not None else df

    game_date_col = _get_column(df, "game_date")
    if not game_date_col:
        return df.iloc[0:0]

    try:
        return df[df[game_date_col].astype(str).str[:10] == game_date]
    except Exception:
        return df.iloc[0:0]


def _nearest_game_date_on_or_before(df, target_date: str, max_days_back: int = STATCAST_CURRENT_GAME_FALLBACK_DAYS) -> Optional[str]:
    dates = _safe_game_dates(df)
    if not dates:
        return None

    try:
        target = datetime.fromisoformat(target_date).date()
    except Exception:
        return None

    candidates: List[date] = []
    for d in dates:
        try:
            parsed = datetime.fromisoformat(d).date()
        except Exception:
            continue
        if parsed <= target and (target - parsed).days <= max_days_back:
            candidates.append(parsed)

    if not candidates:
        return None

    return max(candidates).isoformat()


def _filter_in_play_batted_balls(df):
    if df is None or len(df.index) == 0:
        return df

    type_col = _get_column(df, "type")
    launch_speed_col = _get_column(df, "launch_speed")
    events_col = _get_column(df, "events")

    out = df

    if launch_speed_col:
        try:
            out = out[out[launch_speed_col].notna()]
        except Exception:
            pass

    if type_col:
        try:
            out = out[out[type_col].astype(str).str.upper() == "X"]
        except Exception:
            pass

    if events_col and len(out.index) > 0:
        try:
            out = out[out[events_col].notna()]
        except Exception:
            pass

    return out


def _filter_fastballs(df):
    if df is None or len(df.index) == 0:
        return df

    pitch_type_col = _get_column(df, "pitch_type")
    if not pitch_type_col:
        return df.iloc[0:0]

    try:
        return df[df[pitch_type_col].astype(str).isin(STATCAST_FASTBALL_TYPES)]
    except Exception:
        return df.iloc[0:0]


def _mean(series) -> Optional[float]:
    try:
        if series is None or len(series) == 0:
            return None
        return round(float(series.mean()), 1)
    except Exception:
        return None


def _max(series) -> Optional[float]:
    try:
        if series is None or len(series) == 0:
            return None
        return round(float(series.max()), 1)
    except Exception:
        return None


def _get_recent_date_window(target_date: str, window_days: int = STATCAST_WINDOW_DAYS) -> tuple[str, str]:
    dt = datetime.fromisoformat(target_date)
    start = (dt - timedelta(days=window_days)).date().isoformat()
    end = dt.date().isoformat()
    return start, end


# ============================================================
# Hitter context
# ============================================================
def fetch_hitter_statcast_context(
    player_id: int,
    game_date: Any,
    game_pk: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Returns hitter exit velocity context for the target game date,
    with fallback to the most recent available Statcast date on or before target.
    """
    if not _has_pybaseball():
        _debug("pybaseball unavailable for hitter context")
        return {}

    target_date = _normalize_date(game_date)
    start_date, end_date = _get_recent_date_window(target_date)

    try:
        df = statcast_batter(start_date, end_date, int(player_id))
    except Exception as exc:
        _debug(f"hitter fetch failed player_id={player_id} target_date={target_date}: {exc}")
        return {}

    if df is None or len(df.index) == 0:
        _debug(f"hitter no rows player_id={player_id} target_date={target_date}")
        return {}

    df = _filter_by_player_id(df, int(player_id))
    if df is None or len(df.index) == 0:
        _debug(f"hitter no rows after player filter player_id={player_id} target_date={target_date}")
        return {}

    chosen_date = target_date
    game_df = _filter_by_game_date(df, target_date)

    if game_df is None or len(game_df.index) == 0:
        fallback_date = _nearest_game_date_on_or_before(df, target_date)
        if fallback_date:
            chosen_date = fallback_date
            game_df = _filter_by_game_date(df, fallback_date)
            _debug(f"hitter fallback date player_id={player_id} target_date={target_date} chosen_date={chosen_date}")
        else:
            _debug(f"hitter no game date match player_id={player_id} target_date={target_date} available_dates={_safe_game_dates(df)}")
            return {}

    game_df = _filter_in_play_batted_balls(game_df)
    if game_df is None or len(game_df.index) == 0:
        _debug(f"hitter no batted-ball rows player_id={player_id} chosen_date={chosen_date}")
        return {}

    launch_speed_col = _get_column(game_df, "launch_speed")
    if not launch_speed_col:
        _debug(f"hitter missing launch_speed column player_id={player_id} chosen_date={chosen_date}")
        return {}

    try:
        speeds = game_df[launch_speed_col].dropna().astype(float)
    except Exception:
        _debug(f"hitter launch_speed conversion failed player_id={player_id} chosen_date={chosen_date}")
        return {}

    if len(speeds) == 0:
        _debug(f"hitter empty speeds player_id={player_id} chosen_date={chosen_date}")
        return {}

    max_ev = _max(speeds)
    balls_hit_100_plus = int((speeds >= 100.0).sum())

    out: Dict[str, Any] = {}
    if max_ev is not None:
        out["max_exit_velocity"] = max_ev
    if balls_hit_100_plus > 0:
        out["balls_hit_100_plus"] = balls_hit_100_plus
    out["game_date_used"] = chosen_date

    _debug(
        f"hitter success player_id={player_id} target_date={target_date} chosen_date={chosen_date} "
        f"rows={len(game_df.index)} out={out}"
    )
    return out


# ============================================================
# Starter context
# ============================================================
def _single_start_fastball_context(df) -> Dict[str, Any]:
    if df is None or len(df.index) == 0:
        return {}

    release_speed_col = _get_column(df, "release_speed")
    if not release_speed_col:
        return {}

    fastballs = _filter_fastballs(df)
    if fastballs is None or len(fastballs.index) == 0:
        return {}

    try:
        speeds = fastballs[release_speed_col].dropna().astype(float)
    except Exception:
        return {}

    if len(speeds) == 0:
        return {}

    avg_fb = _mean(speeds)
    max_fb = _max(speeds)

    out: Dict[str, Any] = {}
    if avg_fb is not None:
        out["fastball_avg"] = avg_fb
    if max_fb is not None:
        out["fastball_max"] = max_fb
    return out


def fetch_starter_velocity_context(
    player_id: int,
    game_date: Any,
    lookback_starts: int = 3,
    game_pk: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Returns starter velo context for today's start, plus prior starts.
    Falls back to the nearest available Statcast date on or before target.
    """
    if not _has_pybaseball():
        _debug("pybaseball unavailable for starter context")
        return {}

    target_date = _normalize_date(game_date)
    start_date, end_date = _get_recent_date_window(target_date)

    try:
        df = statcast_pitcher(start_date, end_date, int(player_id))
    except Exception as exc:
        _debug(f"starter fetch failed player_id={player_id} target_date={target_date}: {exc}")
        return {}

    if df is None or len(df.index) == 0:
        _debug(f"starter no rows player_id={player_id} target_date={target_date}")
        return {}

    df = _filter_by_player_id(df, int(player_id))
    if df is None or len(df.index) == 0:
        _debug(f"starter no rows after player filter player_id={player_id} target_date={target_date}")
        return {}

    available_dates = _safe_game_dates(df)
    if not available_dates:
        _debug(f"starter no available game dates player_id={player_id} target_date={target_date}")
        return {}

    chosen_date = target_date
    current_df = _filter_by_game_date(df, target_date)

    if current_df is None or len(current_df.index) == 0:
        fallback_date = _nearest_game_date_on_or_before(df, target_date)
        if fallback_date:
            chosen_date = fallback_date
            current_df = _filter_by_game_date(df, fallback_date)
            _debug(f"starter fallback date player_id={player_id} target_date={target_date} chosen_date={chosen_date}")
        else:
            _debug(f"starter no current/fallback date match player_id={player_id} target_date={target_date} available_dates={available_dates}")
            return {}

    out: Dict[str, Any] = {}
    current_ctx = _single_start_fastball_context(current_df)
    if current_ctx:
        out.update(current_ctx)
        out["game_date_used"] = chosen_date
    else:
        _debug(f"starter current start had no fastball context player_id={player_id} chosen_date={chosen_date}")

    previous: List[Dict[str, Any]] = []
    for gd in sorted(available_dates, reverse=True):
        if gd == chosen_date:
            continue
        start_df = _filter_by_game_date(df, gd)
        start_ctx = _single_start_fastball_context(start_df)
        if not start_ctx:
            continue

        previous.append(
            {
                "game_date": gd,
                "fastball_avg": start_ctx.get("fastball_avg"),
                "fastball_max": start_ctx.get("fastball_max"),
            }
        )
        if len(previous) >= lookback_starts:
            break

    if previous:
        out["previous_starts"] = previous

    _debug(
        f"starter success player_id={player_id} target_date={target_date} chosen_date={chosen_date} "
        f"available_dates={available_dates} out={out}"
    )
    return out


if __name__ == "__main__":
    print("[STATCAST CLIENT V2] ready")

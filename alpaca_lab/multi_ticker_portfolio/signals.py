from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

import pandas as pd

from alpaca_lab.qqq_portfolio.signals import (
    MINUTES_PER_RTH_SESSION,
    build_stock_frame,
    extract_session_features,
)


@dataclass(frozen=True, slots=True)
class TimingProfile:
    name: str
    orb_window: int
    trend_start: int
    credit_minute: int
    straddle_minute: int
    condor_minute: int


TIMING_PROFILES: dict[str, TimingProfile] = {
    "reactive": TimingProfile(
        name="reactive",
        orb_window=5,
        trend_start=20,
        credit_minute=45,
        straddle_minute=5,
        condor_minute=15,
    ),
    "fast": TimingProfile(
        name="fast",
        orb_window=10,
        trend_start=30,
        credit_minute=60,
        straddle_minute=10,
        condor_minute=20,
    ),
    "base": TimingProfile(
        name="base",
        orb_window=15,
        trend_start=45,
        credit_minute=90,
        straddle_minute=15,
        condor_minute=30,
    ),
    "slow": TimingProfile(
        name="slow",
        orb_window=20,
        trend_start=60,
        credit_minute=120,
        straddle_minute=20,
        condor_minute=45,
    ),
    "patient": TimingProfile(
        name="patient",
        orb_window=25,
        trend_start=75,
        credit_minute=150,
        straddle_minute=25,
        condor_minute=60,
    ),
    "governed_late": TimingProfile(
        name="governed_late",
        orb_window=30,
        trend_start=330,
        credit_minute=330,
        straddle_minute=330,
        condor_minute=330,
    ),
}


def get_timing_profile(name: str) -> TimingProfile:
    try:
        return TIMING_PROFILES[name]
    except KeyError as exc:
        raise ValueError(f"Unsupported timing profile: {name}") from exc


def _entry_orb_profile(frame: pd.DataFrame, *, bullish: bool, window: int) -> bool:
    if frame.empty or len(frame) <= window:
        return False
    latest = frame.iloc[-1]
    minute_index = int(latest["minute_index"])
    if minute_index < window or minute_index > min(window + 105, len(frame) - 1):
        return False
    opening_end = window - 1
    opening_range_high = float(frame.loc[:opening_end, "high"].max())
    opening_range_low = float(frame.loc[:opening_end, "low"].min())
    latest_close = float(latest["close"])
    intraday_vwap = float(latest["intraday_vwap"])
    ema_fast = float(latest["ema_fast"])
    ema_slow = float(latest["ema_slow"])
    if bullish:
        return (
            latest_close > opening_range_high * 1.0002
            and latest_close > intraday_vwap
            and ema_fast > ema_slow
        )
    return (
        latest_close < opening_range_low * 0.9998
        and latest_close < intraday_vwap
        and ema_fast < ema_slow
    )


def _entry_trend_profile(frame: pd.DataFrame, *, bullish: bool, start_minute: int) -> bool:
    if frame.empty or len(frame) <= start_minute:
        return False
    latest = frame.iloc[-1]
    minute_index = int(latest["minute_index"])
    if minute_index < start_minute or minute_index > min(start_minute + 105, len(frame) - 1):
        return False
    day_open = float(frame.iloc[0]["open"])
    latest_close = float(latest["close"])
    intraday_vwap = float(latest["intraday_vwap"])
    ema_fast = float(latest["ema_fast"])
    ema_slow = float(latest["ema_slow"])
    move_from_open = (latest_close / day_open) - 1.0
    distance_from_vwap = (latest_close / intraday_vwap) - 1.0 if intraday_vwap else 0.0
    if bullish:
        return (
            move_from_open >= 0.0015
            and distance_from_vwap >= 0.0007
            and ema_fast > ema_slow
        )
    return (
        move_from_open <= -0.0015
        and distance_from_vwap <= -0.0007
        and ema_fast < ema_slow
    )


def _entry_credit_profile(frame: pd.DataFrame, *, bullish: bool, minute_index: int) -> bool:
    if frame.empty or minute_index >= len(frame):
        return False
    row = frame.iloc[minute_index]
    session_range_pct = (
        float(frame.loc[:minute_index, "high"].max()) - float(frame.loc[:minute_index, "low"].min())
    ) / float(frame.iloc[0]["open"])
    if bullish:
        return (
            session_range_pct <= 0.0085
            and float(row["close"]) > float(row["intraday_vwap"])
            and float(row["ema_fast"]) > float(row["ema_slow"])
            and float(row["close"]) > float(frame.iloc[0]["open"])
        )
    return (
        session_range_pct <= 0.0085
        and float(row["close"]) < float(row["intraday_vwap"])
        and float(row["ema_fast"]) < float(row["ema_slow"])
        and float(row["close"]) < float(frame.iloc[0]["open"])
    )


def _entry_straddle_profile(frame: pd.DataFrame, *, minute_index: int) -> bool:
    if frame.empty or minute_index >= len(frame):
        return False
    day_open = float(frame.iloc[0]["open"])
    range_pct = (
        float(frame.loc[:minute_index, "high"].max()) - float(frame.loc[:minute_index, "low"].min())
    ) / day_open
    ret_pct = (float(frame.loc[minute_index, "close"]) / day_open) - 1.0
    return range_pct >= 0.0055 or abs(ret_pct) >= 0.0035


def _entry_condor_profile(frame: pd.DataFrame, *, minute_index: int) -> bool:
    if frame.empty or minute_index >= len(frame):
        return False
    row = frame.iloc[minute_index]
    day_open = float(frame.iloc[0]["open"])
    range_pct = (
        float(frame.loc[:minute_index, "high"].max()) - float(frame.loc[:minute_index, "low"].min())
    ) / day_open
    ret_pct = (float(row["close"]) / day_open) - 1.0
    close_to_vwap = abs((float(row["close"]) / float(row["intraday_vwap"])) - 1.0)
    return range_pct <= 0.0062 and abs(ret_pct) <= 0.0045 and close_to_vwap <= 0.0020


def _latest_research_features(
    frame: pd.DataFrame,
    *,
    fast_window: int,
    slow_window: int,
    breakout_window: int,
) -> pd.Series | None:
    if frame.empty:
        return None
    enriched = frame.copy()
    enriched["fast_sma"] = enriched["close"].rolling(fast_window).mean()
    enriched["slow_sma"] = enriched["close"].rolling(slow_window).mean()
    enriched["rolling_high"] = enriched["high"].shift(1).rolling(breakout_window).max()
    enriched["rolling_low"] = enriched["low"].shift(1).rolling(breakout_window).min()
    enriched["volume_sma"] = enriched["volume"].rolling(slow_window).mean()
    enriched["volume_ratio"] = enriched["volume"] / enriched["volume_sma"].replace(0, pd.NA)
    return enriched.iloc[-1]


def _finite_float(value: object, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if isfinite(parsed) else default


def governed_research_signal_is_true(
    signal_name: str,
    stock_frame: pd.DataFrame,
    *,
    hard_exit_minute: int,
    liquidity_gate: str | None = None,
    min_minutes_since_open: int | None = None,
    max_minutes_since_open: int | None = None,
    min_trend_gap_pct: float | None = None,
    max_trend_gap_pct: float | None = None,
    min_range_pct: float | None = None,
    max_range_pct: float | None = None,
    max_midpoint_distance_pct: float | None = None,
    range_entry_side: str | None = None,
    range_edge_pct: float | None = None,
) -> bool:
    timing_scale = 0 if hard_exit_minute <= 210 else 1 if hard_exit_minute <= 300 else 2
    fast_window = 4 + timing_scale
    slow_window = 18 + timing_scale * 3
    breakout_window = 18 + timing_scale * 3
    latest = _latest_research_features(
        stock_frame,
        fast_window=fast_window,
        slow_window=slow_window,
        breakout_window=breakout_window,
    )
    if latest is None:
        return False
    minute_index = int(latest["minute_index"])
    if min_minutes_since_open is not None and minute_index < int(min_minutes_since_open):
        return False
    if max_minutes_since_open is not None and minute_index > int(max_minutes_since_open):
        return False
    min_volume_ratio = 1.05 if str(liquidity_gate or "tight").lower() == "tight" else 0.80
    if _finite_float(latest.get("volume_ratio")) < min_volume_ratio:
        return False

    close = _finite_float(latest["close"])
    fast_sma = _finite_float(latest.get("fast_sma"))
    slow_sma = _finite_float(latest.get("slow_sma"))
    if close <= 0.0 or fast_sma <= 0.0 or slow_sma <= 0.0:
        return False

    if signal_name == "governed_breakout_call":
        rolling_high = _finite_float(latest.get("rolling_high"))
        trend_gap_pct = (fast_sma - slow_sma) / close
        return (
            rolling_high > 0.0
            and close > rolling_high
            and fast_sma > slow_sma
            and trend_gap_pct >= float(min_trend_gap_pct or 0.0)
        )
    if signal_name == "governed_breakout_put":
        rolling_low = _finite_float(latest.get("rolling_low"))
        trend_gap_pct = (slow_sma - fast_sma) / close
        return (
            rolling_low > 0.0
            and close < rolling_low
            and fast_sma < slow_sma
            and trend_gap_pct >= float(min_trend_gap_pct or 0.0)
        )
    if signal_name == "governed_lower_band_reversion_call":
        rolling_high = _finite_float(latest.get("rolling_high"))
        rolling_low = _finite_float(latest.get("rolling_low"))
        if rolling_high <= 0.0 or rolling_low <= 0.0:
            return False
        range_width = abs(rolling_high - rolling_low)
        range_pct = range_width / close
        trend_gap_pct = abs(fast_sma - slow_sma) / close
        midpoint = (rolling_high + rolling_low) / 2.0
        midpoint_distance_pct = abs(close - midpoint) / close
        if not (
            range_pct >= float(min_range_pct or 0.0015)
            and range_pct <= float(max_range_pct or 0.018)
            and trend_gap_pct <= float(max_trend_gap_pct or 0.004)
            and midpoint_distance_pct <= float(max_midpoint_distance_pct or 0.006)
        ):
            return False
        edge_pct = float(range_edge_pct or 0.001)
        side = str(range_entry_side or "center").lower()
        if side == "lower_band":
            return bool(close <= midpoint * (1.0 - edge_pct))
        if side == "upper_band":
            return bool(close >= midpoint * (1.0 + edge_pct))
        return True
    raise ValueError(f"Unsupported governed research signal: {signal_name}")


def signal_is_true(signal_name: str, stock_frame: pd.DataFrame, *, timing_profile: str) -> bool:
    profile = get_timing_profile(timing_profile)
    if signal_name == "orb_call":
        return _entry_orb_profile(stock_frame, bullish=True, window=profile.orb_window)
    if signal_name == "orb_put":
        return _entry_orb_profile(stock_frame, bullish=False, window=profile.orb_window)
    if signal_name == "trend_call":
        return _entry_trend_profile(stock_frame, bullish=True, start_minute=profile.trend_start)
    if signal_name == "trend_put":
        return _entry_trend_profile(stock_frame, bullish=False, start_minute=profile.trend_start)
    if signal_name == "credit_bull":
        return _entry_credit_profile(stock_frame, bullish=True, minute_index=profile.credit_minute)
    if signal_name == "credit_bear":
        return _entry_credit_profile(stock_frame, bullish=False, minute_index=profile.credit_minute)
    if signal_name == "long_straddle":
        return _entry_straddle_profile(stock_frame, minute_index=profile.straddle_minute)
    if signal_name == "iron_condor":
        return _entry_condor_profile(stock_frame, minute_index=profile.condor_minute)
    raise ValueError(f"Unsupported signal: {signal_name}")


def infer_symbol_regime(stock_frame: pd.DataFrame) -> str:
    features = extract_session_features(stock_frame)
    if features is None:
        return "neutral"
    if _entry_trend_profile(stock_frame, bullish=True, start_minute=30) or _entry_orb_profile(
        stock_frame, bullish=True, window=10
    ):
        return "bull"
    if _entry_trend_profile(stock_frame, bullish=False, start_minute=30) or _entry_orb_profile(
        stock_frame, bullish=False, window=10
    ):
        return "bear"
    return "neutral"


__all__ = [
    "MINUTES_PER_RTH_SESSION",
    "build_stock_frame",
    "extract_session_features",
    "get_timing_profile",
    "infer_symbol_regime",
    "governed_research_signal_is_true",
    "signal_is_true",
]

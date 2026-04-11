from __future__ import annotations

from dataclasses import dataclass

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
    "signal_is_true",
]

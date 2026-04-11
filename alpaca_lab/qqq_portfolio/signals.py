from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


RTH_START_MINUTE = 9 * 60 + 30
RTH_END_MINUTE = 15 * 60 + 59
MINUTES_PER_RTH_SESSION = 390


@dataclass(frozen=True, slots=True)
class SessionFeatures:
    trade_date: date
    minute_index: int
    latest_close: float
    day_open: float
    opening_range_high: float
    opening_range_low: float
    first15_range_pct: float
    first30_range_pct: float
    ret_15_pct: float
    ret_30_pct: float
    distance_from_vwap: float
    ema_spread_pct: float


def build_stock_frame(raw_rows: list[dict]) -> pd.DataFrame:
    if not raw_rows:
        return pd.DataFrame()
    frame = pd.DataFrame(raw_rows).copy()
    frame = frame.rename(
        columns={
            "t": "timestamp_utc",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
        }
    )
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    frame["timestamp_et"] = frame["timestamp_utc"].dt.tz_convert("America/New_York")
    minute_of_day = frame["timestamp_et"].dt.hour * 60 + frame["timestamp_et"].dt.minute
    frame = frame[(minute_of_day >= RTH_START_MINUTE) & (minute_of_day <= RTH_END_MINUTE)].copy()
    if frame.empty:
        return frame
    frame["trade_date"] = frame["timestamp_et"].dt.date
    frame["minute_index"] = minute_of_day.loc[frame.index] - RTH_START_MINUTE
    frame = frame.sort_values("timestamp_et").drop_duplicates("minute_index", keep="last").reset_index(drop=True)
    notional = frame["vwap"].fillna(frame["close"]) * frame["volume"].fillna(0.0)
    frame["cum_notional"] = notional.cumsum()
    frame["cum_volume"] = frame["volume"].fillna(0.0).cumsum()
    frame["intraday_vwap"] = frame["cum_notional"] / frame["cum_volume"].replace(0.0, pd.NA)
    frame["intraday_vwap"] = frame["intraday_vwap"].ffill().fillna(frame["close"])
    frame["ema_fast"] = frame["close"].ewm(span=15, adjust=False).mean()
    frame["ema_slow"] = frame["close"].ewm(span=60, adjust=False).mean()
    return frame


def extract_session_features(frame: pd.DataFrame) -> SessionFeatures | None:
    if frame.empty:
        return None
    latest = frame.iloc[-1]
    day_open = float(frame.iloc[0]["open"])
    latest_close = float(latest["close"])
    opening_slice = frame.iloc[: min(len(frame), 15)]
    first30_slice = frame.iloc[: min(len(frame), 30)]
    opening_range_high = float(opening_slice["high"].max())
    opening_range_low = float(opening_slice["low"].min())
    first15_close = float(opening_slice.iloc[-1]["close"])
    first30_close = float(first30_slice.iloc[-1]["close"])
    intraday_vwap = float(latest["intraday_vwap"])
    ema_fast = float(latest["ema_fast"])
    ema_slow = float(latest["ema_slow"])
    return SessionFeatures(
        trade_date=latest["trade_date"],
        minute_index=int(latest["minute_index"]),
        latest_close=latest_close,
        day_open=day_open,
        opening_range_high=opening_range_high,
        opening_range_low=opening_range_low,
        first15_range_pct=(opening_range_high - opening_range_low) / day_open,
        first30_range_pct=(float(first30_slice["high"].max()) - float(first30_slice["low"].min()))
        / day_open,
        ret_15_pct=(first15_close / day_open) - 1.0,
        ret_30_pct=(first30_close / day_open) - 1.0,
        distance_from_vwap=(latest_close / intraday_vwap) - 1.0 if intraday_vwap else 0.0,
        ema_spread_pct=(ema_fast / ema_slow) - 1.0 if ema_slow else 0.0,
    )


def orb_call_signal(features: SessionFeatures) -> bool:
    return (
        15 <= features.minute_index <= 120
        and features.latest_close > features.opening_range_high * 1.0002
        and features.distance_from_vwap >= 0.0002
        and features.ema_spread_pct > 0.0
    )


def orb_put_signal(features: SessionFeatures) -> bool:
    return (
        15 <= features.minute_index <= 120
        and features.latest_close < features.opening_range_low * 0.9998
        and features.distance_from_vwap <= -0.0002
        and features.ema_spread_pct < 0.0
    )


def trend_call_signal(features: SessionFeatures) -> bool:
    move_from_open = (features.latest_close / features.day_open) - 1.0
    return (
        45 <= features.minute_index <= 150
        and move_from_open >= 0.0015
        and features.distance_from_vwap >= 0.0007
        and features.ema_spread_pct > 0.0
    )


def trend_put_signal(features: SessionFeatures) -> bool:
    move_from_open = (features.latest_close / features.day_open) - 1.0
    return (
        45 <= features.minute_index <= 150
        and move_from_open <= -0.0015
        and features.distance_from_vwap <= -0.0007
        and features.ema_spread_pct < 0.0
    )


def iron_condor_signal(features: SessionFeatures) -> bool:
    return (
        features.minute_index >= 30
        and features.first30_range_pct <= 0.0062
        and abs(features.ret_30_pct) <= 0.0045
        and abs(features.distance_from_vwap) <= 0.0020
        and abs(features.ema_spread_pct) <= 0.0012
    )


def signal_is_true(signal_name: str, features: SessionFeatures) -> bool:
    if signal_name == "orb_call":
        return orb_call_signal(features)
    if signal_name == "orb_put":
        return orb_put_signal(features)
    if signal_name == "trend_call":
        return trend_call_signal(features)
    if signal_name == "trend_put":
        return trend_put_signal(features)
    if signal_name == "iron_condor":
        return iron_condor_signal(features)
    raise ValueError(f"Unsupported signal: {signal_name}")


def infer_intraday_regime(features: SessionFeatures) -> str:
    if orb_call_signal(features) or trend_call_signal(features):
        return "bull"
    if orb_put_signal(features) or trend_put_signal(features):
        return "bear"
    if iron_condor_signal(features):
        return "choppy"
    return "neutral"

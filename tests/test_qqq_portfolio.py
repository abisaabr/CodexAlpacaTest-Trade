from __future__ import annotations

from datetime import date

import pandas as pd

from alpaca_lab.qqq_portfolio.config import default_portfolio_config
from alpaca_lab.qqq_portfolio.signals import (
    SessionFeatures,
    extract_session_features,
    infer_intraday_regime,
    signal_is_true,
)


def test_default_portfolio_contains_validated_book() -> None:
    config = default_portfolio_config()

    assert config.execution.underlying_symbol == "QQQ"
    assert config.execution.submit_paper_orders is True
    assert {strategy.name for strategy in config.strategies} == {
        "trend_long_call_next_expiry",
        "bull_call_spread_next_expiry",
        "orb_long_call_same_day",
        "trend_long_put_next_expiry",
        "bear_put_spread_next_expiry",
        "orb_long_put_same_day",
        "iron_condor_same_day",
    }


def test_intraday_regime_and_signals_match_bull_features() -> None:
    features = SessionFeatures(
        trade_date=date(2026, 4, 13),
        minute_index=60,
        latest_close=510.0,
        day_open=507.0,
        opening_range_high=508.0,
        opening_range_low=505.0,
        first15_range_pct=0.0059,
        first30_range_pct=0.0070,
        ret_15_pct=0.0035,
        ret_30_pct=0.0050,
        distance_from_vwap=0.0012,
        ema_spread_pct=0.0015,
    )

    assert infer_intraday_regime(features) == "bull"
    assert signal_is_true("trend_call", features) is True
    assert signal_is_true("trend_put", features) is False


def test_extract_session_features_from_stock_frame() -> None:
    rows = []
    for minute_index in range(30):
        rows.append(
            {
                "trade_date": date(2026, 4, 13),
                "minute_index": minute_index,
                "open": 500.0 + minute_index * 0.02,
                "high": 500.2 + minute_index * 0.02,
                "low": 499.8 + minute_index * 0.02,
                "close": 500.1 + minute_index * 0.03,
                "volume": 10_000,
                "intraday_vwap": 500.0 + minute_index * 0.02,
                "ema_fast": 500.0 + minute_index * 0.03,
                "ema_slow": 500.0 + minute_index * 0.02,
            }
        )
    frame = pd.DataFrame(rows)

    features = extract_session_features(frame)

    assert features is not None
    assert features.trade_date == date(2026, 4, 13)
    assert features.minute_index == 29
    assert features.latest_close > features.day_open

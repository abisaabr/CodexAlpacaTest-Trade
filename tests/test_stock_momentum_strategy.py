from __future__ import annotations

import pandas as pd

from alpaca_lab.strategies.stock_momentum import ConservativeBreakoutStockStrategy


def test_conservative_breakout_handles_initial_nullable_breakout_state() -> None:
    timestamps = pd.date_range("2026-04-21T13:30:00Z", periods=40, freq="1min")
    bars = pd.DataFrame(
        {
            "symbol": ["QQQ"] * len(timestamps),
            "timestamp": timestamps,
            "open": [100.0 + index * 0.1 for index in range(len(timestamps))],
            "high": [100.2 + index * 0.1 for index in range(len(timestamps))],
            "low": [99.8 + index * 0.1 for index in range(len(timestamps))],
            "close": [100.1 + index * 0.1 for index in range(len(timestamps))],
            "volume": [1000 + index for index in range(len(timestamps))],
        }
    )

    signals = ConservativeBreakoutStockStrategy().generate_signals(bars)

    assert signals["signal"].isna().sum() == 0
    assert set(signals["signal"].unique()).issubset({0, 1})


def test_conservative_breakout_handles_nullable_market_data_columns() -> None:
    timestamps = pd.date_range("2026-04-21T13:30:00Z", periods=40, freq="1min")
    bars = pd.DataFrame(
        {
            "symbol": ["QQQ"] * len(timestamps),
            "timestamp": timestamps,
            "open": pd.Series([100.0 + index * 0.1 for index in range(len(timestamps))], dtype="Float64"),
            "high": pd.Series([100.2 + index * 0.1 for index in range(len(timestamps))], dtype="Float64"),
            "low": pd.Series([99.8 + index * 0.1 for index in range(len(timestamps))], dtype="Float64"),
            "close": pd.Series([100.1 + index * 0.1 for index in range(len(timestamps))], dtype="Float64"),
            "volume": pd.Series([1000 + index for index in range(len(timestamps))], dtype="Int64"),
        }
    )
    bars.loc[0, "high"] = pd.NA

    signals = ConservativeBreakoutStockStrategy().generate_signals(bars)

    assert signals["signal"].isna().sum() == 0
    assert set(signals["signal"].unique()).issubset({0, 1})

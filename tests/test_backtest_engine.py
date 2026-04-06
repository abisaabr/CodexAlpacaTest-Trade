from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpaca_lab.backtest.engine import run_backtest
from alpaca_lab.strategies.base import BaseStrategy


@dataclass(slots=True)
class SingleEntryStrategy(BaseStrategy):
    name: str = "single_entry"

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        frame = bars.sort_values(["symbol", "timestamp"]).copy()
        frame["signal"] = 0
        frame.loc[frame.index[0], "signal"] = 1
        frame["stop_pct"] = 0.01
        frame["target_pct"] = 0.02
        frame["timeout_bars"] = 2
        frame["size_fraction"] = 1.0
        return self.finalize_signal_frame(frame)


def test_backtest_engine_produces_trade_and_summary() -> None:
    bars = pd.DataFrame(
        {
            "symbol": ["SPY", "SPY", "SPY"],
            "timestamp": pd.to_datetime(
                ["2026-04-01T13:30:00Z", "2026-04-01T13:31:00Z", "2026-04-01T13:32:00Z"],
                utc=True,
            ),
            "open": [100.0, 100.0, 101.0],
            "high": [100.5, 101.5, 102.5],
            "low": [99.5, 99.5, 100.5],
            "close": [100.0, 101.0, 102.0],
            "volume": [1000, 1000, 1000],
        }
    )

    result = run_backtest(bars, SingleEntryStrategy(), initial_cash=10_000.0)

    assert result.summary["trade_count"] == 1
    assert not result.trades.empty
    assert not result.equity_curve.empty

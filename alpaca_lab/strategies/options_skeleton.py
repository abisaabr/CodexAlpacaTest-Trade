from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpaca_lab.strategies.base import BaseStrategy


@dataclass(slots=True)
class LongCallMomentumSkeleton(BaseStrategy):
    name: str = "long_call_momentum_skeleton"
    instrument_type: str = "option"
    contract_multiplier: float = 100.0
    breakout_window: int = 20
    min_delta: float = 0.25
    max_delta: float = 0.65
    stop_pct_value: float = 0.35
    target_pct_value: float = 0.60
    timeout_bars_value: int = 10
    size_fraction_value: float = 0.25

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        self.validate_bars(
            bars, ("symbol", "timestamp", "close", "volume", "underlying_close", "option_type")
        )
        frame = bars.sort_values(["symbol", "timestamp"]).copy()
        if "underlying_symbol" not in frame.columns:
            frame["underlying_symbol"] = frame["symbol"]
        frame["reference_breakout"] = frame.groupby("underlying_symbol")[
            "underlying_close"
        ].transform(lambda series: series.gt(series.shift(1).rolling(self.breakout_window).max()))
        if "delta" not in frame.columns:
            frame["delta"] = 0.40
        frame["signal"] = (
            frame["option_type"].astype(str).str.lower().eq("call")
            & frame["reference_breakout"].fillna(False)
            & frame["delta"].between(self.min_delta, self.max_delta, inclusive="both")
            & frame["volume"].fillna(0).gt(0)
        ).astype(int)
        frame["stop_pct"] = self.stop_pct_value
        frame["target_pct"] = self.target_pct_value
        frame["timeout_bars"] = self.timeout_bars_value
        frame["size_fraction"] = self.size_fraction_value
        frame["contract_multiplier"] = self.contract_multiplier
        return self.finalize_signal_frame(frame)

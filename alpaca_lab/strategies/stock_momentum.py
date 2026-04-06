from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpaca_lab.strategies.base import BaseStrategy


def _add_stock_features(
    bars: pd.DataFrame, *, fast_window: int, slow_window: int, breakout_window: int
) -> pd.DataFrame:
    frame = bars.sort_values(["symbol", "timestamp"]).copy()
    grouped = frame.groupby("symbol", sort=False)
    frame["fast_sma"] = grouped["close"].transform(
        lambda series: series.rolling(fast_window).mean()
    )
    frame["slow_sma"] = grouped["close"].transform(
        lambda series: series.rolling(slow_window).mean()
    )
    frame["rolling_high"] = grouped["high"].transform(
        lambda series: series.shift(1).rolling(breakout_window).max()
    )
    frame["volume_sma"] = grouped["volume"].transform(
        lambda series: series.rolling(slow_window).mean()
    )
    frame["volume_ratio"] = frame["volume"] / frame["volume_sma"].replace(0, pd.NA)

    close_diff = grouped["close"].diff()
    gains = close_diff.clip(lower=0)
    losses = -close_diff.clip(upper=0)
    avg_gain = gains.groupby(frame["symbol"]).transform(lambda series: series.rolling(14).mean())
    avg_loss = losses.groupby(frame["symbol"]).transform(lambda series: series.rolling(14).mean())
    relative_strength = avg_gain / avg_loss.replace(0, pd.NA)
    frame["rsi"] = 100 - (100 / (1 + relative_strength))
    return frame


@dataclass(slots=True)
class ConservativeBreakoutStockStrategy(BaseStrategy):
    name: str = "conservative_breakout_stock"
    instrument_type: str = "stock"
    contract_multiplier: float = 1.0
    breakout_window: int = 20
    fast_window: int = 5
    slow_window: int = 20
    min_rsi: float = 52.0
    max_rsi: float = 72.0
    min_volume_ratio: float = 1.0
    stop_pct_value: float = 0.01
    target_pct_value: float = 0.02
    timeout_bars_value: int = 12
    size_fraction_value: float = 1.0

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        self.validate_bars(bars, ("symbol", "timestamp", "open", "high", "low", "close", "volume"))
        frame = _add_stock_features(
            bars,
            fast_window=self.fast_window,
            slow_window=self.slow_window,
            breakout_window=self.breakout_window,
        )
        breakout = (
            frame["close"].gt(frame["rolling_high"])
            & frame["fast_sma"].gt(frame["slow_sma"])
            & frame["rsi"].between(self.min_rsi, self.max_rsi, inclusive="both")
            & frame["volume_ratio"].fillna(0).ge(self.min_volume_ratio)
        )
        prior_breakout = breakout.groupby(frame["symbol"]).shift(1).eq(True)
        frame["signal"] = (breakout & ~prior_breakout).astype(int)
        frame["stop_pct"] = self.stop_pct_value
        frame["target_pct"] = self.target_pct_value
        frame["timeout_bars"] = self.timeout_bars_value
        frame["size_fraction"] = self.size_fraction_value
        return self.finalize_signal_frame(frame)

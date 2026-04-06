from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def compute_max_drawdown(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty:
        return 0.0
    running_peak = equity_curve["equity"].cummax()
    drawdown = (equity_curve["equity"] - running_peak) / running_peak.replace(0, np.nan)
    return float(drawdown.min() if not drawdown.empty else 0.0)


def compute_daily_sharpe_like(equity_curve: pd.DataFrame) -> float | None:
    if equity_curve.empty:
        return None
    daily_returns = (
        equity_curve.set_index("timestamp")["equity"]
        .resample("1D")
        .last()
        .dropna()
        .pct_change()
        .dropna()
    )
    if len(daily_returns) < 2 or daily_returns.std(ddof=0) == 0:
        return None
    return float(np.sqrt(252) * daily_returns.mean() / daily_returns.std(ddof=0))


def summarize_backtest(trades: pd.DataFrame, equity_curve: pd.DataFrame) -> dict[str, Any]:
    net_pnl = float(trades["pnl"].sum()) if not trades.empty else 0.0
    winners = trades[trades["pnl"] > 0] if not trades.empty else trades
    losers = trades[trades["pnl"] < 0] if not trades.empty else trades
    gross_profit = float(winners["pnl"].sum()) if not winners.empty else 0.0
    gross_loss = float(losers["pnl"].sum()) if not losers.empty else 0.0
    profit_factor = None
    if gross_loss != 0:
        profit_factor = abs(gross_profit / gross_loss)
    elif gross_profit > 0:
        profit_factor = float("inf")

    trade_count = int(len(trades))
    win_rate = float(len(winners) / trade_count) if trade_count else 0.0
    expectancy = float(net_pnl / trade_count) if trade_count else 0.0

    return {
        "net_pnl": net_pnl,
        "trade_count": trade_count,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "max_drawdown": compute_max_drawdown(equity_curve),
        "expectancy": expectancy,
        "sharpe_like_daily": compute_daily_sharpe_like(equity_curve),
    }

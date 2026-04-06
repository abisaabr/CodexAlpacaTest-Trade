from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    timestamp: pd.Timestamp
    cash: float
    gross_exposure: float
    equity: float


def equity_curve_to_snapshots(equity_curve: pd.DataFrame) -> list[PortfolioSnapshot]:
    if equity_curve.empty:
        return []
    return [
        PortfolioSnapshot(
            timestamp=row.timestamp,
            cash=float(row.cash),
            gross_exposure=float(row.gross_exposure),
            equity=float(row.equity),
        )
        for row in equity_curve.itertuples(index=False)
    ]

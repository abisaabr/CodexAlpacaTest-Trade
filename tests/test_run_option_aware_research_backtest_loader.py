from pathlib import Path

import pandas as pd

from scripts.run_option_aware_research_backtest import (
    STOCK_SESSION_FILTER_NONE,
    _load_parquet_tree,
    _load_stock_bars,
    _stock_trades_for_variant,
)


def test_load_parquet_tree_restores_partition_columns(tmp_path: Path) -> None:
    path = tmp_path / "stock_bars" / "symbol=SPY" / "chunk=part001"
    path.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-04-28T13:30:00Z"],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [100],
        }
    ).to_parquet(path / "part.parquet", index=False)

    frame = _load_parquet_tree(tmp_path / "stock_bars")

    assert frame.loc[0, "symbol"] == "SPY"
    assert frame.loc[0, "chunk"] == "part001"


def test_load_stock_bars_adds_single_symbol_filter_when_partition_is_absent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "stock_bars" / "date=2026-04-28"
    path.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-04-28T13:30:00Z"],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [100],
        }
    ).to_parquet(path / "part.parquet", index=False)

    frame = _load_stock_bars(tmp_path / "stock_bars", symbol_filter={"QQQ"})

    assert frame.loc[0, "symbol"] == "QQQ"


def test_stock_trades_for_variant_treats_symbolless_frame_as_single_symbol() -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-04-28T13:30:00Z",
                    "2026-04-28T13:31:00Z",
                    "2026-04-28T13:32:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 100.5, 101.0],
            "high": [100.5, 101.0, 101.5],
            "low": [99.5, 100.0, 100.5],
            "close": [100.5, 101.0, 101.5],
            "volume": [100, 100, 100],
        }
    )
    variant = {
        "symbol": "SPY",
        "strategy": "moving_average_crossover",
        "fast_window": 1,
        "slow_window": 2,
    }

    trades = _stock_trades_for_variant(
        variant=variant,
        stock_bars=bars,
        initial_cash=25000,
        allocation_fraction=0.10,
        stock_session_filter=STOCK_SESSION_FILTER_NONE,
    )

    assert isinstance(trades, pd.DataFrame)

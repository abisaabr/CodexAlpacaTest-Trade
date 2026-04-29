from pathlib import Path

import pandas as pd

from scripts.run_option_aware_research_backtest import _load_parquet_tree, _load_stock_bars


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

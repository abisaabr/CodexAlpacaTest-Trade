from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts.build_dense_option_universe import (
    build_dense_option_universe_packet,
    select_dense_option_universe,
)


def test_dense_option_universe_selects_daily_atm_neighbor_contracts() -> None:
    contracts = pd.DataFrame(
        {
            "symbol": [
                "GLD260424C00095000",
                "GLD260424C00100000",
                "GLD260424C00105000",
                "GLD260424P00095000",
                "GLD260424P00100000",
                "GLD260424P00105000",
                "GLD260501C00100000",
            ],
            "underlying_symbol": ["GLD"] * 7,
            "expiration_date": ["2026-04-24"] * 6 + ["2026-05-01"],
            "option_type": ["call", "call", "call", "put", "put", "put", "call"],
            "strike_price": [95.0, 100.0, 105.0, 95.0, 100.0, 105.0, 100.0],
            "inventory_status": ["inactive"] * 7,
        }
    )
    stock_bars = pd.DataFrame(
        {
            "symbol": ["GLD", "GLD"],
            "timestamp": pd.to_datetime(["2026-04-21T13:30:00Z", "2026-04-21T14:00:00Z"], utc=True),
            "open": [99.4, 100.2],
            "high": [99.6, 100.4],
            "low": [99.2, 100.0],
            "close": [99.4, 100.2],
            "volume": [1000, 1000],
        }
    )

    selected = select_dense_option_universe(
        contracts=contracts,
        stock_bars=stock_bars,
        symbol_filter={"GLD"},
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 21),
        min_dte=0,
        max_dte=7,
        strike_steps=1,
        expiration_selection="all_in_dte_window",
        reference_bar="first",
    )

    assert len(selected) == 6
    assert set(selected["option_type"].astype(str)) == {"call", "put"}
    assert set(selected["relative_strike_step"].astype(int)) == {-1, 0, 1}
    assert selected["reference_price"].unique().tolist() == [99.4]
    assert all("reference=dense_daily_first_bar" in value for value in selected["selection_reason"])


def test_dense_option_universe_can_select_next_expiration_after_trade_date() -> None:
    contracts = pd.DataFrame(
        {
            "symbol": [
                "QQQ260403C00600000",
                "QQQ260403P00600000",
                "QQQ260406C00590000",
                "QQQ260406C00600000",
                "QQQ260406C00610000",
                "QQQ260406P00590000",
                "QQQ260406P00600000",
                "QQQ260406P00610000",
                "QQQ260407C00600000",
                "QQQ260407P00600000",
            ],
            "underlying_symbol": ["QQQ"] * 10,
            "expiration_date": ["2026-04-03"] * 2 + ["2026-04-06"] * 6 + ["2026-04-07"] * 2,
            "option_type": ["call", "put", "call", "call", "call", "put", "put", "put", "call", "put"],
            "strike_price": [600.0, 600.0, 590.0, 600.0, 610.0, 590.0, 600.0, 610.0, 600.0, 600.0],
            "inventory_status": ["inactive"] * 10,
        }
    )
    stock_bars = pd.DataFrame(
        {
            "symbol": ["QQQ"],
            "timestamp": pd.to_datetime(["2026-04-03T13:30:00Z"], utc=True),
            "open": [600.1],
            "high": [600.2],
            "low": [600.0],
            "close": [600.1],
            "volume": [1000],
        }
    )

    selected = select_dense_option_universe(
        contracts=contracts,
        stock_bars=stock_bars,
        symbol_filter={"QQQ"},
        start_date=date(2026, 4, 3),
        end_date=date(2026, 4, 3),
        min_dte=1,
        max_dte=7,
        strike_steps=1,
        expiration_selection="next_after_trade_date",
        reference_bar="first",
    )

    assert len(selected) == 6
    assert set(pd.to_datetime(selected["expiration_date"]).dt.date) == {date(2026, 4, 6)}
    assert set(selected["relative_strike_step"].astype(int)) == {-1, 0, 1}
    assert all(
        "expiration_selection=next_after_trade_date" in value
        for value in selected["selection_reason"]
    )


def test_dense_option_universe_packet_writes_partitioned_selected_contracts(tmp_path: Path) -> None:
    stock_path = tmp_path / "stock.parquet"
    contracts_root = tmp_path / "contracts"
    contract_path = contracts_root / "underlying=GLD" / "part.parquet"
    contract_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "symbol": ["GLD260424C00100000", "GLD260424P00100000"],
            "underlying_symbol": ["GLD", "GLD"],
            "expiration_date": ["2026-04-24", "2026-04-24"],
            "option_type": ["call", "put"],
            "strike_price": [100.0, 100.0],
        }
    ).to_parquet(contract_path, index=False)
    pd.DataFrame(
        {
            "symbol": ["GLD"],
            "timestamp": pd.to_datetime(["2026-04-21T13:30:00Z"], utc=True),
            "open": [100.1],
            "high": [100.2],
            "low": [100.0],
            "close": [100.1],
            "volume": [1000],
        }
    ).to_parquet(stock_path, index=False)

    packet = build_dense_option_universe_packet(
        stock_bars_path=stock_path,
        option_contracts_root=contracts_root,
        output_dir=tmp_path / "out",
        symbol_filter={"GLD"},
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 21),
        min_dte=0,
        max_dte=7,
        strike_steps=0,
        expiration_selection="all_in_dte_window",
        reference_bar="first",
    )

    assert packet["broker_facing"] is False
    assert packet["trading_effect"] == "none"
    assert packet["selected_contract_count"] == 2
    assert packet["selected_contract_partitions"] == 1
    assert (tmp_path / "out" / "dense_option_universe_packet.json").exists()
    assert (
        tmp_path
        / "out"
        / "selected_option_contracts"
        / "underlying=GLD"
        / "trade_date=2026-04-21"
        / "part.parquet"
    ).exists()


def test_dense_option_universe_packet_flags_reference_date_coverage_gap(
    tmp_path: Path,
) -> None:
    stock_path = tmp_path / "stock.parquet"
    contracts_root = tmp_path / "contracts"
    contract_path = contracts_root / "underlying=QQQ" / "part.parquet"
    contract_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "symbol": ["QQQ260424C00400000", "QQQ260424P00400000"],
            "underlying_symbol": ["QQQ", "QQQ"],
            "expiration_date": ["2026-04-24", "2026-04-24"],
            "option_type": ["call", "put"],
            "strike_price": [400.0, 400.0],
        }
    ).to_parquet(contract_path, index=False)
    pd.DataFrame(
        {
            "symbol": ["QQQ"],
            "timestamp": pd.to_datetime(["2026-04-20T13:30:00Z"], utc=True),
            "open": [400.1],
            "high": [400.2],
            "low": [400.0],
            "close": [400.1],
            "volume": [1000],
        }
    ).to_parquet(stock_path, index=False)

    packet = build_dense_option_universe_packet(
        stock_bars_path=stock_path,
        option_contracts_root=contracts_root,
        output_dir=tmp_path / "out",
        symbol_filter={"QQQ"},
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 23),
        min_dte=0,
        max_dte=7,
        strike_steps=0,
        expiration_selection="all_in_dte_window",
        reference_bar="first",
    )

    coverage = packet["coverage_diagnostics"]
    assert coverage["status"] == "stock_reference_coverage_gap"
    assert coverage["requested_weekday_trade_date_count"] == 4
    assert coverage["low_stock_reference_coverage_symbols"] == ["QQQ"]
    assert coverage["symbol_coverage"][0]["stock_reference_trade_date_count"] == 1
    assert coverage["symbol_coverage"][0]["selected_trade_date_count"] == 1
    assert coverage["symbol_coverage"][0]["missing_stock_reference_dates_sample"] == [
        "2026-04-21",
        "2026-04-22",
        "2026-04-23",
    ]

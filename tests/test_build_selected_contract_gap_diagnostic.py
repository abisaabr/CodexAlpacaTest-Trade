from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from scripts.build_selected_contract_gap_diagnostic import (
    CLASS_CHAIN_BUILDS,
    CLASS_MISSING_SELECTED_PARTITION,
    CLASS_NO_VALID_WING_CONTRACT,
    build_gap_diagnostic,
)


def _write_requests(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "case_id",
        "symbol",
        "family",
        "trade_date",
        "stock_entry_time",
        "option_type",
        "dte_mode",
        "short_width_steps",
        "wing_width_steps",
        "max_entry_lag_minutes",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_selected(source_root: Path, symbol: str, trade_date: str, rows: list[dict[str, object]]) -> None:
    path = (
        source_root
        / "selected_contracts"
        / f"underlying={symbol}"
        / f"trade_date={trade_date}"
        / "part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path)


def _write_bars(source_root: Path, symbol: str, trade_date: str, rows: list[dict[str, object]]) -> None:
    path = (
        source_root
        / "option_bars"
        / f"underlying={symbol}"
        / f"trade_date={trade_date}"
        / "batch=000"
        / "part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path)


def _contract(
    symbol: str,
    option_symbol: str,
    option_type: str,
    strike: float,
    step: int,
) -> dict[str, object]:
    return {
        "trade_date": "2026-01-02",
        "underlying_symbol": symbol,
        "symbol": option_symbol,
        "expiration_date": "2026-01-09",
        "option_type": option_type,
        "strike_price": strike,
        "dte": 7,
        "relative_strike_step": step,
    }


def _bar(symbol: str, option_symbol: str) -> dict[str, object]:
    return {
        "symbol": option_symbol,
        "underlying_symbol": symbol,
        "trade_date": "2026-01-02",
        "timestamp": pd.Timestamp("2026-01-02T15:31:00Z"),
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 10,
        "trade_count": 1,
    }


def test_selected_contract_gap_diagnostic_classifies_missing_chain_and_replay_mismatch(
    tmp_path: Path,
) -> None:
    requests_csv = tmp_path / "requests.csv"
    source_root = tmp_path / "source"
    _write_requests(
        requests_csv,
        [
            {
                "case_id": "missing_partition",
                "symbol": "MISS",
                "family": "debit_call_vertical",
                "trade_date": "2026-01-02",
                "stock_entry_time": "2026-01-02T15:30:00Z",
                "option_type": "call",
                "dte_mode": "next_expiry",
                "short_width_steps": 1,
                "wing_width_steps": 1,
                "max_entry_lag_minutes": 10,
            },
            {
                "case_id": "chain_builds",
                "symbol": "QQQ",
                "family": "debit_call_vertical",
                "trade_date": "2026-01-02",
                "stock_entry_time": "2026-01-02T15:30:00Z",
                "option_type": "call",
                "dte_mode": "next_expiry",
                "short_width_steps": 1,
                "wing_width_steps": 1,
                "max_entry_lag_minutes": 10,
            },
            {
                "case_id": "missing_wing",
                "symbol": "IWM",
                "family": "debit_put_vertical",
                "trade_date": "2026-01-02",
                "stock_entry_time": "2026-01-02T15:30:00Z",
                "option_type": "put",
                "dte_mode": "next_expiry",
                "short_width_steps": 1,
                "wing_width_steps": 1,
                "max_entry_lag_minutes": 10,
            },
        ],
    )
    _write_selected(
        source_root,
        "QQQ",
        "2026-01-02",
        [
            _contract("QQQ", "QQQ260109C00100000", "call", 100.0, 0),
            _contract("QQQ", "QQQ260109C00101000", "call", 101.0, 1),
        ],
    )
    _write_bars(
        source_root,
        "QQQ",
        "2026-01-02",
        [_bar("QQQ", "QQQ260109C00100000"), _bar("QQQ", "QQQ260109C00101000")],
    )
    _write_selected(
        source_root,
        "IWM",
        "2026-01-02",
        [_contract("IWM", "IWM260109P00200000", "put", 200.0, 0)],
    )
    _write_bars(source_root, "IWM", "2026-01-02", [_bar("IWM", "IWM260109P00200000")])

    summary = build_gap_diagnostic(
        requests_csv=requests_csv,
        source_root=source_root,
        output_dir=tmp_path / "out",
    )

    assert summary["request_count"] == 3
    assert summary["classification_counts"][CLASS_MISSING_SELECTED_PARTITION] == 1
    assert summary["classification_counts"][CLASS_CHAIN_BUILDS] == 1
    assert summary["classification_counts"][CLASS_NO_VALID_WING_CONTRACT] == 1
    assert (tmp_path / "out" / "selected_contract_gap_diagnostic_rows.csv").exists()
    assert (tmp_path / "out" / "selected_contract_gap_diagnostic_summary.json").exists()
    assert (tmp_path / "out" / "selected_contract_gap_diagnostic.md").exists()

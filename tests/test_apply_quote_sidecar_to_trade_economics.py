from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.apply_quote_sidecar_to_trade_economics import apply_quote_sidecar_to_trade_economics


def test_apply_quote_sidecar_uses_causal_asof_quotes_and_trade_prints(tmp_path: Path) -> None:
    trades = tmp_path / "option_aware_trade_economics.csv"
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "qqq_bull",
                "contract_symbol": "QQQ260515C00450000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
                "option_pnl": 12.0,
            }
        ]
    ).to_csv(trades, index=False)
    quote_sidecar = tmp_path / "option_quote_sidecar.csv"
    pd.DataFrame(
        [
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
                "bid": 1.00,
                "ask": 1.04,
            },
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:20+00:00",
                "bid": 1.20,
                "ask": 1.30,
            },
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:44:59+00:00",
                "bid": 1.50,
                "ask": 1.56,
            },
        ]
    ).to_csv(quote_sidecar, index=False)
    option_trades = tmp_path / "option_trade_prints.csv"
    pd.DataFrame(
        [
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:30+00:00",
                "price": 1.10,
                "size": 1,
            },
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:44:00+00:00",
                "price": 1.40,
                "size": 1,
            },
        ]
    ).to_csv(option_trades, index=False)

    summary = apply_quote_sidecar_to_trade_economics(
        trade_economics_csv=trades,
        quote_sidecar_csv=quote_sidecar,
        option_trades_csv=option_trades,
        output_csv=tmp_path / "out" / "option_aware_trade_economics.csv",
    )

    assert summary["quote_source_status_counts"] == {
        "entry:option_quote_bid_ask": 1,
        "exit:option_quote_bid_ask": 1,
    }
    enriched = pd.read_csv(tmp_path / "out" / "option_aware_trade_economics.csv")
    row = enriched.iloc[0]
    assert row["entry_quote_source"] == "option_quote_bid_ask"
    assert row["entry_bid"] == 1.00
    assert row["entry_ask"] == 1.04
    assert row["entry_quote_age_seconds"] == 15.0
    assert row["entry_average_relative_spread"] == pytest.approx(0.04 / 1.02)
    assert row["exit_bid"] == 1.50
    assert row["exit_ask"] == 1.56
    assert row["exit_quote_age_seconds"] == 1.0
    assert row["entry_selection_trade_print_count"] == 1
    assert row["option_trade_print_count"] == 2


def test_apply_quote_sidecar_marks_multileg_partial_bid_ask(tmp_path: Path) -> None:
    trades = tmp_path / "option_aware_trade_economics.csv"
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "qqq_vertical",
                "contract_symbol": "QQQ260515C00450000;QQQ260515C00455000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
                "option_pnl": 12.0,
            }
        ]
    ).to_csv(trades, index=False)
    quote_sidecar = tmp_path / "option_quote_sidecar.csv"
    pd.DataFrame(
        [
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
                "bid": 1.00,
                "ask": 1.04,
            }
        ]
    ).to_csv(quote_sidecar, index=False)

    apply_quote_sidecar_to_trade_economics(
        trade_economics_csv=trades,
        quote_sidecar_csv=quote_sidecar,
        output_csv=tmp_path / "out" / "option_aware_trade_economics.csv",
    )

    enriched = pd.read_csv(tmp_path / "out" / "option_aware_trade_economics.csv")
    row = enriched.iloc[0]
    assert row["entry_quote_source"] == "option_quote_partial_bid_ask"
    assert row["entry_legs_with_bid_ask"] == 1
    assert row["entry_quote_sidecar_leg_count"] == 2
    assert row["entry_quote_sidecar_missing_legs"] == 1
    assert row["exit_quote_source"] == "option_quote_partial_bid_ask"

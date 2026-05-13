from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_broker_fill_strategy_reconciliation import reconcile_broker_fills


def test_reconcile_broker_fills_maps_order_ids_to_strategy_identity(tmp_path: Path) -> None:
    fills_path = tmp_path / "broker_fills.csv"
    fills_path.write_text(
        "\n".join(
            [
                "transaction_time,symbol,side,qty,price,signed_cash_ex_fees,order_id,fill_type",
                "2026-05-07T14:00:00Z,QQQ260508C00696000,buy,1,4.50,-450.0,entry-1,fill",
                "2026-05-07T14:10:00Z,QQQ260508C00696000,sell,1,5.25,525.0,exit-1,fill",
                "2026-05-07T14:15:00Z,SPY260508C00735000,buy,1,2.00,-200.0,unknown-1,fill",
            ]
        ),
        encoding="utf-8",
    )
    journal_path = tmp_path / "order_journal.json"
    journal_path.write_text(
        json.dumps(
            [
                {
                    "order_id": "entry-1",
                    "strategy_name": "qqq_micro_event",
                    "source_strategy_id": "qqq__micro__event",
                    "candidate_variant_id": "micro_1",
                    "underlying_symbol": "QQQ",
                    "regime": "bull",
                    "phase": "entry",
                },
                {
                    "order_id": "exit-1",
                    "strategy_name": "qqq_micro_event",
                    "source_strategy_id": "qqq__micro__event",
                    "candidate_variant_id": "micro_1",
                    "underlying_symbol": "QQQ",
                    "regime": "bull",
                    "phase": "exit",
                },
            ]
        ),
        encoding="utf-8",
    )

    summary = reconcile_broker_fills(
        broker_fills_csv=fills_path,
        order_journal_path=journal_path,
        output_dir=tmp_path / "out",
        trade_date="2026-05-07",
    )

    strategy = pd.read_csv(summary["broker_strategy_cash_summary_csv"])
    matches = pd.read_csv(summary["broker_fill_strategy_matches_csv"])

    assert summary["broker_fill_count"] == 3
    assert summary["matched_fill_count"] == 2
    assert summary["unmatched_fill_count"] == 1
    assert summary["matched_signed_cash_ex_fees"] == 75.0
    assert strategy.iloc[0]["strategy_name"] == "qqq_micro_event"
    assert strategy.iloc[0]["net_cash_ex_fees"] == 75.0
    assert matches["matched_to_strategy"].tolist() == [True, True, False]


def test_reconcile_broker_fills_infers_stale_exit_by_symbol_balance(tmp_path: Path) -> None:
    fills_path = tmp_path / "broker_fills.csv"
    fills_path.write_text(
        "\n".join(
            [
                "transaction_time,symbol,side,qty,price,signed_cash_ex_fees,order_id,fill_type",
                "2026-05-07T14:00:00Z,SPY260508C00735000,buy,2,2.50,-500.0,entry-a,fill",
                "2026-05-07T14:01:00Z,SPY260508C00735000,buy,2,2.55,-510.0,entry-b,fill",
                "2026-05-07T19:59:00Z,SPY260508C00735000,sell,4,0.72,288.0,late-exit,fill",
            ]
        ),
        encoding="utf-8",
    )
    journal_path = tmp_path / "order_journal.json"
    journal_path.write_text(
        json.dumps(
            [
                {
                    "order_id": "entry-a",
                    "strategy_name": "spy_bull_a",
                    "source_strategy_id": "spy__bull__call",
                    "candidate_variant_id": "a",
                    "underlying_symbol": "SPY",
                    "regime": "bull",
                    "phase": "entry",
                },
                {
                    "order_id": "entry-b",
                    "strategy_name": "spy_bull_b",
                    "source_strategy_id": "spy__bull__call",
                    "candidate_variant_id": "b",
                    "underlying_symbol": "SPY",
                    "regime": "bull",
                    "phase": "entry",
                },
            ]
        ),
        encoding="utf-8",
    )

    summary = reconcile_broker_fills(
        broker_fills_csv=fills_path,
        order_journal_path=journal_path,
        output_dir=tmp_path / "out",
        trade_date="2026-05-07",
    )

    inferred = pd.read_csv(summary["broker_strategy_cash_summary_inferred_csv"])
    inferred = inferred.sort_values("strategy_name").reset_index(drop=True)

    assert summary["matched_fill_count"] == 2
    assert summary["unmatched_fill_count"] == 1
    assert summary["inferred_allocation_count"] == 2
    assert summary["still_unmatched_fill_count"] == 0
    assert inferred.loc[0, "strategy_name"] == "spy_bull_a"
    assert inferred.loc[0, "net_cash_ex_fees"] == -356.0
    assert inferred.loc[1, "strategy_name"] == "spy_bull_b"
    assert inferred.loc[1, "net_cash_ex_fees"] == -366.0

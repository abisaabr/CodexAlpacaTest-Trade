from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_multi_ticker_paper_postmortem import build_postmortem


def test_build_postmortem_writes_daily_and_cumulative_strategy_outputs(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-07"
    (state_root / f"session_{trade_date}.json").write_text(
        json.dumps(
            {
                "trade_date": trade_date,
                "starting_equity": 25000.0,
                "virtual_cash": 25125.0,
                "completed_trades": [
                    {
                        "strategy_name": "qqq_greek_delta_call",
                        "underlying_symbol": "QQQ",
                        "regime": "bull",
                        "net_pnl": 200.0,
                    },
                    {
                        "strategy_name": "qqq_greek_delta_call",
                        "underlying_symbol": "QQQ",
                        "regime": "bull",
                        "net_pnl": -75.0,
                    },
                ],
                "open_trades": [],
                "alerts": [],
            }
        ),
        encoding="utf-8",
    )

    summary = build_postmortem(
        state_root=state_root,
        run_root=run_root,
        trade_date=trade_date,
        submit_paper_orders=True,
    )

    daily = pd.read_csv(summary["strategy_daily_performance_ledger_path"])
    cumulative = pd.read_csv(summary["strategy_cumulative_performance_path"])

    assert summary["completed_trade_count"] == 2
    assert summary["net_pnl"] == 125.0
    assert summary["completed_trade_net_pnl"] == 125.0
    assert summary["unattributed_session_pnl"] == 0.0
    assert summary["postmortem_accounting_status"] == "fully_attributed"
    assert (run_root / trade_date / f"paper_trader_postmortem_{trade_date}.md").exists()
    assert daily.iloc[0]["trade_count"] == 2
    assert daily.iloc[0]["win_rate_pct"] == 50.0
    assert cumulative.iloc[0]["net_pnl"] == 125.0


def test_build_postmortem_flags_unattributed_broker_flat_session_exits(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-07"
    (state_root / f"session_{trade_date}.json").write_text(
        json.dumps(
            {
                "trade_date": trade_date,
                "starting_equity": 25000.0,
                "virtual_cash": 24800.0,
                "completed_trades": [
                    {
                        "strategy_name": "spy_bull_call",
                        "underlying_symbol": "SPY",
                        "regime": "bull",
                        "net_pnl": 25.0,
                    }
                ],
                "open_trades": [],
                "alerts": [],
            }
        ),
        encoding="utf-8",
    )
    event_dir = run_root / trade_date
    event_dir.mkdir(parents=True)
    (event_dir / "trade_reconciliation_events.json").write_text(
        json.dumps(
            [
                {
                    "event_type": "exit_result",
                    "status": "broker_flat_without_session_exit",
                    "strategy_name": "spy_bull_call",
                }
            ]
        ),
        encoding="utf-8",
    )

    summary = build_postmortem(
        state_root=state_root,
        run_root=run_root,
        trade_date=trade_date,
        submit_paper_orders=True,
    )

    assert summary["net_pnl"] == -200.0
    assert summary["completed_trade_net_pnl"] == 25.0
    assert summary["unattributed_session_pnl"] == -225.0
    assert summary["broker_flat_without_session_exit_count"] == 1
    assert summary["postmortem_accounting_status"] == "needs_broker_fill_reconciliation"
    postmortem_md = (run_root / trade_date / f"paper_trader_postmortem_{trade_date}.md").read_text(
        encoding="utf-8"
    )
    assert "Unattributed session PnL requires broker-fill reconciliation" in postmortem_md


def test_build_postmortem_surfaces_quote_backed_evidence_gate(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-13"
    (state_root / f"session_{trade_date}.json").write_text(
        json.dumps(
            {
                "trade_date": trade_date,
                "starting_equity": 25000.0,
                "virtual_cash": 25050.0,
                "completed_trades": [
                    {
                        "strategy_name": "qqq_quote_backed",
                        "underlying_symbol": "QQQ",
                        "regime": "bear",
                        "net_pnl": 50.0,
                    }
                ],
                "open_trades": [],
                "alerts": [],
            }
        ),
        encoding="utf-8",
    )
    quote_evidence = tmp_path / "quote_evidence.json"
    quote_evidence.write_text(
        json.dumps(
            {
                "evidence_status": "quote_backed_session_evidence_complete",
                "quote_backed_projection_input_allowed": True,
                "quote_backed_optimizer_input_allowed": True,
                "quote_backed_promotion_input_allowed": False,
                "blockers": ["promotion_gate_not_requested"],
            }
        ),
        encoding="utf-8",
    )

    summary = build_postmortem(
        state_root=state_root,
        run_root=run_root,
        trade_date=trade_date,
        submit_paper_orders=True,
        quote_evidence_json=quote_evidence,
    )

    assert summary["quote_backed_evidence_status"] == "quote_backed_session_evidence_complete"
    assert summary["quote_backed_optimizer_input_allowed"] is True
    assert summary["quote_backed_promotion_input_allowed"] is False
    assert summary["quote_backed_evidence_blockers"] == ["promotion_gate_not_requested"]


def test_build_postmortem_surfaces_final_session_summary_and_eod_state(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-13"
    (state_root / f"session_{trade_date}.json").write_text(
        json.dumps(
            {
                "trade_date": trade_date,
                "starting_equity": 25000.0,
                "virtual_cash": 25000.0,
                "completed_trades": [],
                "open_trades": [],
                "alerts": [],
            }
        ),
        encoding="utf-8",
    )
    session_summary_dir = run_root / trade_date
    session_summary_dir.mkdir(parents=True)
    (session_summary_dir / "multi_ticker_portfolio_session_summary.json").write_text(
        json.dumps(
            {
                "shutdown_reconciled": False,
                "guardrail_fire_count": 3,
                "guardrail_reason_count": {"exit_not_filled": 2, "entry_rejected": 1},
                "guardrail_manual_review_count": 1,
                "guardrail_needs_manual_review": True,
                "end_of_day_cleanup": {
                    "shutdown_reconciled": False,
                    "residual_broker_position_count": 1,
                    "open_trade_count_after_cleanup": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    summary = build_postmortem(
        state_root=state_root,
        run_root=run_root,
        trade_date=trade_date,
        submit_paper_orders=True,
    )

    assert summary["session_summary_found"] is True
    assert summary["shutdown_reconciled"] is False
    assert summary["end_of_day_cleanup_shutdown_reconciled"] is False
    assert summary["end_of_day_cleanup_residual_broker_position_count"] == 1
    assert summary["guardrail_fire_count"] == 3
    assert summary["guardrail_needs_manual_review"] is True

import json
from pathlib import Path

import pandas as pd

from scripts.build_paper_session_quote_evidence_report import (
    build_paper_session_quote_evidence_report,
)


def test_build_paper_session_quote_evidence_report_counts_entry_exit_fields(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-13"
    (state_root / f"session_{trade_date}.json").write_text(
        json.dumps(
            {
                "completed_trades": [
                    {
                        "strategy_name": "qqq_test",
                        "underlying_symbol": "QQQ",
                        "regime": "bull",
                        "exit_reason": "profit_target",
                        "net_pnl": 42.0,
                        "legs": [
                            {
                                "symbol": "QQQ260515C00450000",
                                "side": "long",
                                "bid": 1.0,
                                "ask": 1.1,
                                "mark": 1.05,
                                "quote_time": "2026-05-13T14:00:01Z",
                                "spread_pct": 0.0952381,
                                "freshness_seconds": 0.5,
                                "exit_bid": 1.3,
                                "exit_ask": 1.4,
                                "exit_mark": 1.35,
                                "exit_quote_time": "2026-05-13T14:20:01Z",
                                "exit_spread_pct": 0.0740741,
                                "exit_freshness_seconds": 0.4,
                            }
                        ],
                    }
                ],
                "open_trades": [],
            }
        ),
        encoding="utf-8",
    )

    summary = build_paper_session_quote_evidence_report(
        state_root=state_root,
        run_root=run_root,
        trade_date=trade_date,
    )

    assert summary["status"] == "paper_session_quote_evidence_complete"
    assert summary["evidence_status"] == "complete_entry_exit_quote_fields"
    assert summary["session_quote_field_gate"] == "pass"
    assert summary["sidecar_quote_evidence_required"] is True
    assert summary["quote_backed_projection_input_allowed"] is False
    assert summary["quote_backed_optimizer_input_allowed"] is False
    assert summary["quote_backed_promotion_input_allowed"] is False
    assert summary["entry_bid_ask_coverage_pct"] == 100.0
    assert summary["completed_exit_quote_time_coverage_pct"] == 100.0
    assert summary["broker_facing"] is False

    detail = pd.read_csv(run_root / trade_date / f"paper_session_quote_evidence_{trade_date}.csv")
    assert detail["option_symbol"].tolist() == ["QQQ260515C00450000"]
    assert detail["exit_bid"].tolist() == [1.3]


def test_build_paper_session_quote_evidence_report_flags_missing_exit_fields(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-13"
    (state_root / f"session_{trade_date}.json").write_text(
        json.dumps(
            {
                "completed_trades": [
                    {
                        "strategy_name": "legacy",
                        "underlying_symbol": "SPY",
                        "regime": "bear",
                        "legs": [
                            {
                                "symbol": "SPY260515P00450000",
                                "side": "long",
                                "bid": 2.0,
                                "ask": 2.1,
                                "quote_time": "2026-05-13T14:00:01Z",
                                "spread_pct": 0.048,
                                "freshness_seconds": 0.2,
                            }
                        ],
                    }
                ],
                "open_trades": [],
            }
        ),
        encoding="utf-8",
    )

    summary = build_paper_session_quote_evidence_report(
        state_root=state_root,
        run_root=run_root,
        trade_date=trade_date,
    )

    assert summary["evidence_status"] == "quote_field_gaps_present"
    assert summary["session_quote_field_gate"] == "fail"
    assert summary["quote_backed_optimizer_input_allowed"] is False
    assert summary["entry_bid_ask_coverage_pct"] == 100.0
    assert summary["completed_exit_bid_ask_coverage_pct"] == 0.0

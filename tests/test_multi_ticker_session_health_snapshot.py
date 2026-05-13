from __future__ import annotations

import json

from scripts.build_multi_ticker_session_health_snapshot import (
    _process_summary,
    _session_summary,
)


def test_process_summary_counts_one_matching_trader_and_quote_shadow() -> None:
    processes = [
        {
            "ProcessId": 101,
            "CommandLine": (
                "python scripts/run_multi_ticker_portfolio_paper_trader.py "
                "--portfolio-config config\\multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml "
                "--submit-paper-orders"
            ),
        },
        {
            "ProcessId": 102,
            "CommandLine": (
                "python scripts/run_multi_ticker_realtime_shadow_monitor.py "
                "--portfolio-config config\\multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml "
                "--runtime-selected-leg-symbols-only --stream"
            ),
        },
        {
            "ProcessId": 103,
            "CommandLine": (
                "python scripts/run_multi_ticker_portfolio_paper_trader.py "
                "--portfolio-config config\\multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml "
                "--startup-preflight --no-submit-paper-orders"
            ),
        },
    ]

    summary = _process_summary(
        processes,
        "config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml",
    )

    assert summary["broker_facing_trader_count"] == 1
    assert summary["quote_shadow_count"] == 1
    assert summary["risky_process_count"] == 0


def test_process_summary_flags_live_token() -> None:
    summary = _process_summary(
        [
            {
                "ProcessId": 201,
                "CommandLine": (
                    "python scripts/run_multi_ticker_portfolio_paper_trader.py "
                    "--portfolio-config config/x.yaml --live"
                ),
            }
        ],
        "config/x.yaml",
    )

    assert summary["broker_facing_trader_count"] == 1
    assert summary["risky_process_count"] == 1


def test_session_summary_reports_core_runtime_counts(tmp_path) -> None:
    session_path = tmp_path / "session_2026-05-13.json"
    session_path.write_text(
        json.dumps(
            {
                "completed_trades": [{"strategy_name": "a"}],
                "open_trades": [{"strategy_name": "b"}],
                "realized_pnl": -10.5,
                "eod_flatten_checkpoints_completed": [10],
                "last_heartbeat_at": "2026-05-13T14:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    summary = _session_summary(session_path)

    assert summary["session_found"] is True
    assert summary["completed_trades"] == 1
    assert summary["open_trades"] == 1
    assert summary["realized_pnl"] == -10.5
    assert summary["eod_flatten_checkpoints_completed"] == [10]

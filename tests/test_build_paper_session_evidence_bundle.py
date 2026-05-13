from __future__ import annotations

import json
from pathlib import Path

from scripts.build_paper_session_evidence_bundle import build_paper_session_evidence_bundle


def _write_config(path: Path, *, state_root: Path, run_root: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "name: test_portfolio",
                "strategies: []",
                "execution:",
                f"  state_root: {state_root.as_posix()}",
                f"  run_root: {run_root.as_posix()}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_session(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "completed_trades": [
                    {
                        "strategy_name": "qqq_test",
                        "underlying_symbol": "QQQ",
                        "regime": "bear",
                        "entry_time_et": "2026-05-13T09:49:52-04:00",
                        "exit_time_et": "2026-05-13T09:51:27-04:00",
                        "net_pnl": 12.34,
                        "exit_reason": "profit_target",
                        "legs": [
                            {
                                "symbol": "QQQ260514P00711000",
                                "side": "long",
                                "bid": 7.21,
                                "ask": 7.28,
                                "quote_time": "2026-05-13T13:49:43Z",
                                "spread_pct": 0.0097,
                                "freshness_seconds": 0.2,
                                "exit_bid": 7.44,
                                "exit_ask": 7.52,
                                "exit_quote_time": "2026-05-13T13:51:12Z",
                                "exit_spread_pct": 0.0107,
                                "exit_freshness_seconds": 0.3,
                            }
                        ],
                    }
                ],
                "open_trades": [],
            }
        ),
        encoding="utf-8",
    )


def test_paper_session_evidence_bundle_passes_with_session_and_raw_sidecar(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-13"
    config = tmp_path / "portfolio.yaml"
    _write_config(config, state_root=state_root, run_root=run_root)
    _write_session(state_root / f"session_{trade_date}.json")
    events = tmp_path / "events.jsonl"
    events.write_text(
        "\n".join(
            json.dumps(event)
            for event in [
                {
                    "event_type": "option_quote",
                    "observed_at_utc": "2026-05-13T13:49:43.100000Z",
                    "latency_seconds": 0.1,
                    "payload": {
                        "symbol": "QQQ260514P00711000",
                        "timestamp": "2026-05-13T13:49:43Z",
                        "bid_price": 7.21,
                        "ask_price": 7.28,
                    },
                },
                {
                    "event_type": "option_quote",
                    "observed_at_utc": "2026-05-13T13:51:12.100000Z",
                    "latency_seconds": 0.1,
                    "payload": {
                        "symbol": "QQQ260514P00711000",
                        "timestamp": "2026-05-13T13:51:12Z",
                        "bid_price": 7.44,
                        "ask_price": 7.52,
                    },
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary = build_paper_session_evidence_bundle(
        portfolio_config=config,
        trade_date=trade_date,
        quote_events_jsonl=events,
        output_dir=tmp_path / "bundle",
    )

    assert summary["evidence_status"] == "quote_backed_session_evidence_complete"
    assert summary["quote_backed_projection_input_allowed"] is True
    assert summary["quote_backed_optimizer_input_allowed"] is True
    assert summary["raw_sidecar_quote_gate"] == "pass"
    assert (tmp_path / "bundle" / "paper_session_evidence_bundle_summary.json").exists()


def test_paper_session_evidence_bundle_fails_closed_without_raw_sidecar(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-13"
    config = tmp_path / "portfolio.yaml"
    _write_config(config, state_root=state_root, run_root=run_root)
    _write_session(state_root / f"session_{trade_date}.json")

    summary = build_paper_session_evidence_bundle(
        portfolio_config=config,
        trade_date=trade_date,
        quote_events_jsonl=None,
        output_dir=tmp_path / "bundle",
    )

    assert summary["evidence_status"] == "quote_backed_session_evidence_blocked"
    assert summary["session_quote_field_gate"] == "pass"
    assert summary["raw_sidecar_quote_gate"] == "fail"
    assert summary["quote_backed_promotion_input_allowed"] is False
    assert "raw_opra_sidecar_gate_failed" in summary["blockers"]


def test_paper_session_evidence_bundle_combines_capture_restarts(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-13"
    config = tmp_path / "portfolio.yaml"
    _write_config(config, state_root=state_root, run_root=run_root)
    _write_session(state_root / f"session_{trade_date}.json")
    entry_events = tmp_path / "entry_events.jsonl"
    exit_events = tmp_path / "exit_events.jsonl"
    entry_events.write_text(
        json.dumps(
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T13:49:43.100000Z",
                "payload": {
                    "symbol": "QQQ260514P00711000",
                    "timestamp": "2026-05-13T13:49:43Z",
                    "bid_price": 7.21,
                    "ask_price": 7.28,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    exit_events.write_text(
        json.dumps(
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T13:51:12.100000Z",
                "payload": {
                    "symbol": "QQQ260514P00711000",
                    "timestamp": "2026-05-13T13:51:12Z",
                    "bid_price": 7.44,
                    "ask_price": 7.52,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary = build_paper_session_evidence_bundle(
        portfolio_config=config,
        trade_date=trade_date,
        quote_events_jsonl=[entry_events, exit_events],
        output_dir=tmp_path / "bundle_combined",
    )

    assert summary["evidence_status"] == "quote_backed_session_evidence_complete"
    assert summary["quote_events_jsonl_count"] == 2
    assert summary["raw_sidecar_quote_summary"]["events_jsonl_count"] == 2
    assert summary["quote_quality_sidecar_summary"]["events_jsonl_count"] == 2
    assert summary["quote_backed_optimizer_input_allowed"] is True

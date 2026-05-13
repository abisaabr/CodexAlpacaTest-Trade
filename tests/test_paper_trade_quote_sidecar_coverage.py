from __future__ import annotations

import json

from scripts.build_paper_trade_quote_sidecar_coverage import build_sidecar_coverage


def test_build_sidecar_coverage_matches_completed_trade_entry_and_exit(tmp_path) -> None:
    session_path = tmp_path / "session.json"
    events_path = tmp_path / "events.jsonl"
    session_path.write_text(
        json.dumps(
            {
                "completed_trades": [
                    {
                        "strategy_name": "qqq_test",
                        "underlying_symbol": "QQQ",
                        "regime": "bear",
                        "entry_time_et": "2026-05-13T09:49:52-04:00",
                        "exit_time_et": "2026-05-13T09:51:27-04:00",
                        "net_pnl": -12.34,
                        "exit_reason": "stop_loss",
                        "legs": [
                            {
                                "symbol": "QQQ260514P00711000",
                                "bid": 7.21,
                                "ask": 7.28,
                                "quote_time": "2026-05-13T13:49:43Z",
                                "exit_bid": 7.18,
                                "exit_ask": 7.24,
                                "exit_quote_time": "2026-05-13T13:51:12Z",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    events = [
        {
            "event_type": "option_quote",
            "payload": {
                "symbol": "QQQ260514P00711000",
                "timestamp": "2026-05-13T13:49:42.800000Z",
                "bid_price": 7.2,
                "ask_price": 7.29,
            },
        },
        {
            "event_type": "option_quote",
            "payload": {
                "symbol": "QQQ260514P00711000",
                "timestamp": "2026-05-13T13:51:12.200000Z",
                "bid_price": 7.18,
                "ask_price": 7.24,
            },
        },
    ]
    events_path.write_text(
        "\n".join(json.dumps(event) for event in events) + "\n",
        encoding="utf-8",
    )

    summary = build_sidecar_coverage(
        session_json=session_path,
        events_jsonl=events_path,
        window_seconds=1.0,
    )

    assert summary["completed_leg_count"] == 1
    assert summary["entry_sidecar_coverage_pct"] == 100.0
    assert summary["exit_sidecar_coverage_pct"] == 100.0
    assert summary["complete_quote_backed_leg_pct"] == 100.0
    assert summary["evidence_status"] == "complete_session_and_sidecar_quote_evidence"


def test_build_sidecar_coverage_fails_closed_when_sidecar_event_is_missing(tmp_path) -> None:
    session_path = tmp_path / "session.json"
    events_path = tmp_path / "events.jsonl"
    session_path.write_text(
        json.dumps(
            {
                "completed_trades": [
                    {
                        "entry_time_et": "2026-05-13T09:49:52-04:00",
                        "exit_time_et": "2026-05-13T09:51:27-04:00",
                        "legs": [
                            {
                                "symbol": "QQQ260514P00711000",
                                "bid": 7.21,
                                "ask": 7.28,
                                "quote_time": "2026-05-13T13:49:43Z",
                                "exit_bid": 7.18,
                                "exit_ask": 7.24,
                                "exit_quote_time": "2026-05-13T13:51:12Z",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    events_path.write_text("", encoding="utf-8")

    summary = build_sidecar_coverage(
        session_json=session_path,
        events_jsonl=events_path,
        window_seconds=1.0,
    )

    assert summary["completed_leg_count"] == 1
    assert summary["complete_quote_backed_leg_pct"] == 0.0
    assert summary["evidence_status"] == "quote_sidecar_gaps_present"

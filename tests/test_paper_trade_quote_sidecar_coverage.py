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
    assert summary["quote_backed_evidence_gate"] == "pass"
    assert summary["quote_backed_projection_input_allowed"] is True
    assert summary["quote_backed_optimizer_input_allowed"] is True
    assert summary["quote_backed_promotion_input_allowed"] is True
    row = summary["rows"][0]
    assert row["entry_quote_gap_reason"] == "matched_session_and_sidecar_quote"
    assert row["exit_quote_gap_reason"] == "matched_session_and_sidecar_quote"
    assert round(row["entry_nearest_sidecar_quote_lag_seconds"], 3) == -0.2
    assert round(row["exit_nearest_sidecar_quote_lag_seconds"], 3) == 0.2


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
    assert summary["quote_backed_evidence_gate"] == "fail"
    assert summary["quote_backed_projection_input_allowed"] is False
    assert summary["quote_backed_optimizer_input_allowed"] is False
    assert summary["quote_backed_promotion_input_allowed"] is False
    assert summary["rows"][0]["entry_quote_gap_reason"] == "contract_not_in_sidecar_or_subscription_gap"
    assert summary["rows"][0]["exit_quote_gap_reason"] == "contract_not_in_sidecar_or_subscription_gap"


def test_build_sidecar_coverage_combines_capture_restarts(tmp_path) -> None:
    session_path = tmp_path / "session.json"
    entry_events = tmp_path / "entry_events.jsonl"
    exit_events = tmp_path / "exit_events.jsonl"
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
    entry_events.write_text(
        json.dumps(
            {
                "event_type": "option_quote",
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
                "payload": {
                    "symbol": "QQQ260514P00711000",
                    "timestamp": "2026-05-13T13:51:12Z",
                    "bid_price": 7.18,
                    "ask_price": 7.24,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary = build_sidecar_coverage(
        session_json=session_path,
        events_jsonl=[entry_events, exit_events],
        window_seconds=1.0,
    )

    assert summary["events_jsonl_count"] == 2
    assert summary["entry_sidecar_coverage_pct"] == 100.0
    assert summary["exit_sidecar_coverage_pct"] == 100.0
    assert summary["quote_backed_evidence_gate"] == "pass"
    row = summary["rows"][0]
    assert row["entry_sidecar_source_jsonl"].endswith("entry_events.jsonl")
    assert row["exit_sidecar_source_jsonl"].endswith("exit_events.jsonl")
    assert row["entry_quote_gap_reason"] == "matched_session_and_sidecar_quote"
    assert row["exit_quote_gap_reason"] == "matched_session_and_sidecar_quote"


def test_build_sidecar_coverage_rejects_invalid_bid_ask(tmp_path) -> None:
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
    events_path.write_text(
        json.dumps(
            {
                "event_type": "option_quote",
                "payload": {
                    "symbol": "QQQ260514P00711000",
                    "timestamp": "2026-05-13T13:49:43Z",
                    "bid_price": 7.30,
                    "ask_price": 7.20,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary = build_sidecar_coverage(
        session_json=session_path,
        events_jsonl=events_path,
        window_seconds=1.0,
    )

    assert summary["entry_sidecar_coverage_pct"] == 0.0
    assert summary["complete_quote_backed_leg_pct"] == 0.0
    assert summary["rows"][0]["entry_quote_gap_reason"] == "invalid_sidecar_bid_ask"

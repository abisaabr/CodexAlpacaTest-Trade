from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.build_realtime_quote_quality_sidecar import build_realtime_quote_quality_sidecar


def _write_events(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def test_build_realtime_quote_quality_sidecar_outputs_quote_backed_rows(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    _write_events(
        events,
        [
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:00.250000+00:00",
                "latency_seconds": 0.25,
                "payload": {
                    "symbol": "QQQ260515C00450000",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 1.00,
                    "ask_price": 1.04,
                    "bid_size": 10,
                    "ask_size": 9,
                },
            },
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:10.100000+00:00",
                "latency_seconds": 0.10,
                "payload": {
                    "symbol": "QQQ260515C00450000",
                    "timestamp": "2026-05-13T14:30:10+00:00",
                    "bp": 1.05,
                    "ap": 1.10,
                    "bs": 8,
                    "as": 7,
                },
            },
            {
                "event_type": "option_trade",
                "observed_at_utc": "2026-05-13T14:30:11+00:00",
                "payload": {
                    "symbol": "QQQ260515C00450000",
                    "timestamp": "2026-05-13T14:30:11+00:00",
                    "price": 1.08,
                    "size": 2,
                },
            },
            {
                "event_type": "stock_quote",
                "observed_at_utc": "2026-05-13T14:30:00.050000+00:00",
                "payload": {
                    "symbol": "QQQ",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 450.00,
                    "ask_price": 450.02,
                },
            },
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:00+00:00",
                "payload": {
                    "symbol": "SPY260515C00500000",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 1.00,
                    "ask_price": 0.99,
                },
            },
        ],
    )

    summary = build_realtime_quote_quality_sidecar(
        events_jsonl=events,
        output_dir=tmp_path / "out",
        underlyings={"QQQ"},
    )

    assert summary["quality_status"] == "quote_events_ready_for_asof_replay_join"
    assert summary["stats"]["accepted_option_quotes"] == 2
    assert summary["stats"]["accepted_option_trades"] == 1
    assert summary["stats"]["accepted_stock_quotes"] == 1
    quotes = pd.read_csv(tmp_path / "out" / "option_quote_sidecar.csv")
    assert quotes["quote_source"].tolist() == ["option_quote_bid_ask", "option_quote_bid_ask"]
    assert quotes["relative_spread"].iloc[0] == pytest.approx(0.04 / 1.02)
    minutes = pd.read_csv(tmp_path / "out" / "option_quote_quality_by_minute.csv")
    assert len(minutes) == 1
    assert minutes["symbol"].iloc[0] == "QQQ260515C00450000"
    assert minutes["quote_count"].iloc[0] == 2
    assert minutes["option_trade_print_count"].iloc[0] == 1
    assert minutes["quote_source"].iloc[0] == "option_quote_bid_ask"
    stocks = pd.read_csv(tmp_path / "out" / "stock_quote_sidecar.csv")
    assert stocks["quote_source"].iloc[0] == "stock_quote_bid_ask"


def test_build_realtime_quote_quality_sidecar_reports_no_quotes(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    _write_events(
        events,
        [
            {
                "event_type": "stock_bar",
                "observed_at_utc": "2026-05-13T14:30:00+00:00",
                "payload": {"symbol": "QQQ", "timestamp": "2026-05-13T14:29:00+00:00"},
            }
        ],
    )

    summary = build_realtime_quote_quality_sidecar(
        events_jsonl=events,
        output_dir=tmp_path / "out",
    )

    assert summary["quality_status"] == "no_option_quote_events"
    assert summary["stats"]["accepted_option_quotes"] == 0
    assert (tmp_path / "out" / "quote_quality_sidecar_summary.json").exists()

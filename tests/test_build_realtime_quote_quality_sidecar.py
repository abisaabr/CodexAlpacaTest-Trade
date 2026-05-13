from __future__ import annotations

import json
import subprocess
import sys
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


def test_build_realtime_quote_quality_sidecar_combines_capture_restarts(tmp_path: Path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    _write_events(
        first,
        [
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:00.250000+00:00",
                "payload": {
                    "symbol": "QQQ260515C00450000",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 1.00,
                    "ask_price": 1.04,
                },
            },
        ],
    )
    _write_events(
        second,
        [
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:31:00.250000+00:00",
                "payload": {
                    "symbol": "QQQ260515C00450000",
                    "timestamp": "2026-05-13T14:31:00+00:00",
                    "bid_price": 1.08,
                    "ask_price": 1.12,
                },
            },
        ],
    )

    summary = build_realtime_quote_quality_sidecar(
        events_jsonl=[first, second],
        output_dir=tmp_path / "out_combined",
        underlyings={"QQQ"},
    )

    assert summary["events_jsonl_count"] == 2
    assert summary["quality_status"] == "quote_events_ready_for_asof_replay_join"
    assert summary["stats"]["accepted_option_quotes"] == 2
    quotes = pd.read_csv(tmp_path / "out_combined" / "option_quote_sidecar.csv")
    assert quotes["event_time_utc"].tolist() == [
        "2026-05-13T14:30:00+00:00",
        "2026-05-13T14:31:00+00:00",
    ]


def test_build_realtime_quote_quality_sidecar_filters_exact_option_symbols(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    _write_events(
        events,
        [
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:00.250000+00:00",
                "payload": {
                    "symbol": "QQQ260515C00450000",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 1.00,
                    "ask_price": 1.04,
                },
            },
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:00.250000+00:00",
                "payload": {
                    "symbol": "QQQ260515C00451000",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 0.90,
                    "ask_price": 0.94,
                },
            },
        ],
    )

    summary = build_realtime_quote_quality_sidecar(
        events_jsonl=events,
        output_dir=tmp_path / "out_exact_symbols",
        option_symbols={"QQQ260515C00450000"},
    )

    assert summary["stats"]["accepted_option_quotes"] == 1
    assert summary["stats"]["filtered_events"] == 1
    assert summary["stats"]["option_symbol_filter"] == ["QQQ260515C00450000"]
    quotes = pd.read_csv(tmp_path / "out_exact_symbols" / "option_quote_sidecar.csv")
    assert quotes["option_symbol"].tolist() == ["QQQ260515C00450000"]


def test_build_realtime_quote_quality_sidecar_cli_filters_option_symbols_file(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    _write_events(
        events,
        [
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:00.250000+00:00",
                "payload": {
                    "symbol": "QQQ260515C00450000",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 1.00,
                    "ask_price": 1.04,
                },
            },
            {
                "event_type": "option_quote",
                "observed_at_utc": "2026-05-13T14:30:00.250000+00:00",
                "payload": {
                    "symbol": "SPY260515P00500000",
                    "timestamp": "2026-05-13T14:30:00+00:00",
                    "bid_price": 1.10,
                    "ask_price": 1.14,
                },
            },
        ],
    )
    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("spy260515p00500000\n", encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/build_realtime_quote_quality_sidecar.py",
            "--events-jsonl",
            str(events),
            "--output-dir",
            str(tmp_path / "out_symbols_file"),
            "--option-symbols-file",
            str(symbols_file),
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
    )

    assert "SPY260515P00500000" in completed.stdout
    quotes = pd.read_csv(tmp_path / "out_symbols_file" / "option_quote_sidecar.csv")
    assert quotes["option_symbol"].tolist() == ["SPY260515P00500000"]

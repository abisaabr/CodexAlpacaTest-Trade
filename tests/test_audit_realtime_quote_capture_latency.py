from __future__ import annotations

import importlib.util
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "audit_realtime_quote_capture_latency.py"

SPEC = importlib.util.spec_from_file_location("audit_realtime_quote_capture_latency", MODULE_PATH)
assert SPEC is not None
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_audit_events_summarizes_latency_symbols_and_spreads(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    rows = [
        {
            "event_type": "option_quote",
            "observed_at_utc": "2026-05-13T13:30:01+00:00",
            "latency_seconds": 0.25,
            "payload": {
                "symbol": "QQQ260514P00711000",
                "bid_price": 1.0,
                "ask_price": 1.1,
                "timestamp": "2026-05-13T13:30:00.750000+00:00",
            },
        },
        {
            "event_type": "option_quote",
            "observed_at_utc": "2026-05-13T13:30:02+00:00",
            "latency_seconds": 0.5,
            "payload": {
                "symbol": "QQQ260514P00711000",
                "bid_price": 2.0,
                "ask_price": 2.2,
                "timestamp": "2026-05-13T13:30:01.500000+00:00",
            },
        },
        {
            "event_type": "stock_bar",
            "observed_at_utc": "2026-05-13T13:31:00+00:00",
            "latency_seconds": 60.0,
            "payload": {"symbol": "QQQ"},
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    summary = MODULE.audit_events(path)

    assert summary["event_counts"] == {"option_quote": 2, "stock_bar": 1}
    assert summary["unique_symbol_count"] == 2
    assert summary["latency_seconds"]["count"] == 3
    assert summary["latency_seconds_by_event_type"]["option_quote"]["p90"] == 0.25
    assert summary["option_quote_spread"]["count"] == 2
    assert summary["option_quote_relative_spread"]["p50"] > 0
    assert summary["top_symbols"][0][0] == "QQQ260514P00711000"

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

import pytest

from alpaca_lab.multi_ticker_portfolio.config import default_portfolio_config
from alpaca_lab.multi_ticker_portfolio.realtime_shadow import (
    JsonlEventWriter,
    RealtimeShadowMonitor,
    RealtimeShadowPlan,
    RealtimeShadowStats,
    data_feed_from_name,
    event_latency_seconds,
    merge_option_subscription_symbols,
    option_feed_from_name,
)


def test_feed_name_mapping_accepts_runtime_config_values() -> None:
    assert data_feed_from_name("sip").value == "sip"
    assert data_feed_from_name("iex").value == "iex"
    assert option_feed_from_name("opra").value == "opra"
    assert option_feed_from_name("indicative").value == "indicative"


def test_feed_name_mapping_rejects_unsupported_values() -> None:
    with pytest.raises(ValueError):
        data_feed_from_name("not_a_feed")
    with pytest.raises(ValueError):
        option_feed_from_name("not_a_feed")


def test_event_latency_uses_stream_timestamp() -> None:
    observed = datetime(2026, 5, 6, 14, 31, 2, tzinfo=UTC)
    payload = {"timestamp": (observed - timedelta(seconds=1.25)).isoformat()}

    assert event_latency_seconds(payload, observed_at_utc=observed) == pytest.approx(1.25)


def test_event_latency_tolerates_missing_timestamp() -> None:
    assert event_latency_seconds({"symbol": "QQQ"}, observed_at_utc=datetime.now(UTC)) is None


def test_shadow_stats_tracks_event_counts_and_max_latency() -> None:
    stats = RealtimeShadowStats()

    stats.record("stock_bar", 0.4)
    stats.record("stock_bar", 0.7)
    stats.record("option_quote", 0.2)
    stats.record("unknown", None)

    assert stats.stock_bar_events == 2
    assert stats.option_quote_events == 1
    assert stats.unknown_events == 1
    assert stats.max_event_latency_seconds == pytest.approx(0.7)
    assert stats.last_event_at_utc is not None
    payload = stats.to_dict()
    assert payload["latency_sample_count"] == 3
    assert payload["latency_p50_seconds"] == pytest.approx(0.4)
    assert payload["latency_by_event_type"]["stock_bar"]["sample_count"] == 2
    assert payload["latency_by_event_type"]["stock_bar"]["p50_seconds"] == pytest.approx(0.4)
    assert payload["latency_by_event_type"]["stock_bar"]["max_seconds"] == pytest.approx(0.7)
    assert payload["latency_by_event_type"]["option_quote"]["sample_count"] == 1
    assert payload["latency_by_event_type"]["option_quote"]["p50_seconds"] == pytest.approx(0.2)
    assert "_latency_samples" not in payload
    assert "_latency_samples_by_event_type" not in payload


def test_shadow_stats_separates_quote_latency_from_bar_completion_latency() -> None:
    stats = RealtimeShadowStats()

    stats.record("stock_bar", 64.0)
    stats.record("option_quote", 0.02)
    stats.record("option_quote", 0.04)

    payload = stats.to_dict()

    assert payload["latency_p50_seconds"] == pytest.approx(0.04)
    assert payload["latency_by_event_type"]["stock_bar"]["p50_seconds"] == pytest.approx(64.0)
    assert payload["latency_by_event_type"]["option_quote"]["p50_seconds"] == pytest.approx(0.02)
    assert payload["latency_by_event_type"]["option_quote"]["max_seconds"] == pytest.approx(0.04)


def test_shadow_stats_tracks_option_quote_spreads() -> None:
    stats = RealtimeShadowStats()

    stats.record("option_quote", 0.2, payload={"bid_price": "1.00", "ask_price": "1.10"})
    stats.record("option_quote", 0.3, payload={"bp": 2.0, "ap": 2.4})
    stats.record("option_quote", 0.4, payload={"bid_price": 3.0, "ask_price": 2.9})

    payload = stats.to_dict()

    assert payload["option_quote_events"] == 3
    assert payload["option_quote_spread_sample_count"] == 2
    assert payload["option_quote_spread_p50"] == pytest.approx(0.1)
    assert payload["option_quote_spread_max"] == pytest.approx(0.4)
    assert payload["option_quote_relative_spread_p50"] == pytest.approx(0.1 / 1.05)
    assert "_option_quote_spread_samples" not in payload
    assert "_option_quote_relative_spread_samples" not in payload


def test_shadow_plan_serializes_subscription_scope() -> None:
    plan = RealtimeShadowPlan(
        trade_date="2026-05-06",
        underlyings=["QQQ", "SPY"],
        option_symbols=["QQQ260507C00400000"],
        stock_feed="sip",
        option_feed="opra",
        option_symbol_limit=900,
        generated_at_utc="2026-05-06T14:30:00+00:00",
    )

    payload = plan.to_dict()

    assert payload["trade_date"] == "2026-05-06"
    assert payload["underlyings"] == ["QQQ", "SPY"]
    assert payload["option_symbols"] == ["QQQ260507C00400000"]
    assert payload["forced_option_symbols"] == []
    assert payload["missing_forced_option_symbols"] == []
    assert payload["source"] == "rest_bootstrap_for_realtime_shadow"


def test_merge_option_subscription_symbols_keeps_forced_symbols_first() -> None:
    symbols, notes = merge_option_subscription_symbols(
        discovered_symbols=[
            "SPY260515C00600000",
            "QQQ260515C00450000",
            "IWM260515P00200000",
        ],
        extra_symbols=["QQQ260515P00440000", "QQQ260515C00450000"],
        max_option_symbols=3,
    )

    assert symbols == [
        "QQQ260515C00450000",
        "QQQ260515P00440000",
        "IWM260515P00200000",
    ]
    assert "forced extra option symbols into capture plan: 2" in notes
    assert "truncated option subscriptions from 4 to 3" in notes


def test_merge_option_subscription_symbols_buffers_nearby_runtime_strikes() -> None:
    symbols, notes = merge_option_subscription_symbols(
        discovered_symbols=[
            "AMD260515P00425000",
            "AVGO260515P00412500",
            "QQQ260514C00708000",
            "QQQ260514P00710000",
            "QQQ260514P00715000",
            "SPY260514P00743000",
            "TSM260515C00400000",
        ],
        extra_symbols=[
            "QQQ260514P00711000",
            "SPY260514P00739000",
        ],
        max_option_symbols=5,
    )

    assert symbols == [
        "QQQ260514P00711000",
        "SPY260514P00739000",
        "QQQ260514P00710000",
        "QQQ260514P00715000",
        "SPY260514P00743000",
    ]
    assert "truncated option subscriptions from 9 to 5" in notes
    assert "AMD260515P00425000" not in symbols


def test_session_trade_symbols_are_loaded_for_forced_capture(tmp_path) -> None:
    base = default_portfolio_config()
    config = base.model_copy(
        update={"execution": base.execution.model_copy(update={"state_root": tmp_path})}
    )
    session_path = tmp_path / "session_2026-05-13.json"
    session_path.write_text(
        json.dumps(
            {
                "open_trades": [
                    {
                        "legs": [
                            {"symbol": "QQQ260514P00703000"},
                            {"option_symbol": "AMD260515P00422500"},
                        ]
                    }
                ],
                "completed_trades": [
                    {
                        "legs": [
                            {"symbol": "QQQ260514P00703000"},
                            {"symbol": "SPY260514C00739000"},
                        ]
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monitor = RealtimeShadowMonitor.__new__(RealtimeShadowMonitor)
    monitor.portfolio_config = config
    monitor.include_session_trade_symbols = True

    assert monitor._session_trade_option_symbols(datetime(2026, 5, 13).date()) == [
        "AMD260515P00422500",
        "QQQ260514P00703000",
        "SPY260514C00739000",
    ]


def test_jsonl_event_writer_keeps_valid_lines_until_close(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = JsonlEventWriter(path)

    writer.write({"event_type": "stock_quote", "symbol": "QQQ"})
    writer.write({"event_type": "option_quote", "symbol": "QQQ260508C00697000"})
    writer.close()
    writer.close()

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [
        {"event_type": "stock_quote", "symbol": "QQQ"},
        {"event_type": "option_quote", "symbol": "QQQ260508C00697000"},
    ]


def test_jsonl_event_writer_flushes_each_line_before_close(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = JsonlEventWriter(path)

    writer.write({"event_type": "option_quote", "symbol": "QQQ260508C00697000"})

    assert path.read_text(encoding="utf-8").strip()
    writer.close()

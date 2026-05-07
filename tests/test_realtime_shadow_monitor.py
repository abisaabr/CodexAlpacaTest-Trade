from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from alpaca_lab.multi_ticker_portfolio.realtime_shadow import (
    RealtimeShadowPlan,
    RealtimeShadowStats,
    data_feed_from_name,
    event_latency_seconds,
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
    assert "_latency_samples" not in payload


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
    assert payload["source"] == "rest_bootstrap_for_realtime_shadow"

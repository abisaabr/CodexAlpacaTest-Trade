from __future__ import annotations

import json
from pathlib import Path

from scripts.analyze_micro_scalp_shadow import QuotePoint, load_option_quotes, parse_underlyings, simulate_symbol
from scripts.analyze_micro_scalp_signal_grid import GridSpec, simulate_contract
from scripts.run_microstructure_event_replay_shard import (
    StockQuotePoint,
    _stock_impulse,
    _write_progress,
    simulate_contract as simulate_microstructure_contract,
)


def _quote(ts: float, bid: float, ask: float) -> QuotePoint:
    return QuotePoint(
        ts_epoch=ts,
        observed_epoch=ts,
        bid=bid,
        ask=ask,
        bid_size=10.0,
        ask_size=10.0,
    )


def test_micro_scalp_shadow_uses_ask_to_bid_economics() -> None:
    points = [
        _quote(0.0, 1.00, 1.02),
        _quote(1.0, 1.04, 1.05),
    ]

    opportunities, summary = simulate_symbol(
        symbol="QQQ260508C00690000",
        points=points,
        target_pct=0.01,
        stop_pct=0.006,
        max_hold_seconds=5.0,
        entry_stride=1,
        min_premium=0.15,
        max_premium=12.0,
        max_relative_spread=0.04,
        max_absolute_spread=0.20,
        min_quote_size=1.0,
        fee_per_contract=0.65,
    )

    assert len(opportunities) == 1
    assert opportunities[0]["exit_reason"] == "target"
    assert opportunities[0]["gross_pnl_per_contract"] == 2.0
    assert opportunities[0]["net_pnl_per_contract"] == 0.7
    assert summary["winning_trade_count"] == 1


def test_micro_scalp_signal_grid_is_causal_and_non_overlapping() -> None:
    points = [
        _quote(0.0, 1.00, 1.02),
        _quote(1.0, 1.04, 1.05),
        _quote(2.0, 1.07, 1.08),
        _quote(3.0, 1.02, 1.04),
    ]
    spec = GridSpec(
        lookback_seconds=1.0,
        momentum_threshold_pct=0.015,
        target_pct=0.01,
        stop_pct=0.006,
        max_hold_seconds=5.0,
    )

    trades = simulate_contract(
        symbol="QQQ260508C00690000",
        points=points,
        spec=spec,
        min_premium=0.15,
        max_premium=12.0,
        max_relative_spread=0.04,
        max_absolute_spread=0.20,
        min_quote_size=1.0,
        fee_per_contract=0.65,
    )

    assert len(trades) == 1
    assert trades[0]["exit_reason"] == "target"
    assert trades[0]["entry_time_utc"].startswith("1970-01-01T00:00:01")
    assert trades[0]["net_pnl_per_contract"] == 0.7


def test_load_option_quotes_can_filter_underlyings(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text(
        "\n".join(
            [
                '{"event_type":"option_quote","observed_at_utc":"2026-05-07T14:00:00Z","payload":{"symbol":"QQQ260508C00690000","timestamp":"2026-05-07T14:00:00Z","bid_price":1.0,"ask_price":1.02,"bid_size":1,"ask_size":1}}',
                '{"event_type":"option_quote","observed_at_utc":"2026-05-07T14:00:00Z","payload":{"symbol":"SPY260508C00600000","timestamp":"2026-05-07T14:00:00Z","bid_price":1.0,"ask_price":1.02,"bid_size":1,"ask_size":1}}',
            ]
        ),
        encoding="utf-8",
    )

    quotes, stats = load_option_quotes(events, underlyings=parse_underlyings("QQQ"))

    assert list(quotes) == ["QQQ260508C00690000"]
    assert stats["accepted_quote_count"] == 1
    assert stats["filtered_quote_count"] == 1


def test_microstructure_replay_writes_progress_payload(tmp_path: Path) -> None:
    progress_path = tmp_path / "microstructure_replay_progress.json"

    _write_progress(
        progress_path,
        wave_id="wave_test",
        worker_id="worker_test",
        phase="running_replay",
        completed_grid_count=5,
        total_grid_count=10,
        contract_count=3,
        review_like_count=2,
        started_epoch=1.0,
        ingest_stats={"accepted_option_quote_count": 12},
    )

    payload = json.loads(progress_path.read_text(encoding="utf-8"))
    assert payload["wave_id"] == "wave_test"
    assert payload["worker_id"] == "worker_test"
    assert payload["phase"] == "running_replay"
    assert payload["progress_ratio"] == 0.5
    assert payload["contract_count"] == 3
    assert payload["review_like_count"] == 2
    assert payload["broker_facing"] is False
    assert payload["paper_orders"] is False
    assert payload["ingest_stats"]["accepted_option_quote_count"] == 12


def test_microstructure_stock_impulse_aligns_puts_and_calls() -> None:
    stock_points = [
        StockQuotePoint(ts_epoch=0.0, observed_epoch=0.0, bid=100.0, ask=100.02),
        StockQuotePoint(ts_epoch=1.0, observed_epoch=1.0, bid=100.2, ask=100.22),
        StockQuotePoint(ts_epoch=2.0, observed_epoch=2.0, bid=99.8, ask=99.82),
    ]

    _, call_impulse = _stock_impulse(
        stock_points,
        ts_epoch=1.0,
        lookback_seconds=1.0,
        option_right="call",
        hint_index=0,
    )
    _, put_impulse = _stock_impulse(
        stock_points,
        ts_epoch=2.0,
        lookback_seconds=1.0,
        option_right="put",
        hint_index=0,
    )

    assert call_impulse is not None and call_impulse > 0
    assert put_impulse is not None and put_impulse > 0


def test_microstructure_replay_supports_fast_target_exit() -> None:
    option_points = [
        _quote(0.0, 1.00, 1.02),
        _quote(1.0, 1.05, 1.06),
        _quote(2.0, 1.11, 1.12),
    ]
    stock_points = [
        StockQuotePoint(ts_epoch=0.0, observed_epoch=0.0, bid=100.0, ask=100.02),
        StockQuotePoint(ts_epoch=1.0, observed_epoch=1.0, bid=100.2, ask=100.22),
        StockQuotePoint(ts_epoch=2.0, observed_epoch=2.0, bid=100.3, ask=100.32),
    ]
    spec = {
        "grid_id": "micro_test",
        "grid_row_index": 1,
        "signal_mode": "stock_impulse_option_confirm",
        "option_right": "both",
        "lookback_seconds": 1.0,
        "option_momentum_threshold_pct": 0.02,
        "stock_impulse_threshold_pct": 0.001,
        "target_pct": 0.03,
        "stop_pct": 0.015,
        "max_hold_seconds": 15.0,
        "max_entry_quote_age_seconds": 1.0,
        "max_exit_quote_age_seconds": 1.0,
        "min_premium": 0.15,
        "max_premium": 12.0,
        "max_relative_spread": 0.04,
        "max_absolute_spread": 0.20,
        "max_exit_relative_spread": 0.10,
        "min_quote_size": 1.0,
        "trail_activation_pct": 0.0,
        "trail_retrace_pct": 0.0,
        "spread_compression_factor": 0.75,
        "cooldown_seconds": 0.0,
    }

    trades, counters = simulate_microstructure_contract(
        symbol="QQQ260508C00690000",
        points=option_points,
        stock_points=stock_points,
        spec=spec,
        fee_per_contract=0.65,
    )

    assert counters["signal_count"] == 1
    assert len(trades) == 1
    assert trades[0]["exit_reason"] == "target"
    assert trades[0]["net_pnl_per_contract"] > 0
    assert trades[0]["spread_cost_to_target"] > 0


def test_microstructure_replay_rejects_latency_chase_entries() -> None:
    option_points = [
        _quote(0.0, 1.00, 1.02),
        _quote(1.0, 1.05, 1.06),
        _quote(1.3, 1.18, 1.20),
        _quote(2.0, 1.25, 1.27),
    ]
    stock_points = [
        StockQuotePoint(ts_epoch=0.0, observed_epoch=0.0, bid=100.0, ask=100.02),
        StockQuotePoint(ts_epoch=1.0, observed_epoch=1.0, bid=100.2, ask=100.22),
        StockQuotePoint(ts_epoch=2.0, observed_epoch=2.0, bid=100.3, ask=100.32),
    ]
    spec = {
        "grid_id": "micro_latency_test",
        "grid_row_index": 1,
        "signal_mode": "stock_impulse_option_confirm",
        "option_right": "both",
        "lookback_seconds": 1.0,
        "option_momentum_threshold_pct": 0.02,
        "stock_impulse_threshold_pct": 0.001,
        "target_pct": 0.03,
        "stop_pct": 0.015,
        "max_hold_seconds": 15.0,
        "entry_latency_seconds": 0.25,
        "entry_fill_wait_seconds": 0.50,
        "max_entry_chase_pct": 0.01,
        "max_spread_cost_to_target": 1.0,
        "max_entry_quote_age_seconds": 1.0,
        "max_exit_quote_age_seconds": 1.0,
        "min_premium": 0.15,
        "max_premium": 12.0,
        "max_relative_spread": 0.04,
        "max_absolute_spread": 0.20,
        "max_exit_relative_spread": 0.10,
        "min_quote_size": 1.0,
        "trail_activation_pct": 0.0,
        "trail_retrace_pct": 0.0,
        "spread_compression_factor": 0.75,
        "cooldown_seconds": 0.0,
    }

    trades, counters = simulate_microstructure_contract(
        symbol="QQQ260508C00690000",
        points=option_points,
        stock_points=stock_points,
        spec=spec,
        fee_per_contract=0.65,
    )

    assert counters["signal_count"] == 1
    assert trades == []
    assert counters["fill_failure_reasons"] == {"entry_chase_above_gate": 1}


def test_microstructure_replay_rejects_spread_dominated_targets() -> None:
    option_points = [
        _quote(0.0, 0.95, 0.97),
        _quote(1.0, 1.00, 1.10),
        _quote(2.0, 1.20, 1.25),
    ]
    stock_points = [
        StockQuotePoint(ts_epoch=0.0, observed_epoch=0.0, bid=100.0, ask=100.02),
        StockQuotePoint(ts_epoch=1.0, observed_epoch=1.0, bid=100.2, ask=100.22),
        StockQuotePoint(ts_epoch=2.0, observed_epoch=2.0, bid=100.3, ask=100.32),
    ]
    spec = {
        "grid_id": "micro_spread_cost_test",
        "grid_row_index": 1,
        "signal_mode": "stock_impulse_option_confirm",
        "option_right": "both",
        "lookback_seconds": 1.0,
        "option_momentum_threshold_pct": 0.02,
        "stock_impulse_threshold_pct": 0.001,
        "target_pct": 0.03,
        "stop_pct": 0.015,
        "max_hold_seconds": 15.0,
        "max_entry_chase_pct": 0.05,
        "max_spread_cost_to_target": 0.50,
        "max_entry_quote_age_seconds": 1.0,
        "max_exit_quote_age_seconds": 1.0,
        "min_premium": 0.15,
        "max_premium": 12.0,
        "max_relative_spread": 0.20,
        "max_absolute_spread": 0.20,
        "max_exit_relative_spread": 0.30,
        "min_quote_size": 1.0,
        "trail_activation_pct": 0.0,
        "trail_retrace_pct": 0.0,
        "spread_compression_factor": 0.75,
        "cooldown_seconds": 0.0,
    }

    trades, counters = simulate_microstructure_contract(
        symbol="QQQ260508C00690000",
        points=option_points,
        stock_points=stock_points,
        spec=spec,
        fee_per_contract=0.65,
    )

    assert counters["signal_count"] == 1
    assert trades == []
    assert counters["fill_failure_reasons"] == {"spread_cost_above_target_gate": 1}

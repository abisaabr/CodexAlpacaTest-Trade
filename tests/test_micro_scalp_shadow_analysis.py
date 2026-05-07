from __future__ import annotations

from pathlib import Path

from scripts.analyze_micro_scalp_shadow import QuotePoint, load_option_quotes, parse_underlyings, simulate_symbol
from scripts.analyze_micro_scalp_signal_grid import GridSpec, simulate_contract


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

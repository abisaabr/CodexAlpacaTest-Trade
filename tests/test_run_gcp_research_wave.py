from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scripts.run_gcp_research_wave import (
    REAL_STOCK_BAR_EVIDENCE_MODE,
    _variant_stock_strategy,
    filter_variants,
    load_variants,
    run,
    score_variant,
    variant_timing_parameters,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _write_trending_bars(path: Path) -> None:
    timestamps = pd.date_range("2026-04-21T13:30:00Z", periods=80, freq="1min")
    closes = [100.0 + i * 0.20 for i in range(len(timestamps))]
    frame = pd.DataFrame(
        {
            "symbol": ["QQQ"] * len(timestamps),
            "timestamp": timestamps,
            "open": [close - 0.03 for close in closes],
            "high": [close + 0.04 for close in closes],
            "low": [close - 0.06 for close in closes],
            "close": closes,
            "volume": [10000 + i * 10 for i in range(len(timestamps))],
        }
    )
    frame.to_parquet(path, index=False)


def test_filter_variants_uses_chunk_and_limits(tmp_path: Path) -> None:
    variants = [
        {"variant_id": f"v{i}", "queue_id": "q", "priority": 1, "symbol": "QQQ"}
        for i in range(10)
    ]
    manifest = {"chunks": [{"chunk_id": "chunk_0001", "start_index": 2, "end_index": 7}]}

    selected = filter_variants(
        variants,
        wave_manifest=manifest,
        chunk_id="chunk_0001",
        queue_ids=set(),
        symbols={"QQQ"},
        priorities={1},
        max_variants=3,
    )

    assert [row["variant_id"] for row in selected] == ["v2", "v3", "v4"]


def test_score_variant_keeps_metadata_proxy_non_promotable() -> None:
    row = score_variant(
        {
            "variant_id": "rq002__qqq__tight",
            "queue_id": "RQ-002",
            "priority": 2,
            "symbol": "QQQ",
            "variant_type": "single_leg_repair",
            "parameters": {"liquidity_gate": "tight", "stop_loss_multiple": 0.18},
        },
        evidence_mode="metadata_proxy_smoke",
    )

    assert row["recommendation"] == "hold_for_real_backtest"
    assert row["broker_facing"] is False
    assert row["live_manifest_effect"] == "none"


def test_timing_profile_defaults_drive_stock_proxy_timing() -> None:
    fast = variant_timing_parameters({"timing_profile": "fast"})
    slow = variant_timing_parameters({"timing_profile": "slow"})

    assert fast["hard_exit_minute"] < slow["hard_exit_minute"]
    assert fast["liquidity_gate"] == "tight"
    assert slow["liquidity_gate"] == "baseline"

    fast_strategy = _variant_stock_strategy(
        {"variant_id": "fast", "symbol": "QQQ", "parameters": {"timing_profile": "fast"}}
    )
    slow_strategy = _variant_stock_strategy(
        {"variant_id": "slow", "symbol": "QQQ", "parameters": {"timing_profile": "slow"}}
    )

    assert fast_strategy.timeout_bars < slow_strategy.timeout_bars
    assert fast_strategy.fast_window < slow_strategy.fast_window


def test_choppy_premium_strategy_uses_range_bound_timeout_proxy() -> None:
    strategy = _variant_stock_strategy(
        {
            "variant_id": "qqq_choppy_condor",
            "symbol": "QQQ",
            "source_strategy_id": "qqq__choppy__call__iron_condor",
            "parameters": {
                "family_template": "iron_condor",
                "hard_exit_minute": 75,
                "stock_proxy_mode": "range_bound",
            },
        }
    )

    assert strategy.signal_mode == "range_bound"
    assert strategy.stop_pct == 0.0
    assert strategy.target_pct == 0.0
    assert strategy.timeout_bars == 75


def test_bull_put_credit_spread_keeps_bullish_stock_proxy_direction() -> None:
    strategy = _variant_stock_strategy(
        {
            "variant_id": "qqq_bull_put_credit",
            "symbol": "QQQ",
            "source_strategy_id": "qqq__bull__call__bull_put_credit_spread",
            "parameters": {
                "family_template": "bull_put_credit_spread",
                "timing_profile": "fast",
            },
        }
    )

    assert strategy.direction == 1
    assert strategy.signal_mode == "breakout"


def test_variant_stock_proxy_throttles_to_daily_first_signal() -> None:
    strategy = _variant_stock_strategy(
        {
            "variant_id": "qqq_bull_daily_first",
            "symbol": "QQQ",
            "source_strategy_id": "qqq__bull__call__single_leg_repair",
            "parameters": {
                "entry_signal_mode": "daily_first",
                "hard_exit_minute": 30,
                "max_signals_per_day": 1,
                "min_minutes_since_open": 15,
                "min_trend_gap_pct": 0.0,
                "stock_proxy_mode": "breakout",
            },
        }
    )
    timestamps = pd.date_range("2026-01-05 14:30", periods=80, freq="min", tz="UTC")
    bars = pd.DataFrame(
        {
            "symbol": ["QQQ"] * len(timestamps),
            "timestamp": timestamps,
            "open": [100.0 + index * 0.1 for index in range(len(timestamps))],
            "high": [100.1 + index * 0.1 for index in range(len(timestamps))],
            "low": [99.9 + index * 0.1 for index in range(len(timestamps))],
            "close": [100.0 + index * 0.1 for index in range(len(timestamps))],
            "volume": [1000] * len(timestamps),
        }
    )

    signals = strategy.generate_signals(bars)

    assert int((signals["signal"] != 0).sum()) == 1


def test_variant_stock_proxy_respects_entry_window() -> None:
    strategy = _variant_stock_strategy(
        {
            "variant_id": "qqq_bear_window",
            "symbol": "QQQ",
            "source_strategy_id": "qqq__bear__put__single_leg_repair",
            "parameters": {
                "entry_signal_mode": "daily_first",
                "hard_exit_minute": 30,
                "max_signals_per_day": 1,
                "max_minutes_since_open": 40,
                "min_minutes_since_open": 30,
                "min_trend_gap_pct": 0.0,
                "stock_proxy_mode": "breakout",
            },
        }
    )
    timestamps = pd.date_range("2026-01-05 14:30", periods=80, freq="min", tz="UTC")
    prices = [100.0 - index * 0.1 for index in range(len(timestamps))]
    bars = pd.DataFrame(
        {
            "symbol": ["QQQ"] * len(timestamps),
            "timestamp": timestamps,
            "open": prices,
            "high": [price + 0.05 for price in prices],
            "low": [price - 0.05 for price in prices],
            "close": prices,
            "volume": [1000] * len(timestamps),
        }
    )

    signals = strategy.generate_signals(bars)
    fired = signals.loc[signals["signal"] != 0, "timestamp"]

    assert len(fired) == 1
    fired_local = fired.dt.tz_convert("America/New_York").iloc[0]
    minute_since_open = fired_local.hour * 60 + fired_local.minute - (9 * 60 + 30)
    assert 30 <= minute_since_open <= 40


def test_run_writes_required_research_artifacts(tmp_path: Path) -> None:
    variants_path = tmp_path / "variants.jsonl"
    manifest_path = tmp_path / "wave.json"
    output_dir = tmp_path / "reports"
    _write_jsonl(
        variants_path,
        [
            {
                "variant_id": "rq001__qqq__debit_call_vertical__fast__same_day",
                "queue_id": "RQ-001-defined-risk-family-expansion",
                "priority": 1,
                "symbol": "QQQ",
                "variant_type": "defined_risk_family_expansion",
                "parameters": {
                    "family_template": "debit_call_vertical",
                    "timing_profile": "fast",
                    "dte_mode": "same_day",
                },
            },
            {
                "variant_id": "rq003__pltr__diagnostic",
                "queue_id": "RQ-003-loser-cluster-shadow-diagnostics",
                "priority": 3,
                "symbol": "PLTR",
                "variant_type": "loser_cluster_shadow_diagnostic",
                "parameters": {"avoid_after_loser_similarity": True},
            },
        ],
    )
    _write_json(
        manifest_path,
        {
            "wave_id": "test_wave",
            "chunks": [{"chunk_id": "chunk_0001", "start_index": 0, "end_index": 1}],
        },
    )
    args = argparse.Namespace(
        variants_jsonl=str(variants_path),
        wave_manifest_json=str(manifest_path),
        output_dir=str(output_dir),
        run_id="unit_run",
        chunk_id="chunk_0001",
        queue_id=[],
        symbol=[],
        priority=[],
        max_variants=None,
        evidence_mode="metadata_proxy_smoke",
        allow_non_smoke_evidence=False,
        bars_path=None,
        initial_cash=100_000.0,
        slippage_bps=5.0,
        fee_per_unit=0.01,
        allocation_fraction=0.10,
    )

    result = run(args)

    assert result["selected_variant_count"] == 2
    assert result["broker_facing"] is False
    for path in result["artifacts"].values():
        assert Path(path).exists()
    manifest = json.loads(Path(result["artifacts"]["research_run_manifest"]).read_text())
    assert manifest["required_outputs"] == [
        "research_run_manifest",
        "normalized_backtest_results",
        "train_test_or_walk_forward_summary",
        "after_cost_expectancy_table",
        "drawdown_and_tail_loss_report",
        "loser_cluster_comparison",
        "candidate_hold_kill_quarantine_recommendation",
    ]
    loaded = load_variants(variants_path)
    assert len(loaded) == 2


def test_run_real_stock_bar_smoke_uses_bars_without_promotion(tmp_path: Path) -> None:
    variants_path = tmp_path / "variants.jsonl"
    manifest_path = tmp_path / "wave.json"
    bars_path = tmp_path / "bars.parquet"
    output_dir = tmp_path / "reports"
    _write_trending_bars(bars_path)
    _write_jsonl(
        variants_path,
        [
            {
                "variant_id": "rq002__qqq__repair",
                "queue_id": "RQ-002-single-leg-repair-and-loss-filter",
                "priority": 2,
                "symbol": "QQQ",
                "variant_type": "single_leg_repair",
                "source_strategy_id": "qqq__base__trend_long_call_next_expiry",
                "parameters": {
                    "hard_exit_minute": 210,
                    "liquidity_gate": "baseline",
                    "profit_target_multiple": 0.45,
                    "stop_loss_multiple": 0.18,
                },
            }
        ],
    )
    _write_json(
        manifest_path,
        {
            "wave_id": "test_wave",
            "chunks": [{"chunk_id": "chunk_0001", "start_index": 0, "end_index": 0}],
        },
    )
    args = argparse.Namespace(
        variants_jsonl=str(variants_path),
        wave_manifest_json=str(manifest_path),
        output_dir=str(output_dir),
        run_id="real_bar_unit_run",
        chunk_id="chunk_0001",
        queue_id=[],
        symbol=[],
        priority=[],
        max_variants=None,
        evidence_mode=REAL_STOCK_BAR_EVIDENCE_MODE,
        allow_non_smoke_evidence=False,
        bars_path=str(bars_path),
        initial_cash=100_000.0,
        slippage_bps=5.0,
        fee_per_unit=0.01,
        allocation_fraction=0.10,
    )

    result = run(args)

    assert result["evidence_mode"] == REAL_STOCK_BAR_EVIDENCE_MODE
    assert result["broker_facing"] is False
    rows = json.loads(Path(result["artifacts"]["normalized_backtest_results_json"]).read_text())
    assert rows[0]["actual_trade_count"] > 0
    assert rows[0]["live_manifest_effect"] == "none"
    recommendation = json.loads(
        Path(result["artifacts"]["candidate_hold_kill_quarantine_recommendation"]).read_text()
    )
    assert recommendation["promotion_allowed"] is False
    assert "option-aware research" in recommendation["promotion_note"]

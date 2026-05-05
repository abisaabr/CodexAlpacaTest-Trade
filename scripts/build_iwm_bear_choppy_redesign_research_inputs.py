from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_qqq_regime_research_inputs import _variant, build_queue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an IWM-specific bear/choppy redesign queue focused on lower-target "
            "bear puts and two-sided range-reversion choppy entries."
        )
    )
    parser.add_argument("--symbol", default="IWM")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="ticker365_iwm_bear_choppy_redesign_20260505")
    return parser.parse_args()


def _bear_timing_profiles() -> list[dict[str, Any]]:
    return [
        {
            "timing_profile": "iwm_early_breakdown_20_90",
            "hard_exit_minute": 45,
            "min_minutes_since_open": 20,
            "max_minutes_since_open": 90,
            "min_trend_gap_pct": 0.0004,
            "entry_signal_mode": "daily_first",
            "cooldown_bars": 90,
            "max_signals_per_day": 1,
        },
        {
            "timing_profile": "iwm_morning_breakdown_45_150",
            "hard_exit_minute": 60,
            "min_minutes_since_open": 45,
            "max_minutes_since_open": 150,
            "min_trend_gap_pct": 0.0005,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 75,
            "max_signals_per_day": 2,
        },
        {
            "timing_profile": "iwm_midday_breakdown_120_255",
            "hard_exit_minute": 75,
            "min_minutes_since_open": 120,
            "max_minutes_since_open": 255,
            "min_trend_gap_pct": 0.0006,
            "entry_signal_mode": "daily_first",
            "cooldown_bars": 90,
            "max_signals_per_day": 1,
        },
        {
            "timing_profile": "iwm_late_breakdown_210_345",
            "hard_exit_minute": 60,
            "min_minutes_since_open": 210,
            "max_minutes_since_open": 345,
            "min_trend_gap_pct": 0.0005,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 60,
            "max_signals_per_day": 1,
        },
    ]


def _bear_exit_profiles() -> list[dict[str, Any]]:
    return [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_bear_defensive_scalp",
            "option_profit_target_pct": 0.22,
            "option_stop_loss_pct": 0.10,
            "min_option_hold_minutes": 2,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_bear_balanced",
            "option_profit_target_pct": 0.30,
            "option_stop_loss_pct": 0.14,
            "min_option_hold_minutes": 3,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_bear_runner",
            "option_profit_target_pct": 0.42,
            "option_stop_loss_pct": 0.20,
            "min_option_hold_minutes": 5,
        },
    ]


def _choppy_range_profiles() -> list[dict[str, Any]]:
    return [
        {
            "timing_profile": "iwm_morning_reversion_55_145",
            "hard_exit_minute": 40,
            "max_range_pct": 0.007,
            "max_trend_gap_pct": 0.0018,
            "max_midpoint_distance_pct": 0.0040,
            "min_minutes_since_open": 55,
            "max_minutes_since_open": 145,
        },
        {
            "timing_profile": "iwm_midday_reversion_120_265",
            "hard_exit_minute": 55,
            "max_range_pct": 0.009,
            "max_trend_gap_pct": 0.0022,
            "max_midpoint_distance_pct": 0.0045,
            "min_minutes_since_open": 120,
            "max_minutes_since_open": 265,
        },
        {
            "timing_profile": "iwm_late_reversion_210_335",
            "hard_exit_minute": 50,
            "max_range_pct": 0.011,
            "max_trend_gap_pct": 0.0028,
            "max_midpoint_distance_pct": 0.0050,
            "min_minutes_since_open": 210,
            "max_minutes_since_open": 335,
        },
    ]


def _choppy_exit_profiles() -> list[dict[str, Any]]:
    return [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_reversion_fast",
            "option_profit_target_pct": 0.22,
            "option_stop_loss_pct": 0.10,
            "min_option_hold_minutes": 2,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_reversion_balanced",
            "option_profit_target_pct": 0.32,
            "option_stop_loss_pct": 0.15,
            "min_option_hold_minutes": 3,
        },
    ]


def build_iwm_bear_choppy_rows(*, symbol: str, wave_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for timing_profile in _bear_timing_profiles():
        for exit_profile in _bear_exit_profiles():
            for family in ("single_leg_repair", "debit_put_vertical"):
                parameters = {
                    **timing_profile,
                    **exit_profile,
                    "dte_mode": "next_expiry",
                    "family_template": family,
                    "liquidity_gate": "tight",
                    "stock_proxy_mode": "breakout",
                    "wing_width_steps": 1,
                }
                rows.append(
                    _variant(
                        symbol=symbol,
                        regime="bear",
                        direction="put",
                        family=family,
                        parameters=parameters,
                        priority=1,
                        wave_id=wave_id,
                    )
                )

    direction_specs = [
        ("call", "lower_band", "single_leg_repair"),
        ("call", "lower_band", "debit_call_vertical"),
        ("put", "upper_band", "single_leg_repair"),
        ("put", "upper_band", "debit_put_vertical"),
    ]
    for range_profile in _choppy_range_profiles():
        for direction, range_entry_side, family in direction_specs:
            for range_edge_pct in (0.0005, 0.0010):
                for exit_profile in _choppy_exit_profiles():
                    parameters = {
                        **range_profile,
                        **exit_profile,
                        "cooldown_bars": 60,
                        "dte_mode": "next_expiry",
                        "entry_signal_mode": "rising_edge",
                        "family_template": family,
                        "liquidity_gate": "tight",
                        "max_signals_per_day": 2,
                        "min_range_pct": 0.0015,
                        "profit_target_multiple": 0.24,
                        "range_edge_pct": range_edge_pct,
                        "range_entry_side": range_entry_side,
                        "stock_proxy_mode": "range_bound",
                        "stop_loss_multiple": 0.10,
                        "timeout_only_stock_proxy": True,
                        "wing_width_steps": 1,
                    }
                    rows.append(
                        _variant(
                            symbol=symbol,
                            regime="choppy",
                            direction=direction,
                            family=family,
                            parameters=parameters,
                            priority=1,
                            wave_id=wave_id,
                        )
                    )
    return rows


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    symbol = str(args.symbol).upper()
    rows = build_iwm_bear_choppy_rows(symbol=symbol, wave_id=args.wave_id)
    queue = build_queue(rows=rows, wave_id=args.wave_id)
    manifest = {
        "broker_facing": False,
        "execution_effect": "none",
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "status": "ready_for_iwm_bear_choppy_redesign_backtest",
        "target_regimes": ["bear", "choppy"],
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": args.wave_id,
    }

    variants_path = output_dir / "iwm_bear_choppy_redesign_variants.jsonl"
    queue_path = output_dir / "iwm_bear_choppy_redesign_option_queue.json"
    manifest_path = output_dir / "iwm_bear_choppy_redesign_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

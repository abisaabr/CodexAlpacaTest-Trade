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
        description="Build QQQ choppy range-reversion research inputs."
    )
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="ticker365_qqq_choppy_reversion_20260505")
    return parser.parse_args()


def _range_profiles() -> list[dict[str, Any]]:
    return [
        {
            "timing_profile": "morning_reversion",
            "hard_exit_minute": 45,
            "max_range_pct": 0.006,
            "max_trend_gap_pct": 0.0015,
            "max_midpoint_distance_pct": 0.004,
            "min_minutes_since_open": 45,
            "max_minutes_since_open": 135,
        },
        {
            "timing_profile": "midday_reversion",
            "hard_exit_minute": 60,
            "max_range_pct": 0.008,
            "max_trend_gap_pct": 0.0020,
            "max_midpoint_distance_pct": 0.0045,
            "min_minutes_since_open": 120,
            "max_minutes_since_open": 270,
        },
        {
            "timing_profile": "late_reversion",
            "hard_exit_minute": 60,
            "max_range_pct": 0.010,
            "max_trend_gap_pct": 0.0025,
            "max_midpoint_distance_pct": 0.0050,
            "min_minutes_since_open": 210,
            "max_minutes_since_open": 330,
        },
    ]


def _exit_profiles() -> list[dict[str, Any]]:
    return [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "reversion_fast_takeprofit",
            "option_profit_target_pct": 0.30,
            "option_stop_loss_pct": 0.16,
            "min_option_hold_minutes": 2,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "reversion_balanced_runner",
            "option_profit_target_pct": 0.45,
            "option_stop_loss_pct": 0.22,
            "min_option_hold_minutes": 4,
        },
    ]


def build_choppy_reversion_rows(*, symbol: str, wave_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    direction_specs = [
        ("call", "lower_band", "single_leg_repair"),
        ("call", "lower_band", "debit_call_vertical"),
        ("put", "upper_band", "single_leg_repair"),
        ("put", "upper_band", "debit_put_vertical"),
    ]
    for profile in _range_profiles():
        for direction, range_entry_side, family in direction_specs:
            for range_edge_pct in (0.0005, 0.0010):
                for exit_profile in _exit_profiles():
                    parameters = {
                        **profile,
                        **exit_profile,
                        "cooldown_bars": 60,
                        "dte_mode": "next_expiry",
                        "entry_signal_mode": "rising_edge",
                        "family_template": family,
                        "liquidity_gate": "tight",
                        "max_signals_per_day": 2,
                        "min_range_pct": 0.0015,
                        "profit_target_multiple": 0.30,
                        "range_edge_pct": range_edge_pct,
                        "range_entry_side": range_entry_side,
                        "stock_proxy_mode": "range_bound",
                        "stop_loss_multiple": 0.12,
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

    rows = build_choppy_reversion_rows(symbol=args.symbol.upper(), wave_id=args.wave_id)
    queue = build_queue(rows=rows, wave_id=args.wave_id)
    manifest = {
        "broker_facing": False,
        "execution_effect": "none",
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "status": "ready_for_qqq_choppy_reversion_backtest",
        "target_regimes": ["choppy"],
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": args.wave_id,
    }

    variants_path = output_dir / "qqq_choppy_reversion_variants.jsonl"
    queue_path = output_dir / "qqq_choppy_reversion_option_queue.json"
    manifest_path = output_dir / "qqq_choppy_reversion_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

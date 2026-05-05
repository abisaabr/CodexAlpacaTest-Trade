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
        description="Build a focused QQQ choppy lower-band call refinement wave."
    )
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="ticker365_qqq_choppy_refine_20260505")
    return parser.parse_args()


def build_choppy_refine_rows(*, symbol: str, wave_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    time_windows = [
        ("late_morning_90_150", 90, 150, 45),
        ("late_morning_90_180", 90, 180, 45),
        ("filtered_morning_75_150", 75, 150, 45),
        ("tight_midpoint_105_165", 105, 165, 35),
    ]
    exit_profiles = [
        ("target30_stop12", 0.30, 0.12, 2),
        ("target30_stop14", 0.30, 0.14, 2),
        ("target35_stop16", 0.35, 0.16, 2),
    ]
    for timing_profile, min_minute, max_minute, hard_exit in time_windows:
        for range_edge_pct in (0.0005, 0.0010):
            for max_signals_per_day in (1, 2):
                for exit_name, target_pct, stop_pct, min_hold in exit_profiles:
                    parameters = {
                        "cooldown_bars": 60,
                        "dte_mode": "next_expiry",
                        "entry_signal_mode": "rising_edge",
                        "family_template": "single_leg_repair",
                        "hard_exit_minute": hard_exit,
                        "liquidity_gate": "tight",
                        "max_midpoint_distance_pct": 0.004,
                        "max_minutes_since_open": max_minute,
                        "max_range_pct": 0.006,
                        "max_signals_per_day": max_signals_per_day,
                        "max_trend_gap_pct": 0.0015,
                        "min_minutes_since_open": min_minute,
                        "min_option_hold_minutes": min_hold,
                        "min_range_pct": 0.0015,
                        "option_exit_mode": "premium_target_stop",
                        "option_exit_profile": exit_name,
                        "option_profit_target_pct": target_pct,
                        "option_stop_loss_pct": stop_pct,
                        "profit_target_multiple": 0.30,
                        "range_edge_pct": range_edge_pct,
                        "range_entry_side": "lower_band",
                        "stock_proxy_mode": "range_bound",
                        "stop_loss_multiple": 0.12,
                        "timeout_only_stock_proxy": True,
                        "timing_profile": timing_profile,
                        "wing_width_steps": 1,
                    }
                    rows.append(
                        _variant(
                            symbol=symbol,
                            regime="choppy",
                            direction="call",
                            family="single_leg_repair",
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

    rows = build_choppy_refine_rows(symbol=args.symbol.upper(), wave_id=args.wave_id)
    queue = build_queue(rows=rows, wave_id=args.wave_id)
    manifest = {
        "broker_facing": False,
        "execution_effect": "none",
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "source_diagnostic": "ticker365_qqq_choppy_reversion_20260505T0830Z candidate 1",
        "status": "ready_for_qqq_choppy_refine_backtest",
        "target_regimes": ["choppy"],
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": args.wave_id,
    }

    variants_path = output_dir / "qqq_choppy_refine_variants.jsonl"
    queue_path = output_dir / "qqq_choppy_refine_option_queue.json"
    manifest_path = output_dir / "qqq_choppy_refine_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

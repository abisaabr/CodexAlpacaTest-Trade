from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a research-only microstructure event replay grid."
    )
    parser.add_argument("--wave-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--underlyings", default="QQQ,SPY,IWM")
    parser.add_argument(
        "--profile",
        choices=("smoke", "liquid_exhaustive_v1", "rare_event_larger_move_v2"),
        default="liquid_exhaustive_v1",
    )
    parser.add_argument("--chunk-size", type=int, default=128)
    return parser.parse_args()


def _csv(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _grid(profile: str, underlyings: list[str]) -> list[dict[str, Any]]:
    if profile == "smoke":
        signal_modes = ["option_momentum", "stock_impulse_option_confirm"]
        lookbacks = [1.0, 3.0]
        option_thresholds = [0.02, 0.03]
        stock_thresholds = [0.0002]
        targets = [0.03, 0.05]
        stops = [0.015, 0.025]
        holds = [15.0, 30.0]
        max_relative_spreads = [0.02, 0.04]
        max_quote_ages = [0.5, 1.0]
        trail_pairs = [(0.0, 0.0), (0.04, 0.02)]
        execution_profiles = [
            {
                "execution_profile": "zero_latency_baseline",
                "entry_latency_seconds": 0.0,
                "exit_latency_seconds": 0.0,
                "entry_fill_wait_seconds": 0.0,
                "exit_fill_wait_seconds": 0.0,
                "max_entry_chase_pct": 0.05,
                "max_spread_cost_to_target": 0.75,
                "max_review_avg_spread_cost_to_target": 0.65,
            },
            {
                "execution_profile": "low_latency_strict",
                "entry_latency_seconds": 0.25,
                "exit_latency_seconds": 0.25,
                "entry_fill_wait_seconds": 0.75,
                "exit_fill_wait_seconds": 0.75,
                "max_entry_chase_pct": 0.03,
                "max_spread_cost_to_target": 0.50,
                "max_review_avg_spread_cost_to_target": 0.45,
            },
        ]
    elif profile == "liquid_exhaustive_v1":
        signal_modes = [
            "option_momentum",
            "stock_impulse_option_confirm",
            "stock_impulse_only",
            "spread_compression_momentum",
        ]
        lookbacks = [1.0, 2.0, 3.0, 5.0, 10.0]
        option_thresholds = [0.015, 0.025, 0.04, 0.06]
        stock_thresholds = [0.0001, 0.00025, 0.0005, 0.001]
        targets = [0.03, 0.05, 0.08, 0.12]
        stops = [0.015, 0.025, 0.04]
        holds = [15.0, 30.0, 60.0, 120.0]
        max_relative_spreads = [0.01, 0.02, 0.04]
        max_quote_ages = [0.25, 0.5, 1.0]
        trail_pairs = [(0.0, 0.0), (0.04, 0.02), (0.08, 0.03)]
        execution_profiles = [
            {
                "execution_profile": "zero_latency_baseline",
                "entry_latency_seconds": 0.0,
                "exit_latency_seconds": 0.0,
                "entry_fill_wait_seconds": 0.0,
                "exit_fill_wait_seconds": 0.0,
                "max_entry_chase_pct": 0.05,
                "max_spread_cost_to_target": 0.75,
                "max_review_avg_spread_cost_to_target": 0.65,
            },
            {
                "execution_profile": "low_latency_realistic",
                "entry_latency_seconds": 0.25,
                "exit_latency_seconds": 0.25,
                "entry_fill_wait_seconds": 0.75,
                "exit_fill_wait_seconds": 0.75,
                "max_entry_chase_pct": 0.03,
                "max_spread_cost_to_target": 0.50,
                "max_review_avg_spread_cost_to_target": 0.45,
            },
            {
                "execution_profile": "strict_latency_realistic",
                "entry_latency_seconds": 0.50,
                "exit_latency_seconds": 0.50,
                "entry_fill_wait_seconds": 1.0,
                "exit_fill_wait_seconds": 1.0,
                "max_entry_chase_pct": 0.02,
                "max_spread_cost_to_target": 0.35,
                "max_review_avg_spread_cost_to_target": 0.30,
            },
        ]
    else:
        signal_modes = [
            "stock_impulse_option_confirm",
            "spread_compression_momentum",
            "option_momentum",
        ]
        lookbacks = [5.0, 10.0, 20.0]
        option_thresholds = [0.06, 0.10, 0.16]
        stock_thresholds = [0.0005, 0.001, 0.0015]
        targets = [0.12, 0.18, 0.25, 0.35]
        stops = [0.035, 0.05, 0.075]
        holds = [60.0, 120.0, 240.0, 360.0]
        max_relative_spreads = [0.008, 0.012, 0.02]
        max_quote_ages = [0.15, 0.30, 0.50]
        trail_pairs = [(0.0, 0.0), (0.10, 0.04), (0.15, 0.06)]
        execution_profiles = [
            {
                "execution_profile": "strict_latency_realistic",
                "entry_latency_seconds": 0.50,
                "exit_latency_seconds": 0.50,
                "entry_fill_wait_seconds": 1.0,
                "exit_fill_wait_seconds": 1.0,
                "max_entry_chase_pct": 0.015,
                "max_spread_cost_to_target": 0.25,
                "max_review_avg_spread_cost_to_target": 0.20,
            },
            {
                "execution_profile": "low_latency_realistic",
                "entry_latency_seconds": 0.25,
                "exit_latency_seconds": 0.25,
                "entry_fill_wait_seconds": 0.75,
                "exit_fill_wait_seconds": 0.75,
                "max_entry_chase_pct": 0.025,
                "max_spread_cost_to_target": 0.35,
                "max_review_avg_spread_cost_to_target": 0.30,
            },
        ]

    rows: list[dict[str, Any]] = []
    grid_index = 1
    for signal_mode in signal_modes:
        for lookback in lookbacks:
            for option_threshold in option_thresholds:
                stock_threshold_values = stock_thresholds if "stock_impulse" in signal_mode else [0.0]
                for stock_threshold in stock_threshold_values:
                    for target in targets:
                        for stop in stops:
                            if stop >= target:
                                continue
                            for hold in holds:
                                for max_relative_spread in max_relative_spreads:
                                    for max_quote_age in max_quote_ages:
                                        for trail_activation, trail_retrace in trail_pairs:
                                            if trail_activation and trail_activation >= target:
                                                continue
                                            for execution_profile in execution_profiles:
                                                row = {
                                                    "grid_id": f"micro_{grid_index:05d}",
                                                    "wave_id": "",
                                                    "underlyings": underlyings,
                                                    "signal_mode": signal_mode,
                                                    "option_right": "both",
                                                    "lookback_seconds": lookback,
                                                    "option_momentum_threshold_pct": option_threshold,
                                                    "stock_impulse_threshold_pct": stock_threshold,
                                                    "target_pct": target,
                                                    "stop_pct": stop,
                                                    "max_hold_seconds": hold,
                                                    "max_entry_quote_age_seconds": max_quote_age,
                                                    "max_exit_quote_age_seconds": max(1.0, max_quote_age * 2.0),
                                                    "min_premium": 0.15,
                                                    "max_premium": 12.0,
                                                    "max_relative_spread": max_relative_spread,
                                                    "max_absolute_spread": 0.20,
                                                    "max_exit_relative_spread": max_relative_spread * 2.5,
                                                    "min_quote_size": 1.0,
                                                    "trail_activation_pct": trail_activation,
                                                    "trail_retrace_pct": trail_retrace,
                                                    "spread_compression_factor": 0.75,
                                                    "cooldown_seconds": 0.0,
                                                }
                                                row.update(execution_profile)
                                                rows.append(row)
                                                grid_index += 1
    for row in rows:
        row["wave_id"] = profile
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    underlyings = _csv(args.underlyings)
    rows = _grid(args.profile, underlyings)
    for row in rows:
        row["wave_id"] = args.wave_id
    grid_path = output_dir / "microstructure_research_grid.jsonl"
    _write_jsonl(grid_path, rows)

    chunks = []
    for start_index in range(1, len(rows) + 1, args.chunk_size):
        count = min(args.chunk_size, len(rows) - start_index + 1)
        chunks.append(
            {
                "chunk_id": f"c{start_index:05d}_{start_index + count - 1:05d}",
                "grid_start_index": start_index,
                "grid_count": count,
            }
        )
    manifest = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "wave_id": args.wave_id,
        "profile": args.profile,
        "underlyings": underlyings,
        "grid_count": len(rows),
        "chunk_size": args.chunk_size,
        "chunks": chunks,
        "research_only": True,
        "broker_facing": False,
        "paper_orders": False,
        "promotion_posture": "scouting_only_until_tick_quote_replay_packet_clears",
    }
    manifest_path = output_dir / "microstructure_research_grid_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"grid_jsonl={grid_path}")
    print(f"manifest_json={manifest_path}")
    print(f"grid_count={len(rows)}")
    print(f"chunk_count={len(chunks)}")


if __name__ == "__main__":
    main()

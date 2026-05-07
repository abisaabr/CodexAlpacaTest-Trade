from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_qqq_regime_research_inputs import _variant, build_queue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build research-only Greek-aware strategy inputs. These variants are "
            "intended for historical replay with the entry_delta_target_research_only "
            "selector, not direct broker-facing activation."
        )
    )
    parser.add_argument("--symbols", default="QQQ,SPY,IWM")
    parser.add_argument("--wave-id", default="qqq_spy_iwm_greek_research_20260507")
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _symbols(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def build_rows(*, symbols: list[str], wave_id: str) -> list[dict]:
    rows: list[dict] = []
    directional_profiles = [
        {
            "timing_profile": "greek_morning_momentum",
            "hard_exit_minute": 55,
            "min_minutes_since_open": 20,
            "max_minutes_since_open": 105,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 180,
            "max_signals_per_day": 1,
            "fast_window": 5,
            "slow_window": 21,
            "breakout_window": 21,
            "min_volume_ratio": 1.10,
            "min_trend_gap_pct": 0.0007,
        },
        {
            "timing_profile": "greek_midday_continuation",
            "hard_exit_minute": 85,
            "min_minutes_since_open": 90,
            "max_minutes_since_open": 240,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 210,
            "max_signals_per_day": 1,
            "fast_window": 8,
            "slow_window": 34,
            "breakout_window": 34,
            "min_volume_ratio": 1.20,
            "min_trend_gap_pct": 0.0012,
        },
    ]
    exit_profiles = [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "greek_tight_22_08",
            "option_profit_target_pct": 0.22,
            "option_stop_loss_pct": 0.08,
            "min_option_hold_minutes": 1,
            "profit_target_multiple": 0.24,
            "stop_loss_multiple": 0.09,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "greek_balanced_35_12",
            "option_profit_target_pct": 0.35,
            "option_stop_loss_pct": 0.12,
            "min_option_hold_minutes": 2,
            "profit_target_multiple": 0.38,
            "stop_loss_multiple": 0.13,
        },
    ]
    delta_profiles = [
        {"target_delta": 0.35, "min_abs_delta": 0.25, "max_abs_delta": 0.45, "label": "d35"},
        {"target_delta": 0.50, "min_abs_delta": 0.40, "max_abs_delta": 0.60, "label": "d50"},
        {"target_delta": 0.65, "min_abs_delta": 0.55, "max_abs_delta": 0.78, "label": "d65"},
    ]
    for symbol in symbols:
        for regime, direction in (("bull", "call"), ("bear", "put")):
            sign = -1.0 if direction == "put" else 1.0
            for timing_profile in directional_profiles:
                for exit_profile in exit_profiles:
                    for delta_profile in delta_profiles:
                        signed_delta = sign * float(delta_profile["target_delta"])
                        base_params = {
                            **timing_profile,
                            **exit_profile,
                            "dte_mode": "next_expiry",
                            "stock_proxy_mode": "breakout",
                            "liquidity_gate": "tight",
                            "target_delta": signed_delta,
                            "min_abs_delta": delta_profile["min_abs_delta"],
                            "max_abs_delta": delta_profile["max_abs_delta"],
                        }
                        rows.append(
                            _variant(
                                symbol=symbol,
                                regime=regime,
                                direction=direction,
                                family="single_leg_repair",
                                parameters={**base_params, "family_template": "single_leg_repair"},
                                priority=1,
                                wave_id=wave_id,
                            )
                        )
                        rows.append(
                            _variant(
                                symbol=symbol,
                                regime=regime,
                                direction=direction,
                                family=f"debit_{direction}_vertical",
                                parameters={
                                    **base_params,
                                    "family_template": f"debit_{direction}_vertical",
                                    "wing_width_steps": 1,
                                },
                                priority=2,
                                wave_id=wave_id,
                            )
                        )

        choppy_profiles = [
            {
                "timing_profile": "greek_short_theta_morning_range",
                "hard_exit_minute": 55,
                "min_minutes_since_open": 45,
                "max_minutes_since_open": 135,
                "max_range_pct": 0.006,
                "max_trend_gap_pct": 0.0016,
                "max_midpoint_distance_pct": 0.0025,
            },
            {
                "timing_profile": "greek_short_theta_midday_range",
                "hard_exit_minute": 90,
                "min_minutes_since_open": 120,
                "max_minutes_since_open": 285,
                "max_range_pct": 0.008,
                "max_trend_gap_pct": 0.0020,
                "max_midpoint_distance_pct": 0.0030,
            },
        ]
        for profile in choppy_profiles:
            for body_delta in (0.45, 0.55):
                for family in ("iron_butterfly", "iron_condor"):
                    params = {
                        **profile,
                        "dte_mode": "next_expiry",
                        "family_template": family,
                        "stock_proxy_mode": "range_bound",
                        "timeout_only_stock_proxy": True,
                        "entry_signal_mode": "daily_first",
                        "cooldown_bars": 240,
                        "max_signals_per_day": 1,
                        "min_range_pct": 0.0015,
                        "short_width_steps": 1,
                        "wing_width_steps": 1,
                        "target_delta": body_delta,
                        "min_abs_delta": 0.25,
                        "max_abs_delta": 0.70,
                        "option_exit_mode": "premium_target_stop",
                        "option_exit_profile": "short_theta_50_35",
                        "option_profit_target_pct": 0.50,
                        "option_stop_loss_pct": 0.35,
                        "min_option_hold_minutes": 5,
                    }
                    rows.append(
                        _variant(
                            symbol=symbol,
                            regime="choppy",
                            direction="call",
                            family=family,
                            parameters=params,
                            priority=3,
                            wave_id=wave_id,
                        )
                    )
    return rows


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    symbols = _symbols(args.symbols)
    rows = build_rows(symbols=symbols, wave_id=args.wave_id)
    queue = build_queue(rows=rows, wave_id=args.wave_id)

    variants_path = output_dir / "greek_strategy_variants.jsonl"
    queue_path = output_dir / "greek_strategy_option_queue.json"
    manifest_path = output_dir / "greek_strategy_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "broker_facing": False,
                "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
                "queue_json": str(queue_path),
                "queue_item_count": len(queue["queue_items"]),
                "research_only": True,
                "risk_policy_effect": "none",
                "status": "ready_for_greek_research_backtest",
                "target_symbols": symbols,
                "variants_jsonl": str(variants_path),
                "wave_id": args.wave_id,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    print(f"variants_jsonl={variants_path}")
    print(f"queue_json={queue_path}")
    print(f"queue_item_count={len(queue['queue_items'])}")


if __name__ == "__main__":
    main()

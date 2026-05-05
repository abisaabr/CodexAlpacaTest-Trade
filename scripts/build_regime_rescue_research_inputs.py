from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_iwm_regime_rescue_research_inputs import (  # noqa: E402
    _csv_ints,
    _csv_set,
    build_iwm_regime_rescue_rows,
)
from scripts.build_qqq_regime_research_inputs import _variant, build_queue  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a symbol-generic bear/choppy regime-rescue queue using the "
            "current liquidity-first strategy templates. This is research-only "
            "input generation; it does not start trading or modify live manifests."
        )
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", required=True)
    parser.add_argument("--target-regimes", default="bear,choppy")
    parser.add_argument("--output-prefix", default="")
    parser.add_argument(
        "--choppy-families",
        default="",
        help="Optional comma-separated choppy family filter for bounded rescue waves.",
    )
    parser.add_argument(
        "--choppy-signal-delay-bars",
        default="0",
        help="Comma-separated completed-stock-bar signal delays for choppy rows.",
    )
    parser.add_argument(
        "--choppy-profile-set",
        choices=(
            "rescue",
            "timewindow_refine",
            "timewindow_micro_exit",
            "timewindow_quality_filter",
        ),
        default="rescue",
    )
    parser.add_argument(
        "--bear-profile-set",
        choices=("rescue", "signal_window_refine"),
        default="rescue",
        help=(
            "Bear grid to build. 'rescue' preserves the broad legacy rescue grid. "
            "'signal_window_refine' tests stricter trend/volume/signal windows for "
            "high-fill bear candidates that are recent-profitable but full-period negative."
        ),
    )
    return parser.parse_args()


def _bear_signal_window_refine_rows(*, symbol: str, wave_id: str) -> list[dict]:
    rows: list[dict] = []
    timing_profiles = [
        {
            "timing_profile": "bear_confirmed_break_35_110_sw",
            "hard_exit_minute": 40,
            "min_minutes_since_open": 35,
            "max_minutes_since_open": 110,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 180,
            "max_signals_per_day": 1,
        },
        {
            "timing_profile": "bear_midday_follow_105_210_sw",
            "hard_exit_minute": 55,
            "min_minutes_since_open": 105,
            "max_minutes_since_open": 210,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 180,
            "max_signals_per_day": 1,
        },
        {
            "timing_profile": "bear_late_continuation_220_330_sw",
            "hard_exit_minute": 45,
            "min_minutes_since_open": 220,
            "max_minutes_since_open": 330,
            "entry_signal_mode": "daily_first",
            "cooldown_bars": 240,
            "max_signals_per_day": 1,
        },
    ]
    signal_windows = [
        {"fast_window": 6, "slow_window": 26, "breakout_window": 26, "min_volume_ratio": 1.15},
        {"fast_window": 8, "slow_window": 34, "breakout_window": 34, "min_volume_ratio": 1.25},
        {"fast_window": 10, "slow_window": 45, "breakout_window": 45, "min_volume_ratio": 1.35},
    ]
    exit_profiles = [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "bear_sw_tight_18_06",
            "option_profit_target_pct": 0.18,
            "option_stop_loss_pct": 0.06,
            "min_option_hold_minutes": 1,
            "profit_target_multiple": 0.20,
            "stop_loss_multiple": 0.07,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "bear_sw_balanced_26_09",
            "option_profit_target_pct": 0.26,
            "option_stop_loss_pct": 0.09,
            "min_option_hold_minutes": 2,
            "profit_target_multiple": 0.28,
            "stop_loss_multiple": 0.10,
        },
    ]
    for timing_profile in timing_profiles:
        for signal_window in signal_windows:
            for min_trend_gap_pct in (0.0009, 0.0014, 0.0020):
                for exit_profile in exit_profiles:
                    parameters = {
                        **timing_profile,
                        **signal_window,
                        **exit_profile,
                        "dte_mode": "next_expiry",
                        "family_template": "single_leg_repair",
                        "liquidity_gate": "tight",
                        "min_trend_gap_pct": min_trend_gap_pct,
                        "short_width_steps": 1,
                        "stock_proxy_mode": "breakout",
                        "wing_width_steps": 1,
                    }
                    rows.append(
                        _variant(
                            symbol=symbol,
                            regime="bear",
                            direction="put",
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

    symbol = str(args.symbol).upper()
    symbol_slug = symbol.lower()
    output_prefix = args.output_prefix.strip() or f"{symbol_slug}_regime_rescue"
    target_regimes = _csv_set(args.target_regimes)
    choppy_families = _csv_set(args.choppy_families)

    if args.bear_profile_set == "signal_window_refine":
        rows = []
        if "bear" in target_regimes:
            rows.extend(_bear_signal_window_refine_rows(symbol=symbol, wave_id=args.wave_id))
        remaining_regimes = set(target_regimes) - {"bear"}
        if remaining_regimes:
            rows.extend(
                build_iwm_regime_rescue_rows(
                    symbol=symbol,
                    wave_id=args.wave_id,
                    target_regimes=remaining_regimes,
                    choppy_signal_delay_bars=_csv_ints(args.choppy_signal_delay_bars),
                    choppy_families=choppy_families or None,
                    choppy_profile_set=args.choppy_profile_set,
                )
            )
    else:
        rows = build_iwm_regime_rescue_rows(
            symbol=symbol,
            wave_id=args.wave_id,
            target_regimes=target_regimes,
            choppy_signal_delay_bars=_csv_ints(args.choppy_signal_delay_bars),
            choppy_families=choppy_families or None,
            choppy_profile_set=args.choppy_profile_set,
        )
    queue = build_queue(rows=rows, wave_id=args.wave_id)
    manifest = {
        "broker_facing": False,
        "bear_profile_set": args.bear_profile_set,
        "builder": "build_regime_rescue_research_inputs.py",
        "choppy_families": sorted(choppy_families),
        "choppy_profile_set": args.choppy_profile_set,
        "execution_effect": "none",
        "live_manifest_effect": "none",
        "output_prefix": output_prefix,
        "promotion_allowed": False,
        "risk_policy_effect": "none",
        "status": "ready_for_symbol_regime_rescue_backtest",
        "target_regimes": sorted(target_regimes),
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": args.wave_id,
    }

    variants_path = output_dir / f"{output_prefix}_variants.jsonl"
    queue_path = output_dir / f"{output_prefix}_option_queue.json"
    manifest_path = output_dir / f"{output_prefix}_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

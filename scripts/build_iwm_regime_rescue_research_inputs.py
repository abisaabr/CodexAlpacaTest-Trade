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
            "Build a targeted IWM regime-rescue queue focused on missing governed "
            "promotion-review regimes: bear economics and choppy fill/economics."
        )
    )
    parser.add_argument("--symbol", default="IWM")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="ticker365_iwm_regime_rescue_20260505")
    parser.add_argument("--target-regimes", default="bear,choppy")
    parser.add_argument(
        "--choppy-families",
        default="",
        help="Optional comma-separated choppy family filter for bounded rescue waves.",
    )
    parser.add_argument(
        "--choppy-signal-delay-bars",
        default="0",
        help=(
            "Comma-separated completed-stock-bar signal delays for choppy rows. "
            "Default 0 preserves the historical immediate-entry search."
        ),
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
        help=(
            "Choppy grid to build. 'rescue' preserves the broad missing-regime search; "
            "'timewindow_refine' focuses on the high-fill IWM lower-band call sleeve "
            "identified from prior 365d trade economics; 'timewindow_micro_exit' keeps "
            "that sleeve but tests tighter option exits for full-period economics; "
            "'timewindow_quality_filter' adds stricter choppy-state filters."
        ),
    )
    return parser.parse_args()


def _csv_set(value: str) -> set[str]:
    return {item.strip().lower() for item in str(value or "").split(",") if item.strip()}


def _csv_ints(value: str) -> list[int]:
    items = []
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        parsed = int(item)
        if parsed < 0:
            raise ValueError("signal delay bars must be non-negative")
        items.append(parsed)
    return items or [0]


def _bear_timing_profiles() -> list[dict[str, Any]]:
    return [
        {
            "timing_profile": "iwm_bear_open_drive_15_80",
            "hard_exit_minute": 35,
            "min_minutes_since_open": 15,
            "max_minutes_since_open": 80,
            "min_trend_gap_pct": 0.00025,
            "entry_signal_mode": "daily_first",
            "cooldown_bars": 120,
            "max_signals_per_day": 1,
        },
        {
            "timing_profile": "iwm_bear_confirmed_break_30_120",
            "hard_exit_minute": 55,
            "min_minutes_since_open": 30,
            "max_minutes_since_open": 120,
            "min_trend_gap_pct": 0.00045,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 90,
            "max_signals_per_day": 1,
        },
        {
            "timing_profile": "iwm_bear_midday_follow_95_230",
            "hard_exit_minute": 85,
            "min_minutes_since_open": 95,
            "max_minutes_since_open": 230,
            "min_trend_gap_pct": 0.00060,
            "entry_signal_mode": "rising_edge",
            "cooldown_bars": 90,
            "max_signals_per_day": 1,
        },
        {
            "timing_profile": "iwm_bear_late_continuation_205_335",
            "hard_exit_minute": 65,
            "min_minutes_since_open": 205,
            "max_minutes_since_open": 335,
            "min_trend_gap_pct": 0.00040,
            "entry_signal_mode": "daily_first",
            "cooldown_bars": 90,
            "max_signals_per_day": 1,
        },
    ]


def _bear_exit_profiles() -> list[dict[str, Any]]:
    return [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_bear_micro_scalp",
            "option_profit_target_pct": 0.14,
            "option_stop_loss_pct": 0.06,
            "option_stop_loss_credit_multiple": 0.75,
            "option_stop_loss_risk_pct": 0.18,
            "min_option_hold_minutes": 1,
            "profit_target_multiple": 0.18,
            "stop_loss_multiple": 0.07,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_bear_tight_scalp",
            "option_profit_target_pct": 0.20,
            "option_stop_loss_pct": 0.09,
            "option_stop_loss_credit_multiple": 0.90,
            "option_stop_loss_risk_pct": 0.24,
            "min_option_hold_minutes": 2,
            "profit_target_multiple": 0.24,
            "stop_loss_multiple": 0.10,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_bear_asymmetric_runner",
            "option_profit_target_pct": 0.34,
            "option_stop_loss_pct": 0.13,
            "option_stop_loss_credit_multiple": 1.10,
            "option_stop_loss_risk_pct": 0.30,
            "min_option_hold_minutes": 3,
            "profit_target_multiple": 0.36,
            "stop_loss_multiple": 0.14,
        },
    ]


def _choppy_timing_profiles() -> list[dict[str, Any]]:
    return [
        {
            "timing_profile": "iwm_choppy_lower_band_75_180",
            "hard_exit_minute": 45,
            "min_minutes_since_open": 75,
            "max_minutes_since_open": 180,
            "max_range_pct": 0.0085,
            "max_trend_gap_pct": 0.0020,
            "max_midpoint_distance_pct": 0.0045,
        },
        {
            "timing_profile": "iwm_choppy_midday_bands_120_270",
            "hard_exit_minute": 55,
            "min_minutes_since_open": 120,
            "max_minutes_since_open": 270,
            "max_range_pct": 0.0100,
            "max_trend_gap_pct": 0.0024,
            "max_midpoint_distance_pct": 0.0050,
        },
        {
            "timing_profile": "iwm_choppy_upper_band_late_190_340",
            "hard_exit_minute": 50,
            "min_minutes_since_open": 190,
            "max_minutes_since_open": 340,
            "max_range_pct": 0.0125,
            "max_trend_gap_pct": 0.0030,
            "max_midpoint_distance_pct": 0.0055,
        },
    ]


def _choppy_exit_profiles() -> list[dict[str, Any]]:
    return [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_choppy_fast_reversion",
            "option_profit_target_pct": 0.18,
            "option_stop_loss_pct": 0.08,
            "min_option_hold_minutes": 1,
            "profit_target_multiple": 0.18,
            "stop_loss_multiple": 0.07,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_choppy_balanced_reversion",
            "option_profit_target_pct": 0.28,
            "option_stop_loss_pct": 0.12,
            "min_option_hold_minutes": 2,
            "profit_target_multiple": 0.24,
            "stop_loss_multiple": 0.10,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "iwm_choppy_wide_reversion",
            "option_profit_target_pct": 0.38,
            "option_stop_loss_pct": 0.16,
            "min_option_hold_minutes": 3,
            "profit_target_multiple": 0.30,
            "stop_loss_multiple": 0.13,
        },
    ]


def _bear_rows(*, symbol: str, wave_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    family_specs = [
        ("put", "single_leg_repair", 1),
        ("put", "debit_put_vertical", 1),
        ("put", "bear_call_credit_spread", 1),
        ("put", "credit_call_vertical", 2),
    ]
    for timing_profile in _bear_timing_profiles():
        for exit_profile in _bear_exit_profiles():
            for direction, family, wing_width in family_specs:
                parameters = {
                    **timing_profile,
                    **exit_profile,
                    "dte_mode": "next_expiry",
                    "family_template": family,
                    "liquidity_gate": "tight",
                    "short_width_steps": 1,
                    "stock_proxy_mode": "breakout",
                    "wing_width_steps": wing_width,
                }
                rows.append(
                    _variant(
                        symbol=symbol,
                        regime="bear",
                        direction=direction,
                        family=family,
                        parameters=parameters,
                        priority=1,
                        wave_id=wave_id,
                    )
                )
    return rows


def _choppy_rows(
    *,
    symbol: str,
    wave_id: str,
    signal_delay_bars: list[int] | None = None,
    family_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    delays = signal_delay_bars or [0]
    direction_specs = [
        ("call", "lower_band", "single_leg_repair", 1),
        ("call", "lower_band", "debit_call_vertical", 1),
        ("put", "upper_band", "single_leg_repair", 1),
        ("put", "upper_band", "debit_put_vertical", 1),
        ("call", "center", "iron_butterfly", 1),
        ("call", "center", "iron_condor", 1),
    ]
    for timing_profile in _choppy_timing_profiles():
        for exit_profile in _choppy_exit_profiles():
            for direction, range_entry_side, family, wing_width in direction_specs:
                if family_filter and family not in family_filter:
                    continue
                for range_edge_pct in (0.0004, 0.0009):
                    for delay_bars in delays:
                        parameters = {
                            **timing_profile,
                            **exit_profile,
                            "cooldown_bars": 75,
                            "dte_mode": "next_expiry",
                            "entry_signal_mode": "rising_edge",
                            "family_template": family,
                            "liquidity_gate": "tight",
                            "max_signals_per_day": 1,
                            "min_range_pct": 0.0012,
                            "range_edge_pct": range_edge_pct,
                            "range_entry_side": range_entry_side,
                            "short_width_steps": 1,
                            "stock_proxy_mode": "range_bound",
                            "timeout_only_stock_proxy": True,
                            "wing_width_steps": wing_width,
                        }
                        if delay_bars:
                            parameters["signal_delay_bars"] = delay_bars
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


def _choppy_timewindow_refine_rows(
    *,
    symbol: str,
    wave_id: str,
    family_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    family = "single_leg_repair"
    if family_filter and family not in family_filter:
        return rows

    time_windows = [
        ("iwm_choppy_lower_band_90_135_refine", 90, 135, 45),
        ("iwm_choppy_lower_band_105_135_refine", 105, 135, 45),
        ("iwm_choppy_lower_band_105_150_refine", 105, 150, 45),
        ("iwm_choppy_lower_band_120_150_refine", 120, 150, 45),
        ("iwm_choppy_lower_band_120_165_refine", 120, 165, 45),
        ("iwm_choppy_lower_band_135_165_refine", 135, 165, 45),
    ]
    exit_profiles = [
        ("target30_stop12", 0.30, 0.12),
        ("target30_stop14", 0.30, 0.14),
        ("target35_stop14", 0.35, 0.14),
        ("target35_stop16", 0.35, 0.16),
    ]
    for timing_profile, min_minute, max_minute, hard_exit in time_windows:
        for exit_name, target_pct, stop_pct in exit_profiles:
            for min_hold in (2, 3):
                for range_edge_pct in (0.0009, 0.0010, 0.0011):
                    parameters = {
                        "cooldown_bars": 60,
                        "dte_mode": "next_expiry",
                        "entry_signal_mode": "rising_edge",
                        "family_template": family,
                        "hard_exit_minute": hard_exit,
                        "liquidity_gate": "tight",
                        "max_midpoint_distance_pct": 0.004,
                        "max_minutes_since_open": max_minute,
                        "max_range_pct": 0.006,
                        "max_signals_per_day": 1,
                        "max_trend_gap_pct": 0.0015,
                        "min_minutes_since_open": min_minute,
                        "min_option_hold_minutes": min_hold,
                        "min_range_pct": 0.0015,
                        "option_exit_mode": "premium_target_stop",
                        "option_exit_profile": exit_name,
                        "option_profit_target_pct": target_pct,
                        "option_stop_loss_pct": stop_pct,
                        "profit_target_multiple": target_pct,
                        "range_edge_pct": range_edge_pct,
                        "range_entry_side": "lower_band",
                        "short_width_steps": 1,
                        "stock_proxy_mode": "range_bound",
                        "stop_loss_multiple": stop_pct,
                        "timeout_only_stock_proxy": True,
                        "wing_width_steps": 1,
                    }
                    rows.append(
                        _variant(
                            symbol=symbol,
                            regime="choppy",
                            direction="call",
                            family=family,
                            parameters=parameters,
                            priority=1,
                            wave_id=wave_id,
                        )
                    )
    return rows


def _choppy_timewindow_micro_exit_rows(
    *,
    symbol: str,
    wave_id: str,
    family_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    family = "single_leg_repair"
    if family_filter and family not in family_filter:
        return rows

    time_windows = [
        ("iwm_choppy_lower_band_90_135_micro", 90, 135, 35),
        ("iwm_choppy_lower_band_105_135_micro", 105, 135, 35),
        ("iwm_choppy_lower_band_105_150_micro", 105, 150, 35),
        ("iwm_choppy_lower_band_120_150_micro", 120, 150, 35),
        ("iwm_choppy_lower_band_120_165_micro", 120, 165, 35),
        ("iwm_choppy_lower_band_135_165_micro", 135, 165, 35),
    ]
    exit_profiles = [
        ("micro12_stop05_hold1", 0.12, 0.05, 1),
        ("micro16_stop06_hold1", 0.16, 0.06, 1),
        ("micro20_stop07_hold1", 0.20, 0.07, 1),
        ("micro20_stop08_hold2", 0.20, 0.08, 2),
        ("micro25_stop09_hold2", 0.25, 0.09, 2),
        ("micro25_stop10_hold2", 0.25, 0.10, 2),
    ]
    for timing_profile, min_minute, max_minute, hard_exit in time_windows:
        for exit_name, target_pct, stop_pct, min_hold in exit_profiles:
            for range_edge_pct in (0.0009, 0.0010, 0.0011):
                parameters = {
                    "cooldown_bars": 60,
                    "dte_mode": "next_expiry",
                    "entry_signal_mode": "rising_edge",
                    "family_template": family,
                    "hard_exit_minute": hard_exit,
                    "liquidity_gate": "tight",
                    "max_midpoint_distance_pct": 0.004,
                    "max_minutes_since_open": max_minute,
                    "max_range_pct": 0.006,
                    "max_signals_per_day": 1,
                    "max_trend_gap_pct": 0.0015,
                    "min_minutes_since_open": min_minute,
                    "min_option_hold_minutes": min_hold,
                    "min_range_pct": 0.0015,
                    "option_exit_mode": "premium_target_stop",
                    "option_exit_profile": exit_name,
                    "option_profit_target_pct": target_pct,
                    "option_stop_loss_pct": stop_pct,
                    "profit_target_multiple": target_pct,
                    "range_edge_pct": range_edge_pct,
                    "range_entry_side": "lower_band",
                    "short_width_steps": 1,
                    "stock_proxy_mode": "range_bound",
                    "stop_loss_multiple": stop_pct,
                    "timeout_only_stock_proxy": True,
                    "wing_width_steps": 1,
                }
                rows.append(
                    _variant(
                        symbol=symbol,
                        regime="choppy",
                        direction="call",
                        family=family,
                        parameters=parameters,
                        priority=1,
                        wave_id=wave_id,
                    )
                )
    return rows


def _choppy_timewindow_quality_filter_rows(
    *,
    symbol: str,
    wave_id: str,
    family_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    family_specs = (
        ("call", "single_leg_repair"),
        ("call", "debit_call_vertical"),
        ("call", "broken_wing_call_butterfly"),
    )
    if family_filter:
        family_specs = tuple(
            (direction, family) for direction, family in family_specs if family in family_filter
        )
        if not family_specs:
            return rows

    time_windows = [
        ("iwm_choppy_quality_105_150", 105, 150, 35),
        ("iwm_choppy_quality_105_165", 105, 165, 35),
        ("iwm_choppy_quality_120_150", 120, 150, 35),
        ("iwm_choppy_quality_120_165", 120, 165, 35),
    ]
    exit_profiles = [
        ("micro20_stop07_hold1", 0.20, 0.07, 1),
        ("micro20_stop08_hold2", 0.20, 0.08, 2),
        ("micro25_stop09_hold2", 0.25, 0.09, 2),
        ("target35_stop14_hold2", 0.35, 0.14, 2),
    ]
    quality_filters = [
        {
            "quality_profile": "narrow_low_trend",
            "max_range_pct": 0.0050,
            "max_trend_gap_pct": 0.0010,
            "max_midpoint_distance_pct": 0.0035,
            "min_range_pct": 0.0015,
        },
        {
            "quality_profile": "ultra_narrow_low_trend",
            "max_range_pct": 0.0040,
            "max_trend_gap_pct": 0.0008,
            "max_midpoint_distance_pct": 0.0030,
            "min_range_pct": 0.0015,
        },
        {
            "quality_profile": "defined_range_low_trend",
            "max_range_pct": 0.0060,
            "max_trend_gap_pct": 0.0010,
            "max_midpoint_distance_pct": 0.0035,
            "min_range_pct": 0.0020,
        },
    ]
    for timing_profile, min_minute, max_minute, hard_exit in time_windows:
        for exit_name, target_pct, stop_pct, min_hold in exit_profiles:
            for quality_filter in quality_filters:
                for range_edge_pct in (0.0010, 0.0012):
                    for direction, family in family_specs:
                        parameters = {
                            **quality_filter,
                            "cooldown_bars": 60,
                            "dte_mode": "next_expiry",
                            "entry_signal_mode": "rising_edge",
                            "family_template": family,
                            "hard_exit_minute": hard_exit,
                            "liquidity_gate": "tight",
                            "max_minutes_since_open": max_minute,
                            "max_signals_per_day": 1,
                            "min_minutes_since_open": min_minute,
                            "min_option_hold_minutes": min_hold,
                            "option_exit_mode": "premium_target_stop",
                            "option_exit_profile": exit_name,
                            "option_profit_target_pct": target_pct,
                            "option_stop_loss_pct": stop_pct,
                            "profit_target_multiple": target_pct,
                            "range_edge_pct": range_edge_pct,
                            "range_entry_side": "lower_band",
                            "short_width_steps": 1,
                            "stock_proxy_mode": "range_bound",
                            "stop_loss_multiple": stop_pct,
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


def build_iwm_regime_rescue_rows(
    *,
    symbol: str,
    wave_id: str,
    target_regimes: set[str] | None = None,
    choppy_signal_delay_bars: list[int] | None = None,
    choppy_families: set[str] | None = None,
    choppy_profile_set: str = "rescue",
) -> list[dict[str, Any]]:
    regimes = target_regimes or {"bear", "choppy"}
    rows: list[dict[str, Any]] = []
    if "bear" in regimes:
        rows.extend(_bear_rows(symbol=symbol, wave_id=wave_id))
    if "choppy" in regimes:
        if choppy_profile_set == "timewindow_refine":
            rows.extend(
                _choppy_timewindow_refine_rows(
                    symbol=symbol,
                    wave_id=wave_id,
                    family_filter=choppy_families,
                )
            )
        elif choppy_profile_set == "timewindow_micro_exit":
            rows.extend(
                _choppy_timewindow_micro_exit_rows(
                    symbol=symbol,
                    wave_id=wave_id,
                    family_filter=choppy_families,
                )
            )
        elif choppy_profile_set == "timewindow_quality_filter":
            rows.extend(
                _choppy_timewindow_quality_filter_rows(
                    symbol=symbol,
                    wave_id=wave_id,
                    family_filter=choppy_families,
                )
            )
        else:
            rows.extend(
                _choppy_rows(
                    symbol=symbol,
                    wave_id=wave_id,
                    signal_delay_bars=choppy_signal_delay_bars,
                    family_filter=choppy_families,
                )
            )
    return rows


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    symbol = str(args.symbol).upper()
    target_regimes = _csv_set(args.target_regimes)
    choppy_families = _csv_set(args.choppy_families)
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
        "execution_effect": "none",
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "status": "ready_for_iwm_regime_rescue_backtest",
        "choppy_profile_set": args.choppy_profile_set,
        "target_regimes": sorted(target_regimes),
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": args.wave_id,
    }

    variants_path = output_dir / "iwm_regime_rescue_variants.jsonl"
    queue_path = output_dir / "iwm_regime_rescue_option_queue.json"
    manifest_path = output_dir / "iwm_regime_rescue_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

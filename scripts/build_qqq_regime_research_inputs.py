from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a compact QQQ-first research queue with explicit bull, bear, "
            "and choppy parameter grids for governed paper-readiness replay."
        )
    )
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="ticker365_qqq_regime_redesign_20260505")
    return parser.parse_args()


def _slug(*parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:14]


def _variant(
    *,
    symbol: str,
    regime: str,
    direction: str,
    family: str,
    parameters: dict[str, Any],
    priority: int,
    wave_id: str,
) -> dict[str, Any]:
    family_slug = family.replace("_", "-")
    variant_id = (
        f"portfolio12h__{symbol.lower()}__{regime}__{direction}__{family}__"
        f"{_slug(symbol, regime, direction, family, json.dumps(parameters, sort_keys=True))}"
    )
    return {
        "broker_facing": False,
        "execution_effect": "none",
        "generated_for_wave": wave_id,
        "live_manifest_effect": "none",
        "parameters": parameters,
        "priority": priority,
        "queue_id": "RQ-QQQ-regime-redesign",
        "risk_policy_effect": "none",
        "source_strategy_id": f"{symbol.lower()}__{regime}__{direction}__{family}",
        "source_template_symbol": "QQQ",
        "source_template_variant_id": f"qqq_regime_redesign__{regime}__{family_slug}",
        "state": "research_only",
        "symbol": symbol,
        "variant_id": variant_id,
        "variant_type": "single_leg_repair"
        if family == "single_leg_repair"
        else "defined_risk_family_expansion",
    }


def build_rows(*, symbol: str, wave_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    directional_profiles = [
        {
            "timing_profile": "scalp",
            "hard_exit_minute": 60,
            "stop_loss_multiple": 0.10,
            "profit_target_multiple": 0.22,
            "liquidity_gate": "tight",
        },
        {
            "timing_profile": "fast",
            "hard_exit_minute": 120,
            "stop_loss_multiple": 0.14,
            "profit_target_multiple": 0.32,
            "liquidity_gate": "tight",
        },
        {
            "timing_profile": "base",
            "hard_exit_minute": 180,
            "stop_loss_multiple": 0.18,
            "profit_target_multiple": 0.45,
            "liquidity_gate": "tight",
        },
        {
            "timing_profile": "patient",
            "hard_exit_minute": 240,
            "stop_loss_multiple": 0.22,
            "profit_target_multiple": 0.55,
            "liquidity_gate": "baseline",
        },
        {
            "timing_profile": "slow",
            "hard_exit_minute": 300,
            "stop_loss_multiple": 0.26,
            "profit_target_multiple": 0.70,
            "liquidity_gate": "baseline",
        },
    ]
    for regime, direction in (("bull", "call"), ("bear", "put")):
        for dte_mode in ("next_expiry",):
            for profile in directional_profiles:
                params = {
                    **profile,
                    "dte_mode": dte_mode,
                    "family_template": "single_leg_repair",
                    "stock_proxy_mode": "breakout",
                }
                rows.append(
                    _variant(
                        symbol=symbol,
                        regime=regime,
                        direction=direction,
                        family="single_leg_repair",
                        parameters=params,
                        priority=1,
                        wave_id=wave_id,
                    )
                )

    choppy_profiles = [
        {
            "timing_profile": "scalp",
            "hard_exit_minute": 45,
            "max_range_pct": 0.010,
            "max_trend_gap_pct": 0.0025,
            "max_midpoint_distance_pct": 0.0035,
        },
        {
            "timing_profile": "fast",
            "hard_exit_minute": 75,
            "max_range_pct": 0.014,
            "max_trend_gap_pct": 0.0035,
            "max_midpoint_distance_pct": 0.0050,
        },
        {
            "timing_profile": "base",
            "hard_exit_minute": 120,
            "max_range_pct": 0.018,
            "max_trend_gap_pct": 0.0045,
            "max_midpoint_distance_pct": 0.0065,
        },
    ]
    for family in ("iron_condor", "iron_butterfly", "premium_defense_spread"):
        for profile in choppy_profiles:
            for wing_width in (1, 2):
                params = {
                    **profile,
                    "dte_mode": "next_expiry",
                    "family_template": family,
                    "min_range_pct": 0.0015,
                    "short_width_steps": 1,
                    "stock_proxy_mode": "range_bound",
                    "timeout_only_stock_proxy": True,
                    "wing_width_steps": wing_width,
                }
                rows.append(
                    _variant(
                        symbol=symbol,
                        regime="choppy",
                        direction="call",
                        family=family,
                        parameters=params,
                        priority=1,
                        wave_id=wave_id,
                    )
                )
    return rows


def build_queue(*, rows: list[dict[str, Any]], wave_id: str) -> dict[str, Any]:
    queue_items = []
    for rank, variant in enumerate(rows, start=1):
        parameters = variant.get("parameters", {})
        parts = str(variant["source_strategy_id"]).split("__")
        queue_items.append(
            {
                "blockers": [],
                "broker_facing": False,
                "candidate_variant_id": variant["variant_id"],
                "directional_option_type": parts[2],
                "family": parts[3],
                "intended_regime": parts[1],
                "live_manifest_effect": "none",
                "parameter_set": json.dumps(
                    parameters, sort_keys=True, separators=(",", ":")
                ),
                "promotion_allowed": False,
                "rank": rank,
                "risk_policy_effect": "none",
                "source_strategy_id": variant["source_strategy_id"],
                "source_template_variant_id": variant["source_template_variant_id"],
                "state": "research_follow_up_only",
                "symbol": variant["symbol"],
            }
        )
    return {
        "blocker_counts": {},
        "broker_facing": False,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "live_manifest_effect": "none",
        "promotion_allowed": False,
        "queue_item_count": len(queue_items),
        "queue_items": queue_items,
        "risk_policy_effect": "none",
        "status": "ready_for_qqq_regime_redesign_backtest",
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": wave_id,
    }


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    symbol = str(args.symbol).upper()
    rows = build_rows(symbol=symbol, wave_id=args.wave_id)
    queue = build_queue(rows=rows, wave_id=args.wave_id)

    variants_path = output_dir / "qqq_regime_redesign_variants.jsonl"
    queue_path = output_dir / "qqq_regime_redesign_option_queue.json"
    manifest_path = output_dir / "qqq_regime_redesign_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "queue_json": str(queue_path),
                "queue_item_count": len(queue["queue_items"]),
                "status": "ready",
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

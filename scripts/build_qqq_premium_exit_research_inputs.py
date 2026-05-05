from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_qqq_regime_research_inputs import _variant, build_queue, build_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build QQQ bear/choppy option-native exit research inputs."
    )
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="ticker365_qqq_premium_exit_20260505")
    return parser.parse_args()


def _source_parts(row: dict[str, Any]) -> tuple[str, str, str]:
    parts = str(row["source_strategy_id"]).split("__", 3)
    if len(parts) != 4:
        raise ValueError(f"Unexpected source_strategy_id={row['source_strategy_id']}")
    return parts[1], parts[2], parts[3]


def _exit_profiles(family: str) -> list[dict[str, Any]]:
    if "credit" in family or family in {"iron_condor", "iron_butterfly", "premium_defense_spread"}:
        return [
            {
                "option_exit_mode": "premium_target_stop",
                "option_exit_profile": "credit_fast_capture_tight_stop",
                "option_profit_target_pct": 0.25,
                "option_stop_loss_credit_multiple": 0.65,
                "option_stop_loss_risk_pct": 0.20,
                "min_option_hold_minutes": 5,
            },
            {
                "option_exit_mode": "premium_target_stop",
                "option_exit_profile": "credit_balanced_capture",
                "option_profit_target_pct": 0.40,
                "option_stop_loss_credit_multiple": 0.90,
                "option_stop_loss_risk_pct": 0.30,
                "min_option_hold_minutes": 10,
            },
        ]
    return [
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "debit_fast_takeprofit",
            "option_profit_target_pct": 0.35,
            "option_stop_loss_pct": 0.18,
            "min_option_hold_minutes": 3,
        },
        {
            "option_exit_mode": "premium_target_stop",
            "option_exit_profile": "debit_balanced_runner",
            "option_profit_target_pct": 0.55,
            "option_stop_loss_pct": 0.26,
            "min_option_hold_minutes": 5,
        },
    ]


def build_premium_exit_rows(*, symbol: str, wave_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in build_rows(symbol=symbol, wave_id=wave_id):
        regime, direction, family = _source_parts(row)
        if regime not in {"bear", "choppy"}:
            continue
        base_parameters = dict(row["parameters"])
        for profile in _exit_profiles(family):
            parameters = {**base_parameters, **profile}
            rows.append(
                _variant(
                    symbol=symbol,
                    regime=regime,
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

    rows = build_premium_exit_rows(symbol=args.symbol.upper(), wave_id=args.wave_id)
    queue = build_queue(rows=rows, wave_id=args.wave_id)
    manifest = {
        "broker_facing": False,
        "execution_effect": "none",
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "status": "ready_for_qqq_premium_exit_backtest",
        "target_regimes": ["bear", "choppy"],
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": args.wave_id,
    }

    variants_path = output_dir / "qqq_premium_exit_variants.jsonl"
    queue_path = output_dir / "qqq_premium_exit_option_queue.json"
    manifest_path = output_dir / "qqq_premium_exit_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

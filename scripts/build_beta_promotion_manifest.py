from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.multi_ticker_portfolio.config import _load_strategy_manifest_payload


def _default_live_manifest_path() -> Path:
    return REPO_ROOT / "config" / "strategy_manifests" / "multi_ticker_portfolio_live.yaml"


def _default_output_path() -> Path:
    return REPO_ROOT / "config" / "promotion_manifests" / "multi_ticker_portfolio_beta_promotions.yaml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a GitHub-tracked beta promotion retest manifest from the checked-in live manifest "
            "and the control-plane strategy family registry."
        )
    )
    parser.add_argument(
        "--live-manifest",
        type=Path,
        default=_default_live_manifest_path(),
        help="Checked-in live strategy manifest path.",
    )
    parser.add_argument(
        "--strategy-family-registry",
        type=Path,
        required=True,
        help="Path to strategy_family_registry.json from the control plane.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_default_output_path(),
        help="YAML output path for the beta promotion retest manifest.",
    )
    return parser


def _load_strategy_family_registry(registry_path: Path) -> dict[str, Any]:
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Strategy family registry must contain a top-level JSON object.")
    families = payload.get("families")
    if not isinstance(families, list):
        raise ValueError("Strategy family registry must contain a top-level 'families' list.")
    return payload


def _sorted_unique_strings(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return sorted({str(value).upper() for value in values if str(value).strip()})


def _sorted_unique_base_strategies(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return sorted({str(value) for value in values if str(value).strip()})


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _build_promoted_family_rows(registry_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family_payload in registry_payload.get("families", []):
        if not isinstance(family_payload, dict):
            continue
        promoted_ticker_count = _int_value(family_payload.get("promoted_ticker_count"))
        if promoted_ticker_count <= 0:
            continue
        promoted_tickers = _sorted_unique_strings(family_payload.get("promoted_tickers"))
        live_manifest_tickers = _sorted_unique_strings(family_payload.get("live_manifest_tickers"))
        retest_gap_tickers = sorted(set(promoted_tickers) - set(live_manifest_tickers))
        base_strategies = _sorted_unique_base_strategies(family_payload.get("base_strategies"))
        promoted_base_strategy_count = _int_value(family_payload.get("promoted_base_strategy_count"))
        exact_promoted_base_strategies_available = promoted_base_strategy_count == len(base_strategies)
        rows.append(
            {
                "family": str(family_payload.get("family", "")),
                "priority": str(family_payload.get("priority", "")),
                "steward_action": str(family_payload.get("steward_action", "")),
                "structure_bucket": str(family_payload.get("structure_bucket", "")),
                "directional_bias": str(family_payload.get("directional_bias", "")),
                "strategy_sets": _sorted_unique_base_strategies(family_payload.get("strategy_sets")),
                "base_strategies": base_strategies,
                "selected_base_strategy_count": _int_value(family_payload.get("selected_base_strategy_count")),
                "promoted_base_strategy_count": promoted_base_strategy_count,
                "exact_promoted_base_strategies_available": exact_promoted_base_strategies_available,
                "selected_tickers": _sorted_unique_strings(family_payload.get("selected_tickers")),
                "promoted_tickers": promoted_tickers,
                "live_manifest_tickers": live_manifest_tickers,
                "retest_gap_tickers": retest_gap_tickers,
                "live_manifest_strategy_count": _int_value(family_payload.get("live_manifest_strategy_count")),
                "note": str(family_payload.get("note", "")),
                "retest_scope_note": (
                    "Retest the exact live-manifest strategies for live tickers and reconstruct "
                    "cleanroom variants for retest_gap_tickers from the listed base_strategies."
                )
                if exact_promoted_base_strategies_available
                else (
                    "The registry snapshot preserved promoted counts but not the exact promoted base-strategy subset. "
                    "Use the listed base_strategies as the governed retest superset for the promoted_tickers scope."
                ),
            }
        )
    return sorted(rows, key=lambda row: row["family"])


def build_beta_promotion_manifest(
    *,
    live_manifest_path: Path,
    strategy_family_registry_path: Path,
) -> dict[str, Any]:
    live_strategies = _load_strategy_manifest_payload(live_manifest_path)
    registry_payload = _load_strategy_family_registry(strategy_family_registry_path)
    promoted_family_rows = _build_promoted_family_rows(registry_payload)
    live_by_family = Counter(str(strategy["family"]) for strategy in live_strategies)
    live_by_symbol = Counter(str(strategy["underlying_symbol"]).upper() for strategy in live_strategies)
    retest_gap_symbols = sorted(
        {
            ticker
            for row in promoted_family_rows
            for ticker in row["retest_gap_tickers"]
        }
    )
    return {
        "version": 1,
        "description": (
            "GitHub-tracked beta promotion retest manifest. "
            "Contains the exact live runner strategies plus the broader promoted family/ticker retest scope."
        ),
        "generated_at_et": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        "source_live_manifest_path": str(live_manifest_path),
        "source_strategy_family_registry_path": str(strategy_family_registry_path),
        "source_strategy_family_registry_generated_at": registry_payload.get("generated_at"),
        "summary": {
            "exact_live_strategy_count": len(live_strategies),
            "exact_live_underlying_count": len(live_by_symbol),
            "exact_live_by_underlying_symbol": dict(sorted(live_by_symbol.items())),
            "exact_live_by_family": dict(sorted(live_by_family.items())),
            "promoted_family_count": len(promoted_family_rows),
            "families_with_retest_gap_count": sum(1 for row in promoted_family_rows if row["retest_gap_tickers"]),
            "retest_gap_underlying_count": len(retest_gap_symbols),
            "retest_gap_underlying_symbols": retest_gap_symbols,
        },
        "exact_live_strategies": live_strategies,
        "beta_retest_families": promoted_family_rows,
    }


def main() -> None:
    args = build_parser().parse_args()
    document = build_beta_promotion_manifest(
        live_manifest_path=args.live_manifest.resolve(),
        strategy_family_registry_path=args.strategy_family_registry.resolve(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    print(json.dumps({"output_path": str(args.output.resolve())}, indent=2))


if __name__ == "__main__":
    main()

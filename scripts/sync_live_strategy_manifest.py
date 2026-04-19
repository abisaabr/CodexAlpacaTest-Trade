from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpaca_lab.multi_ticker_portfolio.config import (
    _default_strategies,
    _load_strategy_manifest_payload,
    default_portfolio_config,
    load_portfolio_config,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_manifest_path() -> Path:
    return _repo_root() / "config" / "strategy_manifests" / "multi_ticker_portfolio_live.yaml"


def _default_config_path() -> Path:
    return _repo_root() / "config" / "multi_ticker_paper_portfolio.yaml"


def _load_source_strategy_payloads(source: Path | None) -> list[dict[str, Any]]:
    if source is None:
        return [strategy.model_dump(mode="python") for strategy in _default_strategies()]

    raw = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
    if isinstance(raw, dict):
        strategies = raw.get("strategies")
    elif isinstance(raw, list):
        strategies = raw
    else:
        raise ValueError("Source payload must contain a top-level mapping or list.")
    if not isinstance(strategies, list):
        raise ValueError("Source payload must contain a top-level 'strategies' list.")
    return strategies


def _manifest_document(strategies: list[dict[str, Any]]) -> dict[str, Any]:
    strategy_count = len(strategies)
    by_symbol = Counter(str(strategy["underlying_symbol"]).upper() for strategy in strategies)
    by_family = Counter(str(strategy["family"]) for strategy in strategies)
    return {
        "version": 1,
        "description": "Checked-in live paper-runner strategy manifest.",
        "strategy_count": strategy_count,
        "summary": {
            "underlying_count": len(by_symbol),
            "by_underlying_symbol": dict(sorted(by_symbol.items())),
            "by_family": dict(sorted(by_family.items())),
        },
        "strategies": strategies,
    }


def _write_manifest(target: Path, strategies: list[dict[str, Any]]) -> None:
    document = _manifest_document(strategies)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _ordered_underlying_symbols(strategies: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for strategy in strategies:
        symbol = str(strategy["underlying_symbol"]).upper()
        if symbol not in seen:
            seen.add(symbol)
            ordered.append(symbol)
    return ordered


def _sync_portfolio_config_metadata(config_path: Path, strategies: list[dict[str, Any]]) -> None:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("Portfolio config must contain a top-level mapping.")
    execution = payload.setdefault("execution", {})
    if not isinstance(execution, dict):
        raise ValueError("Portfolio config execution section must be a mapping.")
    execution["underlying_symbols"] = _ordered_underlying_symbols(strategies)
    payload["description"] = "Shared-account intraday options paper portfolio using the checked-in live strategy manifest."
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _print_summary(strategies: list[dict[str, Any]]) -> None:
    by_symbol = Counter(str(strategy["underlying_symbol"]).upper() for strategy in strategies)
    by_name = Counter(str(strategy["name"]) for strategy in strategies)
    print(f"strategy_count={len(strategies)}")
    print(f"underlying_count={len(by_symbol)}")
    print("per_underlying=" + json.dumps(dict(sorted(by_symbol.items())), indent=2))
    print(f"unique_strategy_names={len(by_name)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write or validate the checked-in live paper-runner strategy manifest."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=None,
        help="Optional YAML/JSON source containing a top-level strategies list. Defaults to the hardcoded fallback live book.",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=_default_manifest_path(),
        help="Manifest path to write or validate.",
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=_default_config_path(),
        help="Portfolio config to validate after writing.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Do not write the manifest; only validate the existing manifest/config pair.",
    )
    args = parser.parse_args()

    if args.validate_only:
        manifest_strategies = _load_strategy_manifest_payload(args.target)
        config = load_portfolio_config(args.config_path)
        if len(manifest_strategies) != len(config.strategies):
            raise SystemExit(
                f"Manifest/config mismatch: manifest has {len(manifest_strategies)} strategies but config loads {len(config.strategies)}."
            )
        manifest_symbols = _ordered_underlying_symbols(manifest_strategies)
        config_symbols = list(config.execution.underlying_symbols)
        if manifest_symbols != config_symbols:
            raise SystemExit(
                "Manifest/config mismatch: execution.underlying_symbols does not match manifest symbols."
            )
        _print_summary(manifest_strategies)
        return

    source_strategies = _load_source_strategy_payloads(args.source)
    _write_manifest(args.target, source_strategies)
    _sync_portfolio_config_metadata(args.config_path, source_strategies)
    config = load_portfolio_config(args.config_path)
    _print_summary([strategy.model_dump(mode="python") for strategy in config.strategies])
    print(f"manifest_path={args.target}")
    if config.strategy_manifest_path is not None:
        print(f"loaded_manifest_path={config.strategy_manifest_path}")
    print(f"config_path={args.config_path}")
    print(f"default_config_strategy_count={len(default_portfolio_config().strategies)}")


if __name__ == "__main__":
    main()

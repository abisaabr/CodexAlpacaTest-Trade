from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_CONFIG = REPO_ROOT / "config" / "research_tournaments" / "portfolio_overnight_12h_20260501.yaml"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "gcp_research" / "portfolio_overnight_12h_20260501" / "inputs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic portfolio overnight variants and option-aware queue inputs."
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--variants-jsonl", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-templates-per-symbol", type=int, default=250)
    return parser.parse_args()


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML mapping at {path}")
    return payload


def _load_variants(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _target_symbols(config: dict[str, Any]) -> list[str]:
    symbols = {
        str(symbol).upper()
        for dataset in config.get("datasets", {}).values()
        if isinstance(dataset, dict) and dataset.get("overnight_enabled", True) is not False
        for symbol in dataset.get("symbols", [])
    }
    return sorted(symbols)


def _template_key(row: dict[str, Any]) -> str:
    payload = {
        "queue_id": row.get("queue_id"),
        "priority": row.get("priority"),
        "variant_type": row.get("variant_type"),
        "parameters": row.get("parameters", {}),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _template_slug(template_key: str) -> str:
    return hashlib.sha256(template_key.encode("utf-8")).hexdigest()[:14]


def _direction(row: dict[str, Any]) -> str:
    parameters = row.get("parameters") if isinstance(row.get("parameters"), dict) else {}
    source = " ".join(
        str(value or "")
        for value in [
            row.get("variant_id"),
            row.get("source_strategy_id"),
            row.get("variant_type"),
            parameters.get("family_template"),
        ]
    ).lower()
    if any(token in source for token in ["put", "short", "bear"]):
        return "put"
    return "call"


def _intended_regime(row: dict[str, Any]) -> str:
    parameters = row.get("parameters") if isinstance(row.get("parameters"), dict) else {}
    source = " ".join(
        str(value or "")
        for value in [
            row.get("variant_id"),
            row.get("source_strategy_id"),
            row.get("variant_type"),
            parameters.get("family_template"),
        ]
    ).lower()
    if any(token in source for token in ["iron", "butterfly", "condor", "premium", "choppy"]):
        return "choppy"
    if any(token in source for token in ["put", "short", "bear"]):
        return "bear"
    if any(token in source for token in ["call", "long", "bull", "trend"]):
        return "bull"
    return "unclassified"


def _family(row: dict[str, Any]) -> str:
    parameters = row.get("parameters") if isinstance(row.get("parameters"), dict) else {}
    return str(parameters.get("family_template") or row.get("variant_type") or "unknown")


def _round_robin_by_symbol(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"])].append(row)
    ordered: list[dict[str, Any]] = []
    symbols = sorted(grouped)
    index = 0
    while True:
        added = False
        for symbol in symbols:
            bucket = grouped[symbol]
            if index < len(bucket):
                ordered.append(bucket[index])
                added = True
        if not added:
            return ordered
        index += 1


def build_inputs(
    *,
    config_path: Path,
    variants_jsonl: Path,
    output_dir: Path,
    max_templates_per_symbol: int,
) -> dict[str, Any]:
    config = _load_yaml(config_path)
    source_variants = _load_variants(variants_jsonl)
    if not source_variants:
        raise ValueError(f"No variants found in {variants_jsonl}")

    templates: dict[str, dict[str, Any]] = {}
    for row in source_variants:
        key = _template_key(row)
        templates.setdefault(key, row)
    ordered_templates = sorted(
        templates.items(),
        key=lambda item: (
            int(item[1].get("priority", 999)),
            str(item[1].get("queue_id") or ""),
            str(item[1].get("variant_type") or ""),
            item[0],
        ),
    )
    if max_templates_per_symbol > 0:
        ordered_templates = ordered_templates[:max_templates_per_symbol]

    target_symbols = _target_symbols(config)
    variants: list[dict[str, Any]] = []
    queue_items: list[dict[str, Any]] = []
    for symbol in target_symbols:
        for template_key, template in ordered_templates:
            slug = _template_slug(template_key)
            variant = dict(template)
            variant_id = f"portfolio12h__{symbol.lower()}__{slug}"
            variant["variant_id"] = variant_id
            variant["symbol"] = symbol
            variant["source_template_variant_id"] = template.get("variant_id")
            variant["source_template_symbol"] = template.get("symbol")
            variant["generated_for_wave"] = config.get("wave_id")
            variant["execution_effect"] = "none"
            variant["live_manifest_effect"] = "none"
            variant["risk_policy_effect"] = "none"
            variant["state"] = "research_only"
            variants.append(variant)
            queue_items.append(
                {
                    "rank": len(queue_items) + 1,
                    "state": "research_follow_up_only",
                    "candidate_variant_id": variant_id,
                    "symbol": symbol,
                    "directional_option_type": _direction(variant),
                    "family": _family(variant),
                    "intended_regime": _intended_regime(variant),
                    "parameter_set": json.dumps(
                        variant.get("parameters", {}), sort_keys=True, separators=(",", ":")
                    ),
                    "source_strategy_id": variant_id,
                    "source_template_variant_id": template.get("variant_id"),
                    "promotion_allowed": False,
                    "broker_facing": False,
                    "live_manifest_effect": "none",
                    "risk_policy_effect": "none",
                    "blockers": [],
                }
            )

    variants = _round_robin_by_symbol(variants)
    queue_items = _round_robin_by_symbol(queue_items)
    for rank, item in enumerate(queue_items, start=1):
        item["rank"] = rank

    output_dir.mkdir(parents=True, exist_ok=True)
    variants_path = output_dir / "portfolio_overnight_variants.jsonl"
    queue_path = output_dir / "portfolio_overnight_option_queue.json"
    manifest_path = output_dir / "portfolio_overnight_inputs_manifest.json"
    markdown_path = output_dir / "portfolio_overnight_inputs_manifest.md"

    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in variants) + "\n",
        encoding="utf-8",
    )
    queue = {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "status": "ready_for_option_aware_backtest",
        "wave_id": config.get("wave_id"),
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "source_variants_jsonl": str(variants_jsonl),
        "source_variant_count": len(source_variants),
        "template_count": len(ordered_templates),
        "target_symbols": target_symbols,
        "queue_item_count": len(queue_items),
        "blocker_counts": {},
        "queue_items": queue_items,
    }
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest = {
        "generated_at": queue["generated_at"],
        "status": "portfolio_overnight_inputs_ready",
        "wave_id": config.get("wave_id"),
        "target_symbols": target_symbols,
        "template_count": len(ordered_templates),
        "variant_count": len(variants),
        "queue_item_count": len(queue_items),
        "broker_facing": False,
        "promotion_allowed": False,
        "outputs": {
            "variants_jsonl": str(variants_path),
            "queue_json": str(queue_path),
            "manifest_json": str(manifest_path),
            "manifest_md": str(markdown_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(markdown_path, manifest)
    return manifest


def _write_markdown(path: Path, manifest: dict[str, Any]) -> None:
    lines = [
        "# Portfolio Overnight Research Inputs",
        "",
        f"- Generated at: `{manifest['generated_at']}`",
        f"- Status: `{manifest['status']}`",
        f"- Wave ID: `{manifest['wave_id']}`",
        f"- Template count per symbol: `{manifest['template_count']}`",
        f"- Variant count: `{manifest['variant_count']}`",
        f"- Queue item count: `{manifest['queue_item_count']}`",
        f"- Broker facing: `{str(manifest['broker_facing']).lower()}`",
        f"- Promotion allowed: `{str(manifest['promotion_allowed']).lower()}`",
        "",
        "## Target Symbols",
        "",
        "`" + " ".join(manifest["target_symbols"]) + "`",
        "",
        "## Outputs",
        "",
    ]
    for key, value in manifest["outputs"].items():
        lines.append(f"- `{key}`: `{value}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    manifest = build_inputs(
        config_path=Path(args.config),
        variants_jsonl=Path(args.variants_jsonl),
        output_dir=Path(args.output_dir),
        max_templates_per_symbol=args.max_templates_per_symbol,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

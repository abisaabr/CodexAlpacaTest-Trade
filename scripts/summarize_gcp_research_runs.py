from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUN_ROOT = REPO_ROOT / "reports" / "research_wave"
DEFAULT_OUTPUT_DIR = DEFAULT_RUN_ROOT / "summaries"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize completed GCP research wave runs.")
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id-prefix", default="research_wave_20260424_rq002_real_stock_bar_smoke")
    parser.add_argument("--top-n", type=int, default=25)
    return parser.parse_args()


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_run_dirs(run_root: Path, run_id_prefix: str) -> list[Path]:
    if not run_root.exists():
        return []
    return sorted(
        path
        for path in run_root.iterdir()
        if path.is_dir()
        and path.name.startswith(run_id_prefix)
        and (path / "research_run_manifest.json").exists()
    )


def _result_key(row: dict[str, Any]) -> float:
    return float(row.get("net_expectancy_after_cost_proxy") or 0.0)


def _compact_result(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "variant_id",
        "queue_id",
        "symbol",
        "variant_type",
        "source_strategy_id",
        "actual_trade_count",
        "net_pnl",
        "expectancy_after_cost",
        "win_rate",
        "profit_factor",
        "max_drawdown",
        "tail_loss_proxy",
        "recommendation",
        "evidence_mode",
        "broker_facing",
        "live_manifest_effect",
        "risk_policy_effect",
    ]
    return {key: row.get(key) for key in keys if key in row}


def _deduplicate_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    by_variant: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for row in rows:
        variant_id = str(row.get("variant_id") or "")
        if not variant_id:
            variant_id = f"missing_variant_id::{len(by_variant)}"
        existing = by_variant.get(variant_id)
        if existing is None:
            by_variant[variant_id] = row
            continue
        duplicate_count += 1
        if _result_key(row) > _result_key(existing):
            by_variant[variant_id] = row
    return list(by_variant.values()), duplicate_count


def build_summary(*, run_root: Path, run_id_prefix: str, top_n: int) -> dict[str, Any]:
    run_dirs = _iter_run_dirs(run_root, run_id_prefix)
    manifests: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for run_dir in run_dirs:
        manifest = _load_json(run_dir / "research_run_manifest.json")
        results = _load_json(run_dir / "normalized_backtest_results.json")
        if not isinstance(manifest, dict) or not isinstance(results, list):
            continue
        manifests.append(manifest)
        rows.extend(row for row in results if isinstance(row, dict))

    raw_result_count = len(rows)
    rows, duplicate_result_count = _deduplicate_rows(rows)
    recommendation_counts: dict[str, int] = {}
    symbol_counts: dict[str, int] = {}
    for row in rows:
        recommendation = str(row.get("recommendation") or "unknown")
        symbol = str(row.get("symbol") or "unknown")
        recommendation_counts[recommendation] = recommendation_counts.get(recommendation, 0) + 1
        symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1

    ranked = sorted(rows, key=_result_key, reverse=True)
    quarantine = [row for row in ranked if row.get("recommendation") == "quarantine"]
    candidates = [
        row
        for row in ranked
        if row.get("recommendation") == "candidate_for_deeper_option_backtest"
    ]
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "run_root": str(run_root),
        "run_id_prefix": run_id_prefix,
        "run_count": len(manifests),
        "variant_result_count": len(rows),
        "raw_variant_result_count": raw_result_count,
        "duplicate_variant_result_count": duplicate_result_count,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "recommendation_counts": dict(sorted(recommendation_counts.items())),
        "symbol_counts": dict(sorted(symbol_counts.items())),
        "mean_net_expectancy_after_cost_proxy": round(
            sum(_result_key(row) for row in rows) / len(rows), 4
        )
        if rows
        else 0.0,
        "top_candidates": [_compact_result(row) for row in candidates[:top_n]],
        "worst_quarantine": [_compact_result(row) for row in sorted(quarantine, key=_result_key)[:top_n]],
        "source_runs": [
            {
                "run_id": manifest.get("run_id"),
                "chunk_id": manifest.get("chunk_id"),
                "evidence_mode": manifest.get("evidence_mode"),
                "input_variant_count": manifest.get("input_variant_count"),
                "result_summary": manifest.get("result_summary"),
            }
            for manifest in manifests
        ],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# GCP Research Run Summary",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Run count: `{payload['run_count']}`",
        f"- Variant result count: `{payload['variant_result_count']}`",
        f"- Duplicate variant results removed: `{payload['duplicate_variant_result_count']}`",
        f"- Mean net expectancy after cost: `{payload['mean_net_expectancy_after_cost_proxy']}`",
        f"- Promotion allowed: `{payload['promotion_allowed']}`",
        f"- Broker facing: `{payload['broker_facing']}`",
        "",
        "## Recommendation Counts",
        "",
    ]
    for name, count in payload["recommendation_counts"].items():
        lines.append(f"- `{name}`: `{count}`")
    lines.extend(["", "## Top Candidates", ""])
    for row in payload["top_candidates"][:10]:
        lines.append(
            "- "
            f"`{row.get('variant_id')}` "
            f"`{row.get('symbol')}` "
            f"expectancy `{row.get('expectancy_after_cost')}` "
            f"net_pnl `{row.get('net_pnl')}` "
            f"trades `{row.get('actual_trade_count')}`"
        )
    lines.extend(["", "## Worst Quarantine", ""])
    for row in payload["worst_quarantine"][:10]:
        lines.append(
            "- "
            f"`{row.get('variant_id')}` "
            f"`{row.get('symbol')}` "
            f"expectancy `{row.get('expectancy_after_cost')}` "
            f"net_pnl `{row.get('net_pnl')}` "
            f"trades `{row.get('actual_trade_count')}`"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    payload = build_summary(
        run_root=Path(args.run_root),
        run_id_prefix=args.run_id_prefix,
        top_n=args.top_n,
    )
    output_dir = Path(args.output_dir)
    write_json(output_dir / "gcp_research_run_summary.json", payload)
    write_markdown(output_dir / "gcp_research_run_summary.md", payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

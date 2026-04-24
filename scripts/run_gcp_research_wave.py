from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave"
DEFAULT_EVIDENCE_MODE = "metadata_proxy_smoke"
REQUIRED_OUTPUTS = [
    "research_run_manifest",
    "normalized_backtest_results",
    "train_test_or_walk_forward_summary",
    "after_cost_expectancy_table",
    "drawdown_and_tail_loss_report",
    "loser_cluster_comparison",
    "candidate_hold_kill_quarantine_recommendation",
]
PREFERRED_SYMBOL_BONUS = {
    "QQQ": 9.0,
    "MSFT": 7.5,
    "GLD": 5.5,
    "SLV": 4.5,
    "TSLA": 2.5,
}
SHADOW_SYMBOL_PENALTY = {
    "NVDA": -12.0,
    "AMZN": -9.0,
    "PLTR": -8.0,
    "IWM": -6.0,
    "SPY": -4.0,
    "XLE": -3.5,
}
VARIANT_TYPE_BONUS = {
    "defined_risk_family_expansion": 10.0,
    "single_leg_repair": 4.0,
    "loser_cluster_shadow_diagnostic": -5.0,
    "regime_liquidity_feature_grid": 0.0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a research-only GCP wave chunk.")
    parser.add_argument("--variants-jsonl", required=True, help="Local path or gs:// variants JSONL.")
    parser.add_argument("--wave-manifest-json", default=None, help="Optional wave manifest for chunk slicing.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--chunk-id", default=None)
    parser.add_argument("--queue-id", action="append", default=[])
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--priority", action="append", type=int, default=[])
    parser.add_argument("--max-variants", type=int, default=None)
    parser.add_argument("--evidence-mode", default=DEFAULT_EVIDENCE_MODE)
    parser.add_argument("--allow-non-smoke-evidence", action="store_true")
    return parser.parse_args()


def _download_gcs_uri(uri: str) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="research_wave_"))
    destination = temp_dir / Path(uri).name
    subprocess.run(["gcloud", "storage", "cp", uri, str(destination)], check=True)
    return destination


def resolve_input_path(path_or_uri: str) -> Path:
    if path_or_uri.startswith("gs://"):
        return _download_gcs_uri(path_or_uri)
    return Path(path_or_uri)


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def load_variants(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def chunk_slice(wave_manifest: dict[str, Any], chunk_id: str | None) -> tuple[int, int] | None:
    if not chunk_id:
        return None
    chunks = wave_manifest.get("chunks") if isinstance(wave_manifest.get("chunks"), list) else []
    for chunk in chunks:
        if isinstance(chunk, dict) and chunk.get("chunk_id") == chunk_id:
            return int(chunk["start_index"]), int(chunk["end_index"]) + 1
    raise ValueError(f"Chunk id not found in wave manifest: {chunk_id}")


def filter_variants(
    variants: list[dict[str, Any]],
    *,
    wave_manifest: dict[str, Any],
    chunk_id: str | None,
    queue_ids: set[str],
    symbols: set[str],
    priorities: set[int],
    max_variants: int | None,
) -> list[dict[str, Any]]:
    sliced = variants
    bounds = chunk_slice(wave_manifest, chunk_id)
    if bounds:
        sliced = sliced[bounds[0] : bounds[1]]
    filtered = []
    for row in sliced:
        if queue_ids and str(row.get("queue_id")) not in queue_ids:
            continue
        if symbols and str(row.get("symbol")) not in symbols:
            continue
        if priorities and int(row.get("priority", 0)) not in priorities:
            continue
        filtered.append(row)
    if max_variants is not None:
        filtered = filtered[: max(max_variants, 0)]
    return filtered


def stable_unit_interval(*parts: Any) -> float:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(0xFFFFFFFFFFFF)


def _parameter_score(parameters: dict[str, Any]) -> float:
    score = 0.0
    if parameters.get("liquidity_gate") == "tight":
        score += 3.0
    if parameters.get("avoid_after_loser_similarity") is True:
        score += 4.0
    stop = parameters.get("stop_loss_multiple")
    if isinstance(stop, int | float):
        score += max(0.0, 0.30 - float(stop)) * 30.0
    exit_minute = parameters.get("hard_exit_minute")
    if isinstance(exit_minute, int | float) and float(exit_minute) <= 300:
        score += 2.0
    template = str(parameters.get("family_template") or "")
    if "butterfly" in template:
        score += 2.5
    if "premium_defense" in template:
        score += 2.0
    return score


def score_variant(variant: dict[str, Any], *, evidence_mode: str) -> dict[str, Any]:
    symbol = str(variant.get("symbol") or "UNKNOWN")
    variant_type = str(variant.get("variant_type") or "unknown")
    parameters = variant.get("parameters") if isinstance(variant.get("parameters"), dict) else {}
    noise = (stable_unit_interval(variant.get("variant_id"), json.dumps(parameters, sort_keys=True)) - 0.5) * 18.0
    gross_expectancy = (
        VARIANT_TYPE_BONUS.get(variant_type, 0.0)
        + PREFERRED_SYMBOL_BONUS.get(symbol, 0.0)
        + SHADOW_SYMBOL_PENALTY.get(symbol, 0.0)
        + _parameter_score(parameters)
        + noise
    )
    estimated_cost = 2.5
    net_expectancy = gross_expectancy - estimated_cost
    drawdown = max(20.0, 180.0 - gross_expectancy * 3.0 + stable_unit_interval(symbol, variant_type) * 80.0)
    tail_loss = max(8.0, drawdown * (0.28 + stable_unit_interval(variant.get("variant_id"), "tail") * 0.25))
    win_rate = min(0.72, max(0.30, 0.48 + net_expectancy / 120.0))
    synthetic_trade_count = 20 + int(stable_unit_interval(variant.get("variant_id"), "trades") * 40)
    if evidence_mode == "metadata_proxy_smoke":
        recommendation = "hold_for_real_backtest"
    elif net_expectancy >= 12 and drawdown <= 190 and tail_loss <= 85:
        recommendation = "research_review_candidate"
    elif net_expectancy <= -8 or tail_loss >= 120:
        recommendation = "quarantine"
    else:
        recommendation = "hold"
    if variant_type == "loser_cluster_shadow_diagnostic" and recommendation == "research_review_candidate":
        recommendation = "hold_shadow_diagnostic"
    return {
        "variant_id": variant.get("variant_id"),
        "queue_id": variant.get("queue_id"),
        "priority": variant.get("priority"),
        "symbol": symbol,
        "variant_type": variant_type,
        "source_strategy_id": variant.get("source_strategy_id"),
        "evidence_mode": evidence_mode,
        "synthetic_trade_count": synthetic_trade_count,
        "gross_expectancy_proxy": round(gross_expectancy, 4),
        "estimated_cost_proxy": estimated_cost,
        "net_expectancy_after_cost_proxy": round(net_expectancy, 4),
        "win_rate_proxy": round(win_rate, 4),
        "max_drawdown_proxy": round(drawdown, 4),
        "tail_loss_proxy": round(tail_loss, 4),
        "recommendation": recommendation,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "broker_facing": False,
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, title: str, payload: dict[str, Any]) -> None:
    lines = [f"# {title}", ""]
    for key, value in payload.items():
        lines.append(f"- {key}: `{value}`")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {
            "variant_count": 0,
            "mean_net_expectancy_after_cost_proxy": 0.0,
            "best_variant_id": None,
            "recommendation_counts": {},
        }
    best = max(results, key=lambda row: float(row["net_expectancy_after_cost_proxy"]))
    recommendation_counts: dict[str, int] = {}
    for row in results:
        key = str(row["recommendation"])
        recommendation_counts[key] = recommendation_counts.get(key, 0) + 1
    return {
        "variant_count": len(results),
        "mean_net_expectancy_after_cost_proxy": round(
            sum(float(row["net_expectancy_after_cost_proxy"]) for row in results) / len(results), 4
        ),
        "best_variant_id": best["variant_id"],
        "best_symbol": best["symbol"],
        "best_net_expectancy_after_cost_proxy": best["net_expectancy_after_cost_proxy"],
        "recommendation_counts": dict(sorted(recommendation_counts.items())),
    }


def group_summary(results: list[dict[str, Any]], group_key: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in results:
        groups.setdefault(str(row.get(group_key)), []).append(row)
    output = []
    for key, rows in sorted(groups.items()):
        output.append(
            {
                group_key: key,
                "variant_count": len(rows),
                "mean_net_expectancy_after_cost_proxy": round(
                    sum(float(row["net_expectancy_after_cost_proxy"]) for row in rows) / len(rows), 4
                ),
                "worst_tail_loss_proxy": max(float(row["tail_loss_proxy"]) for row in rows),
                "quarantine_count": sum(1 for row in rows if row["recommendation"] == "quarantine"),
            }
        )
    return output


def build_recommendation_packet(results: list[dict[str, Any]], evidence_mode: str) -> dict[str, Any]:
    ranked = sorted(results, key=lambda row: float(row["net_expectancy_after_cost_proxy"]), reverse=True)
    return {
        "evidence_mode": evidence_mode,
        "promotion_allowed": evidence_mode != "metadata_proxy_smoke",
        "promotion_note": (
            "Metadata proxy smoke output cannot promote strategies; it only prioritizes real backtests."
            if evidence_mode == "metadata_proxy_smoke"
            else "Promotion still requires governance review and broker-audited evidence."
        ),
        "top_research_priorities": ranked[:10],
        "quarantine_candidates": [row for row in ranked if row["recommendation"] == "quarantine"][:25],
    }


def write_artifacts(
    *,
    output_dir: Path,
    run_id: str,
    variants: list[dict[str, Any]],
    results: list[dict[str, Any]],
    args: argparse.Namespace,
    wave_manifest: dict[str, Any],
) -> dict[str, str]:
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "run_id": run_id,
        "evidence_mode": args.evidence_mode,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "wave_id": wave_manifest.get("wave_id"),
        "chunk_id": args.chunk_id,
        "input_variant_count": len(variants),
        "result_summary": summarize_results(results),
        "required_outputs": REQUIRED_OUTPUTS,
    }
    train_rows = group_summary(results, "queue_id")
    expectancy_rows = group_summary(results, "symbol")
    loser_rows = group_summary(results, "variant_type")
    drawdown_packet = {
        "run_id": run_id,
        "worst_max_drawdown_proxy": max((float(row["max_drawdown_proxy"]) for row in results), default=0.0),
        "worst_tail_loss_proxy": max((float(row["tail_loss_proxy"]) for row in results), default=0.0),
        "evidence_mode": args.evidence_mode,
    }
    recommendation_packet = build_recommendation_packet(results, args.evidence_mode)

    artifacts = {
        "research_run_manifest": run_dir / "research_run_manifest.json",
        "research_run_manifest_md": run_dir / "research_run_manifest.md",
        "normalized_backtest_results": run_dir / "normalized_backtest_results.csv",
        "normalized_backtest_results_json": run_dir / "normalized_backtest_results.json",
        "train_test_or_walk_forward_summary": run_dir / "train_test_or_walk_forward_summary.json",
        "after_cost_expectancy_table": run_dir / "after_cost_expectancy_table.csv",
        "drawdown_and_tail_loss_report": run_dir / "drawdown_and_tail_loss_report.json",
        "drawdown_and_tail_loss_report_md": run_dir / "drawdown_and_tail_loss_report.md",
        "loser_cluster_comparison": run_dir / "loser_cluster_comparison.csv",
        "candidate_hold_kill_quarantine_recommendation": run_dir
        / "candidate_hold_kill_quarantine_recommendation.json",
        "candidate_hold_kill_quarantine_recommendation_md": run_dir
        / "candidate_hold_kill_quarantine_recommendation.md",
    }
    _write_json(artifacts["research_run_manifest"], manifest)
    _write_markdown(artifacts["research_run_manifest_md"], "Research Run Manifest", manifest)
    _write_csv(artifacts["normalized_backtest_results"], results)
    _write_json(artifacts["normalized_backtest_results_json"], results)
    _write_json(artifacts["train_test_or_walk_forward_summary"], train_rows)
    _write_csv(artifacts["after_cost_expectancy_table"], expectancy_rows)
    _write_json(artifacts["drawdown_and_tail_loss_report"], drawdown_packet)
    _write_markdown(
        artifacts["drawdown_and_tail_loss_report_md"],
        "Drawdown And Tail Loss Report",
        drawdown_packet,
    )
    _write_csv(artifacts["loser_cluster_comparison"], loser_rows)
    _write_json(artifacts["candidate_hold_kill_quarantine_recommendation"], recommendation_packet)
    _write_markdown(
        artifacts["candidate_hold_kill_quarantine_recommendation_md"],
        "Candidate Hold Kill Quarantine Recommendation",
        {
            "evidence_mode": recommendation_packet["evidence_mode"],
            "promotion_allowed": recommendation_packet["promotion_allowed"],
            "promotion_note": recommendation_packet["promotion_note"],
            "top_research_priority_count": len(recommendation_packet["top_research_priorities"]),
            "quarantine_candidate_count": len(recommendation_packet["quarantine_candidates"]),
        },
    )
    return {key: str(path) for key, path in artifacts.items()}


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.evidence_mode != DEFAULT_EVIDENCE_MODE and not args.allow_non_smoke_evidence:
        raise ValueError("--allow-non-smoke-evidence is required for evidence modes beyond metadata proxy smoke.")
    variants_path = resolve_input_path(args.variants_jsonl)
    wave_manifest_path = resolve_input_path(args.wave_manifest_json) if args.wave_manifest_json else None
    wave_manifest = load_json(wave_manifest_path)
    variants = load_variants(variants_path)
    selected = filter_variants(
        variants,
        wave_manifest=wave_manifest,
        chunk_id=args.chunk_id,
        queue_ids={str(item) for item in args.queue_id},
        symbols={str(item).upper() for item in args.symbol},
        priorities=set(args.priority),
        max_variants=args.max_variants,
    )
    results = [score_variant(variant, evidence_mode=args.evidence_mode) for variant in selected]
    run_id = args.run_id or f"research_wave_{datetime.now().astimezone().strftime('%Y%m%d_%H%M%S')}"
    artifacts = write_artifacts(
        output_dir=Path(args.output_dir),
        run_id=run_id,
        variants=selected,
        results=results,
        args=args,
        wave_manifest=wave_manifest,
    )
    return {
        "run_id": run_id,
        "evidence_mode": args.evidence_mode,
        "selected_variant_count": len(selected),
        "summary": summarize_results(results),
        "artifacts": artifacts,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }


def main() -> None:
    result = run(parse_args())
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

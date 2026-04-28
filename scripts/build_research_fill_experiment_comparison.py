from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_research_portfolio_report import _float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare research-only fill experiments from wave-level rollup JSON files. "
            "Inputs are labels mapped to research_wave_portfolio_rollup.json paths."
        )
    )
    parser.add_argument(
        "--input",
        action="append",
        required=True,
        help="Experiment input in label=path form. Repeat for multiple experiments.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--baseline-label", default=None)
    parser.add_argument("--fill-coverage-gate", type=float, default=0.90)
    parser.add_argument("--top-n", type=int, default=25)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _parse_input(value: str) -> tuple[str, Path]:
    label, separator, path = value.partition("=")
    if not separator or not label.strip() or not path.strip():
        raise ValueError("--input must be in label=path form.")
    return label.strip(), Path(path.strip())


def _candidate_key(row: dict[str, Any]) -> str:
    return str(row.get("candidate_variant_id") or "")


def _lane_summary(label: str, packet: dict[str, Any]) -> dict[str, Any]:
    top_candidates = [row for row in packet.get("top_candidates", []) if isinstance(row, dict)]
    fill_values = [_float(row.get("min_fill_coverage")) for row in top_candidates]
    positive_economics = [
        row
        for row in top_candidates
        if _float(row.get("min_net_pnl")) > 0 and _float(row.get("min_test_net_pnl")) > 0
    ]
    return {
        "label": label,
        "decision": packet.get("decision"),
        "source_report_count": int(packet.get("source_report_count") or 0),
        "candidate_count": int(packet.get("candidate_count") or len(top_candidates)),
        "eligible_for_promotion_review_count": int(
            packet.get("eligible_for_promotion_review_count") or 0
        ),
        "capital_plan_count": len(packet.get("capital_plan") or []),
        "fill_failure_counts": packet.get("fill_failure_counts") or {},
        "positive_economics_candidate_count": len(positive_economics),
        "max_min_fill_coverage": round(max(fill_values), 6) if fill_values else 0.0,
        "median_min_fill_coverage": (
            round(sorted(fill_values)[len(fill_values) // 2], 6) if fill_values else 0.0
        ),
    }


def _best_symbol_rows(label: str, packet: dict[str, Any]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in packet.get("top_candidates", []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        current = best.get(symbol)
        if current is None or _float(row.get("research_score")) > _float(
            current.get("research_score")
        ):
            selected = {
                "label": label,
                "symbol": symbol,
                "candidate_variant_id": row.get("candidate_variant_id"),
                "research_score": row.get("research_score"),
                "min_net_pnl": row.get("min_net_pnl"),
                "min_test_net_pnl": row.get("min_test_net_pnl"),
                "min_fill_coverage": row.get("min_fill_coverage"),
                "max_fill_coverage": row.get("max_fill_coverage"),
                "fill_failure_reason": row.get("fill_failure_reason"),
                "promotion_status": row.get("promotion_status"),
                "promotion_blockers": row.get("promotion_blockers", []),
            }
            best[symbol] = selected
    return sorted(
        best.values(),
        key=lambda row: (
            row.get("promotion_status") == "eligible_for_promotion_review",
            _float(row.get("research_score")),
            _float(row.get("min_fill_coverage")),
        ),
        reverse=True,
    )


def _comparison_rows(
    packets: dict[str, dict[str, Any]],
    *,
    baseline_label: str | None,
    fill_coverage_gate: float,
) -> list[dict[str, Any]]:
    by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for label, packet in packets.items():
        for row in packet.get("top_candidates", []):
            if not isinstance(row, dict):
                continue
            key = _candidate_key(row)
            if not key:
                continue
            item = row.copy()
            item["label"] = label
            by_candidate[key].append(item)

    rows: list[dict[str, Any]] = []
    for candidate_id, group in by_candidate.items():
        if len(group) < 2 and baseline_label is not None:
            continue
        best = max(group, key=lambda row: _float(row.get("min_fill_coverage")))
        baseline = None
        if baseline_label is not None:
            baseline = next((row for row in group if row["label"] == baseline_label), None)
        if baseline is None:
            baseline = min(group, key=lambda row: _float(row.get("min_fill_coverage")))
        fill_delta = _float(best.get("min_fill_coverage")) - _float(
            baseline.get("min_fill_coverage")
        )
        rows.append(
            {
                "candidate_variant_id": candidate_id,
                "symbol": best.get("symbol"),
                "best_label": best["label"],
                "baseline_label": baseline["label"],
                "best_min_fill_coverage": best.get("min_fill_coverage"),
                "baseline_min_fill_coverage": baseline.get("min_fill_coverage"),
                "fill_delta": round(fill_delta, 6),
                "best_min_net_pnl": best.get("min_net_pnl"),
                "best_min_test_net_pnl": best.get("min_test_net_pnl"),
                "best_research_score": best.get("research_score"),
                "best_promotion_status": best.get("promotion_status"),
                "best_promotion_blockers": best.get("promotion_blockers", []),
                "crosses_fill_gate": _float(best.get("min_fill_coverage")) >= fill_coverage_gate,
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["crosses_fill_gate"],
            _float(row.get("fill_delta")),
            _float(row.get("best_research_score")),
        ),
        reverse=True,
    )


def _decision(packet: dict[str, Any]) -> str:
    lane_summaries = packet["lane_summaries"]
    if any(row["eligible_for_promotion_review_count"] > 0 for row in lane_summaries):
        return "ready_for_governed_validation_review"
    if any(
        row["fill_failure_counts"].get("selected_contract_universe_gap", 0) > 0
        for row in lane_summaries
    ):
        return "continue_dense_or_broader_contract_universe_repair"
    if any(
        row["fill_failure_counts"].get("entry_bar_gap_or_entry_timing_mismatch", 0) > 0
        or row["fill_failure_counts"].get("exit_bar_gap_or_exit_policy_mismatch", 0) > 0
        for row in lane_summaries
    ):
        return "strategy_entry_exit_redesign_required"
    return "research_only_blocked"


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Research Fill Experiment Comparison",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Baseline label: `{packet['baseline_label']}`",
        f"- Fill coverage gate: `{packet['fill_coverage_gate']}`",
        f"- Broker-facing: `{packet['broker_facing']}`",
        "",
        "## Lane Summaries",
        "",
    ]
    for row in packet["lane_summaries"]:
        lines.append(
            "- "
            f"`{row['label']}` decision `{row['decision']}` candidates `{row['candidate_count']}` "
            f"eligible `{row['eligible_for_promotion_review_count']}` "
            f"positive `{row['positive_economics_candidate_count']}` "
            f"max_fill `{row['max_min_fill_coverage']}`"
        )
    lines.extend(["", "## Fill Failure Counts", ""])
    for row in packet["lane_summaries"]:
        counts = ", ".join(f"{key}={value}" for key, value in row["fill_failure_counts"].items())
        lines.append(f"- `{row['label']}`: {counts or 'none'}")
    lines.extend(["", "## Best Fill Improvements", ""])
    if not packet["candidate_comparisons"]:
        lines.append("- No overlapping candidate comparisons available.")
    for row in packet["candidate_comparisons"][: packet["top_n"]]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"best `{row['best_label']}` fill `{row['best_min_fill_coverage']}` "
            f"baseline `{row['baseline_label']}` fill `{row['baseline_min_fill_coverage']}` "
            f"delta `{row['fill_delta']}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_research_fill_experiment_comparison(
    *,
    inputs: list[tuple[str, Path]],
    output_dir: Path,
    baseline_label: str | None = None,
    fill_coverage_gate: float = 0.90,
    top_n: int = 25,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    packets = {label: _load_json(path) for label, path in inputs}
    if baseline_label is not None and baseline_label not in packets:
        raise ValueError(f"baseline label not found: {baseline_label}")
    lane_summaries = [_lane_summary(label, packet) for label, packet in packets.items()]
    candidate_comparisons = _comparison_rows(
        packets, baseline_label=baseline_label, fill_coverage_gate=fill_coverage_gate
    )
    symbol_leaders: list[dict[str, Any]] = []
    for label, packet in packets.items():
        symbol_leaders.extend(_best_symbol_rows(label, packet))
    all_fill_failures: Counter[str] = Counter()
    for summary in lane_summaries:
        all_fill_failures.update(
            {str(key): int(value) for key, value in summary["fill_failure_counts"].items()}
        )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "research_fill_experiment_comparison_complete",
        "decision": "pending",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "baseline_label": baseline_label,
        "fill_coverage_gate": fill_coverage_gate,
        "top_n": top_n,
        "lane_summaries": lane_summaries,
        "aggregate_fill_failure_counts": dict(sorted(all_fill_failures.items())),
        "candidate_comparisons": candidate_comparisons[: max(top_n * 4, top_n)],
        "symbol_leaders": sorted(
            symbol_leaders,
            key=lambda row: (
                row.get("promotion_status") == "eligible_for_promotion_review",
                _float(row.get("research_score")),
                _float(row.get("min_fill_coverage")),
            ),
            reverse=True,
        )[: max(top_n * 2, top_n)],
        "next_step_contract": [
            "Use this comparison to choose the next research lane; do not promote directly from it.",
            "If dense or broader contract universes improve fill, make that data foundation canonical for brute force.",
            "If selected-contract gaps persist, expand contract universe before judging strategy economics.",
            "If entry/exit timing gaps dominate after dense coverage, redesign strategy timing or quarantine the family.",
            "Do not modify live manifests, risk policy, or broker-facing strategy selection from this packet alone.",
        ],
    }
    packet["decision"] = _decision(packet)
    json_path = output_dir / "research_fill_experiment_comparison.json"
    md_path = output_dir / "research_fill_experiment_comparison.md"
    json_path.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")
    _write_markdown(md_path, packet)
    return packet


def main() -> None:
    args = parse_args()
    inputs = [_parse_input(value) for value in args.input]
    packet = build_research_fill_experiment_comparison(
        inputs=inputs,
        output_dir=Path(args.output_dir),
        baseline_label=args.baseline_label,
        fill_coverage_gate=args.fill_coverage_gate,
        top_n=args.top_n,
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_research_portfolio_report import (
    _blocker_counts,
    _data_repair_candidates,
    _eligible_regimes,
    _fill_failure_reason,
    _fill_failure_counts,
    _float,
    _normalize_required_regimes,
    _regime_summary,
    _strategy_redesign_candidates,
    build_capital_plan,
)
from scripts.build_research_promotion_review_packet import (
    build_research_promotion_review_packet,
)

DEFAULT_SEARCH_PATTERN = "research_portfolio_report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a research-only wave-level portfolio rollup from shard-level "
            "research_portfolio_report.json files."
        )
    )
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--pattern", default=DEFAULT_SEARCH_PATTERN)
    parser.add_argument("--fill-coverage-gate", type=float, default=0.90)
    parser.add_argument("--min-option-trades", type=int, default=20)
    parser.add_argument("--min-test-net-pnl", type=float, default=0.0)
    parser.add_argument("--max-positions", type=int, default=8)
    parser.add_argument("--max-strategies-per-symbol", type=int, default=2)
    parser.add_argument("--max-symbol-weight", type=float, default=0.25)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--max-review-candidates", type=int, default=20)
    parser.add_argument(
        "--required-regimes",
        default="bull,bear,choppy",
        help=(
            "Comma-separated regime set required for regime-complete rollup "
            "summary. This does not change candidate-level promotion gates."
        ),
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def discover_portfolio_reports(report_root: Path, pattern: str) -> list[Path]:
    if report_root.is_file():
        return [report_root]
    return sorted(path for path in report_root.rglob(pattern) if path.is_file())


def _candidate_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("promotion_status") == "eligible_for_promotion_review",
        _float(row.get("min_fill_coverage")),
        _float(row.get("min_test_net_pnl")),
        _float(row.get("min_net_pnl")),
        _float(row.get("research_score")),
    )


def _dedupe_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        candidate_id = str(row.get("candidate_variant_id") or "")
        if candidate_id:
            grouped[candidate_id].append(row)

    deduped: list[dict[str, Any]] = []
    for candidate_id, group in grouped.items():
        # Keep the best complete shard view, but retain all source report paths for audit.
        selected = sorted(group, key=_candidate_sort_key, reverse=True)[0].copy()
        source_reports = sorted(
            {
                str(row.get("source_report_path"))
                for row in group
                if str(row.get("source_report_path") or "")
            }
        )
        selected["source_report_paths"] = source_reports
        selected["source_report_count"] = len(source_reports)
        selected["candidate_variant_id"] = candidate_id
        deduped.append(selected)
    return sorted(
        deduped,
        key=lambda row: (
            row.get("promotion_status") == "eligible_for_promotion_review",
            _float(row.get("research_score")),
            _float(row.get("min_test_net_pnl")),
            _float(row.get("min_fill_coverage")),
        ),
        reverse=True,
    )


def _load_candidates(
    report_paths: list[Path], *, fill_coverage_gate: float
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    source_reports: list[dict[str, Any]] = []
    for path in report_paths:
        packet = _load_json(path)
        source_reports.append(
            {
                "path": str(path),
                "candidate_count": int(packet.get("candidate_count") or 0),
                "eligible_for_promotion_review_count": int(
                    packet.get("eligible_for_promotion_review_count") or 0
                ),
                "promotion_allowed": bool(packet.get("promotion_allowed")),
                "broker_facing": bool(packet.get("broker_facing")),
                "live_manifest_effect": packet.get("live_manifest_effect"),
                "risk_policy_effect": packet.get("risk_policy_effect"),
            }
        )
        for item in packet.get("top_candidates", []):
            if not isinstance(item, dict):
                continue
            row = item.copy()
            row["source_report_path"] = str(path)
            if not row.get("fill_failure_reason"):
                row["fill_failure_reason"] = _fill_failure_reason(row, fill_coverage_gate)
            rows.append(row)
    return _dedupe_candidates(rows), source_reports


def _symbol_summary(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in candidates:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        item = by_symbol.setdefault(
            symbol,
            {
                "symbol": symbol,
                "candidate_count": 0,
                "eligible_for_promotion_review_count": 0,
                "best_research_score": None,
                "best_min_net_pnl": None,
                "best_min_test_net_pnl": None,
                "best_min_fill_coverage": None,
                "fill_failure_counts": Counter(),
            },
        )
        item["candidate_count"] += 1
        if row.get("promotion_status") == "eligible_for_promotion_review":
            item["eligible_for_promotion_review_count"] += 1
        item["fill_failure_counts"][str(row.get("fill_failure_reason") or "unknown")] += 1
        if item["best_research_score"] is None or _float(row.get("research_score")) > _float(
            item["best_research_score"]
        ):
            item["best_research_score"] = row.get("research_score")
            item["best_min_net_pnl"] = row.get("min_net_pnl")
            item["best_min_test_net_pnl"] = row.get("min_test_net_pnl")
            item["best_min_fill_coverage"] = row.get("min_fill_coverage")

    result: list[dict[str, Any]] = []
    for item in by_symbol.values():
        item["fill_failure_counts"] = dict(sorted(item["fill_failure_counts"].items()))
        result.append(item)
    return sorted(
        result,
        key=lambda row: (
            row["eligible_for_promotion_review_count"],
            _float(row.get("best_research_score")),
        ),
        reverse=True,
    )


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Research Wave Portfolio Rollup",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Status: `{packet['status']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Candidate-level decision: `{packet.get('candidate_level_decision')}`",
        f"- Source report count: `{packet['source_report_count']}`",
        f"- Candidate count: `{packet['candidate_count']}`",
        f"- Eligible count: `{packet['eligible_for_promotion_review_count']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Live manifest effect: `{packet['live_manifest_effect']}`",
        f"- Risk policy effect: `{packet['risk_policy_effect']}`",
        "",
        "## Capital Plan",
        "",
    ]
    if not packet["capital_plan"]:
        lines.append("- No research-only capital plan candidates passed the rollup filters.")
    for row in packet["capital_plan"]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"weight `{row['research_only_weight']:.2%}` dollars `${row['research_only_dollars']}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"fill `{row['min_fill_coverage']}` status `{row['promotion_status']}`"
        )

    lines.extend(["", "## Top Candidates", ""])
    for row in packet["top_candidates"][:25]:
        blockers = ", ".join(str(item) for item in row.get("promotion_blockers", [])) or "none"
        lines.append(
            "- "
            f"`{row.get('symbol')}` `{row.get('candidate_variant_id')}` "
            f"score `{row.get('research_score')}` min_net `{row.get('min_net_pnl')}` "
            f"min_test `{row.get('min_test_net_pnl')}` "
            f"fill `{row.get('min_fill_coverage')}-{row.get('max_fill_coverage')}` "
            f"blockers `{blockers}`"
        )

    lines.extend(["", "## Fill Failure Counts", ""])
    for reason, count in packet["fill_failure_counts"].items():
        lines.append(f"- `{reason}`: `{count}`")

    lines.extend(["", "## Regime Completeness", ""])
    lines.append(f"- Required regimes: `{', '.join(packet['required_regimes'])}`")
    lines.append(f"- Eligible regimes: `{', '.join(packet['eligible_regimes']) or 'none'}`")
    lines.append(
        f"- Missing eligible regimes: `{', '.join(packet['missing_eligible_regimes']) or 'none'}`"
    )
    lines.append(
        f"- Regime complete for promotion review: `{packet['regime_complete_for_promotion_review']}`"
    )
    for row in packet["regime_summary"]:
        lines.append(
            "- "
            f"`{row['intended_regime']}` candidates `{row['candidate_count']}` "
            f"eligible `{row['eligible_for_promotion_review_count']}` "
            f"best `{row.get('best_candidate_variant_id')}` "
            f"best_fill `{row.get('best_min_fill_coverage')}` "
            f"best_status `{row.get('best_promotion_status')}`"
        )

    lines.extend(["", "## Symbol Summary", ""])
    for row in packet["symbol_summary"][:25]:
        lines.append(
            "- "
            f"`{row['symbol']}` candidates `{row['candidate_count']}` "
            f"eligible `{row['eligible_for_promotion_review_count']}` "
            f"best_score `{row['best_research_score']}` "
            f"best_fill `{row['best_min_fill_coverage']}`"
        )

    lines.extend(["", "## Data Repair Priority", ""])
    if not packet["data_repair_priority_candidates"]:
        lines.append("- No positive-economics data-repair candidates selected.")
    for row in packet["data_repair_priority_candidates"]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"fill `{row['min_fill_coverage']}-{row['max_fill_coverage']}` "
            f"reason `{row['fill_failure_reason']}` score `{row['research_score']}`"
        )

    lines.extend(["", "## Strategy Redesign Priority", ""])
    if not packet["strategy_redesign_candidates"]:
        lines.append("- No strategy-redesign candidates selected.")
    for row in packet["strategy_redesign_candidates"]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"reason `{row['fill_failure_reason']}`"
        )

    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_research_wave_portfolio_rollup(
    *,
    report_root: Path,
    output_dir: Path,
    pattern: str = DEFAULT_SEARCH_PATTERN,
    fill_coverage_gate: float,
    min_option_trades: int,
    min_test_net_pnl: float,
    max_positions: int,
    max_strategies_per_symbol: int,
    max_symbol_weight: float,
    initial_cash: float,
    max_review_candidates: int,
    required_regimes: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_paths = discover_portfolio_reports(report_root, pattern)
    candidates, source_reports = _load_candidates(
        report_paths, fill_coverage_gate=fill_coverage_gate
    )
    capital_plan = build_capital_plan(
        candidates,
        max_positions=max_positions,
        max_strategies_per_symbol=max_strategies_per_symbol,
        max_symbol_weight=max_symbol_weight,
        initial_cash=initial_cash,
        min_option_trades=min_option_trades,
    )
    eligible_count = sum(
        1 for row in candidates if row.get("promotion_status") == "eligible_for_promotion_review"
    )
    normalized_required_regimes = _normalize_required_regimes(required_regimes)
    regime_summary = _regime_summary(
        candidates, required_regimes=normalized_required_regimes
    )
    eligible_regimes = _eligible_regimes(regime_summary)
    missing_eligible_regimes = [
        regime for regime in normalized_required_regimes if regime not in set(eligible_regimes)
    ]
    allocated_weight = round(sum(row["research_only_weight"] for row in capital_plan), 6)
    candidate_level_decision = (
        "ready_for_governed_validation_review"
        if eligible_count > 0
        else "research_only_blocked"
    )
    decision = candidate_level_decision
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "research_wave_portfolio_rollup_complete",
        "decision": decision,
        "candidate_level_decision": candidate_level_decision,
        "promotion_allowed": eligible_count > 0,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "report_root": str(report_root),
        "source_report_count": len(report_paths),
        "source_reports": source_reports,
        "candidate_count": len(candidates),
        "eligible_for_promotion_review_count": eligible_count,
        "fill_coverage_gate": fill_coverage_gate,
        "min_option_trades": min_option_trades,
        "min_test_net_pnl": min_test_net_pnl,
        "max_positions": max_positions,
        "max_strategies_per_symbol": max_strategies_per_symbol,
        "max_symbol_weight": max_symbol_weight,
        "initial_cash": initial_cash,
        "capital_plan": capital_plan,
        "capital_plan_allocated_weight": allocated_weight,
        "capital_plan_unallocated_weight": round(max(1.0 - allocated_weight, 0.0), 6),
        "capital_plan_unallocated_dollars": round(
            max(1.0 - allocated_weight, 0.0) * initial_cash, 2
        ),
        "top_candidates": candidates[:100],
        "blocker_counts": _blocker_counts(candidates),
        "fill_failure_counts": _fill_failure_counts(candidates),
        "symbol_summary": _symbol_summary(candidates),
        "required_regimes": normalized_required_regimes,
        "regime_summary": regime_summary,
        "eligible_regimes": eligible_regimes,
        "missing_eligible_regimes": missing_eligible_regimes,
        "regime_complete_for_promotion_review": not missing_eligible_regimes,
        "promotion_allowed_regime_complete": eligible_count > 0,
        "regime_completeness_policy": "informational_only_not_a_hard_promotion_gate",
        "data_repair_priority_candidates": _data_repair_candidates(candidates, max_items=20),
        "strategy_redesign_candidates": _strategy_redesign_candidates(candidates, max_items=20),
        "next_step_contract": [
            "Treat this rollup as research-only until governed promotion review and broker-audited paper evidence are complete.",
            "Promote only candidates that keep fill coverage at or above 0.90 across the required replay stack.",
            "Treat missing bull/bear/choppy regimes as follow-up research targets, not as a hard blocker for otherwise eligible sleeves.",
            "When selected-contract gaps dominate, rerun candidates through dense daily liquid-contract universes before strategy redesign.",
            "When entry/exit timing gaps dominate despite dense coverage, redesign exits or quarantine the strategy family.",
            "Do not modify live manifests, strategy selection, or risk policy from this rollup alone.",
        ],
    }
    rollup_json = output_dir / "research_wave_portfolio_rollup.json"
    rollup_md = output_dir / "research_wave_portfolio_rollup.md"
    rollup_json.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")
    _write_markdown(rollup_md, packet)
    build_research_promotion_review_packet(
        portfolio_report_json=rollup_json,
        output_dir=output_dir / "promotion_review_packet",
        max_review_candidates=max_review_candidates,
    )
    return packet


def main() -> None:
    args = parse_args()
    packet = build_research_wave_portfolio_rollup(
        report_root=Path(args.report_root),
        output_dir=Path(args.output_dir),
        pattern=args.pattern,
        fill_coverage_gate=args.fill_coverage_gate,
        min_option_trades=args.min_option_trades,
        min_test_net_pnl=args.min_test_net_pnl,
        max_positions=args.max_positions,
        max_strategies_per_symbol=args.max_strategies_per_symbol,
        max_symbol_weight=args.max_symbol_weight,
        initial_cash=args.initial_cash,
        max_review_candidates=args.max_review_candidates,
        required_regimes=args.required_regimes.split(","),
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

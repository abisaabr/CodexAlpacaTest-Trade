from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a research-only promotion-review packet from a portfolio report."
        )
    )
    parser.add_argument("--portfolio-report-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-review-candidates", type=int, default=20)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _candidate_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_variant_id": row.get("candidate_variant_id"),
        "base_candidate_variant_id": row.get("base_candidate_variant_id"),
        "candidate_identity_mode": row.get("candidate_identity_mode"),
        "aggregate_profile": row.get("aggregate_profile"),
        "symbol": row.get("symbol"),
        "strategy_id": row.get("strategy_id") or row.get("source_strategy_id"),
        "source_strategy_id": row.get("source_strategy_id"),
        "family": row.get("family"),
        "intended_regime": row.get("intended_regime"),
        "parameter_set": row.get("parameter_set"),
        "directional_option_type": row.get("directional_option_type"),
        "research_score": row.get("research_score"),
        "min_net_pnl": row.get("min_net_pnl"),
        "min_test_net_pnl": row.get("min_test_net_pnl"),
        "min_fill_coverage": row.get("min_fill_coverage"),
        "max_fill_coverage": row.get("max_fill_coverage"),
        "min_strategy_fill_coverage": row.get(
            "min_strategy_fill_coverage", row.get("min_fill_coverage")
        ),
        "max_strategy_fill_coverage": row.get(
            "max_strategy_fill_coverage", row.get("max_fill_coverage")
        ),
        "min_data_foundation_coverage": row.get("min_data_foundation_coverage"),
        "min_entry_bar_coverage": row.get("min_entry_bar_coverage"),
        "min_exit_bar_coverage": row.get("min_exit_bar_coverage"),
        "fill_coverage_unit": row.get("fill_coverage_unit"),
        "min_option_trade_count": row.get("min_option_trade_count"),
        "worst_drawdown": row.get("worst_drawdown"),
        "promotion_status": row.get("promotion_status"),
        "promotion_blockers": row.get("promotion_blockers", []),
    }


def _base_candidate_id(row: dict[str, Any]) -> str:
    return str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or "")


def _dedupe_review_candidates(rows: list[dict[str, Any]], max_items: int) -> list[dict[str, Any]]:
    review_candidates: list[dict[str, Any]] = []
    seen_base_ids: set[str] = set()
    for row in rows:
        if row.get("promotion_status") != "eligible_for_promotion_review":
            continue
        base_id = _base_candidate_id(row)
        if base_id in seen_base_ids:
            continue
        seen_base_ids.add(base_id)
        review_candidates.append(_candidate_summary(row))
        if len(review_candidates) >= max_items:
            break
    return review_candidates


def _blocker_counts(candidates: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in candidates:
        blockers = row.get("promotion_blockers") or []
        if isinstance(blockers, str):
            blockers = [blockers]
        counts.update(str(blocker) for blocker in blockers if str(blocker))
    return dict(sorted(counts.items()))


def _symbol_exposure(capital_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exposure: dict[str, dict[str, Any]] = {}
    for row in capital_plan:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        item = exposure.setdefault(
            symbol,
            {
                "symbol": symbol,
                "strategy_count": 0,
                "research_only_weight": 0.0,
                "research_only_dollars": 0.0,
                "candidate_variant_ids": [],
            },
        )
        item["strategy_count"] += 1
        item["research_only_weight"] += _float(row.get("research_only_weight"))
        item["research_only_dollars"] += _float(row.get("research_only_dollars"))
        item["candidate_variant_ids"].append(row.get("candidate_variant_id"))
    result = []
    for item in exposure.values():
        item["research_only_weight"] = round(item["research_only_weight"], 6)
        item["research_only_dollars"] = round(item["research_only_dollars"], 2)
        result.append(item)
    return sorted(result, key=lambda row: row["research_only_weight"], reverse=True)


def _repair_targets(
    candidates: list[dict[str, Any]],
    *,
    max_targets: int,
) -> list[dict[str, Any]]:
    blocked = [
        row
        for row in candidates
        if row.get("promotion_status") != "eligible_for_promotion_review"
        and row.get("promotion_blockers")
    ]
    blocked = sorted(
        blocked,
        key=lambda row: (
            "fill_coverage" in ",".join(str(item) for item in row.get("promotion_blockers", [])),
            _float(row.get("research_score")),
            _float(row.get("min_net_pnl")),
        ),
        reverse=True,
    )
    return [_candidate_summary(row) for row in blocked[:max_targets]]


def _next_actions(packet: dict[str, Any]) -> list[str]:
    if packet["decision"] == "ready_for_governed_validation_review":
        return [
            "Review promotion-review candidates against the strategy-governance policy before any activation discussion.",
            "Require a clean broker-audited paper session before control-plane promotion beyond research review.",
            "Do not modify live manifests, strategy selection, or risk policy from this packet alone.",
        ]
    return [
        "Separate raw data repair from strategy/replay redesign before rerunning blocked candidates.",
        "For strong data-foundation but low strategy-fill candidates, redesign entry timing, exit timing, and option structure rather than downloading more raw bars first.",
        "Rerun option-aware stress replay after the targeted repair or redesign.",
        "Keep all candidates research-only until promotion-review gates pass.",
    ]


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Research Promotion Review Packet",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Promotion scope: `{packet['promotion_scope']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Candidate count: `{packet['gate_summary']['candidate_count']}`",
        f"- Eligible count: `{packet['gate_summary']['eligible_for_promotion_review_count']}`",
        f"- Top-candidate count: `{packet['gate_summary']['top_candidate_count']}`",
        f"- Blocker count scope: `{packet['gate_summary']['blocker_count_scope']}`",
        f"- Fill coverage unit: `{packet['gate_summary'].get('fill_coverage_unit')}`",
        f"- Fill coverage semantics: {packet['gate_summary'].get('fill_coverage_semantics')}",
        f"- Capital allocated weight: `{packet['gate_summary']['capital_plan_allocated_weight']}`",
        f"- Capital unallocated dollars: `${packet['gate_summary']['capital_plan_unallocated_dollars']}`",
        "",
        "## Review Candidates",
        "",
    ]
    if not packet["review_candidates"]:
        lines.append("- No candidates are eligible for governed promotion review.")
    for row in packet["review_candidates"]:
        blockers = ", ".join(row.get("promotion_blockers", [])) or "none"
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"family `{row.get('family') or 'unknown'}` "
            f"regime `{row.get('intended_regime') or 'unknown'}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"strategy_fill `{row['min_fill_coverage']}` "
            f"data_foundation `{row.get('min_data_foundation_coverage')}` "
            f"blockers `{blockers}`"
        )
    lines.extend(["", "## Symbol Exposure", ""])
    if not packet["symbol_exposure"]:
        lines.append("- No research-only capital exposure is proposed.")
    for row in packet["symbol_exposure"]:
        lines.append(
            "- "
            f"`{row['symbol']}` strategies `{row['strategy_count']}` "
            f"weight `{row['research_only_weight']:.2%}` "
            f"dollars `${row['research_only_dollars']}`"
        )
    lines.extend(["", "## Blocker Counts", ""])
    if not packet["blocker_counts"]:
        lines.append("- No blockers found in the configured blocker-count scope.")
    for blocker, count in packet["blocker_counts"].items():
        lines.append(f"- `{blocker}`: `{count}`")
    if packet.get("top_candidate_blocker_counts") != packet.get("blocker_counts"):
        lines.extend(["", "Top-candidate blocker counts:", ""])
        if not packet.get("top_candidate_blocker_counts"):
            lines.append("- No blockers found in the top-candidate set.")
        for blocker, count in packet["top_candidate_blocker_counts"].items():
            lines.append(f"- `{blocker}`: `{count}`")
    lines.extend(["", "## Data Repair Targets", ""])
    if not packet["data_repair_targets"]:
        lines.append("- No data repair targets selected.")
    for row in packet["data_repair_targets"]:
        blockers = ", ".join(row.get("promotion_blockers", [])) or "none"
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"family `{row.get('family') or 'unknown'}` "
            f"regime `{row.get('intended_regime') or 'unknown'}` "
            f"score `{row['research_score']}` blockers `{blockers}`"
        )
    lines.extend(["", "## Strategy Redesign Targets", ""])
    if not packet.get("strategy_redesign_targets"):
        lines.append("- No strategy redesign targets selected.")
    for row in packet.get("strategy_redesign_targets", []):
        blockers = ", ".join(row.get("promotion_blockers", [])) or "none"
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"family `{row.get('family') or 'unknown'}` "
            f"regime `{row.get('intended_regime') or 'unknown'}` "
            f"strategy_fill `{row.get('min_strategy_fill_coverage')}` "
            f"data_foundation `{row.get('min_data_foundation_coverage')}` "
            f"blockers `{blockers}`"
        )
    lines.extend(["", "## Next Actions", ""])
    for item in packet["next_actions"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_research_promotion_review_packet(
    *,
    portfolio_report_json: Path,
    output_dir: Path,
    max_review_candidates: int = 20,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    source = _load_json(portfolio_report_json)
    top_candidates = [
        item for item in source.get("top_candidates", []) if isinstance(item, dict)
    ]
    capital_plan = [item for item in source.get("capital_plan", []) if isinstance(item, dict)]
    full_blocker_counts = source.get("blocker_counts")
    if not isinstance(full_blocker_counts, dict):
        full_blocker_counts = _blocker_counts(top_candidates)
    top_candidate_blocker_counts = _blocker_counts(top_candidates)
    source_data_repair_targets = [
        _candidate_summary(item)
        for item in source.get("data_repair_priority_candidates", [])
        if isinstance(item, dict)
    ]
    source_has_data_repair_targets = "data_repair_priority_candidates" in source
    source_strategy_redesign_targets = [
        _candidate_summary(item)
        for item in source.get("strategy_redesign_candidates", [])
        if isinstance(item, dict)
    ]
    review_candidates = _dedupe_review_candidates(top_candidates, max_review_candidates)
    eligible_count = int(source.get("eligible_for_promotion_review_count") or 0)
    unique_eligible_base_count = len(
        {
            _base_candidate_id(row)
            for row in top_candidates
            if row.get("promotion_status") == "eligible_for_promotion_review"
        }
    )
    decision = (
        "ready_for_governed_validation_review"
        if unique_eligible_base_count > 0 and review_candidates
        else "research_only_blocked"
    )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "research_promotion_review_packet_complete",
        "decision": decision,
        "promotion_scope": "research_governed_validation_review_only",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "source_portfolio_report_json": str(portfolio_report_json),
        "source_replay_root": source.get("replay_root"),
        "gate_summary": {
            "promotion_allowed_from_source_report": bool(source.get("promotion_allowed")),
            "candidate_count": int(source.get("candidate_count") or len(top_candidates)),
            "eligible_for_promotion_review_count": eligible_count,
            "unique_eligible_base_candidate_count": unique_eligible_base_count,
            "fill_coverage_gate": source.get("fill_coverage_gate"),
            "strategy_fill_coverage_gate": source.get(
                "strategy_fill_coverage_gate", source.get("fill_coverage_gate")
            ),
            "fill_coverage_unit": source.get("fill_coverage_unit"),
            "fill_coverage_semantics": source.get("fill_coverage_semantics"),
            "candidate_identity_mode": source.get("candidate_identity_mode"),
            "min_option_trades": source.get("min_option_trades"),
            "min_test_net_pnl": source.get("min_test_net_pnl"),
            "capital_plan_allocated_weight": source.get("capital_plan_allocated_weight"),
            "capital_plan_unallocated_weight": source.get("capital_plan_unallocated_weight"),
            "capital_plan_unallocated_dollars": source.get(
                "capital_plan_unallocated_dollars"
            ),
            "top_candidate_count": len(top_candidates),
            "blocker_count_scope": (
                "full_candidate_population"
                if isinstance(source.get("blocker_counts"), dict)
                else "top_candidates_only"
            ),
        },
        "portfolio_constraints": {
            "initial_cash": source.get("initial_cash"),
            "max_positions": source.get("max_positions"),
            "max_strategies_per_symbol": source.get("max_strategies_per_symbol"),
            "max_symbol_weight": source.get("max_symbol_weight"),
        },
        "review_candidates": review_candidates,
        "capital_plan": capital_plan,
        "symbol_exposure": _symbol_exposure(capital_plan),
        "blocker_counts": full_blocker_counts,
        "top_candidate_blocker_counts": top_candidate_blocker_counts,
        "data_repair_targets": (
            source_data_repair_targets
            if source_has_data_repair_targets
            else _repair_targets(top_candidates, max_targets=max_review_candidates)
        ),
        "strategy_redesign_targets": source_strategy_redesign_targets,
        "hard_rules": [
            "This packet is not broker-facing.",
            "This packet does not authorize live manifest changes.",
            "This packet does not change strategy selection or risk policy.",
            "Promotion here means research/governed-validation review only.",
        ],
    }
    packet["next_actions"] = _next_actions(packet)

    json_path = output_dir / "research_promotion_review_packet.json"
    md_path = output_dir / "research_promotion_review_packet.md"
    json_path.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")
    _write_markdown(md_path, packet)
    return packet


def main() -> None:
    args = parse_args()
    packet = build_research_promotion_review_packet(
        portfolio_report_json=Path(args.portfolio_report_json),
        output_dir=Path(args.output_dir),
        max_review_candidates=args.max_review_candidates,
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

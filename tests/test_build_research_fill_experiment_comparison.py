from __future__ import annotations

import json
from pathlib import Path

from scripts.build_research_fill_experiment_comparison import (
    build_research_fill_experiment_comparison,
)


def _write_rollup(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _candidate(candidate_id: str, *, symbol: str, fill: float, score: float = 1000.0) -> dict:
    return {
        "candidate_variant_id": candidate_id,
        "symbol": symbol,
        "research_score": score,
        "min_net_pnl": 1000.0,
        "min_test_net_pnl": 100.0,
        "min_fill_coverage": fill,
        "max_fill_coverage": fill,
        "promotion_status": (
            "eligible_for_promotion_review" if fill >= 0.90 else "research_only_blocked"
        ),
        "promotion_blockers": [] if fill >= 0.90 else ["fill_coverage_below_0.90"],
        "fill_failure_reason": (
            "fill_gate_clear" if fill >= 0.90 else "selected_contract_universe_gap"
        ),
    }


def test_fill_experiment_comparison_tracks_lane_summaries_and_deltas(tmp_path: Path) -> None:
    sparse = tmp_path / "sparse.json"
    dense = tmp_path / "dense.json"
    _write_rollup(
        sparse,
        {
            "decision": "research_only_blocked",
            "source_report_count": 2,
            "candidate_count": 2,
            "eligible_for_promotion_review_count": 0,
            "fill_failure_counts": {"selected_contract_universe_gap": 2},
            "top_candidates": [
                _candidate("a", symbol="AAPL", fill=0.20),
                _candidate("b", symbol="MSFT", fill=0.40),
            ],
        },
    )
    _write_rollup(
        dense,
        {
            "decision": "ready_for_governed_validation_review",
            "source_report_count": 2,
            "candidate_count": 2,
            "eligible_for_promotion_review_count": 1,
            "fill_failure_counts": {"fill_gate_clear": 1, "selected_contract_universe_gap": 1},
            "top_candidates": [
                _candidate("a", symbol="AAPL", fill=0.92, score=2000.0),
                _candidate("b", symbol="MSFT", fill=0.55),
            ],
        },
    )

    packet = build_research_fill_experiment_comparison(
        inputs=[("sparse", sparse), ("dense", dense)],
        output_dir=tmp_path / "out",
        baseline_label="sparse",
        fill_coverage_gate=0.90,
        top_n=10,
    )

    assert packet["decision"] == "ready_for_governed_validation_review"
    assert packet["lane_summaries"][1]["eligible_for_promotion_review_count"] == 1
    assert packet["candidate_comparisons"][0]["candidate_variant_id"] == "a"
    assert packet["candidate_comparisons"][0]["fill_delta"] == 0.72
    assert packet["candidate_comparisons"][0]["crosses_fill_gate"] is True
    assert (tmp_path / "out" / "research_fill_experiment_comparison.json").exists()
    assert (tmp_path / "out" / "research_fill_experiment_comparison.md").exists()


def test_fill_experiment_comparison_recommends_dense_repair_for_universe_gaps(
    tmp_path: Path,
) -> None:
    rollup = tmp_path / "atm.json"
    _write_rollup(
        rollup,
        {
            "decision": "research_only_blocked",
            "candidate_count": 1,
            "eligible_for_promotion_review_count": 0,
            "fill_failure_counts": {"selected_contract_universe_gap": 1},
            "top_candidates": [_candidate("a", symbol="AAPL", fill=0.10)],
        },
    )

    packet = build_research_fill_experiment_comparison(
        inputs=[("atm", rollup)],
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        top_n=10,
    )

    assert packet["decision"] == "continue_dense_or_broader_contract_universe_repair"

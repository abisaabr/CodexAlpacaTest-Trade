from __future__ import annotations

import json
from pathlib import Path

from scripts.analyze_option_fill_failures import build_report


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_fill_failure_diagnostic_preserves_gate_and_identity_context(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate_summary.json"
    failures_path = tmp_path / "fill_failures.json"
    _write_json(
        candidate_path,
        [
            {
                "candidate_variant_id": "iwm_candidate__profile_strict",
                "base_candidate_variant_id": "iwm_candidate",
                "candidate_identity_mode": "variant_profile",
                "aggregate_profile": "strict",
                "strategy_id": "iwm__choppy__put__debit_put_vertical",
                "source_strategy_id": "iwm__choppy__put__debit_put_vertical",
                "symbol": "IWM",
                "family": "debit_put_vertical",
                "intended_regime": "choppy",
                "net_pnl": 1000.0,
                "test_net_pnl": 100.0,
                "strategy_fill_coverage": 0.75,
                "option_trade_count": 25,
            }
        ],
    )
    _write_json(
        failures_path,
        [
            {
                "candidate_variant_id": "iwm_candidate__profile_strict",
                "failure_reason": "no_entry_bar",
                "trade_date": "2026-01-02",
                "option_structure": "debit_put_vertical",
                "entry_lookup_mode": "first_bar_at_or_after_entry_within_lag",
                "exit_lookup_mode": "first_bar_at_or_after_exit_within_lag",
                "contract_selection_method": "entry_liquidity_first_research_only",
            },
            {
                "candidate_variant_id": "iwm_candidate__profile_strict",
                "failure_reason": "no_selected_contract",
                "trade_date": "2026-01-03",
                "option_structure": "debit_put_vertical",
                "entry_lookup_mode": "first_bar_at_or_after_entry_within_lag",
                "exit_lookup_mode": "first_bar_at_or_after_exit_within_lag",
                "contract_selection_method": "entry_liquidity_first_research_only",
            },
        ],
    )

    report = build_report(
        candidate_summary_json=candidate_path,
        fill_failures_json=failures_path,
        output_dir=tmp_path / "out",
        candidate_variant_id="iwm_candidate__profile_strict",
        report_id="iwm_diag",
    )

    assert report["diagnostic_scope"] == "fill_failure_counts_only_not_promotion_equivalent"
    assert report["candidate"]["base_candidate_variant_id"] == "iwm_candidate"
    assert report["promotion_gate_context"]["diagnostic_only_not_promotion_equivalent"] is True
    assert report["promotion_gate_context"]["basic_gate_blockers"] == [
        "fill_coverage_below_0.90"
    ]
    assert report["dominant_classification"] == [
        "selected_contract_universe_gap",
        "entry_bar_gap_or_entry_timing_mismatch",
    ]
    assert (tmp_path / "out" / "iwm_diag.json").exists()
    assert "Diagnostic only" in (tmp_path / "out" / "iwm_diag.md").read_text(
        encoding="utf-8"
    )

from __future__ import annotations

import json
from pathlib import Path

from scripts.build_research_promotion_review_packet import (
    build_research_promotion_review_packet,
)


def _write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_promotion_review_packet_marks_eligible_research_candidates(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "research_portfolio_report.json"
    _write_report(
        report_path,
        {
            "promotion_allowed": True,
            "candidate_count": 3,
            "eligible_for_promotion_review_count": 2,
            "fill_coverage_gate": 0.90,
            "min_option_trades": 20,
            "min_test_net_pnl": 0,
            "capital_plan_allocated_weight": 0.75,
            "capital_plan_unallocated_weight": 0.25,
            "capital_plan_unallocated_dollars": 6_250.0,
            "initial_cash": 25_000.0,
            "max_positions": 12,
            "max_strategies_per_symbol": 2,
            "max_symbol_weight": 0.25,
            "capital_plan": [
                {
                    "candidate_variant_id": "amd_a",
                    "symbol": "AMD",
                    "source_strategy_id": "amd_strategy",
                    "directional_option_type": "call",
                    "research_only_weight": 0.25,
                    "research_only_dollars": 6_250.0,
                    "min_net_pnl": 1_000.0,
                    "min_test_net_pnl": 100.0,
                    "min_fill_coverage": 0.92,
                    "promotion_status": "eligible_for_promotion_review",
                },
                {
                    "candidate_variant_id": "amd_b",
                    "symbol": "AMD",
                    "source_strategy_id": "amd_strategy_b",
                    "directional_option_type": "call",
                    "research_only_weight": 0.25,
                    "research_only_dollars": 6_250.0,
                    "min_net_pnl": 900.0,
                    "min_test_net_pnl": 90.0,
                    "min_fill_coverage": 0.91,
                    "promotion_status": "eligible_for_promotion_review",
                },
                {
                    "candidate_variant_id": "msft_a",
                    "symbol": "MSFT",
                    "source_strategy_id": "msft_strategy",
                    "directional_option_type": "call",
                    "research_only_weight": 0.25,
                    "research_only_dollars": 6_250.0,
                    "min_net_pnl": 800.0,
                    "min_test_net_pnl": 80.0,
                    "min_fill_coverage": 0.93,
                    "promotion_status": "eligible_for_promotion_review",
                },
            ],
            "top_candidates": [
                {
                    "candidate_variant_id": "amd_a",
                    "symbol": "AMD",
                    "source_strategy_id": "amd_strategy",
                    "directional_option_type": "call",
                    "research_score": 10_000.0,
                    "min_net_pnl": 1_000.0,
                    "min_test_net_pnl": 100.0,
                    "min_fill_coverage": 0.92,
                    "max_fill_coverage": 0.95,
                    "min_option_trade_count": 30,
                    "worst_drawdown": -200.0,
                    "promotion_status": "eligible_for_promotion_review",
                    "promotion_blockers": [],
                },
                {
                    "candidate_variant_id": "msft_a",
                    "symbol": "MSFT",
                    "source_strategy_id": "msft_strategy",
                    "directional_option_type": "call",
                    "research_score": 9_000.0,
                    "min_net_pnl": 800.0,
                    "min_test_net_pnl": 80.0,
                    "min_fill_coverage": 0.93,
                    "max_fill_coverage": 0.94,
                    "min_option_trade_count": 25,
                    "worst_drawdown": -150.0,
                    "promotion_status": "eligible_for_promotion_review",
                    "promotion_blockers": [],
                },
                {
                    "candidate_variant_id": "nvda_blocked",
                    "symbol": "NVDA",
                    "source_strategy_id": "nvda_strategy",
                    "directional_option_type": "call",
                    "research_score": 8_000.0,
                    "min_net_pnl": 700.0,
                    "min_test_net_pnl": 70.0,
                    "min_fill_coverage": 0.80,
                    "max_fill_coverage": 0.88,
                    "min_option_trade_count": 40,
                    "worst_drawdown": -100.0,
                    "promotion_status": "research_only_blocked",
                    "promotion_blockers": ["fill_coverage_below_0.90"],
                },
            ],
        },
    )

    packet = build_research_promotion_review_packet(
        portfolio_report_json=report_path,
        output_dir=tmp_path / "out",
        max_review_candidates=10,
    )

    assert packet["decision"] == "ready_for_governed_validation_review"
    assert packet["broker_facing"] is False
    assert packet["live_manifest_effect"] == "none"
    assert packet["risk_policy_effect"] == "none"
    assert packet["promotion_scope"] == "research_governed_validation_review_only"
    assert len(packet["review_candidates"]) == 2
    assert packet["blocker_counts"] == {"fill_coverage_below_0.90": 1}
    assert packet["symbol_exposure"][0]["symbol"] == "AMD"
    assert packet["symbol_exposure"][0]["strategy_count"] == 2
    assert packet["symbol_exposure"][0]["research_only_weight"] == 0.5
    assert (tmp_path / "out" / "research_promotion_review_packet.json").exists()
    assert (tmp_path / "out" / "research_promotion_review_packet.md").exists()


def test_promotion_review_packet_blocks_when_no_candidate_is_eligible(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "research_portfolio_report.json"
    _write_report(
        report_path,
        {
            "promotion_allowed": False,
            "candidate_count": 1,
            "eligible_for_promotion_review_count": 0,
            "capital_plan": [],
            "top_candidates": [
                {
                    "candidate_variant_id": "amd_blocked",
                    "symbol": "AMD",
                    "research_score": 1_000.0,
                    "min_net_pnl": 500.0,
                    "min_test_net_pnl": 50.0,
                    "min_fill_coverage": 0.70,
                    "promotion_status": "research_only_blocked",
                    "promotion_blockers": [
                        "fill_coverage_below_0.90",
                        "option_trades_below_20",
                    ],
                }
            ],
        },
    )

    packet = build_research_promotion_review_packet(
        portfolio_report_json=report_path,
        output_dir=tmp_path / "out",
    )

    assert packet["decision"] == "research_only_blocked"
    assert packet["review_candidates"] == []
    assert packet["symbol_exposure"] == []
    assert packet["blocker_counts"] == {
        "fill_coverage_below_0.90": 1,
        "option_trades_below_20": 1,
    }
    assert packet["data_repair_targets"][0]["candidate_variant_id"] == "amd_blocked"

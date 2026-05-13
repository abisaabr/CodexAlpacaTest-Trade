from __future__ import annotations

import json
from pathlib import Path

from scripts.build_research_wave_portfolio_rollup import (
    build_research_wave_portfolio_rollup,
)


def _write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _candidate(
    candidate_id: str,
    *,
    symbol: str,
    score: float,
    fill: float,
    regime: str = "bull",
    min_net: float = 1000.0,
    min_test: float = 100.0,
    trades: int = 30,
    status: str = "eligible_for_promotion_review",
    blockers: list[str] | None = None,
    fill_failure_reason: str = "fill_gate_clear",
) -> dict:
    return {
        "candidate_variant_id": candidate_id,
        "symbol": symbol,
        "source_strategy_id": f"{symbol.lower()}_strategy",
        "intended_regime": regime,
        "directional_option_type": "call",
        "profile_count": 3,
        "research_score": score,
        "min_net_pnl": min_net,
        "median_net_pnl": min_net,
        "min_test_net_pnl": min_test,
        "median_test_net_pnl": min_test,
        "min_fill_coverage": fill,
        "max_fill_coverage": fill,
        "min_option_trade_count": trades,
        "worst_drawdown": -250.0,
        "promotion_status": status,
        "promotion_blockers": blockers or [],
        "fill_failure_reason": fill_failure_reason,
    }


def test_wave_rollup_builds_global_capital_plan_and_promotion_packet(tmp_path: Path) -> None:
    report_root = tmp_path / "reports"
    _write_report(
        report_root / "SPY" / "portfolio_report" / "research_portfolio_report.json",
        {
            "candidate_count": 2,
            "eligible_for_promotion_review_count": 1,
            "promotion_allowed": True,
            "broker_facing": False,
            "live_manifest_effect": "none",
            "risk_policy_effect": "none",
            "top_candidates": [
                _candidate("spy_a", symbol="SPY", score=2000.0, fill=0.94),
                _candidate(
                    "spy_blocked",
                    symbol="SPY",
                    score=5000.0,
                    fill=0.82,
                    status="research_only_blocked",
                    blockers=["fill_coverage_below_0.90"],
                    fill_failure_reason="entry_bar_gap_or_entry_timing_mismatch",
                ),
            ],
        },
    )
    _write_report(
        report_root / "QQQ" / "portfolio_report" / "research_portfolio_report.json",
        {
            "candidate_count": 1,
            "eligible_for_promotion_review_count": 1,
            "promotion_allowed": True,
            "broker_facing": False,
            "live_manifest_effect": "none",
            "risk_policy_effect": "none",
            "top_candidates": [
                _candidate("qqq_a", symbol="QQQ", score=1800.0, fill=0.96),
            ],
        },
    )

    packet = build_research_wave_portfolio_rollup(
        report_root=report_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=4,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
        max_review_candidates=10,
    )

    assert packet["source_report_count"] == 2
    assert packet["candidate_count"] == 3
    assert packet["eligible_for_promotion_review_count"] == 2
    assert packet["decision"] == "ready_for_governed_validation_review"
    assert packet["candidate_level_decision"] == "ready_for_governed_validation_review"
    assert {row["candidate_variant_id"] for row in packet["capital_plan"]} == {
        "spy_a",
        "qqq_a",
    }
    assert packet["fill_failure_counts"] == {
        "entry_bar_gap_or_entry_timing_mismatch": 1,
        "fill_gate_clear": 2,
    }
    assert (tmp_path / "out" / "research_wave_portfolio_rollup.json").exists()
    assert (tmp_path / "out" / "research_wave_portfolio_rollup.md").exists()
    assert (
        tmp_path / "out" / "promotion_review_packet" / "research_promotion_review_packet.json"
    ).exists()


def test_wave_rollup_deduplicates_candidates_with_best_complete_view(tmp_path: Path) -> None:
    report_root = tmp_path / "reports"
    _write_report(
        report_root / "low" / "portfolio_report" / "research_portfolio_report.json",
        {
            "top_candidates": [
                _candidate(
                    "same_candidate",
                    symbol="AAPL",
                    score=1000.0,
                    fill=0.70,
                    status="research_only_blocked",
                    blockers=["fill_coverage_below_0.90"],
                    fill_failure_reason="selected_contract_universe_gap",
                )
            ]
        },
    )
    _write_report(
        report_root / "high" / "portfolio_report" / "research_portfolio_report.json",
        {"top_candidates": [_candidate("same_candidate", symbol="AAPL", score=1200.0, fill=0.95)]},
    )

    packet = build_research_wave_portfolio_rollup(
        report_root=report_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=4,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
        max_review_candidates=10,
    )

    assert packet["candidate_count"] == 1
    assert packet["top_candidates"][0]["promotion_status"] == "eligible_for_promotion_review"
    assert packet["top_candidates"][0]["source_report_count"] == 2


def test_wave_rollup_discovers_symbol_prefixed_report_files(tmp_path: Path) -> None:
    report_root = tmp_path / "reports"
    _write_report(
        report_root / "SPY_research_portfolio_report.json",
        {"top_candidates": [_candidate("spy_a", symbol="SPY", score=1000.0, fill=0.95)]},
    )

    packet = build_research_wave_portfolio_rollup(
        report_root=report_root,
        output_dir=tmp_path / "out",
        pattern="*_research_portfolio_report.json",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=4,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
        max_review_candidates=10,
    )

    assert packet["source_report_count"] == 1
    assert packet["top_candidates"][0]["candidate_variant_id"] == "spy_a"


def test_wave_rollup_infers_fill_failure_reason_for_older_reports(tmp_path: Path) -> None:
    report_root = tmp_path / "reports"
    candidate = _candidate(
        "spy_blocked",
        symbol="SPY",
        score=1000.0,
        fill=0.50,
        status="research_only_blocked",
        blockers=["fill_coverage_below_0.90"],
        fill_failure_reason="",
    )
    candidate["max_missing_no_selected_contract"] = 0
    candidate["max_missing_no_entry_bar"] = 8
    candidate["max_missing_no_exit_bar"] = 2
    _write_report(report_root / "research_portfolio_report.json", {"top_candidates": [candidate]})

    packet = build_research_wave_portfolio_rollup(
        report_root=report_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=4,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
        max_review_candidates=10,
    )

    assert packet["fill_failure_counts"] == {"entry_bar_gap_or_entry_timing_mismatch": 1}
    assert packet["data_repair_priority_candidates"][0]["candidate_variant_id"] == "spy_blocked"


def test_wave_rollup_tracks_regime_completeness(tmp_path: Path) -> None:
    report_root = tmp_path / "reports"
    _write_report(
        report_root / "bull" / "research_portfolio_report.json",
        {
            "top_candidates": [
                _candidate(
                    "iwm_bull_eligible",
                    symbol="IWM",
                    score=2000.0,
                    fill=0.994,
                    regime="bull",
                )
            ]
        },
    )
    _write_report(
        report_root / "bear" / "research_portfolio_report.json",
        {
            "top_candidates": [
                _candidate(
                    "iwm_bear_blocked",
                    symbol="IWM",
                    score=1000.0,
                    fill=0.96,
                    regime="bear",
                    min_test=-50.0,
                    status="research_only_blocked",
                    blockers=["test_net_pnl_not_above_0"],
                )
            ]
        },
    )
    _write_report(
        report_root / "choppy" / "research_portfolio_report.json",
        {
            "top_candidates": [
                _candidate(
                    "iwm_choppy_blocked",
                    symbol="IWM",
                    score=3000.0,
                    fill=0.7587,
                    regime="choppy",
                    min_net=15_732.003,
                    min_test=47_262.66,
                    status="research_only_blocked",
                    blockers=["fill_coverage_below_0.90"],
                    fill_failure_reason="entry_bar_gap_or_entry_timing_mismatch",
                )
            ]
        },
    )

    packet = build_research_wave_portfolio_rollup(
        report_root=report_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=4,
        max_strategies_per_symbol=3,
        max_symbol_weight=1.0,
        initial_cash=25_000.0,
        max_review_candidates=10,
    )

    assert packet["eligible_regimes"] == ["bull"]
    assert packet["missing_eligible_regimes"] == ["bear", "choppy"]
    assert packet["regime_complete_for_promotion_review"] is False
    assert packet["promotion_allowed_regime_complete"] is True
    assert packet["regime_completeness_policy"] == "informational_only_not_a_hard_promotion_gate"
    assert packet["governance_review_scope"] == "per_regime_governed_validation_review"
    assert packet["decision"] == "ready_for_governed_validation_review"
    summary_by_regime = {row["intended_regime"]: row for row in packet["regime_summary"]}
    assert summary_by_regime["bull"]["eligible_for_promotion_review_count"] == 1
    assert summary_by_regime["bear"]["blocker_counts"] == {
        "test_net_pnl_not_above_0": 1
    }
    assert summary_by_regime["choppy"]["blocker_counts"] == {
        "fill_coverage_below_0.90": 1
    }

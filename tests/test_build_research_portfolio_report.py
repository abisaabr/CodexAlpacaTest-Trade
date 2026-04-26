from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.build_research_portfolio_report import build_research_portfolio_report


def _write_profile(root: Path, profile: str, rows: list[dict]) -> None:
    path = root / profile / "option_aware_candidate_summary.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_research_portfolio_report_blocks_low_fill_but_builds_interim_plan(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    base_rows = [
        {
            "candidate_variant_id": "amd_candidate",
            "symbol": "AMD",
            "source_strategy_id": "amd_strategy",
            "directional_option_type": "call",
            "net_pnl": 5000.0,
            "test_net_pnl": 300.0,
            "fill_coverage": 0.55,
            "option_trade_count": 30,
            "max_drawdown": -900.0,
            "win_rate": 0.7,
            "profit_factor": 4.0,
            "missing_option_price_count": 10,
            "missing_no_selected_contract": 3,
            "missing_no_entry_bar": 5,
            "missing_no_exit_bar": 2,
        },
        {
            "candidate_variant_id": "orcl_candidate",
            "symbol": "ORCL",
            "source_strategy_id": "orcl_strategy",
            "directional_option_type": "call",
            "net_pnl": 3000.0,
            "test_net_pnl": 100.0,
            "fill_coverage": 0.40,
            "option_trade_count": 24,
            "max_drawdown": -600.0,
            "win_rate": 0.6,
            "profit_factor": 3.0,
            "missing_option_price_count": 12,
            "missing_no_selected_contract": 4,
            "missing_no_entry_bar": 6,
            "missing_no_exit_bar": 2,
        },
        {
            "candidate_variant_id": "ual_thin_candidate",
            "symbol": "UAL",
            "source_strategy_id": "ual_strategy",
            "directional_option_type": "put",
            "net_pnl": 9000.0,
            "test_net_pnl": 1000.0,
            "fill_coverage": 0.10,
            "option_trade_count": 3,
            "max_drawdown": -100.0,
            "win_rate": 1.0,
            "profit_factor": 10.0,
            "missing_option_price_count": 10,
            "missing_no_selected_contract": 3,
            "missing_no_entry_bar": 5,
            "missing_no_exit_bar": 2,
        },
    ]
    _write_profile(replay_root, "stress_a", base_rows)
    _write_profile(replay_root, "stress_b", base_rows)

    packet = build_research_portfolio_report(
        replay_root=replay_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=5,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
    )

    assert packet["broker_facing"] is False
    assert packet["promotion_allowed"] is False
    assert packet["eligible_for_promotion_review_count"] == 0
    assert len(packet["capital_plan"]) == 2
    assert {row["symbol"] for row in packet["capital_plan"]} == {"AMD", "ORCL"}
    assert sum(row["research_only_weight"] for row in packet["capital_plan"]) == 1.0
    assert packet["capital_plan"][0]["research_only_dollars"] == 12_500.0
    assert "fill_coverage_below_0.90" in packet["top_candidates"][0]["promotion_blockers"]
    assert (tmp_path / "out" / "research_portfolio_report.json").exists()
    assert (tmp_path / "out" / "research_portfolio_report.md").exists()


def test_research_portfolio_report_allows_review_when_gates_pass(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    _write_profile(
        replay_root,
        "stress_a",
        [
            {
                "candidate_variant_id": "amd_candidate",
                "symbol": "AMD",
                "source_strategy_id": "amd_strategy",
                "directional_option_type": "call",
                "net_pnl": 5000.0,
                "test_net_pnl": 300.0,
                "fill_coverage": 0.95,
                "option_trade_count": 30,
                "max_drawdown": -900.0,
                "win_rate": 0.7,
                "profit_factor": 4.0,
                "missing_option_price_count": 0,
                "missing_no_selected_contract": 0,
                "missing_no_entry_bar": 0,
                "missing_no_exit_bar": 0,
            }
        ],
    )

    packet = build_research_portfolio_report(
        replay_root=replay_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=5,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
    )

    assert packet["promotion_allowed"] is True
    assert packet["eligible_for_promotion_review_count"] == 1
    assert packet["top_candidates"][0]["promotion_status"] == "eligible_for_promotion_review"
    assert packet["capital_plan"][0]["research_only_weight"] == 0.5
    assert packet["capital_plan_unallocated_dollars"] == 12_500.0


def test_research_portfolio_report_prefers_eligible_candidate_over_blocked_high_score(
    tmp_path: Path,
) -> None:
    replay_root = tmp_path / "replay"
    rows = [
        {
            "candidate_variant_id": "amd_blocked_high_score",
            "symbol": "AMD",
            "source_strategy_id": "amd_strategy",
            "directional_option_type": "call",
            "net_pnl": 50_000.0,
            "test_net_pnl": 5_000.0,
            "fill_coverage": 0.88,
            "option_trade_count": 60,
            "max_drawdown": -900.0,
            "win_rate": 0.7,
            "profit_factor": 4.0,
            "missing_option_price_count": 0,
            "missing_no_selected_contract": 0,
            "missing_no_entry_bar": 0,
            "missing_no_exit_bar": 8,
        },
        {
            "candidate_variant_id": "amd_eligible_lower_score",
            "symbol": "AMD",
            "source_strategy_id": "amd_strategy",
            "directional_option_type": "call",
            "net_pnl": 20_000.0,
            "test_net_pnl": 2_000.0,
            "fill_coverage": 0.92,
            "option_trade_count": 55,
            "max_drawdown": -1_000.0,
            "win_rate": 0.65,
            "profit_factor": 3.5,
            "missing_option_price_count": 0,
            "missing_no_selected_contract": 0,
            "missing_no_entry_bar": 0,
            "missing_no_exit_bar": 4,
        },
    ]
    _write_profile(replay_root, "stress_a", rows)
    _write_profile(replay_root, "stress_b", rows)

    packet = build_research_portfolio_report(
        replay_root=replay_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=5,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
    )

    assert packet["eligible_for_promotion_review_count"] == 1
    assert packet["capital_plan"][0]["candidate_variant_id"] == "amd_eligible_lower_score"
    assert packet["capital_plan"][0]["promotion_status"] == "eligible_for_promotion_review"


def test_research_portfolio_report_allows_multiple_strategies_per_symbol_with_symbol_cap(
    tmp_path: Path,
) -> None:
    replay_root = tmp_path / "replay"
    rows = [
        {
            "candidate_variant_id": "amd_strategy_a",
            "symbol": "AMD",
            "source_strategy_id": "amd_opening_call",
            "directional_option_type": "call",
            "net_pnl": 10_000.0,
            "test_net_pnl": 1_000.0,
            "fill_coverage": 0.95,
            "option_trade_count": 60,
            "max_drawdown": -900.0,
            "win_rate": 0.7,
            "profit_factor": 4.0,
            "missing_option_price_count": 0,
            "missing_no_selected_contract": 0,
            "missing_no_entry_bar": 0,
            "missing_no_exit_bar": 0,
        },
        {
            "candidate_variant_id": "amd_strategy_b",
            "symbol": "AMD",
            "source_strategy_id": "amd_late_call",
            "directional_option_type": "call",
            "net_pnl": 8_000.0,
            "test_net_pnl": 800.0,
            "fill_coverage": 0.94,
            "option_trade_count": 55,
            "max_drawdown": -800.0,
            "win_rate": 0.68,
            "profit_factor": 3.5,
            "missing_option_price_count": 0,
            "missing_no_selected_contract": 0,
            "missing_no_entry_bar": 0,
            "missing_no_exit_bar": 0,
        },
        {
            "candidate_variant_id": "amd_strategy_c",
            "symbol": "AMD",
            "source_strategy_id": "amd_extra",
            "directional_option_type": "call",
            "net_pnl": 7_000.0,
            "test_net_pnl": 700.0,
            "fill_coverage": 0.93,
            "option_trade_count": 50,
            "max_drawdown": -700.0,
            "win_rate": 0.66,
            "profit_factor": 3.0,
            "missing_option_price_count": 0,
            "missing_no_selected_contract": 0,
            "missing_no_entry_bar": 0,
            "missing_no_exit_bar": 0,
        },
        {
            "candidate_variant_id": "orcl_strategy",
            "symbol": "ORCL",
            "source_strategy_id": "orcl_opening_call",
            "directional_option_type": "call",
            "net_pnl": 6_000.0,
            "test_net_pnl": 600.0,
            "fill_coverage": 0.96,
            "option_trade_count": 45,
            "max_drawdown": -700.0,
            "win_rate": 0.64,
            "profit_factor": 3.0,
            "missing_option_price_count": 0,
            "missing_no_selected_contract": 0,
            "missing_no_entry_bar": 0,
            "missing_no_exit_bar": 0,
        },
    ]
    _write_profile(replay_root, "stress_a", rows)

    packet = build_research_portfolio_report(
        replay_root=replay_root,
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_option_trades=20,
        min_test_net_pnl=0.0,
        max_positions=5,
        max_strategies_per_symbol=2,
        max_symbol_weight=0.50,
        initial_cash=25_000.0,
    )

    amd_plan = [row for row in packet["capital_plan"] if row["symbol"] == "AMD"]
    assert [row["candidate_variant_id"] for row in amd_plan] == [
        "amd_strategy_a",
        "amd_strategy_b",
    ]
    assert "amd_strategy_c" not in {
        row["candidate_variant_id"] for row in packet["capital_plan"]
    }
    assert sum(row["research_only_weight"] for row in amd_plan) == 0.5
    assert sum(row["research_only_weight"] for row in packet["capital_plan"]) == 1.0

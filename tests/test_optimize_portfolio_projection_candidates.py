from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.optimize_portfolio_projection_candidates import (
    _prepare_daily_vectors,
    _simulate_selected,
    _simulate_selected_fast,
    optimize_portfolio_candidates,
)


def test_optimizer_selects_train_test_positive_candidates_under_constraints(tmp_path: Path) -> None:
    portfolio = tmp_path / "portfolio.json"
    capital_plan = []
    rows = []
    specs = [
        ("qqq_bull", "QQQ", "bull", "single_leg", [100, 80, 60, 50]),
        ("iwm_bear", "IWM", "bear", "vertical", [50, 40, 30, 20]),
        ("spy_choppy", "SPY", "choppy", "single_leg", [70, -200, 80, 10]),
    ]
    dates = ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"]
    for candidate, symbol, regime, family, pnls in specs:
        capital_plan.append(
            {
                "candidate_variant_id": f"{candidate}__profile_profile-a",
                "base_candidate_variant_id": candidate,
                "aggregate_profile": "profile_a",
                "symbol": symbol,
                "family": family,
                "intended_regime": regime,
                "research_only_weight": 1 / 3,
                "research_only_dollars": 8333.33,
            }
        )
        for trade_date, pnl in zip(dates, pnls, strict=True):
            rows.append(
                {
                    "trade_date": trade_date,
                    "candidate_variant_id": f"{candidate}__profile_profile-a",
                    "base_candidate_variant_id": candidate,
                    "aggregate_profile": "profile_a",
                    "symbol": symbol,
                    "family": family,
                    "intended_regime": regime,
                    "source_pnl_per_combo": pnl,
                    "scaled_option_pnl": pnl,
                }
            )
    portfolio.write_text(json.dumps({"capital_plan": capital_plan}), encoding="utf-8")
    trades = tmp_path / "scaled.csv"
    pd.DataFrame(rows).to_csv(trades, index=False)

    summary = optimize_portfolio_candidates(
        portfolio_report_json=portfolio,
        scaled_trades_csv=trades,
        output_dir=tmp_path / "out",
        initial_cash=25_000.0,
        backtest_allocation_fraction=0.05,
        train_end_date="2025-01-03",
        min_train_trades=2,
        min_test_trades=2,
        max_candidates=5,
        max_per_symbol=2,
        max_per_regime=2,
        max_per_family=2,
        min_symbols=2,
        min_regimes=2,
        min_families=2,
        max_drawdown_pct=50.0,
        objective="total_pnl",
    )

    assert summary["status"] == "passed"
    assert summary["selected_candidate_count"] == 2
    assert summary["selected_symbol_counts"] == {"QQQ": 1, "IWM": 1}
    assert summary["optimizer_metadata"]["search_mode"] == "exact_subset"
    assert (tmp_path / "out" / "optimized_portfolio_report.json").exists()


def test_optimizer_enforces_concentration_limits_during_exact_search(tmp_path: Path) -> None:
    portfolio = tmp_path / "portfolio.json"
    capital_plan = []
    rows = []
    specs = [
        ("qqq_bull", "QQQ", "bull", "single_leg", [100, 100, 100, 100]),
        ("spy_bear", "SPY", "bear", "single_leg", [95, 95, 95, 95]),
        ("avgo_choppy", "AVGO", "choppy", "vertical", [35, 35, 35, 35]),
    ]
    dates = ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"]
    for candidate, symbol, regime, family, pnls in specs:
        capital_plan.append(
            {
                "candidate_variant_id": f"{candidate}__profile_profile-a",
                "base_candidate_variant_id": candidate,
                "aggregate_profile": "profile_a",
                "symbol": symbol,
                "family": family,
                "intended_regime": regime,
                "research_only_weight": 1 / 3,
                "research_only_dollars": 8333.33,
            }
        )
        for trade_date, pnl in zip(dates, pnls, strict=True):
            rows.append(
                {
                    "trade_date": trade_date,
                    "candidate_variant_id": f"{candidate}__profile_profile-a",
                    "base_candidate_variant_id": candidate,
                    "aggregate_profile": "profile_a",
                    "symbol": symbol,
                    "family": family,
                    "intended_regime": regime,
                    "source_pnl_per_combo": pnl,
                    "scaled_option_pnl": pnl,
                }
            )
    portfolio.write_text(json.dumps({"capital_plan": capital_plan}), encoding="utf-8")
    trades = tmp_path / "scaled.csv"
    pd.DataFrame(rows).to_csv(trades, index=False)

    summary = optimize_portfolio_candidates(
        portfolio_report_json=portfolio,
        scaled_trades_csv=trades,
        output_dir=tmp_path / "out_concentration",
        initial_cash=25_000.0,
        backtest_allocation_fraction=0.05,
        train_end_date="2025-01-03",
        min_train_trades=2,
        min_test_trades=2,
        max_candidates=2,
        max_per_symbol=2,
        max_per_regime=2,
        max_per_family=2,
        min_symbols=2,
        min_regimes=2,
        min_families=1,
        max_drawdown_pct=50.0,
        max_family_trade_share=0.60,
        objective="total_pnl",
    )

    assert summary["status"] == "passed"
    assert summary["selected_family_counts"] == {"single_leg": 1, "vertical": 1}
    assert summary["selected_concentration"]["family_trade_share"] == {
        "single_leg": 0.5,
        "vertical": 0.5,
    }
    assert (
        summary["optimizer_metadata"]["infeasible_reason_counts"]["max_family_trade_share"]
        >= 1
    )


def test_optimizer_can_fail_on_minimum_average_daily_pnl(tmp_path: Path) -> None:
    portfolio = tmp_path / "portfolio.json"
    capital_plan = []
    rows = []
    specs = [
        ("qqq_bull", "QQQ", "bull", "single_leg", [10, 10, 10, 10]),
        ("iwm_bear", "IWM", "bear", "vertical", [10, 10, 10, 10]),
    ]
    dates = ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"]
    for candidate, symbol, regime, family, pnls in specs:
        capital_plan.append(
            {
                "candidate_variant_id": f"{candidate}__profile_profile-a",
                "base_candidate_variant_id": candidate,
                "aggregate_profile": "profile_a",
                "symbol": symbol,
                "family": family,
                "intended_regime": regime,
                "research_only_weight": 0.5,
                "research_only_dollars": 12_500.0,
            }
        )
        for trade_date, pnl in zip(dates, pnls, strict=True):
            rows.append(
                {
                    "trade_date": trade_date,
                    "candidate_variant_id": f"{candidate}__profile_profile-a",
                    "base_candidate_variant_id": candidate,
                    "aggregate_profile": "profile_a",
                    "symbol": symbol,
                    "family": family,
                    "intended_regime": regime,
                    "source_pnl_per_combo": pnl,
                    "scaled_option_pnl": pnl,
                }
            )
    portfolio.write_text(json.dumps({"capital_plan": capital_plan}), encoding="utf-8")
    trades = tmp_path / "scaled.csv"
    pd.DataFrame(rows).to_csv(trades, index=False)

    summary = optimize_portfolio_candidates(
        portfolio_report_json=portfolio,
        scaled_trades_csv=trades,
        output_dir=tmp_path / "out_daily_target",
        initial_cash=25_000.0,
        backtest_allocation_fraction=0.05,
        train_end_date="2025-01-03",
        min_train_trades=2,
        min_test_trades=2,
        max_candidates=2,
        max_per_symbol=2,
        max_per_regime=2,
        max_per_family=2,
        min_symbols=2,
        min_regimes=2,
        min_families=2,
        max_drawdown_pct=50.0,
        min_average_daily_pnl=250.0,
        objective="total_pnl",
    )

    assert summary["status"] == "failed"
    assert summary["selected_candidate_count"] == 0
    assert "min_average_daily_pnl" in summary["failure_reasons"]


def test_fast_exact_simulation_matches_reference_simulation() -> None:
    rows = [
        {
            "trade_date": "2025-01-02",
            "base_candidate_variant_id": "qqq_bull",
            "aggregate_profile": "profile_a",
            "source_pnl_per_combo": 100,
        },
        {
            "trade_date": "2025-01-03",
            "base_candidate_variant_id": "qqq_bull",
            "aggregate_profile": "profile_a",
            "source_pnl_per_combo": -20,
        },
        {
            "trade_date": "2025-01-03",
            "base_candidate_variant_id": "iwm_bear",
            "aggregate_profile": "profile_b",
            "source_pnl_per_combo": 40,
        },
        {
            "trade_date": "2025-01-06",
            "base_candidate_variant_id": "iwm_bear",
            "aggregate_profile": "profile_b",
            "source_pnl_per_combo": 80,
        },
    ]
    trades = pd.DataFrame(rows)
    trades["_optimizer_key"] = trades.apply(
        lambda row: (row["base_candidate_variant_id"], row["aggregate_profile"]),
        axis=1,
    )
    selected = {("qqq_bull", "profile_a"), ("iwm_bear", "profile_b")}

    reference = _simulate_selected(
        trades,
        selected,
        initial_cash=25_000.0,
        backtest_allocation_fraction=0.05,
    )
    fast = _simulate_selected_fast(
        _prepare_daily_vectors(trades),
        selected,
        initial_cash=25_000.0,
        backtest_allocation_fraction=0.05,
    )

    reference.pop("daily_rows")
    assert fast == reference

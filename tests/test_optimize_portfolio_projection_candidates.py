from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.optimize_portfolio_projection_candidates import optimize_portfolio_candidates


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

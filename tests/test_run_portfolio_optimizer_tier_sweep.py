from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.run_portfolio_optimizer_tier_sweep import run_optimizer_tier_sweep


def test_run_optimizer_tier_sweep_writes_reproducible_tier_outputs(tmp_path: Path) -> None:
    report = tmp_path / "portfolio_report.json"
    report.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "base_candidate_variant_id": "qqq_a",
                        "candidate_variant_id": "qqq_a",
                        "aggregate_profile": "strict",
                        "symbol": "QQQ",
                        "family": "broken_wing_put_butterfly",
                        "intended_regime": "bear",
                    },
                    {
                        "base_candidate_variant_id": "spy_b",
                        "candidate_variant_id": "spy_b",
                        "aggregate_profile": "strict",
                        "symbol": "SPY",
                        "family": "debit_put_vertical",
                        "intended_regime": "bear",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    trades = tmp_path / "scaled_trades.csv"
    pd.DataFrame(
        [
            {
                "trade_date": "2026-01-02",
                "candidate_variant_id": "qqq_a",
                "aggregate_profile": "strict",
                "symbol": "QQQ",
                "family": "broken_wing_put_butterfly",
                "intended_regime": "bear",
                "scaled_projection_pnl": 20.0,
            },
            {
                "trade_date": "2026-02-03",
                "candidate_variant_id": "qqq_a",
                "aggregate_profile": "strict",
                "symbol": "QQQ",
                "family": "broken_wing_put_butterfly",
                "intended_regime": "bear",
                "scaled_projection_pnl": 30.0,
            },
            {
                "trade_date": "2026-01-02",
                "candidate_variant_id": "spy_b",
                "aggregate_profile": "strict",
                "symbol": "SPY",
                "family": "debit_put_vertical",
                "intended_regime": "bear",
                "scaled_projection_pnl": 15.0,
            },
            {
                "trade_date": "2026-02-03",
                "candidate_variant_id": "spy_b",
                "aggregate_profile": "strict",
                "symbol": "SPY",
                "family": "debit_put_vertical",
                "intended_regime": "bear",
                "scaled_projection_pnl": 25.0,
            },
        ]
    ).to_csv(trades, index=False)

    summary = run_optimizer_tier_sweep(
        portfolio_report_json=report,
        scaled_trades_csv=trades,
        output_dir=tmp_path / "out",
        initial_cash=25_000.0,
        backtest_allocation_fraction=0.05,
        min_train_trades=1,
        min_test_trades=1,
        tiers=["unconstrained_max_profit", "drawdown_minimized_benchmark"],
    )

    assert summary["status"] == "optimizer_tier_sweep_complete"
    assert summary["tier_count"] == 2
    assert summary["paper_runner_state_changed"] is False
    assert (tmp_path / "out" / "optimizer_tier_sweep_summary.json").exists()
    assert (tmp_path / "out" / "unconstrained_max_profit" / "optimizer_summary.json").exists()
    assert summary["tiers"][0]["selected_candidate_count"] >= 1

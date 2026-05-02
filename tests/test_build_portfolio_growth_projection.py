from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_portfolio_growth_projection import build_growth_projection


def test_full_year_calendar_carries_cash_and_reports_regime_gaps(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull_long_call",
                "option_pnl": 100.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T20:00:00Z",
                "quantity": 1,
                "fees": 0.65,
            },
            {
                "trade_date": "2025-01-06",
                "candidate_variant_id": "qqq_bull_long_call",
                "option_pnl": -25.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250107C00100000",
                "option_entry_time": "2025-01-06T15:00:00Z",
                "option_exit_time": "2025-01-06T20:00:00Z",
                "quantity": 1,
                "fees": 0.65,
            },
        ]
    ).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)

    portfolio_report = {
        "capital_plan": [
            {
                "candidate_variant_id": "qqq_bull_long_call__profile_profile-a",
                "base_candidate_variant_id": "qqq_bull_long_call",
                "aggregate_profile": "profile_a",
                "symbol": "QQQ",
                "family": "long_call",
                "intended_regime": "bull",
                "research_only_weight": 0.2,
                "research_only_dollars": 5000.0,
            }
        ]
    }
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(json.dumps(portfolio_report), encoding="utf-8")

    calendar_path = tmp_path / "calendar.csv"
    pd.DataFrame(
        [
            {"trade_date": "2025-01-02", "regime": "bull"},
            {"trade_date": "2025-01-03", "regime": "choppy"},
            {"trade_date": "2025-01-06", "regime": "bull"},
            {"trade_date": "2025-01-07", "regime": "bear"},
            {"trade_date": "2025-01-08", "regime": "choppy"},
        ]
    ).to_csv(calendar_path, index=False)

    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=tmp_path / "out",
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        calendar_csv=calendar_path,
    )

    assert packet["matched_daily_count"] == 2
    assert packet["full_year_daily_count"] == 5
    assert packet["projection_calendar"]["raw_dataset_trading_days"] == 5
    assert packet["projection_calendar"]["strategy_active_days"] == 2
    assert packet["projection_calendar"]["inactive_cash_days"] == 3
    assert packet["historical_metrics"]["trading_days"] == 5
    assert packet["active_day_historical_metrics"]["trading_days"] == 2
    assert packet["regime_coverage"]["required_regime_status"]["bull"]["covered"] is True
    assert packet["regime_coverage"]["required_regime_status"]["bear"]["covered"] is False
    assert "missing_bear_strategy_coverage" in packet["evidence_grade"]["blockers"]
    assert (tmp_path / "out" / "portfolio_growth_equity_curve.csv").exists()
    assert (tmp_path / "out" / "portfolio_growth_active_day_equity_curve.csv").exists()

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


def test_combined_reports_reweight_by_symbol_cap(tmp_path: Path) -> None:
    replay_a = tmp_path / "replay_a"
    replay_b = tmp_path / "replay_b"
    (replay_a / "profile_qqq").mkdir(parents=True)
    (replay_b / "profile_iwm").mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 100.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T20:00:00Z",
                "quantity": 1,
            }
        ]
    ).to_csv(replay_a / "profile_qqq" / "option_aware_trade_economics.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "iwm_bear",
                "option_pnl": 50.0,
                "symbol": "IWM",
                "contract_symbol": "IWM250103P00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T20:00:00Z",
                "quantity": 1,
            }
        ]
    ).to_csv(replay_b / "profile_iwm" / "option_aware_trade_economics.csv", index=False)

    report_a = tmp_path / "report_a.json"
    report_a.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-qqq",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_qqq",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 1.0,
                        "research_only_dollars": 25_000.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    report_b = tmp_path / "report_b.json"
    report_b.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "iwm_bear__profile_profile-iwm",
                        "base_candidate_variant_id": "iwm_bear",
                        "aggregate_profile": "profile_iwm",
                        "symbol": "IWM",
                        "family": "single_leg_repair",
                        "intended_regime": "bear",
                        "research_only_weight": 1.0,
                        "research_only_dollars": 25_000.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    packet = build_growth_projection(
        portfolio_report_json=report_a,
        replay_root=replay_a,
        output_dir=tmp_path / "out_combined",
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        additional_portfolio_report_jsons=[report_b],
        additional_replay_roots=[replay_b],
        max_symbol_weight=0.60,
    )

    assert packet["capital_plan_count"] == 2
    assert packet["matched_trade_count"] == 2
    assert packet["capital_plan_merge"]["mode"] == "portfolio_level_symbol_cap_reweight"
    assert packet["capital_plan_merge"]["reweighted_symbol_weights"] == {"IWM": 0.5, "QQQ": 0.5}
    assert {row["research_only_weight"] for row in packet["capital_plan"]} == {0.5}


def test_production_runtime_sizes_by_risk_fraction(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 50.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T16:00:00Z",
                "entry_debit_per_unit": 500.0,
                "risk_per_unit": 500.0,
                "quantity": 1,
            }
        ]
    ).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 1.0,
                        "research_only_dollars": 25_000.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    risk_path = tmp_path / "risk.yaml"
    risk_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_open_risk_fraction: 0.15",
                "  max_open_positions: 10",
                "  max_positions_per_regime: 10",
                "  max_positions_per_symbol: 3",
                "  max_open_risk_fraction_per_symbol: 0.10",
                "  bucket_caps: []",
            ]
        ),
        encoding="utf-8",
    )

    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=tmp_path / "out_production",
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        risk_simulation_mode="production_runtime",
        production_risk_config_yaml=risk_path,
        production_default_risk_fraction=0.05,
        production_default_max_contracts=6,
    )

    assert packet["risk_simulation_mode"] == "production_runtime"
    assert packet["accepted_trade_count"] == 1
    assert packet["production_risk_simulation"]["accepted_trade_count"] == 1
    assert packet["production_risk_simulation"]["rejected_trade_count"] == 0
    scaled_trades = pd.read_csv(tmp_path / "out_production" / "portfolio_growth_scaled_trades.csv")
    assert scaled_trades["production_quantity"].iloc[0] == 2
    assert scaled_trades["scaled_option_pnl"].iloc[0] == 100.0
    assert (tmp_path / "out_production" / "portfolio_growth_risk_events.csv").exists()


def test_production_runtime_enforces_per_symbol_open_position_cap(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull_a",
                "option_pnl": 50.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T16:00:00Z",
                "entry_debit_per_unit": 500.0,
                "risk_per_unit": 500.0,
                "quantity": 1,
            },
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull_b",
                "option_pnl": 25.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00101000",
                "option_entry_time": "2025-01-02T15:05:00Z",
                "option_exit_time": "2025-01-02T16:05:00Z",
                "entry_debit_per_unit": 500.0,
                "risk_per_unit": 500.0,
                "quantity": 1,
            },
        ]
    ).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull_a__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull_a",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 0.5,
                        "research_only_dollars": 12_500.0,
                    },
                    {
                        "candidate_variant_id": "qqq_bull_b__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull_b",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 0.5,
                        "research_only_dollars": 12_500.0,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    risk_path = tmp_path / "risk.yaml"
    risk_path.write_text(
        "\n".join(
            [
                "risk:",
                "  max_open_risk_fraction: 0.15",
                "  max_open_positions: 10",
                "  max_positions_per_regime: 10",
                "  max_positions_per_symbol: 1",
                "  max_positions_per_regime_window: null",
                "  max_positions_per_bucket_regime_window: null",
                "  max_open_risk_fraction_per_symbol: 0.10",
                "  bucket_caps: []",
            ]
        ),
        encoding="utf-8",
    )

    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=tmp_path / "out_symbol_cap",
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        risk_simulation_mode="production_runtime",
        production_risk_config_yaml=risk_path,
        production_default_risk_fraction=0.05,
        production_default_max_contracts=6,
    )

    assert packet["production_risk_simulation"]["accepted_trade_count"] == 1
    assert packet["production_risk_simulation"]["rejected_trade_count"] == 1
    assert packet["production_risk_simulation"]["rejection_reason_counts"] == {
        "max_positions_per_symbol": 1
    }


def test_projection_reports_unmatched_capital_plan_rows(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 100.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T16:00:00Z",
                "quantity": 1,
            }
        ]
    ).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 0.5,
                        "research_only_dollars": 12_500.0,
                    },
                    {
                        "candidate_variant_id": "iwm_missing__profile_profile-a",
                        "base_candidate_variant_id": "iwm_missing",
                        "aggregate_profile": "profile_a",
                        "symbol": "IWM",
                        "family": "debit_put_vertical",
                        "intended_regime": "bear",
                        "research_only_weight": 0.5,
                        "research_only_dollars": 12_500.0,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=tmp_path / "out_unmatched",
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
    )

    coverage = packet["projection_hardening"]["match_coverage"]
    assert coverage["matched_capital_plan_count"] == 1
    assert coverage["unmatched_capital_plan_count"] == 1
    assert coverage["unmatched_sample"][0]["base_candidate_variant_id"] == "iwm_missing"
    assert "unmatched_capital_plan_strategies" in packet["evidence_grade"]["warnings"]
    assert (tmp_path / "out_unmatched" / "portfolio_growth_unmatched_capital_plan.csv").exists()


def test_projection_filters_market_quality_and_fill_probability(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    rows = []
    for index, values in enumerate(
        [
            ("2025-01-02", 100.0, 0.01, 1.0, 12),
            ("2025-01-03", 75.0, 0.25, 1.0, 12),
            ("2025-01-06", 50.0, 0.01, 90.0, 0),
        ],
        start=1,
    ):
        trade_date, pnl, spread, age, prints = values
        rows.append(
            {
                "trade_date": trade_date,
                "candidate_variant_id": "qqq_bull",
                "option_pnl": pnl,
                "symbol": "QQQ",
                "contract_symbol": f"QQQ25010{index}C00100000",
                "option_entry_time": f"{trade_date}T15:0{index}:00Z",
                "option_exit_time": f"{trade_date}T16:0{index}:00Z",
                "quantity": 1,
                "entry_spread_pct": spread,
                "entry_quote_age_seconds": age,
                "entry_selection_trade_print_count": prints,
            }
        )
    pd.DataFrame(rows).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 1.0,
                        "research_only_dollars": 25_000.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "out_quality"
    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=output_dir,
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        stress_max_entry_spread_pct=0.05,
        fill_model_enabled=True,
        min_fill_probability=0.5,
    )

    hardening = packet["projection_hardening"]
    assert hardening["market_quality_stress"]["input_trade_count"] == 3
    assert hardening["market_quality_stress"]["rejected_trade_count"] == 1
    assert hardening["fill_probability_model"]["input_trade_count"] == 2
    assert hardening["fill_probability_model"]["rejected_trade_count"] == 1
    assert packet["matched_trade_count"] == 1
    assert (output_dir / "portfolio_growth_hardening_rejections.csv").exists()
    scaled_trades = pd.read_csv(output_dir / "portfolio_growth_scaled_trades.csv")
    assert scaled_trades["projected_fill_probability"].iloc[0] >= 0.5
    assert scaled_trades["entry_spread_pct_source_column"].iloc[0] == "entry_spread_pct"


def test_projection_uses_backtester_relative_spread_aliases_and_quote_source(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 100.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250102C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T16:00:00Z",
                "quantity": 1,
                "entry_average_relative_spread": 0.03,
                "exit_max_relative_spread": 0.04,
                "entry_quote_age_seconds": 0.0,
                "exit_quote_age_seconds": 0.0,
                "entry_quote_source": "option_quote_bid_ask",
                "exit_quote_source": "option_quote_bid_ask",
            },
            {
                "trade_date": "2025-01-03",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 75.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00100000",
                "option_entry_time": "2025-01-03T15:00:00Z",
                "option_exit_time": "2025-01-03T16:00:00Z",
                "quantity": 1,
                "entry_average_relative_spread": 0.03,
                "exit_max_relative_spread": 0.04,
                "entry_quote_age_seconds": 0.0,
                "exit_quote_age_seconds": 0.0,
                "entry_quote_source": "option_bar_close_no_bid_ask",
                "exit_quote_source": "option_bar_close_no_bid_ask",
            },
        ]
    ).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 1.0,
                        "research_only_dollars": 25_000.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "out_quality_aliases"
    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=output_dir,
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        stress_max_entry_spread_pct=0.05,
        stress_max_exit_spread_pct=0.05,
        fill_model_enabled=True,
        min_fill_probability=0.5,
    )

    hardening = packet["projection_hardening"]
    assert hardening["market_quality_stress"]["source_columns"]["entry_spread_pct"] == (
        "entry_average_relative_spread"
    )
    assert hardening["market_quality_stress"]["source_columns"]["exit_spread_pct"] == (
        "exit_max_relative_spread"
    )
    assert hardening["market_quality_diagnostics"]["quote_source_counts"]["entry_quote_source"] == {
        "option_quote_bid_ask": 1,
    }
    assert hardening["market_quality_diagnostics"]["rows_with_any_no_bid_ask_quote_source"] == 0
    assert hardening["fill_probability_model"]["rejected_trade_count"] == 1
    scaled_trades = pd.read_csv(output_dir / "portfolio_growth_scaled_trades.csv")
    assert scaled_trades["entry_quote_source"].tolist() == ["option_quote_bid_ask"]
    assert scaled_trades["entry_spread_pct_source_column"].tolist() == [
        "entry_average_relative_spread"
    ]


def test_fill_probability_haircuts_positive_pnl_but_not_losses(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 100.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250102C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T16:00:00Z",
                "quantity": 1,
                "entry_quote_age_seconds": 0.0,
                "exit_quote_age_seconds": 0.0,
                "entry_quote_source": "option_bar_close_no_bid_ask",
                "exit_quote_source": "option_bar_close_no_bid_ask",
            },
            {
                "trade_date": "2025-01-03",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": -50.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250103C00100000",
                "option_entry_time": "2025-01-03T15:00:00Z",
                "option_exit_time": "2025-01-03T16:00:00Z",
                "quantity": 1,
                "entry_quote_age_seconds": 0.0,
                "exit_quote_age_seconds": 0.0,
                "entry_quote_source": "option_bar_close_no_bid_ask",
                "exit_quote_source": "option_bar_close_no_bid_ask",
            },
        ]
    ).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 1.0,
                        "research_only_dollars": 25_000.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "out_fill_haircut"
    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=output_dir,
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        fill_model_enabled=True,
        fill_model_haircut_positive_pnl=True,
    )

    haircut = packet["projection_hardening"]["fill_probability_pnl_haircut"]
    assert haircut["status"] == "enabled"
    assert haircut["adjusted_trade_count"] == 1
    assert haircut["total_positive_pnl_before_haircut"] == 100.0
    assert haircut["total_positive_pnl_after_haircut"] == 41.25
    scaled_trades = pd.read_csv(output_dir / "portfolio_growth_scaled_trades.csv")
    assert scaled_trades["source_option_pnl_before_fill_haircut"].round(2).tolist() == [
        100.0,
        -50.0,
    ]
    assert scaled_trades["source_option_pnl"].round(2).tolist() == [41.25, -50.0]
    assert scaled_trades["fill_probability_pnl_multiplier"].round(4).tolist() == [0.4125, 1.0]


def test_train_test_and_diversification_constraints_report(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    profile_dir = replay_root / "profile_a"
    profile_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 100.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250102C00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T16:00:00Z",
                "quantity": 1,
            },
            {
                "trade_date": "2025-01-06",
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 75.0,
                "symbol": "QQQ",
                "contract_symbol": "QQQ250106C00100000",
                "option_entry_time": "2025-01-06T15:00:00Z",
                "option_exit_time": "2025-01-06T16:00:00Z",
                "quantity": 1,
            },
            {
                "trade_date": "2025-01-02",
                "candidate_variant_id": "iwm_bear",
                "option_pnl": 60.0,
                "symbol": "IWM",
                "contract_symbol": "IWM250102P00100000",
                "option_entry_time": "2025-01-02T15:00:00Z",
                "option_exit_time": "2025-01-02T16:00:00Z",
                "quantity": 1,
            },
            {
                "trade_date": "2025-01-06",
                "candidate_variant_id": "iwm_bear",
                "option_pnl": -200.0,
                "symbol": "IWM",
                "contract_symbol": "IWM250106P00100000",
                "option_entry_time": "2025-01-06T15:00:00Z",
                "option_exit_time": "2025-01-06T16:00:00Z",
                "quantity": 1,
            },
        ]
    ).to_csv(profile_dir / "option_aware_trade_economics.csv", index=False)
    portfolio_path = tmp_path / "portfolio_report.json"
    portfolio_path.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg_repair",
                        "intended_regime": "bull",
                        "research_only_weight": 0.5,
                        "research_only_dollars": 12_500.0,
                    },
                    {
                        "candidate_variant_id": "iwm_bear__profile_profile-a",
                        "base_candidate_variant_id": "iwm_bear",
                        "aggregate_profile": "profile_a",
                        "symbol": "IWM",
                        "family": "debit_put_vertical",
                        "intended_regime": "bear",
                        "research_only_weight": 0.5,
                        "research_only_dollars": 12_500.0,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "out_train_test"
    packet = build_growth_projection(
        portfolio_report_json=portfolio_path,
        replay_root=replay_root,
        output_dir=output_dir,
        initial_cash=25_000.0,
        target_equity=300_000.0,
        backtest_allocation_fraction=0.05,
        annual_trading_days=252,
        projection_years=1,
        bootstrap_runs=25,
        seed=1,
        optimization_train_end_date="2025-01-03",
        optimization_min_train_trades=1,
        optimization_min_test_trades=1,
        optimization_max_per_symbol_regime=1,
        diversification_min_families=3,
    )

    train_test = packet["projection_hardening"]["train_test_optimization"]
    diversification = packet["projection_hardening"]["diversification_constraints"]
    assert train_test["status"] == "enabled"
    assert train_test["eligible_candidate_count"] == 1
    assert train_test["selected_candidate_count"] == 1
    assert train_test["blocker_counts"]["test_pnl_not_positive"] == 1
    assert diversification["status"] == "failed"
    assert "min_families" in diversification["failure_reasons"]
    assert "diversification_constraints_failed" in packet["evidence_grade"]["blockers"]
    assert (output_dir / "portfolio_growth_train_test_candidates.csv").exists()

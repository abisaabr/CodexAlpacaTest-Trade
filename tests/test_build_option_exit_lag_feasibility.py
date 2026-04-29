from __future__ import annotations

from scripts.build_option_exit_lag_feasibility import (
    _candidate_summaries,
    _summarize_lag_rows,
    classify_missing_exit,
)


def test_classify_missing_exit_identifies_data_and_timing_modes() -> None:
    assert (
        classify_missing_exit(
            trade_print_count=3,
            first_available_lag_minutes=None,
            lag_minutes=10,
            max_probe_lag_minutes=180,
        )
        == "bar_gap_trade_print_available"
    )
    assert (
        classify_missing_exit(
            trade_print_count=0,
            first_available_lag_minutes=25,
            lag_minutes=10,
            max_probe_lag_minutes=180,
        )
        == "short_lag_execution_timing_mismatch"
    )
    assert (
        classify_missing_exit(
            trade_print_count=0,
            first_available_lag_minutes=140,
            lag_minutes=10,
            max_probe_lag_minutes=180,
        )
        == "late_exit_liquidity_window"
    )
    assert (
        classify_missing_exit(
            trade_print_count=0,
            first_available_lag_minutes=None,
            lag_minutes=10,
            max_probe_lag_minutes=180,
        )
        == "illiquid_or_missing_exit_data"
    )


def test_candidate_summaries_mark_wide_lag_only_as_not_promotion() -> None:
    candidate = {
        "candidate_variant_id": "nvda_candidate",
        "symbol": "NVDA",
        "source_strategy_id": "nvda_strategy",
        "directional_option_type": "call",
    }
    trade_rows = [
        {
            "candidate_variant_id": "nvda_candidate",
            "source_trade_index": 0,
            "exit_lag_minutes": 10,
            "exit_status": "filled",
            "exit_missing_classification": "filled",
            "first_available_exit_lag_minutes": 0,
        },
        {
            "candidate_variant_id": "nvda_candidate",
            "source_trade_index": 1,
            "exit_lag_minutes": 10,
            "exit_status": "missing_exit_bar",
            "exit_missing_classification": "short_lag_execution_timing_mismatch",
            "exit_trade_print_count": 0,
            "first_available_exit_lag_minutes": 45,
        },
        {
            "candidate_variant_id": "nvda_candidate",
            "source_trade_index": 0,
            "exit_lag_minutes": 60,
            "exit_status": "filled",
            "exit_missing_classification": "filled",
            "first_available_exit_lag_minutes": 0,
        },
        {
            "candidate_variant_id": "nvda_candidate",
            "source_trade_index": 1,
            "exit_lag_minutes": 60,
            "exit_status": "filled",
            "exit_missing_classification": "filled",
            "first_available_exit_lag_minutes": 45,
        },
    ]

    profiles = _summarize_lag_rows(
        trade_rows,
        candidate=candidate,
        exit_lags=[10, 60],
        fill_coverage_gate=0.90,
    )
    summaries = _candidate_summaries(
        profiles,
        exit_lags=[10, 60],
        fill_coverage_gate=0.90,
    )

    assert profiles[0]["fill_coverage"] == 0.5
    assert profiles[0]["classification_counts"] == {
        "short_lag_execution_timing_mismatch": 1
    }
    assert profiles[1]["fill_coverage"] == 1.0
    assert summaries[0]["shortest_passing_exit_lag_minutes"] == 60
    assert summaries[0]["full_stack_fill_gate_pass"] is False
    assert summaries[0]["wide_lag_fill_gate_pass"] is True
    assert summaries[0]["recommendation"] == "exit_policy_research_candidate_not_promotion"

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.repair_projection_replay_lineage import build_lineage_repair


def test_lineage_repair_finds_search_root_and_copies_missing_csv(tmp_path: Path) -> None:
    portfolio = tmp_path / "portfolio.json"
    portfolio.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg",
                        "intended_regime": "bull",
                    },
                    {
                        "candidate_variant_id": "iwm_bear__profile_profile-b",
                        "base_candidate_variant_id": "iwm_bear",
                        "aggregate_profile": "profile_b",
                        "symbol": "IWM",
                        "family": "single_leg",
                        "intended_regime": "bear",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    current = tmp_path / "current" / "profile_a"
    current.mkdir(parents=True)
    pd.DataFrame([{"candidate_variant_id": "qqq_bull", "option_pnl": 1.0}]).to_csv(
        current / "option_aware_trade_economics.csv",
        index=False,
    )
    search = tmp_path / "search" / "profile_b"
    search.mkdir(parents=True)
    pd.DataFrame([{"candidate_variant_id": "iwm_bear", "option_pnl": 2.0}]).to_csv(
        search / "option_aware_trade_economics.csv",
        index=False,
    )

    summary = build_lineage_repair(
        portfolio_report_json=portfolio,
        replay_roots=[tmp_path / "current"],
        search_roots=[tmp_path / "search"],
        output_dir=tmp_path / "out",
        repaired_replay_root=tmp_path / "repaired",
    )

    assert summary["capital_plan_count"] == 2
    assert summary["matched_current_count"] == 1
    assert summary["repaired_from_search_count"] == 1
    assert summary["unmatched_count"] == 0
    assert summary["quote_quality_status_counts"] == {"quote_quality_gap": 2}
    assert (tmp_path / "repaired" / "profile_b" / "option_aware_trade_economics.csv").exists()
    quality = pd.read_csv(tmp_path / "out" / "quote_quality_lineage.csv")
    assert set(quality["recommended_data_action"]) == {
        "rerun_replay_with_bid_ask_spread_quote_age_and_trade_prints"
    }


def test_lineage_repair_marks_quote_backed_replay_ready(tmp_path: Path) -> None:
    portfolio = tmp_path / "portfolio.json"
    portfolio.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg",
                        "intended_regime": "bull",
                    },
                    {
                        "candidate_variant_id": "missing__profile_profile-b",
                        "base_candidate_variant_id": "missing",
                        "aggregate_profile": "profile_b",
                        "symbol": "IWM",
                        "family": "single_leg",
                        "intended_regime": "bear",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    current = tmp_path / "current" / "profile_a"
    current.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 1.0,
                "entry_quote_source": "option_quote_bid_ask",
                "exit_quote_source": "option_quote_bid_ask",
                "entry_average_relative_spread": 0.02,
                "exit_max_relative_spread": 0.03,
                "entry_quote_age_seconds": 0.2,
                "exit_quote_age_seconds": 0.4,
                "quote_backed_replay_status": "quote_backed_replay",
                "quote_backed_option_pnl": 0.75,
                "quote_backed_max_quote_age_seconds": 0.4,
                "quote_backed_max_relative_spread": 0.03,
                "entry_selection_trade_print_count": 3,
                "option_trade_print_count": 7,
            }
        ]
    ).to_csv(current / "option_aware_trade_economics.csv", index=False)

    summary = build_lineage_repair(
        portfolio_report_json=portfolio,
        replay_roots=[tmp_path / "current"],
        search_roots=[],
        output_dir=tmp_path / "out",
    )

    assert summary["quote_quality_status_counts"] == {
        "quote_backed_replay": 1,
        "replay_lineage_missing": 1,
    }
    quality = pd.read_csv(tmp_path / "out" / "quote_quality_lineage.csv")
    ready = quality[quality["base_candidate_variant_id"] == "qqq_bull"].iloc[0]
    assert ready["recommended_data_action"] == "projection_ready_quote_backed_replay"
    assert ready["entry_bid_ask_rate"] == 1.0
    assert ready["quote_backed_replay_rate"] == 1.0
    assert ready["quote_backed_pnl_coverage"] == 1.0
    assert ready["quote_backed_age_pass_rate"] == 1.0
    assert ready["quote_backed_spread_pass_rate"] == 1.0


def test_lineage_repair_rejects_bid_ask_fields_without_side_aware_replay_pnl(tmp_path: Path) -> None:
    portfolio = tmp_path / "portfolio.json"
    portfolio.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg",
                        "intended_regime": "bull",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    current = tmp_path / "current" / "profile_a"
    current.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 1.0,
                "entry_quote_source": "option_quote_bid_ask",
                "exit_quote_source": "option_quote_bid_ask",
                "entry_average_relative_spread": 0.02,
                "exit_max_relative_spread": 0.03,
                "entry_quote_age_seconds": 0.2,
                "exit_quote_age_seconds": 0.4,
            }
        ]
    ).to_csv(current / "option_aware_trade_economics.csv", index=False)

    summary = build_lineage_repair(
        portfolio_report_json=portfolio,
        replay_roots=[tmp_path / "current"],
        search_roots=[],
        output_dir=tmp_path / "out",
    )

    assert summary["quote_quality_status_counts"] == {"quote_quality_gap": 1}
    quality = pd.read_csv(tmp_path / "out" / "quote_quality_lineage.csv")
    row = quality.iloc[0]
    assert row["recommended_data_action"] == "rerun_replay_with_side_aware_quote_backed_pnl"
    assert pd.isna(row["quote_backed_replay_rate"])


def test_lineage_repair_rejects_stale_quote_backed_replay(tmp_path: Path) -> None:
    portfolio = tmp_path / "portfolio.json"
    portfolio.write_text(
        json.dumps(
            {
                "capital_plan": [
                    {
                        "candidate_variant_id": "qqq_bull__profile_profile-a",
                        "base_candidate_variant_id": "qqq_bull",
                        "aggregate_profile": "profile_a",
                        "symbol": "QQQ",
                        "family": "single_leg",
                        "intended_regime": "bull",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    current = tmp_path / "current" / "profile_a"
    current.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "qqq_bull",
                "option_pnl": 1.0,
                "entry_quote_source": "option_quote_bid_ask",
                "exit_quote_source": "option_quote_bid_ask",
                "entry_average_relative_spread": 0.02,
                "exit_max_relative_spread": 0.03,
                "entry_quote_age_seconds": 0.2,
                "exit_quote_age_seconds": 0.4,
                "quote_backed_replay_status": "quote_backed_replay",
                "quote_backed_option_pnl": 0.75,
                "quote_backed_max_quote_age_seconds": 120.0,
                "quote_backed_max_relative_spread": 0.03,
            }
        ]
    ).to_csv(current / "option_aware_trade_economics.csv", index=False)

    summary = build_lineage_repair(
        portfolio_report_json=portfolio,
        replay_roots=[tmp_path / "current"],
        search_roots=[],
        output_dir=tmp_path / "out",
    )

    assert summary["quote_quality_status_counts"] == {"quote_quality_gap": 1}
    quality = pd.read_csv(tmp_path / "out" / "quote_quality_lineage.csv")
    row = quality.iloc[0]
    assert row["recommended_data_action"] == "reject_or_repair_stale_or_wide_quote_backed_replay"
    assert row["quote_backed_age_pass_rate"] == 0.0

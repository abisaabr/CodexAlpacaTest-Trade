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
    assert (tmp_path / "repaired" / "profile_b" / "option_aware_trade_economics.csv").exists()

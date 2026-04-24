from __future__ import annotations

import json
from pathlib import Path

from scripts.summarize_gcp_research_runs import build_summary


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_research_summary_aggregates_candidates_and_quarantine(tmp_path: Path) -> None:
    run_dir = tmp_path / "research_wave_20260424_rq002_real_stock_bar_smoke_chunk_0004"
    _write_json(
        run_dir / "research_run_manifest.json",
        {
            "run_id": run_dir.name,
            "chunk_id": "chunk_0004",
            "evidence_mode": "real_stock_bar_smoke",
            "input_variant_count": 2,
            "result_summary": {"variant_count": 2},
        },
    )
    _write_json(
        run_dir / "normalized_backtest_results.json",
        [
            {
                "variant_id": "candidate",
                "symbol": "QQQ",
                "recommendation": "candidate_for_deeper_option_backtest",
                "net_expectancy_after_cost_proxy": 12.5,
                "expectancy_after_cost": 12.5,
                "net_pnl": 125.0,
                "actual_trade_count": 10,
                "broker_facing": False,
                "live_manifest_effect": "none",
                "risk_policy_effect": "none",
            },
            {
                "variant_id": "loser",
                "symbol": "QQQ",
                "recommendation": "quarantine",
                "net_expectancy_after_cost_proxy": -33.0,
                "expectancy_after_cost": -33.0,
                "net_pnl": -330.0,
                "actual_trade_count": 10,
                "broker_facing": False,
                "live_manifest_effect": "none",
                "risk_policy_effect": "none",
            },
        ],
    )

    payload = build_summary(
        run_root=tmp_path,
        run_id_prefix="research_wave_20260424_rq002_real_stock_bar_smoke",
        top_n=5,
    )

    assert payload["run_count"] == 1
    assert payload["variant_result_count"] == 2
    assert payload["promotion_allowed"] is False
    assert payload["recommendation_counts"] == {
        "candidate_for_deeper_option_backtest": 1,
        "quarantine": 1,
    }
    assert payload["top_candidates"][0]["variant_id"] == "candidate"
    assert payload["worst_quarantine"][0]["variant_id"] == "loser"


def test_research_summary_deduplicates_overlapping_runs(tmp_path: Path) -> None:
    for suffix, expectancy in [("a", 10.0), ("b", 12.0)]:
        run_dir = tmp_path / f"research_wave_20260424_rq002_real_stock_bar_smoke_{suffix}"
        _write_json(
            run_dir / "research_run_manifest.json",
            {
                "run_id": run_dir.name,
                "chunk_id": suffix,
                "evidence_mode": "real_stock_bar_smoke",
                "input_variant_count": 1,
                "result_summary": {"variant_count": 1},
            },
        )
        _write_json(
            run_dir / "normalized_backtest_results.json",
            [
                {
                    "variant_id": "duplicate",
                    "symbol": "GLD",
                    "recommendation": "candidate_for_deeper_option_backtest",
                    "net_expectancy_after_cost_proxy": expectancy,
                    "expectancy_after_cost": expectancy,
                    "net_pnl": expectancy * 10,
                    "actual_trade_count": 5,
                }
            ],
        )

    payload = build_summary(
        run_root=tmp_path,
        run_id_prefix="research_wave_20260424_rq002_real_stock_bar_smoke",
        top_n=5,
    )

    assert payload["raw_variant_result_count"] == 2
    assert payload["variant_result_count"] == 1
    assert payload["duplicate_variant_result_count"] == 1
    assert payload["top_candidates"][0]["expectancy_after_cost"] == 12.0

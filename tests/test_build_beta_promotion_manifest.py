from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

MODULE = importlib.import_module("build_beta_promotion_manifest")


def test_build_beta_promotion_manifest_includes_exact_live_and_gap_scope(tmp_path: Path) -> None:
    live_manifest_path = tmp_path / "live.yaml"
    live_manifest_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "strategies": [
                    {
                        "name": "qqq__reactive__call_backspread_next_expiry",
                        "underlying_symbol": "QQQ",
                        "family": "Call backspread",
                    },
                    {
                        "name": "qqq__fast__iron_butterfly_same_day",
                        "underlying_symbol": "QQQ",
                        "family": "Iron butterfly",
                    },
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    registry_path = tmp_path / "strategy_family_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-04-22T15:54:34-04:00",
                "families": [
                    {
                        "family": "Call backspread",
                        "priority": "live_benchmark",
                        "steward_action": "benchmark_against_current_live_book",
                        "structure_bucket": "backspread",
                        "directional_bias": "bull_convexity",
                        "strategy_sets": ["family_expansion"],
                        "base_strategies": [
                            "call_backspread_next_expiry",
                            "call_backspread_next_expiry_aggressive",
                        ],
                        "selected_base_strategy_count": 2,
                        "promoted_base_strategy_count": 2,
                        "selected_tickers": ["QQQ"],
                        "promoted_tickers": ["QQQ"],
                        "live_manifest_tickers": ["QQQ"],
                        "live_manifest_strategy_count": 2,
                        "promoted_ticker_count": 1,
                        "note": "Already live.",
                    },
                    {
                        "family": "Put backspread",
                        "priority": "promotion_follow_up",
                        "steward_action": "review_for_live_manifest_addition",
                        "structure_bucket": "backspread",
                        "directional_bias": "bear_convexity",
                        "strategy_sets": ["down_choppy_exhaustive", "family_expansion"],
                        "base_strategies": [
                            "put_backspread_next_expiry",
                            "put_backspread_next_expiry_aggressive",
                        ],
                        "selected_base_strategy_count": 2,
                        "promoted_base_strategy_count": 2,
                        "selected_tickers": ["AAPL", "AMD"],
                        "promoted_tickers": ["AAPL", "AMD"],
                        "live_manifest_tickers": [],
                        "live_manifest_strategy_count": 0,
                        "promoted_ticker_count": 2,
                        "note": "Promoted but not live.",
                    },
                    {
                        "family": "Credit put spread",
                        "promoted_ticker_count": 0,
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    document = MODULE.build_beta_promotion_manifest(
        live_manifest_path=live_manifest_path,
        strategy_family_registry_path=registry_path,
    )

    assert document["summary"]["exact_live_strategy_count"] == 2
    assert document["summary"]["promoted_family_count"] == 2
    assert document["summary"]["families_with_retest_gap_count"] == 1
    assert document["summary"]["retest_gap_underlying_symbols"] == ["AAPL", "AMD"]
    assert len(document["exact_live_strategies"]) == 2
    assert [row["family"] for row in document["beta_retest_families"]] == [
        "Call backspread",
        "Put backspread",
    ]
    call_backspread = document["beta_retest_families"][0]
    assert call_backspread["exact_promoted_base_strategies_available"] is True
    put_backspread = document["beta_retest_families"][1]
    assert put_backspread["exact_promoted_base_strategies_available"] is True
    assert put_backspread["retest_gap_tickers"] == ["AAPL", "AMD"]
    assert put_backspread["base_strategies"] == [
        "put_backspread_next_expiry",
        "put_backspread_next_expiry_aggressive",
    ]


def test_build_beta_promotion_manifest_marks_family_scope_as_superset_when_subset_is_unknown(
    tmp_path: Path,
) -> None:
    live_manifest_path = tmp_path / "live.yaml"
    live_manifest_path.write_text(yaml.safe_dump({"strategies": []}, sort_keys=False), encoding="utf-8")
    registry_path = tmp_path / "strategy_family_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-04-22T15:54:34-04:00",
                "families": [
                    {
                        "family": "Long straddle",
                        "priority": "promotion_follow_up",
                        "steward_action": "review_for_live_manifest_addition",
                        "structure_bucket": "long_vol",
                        "directional_bias": "choppy_or_expansion",
                        "strategy_sets": ["standard"],
                        "base_strategies": [
                            "long_straddle_next_expiry",
                            "long_straddle_same_day",
                            "long_straddle_same_day_d45",
                        ],
                        "selected_base_strategy_count": 3,
                        "promoted_base_strategy_count": 2,
                        "selected_tickers": ["AAPL"],
                        "promoted_tickers": ["AAPL"],
                        "live_manifest_tickers": [],
                        "live_manifest_strategy_count": 0,
                        "promoted_ticker_count": 1,
                        "note": "Promoted but not live.",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    document = MODULE.build_beta_promotion_manifest(
        live_manifest_path=live_manifest_path,
        strategy_family_registry_path=registry_path,
    )

    row = document["beta_retest_families"][0]
    assert row["exact_promoted_base_strategies_available"] is False
    assert "governed retest superset" in row["retest_scope_note"]

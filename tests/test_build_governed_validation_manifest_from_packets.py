from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.build_governed_validation_manifest_from_packets import build_manifest


def _candidate(
    *,
    candidate_id: str,
    family: str,
    symbol: str = "QQQ",
    regime: str = "bear",
    option_type: str = "put",
) -> dict[str, object]:
    return {
        "candidate_variant_id": candidate_id,
        "base_candidate_variant_id": candidate_id.rsplit("__profile_", 1)[0],
        "aggregate_profile": "unit_profile",
        "symbol": symbol,
        "strategy_id": f"{symbol.lower()}__{regime}__{option_type}__{family}",
        "source_strategy_id": f"{symbol.lower()}__{regime}__{option_type}__{family}",
        "family": family,
        "intended_regime": regime,
        "directional_option_type": option_type,
        "parameter_set": json.dumps(
            {
                "dte_mode": "next_expiry",
                "family_template": family,
                "hard_exit_minute": 55,
                "min_minutes_since_open": 35,
                "max_minutes_since_open": 210,
                "profit_target_multiple": 0.26,
                "stop_loss_multiple": 0.09,
                "stock_proxy_mode": "breakout",
                "entry_signal_mode": "rising_edge",
                "liquidity_gate": "tight",
            }
        ),
        "min_fill_coverage": 1.0,
        "min_data_foundation_coverage": 1.0,
        "min_option_trade_count": 25,
        "min_net_pnl": 100.0,
        "min_test_net_pnl": 50.0,
        "promotion_status": "eligible_for_promotion_review",
        "promotion_blockers": [],
    }


def test_manifest_builder_includes_native_multileg_families(
    tmp_path: Path,
) -> None:
    packet_path = tmp_path / "packet.json"
    packet_path.write_text(
        json.dumps(
            {
                "decision": "ready_for_governed_validation_review",
                "gate_summary": {"eligible_for_promotion_review_count": 2},
                "review_candidates": [
                    _candidate(
                        candidate_id="portfolio12h__qqq__bear__put__single_leg_repair__abc__profile_unit",
                        family="single_leg_repair",
                    ),
                    _candidate(
                        candidate_id="portfolio12h__iwm__bull__put__bull_put_credit_spread__def__profile_unit",
                        family="bull_put_credit_spread",
                        symbol="IWM",
                        regime="bull",
                    ),
                ],
            }
        ),
        encoding="utf-8",
    )

    manifest = build_manifest(
        packet_paths=[packet_path],
        output_path=tmp_path / "manifest.yaml",
        generated_for="unit",
        packet_uris=[],
    )

    assert manifest["strategy_count"] == 2
    assert manifest["skipped_candidates"] == []

    strategies = {
        strategy["source_strategy_id"]: strategy for strategy in manifest["strategies"]
    }
    assert (
        strategies["qqq__bear__put__single_leg_repair"]["runner_semantics_status"]
        == "packet_translated_to_runtime_single_leg"
    )

    spread = strategies["iwm__bull__put__bull_put_credit_spread"]
    assert spread["runner_semantics_status"] == "packet_translated_to_runtime_native_multileg"
    assert [(leg["option_type"], leg["side"]) for leg in spread["legs"]] == [
        ("put", "short"),
        ("put", "long"),
    ]
    assert [leg["target_delta"] for leg in spread["legs"]] == pytest.approx([-0.55, -0.37])
    assert [leg["min_abs_delta"] for leg in spread["legs"]] == pytest.approx([0.35, 0.17])
    assert [leg["max_abs_delta"] for leg in spread["legs"]] == pytest.approx([0.75, 0.57])

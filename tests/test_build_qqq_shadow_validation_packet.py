from __future__ import annotations

from pathlib import Path

from alpaca_lab.multi_ticker_portfolio.config import load_portfolio_config
from alpaca_lab.multi_ticker_portfolio.signals import get_timing_profile
from scripts.build_qqq_shadow_validation_packet import build_shadow_validation_packet


def test_qqq_governed_shadow_config_is_no_order_and_q_only() -> None:
    config = load_portfolio_config("config/multi_ticker_qqq_governed_shadow_validation.yaml")

    assert config.execution.submit_paper_orders is False
    assert tuple(config.execution.underlying_symbols) == ("QQQ",)
    assert config.ownership.lease_backend == "gcs_generation_match"
    assert len(config.strategies) == 3
    assert {strategy.regime for strategy in config.strategies} == {"bull", "bear", "choppy"}
    assert {strategy.timing_profile for strategy in config.strategies} == {"governed_late"}
    assert {strategy.runner_semantics_status for strategy in config.strategies} == {
        "proxy_shadow_not_promotion_equivalent"
    }


def test_governed_late_timing_profile_matches_research_cutoff() -> None:
    profile = get_timing_profile("governed_late")

    assert profile.trend_start == 330
    assert profile.credit_minute == 330
    assert profile.condor_minute == 330


def test_build_qqq_shadow_validation_packet(tmp_path: Path) -> None:
    packet = build_shadow_validation_packet(
        promotion_manifest_path=Path(
            "config/promotion_manifests/qqq_option_native_governed_validation_20260430.yaml"
        ),
        shadow_config_path=Path("config/multi_ticker_qqq_governed_shadow_validation.yaml"),
        output_dir=tmp_path,
    )

    assert packet["decision"] == "ready_for_no_order_gcp_shadow_validation"
    assert packet["ready_for_no_order_gcp_shadow_run"] is True
    assert packet["ready_for_broker_facing_paper"] is False
    assert {check["status"] for check in packet["checks"]} == {"passed"}
    assert len(packet["shadow_strategies"]) == 3
    assert all("--no-submit-paper-orders" in command for command in packet["commands"][1:])
    assert (tmp_path / "qqq_governed_shadow_validation_packet.json").exists()
    assert (tmp_path / "qqq_governed_shadow_validation_packet.md").exists()

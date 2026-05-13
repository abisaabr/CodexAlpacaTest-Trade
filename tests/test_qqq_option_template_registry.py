from __future__ import annotations

from pathlib import Path

import pytest

from alpaca_lab.options.order_templates import (
    OptionLegTemplate,
    qqq_option_native_templates,
)
from scripts.build_qqq_option_template_registry import build_template_registry


def test_qqq_option_native_templates_cover_target_regimes_and_multileg() -> None:
    templates = qqq_option_native_templates()

    regimes = {template.intended_regime for template in templates}
    assert regimes == {"bull", "bear", "choppy"}
    assert any(template.family == "iron_butterfly" and template.is_multi_leg for template in templates)
    assert any(template.family == "call_backspread" for template in templates)
    assert any(template.family == "put_backspread" for template in templates)
    assert any(template.family == "call_credit_spread" for template in templates)
    assert all(template.leg_count >= 1 for template in templates)


def test_leg_template_validates_quantity_and_dte() -> None:
    with pytest.raises(ValueError, match="quantity"):
        OptionLegTemplate("bad_leg", "call", "buy", quantity=0)
    with pytest.raises(ValueError, match="DTE"):
        OptionLegTemplate("bad_dte", "put", "sell", min_dte=5, max_dte=1)


def test_build_template_registry_writes_research_only_packet(tmp_path: Path) -> None:
    packet = build_template_registry(output_dir=tmp_path / "out")

    assert packet["broker_facing"] is False
    assert packet["live_manifest_effect"] == "none"
    assert packet["risk_policy_effect"] == "none"
    assert packet["regime_counts"] == {"bear": 7, "bull": 5, "choppy": 6}
    assert packet["template_count"] == 18
    assert (tmp_path / "out" / "qqq_option_template_registry.json").exists()
    assert (tmp_path / "out" / "qqq_option_template_registry.md").exists()

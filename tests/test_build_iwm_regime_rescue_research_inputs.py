from __future__ import annotations

import json
from pathlib import Path

from scripts.build_iwm_regime_rescue_research_inputs import build_iwm_regime_rescue_rows, main
from scripts.build_qqq_regime_research_inputs import build_queue


def test_iwm_regime_rescue_rows_target_missing_regimes_only() -> None:
    rows = build_iwm_regime_rescue_rows(symbol="IWM", wave_id="test_wave")

    regimes = {row["source_strategy_id"].split("__")[1] for row in rows}
    families = {row["source_strategy_id"].split("__")[3] for row in rows}

    assert regimes == {"bear", "choppy"}
    assert "single_leg_repair" in families
    assert "debit_put_vertical" in families
    assert "bear_call_credit_spread" in families
    assert "iron_butterfly" in families
    assert len(rows) == 156
    assert all(row["broker_facing"] is False for row in rows)
    assert all(row["live_manifest_effect"] == "none" for row in rows)
    assert all(row["risk_policy_effect"] == "none" for row in rows)


def test_iwm_regime_rescue_queue_preserves_identity_and_research_only_state() -> None:
    rows = build_iwm_regime_rescue_rows(symbol="IWM", wave_id="test_wave")
    queue = build_queue(rows=rows, wave_id="test_wave")

    assert queue["queue_item_count"] == len(rows)
    assert queue["broker_facing"] is False
    assert queue["promotion_allowed"] is False
    assert {item["intended_regime"] for item in queue["queue_items"]} == {"bear", "choppy"}
    assert all(item["candidate_variant_id"] for item in queue["queue_items"])
    assert all(item["source_strategy_id"].startswith("iwm__") for item in queue["queue_items"])


def test_iwm_choppy_signal_delay_family_filter_builds_bounded_queue() -> None:
    rows = build_iwm_regime_rescue_rows(
        symbol="IWM",
        wave_id="test_wave",
        target_regimes={"choppy"},
        choppy_families={"debit_put_vertical"},
        choppy_signal_delay_bars=[1, 2],
    )

    assert len(rows) == 36
    assert {row["parameters"]["family_template"] for row in rows} == {"debit_put_vertical"}
    assert {row["parameters"]["signal_delay_bars"] for row in rows} == {1, 2}
    assert {row["source_strategy_id"].split("__")[1] for row in rows} == {"choppy"}
    assert all(row["broker_facing"] is False for row in rows)


def test_iwm_choppy_timewindow_refine_builds_high_fill_single_leg_grid() -> None:
    rows = build_iwm_regime_rescue_rows(
        symbol="IWM",
        wave_id="test_wave",
        target_regimes={"choppy"},
        choppy_profile_set="timewindow_refine",
    )

    assert len(rows) == 144
    assert {row["source_strategy_id"].split("__")[1] for row in rows} == {"choppy"}
    assert {row["parameters"]["family_template"] for row in rows} == {"single_leg_repair"}
    assert {row["parameters"]["range_entry_side"] for row in rows} == {"lower_band"}
    assert {row["parameters"]["stock_proxy_mode"] for row in rows} == {"range_bound"}
    assert {row["parameters"]["min_minutes_since_open"] for row in rows} == {
        90,
        105,
        120,
        135,
    }
    assert min(row["parameters"]["max_minutes_since_open"] for row in rows) == 135
    assert max(row["parameters"]["max_minutes_since_open"] for row in rows) == 165
    assert all(row["broker_facing"] is False for row in rows)


def test_iwm_choppy_timewindow_micro_exit_builds_tight_exit_grid() -> None:
    rows = build_iwm_regime_rescue_rows(
        symbol="IWM",
        wave_id="test_wave",
        target_regimes={"choppy"},
        choppy_profile_set="timewindow_micro_exit",
    )

    assert len(rows) == 108
    assert {row["parameters"]["family_template"] for row in rows} == {"single_leg_repair"}
    assert {row["parameters"]["range_entry_side"] for row in rows} == {"lower_band"}
    assert min(row["parameters"]["option_stop_loss_pct"] for row in rows) == 0.05
    assert max(row["parameters"]["option_stop_loss_pct"] for row in rows) == 0.10
    assert min(row["parameters"]["option_profit_target_pct"] for row in rows) == 0.12
    assert max(row["parameters"]["option_profit_target_pct"] for row in rows) == 0.25
    assert {row["parameters"]["min_option_hold_minutes"] for row in rows} == {1, 2}
    assert all(row["broker_facing"] is False for row in rows)


def test_iwm_choppy_timewindow_quality_filter_builds_stricter_choppy_grid() -> None:
    rows = build_iwm_regime_rescue_rows(
        symbol="IWM",
        wave_id="test_wave",
        target_regimes={"choppy"},
        choppy_profile_set="timewindow_quality_filter",
    )

    assert len(rows) == 288
    assert {row["parameters"]["family_template"] for row in rows} == {
        "broken_wing_call_butterfly",
        "debit_call_vertical",
        "single_leg_repair",
    }
    assert {row["parameters"]["range_entry_side"] for row in rows} == {"lower_band"}
    assert max(row["parameters"]["max_range_pct"] for row in rows) == 0.006
    assert min(row["parameters"]["max_range_pct"] for row in rows) == 0.004
    assert max(row["parameters"]["max_trend_gap_pct"] for row in rows) == 0.001
    assert min(row["parameters"]["max_midpoint_distance_pct"] for row in rows) == 0.003
    assert {row["parameters"]["quality_profile"] for row in rows} == {
        "defined_range_low_trend",
        "narrow_low_trend",
        "ultra_narrow_low_trend",
    }
    assert all(row["broker_facing"] is False for row in rows)


def test_iwm_regime_rescue_cli_writes_expected_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_iwm_regime_rescue_research_inputs.py",
            "--symbol",
            "IWM",
            "--wave-id",
            "test_wave",
            "--output-dir",
            str(tmp_path),
        ],
    )
    main()

    variants_path = tmp_path / "iwm_regime_rescue_variants.jsonl"
    queue_path = tmp_path / "iwm_regime_rescue_option_queue.json"
    manifest_path = tmp_path / "iwm_regime_rescue_manifest.json"
    assert variants_path.exists()
    assert queue_path.exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    assert manifest["target_regimes"] == ["bear", "choppy"]
    assert manifest["template_count"] == 156
    assert queue["queue_item_count"] == 156

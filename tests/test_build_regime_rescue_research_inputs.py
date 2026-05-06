from __future__ import annotations

import json
from pathlib import Path

from scripts.build_regime_rescue_research_inputs import main


def test_symbol_generic_regime_rescue_cli_writes_symbol_prefixed_files(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_regime_rescue_research_inputs.py",
            "--symbol",
            "SPY",
            "--wave-id",
            "test_wave",
            "--output-dir",
            str(tmp_path),
        ],
    )
    main()

    variants_path = tmp_path / "spy_regime_rescue_variants.jsonl"
    queue_path = tmp_path / "spy_regime_rescue_option_queue.json"
    manifest_path = tmp_path / "spy_regime_rescue_manifest.json"
    assert variants_path.exists()
    assert queue_path.exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    assert manifest["broker_facing"] is False
    assert manifest["target_symbols"] == ["SPY"]
    assert manifest["target_regimes"] == ["bear", "choppy"]
    assert manifest["template_count"] == 156
    assert manifest["promotion_allowed"] is False
    assert queue["queue_item_count"] == 156
    assert {item["symbol"] for item in queue["queue_items"]} == {"SPY"}
    assert {item["intended_regime"] for item in queue["queue_items"]} == {"bear", "choppy"}


def test_symbol_generic_bear_signal_window_refine_builds_strict_bear_grid(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_regime_rescue_research_inputs.py",
            "--symbol",
            "QQQ",
            "--wave-id",
            "test_wave",
            "--output-dir",
            str(tmp_path),
            "--target-regimes",
            "bear",
            "--bear-profile-set",
            "signal_window_refine",
        ],
    )
    main()

    variants_path = tmp_path / "qqq_regime_rescue_variants.jsonl"
    rows = [
        json.loads(line)
        for line in variants_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 54
    assert {row["symbol"] for row in rows} == {"QQQ"}
    assert {row["source_strategy_id"].split("__")[1] for row in rows} == {"bear"}
    assert {row["parameters"]["family_template"] for row in rows} == {"single_leg_repair"}
    assert {row["parameters"]["breakout_window"] for row in rows} == {26, 34, 45}
    assert min(row["parameters"]["min_trend_gap_pct"] for row in rows) == 0.0009
    assert max(row["parameters"]["min_trend_gap_pct"] for row in rows) == 0.002


def test_symbol_generic_full_regime_grid_includes_baseline_bull_rows(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_regime_rescue_research_inputs.py",
            "--symbol",
            "AAPL",
            "--wave-id",
            "test_wave",
            "--output-dir",
            str(tmp_path),
            "--target-regimes",
            "bull,bear,choppy",
        ],
    )
    main()

    manifest = json.loads((tmp_path / "aapl_regime_rescue_manifest.json").read_text())
    rows = [
        json.loads(line)
        for line in (tmp_path / "aapl_regime_rescue_variants.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]

    assert manifest["target_regimes"] == ["bear", "bull", "choppy"]
    assert manifest["bull_profile_set"] == "baseline"
    assert manifest["template_count"] == 172
    assert len(rows) == 172
    assert {row["symbol"] for row in rows} == {"AAPL"}
    assert {row["source_strategy_id"].split("__")[1] for row in rows} == {
        "bull",
        "bear",
        "choppy",
    }
    bull_rows = [
        row for row in rows if row["source_strategy_id"].split("__")[1] == "bull"
    ]
    assert len(bull_rows) == 16
    assert {row["parameters"]["stock_proxy_mode"] for row in bull_rows} == {"breakout"}


def test_symbol_generic_bull_momentum_refine_builds_bull_grid(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_regime_rescue_research_inputs.py",
            "--symbol",
            "ORCL",
            "--wave-id",
            "test_wave",
            "--output-dir",
            str(tmp_path),
            "--target-regimes",
            "bull",
            "--bull-profile-set",
            "momentum_refine",
        ],
    )
    main()

    manifest = json.loads((tmp_path / "orcl_regime_rescue_manifest.json").read_text())
    rows = [
        json.loads(line)
        for line in (tmp_path / "orcl_regime_rescue_variants.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]

    assert manifest["target_regimes"] == ["bull"]
    assert manifest["bull_profile_set"] == "momentum_refine"
    assert manifest["template_count"] == 162
    assert len(rows) == 162
    assert {row["symbol"] for row in rows} == {"ORCL"}
    assert {row["source_strategy_id"].split("__")[1] for row in rows} == {"bull"}
    assert {row["parameters"]["family_template"] for row in rows} == {
        "debit_call_vertical",
        "single_leg_repair",
    }
    assert {row["parameters"]["option_exit_mode"] for row in rows} == {
        "premium_target_stop"
    }
    assert {row["parameters"]["breakout_window"] for row in rows} == {21, 34, 55}

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.run_gcp_research_wave import filter_variants, load_variants, run, score_variant


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_filter_variants_uses_chunk_and_limits(tmp_path: Path) -> None:
    variants = [
        {"variant_id": f"v{i}", "queue_id": "q", "priority": 1, "symbol": "QQQ"}
        for i in range(10)
    ]
    manifest = {"chunks": [{"chunk_id": "chunk_0001", "start_index": 2, "end_index": 7}]}

    selected = filter_variants(
        variants,
        wave_manifest=manifest,
        chunk_id="chunk_0001",
        queue_ids=set(),
        symbols={"QQQ"},
        priorities={1},
        max_variants=3,
    )

    assert [row["variant_id"] for row in selected] == ["v2", "v3", "v4"]


def test_score_variant_keeps_metadata_proxy_non_promotable() -> None:
    row = score_variant(
        {
            "variant_id": "rq002__qqq__tight",
            "queue_id": "RQ-002",
            "priority": 2,
            "symbol": "QQQ",
            "variant_type": "single_leg_repair",
            "parameters": {"liquidity_gate": "tight", "stop_loss_multiple": 0.18},
        },
        evidence_mode="metadata_proxy_smoke",
    )

    assert row["recommendation"] == "hold_for_real_backtest"
    assert row["broker_facing"] is False
    assert row["live_manifest_effect"] == "none"


def test_run_writes_required_research_artifacts(tmp_path: Path) -> None:
    variants_path = tmp_path / "variants.jsonl"
    manifest_path = tmp_path / "wave.json"
    output_dir = tmp_path / "reports"
    _write_jsonl(
        variants_path,
        [
            {
                "variant_id": "rq001__qqq__debit_call_vertical__fast__same_day",
                "queue_id": "RQ-001-defined-risk-family-expansion",
                "priority": 1,
                "symbol": "QQQ",
                "variant_type": "defined_risk_family_expansion",
                "parameters": {
                    "family_template": "debit_call_vertical",
                    "timing_profile": "fast",
                    "dte_mode": "same_day",
                },
            },
            {
                "variant_id": "rq003__pltr__diagnostic",
                "queue_id": "RQ-003-loser-cluster-shadow-diagnostics",
                "priority": 3,
                "symbol": "PLTR",
                "variant_type": "loser_cluster_shadow_diagnostic",
                "parameters": {"avoid_after_loser_similarity": True},
            },
        ],
    )
    _write_json(
        manifest_path,
        {
            "wave_id": "test_wave",
            "chunks": [{"chunk_id": "chunk_0001", "start_index": 0, "end_index": 1}],
        },
    )
    args = argparse.Namespace(
        variants_jsonl=str(variants_path),
        wave_manifest_json=str(manifest_path),
        output_dir=str(output_dir),
        run_id="unit_run",
        chunk_id="chunk_0001",
        queue_id=[],
        symbol=[],
        priority=[],
        max_variants=None,
        evidence_mode="metadata_proxy_smoke",
        allow_non_smoke_evidence=False,
    )

    result = run(args)

    assert result["selected_variant_count"] == 2
    assert result["broker_facing"] is False
    for path in result["artifacts"].values():
        assert Path(path).exists()
    manifest = json.loads(Path(result["artifacts"]["research_run_manifest"]).read_text())
    assert manifest["required_outputs"] == [
        "research_run_manifest",
        "normalized_backtest_results",
        "train_test_or_walk_forward_summary",
        "after_cost_expectancy_table",
        "drawdown_and_tail_loss_report",
        "loser_cluster_comparison",
        "candidate_hold_kill_quarantine_recommendation",
    ]
    loaded = load_variants(variants_path)
    assert len(loaded) == 2

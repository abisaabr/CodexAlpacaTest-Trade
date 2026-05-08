from __future__ import annotations

from pathlib import Path

from scripts.hourly_gcp_research_sweep import (
    DEFAULT_GCS_RESULTS_ROOT,
    extract_worker_instance,
    summarize_promotion_packet,
)


def test_extract_worker_instance_rejects_missing_metadata() -> None:
    instance = {"name": "vm1", "zone": "projects/x/zones/us-central1-a", "status": "TERMINATED"}
    assert extract_worker_instance(instance, results_root=DEFAULT_GCS_RESULTS_ROOT) is None


def test_extract_worker_instance_accepts_research_worker() -> None:
    instance = {
        "name": "micro-event-c00001-00002-x",
        "zone": "projects/x/zones/us-central1-a",
        "status": "TERMINATED",
        "metadata": {
            "items": [
                {"key": "gcs_prefix", "value": "gs://codexalpaca-control-us/research_results/wave_20260508T0000Z"},
                {"key": "worker_id", "value": "micro_event_c00001_00002"},
            ]
        },
    }
    worker = extract_worker_instance(instance, results_root=DEFAULT_GCS_RESULTS_ROOT)
    assert worker is not None
    assert worker.wave_id == "wave_20260508T0000Z"
    assert worker.worker_id == "micro_event_c00001_00002"
    assert worker.worker_gcs_uri.endswith("/workers/micro_event_c00001_00002")


def test_extract_worker_instance_ignores_non_research_prefix() -> None:
    instance = {
        "name": "vm1",
        "zone": "projects/x/zones/us-central1-a",
        "status": "TERMINATED",
        "metadata": {
            "items": [
                {"key": "gcs_prefix", "value": "gs://some-other-bucket/whatever"},
                {"key": "worker_id", "value": "w1"},
                {"key": "wave_id", "value": "wave"},
            ]
        },
    }
    assert extract_worker_instance(instance, results_root=DEFAULT_GCS_RESULTS_ROOT) is None


def test_summarize_promotion_packet_counts_eligible_and_blocked() -> None:
    payload = {
        "decision": "research_only_blocked",
        "candidate_level_decision": "research_only_blocked",
        "promotion_scope": "research_governed_validation_review_only",
        "gate_summary": {"eligible_for_promotion_review_count": 2, "unique_eligible_base_candidate_count": 1},
        "review_candidates": [
            {"promotion_status": "eligible_for_promotion_review"},
            {"promotion_status": "eligible_for_promotion_review"},
            {"promotion_status": "research_only_blocked"},
        ],
    }
    summary = summarize_promotion_packet(payload, path=Path("x") / "research_promotion_review_packet.json")
    assert summary.eligible_for_promotion_review_count == 2
    assert summary.unique_eligible_base_candidate_count == 1
    assert summary.eligible_candidates == 2
    assert summary.blocked_candidates == 1


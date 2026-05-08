from __future__ import annotations

from pathlib import Path

from scripts.hourly_gcp_research_sweep import (
    DEFAULT_GCS_RESULTS_ROOT,
    _build_status_note,
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


def test_build_status_note_renders_concise_summary() -> None:
    packet = {
        "generated_at_utc": "2026-05-08T00:00:00+00:00",
        "project": "codexalpaca",
        "run_id": "run123",
        "dry_run": False,
        "delete_synced_terminated": True,
        "instance_scan": {
            "instance_count": 10,
            "terminated_research_worker_count": 2,
            "running_research_worker_count": 1,
        },
        "sync_summary": {"jobs": 2, "synced": 1, "empty": 1, "errors": 0},
        "promotion_summary": {
            "packets_scanned": 1,
            "packets_with_eligible_count_gt_0": 0,
            "eligible_review_candidates": 0,
            "blocked_review_candidates": 3,
        },
        "cleanup_summary": {"deleted": 1, "skipped": 1, "errors": 0},
    }
    note = _build_status_note(packet)
    assert "# Hourly GCP Research Sweep" in note
    assert "Jobs: `2` (synced `1`, empty `1`, errors `0`)" in note
    assert "Skipped (not synced / dry-run / no delete flag): `1`" in note

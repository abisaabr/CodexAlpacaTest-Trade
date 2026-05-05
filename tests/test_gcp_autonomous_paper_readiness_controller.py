import argparse
import json

from scripts import gcp_autonomous_paper_readiness_controller as controller


def test_aggregate_phase_reads_nested_promotion_packet(monkeypatch) -> None:
    args = argparse.Namespace(full_prefix="gs://control/wave")
    status_uri = f"{args.full_prefix}/{controller.AGGREGATE_STATUS_PATH}"
    nested_packet_uri = (
        f"{args.full_prefix}/{controller.AGGREGATE_PROMOTION_PACKET_PATHS[0]}"
    )
    objects = {
        status_uri: {"phase": "aggregate_completed"},
        nested_packet_uri: {
            "decision": "operator_review_ready_no_orders",
            "gate_summary": {"eligible_for_promotion_review_count": 2},
        },
    }

    monkeypatch.setattr(
        controller,
        "gcs_object_exists",
        lambda _args, uri: uri in objects,
    )
    monkeypatch.setattr(
        controller,
        "storage_cat",
        lambda _args, uri: json.dumps(objects[uri]),
    )

    aggregate = controller.aggregate_phase(args)

    assert aggregate["status"]["phase"] == "aggregate_completed"
    assert aggregate["packet_uri"] == nested_packet_uri
    assert aggregate["packet_found"] is True
    assert aggregate["decision"] == "operator_review_ready_no_orders"
    assert aggregate["eligible_for_promotion_review_count"] == 2


def test_aggregate_phase_keeps_legacy_flat_packet_fallback(monkeypatch) -> None:
    args = argparse.Namespace(full_prefix="gs://control/wave")
    status_uri = f"{args.full_prefix}/{controller.AGGREGATE_STATUS_PATH}"
    flat_packet_uri = (
        f"{args.full_prefix}/{controller.AGGREGATE_PROMOTION_PACKET_PATHS[1]}"
    )
    objects = {
        status_uri: {"phase": "aggregate_completed"},
        flat_packet_uri: {
            "decision": "blocked_no_eligible_candidates",
            "gate_summary": {"eligible_for_promotion_review_count": 0},
        },
    }

    monkeypatch.setattr(
        controller,
        "gcs_object_exists",
        lambda _args, uri: uri in objects,
    )
    monkeypatch.setattr(
        controller,
        "storage_cat",
        lambda _args, uri: json.dumps(objects[uri]),
    )

    aggregate = controller.aggregate_phase(args)

    assert aggregate["packet_uri"] == flat_packet_uri
    assert aggregate["packet_found"] is True
    assert aggregate["eligible_for_promotion_review_count"] == 0


def test_controller_pass_short_circuits_completed_aggregate(monkeypatch) -> None:
    args = argparse.Namespace(
        micro_prefix="gs://control/micro",
        full_prefix="gs://control/full",
        micro_summary_threshold=72,
    )
    selection = {
        "selected_profile": "strict-e0x60",
        "selected_expansion_selectors": ["entry_liquidity_first_research_only"],
        "expected_summary_count": 18,
    }
    written: dict[str, object] = {}

    monkeypatch.setattr(controller, "sync_source_and_inputs", lambda _args: None)
    monkeypatch.setattr(controller, "compute_instances", lambda _args: [])
    monkeypatch.setattr(
        controller,
        "count_objects",
        lambda _args, prefix, _pattern: 72 if prefix == args.micro_prefix else 18,
    )
    monkeypatch.setattr(
        controller,
        "aggregate_phase",
        lambda _args: {
            "status": {"phase": "aggregate_completed", "expected_summary_count": 18},
            "packet_found": True,
            "packet_uri": "gs://control/full/aggregate/promotion_packet/nested/research_promotion_review_packet.json",
            "decision": "blocked_no_eligible_candidates",
            "eligible_for_promotion_review_count": 0,
        },
    )
    monkeypatch.setattr(controller, "existing_strict_profile_selection", lambda _args: selection)
    monkeypatch.setattr(
        controller,
        "select_best_strict_profile",
        lambda _args: (_ for _ in ()).throw(AssertionError("heatmap should not rebuild")),
    )
    monkeypatch.setattr(controller, "write_status", lambda _args, payload: written.update(payload))

    status = controller.controller_pass(args)

    assert status["phase"] == "research_blocked_or_redesign_needed"
    assert status["next_action"] == "design_next_research_wave_do_not_arm_runner"
    assert status["actions"] == ["aggregate_complete_no_vm_changes_needed"]
    assert status["selected_profile"] == "strict-e0x60"
    assert status["full_expected_summary_count"] == 18
    assert written["aggregate_packet_found"] is True

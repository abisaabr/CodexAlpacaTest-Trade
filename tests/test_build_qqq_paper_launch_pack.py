from __future__ import annotations

from pathlib import Path

from scripts.build_qqq_paper_launch_pack import build_launch_pack


def test_build_qqq_paper_launch_pack_blocks_broker_facing_session(tmp_path: Path) -> None:
    packet = build_launch_pack(
        promotion_manifest_path=Path(
            "config/promotion_manifests/qqq_option_native_governed_validation_20260430.yaml"
        ),
        output_dir=tmp_path,
    )

    assert packet["ready_for_broker_free_shadow_validation"] is True
    assert packet["ready_for_broker_facing_paper"] is False
    assert packet["decision"] == "ready_for_broker_free_shadow_validation_only"
    assert {check["status"] for check in packet["checks"]} == {"passed"}
    assert (tmp_path / "qqq_option_native_paper_launch_pack.json").exists()
    assert (tmp_path / "qqq_option_native_paper_launch_pack.md").exists()

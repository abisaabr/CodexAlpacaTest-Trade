from __future__ import annotations

from pathlib import Path

import yaml

from scripts.build_portfolio_overnight_tournament_packet import build_packet


def test_build_portfolio_overnight_tournament_packet(tmp_path: Path) -> None:
    packet = build_packet(
        config_path=Path("config/research_tournaments/portfolio_overnight_12h_20260501.yaml"),
        output_dir=tmp_path,
    )

    assert packet["decision"] == "ready_for_operator_approved_research_fleet_launch"
    assert packet["launch_requires_explicit_operator_approval"] is True
    assert packet["broker_facing"] is False
    assert packet["paper_orders"] is False
    assert packet["initial_cash"] == 25000
    assert packet["promotion_gates"]["fill_coverage_gate"] == 0.90
    assert packet["promotion_gates"]["max_symbol_weight"] == 0.20
    assert packet["worker_count"] == 8
    assert "QQQ" in packet["dataset_symbols"]
    assert "option_aware_tournament" in packet["worker_roles"]
    assert all("gcloud compute instances create" in worker["create_vm_command"] for worker in packet["workers"])
    assert all("_" not in worker["create_vm_command"].split()[4] for worker in packet["workers"])
    assert all("<generated_startup_script.sh>" not in worker["create_vm_command"] for worker in packet["workers"])
    assert all("startup_scripts/" in worker["create_vm_command"] for worker in packet["workers"])
    assert all("C:\\" not in worker["create_vm_command"] for worker in packet["workers"])
    assert all(Path(worker["startup_script_path"]).exists() for worker in packet["workers"])
    assert all("rm -rf ${REPO_DIR}" in Path(worker["startup_script_path"]).read_text() for worker in packet["workers"])
    assert all("--image-family debian-12" in worker["create_vm_command"] for worker in packet["workers"])
    assert all("--scopes cloud-platform" in worker["create_vm_command"] for worker in packet["workers"])
    assert all("--boot-disk-size 250GB" not in worker["create_vm_command"] for worker in packet["workers"])
    assert any("--boot-disk-size 20GB" in worker["create_vm_command"] for worker in packet["workers"])
    assert any("--boot-disk-size 40GB" in worker["create_vm_command"] for worker in packet["workers"])
    assert "e2-standard-8" not in "\n".join(worker["create_vm_command"] for worker in packet["workers"])
    assert any("--machine-type e2-standard-2" in worker["create_vm_command"] for worker in packet["workers"])
    assert any("--machine-type e2-standard-4" in worker["create_vm_command"] for worker in packet["workers"])
    assert all(
        "--provisioning-model SPOT --instance-termination-action STOP" in worker["create_vm_command"]
        for worker in packet["workers"]
    )
    coverage_workers = [worker for worker in packet["workers"] if worker["role"] == "data_coverage"]
    assert all("for symbol in" in worker["research_command"] for worker in coverage_workers)
    assert all("gcs_data_inventory.tsv" in Path(worker["startup_script_path"]).read_text() for worker in coverage_workers)
    option_workers = [worker for worker in packet["workers"] if worker["role"] == "option_aware_tournament"]
    assert all(
        "entry_liquidity_first_research_only" in Path(worker["startup_script_path"]).read_text()
        for worker in option_workers
    )
    assert all(
        "completed_run_id=" in Path(worker["startup_script_path"]).read_text()
        for worker in option_workers
    )
    assert all(
        "gcloud storage cp --recursive reports/research_wave/" in Path(worker["startup_script_path"]).read_text()
        for worker in option_workers
    )
    core_a_script = Path(option_workers[0]["startup_script_path"]).read_text()
    assert "research_wave/dense_universe/selected_option_contracts" in core_a_script
    assert "contract_inventory_silver/option_contract_inventory" not in core_a_script
    assert (tmp_path / "portfolio_overnight_12h_tournament_packet.json").exists()
    assert (tmp_path / "portfolio_overnight_12h_tournament_packet.md").exists()
    assert (tmp_path / "startup_scripts").is_dir()


def test_partial_aggregate_worker_can_run_immediately_to_separate_prefix(tmp_path: Path) -> None:
    config_path = Path("config/research_tournaments/portfolio_overnight_12h_20260501.yaml")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["workers"] = [
        {
            "worker_id": "partial_aggregate_now",
            "role": "aggregate_and_promote",
            "machine_type": "e2-standard-2",
            "boot_disk_size_gb": 20,
            "zone": "us-central1-a",
            "spot": False,
            "start_after_seconds": 0,
            "aggregate_output_subdir": "aggregate_partial_smoke",
        }
    ]
    partial_config_path = tmp_path / "partial_aggregate_config.yaml"
    partial_config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    packet = build_packet(config_path=partial_config_path, output_dir=tmp_path / "packet")
    worker = packet["workers"][0]
    startup_script = Path(worker["startup_script_path"]).read_text(encoding="utf-8")

    assert "--provisioning-model STANDARD" in worker["create_vm_command"]
    assert "--instance-termination-action STOP" not in worker["create_vm_command"]
    assert "aggregate_wait_seconds=0" in startup_script
    assert "aggregate_output_subdir='aggregate_partial_smoke'" in startup_script
    assert "candidate_identity_mode='variant_profile'" in startup_script
    assert "--candidate-identity-mode variant_profile" in startup_script
    assert "${GCS_PREFIX}/aggregate_partial_smoke/portfolio_report/" in startup_script
    assert "${GCS_PREFIX}/aggregate_partial_smoke/promotion_packet/" in startup_script


def test_fastlane_packet_uses_bounded_top_n_and_standard_vms(tmp_path: Path) -> None:
    packet = build_packet(
        config_path=Path("config/research_tournaments/portfolio_fastlane_top40_20260501.yaml"),
        output_dir=tmp_path,
    )

    assert packet["scope"] == "research_only_portfolio_fastlane_top40_recovery"
    assert packet["broker_facing"] is False
    assert packet["paper_orders"] is False
    assert packet["worker_count"] == 5
    assert all("--provisioning-model STANDARD" in worker["create_vm_command"] for worker in packet["workers"])
    assert all("--machine-type e2-standard-2" in worker["create_vm_command"] for worker in packet["workers"])
    option_workers = [worker for worker in packet["workers"] if worker["role"] == "option_aware_tournament"]
    assert len(option_workers) == 4
    for worker in option_workers:
        startup_script = Path(worker["startup_script_path"]).read_text(encoding="utf-8")
        assert "export PYTHONUNBUFFERED=1" in startup_script
        assert "python -u scripts/run_option_aware_research_backtest.py" in startup_script
        assert "--top-n 40" in startup_script
        assert "--test-date-count 20" in startup_script
        assert "run_started_utc=" in startup_script
        assert "run_completed_utc=" in startup_script
        assert "entry_liquidity_first_research_only" in startup_script
    aggregate_worker = next(worker for worker in packet["workers"] if worker["role"] == "aggregate_and_promote")
    aggregate_script = Path(aggregate_worker["startup_script_path"]).read_text(encoding="utf-8")
    assert "aggregate_wait_seconds=25200" in aggregate_script
    assert "aggregate_fastlane_top40_20260501" in aggregate_script
    assert "--candidate-identity-mode variant_profile" in aggregate_script

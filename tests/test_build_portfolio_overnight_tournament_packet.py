from __future__ import annotations

from pathlib import Path

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
    assert all("--image-family debian-12" in worker["create_vm_command"] for worker in packet["workers"])
    assert all("--scopes cloud-platform" in worker["create_vm_command"] for worker in packet["workers"])
    coverage_workers = [worker for worker in packet["workers"] if worker["role"] == "data_coverage"]
    assert all("for symbol in" in worker["research_command"] for worker in coverage_workers)
    assert all("gcs_data_inventory.tsv" in Path(worker["startup_script_path"]).read_text() for worker in coverage_workers)
    option_workers = [worker for worker in packet["workers"] if worker["role"] == "option_aware_tournament"]
    assert all(
        "entry_liquidity_first_research_only" in Path(worker["startup_script_path"]).read_text()
        for worker in option_workers
    )
    assert (tmp_path / "portfolio_overnight_12h_tournament_packet.json").exists()
    assert (tmp_path / "portfolio_overnight_12h_tournament_packet.md").exists()
    assert (tmp_path / "startup_scripts").is_dir()

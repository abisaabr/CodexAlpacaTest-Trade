from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_CONFIG = REPO_ROOT / "config" / "research_tournaments" / "portfolio_overnight_12h_20260501.yaml"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "gcp_research" / "portfolio_overnight_12h_20260501"


def _run_git(*args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = (result.stdout or "").strip()
    return output or None


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML mapping at {path}")
    return payload


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _slug(value: Any, *, max_length: int = 63) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", str(value).lower()).strip("-")
    if not slug:
        slug = "x"
    if not slug[0].isalpha():
        slug = f"x-{slug}"
    slug = slug[:max_length].strip("-")
    if not slug[-1].isalnum():
        slug = f"{slug}x"[:max_length]
    return slug


def _bash_words(items: list[Any]) -> str:
    return " ".join(str(item) for item in items)


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def _shell_quote(value: Any) -> str:
    return "'" + str(value).replace("'", "'\"'\"'") + "'"


def _dataset_for_symbol(config: dict[str, Any], symbol: str) -> tuple[str, dict[str, Any]]:
    symbol = symbol.upper()
    candidates = []
    for dataset_id, dataset in config.get("datasets", {}).items():
        if not isinstance(dataset, dict):
            continue
        symbols = {str(item).upper() for item in dataset.get("symbols", [])}
        if symbol in symbols:
            candidates.append((int(dataset.get("priority", 999)), str(dataset_id), dataset))
    if not candidates:
        raise ValueError(f"No dataset configured for symbol {symbol}")
    _, dataset_id, dataset = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
    return dataset_id, dataset


def _symbol_stage_uris(
    config: dict[str, Any],
    symbol: str,
    *,
    stage: str = "365d_5x5",
) -> dict[str, str]:
    symbol = symbol.upper()
    dataset_id, dataset = _dataset_for_symbol(config, symbol)
    stock_root = str(dataset["stock_root"]).rstrip("/")
    option_root = str(dataset["option_root"]).rstrip("/")
    if dataset_id == "qqq_dense_365d":
        selected_root = str(dataset["selected_contracts_root"]).rstrip("/")
        return {
            "dataset_id": dataset_id,
            "stock": f"{stock_root}/stock_ref_silver/stock_bars/",
            "contracts": f"{selected_root}/",
            "bars": f"{option_root}/option_bars_silver/option_bars/underlying={symbol}/",
        }
    selected_template = dataset.get("selected_contracts_root_template")
    if selected_template:
        contracts = str(selected_template).format(symbol=symbol, stage=stage).rstrip("/") + "/"
    else:
        contracts = f"{option_root}/{symbol}/{stage}/contract_inventory_silver/option_contract_inventory/"
    return {
        "dataset_id": dataset_id,
        "stock": f"{stock_root}/{symbol}/{stage}/stock_ref_silver/stock_bars/",
        "contracts": contracts,
        "bars": f"{option_root}/{symbol}/{stage}/option_bars_silver/option_bars/",
    }


def _worker_command(worker: dict[str, Any], config: dict[str, Any]) -> str:
    role = str(worker["role"])
    symbols_list = [str(symbol).upper() for symbol in worker.get("symbols", [])]
    stages_list = [str(stage) for stage in worker.get("stages", ["365d_5x5"])]
    if role == "data_coverage":
        return (
            f"for symbol in {_bash_words(symbols_list)}; do "
            f"for stage in {_bash_words(stages_list)}; do "
            "echo \"inventory $symbol $stage\" >> reports/research_wave/data_inventory.tsv; "
            "done; done"
        )
    if role == "qqq_option_native_deep_grid":
        return "Runs the exact QQQ regime-label and option-native profile commands in the generated startup script."
    if role == "option_aware_tournament":
        return (
            f"Runs per-symbol nearest_contract and entry_liquidity_first_research_only "
            f"option-aware replays for {_bash_words(symbols_list)} in the generated startup script."
        )
    if role == "aggregate_and_promote":
        return (
            "python scripts/build_research_portfolio_report.py "
            "--replay-root <merged_worker_outputs> "
            "--output-dir reports/research_wave/portfolio_overnight_12h_aggregate "
            "--fill-coverage-gate 0.90 --min-option-trades 20 --min-test-net-pnl 0 "
            "--max-positions 8 --max-strategies-per-symbol 2 --max-symbol-weight 0.20 "
            "--initial-cash 25000; "
            "python scripts/build_research_promotion_review_packet.py "
            "--portfolio-report-json reports/research_wave/portfolio_overnight_12h_aggregate/research_portfolio_report.json "
            "--output-dir reports/research_wave/portfolio_overnight_12h_promotion_packet"
        )
    return "echo Unsupported worker role"


def _script_header(worker: dict[str, Any], config: dict[str, Any]) -> list[str]:
    worker_id = str(worker["worker_id"])
    gcs_prefix = str(config["gcs_prefix"]).rstrip("/")
    worker_prefix = f"{gcs_prefix}/workers/{worker_id}"
    return [
        "#!/usr/bin/env bash",
        "set -Eeuo pipefail",
        "export DEBIAN_FRONTEND=noninteractive",
        f"WORKER_ID={_shell_quote(worker_id)}",
        f"WAVE_ID={_shell_quote(config['wave_id'])}",
        f"GCS_PREFIX={_shell_quote(gcs_prefix)}",
        f"WORKER_PREFIX={_shell_quote(worker_prefix)}",
        f"SOURCE_ARCHIVE_URI={_shell_quote(config['source_archive_uri'])}",
        f"INPUT_VARIANTS_URI={_shell_quote(config['input_variants_uri'])}",
        f"INPUT_QUEUE_URI={_shell_quote(config['input_queue_uri'])}",
        "WORKROOT=/mnt/codexalpaca",
        "REPO_DIR=${WORKROOT}/repo",
        "DATA_DIR=${WORKROOT}/data",
        "EMPTY_OPTION_TRADES=${WORKROOT}/empty_option_trades",
        "mkdir -p ${WORKROOT} ${DATA_DIR} ${EMPTY_OPTION_TRADES}",
        "exec > >(tee -a ${WORKROOT}/startup.log) 2>&1",
        "date -u '+startup_utc=%Y-%m-%dT%H:%M:%SZ'",
        "apt-get update",
        "apt-get install -y python3 python3-venv python3-pip ca-certificates",
        "gcloud storage cp ${SOURCE_ARCHIVE_URI} ${WORKROOT}/source.tar.gz",
        "rm -rf ${REPO_DIR}",
        "mkdir -p ${REPO_DIR}",
        "tar -xzf ${WORKROOT}/source.tar.gz -C ${REPO_DIR}",
        "cd ${REPO_DIR}",
        "python3 -m venv .venv",
        "source .venv/bin/activate",
        "python -m pip install --upgrade pip",
        'python -m pip install -e ".[gcp]"',
        "mkdir -p inputs reports/research_wave",
        "gcloud storage cp ${INPUT_VARIANTS_URI} inputs/portfolio_overnight_variants.jsonl",
        "gcloud storage cp ${INPUT_QUEUE_URI} inputs/portfolio_overnight_option_queue.json",
        "git rev-parse HEAD > ${WORKROOT}/source_commit.txt 2>/dev/null || echo archive > ${WORKROOT}/source_commit.txt",
        ": > ${WORKROOT}/command.txt",
    ]


def _script_footer() -> list[str]:
    return [
        "python - <<'PY' > ${WORKROOT}/artifacts_manifest.json",
        "import json",
        "from pathlib import Path",
        "root = Path('reports/research_wave')",
        "files = sorted(str(path) for path in root.rglob('*') if path.is_file())",
        "print(json.dumps({'file_count': len(files), 'files': files[:500]}, indent=2))",
        "PY",
        "gcloud storage cp ${WORKROOT}/startup.log ${WORKER_PREFIX}/startup.log || true",
        "gcloud storage cp ${WORKROOT}/command.txt ${WORKER_PREFIX}/command.txt || true",
        "gcloud storage cp ${WORKROOT}/source_commit.txt ${WORKER_PREFIX}/source_commit.txt || true",
        "gcloud storage cp ${WORKROOT}/artifacts_manifest.json ${WORKER_PREFIX}/artifacts_manifest.json || true",
        "gcloud storage cp --recursive reports/research_wave ${WORKER_PREFIX}/reports/ || true",
        "date -u '+completed_utc=%Y-%m-%dT%H:%M:%SZ'",
    ]


def _stage_symbol_lines(config: dict[str, Any], symbol: str) -> list[str]:
    uris = _symbol_stage_uris(config, symbol)
    symbol_slug = _slug(symbol)
    local_stock = f"${{DATA_DIR}}/{symbol_slug}/stock"
    local_contracts = f"${{DATA_DIR}}/{symbol_slug}/contracts"
    local_bars = f"${{DATA_DIR}}/{symbol_slug}/option_bars"
    return [
        f"mkdir -p {local_stock} {local_contracts} {local_bars}",
        f"gcloud storage cp --recursive {_shell_quote(uris['stock'])} {local_stock}/",
        f"gcloud storage cp --recursive {_shell_quote(uris['contracts'])} {local_contracts}/",
        f"gcloud storage cp --recursive {_shell_quote(uris['bars'])} {local_bars}/",
    ]


def _option_aware_worker_lines(worker: dict[str, Any], config: dict[str, Any]) -> list[str]:
    worker_id = str(worker["worker_id"])
    lines: list[str] = []
    for symbol in [str(item).upper() for item in worker.get("symbols", [])]:
        symbol_slug = _slug(symbol)
        lines.extend(_stage_symbol_lines(config, symbol))
        for selector in ["nearest_contract", "entry_liquidity_first_research_only"]:
            run_id = f"{worker_id}_{symbol.lower()}_{selector}"
            output_dir = f"reports/research_wave/{run_id}"
            command = (
                "python scripts/run_option_aware_research_backtest.py "
                "--queue-json inputs/portfolio_overnight_option_queue.json "
                "--variants-jsonl inputs/portfolio_overnight_variants.jsonl "
                f"--stock-bars-path ${{DATA_DIR}}/{symbol_slug}/stock "
                f"--selected-contracts-root ${{DATA_DIR}}/{symbol_slug}/contracts "
                f"--option-bars-root ${{DATA_DIR}}/{symbol_slug}/option_bars "
                "--option-trades-root ${EMPTY_OPTION_TRADES} "
                f"--symbol-filter {symbol} "
                "--top-n 250 --test-date-count 20 --initial-cash 25000 "
                "--allocation-fraction 0.05 --slippage-bps 10 --fee-per-contract 0.65 "
                f"--contract-selection-method {selector} "
                f"--output-dir {output_dir} --run-id {run_id}"
            )
            lines.append(f"echo {_shell_quote(command)} >> ${{WORKROOT}}/command.txt")
            lines.append(command)
    return lines


def _qqq_worker_lines(worker: dict[str, Any], config: dict[str, Any]) -> list[str]:
    lines = _stage_symbol_lines(config, "QQQ")
    lines.append(
        "python scripts/build_qqq_regime_labels.py "
        "--stock-bars-path ${DATA_DIR}/qqq/stock "
        "--output-dir reports/research_wave/qqq_regime_labels --symbol QQQ"
    )
    profiles = [
        ("first_common_e330_x390_lag15_15_nearest", "first_common_within_cutoff", 330, 390, 15, 15),
        ("first_common_e300_x390_lag30_60_nearest", "first_common_within_cutoff", 300, 390, 30, 60),
        ("fixed_e330_x390_lag15_15_nearest", "fixed_offset", 330, 390, 15, 15),
    ]
    for profile, timing_mode, entry_offset, exit_offset, entry_lag, exit_lag in profiles:
        run_id = f"{worker['worker_id']}_{profile}"
        command = (
            "python scripts/run_qqq_option_native_tournament.py "
            "--stock-bars-path ${DATA_DIR}/qqq/stock "
            "--selected-contracts-root ${DATA_DIR}/qqq/contracts "
            "--option-bars-root ${DATA_DIR}/qqq/option_bars "
            "--option-trades-root ${EMPTY_OPTION_TRADES} "
            "--regime-labels-csv reports/research_wave/qqq_regime_labels/qqq_regime_labels.csv "
            "--symbol QQQ "
            f"--entry-timing-mode {timing_mode} "
            f"--entry-offset-minutes {entry_offset} --exit-offset-minutes {exit_offset} "
            f"--max-entry-lag-minutes {entry_lag} --max-exit-lag-minutes {exit_lag} "
            "--test-date-count 20 --initial-cash 25000 --allocation-fraction 0.05 "
            "--slippage-bps 10 --fee-per-contract 0.65 --contract-selection-method nearest_contract "
            f"--output-dir reports/research_wave/{run_id} --run-id {run_id}"
        )
        lines.append(f"echo {_shell_quote(command)} >> ${{WORKROOT}}/command.txt")
        lines.append(command)
    return lines


def _data_coverage_worker_lines(worker: dict[str, Any], config: dict[str, Any]) -> list[str]:
    lines = ["mkdir -p reports/research_wave/data_inventory"]
    inventory = "reports/research_wave/data_inventory/gcs_data_inventory.tsv"
    lines.append(f"echo 'symbol\tstage\tdataset\tstock_uri\tcontracts_uri\tbars_uri' > {inventory}")
    for symbol in [str(item).upper() for item in worker.get("symbols", [])]:
        for stage in [str(item) for item in worker.get("stages", ["365d_5x5"])]:
            uris = _symbol_stage_uris(config, symbol, stage=stage)
            line = (
                f"{symbol}\\t{stage}\\t{uris['dataset_id']}\\t"
                f"{uris['stock']}\\t{uris['contracts']}\\t{uris['bars']}"
            )
            lines.append(f"echo {_shell_quote(line)} >> {inventory}")
            for label in ["stock", "contracts", "bars"]:
                lines.append(
                    f"gcloud storage du -s {_shell_quote(uris[label])} "
                    f">> reports/research_wave/data_inventory/{symbol}_{stage}_{label}_du.txt || true"
                )
    return lines


def _aggregate_worker_lines(worker: dict[str, Any], config: dict[str, Any]) -> list[str]:
    sleep_hours = int(worker.get("start_after_hours", 11))
    gates = config["promotion_gates"]
    return [
        f"sleep {sleep_hours * 3600}",
        "mkdir -p ${WORKROOT}/worker_outputs",
        "gcloud storage cp --recursive ${GCS_PREFIX}/workers/ ${WORKROOT}/worker_outputs/ || true",
        "python scripts/build_research_portfolio_report.py "
        "--replay-root ${WORKROOT}/worker_outputs "
        "--output-dir reports/research_wave/portfolio_overnight_12h_aggregate "
        f"--fill-coverage-gate {gates['fill_coverage_gate']} "
        f"--min-option-trades {gates['min_option_trades']} "
        f"--min-test-net-pnl {gates['min_test_net_pnl']} "
        f"--max-positions {gates['max_positions']} "
        f"--max-strategies-per-symbol {gates['max_strategies_per_symbol']} "
        f"--max-symbol-weight {gates['max_symbol_weight']} "
        f"--initial-cash {gates['initial_cash']}",
        "python scripts/build_research_promotion_review_packet.py "
        "--portfolio-report-json reports/research_wave/portfolio_overnight_12h_aggregate/research_portfolio_report.json "
        "--output-dir reports/research_wave/portfolio_overnight_12h_promotion_packet",
        "gcloud storage cp --recursive reports/research_wave/portfolio_overnight_12h_aggregate ${GCS_PREFIX}/aggregate/portfolio_report/",
        "gcloud storage cp --recursive reports/research_wave/portfolio_overnight_12h_promotion_packet ${GCS_PREFIX}/aggregate/promotion_packet/",
    ]


def _startup_script(worker: dict[str, Any], config: dict[str, Any]) -> str:
    role = str(worker["role"])
    lines = _script_header(worker, config)
    if role == "option_aware_tournament":
        lines.extend(_option_aware_worker_lines(worker, config))
    elif role == "qqq_option_native_deep_grid":
        lines.extend(_qqq_worker_lines(worker, config))
    elif role == "data_coverage":
        lines.extend(_data_coverage_worker_lines(worker, config))
    elif role == "aggregate_and_promote":
        lines.extend(_aggregate_worker_lines(worker, config))
    else:
        lines.append(f"echo unsupported role: {_shell_quote(role)}")
    lines.extend(_script_footer())
    return "\n".join(lines) + "\n"


def _create_vm_command(
    worker: dict[str, Any],
    config: dict[str, Any],
    *,
    startup_script_arg: str,
) -> str:
    wave_slug = _slug(config["wave_id"], max_length=38)
    worker_slug = _slug(worker["worker_id"], max_length=24)
    role_slug = _slug(worker["role"], max_length=48)
    instance_name = _slug(f"{wave_slug}-{worker_slug}")
    labels = f"wave={wave_slug},role={role_slug},worker={worker_slug}"
    boot_disk_size_gb = int(worker.get("boot_disk_size_gb", config.get("default_boot_disk_size_gb", 40)))
    return (
        "gcloud compute instances create "
        f"{instance_name} "
        f"--zone {worker['zone']} "
        f"--machine-type {worker['machine_type']} "
        "--image-family debian-12 --image-project debian-cloud "
        f"--service-account {config['service_account']} --scopes cloud-platform "
        "--provisioning-model SPOT --instance-termination-action STOP "
        f"--boot-disk-size {boot_disk_size_gb}GB --boot-disk-type pd-balanced "
        f"--labels {labels} "
        f"--metadata-from-file startup-script={startup_script_arg}"
    )


def _write_markdown(path: Path, packet: dict[str, Any]) -> Path:
    lines = [
        "# Portfolio Overnight 12h Tournament Packet",
        "",
        f"- Generated UTC: `{packet['generated_at_utc']}`",
        f"- Wave ID: `{packet['wave_id']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Broker facing: `{str(packet['broker_facing']).lower()}`",
        f"- Paper orders: `{str(packet['paper_orders']).lower()}`",
        f"- Duration hours: `{packet['duration_hours']}`",
        f"- Initial cash: `${packet['initial_cash']}`",
        f"- GCS prefix: `{packet['gcs_prefix']}`",
        "",
        "## Worker Fleet",
        "",
        "| Worker | Role | Machine | Symbols | Startup Script | Command |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for worker in packet["workers"]:
        symbols = ",".join(worker.get("symbols", [])) or "aggregate"
        command = worker["research_command"].replace("|", "\\|")
        startup_script = worker["startup_script_path"].replace("|", "\\|")
        lines.append(
            f"| `{worker['worker_id']}` | `{worker['role']}` | `{worker['machine_type']}` | `{symbols}` | `{startup_script}` | `{command}` |"
        )
    lines.extend(["", "## Promotion Gates", ""])
    for key, value in packet["promotion_gates"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Hard Rules", ""])
    for rule in packet["hard_rules"]:
        lines.append(f"- {rule}")
    lines.extend(["", "## VM Create Commands", ""])
    lines.append("These commands are billable and should only be run after explicit operator approval.")
    lines.append("Each command points at a generated startup script in this packet output directory.")
    lines.append("")
    lines.append("```powershell")
    for worker in packet["workers"]:
        lines.append(worker["create_vm_command"])
    lines.append("```")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def build_packet(*, config_path: Path, output_dir: Path) -> dict[str, Any]:
    config = _load_yaml(config_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    startup_dir = output_dir / "startup_scripts"
    workers = []
    for worker in config.get("workers", []):
        if not isinstance(worker, dict):
            continue
        item = dict(worker)
        startup_relative_path = Path("startup_scripts") / f"{_slug(item['worker_id'])}.sh"
        startup_path = output_dir / startup_relative_path
        _write_text(startup_path, _startup_script(item, config))
        item["research_command"] = _worker_command(item, config)
        item["startup_script_path"] = str(startup_path)
        item["startup_script_relative_path"] = str(startup_relative_path)
        item["create_vm_command"] = _create_vm_command(
            item,
            config,
            startup_script_arg=str(startup_relative_path).replace("\\", "/"),
        )
        workers.append(item)
    worker_roles = sorted({str(worker["role"]) for worker in workers})
    all_symbols = sorted(
        {
            str(symbol).upper()
            for dataset in config.get("datasets", {}).values()
            if isinstance(dataset, dict)
            for symbol in dataset.get("symbols", [])
        }
    )
    packet = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "decision": "ready_for_operator_approved_research_fleet_launch",
        "launch_requires_explicit_operator_approval": True,
        "wave_id": config["wave_id"],
        "duration_hours": config["duration_hours"],
        "initial_cash": config["initial_cash"],
        "scope": config["scope"],
        "broker_facing": bool(config["broker_facing"]),
        "paper_orders": bool(config["paper_orders"]),
        "live_manifest_effect": config["live_manifest_effect"],
        "risk_policy_effect": config["risk_policy_effect"],
        "gcs_prefix": config["gcs_prefix"],
        "config_path": str(config_path),
        "config_sha256": _sha256(config_path),
        "runner_git": {
            "branch": _run_git("branch", "--show-current"),
            "commit": _run_git("rev-parse", "HEAD"),
            "dirty": bool(_run_git("status", "--porcelain")),
        },
        "dataset_symbols": all_symbols,
        "dataset_symbol_count": len(all_symbols),
        "datasets": config.get("datasets", {}),
        "worker_count": len(workers),
        "worker_roles": worker_roles,
        "workers": workers,
        "promotion_gates": config["promotion_gates"],
        "portfolio_objective": config["portfolio_objective"],
        "logging_contract": config["logging_contract"],
        "hard_rules": config["hard_rules"],
        "next_actions": [
            "Mirror this packet and config to GCS.",
            "Create a source archive for the exact branch/commit and mirror it under the wave prefix.",
            "Launch research VMs only after explicit operator approval because this creates billable infrastructure.",
            "After workers finish or at the 11-hour mark, run aggregate_and_promote and publish the promotion packet.",
            "Do not paper trade from this packet; eligible strategies are governed-validation review candidates only.",
        ],
    }
    packet["outputs"] = {
        "json": str(output_dir / "portfolio_overnight_12h_tournament_packet.json"),
        "markdown": str(output_dir / "portfolio_overnight_12h_tournament_packet.md"),
        "startup_scripts": str(startup_dir),
    }
    _write_json(output_dir / "portfolio_overnight_12h_tournament_packet.json", packet)
    _write_markdown(output_dir / "portfolio_overnight_12h_tournament_packet.md", packet)
    return packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the portfolio overnight 12h tournament packet.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    packet = build_packet(config_path=Path(args.config), output_dir=Path(args.output_dir))
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

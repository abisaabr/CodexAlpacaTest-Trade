from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT = "codexalpaca"
DEFAULT_WAVE_ID = "ticker_365d_all_available_20260501T2300Z"
DEFAULT_GCS_PREFIX = (
    "gs://codexalpaca-control-us/research_results/"
    "ticker_365d_all_available_20260501T2300Z"
)
DEFAULT_SERVICE_ACCOUNT = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
DEFAULT_SOURCE_ARCHIVE_URI = f"{DEFAULT_GCS_PREFIX}/inputs/source/codexalpaca_repo_source.tar.gz"
DEFAULT_INPUT_VARIANTS_URI = (
    "gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/"
    "inputs/portfolio_overnight_variants.jsonl"
)
DEFAULT_INPUT_QUEUE_URI = (
    "gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/"
    "inputs/portfolio_overnight_option_queue.json"
)
DEFAULT_LAUNCH_ROWS_URI = f"{DEFAULT_GCS_PREFIX}/inputs/ticker_365d_all_available_launch_rows.json"
DEFAULT_GCLOUD = (
    r"C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
)
DEFAULT_PYTHON = r"C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"

TICKER_INSTANCE_RE = re.compile(
    r"^ticker365-(?P<symbol>[a-z0-9]+)-20260501-2300z(?:-r(?P<retry>\d+))?$"
)
PROTECTED_RUNNING_NAMES = {
    "vm-execution-paper-01",
}
HARD_RULES = [
    "Do not start trading.",
    "Do not submit paper orders.",
    "Do not modify live manifests.",
    "Do not change risk policy.",
    "Do not lower fill_coverage >= 0.90.",
    "Promotion means governed validation review until a packet explicitly says eligible.",
]


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(message: str) -> None:
    print(f"{utc_now()} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quota-safe watchdog for the all-available ticker 365d GCP research wave. "
            "It stops completed ticker shards, launches the next pending shards when CPU "
            "quota is available, and triggers the aggregate packet after all summaries land."
        )
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--wave-id", default=DEFAULT_WAVE_ID)
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX)
    parser.add_argument("--gcloud", default=os.environ.get("GCLOUD", DEFAULT_GCLOUD))
    parser.add_argument("--python", default=os.environ.get("PYTHON", DEFAULT_PYTHON))
    parser.add_argument("--service-account", default=DEFAULT_SERVICE_ACCOUNT)
    parser.add_argument("--source-archive-uri", default=DEFAULT_SOURCE_ARCHIVE_URI)
    parser.add_argument("--input-variants-uri", default=DEFAULT_INPUT_VARIANTS_URI)
    parser.add_argument("--input-queue-uri", default=DEFAULT_INPUT_QUEUE_URI)
    parser.add_argument("--launch-rows-uri", default=DEFAULT_LAUNCH_ROWS_URI)
    parser.add_argument("--machine-type", default="e2-standard-2")
    parser.add_argument("--boot-disk-size-gb", type=int, default=40)
    parser.add_argument("--aggregate-zone", default="us-central1-a")
    parser.add_argument("--aggregate-machine-type", default="e2-standard-2")
    parser.add_argument("--max-launches-per-run", type=int, default=4)
    parser.add_argument("--max-retry-attempts", type=int, default=2)
    parser.add_argument("--expected-summaries", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-refresh-inputs", action="store_true")
    return parser.parse_args()


class CommandError(RuntimeError):
    def __init__(self, command: list[str], returncode: int, output: str) -> None:
        super().__init__(
            f"command failed ({returncode}): {' '.join(command)}\n{output.strip()}"
        )
        self.command = command
        self.returncode = returncode
        self.output = output


def run_command(
    command: list[str],
    *,
    check: bool = True,
    allow_no_objects: bool = False,
    timeout: int = 900,
) -> str:
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    output = result.stdout or ""
    if result.returncode != 0:
        if allow_no_objects and "matched no objects" in output:
            return ""
        if check:
            raise CommandError(command, result.returncode, output)
    return output


def run_json(command: list[str], *, default: Any = None) -> Any:
    output = run_command(command)
    if not output.strip():
        return default
    return json.loads(output)


def gcloud(args: argparse.Namespace, *parts: str) -> list[str]:
    return [args.gcloud, *parts]


def storage_ls(args: argparse.Namespace, uri: str) -> list[str]:
    output = run_command(
        gcloud(args, "storage", "ls", "--recursive", uri),
        check=False,
        allow_no_objects=True,
    )
    return [line.strip() for line in output.splitlines() if line.strip().startswith("gs://")]


def storage_cat(args: argparse.Namespace, uri: str) -> str:
    return run_command(gcloud(args, "storage", "cat", uri))


def load_launch_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    local_path = REPO_ROOT / "reports" / "gcp_research" / "ticker_365d_all_available_launch_rows.json"
    if local_path.exists():
        return json.loads(local_path.read_text(encoding="utf-8"))
    try:
        return json.loads(storage_cat(args, args.launch_rows_uri))
    except Exception as exc:
        raise RuntimeError(
            "Unable to load ticker launch rows from local reports or GCS. "
            f"Tried {local_path} and {args.launch_rows_uri}."
        ) from exc


def instance_zone(instance: dict[str, Any]) -> str:
    return str(instance.get("zone", "")).split("/")[-1]


def machine_type_name(instance: dict[str, Any]) -> str:
    return str(instance.get("machineType", "")).split("/")[-1]


def machine_type_cpus(machine_type: str) -> int:
    match = re.search(r"standard-(\d+)$", machine_type)
    if match:
        return int(match.group(1))
    match = re.search(r"highmem-(\d+)$", machine_type)
    if match:
        return int(match.group(1))
    match = re.search(r"highcpu-(\d+)$", machine_type)
    if match:
        return int(match.group(1))
    if machine_type.endswith("-micro") or machine_type.endswith("-small"):
        return 1
    return 2


def list_instances(args: argparse.Namespace) -> list[dict[str, Any]]:
    payload = run_json(
        gcloud(args, "compute", "instances", "list", "--project", args.project, "--format=json"),
        default=[],
    )
    return payload if isinstance(payload, list) else []


def quota_snapshot(args: argparse.Namespace, instances: list[dict[str, Any]]) -> dict[str, Any]:
    usage_from_instances = sum(
        machine_type_cpus(machine_type_name(instance))
        for instance in instances
        if instance.get("status") == "RUNNING"
    )
    try:
        project_info = run_json(
            gcloud(args, "compute", "project-info", "describe", "--project", args.project, "--format=json"),
            default={},
        )
        quotas = project_info.get("quotas", []) if isinstance(project_info, dict) else []
        for quota in quotas:
            if quota.get("metric") == "CPUS_ALL_REGIONS":
                usage = float(quota.get("usage", usage_from_instances))
                limit = float(quota.get("limit", 32))
                return {
                    "metric": "CPUS_ALL_REGIONS",
                    "usage": usage,
                    "limit": limit,
                    "free": max(0, int(limit - usage)),
                    "usage_from_running_instances": usage_from_instances,
                }
    except Exception as exc:
        log(f"quota_lookup_failed using_running_instance_fallback error={exc}")
    return {
        "metric": "CPUS_ALL_REGIONS",
        "usage": float(usage_from_instances),
        "limit": 32.0,
        "free": max(0, 32 - usage_from_instances),
        "usage_from_running_instances": usage_from_instances,
    }


def worker_id(symbol: str) -> str:
    return f"ticker365_{symbol.lower()}"


def base_instance_name(symbol: str) -> str:
    return f"ticker365-{symbol.lower()}-20260501-2300z"


def ticker_instances(instances: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for instance in instances:
        name = str(instance.get("name", ""))
        match = TICKER_INSTANCE_RE.match(name)
        if not match:
            continue
        grouped.setdefault(match.group("symbol").upper(), []).append(instance)
    return grouped


def load_worker_statuses(args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    statuses: dict[str, dict[str, Any]] = {}
    for uri in storage_ls(args, f"{args.gcs_prefix}/workers/**/ticker_365d_status.json"):
        try:
            payload = json.loads(storage_cat(args, uri))
            wid = str(payload.get("worker_id") or "")
            if wid:
                payload["status_uri"] = uri
                statuses[wid] = payload
        except Exception as exc:
            log(f"status_read_failed uri={uri} error={exc}")
    return statuses


def count_by_worker(uris: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for uri in uris:
        match = re.search(r"/workers/([^/]+)/", uri)
        if not match:
            continue
        counts[match.group(1)] = counts.get(match.group(1), 0) + 1
    return counts


def worker_artifact_counts(args: argparse.Namespace) -> dict[str, dict[str, int]]:
    summaries = count_by_worker(
        storage_ls(args, f"{args.gcs_prefix}/workers/**/option_aware_candidate_summary.json")
    )
    reports = count_by_worker(
        storage_ls(args, f"{args.gcs_prefix}/workers/**/research_portfolio_report.json")
    )
    packets = count_by_worker(
        storage_ls(args, f"{args.gcs_prefix}/workers/**/research_promotion_review_packet.json")
    )
    all_workers = set(summaries) | set(reports) | set(packets)
    return {
        wid: {
            "candidate_summary_count": summaries.get(wid, 0),
            "portfolio_report_count": reports.get(wid, 0),
            "promotion_packet_count": packets.get(wid, 0),
        }
        for wid in sorted(all_workers)
    }


def aggregate_state(args: argparse.Namespace) -> dict[str, Any]:
    packet_uris = storage_ls(
        args,
        f"{args.gcs_prefix}/aggregate/promotion_packet/**/research_promotion_review_packet.json",
    )
    report_uris = storage_ls(
        args,
        f"{args.gcs_prefix}/aggregate/portfolio_report/**/research_portfolio_report.json",
    )
    growth_uris = storage_ls(
        args,
        f"{args.gcs_prefix}/aggregate/growth_projection/**/portfolio_growth_projection.json",
    )
    packet_summary: dict[str, Any] = {}
    if packet_uris:
        try:
            packet_summary = json.loads(storage_cat(args, packet_uris[0]))
        except Exception as exc:
            packet_summary = {"read_error": str(exc)}
    return {
        "promotion_packet_uris": packet_uris,
        "portfolio_report_uris": report_uris,
        "growth_projection_uris": growth_uris,
        "promotion_packet_summary": summarize_promotion_packet(packet_summary),
    }


def summarize_promotion_packet(packet: dict[str, Any]) -> dict[str, Any]:
    if not packet:
        return {}
    gate_summary = packet.get("gate_summary", {})
    return {
        "decision": packet.get("decision"),
        "candidate_count": gate_summary.get("candidate_count"),
        "eligible_for_promotion_review_count": gate_summary.get(
            "eligible_for_promotion_review_count"
        ),
        "promotion_scope": packet.get("promotion_scope"),
        "broker_facing": packet.get("broker_facing"),
        "review_candidate_count": len(packet.get("review_candidates", []) or []),
        "symbol_exposure": packet.get("symbol_exposure", []),
    }


def create_source_archive(args: argparse.Namespace) -> Path:
    archive_path = REPO_ROOT / "reports" / "gcp_research" / "codexalpaca_repo_source_watchdog.tar.gz"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    tracked_files = run_command(["git", "ls-files"]).splitlines()
    with tarfile.open(archive_path, "w:gz") as tar:
        for rel in tracked_files:
            path = REPO_ROOT / rel
            if path.is_file():
                tar.add(path, arcname=rel)
    return archive_path


def refresh_inputs(args: argparse.Namespace) -> list[str]:
    if args.no_refresh_inputs or args.dry_run:
        return []
    uploaded: list[str] = []
    archive_path = create_source_archive(args)
    startup_path = REPO_ROOT / "scripts" / "gcp_single_ticker_365d_shard.sh"
    aggregate_path = REPO_ROOT / "scripts" / "gcp_ticker_365d_aggregate_watch.sh"
    docs_path = REPO_ROOT / "docs" / "gcp_research" / "ticker_365d_all_available_backtest_20260501.md"
    uploads = [
        (archive_path, args.source_archive_uri),
        (startup_path, f"{args.gcs_prefix}/inputs/startup/gcp_single_ticker_365d_shard.sh"),
        (aggregate_path, f"{args.gcs_prefix}/inputs/startup/gcp_ticker_365d_aggregate_watch.sh"),
        (docs_path, f"{args.gcs_prefix}/docs/ticker_365d_all_available_backtest_20260501.md"),
    ]
    for source, target in uploads:
        if source.exists():
            run_command(gcloud(args, "storage", "cp", str(source), target))
            uploaded.append(target)
    return uploaded


def completed_worker(phase: str, counts: dict[str, int]) -> bool:
    if phase == "completed":
        return True
    return (
        counts.get("candidate_summary_count", 0) >= 2
        and counts.get("portfolio_report_count", 0) >= 1
        and counts.get("promotion_packet_count", 0) >= 1
    )


def stop_completed_instances(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    instances_by_symbol: dict[str, list[dict[str, Any]]],
    statuses: dict[str, dict[str, Any]],
    counts_by_worker: dict[str, dict[str, int]],
) -> list[dict[str, Any]]:
    stopped: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row["symbol"]).upper()
        wid = worker_id(symbol)
        phase = str(statuses.get(wid, {}).get("phase", "unknown"))
        counts = counts_by_worker.get(wid, {})
        if not completed_worker(phase, counts):
            continue
        for instance in instances_by_symbol.get(symbol, []):
            name = str(instance.get("name"))
            if name in PROTECTED_RUNNING_NAMES:
                continue
            if instance.get("status") != "RUNNING":
                continue
            zone = instance_zone(instance)
            log(f"stopping_completed_ticker_instance symbol={symbol} instance={name} zone={zone}")
            if not args.dry_run:
                run_command(
                    gcloud(
                        args,
                        "compute",
                        "instances",
                        "stop",
                        name,
                        "--zone",
                        zone,
                        "--project",
                        args.project,
                        "--quiet",
                    ),
                    timeout=600,
                )
            stopped.append({"symbol": symbol, "instance": name, "zone": zone})
    return stopped


def next_instance_name(symbol: str, existing: list[dict[str, Any]]) -> str:
    base = base_instance_name(symbol)
    if not existing:
        return base
    retry_numbers = [1]
    for instance in existing:
        match = TICKER_INSTANCE_RE.match(str(instance.get("name", "")))
        if match and match.group("retry"):
            retry_numbers.append(int(match.group("retry")))
    return f"{base}-r{max(retry_numbers) + 1}"


def active_instance_exists(instances: list[dict[str, Any]]) -> bool:
    active_statuses = {"PROVISIONING", "STAGING", "RUNNING", "REPAIRING", "SUSPENDING"}
    return any(instance.get("status") in active_statuses for instance in instances)


def launch_ticker_instance(
    args: argparse.Namespace,
    row: dict[str, Any],
    instance_name: str,
) -> None:
    symbol = str(row["symbol"]).upper()
    metadata = {
        "symbol": symbol,
        "worker_id": worker_id(symbol),
        "wave_id": args.wave_id,
        "gcs_prefix": args.gcs_prefix,
        "source_archive_uri": args.source_archive_uri,
        "input_variants_uri": args.input_variants_uri,
        "input_queue_uri": args.input_queue_uri,
        "stock_uri": str(row["stock"]),
        "contracts_uri": str(row["contracts"]),
        "bars_uri": str(row["bars"]),
        "initial_cash": "25000",
    }
    metadata_arg = ",".join(f"{key}={value}" for key, value in metadata.items())
    labels = (
        f"wave=ticker365-20260501,role=ticker365,symbol={symbol.lower()},"
        f"dataset={str(row.get('dataset_id', 'unknown')).replace('_', '-')[:32]}"
    )
    startup_script = str(REPO_ROOT / "scripts" / "gcp_single_ticker_365d_shard.sh")
    command = gcloud(
        args,
        "compute",
        "instances",
        "create",
        instance_name,
        "--project",
        args.project,
        "--zone",
        str(row["zone"]),
        "--machine-type",
        args.machine_type,
        "--image-family",
        "debian-12",
        "--image-project",
        "debian-cloud",
        "--service-account",
        args.service_account,
        "--scopes",
        "cloud-platform",
        "--provisioning-model",
        "SPOT",
        "--instance-termination-action",
        "STOP",
        "--boot-disk-size",
        f"{args.boot_disk_size_gb}GB",
        "--boot-disk-type",
        "pd-balanced",
        "--labels",
        labels,
        "--metadata",
        metadata_arg,
        "--metadata-from-file",
        f"startup-script={startup_script}",
    )
    log(f"launching_ticker_shard symbol={symbol} instance={instance_name} zone={row['zone']}")
    if not args.dry_run:
        run_command(command, timeout=900)


def launch_pending_instances(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    instances_by_symbol: dict[str, list[dict[str, Any]]],
    statuses: dict[str, dict[str, Any]],
    counts_by_worker: dict[str, dict[str, int]],
    quota: dict[str, Any],
) -> list[dict[str, Any]]:
    free_cpus = int(quota.get("free", 0))
    cpus_per_worker = machine_type_cpus(args.machine_type)
    launch_capacity = min(args.max_launches_per_run, free_cpus // cpus_per_worker)
    if launch_capacity <= 0:
        return []

    launched: list[dict[str, Any]] = []
    for row in rows:
        if len(launched) >= launch_capacity:
            break
        symbol = str(row["symbol"]).upper()
        wid = worker_id(symbol)
        phase = str(statuses.get(wid, {}).get("phase", "not_started"))
        counts = counts_by_worker.get(wid, {})
        existing = instances_by_symbol.get(symbol, [])
        attempts = len(existing)
        if completed_worker(phase, counts):
            continue
        if phase == "failed":
            continue
        if active_instance_exists(existing):
            continue
        if attempts >= args.max_retry_attempts:
            continue
        instance_name = next_instance_name(symbol, existing)
        launch_ticker_instance(args, row, instance_name)
        launched.append({"symbol": symbol, "instance": instance_name, "zone": row["zone"]})
    return launched


def aggregate_instance_exists(instances: list[dict[str, Any]]) -> bool:
    return any(str(instance.get("name", "")).startswith("ticker365-agg-20260501-2300z") for instance in instances)


def launch_aggregate_if_ready(
    args: argparse.Namespace,
    instances: list[dict[str, Any]],
    aggregate: dict[str, Any],
    total_summary_count: int,
    expected_summary_count: int,
    quota: dict[str, Any],
) -> list[dict[str, Any]]:
    if aggregate.get("promotion_packet_uris"):
        return []
    if aggregate_instance_exists(instances):
        return []
    if total_summary_count < expected_summary_count:
        return []
    cpus_required = machine_type_cpus(args.aggregate_machine_type)
    if int(quota.get("free", 0)) < cpus_required:
        return []
    name = "ticker365-agg-20260501-2300z"
    startup_script = str(REPO_ROOT / "scripts" / "gcp_ticker_365d_aggregate_watch.sh")
    labels = "wave=ticker365-20260501,role=ticker365-aggregate"
    command = gcloud(
        args,
        "compute",
        "instances",
        "create",
        name,
        "--project",
        args.project,
        "--zone",
        args.aggregate_zone,
        "--machine-type",
        args.aggregate_machine_type,
        "--image-family",
        "debian-12",
        "--image-project",
        "debian-cloud",
        "--service-account",
        args.service_account,
        "--scopes",
        "cloud-platform",
        "--provisioning-model",
        "SPOT",
        "--instance-termination-action",
        "STOP",
        "--boot-disk-size",
        "40GB",
        "--boot-disk-type",
        "pd-balanced",
        "--labels",
        labels,
        "--metadata-from-file",
        f"startup-script={startup_script}",
    )
    log(f"launching_aggregate_instance instance={name} zone={args.aggregate_zone}")
    if not args.dry_run:
        run_command(command, timeout=900)
    return [{"instance": name, "zone": args.aggregate_zone}]


def write_status_files(args: argparse.Namespace, status: dict[str, Any]) -> tuple[Path, Path]:
    status_dir = REPO_ROOT / "reports" / "gcp_research" / "watchdog"
    status_dir.mkdir(parents=True, exist_ok=True)
    json_path = status_dir / "ticker_365d_watch_status_20260501.json"
    md_path = status_dir / "ticker_365d_watch_status_20260501.md"
    json_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines = [
        "# Ticker 365d Watch Status - 2026-05-01",
        "",
        f"- Generated UTC: `{status['generated_at_utc']}`",
        f"- Wave ID: `{status['wave_id']}`",
        f"- GCS prefix: `{status['gcs_prefix']}`",
        f"- Quota: `{status['quota']['usage']}/{status['quota']['limit']}` CPUs, free `{status['quota']['free']}`",
        f"- Worker summaries: `{status['summary_counts']['total']}/{status['summary_counts']['expected']}`",
        f"- Completed ticker workers: `{status['worker_counts']['completed']}/{status['worker_counts']['total']}`",
        f"- Running ticker workers: `{status['worker_counts']['running']}`",
        f"- Failed ticker workers: `{status['worker_counts']['failed']}`",
        f"- Pending ticker workers: `{status['worker_counts']['pending']}`",
        f"- Aggregate packet ready: `{bool(status['aggregate']['promotion_packet_uris'])}`",
        f"- Paper trader handoff: `{status['paper_trader_handoff']['status']}`",
        "",
        "## Actions This Run",
        "",
    ]
    if not status["actions"]["stopped_instances"] and not status["actions"]["launched_instances"] and not status["actions"]["launched_aggregate_instances"]:
        lines.append("- No VM changes were needed this run.")
    for item in status["actions"]["stopped_instances"]:
        lines.append(f"- Stopped completed shard `{item['instance']}` for `{item['symbol']}`.")
    for item in status["actions"]["launched_instances"]:
        lines.append(f"- Launched shard `{item['instance']}` for `{item['symbol']}` in `{item['zone']}`.")
    for item in status["actions"]["launched_aggregate_instances"]:
        lines.append(f"- Launched aggregate VM `{item['instance']}` in `{item['zone']}`.")

    lines.extend(["", "## Worker State", ""])
    lines.append("| Symbol | Phase | Instance Statuses | Summaries | Reports | Packets |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: |")
    for worker in status["workers"]:
        instance_status = ", ".join(
            f"{item['name']}:{item['status']}" for item in worker["instances"]
        ) or "none"
        lines.append(
            "| "
            f"`{worker['symbol']}` | `{worker['phase']}` | {instance_status} | "
            f"{worker['candidate_summary_count']} | {worker['portfolio_report_count']} | "
            f"{worker['promotion_packet_count']} |"
        )

    lines.extend(["", "## Hard Rules", ""])
    for rule in HARD_RULES:
        lines.append(f"- {rule}")
    lines.extend(["", "## Next Action", "", f"- {status['next_action']}"])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def upload_status_files(args: argparse.Namespace, paths: list[Path]) -> None:
    if args.dry_run:
        return
    for path in paths:
        run_command(gcloud(args, "storage", "cp", str(path), f"{args.gcs_prefix}/watchdog/{path.name}"))


def paper_trader_handoff_state(aggregate: dict[str, Any]) -> dict[str, Any]:
    summary = aggregate.get("promotion_packet_summary") or {}
    eligible = int(summary.get("eligible_for_promotion_review_count") or 0)
    if eligible > 0:
        return {
            "status": "eligible_packet_ready_for_operator_review",
            "detail": (
                "An eligible governed-review packet exists. Prepare a no-order paper "
                "trader staging handoff before any manifest or broker-facing change."
            ),
            "paper_orders": False,
            "live_manifest_effect": "none",
        }
    if aggregate.get("promotion_packet_uris"):
        return {
            "status": "aggregate_packet_ready_no_eligible_candidates",
            "detail": "No strategy should move forward until repair/retest produces eligible candidates.",
            "paper_orders": False,
            "live_manifest_effect": "none",
        }
    return {
        "status": "blocked_until_aggregate_promotion_packet",
        "detail": "The watchdog is still collecting ticker shards and has not staged paper-trader changes.",
        "paper_orders": False,
        "live_manifest_effect": "none",
    }


def build_status(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    instances_by_symbol: dict[str, list[dict[str, Any]]],
    statuses: dict[str, dict[str, Any]],
    counts_by_worker: dict[str, dict[str, int]],
    quota: dict[str, Any],
    aggregate: dict[str, Any],
    actions: dict[str, Any],
    uploaded_inputs: list[str],
) -> dict[str, Any]:
    workers: list[dict[str, Any]] = []
    completed = failed = running = pending = 0
    for row in rows:
        symbol = str(row["symbol"]).upper()
        wid = worker_id(symbol)
        status_payload = statuses.get(wid, {})
        phase = str(status_payload.get("phase", "not_started"))
        counts = counts_by_worker.get(wid, {})
        instance_items = [
            {
                "name": str(instance.get("name")),
                "zone": instance_zone(instance),
                "machine_type": machine_type_name(instance),
                "status": str(instance.get("status")),
            }
            for instance in instances_by_symbol.get(symbol, [])
        ]
        is_completed = completed_worker(phase, counts)
        is_failed = phase == "failed"
        is_running = active_instance_exists(instances_by_symbol.get(symbol, []))
        if is_completed:
            completed += 1
        elif is_failed:
            failed += 1
        elif is_running:
            running += 1
        else:
            pending += 1
        workers.append(
            {
                "symbol": symbol,
                "dataset_id": row.get("dataset_id"),
                "phase": phase,
                "status_uri": status_payload.get("status_uri"),
                "generated_at_utc": status_payload.get("generated_at_utc"),
                "candidate_summary_count": counts.get("candidate_summary_count", 0),
                "portfolio_report_count": counts.get("portfolio_report_count", 0),
                "promotion_packet_count": counts.get("promotion_packet_count", 0),
                "instances": instance_items,
                "completed": is_completed,
                "failed": is_failed,
            }
        )
    expected_summaries = args.expected_summaries or (len(rows) * 2)
    total_summary_count = sum(
        worker.get("candidate_summary_count", 0) for worker in workers
    )
    if aggregate.get("promotion_packet_uris"):
        next_action = "Review aggregate promotion packet and create a no-order paper-trader staging handoff if eligible."
    elif total_summary_count >= expected_summaries:
        next_action = "Aggregate should run or is running; wait for promotion packet and growth projection."
    elif int(quota.get("free", 0)) >= machine_type_cpus(args.machine_type):
        next_action = "Launch next pending ticker shard tranche."
    else:
        next_action = "Wait for running ticker shards to complete, then reclaim quota and launch the next tranche."

    return {
        "generated_at_utc": utc_now(),
        "wave_id": args.wave_id,
        "gcs_prefix": args.gcs_prefix,
        "project": args.project,
        "dry_run": bool(args.dry_run),
        "hard_rules": HARD_RULES,
        "quota": quota,
        "summary_counts": {
            "total": total_summary_count,
            "expected": expected_summaries,
        },
        "worker_counts": {
            "total": len(rows),
            "completed": completed,
            "running": running,
            "failed": failed,
            "pending": pending,
        },
        "workers": workers,
        "aggregate": aggregate,
        "paper_trader_handoff": paper_trader_handoff_state(aggregate),
        "actions": actions,
        "uploaded_inputs": uploaded_inputs,
        "next_action": next_action,
    }


def main() -> int:
    args = parse_args()
    os.environ.setdefault("CLOUDSDK_PYTHON", args.python)
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", args.project)

    log("ticker_365d_watchdog_start")
    rows = load_launch_rows(args)
    expected_summaries = args.expected_summaries or (len(rows) * 2)
    uploaded_inputs = refresh_inputs(args)
    if uploaded_inputs:
        log(f"refreshed_inputs count={len(uploaded_inputs)}")

    instances = list_instances(args)
    instances_by_symbol = ticker_instances(instances)
    statuses = load_worker_statuses(args)
    counts_by_worker = worker_artifact_counts(args)
    aggregate = aggregate_state(args)
    quota = quota_snapshot(args, instances)

    stopped = stop_completed_instances(
        args, rows, instances_by_symbol, statuses, counts_by_worker
    )
    if stopped and not args.dry_run:
        time.sleep(10)
        instances = list_instances(args)
        instances_by_symbol = ticker_instances(instances)
        quota = quota_snapshot(args, instances)

    launched = launch_pending_instances(
        args, rows, instances_by_symbol, statuses, counts_by_worker, quota
    )
    if launched and not args.dry_run:
        time.sleep(10)
        instances = list_instances(args)
        instances_by_symbol = ticker_instances(instances)
        quota = quota_snapshot(args, instances)

    total_summary_count = sum(
        item.get("candidate_summary_count", 0) for item in counts_by_worker.values()
    )
    launched_aggregate = launch_aggregate_if_ready(
        args, instances, aggregate, total_summary_count, expected_summaries, quota
    )
    if launched_aggregate and not args.dry_run:
        time.sleep(5)
        instances = list_instances(args)
        instances_by_symbol = ticker_instances(instances)
        quota = quota_snapshot(args, instances)

    aggregate = aggregate_state(args)
    actions = {
        "stopped_instances": stopped,
        "launched_instances": launched,
        "launched_aggregate_instances": launched_aggregate,
    }
    status = build_status(
        args,
        rows,
        instances_by_symbol,
        statuses,
        counts_by_worker,
        quota,
        aggregate,
        actions,
        uploaded_inputs,
    )
    json_path, md_path = write_status_files(args, status)
    upload_status_files(args, [json_path, md_path])
    log(
        "ticker_365d_watchdog_done "
        f"summaries={status['summary_counts']['total']}/{status['summary_counts']['expected']} "
        f"completed={status['worker_counts']['completed']}/{status['worker_counts']['total']} "
        f"running={status['worker_counts']['running']} "
        f"pending={status['worker_counts']['pending']} "
        f"failed={status['worker_counts']['failed']} "
        f"launched={len(launched)} stopped={len(stopped)} aggregate_launched={len(launched_aggregate)}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        log(f"ticker_365d_watchdog_failed error={exc}")
        raise

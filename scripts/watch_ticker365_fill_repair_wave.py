from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tarfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT = "codexalpaca"
DEFAULT_WAVE_ID = "ticker365_fill_repair_20260503T1330Z"
DEFAULT_GCS_PREFIX = (
    "gs://codexalpaca-control-us/research_results/"
    "ticker365_fill_repair_20260503T1330Z"
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
DEFAULT_LAUNCH_ROWS_URI = (
    "gs://codexalpaca-control-us/research_results/"
    "ticker_365d_all_available_20260501T2300Z/inputs/"
    "ticker_365d_all_available_launch_rows.json"
)
DEFAULT_GCLOUD = (
    r"C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
)
DEFAULT_PYTHON = r"C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
DEFAULT_INSTANCE_SUFFIX = "20260503b"
DEFAULT_LAG_PROFILES = "10:10,30:60"
DEFAULT_SELECTORS = "nearest_contract,entry_liquidity_first_research_only"
DEFAULT_FALLBACK_ZONES = "us-west1-a,us-central1-a,us-east1-b,us-east4-a"
HARD_RULES = [
    "Do not start trading.",
    "Do not submit paper orders.",
    "Do not modify live manifests.",
    "Do not change risk policy.",
    "Do not lower fill_coverage >= 0.90.",
    "Promotion means governed validation review until a packet explicitly says eligible.",
]


class CommandError(RuntimeError):
    def __init__(self, command: list[str], returncode: int, output: str) -> None:
        super().__init__(
            f"command failed ({returncode}): {' '.join(command)}\n{output.strip()}"
        )
        self.command = command
        self.returncode = returncode
        self.output = output


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(message: str) -> None:
    print(f"{utc_now()} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quota-aware GCP watchdog for ticker365 fill-repair timing sweeps. "
            "This is research-only and cannot place orders."
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
    parser.add_argument("--machine-type", default="e2-standard-4")
    parser.add_argument("--boot-disk-size-gb", type=int, default=40)
    parser.add_argument("--boot-disk-type", default="pd-standard")
    parser.add_argument("--aggregate-zone", default="us-central1-a")
    parser.add_argument(
        "--aggregate-fallback-zones",
        default=DEFAULT_FALLBACK_ZONES,
        help="Comma-separated aggregate launch fallback zones used when the primary zone is quota-blocked.",
    )
    parser.add_argument("--aggregate-machine-type", default="e2-standard-2")
    parser.add_argument("--instance-suffix", default=DEFAULT_INSTANCE_SUFFIX)
    parser.add_argument("--fallback-zones", default=DEFAULT_FALLBACK_ZONES)
    parser.add_argument(
        "--prefer-fallback-zones",
        action="store_true",
        help="Try fallback zones before each launch row's preferred zone.",
    )
    parser.add_argument("--max-launches-per-run", type=int, default=8)
    parser.add_argument("--max-retry-attempts", type=int, default=3)
    parser.add_argument("--lag-profiles", default=DEFAULT_LAG_PROFILES)
    parser.add_argument("--selectors", default=DEFAULT_SELECTORS)
    parser.add_argument(
        "--entry-bar-lookup-mode",
        default="first_bar_at_or_after_entry_within_lag",
        choices=[
            "first_bar_at_or_after_entry_within_lag",
            "first_bar_at_or_after_or_asof_entry_within_lag",
        ],
    )
    parser.add_argument("--max-entry-staleness-minutes", type=float, default=5.0)
    parser.add_argument(
        "--exit-bar-lookup-mode",
        default="first_bar_at_or_after_exit_within_lag",
        choices=[
            "first_bar_at_or_after_exit_within_lag",
            "first_bar_at_or_after_or_prior_exit_within_lag",
        ],
    )
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument(
        "--expected-summary-count-override",
        type=int,
        default=None,
        help=(
            "Override aggregate readiness summary count for manually sharded waves, "
            "for example QQQ candidate-window shards."
        ),
    )
    parser.add_argument("--test-date-count", type=int, default=20)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--allocation-fraction", type=float, default=0.05)
    parser.add_argument("--slippage-bps", type=float, default=10.0)
    parser.add_argument("--fee-per-contract", type=float, default=0.65)
    parser.add_argument(
        "--stock-session-filter",
        default="option_rth_same_day",
        choices=["none", "option_rth_same_day"],
    )
    parser.add_argument("--target-equity", type=float, default=300_000.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-refresh-inputs", action="store_true")
    parser.add_argument(
        "--delete-completed-worker-instances",
        action="store_true",
        help=(
            "Delete only completed, stopped worker VMs for this exact wave suffix after "
            "their GCS artifacts are confirmed. This frees regional instance quota for "
            "the aggregate builder without touching live/paper infrastructure."
        ),
    )
    return parser.parse_args()


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
    return json.loads(output) if output.strip() else default


def gcloud(args: argparse.Namespace, *parts: str) -> list[str]:
    return [args.gcloud, *parts]


def metadata_arg(metadata: dict[str, str]) -> str:
    return ",".join(f"{key}={value}" for key, value in metadata.items())


def _csv_count(value: str) -> int:
    return len([item for item in value.split(",") if item.strip()])


def _csv_values(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def expected_summary_count(args: argparse.Namespace, row_count: int) -> int:
    return row_count * _csv_count(args.selectors) * _csv_count(args.lag_profiles)


def expected_per_worker(args: argparse.Namespace) -> int:
    return _csv_count(args.selectors) * _csv_count(args.lag_profiles)


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
    if args.launch_rows_uri != DEFAULT_LAUNCH_ROWS_URI:
        rows = json.loads(storage_cat(args, args.launch_rows_uri))
    elif local_path.exists():
        rows = json.loads(local_path.read_text(encoding="utf-8"))
    else:
        rows = json.loads(storage_cat(args, args.launch_rows_uri))
    return [row for row in rows if isinstance(row, dict)]


def machine_type_cpus(machine_type: str) -> int:
    match = re.search(r"(?:standard|highmem|highcpu)-(\d+)$", machine_type)
    if match:
        return int(match.group(1))
    if machine_type.endswith(("-micro", "-small")):
        return 1
    return 2


def instance_zone(instance: dict[str, Any]) -> str:
    return str(instance.get("zone", "")).split("/")[-1]


def machine_type_name(instance: dict[str, Any]) -> str:
    return str(instance.get("machineType", "")).split("/")[-1]


def list_instances(args: argparse.Namespace) -> list[dict[str, Any]]:
    payload = run_json(
        gcloud(args, "compute", "instances", "list", "--project", args.project, "--format=json"),
        default=[],
    )
    return payload if isinstance(payload, list) else []


def quota_snapshot(args: argparse.Namespace, instances: list[dict[str, Any]]) -> dict[str, Any]:
    running_cpu = sum(
        machine_type_cpus(machine_type_name(instance))
        for instance in instances
        if instance.get("status") == "RUNNING"
    )
    try:
        info = run_json(
            gcloud(
                args,
                "compute",
                "project-info",
                "describe",
                "--project",
                args.project,
                "--format=json",
            ),
            default={},
        )
        for quota in info.get("quotas", []):
            if quota.get("metric") == "CPUS_ALL_REGIONS":
                usage = float(quota.get("usage", running_cpu))
                limit = float(quota.get("limit", 32))
                return {
                    "metric": "CPUS_ALL_REGIONS",
                    "usage": usage,
                    "limit": limit,
                    "free": max(0, int(limit - usage)),
                    "usage_from_running_instances": running_cpu,
                }
    except Exception as exc:
        log(f"quota_lookup_failed fallback_running_cpu={running_cpu} error={exc}")
    return {
        "metric": "CPUS_ALL_REGIONS",
        "usage": float(running_cpu),
        "limit": 32.0,
        "free": max(0, 32 - running_cpu),
        "usage_from_running_instances": running_cpu,
    }


def worker_id(symbol: str) -> str:
    return f"ticker365fillrepair_{symbol.lower()}"


def instance_base(args: argparse.Namespace, symbol: str) -> str:
    return f"ticker365-repair-{symbol.lower()}-{args.instance_suffix}"


def aggregate_instance_name(args: argparse.Namespace) -> str:
    return f"ticker365-repair-agg-{args.instance_suffix}"


def repair_instances(args: argparse.Namespace, instances: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for instance in instances:
        name = str(instance.get("name", ""))
        prefix = "ticker365-repair-"
        suffix = f"-{args.instance_suffix}"
        if not name.startswith(prefix) or suffix not in name:
            continue
        if name.startswith(f"ticker365-repair-agg-{args.instance_suffix}"):
            continue
        labels = instance.get("labels") or {}
        label_symbol = str(labels.get("symbol") or "").upper()
        if label_symbol:
            grouped.setdefault(label_symbol, []).append(instance)
            continue
        middle = name[len(prefix) :].split(suffix, 1)[0]
        grouped.setdefault(middle.split("-", 1)[0].upper(), []).append(instance)
    return grouped


def active_instance_exists(instances: list[dict[str, Any]]) -> bool:
    active_statuses = {"PROVISIONING", "STAGING", "RUNNING", "REPAIRING", "SUSPENDING"}
    return any(instance.get("status") in active_statuses for instance in instances)


def count_by_worker(uris: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for uri in uris:
        match = re.search(r"/workers/([^/]+)/", uri)
        if match:
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


def completed_worker(phase: str, counts: dict[str, int], expected_count: int) -> bool:
    if phase == "completed":
        return True
    return (
        counts.get("candidate_summary_count", 0) >= expected_count
        and counts.get("portfolio_report_count", 0) >= 1
        and counts.get("promotion_packet_count", 0) >= 1
    )


def _infer_symbol_from_worker_id(worker_id_value: str) -> str:
    match = re.search(r"ticker365fillrepair_([a-z0-9]+)(?:_|$)", worker_id_value.lower())
    return match.group(1).upper() if match else ""


def _profile_shard_suffix(canonical_worker_id: str, worker_id_value: str) -> str:
    prefix = f"{canonical_worker_id}_"
    if worker_id_value.startswith(prefix):
        return worker_id_value[len(prefix) :]
    return ""


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _instances_for_profile_shard(
    *,
    instances: list[dict[str, Any]],
    canonical_worker_id: str,
    profile_worker_id: str,
) -> list[dict[str, Any]]:
    suffix = _profile_shard_suffix(canonical_worker_id, profile_worker_id)
    if not suffix:
        return []
    needle = f"-{_slug(suffix)}-"
    return [
        instance
        for instance in instances
        if needle in f"-{_slug(str(instance.get('name', '')))}-"
    ]


def _count_from_status_list(value: Any, default_value: str) -> int:
    text = str(value or default_value).replace(";", ",")
    return _csv_count(text)


def _expected_for_profile_worker(
    args: argparse.Namespace, payload: dict[str, Any]
) -> int:
    explicit = payload.get("expected_candidate_summary_count")
    if explicit not in (None, ""):
        try:
            return max(1, int(explicit))
        except (TypeError, ValueError):
            pass
    return _count_from_status_list(payload.get("selectors"), args.selectors) * _count_from_status_list(
        payload.get("lag_profiles"), args.lag_profiles
    )


def profile_shard_status(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    instances_by_symbol: dict[str, list[dict[str, Any]]],
    statuses: dict[str, dict[str, Any]],
    counts_by_worker: dict[str, dict[str, int]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    canonical_worker_ids = {worker_id(str(row["symbol"]).upper()) for row in rows}
    extra_worker_ids = sorted((set(statuses) | set(counts_by_worker)) - canonical_worker_ids)
    profile_workers: list[dict[str, Any]] = []
    completed = failed = running = pending = 0

    for wid in extra_worker_ids:
        payload = statuses.get(wid, {})
        symbol = str(payload.get("symbol") or _infer_symbol_from_worker_id(wid)).upper()
        canonical_worker_id = worker_id(symbol) if symbol else ""
        counts = counts_by_worker.get(wid, {})
        phase = str(payload.get("phase", "not_started"))
        expected_profile_worker = _expected_for_profile_worker(args, payload)
        instances = _instances_for_profile_shard(
            instances=instances_by_symbol.get(symbol, []),
            canonical_worker_id=canonical_worker_id,
            profile_worker_id=wid,
        )
        is_completed = completed_worker(phase, counts, expected_profile_worker)
        is_failed = phase == "failed"
        is_running = active_instance_exists(instances)
        if is_completed:
            completed += 1
        elif is_failed:
            failed += 1
        elif is_running:
            running += 1
        else:
            pending += 1
        profile_workers.append(
            {
                "worker_id": wid,
                "symbol": symbol,
                "phase": phase,
                "detail": payload.get("detail"),
                "status_uri": payload.get("status_uri"),
                "candidate_summary_count": counts.get("candidate_summary_count", 0),
                "expected_candidate_summary_count": expected_profile_worker,
                "portfolio_report_count": counts.get("portfolio_report_count", 0),
                "promotion_packet_count": counts.get("promotion_packet_count", 0),
                "instances": [
                    {
                        "name": str(instance.get("name")),
                        "zone": instance_zone(instance),
                        "machine_type": machine_type_name(instance),
                        "status": str(instance.get("status")),
                    }
                    for instance in instances
                ],
                "completed": is_completed,
                "failed": is_failed,
            }
        )

    return profile_workers, {
        "total": len(profile_workers),
        "completed": completed,
        "running": running,
        "failed": failed,
        "pending": pending,
    }


def aggregate_state(args: argparse.Namespace) -> dict[str, Any]:
    packets = storage_ls(
        args,
        f"{args.gcs_prefix}/aggregate/promotion_packet/**/research_promotion_review_packet.json",
    )
    reports = storage_ls(
        args,
        f"{args.gcs_prefix}/aggregate/portfolio_report/**/research_portfolio_report.json",
    )
    growth = storage_ls(
        args,
        f"{args.gcs_prefix}/aggregate/growth_projection/**/portfolio_growth_projection.json",
    )
    summary: dict[str, Any] = {}
    if packets:
        try:
            packet = json.loads(storage_cat(args, packets[0]))
            gate = packet.get("gate_summary", {})
            summary = {
                "decision": packet.get("decision"),
                "candidate_count": gate.get("candidate_count"),
                "eligible_for_promotion_review_count": gate.get(
                    "eligible_for_promotion_review_count"
                ),
                "broker_facing": packet.get("broker_facing"),
            }
        except Exception as exc:
            summary = {"read_error": str(exc)}
    return {
        "promotion_packet_uris": packets,
        "portfolio_report_uris": reports,
        "growth_projection_uris": growth,
        "promotion_packet_summary": summary,
    }


def create_source_archive(args: argparse.Namespace) -> Path:
    archive_path = REPO_ROOT / "reports" / "gcp_research" / "ticker365_fill_repair_source.tar.gz"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    tracked = run_command(["git", "ls-files"]).splitlines()
    with tarfile.open(archive_path, "w:gz") as tar:
        for rel in tracked:
            path = REPO_ROOT / rel
            if path.is_file():
                tar.add(path, arcname=rel)
    return archive_path


def refresh_inputs(args: argparse.Namespace, rows: list[dict[str, Any]]) -> list[str]:
    if args.dry_run or args.no_refresh_inputs:
        return []
    uploaded: list[str] = []
    archive_path = create_source_archive(args)
    launch_rows_path = (
        REPO_ROOT
        / "reports"
        / "gcp_research"
        / f"{args.wave_id}_launch_rows.json"
    )
    launch_rows_path.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    uploads = [
        (archive_path, args.source_archive_uri),
        (
            REPO_ROOT / "scripts" / "gcp_single_ticker_365d_shard.sh",
            f"{args.gcs_prefix}/inputs/startup/gcp_single_ticker_365d_shard.sh",
        ),
        (
            REPO_ROOT / "scripts" / "gcp_ticker_365d_aggregate_watch.sh",
            f"{args.gcs_prefix}/inputs/startup/gcp_ticker_365d_aggregate_watch.sh",
        ),
    ]
    uploads.append(
        (
            launch_rows_path,
            f"{args.gcs_prefix}/inputs/ticker_365d_all_available_launch_rows.json",
        )
    )
    for source, target in uploads:
        if source.exists():
            run_command(gcloud(args, "storage", "cp", str(source), target), timeout=900)
            uploaded.append(target)
    return uploaded


def next_instance_name(args: argparse.Namespace, symbol: str, existing: list[dict[str, Any]]) -> str:
    base = instance_base(args, symbol)
    if not existing:
        return base
    retry = 1
    for instance in existing:
        name = str(instance.get("name", ""))
        match = re.search(r"-r(\d+)$", name)
        if match:
            retry = max(retry, int(match.group(1)))
    return f"{base}-r{retry + 1}"


def launch_worker(
    args: argparse.Namespace,
    row: dict[str, Any],
    instance_name: str,
    *,
    zone: str,
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
        "initial_cash": f"{args.initial_cash:g}",
        "top_n": str(args.top_n),
        "test_date_count": str(args.test_date_count),
        "allocation_fraction": f"{args.allocation_fraction:g}",
        "slippage_bps": f"{args.slippage_bps:g}",
        "fee_per_contract": f"{args.fee_per_contract:g}",
        "selectors": args.selectors.replace(",", ";"),
        "lag_profiles": args.lag_profiles.replace(",", ";"),
        "entry_bar_lookup_mode": args.entry_bar_lookup_mode,
        "max_entry_staleness_minutes": f"{args.max_entry_staleness_minutes:g}",
        "exit_bar_lookup_mode": args.exit_bar_lookup_mode,
        "stock_session_filter": args.stock_session_filter,
    }
    dataset_label = str(row.get("dataset_id", "unknown")).replace("_", "-")[:32]
    labels = (
        f"wave=ticker365-fillrepair,role=ticker365-repair,"
        f"symbol={symbol.lower()},dataset={dataset_label}"
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
        zone,
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
        args.boot_disk_type,
        "--labels",
        labels,
        "--metadata",
        metadata_arg(metadata),
        "--metadata-from-file",
        f"startup-script={startup_script}",
    )
    log(f"launching_fill_repair_worker symbol={symbol} instance={instance_name} zone={zone}")
    if not args.dry_run:
        run_command(command, timeout=900)


def stop_completed_instances(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    instances_by_symbol: dict[str, list[dict[str, Any]]],
    statuses: dict[str, dict[str, Any]],
    counts_by_worker: dict[str, dict[str, int]],
) -> list[dict[str, str]]:
    stopped: list[dict[str, str]] = []
    expected = expected_per_worker(args)
    for row in rows:
        symbol = str(row["symbol"]).upper()
        wid = worker_id(symbol)
        phase = str(statuses.get(wid, {}).get("phase", "unknown"))
        counts = counts_by_worker.get(wid, {})
        if not completed_worker(phase, counts, expected):
            continue
        for instance in instances_by_symbol.get(symbol, []):
            if instance.get("status") != "RUNNING":
                continue
            name = str(instance.get("name"))
            zone = instance_zone(instance)
            log(f"stopping_completed_fill_repair_worker symbol={symbol} instance={name}")
            if not args.dry_run:
                run_command(
                    gcloud(args, "compute", "instances", "stop", name, "--zone", zone, "--quiet"),
                    timeout=600,
                )
            stopped.append({"symbol": symbol, "instance": name, "zone": zone})
    return stopped


def delete_completed_worker_instances(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    instances_by_symbol: dict[str, list[dict[str, Any]]],
    statuses: dict[str, dict[str, Any]],
    counts_by_worker: dict[str, dict[str, int]],
) -> list[dict[str, str]]:
    if not args.delete_completed_worker_instances:
        return []
    deleted: list[dict[str, str]] = []
    expected = expected_per_worker(args)
    valid_symbols = {str(row["symbol"]).upper() for row in rows}
    expected_suffix = f"-{args.instance_suffix}"
    for row in rows:
        symbol = str(row["symbol"]).upper()
        wid = worker_id(symbol)
        phase = str(statuses.get(wid, {}).get("phase", "unknown"))
        counts = counts_by_worker.get(wid, {})
        if not completed_worker(phase, counts, expected):
            continue
        for instance in instances_by_symbol.get(symbol, []):
            name = str(instance.get("name", ""))
            status = str(instance.get("status", ""))
            labels = instance.get("labels") or {}
            if symbol not in valid_symbols:
                continue
            if status != "TERMINATED":
                continue
            if labels.get("role") != "ticker365-repair":
                continue
            if not name.startswith(f"ticker365-repair-{symbol.lower()}-"):
                continue
            if not name.endswith(expected_suffix):
                continue
            zone = instance_zone(instance)
            log(f"deleting_completed_fill_repair_worker symbol={symbol} instance={name} zone={zone}")
            if not args.dry_run:
                run_command(
                    gcloud(args, "compute", "instances", "delete", name, "--zone", zone, "--quiet"),
                    timeout=900,
                )
            deleted.append({"symbol": symbol, "instance": name, "zone": zone})
    return deleted


def launch_pending_workers(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    instances_by_symbol: dict[str, list[dict[str, Any]]],
    statuses: dict[str, dict[str, Any]],
    counts_by_worker: dict[str, dict[str, int]],
    quota: dict[str, Any],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    cpus_per_worker = machine_type_cpus(args.machine_type)
    capacity = min(args.max_launches_per_run, int(quota.get("free", 0)) // cpus_per_worker)
    launched: list[dict[str, str]] = []
    launch_errors: list[dict[str, str]] = []
    if capacity <= 0:
        return launched, launch_errors
    expected = expected_per_worker(args)
    for row in rows:
        if len(launched) >= capacity:
            break
        symbol = str(row["symbol"]).upper()
        wid = worker_id(symbol)
        phase = str(statuses.get(wid, {}).get("phase", "not_started"))
        counts = counts_by_worker.get(wid, {})
        existing = instances_by_symbol.get(symbol, [])
        if completed_worker(phase, counts, expected):
            continue
        if phase == "failed":
            continue
        if active_instance_exists(existing):
            continue
        if len(existing) >= args.max_retry_attempts:
            continue
        name = next_instance_name(args, symbol, existing)
        preferred_zone = str(row["zone"])
        if args.prefer_fallback_zones:
            zones = _csv_values(args.fallback_zones)
            if preferred_zone not in zones:
                zones.append(preferred_zone)
        else:
            zones = [preferred_zone]
            zones.extend(zone for zone in _csv_values(args.fallback_zones) if zone not in zones)
        launched_row = None
        for zone in zones:
            try:
                launch_worker(args, row, name, zone=zone)
            except CommandError as exc:
                message = exc.output.strip().splitlines()[-1] if exc.output.strip() else str(exc)
                if "already exists" in message:
                    log(
                        "fill_repair_launch_already_exists "
                        f"symbol={symbol} instance={name} zone={zone}"
                    )
                    launched_row = {"symbol": symbol, "instance": name, "zone": zone}
                    break
                log(
                    "fill_repair_launch_failed "
                    f"symbol={symbol} instance={name} zone={zone} error={message}"
                )
                launch_errors.append(
                    {
                        "symbol": symbol,
                        "instance": name,
                        "zone": zone,
                        "error": message,
                    }
                )
                continue
            launched_row = {"symbol": symbol, "instance": name, "zone": zone}
            break
        if launched_row is None:
            continue
        launched.append(launched_row)
    return launched, launch_errors


def aggregate_instance_exists(args: argparse.Namespace, instances: list[dict[str, Any]]) -> bool:
    name = aggregate_instance_name(args)
    return any(
        str(instance.get("name", "")) == name
        and instance.get("status") in {"PROVISIONING", "STAGING", "RUNNING", "REPAIRING"}
        for instance in instances
    )


def launch_aggregate_if_ready(
    args: argparse.Namespace,
    instances: list[dict[str, Any]],
    aggregate: dict[str, Any],
    summary_count: int,
    expected_count: int,
    quota: dict[str, Any],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    if aggregate.get("promotion_packet_uris"):
        return [], []
    if summary_count < expected_count:
        return [], []
    if aggregate_instance_exists(args, instances):
        return [], []
    cpus_required = machine_type_cpus(args.aggregate_machine_type)
    if int(quota.get("free", 0)) < cpus_required:
        return [], []
    name = aggregate_instance_name(args)
    metadata = {
        "wave_id": args.wave_id,
        "gcs_prefix": args.gcs_prefix,
        "source_archive_uri": args.source_archive_uri,
        "expected_summary_count": str(expected_count),
        "initial_cash": f"{args.initial_cash:g}",
        "target_equity": f"{args.target_equity:g}",
        "check_interval_seconds": "900",
        "max_wait_seconds": "43200",
    }
    startup_script = str(REPO_ROOT / "scripts" / "gcp_ticker_365d_aggregate_watch.sh")
    zones = [args.aggregate_zone]
    zones.extend(zone for zone in _csv_values(args.aggregate_fallback_zones) if zone not in zones)
    launch_errors: list[dict[str, str]] = []
    for zone in zones:
        command = gcloud(
            args,
            "compute",
            "instances",
            "create",
            name,
            "--project",
            args.project,
            "--zone",
            zone,
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
            args.boot_disk_type,
            "--labels",
            "wave=ticker365-fillrepair,role=ticker365-repair-aggregate",
            "--metadata",
            metadata_arg(metadata),
            "--metadata-from-file",
            f"startup-script={startup_script}",
        )
        log(f"launching_fill_repair_aggregate instance={name} zone={zone}")
        if args.dry_run:
            return [{"instance": name, "zone": zone}], launch_errors
        try:
            run_command(command, timeout=900)
        except CommandError as exc:
            message = exc.output.strip().splitlines()[-1] if exc.output.strip() else str(exc)
            if "already exists" in message:
                return [{"instance": name, "zone": zone}], launch_errors
            log(f"fill_repair_aggregate_launch_failed instance={name} zone={zone} error={message}")
            launch_errors.append({"instance": name, "zone": zone, "error": message})
            continue
        return [{"instance": name, "zone": zone}], launch_errors
    return [], launch_errors


def paper_handoff(aggregate: dict[str, Any]) -> dict[str, Any]:
    summary = aggregate.get("promotion_packet_summary") or {}
    eligible = int(summary.get("eligible_for_promotion_review_count") or 0)
    if eligible > 0:
        return {
            "status": "eligible_packet_ready_for_operator_review_no_orders",
            "paper_orders": False,
            "live_manifest_effect": "none",
        }
    if aggregate.get("promotion_packet_uris"):
        return {
            "status": "aggregate_packet_ready_no_eligible_candidates",
            "paper_orders": False,
            "live_manifest_effect": "none",
        }
    return {
        "status": "blocked_until_fill_repair_aggregate",
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
    expected_worker = expected_per_worker(args)
    workers: list[dict[str, Any]] = []
    completed = failed = running = pending = 0
    profile_workers, profile_counts = profile_shard_status(
        args, rows, instances_by_symbol, statuses, counts_by_worker
    )
    for row in rows:
        symbol = str(row["symbol"]).upper()
        wid = worker_id(symbol)
        counts = counts_by_worker.get(wid, {})
        phase = str(statuses.get(wid, {}).get("phase", "not_started"))
        instances = instances_by_symbol.get(symbol, [])
        is_completed = completed_worker(phase, counts, expected_worker)
        is_failed = phase == "failed"
        is_running = active_instance_exists(instances)
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
                "status_uri": statuses.get(wid, {}).get("status_uri"),
                "candidate_summary_count": counts.get("candidate_summary_count", 0),
                "expected_candidate_summary_count": expected_worker,
                "portfolio_report_count": counts.get("portfolio_report_count", 0),
                "promotion_packet_count": counts.get("promotion_packet_count", 0),
                "instances": [
                    {
                        "name": str(instance.get("name")),
                        "zone": instance_zone(instance),
                        "machine_type": machine_type_name(instance),
                        "status": str(instance.get("status")),
                    }
                    for instance in instances
                ],
                "completed": is_completed,
                "failed": is_failed,
            }
        )
    summary_count = sum(
        worker_counts.get("candidate_summary_count", 0)
        for worker_counts in counts_by_worker.values()
    )
    expected_total = args.expected_summary_count_override or expected_summary_count(args, len(rows))
    if aggregate.get("promotion_packet_uris"):
        next_action = "Review repair aggregate promotion packet; stage no-order paper handoff only if eligible."
    elif summary_count >= expected_total:
        next_action = "Wait for or launch aggregate packet builder."
    elif profile_counts["running"] > 0 or running > 0:
        next_action = "Wait for running fill-repair profile shards to finish and self-stop."
    elif args.max_launches_per_run <= 0:
        next_action = "No worker launches requested this run; wait for profile shard automation or launch manually."
    elif int(quota.get("free", 0)) >= machine_type_cpus(args.machine_type):
        next_action = "Launch next fill-repair worker tranche."
    else:
        next_action = "Wait for running fill-repair workers to finish and self-stop."
    return {
        "generated_at_utc": utc_now(),
        "wave_id": args.wave_id,
        "gcs_prefix": args.gcs_prefix,
        "project": args.project,
        "dry_run": bool(args.dry_run),
        "hard_rules": HARD_RULES,
        "lag_profiles": args.lag_profiles,
        "selectors": args.selectors,
        "entry_bar_lookup_mode": args.entry_bar_lookup_mode,
        "max_entry_staleness_minutes": args.max_entry_staleness_minutes,
        "exit_bar_lookup_mode": args.exit_bar_lookup_mode,
        "stock_session_filter": args.stock_session_filter,
        "top_n": args.top_n,
        "expected_summary_count_override": args.expected_summary_count_override,
        "quota": quota,
        "summary_counts": {"total": summary_count, "expected": expected_total},
        "worker_counts": {
            "total": len(rows),
            "completed": completed,
            "running": running,
            "failed": failed,
            "pending": pending,
            "profile_shards": profile_counts,
        },
        "workers": workers,
        "profile_shards": profile_workers,
        "aggregate": aggregate,
        "paper_trader_handoff": paper_handoff(aggregate),
        "actions": actions,
        "uploaded_inputs": uploaded_inputs,
        "next_action": next_action,
    }


def write_status(args: argparse.Namespace, status: dict[str, Any]) -> tuple[Path, Path]:
    out_dir = REPO_ROOT / "reports" / "gcp_research" / "ticker365_fill_repair_20260503"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "ticker365_fill_repair_watch_status.json"
    md_path = out_dir / "ticker365_fill_repair_watch_status.md"
    json_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# Ticker365 Fill Repair Watch Status - 2026-05-03",
        "",
        f"- Generated UTC: `{status['generated_at_utc']}`",
        f"- Wave ID: `{status['wave_id']}`",
        f"- GCS prefix: `{status['gcs_prefix']}`",
        f"- Lag profiles: `{status['lag_profiles']}`",
        f"- Selectors: `{status['selectors']}`",
        f"- Entry lookup mode: `{status['entry_bar_lookup_mode']}`",
        f"- Exit lookup mode: `{status['exit_bar_lookup_mode']}`",
        f"- Stock session filter: `{status['stock_session_filter']}`",
        f"- Top N per symbol: `{status['top_n']}`",
        f"- Quota: `{status['quota']['usage']}/{status['quota']['limit']}` CPUs, free `{status['quota']['free']}`",
        f"- Candidate summaries: `{status['summary_counts']['total']}/{status['summary_counts']['expected']}`",
        f"- Completed workers: `{status['worker_counts']['completed']}/{status['worker_counts']['total']}`",
        f"- Running workers: `{status['worker_counts']['running']}`",
        f"- Pending workers: `{status['worker_counts']['pending']}`",
        f"- Profile shards: `{status['worker_counts']['profile_shards']['completed']}/{status['worker_counts']['profile_shards']['total']}` completed, `{status['worker_counts']['profile_shards']['running']}` running",
        f"- Aggregate packet ready: `{bool(status['aggregate']['promotion_packet_uris'])}`",
        f"- Paper handoff: `{status['paper_trader_handoff']['status']}`",
        "",
        "## Actions This Run",
        "",
    ]
    actions = status["actions"]
    if (
        not actions["stopped_instances"]
        and not actions.get("deleted_instances")
        and not actions["launched_instances"]
        and not actions["launched_aggregate_instances"]
        and not actions.get("launch_errors")
        and not actions.get("aggregate_launch_errors")
    ):
        lines.append("- No VM changes were needed this run.")
    for item in actions["stopped_instances"]:
        lines.append(f"- Stopped completed worker `{item['instance']}` for `{item['symbol']}`.")
    for item in actions.get("deleted_instances", []):
        lines.append(
            f"- Deleted completed stopped worker `{item['instance']}` for `{item['symbol']}` after GCS artifacts were confirmed."
        )
    for item in actions["launched_instances"]:
        lines.append(f"- Launched fill-repair worker `{item['instance']}` for `{item['symbol']}`.")
    for item in actions.get("launch_errors", []):
        lines.append(
            f"- Launch retry for `{item['symbol']}` failed on `{item['instance']}`: `{item['error']}`."
        )
    for item in actions["launched_aggregate_instances"]:
        lines.append(f"- Launched aggregate VM `{item['instance']}`.")
    for item in actions.get("aggregate_launch_errors", []):
        lines.append(
            f"- Aggregate launch retry failed on `{item['zone']}` for `{item['instance']}`: `{item['error']}`."
        )
    lines.extend(["", "## Worker State", ""])
    lines.append("| Symbol | Phase | Candidate Summaries | Reports | Packets | Instances |")
    lines.append("| --- | --- | ---: | ---: | ---: | --- |")
    for worker in status["workers"]:
        instance_text = ", ".join(
            f"{item['name']}:{item['status']}" for item in worker["instances"]
        ) or "none"
        lines.append(
            "| "
            f"`{worker['symbol']}` | `{worker['phase']}` | "
            f"{worker['candidate_summary_count']}/{worker['expected_candidate_summary_count']} | "
            f"{worker['portfolio_report_count']} | {worker['promotion_packet_count']} | "
            f"{instance_text} |"
        )
    if status.get("profile_shards"):
        lines.extend(["", "## Profile Shard State", ""])
        lines.append("| Worker ID | Symbol | Phase | Candidate Summaries | Reports | Packets | Instances |")
        lines.append("| --- | --- | --- | ---: | ---: | ---: | --- |")
        for worker in status["profile_shards"]:
            instance_text = ", ".join(
                f"{item['name']}:{item['status']}" for item in worker["instances"]
            ) or "none"
            lines.append(
                "| "
                f"`{worker['worker_id']}` | `{worker['symbol']}` | `{worker['phase']}` | "
                f"{worker['candidate_summary_count']}/{worker['expected_candidate_summary_count']} | "
                f"{worker['portfolio_report_count']} | {worker['promotion_packet_count']} | "
                f"{instance_text} |"
            )
    lines.extend(["", "## Hard Rules", ""])
    for rule in HARD_RULES:
        lines.append(f"- {rule}")
    lines.extend(["", "## Next Action", "", f"- {status['next_action']}"])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def upload_status(args: argparse.Namespace, paths: list[Path]) -> None:
    if args.dry_run:
        return
    for path in paths:
        run_command(gcloud(args, "storage", "cp", str(path), f"{args.gcs_prefix}/watchdog/{path.name}"))


def main() -> int:
    args = parse_args()
    os.environ.setdefault("CLOUDSDK_PYTHON", args.python)
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", args.project)

    log("ticker365_fill_repair_watchdog_start")
    rows = load_launch_rows(args)
    uploaded = refresh_inputs(args, rows)
    if uploaded:
        log(f"refreshed_inputs count={len(uploaded)}")

    instances = list_instances(args)
    instances_by_symbol = repair_instances(args, instances)
    statuses = load_worker_statuses(args)
    counts = worker_artifact_counts(args)
    quota = quota_snapshot(args, instances)
    aggregate = aggregate_state(args)

    stopped = stop_completed_instances(args, rows, instances_by_symbol, statuses, counts)
    if stopped and not args.dry_run:
        time.sleep(10)
        instances = list_instances(args)
        instances_by_symbol = repair_instances(args, instances)
        quota = quota_snapshot(args, instances)

    deleted = delete_completed_worker_instances(args, rows, instances_by_symbol, statuses, counts)
    if deleted and not args.dry_run:
        time.sleep(10)
        instances = list_instances(args)
        instances_by_symbol = repair_instances(args, instances)
        quota = quota_snapshot(args, instances)

    launched, launch_errors = launch_pending_workers(
        args, rows, instances_by_symbol, statuses, counts, quota
    )
    if launched and not args.dry_run:
        time.sleep(10)
        instances = list_instances(args)
        instances_by_symbol = repair_instances(args, instances)
        quota = quota_snapshot(args, instances)

    summary_count = sum(
        worker_counts.get("candidate_summary_count", 0) for worker_counts in counts.values()
    )
    expected_total = args.expected_summary_count_override or expected_summary_count(args, len(rows))
    launched_aggregate, aggregate_launch_errors = launch_aggregate_if_ready(
        args, instances, aggregate, summary_count, expected_total, quota
    )
    if launched_aggregate and not args.dry_run:
        time.sleep(5)
        instances = list_instances(args)
        instances_by_symbol = repair_instances(args, instances)
        quota = quota_snapshot(args, instances)

    actions = {
        "stopped_instances": stopped,
        "deleted_instances": deleted,
        "launched_instances": launched,
        "launch_errors": launch_errors,
        "launched_aggregate_instances": launched_aggregate,
        "aggregate_launch_errors": aggregate_launch_errors,
    }
    status = build_status(
        args,
        rows,
        instances_by_symbol,
        statuses,
        counts,
        quota,
        aggregate_state(args),
        actions,
        uploaded,
    )
    paths = write_status(args, status)
    upload_status(args, list(paths))
    log(
        "ticker365_fill_repair_watchdog_complete "
        f"summaries={status['summary_counts']['total']}/{status['summary_counts']['expected']} "
        f"running={status['worker_counts']['running']} launched={len(launched)} "
        f"stopped={len(stopped)} deleted={len(deleted)} aggregate_launched={len(launched_aggregate)}"
    )
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

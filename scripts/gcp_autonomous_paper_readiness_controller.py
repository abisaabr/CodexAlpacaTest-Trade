from __future__ import annotations

import argparse
import csv
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
DEFAULT_SERVICE_ACCOUNT = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
DEFAULT_GCLOUD = os.environ.get("GCLOUD", "gcloud")
MICRO_WAVE_ID = "ticker365_qqq_fill_squash_micro_20260504T1730Z"
FULL_WAVE_ID = "ticker365_qqq_fill_squash_full126_20260504T1900Z"
AUTONOMOUS_WAVE_ID = "autonomous_paper_readiness_20260504"
MICRO_PREFIX = f"gs://codexalpaca-control-us/research_results/{MICRO_WAVE_ID}"
FULL_PREFIX = f"gs://codexalpaca-control-us/research_results/{FULL_WAVE_ID}"
STATUS_PREFIX = (
    "gs://codexalpaca-control-us/research_results/"
    f"{AUTONOMOUS_WAVE_ID}/controller"
)
SOURCE_ARCHIVE_URI = (
    "gs://codexalpaca-control-us/research_results/"
    f"{AUTONOMOUS_WAVE_ID}/inputs/source/codexalpaca_repo_source.tar.gz"
)
INPUT_DIR = REPO_ROOT / "docs" / "gcp_research" / "qqq_timing_redesign_20260504" / "inputs"
STARTUP_SHARD = REPO_ROOT / "scripts" / "gcp_single_ticker_365d_shard.sh"
STARTUP_AGGREGATE = REPO_ROOT / "scripts" / "gcp_ticker_365d_aggregate_watch.sh"
LOCAL_STATUS_DIR = REPO_ROOT / "reports" / "gcp_research" / "autonomous_paper_readiness_20260504"

STOCK_URI = (
    "gs://codexalpaca-data-us/research_stock_data/"
    "qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/"
)
CONTRACTS_URI = (
    "gs://codexalpaca-control-us/research_results/"
    "qqq_365d_next_trading_day_5x5_20260428/research_wave/"
    "qqq_365d_next_trading_day_5x5_20260428/dense_universe/"
    "selected_option_contracts/"
)
BARS_URI = (
    "gs://codexalpaca-data-us/research_option_data/"
    "qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/"
    "option_bars/underlying=QQQ/"
)
AGGREGATE_STATUS_PATH = "aggregate/status/ticker_365d_aggregate_status.json"
AGGREGATE_PROMOTION_PACKET_PATHS = [
    (
        "aggregate/promotion_packet/ticker_365d_all_available_promotion_packet/"
        "research_promotion_review_packet.json"
    ),
    "aggregate/promotion_packet/research_promotion_review_packet.json",
]
HARD_RULES = [
    "Do not start trading.",
    "Do not submit paper orders.",
    "Do not modify live manifests.",
    "Do not change risk policy.",
    "Do not lower fill_coverage >= 0.90.",
    "Diagnostic timing profiles cannot be promoted.",
    "Promotion means governed validation review until a packet explicitly says eligible.",
    "Runtime arming and paper launch require explicit operator approval.",
]
ACTIVE_STATUSES = {"PROVISIONING", "STAGING", "RUNNING", "REPAIRING", "SUSPENDING"}
FALLBACK_ZONES = ["us-east1-b", "us-central1-a", "us-west1-a", "us-east4-a"]
ALL_SELECTORS = ["nearest_contract", "entry_liquidity_first_research_only"]

MICRO_PROFILES = [
    {
        "name": "strict-e0x60",
        "slug": "strict_e0x60",
        "lag_profile": "0:60",
        "entry_mode": "first_bar_at_or_after_entry_within_lag",
        "entry_staleness": "0",
        "exit_mode": "first_bar_at_or_after_exit_within_lag",
        "session_filter": "option_rth_same_day",
        "preferred_zone": "us-east1-b",
        "diagnostic": False,
    },
    {
        "name": "strict-e15x120",
        "slug": "strict_e15x120",
        "lag_profile": "15:120",
        "entry_mode": "first_bar_at_or_after_entry_within_lag",
        "entry_staleness": "0",
        "exit_mode": "first_bar_at_or_after_exit_within_lag",
        "session_filter": "option_rth_same_day",
        "preferred_zone": "us-central1-a",
        "diagnostic": False,
    },
    {
        "name": "strict-e30x180",
        "slug": "strict_e30x180",
        "lag_profile": "30:180",
        "entry_mode": "first_bar_at_or_after_entry_within_lag",
        "entry_staleness": "0",
        "exit_mode": "first_bar_at_or_after_exit_within_lag",
        "session_filter": "option_rth_same_day",
        "preferred_zone": "us-west1-a",
        "diagnostic": False,
    },
    {
        "name": "asof-e5s1-x120",
        "slug": "asof_e5s1_x120",
        "lag_profile": "5:120",
        "entry_mode": "first_bar_at_or_after_or_asof_entry_within_lag",
        "entry_staleness": "1",
        "exit_mode": "first_bar_at_or_after_exit_within_lag",
        "session_filter": "option_rth_same_day",
        "preferred_zone": "us-east4-a",
        "diagnostic": True,
    },
    {
        "name": "diag-priorexit-e15x120",
        "slug": "diag_priorexit_e15x120",
        "lag_profile": "15:120",
        "entry_mode": "first_bar_at_or_after_entry_within_lag",
        "entry_staleness": "0",
        "exit_mode": "first_bar_at_or_after_or_prior_exit_within_lag",
        "session_filter": "option_rth_same_day",
        "preferred_zone": "us-east1-b",
        "diagnostic": True,
    },
    {
        "name": "diag-nosession-e15x120",
        "slug": "diag_nosession_e15x120",
        "lag_profile": "15:120",
        "entry_mode": "first_bar_at_or_after_entry_within_lag",
        "entry_staleness": "0",
        "exit_mode": "first_bar_at_or_after_exit_within_lag",
        "session_filter": "none",
        "preferred_zone": "us-central1-a",
        "diagnostic": True,
    },
]
STRICT_PROFILE_MAP = {item["slug"]: item for item in MICRO_PROFILES if not item["diagnostic"]}


class CommandError(RuntimeError):
    def __init__(self, command: list[str], returncode: int, output: str) -> None:
        super().__init__(f"command failed ({returncode}): {' '.join(command)}\n{output}")
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
            "GCP-resident research controller for QQQ fill-squash, full expansion, "
            "aggregate promotion review, and paper-readiness handoff. It cannot trade."
        )
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--gcloud", default=DEFAULT_GCLOUD)
    parser.add_argument("--service-account", default=DEFAULT_SERVICE_ACCOUNT)
    parser.add_argument("--status-prefix", default=STATUS_PREFIX)
    parser.add_argument("--source-archive-uri", default=SOURCE_ARCHIVE_URI)
    parser.add_argument("--micro-prefix", default=MICRO_PREFIX)
    parser.add_argument("--full-prefix", default=FULL_PREFIX)
    parser.add_argument("--micro-summary-threshold", type=int, default=72)
    parser.add_argument("--full-top-n", type=int, default=126)
    parser.add_argument("--chunk-size", type=int, default=7)
    parser.add_argument("--max-launches-per-pass", type=int, default=16)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--sleep-seconds", type=int, default=900)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-both-selectors", action="store_true")
    parser.add_argument("--allow-delete-terminated", action="store_true")
    parser.add_argument(
        "--stale-worker-max-age-minutes",
        type=int,
        default=240,
        help="Restart incomplete QQQ research workers older than this age. Set 0 to disable.",
    )
    parser.add_argument("--sync-source", action="store_true", default=True)
    parser.add_argument("--target-equity", type=float, default=300_000.0)
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
        if allow_no_objects and (
            "matched no objects" in output
            or "One or more URLs matched no objects" in output
            or "No URLs matched" in output
        ):
            return ""
        if check:
            raise CommandError(command, result.returncode, output)
    return output


def gcloud(args: argparse.Namespace, *parts: str) -> list[str]:
    return [args.gcloud, *parts]


def storage_ls(args: argparse.Namespace, uri: str) -> list[str]:
    output = run_command(
        gcloud(args, "storage", "ls", "--recursive", uri, "--project", args.project),
        check=False,
        allow_no_objects=True,
    )
    return [line.strip() for line in output.splitlines() if line.strip().startswith("gs://")]


def storage_cat(args: argparse.Namespace, uri: str) -> str:
    return run_command(gcloud(args, "storage", "cat", uri, "--project", args.project))


def storage_cp(args: argparse.Namespace, source: str | Path, dest: str) -> None:
    if args.dry_run:
        log(f"dry_run storage_cp source={source} dest={dest}")
        return
    run_command(gcloud(args, "storage", "cp", str(source), dest, "--project", args.project))


def storage_cp_recursive(args: argparse.Namespace, source: str | Path, dest: str) -> None:
    if args.dry_run:
        log(f"dry_run storage_cp_recursive source={source} dest={dest}")
        return
    run_command(
        gcloud(args, "storage", "cp", "--recursive", str(source), dest, "--project", args.project),
        timeout=1800,
    )


def gcs_object_exists(args: argparse.Namespace, uri: str) -> bool:
    output = run_command(
        gcloud(args, "storage", "ls", uri, "--project", args.project),
        check=False,
        allow_no_objects=True,
    )
    return any(line.strip().startswith("gs://") for line in output.splitlines())


def count_objects(args: argparse.Namespace, prefix: str, pattern: str) -> int:
    return len(storage_ls(args, f"{prefix.rstrip('/')}/{pattern}"))


def parse_json_or_none(text: str) -> Any | None:
    try:
        return json.loads(text)
    except Exception:
        return None


def run_json(command: list[str]) -> Any:
    output = run_command(command)
    text = output.strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start_candidates = [index for index in (text.find("["), text.find("{")) if index >= 0]
        if start_candidates:
            return json.loads(text[min(start_candidates) :])
        raise


def compute_instances(args: argparse.Namespace) -> list[dict[str, Any]]:
    payload = run_json(
        gcloud(args, "compute", "instances", "list", "--project", args.project, "--format=json")
    )
    return payload if isinstance(payload, list) else []


def instance_zone(instance: dict[str, Any]) -> str:
    zone = str(instance.get("zone") or "")
    return zone.rsplit("/", 1)[-1]


def machine_type_name(instance: dict[str, Any]) -> str:
    machine = str(instance.get("machineType") or "")
    return machine.rsplit("/", 1)[-1]


def machine_cpus(machine_type: str) -> int:
    match = re.search(r"-(?:standard|highmem|highcpu)-(\d+)$", machine_type)
    if match:
        return int(match.group(1))
    if machine_type.endswith("-micro") or machine_type.endswith("-small"):
        return 1
    return 2


def running_cpu(instances: list[dict[str, Any]]) -> int:
    return sum(
        machine_cpus(machine_type_name(instance))
        for instance in instances
        if instance.get("status") == "RUNNING"
    )


def instances_by_name(instances: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    return [instance for instance in instances if str(instance.get("name")) == name]


def active_instance_exists(instances: list[dict[str, Any]]) -> bool:
    return any(str(instance.get("status")) in ACTIVE_STATUSES for instance in instances)


def instance_age_minutes(instance: dict[str, Any]) -> float | None:
    value = str(instance.get("creationTimestamp") or "")
    if not value:
        return None
    try:
        created = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (datetime.now(UTC) - created.astimezone(UTC)).total_seconds() / 60.0


def safe_slug(value: str, *, dash: bool = False) -> str:
    replacement = "-" if dash else "_"
    return re.sub(r"[^A-Za-z0-9]+", replacement, value).strip(replacement).lower()


def chunks(top_n: int, chunk_size: int) -> list[dict[str, int | str]]:
    rows: list[dict[str, int | str]] = []
    for start in range(1, top_n + 1, chunk_size):
        end = min(start + chunk_size - 1, top_n)
        rows.append({"name": f"c{start:03d}-{end:03d}", "start": start, "count": end - start + 1})
    return rows


def worker_complete(
    args: argparse.Namespace,
    prefix: str,
    worker_id: str,
    lag_profile: str,
    selectors: list[str],
) -> bool:
    entry, exit_ = lag_profile.split(":", 1)
    for selector in selectors:
        selector_slug = safe_slug(selector)
        run_id = f"{worker_id}_qqq_e{entry}_x{exit_}_{selector_slug}"
        exact = (
            f"{prefix.rstrip('/')}/workers/{worker_id}/reports/research_wave/"
            f"{run_id}/option_aware_candidate_summary.json"
        )
        wildcard = (
            f"{prefix.rstrip('/')}/workers/{worker_id}/reports/research_wave/"
            f"{run_id}/**/option_aware_candidate_summary.json"
        )
        if not (gcs_object_exists(args, exact) or bool(storage_ls(args, wildcard))):
            return False
    return True


def metadata_arg(metadata: dict[str, str]) -> str:
    return ",".join(f"{key}={value}" for key, value in sorted(metadata.items()))


def create_source_archive(args: argparse.Namespace) -> Path:
    output = LOCAL_STATUS_DIR / "codexalpaca_repo_source.tar.gz"
    output.parent.mkdir(parents=True, exist_ok=True)
    tracked_output = run_command(["git", "ls-files"], check=False)
    if tracked_output.strip():
        rel_paths = [line.strip() for line in tracked_output.splitlines() if line.strip()]
    else:
        rel_paths = []
        excluded = {".git", ".venv", "__pycache__", "reports", "logs"}
        for path in REPO_ROOT.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(REPO_ROOT)
            if any(part in excluded for part in rel.parts):
                continue
            rel_paths.append(str(rel).replace("\\", "/"))
    with tarfile.open(output, "w:gz") as tar:
        for rel in rel_paths:
            path = REPO_ROOT / rel
            if path.exists() and path.is_file():
                tar.add(path, arcname=rel)
    return output


def sync_source_and_inputs(args: argparse.Namespace) -> None:
    if args.sync_source:
        archive = create_source_archive(args)
        storage_cp(args, archive, args.source_archive_uri)
    input_files = {
        "qqq_timing_redesign_variants.jsonl": "qqq_timing_redesign_variants.jsonl",
        "qqq_timing_redesign_option_queue.json": "qqq_timing_redesign_option_queue.json",
        "qqq_timing_redesign_launch_rows.json": "qqq_timing_redesign_launch_rows.json",
    }
    for local_name, remote_name in input_files.items():
        local_path = INPUT_DIR / local_name
        if local_path.exists():
            storage_cp(args, local_path, f"{args.micro_prefix}/inputs/{remote_name}")
            storage_cp(args, local_path, f"{args.full_prefix}/inputs/{remote_name}")
    launch_rows = INPUT_DIR / "qqq_timing_redesign_launch_rows.json"
    if launch_rows.exists():
        storage_cp(
            args,
            launch_rows,
            f"{args.full_prefix}/inputs/ticker_365d_all_available_launch_rows.json",
        )


def delete_terminated_if_complete(
    args: argparse.Namespace,
    name: str,
    instances: list[dict[str, Any]],
    *,
    completed: bool,
) -> None:
    if not args.allow_delete_terminated or not completed:
        return
    for instance in instances:
        if str(instance.get("status")) != "TERMINATED":
            continue
        zone = instance_zone(instance)
        log(f"deleting_terminated_completed_instance name={name} zone={zone}")
        if not args.dry_run:
            output = run_command(
                gcloud(args, "compute", "instances", "delete", name, "--zone", zone, "--quiet"),
                check=False,
                timeout=900,
            )
            if "was not found" in output or "not found" in output.lower():
                log(f"terminated_completed_instance_already_deleted name={name} zone={zone}")
            elif "ERROR:" in output:
                log(
                    "delete_terminated_completed_instance_nonfatal_error "
                    f"name={name} zone={zone} output={output.strip()[:500]}"
                )


def restart_stale_active_instances(
    args: argparse.Namespace,
    name: str,
    instances: list[dict[str, Any]],
) -> bool:
    if args.stale_worker_max_age_minutes <= 0:
        return False
    restarted = False
    for instance in instances:
        status = str(instance.get("status"))
        if status not in ACTIVE_STATUSES:
            continue
        age = instance_age_minutes(instance)
        if age is None or age < args.stale_worker_max_age_minutes:
            continue
        zone = instance_zone(instance)
        log(
            "restarting_stale_research_worker "
            f"name={name} zone={zone} status={status} age_minutes={age:.1f}"
        )
        if not args.dry_run:
            run_command(
                gcloud(args, "compute", "instances", "delete", name, "--zone", zone, "--quiet"),
                timeout=900,
            )
        restarted = True
    return restarted


def launch_instance(
    args: argparse.Namespace,
    *,
    name: str,
    zones: list[str],
    machine_type: str,
    boot_disk_size: str,
    labels: str,
    metadata: dict[str, str],
    startup_script: Path,
) -> bool:
    for zone in zones:
        log(f"launching_instance name={name} zone={zone} machine={machine_type}")
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
            machine_type,
            "--image-family",
            "debian-12",
            "--image-project",
            "debian-cloud",
            "--service-account",
            args.service_account,
            "--scopes",
            "cloud-platform",
            "--boot-disk-size",
            boot_disk_size,
            "--boot-disk-type",
            "pd-standard",
            "--labels",
            labels,
            "--metadata",
            metadata_arg(metadata),
            "--metadata-from-file",
            f"startup-script={startup_script}",
        )
        if args.dry_run:
            log(f"dry_run {' '.join(command)}")
            return True
        output = run_command(command, check=False, timeout=900)
        if "Created" in output or "RUNNING" in output:
            return True
        log(f"instance_launch_failed name={name} zone={zone} output={output.strip()[:500]}")
    return False


def launch_shard(
    args: argparse.Namespace,
    *,
    instances: list[dict[str, Any]],
    prefix: str,
    wave_id: str,
    profile: dict[str, Any],
    chunk: dict[str, int | str],
    selectors: list[str],
    full: bool,
) -> bool:
    chunk_name = str(chunk["name"])
    chunk_worker = chunk_name.replace("-", "_")
    prefix_name = "qqqfull" if full else "qqqfs"
    worker_prefix = "qqqfillfull" if full else "qqqfillsquash"
    suffix = "20260504qx" if full else "20260504qs"
    instance_name = f"{prefix_name}-{profile['name']}-{chunk_name}-{suffix}"
    worker_id = f"{worker_prefix}_qqq_{profile['slug']}_{chunk_worker}"
    completed = worker_complete(args, prefix, worker_id, str(profile["lag_profile"]), selectors)
    existing = instances_by_name(instances, instance_name)
    if completed:
        log(f"shard_complete worker_id={worker_id} instance={instance_name}")
        delete_terminated_if_complete(args, instance_name, existing, completed=True)
        return False
    if active_instance_exists(existing):
        if restart_stale_active_instances(args, instance_name, existing):
            instances = compute_instances(args)
            existing = instances_by_name(instances, instance_name)
            if active_instance_exists(existing):
                log(f"shard_active_after_stale_restart worker_id={worker_id} instance={instance_name}")
                return False
        else:
            log(f"shard_active worker_id={worker_id} instance={instance_name}")
            return False
    if active_instance_exists(existing):
        log(f"shard_active worker_id={worker_id} instance={instance_name}")
        return False
    delete_terminated_if_complete(args, instance_name, existing, completed=True)
    zones = [str(profile["preferred_zone"])] + [
        zone for zone in FALLBACK_ZONES if zone != profile["preferred_zone"]
    ]
    metadata = {
        "symbol": "QQQ",
        "profile_name": str(profile["name"]),
        "worker_id": worker_id,
        "wave_id": wave_id,
        "gcs_prefix": prefix,
        "source_archive_uri": args.source_archive_uri,
        "input_variants_uri": f"{prefix}/inputs/qqq_timing_redesign_variants.jsonl",
        "input_queue_uri": f"{prefix}/inputs/qqq_timing_redesign_option_queue.json",
        "stock_uri": STOCK_URI,
        "contracts_uri": CONTRACTS_URI,
        "bars_uri": BARS_URI,
        "initial_cash": "25000",
        "top_n": "126" if full else "42",
        "candidate_start_index": str(chunk["start"]),
        "candidate_count": str(chunk["count"]),
        "test_date_count": "20",
        "allocation_fraction": "0.05",
        "slippage_bps": "10",
        "fee_per_contract": "0.65",
        "selectors": ";".join(selectors),
        "lag_profiles": str(profile["lag_profile"]),
        "entry_bar_lookup_mode": str(profile["entry_mode"]),
        "max_entry_staleness_minutes": str(profile["entry_staleness"]),
        "exit_bar_lookup_mode": str(profile["exit_mode"]),
        "stock_session_filter": str(profile["session_filter"]),
    }
    labels = (
        "wave=qqq-fill-full,role=qqq-full126,symbol=qqq,dataset=qqq-dense-365d"
        if full
        else "wave=qqq-fill-squash,role=qqq-micro,symbol=qqq,dataset=qqq-dense-365d"
    )
    return launch_instance(
        args,
        name=instance_name,
        zones=zones,
        machine_type="e2-standard-2" if full else "e2-standard-4",
        boot_disk_size="200GB",
        labels=labels,
        metadata=metadata,
        startup_script=STARTUP_SHARD,
    )


def build_heatmap(args: argparse.Namespace) -> Path:
    heatmap_dir = LOCAL_STATUS_DIR / "qqq_fill_squash_micro"
    heatmap_dir.mkdir(parents=True, exist_ok=True)
    run_command(
        [
            os.environ.get("PYTHON", "python"),
            "scripts/build_qqq_fill_squash_heatmap.py",
            "--gcloud",
            args.gcloud,
            "--gcs-prefix",
            args.micro_prefix,
            "--output-dir",
            str(heatmap_dir),
        ],
        timeout=1800,
    )
    storage_cp_recursive(args, heatmap_dir, f"{args.micro_prefix}/heatmap_partial/")
    return heatmap_dir / "qqq_fill_squash_heatmap.csv"


def select_best_strict_profile(args: argparse.Namespace) -> tuple[dict[str, Any], list[str], dict[str, Any]]:
    heatmap_csv = build_heatmap(args)
    rows: list[dict[str, str]] = []
    with heatmap_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    strict_rows = [
        row
        for row in rows
        if row.get("profile", "").startswith("strict_")
        and row.get("entry_lookup_mode") == "first_bar_at_or_after_entry_within_lag"
        and row.get("exit_lookup_mode") == "first_bar_at_or_after_exit_within_lag"
        and row.get("source_session_filter") == "option_rth_same_day"
    ]
    if not strict_rows:
        raise RuntimeError("No legitimate strict micro profile rows found")

    def score(row: dict[str, str]) -> tuple[float, float, float, float, float]:
        return (
            float(row.get("research_gate_passes") or 0),
            float(row.get("fill_gate_passes") or 0),
            float(row.get("mean_fill") or 0),
            float(row.get("max_test_net_pnl") or 0),
            float(row.get("mean_net_pnl") or 0),
        )

    best = sorted(strict_rows, key=score, reverse=True)[0]
    profile = STRICT_PROFILE_MAP[best["profile"]]
    selectors = ALL_SELECTORS if args.run_both_selectors else [best["contract_selection_method"]]
    selection = {
        "generated_at_utc": utc_now(),
        "micro_wave_id": MICRO_WAVE_ID,
        "full_wave_id": FULL_WAVE_ID,
        "selected_profile": profile["name"],
        "selected_profile_slug": profile["slug"],
        "selected_micro_selector": best["contract_selection_method"],
        "selected_expansion_selectors": selectors,
        "run_both_selectors": bool(args.run_both_selectors),
        "expected_summary_count": len(chunks(args.full_top_n, args.chunk_size)) * len(selectors),
        "micro_row": best,
        "hard_rules": HARD_RULES,
    }
    selection_path = LOCAL_STATUS_DIR / "qqq_fill_squash_selected_strict_profile.json"
    selection_path.parent.mkdir(parents=True, exist_ok=True)
    selection_path.write_text(json.dumps(selection, indent=2), encoding="utf-8")
    storage_cp(
        args,
        selection_path,
        f"{args.full_prefix}/selection/qqq_fill_squash_selected_strict_profile.json",
    )
    return profile, selectors, selection


def existing_strict_profile_selection(args: argparse.Namespace) -> dict[str, Any] | None:
    selection_uri = (
        f"{args.full_prefix}/selection/qqq_fill_squash_selected_strict_profile.json"
    )
    if not gcs_object_exists(args, selection_uri):
        return None
    selection = parse_json_or_none(storage_cat(args, selection_uri))
    return selection if isinstance(selection, dict) else None


def launch_micro_phase(args: argparse.Namespace, instances: list[dict[str, Any]]) -> list[str]:
    launched: list[str] = []
    for chunk in chunks(42, args.chunk_size):
        for profile in MICRO_PROFILES:
            if len(launched) >= args.max_launches_per_pass:
                return launched
            if running_cpu(instances) + 4 > 32:
                log("micro_launch_paused quota_free_cpu_low")
                return launched
            did_launch = launch_shard(
                args,
                instances=instances,
                prefix=args.micro_prefix,
                wave_id=MICRO_WAVE_ID,
                profile=profile,
                chunk=chunk,
                selectors=ALL_SELECTORS,
                full=False,
            )
            if did_launch:
                launched.append(f"{profile['name']}:{chunk['name']}")
                instances = compute_instances(args)
    return launched


def launch_full_phase(
    args: argparse.Namespace,
    instances: list[dict[str, Any]],
    profile: dict[str, Any],
    selectors: list[str],
) -> list[str]:
    launched: list[str] = []
    for chunk in chunks(args.full_top_n, args.chunk_size):
        if len(launched) >= args.max_launches_per_pass:
            return launched
        if running_cpu(instances) + 2 > 32:
            log("full_launch_paused quota_free_cpu_low")
            return launched
        did_launch = launch_shard(
            args,
            instances=instances,
            prefix=args.full_prefix,
            wave_id=FULL_WAVE_ID,
            profile=profile,
            chunk=chunk,
            selectors=selectors,
            full=True,
        )
        if did_launch:
            launched.append(f"{profile['name']}:{chunk['name']}")
            instances = compute_instances(args)
    return launched


def aggregate_phase(args: argparse.Namespace) -> dict[str, Any]:
    status_uri = f"{args.full_prefix}/{AGGREGATE_STATUS_PATH}"
    packet_uri = f"{args.full_prefix}/{AGGREGATE_PROMOTION_PACKET_PATHS[0]}"
    status = None
    packet = None
    if gcs_object_exists(args, status_uri):
        status = parse_json_or_none(storage_cat(args, status_uri))
    for candidate_path in AGGREGATE_PROMOTION_PACKET_PATHS:
        candidate_uri = f"{args.full_prefix}/{candidate_path}"
        if gcs_object_exists(args, candidate_uri):
            packet_uri = candidate_uri
            packet = parse_json_or_none(storage_cat(args, candidate_uri))
            break
    eligible = None
    decision = None
    if isinstance(packet, dict):
        decision = packet.get("decision")
        eligible = (packet.get("gate_summary") or {}).get("eligible_for_promotion_review_count")
    return {
        "status_uri": status_uri,
        "packet_uri": packet_uri,
        "status": status,
        "packet_found": isinstance(packet, dict),
        "decision": decision,
        "eligible_for_promotion_review_count": eligible,
    }


def launch_aggregate_if_needed(
    args: argparse.Namespace,
    instances: list[dict[str, Any]],
    expected_summary_count: int,
) -> bool:
    aggregate = aggregate_phase(args)
    if isinstance(aggregate.get("status"), dict) and aggregate["status"].get("phase") == "aggregate_completed":
        log("aggregate_already_complete")
        return False
    name = "qqqfull-aggregate-20260504qx"
    existing = instances_by_name(instances, name)
    if active_instance_exists(existing):
        log("aggregate_instance_active")
        return False
    metadata = {
        "wave_id": FULL_WAVE_ID,
        "gcs_prefix": args.full_prefix,
        "source_archive_uri": args.source_archive_uri,
        "expected_summary_count": str(expected_summary_count),
        "check_interval_seconds": "300",
        "max_wait_seconds": "43200",
        "initial_cash": "25000",
        "target_equity": str(args.target_equity),
    }
    return launch_instance(
        args,
        name=name,
        zones=FALLBACK_ZONES,
        machine_type="e2-standard-2",
        boot_disk_size="80GB",
        labels="wave=qqq-fill-full,role=qqq-aggregate,symbol=qqq,dataset=qqq-dense-365d",
        metadata=metadata,
        startup_script=STARTUP_AGGREGATE,
    )


def write_status(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    LOCAL_STATUS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": utc_now(),
        "controller": "gcp_autonomous_paper_readiness_controller",
        "broker_facing": False,
        "paper_orders": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "hard_rules": HARD_RULES,
        **payload,
    }
    json_path = LOCAL_STATUS_DIR / "autonomous_paper_readiness_status.json"
    md_path = LOCAL_STATUS_DIR / "autonomous_paper_readiness_status.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    lines = [
        "# Autonomous Paper Readiness Status",
        "",
        f"- Generated: `{payload['generated_at_utc']}`",
        f"- Phase: `{payload.get('phase')}`",
        f"- Next action: `{payload.get('next_action')}`",
        f"- Micro summaries: `{payload.get('micro_summary_count')}/{payload.get('micro_summary_threshold')}`",
        f"- Full summaries: `{payload.get('full_summary_count')}/{payload.get('full_expected_summary_count')}`",
        f"- Aggregate phase: `{payload.get('aggregate_phase')}`",
        f"- Eligible promotion-review count: `{payload.get('eligible_for_promotion_review_count')}`",
        f"- Paper orders: `{payload['paper_orders']}`",
        "",
        "## Recent Actions",
    ]
    for action in payload.get("actions", []):
        lines.append(f"- {action}")
    lines.extend(["", "## Hard Rules"])
    for rule in HARD_RULES:
        lines.append(f"- {rule}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    storage_cp(args, json_path, f"{args.status_prefix}/autonomous_paper_readiness_status.json")
    storage_cp(args, md_path, f"{args.status_prefix}/autonomous_paper_readiness_status.md")


def controller_pass(args: argparse.Namespace) -> dict[str, Any]:
    sync_source_and_inputs(args)
    instances = compute_instances(args)
    micro_count = count_objects(args, args.micro_prefix, "**/option_aware_candidate_summary.json")
    progress_count = count_objects(args, args.micro_prefix, "**/candidate_summary_progress.jsonl")
    full_count = count_objects(args, args.full_prefix, "**/option_aware_candidate_summary.json")
    aggregate = aggregate_phase(args)
    aggregate_status = aggregate.get("status") if isinstance(aggregate.get("status"), dict) else {}
    aggregate_phase_name = aggregate_status.get("phase") if isinstance(aggregate_status, dict) else None
    eligible = aggregate.get("eligible_for_promotion_review_count")
    profile: dict[str, Any] | None = None
    selectors: list[str] = []
    selection: dict[str, Any] | None = None
    actions: list[str] = []
    phase = "micro_fill_squash"
    next_action = "continue_micro_wave"
    full_expected = 0

    if aggregate_phase_name == "aggregate_completed":
        selection = existing_strict_profile_selection(args)
        full_expected = int(
            aggregate_status.get("expected_summary_count")
            or (selection or {}).get("expected_summary_count")
            or 0
        )
        selectors = list((selection or {}).get("selected_expansion_selectors") or [])
        phase = (
            "research_promotion_candidates_available"
            if eligible and int(eligible) > 0
            else "research_blocked_or_redesign_needed"
        )
        next_action = (
            "run_independent_reproduction_then_runner_preflight"
            if eligible and int(eligible) > 0
            else "design_next_research_wave_do_not_arm_runner"
        )
        status = {
            "phase": phase,
            "next_action": next_action,
            "actions": ["aggregate_complete_no_vm_changes_needed"],
            "micro_wave_id": MICRO_WAVE_ID,
            "micro_prefix": args.micro_prefix,
            "micro_summary_count": micro_count,
            "micro_progress_file_count": progress_count,
            "micro_summary_threshold": args.micro_summary_threshold,
            "full_wave_id": FULL_WAVE_ID,
            "full_prefix": args.full_prefix,
            "full_summary_count": full_count,
            "full_expected_summary_count": full_expected,
            "selected_profile": (selection or {}).get("selected_profile"),
            "selected_selectors": selectors,
            "selection": selection,
            "aggregate_phase": aggregate_phase_name,
            "aggregate_packet_found": aggregate.get("packet_found"),
            "aggregate_packet_uri": aggregate.get("packet_uri"),
            "aggregate_decision": aggregate.get("decision"),
            "eligible_for_promotion_review_count": eligible,
            "running_cpu_estimate": running_cpu(instances),
            "active_qqq_instances": [
                {
                    "name": instance.get("name"),
                    "zone": instance_zone(instance),
                    "status": instance.get("status"),
                    "machine_type": machine_type_name(instance),
                }
                for instance in instances
                if str(instance.get("name", "")).startswith(("qqqfs-", "qqqfull-"))
                and str(instance.get("status")) in ACTIVE_STATUSES
            ],
        }
        write_status(args, status)
        return status

    if micro_count < args.micro_summary_threshold:
        launched = launch_micro_phase(args, instances)
        actions.extend(f"launched_micro_shard {item}" for item in launched)
    else:
        profile, selectors, selection = select_best_strict_profile(args)
        full_expected = int(selection["expected_summary_count"])
        if full_count < full_expected:
            phase = "full_qqq_expansion"
            next_action = "continue_full_expansion"
            launched = launch_full_phase(args, instances, profile, selectors)
            actions.extend(f"launched_full_shard {item}" for item in launched)
        else:
            phase = "aggregate_promotion_review"
            next_action = "launch_or_wait_for_aggregate"
            if launch_aggregate_if_needed(args, compute_instances(args), full_expected):
                actions.append("launched_full_aggregate")

    aggregate = aggregate_phase(args)
    aggregate_status = aggregate.get("status") if isinstance(aggregate.get("status"), dict) else {}
    aggregate_phase_name = aggregate_status.get("phase") if isinstance(aggregate_status, dict) else None
    eligible = aggregate.get("eligible_for_promotion_review_count")
    if aggregate_phase_name == "aggregate_completed":
        if eligible and int(eligible) > 0:
            phase = "research_promotion_candidates_available"
            next_action = "run_independent_reproduction_then_runner_preflight"
        else:
            phase = "research_blocked_or_redesign_needed"
            next_action = "design_next_research_wave_do_not_arm_runner"

    status = {
        "phase": phase,
        "next_action": next_action,
        "actions": actions or ["no_vm_changes_needed"],
        "micro_wave_id": MICRO_WAVE_ID,
        "micro_prefix": args.micro_prefix,
        "micro_summary_count": micro_count,
        "micro_progress_file_count": progress_count,
        "micro_summary_threshold": args.micro_summary_threshold,
        "full_wave_id": FULL_WAVE_ID,
        "full_prefix": args.full_prefix,
        "full_summary_count": full_count,
        "full_expected_summary_count": full_expected,
        "selected_profile": profile.get("name") if profile else None,
        "selected_selectors": selectors,
        "selection": selection,
        "aggregate_phase": aggregate_phase_name,
        "aggregate_packet_found": aggregate.get("packet_found"),
        "aggregate_packet_uri": aggregate.get("packet_uri"),
        "aggregate_decision": aggregate.get("decision"),
        "eligible_for_promotion_review_count": eligible,
        "running_cpu_estimate": running_cpu(instances),
        "active_qqq_instances": [
            {
                "name": instance.get("name"),
                "zone": instance_zone(instance),
                "status": instance.get("status"),
                "machine_type": machine_type_name(instance),
            }
            for instance in instances
            if str(instance.get("name", "")).startswith(("qqqfs-", "qqqfull-"))
            and str(instance.get("status")) in ACTIVE_STATUSES
        ],
    }
    write_status(args, status)
    return status


def main() -> None:
    args = parse_args()
    while True:
        try:
            status = controller_pass(args)
            log(
                "controller_pass_complete "
                f"phase={status['phase']} next={status['next_action']} "
                f"micro={status['micro_summary_count']}/{status['micro_summary_threshold']} "
                f"full={status['full_summary_count']}/{status['full_expected_summary_count']}"
            )
        except Exception as exc:
            error_status = {
                "phase": "controller_error",
                "next_action": "retry_after_sleep_or_inspect_logs",
                "actions": [f"controller_error {type(exc).__name__}: {exc}"],
            }
            try:
                write_status(args, error_status)
            finally:
                log(f"controller_error {type(exc).__name__}: {exc}")
            if not args.loop:
                raise
        if not args.loop:
            break
        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()

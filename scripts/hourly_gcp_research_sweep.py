from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "reports" / "gcp_research" / "_automation" / "hourly-gcp-research-sweep"
DEFAULT_PROJECT = "codexalpaca"
DEFAULT_GCS_RESULTS_ROOT = "gs://codexalpaca-control-us/research_results"
PROMOTION_PACKET_FILENAME = "research_promotion_review_packet.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep GCP research waves for TERMINATED worker VMs, sync their artifacts from GCS, "
            "parse promotion packets, classify candidates, and delete only synced TERMINATED instances."
        )
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--gcloud-bin", default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--gcs-results-root", default=DEFAULT_GCS_RESULTS_ROOT)
    parser.add_argument(
        "--delete-synced-terminated",
        action="store_true",
        help="Actually delete TERMINATED instances after a successful artifact sync.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No-op mode: do not rsync, do not delete; only report what would happen.",
    )
    parser.add_argument(
        "--include-name-prefix",
        action="append",
        default=[],
        help="Optional instance name prefix filter; can be passed multiple times.",
    )
    return parser.parse_args()


def _now_run_id() -> str:
    # Match prior automation convention: local time + offset.
    return datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")


def _now_utc_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _gcloud_path(explicit: str | None) -> str:
    if explicit:
        return explicit
    found = shutil.which("gcloud") or shutil.which("gcloud.cmd")
    if not found:
        raise SystemExit("gcloud not found; pass --gcloud-bin")
    return found


def _metadata_map(instance: dict[str, Any]) -> dict[str, str]:
    items = instance.get("metadata", {}).get("items", [])
    mapped: dict[str, str] = {}
    if not isinstance(items, list):
        return mapped
    for item in items:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if not key:
            continue
        mapped[key] = str(item.get("value") or "")
    return mapped


def _is_research_gcs_prefix(prefix: str, results_root: str) -> bool:
    prefix = prefix.strip()
    results_root = results_root.rstrip("/") + "/"
    return prefix.startswith(results_root)


def _parse_wave_id_from_gcs_prefix(prefix: str, results_root: str) -> str | None:
    results_root = results_root.rstrip("/") + "/"
    if not prefix.startswith(results_root):
        return None
    rest = prefix[len(results_root) :].strip("/")
    if not rest:
        return None
    return rest.split("/", 1)[0]


@dataclass(frozen=True)
class WorkerInstance:
    name: str
    zone: str
    status: str
    wave_id: str
    worker_id: str
    gcs_prefix: str

    @property
    def worker_gcs_uri(self) -> str:
        return f"{self.gcs_prefix.rstrip('/')}/workers/{self.worker_id}"


def extract_worker_instance(
    instance: dict[str, Any], *, results_root: str
) -> WorkerInstance | None:
    name = str(instance.get("name") or "")
    zone = str(instance.get("zone") or "").rsplit("/", 1)[-1]
    status = str(instance.get("status") or "")
    if not name or not zone or not status:
        return None

    metadata = _metadata_map(instance)
    gcs_prefix = str(metadata.get("gcs_prefix") or "")
    worker_id = str(metadata.get("worker_id") or "")
    wave_id = str(metadata.get("wave_id") or "")

    if not gcs_prefix or not worker_id:
        return None
    if not _is_research_gcs_prefix(gcs_prefix, results_root):
        return None
    if not wave_id:
        wave_id = _parse_wave_id_from_gcs_prefix(gcs_prefix, results_root) or ""
    if not wave_id:
        return None
    return WorkerInstance(
        name=name,
        zone=zone,
        status=status,
        wave_id=wave_id,
        worker_id=worker_id,
        gcs_prefix=gcs_prefix,
    )


def _load_instances(gcloud_bin: str, project: str) -> list[dict[str, Any]]:
    result = _run(
        [
            gcloud_bin,
            "compute",
            "instances",
            "list",
            "--project",
            project,
            "--format=json",
        ]
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr or result.stdout)
    summaries = json.loads(result.stdout or "[]")
    if not isinstance(summaries, list):
        return []

    described: list[dict[str, Any]] = []
    for summary in summaries:
        if not isinstance(summary, dict):
            continue
        name = str(summary.get("name") or "")
        zone_url = str(summary.get("zone") or "")
        zone = zone_url.rsplit("/", 1)[-1]
        if not name or not zone:
            continue
        detail = _run(
            [
                gcloud_bin,
                "compute",
                "instances",
                "describe",
                name,
                "--zone",
                zone,
                "--project",
                project,
                "--format=json",
            ]
        )
        if detail.returncode != 0:
            # Keep a minimal record so the sweep still reports something useful.
            described.append(
                {
                    "name": name,
                    "zone": zone,
                    "status": str(summary.get("status") or "unknown"),
                    "describe_error": detail.stderr or detail.stdout,
                }
            )
            continue
        described.append(json.loads(detail.stdout))
    return described


def _iter_promotion_packets(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return root.rglob(PROMOTION_PACKET_FILENAME)


@dataclass(frozen=True)
class PromotionPacketSummary:
    path: str
    decision: str
    candidate_level_decision: str
    promotion_scope: str
    eligible_for_promotion_review_count: int
    unique_eligible_base_candidate_count: int
    eligible_candidates: int
    blocked_candidates: int


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def summarize_promotion_packet(payload: dict[str, Any], *, path: Path) -> PromotionPacketSummary:
    decision = str(payload.get("decision") or "unknown")
    candidate_level_decision = str(payload.get("candidate_level_decision") or "unknown")
    promotion_scope = str(payload.get("promotion_scope") or "unknown")
    gate_summary = payload.get("gate_summary") if isinstance(payload.get("gate_summary"), dict) else {}
    eligible_for_review = _int(gate_summary.get("eligible_for_promotion_review_count"))
    unique_eligible = _int(gate_summary.get("unique_eligible_base_candidate_count"))

    eligible_candidates = 0
    blocked_candidates = 0
    review_candidates = payload.get("review_candidates", [])
    if isinstance(review_candidates, list):
        for candidate in review_candidates:
            if not isinstance(candidate, dict):
                continue
            status = str(candidate.get("promotion_status") or "unknown")
            if status == "eligible_for_promotion_review":
                eligible_candidates += 1
            else:
                blocked_candidates += 1
    return PromotionPacketSummary(
        path=str(path),
        decision=decision,
        candidate_level_decision=candidate_level_decision,
        promotion_scope=promotion_scope,
        eligible_for_promotion_review_count=eligible_for_review,
        unique_eligible_base_candidate_count=unique_eligible,
        eligible_candidates=eligible_candidates,
        blocked_candidates=blocked_candidates,
    )


def _rsync_worker(
    gcloud_bin: str,
    *,
    project: str,
    src_uri: str,
    dest_dir: Path,
    dry_run: bool,
) -> dict[str, Any]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    if dry_run:
        return {"status": "dry_run", "src_uri": src_uri, "dest_dir": str(dest_dir), "file_count": 0}

    result = _run(
        [
            gcloud_bin,
            "storage",
            "rsync",
            "-r",
            src_uri,
            str(dest_dir),
            "--project",
            project,
        ]
    )
    # rsync returns non-zero for missing source; treat that as "empty" rather than error.
    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    if result.returncode != 0 and ("No URLs matched" in stderr or "No URLs matched" in stdout):
        return {
            "status": "empty",
            "src_uri": src_uri,
            "dest_dir": str(dest_dir),
            "exit_code": result.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "file_count": 0,
        }
    if result.returncode != 0:
        return {
            "status": "error",
            "src_uri": src_uri,
            "dest_dir": str(dest_dir),
            "exit_code": result.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "file_count": 0,
        }
    file_count = sum(1 for path in dest_dir.rglob("*") if path.is_file())
    return {
        "status": "synced",
        "src_uri": src_uri,
        "dest_dir": str(dest_dir),
        "exit_code": result.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "file_count": file_count,
    }


def _delete_instance(
    gcloud_bin: str,
    *,
    project: str,
    name: str,
    zone: str,
    dry_run: bool,
) -> dict[str, Any]:
    if dry_run:
        return {"name": name, "zone": zone, "status": "dry_run"}
    result = _run(
        [
            gcloud_bin,
            "compute",
            "instances",
            "delete",
            name,
            "--zone",
            zone,
            "--project",
            project,
            "--quiet",
        ]
    )
    return {
        "name": name,
        "zone": zone,
        "status": "deleted" if result.returncode == 0 else "error",
        "exit_code": result.returncode,
        "stdout": (result.stdout or "").strip(),
        "stderr": (result.stderr or "").strip(),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _build_status_note(packet: dict[str, Any]) -> str:
    generated_at = packet.get("generated_at_utc", "unknown")
    project = packet.get("project", "unknown")
    run_id = packet.get("run_id", "unknown")
    dry_run = str(bool(packet.get("dry_run"))).lower()
    delete_synced_terminated = str(bool(packet.get("delete_synced_terminated"))).lower()

    instance_scan = packet.get("instance_scan") if isinstance(packet.get("instance_scan"), dict) else {}
    instance_count = _int(instance_scan.get("instance_count"))
    terminated_count = _int(instance_scan.get("terminated_research_worker_count"))
    running_count = _int(instance_scan.get("running_research_worker_count"))

    sync_summary = packet.get("sync_summary") if isinstance(packet.get("sync_summary"), dict) else {}
    sync_jobs = _int(sync_summary.get("jobs"))
    sync_synced = _int(sync_summary.get("synced"))
    sync_empty = _int(sync_summary.get("empty"))
    sync_errors = _int(sync_summary.get("errors"))

    promotion_summary = (
        packet.get("promotion_summary") if isinstance(packet.get("promotion_summary"), dict) else {}
    )
    packets_scanned = _int(promotion_summary.get("packets_scanned"))
    packets_with_eligible = _int(promotion_summary.get("packets_with_eligible_count_gt_0"))
    eligible_candidates = _int(promotion_summary.get("eligible_review_candidates"))
    blocked_candidates = _int(promotion_summary.get("blocked_review_candidates"))

    cleanup_summary = packet.get("cleanup_summary") if isinstance(packet.get("cleanup_summary"), dict) else {}
    deleted = _int(cleanup_summary.get("deleted"))
    skipped = _int(cleanup_summary.get("skipped"))
    errors = _int(cleanup_summary.get("errors"))

    lines = [
        "# Hourly GCP Research Sweep",
        "",
        f"- Generated UTC: `{generated_at}`",
        f"- Project: `{project}`",
        f"- Run id: `{run_id}`",
        f"- Dry run: `{dry_run}`",
        f"- Delete synced terminated: `{delete_synced_terminated}`",
        "",
        "## Instance Scan",
        "",
        f"- Total instances described: `{instance_count}`",
        f"- Research worker TERMINATED: `{terminated_count}`",
        f"- Research worker RUNNING: `{running_count}`",
        "",
        "## Artifact Sync",
        "",
        f"- Jobs: `{sync_jobs}` (synced `{sync_synced}`, empty `{sync_empty}`, errors `{sync_errors}`)",
        "",
        "## Promotion Packets",
        "",
        f"- Packets scanned: `{packets_scanned}`",
        f"- Packets w/ eligible_for_promotion_review_count > 0: `{packets_with_eligible}`",
        f"- Eligible review candidates (rows): `{eligible_candidates}`",
        f"- Blocked candidates (rows): `{blocked_candidates}`",
        "",
        "## Cleanup",
        "",
        f"- Deleted instances: `{deleted}`",
        f"- Skipped (not synced / dry-run / no delete flag): `{skipped}`",
        f"- Errors: `{errors}`",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    gcloud_bin = _gcloud_path(args.gcloud_bin)

    run_id = _now_run_id()
    output_dir = args.output_root / run_id
    gcs_sync_dir = output_dir / "gcs_sync"

    instances = _load_instances(gcloud_bin, args.project)

    include_prefixes = [prefix for prefix in args.include_name_prefix if prefix]
    terminated_workers: list[WorkerInstance] = []
    running_workers: list[WorkerInstance] = []
    for instance in instances:
        worker = extract_worker_instance(instance, results_root=args.gcs_results_root)
        if worker is None:
            continue
        if include_prefixes and not any(worker.name.startswith(prefix) for prefix in include_prefixes):
            continue
        if worker.status == "TERMINATED":
            terminated_workers.append(worker)
        elif worker.status == "RUNNING":
            running_workers.append(worker)

    instance_scan = {
        "instance_count": len(instances),
        "terminated_research_worker_count": len(terminated_workers),
        "running_research_worker_count": len(running_workers),
        "terminated_research_workers": [
            {
                "name": w.name,
                "zone": w.zone,
                "status": w.status,
                "wave_id": w.wave_id,
                "worker_id": w.worker_id,
                "gcs_prefix": w.gcs_prefix,
            }
            for w in sorted(terminated_workers, key=lambda w: w.name)
        ],
        "running_research_workers": [
            {
                "name": w.name,
                "zone": w.zone,
                "status": w.status,
                "wave_id": w.wave_id,
                "worker_id": w.worker_id,
                "gcs_prefix": w.gcs_prefix,
            }
            for w in sorted(running_workers, key=lambda w: w.name)
        ],
    }

    sync_results: list[dict[str, Any]] = []
    for worker in sorted(terminated_workers, key=lambda w: (w.wave_id, w.worker_id)):
        local_dir = gcs_sync_dir / worker.wave_id / "workers" / worker.worker_id
        sync_result = _rsync_worker(
            gcloud_bin,
            project=args.project,
            src_uri=worker.worker_gcs_uri,
            dest_dir=local_dir,
            dry_run=args.dry_run,
        )
        sync_results.append(
            {
                "wave_id": worker.wave_id,
                "worker_id": worker.worker_id,
                "instance_name": worker.name,
                "instance_zone": worker.zone,
                "uri": worker.worker_gcs_uri,
                "local_dir": str(local_dir),
                **sync_result,
            }
        )

    sync_summary = {
        "run_id": run_id,
        "jobs": len(sync_results),
        "synced": sum(1 for row in sync_results if row.get("status") == "synced"),
        "empty": sum(1 for row in sync_results if row.get("status") == "empty"),
        "errors": sum(1 for row in sync_results if row.get("status") == "error"),
        "output_dir": str(output_dir),
    }

    promotion_packets: list[PromotionPacketSummary] = []
    for row in sync_results:
        if row.get("status") not in {"synced", "dry_run"}:
            continue
        local_dir = Path(str(row.get("dest_dir") or row.get("local_dir") or ""))
        for packet_path in _iter_promotion_packets(local_dir):
            try:
                payload = json.loads(packet_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            promotion_packets.append(summarize_promotion_packet(payload, path=packet_path))

    promotion_summary = {
        "packets_scanned": len(promotion_packets),
        "packets_with_eligible_count_gt_0": sum(
            1 for pkt in promotion_packets if pkt.eligible_for_promotion_review_count > 0
        ),
        "eligible_review_candidates": sum(pkt.eligible_candidates for pkt in promotion_packets),
        "blocked_review_candidates": sum(pkt.blocked_candidates for pkt in promotion_packets),
        "packets": [pkt.__dict__ for pkt in sorted(promotion_packets, key=lambda p: p.path)],
    }

    deletions: list[dict[str, Any]] = []
    skipped = 0
    errors = 0
    deleted = 0
    if args.delete_synced_terminated:
        synced_by_instance = {
            str(row.get("instance_name") or ""): row for row in sync_results if row.get("status") in {"synced"}
        }
        for worker in sorted(terminated_workers, key=lambda w: w.name):
            sync_row = synced_by_instance.get(worker.name)
            if sync_row is None:
                skipped += 1
                continue
            delete_result = _delete_instance(
                gcloud_bin,
                project=args.project,
                name=worker.name,
                zone=worker.zone,
                dry_run=args.dry_run,
            )
            deletions.append(delete_result)
            if delete_result.get("status") == "deleted":
                deleted += 1
            elif delete_result.get("status") == "dry_run":
                skipped += 1
            else:
                errors += 1
    else:
        skipped = len(terminated_workers)

    cleanup_summary = {"deleted": deleted, "skipped": skipped, "errors": errors}

    packet = {
        "generated_at_utc": _now_utc_iso(),
        "run_id": run_id,
        "project": args.project,
        "dry_run": bool(args.dry_run),
        "delete_synced_terminated": bool(args.delete_synced_terminated),
        "output_dir": str(output_dir),
        "instance_scan": instance_scan,
        "sync_summary": sync_summary,
        "promotion_summary": promotion_summary,
        "cleanup_summary": cleanup_summary,
    }

    _write_json(output_dir / "instance_scan.json", instance_scan)
    _write_json(output_dir / "sync_results.json", sync_results)
    _write_json(output_dir / "sync_summary.json", sync_summary)
    _write_json(output_dir / "promotion_classification.json", promotion_summary)
    _write_json(output_dir / "deletion_results.json", deletions)
    _write_json(output_dir / "sweep_summary.json", cleanup_summary | {"run_id": run_id, "output_dir": str(output_dir)})
    _write_text(output_dir / "status_note.md", _build_status_note(packet))
    _write_json(output_dir / "status_note.json", packet)

    print(json.dumps(packet, indent=2))


if __name__ == "__main__":
    # Hard guardrail: never treat env flags as live trading signals in this repo.
    hard_errors = {
        "LIVE_TRADING": "true",
        "ALPACA_PAPER_TRADE": "false",
        "ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE": "true",
    }
    for key, forbidden in hard_errors.items():
        if str(os.environ.get(key, "")).lower() == forbidden:
            raise SystemExit(f"Hard error: {key}={forbidden} is not allowed in this repo")
    main()

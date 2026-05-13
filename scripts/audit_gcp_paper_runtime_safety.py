from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_DIR = Path("reports/gcp_research/gcp_paper_runtime_safety_20260505")
DEFAULT_GCS_PREFIX = "gs://codexalpaca-control-us/research_results/gcp_paper_runtime_safety_20260505"
UNSAFE_TOKENS = (
    "--submit-paper-orders",
    "submit_paper_orders: true",
    "submit_paper_orders=true",
    "DRY_RUN=false",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit GCP paper/runtime VMs for broker-facing startup risk."
    )
    parser.add_argument("--project", default="codexalpaca")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX)
    parser.add_argument("--gcloud-bin", default=None)
    parser.add_argument("--skip-upload", action="store_true")
    return parser.parse_args()


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _gcloud_path(explicit: str | None) -> str:
    if explicit:
        return explicit
    found = shutil.which("gcloud")
    if not found:
        raise SystemExit("gcloud not found; pass --gcloud-bin")
    return found


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
    instances: list[dict[str, Any]] = []
    for summary in summaries:
        name = str(summary.get("name", ""))
        zone_url = str(summary.get("zone", ""))
        zone = zone_url.rsplit("/", 1)[-1]
        if not name or not zone:
            continue
        described = _run(
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
        if described.returncode != 0:
            instances.append(
                {
                    "name": name,
                    "zone": zone,
                    "describe_error": described.stderr or described.stdout,
                }
            )
            continue
        instances.append(json.loads(described.stdout))
    return instances


def _metadata_text(instance: dict[str, Any]) -> str:
    metadata_items = instance.get("metadata", {}).get("items", [])
    chunks: list[str] = []
    for item in metadata_items:
        key = str(item.get("key", ""))
        value = str(item.get("value", ""))
        chunks.append(f"{key}\n{value}")
    return "\n".join(chunks)


def _instance_record(instance: dict[str, Any]) -> dict[str, Any]:
    name = str(instance.get("name", "unknown"))
    zone = str(instance.get("zone", "")).rsplit("/", 1)[-1]
    status = str(instance.get("status", "unknown"))
    labels = instance.get("labels", {})
    service_accounts = [
        account.get("email")
        for account in instance.get("serviceAccounts", [])
        if account.get("email")
    ]
    metadata = _metadata_text(instance)
    unsafe_hits = [token for token in UNSAFE_TOKENS if token in metadata]
    known_validation_only = (
        "validation-only" in json.dumps(labels).lower()
        or "validation_only" in metadata.lower()
        or "no-submit-paper-orders" in metadata
    )
    if unsafe_hits and status == "RUNNING":
        recommendation = "stop_immediately_and_review"
    elif unsafe_hits:
        recommendation = "do_not_start_without_rewriting_startup_metadata"
    elif status == "RUNNING" and not known_validation_only:
        recommendation = "review_running_instance"
    else:
        recommendation = "ok_monitor_only"
    return {
        "name": name,
        "zone": zone,
        "status": status,
        "machine_type": str(instance.get("machineType", "")).rsplit("/", 1)[-1],
        "labels": labels,
        "service_accounts": service_accounts,
        "unsafe_hits": unsafe_hits,
        "known_validation_only": known_validation_only,
        "recommendation": recommendation,
    }


def _build_markdown(packet: dict[str, Any]) -> str:
    lines = [
        "# GCP Paper Runtime Safety Audit",
        "",
        f"- Generated UTC: `{packet['generated_at_utc']}`",
        f"- Project: `{packet['project']}`",
        f"- Running instances: `{packet['running_instance_count']}`",
        f"- Unsafe metadata instances: `{packet['unsafe_metadata_instance_count']}`",
        "",
        "## Summary",
        "",
    ]
    for record in packet["instances"]:
        unsafe = ", ".join(record["unsafe_hits"]) if record["unsafe_hits"] else "none"
        lines.extend(
            [
                f"### {record['name']}",
                "",
                f"- Zone: `{record['zone']}`",
                f"- Status: `{record['status']}`",
                f"- Machine type: `{record['machine_type']}`",
                f"- Unsafe hits: `{unsafe}`",
                f"- Known validation-only: `{str(record['known_validation_only']).lower()}`",
                f"- Recommendation: `{record['recommendation']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Hard Rules",
            "",
            "- Do not start any VM whose startup metadata contains `--submit-paper-orders` unless an operator explicitly approves broker-facing paper execution.",
            "- Prefer validation-only/no-order preflight paths before any broker-facing launch.",
            "- Keep this audit mirrored to GCS so another machine can reproduce the runtime decision.",
            "",
        ]
    )
    return "\n".join(lines)


def _upload(gcloud_bin: str, local_path: Path, gcs_uri: str) -> dict[str, Any]:
    result = _run([gcloud_bin, "storage", "cp", str(local_path), gcs_uri])
    return {
        "local_path": str(local_path),
        "gcs_uri": gcs_uri,
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def main() -> None:
    args = parse_args()
    gcloud_bin = _gcloud_path(args.gcloud_bin)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    instances = [_instance_record(instance) for instance in _load_instances(gcloud_bin, args.project)]
    packet = {
        "generated_at_utc": _now_iso(),
        "project": args.project,
        "instance_count": len(instances),
        "running_instance_count": sum(1 for item in instances if item["status"] == "RUNNING"),
        "unsafe_metadata_instance_count": sum(1 for item in instances if item["unsafe_hits"]),
        "instances": sorted(instances, key=lambda item: item["name"]),
    }
    json_path = args.output_dir / "gcp_paper_runtime_safety_audit.json"
    md_path = args.output_dir / "gcp_paper_runtime_safety_audit.md"
    json_path.write_text(json.dumps(packet, indent=2), encoding="utf-8")
    md_path.write_text(_build_markdown(packet), encoding="utf-8")
    uploads: list[dict[str, Any]] = []
    if not args.skip_upload:
        uploads.append(_upload(gcloud_bin, json_path, f"{args.gcs_prefix}/gcp_paper_runtime_safety_audit.json"))
        uploads.append(_upload(gcloud_bin, md_path, f"{args.gcs_prefix}/gcp_paper_runtime_safety_audit.md"))
    print(json.dumps({**packet, "outputs": {"json": str(json_path), "markdown": str(md_path)}, "uploads": uploads}, indent=2))


if __name__ == "__main__":
    main()

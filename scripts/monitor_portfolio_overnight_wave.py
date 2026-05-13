from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

DEFAULT_WAVE_ID = "portfolio_overnight_12h_20260501"
DEFAULT_GCS_PREFIX = "gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/"
DEFAULT_FASTLANE_AGGREGATE_SUBDIR = "aggregate_fastlane_top40_20260501"


def _run(args: list[str], *, timeout: int = 120) -> dict[str, Any]:
    started = datetime.now(UTC)
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except Exception as exc:  # pragma: no cover - defensive monitor path
        return {
            "args": args,
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
            "started_utc": started.isoformat(),
            "finished_utc": datetime.now(UTC).isoformat(),
        }
    return {
        "args": args,
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
    }


def _json_stdout(command: dict[str, Any]) -> Any:
    if not command["ok"]:
        return None
    try:
        return json.loads(command["stdout"] or "null")
    except json.JSONDecodeError:
        return None


def _lines_stdout(command: dict[str, Any]) -> list[str]:
    if not command["ok"]:
        return []
    return [line.strip() for line in command["stdout"].splitlines() if line.strip()]


def _snapshot(*, gcloud_bin: str, wave_id: str, gcs_prefix: str) -> dict[str, Any]:
    gcs_prefix = gcs_prefix.rstrip("/")
    wave_name_slug = wave_id.replace("_", "-")
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    vm_cmd = _run(
        [
            gcloud_bin,
            "compute",
            "instances",
            "list",
            f"--filter=name~{wave_name_slug}",
            "--format=json(name,status,machineType,lastStartTimestamp,lastStopTimestamp,creationTimestamp)",
        ],
        timeout=120,
    )
    workers_cmd = _run(
        [gcloud_bin, "storage", "ls", f"{gcs_prefix}/workers/", "--recursive"],
        timeout=180,
    )
    aggregate_cmd = _run(
        [gcloud_bin, "storage", "ls", f"{gcs_prefix}/aggregate/", "--recursive"],
        timeout=120,
    )
    fastlane_aggregate_cmd = _run(
        [
            gcloud_bin,
            "storage",
            "ls",
            f"{gcs_prefix}/{DEFAULT_FASTLANE_AGGREGATE_SUBDIR}/",
            "--recursive",
        ],
        timeout=120,
    )
    worker_artifacts = _lines_stdout(workers_cmd)
    aggregate_artifacts = _lines_stdout(aggregate_cmd)
    fastlane_aggregate_artifacts = _lines_stdout(fastlane_aggregate_cmd)
    key_worker_artifacts = [
        line
        for line in worker_artifacts
        if any(
            marker in line
            for marker in (
                "artifacts_manifest.json",
                "option_aware_candidate_summary.csv",
                "research_portfolio_report.json",
                "research_promotion_review_packet.json",
                "startup.log",
            )
        )
    ]
    return {
        "wave_id": wave_id,
        "status_updated_utc": now,
        "gcs_prefix": f"{gcs_prefix}/",
        "safety": {
            "read_only_monitor": True,
            "broker_facing": False,
            "paper_orders": False,
            "live_manifest_changed": False,
            "risk_policy_changed": False,
        },
        "commands": {
            "instances": vm_cmd,
            "workers": {key: value for key, value in workers_cmd.items() if key != "stdout"},
            "aggregate": {key: value for key, value in aggregate_cmd.items() if key != "stdout"},
            "fastlane_aggregate": {
                key: value for key, value in fastlane_aggregate_cmd.items() if key != "stdout"
            },
        },
        "instances": _json_stdout(vm_cmd) or [],
        "worker_artifact_count": len(worker_artifacts),
        "key_worker_artifacts": key_worker_artifacts[-200:],
        "aggregate_artifact_count": len(aggregate_artifacts),
        "aggregate_artifacts": aggregate_artifacts[-200:],
        "fastlane_aggregate_subdir": DEFAULT_FASTLANE_AGGREGATE_SUBDIR,
        "fastlane_aggregate_artifact_count": len(fastlane_aggregate_artifacts),
        "fastlane_aggregate_artifacts": fastlane_aggregate_artifacts[-200:],
    }


def _write_and_upload(
    *,
    snapshot: dict[str, Any],
    output_dir: Path,
    gcloud_bin: str,
    gcs_prefix: str,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = snapshot["status_updated_utc"].replace(":", "").replace("-", "")
    stamp = stamp.replace("T", "_").replace("Z", "Z")
    latest_path = output_dir / "portfolio_overnight_monitor_latest.json"
    stamped_path = output_dir / f"portfolio_overnight_monitor_{stamp}.json"
    payload = json.dumps(snapshot, indent=2, sort_keys=True)
    latest_path.write_text(payload, encoding="utf-8")
    stamped_path.write_text(payload, encoding="utf-8")
    monitor_prefix = gcs_prefix.rstrip("/") + "/monitor"
    _run(
        [
            gcloud_bin,
            "storage",
            "cp",
            str(latest_path),
            f"{monitor_prefix}/portfolio_overnight_monitor_latest.json",
        ],
        timeout=120,
    )
    _run(
        [
            gcloud_bin,
            "storage",
            "cp",
            str(stamped_path),
            f"{monitor_prefix}/{stamped_path.name}",
        ],
        timeout=120,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only monitor for the portfolio overnight GCP research wave."
    )
    parser.add_argument("--wave-id", default=DEFAULT_WAVE_ID)
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX)
    parser.add_argument("--gcloud-bin", default="gcloud")
    parser.add_argument("--output-dir", default="reports/gcp_research/portfolio_overnight_monitor")
    parser.add_argument("--interval-seconds", type=int, default=900)
    parser.add_argument("--duration-hours", type=float, default=12.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    deadline = datetime.now(UTC) + timedelta(hours=args.duration_hours)
    while True:
        snapshot = _snapshot(
            gcloud_bin=str(args.gcloud_bin),
            wave_id=str(args.wave_id),
            gcs_prefix=str(args.gcs_prefix),
        )
        _write_and_upload(
            snapshot=snapshot,
            output_dir=output_dir,
            gcloud_bin=str(args.gcloud_bin),
            gcs_prefix=str(args.gcs_prefix),
        )
        print(
            json.dumps(
                {
                    "status_updated_utc": snapshot["status_updated_utc"],
                    "instances": len(snapshot["instances"]),
                    "worker_artifact_count": snapshot["worker_artifact_count"],
                    "aggregate_artifact_count": snapshot["aggregate_artifact_count"],
                    "fastlane_aggregate_artifact_count": snapshot[
                        "fastlane_aggregate_artifact_count"
                    ],
                },
                sort_keys=True,
            )
        )
        if args.once or datetime.now(UTC) >= deadline:
            break
        time.sleep(max(60, int(args.interval_seconds)))


if __name__ == "__main__":
    main()

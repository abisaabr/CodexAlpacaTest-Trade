from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

DEFAULT_WAVE_ID = "portfolio_overnight_12h_20260501"
DEFAULT_GCS_PREFIX = "gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/"
DEFAULT_OUTPUT_DIR = "reports/gcp_research/paper_trader_readiness_20260501"
DEFAULT_PAPER_VM = "vm-execution-paper-01"
DEFAULT_PAPER_VM_ZONE = "us-east1-b"
DEFAULT_QQQ_PROMOTION_MANIFEST = (
    "config/promotion_manifests/qqq_option_native_governed_validation_20260430.yaml"
)
DEFAULT_LIVE_MANIFEST = "config/strategy_manifests/multi_ticker_portfolio_live.yaml"
DEFAULT_PAPER_CONFIG = "config/multi_ticker_paper_portfolio.yaml"


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


def _yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _json_gcs(*, gcloud_bin: str, uri: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    command = _run([gcloud_bin, "storage", "cat", uri], timeout=120)
    payload = _json_stdout(command)
    return (payload if isinstance(payload, dict) else None), {
        key: value for key, value in command.items() if key != "stdout"
    }


def _list_gcs(*, gcloud_bin: str, uri: str) -> tuple[list[str], dict[str, Any]]:
    command = _run([gcloud_bin, "storage", "ls", uri, "--recursive"], timeout=180)
    lines = [line.strip() for line in command["stdout"].splitlines() if line.strip()]
    return lines, {key: value for key, value in command.items() if key != "stdout"}


def _gate(name: str, passed: bool, detail: str, *, severity: str = "blocker") -> dict[str, Any]:
    return {
        "name": name,
        "status": "passed" if passed else "failed",
        "severity": severity,
        "detail": detail,
    }


def _eligible_count(packet: dict[str, Any] | None) -> int:
    if not packet:
        return 0
    gate_summary = packet.get("gate_summary")
    if isinstance(gate_summary, dict):
        value = gate_summary.get("eligible_for_promotion_review_count")
        if value is not None:
            return int(value)
    value = packet.get("eligible_for_promotion_review_count")
    return int(value or 0)


def _review_candidates(packet: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not packet:
        return []
    rows = packet.get("review_candidates") or packet.get("strategies") or []
    return [row for row in rows if isinstance(row, dict)]


def _paper_vm_status(
    *,
    gcloud_bin: str,
    paper_vm_name: str,
    paper_vm_zone: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    command = _run(
        [
            gcloud_bin,
            "compute",
            "instances",
            "describe",
            paper_vm_name,
            "--zone",
            paper_vm_zone,
            "--format=json(name,status,machineType,lastStartTimestamp,labels,tags,metadata.items)",
        ],
        timeout=120,
    )
    payload = _json_stdout(command)
    return (payload if isinstance(payload, dict) else None), {
        key: value for key, value in command.items() if key != "stdout"
    }


def _wave_instances(*, gcloud_bin: str, wave_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    wave_name_slug = wave_id.replace("_", "-")
    command = _run(
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
    payload = _json_stdout(command)
    instances = payload if isinstance(payload, list) else []
    return [item for item in instances if isinstance(item, dict)], {
        key: value for key, value in command.items() if key != "stdout"
    }


def _local_manifest_summary(*, paper_config: Path, live_manifest: Path) -> dict[str, Any]:
    paper = _yaml_file(paper_config)
    manifest = _yaml_file(live_manifest)
    strategies = manifest.get("strategies") or []
    strategy_rows = [row for row in strategies if isinstance(row, dict)]
    regimes = sorted({str(row.get("regime")) for row in strategy_rows if row.get("regime")})
    symbols = sorted({str(row.get("underlying_symbol")) for row in strategy_rows if row.get("underlying_symbol")})
    execution = paper.get("execution") if isinstance(paper.get("execution"), dict) else {}
    return {
        "paper_config_path": str(paper_config),
        "live_manifest_path": str(live_manifest),
        "paper_config_present": bool(paper),
        "live_manifest_present": bool(manifest),
        "config_submit_paper_orders": execution.get("submit_paper_orders"),
        "strategy_count": len(strategy_rows),
        "symbols": symbols,
        "regimes": regimes,
    }


def _local_qqq_fallback_summary(path: Path) -> dict[str, Any]:
    manifest = _yaml_file(path)
    strategies = [row for row in manifest.get("strategies", []) if isinstance(row, dict)]
    eligible = [
        row
        for row in strategies
        if str(row.get("promotion_status")) == "eligible_for_promotion_review"
    ]
    return {
        "path": str(path),
        "present": bool(manifest),
        "decision": (manifest.get("promotion_packet") or {}).get("decision"),
        "eligible_count": len(eligible),
        "regimes": sorted({str(row.get("intended_regime")) for row in eligible if row.get("intended_regime")}),
        "broker_facing": manifest.get("broker_facing"),
        "live_manifest_effect": manifest.get("live_manifest_effect"),
        "risk_policy_effect": manifest.get("risk_policy_effect"),
    }


def _readiness_decision(gates: list[dict[str, Any]], promotion_packet: dict[str, Any] | None) -> str:
    failed_blockers = [
        gate for gate in gates if gate["status"] != "passed" and gate["severity"] == "blocker"
    ]
    if failed_blockers:
        return "blocked_waiting_for_evidence"
    if not promotion_packet:
        return "standby_waiting_for_overnight_promotion_packet"
    return "operator_review_ready_no_orders"


def build_readiness_snapshot(
    *,
    gcloud_bin: str,
    wave_id: str,
    gcs_prefix: str,
    paper_vm_name: str,
    paper_vm_zone: str,
    paper_config: Path,
    live_manifest: Path,
    qqq_promotion_manifest: Path,
) -> dict[str, Any]:
    gcs_prefix = gcs_prefix.rstrip("/")
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    promotion_uri = f"{gcs_prefix}/aggregate/promotion_packet/research_promotion_review_packet.json"
    portfolio_report_uri = f"{gcs_prefix}/aggregate/portfolio_report/research_portfolio_report.json"
    promotion_packet, promotion_command = _json_gcs(gcloud_bin=gcloud_bin, uri=promotion_uri)
    portfolio_report, portfolio_command = _json_gcs(gcloud_bin=gcloud_bin, uri=portfolio_report_uri)
    worker_artifacts, workers_command = _list_gcs(gcloud_bin=gcloud_bin, uri=f"{gcs_prefix}/workers/")
    aggregate_artifacts, aggregate_command = _list_gcs(gcloud_bin=gcloud_bin, uri=f"{gcs_prefix}/aggregate/")
    wave_instances, instances_command = _wave_instances(gcloud_bin=gcloud_bin, wave_id=wave_id)
    paper_vm, paper_vm_command = _paper_vm_status(
        gcloud_bin=gcloud_bin,
        paper_vm_name=paper_vm_name,
        paper_vm_zone=paper_vm_zone,
    )
    manifest_summary = _local_manifest_summary(paper_config=paper_config, live_manifest=live_manifest)
    qqq_fallback = _local_qqq_fallback_summary(qqq_promotion_manifest)
    eligible_count = _eligible_count(promotion_packet)
    review_candidates = _review_candidates(promotion_packet)
    running_workers = [
        item for item in wave_instances if str(item.get("status", "")).upper() == "RUNNING"
    ]
    terminated_workers = [
        item for item in wave_instances if str(item.get("status", "")).upper() == "TERMINATED"
    ]
    promotion_safety_ok = bool(
        promotion_packet
        and promotion_packet.get("broker_facing") is False
        and promotion_packet.get("live_manifest_effect") == "none"
        and promotion_packet.get("risk_policy_effect") == "none"
    )
    gates = [
        _gate(
            "paper_vm_running",
            bool(paper_vm and paper_vm.get("status") == "RUNNING"),
            f"{paper_vm_name} status is {paper_vm.get('status') if paper_vm else 'unknown'}",
        ),
        _gate(
            "paper_vm_validation_only_label",
            bool(paper_vm and (paper_vm.get("labels") or {}).get("stage") == "validation"),
            "Paper VM remains labeled validation stage.",
            severity="warning",
        ),
        _gate(
            "overnight_workers_active_or_complete",
            bool(running_workers or aggregate_artifacts),
            f"Running wave VMs: {len(running_workers)}; aggregate artifacts: {len(aggregate_artifacts)}",
        ),
        _gate(
            "no_terminated_wave_workers",
            not terminated_workers,
            f"Terminated wave VMs still listed: {[item.get('name') for item in terminated_workers]}",
            severity="warning",
        ),
        _gate(
            "overnight_promotion_packet_present",
            promotion_packet is not None,
            promotion_uri if promotion_packet else "Overnight aggregate promotion packet not present yet.",
        ),
        _gate(
            "overnight_packet_has_eligible_candidates",
            eligible_count > 0,
            f"Eligible candidates in overnight packet: {eligible_count}",
        ),
        _gate(
            "overnight_packet_safety_scope",
            promotion_safety_ok,
            "Promotion packet is research-only, non-broker-facing, and has no manifest/risk effect."
            if promotion_safety_ok
            else "Waiting for promotion packet safety-scope check.",
        ),
        _gate(
            "local_paper_config_present",
            manifest_summary["paper_config_present"] and manifest_summary["live_manifest_present"],
            f"Paper config present={manifest_summary['paper_config_present']}; live manifest present={manifest_summary['live_manifest_present']}",
        ),
        _gate(
            "startup_preflight_available",
            Path("scripts/run_multi_ticker_portfolio_paper_trader.py").exists(),
            "Startup preflight command is available with --startup-preflight --no-submit-paper-orders.",
        ),
        _gate(
            "qqq_fallback_governed_candidates",
            qqq_fallback["eligible_count"] >= 3,
            f"Local QQQ governed fallback eligible count: {qqq_fallback['eligible_count']}",
            severity="warning",
        ),
    ]
    decision = _readiness_decision(gates, promotion_packet)
    return {
        "wave_id": wave_id,
        "status_updated_utc": now,
        "decision": decision,
        "hard_safety": {
            "read_only_monitor": True,
            "started_trading": False,
            "submitted_paper_orders": False,
            "changed_live_manifest": False,
            "changed_risk_policy": False,
            "operator_approval_required_before_paper_orders": True,
        },
        "gcs_prefix": f"{gcs_prefix}/",
        "promotion_packet_uri": promotion_uri,
        "portfolio_report_uri": portfolio_report_uri,
        "paper_vm": paper_vm,
        "wave_instances": wave_instances,
        "running_wave_vm_count": len(running_workers),
        "worker_artifact_count": len(worker_artifacts),
        "aggregate_artifact_count": len(aggregate_artifacts),
        "promotion_packet_present": promotion_packet is not None,
        "portfolio_report_present": portfolio_report is not None,
        "eligible_for_promotion_review_count": eligible_count,
        "review_candidates": review_candidates[:20],
        "manifest_summary": manifest_summary,
        "qqq_fallback": qqq_fallback,
        "readiness_gates": gates,
        "commands": {
            "promotion_packet": promotion_command,
            "portfolio_report": portfolio_command,
            "workers": workers_command,
            "aggregate": aggregate_command,
            "instances": instances_command,
            "paper_vm": paper_vm_command,
        },
        "operator_preflight_command": (
            "python scripts\\run_multi_ticker_portfolio_paper_trader.py "
            "--portfolio-config config\\multi_ticker_paper_portfolio.yaml "
            "--startup-preflight --no-submit-paper-orders"
        ),
        "operator_paper_launch_command_requires_explicit_approval": (
            "python scripts\\run_multi_ticker_portfolio_paper_trader.py "
            "--portfolio-config config\\multi_ticker_paper_portfolio.yaml "
            "--submit-paper-orders"
        ),
    }


def _write_markdown(path: Path, snapshot: dict[str, Any]) -> None:
    gates = snapshot["readiness_gates"]
    candidates = snapshot["review_candidates"]
    lines = [
        "# Paper Trader Readiness Watch",
        "",
        f"- Updated UTC: `{snapshot['status_updated_utc']}`",
        f"- Decision: `{snapshot['decision']}`",
        f"- GCS prefix: `{snapshot['gcs_prefix']}`",
        f"- Promotion packet present: `{snapshot['promotion_packet_present']}`",
        f"- Eligible overnight candidates: `{snapshot['eligible_for_promotion_review_count']}`",
        f"- Running wave VMs: `{snapshot['running_wave_vm_count']}`",
        f"- Worker artifact count: `{snapshot['worker_artifact_count']}`",
        f"- Aggregate artifact count: `{snapshot['aggregate_artifact_count']}`",
        "",
        "## Safety",
        "",
        "- No trading was started.",
        "- No paper orders were submitted.",
        "- No live manifest was changed.",
        "- No risk policy was changed.",
        "- Paper order launch still requires explicit operator approval.",
        "",
        "## Gates",
        "",
    ]
    for gate in gates:
        lines.append(
            f"- `{gate['name']}`: `{gate['status']}` ({gate['severity']}) - {gate['detail']}"
        )
    lines.extend(["", "## Overnight Review Candidates", ""])
    if not candidates:
        lines.append("- No overnight promotion-review candidates are available yet.")
    for row in candidates:
        lines.append(
            "- "
            f"`{row.get('symbol')}` `{row.get('candidate_variant_id')}` "
            f"family `{row.get('family')}` regime `{row.get('intended_regime')}` "
            f"fill `{row.get('min_fill_coverage')}` test_pnl `{row.get('min_test_net_pnl')}`"
        )
    lines.extend(
        [
            "",
            "## Operator Commands",
            "",
            "Preflight only, no orders:",
            "",
            "```powershell",
            snapshot["operator_preflight_command"],
            "```",
            "",
            "Paper order launch requires a separate explicit approval:",
            "",
            "```powershell",
            snapshot["operator_paper_launch_command_requires_explicit_approval"],
            "```",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


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
    latest_json = output_dir / "paper_trader_readiness_latest.json"
    latest_md = output_dir / "paper_trader_readiness_latest.md"
    stamped_json = output_dir / f"paper_trader_readiness_{stamp}.json"
    stamped_md = output_dir / f"paper_trader_readiness_{stamp}.md"
    payload = json.dumps(snapshot, indent=2, sort_keys=True, default=str)
    latest_json.write_text(payload, encoding="utf-8")
    stamped_json.write_text(payload, encoding="utf-8")
    _write_markdown(latest_md, snapshot)
    _write_markdown(stamped_md, snapshot)
    readiness_prefix = gcs_prefix.rstrip("/") + "/paper_readiness"
    for local_path, remote_name in (
        (latest_json, "paper_trader_readiness_latest.json"),
        (latest_md, "paper_trader_readiness_latest.md"),
        (stamped_json, stamped_json.name),
        (stamped_md, stamped_md.name),
    ):
        _run(
            [
                gcloud_bin,
                "storage",
                "cp",
                str(local_path),
                f"{readiness_prefix}/{remote_name}",
            ],
            timeout=120,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only readiness monitor for paper trader launch preparation."
    )
    parser.add_argument("--wave-id", default=DEFAULT_WAVE_ID)
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX)
    parser.add_argument("--gcloud-bin", default="gcloud")
    parser.add_argument("--paper-vm-name", default=DEFAULT_PAPER_VM)
    parser.add_argument("--paper-vm-zone", default=DEFAULT_PAPER_VM_ZONE)
    parser.add_argument("--paper-config", default=DEFAULT_PAPER_CONFIG)
    parser.add_argument("--live-manifest", default=DEFAULT_LIVE_MANIFEST)
    parser.add_argument("--qqq-promotion-manifest", default=DEFAULT_QQQ_PROMOTION_MANIFEST)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--interval-seconds", type=int, default=900)
    parser.add_argument("--duration-hours", type=float, default=12.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    deadline = datetime.now(UTC) + timedelta(hours=args.duration_hours)
    while True:
        snapshot = build_readiness_snapshot(
            gcloud_bin=str(args.gcloud_bin),
            wave_id=str(args.wave_id),
            gcs_prefix=str(args.gcs_prefix),
            paper_vm_name=str(args.paper_vm_name),
            paper_vm_zone=str(args.paper_vm_zone),
            paper_config=Path(args.paper_config),
            live_manifest=Path(args.live_manifest),
            qqq_promotion_manifest=Path(args.qqq_promotion_manifest),
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
                    "decision": snapshot["decision"],
                    "eligible_for_promotion_review_count": snapshot[
                        "eligible_for_promotion_review_count"
                    ],
                    "running_wave_vm_count": snapshot["running_wave_vm_count"],
                },
                sort_keys=True,
            )
        )
        if args.once or datetime.now(UTC) >= deadline:
            break
        time.sleep(max(60, int(args.interval_seconds)))


if __name__ == "__main__":
    main()

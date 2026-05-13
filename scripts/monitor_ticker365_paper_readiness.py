from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

DEFAULT_GCS_PREFIX = (
    "gs://codexalpaca-control-us/research_results/"
    "ticker_365d_all_available_20260501T2300Z"
)
DEFAULT_OUTPUT_DIR = "reports/gcp_research/ticker365_paper_readiness_20260503"
DEFAULT_GCLOUD = r"C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"

PROMOTION_PACKET_PATH = (
    "aggregate/promotion_packet/ticker_365d_all_available_promotion_packet/"
    "research_promotion_review_packet.json"
)
PORTFOLIO_REPORT_PATH = (
    "aggregate/portfolio_report/ticker_365d_all_available_portfolio_report/"
    "research_portfolio_report.json"
)
GROWTH_PROJECTION_PATH = (
    "aggregate/growth_projection/ticker_365d_all_available_growth_projection/"
    "portfolio_growth_projection.json"
)
AGGREGATE_STATUS_PATH = "aggregate/status/ticker_365d_aggregate_status.json"


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only ticker365 paper-readiness monitor."
    )
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX)
    parser.add_argument("--gcloud-bin", default=DEFAULT_GCLOUD)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--interval-seconds", type=int, default=1800)
    parser.add_argument("--duration-hours", type=float, default=12.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def _run(command: list[str], *, timeout: int = 180) -> dict[str, Any]:
    started = utc_now()
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except Exception as exc:  # pragma: no cover - monitor defensive path
        return {
            "args": command,
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
            "started_utc": started,
            "finished_utc": utc_now(),
        }
    return {
        "args": command,
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "started_utc": started,
        "finished_utc": utc_now(),
    }


def _json_from_stdout(command: dict[str, Any]) -> dict[str, Any] | None:
    if not command["ok"]:
        return None
    try:
        payload = json.loads(command["stdout"] or "null")
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _json_gcs(*, gcloud_bin: str, uri: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    command = _run([gcloud_bin, "storage", "cat", uri])
    payload = _json_from_stdout(command)
    return payload, {key: value for key, value in command.items() if key != "stdout"}


def _list_gcs(*, gcloud_bin: str, uri: str) -> tuple[list[str], dict[str, Any]]:
    command = _run([gcloud_bin, "storage", "ls", "--recursive", uri], timeout=300)
    lines = [line.strip() for line in command["stdout"].splitlines() if line.strip()]
    return lines, {key: value for key, value in command.items() if key != "stdout"}


def _running_instances(*, gcloud_bin: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    command = _run(
        [
            gcloud_bin,
            "compute",
            "instances",
            "list",
            "--filter=status=RUNNING",
            "--format=json(name,zone.basename(),machineType.basename(),labels,status)",
        ]
    )
    if command["ok"]:
        try:
            payload = json.loads(command["stdout"] or "[]")
        except json.JSONDecodeError:
            payload = []
    else:
        payload = []
    instances = [row for row in payload if isinstance(row, dict)]
    return instances, {key: value for key, value in command.items() if key != "stdout"}


def _eligible_count(packet: dict[str, Any] | None) -> int:
    if not packet:
        return 0
    gate_summary = packet.get("gate_summary")
    if isinstance(gate_summary, dict):
        return int(gate_summary.get("eligible_for_promotion_review_count") or 0)
    return int(packet.get("eligible_for_promotion_review_count") or 0)


def _unique_eligible_count(packet: dict[str, Any] | None) -> int:
    if not packet:
        return 0
    gate_summary = packet.get("gate_summary")
    if isinstance(gate_summary, dict):
        return int(gate_summary.get("unique_eligible_base_candidate_count") or 0)
    return 0


def _promotion_safety_ok(packet: dict[str, Any] | None) -> bool:
    return bool(
        packet
        and packet.get("broker_facing") is False
        and packet.get("live_manifest_effect") == "none"
        and packet.get("risk_policy_effect") == "none"
    )


def _growth_grade(growth: dict[str, Any] | None) -> dict[str, Any]:
    if not growth:
        return {"grade": "missing", "blockers": ["growth_projection_missing"], "warnings": []}
    grade = growth.get("evidence_grade")
    return grade if isinstance(grade, dict) else {"grade": "unknown", "blockers": [], "warnings": []}


def _decision(
    *,
    aggregate_status: dict[str, Any] | None,
    promotion_packet: dict[str, Any] | None,
    growth_projection: dict[str, Any] | None,
) -> str:
    if not aggregate_status or aggregate_status.get("phase") != "aggregate_completed":
        return "waiting_for_aggregate_completion"
    if not promotion_packet:
        return "blocked_missing_promotion_packet"
    if not _promotion_safety_ok(promotion_packet):
        return "blocked_promotion_packet_safety_scope"
    if _eligible_count(promotion_packet) <= 0:
        return "blocked_no_eligible_candidates_fill_repair_required"
    grade = _growth_grade(growth_projection)
    if grade.get("grade") == "not_institutional_expectation" or grade.get("blockers"):
        return "blocked_growth_projection_not_institutional"
    return "operator_review_ready_no_orders"


def build_snapshot(*, gcloud_bin: str, gcs_prefix: str) -> dict[str, Any]:
    gcs_prefix = gcs_prefix.rstrip("/")
    uris = {
        "aggregate_status": f"{gcs_prefix}/{AGGREGATE_STATUS_PATH}",
        "promotion_packet": f"{gcs_prefix}/{PROMOTION_PACKET_PATH}",
        "portfolio_report": f"{gcs_prefix}/{PORTFOLIO_REPORT_PATH}",
        "growth_projection": f"{gcs_prefix}/{GROWTH_PROJECTION_PATH}",
        "aggregate_root": f"{gcs_prefix}/aggregate/",
    }
    aggregate_status, aggregate_status_command = _json_gcs(
        gcloud_bin=gcloud_bin, uri=uris["aggregate_status"]
    )
    promotion_packet, promotion_packet_command = _json_gcs(
        gcloud_bin=gcloud_bin, uri=uris["promotion_packet"]
    )
    portfolio_report, portfolio_report_command = _json_gcs(
        gcloud_bin=gcloud_bin, uri=uris["portfolio_report"]
    )
    growth_projection, growth_projection_command = _json_gcs(
        gcloud_bin=gcloud_bin, uri=uris["growth_projection"]
    )
    aggregate_artifacts, aggregate_artifacts_command = _list_gcs(
        gcloud_bin=gcloud_bin, uri=uris["aggregate_root"]
    )
    running, running_command = _running_instances(gcloud_bin=gcloud_bin)
    grade = _growth_grade(growth_projection)
    decision = _decision(
        aggregate_status=aggregate_status,
        promotion_packet=promotion_packet,
        growth_projection=growth_projection,
    )
    gate_summary = promotion_packet.get("gate_summary", {}) if promotion_packet else {}
    projection_calendar = growth_projection.get("projection_calendar", {}) if growth_projection else {}
    historical = growth_projection.get("historical_metrics", {}) if growth_projection else {}
    return {
        "generated_at_utc": utc_now(),
        "status": "ticker365_paper_readiness_snapshot_complete",
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
        "uris": uris,
        "aggregate_phase": aggregate_status.get("phase") if aggregate_status else None,
        "aggregate_artifact_count": len(aggregate_artifacts),
        "running_instance_count": len(running),
        "running_instances": running,
        "promotion": {
            "packet_present": promotion_packet is not None,
            "decision": promotion_packet.get("decision") if promotion_packet else None,
            "safety_scope_ok": _promotion_safety_ok(promotion_packet),
            "candidate_count": int(gate_summary.get("candidate_count") or 0),
            "eligible_for_promotion_review_count": _eligible_count(promotion_packet),
            "unique_eligible_base_candidate_count": _unique_eligible_count(promotion_packet),
            "fill_coverage_gate": gate_summary.get("fill_coverage_gate"),
            "blocker_counts": promotion_packet.get("blocker_counts", {}) if promotion_packet else {},
            "fill_failure_counts": (
                portfolio_report.get("fill_failure_counts", {}) if portfolio_report else {}
            ),
        },
        "growth_projection": {
            "packet_present": growth_projection is not None,
            "evidence_grade": grade,
            "raw_dataset_trading_days": projection_calendar.get("raw_dataset_trading_days"),
            "strategy_active_days": projection_calendar.get("strategy_active_days"),
            "inactive_cash_days": projection_calendar.get("inactive_cash_days"),
            "active_day_coverage_pct": projection_calendar.get("active_day_coverage_pct"),
            "ending_equity": historical.get("ending_equity"),
            "total_return_pct": historical.get("total_return_pct"),
            "max_drawdown_pct": historical.get("max_drawdown_pct"),
            "target_hit_in_historical_curve": historical.get("target_hit_in_historical_curve"),
        },
        "next_required_phase": [
            "Do not stage or start paper orders from this aggregate.",
            "Use the promotion packet's strategy_redesign_targets; raw data coverage is strong.",
            "Repair strategy fill timing/replay semantics for top positive-PnL candidates.",
            "Rerun a bounded fill-repair tournament before rebuilding the aggregate packet.",
            "Only prepare a no-order paper launch packet after eligible_for_promotion_review_count is positive and growth evidence is not blocked.",
        ],
        "commands": {
            "aggregate_status": aggregate_status_command,
            "promotion_packet": promotion_packet_command,
            "portfolio_report": portfolio_report_command,
            "growth_projection": growth_projection_command,
            "aggregate_artifacts": aggregate_artifacts_command,
            "running_instances": running_command,
        },
    }


def _write_markdown(path: Path, snapshot: dict[str, Any]) -> None:
    promotion = snapshot["promotion"]
    growth = snapshot["growth_projection"]
    grade = growth.get("evidence_grade") or {}
    lines = [
        "# Ticker365 Paper Readiness",
        "",
        f"- Generated UTC: `{snapshot['generated_at_utc']}`",
        f"- Decision: `{snapshot['decision']}`",
        f"- Aggregate phase: `{snapshot['aggregate_phase']}`",
        f"- Running instances: `{snapshot['running_instance_count']}`",
        f"- Promotion packet present: `{promotion['packet_present']}`",
        f"- Eligible candidates: `{promotion['eligible_for_promotion_review_count']}`",
        f"- Unique eligible base candidates: `{promotion['unique_eligible_base_candidate_count']}`",
        f"- Fill gate: `{promotion['fill_coverage_gate']}`",
        f"- Growth evidence grade: `{grade.get('grade')}`",
        f"- Growth blockers: `{', '.join(grade.get('blockers') or [])}`",
        f"- Full-year trading days: `{growth.get('raw_dataset_trading_days')}`",
        f"- Strategy active days: `{growth.get('strategy_active_days')}`",
        f"- Inactive cash days: `{growth.get('inactive_cash_days')}`",
        f"- Ending equity: `{growth.get('ending_equity')}`",
        f"- Max drawdown pct: `{growth.get('max_drawdown_pct')}`",
        "",
        "## Blockers",
        "",
    ]
    blocker_counts = promotion.get("blocker_counts") or {}
    if blocker_counts:
        for key, value in sorted(blocker_counts.items()):
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- No promotion blockers reported.")
    lines.extend(["", "## Fill Failures", ""])
    fill_failures = promotion.get("fill_failure_counts") or {}
    if fill_failures:
        for key, value in sorted(fill_failures.items()):
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- No fill-failure counts reported.")
    lines.extend(["", "## Next Required Phase", ""])
    for item in snapshot["next_required_phase"]:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "Hard rule: this monitor is read-only. It does not start trading, submit paper orders, modify live manifests, or change risk policy.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outputs(*, output_dir: Path, snapshot: dict[str, Any], gcloud_bin: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_stamp = snapshot["generated_at_utc"].replace(":", "").replace("-", "")
    latest_json = output_dir / "ticker365_paper_readiness_latest.json"
    latest_md = output_dir / "ticker365_paper_readiness_latest.md"
    stamped_json = output_dir / f"ticker365_paper_readiness_{safe_stamp}.json"
    stamped_md = output_dir / f"ticker365_paper_readiness_{safe_stamp}.md"
    text = json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
    latest_json.write_text(text, encoding="utf-8")
    stamped_json.write_text(text, encoding="utf-8")
    _write_markdown(latest_md, snapshot)
    _write_markdown(stamped_md, snapshot)
    remote_prefix = snapshot["gcs_prefix"].rstrip("/") + "/paper_readiness"
    for local_path, remote_name in [
        (latest_json, latest_json.name),
        (latest_md, latest_md.name),
        (stamped_json, stamped_json.name),
        (stamped_md, stamped_md.name),
    ]:
        _run([gcloud_bin, "storage", "cp", str(local_path), f"{remote_prefix}/{remote_name}"])


def main() -> None:
    args = parse_args()
    deadline = datetime.now(UTC) + timedelta(hours=args.duration_hours)
    while True:
        snapshot = build_snapshot(gcloud_bin=args.gcloud_bin, gcs_prefix=args.gcs_prefix)
        write_outputs(
            output_dir=Path(args.output_dir),
            snapshot=snapshot,
            gcloud_bin=args.gcloud_bin,
        )
        print(
            json.dumps(
                {
                    "generated_at_utc": snapshot["generated_at_utc"],
                    "decision": snapshot["decision"],
                    "eligible": snapshot["promotion"]["eligible_for_promotion_review_count"],
                    "growth_grade": snapshot["growth_projection"]["evidence_grade"].get("grade"),
                    "running_instances": snapshot["running_instance_count"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.once or datetime.now(UTC) >= deadline:
            break
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()

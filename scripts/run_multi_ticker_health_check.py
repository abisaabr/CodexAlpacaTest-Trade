from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.execution.ownership import FileOwnershipLease, NoopOwnershipLease
from alpaca_lab.logging_utils import configure_logging, get_logger
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from alpaca_lab.notifications import NtfyNotifier

ET = ZoneInfo("America/New_York")
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the multi-ticker paper trader health and self-heal safe operational issues."
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "multi_ticker_paper_portfolio.yaml"),
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument(
        "--restart-if-needed",
        action="store_true",
        help="Restart the portfolio runner or reinstall the scheduled task when safe operational issues are found.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary.")
    return parser.parse_args()


def _now_et() -> datetime:
    return datetime.now(UTC).astimezone(ET)


def _run_powershell(script: str) -> str:
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            script,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"PowerShell exited with {completed.returncode}")
    return completed.stdout.strip()


def _run_powershell_json(script: str) -> Any:
    raw = _run_powershell(script)
    if not raw:
        return None
    return json.loads(raw)


def _iso_or_none(value: Any) -> str | None:
    if value in (None, "", "null"):
        return None
    text = str(value).strip()
    date_match = re.fullmatch(r"/Date\((\d+)\)/", text)
    if date_match:
        return datetime.fromtimestamp(int(date_match.group(1)) / 1000, tz=ET).isoformat()
    try:
        return datetime.fromisoformat(text).isoformat()
    except ValueError:
        return text


def _get_task_info(task_name: str) -> dict[str, Any] | None:
    task_name_ps = task_name.replace("'", "''")
    script = f"""
    $task = Get-ScheduledTask -TaskName '{task_name_ps}' -ErrorAction SilentlyContinue
    if ($null -eq $task) {{ return }}
    $info = $task | Get-ScheduledTaskInfo
    [pscustomobject]@{{
      task_name = $task.TaskName
      state = [string]$task.State
      last_run_time = $info.LastRunTime
      last_task_result = $info.LastTaskResult
      next_run_time = $info.NextRunTime
      number_of_missed_runs = $info.NumberOfMissedRuns
    }} | ConvertTo-Json -Compress
    """
    payload = _run_powershell_json(script)
    if payload is None:
        return None
    payload["last_run_time"] = _iso_or_none(payload.get("last_run_time"))
    payload["next_run_time"] = _iso_or_none(payload.get("next_run_time"))
    return payload


def _get_trader_processes(python_path: Path) -> list[dict[str, Any]]:
    exe_ps = str(python_path).replace("'", "''")
    script = f"""
    $rows = Get-CimInstance Win32_Process |
      Where-Object {{
        $_.Name -eq 'python.exe' -and
        $_.ExecutablePath -eq '{exe_ps}' -and
        $_.CommandLine -like '*run_multi_ticker_portfolio_paper_trader.py*'
      }} |
      Select-Object ProcessId, CreationDate, ExecutablePath, CommandLine
    if ($null -eq $rows) {{ return }}
    $rows | ConvertTo-Json -Compress
    """
    payload = _run_powershell_json(script)
    if payload is None:
        return []
    if isinstance(payload, dict):
        payload = [payload]
    for row in payload:
        row["creation_date"] = _iso_or_none(row.get("CreationDate"))
        row["process_id"] = int(row.pop("ProcessId"))
        row["command_line"] = row.pop("CommandLine", "")
        row["executable_path"] = row.pop("ExecutablePath", "")
    return payload


def _start_runner(repo_root: Path) -> int | None:
    runner = repo_root / "scripts" / "run_multi_ticker_portfolio_session.ps1"
    runner_ps = str(runner).replace("'", "''")
    cwd_ps = str(repo_root).replace("'", "''")
    script = f"""
    $proc = Start-Process -FilePath 'powershell.exe' `
      -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File','{runner_ps}') `
      -WorkingDirectory '{cwd_ps}' `
      -PassThru
    [pscustomobject]@{{ pid = $proc.Id }} | ConvertTo-Json -Compress
    """
    payload = _run_powershell_json(script)
    if not payload:
        return None
    return int(payload["pid"])


def _reinstall_main_task(repo_root: Path) -> str:
    installer = repo_root / "scripts" / "install_multi_ticker_paper_task.ps1"
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(installer),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
        cwd=str(repo_root),
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"Installer exited with {completed.returncode}")
    return (completed.stdout or "").strip()


def _session_snapshot(session_path: Path) -> dict[str, Any]:
    payload = json.loads(session_path.read_text(encoding="utf-8"))
    stat = session_path.stat()
    payload["session_path"] = str(session_path)
    payload["session_mtime_et"] = datetime.fromtimestamp(stat.st_mtime, tz=ET).isoformat()
    return payload


def _load_health_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _issue(code: str, message: str, *, severity: str = "warning", auto_fixed: bool = False) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "auto_fixed": auto_fixed,
    }


def _send_health_notification(
    notifier: NtfyNotifier,
    report: dict[str, Any],
    state_path: Path,
) -> bool:
    if not notifier.enabled:
        return False

    prior = _load_health_state(state_path)
    issues = report.get("issues", [])
    fixes = report.get("auto_fixes", [])
    status = report.get("status", "unknown")
    signature_payload = {
        "status": status,
        "issues": issues,
        "auto_fixes": fixes,
    }
    signature = hashlib.sha256(
        json.dumps(signature_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    now_et = _now_et()
    previous_signature = str(prior.get("last_signature", ""))
    previous_status = str(prior.get("last_status", ""))

    should_send = False
    if status != "ok":
        should_send = signature != previous_signature
    elif previous_status not in ("", "ok"):
        should_send = True

    payload = {
        "last_signature": signature,
        "last_status": status,
        "last_checked_at": now_et.isoformat(),
    }
    _save_json(state_path, payload)

    if not should_send:
        return False

    if status == "ok":
        return notifier.send_lines(
            "**Portfolio Health Check Recovery**",
            f"Checked at: {now_et.isoformat()}",
            "The multi-ticker paper trader health check is back to normal.",
        )

    lines: list[str] = [
        "**Portfolio Health Check Alert**",
        f"Checked at: {now_et.isoformat()}",
        f"Status: {status}",
    ]
    if fixes:
        lines.append("Auto-fixes:")
        lines.extend(f"- {item['message']}" for item in fixes)
    if issues:
        lines.append("Issues:")
        lines.extend(f"- {item['message']}" for item in issues[:8])
    return notifier.send_lines(*lines)


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    logger = get_logger("multi_ticker_health_check")
    portfolio_config = load_portfolio_config(args.portfolio_config)
    broker = AlpacaBrokerAdapter(settings, dry_run=True)
    notifier = NtfyNotifier(settings)
    if portfolio_config.ownership.enabled:
        ownership_lease = FileOwnershipLease(
            path=portfolio_config.ownership.lease_path,
            owner_label=portfolio_config.ownership.machine_label,
            ttl_seconds=portfolio_config.ownership.lease_ttl_seconds,
        )
    else:
        ownership_lease = NoopOwnershipLease()
    ownership_status = ownership_lease.inspect()
    standby_mode = ownership_status.blocked and not ownership_status.held_by_self

    now_et = _now_et()
    health_root = Path(portfolio_config.execution.run_root).parent / "health"
    health_root.mkdir(parents=True, exist_ok=True)
    health_state_path = health_root / "notification_state.json"

    issues: list[dict[str, Any]] = []
    auto_fixes: list[dict[str, Any]] = []

    task_name = portfolio_config.execution.task_name
    task_info = _get_task_info(task_name)
    if task_info is None:
        issues.append(_issue("scheduled_task_missing", f"Scheduled task '{task_name}' is missing."))
        if args.restart_if_needed:
            output = _reinstall_main_task(PROJECT_ROOT)
            auto_fixes.append(
                _issue(
                    "scheduled_task_reinstalled",
                    f"Reinstalled scheduled task '{task_name}'. {output}",
                    severity="info",
                    auto_fixed=True,
                )
            )
            task_info = _get_task_info(task_name)

    if task_info is not None and str(task_info.get("state")) not in {"Ready", "Running"}:
        issues.append(
            _issue(
                "scheduled_task_state",
                f"Scheduled task '{task_name}' is in unexpected state {task_info.get('state')}.",
            )
        )

    python_path = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    trader_processes = _get_trader_processes(python_path)

    clock_error = None
    try:
        clock = broker.get_clock()
        market_open = bool(clock.get("is_open", False))
        trade_date = datetime.fromisoformat(
            str(clock["timestamp"]).replace("Z", "+00:00")
        ).astimezone(ET).date()
    except Exception as exc:  # noqa: BLE001
        market_open = False
        trade_date = now_et.date()
        clock_error = str(exc)
        issues.append(_issue("clock_unavailable", f"Unable to read Alpaca clock: {exc}", severity="error"))

    session_path = Path(portfolio_config.execution.state_root) / f"session_{trade_date.isoformat()}.json"
    session_payload = _session_snapshot(session_path) if session_path.exists() else None

    if market_open:
        if standby_mode and trader_processes:
            issues.append(
                _issue(
                    "standby_runner_active",
                    "This machine is in standby because another machine owns the lease, but a local trader process is still running.",
                    severity="error",
                )
            )
        elif not trader_processes:
            issues.append(_issue("trader_not_running", "No active portfolio trader process was found during market hours.", severity="error"))
            if args.restart_if_needed:
                pid = _start_runner(PROJECT_ROOT)
                time.sleep(6)
                trader_processes = _get_trader_processes(python_path)
                if trader_processes:
                    auto_fixes.append(
                        _issue(
                            "trader_restarted",
                            f"Restarted the portfolio runner during market hours (wrapper pid {pid}).",
                            severity="info",
                            auto_fixed=True,
                        )
                    )
                else:
                    issues.append(
                        _issue(
                            "trader_restart_failed",
                            "Attempted to restart the portfolio runner, but no active trader process appeared.",
                            severity="error",
                        )
                    )

        if not standby_mode and session_payload is None:
            issues.append(
                _issue(
                    "session_missing",
                    f"Today's session file is missing: {session_path}",
                    severity="error",
                )
            )
        elif not standby_mode:
            session_mtime = datetime.fromisoformat(session_payload["session_mtime_et"])
            max_age_seconds = max(300, portfolio_config.execution.poll_interval_seconds * 6)
            age_seconds = (now_et - session_mtime).total_seconds()
            if age_seconds > max_age_seconds:
                issues.append(
                    _issue(
                        "session_stale",
                        f"Today's session file is stale by {int(age_seconds)} seconds during market hours.",
                        severity="error",
                    )
                )
            if now_et.hour >= 10 and not session_payload.get("notified_morning", False):
                issues.append(
                    _issue(
                        "morning_notification_missing",
                        "Morning notification has not been marked delivered after the market open window.",
                    )
                )
            if now_et.hour >= 13 and not session_payload.get("notified_midday", False):
                issues.append(
                    _issue(
                        "midday_notification_missing",
                        "Midday notification has not been marked delivered after the midday window.",
                    )
                )
    else:
        if task_info is not None and task_info.get("next_run_time") is None:
            issues.append(
                _issue(
                    "next_run_missing",
                    f"Scheduled task '{task_name}' does not have a next run time.",
                    severity="error",
                )
            )
        if (
            not standby_mode
            and now_et.hour >= 16
            and session_payload is not None
            and not session_payload.get("notified_end_of_day", False)
        ):
            issues.append(
                _issue(
                    "end_of_day_notification_missing",
                    "End-of-day notification was not marked delivered after the close.",
                    severity="warning",
                )
            )

    if not notifier.enabled:
        issues.append(
            _issue(
                "ntfy_not_configured",
                "NTFY_TOPIC is not configured, so health alerts cannot be delivered.",
                severity="warning",
            )
        )

    report = {
        "checked_at": now_et.isoformat(),
        "market_open": market_open,
        "trade_date": trade_date.isoformat(),
        "clock_error": clock_error,
        "ownership": {
            "enabled": ownership_status.enabled,
            "held_by_self": ownership_status.held_by_self,
            "blocked": ownership_status.blocked,
            "owner_id": ownership_status.owner_id,
            "owner_label": ownership_status.owner_label,
            "blocked_by_owner_id": ownership_status.blocked_by_owner_id,
            "blocked_by_owner_label": ownership_status.blocked_by_owner_label,
            "lease_path": ownership_status.lease_path,
            "expires_at": ownership_status.expires_at,
        },
        "task_info": task_info,
        "trader_processes": trader_processes,
        "session": session_payload,
        "auto_fixes": auto_fixes,
        "issues": issues,
        "status": "ok" if not issues else ("auto_fixed" if auto_fixes else "issues_detected"),
    }

    timestamp_label = now_et.strftime("%Y%m%d_%H%M%S")
    _save_json(health_root / f"health_check_{timestamp_label}.json", report)
    _save_json(health_root / "latest_health_check.json", report)
    _send_health_notification(notifier, report, health_state_path)

    logger.info("health check status=%s issues=%s auto_fixes=%s", report["status"], len(issues), len(auto_fixes))

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"status={report['status']}")
        for issue in issues:
            print(f"issue[{issue['severity']}]: {issue['message']}")
        for fix in auto_fixes:
            print(f"auto_fix: {fix['message']}")

    raise SystemExit(0)


if __name__ == "__main__":
    main()

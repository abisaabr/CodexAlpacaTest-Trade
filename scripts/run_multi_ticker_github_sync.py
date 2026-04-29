from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging, get_logger
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from alpaca_lab.notifications import NtfyNotifier

ET = ZoneInfo("America/New_York")
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class SyncDecision:
    status: str
    should_apply: bool
    message: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch the configured Git branch, apply manifest updates when safe, and notify via ntfy."
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "multi_ticker_paper_portfolio.yaml"),
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument("--remote", default="origin", help="Git remote to fetch and pull from.")
    parser.add_argument(
        "--branch",
        default=None,
        help="Git branch to track. Defaults to the currently checked-out branch.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary.")
    return parser.parse_args()


def _now_et() -> datetime:
    return datetime.now(UTC).astimezone(ET)


def _run_git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if check and completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"git {' '.join(args)} failed with {completed.returncode}")
    return completed


def _run_powershell_json(script: str) -> Any:
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
    raw = completed.stdout.strip()
    if not raw:
        return None
    return json.loads(raw)


def _iso_or_none(value: Any) -> str | None:
    if value in (None, "", "null"):
        return None
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text).isoformat()
    except ValueError:
        return text


def _get_current_branch() -> str:
    return _run_git("branch", "--show-current").stdout.strip()


def _get_head_sha(ref: str) -> str:
    return _run_git("rev-parse", ref).stdout.strip()


def _working_tree_dirty() -> bool:
    return bool(_run_git("status", "--porcelain", check=False).stdout.strip())


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
    rows: list[dict[str, Any]] = []
    for row in payload:
        rows.append(
            {
                "process_id": int(row["ProcessId"]),
                "creation_date": _iso_or_none(row.get("CreationDate")),
                "command_line": row.get("CommandLine", ""),
                "executable_path": row.get("ExecutablePath", ""),
            }
        )
    return rows


def _market_and_position_state(settings: Any) -> tuple[bool | None, int | None, str | None]:
    try:
        broker = AlpacaBrokerAdapter(settings, dry_run=True)
        clock = broker.get_clock()
        market_open = bool(clock.get("is_open", False))
        positions = broker.get_positions()
        return market_open, len(positions), None
    except Exception as exc:  # noqa: BLE001
        return None, None, str(exc)


def _validate_manifest() -> None:
    completed = subprocess.run(
        ["python", "scripts/sync_live_strategy_manifest.py", "--validate-only"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        raise RuntimeError(stderr or stdout or "Live strategy manifest validation failed.")


def _decide_sync_action(
    *,
    local_sha: str,
    remote_sha: str,
    dirty: bool,
    market_open: bool | None,
    trader_process_count: int,
    broker_position_count: int | None,
) -> SyncDecision:
    if local_sha == remote_sha:
        return SyncDecision("up_to_date", False, "Local branch already matches the remote branch.")
    if dirty:
        return SyncDecision("blocked_dirty_worktree", False, "Local repo has uncommitted changes, so auto-pull is blocked.")
    if market_open is None or broker_position_count is None:
        return SyncDecision("blocked_unknown_safety", False, "Could not determine market/position safety, so auto-pull is blocked.")

    blockers: list[str] = []
    if market_open:
        blockers.append("market_open")
    if trader_process_count > 0:
        blockers.append("trader_running")
    if broker_position_count > 0:
        blockers.append("broker_positions")

    if blockers:
        joined = ", ".join(blockers)
        return SyncDecision("pending_update", False, f"Remote branch is ahead, but auto-pull is waiting for a safe window ({joined}).")

    return SyncDecision("update_ready", True, "Remote branch is ahead and the machine is flat outside market hours.")


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _send_notification(notifier: NtfyNotifier, report: dict[str, Any], state_path: Path) -> bool:
    if not notifier.enabled:
        return False

    prior = {}
    if state_path.exists():
        prior = json.loads(state_path.read_text(encoding="utf-8"))

    signature_payload = {
        "status": report["status"],
        "branch": report["branch"],
        "local_sha": report["local_sha"],
        "remote_sha": report["remote_sha"],
        "applied_sha": report.get("applied_sha"),
        "message": report["message"],
    }
    signature = hashlib.sha256(
        json.dumps(signature_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()

    previous_signature = str(prior.get("last_signature", ""))
    previous_status = str(prior.get("last_status", ""))
    should_send = False
    if report["status"] in {"updated", "error"}:
        should_send = signature != previous_signature
    elif report["status"] == "pending_update":
        should_send = signature != previous_signature
    elif previous_status in {"pending_update", "error", "updated"}:
        should_send = True

    _save_json(
        state_path,
        {
            "last_signature": signature,
            "last_status": report["status"],
            "last_checked_at": report["checked_at"],
        },
    )
    if not should_send:
        return False

    title_map = {
        "updated": "**Portfolio GitHub Sync Applied**",
        "pending_update": "**Portfolio GitHub Sync Pending**",
        "error": "**Portfolio GitHub Sync Alert**",
    }
    lines = [
        title_map.get(report["status"], "**Portfolio GitHub Sync**"),
        f"Checked at: {report['checked_at']}",
        f"Branch: {report['branch']}",
        f"Status: {report['status']}",
        report["message"],
    ]
    if report.get("applied_sha"):
        lines.append(f"Applied SHA: {report['applied_sha']}")
    elif report.get("remote_sha") and report["remote_sha"] != report.get("local_sha"):
        lines.append(f"Remote SHA: {report['remote_sha']}")
    return notifier.send_lines(*lines)


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    logger = get_logger("multi_ticker_github_sync")
    portfolio_config = load_portfolio_config(args.portfolio_config)
    notifier = NtfyNotifier(settings)

    branch = args.branch or _get_current_branch()
    remote_ref = f"{args.remote}/{branch}"
    now_et = _now_et()

    health_root = Path(portfolio_config.execution.run_root).parent / "health"
    health_root.mkdir(parents=True, exist_ok=True)
    state_path = health_root / "github_sync_notification_state.json"

    python_path = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    trader_processes = _get_trader_processes(python_path)

    fetch_error: str | None = None
    local_sha: str | None = None
    remote_sha: str | None = None
    market_open, broker_position_count, broker_error = _market_and_position_state(settings)
    dirty = _working_tree_dirty()
    status = "unknown"
    message = ""
    applied_sha: str | None = None

    try:
        _run_git("fetch", args.remote, branch)
        local_sha = _get_head_sha("HEAD")
        remote_sha = _get_head_sha(remote_ref)
        decision = _decide_sync_action(
            local_sha=local_sha,
            remote_sha=remote_sha,
            dirty=dirty,
            market_open=market_open,
            trader_process_count=len(trader_processes),
            broker_position_count=broker_position_count,
        )
        status = decision.status
        message = decision.message
        if decision.should_apply:
            _run_git("pull", "--ff-only", args.remote, branch)
            _validate_manifest()
            applied_sha = _get_head_sha("HEAD")
            status = "updated"
            message = f"Fast-forwarded {branch} from {local_sha[:7]} to {applied_sha[:7]} and validated the live strategy manifest."
            local_sha = applied_sha
    except Exception as exc:  # noqa: BLE001
        fetch_error = str(exc)
        status = "error"
        message = fetch_error

    report = {
        "checked_at": now_et.isoformat(),
        "status": status,
        "message": message,
        "branch": branch,
        "remote": args.remote,
        "local_sha": local_sha,
        "remote_sha": remote_sha,
        "applied_sha": applied_sha,
        "dirty_worktree": dirty,
        "market_open": market_open,
        "broker_position_count": broker_position_count,
        "broker_error": broker_error,
        "trader_process_count": len(trader_processes),
        "fetch_error": fetch_error,
    }

    timestamp_label = now_et.strftime("%Y%m%d_%H%M%S")
    _save_json(health_root / f"github_sync_{timestamp_label}.json", report)
    _save_json(health_root / "latest_github_sync.json", report)
    _send_notification(notifier, report, state_path)

    logger.info("github sync status=%s branch=%s message=%s", status, branch, message)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"status={status}")
        print(f"branch={branch}")
        print(message)


if __name__ == "__main__":
    main()

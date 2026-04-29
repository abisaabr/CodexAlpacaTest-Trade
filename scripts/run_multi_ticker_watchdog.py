from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging, get_logger
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from alpaca_lab.notifications import DiscordWebhookNotifier, EmailNotifier, NtfyNotifier

ET = ZoneInfo("America/New_York")
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(slots=True)
class WatchdogIssue:
    code: str
    severity: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a portable multi-ticker paper-trader watchdog."
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "multi_ticker_paper_portfolio.yaml"),
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument(
        "--stale-seconds",
        type=int,
        default=900,
        help="Maximum allowed session-file age during market hours before the watchdog flags it.",
    )
    parser.add_argument(
        "--morning-grace-minutes",
        type=int,
        default=20,
        help="Grace period after the open before missing session or morning-alert issues are raised.",
    )
    parser.add_argument(
        "--midday-minute",
        type=int,
        default=12 * 60 + 30,
        help="Minutes after midnight ET when a missing midday update becomes an issue.",
    )
    parser.add_argument(
        "--close-grace-minutes",
        type=int,
        default=15,
        help="Grace period after the close before a missing end-of-day alert becomes an issue.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=3600,
        help="Loop sleep interval when --loop is enabled.",
    )
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Send deduplicated alerts through ntfy, email, and Discord when state changes.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Run a single pass and exit non-zero when watchdog issues are present.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the watchdog report as JSON.")
    return parser.parse_args()


def _now_et() -> datetime:
    return datetime.now(UTC).astimezone(ET)


def _trade_date_from_clock(clock: dict[str, Any]) -> date:
    timestamp = clock.get("timestamp")
    if timestamp:
        return datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).astimezone(ET).date()
    return _now_et().date()


def _rth_open_for(day: date) -> datetime:
    return datetime.combine(day, dt_time(9, 30), tzinfo=ET)


def _rth_close_for(day: date) -> datetime:
    return datetime.combine(day, dt_time(16, 0), tzinfo=ET)


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _load_session_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = _read_json(path, {})
    payload["session_path"] = str(path)
    return payload


def _report_status(issues: list[WatchdogIssue]) -> str:
    if any(issue.severity == "error" for issue in issues):
        return "error"
    if issues:
        return "warning"
    return "ok"


def _build_report(
    *,
    clock: dict[str, Any],
    session_payload: dict[str, Any] | None,
    stale_seconds: int,
    morning_grace_minutes: int,
    midday_minute: int,
    close_grace_minutes: int,
) -> dict[str, Any]:
    trade_date = _trade_date_from_clock(clock)
    now_et = _now_et()
    open_time = _rth_open_for(trade_date)
    close_time = _rth_close_for(trade_date)
    issues: list[WatchdogIssue] = []
    session_last_updated = _parse_timestamp(
        None if session_payload is None else session_payload.get("last_updated_at")
    )

    if bool(clock.get("is_open", False)):
        if session_payload is None and now_et >= open_time + timedelta(minutes=morning_grace_minutes):
            issues.append(
                WatchdogIssue(
                    code="session_missing",
                    severity="error",
                    message="Market is open but no session file exists yet for the current trade date.",
                )
            )
        if session_last_updated is not None:
            age_seconds = max(0, int((now_et - session_last_updated).total_seconds()))
            if age_seconds > stale_seconds:
                issues.append(
                    WatchdogIssue(
                        code="session_stale",
                        severity="error",
                        message=(
                            f"Session file is stale by {age_seconds} seconds, which exceeds the "
                            f"{stale_seconds}-second threshold."
                        ),
                    )
                )
        if session_payload is not None:
            startup_status = str(session_payload.get("startup_check_status", "pending"))
            if startup_status == "failed":
                issues.append(
                    WatchdogIssue(
                        code="startup_failed",
                        severity="error",
                        message="The morning startup check failed and blocked trading.",
                    )
                )
            if (
                now_et >= open_time + timedelta(minutes=morning_grace_minutes)
                and not bool(session_payload.get("notified_morning", False))
            ):
                issues.append(
                    WatchdogIssue(
                        code="morning_notification_missing",
                        severity="warning",
                        message="The morning notification was not sent after the grace period.",
                    )
                )
            midday_cutoff = datetime.combine(
                trade_date,
                dt_time(hour=midday_minute // 60, minute=midday_minute % 60),
                tzinfo=ET,
            )
            if now_et >= midday_cutoff and not bool(session_payload.get("notified_midday", False)):
                issues.append(
                    WatchdogIssue(
                        code="midday_notification_missing",
                        severity="warning",
                        message="The midday notification has not been sent yet.",
                    )
                )
    elif (
        session_payload is not None
        and now_et >= close_time + timedelta(minutes=close_grace_minutes)
        and not bool(session_payload.get("notified_end_of_day", False))
    ):
        issues.append(
            WatchdogIssue(
                code="end_of_day_notification_missing",
                severity="warning",
                message="The end-of-day notification was not sent after the closing grace period.",
            )
        )

    return {
        "checked_at": now_et.isoformat(),
        "trade_date": trade_date.isoformat(),
        "clock": {
            "is_open": bool(clock.get("is_open", False)),
            "timestamp": clock.get("timestamp"),
            "next_open": clock.get("next_open"),
            "next_close": clock.get("next_close"),
        },
        "session": None
        if session_payload is None
        else {
            "path": session_payload.get("session_path"),
            "startup_check_status": session_payload.get("startup_check_status"),
            "blocked_new_entries": bool(session_payload.get("blocked_new_entries", False)),
            "last_updated_at": session_payload.get("last_updated_at"),
            "notified_morning": bool(session_payload.get("notified_morning", False)),
            "notified_midday": bool(session_payload.get("notified_midday", False)),
            "notified_end_of_day": bool(session_payload.get("notified_end_of_day", False)),
            "open_trades": len(session_payload.get("open_trades", [])),
            "completed_trades": len(session_payload.get("completed_trades", [])),
        },
        "issues": [issue.to_dict() for issue in issues],
        "status": _report_status(issues),
    }


def _signature_for_report(report: dict[str, Any]) -> str:
    payload = {
        "status": report.get("status"),
        "trade_date": report.get("trade_date"),
        "issues": report.get("issues", []),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _send_notifications(settings: Any, report: dict[str, Any], state_path: Path) -> bool:
    notifiers = [
        NtfyNotifier(settings),
        EmailNotifier(settings),
        DiscordWebhookNotifier(settings),
    ]
    enabled_notifiers = [notifier for notifier in notifiers if notifier.enabled]
    if not enabled_notifiers:
        return False

    prior = _read_json(state_path, {})
    signature = _signature_for_report(report)
    prior_signature = str(prior.get("last_signature", ""))
    prior_status = str(prior.get("last_status", ""))
    should_send = False
    if report["status"] != "ok":
        should_send = signature != prior_signature
    elif prior_status not in ("", "ok"):
        should_send = True

    _write_json(
        state_path,
        {
            "last_signature": signature,
            "last_status": report["status"],
            "last_checked_at": report["checked_at"],
        },
    )

    if not should_send:
        return False

    if report["status"] == "ok":
        lines = [
            "**Portable Watchdog Recovery**",
            f"Checked at: {report['checked_at']}",
            "The portable paper-trader watchdog is back to normal.",
        ]
    else:
        lines = [
            "**Portable Watchdog Alert**",
            f"Checked at: {report['checked_at']}",
            f"Trade date: {report['trade_date']}",
            f"Status: {report['status']}",
        ]
        for issue in report["issues"]:
            lines.append(f"- [{issue['severity']}] {issue['message']}")

    delivered = False
    for notifier in enabled_notifiers:
        try:
            delivered = notifier.send_lines(*lines) or delivered
        except Exception:  # pragma: no cover - defensive fanout
            continue
    return delivered


def _run_once(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    logger = get_logger("multi_ticker_watchdog")
    portfolio_config = load_portfolio_config(args.portfolio_config)
    broker = AlpacaBrokerAdapter(settings, dry_run=True)
    health_dir = portfolio_config.execution.run_root.parent / "health"
    latest_report_path = health_dir / "portable_watchdog_latest.json"
    notify_state_path = health_dir / "portable_watchdog_notify_state.json"

    try:
        clock = broker.get_clock()
    except Exception as exc:
        logger.exception("Watchdog failed to fetch Alpaca clock: %s", exc)
        report = {
            "checked_at": _now_et().isoformat(),
            "trade_date": _now_et().date().isoformat(),
            "clock": None,
            "session": None,
            "issues": [
                {
                    "code": "clock_fetch_failed",
                    "severity": "error",
                    "message": f"Failed to fetch Alpaca clock: {exc}",
                }
            ],
            "status": "error",
        }
    else:
        trade_date = _trade_date_from_clock(clock)
        session_path = portfolio_config.execution.state_root / f"session_{trade_date.isoformat()}.json"
        session_payload = _load_session_payload(session_path)
        report = _build_report(
            clock=clock,
            session_payload=session_payload,
            stale_seconds=args.stale_seconds,
            morning_grace_minutes=args.morning_grace_minutes,
            midday_minute=args.midday_minute,
            close_grace_minutes=args.close_grace_minutes,
        )

    _write_json(latest_report_path, report)
    if args.notify:
        _send_notifications(settings, report, notify_state_path)

    exit_code = 0 if report["status"] == "ok" else 1
    if args.json:
        print(json.dumps(report, indent=2))
    return report, exit_code


def main() -> None:
    args = parse_args()
    if args.loop:
        while True:
            _run_once(args)
            time.sleep(max(60, args.interval_seconds))
    else:
        _, exit_code = _run_once(args)
        if args.check_only:
            raise SystemExit(exit_code)


if __name__ == "__main__":
    main()

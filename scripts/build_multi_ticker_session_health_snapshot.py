from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
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
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from scripts.audit_realtime_quote_capture_latency import audit_events


ET = ZoneInfo("America/New_York")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a broker-safe health snapshot for the active multi-ticker PAPER "
            "session, quote capture, process scope, and EOD readiness."
        )
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default="config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml",
    )
    parser.add_argument("--trade-date", default=datetime.now(ET).date().isoformat())
    parser.add_argument("--quote-events-jsonl", default=None)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--quote-tail-bytes", type=int, default=30_000_000)
    parser.add_argument("--session-fresh-seconds", type=int, default=180)
    parser.add_argument("--quote-fresh-seconds", type=int, default=30)
    parser.add_argument(
        "--skip-broker",
        action="store_true",
        help="Skip broker account/order/position reads.",
    )
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _mtime_age_seconds(path: Path) -> float | None:
    if not path.exists():
        return None
    return max(0.0, datetime.now(UTC).timestamp() - path.stat().st_mtime)


def _run_process_query() -> list[dict[str, Any]]:
    command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -match 'run_multi_ticker_(portfolio_paper_trader|realtime_shadow_monitor)\\.py' } | "
        "Select-Object ProcessId,CommandLine | ConvertTo-Json -Depth 3"
    )
    try:
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [{"query_error": repr(exc)}]
    if completed.returncode != 0:
        return [{"query_error": completed.stderr.strip() or completed.stdout.strip()}]
    raw = completed.stdout.strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return [{"query_error": raw[:500]}]
    rows = payload if isinstance(payload, list) else [payload]
    return [row for row in rows if isinstance(row, dict)]


def _normalize_command(value: Any) -> str:
    return " ".join(str(value or "").replace("\\", "/").split())


def _process_summary(processes: list[dict[str, Any]], portfolio_config: str) -> dict[str, Any]:
    config_token = _normalize_command(portfolio_config).lower()
    trader_processes: list[dict[str, Any]] = []
    quote_processes: list[dict[str, Any]] = []
    risky_processes: list[dict[str, Any]] = []
    for process in processes:
        command = _normalize_command(process.get("CommandLine"))
        lower = command.lower()
        row = {
            "pid": process.get("ProcessId"),
            "command": command,
        }
        if "run_multi_ticker_portfolio_paper_trader.py" in lower:
            if "--startup-preflight" not in lower and (
                not config_token or config_token in lower
            ):
                trader_processes.append(row)
        if "run_multi_ticker_realtime_shadow_monitor.py" in lower:
            quote_processes.append(row)
        if "--live" in lower or "live_trading=true" in lower:
            risky_processes.append(row)
    return {
        "process_query_error": [row for row in processes if row.get("query_error")],
        "broker_facing_trader_count": len(trader_processes),
        "broker_facing_traders": trader_processes,
        "quote_shadow_count": len(quote_processes),
        "quote_shadows": quote_processes,
        "risky_process_count": len(risky_processes),
        "risky_processes": risky_processes,
    }


def _session_summary(session_path: Path) -> dict[str, Any]:
    payload = _read_json(session_path)
    return {
        "session_path": str(session_path),
        "session_found": bool(payload),
        "session_mtime_age_seconds": _mtime_age_seconds(session_path),
        "completed_trades": len(payload.get("completed_trades") or []),
        "open_trades": len(payload.get("open_trades") or []),
        "realized_pnl": payload.get("realized_pnl"),
        "eod_flatten_checkpoints_completed": payload.get(
            "eod_flatten_checkpoints_completed"
        )
        or [],
        "last_updated_at": payload.get("last_updated_at"),
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
    }


def _broker_summary(settings: Any, *, skip_broker: bool) -> dict[str, Any]:
    if skip_broker:
        return {"broker_skipped": True}
    broker = AlpacaBrokerAdapter(settings, dry_run=True)
    account: dict[str, Any] = {}
    orders: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        account = broker.get_account()
    except Exception as exc:  # noqa: BLE001 - health snapshot must keep going.
        errors.append(f"account: {exc!r}")
    try:
        orders = broker.get_orders(status="open", limit=100)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"open_orders: {exc!r}")
    try:
        positions = broker.get_positions()
    except Exception as exc:  # noqa: BLE001
        errors.append(f"positions: {exc!r}")
    return {
        "broker_skipped": False,
        "broker_errors": errors,
        "account_equity": account.get("equity"),
        "account_buying_power": account.get("buying_power"),
        "open_order_count": len(orders),
        "open_orders": orders,
        "position_count": len(positions),
        "positions": positions,
    }


def build_health_snapshot(
    *,
    settings: Any,
    portfolio_config: Any,
    portfolio_config_path: str,
    trade_date: str,
    quote_events_jsonl: Path | None,
    quote_tail_bytes: int,
    session_fresh_seconds: int,
    quote_fresh_seconds: int,
    skip_broker: bool,
) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    paper_lock: dict[str, Any]
    try:
        settings.assert_paper_only_runtime()
        paper_lock = settings.redacted()
    except Exception as exc:  # noqa: BLE001
        paper_lock = {"paper_only_runtime_error": repr(exc)}
        issues.append("paper_only_runtime_check_failed")

    session_path = Path(portfolio_config.execution.state_root) / f"session_{trade_date}.json"
    lease_path = Path(portfolio_config.ownership.lease_path)
    process_info = _process_summary(_run_process_query(), portfolio_config_path)
    session_info = _session_summary(session_path)
    broker_info = _broker_summary(settings, skip_broker=skip_broker)
    quote_info: dict[str, Any] = {"quote_events_jsonl": str(quote_events_jsonl) if quote_events_jsonl else None}
    if quote_events_jsonl:
        if quote_events_jsonl.exists():
            quote_info.update(audit_events(quote_events_jsonl, tail_bytes=quote_tail_bytes))
        else:
            quote_info["quote_events_missing"] = True
            warnings.append("quote_events_jsonl_missing")

    if process_info["broker_facing_trader_count"] != 1:
        issues.append("expected_exactly_one_broker_facing_trader")
    if process_info["risky_process_count"]:
        issues.append("risky_live_process_token_detected")
    if not session_info["session_found"]:
        issues.append("session_json_missing")
    elif (
        session_info["session_mtime_age_seconds"] is not None
        and session_info["session_mtime_age_seconds"] > session_fresh_seconds
    ):
        warnings.append("session_json_stale")
    if not lease_path.exists():
        issues.append("ownership_lease_missing")
    if broker_info.get("broker_errors"):
        warnings.append("broker_read_errors")
    broker_has_open_risk = bool(
        broker_info.get("open_order_count", 0) or broker_info.get("position_count", 0)
    )
    if broker_has_open_risk and not session_info["open_trades"]:
        warnings.append("broker_has_open_orders_or_positions_without_session_open_trades")
    quote_stale = quote_info.get("last_observed_stale_seconds")
    if isinstance(quote_stale, (int, float)) and quote_stale > quote_fresh_seconds:
        warnings.append("quote_capture_stale")

    status = "healthy"
    if warnings:
        status = "warning"
    if issues:
        status = "issue"
    return {
        "status": status,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "trade_date": trade_date,
        "portfolio_config": portfolio_config_path,
        "paper_lock": paper_lock,
        "issues": issues,
        "warnings": warnings,
        "processes": process_info,
        "ownership_lease": {
            "lease_path": str(lease_path),
            "lease_found": lease_path.exists(),
            "lease_mtime_age_seconds": _mtime_age_seconds(lease_path),
            "lease_payload": _read_json(lease_path),
        },
        "session": session_info,
        "broker": broker_info,
        "broker_open_risk_present": broker_has_open_risk,
        "quote_capture": quote_info,
        "eod_flatten": {
            "configured_minutes_before_close": list(
                portfolio_config.execution.eod_flatten_minutes_before_close
            ),
            "auto_flatten_unexpected_positions": bool(
                portfolio_config.execution.auto_flatten_unexpected_positions
            ),
            "completed_checkpoints": session_info[
                "eod_flatten_checkpoints_completed"
            ],
        },
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    snapshot = build_health_snapshot(
        settings=settings,
        portfolio_config=portfolio_config,
        portfolio_config_path=str(args.portfolio_config),
        trade_date=str(args.trade_date),
        quote_events_jsonl=Path(args.quote_events_jsonl) if args.quote_events_jsonl else None,
        quote_tail_bytes=max(0, int(args.quote_tail_bytes)),
        session_fresh_seconds=max(1, int(args.session_fresh_seconds)),
        quote_fresh_seconds=max(1, int(args.quote_fresh_seconds)),
        skip_broker=bool(args.skip_broker),
    )
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(snapshot, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

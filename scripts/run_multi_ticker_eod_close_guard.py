from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.multi_ticker_portfolio import MultiTickerPortfolioPaperTrader, load_portfolio_config

ET = ZoneInfo("America/New_York")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a dedicated end-of-day close safeguard for the multi-ticker paper trader."
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default="config/multi_ticker_paper_portfolio.yaml",
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument(
        "--submit-paper-orders",
        action="store_true",
        help="Explicitly enable Alpaca paper order submission for the close safeguard.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=30,
        help="How long to wait between close safeguard passes while positions remain.",
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=20,
        help="Maximum total runtime for the close safeguard loop.",
    )
    parser.add_argument(
        "--loop-daily",
        action="store_true",
        help="Run as a long-lived daemon and execute the close safeguard once per market day.",
    )
    parser.add_argument(
        "--idle-seconds",
        type=int,
        default=60,
        help="Sleep duration between daily trigger checks when --loop-daily is enabled.",
    )
    return parser.parse_args()


def _now_et() -> datetime:
    return datetime.now(tz=ET)


def resolve_submit_paper_orders(args: argparse.Namespace, portfolio_config: object) -> bool:
    del portfolio_config
    return bool(args.submit_paper_orders)


def _run_close_guard_once(args: argparse.Namespace) -> dict[str, object]:
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    trader = MultiTickerPortfolioPaperTrader(
        settings,
        portfolio_config,
        submit_paper_orders=resolve_submit_paper_orders(args, portfolio_config),
    )
    logger = trader.logger
    lease_status = trader.acquire_runtime_ownership(role="eod_close_guard")
    if lease_status.blocked:
        return {
            "status": "ownership_blocked",
            "trade_date": None,
            "lease": {
                "lease_path": lease_status.lease_path,
                "blocked_by_owner_id": lease_status.blocked_by_owner_id,
                "blocked_by_owner_label": lease_status.blocked_by_owner_label,
                "expires_at": lease_status.expires_at,
            },
        }
    deadline = _now_et() + timedelta(minutes=max(1, args.max_runtime_minutes))
    ledger = trader.load_ledger()
    clock = trader.broker.get_clock()
    trade_date = datetime.fromisoformat(str(clock["timestamp"]).replace("Z", "+00:00")).astimezone(ET).date()
    passes: list[dict[str, object]] = []

    while True:
        lease_status = trader.acquire_runtime_ownership(role="eod_close_guard")
        if lease_status.blocked:
            return {
                "status": "ownership_blocked",
                "trade_date": trade_date.isoformat(),
                "lease": {
                    "lease_path": lease_status.lease_path,
                    "blocked_by_owner_id": lease_status.blocked_by_owner_id,
                    "blocked_by_owner_label": lease_status.blocked_by_owner_label,
                    "expires_at": lease_status.expires_at,
                },
                "passes": passes,
            }
        session = trader.load_or_create_session(trade_date, ledger)
        stock_frames = trader._fetch_today_stock_frames(trade_date)
        cleanup_summary = trader._run_end_of_day_cleanup_safeguard(
            session=session,
            trade_date=trade_date,
            stock_frames=stock_frames,
        )
        trader.save_session(session)
        pass_result = {
            "timestamp_et": _now_et().isoformat(),
            "shutdown_reconciled": bool(cleanup_summary.get("shutdown_reconciled", False)),
            "open_trade_count": len(session.open_trades),
            "residual_broker_positions": cleanup_summary.get("residual_broker_positions", []),
            "cleanup_summary": cleanup_summary,
        }
        passes.append(pass_result)
        logger.info(
            "eod close guard pass reconciled=%s open_trades=%s residual_positions=%s",
            pass_result["shutdown_reconciled"],
            pass_result["open_trade_count"],
            len(pass_result["residual_broker_positions"]),
        )
        report_path = trader._session_run_dir(trade_date) / "eod_close_guard_report.json"
        report_path.write_text(json.dumps({"trade_date": trade_date.isoformat(), "passes": passes}, indent=2))
        if pass_result["shutdown_reconciled"]:
            return {"status": "reconciled", "trade_date": trade_date.isoformat(), "passes": passes}
        if _now_et() >= deadline:
            return {"status": "timed_out", "trade_date": trade_date.isoformat(), "passes": passes}
        time.sleep(max(5, args.poll_seconds))


def main() -> None:
    args = parse_args()
    if not args.loop_daily:
        result = _run_close_guard_once(args)
        print(json.dumps(result, indent=2))
        if result["status"] not in {"reconciled", "ownership_blocked"}:
            raise SystemExit(1)
        return

    last_run_date: str | None = None
    while True:
        now_et = _now_et()
        if now_et.weekday() < 5 and (now_et.hour > 15 or (now_et.hour == 15 and now_et.minute >= 58)):
            today = now_et.date().isoformat()
            if today != last_run_date:
                result = _run_close_guard_once(args)
                print(json.dumps(result, indent=2))
                last_run_date = today
        time.sleep(max(15, args.idle_seconds))


if __name__ == "__main__":
    main()

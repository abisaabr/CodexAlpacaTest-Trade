from __future__ import annotations

import argparse
import json
import time
import traceback
from datetime import UTC, date, datetime
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
from alpaca_lab.multi_ticker_portfolio import (
    MultiTickerPortfolioPaperTrader,
    load_portfolio_config,
)

ET = ZoneInfo("America/New_York")
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the multi-ticker paper trader as a portable long-running daemon "
            "that waits for the next session instead of exiting after the close."
        )
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "multi_ticker_paper_portfolio.yaml"),
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument(
        "--submit-paper-orders",
        action="store_true",
        help="Explicitly enable Alpaca paper order submission for the portfolio session.",
    )
    parser.add_argument(
        "--idle-sleep-seconds",
        type=int,
        default=900,
        help="Maximum sleep time between completed sessions or retry loops.",
    )
    return parser.parse_args()


def _now_et() -> datetime:
    return datetime.now(UTC).astimezone(ET)


def _trade_date_from_clock(clock: dict[str, Any]) -> date:
    timestamp = clock.get("timestamp")
    if timestamp:
        return datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).astimezone(ET).date()
    return _now_et().date()


def _seconds_until_next_open(clock: dict[str, Any], fallback_seconds: int) -> int:
    next_open = clock.get("next_open")
    if next_open:
        next_open_dt = datetime.fromisoformat(str(next_open).replace("Z", "+00:00"))
        seconds = int((next_open_dt - datetime.now(UTC)).total_seconds())
        return max(60, min(fallback_seconds, seconds)) if seconds > 0 else 60
    return max(60, fallback_seconds)


def _sleep_for(seconds: int, logger_name: str, reason: str) -> None:
    logger = get_logger(logger_name)
    logger.info("Sleeping %s seconds (%s)", seconds, reason)
    time.sleep(max(1, seconds))


def resolve_submit_paper_orders(args: argparse.Namespace, portfolio_config: object) -> bool:
    del portfolio_config
    return bool(args.submit_paper_orders)


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    logger = get_logger("multi_ticker_portable_daemon")
    portfolio_config = load_portfolio_config(args.portfolio_config)
    submit_paper_orders = resolve_submit_paper_orders(args, portfolio_config)
    clock_broker = AlpacaBrokerAdapter(settings, dry_run=True)
    last_finished_trade_date: str | None = None

    while True:
        try:
            pre_clock = clock_broker.get_clock()
            trade_date = _trade_date_from_clock(pre_clock).isoformat()
            if last_finished_trade_date == trade_date and not bool(pre_clock.get("is_open", False)):
                wait_seconds = _seconds_until_next_open(pre_clock, args.idle_sleep_seconds)
                _sleep_for(
                    wait_seconds,
                    "multi_ticker_portable_daemon",
                    f"waiting for next open after {trade_date}",
                )
                continue

            trader = MultiTickerPortfolioPaperTrader(
                settings,
                portfolio_config,
                submit_paper_orders=submit_paper_orders,
            )
            result = trader.run(run_once=False)
            print(json.dumps(result, indent=2))

            status = str(result.get("status", ""))
            if status == "ownership_blocked":
                _sleep_for(
                    max(60, min(args.idle_sleep_seconds, 300)),
                    "multi_ticker_portable_daemon",
                    "standing by because another machine owns the portfolio lease",
                )
                continue
            if status in {"session_complete", "after_close", "startup_check_failed"}:
                last_finished_trade_date = trade_date
            else:
                last_finished_trade_date = None

            post_clock = clock_broker.get_clock()
            if not bool(post_clock.get("is_open", False)):
                wait_seconds = _seconds_until_next_open(post_clock, args.idle_sleep_seconds)
                _sleep_for(
                    wait_seconds,
                    "multi_ticker_portable_daemon",
                    f"post-session wait after {trade_date}",
                )
            else:
                _sleep_for(
                    max(60, min(args.idle_sleep_seconds, 300)),
                    "multi_ticker_portable_daemon",
                    f"non-terminal status {status or 'unknown'}",
                )
        except KeyboardInterrupt:
            logger.info("Portable daemon interrupted; exiting cleanly.")
            return
        except Exception as exc:  # pragma: no cover - protective runtime loop
            logger.exception("Portable daemon iteration failed: %s", exc)
            print(
                json.dumps(
                    {
                        "status": "daemon_error",
                        "message": str(exc),
                        "traceback": traceback.format_exc().splitlines(),
                    },
                    indent=2,
                )
            )
            _sleep_for(
                max(60, min(args.idle_sleep_seconds, 300)),
                "multi_ticker_portable_daemon",
                "retrying after error",
            )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.multi_ticker_portfolio import (
    MultiTickerPortfolioPaperTrader,
    load_portfolio_config,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the shared-account multi-ticker options paper-trader portfolio."
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
        help="Explicitly enable Alpaca paper order submission for the portfolio session.",
    )
    parser.add_argument(
        "--no-submit-paper-orders",
        action="store_true",
        help="Force a dry-run broker adapter even if the portfolio config enables paper orders.",
    )
    parser.add_argument(
        "--startup-preflight",
        action="store_true",
        help="Run launch-time readiness checks and exit before the trading loop.",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single diagnostic cycle instead of waiting through the full RTH session.",
    )
    return parser.parse_args()


def resolve_submit_paper_orders(args: argparse.Namespace, portfolio_config: object) -> bool:
    del portfolio_config
    if args.no_submit_paper_orders or args.startup_preflight:
        return False
    return bool(args.submit_paper_orders)


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging("CRITICAL" if args.startup_preflight else settings.log_level)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    submit_paper_orders = resolve_submit_paper_orders(args, portfolio_config)
    trader = MultiTickerPortfolioPaperTrader(
        settings,
        portfolio_config,
        submit_paper_orders=submit_paper_orders,
    )
    result = (
        trader.run_startup_preflight()
        if args.startup_preflight
        else trader.run(run_once=args.run_once)
    )
    print(json.dumps(result, indent=2))
    if args.startup_preflight and str(result.get("startup_check_status")) != "passed":
        raise SystemExit(43)
    if str(result.get("status")) == "ownership_blocked":
        raise SystemExit(42)


if __name__ == "__main__":
    main()

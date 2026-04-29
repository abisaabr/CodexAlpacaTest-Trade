from __future__ import annotations

import argparse
import json

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.qqq_portfolio import QQQPortfolioPaperTrader, load_portfolio_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the daily QQQ options paper-trader portfolio."
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default="config/qqq_paper_portfolio.yaml",
        help="QQQ portfolio YAML config.",
    )
    parser.add_argument(
        "--submit-paper-orders",
        action="store_true",
        help="Explicitly enable Alpaca paper order submission for the portfolio session.",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single diagnostic cycle instead of waiting through the full RTH session.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    trader = QQQPortfolioPaperTrader(
        settings,
        portfolio_config,
        submit_paper_orders=args.submit_paper_orders or portfolio_config.execution.submit_paper_orders,
    )
    result = trader.run(run_once=args.run_once)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

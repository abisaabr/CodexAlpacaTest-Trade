from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from alpaca_lab.multi_ticker_portfolio.realtime_shadow import RealtimeShadowMonitor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build or run a no-submit realtime SIP/OPRA shadow monitor for the "
            "multi-ticker paper portfolio."
        )
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default="config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml",
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/multi_ticker_portfolio/realtime_shadow",
        help="Directory for subscription plan, JSONL stream events, and summary.",
    )
    parser.add_argument(
        "--max-option-symbols",
        type=int,
        default=900,
        help="Maximum option contracts to subscribe to. Keep below Alpaca plan limits.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=300,
        help="Realtime stream duration when --stream is set.",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Actually connect to SIP/OPRA streams. Omit for safe plan-only mode.",
    )
    parser.add_argument(
        "--include-stock-quotes",
        action="store_true",
        help="Also subscribe to stock quotes. Default keeps stock semantics bar-close compatible.",
    )
    parser.add_argument(
        "--include-option-trades",
        action="store_true",
        help="Also subscribe to option trades in addition to option quotes.",
    )
    parser.add_argument(
        "--no-trade-updates",
        action="store_true",
        help="Disable paper trade-update stream subscription.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    monitor = RealtimeShadowMonitor(
        settings,
        portfolio_config,
        output_dir=Path(args.output_dir),
        max_option_symbols=args.max_option_symbols,
        include_stock_quotes=args.include_stock_quotes,
        include_option_trades=args.include_option_trades,
        include_trade_updates=not args.no_trade_updates,
    )
    plan = monitor.build_plan()
    plan_path = monitor.write_plan(plan)
    if args.stream:
        summary_path = monitor.run_stream(plan, duration_seconds=args.duration_seconds)
        status = "stream_complete"
    else:
        summary_path = monitor.write_summary(plan, status="plan_only")
        status = "plan_only"
    print(
        json.dumps(
            {
                "status": status,
                "mode": "no_submit_shadow",
                "plan_path": str(plan_path),
                "summary_path": str(summary_path),
                "underlying_count": len(plan.underlyings),
                "option_symbol_count": len(plan.option_symbols),
                "stock_feed": plan.stock_feed,
                "option_feed": plan.option_feed,
                "stream_requested": bool(args.stream),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import csv
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
        "--underlying",
        action="append",
        default=[],
        help=(
            "Optional underlying filter for targeted capture, e.g. --underlying QQQ. "
            "May be supplied multiple times. Omit to use the full portfolio config."
        ),
    )
    parser.add_argument(
        "--extra-option-symbols-file",
        action="append",
        default=[],
        help=(
            "Optional text/CSV file of additional option symbols to force into the "
            "subscription plan. CSV columns option_symbol or contract_symbol are supported."
        ),
    )
    parser.add_argument(
        "--runtime-selected-leg-symbols",
        action="store_true",
        help=(
            "Force the current option legs selected by the portfolio runner into the "
            "OPRA subscription plan."
        ),
    )
    parser.add_argument(
        "--runtime-selected-leg-symbols-only",
        action="store_true",
        help=(
            "Subscribe only to the current runtime-selected option legs. This is the "
            "lowest-latency quote-evidence mode for the active paper strategy set."
        ),
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


def _load_extra_option_symbols(paths: list[str]) -> list[str]:
    symbols: list[str] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(f"extra option symbols file not found: {path}")
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames:
                    fieldnames = {name.lower(): name for name in reader.fieldnames}
                    symbol_field = fieldnames.get("option_symbol") or fieldnames.get("contract_symbol")
                    if symbol_field:
                        for row in reader:
                            value = str(row.get(symbol_field) or "").strip().upper()
                            if value:
                                symbols.append(value)
                        continue
        for line in path.read_text(encoding="utf-8").splitlines():
            value = line.strip().split(",", 1)[0].strip().upper()
            if value and not value.startswith("#"):
                symbols.append(value)
    return sorted(set(symbols))


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    extra_option_symbols = _load_extra_option_symbols(args.extra_option_symbols_file)
    runtime_leg_rows: list[dict[str, object]] = []
    runtime_leg_errors: list[dict[str, str]] = []
    if args.runtime_selected_leg_symbols or args.runtime_selected_leg_symbols_only:
        from scripts.build_runtime_leg_quote_capture_symbols import (
            build_runtime_leg_quote_capture_rows,
        )

        _trade_date, runtime_leg_rows, runtime_leg_errors = build_runtime_leg_quote_capture_rows(
            settings,
            portfolio_config,
        )
        requested_underlyings = {str(symbol).strip().upper() for symbol in args.underlying}
        if requested_underlyings:
            runtime_leg_rows = [
                row
                for row in runtime_leg_rows
                if str(row.get("underlying_symbol") or "").strip().upper()
                in requested_underlyings
            ]
        runtime_symbols = sorted(
            {
                str(row.get("option_symbol") or "").strip().upper()
                for row in runtime_leg_rows
                if str(row.get("option_symbol") or "").strip()
            }
        )
        extra_option_symbols = sorted(set(extra_option_symbols).union(runtime_symbols))
        if args.runtime_selected_leg_symbols_only:
            args.max_option_symbols = len(extra_option_symbols)
    monitor = RealtimeShadowMonitor(
        settings,
        portfolio_config,
        output_dir=Path(args.output_dir),
        max_option_symbols=args.max_option_symbols,
        underlyings=args.underlying,
        extra_option_symbols=extra_option_symbols,
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
                "extra_option_symbol_count": len(extra_option_symbols),
                "runtime_selected_leg_row_count": len(runtime_leg_rows),
                "runtime_selected_leg_error_count": len(runtime_leg_errors),
                "runtime_selected_leg_symbols_only": bool(args.runtime_selected_leg_symbols_only),
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

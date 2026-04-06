from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml
from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.data.historical_builder import HistoricalBuildRequest, HistoricalDatasetBuilder
from alpaca_lab.logging_utils import configure_logging

DEFAULT_SYMBOLS = "QQQ,SPY,NVDA,TSLA,META,AAPL,AMD,GOOG,MSFT,AMZN,PLTR,ASML"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a chunked, resumable historical Alpaca dataset."
    )
    parser.add_argument(
        "--config", default=None, help="Optional YAML config path for the build request."
    )
    parser.add_argument(
        "--days", type=int, default=365, help="Calendar days to include when start/end are omitted."
    )
    parser.add_argument("--start-date", default=None, help="Inclusive YYYY-MM-DD start date.")
    parser.add_argument("--end-date", default=None, help="Inclusive YYYY-MM-DD end date.")
    parser.add_argument(
        "--symbols", default=DEFAULT_SYMBOLS, help="Comma-separated stock and option underlyings."
    )
    parser.add_argument(
        "--stock-chunk-days", type=int, default=5, help="Days per stock-bar request chunk."
    )
    parser.add_argument(
        "--contract-chunk-days",
        type=int,
        default=30,
        help="Days per options-contract inventory request chunk.",
    )
    parser.add_argument(
        "--option-batch-size", type=int, default=25, help="Contracts per options data request."
    )
    parser.add_argument("--min-dte", type=int, default=0, help="Minimum days to expiration.")
    parser.add_argument("--max-dte", type=int, default=14, help="Maximum days to expiration.")
    parser.add_argument(
        "--strike-steps", type=int, default=3, help="ATM strike steps on each side to include."
    )
    parser.add_argument(
        "--option-types",
        default="call,put",
        help="Comma-separated option types to include, usually call,put.",
    )
    parser.add_argument("--reference-window-minutes", type=int, default=5)
    parser.add_argument("--feed", default=None, help="Optional Alpaca stock feed override.")
    parser.add_argument(
        "--build-name", default=None, help="Optional deterministic build name override."
    )
    parser.add_argument(
        "--skip-option-bars", action="store_true", help="Skip historical option bar pulls."
    )
    parser.add_argument(
        "--skip-option-trades", action="store_true", help="Skip historical option trade pulls."
    )
    parser.add_argument(
        "--skip-latest-enrichment",
        action="store_true",
        help="Skip current latest-quote/snapshot enrichment for active selected contracts.",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Re-run even if chunks are already completed."
    )
    return parser.parse_args()


def _load_request_file(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("Historical build config must contain a top-level mapping.")
    return payload


def _resolve_dates(args: argparse.Namespace) -> tuple[date, date]:
    if args.start_date and args.end_date:
        return date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
    end_date = date.fromisoformat(args.end_date) if args.end_date else date.today()
    start_date = (
        date.fromisoformat(args.start_date)
        if args.start_date
        else end_date - timedelta(days=args.days - 1)
    )
    return start_date, end_date


def main() -> None:
    args = parse_args()
    request_file = _load_request_file(args.config)
    start_date, end_date = _resolve_dates(args)

    request_payload: dict[str, Any] = {
        "stock_symbols": args.symbols,
        "option_underlyings": args.symbols,
        "start_date": start_date,
        "end_date": end_date,
        "stock_chunk_days": args.stock_chunk_days,
        "contract_chunk_days": args.contract_chunk_days,
        "option_batch_size": args.option_batch_size,
        "min_dte": args.min_dte,
        "max_dte": args.max_dte,
        "strike_steps": args.strike_steps,
        "option_types": args.option_types,
        "reference_window_minutes": args.reference_window_minutes,
        "feed": args.feed,
        "build_name": args.build_name,
        "include_option_bars": not args.skip_option_bars,
        "include_option_trades": not args.skip_option_trades,
        "include_latest_enrichment": not args.skip_latest_enrichment,
        "overwrite": args.overwrite,
    }
    request_payload.update(request_file)

    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)

    broker = AlpacaBrokerAdapter(settings, dry_run=True)
    builder = HistoricalDatasetBuilder(settings, broker)
    request = HistoricalBuildRequest.model_validate(request_payload)
    result = builder.build(request)

    print(
        json.dumps(
            {
                "build_name": result.build_name,
                "manifest_path": str(result.manifest_path),
                "report_root": str(result.report_root),
                "silver_root": str(result.silver_root),
                "raw_root": str(result.raw_root),
                "quality_report_path": str(result.quality_report_path),
                "summary_report_path": str(result.summary_report_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

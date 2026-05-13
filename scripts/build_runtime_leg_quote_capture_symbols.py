from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from alpaca_lab.multi_ticker_portfolio.trader import MultiTickerPortfolioPaperTrader, _now_et


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a targeted OPRA quote-capture universe from the option legs that "
            "the multi-ticker PAPER runner would select at the current snapshot."
        )
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        required=True,
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Output directory for CSV/TXT/summary files. Defaults to the portfolio "
            "run_root when omitted."
        ),
    )
    parser.add_argument(
        "--prefix",
        default="targeted_runtime_leg_symbols",
        help="Output filename prefix.",
    )
    return parser.parse_args()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _counter_payload(counter: Counter[str]) -> dict[str, int]:
    return {key: int(value) for key, value in sorted(counter.items())}


def build_runtime_leg_quote_capture_rows(
    settings: Any,
    portfolio_config: Any,
) -> tuple[date, list[dict[str, Any]], list[dict[str, str]]]:
    """Return current runtime-selected option legs for targeted OPRA capture."""

    trader = MultiTickerPortfolioPaperTrader(
        settings,
        portfolio_config,
        submit_paper_orders=False,
    )
    trade_date = _now_et().date()
    stock_frames = trader._fetch_today_stock_frames(trade_date)

    rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for underlying_symbol in portfolio_config.execution.underlying_symbols:
        frame = stock_frames.get(underlying_symbol)
        if frame is None or frame.empty:
            errors.append(
                {
                    "underlying_symbol": underlying_symbol,
                    "error": "missing_stock_frame",
                }
            )
            continue
        spot_price = float(frame.iloc[-1]["close"])
        try:
            contracts = trader._refresh_contract_cache_if_needed(trade_date, underlying_symbol)
            candidate_symbols, metadata = trader._candidate_symbols_for_snapshot(
                contracts,
                spot_price,
                trade_date,
            )
            option_chain = trader._fetch_option_chain(
                candidate_symbols,
                metadata,
                spot_price=spot_price,
                trade_date=trade_date,
                underlying_symbol=underlying_symbol,
            )
        except Exception as exc:  # noqa: BLE001 - diagnostic script should keep going.
            errors.append(
                {
                    "underlying_symbol": underlying_symbol,
                    "error": repr(exc),
                }
            )
            continue

        for strategy in portfolio_config.strategies:
            if strategy.underlying_symbol != underlying_symbol:
                continue
            try:
                selected_legs = trader._select_legs(strategy, option_chain, trade_date)
            except Exception as exc:  # noqa: BLE001 - diagnostic script should keep going.
                errors.append(
                    {
                        "underlying_symbol": underlying_symbol,
                        "strategy_name": strategy.name,
                        "error": repr(exc),
                    }
                )
                continue
            for leg_index, leg in enumerate(selected_legs, start=1):
                rows.append(
                    {
                        "option_symbol": leg.symbol,
                        "underlying_symbol": underlying_symbol,
                        "strategy_name": strategy.name,
                        "regime": strategy.regime,
                        "family": strategy.family,
                        "dte_mode": strategy.dte_mode,
                        "leg_index": leg_index,
                        "option_type": leg.option_type,
                        "side": leg.side,
                        "target_delta": _safe_float(leg.target_delta),
                        "delta": _safe_float(leg.delta),
                        "bid": _safe_float(leg.bid),
                        "ask": _safe_float(leg.ask),
                        "spread_pct": _safe_float(leg.spread_pct),
                        "freshness_seconds": _safe_float(leg.freshness_seconds),
                        "quote_time": leg.quote_time,
                    }
                )
    return trade_date, rows, errors


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    trade_date, rows, errors = build_runtime_leg_quote_capture_rows(settings, portfolio_config)

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(portfolio_config.execution.run_root)
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    csv_path = output_dir / f"{args.prefix}_{timestamp}.csv"
    txt_path = output_dir / f"{args.prefix}_{timestamp}.txt"
    summary_path = output_dir / f"{args.prefix}_{timestamp}_summary.json"

    fieldnames = [
        "option_symbol",
        "underlying_symbol",
        "strategy_name",
        "regime",
        "family",
        "dte_mode",
        "leg_index",
        "option_type",
        "side",
        "target_delta",
        "delta",
        "bid",
        "ask",
        "spread_pct",
        "freshness_seconds",
        "quote_time",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    unique_symbols = sorted({str(row["option_symbol"]) for row in rows})
    txt_path.write_text("\n".join(unique_symbols) + ("\n" if unique_symbols else ""), encoding="utf-8")

    by_underlying = Counter(str(row["underlying_symbol"]) for row in rows)
    unique_by_underlying = {
        underlying: len(
            {
                str(row["option_symbol"])
                for row in rows
                if str(row["underlying_symbol"]) == underlying
            }
        )
        for underlying in sorted(by_underlying)
    }
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "portfolio_config": str(args.portfolio_config),
        "trade_date": trade_date.isoformat(),
        "csv_path": str(csv_path),
        "txt_path": str(txt_path),
        "summary_path": str(summary_path),
        "selected_leg_rows": len(rows),
        "unique_option_symbols": len(unique_symbols),
        "by_underlying": _counter_payload(by_underlying),
        "unique_by_underlying": unique_by_underlying,
        "by_regime": _counter_payload(Counter(str(row["regime"]) for row in rows)),
        "error_count": len(errors),
        "errors": errors,
        "recommended_shadow_args": [
            "--max-option-symbols",
            str(len(unique_symbols)),
            "--extra-option-symbols-file",
            str(txt_path),
            "--stream",
            "--no-trade-updates",
        ],
        "notes": [
            "Use this file as the forced OPRA universe for no-submit quote capture.",
            "Omit stock quotes and option trades when latency is more important than broad microstructure discovery.",
        ],
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

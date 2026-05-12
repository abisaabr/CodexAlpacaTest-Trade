from __future__ import annotations

import argparse
import json
import math
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Apply realtime quote sidecars to option-aware trade economics using "
            "causal as-of joins. This is research-only and never submits orders."
        )
    )
    parser.add_argument("--trade-economics-csv", required=True)
    parser.add_argument("--quote-sidecar-csv", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--output-summary-json", default=None)
    parser.add_argument(
        "--option-trades-csv",
        default=None,
        help="Optional option_trade_prints.csv emitted by build_realtime_quote_quality_sidecar.py.",
    )
    parser.add_argument("--entry-selection-window-seconds", type=float, default=60.0)
    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class QuoteIndex:
    frame: pd.DataFrame
    times: list[pd.Timestamp]


def _timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _contract_symbols(row: pd.Series) -> list[str]:
    raw = str(row.get("contract_symbol") or "")
    return [item.strip().upper() for item in raw.split(";") if item.strip()]


def _build_quote_index(quotes: pd.DataFrame) -> dict[str, QuoteIndex]:
    if quotes.empty:
        return {}
    symbol_column = "option_symbol" if "option_symbol" in quotes.columns else "symbol"
    if symbol_column not in quotes.columns or "event_time_utc" not in quotes.columns:
        raise ValueError("quote sidecar must contain option_symbol or symbol plus event_time_utc")
    frame = quotes.copy()
    frame["_event_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["_event_ts"])
    frame[symbol_column] = frame[symbol_column].astype(str).str.upper()
    index: dict[str, QuoteIndex] = {}
    for symbol, group in frame.sort_values([symbol_column, "_event_ts"]).groupby(symbol_column):
        group = group.reset_index(drop=True)
        index[str(symbol)] = QuoteIndex(frame=group, times=list(group["_event_ts"]))
    return index


def _latest_quote(index: dict[str, QuoteIndex], symbol: str, decision_time: pd.Timestamp) -> pd.Series | None:
    quote_index = index.get(symbol.upper())
    if quote_index is None:
        return None
    position = bisect_right(quote_index.times, decision_time) - 1
    if position < 0:
        return None
    return quote_index.frame.iloc[position]


def _quote_quality_for_side(
    *,
    row: pd.Series,
    quote_index: dict[str, QuoteIndex],
    prefix: str,
    decision_time: pd.Timestamp | None,
) -> dict[str, Any]:
    contracts = _contract_symbols(row)
    if decision_time is None or not contracts:
        return {
            f"{prefix}_quote_source": "option_quote_sidecar_missing_decision_time",
            f"{prefix}_legs_with_bid_ask": 0,
            f"{prefix}_quote_sidecar_leg_count": len(contracts),
            f"{prefix}_quote_sidecar_missing_legs": len(contracts),
        }
    relative_spreads: list[float] = []
    quote_ages: list[float] = []
    quote_times: list[str] = []
    bid_values: list[float] = []
    ask_values: list[float] = []
    missing = 0
    for symbol in contracts:
        quote = _latest_quote(quote_index, symbol, decision_time)
        if quote is None:
            missing += 1
            continue
        bid = _safe_float(quote.get("bid", quote.get("bid_price")))
        ask = _safe_float(quote.get("ask", quote.get("ask_price")))
        quote_time = _timestamp(quote.get("event_time_utc"))
        if bid is None or ask is None or quote_time is None or not (bid > 0.0 and ask >= bid):
            missing += 1
            continue
        mid = (bid + ask) / 2.0
        if mid <= 0.0:
            missing += 1
            continue
        bid_values.append(bid)
        ask_values.append(ask)
        relative_spreads.append((ask - bid) / mid)
        quote_ages.append(max((decision_time - quote_time).total_seconds(), 0.0))
        quote_times.append(quote_time.isoformat())
    found = len(relative_spreads)
    if found == len(contracts):
        source = "option_quote_bid_ask"
    elif found:
        source = "option_quote_partial_bid_ask"
    else:
        source = "option_quote_sidecar_missing"
    output: dict[str, Any] = {
        f"{prefix}_average_relative_spread": round(sum(relative_spreads) / found, 8)
        if found
        else None,
        f"{prefix}_max_relative_spread": round(max(relative_spreads), 8) if found else None,
        f"{prefix}_quote_age_seconds": round(max(quote_ages), 6) if found else None,
        f"{prefix}_quote_source": source,
        f"{prefix}_legs_with_bid_ask": found,
        f"{prefix}_quote_sidecar_leg_count": len(contracts),
        f"{prefix}_quote_sidecar_missing_legs": missing,
        f"{prefix}_quote_time": ";".join(quote_times),
    }
    if len(contracts) == 1 and found == 1:
        bid = bid_values[0]
        ask = ask_values[0]
        mid = (bid + ask) / 2.0
        spread = ask - bid
        output.update(
            {
                f"{prefix}_bid": bid,
                f"{prefix}_ask": ask,
                f"{prefix}_mid": round(mid, 6),
                f"{prefix}_absolute_spread": round(spread, 6),
                f"{prefix}_relative_spread": round(spread / mid, 8) if mid > 0.0 else None,
                f"{prefix}_spread_pct": round(spread / mid, 8) if mid > 0.0 else None,
            }
        )
    return output


def _build_trade_index(trades: pd.DataFrame) -> dict[str, list[pd.Timestamp]]:
    if trades.empty:
        return {}
    if "option_symbol" not in trades.columns or "event_time_utc" not in trades.columns:
        raise ValueError("option trades CSV must contain option_symbol and event_time_utc")
    frame = trades.copy()
    frame["_event_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["_event_ts"])
    frame["option_symbol"] = frame["option_symbol"].astype(str).str.upper()
    return {
        str(symbol): list(group.sort_values("_event_ts")["_event_ts"])
        for symbol, group in frame.groupby("option_symbol")
    }


def _count_prints(
    trade_index: dict[str, list[pd.Timestamp]],
    symbols: list[str],
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> int | None:
    if start is None or end is None:
        return None
    total = 0
    for symbol in symbols:
        times = trade_index.get(symbol.upper(), [])
        total += max(0, bisect_right(times, end) - bisect_right(times, start))
    return total


def apply_quote_sidecar_to_trade_economics(
    *,
    trade_economics_csv: Path,
    quote_sidecar_csv: Path,
    output_csv: Path,
    option_trades_csv: Path | None = None,
    entry_selection_window_seconds: float = 60.0,
    output_summary_json: Path | None = None,
) -> dict[str, Any]:
    trades = pd.read_csv(trade_economics_csv, low_memory=False)
    quote_sidecar = pd.read_csv(quote_sidecar_csv, low_memory=False)
    quote_index = _build_quote_index(quote_sidecar)
    trade_print_index = (
        _build_trade_index(pd.read_csv(option_trades_csv, low_memory=False))
        if option_trades_csv
        else {}
    )
    enriched_rows: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    for _, row in trades.iterrows():
        entry_time = _timestamp(row.get("stock_entry_time") or row.get("option_entry_time"))
        exit_time = _timestamp(row.get("stock_exit_time") or row.get("option_exit_time"))
        output = row.to_dict()
        for prefix, decision_time in (("entry", entry_time), ("exit", exit_time)):
            quality = _quote_quality_for_side(
                row=row,
                quote_index=quote_index,
                prefix=prefix,
                decision_time=decision_time,
            )
            output.update(quality)
            status = str(quality.get(f"{prefix}_quote_source") or "unknown")
            status_counts[f"{prefix}:{status}"] = status_counts.get(f"{prefix}:{status}", 0) + 1
        if trade_print_index:
            symbols = _contract_symbols(row)
            entry_window_end = (
                entry_time + pd.Timedelta(seconds=entry_selection_window_seconds)
                if entry_time is not None
                else None
            )
            entry_prints = _count_prints(trade_print_index, symbols, entry_time, entry_window_end)
            full_prints = _count_prints(trade_print_index, symbols, entry_time, exit_time)
            if entry_prints is not None:
                output["entry_selection_trade_print_count"] = entry_prints
            if full_prints is not None:
                output["option_trade_print_count"] = full_prints
        enriched_rows.append(output)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(enriched_rows).to_csv(output_csv, index=False)
    summary = {
        "status": "quote_sidecar_trade_economics_complete",
        "trade_economics_csv": str(trade_economics_csv),
        "quote_sidecar_csv": str(quote_sidecar_csv),
        "option_trades_csv": str(option_trades_csv) if option_trades_csv else None,
        "output_csv": str(output_csv),
        "trade_rows": int(len(enriched_rows)),
        "quote_symbol_count": int(len(quote_index)),
        "quote_source_status_counts": status_counts,
    }
    summary_path = output_summary_json or output_csv.with_suffix(".summary.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    summary = apply_quote_sidecar_to_trade_economics(
        trade_economics_csv=Path(args.trade_economics_csv),
        quote_sidecar_csv=Path(args.quote_sidecar_csv),
        option_trades_csv=Path(args.option_trades_csv) if args.option_trades_csv else None,
        output_csv=Path(args.output_csv),
        output_summary_json=Path(args.output_summary_json) if args.output_summary_json else None,
        entry_selection_window_seconds=args.entry_selection_window_seconds,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


OPTION_SYMBOL_RE = re.compile(r"^([A-Z]+)\d{6}[CP]\d{8}$")
OPTION_QUOTE_COLUMNS = [
    "symbol",
    "event_time_utc",
    "observed_at_utc",
    "quote_age_seconds",
    "latency_seconds",
    "bid",
    "ask",
    "bid_price",
    "ask_price",
    "bid_size",
    "ask_size",
    "mid",
    "absolute_spread",
    "relative_spread",
    "spread_pct",
    "quote_source",
    "underlying_symbol",
    "option_symbol",
]
STOCK_QUOTE_COLUMNS = [
    "symbol",
    "event_time_utc",
    "observed_at_utc",
    "quote_age_seconds",
    "latency_seconds",
    "bid",
    "ask",
    "bid_price",
    "ask_price",
    "bid_size",
    "ask_size",
    "mid",
    "absolute_spread",
    "relative_spread",
    "spread_pct",
    "quote_source",
    "underlying_symbol",
]
OPTION_TRADE_COLUMNS = [
    "option_symbol",
    "underlying_symbol",
    "event_time_utc",
    "observed_at_utc",
    "trade_age_seconds",
    "price",
    "size",
]
OPTION_MINUTE_COLUMNS = [
    "symbol",
    "option_symbol",
    "underlying_symbol",
    "timestamp",
    "quote_time",
    "latest_quote_time",
    "bid",
    "ask",
    "bid_price",
    "ask_price",
    "mid",
    "absolute_spread",
    "relative_spread",
    "spread_pct",
    "quote_source",
    "quote_count",
    "quote_age_seconds",
    "quote_age_seconds_p50",
    "relative_spread_p50",
    "relative_spread_p90",
    "relative_spread_max",
    "event_latency_seconds_p50",
    "option_trade_print_count",
    "option_trade_print_volume",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert no-submit realtime shadow websocket events into quote-quality "
            "sidecars for quote-backed replay hardening. This never submits orders."
        )
    )
    parser.add_argument("--events-jsonl", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--underlyings",
        default="",
        help="Optional comma-separated underlying filter, e.g. QQQ,SPY,IWM.",
    )
    parser.add_argument(
        "--option-symbols",
        default="",
        help="Optional comma-separated exact OPRA option symbol filter.",
    )
    parser.add_argument(
        "--option-symbols-file",
        default="",
        help=(
            "Optional file containing exact OPRA option symbols, separated by "
            "newlines, commas, semicolons, or whitespace. Merged with --option-symbols."
        ),
    )
    return parser.parse_args()


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _payload_value(payload: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = payload.get(name)
        if value not in (None, ""):
            return value
    return None


def _underlying_from_option_symbol(symbol: str) -> str:
    match = OPTION_SYMBOL_RE.match(symbol.upper())
    return match.group(1) if match else "UNKNOWN"


def _csv_set(value: str) -> set[str]:
    return {item.strip().upper() for item in value.split(",") if item.strip()}


def _symbol_file_set(path: Path | None) -> set[str]:
    if path is None:
        return set()
    text = path.read_text(encoding="utf-8")
    return {item.strip().upper() for item in re.split(r"[\s,;]+", text) if item.strip()}


def _quote_row(
    *,
    symbol: str,
    event: dict[str, Any],
    payload: dict[str, Any],
    is_option: bool,
) -> dict[str, Any] | None:
    observed_at = _parse_timestamp(event.get("observed_at_utc"))
    quote_time = _parse_timestamp(_payload_value(payload, "timestamp", "t"))
    bid = _safe_float(_payload_value(payload, "bid_price", "bp", "bid"))
    ask = _safe_float(_payload_value(payload, "ask_price", "ap", "ask"))
    if observed_at is None or quote_time is None or bid is None or ask is None:
        return None
    if not (bid > 0.0 and ask > 0.0 and ask >= bid):
        return None
    bid_size = _safe_float(_payload_value(payload, "bid_size", "bs"))
    ask_size = _safe_float(_payload_value(payload, "ask_size", "as"))
    mid = (bid + ask) / 2.0
    spread = ask - bid
    relative_spread = spread / mid if mid > 0.0 else None
    row = {
        "symbol": symbol,
        "event_time_utc": quote_time.isoformat(),
        "observed_at_utc": observed_at.isoformat(),
        "quote_age_seconds": round(max((observed_at - quote_time).total_seconds(), 0.0), 6),
        "latency_seconds": _safe_float(event.get("latency_seconds")),
        "bid": bid,
        "ask": ask,
        "bid_price": bid,
        "ask_price": ask,
        "bid_size": bid_size if bid_size is not None else 0.0,
        "ask_size": ask_size if ask_size is not None else 0.0,
        "mid": round(mid, 6),
        "absolute_spread": round(spread, 6),
        "relative_spread": round(relative_spread, 8) if relative_spread is not None else None,
        "spread_pct": round(relative_spread, 8) if relative_spread is not None else None,
        "quote_source": "option_quote_bid_ask" if is_option else "stock_quote_bid_ask",
    }
    if is_option:
        row["underlying_symbol"] = _underlying_from_option_symbol(symbol)
        row["option_symbol"] = symbol
    else:
        row["underlying_symbol"] = symbol
    return row


def _trade_row(event: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any] | None:
    symbol = str(payload.get("symbol") or "").upper()
    if not symbol:
        return None
    observed_at = _parse_timestamp(event.get("observed_at_utc"))
    trade_time = _parse_timestamp(_payload_value(payload, "timestamp", "t"))
    price = _safe_float(_payload_value(payload, "price", "p"))
    size = _safe_float(_payload_value(payload, "size", "s"))
    if observed_at is None or trade_time is None or price is None:
        return None
    return {
        "option_symbol": symbol,
        "underlying_symbol": _underlying_from_option_symbol(symbol),
        "event_time_utc": trade_time.isoformat(),
        "observed_at_utc": observed_at.isoformat(),
        "trade_age_seconds": round(max((observed_at - trade_time).total_seconds(), 0.0), 6),
        "price": price,
        "size": size if size is not None else 0.0,
    }


def _minute_key(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.floor("min")


def _aggregate_option_quotes(
    quotes: pd.DataFrame,
    trades: pd.DataFrame,
) -> pd.DataFrame:
    if quotes.empty:
        return pd.DataFrame(columns=OPTION_MINUTE_COLUMNS)
    frame = quotes.copy()
    frame["_minute"] = _minute_key(frame["event_time_utc"])
    frame = frame.sort_values(["option_symbol", "_minute", "event_time_utc", "observed_at_utc"])
    trade_counts: dict[tuple[str, pd.Timestamp], dict[str, Any]] = {}
    if not trades.empty:
        trades_frame = trades.copy()
        trades_frame["_minute"] = _minute_key(trades_frame["event_time_utc"])
        grouped_trades = trades_frame.groupby(["option_symbol", "_minute"], dropna=False)
        for key, group in grouped_trades:
            trade_counts[key] = {
                "option_trade_print_count": int(len(group)),
                "option_trade_print_volume": float(group["size"].sum()),
            }
    rows: list[dict[str, Any]] = []
    for (symbol, minute), group in frame.groupby(["option_symbol", "_minute"], sort=True):
        latest = group.iloc[-1]
        trade_summary = trade_counts.get((symbol, minute), {})
        rows.append(
            {
                "symbol": symbol,
                "option_symbol": symbol,
                "underlying_symbol": latest["underlying_symbol"],
                "timestamp": minute.isoformat(),
                "quote_time": latest["event_time_utc"],
                "latest_quote_time": latest["event_time_utc"],
                "bid": latest["bid"],
                "ask": latest["ask"],
                "bid_price": latest["bid"],
                "ask_price": latest["ask"],
                "mid": latest["mid"],
                "absolute_spread": latest["absolute_spread"],
                "relative_spread": latest["relative_spread"],
                "spread_pct": latest["spread_pct"],
                "quote_source": "option_quote_bid_ask",
                "quote_count": int(len(group)),
                "quote_age_seconds": round(float(group["quote_age_seconds"].max()), 6),
                "quote_age_seconds_p50": round(float(group["quote_age_seconds"].median()), 6),
                "relative_spread_p50": round(float(group["relative_spread"].median()), 8),
                "relative_spread_p90": round(float(group["relative_spread"].quantile(0.90)), 8),
                "relative_spread_max": round(float(group["relative_spread"].max()), 8),
                "event_latency_seconds_p50": (
                    round(float(group["latency_seconds"].dropna().median()), 6)
                    if group["latency_seconds"].notna().any()
                    else None
                ),
                "option_trade_print_count": int(trade_summary.get("option_trade_print_count", 0)),
                "option_trade_print_volume": float(trade_summary.get("option_trade_print_volume", 0.0)),
            }
        )
    return pd.DataFrame(rows)


def build_realtime_quote_quality_sidecar(
    *,
    events_jsonl: Path,
    output_dir: Path,
    underlyings: set[str] | None = None,
    option_symbols: set[str] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    option_quotes: list[dict[str, Any]] = []
    stock_quotes: list[dict[str, Any]] = []
    option_trades: list[dict[str, Any]] = []
    stats: dict[str, Any] = {
        "events_seen": 0,
        "option_quote_events": 0,
        "accepted_option_quotes": 0,
        "stock_quote_events": 0,
        "accepted_stock_quotes": 0,
        "option_trade_events": 0,
        "accepted_option_trades": 0,
        "filtered_events": 0,
        "rejected_events": 0,
        "underlying_filter": sorted(underlyings) if underlyings else [],
        "option_symbol_filter": sorted(option_symbols) if option_symbols else [],
    }
    with events_jsonl.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            stats["events_seen"] += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                stats["rejected_events"] += 1
                continue
            event_type = str(event.get("event_type") or "")
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            symbol = str(payload.get("symbol") or "").upper()
            if event_type == "option_quote":
                stats["option_quote_events"] += 1
                underlying = _underlying_from_option_symbol(symbol)
                if option_symbols and symbol not in option_symbols:
                    stats["filtered_events"] += 1
                    continue
                if underlyings and underlying not in underlyings:
                    stats["filtered_events"] += 1
                    continue
                row = _quote_row(symbol=symbol, event=event, payload=payload, is_option=True)
                if row:
                    option_quotes.append(row)
                    stats["accepted_option_quotes"] += 1
                else:
                    stats["rejected_events"] += 1
            elif event_type == "stock_quote":
                stats["stock_quote_events"] += 1
                if underlyings and symbol not in underlyings:
                    stats["filtered_events"] += 1
                    continue
                row = _quote_row(symbol=symbol, event=event, payload=payload, is_option=False)
                if row:
                    stock_quotes.append(row)
                    stats["accepted_stock_quotes"] += 1
                else:
                    stats["rejected_events"] += 1
            elif event_type == "option_trade":
                stats["option_trade_events"] += 1
                underlying = _underlying_from_option_symbol(symbol)
                if option_symbols and symbol not in option_symbols:
                    stats["filtered_events"] += 1
                    continue
                if underlyings and underlying not in underlyings:
                    stats["filtered_events"] += 1
                    continue
                row = _trade_row(event, payload)
                if row:
                    option_trades.append(row)
                    stats["accepted_option_trades"] += 1
                else:
                    stats["rejected_events"] += 1

    option_quote_frame = pd.DataFrame(option_quotes, columns=OPTION_QUOTE_COLUMNS)
    stock_quote_frame = pd.DataFrame(stock_quotes, columns=STOCK_QUOTE_COLUMNS)
    option_trade_frame = pd.DataFrame(option_trades, columns=OPTION_TRADE_COLUMNS)
    minute_frame = _aggregate_option_quotes(option_quote_frame, option_trade_frame)

    option_quote_path = output_dir / "option_quote_sidecar.csv"
    stock_quote_path = output_dir / "stock_quote_sidecar.csv"
    option_trade_path = output_dir / "option_trade_prints.csv"
    minute_path = output_dir / "option_quote_quality_by_minute.csv"
    option_quote_frame.to_csv(option_quote_path, index=False)
    stock_quote_frame.to_csv(stock_quote_path, index=False)
    option_trade_frame.to_csv(option_trade_path, index=False)
    minute_frame.to_csv(minute_path, index=False)

    summary = {
        "status": "quote_quality_sidecar_complete",
        "events_jsonl": str(events_jsonl),
        "output_dir": str(output_dir),
        "stats": stats,
        "quality_status": (
            "quote_events_ready_for_asof_replay_join"
            if stats["accepted_option_quotes"] > 0
            else "no_option_quote_events"
        ),
        "causality_contract": (
            "Use option_quote_sidecar.csv for as-of joins at or before each "
            "strategy decision timestamp. The minute aggregate is diagnostic "
            "only unless replay code explicitly treats its timestamp causally."
        ),
        "outputs": {
            "option_quote_sidecar_csv": str(option_quote_path),
            "option_quote_quality_by_minute_csv": str(minute_path),
            "option_trade_prints_csv": str(option_trade_path),
            "stock_quote_sidecar_csv": str(stock_quote_path),
        },
    }
    (output_dir / "quote_quality_sidecar_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def main() -> None:
    args = parse_args()
    option_symbols = _csv_set(args.option_symbols)
    option_symbols.update(_symbol_file_set(Path(args.option_symbols_file) if args.option_symbols_file else None))
    summary = build_realtime_quote_quality_sidecar(
        events_jsonl=Path(args.events_jsonl),
        output_dir=Path(args.output_dir),
        underlyings=_csv_set(args.underlyings) or None,
        option_symbols=option_symbols or None,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

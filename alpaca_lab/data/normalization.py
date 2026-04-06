from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

import pandas as pd

from alpaca_lab.data.schemas import (
    OPTION_BAR_SCHEMA,
    OPTION_CONTRACT_SCHEMA,
    OPTION_QUOTE_SCHEMA,
    OPTION_SNAPSHOT_SCHEMA,
    OPTION_TRADE_SCHEMA,
    SELECTED_OPTION_SCHEMA,
    STOCK_BAR_SCHEMA,
)


SHORT_BAR_RENAMES = {
    "t": "timestamp",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "n": "trade_count",
    "vw": "vwap",
}


def _as_payloads(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        return [payload]
    return list(payload)


def _records_from_symbol_map(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    key: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for page in _as_payloads(payload):
        symbol_map = page.get(key, {})
        if not isinstance(symbol_map, Mapping):
            continue
        for symbol, rows in symbol_map.items():
            for row in rows:
                record = dict(row)
                record["symbol"] = symbol
                records.append(record)
    return records


def normalize_stock_bars(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    frame = pd.DataFrame(_records_from_symbol_map(payload, "bars"))
    if frame.empty:
        return frame
    frame = frame.rename(columns=SHORT_BAR_RENAMES)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    for column in ("open", "high", "low", "close", "volume", "trade_count", "vwap"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    columns = [
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
    ]
    return (
        frame.reindex(columns=[column for column in columns if column in frame.columns])
        .drop_duplicates(subset=["symbol", "timestamp"])
        .sort_values(["symbol", "timestamp"])
        .reset_index(drop=True)
    )


def normalize_option_contracts(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for page in _as_payloads(payload):
        for row in page.get("option_contracts", []):
            records.append(dict(row))
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    for column in ("expiration_date", "open_interest_date", "close_price_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    for column in ("strike_price", "size", "open_interest", "close_price"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.drop_duplicates(subset=["symbol"])
        .sort_values(["underlying_symbol", "expiration_date", "strike_price", "symbol"])
        .reset_index(drop=True)
    )


def normalize_option_bars(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    frame = pd.DataFrame(_records_from_symbol_map(payload, "bars"))
    if frame.empty:
        return frame
    frame = frame.rename(columns=SHORT_BAR_RENAMES)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    for column in ("open", "high", "low", "close", "volume", "trade_count", "vwap"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.drop_duplicates(subset=["symbol", "timestamp"])
        .sort_values(["symbol", "timestamp"])
        .reset_index(drop=True)
    )


def normalize_stock_bar_chunk(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    source_feed: str,
    chunk_start: datetime,
    chunk_end: datetime,
) -> pd.DataFrame:
    frame = normalize_stock_bars(payload)
    if frame.empty:
        return STOCK_BAR_SCHEMA.apply(frame)
    frame["source_feed"] = source_feed
    frame["chunk_start"] = chunk_start
    frame["chunk_end"] = chunk_end
    return STOCK_BAR_SCHEMA.apply(frame)


def normalize_option_contract_inventory(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    inventory_status: str | None,
    inventory_collected_at: datetime,
    expiration_window_start: date,
    expiration_window_end: date,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for page in _as_payloads(payload):
        for row in page.get("option_contracts", []):
            record = dict(row)
            record["contract_id"] = record.pop("id", None)
            record["option_type"] = record.pop("type", None)
            record["contract_size"] = record.pop("size", None)
            record["inventory_status"] = inventory_status
            record["inventory_collected_at"] = inventory_collected_at
            record["expiration_window_start"] = expiration_window_start
            record["expiration_window_end"] = expiration_window_end
            records.append(record)
    frame = pd.DataFrame(records)
    if frame.empty:
        return OPTION_CONTRACT_SCHEMA.apply(frame)
    return OPTION_CONTRACT_SCHEMA.apply(frame)


def normalize_option_trade_chunk(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    trade_date: date,
    underlying_lookup: Mapping[str, str] | None = None,
    chunk_id: str | None = None,
) -> pd.DataFrame:
    records = _records_from_symbol_map(payload, "trades")
    frame = pd.DataFrame(records)
    if frame.empty:
        return OPTION_TRADE_SCHEMA.apply(frame)
    frame = frame.rename(columns={"t": "timestamp", "p": "price", "s": "size", "x": "exchange", "c": "conditions"})
    frame["trade_date"] = trade_date
    frame["underlying_symbol"] = frame["symbol"].map(underlying_lookup or {})
    frame["chunk_id"] = chunk_id
    if "conditions" in frame.columns:
        frame["conditions"] = frame["conditions"].apply(lambda value: str(value) if value is not None else pd.NA)
    return OPTION_TRADE_SCHEMA.apply(frame)


def normalize_option_bar_chunk(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    trade_date: date,
    underlying_lookup: Mapping[str, str] | None = None,
    chunk_id: str | None = None,
) -> pd.DataFrame:
    frame = normalize_option_bars(payload)
    if frame.empty:
        return OPTION_BAR_SCHEMA.apply(frame)
    frame["trade_date"] = trade_date
    frame["underlying_symbol"] = frame["symbol"].map(underlying_lookup or {})
    frame["chunk_id"] = chunk_id
    return OPTION_BAR_SCHEMA.apply(frame)


def normalize_option_latest_quotes(
    payload: Mapping[str, Any],
    *,
    collected_at: datetime,
    underlying_lookup: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    quotes = payload.get("quotes", {})
    if isinstance(quotes, Mapping):
        for symbol, row in quotes.items():
            record = {
                "symbol": symbol,
                "underlying_symbol": (underlying_lookup or {}).get(symbol),
                "quote_timestamp": row.get("t"),
                "bid_price": row.get("bp"),
                "bid_size": row.get("bs"),
                "bid_exchange": row.get("bx"),
                "ask_price": row.get("ap"),
                "ask_size": row.get("as"),
                "ask_exchange": row.get("ax"),
                "quote_condition": row.get("c"),
                "collected_at": collected_at,
            }
            records.append(record)
    frame = pd.DataFrame(records)
    if frame.empty:
        return OPTION_QUOTE_SCHEMA.apply(frame)
    return OPTION_QUOTE_SCHEMA.apply(frame)


def normalize_option_snapshots(
    payload: Mapping[str, Any],
    *,
    collected_at: datetime,
    underlying_lookup: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    snapshots = payload.get("snapshots", {})
    if isinstance(snapshots, Mapping):
        for symbol, row in snapshots.items():
            latest_quote = row.get("latestQuote", {}) if isinstance(row, Mapping) else {}
            latest_trade = row.get("latestTrade", {}) if isinstance(row, Mapping) else {}
            minute_bar = row.get("minuteBar", {}) if isinstance(row, Mapping) else {}
            daily_bar = row.get("dailyBar", {}) if isinstance(row, Mapping) else {}
            prev_daily_bar = row.get("prevDailyBar", {}) if isinstance(row, Mapping) else {}
            greeks = row.get("greeks", {}) if isinstance(row, Mapping) else {}
            records.append(
                {
                    "symbol": symbol,
                    "underlying_symbol": (underlying_lookup or {}).get(symbol),
                    "collected_at": collected_at,
                    "implied_volatility": row.get("impliedVolatility"),
                    "delta": greeks.get("delta"),
                    "gamma": greeks.get("gamma"),
                    "theta": greeks.get("theta"),
                    "vega": greeks.get("vega"),
                    "rho": greeks.get("rho"),
                    "latest_quote_timestamp": latest_quote.get("t"),
                    "latest_bid_price": latest_quote.get("bp"),
                    "latest_bid_size": latest_quote.get("bs"),
                    "latest_bid_exchange": latest_quote.get("bx"),
                    "latest_ask_price": latest_quote.get("ap"),
                    "latest_ask_size": latest_quote.get("as"),
                    "latest_ask_exchange": latest_quote.get("ax"),
                    "latest_quote_condition": latest_quote.get("c"),
                    "latest_trade_timestamp": latest_trade.get("t"),
                    "latest_trade_price": latest_trade.get("p"),
                    "latest_trade_size": latest_trade.get("s"),
                    "latest_trade_exchange": latest_trade.get("x"),
                    "latest_trade_condition": latest_trade.get("c"),
                    "minute_bar_timestamp": minute_bar.get("t"),
                    "minute_bar_open": minute_bar.get("o"),
                    "minute_bar_high": minute_bar.get("h"),
                    "minute_bar_low": minute_bar.get("l"),
                    "minute_bar_close": minute_bar.get("c"),
                    "minute_bar_volume": minute_bar.get("v"),
                    "minute_bar_trade_count": minute_bar.get("n"),
                    "minute_bar_vwap": minute_bar.get("vw"),
                    "daily_bar_timestamp": daily_bar.get("t"),
                    "daily_bar_open": daily_bar.get("o"),
                    "daily_bar_high": daily_bar.get("h"),
                    "daily_bar_low": daily_bar.get("l"),
                    "daily_bar_close": daily_bar.get("c"),
                    "daily_bar_volume": daily_bar.get("v"),
                    "daily_bar_trade_count": daily_bar.get("n"),
                    "daily_bar_vwap": daily_bar.get("vw"),
                    "prev_daily_bar_timestamp": prev_daily_bar.get("t"),
                    "prev_daily_bar_open": prev_daily_bar.get("o"),
                    "prev_daily_bar_high": prev_daily_bar.get("h"),
                    "prev_daily_bar_low": prev_daily_bar.get("l"),
                    "prev_daily_bar_close": prev_daily_bar.get("c"),
                    "prev_daily_bar_volume": prev_daily_bar.get("v"),
                    "prev_daily_bar_trade_count": prev_daily_bar.get("n"),
                    "prev_daily_bar_vwap": prev_daily_bar.get("vw"),
                }
            )
    frame = pd.DataFrame(records)
    if frame.empty:
        return OPTION_SNAPSHOT_SCHEMA.apply(frame)
    return OPTION_SNAPSHOT_SCHEMA.apply(frame)


def normalize_selected_option_contracts(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return SELECTED_OPTION_SCHEMA.apply(frame)
    return SELECTED_OPTION_SCHEMA.apply(frame)

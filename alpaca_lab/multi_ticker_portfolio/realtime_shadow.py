from __future__ import annotations

import json
import re
import threading
import time
from decimal import Decimal
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from alpaca.data.enums import DataFeed, OptionsFeed
from alpaca.data.live.option import OptionDataStream
from alpaca.data.live.stock import StockDataStream
from alpaca.trading.stream import TradingStream

from alpaca_lab.config import LabSettings
from alpaca_lab.multi_ticker_portfolio.config import MultiTickerPortfolioConfig
from alpaca_lab.multi_ticker_portfolio.trader import MultiTickerPortfolioPaperTrader, _now_et


@dataclass(frozen=True, slots=True)
class RealtimeShadowPlan:
    trade_date: str
    underlyings: list[str]
    option_symbols: list[str]
    stock_feed: str
    option_feed: str
    option_symbol_limit: int
    generated_at_utc: str
    forced_option_symbols: list[str] = field(default_factory=list)
    missing_forced_option_symbols: list[str] = field(default_factory=list)
    source: str = "rest_bootstrap_for_realtime_shadow"
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RealtimeShadowStats:
    stock_bar_events: int = 0
    stock_updated_bar_events: int = 0
    stock_quote_events: int = 0
    option_quote_events: int = 0
    option_trade_events: int = 0
    trade_update_events: int = 0
    unknown_events: int = 0
    last_event_at_utc: str | None = None
    max_event_latency_seconds: float | None = None
    min_event_latency_seconds: float | None = None
    _latency_samples: list[float] = field(default_factory=list, repr=False)
    _latency_samples_by_event_type: dict[str, list[float]] = field(
        default_factory=dict,
        repr=False,
    )
    _option_quote_spread_samples: list[float] = field(default_factory=list, repr=False)
    _option_quote_relative_spread_samples: list[float] = field(default_factory=list, repr=False)

    def record(
        self,
        event_type: str,
        latency_seconds: float | None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if event_type == "stock_bar":
            self.stock_bar_events += 1
        elif event_type == "stock_updated_bar":
            self.stock_updated_bar_events += 1
        elif event_type == "stock_quote":
            self.stock_quote_events += 1
        elif event_type == "option_quote":
            self.option_quote_events += 1
            self._record_option_quote_spread(payload or {})
        elif event_type == "option_trade":
            self.option_trade_events += 1
        elif event_type == "trade_update":
            self.trade_update_events += 1
        else:
            self.unknown_events += 1
        self.last_event_at_utc = datetime.now(UTC).isoformat()
        if latency_seconds is not None:
            latency_value = float(latency_seconds)
            self._latency_samples.append(latency_value)
            self._latency_samples_by_event_type.setdefault(event_type, []).append(latency_value)
            self.max_event_latency_seconds = max(self.max_event_latency_seconds or 0.0, latency_value)
            self.min_event_latency_seconds = (
                latency_value
                if self.min_event_latency_seconds is None
                else min(self.min_event_latency_seconds, latency_value)
            )

    def _record_option_quote_spread(self, payload: dict[str, Any]) -> None:
        bid = _coerce_float(payload.get("bid_price", payload.get("bp")))
        ask = _coerce_float(payload.get("ask_price", payload.get("ap")))
        if bid is None or ask is None or ask < bid or bid < 0.0:
            return
        spread = ask - bid
        midpoint = (ask + bid) / 2.0
        self._option_quote_spread_samples.append(spread)
        if midpoint > 0.0:
            self._option_quote_relative_spread_samples.append(spread / midpoint)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        samples = sorted(payload.pop("_latency_samples", []))
        latency_samples_by_event_type = payload.pop("_latency_samples_by_event_type", {})
        spread_samples = sorted(payload.pop("_option_quote_spread_samples", []))
        relative_spread_samples = sorted(
            payload.pop("_option_quote_relative_spread_samples", [])
        )
        payload["latency_sample_count"] = len(samples)
        if samples:
            payload["latency_p50_seconds"] = _percentile(samples, 0.50)
            payload["latency_p90_seconds"] = _percentile(samples, 0.90)
            payload["latency_p99_seconds"] = _percentile(samples, 0.99)
        else:
            payload["latency_p50_seconds"] = None
            payload["latency_p90_seconds"] = None
            payload["latency_p99_seconds"] = None
        payload["latency_by_event_type"] = {
            str(event_type): _sample_summary(sorted(event_samples))
            for event_type, event_samples in sorted(latency_samples_by_event_type.items())
            if event_samples
        }
        payload["option_quote_spread_sample_count"] = len(spread_samples)
        if spread_samples:
            payload["option_quote_spread_p50"] = _percentile(spread_samples, 0.50)
            payload["option_quote_spread_p90"] = _percentile(spread_samples, 0.90)
            payload["option_quote_spread_p99"] = _percentile(spread_samples, 0.99)
            payload["option_quote_spread_max"] = spread_samples[-1]
        else:
            payload["option_quote_spread_p50"] = None
            payload["option_quote_spread_p90"] = None
            payload["option_quote_spread_p99"] = None
            payload["option_quote_spread_max"] = None
        if relative_spread_samples:
            payload["option_quote_relative_spread_p50"] = _percentile(
                relative_spread_samples, 0.50
            )
            payload["option_quote_relative_spread_p90"] = _percentile(
                relative_spread_samples, 0.90
            )
            payload["option_quote_relative_spread_p99"] = _percentile(
                relative_spread_samples, 0.99
            )
            payload["option_quote_relative_spread_max"] = relative_spread_samples[-1]
        else:
            payload["option_quote_relative_spread_p50"] = None
            payload["option_quote_relative_spread_p90"] = None
            payload["option_quote_relative_spread_p99"] = None
            payload["option_quote_relative_spread_max"] = None
        return payload


def _percentile(sorted_samples: list[float], pct: float) -> float:
    return sorted_samples[int((len(sorted_samples) - 1) * pct)]


def _sample_summary(sorted_samples: list[float]) -> dict[str, float | int]:
    return {
        "sample_count": len(sorted_samples),
        "min_seconds": sorted_samples[0],
        "p50_seconds": _percentile(sorted_samples, 0.50),
        "p90_seconds": _percentile(sorted_samples, 0.90),
        "p99_seconds": _percentile(sorted_samples, 0.99),
        "max_seconds": sorted_samples[-1],
    }


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_option_symbols(symbols: list[str] | None) -> list[str]:
    if not symbols:
        return []
    return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})


_OPTION_SYMBOL_RE = re.compile(r"^(.+?)(\d{6})([CP])(\d{8})$")


@dataclass(frozen=True, slots=True)
class ParsedOptionSymbol:
    root: str
    expiration: str
    right: str
    strike: int


def _parse_option_symbol(symbol: str) -> ParsedOptionSymbol | None:
    match = _OPTION_SYMBOL_RE.match(str(symbol).strip().upper())
    if not match:
        return None
    root, expiration, right, raw_strike = match.groups()
    return ParsedOptionSymbol(
        root=root,
        expiration=expiration,
        right=right,
        strike=int(raw_strike),
    )


def _prioritise_discovered_symbols(
    *,
    discovered_symbols: list[str],
    extra_symbols: list[str],
) -> list[str]:
    if not extra_symbols:
        return discovered_symbols
    exact_groups: dict[tuple[str, str, str], list[int]] = {}
    expiry_groups: dict[tuple[str, str], list[int]] = {}
    root_groups: dict[str, list[int]] = {}
    for symbol in extra_symbols:
        parsed = _parse_option_symbol(symbol)
        if parsed is None:
            continue
        exact_groups.setdefault((parsed.root, parsed.expiration, parsed.right), []).append(
            parsed.strike
        )
        expiry_groups.setdefault((parsed.root, parsed.expiration), []).append(parsed.strike)
        root_groups.setdefault(parsed.root, []).append(parsed.strike)

    def rank(symbol: str) -> tuple[int, int, str]:
        parsed = _parse_option_symbol(symbol)
        if parsed is None:
            return (4, 10**12, symbol)
        exact_key = (parsed.root, parsed.expiration, parsed.right)
        if exact_key in exact_groups:
            return (
                0,
                min(abs(parsed.strike - extra_strike) for extra_strike in exact_groups[exact_key]),
                symbol,
            )
        expiry_key = (parsed.root, parsed.expiration)
        if expiry_key in expiry_groups:
            return (
                1,
                min(abs(parsed.strike - extra_strike) for extra_strike in expiry_groups[expiry_key]),
                symbol,
            )
        if parsed.root in root_groups:
            return (
                2,
                min(abs(parsed.strike - extra_strike) for extra_strike in root_groups[parsed.root]),
                symbol,
            )
        return (3, 10**12, symbol)

    return sorted(discovered_symbols, key=rank)


def merge_option_subscription_symbols(
    *,
    discovered_symbols: list[str],
    extra_symbols: list[str] | None,
    max_option_symbols: int,
) -> tuple[list[str], list[str]]:
    """Merge runtime-discovered contracts with forced capture symbols.

    Extra symbols are retained first so a targeted evidence-capture run cannot
    lose its explicit universe because of global subscription truncation.
    """

    notes: list[str] = []
    extras = _normalise_option_symbols(extra_symbols)
    discovered = [
        symbol
        for symbol in _normalise_option_symbols(discovered_symbols)
        if symbol not in set(extras)
    ]
    discovered = _prioritise_discovered_symbols(
        discovered_symbols=discovered,
        extra_symbols=extras,
    )
    merged = extras + discovered
    if len(merged) > max_option_symbols:
        notes.append(
            f"truncated option subscriptions from {len(merged)} to {max_option_symbols}"
        )
        if len(extras) > max_option_symbols:
            notes.append(
                f"extra option symbols alone exceeded limit: {len(extras)} > {max_option_symbols}"
            )
        merged = merged[:max_option_symbols]
    if extras:
        notes.append(f"forced extra option symbols into capture plan: {len(extras)}")
    return merged, notes


def data_feed_from_name(feed: str | None) -> DataFeed:
    normalized = str(feed or "iex").strip().lower()
    for candidate in DataFeed:
        if candidate.value == normalized:
            return candidate
    raise ValueError(f"Unsupported Alpaca stock data feed for realtime shadow: {feed}")


def option_feed_from_name(feed: str | None) -> OptionsFeed:
    normalized = str(feed or "indicative").strip().lower()
    for candidate in OptionsFeed:
        if candidate.value == normalized:
            return candidate
    raise ValueError(f"Unsupported Alpaca option data feed for realtime shadow: {feed}")


def _secret_value(value: Any, *, name: str) -> str:
    if value is None:
        raise ValueError(f"{name} is required for realtime Alpaca streaming")
    getter = getattr(value, "get_secret_value", None)
    raw = getter() if callable(getter) else str(value)
    if not raw:
        raise ValueError(f"{name} is empty")
    return str(raw)


def _coerce_event_payload(event: Any) -> dict[str, Any]:
    if isinstance(event, dict):
        return dict(event)
    if hasattr(event, "model_dump"):
        return dict(event.model_dump())
    if hasattr(event, "dict"):
        return dict(event.dict())
    payload: dict[str, Any] = {}
    for key in ("symbol", "timestamp", "price", "size", "bid_price", "ask_price", "event"):
        if hasattr(event, key):
            payload[key] = getattr(event, key)
    return payload or {"repr": repr(event)}


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _event_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def event_latency_seconds(payload: dict[str, Any], *, observed_at_utc: datetime) -> float | None:
    timestamp = payload.get("timestamp") or payload.get("t")
    event_time = _event_timestamp(timestamp)
    if event_time is None:
        return None
    return max(0.0, (observed_at_utc - event_time).total_seconds())


class JsonlEventWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._handle = self.path.open("a", encoding="utf-8", buffering=1)

    def write(self, payload: dict[str, Any]) -> None:
        line = json.dumps(_json_safe(payload), sort_keys=True)
        with self._lock:
            self._handle.write(line + "\n")
            self._handle.flush()

    def close(self) -> None:
        with self._lock:
            if self._handle.closed:
                return
            self._handle.flush()
            self._handle.close()


class RealtimeShadowMonitor:
    """No-submit realtime data monitor for comparing stream timing to the polling runner.

    This class does not submit orders. It only records SIP/OPRA/trade-update stream
    events and the bootstrap subscription universe used to observe the active book.
    """

    def __init__(
        self,
        settings: LabSettings,
        portfolio_config: MultiTickerPortfolioConfig,
        *,
        output_dir: Path,
        max_option_symbols: int = 900,
        underlyings: list[str] | None = None,
        extra_option_symbols: list[str] | None = None,
        include_stock_quotes: bool = False,
        include_option_trades: bool = False,
        include_trade_updates: bool = True,
        runtime_refresh_seconds: int = 0,
        runtime_refresh_max_total_symbols: int | None = None,
        include_session_trade_symbols: bool = False,
    ) -> None:
        self.settings = settings
        self.portfolio_config = portfolio_config
        self.output_dir = output_dir
        self.max_option_symbols = max_option_symbols
        self.underlyings = [symbol.strip().upper() for symbol in underlyings or [] if symbol.strip()]
        self.extra_option_symbols = extra_option_symbols or []
        self.include_stock_quotes = include_stock_quotes
        self.include_option_trades = include_option_trades
        self.include_trade_updates = include_trade_updates
        self.runtime_refresh_seconds = max(0, int(runtime_refresh_seconds or 0))
        self.runtime_refresh_max_total_symbols = runtime_refresh_max_total_symbols
        self.include_session_trade_symbols = bool(include_session_trade_symbols)
        self.stats = RealtimeShadowStats()
        self.writer = JsonlEventWriter(output_dir / "realtime_shadow_events.jsonl")

    def _session_trade_option_symbols(self, trade_date: date) -> list[str]:
        if not self.include_session_trade_symbols:
            return []
        state_root = self.portfolio_config.execution.state_root
        session_path = Path(state_root) / f"session_{trade_date.isoformat()}.json"
        if not session_path.exists():
            return []
        try:
            payload = json.loads(session_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        symbols: set[str] = set()
        for bucket in ("open_trades", "completed_trades"):
            rows = payload.get(bucket) or []
            if not isinstance(rows, list):
                continue
            for trade in rows:
                if not isinstance(trade, dict):
                    continue
                for leg in trade.get("legs") or []:
                    if not isinstance(leg, dict):
                        continue
                    symbol = str(leg.get("symbol") or leg.get("option_symbol") or "").strip().upper()
                    if symbol:
                        symbols.add(symbol)
        return sorted(symbols)

    def build_plan(self) -> RealtimeShadowPlan:
        trader = MultiTickerPortfolioPaperTrader(
            self.settings,
            self.portfolio_config,
            submit_paper_orders=False,
        )
        clock = trader.broker.get_clock()
        raw_trade_date = clock.get("timestamp") or _now_et().isoformat()
        trade_date = datetime.fromisoformat(str(raw_trade_date).replace("Z", "+00:00")).astimezone(
            _now_et().tzinfo
        ).date()
        stock_frames = trader._fetch_today_stock_frames(trade_date)
        option_symbols: list[str] = []
        notes: list[str] = []
        configured_underlyings = list(self.portfolio_config.execution.underlying_symbols)
        requested_underlyings = set(self.underlyings)
        selected_underlyings = (
            [symbol for symbol in configured_underlyings if symbol in requested_underlyings]
            if requested_underlyings
            else configured_underlyings
        )
        for missing in sorted(requested_underlyings.difference(configured_underlyings)):
            notes.append(f"{missing}: requested underlying is not in the portfolio config")
        for underlying_symbol in selected_underlyings:
            frame = stock_frames.get(underlying_symbol)
            if frame is None or frame.empty:
                notes.append(f"{underlying_symbol}: no stock frame available during bootstrap")
                continue
            spot_price = float(frame.iloc[-1]["close"])
            contracts = trader._refresh_contract_cache_if_needed(trade_date, underlying_symbol)
            symbols, _metadata = trader._candidate_symbols_for_snapshot(
                contracts,
                spot_price,
                trade_date,
            )
            option_symbols.extend(symbols)
        session_trade_symbols = self._session_trade_option_symbols(trade_date)
        extra_symbols = sorted(set(self.extra_option_symbols).union(session_trade_symbols))
        unique_options, merge_notes = merge_option_subscription_symbols(
            discovered_symbols=option_symbols,
            extra_symbols=extra_symbols,
            max_option_symbols=self.max_option_symbols,
        )
        notes.extend(merge_notes)
        selected_option_set = set(unique_options)
        missing_forced_symbols = [
            symbol for symbol in extra_symbols if symbol not in selected_option_set
        ]
        if missing_forced_symbols:
            notes.append(
                "missing forced option symbols after subscription merge: "
                f"{len(missing_forced_symbols)}"
            )
        if session_trade_symbols:
            notes.append(f"forced session trade leg symbols into capture plan: {len(session_trade_symbols)}")
        return RealtimeShadowPlan(
            trade_date=trade_date.isoformat(),
            underlyings=selected_underlyings,
            option_symbols=unique_options,
            forced_option_symbols=extra_symbols,
            missing_forced_option_symbols=missing_forced_symbols,
            stock_feed=(
                self.portfolio_config.execution.stock_feed or self.settings.alpaca_data_feed
            ),
            option_feed=self.portfolio_config.execution.option_feed,
            option_symbol_limit=self.max_option_symbols,
            generated_at_utc=datetime.now(UTC).isoformat(),
            notes=notes,
        )

    def write_plan(self, plan: RealtimeShadowPlan) -> Path:
        path = self.output_dir / "realtime_shadow_subscription_plan.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(plan.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return path

    def write_summary(self, plan: RealtimeShadowPlan, *, status: str) -> Path:
        path = self.output_dir / "realtime_shadow_summary.json"
        payload = {
            "status": status,
            "summary_generated_at_utc": datetime.now(UTC).isoformat(),
            "plan": plan.to_dict(),
            "stats": self.stats.to_dict(),
            "mode": "no_submit_shadow",
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _record_event(self, event_type: str, event: Any) -> None:
        observed_at = datetime.now(UTC)
        payload = _coerce_event_payload(event)
        latency = event_latency_seconds(payload, observed_at_utc=observed_at)
        self.stats.record(event_type, latency, payload=payload)
        self.writer.write(
            {
                "event_type": event_type,
                "observed_at_utc": observed_at.isoformat(),
                "latency_seconds": latency,
                "payload": payload,
            }
        )

    def _current_runtime_option_symbols(self) -> tuple[list[str], list[dict[str, str]]]:
        from scripts.build_runtime_leg_quote_capture_symbols import (
            build_runtime_leg_quote_capture_rows,
        )

        _trade_date, rows, errors = build_runtime_leg_quote_capture_rows(
            self.settings,
            self.portfolio_config,
        )
        requested_underlyings = set(self.underlyings)
        if requested_underlyings:
            rows = [
                row
                for row in rows
                if str(row.get("underlying_symbol") or "").strip().upper()
                in requested_underlyings
            ]
        symbols = sorted(
            {
                str(row.get("option_symbol") or "").strip().upper()
                for row in rows
                if str(row.get("option_symbol") or "").strip()
            }
        )
        return symbols, errors

    def _write_subscription_update(
        self,
        *,
        status: str,
        active_symbol_count: int,
        missing_symbols: list[str],
        added_symbols: list[str],
        errors: list[dict[str, str]],
        max_total_symbols: int,
    ) -> None:
        payload = {
            "status": status,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "active_symbol_count": active_symbol_count,
            "missing_symbol_count": len(missing_symbols),
            "missing_symbols": missing_symbols,
            "added_symbol_count": len(added_symbols),
            "added_symbols": added_symbols,
            "runtime_error_count": len(errors),
            "runtime_errors": errors,
            "max_total_symbols": max_total_symbols,
        }
        path = self.output_dir / "runtime_quote_subscription_updates.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
        self.writer.write({"event_type": "subscription_update", "payload": payload})

    def run_stream(self, plan: RealtimeShadowPlan, *, duration_seconds: int) -> Path:
        api_key = _secret_value(self.settings.alpaca_api_key, name="alpaca_api_key")
        secret_key = _secret_value(self.settings.alpaca_secret_key, name="alpaca_secret_key")
        stock_stream = StockDataStream(
            api_key,
            secret_key,
            raw_data=False,
            feed=data_feed_from_name(plan.stock_feed),
        )
        option_stream = OptionDataStream(
            api_key,
            secret_key,
            raw_data=False,
            feed=option_feed_from_name(plan.option_feed),
        )
        trading_stream = TradingStream(api_key, secret_key, paper=True, raw_data=False)

        async def on_stock_bar(event: Any) -> None:
            self._record_event("stock_bar", event)

        async def on_stock_updated_bar(event: Any) -> None:
            self._record_event("stock_updated_bar", event)

        async def on_stock_quote(event: Any) -> None:
            self._record_event("stock_quote", event)

        async def on_option_quote(event: Any) -> None:
            self._record_event("option_quote", event)

        async def on_option_trade(event: Any) -> None:
            self._record_event("option_trade", event)

        async def on_trade_update(event: Any) -> None:
            self._record_event("trade_update", event)

        stock_stream.subscribe_bars(on_stock_bar, *plan.underlyings)
        stock_stream.subscribe_updated_bars(on_stock_updated_bar, *plan.underlyings)
        if self.include_stock_quotes:
            stock_stream.subscribe_quotes(on_stock_quote, *plan.underlyings)
        if plan.option_symbols:
            option_stream.subscribe_quotes(on_option_quote, *plan.option_symbols)
            if self.include_option_trades:
                option_stream.subscribe_trades(on_option_trade, *plan.option_symbols)
        if self.include_trade_updates:
            trading_stream.subscribe_trade_updates(on_trade_update)

        streams = [stock_stream, option_stream]
        if self.include_trade_updates:
            streams.append(trading_stream)
        threads = [
            threading.Thread(target=stream.run, name=f"realtime-shadow-{index}", daemon=True)
            for index, stream in enumerate(streams, start=1)
        ]
        for thread in threads:
            thread.start()
        started_at = time.monotonic()
        active_plan = plan
        active_option_symbols = set(plan.option_symbols)
        refresh_interval = self.runtime_refresh_seconds
        refresh_max_total = int(
            self.runtime_refresh_max_total_symbols
            or max(self.max_option_symbols, len(active_option_symbols))
        )
        next_refresh_at = started_at + refresh_interval if refresh_interval > 0 else None
        status = "completed"
        try:
            while time.monotonic() - started_at < duration_seconds:
                if next_refresh_at is not None and time.monotonic() >= next_refresh_at:
                    try:
                        runtime_symbols, runtime_errors = self._current_runtime_option_symbols()
                        session_symbols = self._session_trade_option_symbols(
                            date.fromisoformat(active_plan.trade_date)
                        )
                        refresh_symbols = sorted(set(runtime_symbols).union(session_symbols))
                        missing_symbols = [
                            symbol
                            for symbol in refresh_symbols
                            if symbol not in active_option_symbols
                        ]
                        available_slots = max(0, refresh_max_total - len(active_option_symbols))
                        added_symbols = missing_symbols[:available_slots]
                        if added_symbols:
                            option_stream.subscribe_quotes(on_option_quote, *added_symbols)
                            active_option_symbols.update(added_symbols)
                            active_plan = replace(
                                active_plan,
                                option_symbols=sorted(active_option_symbols),
                                option_symbol_limit=max(
                                    active_plan.option_symbol_limit,
                                    len(active_option_symbols),
                                ),
                                notes=[
                                    *active_plan.notes,
                                    (
                                        "runtime refresh added "
                                        f"{len(added_symbols)} OPRA symbols at "
                                        f"{datetime.now(UTC).isoformat()}"
                                    ),
                                ],
                            )
                            self.write_plan(active_plan)
                        refresh_status = (
                            "runtime_refresh_added_symbols"
                            if added_symbols
                            else "runtime_refresh_no_new_symbols"
                        )
                        if missing_symbols and not added_symbols and available_slots <= 0:
                            refresh_status = "runtime_refresh_symbol_cap_reached"
                        self._write_subscription_update(
                            status=refresh_status,
                            active_symbol_count=len(active_option_symbols),
                            missing_symbols=missing_symbols,
                            added_symbols=added_symbols,
                            errors=runtime_errors,
                            max_total_symbols=refresh_max_total,
                        )
                    except Exception as exc:  # noqa: BLE001 - shadow capture should keep running.
                        self._write_subscription_update(
                            status=f"runtime_refresh_failed:{exc!r}",
                            active_symbol_count=len(active_option_symbols),
                            missing_symbols=[],
                            added_symbols=[],
                            errors=[],
                            max_total_symbols=refresh_max_total,
                        )
                    next_refresh_at = time.monotonic() + refresh_interval
                time.sleep(min(1.0, max(0.1, duration_seconds / 30)))
        except KeyboardInterrupt:
            status = "interrupted"
        finally:
            for stream in streams:
                try:
                    stream.stop()
                except Exception:  # noqa: BLE001
                    try:
                        stream.stop_ws()
                    except Exception:  # noqa: BLE001
                        pass
            self.writer.close()
        return self.write_summary(active_plan, status=status)

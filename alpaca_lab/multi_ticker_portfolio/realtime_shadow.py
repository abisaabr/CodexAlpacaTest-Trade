from __future__ import annotations

import json
import threading
import time
from decimal import Decimal
from dataclasses import asdict, dataclass, field
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


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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

    def write(self, payload: dict[str, Any]) -> None:
        line = json.dumps(_json_safe(payload), sort_keys=True)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")


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
        include_stock_quotes: bool = False,
        include_option_trades: bool = False,
        include_trade_updates: bool = True,
    ) -> None:
        self.settings = settings
        self.portfolio_config = portfolio_config
        self.output_dir = output_dir
        self.max_option_symbols = max_option_symbols
        self.include_stock_quotes = include_stock_quotes
        self.include_option_trades = include_option_trades
        self.include_trade_updates = include_trade_updates
        self.stats = RealtimeShadowStats()
        self.writer = JsonlEventWriter(output_dir / "realtime_shadow_events.jsonl")

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
        for underlying_symbol in self.portfolio_config.execution.underlying_symbols:
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
        unique_options = sorted(set(option_symbols))
        if len(unique_options) > self.max_option_symbols:
            notes.append(
                f"truncated option subscriptions from {len(unique_options)} to {self.max_option_symbols}"
            )
            unique_options = unique_options[: self.max_option_symbols]
        return RealtimeShadowPlan(
            trade_date=trade_date.isoformat(),
            underlyings=list(self.portfolio_config.execution.underlying_symbols),
            option_symbols=unique_options,
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
        status = "completed"
        try:
            while time.monotonic() - started_at < duration_seconds:
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
        return self.write_summary(plan, status=status)

from __future__ import annotations

import json
import math
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter, OrderRequest
from alpaca_lab.config import LabSettings
from alpaca_lab.logging_utils import get_logger
from alpaca_lab.multi_ticker_portfolio.config import MultiTickerPortfolioConfig, StrategyConfig
from alpaca_lab.multi_ticker_portfolio.signals import (
    build_stock_frame,
    infer_symbol_regime,
    signal_is_true,
)
from alpaca_lab.notifications import DiscordWebhookNotifier, EmailNotifier, NtfyNotifier
from alpaca_lab.qqq_portfolio.greeks import bs_greeks, implied_volatility
from alpaca_lab.reporting import append_journal_entry, write_alert_queue, write_summary_bundle

ET = ZoneInfo("America/New_York")
OPEN_STATUSES = {"accepted", "new", "partially_filled", "pending_new", "accepted_for_bidding"}
TERMINAL_STATUSES = {"filled", "canceled", "expired", "done_for_day", "rejected"}
CONTRACT_MULTIPLIER = 100.0
ENTRY_COMMISSION_PER_CONTRACT = 0.65
EXIT_COMMISSION_PER_CONTRACT = 0.65


@dataclass(slots=True)
class SelectedLeg:
    symbol: str
    expiration_date: str
    option_type: str
    side: str
    strike_price: float
    target_delta: float
    bid: float
    ask: float
    mark: float
    delta: float
    gamma: float
    theta: float
    vega: float
    quote_time: str | None


@dataclass(slots=True)
class OpenTrade:
    strategy_name: str
    underlying_symbol: str
    regime: str
    quantity: int
    entry_time_et: str
    entry_minute: int
    hard_exit_minute: int
    underlying_entry: float
    entry_debit: float
    max_loss_per_combo: float
    max_profit_per_combo: float
    profit_target_dollars: float
    stop_loss_dollars: float
    entry_order_id: str | None
    entry_fill_price: float
    legs: list[dict[str, Any]]
    entry_attempt_id: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CompletedTrade:
    strategy_name: str
    underlying_symbol: str
    regime: str
    quantity: int
    entry_time_et: str
    exit_time_et: str
    entry_minute: int
    exit_minute: int
    entry_fill_price: float
    exit_fill_price: float
    underlying_entry: float
    underlying_exit: float
    exit_reason: str
    entry_order_id: str | None
    exit_order_id: str | None
    net_pnl: float
    max_loss_per_combo: float
    max_profit_per_combo: float
    delta_shares_at_entry: float
    vega_dollars_1pct_at_entry: float
    legs: list[dict[str, Any]]
    entry_attempt_id: str | None = None


@dataclass(slots=True)
class PortfolioLedger:
    realized_equity: float
    high_watermark: float
    closed_days: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class SessionState:
    trade_date: str
    starting_equity: float
    virtual_cash: float
    blocked_new_entries: bool = False
    block_reason: str | None = None
    signals_fired: list[str] = field(default_factory=list)
    open_trades: list[dict[str, Any]] = field(default_factory=list)
    completed_trades: list[dict[str, Any]] = field(default_factory=list)
    alerts: list[dict[str, Any]] = field(default_factory=list)
    last_symbol_regimes: dict[str, str] = field(default_factory=dict)
    startup_check_status: str = "pending"
    startup_check_details: dict[str, Any] = field(default_factory=dict)
    notified_morning: bool = False
    notified_midday: bool = False
    notified_end_of_day: bool = False
    last_updated_at: str | None = None


@dataclass(slots=True)
class SymbolSnapshot:
    underlying_symbol: str
    trade_date: date
    stock_frame: pd.DataFrame
    option_chain: pd.DataFrame
    mark_map: dict[str, float]
    latest_close: float
    current_minute: int
    latest_timestamp_et: datetime | None


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _now_et() -> datetime:
    return datetime.now(UTC).astimezone(ET)


def _rth_open_for(day: date) -> datetime:
    return datetime.combine(day, dt_time(9, 30), tzinfo=ET)


def _trade_date_from_clock(clock: dict[str, Any]) -> date:
    if clock.get("timestamp"):
        return (
            datetime.fromisoformat(str(clock["timestamp"]).replace("Z", "+00:00"))
            .astimezone(ET)
            .date()
        )
    return _now_et().date()


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _position_mark_cashflow(legs: list[dict[str, Any]], mark_map: dict[str, float]) -> float:
    cashflow = 0.0
    for leg in legs:
        mark = mark_map.get(str(leg["symbol"]))
        if mark is None:
            raise KeyError(str(leg["symbol"]))
        if leg["side"] == "long":
            cashflow += mark * CONTRACT_MULTIPLIER
        else:
            cashflow -= mark * CONTRACT_MULTIPLIER
    return cashflow


def _option_intrinsic(option_type: str, strike_price: float, spot: float) -> float:
    if option_type == "call":
        return max(spot - strike_price, 0.0)
    return max(strike_price - spot, 0.0)


def _combo_payoff_at_expiry(legs: list[dict[str, Any]], spot: float) -> float:
    payoff = 0.0
    for leg in legs:
        intrinsic = _option_intrinsic(str(leg["option_type"]), float(leg["strike_price"]), spot)
        entry_price = float(leg["entry_fill_price"])
        if leg["side"] == "long":
            payoff += (intrinsic - entry_price) * CONTRACT_MULTIPLIER
        else:
            payoff += (entry_price - intrinsic) * CONTRACT_MULTIPLIER
    return payoff


def _estimate_combo_bounds(legs: list[dict[str, Any]]) -> tuple[float, float]:
    strikes = [float(leg["strike_price"]) for leg in legs]
    if not strikes:
        return 0.0, 0.0
    lower = min(strikes)
    upper = max(strikes)
    span = max(1.0, upper - lower)
    candidates = {
        0.01,
        max(0.01, lower - 2.0 * span),
        max(0.01, lower - span),
        lower,
        (lower + upper) / 2.0,
        upper,
        upper + span,
        upper + 2.0 * span,
    }
    pnl_values = [_combo_payoff_at_expiry(legs, spot) for spot in sorted(candidates)]
    return max(0.01, -min(pnl_values)), max(pnl_values)


def _entry_cashflow_from_debit(entry_debit: float, quantity: int, leg_count: int) -> float:
    gross = -entry_debit * CONTRACT_MULTIPLIER * quantity
    commission = ENTRY_COMMISSION_PER_CONTRACT * leg_count * quantity
    return gross - commission


def _exit_cashflow_from_fill(*, fill_price: float, quantity: int, leg_count: int) -> float:
    gross = fill_price * CONTRACT_MULTIPLIER * quantity
    commission = EXIT_COMMISSION_PER_CONTRACT * leg_count * quantity
    return gross - commission


def _current_equity(state: SessionState, mark_map: dict[str, float] | None = None) -> float:
    equity = state.virtual_cash
    mark_map = mark_map or {}
    for trade_payload in state.open_trades:
        try:
            mark_cashflow = _position_mark_cashflow(trade_payload["legs"], mark_map)
        except KeyError:
            continue
        commission = EXIT_COMMISSION_PER_CONTRACT * len(trade_payload["legs"]) * int(
            trade_payload["quantity"]
        )
        equity += mark_cashflow * int(trade_payload["quantity"]) - commission
    return equity


def _format_signed_dollars(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.2f}"


class MultiTickerPortfolioPaperTrader:
    def __init__(
        self,
        settings: LabSettings,
        portfolio_config: MultiTickerPortfolioConfig,
        *,
        broker: AlpacaBrokerAdapter | None = None,
        submit_paper_orders: bool | None = None,
    ) -> None:
        self.settings = settings
        self.portfolio_config = portfolio_config
        self.submit_paper_orders = (
            portfolio_config.execution.submit_paper_orders
            if submit_paper_orders is None
            else submit_paper_orders
        )
        self.broker = broker or AlpacaBrokerAdapter(settings, dry_run=not self.submit_paper_orders)
        self.ntfy_notifier = NtfyNotifier(settings)
        self.discord_notifier = DiscordWebhookNotifier(settings)
        self.email_notifier = EmailNotifier(settings)
        self.logger = get_logger("multi_ticker_portfolio")
        self._failed_notification_phases: set[str] = set()
        self.state_root = portfolio_config.execution.state_root
        self.run_root = portfolio_config.execution.run_root
        self.ledger_path = self.state_root / "ledger.json"
        self.contract_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.contract_cache_loaded_at: dict[str, datetime] = {}
        self.underlyings = list(portfolio_config.execution.underlying_symbols)

    def load_ledger(self) -> PortfolioLedger:
        payload = _read_json(
            self.ledger_path,
            {
                "realized_equity": self.portfolio_config.risk.sleeve_starting_equity,
                "high_watermark": self.portfolio_config.risk.sleeve_starting_equity,
                "closed_days": [],
            },
        )
        return PortfolioLedger(
            realized_equity=float(payload["realized_equity"]),
            high_watermark=float(payload["high_watermark"]),
            closed_days=list(payload.get("closed_days", [])),
        )

    def save_ledger(self, ledger: PortfolioLedger) -> Path:
        return _write_json(self.ledger_path, asdict(ledger))

    def session_path(self, trade_date: date) -> Path:
        return self.state_root / f"session_{trade_date.isoformat()}.json"

    def load_or_create_session(self, trade_date: date, ledger: PortfolioLedger) -> SessionState:
        path = self.session_path(trade_date)
        if path.exists():
            payload = _read_json(path, {})
            return SessionState(
                trade_date=str(payload["trade_date"]),
                starting_equity=float(payload["starting_equity"]),
                virtual_cash=float(payload["virtual_cash"]),
                blocked_new_entries=bool(payload.get("blocked_new_entries", False)),
                block_reason=payload.get("block_reason"),
                signals_fired=list(payload.get("signals_fired", [])),
                open_trades=list(payload.get("open_trades", [])),
                completed_trades=list(payload.get("completed_trades", [])),
                alerts=list(payload.get("alerts", [])),
                last_symbol_regimes=dict(payload.get("last_symbol_regimes", {})),
                startup_check_status=str(payload.get("startup_check_status", "pending")),
                startup_check_details=dict(payload.get("startup_check_details", {})),
                notified_morning=bool(payload.get("notified_morning", False)),
                notified_midday=bool(payload.get("notified_midday", False)),
                notified_end_of_day=bool(payload.get("notified_end_of_day", False)),
                last_updated_at=payload.get("last_updated_at"),
            )
        return SessionState(
            trade_date=trade_date.isoformat(),
            starting_equity=ledger.realized_equity,
            virtual_cash=ledger.realized_equity,
        )

    def save_session(self, session: SessionState) -> Path:
        session.last_updated_at = _now_et().isoformat()
        return _write_json(self.session_path(date.fromisoformat(session.trade_date)), asdict(session))

    def _session_run_dir(self, trade_date: date) -> Path:
        run_dir = self.run_root / trade_date.isoformat()
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def _trade_reconciliation_events_path(self, trade_date: date) -> Path:
        return self._session_run_dir(trade_date) / "trade_reconciliation_events.json"

    def _append_trade_event(self, trade_date: date, event: dict[str, Any]) -> Path:
        payload = {"timestamp_et": _now_et().isoformat(), **event}
        return append_journal_entry(self._trade_reconciliation_events_path(trade_date), payload)

    def _serialize_order_request(self, request: OrderRequest) -> dict[str, Any]:
        payload = request.to_payload()
        return {
            "symbol": request.symbol,
            "side": request.side,
            "qty": request.qty,
            "notional": request.notional,
            "order_type": request.order_type,
            "time_in_force": request.time_in_force,
            "limit_price": request.limit_price,
            "stop_price": request.stop_price,
            "client_order_id": request.client_order_id,
            "asset_class": request.asset_class,
            "strategy_name": request.strategy_name,
            "legs": payload.get("legs"),
            "extra": request.extra,
        }

    def _event_base_for_trade(self, trade: OpenTrade, *, phase: str) -> dict[str, Any]:
        return {
            "attempt_id": trade.entry_attempt_id,
            "trade_date": date.fromisoformat(trade.entry_time_et[:10]).isoformat(),
            "strategy_name": trade.strategy_name,
            "underlying_symbol": trade.underlying_symbol,
            "regime": trade.regime,
            "phase": phase,
            "entry_minute": int(trade.entry_minute),
            "quantity": int(trade.quantity),
        }

    def _notify_lines(self, *lines: object) -> bool:
        if not self.submit_paper_orders:
            return False
        delivered = False
        for notifier in (
            getattr(self, "ntfy_notifier", None),
            getattr(self, "email_notifier", None),
            getattr(self, "discord_notifier", None),
        ):
            if notifier is None or not getattr(notifier, "enabled", False):
                continue
            delivered = notifier.send_lines(*lines) or delivered
        return delivered

    def _open_positions_by_ticker_line(self, session: SessionState) -> str:
        counts = Counter(str(trade["underlying_symbol"]) for trade in session.open_trades)
        if not counts:
            return "Open positions by ticker: none"
        summary = ", ".join(f"{symbol} x{counts[symbol]}" for symbol in sorted(counts))
        return f"Open positions by ticker: {summary}"

    def _strategy_pnl_summary_lines(
        self,
        session: SessionState,
        *,
        limit: int = 3,
    ) -> list[str]:
        if not session.completed_trades:
            return ["Strategy day PnL: no closed trades yet"]

        totals: dict[str, float] = {}
        for trade in session.completed_trades:
            strategy_name = str(trade.get("strategy_name", "unknown"))
            totals[strategy_name] = totals.get(strategy_name, 0.0) + float(trade.get("net_pnl", 0.0))

        winners = [
            (name, pnl)
            for name, pnl in sorted(totals.items(), key=lambda item: (-item[1], item[0]))
            if pnl > 0
        ][:limit]
        losers = [
            (name, pnl)
            for name, pnl in sorted(totals.items(), key=lambda item: (item[1], item[0]))
            if pnl < 0
        ][:limit]

        lines: list[str] = []
        if winners:
            winner_summary = "; ".join(
                f"{name} {_format_signed_dollars(pnl)}" for name, pnl in winners
            )
            lines.append(f"Top strategy PnL: {winner_summary}")
        if losers:
            loser_summary = "; ".join(
                f"{name} {_format_signed_dollars(pnl)}" for name, pnl in losers
            )
            lines.append(f"Lagging strategies: {loser_summary}")
        if not lines:
            lines.append("Strategy day PnL: flat so far")
        return lines

    def _build_morning_notification_lines(
        self,
        session: SessionState,
        details: dict[str, Any],
    ) -> list[object]:
        return [
            "**Multi-Ticker Portfolio Morning Check**",
            f"Trade date: {session.trade_date}",
            f"Buying power: ${float(details.get('buying_power', 0.0)):,.2f}",
            f"Required buying power: ${float(details.get('required_buying_power', 0.0)):,.2f}",
            f"Strategies loaded: {len(self.portfolio_config.strategies)} across {len(self.underlyings)} tickers",
            self._open_positions_by_ticker_line(session),
            "Startup check passed. Paper trader is live for RTH.",
        ]

    def _build_midday_notification_lines(
        self,
        session: SessionState,
        *,
        current_equity: float,
    ) -> list[object]:
        day_pnl = current_equity - session.starting_equity
        open_symbols = sorted({trade["underlying_symbol"] for trade in session.open_trades})
        return [
            "**Multi-Ticker Portfolio Midday Update**",
            f"Trade date: {session.trade_date}",
            f"Current equity: ${current_equity:,.2f}",
            f"Day PnL: {_format_signed_dollars(day_pnl)}",
            f"Completed trades: {len(session.completed_trades)}",
            f"Open trades: {len(session.open_trades)}",
            f"Active symbols: {', '.join(open_symbols) if open_symbols else 'none'}",
            self._open_positions_by_ticker_line(session),
            *self._strategy_pnl_summary_lines(session),
        ]

    def _build_end_of_day_notification_lines(
        self,
        session: SessionState,
        *,
        ending_equity: float,
    ) -> list[object]:
        return [
            "**Multi-Ticker Portfolio End Of Day**",
            f"Trade date: {session.trade_date}",
            f"Ending equity: ${ending_equity:,.2f}",
            f"Day PnL: {_format_signed_dollars(ending_equity - session.starting_equity)}",
            f"Completed trades: {len(session.completed_trades)}",
            f"Blocked new entries: {'yes' if session.blocked_new_entries else 'no'}",
            self._open_positions_by_ticker_line(session),
            *self._strategy_pnl_summary_lines(session),
        ]

    def _record_notification_failure(self, session: SessionState, phase: str) -> None:
        failed_phases = getattr(self, "_failed_notification_phases", None)
        if failed_phases is None:
            failed_phases = set()
            self._failed_notification_phases = failed_phases
        failed_phases.add(phase)
        message = f"{phase.title()} notification delivery failed"
        if any(alert.get("message") == message for alert in session.alerts):
            return
        self._alert(session, "warning", message)

    def _fetch_today_stock_frames(self, trade_date: date) -> dict[str, pd.DataFrame]:
        start = _rth_open_for(trade_date).astimezone(UTC)
        end = _now_et().astimezone(UTC)
        if end <= start:
            return {symbol: build_stock_frame([]) for symbol in self.underlyings}
        payload = self.broker.get_stock_bars(
            self.underlyings,
            start=start,
            end=end,
            timeframe="1Min",
            feed=self.portfolio_config.execution.stock_feed or self.settings.alpaca_data_feed,
            limit=10000,
        )
        frames: dict[str, pd.DataFrame] = {}
        for symbol in self.underlyings:
            rows = payload.get("bars", {}).get(symbol, [])
            frames[symbol] = build_stock_frame(rows)
        return frames

    def _refresh_contract_cache_if_needed(
        self,
        trade_date: date,
        underlying_symbol: str,
    ) -> list[dict[str, Any]]:
        loaded_at = self.contract_cache_loaded_at.get(underlying_symbol)
        stale = (
            loaded_at is None
            or (_now_et() - loaded_at).total_seconds()
            >= self.portfolio_config.execution.contract_refresh_minutes * 60
        )
        cache_key = (underlying_symbol, trade_date.isoformat())
        if stale or cache_key not in self.contract_cache:
            expiration_lte = trade_date + timedelta(days=self.portfolio_config.execution.max_dte_days)
            payload = self.broker.get_option_contracts(
                [underlying_symbol],
                expiration_date_gte=trade_date.isoformat(),
                expiration_date_lte=expiration_lte.isoformat(),
                status="active",
                limit=1000,
            )
            contracts: list[dict[str, Any]] = []
            for contract in payload.get("option_contracts", []):
                expiration_date = date.fromisoformat(str(contract["expiration_date"]))
                contracts.append(
                    {
                        "symbol": str(contract["symbol"]),
                        "underlying_symbol": underlying_symbol,
                        "expiration_date": expiration_date,
                        "strike_price": float(contract["strike_price"]),
                        "option_type": str(contract["type"]),
                    }
                )
            self.contract_cache[cache_key] = contracts
            self.contract_cache_loaded_at[underlying_symbol] = _now_et()
        return self.contract_cache[cache_key]

    def _candidate_symbols_for_snapshot(
        self,
        contracts: list[dict[str, Any]],
        spot_price: float,
        trade_date: date,
    ) -> tuple[list[str], dict[str, dict[str, Any]]]:
        if not contracts:
            return [], {}
        same_day_expiry = trade_date
        next_expiry_candidates = sorted(
            {contract["expiration_date"] for contract in contracts if contract["expiration_date"] > trade_date}
        )
        next_expiry = next_expiry_candidates[0] if next_expiry_candidates else None
        keep: list[dict[str, Any]] = []
        for expiry in [same_day_expiry, next_expiry]:
            if expiry is None:
                continue
            for option_type in ("call", "put"):
                subset = [
                    contract
                    for contract in contracts
                    if contract["expiration_date"] == expiry and contract["option_type"] == option_type
                ]
                subset = sorted(subset, key=lambda item: abs(item["strike_price"] - spot_price))
                keep.extend(subset[:14])
        metadata = {contract["symbol"]: contract for contract in keep}
        return [contract["symbol"] for contract in keep], metadata

    def _mark_from_snapshot(
        self,
        snapshot: dict[str, Any],
    ) -> tuple[float | None, float | None, float | None, str | None]:
        latest_quote = snapshot.get("latestQuote", {}) or {}
        latest_trade = snapshot.get("latestTrade", {}) or {}
        minute_bar = snapshot.get("minuteBar", {}) or {}
        bid = latest_quote.get("bp")
        ask = latest_quote.get("ap")
        quote_time = latest_quote.get("t")
        bid_value = float(bid) if bid not in (None, "") else None
        ask_value = float(ask) if ask not in (None, "") else None
        if bid_value is not None and ask_value is not None and ask_value >= bid_value > 0.0:
            return bid_value, ask_value, (bid_value + ask_value) / 2.0, quote_time
        trade_price = latest_trade.get("p")
        if trade_price not in (None, ""):
            price = float(trade_price)
            return price, price, price, latest_trade.get("t")
        bar_close = minute_bar.get("c")
        if bar_close not in (None, ""):
            price = float(bar_close)
            return price, price, price, minute_bar.get("t")
        return None, None, None, None

    def _fetch_option_chain(
        self,
        symbols: list[str],
        metadata: dict[str, dict[str, Any]],
        *,
        spot_price: float,
        trade_date: date,
        underlying_symbol: str,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        rows: list[dict[str, Any]] = []
        now_et = _now_et()
        for batch in _chunked(symbols, 50):
            payload = self.broker.get_option_snapshots(batch)
            for symbol, snapshot in payload.get("snapshots", {}).items():
                info = metadata.get(symbol)
                if info is None:
                    continue
                bid, ask, mark, quote_time = self._mark_from_snapshot(snapshot)
                if mark is None or mark <= 0.0:
                    continue
                expiration_date = info["expiration_date"]
                expiration_ts = datetime.combine(expiration_date, dt_time(16, 0), tzinfo=ET)
                years_to_expiry = max(60.0, (expiration_ts - now_et).total_seconds()) / (
                    365.0 * 24.0 * 3600.0
                )
                iv = implied_volatility(
                    spot=spot_price,
                    strike=float(info["strike_price"]),
                    years=years_to_expiry,
                    market_price=mark,
                    option_type=str(info["option_type"]),
                )
                if iv is None:
                    continue
                greeks = bs_greeks(
                    spot=spot_price,
                    strike=float(info["strike_price"]),
                    years=years_to_expiry,
                    sigma=iv,
                    option_type=str(info["option_type"]),
                )
                spread_pct = 0.0
                if bid is not None and ask is not None and ask > 0.0:
                    spread_pct = max(0.0, (ask - bid) / max(mark, 0.01))
                freshness_seconds = None
                if quote_time:
                    freshness_seconds = max(
                        0.0,
                        (
                            now_et
                            - datetime.fromisoformat(str(quote_time).replace("Z", "+00:00")).astimezone(ET)
                        ).total_seconds(),
                    )
                rows.append(
                    {
                        "symbol": symbol,
                        "underlying_symbol": underlying_symbol,
                        "expiration_date": expiration_date,
                        "dte": max(0, (expiration_date - trade_date).days),
                        "option_type": str(info["option_type"]),
                        "strike_price": float(info["strike_price"]),
                        "bid": float(bid if bid is not None else mark),
                        "ask": float(ask if ask is not None else mark),
                        "mark": float(mark),
                        "delta": float(greeks["delta"]),
                        "gamma": float(greeks["gamma"]),
                        "theta": float(greeks["theta"]),
                        "vega": float(greeks["vega"]),
                        "implied_vol": float(iv),
                        "spread_pct": float(spread_pct),
                        "freshness_seconds": freshness_seconds,
                        "quote_time": quote_time,
                    }
                )
        return pd.DataFrame(rows)

    def _select_legs(
        self,
        strategy: StrategyConfig,
        option_chain: pd.DataFrame,
        trade_date: date,
    ) -> list[SelectedLeg]:
        if option_chain.empty:
            return []
        if strategy.dte_mode == "same_day":
            expiry = trade_date
        else:
            future_dates = sorted(
                value for value in option_chain["expiration_date"].unique().tolist() if value > trade_date
            )
            if not future_dates:
                return []
            expiry = future_dates[0]
        used_symbols: set[str] = set()
        legs: list[SelectedLeg] = []
        for leg_template in strategy.legs:
            subset = option_chain[
                (option_chain["expiration_date"] == expiry)
                & (option_chain["option_type"] == leg_template.option_type)
                & (option_chain["mark"] > 0.0)
                & (option_chain["spread_pct"] <= self.portfolio_config.execution.max_relative_spread)
            ].copy()
            if subset.empty:
                return []
            subset = subset[~subset["symbol"].isin(used_symbols)].copy()
            subset = subset[
                (subset["delta"].abs() >= leg_template.min_abs_delta)
                & (subset["delta"].abs() <= leg_template.max_abs_delta)
            ].copy()
            if subset.empty:
                return []
            subset = subset[
                subset["freshness_seconds"].isna()
                | (subset["freshness_seconds"] <= self.portfolio_config.execution.quote_stale_seconds)
            ].copy()
            if subset.empty:
                return []
            subset["delta_distance"] = (subset["delta"] - leg_template.target_delta).abs()
            subset = subset.sort_values(["delta_distance", "spread_pct", "mark"], ascending=[True, True, False])
            chosen = subset.iloc[0]
            selected_leg = SelectedLeg(
                symbol=str(chosen["symbol"]),
                expiration_date=str(chosen["expiration_date"]),
                option_type=str(chosen["option_type"]),
                side=leg_template.side,
                strike_price=float(chosen["strike_price"]),
                target_delta=float(leg_template.target_delta),
                bid=float(chosen["bid"]),
                ask=float(chosen["ask"]),
                mark=float(chosen["mark"]),
                delta=float(chosen["delta"]),
                gamma=float(chosen["gamma"]),
                theta=float(chosen["theta"]),
                vega=float(chosen["vega"]),
                quote_time=chosen["quote_time"] if pd.notna(chosen["quote_time"]) else None,
            )
            legs.append(selected_leg)
            used_symbols.add(selected_leg.symbol)
        return legs

    def _effective_risk_scale(self, ledger: PortfolioLedger, current_equity: float) -> float:
        high_watermark = max(ledger.high_watermark, current_equity)
        drawdown_pct = 0.0
        if high_watermark > 0.0:
            drawdown_pct = (1.0 - current_equity / high_watermark) * 100.0
        if drawdown_pct >= self.portfolio_config.risk.delever_drawdown_pct:
            return self.portfolio_config.risk.delever_risk_scale
        return 1.0

    def _regime_position_count(self, session: SessionState, regime: str) -> int:
        return sum(1 for trade in session.open_trades if trade["regime"] == regime)

    def _symbol_position_count(self, session: SessionState, underlying_symbol: str) -> int:
        return sum(1 for trade in session.open_trades if trade["underlying_symbol"] == underlying_symbol)

    def _daily_loss_gate_check(self, session: SessionState, current_equity: float) -> tuple[bool, str | None]:
        gate_pct = self.portfolio_config.risk.daily_loss_gate_pct
        if gate_pct is None or gate_pct <= 0.0:
            return False, None
        threshold = session.starting_equity * (1.0 - gate_pct)
        if current_equity <= threshold:
            return True, f"daily_loss_gate triggered at equity {current_equity:.2f}"
        return False, None

    def _mark_to_close(self, open_trade: OpenTrade, option_chain: pd.DataFrame) -> dict[str, float]:
        mark_map: dict[str, float] = {}
        for leg in open_trade.legs:
            subset = option_chain[option_chain["symbol"] == leg["symbol"]]
            if subset.empty:
                continue
            mark_map[str(leg["symbol"])] = float(subset.iloc[0]["mark"])
        return mark_map

    def _evaluate_entry(
        self,
        *,
        strategy: StrategyConfig,
        session: SessionState,
        ledger: PortfolioLedger,
        option_chain: pd.DataFrame,
        spot_price: float,
        current_minute: int,
        current_equity: float,
        attempt_id: str,
    ) -> tuple[OpenTrade | None, dict[str, Any]]:
        event: dict[str, Any] = {
            "event_type": "signal_decision",
            "attempt_id": attempt_id,
            "trade_date": session.trade_date,
            "strategy_name": strategy.name,
            "underlying_symbol": strategy.underlying_symbol,
            "regime": strategy.regime,
            "signal_name": strategy.signal_name,
            "timing_profile": strategy.timing_profile,
            "current_minute": int(current_minute),
            "current_equity": round(float(current_equity), 4),
            "decision": "skipped",
            "decision_reason": None,
        }
        if strategy.name in session.signals_fired:
            event["decision_reason"] = "duplicate_signal"
            return None, event
        if len(session.open_trades) >= self.portfolio_config.risk.max_open_positions:
            event["decision_reason"] = "max_open_positions"
            return None, event
        if self._regime_position_count(session, strategy.regime) >= self.portfolio_config.risk.max_positions_per_regime:
            event["decision_reason"] = "max_positions_per_regime"
            return None, event
        if self._symbol_position_count(session, strategy.underlying_symbol) >= self.portfolio_config.risk.max_positions_per_symbol:
            event["decision_reason"] = "max_positions_per_symbol"
            return None, event
        if any(trade["strategy_name"] == strategy.name for trade in session.open_trades):
            event["decision_reason"] = "strategy_already_open"
            return None, event
        legs = self._select_legs(strategy, option_chain, date.fromisoformat(session.trade_date))
        if not legs:
            event["decision_reason"] = "no_eligible_legs"
            return None, event
        entry_debit = sum(leg.mark if leg.side == "long" else -leg.mark for leg in legs)
        leg_payloads = [
            {
                "symbol": leg.symbol,
                "expiration_date": leg.expiration_date,
                "option_type": leg.option_type,
                "side": leg.side,
                "strike_price": leg.strike_price,
                "target_delta": leg.target_delta,
                "entry_fill_price": leg.mark,
                "bid": leg.bid,
                "ask": leg.ask,
                "mark": leg.mark,
                "delta": leg.delta,
                "gamma": leg.gamma,
                "theta": leg.theta,
                "vega": leg.vega,
            }
            for leg in legs
        ]
        max_loss_per_combo, max_profit_per_combo = _estimate_combo_bounds(leg_payloads)
        if max_loss_per_combo <= 0.0:
            event["decision_reason"] = "invalid_max_loss"
            return None, event
        risk_scale = self._effective_risk_scale(ledger, current_equity)
        reserved_risk = sum(
            float(trade["max_loss_per_combo"]) * int(trade["quantity"]) for trade in session.open_trades
        )
        remaining_risk = max(
            0.0,
            current_equity * self.portfolio_config.risk.max_open_risk_fraction * risk_scale - reserved_risk,
        )
        per_trade_budget = current_equity * strategy.risk_fraction * risk_scale
        allocatable_risk = min(remaining_risk, per_trade_budget)
        quantity_by_risk = math.floor(allocatable_risk / max_loss_per_combo)
        if quantity_by_risk < 1:
            event["decision_reason"] = "risk_budget_too_small"
            event["risk_scale"] = round(risk_scale, 6)
            event["reserved_risk"] = round(reserved_risk, 4)
            event["remaining_risk"] = round(remaining_risk, 4)
            event["per_trade_budget"] = round(per_trade_budget, 4)
            return None, event
        debit_cash = max(0.0, entry_debit * CONTRACT_MULTIPLIER)
        quantity_by_cash = (
            math.floor(max(0.0, session.virtual_cash) / debit_cash)
            if debit_cash > 0.0
            else strategy.max_contracts
        )
        quantity = min(strategy.max_contracts, quantity_by_risk, quantity_by_cash)
        if quantity < 1:
            event["decision_reason"] = "insufficient_cash"
            event["debit_cash_per_combo"] = round(debit_cash, 4)
            event["virtual_cash"] = round(float(session.virtual_cash), 4)
            return None, event
        open_trade = OpenTrade(
            strategy_name=strategy.name,
            underlying_symbol=strategy.underlying_symbol,
            regime=strategy.regime,
            quantity=quantity,
            entry_time_et=_now_et().isoformat(),
            entry_minute=current_minute,
            hard_exit_minute=strategy.hard_exit_minute,
            underlying_entry=spot_price,
            entry_debit=entry_debit,
            max_loss_per_combo=max_loss_per_combo,
            max_profit_per_combo=max_profit_per_combo,
            profit_target_dollars=abs(entry_debit) * CONTRACT_MULTIPLIER * strategy.profit_target_multiple,
            stop_loss_dollars=abs(entry_debit) * CONTRACT_MULTIPLIER * strategy.stop_loss_multiple,
            entry_order_id=None,
            entry_fill_price=legs[0].mark,
            legs=leg_payloads,
            entry_attempt_id=attempt_id,
        )
        delta_shares, vega_dollars = self._expected_entry_greeks(open_trade)
        event.update(
            {
                "decision": "eligible",
                "decision_reason": "eligible",
                "quantity_planned": int(quantity),
                "risk_scale": round(risk_scale, 6),
                "reserved_risk": round(reserved_risk, 4),
                "remaining_risk": round(remaining_risk, 4),
                "per_trade_budget": round(per_trade_budget, 4),
                "allocatable_risk": round(allocatable_risk, 4),
                "quantity_by_risk": int(quantity_by_risk),
                "quantity_by_cash": int(quantity_by_cash),
                "expected_entry_debit": round(float(entry_debit), 4),
                "expected_entry_fill_price": round(float(open_trade.entry_fill_price), 4),
                "max_loss_per_combo": round(float(max_loss_per_combo), 4),
                "max_profit_per_combo": round(float(max_profit_per_combo), 4),
                "expected_delta_shares": round(float(delta_shares), 4),
                "expected_vega_dollars_1pct": round(float(vega_dollars), 4),
            }
        )
        return open_trade, event

    def _simple_entry_order_requests(self, trade: OpenTrade) -> list[OrderRequest]:
        leg = trade.legs[0]
        mark = float(leg["mark"])
        ask = float(leg["ask"])
        limits = [min(ask, mark + 0.02), ask]
        return [
            self.broker.build_order_request(
                symbol=str(leg["symbol"]),
                side="buy",
                strategy_name=trade.strategy_name,
                asset_class="option",
                qty=float(trade.quantity),
                order_type="limit",
                time_in_force="day",
                limit_price=round(max(0.01, price), 2),
                extra={"position_intent": "buy_to_open"},
            )
            for price in limits
        ]

    def _simple_exit_order_requests(
        self,
        trade: OpenTrade,
        mark_map: dict[str, float],
        *,
        market_fallback: bool,
    ) -> list[OrderRequest]:
        leg = trade.legs[0]
        mark = float(mark_map[str(leg["symbol"])])
        bid = float(leg["bid"])
        requests = [
            self.broker.build_order_request(
                symbol=str(leg["symbol"]),
                side="sell",
                strategy_name=f"{trade.strategy_name}_exit",
                asset_class="option",
                qty=float(trade.quantity),
                order_type="limit",
                time_in_force="day",
                limit_price=round(max(0.01, max(bid, mark - 0.02)), 2),
                extra={"position_intent": "sell_to_close"},
            ),
            self.broker.build_order_request(
                symbol=str(leg["symbol"]),
                side="sell",
                strategy_name=f"{trade.strategy_name}_exit",
                asset_class="option",
                qty=float(trade.quantity),
                order_type="limit",
                time_in_force="day",
                limit_price=round(max(0.01, bid), 2),
                extra={"position_intent": "sell_to_close"},
            ),
        ]
        if market_fallback:
            requests.append(
                self.broker.build_order_request(
                    symbol=str(leg["symbol"]),
                    side="sell",
                    strategy_name=f"{trade.strategy_name}_exit",
                    asset_class="option",
                    qty=float(trade.quantity),
                    order_type="market",
                    time_in_force="day",
                    extra={"position_intent": "sell_to_close"},
                )
            )
        return requests

    def _wait_for_terminal_order(self, order_id: str) -> dict[str, Any]:
        deadline = time.time() + self.portfolio_config.execution.order_fill_timeout_seconds
        last = self.broker.get_order(order_id)
        while time.time() < deadline:
            status = str(last.get("status", ""))
            if status in TERMINAL_STATUSES:
                return last
            time.sleep(self.portfolio_config.execution.order_status_poll_seconds)
            last = self.broker.get_order(order_id)
        return last

    def _is_filled(self, order_payload: dict[str, Any]) -> bool:
        status = str(order_payload.get("status", ""))
        if status == "filled":
            return True
        filled_qty = float(order_payload.get("filled_qty") or 0.0)
        total_qty = float(order_payload.get("qty") or 0.0)
        return total_qty > 0.0 and filled_qty >= total_qty

    def _execute_attempts(
        self,
        requests: list[OrderRequest],
        *,
        journal_name: str,
        trade: OpenTrade,
        phase: str,
    ) -> tuple[dict[str, Any], float]:
        trade_date = date.fromisoformat(trade.entry_time_et[:10])
        run_dir = self._session_run_dir(trade_date)
        event_base = self._event_base_for_trade(trade, phase=phase)
        for request_index, request in enumerate(requests, start=1):
            response = self.broker.submit_order(
                request,
                dry_run=not self.submit_paper_orders,
                explicitly_requested=self.submit_paper_orders,
            )
            append_journal_entry(run_dir / "order_journal.json", {"journal": journal_name, "response": response})
            self._append_trade_event(
                trade_date,
                {
                    **event_base,
                    "event_type": "order_submission",
                    "request_index": request_index,
                    "request": self._serialize_order_request(request),
                    "response_status": response.get("status"),
                    "order_id": response.get("id"),
                    "client_order_id": request.client_order_id,
                },
            )
            if response.get("status") == "dry_run":
                fallback_price = 0.0 if request.order_type == "market" else float(request.limit_price or 0.0)
                self._append_trade_event(
                    trade_date,
                    {
                        **event_base,
                        "event_type": "order_terminal",
                        "request_index": request_index,
                        "status": "dry_run",
                        "order_id": response.get("id"),
                        "client_order_id": request.client_order_id,
                        "filled_avg_price": fallback_price,
                    },
                )
                return response, fallback_price
            order_id = str(response.get("id") or "")
            terminal = self._wait_for_terminal_order(order_id)
            append_journal_entry(run_dir / "order_journal.json", {"journal": journal_name, "terminal": terminal})
            self._append_trade_event(
                trade_date,
                {
                    **event_base,
                    "event_type": "order_terminal",
                    "request_index": request_index,
                    "status": terminal.get("status"),
                    "order_id": order_id,
                    "client_order_id": request.client_order_id,
                    "filled_qty": terminal.get("filled_qty"),
                    "qty": terminal.get("qty"),
                    "filled_avg_price": terminal.get("filled_avg_price"),
                },
            )
            if self._is_filled(terminal):
                filled_avg_price = float(terminal.get("filled_avg_price") or request.limit_price or 0.0)
                return terminal, filled_avg_price
            if str(terminal.get("status", "")) in OPEN_STATUSES:
                self.broker.cancel_order(order_id, dry_run=False, explicitly_requested=True)
                self._append_trade_event(
                    trade_date,
                    {
                        **event_base,
                        "event_type": "order_cancel",
                        "request_index": request_index,
                        "order_id": order_id,
                        "client_order_id": request.client_order_id,
                    },
                )
        return {"status": "not_filled"}, 0.0

    def _alert(self, session: SessionState, level: str, message: str) -> None:
        alert = {"timestamp_et": _now_et().isoformat(), "level": level, "message": message}
        session.alerts.append(alert)
        self.logger.warning("multi ticker portfolio alert %s", alert)

    def _expected_entry_greeks(self, trade: OpenTrade) -> tuple[float, float]:
        delta_shares = 0.0
        vega_dollars = 0.0
        for leg in trade.legs:
            sign = 1.0 if leg["side"] == "long" else -1.0
            delta_shares += sign * float(leg["delta"]) * CONTRACT_MULTIPLIER
            vega_dollars += sign * float(leg["vega"]) * CONTRACT_MULTIPLIER
        return delta_shares * int(trade.quantity), vega_dollars * int(trade.quantity)

    def _run_entry(self, trade: OpenTrade, session: SessionState, current_equity: float) -> bool:
        delta_shares, vega_dollars = self._expected_entry_greeks(trade)
        trade_date = date.fromisoformat(session.trade_date)
        expected_entry_fill_price = float(trade.entry_fill_price)
        if abs(delta_shares) >= self.portfolio_config.risk.soft_alert_delta_shares:
            self._alert(
                session,
                "warning",
                f"{trade.strategy_name} entry delta alert: {delta_shares:.2f} shares equivalent",
            )
        if abs(vega_dollars) >= self.portfolio_config.risk.soft_alert_vega_dollars_1pct:
            self._alert(
                session,
                "warning",
                f"{trade.strategy_name} entry vega alert: {vega_dollars:.2f} dollars per 1 vol point",
            )
        response, fill_price = self._execute_attempts(
            self._simple_entry_order_requests(trade),
            journal_name=f"{trade.strategy_name}_entry",
            trade=trade,
            phase="entry",
        )
        if response.get("status") == "not_filled":
            self._alert(session, "warning", f"{trade.strategy_name} entry did not fill")
            self._append_trade_event(
                trade_date,
                {
                    **self._event_base_for_trade(trade, phase="entry"),
                    "event_type": "entry_result",
                    "status": "not_filled",
                    "expected_entry_fill_price": round(expected_entry_fill_price, 4),
                    "actual_entry_fill_price": None,
                    "entry_slippage": None,
                    "virtual_cash_after": round(float(session.virtual_cash), 4),
                },
            )
            return False
        trade.entry_order_id = str(response.get("id") or "") if response.get("id") else None
        trade.entry_fill_price = fill_price if fill_price > 0.0 else trade.entry_fill_price
        trade.entry_debit = trade.entry_fill_price
        trade.legs[0]["entry_fill_price"] = trade.entry_fill_price
        session.virtual_cash += _entry_cashflow_from_debit(
            float(trade.entry_debit), int(trade.quantity), len(trade.legs)
        )
        session.open_trades.append(asdict(trade))
        session.signals_fired.append(trade.strategy_name)
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="entry"),
                "event_type": "entry_result",
                "status": str(response.get("status") or "filled"),
                "order_id": trade.entry_order_id,
                "expected_entry_fill_price": round(expected_entry_fill_price, 4),
                "actual_entry_fill_price": round(float(trade.entry_fill_price), 4),
                "entry_slippage": round(float(trade.entry_fill_price) - expected_entry_fill_price, 4),
                "virtual_cash_after": round(float(session.virtual_cash), 4),
            },
        )
        self.logger.info(
            "entered %s symbol=%s qty=%s equity=%.2f submit=%s",
            trade.strategy_name,
            trade.underlying_symbol,
            trade.quantity,
            current_equity,
            self.submit_paper_orders,
        )
        return True

    def _should_exit_trade(
        self,
        trade: OpenTrade,
        current_minute: int,
        mark_map: dict[str, float],
    ) -> tuple[bool, str, float]:
        current_close_cashflow = _position_mark_cashflow(trade.legs, mark_map)
        current_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), int(trade.quantity), len(trade.legs))
            + current_close_cashflow * int(trade.quantity)
            - EXIT_COMMISSION_PER_CONTRACT * len(trade.legs) * int(trade.quantity)
        )
        if current_pnl >= trade.profit_target_dollars * int(trade.quantity):
            return True, "profit_target", current_pnl
        if current_pnl <= -trade.stop_loss_dollars * int(trade.quantity):
            return True, "stop_loss", current_pnl
        if current_minute >= trade.hard_exit_minute:
            return True, "time_exit", current_pnl
        return False, "", current_pnl

    def _run_exit(
        self,
        trade_payload: dict[str, Any],
        session: SessionState,
        snapshot: SymbolSnapshot,
        exit_reason: str,
    ) -> bool:
        trade = OpenTrade(**trade_payload)
        mark_map = self._mark_to_close(trade, snapshot.option_chain)
        if len(mark_map) != len(trade.legs):
            return False
        trade_date = date.fromisoformat(session.trade_date)
        expected_exit_fill_price = (
            float(next(iter(mark_map.values())))
            if len(mark_map) == 1
            else _position_mark_cashflow(trade.legs, mark_map) / CONTRACT_MULTIPLIER
        )
        current_close_cashflow = _position_mark_cashflow(trade.legs, mark_map)
        expected_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), int(trade.quantity), len(trade.legs))
            + current_close_cashflow * int(trade.quantity)
            - EXIT_COMMISSION_PER_CONTRACT * len(trade.legs) * int(trade.quantity)
        )
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="exit"),
                "event_type": "exit_trigger",
                "exit_reason": exit_reason,
                "expected_exit_fill_price": round(float(expected_exit_fill_price), 4),
                "expected_net_pnl": round(float(expected_pnl), 4),
            },
        )
        market_fallback = (
            self.portfolio_config.execution.allow_market_exit_fallback
            and snapshot.current_minute >= self.portfolio_config.execution.market_exit_fallback_minute
        )
        response, fill_price = self._execute_attempts(
            self._simple_exit_order_requests(trade, mark_map, market_fallback=market_fallback),
            journal_name=f"{trade.strategy_name}_exit",
            trade=trade,
            phase="exit",
        )
        if response.get("status") == "not_filled":
            self._alert(session, "warning", f"{trade.strategy_name} exit did not fill")
            self._append_trade_event(
                trade_date,
                {
                    **self._event_base_for_trade(trade, phase="exit"),
                    "event_type": "exit_result",
                    "status": "not_filled",
                    "exit_reason": exit_reason,
                    "order_id": None,
                    "expected_exit_fill_price": round(float(expected_exit_fill_price), 4),
                    "actual_exit_fill_price": None,
                    "exit_slippage": None,
                    "net_pnl": None,
                    "virtual_cash_after": round(float(session.virtual_cash), 4),
                },
            )
            return False
        exit_cashflow = _exit_cashflow_from_fill(
            fill_price=fill_price,
            quantity=int(trade.quantity),
            leg_count=len(trade.legs),
        )
        session.virtual_cash += exit_cashflow
        delta_shares, vega_dollars = self._expected_entry_greeks(trade)
        net_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), int(trade.quantity), len(trade.legs))
            + exit_cashflow
        )
        completed = CompletedTrade(
            strategy_name=trade.strategy_name,
            underlying_symbol=trade.underlying_symbol,
            regime=trade.regime,
            quantity=int(trade.quantity),
            entry_time_et=trade.entry_time_et,
            exit_time_et=_now_et().isoformat(),
            entry_minute=int(trade.entry_minute),
            exit_minute=snapshot.current_minute,
            entry_fill_price=float(trade.entry_fill_price),
            exit_fill_price=float(fill_price),
            underlying_entry=float(trade.underlying_entry),
            underlying_exit=float(snapshot.latest_close),
            exit_reason=exit_reason,
            entry_order_id=trade.entry_order_id,
            exit_order_id=str(response.get("id") or "") if response.get("id") else None,
            net_pnl=round(net_pnl, 4),
            max_loss_per_combo=float(trade.max_loss_per_combo),
            max_profit_per_combo=float(trade.max_profit_per_combo),
            delta_shares_at_entry=round(delta_shares, 4),
            vega_dollars_1pct_at_entry=round(vega_dollars, 4),
            legs=list(trade.legs),
            entry_attempt_id=trade.entry_attempt_id,
        )
        session.completed_trades.append(asdict(completed))
        session.open_trades = [
            item for item in session.open_trades if item["strategy_name"] != trade.strategy_name
        ]
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="exit"),
                "event_type": "exit_result",
                "status": str(response.get("status") or "filled"),
                "exit_reason": exit_reason,
                "order_id": completed.exit_order_id,
                "expected_exit_fill_price": round(float(expected_exit_fill_price), 4),
                "actual_exit_fill_price": round(float(fill_price), 4),
                "exit_slippage": round(float(fill_price) - float(expected_exit_fill_price), 4),
                "net_pnl": round(float(net_pnl), 4),
                "virtual_cash_after": round(float(session.virtual_cash), 4),
            },
        )
        self.logger.info("exited %s reason=%s pnl=%.2f", trade.strategy_name, exit_reason, net_pnl)
        return True

    def _build_symbol_snapshot(
        self,
        *,
        trade_date: date,
        underlying_symbol: str,
        stock_frame: pd.DataFrame,
        session: SessionState,
    ) -> SymbolSnapshot | None:
        if stock_frame.empty:
            return None
        latest = stock_frame.iloc[-1]
        spot_price = float(latest["close"])
        current_minute = int(latest["minute_index"])
        contracts = self._refresh_contract_cache_if_needed(trade_date, underlying_symbol)
        symbols, metadata = self._candidate_symbols_for_snapshot(contracts, spot_price, trade_date)
        open_symbols = [
            str(leg["symbol"])
            for trade in session.open_trades
            if trade["underlying_symbol"] == underlying_symbol
            for leg in trade["legs"]
            if str(leg["symbol"]) not in symbols
        ]
        for symbol in open_symbols:
            match = next((contract for contract in contracts if contract["symbol"] == symbol), None)
            if match is not None:
                metadata[symbol] = match
                symbols.append(symbol)
        option_chain = self._fetch_option_chain(
            symbols,
            metadata,
            spot_price=spot_price,
            trade_date=trade_date,
            underlying_symbol=underlying_symbol,
        )
        mark_map = {
            str(row.symbol): float(row.mark)
            for row in option_chain.itertuples(index=False)
            if pd.notna(row.mark)
        }
        latest_timestamp = latest["timestamp_et"]
        if pd.isna(latest_timestamp):
            latest_timestamp = None
        return SymbolSnapshot(
            underlying_symbol=underlying_symbol,
            trade_date=trade_date,
            stock_frame=stock_frame,
            option_chain=option_chain,
            mark_map=mark_map,
            latest_close=spot_price,
            current_minute=current_minute,
            latest_timestamp_et=latest_timestamp.to_pydatetime() if latest_timestamp is not None else None,
        )

    def _perform_startup_check(
        self,
        *,
        session: SessionState,
        trade_date: date,
        snapshots: dict[str, SymbolSnapshot],
    ) -> tuple[str, dict[str, Any]]:
        details: dict[str, Any] = {"trade_date": trade_date.isoformat(), "underlyings": {}}
        now_et = _now_et()
        open_et = _rth_open_for(trade_date)
        grace_cutoff = open_et + timedelta(minutes=5)

        account = self.broker.get_account()
        positions = self.broker.get_positions()
        buying_power = float(account.get("buying_power") or 0.0)
        details["buying_power"] = round(buying_power, 2)
        details["required_buying_power"] = round(
            self.portfolio_config.risk.min_required_buying_power,
            2,
        )
        details["broker_position_count"] = len(positions)

        failures: list[str] = []
        pending_reasons: list[str] = []
        if buying_power < self.portfolio_config.risk.min_required_buying_power:
            failures.append(
                f"buying power {buying_power:.2f} below required {self.portfolio_config.risk.min_required_buying_power:.2f}"
            )
        if not session.open_trades and positions:
            failures.append("broker reported unexpected open positions at session start")

        for symbol in self.underlyings:
            snapshot = snapshots.get(symbol)
            symbol_details: dict[str, Any] = {}
            if snapshot is None:
                symbol_details["status"] = "missing_stock_frame"
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} stock frame not ready yet")
                else:
                    failures.append(f"{symbol} stock frame missing after startup grace period")
                details["underlyings"][symbol] = symbol_details
                continue
            freshness_seconds = None
            if snapshot.latest_timestamp_et is not None:
                freshness_seconds = max(0.0, (now_et - snapshot.latest_timestamp_et).total_seconds())
            symbol_details["latest_minute"] = snapshot.current_minute
            symbol_details["stock_freshness_seconds"] = freshness_seconds
            same_day_calls = int(
                (
                    (snapshot.option_chain["dte"] == 0)
                    & (snapshot.option_chain["option_type"] == "call")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            same_day_puts = int(
                (
                    (snapshot.option_chain["dte"] == 0)
                    & (snapshot.option_chain["option_type"] == "put")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            next_expiry_calls = int(
                (
                    (snapshot.option_chain["dte"] > 0)
                    & (snapshot.option_chain["option_type"] == "call")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            next_expiry_puts = int(
                (
                    (snapshot.option_chain["dte"] > 0)
                    & (snapshot.option_chain["option_type"] == "put")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            symbol_details["same_day_calls"] = same_day_calls
            symbol_details["same_day_puts"] = same_day_puts
            symbol_details["next_expiry_calls"] = next_expiry_calls
            symbol_details["next_expiry_puts"] = next_expiry_puts
            required_inventory = {
                "same_day_calls": False,
                "same_day_puts": False,
                "next_expiry_calls": False,
                "next_expiry_puts": False,
            }
            for strategy in self.portfolio_config.strategies_by_symbol.get(symbol, []):
                dte_prefix = "same_day" if strategy.dte_mode == "same_day" else "next_expiry"
                for leg in strategy.legs:
                    bucket = f"{dte_prefix}_{leg.option_type}s"
                    required_inventory[bucket] = True
            symbol_details["required_inventory"] = required_inventory
            if freshness_seconds is None:
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} latest stock bar timestamp not ready yet")
                else:
                    failures.append(f"{symbol} latest stock bar timestamp missing")
            elif freshness_seconds > self.portfolio_config.execution.stock_freshness_seconds:
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} stock data stale at {freshness_seconds:.0f}s")
                else:
                    failures.append(f"{symbol} stock data stale at {freshness_seconds:.0f}s")
            missing_inventory: list[str] = []
            inventory_counts = {
                "same_day_calls": same_day_calls,
                "same_day_puts": same_day_puts,
                "next_expiry_calls": next_expiry_calls,
                "next_expiry_puts": next_expiry_puts,
            }
            available_strategies: list[str] = []
            unavailable_strategies: list[dict[str, Any]] = []
            for strategy in self.portfolio_config.strategies_by_symbol.get(symbol, []):
                strategy_missing: list[str] = []
                dte_prefix = "same_day" if strategy.dte_mode == "same_day" else "next_expiry"
                for leg in strategy.legs:
                    bucket = f"{dte_prefix}_{leg.option_type}s"
                    if inventory_counts[bucket] == 0:
                        strategy_missing.append(bucket)
                if strategy_missing:
                    unavailable_strategies.append(
                        {
                            "name": strategy.name,
                            "missing_inventory": strategy_missing,
                        }
                    )
                    missing_inventory.extend(strategy_missing)
                else:
                    available_strategies.append(strategy.name)
            symbol_details["available_strategies"] = available_strategies
            if unavailable_strategies:
                symbol_details["unavailable_strategies"] = unavailable_strategies
            if not available_strategies:
                symbol_details["missing_inventory"] = sorted(set(missing_inventory))
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} option inventory incomplete")
                else:
                    failures.append(f"{symbol} option inventory incomplete")
            details["underlyings"][symbol] = symbol_details

        if failures:
            details["failures"] = failures
            return "failed", details
        if pending_reasons:
            details["pending_reasons"] = pending_reasons
            return "pending", details
        details["status"] = "passed"
        return "passed", details

    def _send_morning_notification(self, session: SessionState, details: dict[str, Any]) -> None:
        failed_phases = getattr(self, "_failed_notification_phases", set())
        if session.notified_morning or "morning" in failed_phases:
            return
        delivered = self._notify_lines(*self._build_morning_notification_lines(session, details))
        if delivered:
            session.notified_morning = True
        else:
            self._record_notification_failure(session, "morning")

    def _maybe_send_midday_notification(
        self,
        *,
        session: SessionState,
        current_equity: float,
        snapshots: dict[str, SymbolSnapshot],
    ) -> None:
        failed_phases = getattr(self, "_failed_notification_phases", set())
        if session.notified_midday or "midday" in failed_phases:
            return
        current_minute = max((snapshot.current_minute for snapshot in snapshots.values()), default=-1)
        if current_minute < self.portfolio_config.execution.midday_report_minute:
            return
        delivered = self._notify_lines(
            *self._build_midday_notification_lines(session, current_equity=current_equity)
        )
        if delivered:
            session.notified_midday = True
        else:
            self._record_notification_failure(session, "midday")

    def _reconcile_and_trade(
        self,
        *,
        session: SessionState,
        ledger: PortfolioLedger,
        stock_frames: dict[str, pd.DataFrame],
    ) -> tuple[dict[str, SymbolSnapshot], float]:
        trade_date = date.fromisoformat(session.trade_date)
        snapshots: dict[str, SymbolSnapshot] = {}
        for symbol, stock_frame in stock_frames.items():
            snapshot = self._build_symbol_snapshot(
                trade_date=trade_date,
                underlying_symbol=symbol,
                stock_frame=stock_frame,
                session=session,
            )
            if snapshot is None:
                continue
            snapshots[symbol] = snapshot
            session.last_symbol_regimes[symbol] = infer_symbol_regime(stock_frame)

        combined_mark_map = {
            symbol: mark
            for snapshot in snapshots.values()
            for symbol, mark in snapshot.mark_map.items()
        }
        current_equity = _current_equity(session, combined_mark_map)
        loss_gate, reason = self._daily_loss_gate_check(session, current_equity)
        if loss_gate:
            session.blocked_new_entries = True
            session.block_reason = reason

        exiting: list[tuple[dict[str, Any], SymbolSnapshot, str]] = []
        for trade_payload in list(session.open_trades):
            trade = OpenTrade(**trade_payload)
            snapshot = snapshots.get(trade.underlying_symbol)
            if snapshot is None:
                continue
            mark_map = self._mark_to_close(trade, snapshot.option_chain)
            if len(mark_map) != len(trade.legs):
                continue
            should_exit, exit_reason, _current_pnl = self._should_exit_trade(
                trade, snapshot.current_minute, mark_map
            )
            if should_exit:
                exiting.append((trade_payload, snapshot, exit_reason))
        for trade_payload, snapshot, exit_reason in exiting:
            self._run_exit(trade_payload, session, snapshot, exit_reason)

        combined_mark_map = {
            symbol: mark
            for snapshot in snapshots.values()
            for symbol, mark in snapshot.mark_map.items()
        }
        current_equity = _current_equity(session, combined_mark_map)
        self._maybe_send_midday_notification(
            session=session,
            current_equity=current_equity,
            snapshots=snapshots,
        )
        if session.blocked_new_entries:
            return snapshots, current_equity

        for underlying_symbol, strategies in self.portfolio_config.strategies_by_symbol.items():
            snapshot = snapshots.get(underlying_symbol)
            if snapshot is None or snapshot.option_chain.empty:
                continue
            for strategy in strategies:
                if not signal_is_true(
                    strategy.signal_name,
                    snapshot.stock_frame,
                    timing_profile=strategy.timing_profile,
                ):
                    continue
                attempt_id = f"{strategy.name}:{session.trade_date}:{snapshot.current_minute}:{time.time_ns()}"
                open_trade, signal_event = self._evaluate_entry(
                    strategy=strategy,
                    session=session,
                    ledger=ledger,
                    option_chain=snapshot.option_chain,
                    spot_price=snapshot.latest_close,
                    current_minute=snapshot.current_minute,
                    current_equity=current_equity,
                    attempt_id=attempt_id,
                )
                self._append_trade_event(trade_date, signal_event)
                if open_trade is None:
                    continue
                if self._run_entry(open_trade, session, current_equity):
                    current_equity = _current_equity(session, combined_mark_map)
        return snapshots, current_equity

    def _flatten_all(self, session: SessionState, stock_frames: dict[str, pd.DataFrame]) -> None:
        if not session.open_trades:
            return
        trade_date = date.fromisoformat(session.trade_date)
        snapshots: dict[str, SymbolSnapshot] = {}
        for symbol in {trade["underlying_symbol"] for trade in session.open_trades}:
            stock_frame = stock_frames.get(symbol, pd.DataFrame())
            snapshot = self._build_symbol_snapshot(
                trade_date=trade_date,
                underlying_symbol=symbol,
                stock_frame=stock_frame,
                session=session,
            )
            if snapshot is not None:
                snapshots[symbol] = snapshot
        for trade_payload in list(session.open_trades):
            snapshot = snapshots.get(trade_payload["underlying_symbol"])
            if snapshot is None:
                continue
            self._run_exit(trade_payload, session, snapshot, "forced_flatten")

    def _load_trade_reconciliation_events(self, trade_date: date) -> pd.DataFrame:
        payload = _read_json(self._trade_reconciliation_events_path(trade_date), [])
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list) or not payload:
            return pd.DataFrame()
        frame = pd.DataFrame(payload)
        if "timestamp_et" in frame.columns:
            frame["timestamp_et"] = frame["timestamp_et"].astype(str)
        return frame

    def _build_trade_reconciliation_outputs(
        self,
        *,
        session: SessionState,
        trade_date: date,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        events_df = self._load_trade_reconciliation_events(trade_date)
        completed_df = pd.DataFrame(session.completed_trades)
        if completed_df.empty:
            completed_df = pd.DataFrame(columns=[field for field in CompletedTrade.__dataclass_fields__.keys()])
        else:
            completed_df = completed_df.copy()

        if events_df.empty:
            empty = pd.DataFrame()
            summary = {
                "signal_attempt_count": 0,
                "eligible_signal_count": 0,
                "skipped_signal_count": 0,
                "entry_submission_count": 0,
                "exit_submission_count": 0,
                "entry_fill_count": 0,
                "entry_not_filled_count": 0,
                "exit_fill_count": 0,
                "open_reconciled_trade_count": len(session.open_trades),
                "completed_reconciled_trade_count": len(session.completed_trades),
                "realized_reconciled_net_pnl": round(
                    float(completed_df["net_pnl"].sum()) if "net_pnl" in completed_df else 0.0,
                    4,
                ),
            }
            return empty, empty, empty, empty, summary

        if "phase" not in events_df.columns:
            events_df["phase"] = None
        if "attempt_id" not in events_df.columns:
            events_df["attempt_id"] = None

        signal_df = events_df.loc[events_df["event_type"] == "signal_decision"].copy()
        if signal_df.empty:
            signal_df = pd.DataFrame(columns=["attempt_id"])
        entry_results = (
            events_df.loc[events_df["event_type"] == "entry_result"]
            .sort_values("timestamp_et")
            .drop_duplicates(subset=["attempt_id"], keep="last")
        )
        exit_triggers = (
            events_df.loc[events_df["event_type"] == "exit_trigger"]
            .sort_values("timestamp_et")
            .drop_duplicates(subset=["attempt_id"], keep="last")
        )
        exit_results = (
            events_df.loc[events_df["event_type"] == "exit_result"]
            .sort_values("timestamp_et")
            .drop_duplicates(subset=["attempt_id"], keep="last")
        )
        if not completed_df.empty and "entry_attempt_id" in completed_df.columns:
            completed_lookup = completed_df.set_index("entry_attempt_id", drop=False).to_dict("index")
        else:
            completed_lookup = {}
        open_attempt_ids = {
            str(trade.get("entry_attempt_id"))
            for trade in session.open_trades
            if trade.get("entry_attempt_id") is not None
        }

        reconciliation_rows: list[dict[str, Any]] = []
        for signal_row in signal_df.sort_values("timestamp_et").to_dict("records"):
            attempt_id = str(signal_row.get("attempt_id") or "")
            entry_row = (
                entry_results.loc[entry_results["attempt_id"] == attempt_id].iloc[0].to_dict()
                if attempt_id and not entry_results.loc[entry_results["attempt_id"] == attempt_id].empty
                else {}
            )
            exit_trigger_row = (
                exit_triggers.loc[exit_triggers["attempt_id"] == attempt_id].iloc[0].to_dict()
                if attempt_id and not exit_triggers.loc[exit_triggers["attempt_id"] == attempt_id].empty
                else {}
            )
            exit_row = (
                exit_results.loc[exit_results["attempt_id"] == attempt_id].iloc[0].to_dict()
                if attempt_id and not exit_results.loc[exit_results["attempt_id"] == attempt_id].empty
                else {}
            )
            completed_row = completed_lookup.get(attempt_id, {})
            if signal_row.get("decision") != "eligible":
                final_status = "skipped"
            elif exit_row:
                final_status = "completed" if exit_row.get("status") != "not_filled" else "exit_not_filled"
            elif attempt_id in open_attempt_ids:
                final_status = "open"
            elif entry_row:
                final_status = "entered" if entry_row.get("status") != "not_filled" else "entry_not_filled"
            else:
                final_status = "signal_only"
            reconciliation_rows.append(
                {
                    "attempt_id": attempt_id,
                    "trade_date": signal_row.get("trade_date"),
                    "strategy_name": signal_row.get("strategy_name"),
                    "underlying_symbol": signal_row.get("underlying_symbol"),
                    "regime": signal_row.get("regime"),
                    "signal_name": signal_row.get("signal_name"),
                    "timing_profile": signal_row.get("timing_profile"),
                    "signal_time_et": signal_row.get("timestamp_et"),
                    "signal_minute": signal_row.get("current_minute"),
                    "signal_decision": signal_row.get("decision"),
                    "signal_reason": signal_row.get("decision_reason"),
                    "quantity_planned": signal_row.get("quantity_planned"),
                    "expected_entry_fill_price": signal_row.get("expected_entry_fill_price"),
                    "entry_submission_count": int(
                        len(
                            events_df.loc[
                                (events_df["attempt_id"] == attempt_id)
                                & (events_df["event_type"] == "order_submission")
                                & (events_df["phase"] == "entry")
                            ]
                        )
                    ),
                    "entry_status": entry_row.get("status"),
                    "entry_order_id": entry_row.get("order_id"),
                    "entry_actual_fill_price": entry_row.get("actual_entry_fill_price"),
                    "entry_slippage": entry_row.get("entry_slippage"),
                    "exit_trigger_reason": exit_trigger_row.get("exit_reason"),
                    "expected_exit_fill_price": exit_row.get(
                        "expected_exit_fill_price",
                        exit_trigger_row.get("expected_exit_fill_price"),
                    ),
                    "exit_submission_count": int(
                        len(
                            events_df.loc[
                                (events_df["attempt_id"] == attempt_id)
                                & (events_df["event_type"] == "order_submission")
                                & (events_df["phase"] == "exit")
                            ]
                        )
                    ),
                    "exit_status": exit_row.get("status"),
                    "exit_order_id": exit_row.get("order_id"),
                    "exit_actual_fill_price": exit_row.get("actual_exit_fill_price"),
                    "exit_slippage": exit_row.get("exit_slippage"),
                    "exit_reason": completed_row.get("exit_reason", exit_row.get("exit_reason")),
                    "realized_net_pnl": completed_row.get("net_pnl", exit_row.get("net_pnl")),
                    "final_status": final_status,
                }
            )

        reconciliation_df = pd.DataFrame(reconciliation_rows)
        completed_for_perf = reconciliation_df.loc[
            reconciliation_df["final_status"] == "completed"
        ].copy() if not reconciliation_df.empty else pd.DataFrame()

        def _performance_table(group_col: str) -> pd.DataFrame:
            if completed_for_perf.empty:
                return pd.DataFrame(
                    columns=[
                        group_col,
                        "trade_count",
                        "net_pnl",
                        "avg_pnl",
                        "win_rate_pct",
                    ]
                )
            grouped = completed_for_perf.groupby(group_col, dropna=False)
            table = grouped["realized_net_pnl"].agg(["count", "sum", "mean"]).reset_index()
            wins = (
                completed_for_perf.assign(_win=completed_for_perf["realized_net_pnl"].astype(float) > 0.0)
                .groupby(group_col, dropna=False)["_win"]
                .mean()
                .reset_index(name="win_rate_pct")
            )
            merged = table.merge(wins, on=group_col, how="left")
            merged = merged.rename(
                columns={
                    "count": "trade_count",
                    "sum": "net_pnl",
                    "mean": "avg_pnl",
                }
            )
            merged["net_pnl"] = merged["net_pnl"].round(4)
            merged["avg_pnl"] = merged["avg_pnl"].round(4)
            merged["win_rate_pct"] = (merged["win_rate_pct"] * 100.0).round(2)
            return merged.sort_values("net_pnl", ascending=False).reset_index(drop=True)

        ticker_df = _performance_table("underlying_symbol")
        strategy_df = _performance_table("strategy_name")
        summary = {
            "signal_attempt_count": int(len(signal_df)),
            "eligible_signal_count": int((signal_df["decision"] == "eligible").sum()) if "decision" in signal_df else 0,
            "skipped_signal_count": int((signal_df["decision"] != "eligible").sum()) if "decision" in signal_df else 0,
            "entry_submission_count": int(
                len(events_df.loc[(events_df["event_type"] == "order_submission") & (events_df["phase"] == "entry")])
            ),
            "exit_submission_count": int(
                len(events_df.loc[(events_df["event_type"] == "order_submission") & (events_df["phase"] == "exit")])
            ),
            "entry_fill_count": int(
                len(entry_results.loc[entry_results["status"].isin(["filled", "dry_run"])])
            ) if not entry_results.empty else 0,
            "entry_not_filled_count": int(
                len(entry_results.loc[entry_results["status"] == "not_filled"])
            ) if not entry_results.empty else 0,
            "exit_fill_count": int(
                len(exit_results.loc[exit_results["status"].isin(["filled", "dry_run"])])
            ) if not exit_results.empty else 0,
            "open_reconciled_trade_count": int((reconciliation_df["final_status"] == "open").sum()) if not reconciliation_df.empty else 0,
            "completed_reconciled_trade_count": int((reconciliation_df["final_status"] == "completed").sum()) if not reconciliation_df.empty else 0,
            "realized_reconciled_net_pnl": round(
                float(pd.to_numeric(completed_for_perf["realized_net_pnl"], errors="coerce").fillna(0.0).sum()),
                4,
            ) if not completed_for_perf.empty else 0.0,
        }
        return events_df, reconciliation_df, ticker_df, strategy_df, summary

    def _backfill_open_trade_reconciliation(self, session: SessionState) -> bool:
        if not session.open_trades:
            return False
        trade_date = date.fromisoformat(session.trade_date)
        events_df = self._load_trade_reconciliation_events(trade_date)
        existing_attempt_ids = (
            set(events_df["attempt_id"].dropna().astype(str).tolist())
            if not events_df.empty and "attempt_id" in events_df.columns
            else set()
        )
        updated = False
        for index, trade_payload in enumerate(session.open_trades):
            if not trade_payload.get("entry_attempt_id"):
                trade_payload["entry_attempt_id"] = (
                    f"recovered:{trade_payload['strategy_name']}:{trade_payload['entry_minute']}:{index}"
                )
                updated = True
            attempt_id = str(trade_payload["entry_attempt_id"])
            if attempt_id in existing_attempt_ids:
                continue
            trade = OpenTrade(**trade_payload)
            delta_shares, vega_dollars = self._expected_entry_greeks(trade)
            self._append_trade_event(
                trade_date,
                {
                    "event_type": "signal_decision",
                    "attempt_id": attempt_id,
                    "trade_date": session.trade_date,
                    "strategy_name": trade.strategy_name,
                    "underlying_symbol": trade.underlying_symbol,
                    "regime": trade.regime,
                    "signal_name": "recovered_open_trade",
                    "timing_profile": "recovered",
                    "current_minute": int(trade.entry_minute),
                    "current_equity": round(float(session.starting_equity), 4),
                    "decision": "eligible",
                    "decision_reason": "backfilled_open_trade",
                    "quantity_planned": int(trade.quantity),
                    "expected_entry_debit": round(float(trade.entry_debit), 4),
                    "expected_entry_fill_price": round(float(trade.entry_fill_price), 4),
                    "expected_delta_shares": round(float(delta_shares), 4),
                    "expected_vega_dollars_1pct": round(float(vega_dollars), 4),
                    "backfilled": True,
                },
            )
            self._append_trade_event(
                trade_date,
                {
                    **self._event_base_for_trade(trade, phase="entry"),
                    "event_type": "entry_result",
                    "status": "filled",
                    "order_id": trade.entry_order_id,
                    "expected_entry_fill_price": round(float(trade.entry_fill_price), 4),
                    "actual_entry_fill_price": round(float(trade.entry_fill_price), 4),
                    "entry_slippage": 0.0,
                    "virtual_cash_after": round(float(session.virtual_cash), 4),
                    "backfilled": True,
                },
            )
            existing_attempt_ids.add(attempt_id)
            updated = True
        return updated

    def finalize_session(
        self,
        session: SessionState,
        ledger: PortfolioLedger,
        *,
        stock_frames: dict[str, pd.DataFrame] | None = None,
    ) -> dict[str, Any]:
        if stock_frames is not None:
            self._flatten_all(session, stock_frames)
        ending_equity = session.virtual_cash
        ledger.realized_equity = ending_equity
        ledger.high_watermark = max(ledger.high_watermark, ending_equity)
        ledger.closed_days.append(
            {
                "trade_date": session.trade_date,
                "starting_equity": session.starting_equity,
                "ending_equity": ending_equity,
                "net_pnl": ending_equity - session.starting_equity,
                "completed_trades": len(session.completed_trades),
                "blocked_new_entries": session.blocked_new_entries,
                "block_reason": session.block_reason,
            }
        )
        self.save_ledger(ledger)
        self.save_session(session)
        run_dir = self._session_run_dir(date.fromisoformat(session.trade_date))
        completed_df = pd.DataFrame(session.completed_trades)
        (
            reconciliation_events_df,
            reconciliation_df,
            ticker_performance_df,
            strategy_performance_df,
            reconciliation_summary,
        ) = self._build_trade_reconciliation_outputs(
            session=session,
            trade_date=date.fromisoformat(session.trade_date),
        )
        summary = {
            "trade_date": session.trade_date,
            "submit_paper_orders": self.submit_paper_orders,
            "starting_equity": round(session.starting_equity, 2),
            "ending_equity": round(ending_equity, 2),
            "net_pnl": round(ending_equity - session.starting_equity, 2),
            "completed_trade_count": len(session.completed_trades),
            "blocked_new_entries": session.blocked_new_entries,
            "block_reason": session.block_reason,
            "last_symbol_regimes": session.last_symbol_regimes,
            "startup_check_status": session.startup_check_status,
        }
        summary.update(reconciliation_summary)
        write_summary_bundle(
            run_dir,
            name="multi_ticker_portfolio_session_summary",
            summary=summary,
            table_map={
                "completed_trades": completed_df,
                "trade_reconciliation": reconciliation_df,
                "trade_reconciliation_events": reconciliation_events_df,
                "ticker_performance": ticker_performance_df,
                "strategy_performance": strategy_performance_df,
            },
        )
        write_alert_queue(run_dir / "alerts.json", session.alerts)
        failed_phases = getattr(self, "_failed_notification_phases", set())
        if not session.notified_end_of_day and "end-of-day" not in failed_phases:
            delivered = self._notify_lines(
                *self._build_end_of_day_notification_lines(session, ending_equity=ending_equity)
            )
            if delivered:
                session.notified_end_of_day = True
            else:
                self._record_notification_failure(session, "end-of-day")
            self.save_session(session)
        return summary

    def run(self, *, run_once: bool = False) -> dict[str, Any]:
        clock = self.broker.get_clock()
        trade_date = _trade_date_from_clock(clock)
        now_et = _now_et()
        ledger = self.load_ledger()
        session = self.load_or_create_session(trade_date, ledger)
        if self._backfill_open_trade_reconciliation(session):
            self.save_session(session)
        startup_block_markers = (
            "option inventory incomplete",
            "stock data stale",
            "latest stock bar timestamp",
            "buying power",
            "unexpected open positions",
        )
        if (
            session.startup_check_status == "passed"
            and session.blocked_new_entries
            and not session.open_trades
            and not session.completed_trades
            and session.block_reason
            and any(marker in session.block_reason.lower() for marker in startup_block_markers)
        ):
            session.blocked_new_entries = False
            session.block_reason = None
            self.save_session(session)
        while not bool(clock.get("is_open", False)):
            if now_et < _rth_open_for(trade_date):
                seconds_to_open = (_rth_open_for(trade_date) - now_et).total_seconds()
                if run_once:
                    return {
                        "status": "before_open",
                        "trade_date": trade_date.isoformat(),
                        "seconds_to_open": int(seconds_to_open),
                    }
                time.sleep(max(1.0, min(60.0, seconds_to_open)))
                clock = self.broker.get_clock()
                refreshed_trade_date = _trade_date_from_clock(clock)
                if refreshed_trade_date != trade_date:
                    trade_date = refreshed_trade_date
                    session = self.load_or_create_session(trade_date, ledger)
                    if self._backfill_open_trade_reconciliation(session):
                        self.save_session(session)
                now_et = _now_et()
                continue

            stock_frames = self._fetch_today_stock_frames(trade_date)
            summary = self.finalize_session(session, ledger, stock_frames=stock_frames)
            summary["status"] = "after_close"
            return summary

        while True:
            stock_frames = self._fetch_today_stock_frames(trade_date)
            snapshots = {
                symbol: snapshot
                for symbol, snapshot in (
                    (
                        symbol,
                        self._build_symbol_snapshot(
                            trade_date=trade_date,
                            underlying_symbol=symbol,
                            stock_frame=stock_frame,
                            session=session,
                        ),
                    )
                    for symbol, stock_frame in stock_frames.items()
                )
                if snapshot is not None
            }
            if session.startup_check_status != "passed":
                status, details = self._perform_startup_check(
                    session=session,
                    trade_date=trade_date,
                    snapshots=snapshots,
                )
                session.startup_check_status = status
                session.startup_check_details = details
                self.save_session(session)
                if status == "pending":
                    if run_once:
                        return {
                            "status": "startup_check_pending",
                            "trade_date": trade_date.isoformat(),
                            "details": details,
                        }
                    time.sleep(self.portfolio_config.execution.poll_interval_seconds)
                    continue
                if status == "failed":
                    session.blocked_new_entries = True
                    session.block_reason = "; ".join(details.get("failures", []))
                    self._alert(session, "error", session.block_reason or "startup check failed")
                    self._notify_lines(
                        "**Multi-Ticker Portfolio Morning Check Failed**",
                        f"Trade date: {session.trade_date}",
                        *(details.get("failures", []) or ["startup check failed"]),
                    )
                    self.save_session(session)
                    return {
                        "status": "startup_check_failed",
                        "trade_date": trade_date.isoformat(),
                        "details": details,
                    }
                self._send_morning_notification(session, details)
                session.blocked_new_entries = False
                session.block_reason = None
                self.save_session(session)

            if any(not frame.empty for frame in stock_frames.values()):
                _, current_equity = self._reconcile_and_trade(
                    session=session,
                    ledger=ledger,
                    stock_frames=stock_frames,
                )
                self.save_session(session)
            else:
                current_equity = _current_equity(session)
            if run_once:
                return {
                    "status": "ran_once",
                    "trade_date": trade_date.isoformat(),
                    "open_trades": len(session.open_trades),
                    "completed_trades": len(session.completed_trades),
                    "blocked_new_entries": session.blocked_new_entries,
                    "current_equity": round(current_equity, 2),
                    "startup_check_status": session.startup_check_status,
                }
            clock = self.broker.get_clock()
            if not bool(clock.get("is_open", False)):
                summary = self.finalize_session(session, ledger, stock_frames=stock_frames)
                summary["status"] = "session_complete"
                return summary
            time.sleep(self.portfolio_config.execution.poll_interval_seconds)

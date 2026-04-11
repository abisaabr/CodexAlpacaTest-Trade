from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter, OrderLeg, OrderRequest
from alpaca_lab.config import LabSettings
from alpaca_lab.logging_utils import get_logger
from alpaca_lab.reporting import append_journal_entry, write_alert_queue, write_summary_bundle
from alpaca_lab.qqq_portfolio.config import QQQPortfolioConfig, StrategyConfig
from alpaca_lab.qqq_portfolio.greeks import bs_greeks, implied_volatility
from alpaca_lab.qqq_portfolio.signals import (
    MINUTES_PER_RTH_SESSION,
    build_stock_frame,
    extract_session_features,
    infer_intraday_regime,
    signal_is_true,
)


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

    def open_order_leg(self) -> OrderLeg:
        return OrderLeg(
            symbol=self.symbol,
            side="buy" if self.side == "long" else "sell",
            ratio_qty=1,
            position_intent="buy_to_open" if self.side == "long" else "sell_to_open",
        )

    def close_order_leg(self) -> OrderLeg:
        return OrderLeg(
            symbol=self.symbol,
            side="sell" if self.side == "long" else "buy",
            ratio_qty=1,
            position_intent="sell_to_close" if self.side == "long" else "buy_to_close",
        )


@dataclass(slots=True)
class OpenTrade:
    strategy_name: str
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
    entry_order_class: str
    entry_order_type: str
    entry_fill_price: float
    legs: list[dict[str, Any]]
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CompletedTrade:
    strategy_name: str
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
    last_regime: str = "neutral"
    last_updated_at: str | None = None


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


def _round_limit_price(value: float) -> float:
    if value >= 0.0:
        return round(max(0.01, value), 2)
    return round(min(-0.01, value), 2)


def _mark_from_snapshot(snapshot: dict[str, Any]) -> tuple[float | None, float | None, float | None, str | None]:
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


def _combo_debit_from_marks(legs: list[SelectedLeg]) -> float:
    return sum(leg.mark if leg.side == "long" else -leg.mark for leg in legs)


def _combo_debit_from_natural(legs: list[SelectedLeg]) -> float:
    debit = 0.0
    for leg in legs:
        if leg.side == "long":
            debit += leg.ask
        else:
            debit -= leg.bid
    return debit


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


def _exit_cashflow_from_fill(
    *,
    fill_price: float,
    quantity: int,
    leg_count: int,
    order_class: str,
    side: str | None = None,
) -> float:
    if order_class == "mleg":
        gross = -fill_price * CONTRACT_MULTIPLIER * quantity
    else:
        gross = fill_price * CONTRACT_MULTIPLIER * quantity if side == "sell" else -fill_price * CONTRACT_MULTIPLIER * quantity
    commission = EXIT_COMMISSION_PER_CONTRACT * leg_count * quantity
    return gross - commission


def _current_equity(state: SessionState, mark_map: dict[str, float] | None = None) -> float:
    equity = state.virtual_cash
    mark_map = mark_map or {}
    for trade_payload in state.open_trades:
        if not mark_map:
            continue
        try:
            mark_cashflow = _position_mark_cashflow(trade_payload["legs"], mark_map)
        except KeyError:
            continue
        commission = EXIT_COMMISSION_PER_CONTRACT * len(trade_payload["legs"]) * int(
            trade_payload["quantity"]
        )
        equity += mark_cashflow * int(trade_payload["quantity"]) - commission
    return equity


class QQQPortfolioPaperTrader:
    def __init__(
        self,
        settings: LabSettings,
        portfolio_config: QQQPortfolioConfig,
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
        self.logger = get_logger("qqq_portfolio")
        self.state_root = portfolio_config.execution.state_root
        self.run_root = portfolio_config.execution.run_root
        self.ledger_path = self.state_root / "ledger.json"
        self.contract_cache: dict[str, list[dict[str, Any]]] = {}
        self.contract_cache_loaded_at: datetime | None = None

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
                last_regime=str(payload.get("last_regime", "neutral")),
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

    def _fetch_today_stock_frame(self, trade_date: date) -> pd.DataFrame:
        start = _rth_open_for(trade_date).astimezone(UTC)
        end = _now_et().astimezone(UTC)
        payload = self.broker.get_stock_bars(
            [self.portfolio_config.execution.underlying_symbol],
            start=start,
            end=end,
            timeframe="1Min",
            feed=self.portfolio_config.execution.stock_feed or self.settings.alpaca_data_feed,
            limit=10000,
        )
        rows = payload.get("bars", {}).get(self.portfolio_config.execution.underlying_symbol, [])
        return build_stock_frame(rows)

    def _refresh_contract_cache_if_needed(self, trade_date: date) -> list[dict[str, Any]]:
        stale = (
            self.contract_cache_loaded_at is None
            or (_now_et() - self.contract_cache_loaded_at).total_seconds()
            >= self.portfolio_config.execution.contract_refresh_minutes * 60
        )
        cache_key = trade_date.isoformat()
        if stale or cache_key not in self.contract_cache:
            expiration_lte = trade_date + timedelta(days=self.portfolio_config.execution.max_dte_days)
            payload = self.broker.get_option_contracts(
                [self.portfolio_config.execution.underlying_symbol],
                expiration_date_gte=trade_date.isoformat(),
                expiration_date_lte=expiration_lte.isoformat(),
                status="active",
                limit=1000,
            )
            contracts = []
            for contract in payload.get("option_contracts", []):
                expiration_date = date.fromisoformat(str(contract["expiration_date"]))
                contracts.append(
                    {
                        "symbol": str(contract["symbol"]),
                        "expiration_date": expiration_date,
                        "strike_price": float(contract["strike_price"]),
                        "option_type": str(contract["type"]),
                    }
                )
            self.contract_cache[cache_key] = contracts
            self.contract_cache_loaded_at = _now_et()
        return self.contract_cache[cache_key]

    def _candidate_symbols_for_snapshot(
        self, contracts: list[dict[str, Any]], spot_price: float, trade_date: date
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

    def _fetch_option_chain(
        self, symbols: list[str], metadata: dict[str, dict[str, Any]], spot_price: float, trade_date: date
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
                bid, ask, mark, quote_time = _mark_from_snapshot(snapshot)
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
            if subset.empty:
                return []
            subset = subset[
                (subset["delta"].abs() >= leg_template.min_abs_delta)
                & (subset["delta"].abs() <= leg_template.max_abs_delta)
            ].copy()
            if subset.empty:
                return []
            if "freshness_seconds" in subset.columns:
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

    def _daily_loss_gate_check(self, session: SessionState, current_equity: float) -> tuple[bool, str | None]:
        threshold = session.starting_equity * (1.0 - self.portfolio_config.risk.daily_loss_gate_pct)
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
        current_regime: str,
        current_equity: float,
    ) -> OpenTrade | None:
        if strategy.name in session.signals_fired:
            return None
        if current_regime != strategy.regime:
            return None
        if len(session.open_trades) >= self.portfolio_config.risk.max_open_positions:
            return None
        if self._regime_position_count(session, strategy.regime) >= self.portfolio_config.risk.max_positions_per_regime:
            return None
        if any(trade["strategy_name"] == strategy.name for trade in session.open_trades):
            return None
        legs = self._select_legs(strategy, option_chain, date.fromisoformat(session.trade_date))
        if not legs:
            return None
        entry_debit = _combo_debit_from_marks(legs)
        leg_payloads = []
        for leg in legs:
            leg_payloads.append(
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
            )
        max_loss_per_combo, max_profit_per_combo = _estimate_combo_bounds(leg_payloads)
        if max_loss_per_combo <= 0.0:
            return None
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
            return None
        debit_cash = max(0.0, entry_debit * CONTRACT_MULTIPLIER)
        quantity_by_cash = (
            math.floor(max(0.0, session.virtual_cash) / debit_cash)
            if debit_cash > 0.0
            else strategy.max_contracts
        )
        quantity = min(strategy.max_contracts, quantity_by_risk, quantity_by_cash)
        if quantity < 1:
            return None
        return OpenTrade(
            strategy_name=strategy.name,
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
            entry_order_class="mleg" if len(legs) > 1 else "simple",
            entry_order_type="limit",
            entry_fill_price=entry_debit if len(legs) > 1 else legs[0].mark,
            legs=leg_payloads,
        )

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

    def _multileg_entry_order_requests(self, trade: OpenTrade) -> list[OrderRequest]:
        legs = [
            SelectedLeg(
                symbol=str(leg["symbol"]),
                expiration_date=str(leg["expiration_date"]),
                option_type=str(leg["option_type"]),
                side=str(leg["side"]),
                strike_price=float(leg["strike_price"]),
                target_delta=float(leg["target_delta"]),
                bid=float(leg["bid"]),
                ask=float(leg["ask"]),
                mark=float(leg["mark"]),
                delta=float(leg["delta"]),
                gamma=float(leg["gamma"]),
                theta=float(leg["theta"]),
                vega=float(leg["vega"]),
                quote_time=None,
            )
            for leg in trade.legs
        ]
        mid_debit = _combo_debit_from_marks(legs)
        natural_debit = _combo_debit_from_natural(legs)
        limits = [_round_limit_price(mid_debit + 0.02), _round_limit_price(natural_debit)]
        return [
            self.broker.build_multileg_order_request(
                strategy_name=trade.strategy_name,
                qty=trade.quantity,
                order_type="limit",
                time_in_force="day",
                limit_price=limit_price,
                legs=[leg.open_order_leg() for leg in legs],
            )
            for limit_price in limits
        ]

    def _simple_exit_order_requests(
        self, trade: OpenTrade, mark_map: dict[str, float], *, market_fallback: bool
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

    def _multileg_exit_order_requests(
        self, trade: OpenTrade, mark_map: dict[str, float], *, market_fallback: bool
    ) -> list[OrderRequest]:
        selected_legs = [
            SelectedLeg(
                symbol=str(leg["symbol"]),
                expiration_date=str(leg["expiration_date"]),
                option_type=str(leg["option_type"]),
                side=str(leg["side"]),
                strike_price=float(leg["strike_price"]),
                target_delta=float(leg["target_delta"]),
                bid=float(leg["bid"]),
                ask=float(leg["ask"]),
                mark=float(mark_map[str(leg["symbol"])]),
                delta=float(leg["delta"]),
                gamma=float(leg["gamma"]),
                theta=float(leg["theta"]),
                vega=float(leg["vega"]),
                quote_time=None,
            )
            for leg in trade.legs
        ]
        mid_debit = _combo_debit_from_marks(selected_legs)
        natural_debit = _combo_debit_from_natural(selected_legs)
        requests = [
            self.broker.build_multileg_order_request(
                strategy_name=f"{trade.strategy_name}_exit",
                qty=int(trade.quantity),
                order_type="limit",
                time_in_force="day",
                limit_price=_round_limit_price(mid_debit + 0.02),
                legs=[leg.close_order_leg() for leg in selected_legs],
            ),
            self.broker.build_multileg_order_request(
                strategy_name=f"{trade.strategy_name}_exit",
                qty=int(trade.quantity),
                order_type="limit",
                time_in_force="day",
                limit_price=_round_limit_price(natural_debit),
                legs=[leg.close_order_leg() for leg in selected_legs],
            ),
        ]
        if market_fallback:
            requests.append(
                self.broker.build_multileg_order_request(
                    strategy_name=f"{trade.strategy_name}_exit",
                    qty=int(trade.quantity),
                    order_type="market",
                    time_in_force="day",
                    legs=[leg.close_order_leg() for leg in selected_legs],
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
        self, requests: list[OrderRequest], *, journal_name: str
    ) -> tuple[dict[str, Any], float]:
        run_dir = self._session_run_dir(_now_et().date())
        for request in requests:
            response = self.broker.submit_order(
                request,
                dry_run=not self.submit_paper_orders,
                explicitly_requested=self.submit_paper_orders,
            )
            append_journal_entry(run_dir / "order_journal.json", {"journal": journal_name, "response": response})
            if response.get("status") == "dry_run":
                fallback_price = 0.0 if request.order_type == "market" else float(request.limit_price or 0.0)
                return response, fallback_price
            order_id = str(response.get("id") or "")
            terminal = self._wait_for_terminal_order(order_id)
            append_journal_entry(run_dir / "order_journal.json", {"journal": journal_name, "terminal": terminal})
            if self._is_filled(terminal):
                filled_avg_price = float(terminal.get("filled_avg_price") or request.limit_price or 0.0)
                return terminal, filled_avg_price
            if str(terminal.get("status", "")) in OPEN_STATUSES:
                self.broker.cancel_order(order_id, dry_run=False, explicitly_requested=True)
        return {"status": "not_filled"}, 0.0

    def _alert(self, session: SessionState, level: str, message: str) -> None:
        alert = {"timestamp_et": _now_et().isoformat(), "level": level, "message": message}
        session.alerts.append(alert)
        self.logger.warning("qqq portfolio alert %s", alert)

    def _expected_entry_greeks(self, trade: OpenTrade) -> tuple[float, float]:
        delta_shares = 0.0
        vega_dollars = 0.0
        for leg in trade.legs:
            sign = 1.0 if leg["side"] == "long" else -1.0
            delta_shares += sign * float(leg["delta"]) * CONTRACT_MULTIPLIER
            vega_dollars += sign * float(leg["vega"]) * CONTRACT_MULTIPLIER
        return delta_shares * int(trade.quantity), vega_dollars * int(trade.quantity)

    def _run_entry(
        self,
        trade: OpenTrade,
        session: SessionState,
        current_equity: float,
    ) -> bool:
        delta_shares, vega_dollars = self._expected_entry_greeks(trade)
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
        requests = (
            self._simple_entry_order_requests(trade)
            if trade.entry_order_class == "simple"
            else self._multileg_entry_order_requests(trade)
        )
        response, fill_price = self._execute_attempts(requests, journal_name=f"{trade.strategy_name}_entry")
        if response.get("status") == "not_filled":
            self._alert(session, "warning", f"{trade.strategy_name} entry did not fill")
            return False
        trade.entry_order_id = str(response.get("id") or "") if response.get("id") else None
        if trade.entry_order_class == "mleg":
            trade.entry_fill_price = fill_price
            trade.entry_debit = fill_price
        else:
            trade.entry_fill_price = fill_price if fill_price > 0.0 else trade.entry_fill_price
            trade.entry_debit = trade.entry_fill_price
            trade.legs[0]["entry_fill_price"] = trade.entry_fill_price
        session.virtual_cash += _entry_cashflow_from_debit(
            float(trade.entry_debit), int(trade.quantity), len(trade.legs)
        )
        session.open_trades.append(asdict(trade))
        session.signals_fired.append(trade.strategy_name)
        self.logger.info(
            "entered %s qty=%s equity=%.2f submit=%s",
            trade.strategy_name,
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
        option_chain: pd.DataFrame,
        spot_price: float,
        current_minute: int,
        exit_reason: str,
    ) -> bool:
        trade = OpenTrade(**trade_payload)
        mark_map = self._mark_to_close(trade, option_chain)
        if len(mark_map) != len(trade.legs):
            return False
        market_fallback = (
            self.portfolio_config.execution.allow_market_exit_fallback
            and current_minute >= self.portfolio_config.execution.market_exit_fallback_minute
        )
        requests = (
            self._simple_exit_order_requests(trade, mark_map, market_fallback=market_fallback)
            if trade.entry_order_class == "simple"
            else self._multileg_exit_order_requests(trade, mark_map, market_fallback=market_fallback)
        )
        response, fill_price = self._execute_attempts(requests, journal_name=f"{trade.strategy_name}_exit")
        if response.get("status") == "not_filled":
            self._alert(session, "warning", f"{trade.strategy_name} exit did not fill")
            return False
        if trade.entry_order_class == "simple":
            exit_cashflow = _exit_cashflow_from_fill(
                fill_price=fill_price,
                quantity=int(trade.quantity),
                leg_count=len(trade.legs),
                order_class="simple",
                side="sell",
            )
        else:
            exit_cashflow = _exit_cashflow_from_fill(
                fill_price=fill_price,
                quantity=int(trade.quantity),
                leg_count=len(trade.legs),
                order_class="mleg",
            )
        session.virtual_cash += exit_cashflow
        delta_shares, vega_dollars = self._expected_entry_greeks(trade)
        net_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), int(trade.quantity), len(trade.legs))
            + exit_cashflow
        )
        completed = CompletedTrade(
            strategy_name=trade.strategy_name,
            regime=trade.regime,
            quantity=int(trade.quantity),
            entry_time_et=trade.entry_time_et,
            exit_time_et=_now_et().isoformat(),
            entry_minute=int(trade.entry_minute),
            exit_minute=current_minute,
            entry_fill_price=float(trade.entry_fill_price),
            exit_fill_price=float(fill_price),
            underlying_entry=float(trade.underlying_entry),
            underlying_exit=spot_price,
            exit_reason=exit_reason,
            entry_order_id=trade.entry_order_id,
            exit_order_id=str(response.get("id") or "") if response.get("id") else None,
            net_pnl=round(net_pnl, 4),
            max_loss_per_combo=float(trade.max_loss_per_combo),
            max_profit_per_combo=float(trade.max_profit_per_combo),
            delta_shares_at_entry=round(delta_shares, 4),
            vega_dollars_1pct_at_entry=round(vega_dollars, 4),
            legs=list(trade.legs),
        )
        session.completed_trades.append(asdict(completed))
        session.open_trades = [
            item for item in session.open_trades if item["strategy_name"] != trade.strategy_name
        ]
        self.logger.info("exited %s reason=%s pnl=%.2f", trade.strategy_name, exit_reason, net_pnl)
        return True

    def _reconcile_and_trade(
        self,
        *,
        session: SessionState,
        ledger: PortfolioLedger,
        stock_frame: pd.DataFrame,
    ) -> None:
        features = extract_session_features(stock_frame)
        if features is None:
            return
        current_regime = infer_intraday_regime(features)
        session.last_regime = current_regime
        contracts = self._refresh_contract_cache_if_needed(features.trade_date)
        symbols, metadata = self._candidate_symbols_for_snapshot(
            contracts, features.latest_close, features.trade_date
        )
        option_chain = self._fetch_option_chain(symbols, metadata, features.latest_close, features.trade_date)
        open_symbols = [
            str(leg["symbol"])
            for trade in session.open_trades
            for leg in trade["legs"]
            if str(leg["symbol"]) not in symbols
        ]
        if open_symbols:
            open_metadata = {item["symbol"]: item for item in contracts if item["symbol"] in open_symbols}
            extra_chain = self._fetch_option_chain(open_symbols, open_metadata, features.latest_close, features.trade_date)
            if not extra_chain.empty:
                option_chain = pd.concat([option_chain, extra_chain], ignore_index=True)
        current_mark_map = {
            str(row.symbol): float(row.mark)
            for row in option_chain.itertuples(index=False)
            if pd.notna(row.mark)
        }
        current_equity = _current_equity(session, current_mark_map)
        loss_gate, reason = self._daily_loss_gate_check(session, current_equity)
        if loss_gate:
            session.blocked_new_entries = True
            session.block_reason = reason
        exiting: list[tuple[dict[str, Any], str]] = []
        for trade_payload in list(session.open_trades):
            trade = OpenTrade(**trade_payload)
            mark_map = self._mark_to_close(trade, option_chain)
            if len(mark_map) != len(trade.legs):
                continue
            should_exit, exit_reason, _current_pnl = self._should_exit_trade(
                trade, features.minute_index, mark_map
            )
            if should_exit:
                exiting.append((trade_payload, exit_reason))
        for trade_payload, exit_reason in exiting:
            self._run_exit(
                trade_payload,
                session,
                option_chain,
                features.latest_close,
                features.minute_index,
                exit_reason,
            )
        current_equity = _current_equity(session, current_mark_map)
        if session.blocked_new_entries or current_regime == "neutral":
            return
        for strategy in self.portfolio_config.strategies:
            if strategy.regime != current_regime:
                continue
            if not signal_is_true(strategy.signal_name, features):
                continue
            open_trade = self._evaluate_entry(
                strategy=strategy,
                session=session,
                ledger=ledger,
                option_chain=option_chain,
                spot_price=features.latest_close,
                current_minute=features.minute_index,
                current_regime=current_regime,
                current_equity=current_equity,
            )
            if open_trade is None:
                continue
            self._run_entry(open_trade, session, current_equity)

    def _flatten_all(self, session: SessionState, stock_frame: pd.DataFrame) -> None:
        if not session.open_trades:
            return
        features = extract_session_features(stock_frame)
        if features is None:
            return
        contracts = self._refresh_contract_cache_if_needed(features.trade_date)
        open_symbols = [str(leg["symbol"]) for trade in session.open_trades for leg in trade["legs"]]
        metadata = {item["symbol"]: item for item in contracts if item["symbol"] in open_symbols}
        option_chain = self._fetch_option_chain(open_symbols, metadata, features.latest_close, features.trade_date)
        for trade_payload in list(session.open_trades):
            self._run_exit(
                trade_payload,
                session,
                option_chain,
                features.latest_close,
                MINUTES_PER_RTH_SESSION - 1,
                "forced_flatten",
            )

    def finalize_session(
        self,
        session: SessionState,
        ledger: PortfolioLedger,
        *,
        stock_frame: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        if stock_frame is not None:
            self._flatten_all(session, stock_frame)
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
        summary = {
            "trade_date": session.trade_date,
            "submit_paper_orders": self.submit_paper_orders,
            "starting_equity": round(session.starting_equity, 2),
            "ending_equity": round(ending_equity, 2),
            "net_pnl": round(ending_equity - session.starting_equity, 2),
            "completed_trade_count": len(session.completed_trades),
            "blocked_new_entries": session.blocked_new_entries,
            "block_reason": session.block_reason,
            "last_regime": session.last_regime,
        }
        summary_paths = write_summary_bundle(
            run_dir,
            name="qqq_portfolio_session_summary",
            summary=summary,
            table_map={"completed_trades": completed_df},
        )
        summary_paths["alerts"] = write_alert_queue(run_dir / "alerts.json", session.alerts)
        return summary

    def run(self, *, run_once: bool = False) -> dict[str, Any]:
        clock = self.broker.get_clock()
        trade_date = _trade_date_from_clock(clock)
        now_et = _now_et()
        ledger = self.load_ledger()
        session = self.load_or_create_session(trade_date, ledger)
        if not bool(clock.get("is_open", False)):
            if now_et < _rth_open_for(trade_date):
                seconds_to_open = (_rth_open_for(trade_date) - now_et).total_seconds()
                if run_once:
                    return {
                        "status": "before_open",
                        "trade_date": trade_date.isoformat(),
                        "seconds_to_open": int(seconds_to_open),
                    }
                time.sleep(max(1.0, min(60.0, seconds_to_open)))
            else:
                stock_frame = self._fetch_today_stock_frame(trade_date)
                summary = self.finalize_session(session, ledger, stock_frame=stock_frame)
                summary["status"] = "after_close"
                return summary
        while True:
            stock_frame = self._fetch_today_stock_frame(trade_date)
            if not stock_frame.empty:
                self._reconcile_and_trade(session=session, ledger=ledger, stock_frame=stock_frame)
                self.save_session(session)
            if run_once:
                return {
                    "status": "ran_once",
                    "trade_date": trade_date.isoformat(),
                    "open_trades": len(session.open_trades),
                    "completed_trades": len(session.completed_trades),
                    "last_regime": session.last_regime,
                    "blocked_new_entries": session.blocked_new_entries,
                }
            clock = self.broker.get_clock()
            if not bool(clock.get("is_open", False)):
                summary = self.finalize_session(session, ledger, stock_frame=stock_frame)
                summary["status"] = "session_complete"
                return summary
            time.sleep(self.portfolio_config.execution.poll_interval_seconds)

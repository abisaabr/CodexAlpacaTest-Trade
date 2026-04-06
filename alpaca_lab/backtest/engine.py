from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd

from alpaca_lab.backtest.metrics import summarize_backtest
from alpaca_lab.strategies.base import BaseStrategy


@dataclass(slots=True)
class LinearCostModel:
    slippage_bps: float = 5.0
    fee_per_unit: float = 0.0

    def apply_entry(self, price: float, direction: int) -> float:
        return float(price * (1 + (self.slippage_bps / 10000.0) * direction))

    def apply_exit(self, price: float, direction: int) -> float:
        return float(price * (1 - (self.slippage_bps / 10000.0) * direction))

    def estimate_fees(self, quantity: float) -> float:
        return float(abs(quantity) * self.fee_per_unit)


@dataclass(slots=True)
class FixedFractionSizer:
    base_allocation_fraction: float = 0.10
    minimum_quantity: int = 1
    round_to_whole_units: bool = True

    def size(
        self,
        *,
        equity: float,
        entry_price: float,
        contract_multiplier: float,
        size_fraction: float,
    ) -> float:
        if equity <= 0 or entry_price <= 0 or contract_multiplier <= 0:
            return 0.0
        budget = equity * self.base_allocation_fraction * max(size_fraction, 0.0)
        if budget <= 0:
            return 0.0
        raw_quantity = budget / (entry_price * contract_multiplier)
        if self.round_to_whole_units:
            raw_quantity = math.floor(raw_quantity)
        if raw_quantity < self.minimum_quantity:
            return 0.0
        return float(raw_quantity)


@dataclass(slots=True)
class BacktestResult:
    strategy_name: str
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    summary: dict[str, Any]


@dataclass(slots=True)
class _OpenPosition:
    symbol: str
    direction: int
    quantity: float
    contract_multiplier: float
    entry_time: pd.Timestamp
    entry_price: float
    stop_price: float | None
    target_price: float | None
    timeout_bars: int
    bars_held: int
    entry_fee: float


def _derive_stop_price(entry_price: float, direction: int, stop_pct: float | None) -> float | None:
    if not stop_pct:
        return None
    if direction > 0:
        return float(entry_price * (1 - stop_pct))
    return float(entry_price * (1 + stop_pct))


def _derive_target_price(
    entry_price: float, direction: int, target_pct: float | None
) -> float | None:
    if not target_pct:
        return None
    if direction > 0:
        return float(entry_price * (1 + target_pct))
    return float(entry_price * (1 - target_pct))


def _empty_trade_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "entry_time",
            "exit_time",
            "direction",
            "quantity",
            "entry_price",
            "exit_price",
            "notional",
            "pnl",
            "return_pct",
            "fees",
            "bars_held",
            "exit_reason",
        ]
    )


def run_backtest(
    bars: pd.DataFrame,
    strategy: BaseStrategy,
    *,
    initial_cash: float = 100_000.0,
    cost_model: LinearCostModel | None = None,
    position_sizer: FixedFractionSizer | None = None,
    default_timeout_bars: int = 10,
) -> BacktestResult:
    cost_model = cost_model or LinearCostModel()
    position_sizer = position_sizer or FixedFractionSizer()
    signal_frame = (
        strategy.generate_signals(bars.copy())
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
    )

    cash = float(initial_cash)
    positions: dict[str, _OpenPosition] = {}
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    last_close_by_symbol: dict[str, float] = {}

    for timestamp, group in signal_frame.groupby("timestamp", sort=True):
        equity_snapshot = equity_rows[-1]["equity"] if equity_rows else initial_cash

        for row in group.itertuples(index=False):
            symbol = row.symbol
            close = float(row.close)
            high = float(getattr(row, "high", close))
            low = float(getattr(row, "low", close))
            signal = int(getattr(row, "signal", 0))
            last_close_by_symbol[symbol] = close

            position = positions.get(symbol)
            if position is not None:
                position.bars_held += 1
                exit_reason: str | None = None
                raw_exit_price = close

                if position.direction > 0:
                    if position.stop_price is not None and low <= position.stop_price:
                        exit_reason = "stop"
                        raw_exit_price = position.stop_price
                    elif position.target_price is not None and high >= position.target_price:
                        exit_reason = "target"
                        raw_exit_price = position.target_price
                else:
                    if position.stop_price is not None and high >= position.stop_price:
                        exit_reason = "stop"
                        raw_exit_price = position.stop_price
                    elif position.target_price is not None and low <= position.target_price:
                        exit_reason = "target"
                        raw_exit_price = position.target_price

                if exit_reason is None and position.bars_held >= max(position.timeout_bars, 1):
                    exit_reason = "timeout"
                if exit_reason is None and signal * position.direction < 0:
                    exit_reason = "signal_flip"

                if exit_reason is not None:
                    exit_price = cost_model.apply_exit(raw_exit_price, position.direction)
                    exit_fee = cost_model.estimate_fees(position.quantity)
                    notional = (
                        position.quantity * position.contract_multiplier * position.entry_price
                    )
                    if position.direction > 0:
                        cash += (
                            position.quantity * position.contract_multiplier * exit_price - exit_fee
                        )
                    else:
                        cash -= (
                            position.quantity * position.contract_multiplier * exit_price + exit_fee
                        )
                    pnl = (
                        position.direction
                        * position.quantity
                        * position.contract_multiplier
                        * (exit_price - position.entry_price)
                        - position.entry_fee
                        - exit_fee
                    )
                    trade_rows.append(
                        {
                            "symbol": symbol,
                            "entry_time": position.entry_time,
                            "exit_time": timestamp,
                            "direction": position.direction,
                            "quantity": position.quantity,
                            "entry_price": position.entry_price,
                            "exit_price": exit_price,
                            "notional": notional,
                            "pnl": pnl,
                            "return_pct": pnl / notional if notional else 0.0,
                            "fees": position.entry_fee + exit_fee,
                            "bars_held": position.bars_held,
                            "exit_reason": exit_reason,
                        }
                    )
                    positions.pop(symbol)
                    position = None

            if position is None and signal != 0:
                contract_multiplier = float(
                    getattr(row, "contract_multiplier", strategy.contract_multiplier)
                )
                size_fraction = float(getattr(row, "size_fraction", 1.0))
                entry_price = cost_model.apply_entry(close, signal)
                quantity = position_sizer.size(
                    equity=equity_snapshot,
                    entry_price=entry_price,
                    contract_multiplier=contract_multiplier,
                    size_fraction=size_fraction,
                )
                if quantity > 0:
                    entry_fee = cost_model.estimate_fees(quantity)
                    notional = quantity * contract_multiplier * entry_price
                    if signal > 0:
                        cash -= notional + entry_fee
                    else:
                        cash += notional - entry_fee
                    positions[symbol] = _OpenPosition(
                        symbol=symbol,
                        direction=signal,
                        quantity=quantity,
                        contract_multiplier=contract_multiplier,
                        entry_time=timestamp,
                        entry_price=entry_price,
                        stop_price=_derive_stop_price(
                            entry_price, signal, getattr(row, "stop_pct", None)
                        ),
                        target_price=_derive_target_price(
                            entry_price, signal, getattr(row, "target_pct", None)
                        ),
                        timeout_bars=int(getattr(row, "timeout_bars", default_timeout_bars)),
                        bars_held=0,
                        entry_fee=entry_fee,
                    )

        market_value = 0.0
        gross_exposure = 0.0
        for position in positions.values():
            current_price = last_close_by_symbol.get(position.symbol, position.entry_price)
            market_value += (
                position.direction
                * position.quantity
                * position.contract_multiplier
                * current_price
            )
            gross_exposure += position.quantity * position.contract_multiplier * current_price
        equity_rows.append(
            {
                "timestamp": timestamp,
                "cash": cash,
                "gross_exposure": gross_exposure,
                "equity": cash + market_value,
            }
        )

    final_timestamp = (
        signal_frame["timestamp"].max() if not signal_frame.empty else pd.Timestamp.utcnow()
    )
    for symbol, position in list(positions.items()):
        final_close = last_close_by_symbol.get(symbol, position.entry_price)
        exit_price = cost_model.apply_exit(final_close, position.direction)
        exit_fee = cost_model.estimate_fees(position.quantity)
        notional = position.quantity * position.contract_multiplier * position.entry_price
        if position.direction > 0:
            cash += position.quantity * position.contract_multiplier * exit_price - exit_fee
        else:
            cash -= position.quantity * position.contract_multiplier * exit_price + exit_fee
        pnl = (
            position.direction
            * position.quantity
            * position.contract_multiplier
            * (exit_price - position.entry_price)
            - position.entry_fee
            - exit_fee
        )
        trade_rows.append(
            {
                "symbol": symbol,
                "entry_time": position.entry_time,
                "exit_time": final_timestamp,
                "direction": position.direction,
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "exit_price": exit_price,
                "notional": notional,
                "pnl": pnl,
                "return_pct": pnl / notional if notional else 0.0,
                "fees": position.entry_fee + exit_fee,
                "bars_held": position.bars_held,
                "exit_reason": "end_of_data",
            }
        )

    trades = pd.DataFrame(trade_rows) if trade_rows else _empty_trade_frame()
    equity_curve = pd.DataFrame(equity_rows)
    if equity_curve.empty:
        equity_curve = pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp.utcnow(),
                    "cash": initial_cash,
                    "gross_exposure": 0.0,
                    "equity": initial_cash,
                }
            ]
        )
    summary = summarize_backtest(trades, equity_curve)
    return BacktestResult(
        strategy_name=strategy.name, trades=trades, equity_curve=equity_curve, summary=summary
    )

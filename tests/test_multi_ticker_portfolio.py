from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca_lab.brokers.alpaca import OrderRequest
from alpaca_lab.config import LabSettings
from alpaca_lab.multi_ticker_portfolio.config import (
    EventBlackoutConfig,
    default_portfolio_config,
    load_portfolio_config,
)
from alpaca_lab.multi_ticker_portfolio.signals import signal_is_true
from alpaca_lab.multi_ticker_portfolio.trader import (
    AUTO_FLATTEN_UNEXPECTED_INTRADAY_REASON,
    MultiTickerPortfolioPaperTrader,
    OpenTrade,
    PortfolioLedger,
    SelectedLeg,
    SessionState,
    SymbolSnapshot,
)


def _build_frame(rows: int, *, close_fn, vwap_offset: float = -0.05, ema_fast_offset: float = 0.02, ema_slow_offset: float = 0.0) -> pd.DataFrame:
    data = []
    for minute_index in range(rows):
        close = close_fn(minute_index)
        data.append(
            {
                "timestamp_et": datetime(2026, 4, 13, 9, 30) + pd.Timedelta(minutes=minute_index),
                "minute_index": minute_index,
                "open": 100.0,
                "high": max(100.1, close + 0.05),
                "low": min(99.9, close - 0.05),
                "close": close,
                "intraday_vwap": close + vwap_offset,
                "ema_fast": close + ema_fast_offset,
                "ema_slow": close + ema_slow_offset,
            }
        )
    return pd.DataFrame(data)


def _sample_open_trade(
    *,
    strategy_name: str,
    underlying_symbol: str,
    regime: str = "bull",
    quantity: int = 1,
    delta: float = 0.40,
    vega: float = 0.08,
) -> dict[str, object]:
    return asdict(
        OpenTrade(
            strategy_name=strategy_name,
            underlying_symbol=underlying_symbol,
            regime=regime,
            quantity=quantity,
            entry_time_et="2026-04-15T10:30:00-04:00",
            entry_minute=60,
            hard_exit_minute=360,
            underlying_entry=500.0,
            entry_debit=3.0,
            max_loss_per_combo=300.0,
            max_profit_per_combo=900.0,
            profit_target_dollars=135.0,
            stop_loss_dollars=90.0,
            entry_order_id=None,
            entry_fill_price=3.0,
            legs=[
                {
                    "symbol": f"{underlying_symbol}260417C00600000",
                    "expiration_date": "2026-04-17",
                    "option_type": "call",
                    "side": "long",
                    "strike_price": 600.0,
                    "target_delta": delta,
                    "entry_fill_price": 3.0,
                    "bid": 2.95,
                    "ask": 3.05,
                    "mark": 3.0,
                    "delta": delta,
                    "gamma": 0.06,
                    "theta": -0.09,
                    "vega": vega,
                }
            ],
            entry_attempt_id=f"{strategy_name}-attempt",
        )
    )


def test_simple_order_requests_generate_unique_client_order_ids(monkeypatch) -> None:
    class _BrokerStub:
        def build_order_request(self, **kwargs) -> OrderRequest:
            kwargs["client_order_id"] = str(kwargs.pop("client_order_key", "")) or None
            return OrderRequest(**kwargs)

    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.broker = _BrokerStub()
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._now_et",
        lambda: datetime(2026, 4, 16, 10, 15, 30, 123456, tzinfo=ZoneInfo("America/New_York")),
    )

    trade = OpenTrade(
        **_sample_open_trade(
            strategy_name="qqq__fast__trend_long_call_next_expiry",
            underlying_symbol="QQQ",
        )
    )
    trade.legs[0]["mark"] = 1.18
    trade.legs[0]["ask"] = 1.20
    trade.legs[0]["bid"] = 1.00

    entry_requests = trader._simple_entry_order_requests(trade)
    exit_requests = trader._simple_exit_order_requests(
        trade,
        {str(trade.legs[0]["symbol"]): 1.02},
        market_fallback=True,
    )

    assert len({request.client_order_id for request in entry_requests}) == len(entry_requests)
    assert len({request.client_order_id for request in exit_requests}) == len(exit_requests)


def test_default_multi_ticker_portfolio_contains_all_symbols() -> None:
    config = default_portfolio_config()

    counts = Counter(strategy.underlying_symbol for strategy in config.strategies)
    ordered_symbols: list[str] = []
    for strategy in config.strategies:
        if strategy.underlying_symbol not in ordered_symbols:
            ordered_symbols.append(strategy.underlying_symbol)

    assert tuple(config.execution.underlying_symbols) == tuple(ordered_symbols)
    assert config.strategy_manifest_path is not None
    assert config.strategy_manifest_path.name == "multi_ticker_portfolio_live.yaml"
    assert len(config.strategies) == len({strategy.name for strategy in config.strategies})
    assert all(counts[symbol] >= 1 for symbol in config.execution.underlying_symbols)
    assert counts["QQQ"] >= 1
    assert counts["SPY"] >= 1
    assert counts["IWM"] >= 1
    assert counts["NVDA"] >= 1
    assert counts["TSLA"] >= 1


def test_default_multi_ticker_portfolio_includes_current_choppy_strategy() -> None:
    config = default_portfolio_config()
    alias = next(
        strategy
        for strategy in config.strategies
        if strategy.name == "qqq__fast__iron_butterfly_same_day"
    )

    assert alias.underlying_symbol == "QQQ"
    assert alias.regime == "choppy"
    assert alias.signal_name == "iron_condor"


def test_fast_trend_call_triggers_before_base_profile() -> None:
    frame = _build_frame(
        36,
        close_fn=lambda idx: 100.0 + 0.008 * idx,
        vwap_offset=-0.12,
        ema_fast_offset=0.04,
        ema_slow_offset=-0.02,
    )

    assert signal_is_true("trend_call", frame, timing_profile="fast") is True
    assert signal_is_true("trend_call", frame, timing_profile="base") is False


def test_reactive_trend_call_triggers_before_fast_profile() -> None:
    frame = _build_frame(
        26,
        close_fn=lambda idx: 100.0 + 0.010 * idx,
        vwap_offset=-0.12,
        ema_fast_offset=0.05,
        ema_slow_offset=-0.03,
    )

    assert signal_is_true("trend_call", frame, timing_profile="reactive") is True
    assert signal_is_true("trend_call", frame, timing_profile="fast") is False


def test_fast_orb_put_triggers_before_base_profile() -> None:
    def close_fn(idx: int) -> float:
        if idx < 10:
            return 100.0 + idx * 0.02
        if idx == 12:
            return 99.4
        return 99.95

    frame = _build_frame(
        13,
        close_fn=close_fn,
        vwap_offset=0.10,
        ema_fast_offset=-0.05,
        ema_slow_offset=0.03,
    )
    frame.loc[:9, "low"] = 99.9
    frame.loc[12, "low"] = 99.35

    assert signal_is_true("orb_put", frame, timing_profile="fast") is True
    assert signal_is_true("orb_put", frame, timing_profile="base") is False


def test_slow_trend_call_triggers_before_patient_profile() -> None:
    frame = _build_frame(
        70,
        close_fn=lambda idx: 100.0 + 0.008 * idx,
        vwap_offset=-0.10,
        ema_fast_offset=0.04,
        ema_slow_offset=-0.02,
    )

    assert signal_is_true("trend_call", frame, timing_profile="slow") is True
    assert signal_is_true("trend_call", frame, timing_profile="patient") is False


def test_portfolio_config_allows_disabling_daily_loss_gate(tmp_path: Path) -> None:
    config_path = tmp_path / "portfolio.yaml"
    config_path.write_text(
        "risk:\n"
        "  daily_loss_gate_pct: null\n"
        "  delever_drawdown_pct: 8.0\n"
        "  delever_risk_scale: 0.5\n",
        encoding="utf-8",
    )

    config = load_portfolio_config(config_path)

    assert config.risk.daily_loss_gate_pct is None
    assert config.risk.delever_drawdown_pct == 8.0
    assert config.risk.delever_risk_scale == 0.5


def test_portfolio_config_loads_risk_controls_overlay(tmp_path: Path) -> None:
    risk_controls_path = tmp_path / "risk_controls.yaml"
    risk_controls_path.write_text(
        "risk:\n"
        "  broker_min_equity_to_trade: 31000\n"
        "  broker_equity_emergency_stop: 30500\n"
        "execution:\n"
        "  max_relative_spread: 0.22\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "portfolio.yaml"
    config_path.write_text(
        f"risk_controls_path: {risk_controls_path.name}\n"
        "execution:\n"
        "  stock_feed: iex\n",
        encoding="utf-8",
    )

    config = load_portfolio_config(config_path)

    assert config.risk.broker_min_equity_to_trade == 31_000
    assert config.risk.broker_equity_emergency_stop == 30_500
    assert config.execution.max_relative_spread == 0.22
    assert config.execution.stock_feed == "iex"


def test_portfolio_config_loads_strategies_from_manifest_path(tmp_path: Path) -> None:
    strategy = default_portfolio_config().strategies[0].model_dump(mode="python")
    strategy["timing_profile"] = "reactive"
    manifest_path = tmp_path / "strategy_manifest.yaml"
    manifest_path.write_text(
        "version: 1\n"
        "strategies:\n"
        f"  - name: {strategy['name']}\n"
        f"    underlying_symbol: {strategy['underlying_symbol']}\n"
        f"    regime: {strategy['regime']}\n"
        f"    family: {strategy['family']}\n"
        f"    description: {strategy['description']}\n"
        f"    dte_mode: {strategy['dte_mode']}\n"
        f"    signal_name: {strategy['signal_name']}\n"
        f"    timing_profile: {strategy['timing_profile']}\n"
        f"    hard_exit_minute: {strategy['hard_exit_minute']}\n"
        f"    risk_fraction: {strategy['risk_fraction']}\n"
        f"    max_contracts: {strategy['max_contracts']}\n"
        f"    profit_target_multiple: {strategy['profit_target_multiple']}\n"
        f"    stop_loss_multiple: {strategy['stop_loss_multiple']}\n"
        "    legs:\n"
        f"      - option_type: {strategy['legs'][0]['option_type']}\n"
        f"        side: {strategy['legs'][0]['side']}\n"
        f"        target_delta: {strategy['legs'][0]['target_delta']}\n"
        f"        min_abs_delta: {strategy['legs'][0]['min_abs_delta']}\n"
        f"        max_abs_delta: {strategy['legs'][0]['max_abs_delta']}\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "portfolio.yaml"
    config_path.write_text(
        f"strategy_manifest_path: {manifest_path.name}\n",
        encoding="utf-8",
    )

    config = load_portfolio_config(config_path)

    assert config.strategy_manifest_path == manifest_path.resolve()
    assert len(config.strategies) == 1
    assert config.strategies[0].name == strategy["name"]


def test_disabled_daily_loss_gate_never_blocks_entries() -> None:
    config = default_portfolio_config()
    config = config.model_copy(
        update={
            "risk": config.risk.model_copy(
                update={
                    "daily_loss_gate_pct": None,
                }
            )
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    session = SessionState(
        trade_date="2026-04-13",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    blocked, reason = trader._daily_loss_gate_check(session, current_equity=20_000.0)

    assert blocked is False
    assert reason is None


def test_evaluate_entry_respects_per_symbol_risk_cap() -> None:
    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={"underlying_symbols": ("QQQ",)}
            ),
            "strategies": tuple(
                strategy
                for strategy in default_portfolio_config().strategies
                if strategy.name == "qqq__fast__trend_long_call_next_expiry"
            ),
        }
    )
    strategy = config.strategies[0]
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._select_legs = lambda *_args, **_kwargs: [
        SelectedLeg(
            symbol="QQQ260417C00600000",
            expiration_date="2026-04-17",
            option_type="call",
            side="long",
            strike_price=600.0,
            target_delta=0.60,
            bid=2.95,
            ask=3.05,
            mark=3.0,
            delta=0.58,
            gamma=0.06,
            theta=-0.09,
            vega=0.12,
            quote_time=None,
        )
    ]
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
        open_trades=[
            {
                "strategy_name": "qqq__slow__trend_long_call_next_expiry",
                "underlying_symbol": "QQQ",
                "regime": "bull",
                "quantity": 1,
                "max_loss_per_combo": 1_100.0,
            }
        ],
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)

    open_trade, event = trader._evaluate_entry(
        strategy=strategy,
        session=session,
        ledger=ledger,
        option_chain=pd.DataFrame(),
        spot_price=500.0,
        current_minute=60,
        current_equity=25_000.0,
        broker_equity=30_000.0,
        attempt_id="attempt-symbol-cap",
    )

    assert open_trade is None
    assert event["decision_reason"] == "per_symbol_risk_cap"


def test_evaluate_entry_respects_bucket_risk_cap() -> None:
    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={"underlying_symbols": ("QQQ", "SPY")}
            ),
            "strategies": tuple(
                strategy
                for strategy in default_portfolio_config().strategies
                if strategy.name == "qqq__fast__trend_long_call_next_expiry"
            ),
        }
    )
    strategy = config.strategies[0]
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._select_legs = lambda *_args, **_kwargs: [
        SelectedLeg(
            symbol="QQQ260417C00600000",
            expiration_date="2026-04-17",
            option_type="call",
            side="long",
            strike_price=600.0,
            target_delta=0.60,
            bid=2.95,
            ask=3.05,
            mark=3.0,
            delta=0.58,
            gamma=0.06,
            theta=-0.09,
            vega=0.12,
            quote_time=None,
        )
    ]
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
        open_trades=[
            {
                "strategy_name": "spy__fast__trend_long_call_next_expiry",
                "underlying_symbol": "SPY",
                "regime": "bull",
                "quantity": 1,
                "max_loss_per_combo": 1_900.0,
            }
        ],
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)

    open_trade, event = trader._evaluate_entry(
        strategy=strategy,
        session=session,
        ledger=ledger,
        option_chain=pd.DataFrame(),
        spot_price=500.0,
        current_minute=60,
        current_equity=25_000.0,
        broker_equity=30_000.0,
        attempt_id="attempt-bucket-cap",
    )

    assert open_trade is None
    assert event["decision_reason"] == "bucket_risk_cap:index_beta"


def test_evaluate_entry_blocks_on_projected_delta_hard_cap() -> None:
    base = default_portfolio_config()
    config = base.model_copy(
        update={
            "risk": base.risk.model_copy(
                update={
                    "hard_cap_delta_shares": 95.0,
                    "hard_cap_vega_dollars_1pct": None,
                }
            ),
            "execution": base.execution.model_copy(update={"underlying_symbols": ("QQQ", "SPY")}),
            "strategies": tuple(
                strategy for strategy in base.strategies if strategy.name == "qqq__fast__trend_long_call_next_expiry"
            ),
        }
    )
    strategy = config.strategies[0]
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._select_legs = lambda *_args, **_kwargs: [
        SelectedLeg(
            symbol="QQQ260417C00600000",
            expiration_date="2026-04-17",
            option_type="call",
            side="long",
            strike_price=600.0,
            target_delta=0.60,
            bid=2.95,
            ask=3.05,
            mark=3.0,
            delta=0.58,
            gamma=0.06,
            theta=-0.09,
            vega=0.12,
            quote_time=None,
        )
    ]
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
        open_trades=[
            _sample_open_trade(
                strategy_name="spy__fast__trend_long_call_next_expiry",
                underlying_symbol="SPY",
                delta=0.42,
                vega=0.04,
            )
        ],
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)

    open_trade, event = trader._evaluate_entry(
        strategy=strategy,
        session=session,
        ledger=ledger,
        option_chain=pd.DataFrame(),
        spot_price=500.0,
        current_minute=60,
        current_equity=25_000.0,
        broker_equity=30_000.0,
        attempt_id="attempt-delta-cap",
    )

    assert open_trade is None
    assert event["decision_reason"] == "projected_delta_hard_cap"
    assert event["projected_portfolio_delta_shares"] == 274.0


def test_evaluate_entry_blocks_on_projected_vega_hard_cap() -> None:
    base = default_portfolio_config()
    config = base.model_copy(
        update={
            "risk": base.risk.model_copy(
                update={
                    "hard_cap_delta_shares": None,
                    "hard_cap_vega_dollars_1pct": 18.0,
                }
            ),
            "execution": base.execution.model_copy(update={"underlying_symbols": ("QQQ", "SPY")}),
            "strategies": tuple(
                strategy for strategy in base.strategies if strategy.name == "qqq__fast__trend_long_call_next_expiry"
            ),
        }
    )
    strategy = config.strategies[0]
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._select_legs = lambda *_args, **_kwargs: [
        SelectedLeg(
            symbol="QQQ260417C00600000",
            expiration_date="2026-04-17",
            option_type="call",
            side="long",
            strike_price=600.0,
            target_delta=0.60,
            bid=2.95,
            ask=3.05,
            mark=3.0,
            delta=0.20,
            gamma=0.06,
            theta=-0.09,
            vega=0.12,
            quote_time=None,
        )
    ]
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
        open_trades=[
            _sample_open_trade(
                strategy_name="spy__fast__trend_long_call_next_expiry",
                underlying_symbol="SPY",
                delta=0.10,
                vega=0.07,
            )
        ],
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)

    open_trade, event = trader._evaluate_entry(
        strategy=strategy,
        session=session,
        ledger=ledger,
        option_chain=pd.DataFrame(),
        spot_price=500.0,
        current_minute=60,
        current_equity=25_000.0,
        broker_equity=30_000.0,
        attempt_id="attempt-vega-cap",
    )

    assert open_trade is None
    assert event["decision_reason"] == "projected_vega_hard_cap"
    assert event["projected_portfolio_vega_dollars_1pct"] == 55.0


def test_evaluate_entry_respects_same_day_entry_cutoff() -> None:
    base = default_portfolio_config()
    config = base.model_copy(
        update={
            "risk": base.risk.model_copy(
                update={
                    "entry_cutoff_minute": 345,
                    "same_day_entry_cutoff_minute": 300,
                }
            ),
            "execution": base.execution.model_copy(update={"underlying_symbols": ("QQQ",)}),
            "strategies": tuple(
                strategy for strategy in base.strategies if strategy.name == "qqq__fast__iron_butterfly_same_day"
            ),
        }
    )
    strategy = config.strategies[0]
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._select_legs = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not select legs"))
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)

    open_trade, event = trader._evaluate_entry(
        strategy=strategy,
        session=session,
        ledger=ledger,
        option_chain=pd.DataFrame(),
        spot_price=500.0,
        current_minute=301,
        current_equity=25_000.0,
        broker_equity=30_000.0,
        attempt_id="attempt-cutoff",
    )

    assert open_trade is None
    assert event["decision_reason"] == "late_day_entry_cutoff"
    assert event["entry_cutoff_minute"] == 300


def test_evaluate_entry_respects_event_blackout() -> None:
    base = default_portfolio_config()
    config = base.model_copy(
        update={
            "risk": base.risk.model_copy(
                update={
                    "event_blackouts": (
                        EventBlackoutConfig(
                            name="qqq_cpi",
                            reason="CPI release blackout",
                            start_date="2026-04-15",
                            end_date="2026-04-15",
                            start_minute=0,
                            end_minute=390,
                            symbols=("QQQ",),
                        ),
                    )
                }
            ),
            "execution": base.execution.model_copy(update={"underlying_symbols": ("QQQ",)}),
            "strategies": tuple(
                strategy for strategy in base.strategies if strategy.name == "qqq__fast__trend_long_call_next_expiry"
            ),
        }
    )
    strategy = config.strategies[0]
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._select_legs = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not select legs"))
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)

    open_trade, event = trader._evaluate_entry(
        strategy=strategy,
        session=session,
        ledger=ledger,
        option_chain=pd.DataFrame(),
        spot_price=500.0,
        current_minute=60,
        current_equity=25_000.0,
        broker_equity=30_000.0,
        attempt_id="attempt-blackout",
    )

    assert open_trade is None
    assert event["decision_reason"] == "event_blackout:qqq_cpi"
    assert event["event_blackouts"][0]["reason"] == "CPI release blackout"


def test_entry_execution_circuit_breaker_triggers_on_failure_streak() -> None:
    base = default_portfolio_config()
    config = base.model_copy(
        update={
            "risk": base.risk.model_copy(
                update={
                    "entry_failure_streak_limit": 3,
                    "entry_adverse_slippage_fraction_limit": None,
                }
            )
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._alert = lambda session, level, message: session.alerts.append(
        {"level": level, "message": message}
    )
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    trader._record_entry_execution_outcome(session, success=False, failure_status="rejected")
    trader._record_entry_execution_outcome(session, success=False, failure_status="rejected")
    trader._record_entry_execution_outcome(session, success=False, failure_status="rejected")

    assert session.blocked_new_entries is True
    assert session.execution_guardrails["circuit_breaker_triggered"] is True
    assert "consecutive entry failures" in (session.block_reason or "")


def test_entry_execution_circuit_breaker_triggers_on_adverse_slippage_average() -> None:
    base = default_portfolio_config()
    config = base.model_copy(
        update={
            "risk": base.risk.model_copy(
                update={
                    "entry_failure_streak_limit": None,
                    "entry_adverse_slippage_fraction_limit": 0.10,
                    "entry_adverse_slippage_lookback": 3,
                }
            )
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader._alert = lambda session, level, message: session.alerts.append(
        {"level": level, "message": message}
    )
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    trader._record_entry_execution_outcome(session, success=True, adverse_slippage_fraction=0.06)
    trader._record_entry_execution_outcome(session, success=True, adverse_slippage_fraction=0.11)
    trader._record_entry_execution_outcome(session, success=True, adverse_slippage_fraction=0.15)

    assert session.blocked_new_entries is True
    assert session.execution_guardrails["circuit_breaker_triggered"] is True
    assert "average adverse entry slippage" in (session.block_reason or "")


def test_morning_notification_only_marks_sent_after_success() -> None:
    config = default_portfolio_config()
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = tuple(config.execution.underlying_symbols)
    trader.submit_paper_orders = True
    trader.notifier = object()

    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

    trader.logger = _LoggerStub()

    session = SessionState(
        trade_date="2026-04-13",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    trader._notify_lines = lambda *_lines: False
    trader._send_morning_notification(
        session,
        {
            "buying_power": 25_000.0,
            "required_buying_power": 7_500.0,
        },
    )

    assert session.notified_morning is False
    assert any(alert["message"] == "Morning notification delivery failed" for alert in session.alerts)


def test_open_positions_summary_line_groups_by_ticker() -> None:
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    session = SessionState(
        trade_date="2026-04-13",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
        open_trades=[
            {"underlying_symbol": "QQQ"},
            {"underlying_symbol": "SPY"},
            {"underlying_symbol": "QQQ"},
        ],
    )

    assert trader._open_positions_by_ticker_line(session) == "Open positions by ticker: QQQ x2, SPY x1"


def test_strategy_pnl_summary_lines_include_winners_and_losers() -> None:
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    session = SessionState(
        trade_date="2026-04-13",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
        completed_trades=[
            {"strategy_name": "qqq__fast__trend_long_call_next_expiry", "net_pnl": 120.0},
            {"strategy_name": "qqq__fast__trend_long_call_next_expiry", "net_pnl": 30.0},
            {"strategy_name": "xle__fast__trend_long_put_next_expiry", "net_pnl": -45.0},
            {"strategy_name": "spy__fast__trend_long_call_next_expiry", "net_pnl": 80.0},
        ],
    )

    lines = trader._strategy_pnl_summary_lines(session)

    assert lines[0] == (
        "Top strategy PnL: qqq__fast__trend_long_call_next_expiry +$150.00; "
        "spy__fast__trend_long_call_next_expiry +$80.00"
    )
    assert lines[1] == "Lagging strategies: xle__fast__trend_long_put_next_expiry -$45.00"


def test_midday_notification_lines_include_open_positions_and_strategy_pnl() -> None:
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    session = SessionState(
        trade_date="2026-04-13",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
        open_trades=[
            {"underlying_symbol": "QQQ"},
            {"underlying_symbol": "NVDA"},
            {"underlying_symbol": "QQQ"},
        ],
        completed_trades=[
            {"strategy_name": "qqq__fast__trend_long_call_next_expiry", "net_pnl": 95.0},
            {"strategy_name": "xle__fast__trend_long_put_next_expiry", "net_pnl": -20.0},
        ],
    )

    lines = trader._build_midday_notification_lines(session, current_equity=25_120.0)

    assert "Open positions by ticker: NVDA x1, QQQ x2" in lines
    assert "Top strategy PnL: qqq__fast__trend_long_call_next_expiry +$95.00" in lines
    assert "Lagging strategies: xle__fast__trend_long_put_next_expiry -$20.00" in lines


def test_startup_check_only_requires_inventory_for_promoted_dte_modes() -> None:
    full_config = default_portfolio_config()
    nvda_strategies = tuple(
        strategy for strategy in full_config.strategies if strategy.underlying_symbol == "NVDA"
    )
    config = full_config.model_copy(
        update={
            "execution": full_config.execution.model_copy(update={"underlying_symbols": ("NVDA",)}),
            "strategies": nvda_strategies,
        }
    )

    class _BrokerStub:
        def get_account(self) -> dict[str, object]:
            return {"buying_power": 25_000.0}

        def get_positions(self) -> list[object]:
            return []

    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("NVDA",)
    trader.broker = _BrokerStub()

    option_chain = pd.DataFrame(
        [
            {"dte": 3, "option_type": "call"},
            {"dte": 3, "option_type": "put"},
        ]
    )
    now_et = pd.Timestamp.now(tz=ZoneInfo("America/New_York")).to_pydatetime()
    snapshot = SymbolSnapshot(
        underlying_symbol="NVDA",
        trade_date=now_et.date(),
        stock_frame=pd.DataFrame([{"close": 100.0}]),
        option_chain=option_chain,
        mark_map={},
        latest_close=100.0,
        current_minute=10,
        latest_timestamp_et=now_et,
    )
    session = SessionState(
        trade_date=now_et.date().isoformat(),
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    status, details = trader._perform_startup_check(
        session=session,
        trade_date=now_et.date(),
        snapshots={"NVDA": snapshot},
    )

    assert status == "passed"
    assert details["underlyings"]["NVDA"]["required_inventory"] == {
        "same_day_calls": False,
        "same_day_puts": False,
        "next_expiry_calls": True,
        "next_expiry_puts": True,
    }


def test_startup_check_fails_when_broker_equity_below_trade_threshold() -> None:
    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={"underlying_symbols": ("QQQ",)}
            ),
            "strategies": tuple(
                strategy
                for strategy in default_portfolio_config().strategies
                if strategy.underlying_symbol == "QQQ"
            ),
        }
    )

    class _BrokerStub:
        def get_account(self) -> dict[str, object]:
            return {"buying_power": 25_000.0, "equity": 25_400.0}

        def get_positions(self) -> list[object]:
            return []

    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("QQQ",)
    trader.broker = _BrokerStub()

    now_et = datetime(2026, 4, 15, 9, 36, tzinfo=ZoneInfo("America/New_York"))
    snapshot = SymbolSnapshot(
        underlying_symbol="QQQ",
        trade_date=now_et.date(),
        stock_frame=pd.DataFrame([{"close": 500.0}]),
        option_chain=pd.DataFrame(
            [
                {"dte": 0, "option_type": "call"},
                {"dte": 0, "option_type": "put"},
                {"dte": 1, "option_type": "call"},
                {"dte": 1, "option_type": "put"},
            ]
        ),
        mark_map={},
        latest_close=500.0,
        current_minute=6,
        latest_timestamp_et=now_et,
    )
    session = SessionState(
        trade_date=now_et.date().isoformat(),
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    status, details = trader._perform_startup_check(
        session=session,
        trade_date=now_et.date(),
        snapshots={"QQQ": snapshot},
    )

    assert status == "failed"
    assert (
        "broker equity 25400.00 below minimum trading threshold 26000.00"
        in details["failures"]
    )


def test_startup_check_allows_symbol_when_at_least_one_strategy_is_feasible() -> None:
    full_config = default_portfolio_config()
    qqq_strategies = tuple(
        strategy for strategy in full_config.strategies if strategy.underlying_symbol == "QQQ"
    )
    config = full_config.model_copy(
        update={
            "execution": full_config.execution.model_copy(update={"underlying_symbols": ("QQQ",)}),
            "strategies": qqq_strategies,
        }
    )

    class _BrokerStub:
        def get_account(self) -> dict[str, object]:
            return {"buying_power": 25_000.0}

        def get_positions(self) -> list[object]:
            return []

    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("QQQ",)
    trader.broker = _BrokerStub()

    option_chain = pd.DataFrame(
        [
            {"dte": 3, "option_type": "call"},
            {"dte": 3, "option_type": "put"},
        ]
    )
    now_et = pd.Timestamp.now(tz=ZoneInfo("America/New_York")).to_pydatetime()
    snapshot = SymbolSnapshot(
        underlying_symbol="QQQ",
        trade_date=now_et.date(),
        stock_frame=pd.DataFrame([{"close": 100.0}]),
        option_chain=option_chain,
        mark_map={},
        latest_close=100.0,
        current_minute=10,
        latest_timestamp_et=now_et,
    )
    session = SessionState(
        trade_date=now_et.date().isoformat(),
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    status, details = trader._perform_startup_check(
        session=session,
        trade_date=now_et.date(),
        snapshots={"QQQ": snapshot},
    )

    assert status == "passed"
    assert details["underlyings"]["QQQ"]["available_strategies"] == [
        "qqq__fast__trend_long_call_next_expiry_d70",
        "qqq__fast__trend_long_call_next_expiry",
        "qqq__base__orb_long_put_next_expiry",
        "qqq__slow__orb_long_put_next_expiry",
    ]
    assert details["underlyings"]["QQQ"]["unavailable_strategies"] == [
        {
            "name": "qqq__fast__iron_butterfly_same_day",
            "missing_inventory": [
                "same_day_calls",
                "same_day_calls",
                "same_day_puts",
                "same_day_puts",
            ],
        }
    ]


def test_trade_reconciliation_outputs_roll_up_signals_and_pnl(tmp_path: Path) -> None:
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.run_root = tmp_path / "runs"

    trade_date = "2026-04-13"
    run_dir = trader.run_root / trade_date
    run_dir.mkdir(parents=True, exist_ok=True)
    events = [
        {
            "timestamp_et": "2026-04-13T10:00:00-04:00",
            "event_type": "signal_decision",
            "attempt_id": "attempt-1",
            "trade_date": trade_date,
            "strategy_name": "jpm__fast__trend_long_call_next_expiry",
            "underlying_symbol": "JPM",
            "regime": "bull",
            "signal_name": "trend_call",
            "timing_profile": "fast",
            "current_minute": 30,
            "decision": "eligible",
            "decision_reason": "eligible",
            "quantity_planned": 2,
            "expected_entry_fill_price": 1.25,
        },
        {
            "timestamp_et": "2026-04-13T10:00:05-04:00",
            "event_type": "order_submission",
            "phase": "entry",
            "attempt_id": "attempt-1",
            "strategy_name": "jpm__fast__trend_long_call_next_expiry",
            "underlying_symbol": "JPM",
            "order_id": "entry-1",
        },
        {
            "timestamp_et": "2026-04-13T10:00:08-04:00",
            "event_type": "entry_result",
            "phase": "entry",
            "attempt_id": "attempt-1",
            "strategy_name": "jpm__fast__trend_long_call_next_expiry",
            "underlying_symbol": "JPM",
            "status": "filled",
            "order_id": "entry-1",
            "actual_entry_fill_price": 1.27,
            "entry_slippage": 0.02,
        },
        {
            "timestamp_et": "2026-04-13T11:15:00-04:00",
            "event_type": "exit_trigger",
            "phase": "exit",
            "attempt_id": "attempt-1",
            "strategy_name": "jpm__fast__trend_long_call_next_expiry",
            "underlying_symbol": "JPM",
            "exit_reason": "profit_target",
            "expected_exit_fill_price": 1.72,
        },
        {
            "timestamp_et": "2026-04-13T11:15:04-04:00",
            "event_type": "order_submission",
            "phase": "exit",
            "attempt_id": "attempt-1",
            "strategy_name": "jpm__fast__trend_long_call_next_expiry",
            "underlying_symbol": "JPM",
            "order_id": "exit-1",
        },
        {
            "timestamp_et": "2026-04-13T11:15:08-04:00",
            "event_type": "exit_result",
            "phase": "exit",
            "attempt_id": "attempt-1",
            "strategy_name": "jpm__fast__trend_long_call_next_expiry",
            "underlying_symbol": "JPM",
            "status": "filled",
            "order_id": "exit-1",
            "actual_exit_fill_price": 1.7,
            "exit_slippage": -0.02,
            "net_pnl": 85.4,
            "exit_reason": "profit_target",
        },
        {
            "timestamp_et": "2026-04-13T10:05:00-04:00",
            "event_type": "signal_decision",
            "attempt_id": "attempt-2",
            "trade_date": trade_date,
            "strategy_name": "xle__base__orb_long_call_same_day__choppy",
            "underlying_symbol": "XLE",
            "regime": "choppy",
            "signal_name": "orb_call",
            "timing_profile": "base",
            "current_minute": 35,
            "decision": "skipped",
            "decision_reason": "no_eligible_legs",
        },
    ]
    (run_dir / "trade_reconciliation_events.json").write_text(
        json.dumps(events, indent=2),
        encoding="utf-8",
    )

    session = SessionState(
        trade_date=trade_date,
        starting_equity=25_000.0,
        virtual_cash=25_085.4,
        completed_trades=[
            {
                "strategy_name": "jpm__fast__trend_long_call_next_expiry",
                "underlying_symbol": "JPM",
                "regime": "bull",
                "quantity": 2,
                "entry_time_et": "2026-04-13T10:00:00-04:00",
                "exit_time_et": "2026-04-13T11:15:08-04:00",
                "entry_minute": 30,
                "exit_minute": 105,
                "entry_fill_price": 1.27,
                "exit_fill_price": 1.70,
                "underlying_entry": 245.0,
                "underlying_exit": 247.3,
                "exit_reason": "profit_target",
                "entry_order_id": "entry-1",
                "exit_order_id": "exit-1",
                "net_pnl": 85.4,
                "max_loss_per_combo": 127.0,
                "max_profit_per_combo": 323.0,
                "delta_shares_at_entry": 120.0,
                "vega_dollars_1pct_at_entry": 18.0,
                "legs": [],
                "entry_attempt_id": "attempt-1",
            }
        ],
    )

    events_df, reconciliation_df, ticker_df, strategy_df, summary = trader._build_trade_reconciliation_outputs(
        session=session,
        trade_date=datetime.fromisoformat(f"{trade_date}T00:00:00").date(),
    )

    assert len(events_df) == 7
    assert len(reconciliation_df) == 2
    assert summary["signal_attempt_count"] == 2
    assert summary["eligible_signal_count"] == 1
    assert summary["completed_reconciled_trade_count"] == 1
    assert summary["realized_reconciled_net_pnl"] == 85.4
    jpm_row = reconciliation_df.loc[reconciliation_df["attempt_id"] == "attempt-1"].iloc[0]
    assert jpm_row["final_status"] == "completed"
    assert jpm_row["entry_status"] == "filled"
    assert jpm_row["exit_status"] == "filled"
    assert float(ticker_df.loc[ticker_df["underlying_symbol"] == "JPM", "net_pnl"].iloc[0]) == 85.4
    assert float(strategy_df.loc[strategy_df["strategy_name"] == "jpm__fast__trend_long_call_next_expiry", "net_pnl"].iloc[0]) == 85.4


def test_guardrail_scorecard_rolls_up_firings_and_recommendations() -> None:
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trade_date = datetime.fromisoformat("2026-04-13T00:00:00").date()
    events_df = pd.DataFrame(
        [
            {
                "timestamp_et": "2026-04-13T10:00:00-04:00",
                "event_type": "signal_decision",
                "attempt_id": "attempt-guardrail-1",
                "strategy_name": "qqq__fast__trend_long_call_next_expiry",
                "underlying_symbol": "QQQ",
                "regime": "bull",
                "current_minute": 45,
                "decision": "skipped",
                "decision_reason": "projected_delta_hard_cap",
            },
            {
                "timestamp_et": "2026-04-13T15:20:00-04:00",
                "event_type": "signal_decision",
                "attempt_id": "attempt-guardrail-2",
                "strategy_name": "qqq__slow__orb_long_put_same_day",
                "underlying_symbol": "QQQ",
                "regime": "bear",
                "current_minute": 350,
                "decision": "skipped",
                "decision_reason": "late_day_entry_cutoff",
            },
        ]
    )
    session = SessionState(
        trade_date="2026-04-13",
        starting_equity=25_000.0,
        virtual_cash=24_950.0,
        blocked_new_entries=True,
        block_reason="entry_execution_circuit_breaker: 3 consecutive entry failures (last status rejected)",
        execution_guardrails={
            "circuit_breaker_triggered": True,
            "entry_failure_streak": 3,
        },
        alerts=[
            {
                "timestamp_et": "2026-04-13T15:25:00-04:00",
                "level": "warning",
                "message": "End-Of-Day notification delivery failed",
            }
        ],
        last_updated_at="2026-04-13T16:01:00-04:00",
    )
    cleanup_summary = {
        "known_trade_cleanup_count": 1,
        "unexpected_position_cleanup_count": 1,
        "unexpected_position_cleanup": [
            {
                "timestamp_et": "2026-04-13T16:00:30-04:00",
                "symbol": "QQQ260417C00600000",
                "status": "filled",
                "reason": "auto_flatten_unexpected_end_of_day_position",
            }
        ],
    }

    summary, tables = trader._build_guardrail_scorecard_outputs(
        session=session,
        trade_date=trade_date,
        events_df=events_df,
        cleanup_summary=cleanup_summary,
    )

    firings_df = tables["guardrail_firings"]
    recommendations_df = tables["guardrail_recommendations"]

    assert summary["guardrail_fire_count"] == 6
    assert summary["guardrail_reason_count"] >= 5
    assert summary["needs_manual_review"] is True
    assert int((firings_df["source"] == "cleanup").sum()) == 2
    assert "projected_delta_hard_cap" in set(firings_df["reason"].astype(str))
    assert any(recommendations_df["action"] == "already_auto_fixed")
    assert any(recommendations_df["action"] == "manual_review")


def test_backfill_open_trade_reconciliation_assigns_attempt_ids(tmp_path: Path) -> None:
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.run_root = tmp_path / "runs"

    session = SessionState(
        trade_date="2026-04-13",
        starting_equity=25_000.0,
        virtual_cash=24_000.0,
        open_trades=[
            {
                "strategy_name": "spy__fast__trend_long_call_next_expiry",
                "underlying_symbol": "SPY",
                "regime": "bull",
                "quantity": 2,
                "entry_time_et": "2026-04-13T10:00:00-04:00",
                "entry_minute": 30,
                "hard_exit_minute": 360,
                "underlying_entry": 680.0,
                "entry_debit": 2.8,
                "max_loss_per_combo": 280.0,
                "max_profit_per_combo": 120.0,
                "profit_target_dollars": 126.0,
                "stop_loss_dollars": 84.0,
                "entry_order_id": "entry-123",
                "entry_fill_price": 2.8,
                "legs": [
                    {
                        "symbol": "SPY260414C00680000",
                        "expiration_date": "2026-04-14",
                        "option_type": "call",
                        "side": "long",
                        "strike_price": 680.0,
                        "target_delta": 0.6,
                        "entry_fill_price": 2.8,
                        "bid": 2.79,
                        "ask": 2.81,
                        "mark": 2.8,
                        "delta": 0.58,
                        "gamma": 0.06,
                        "theta": -0.9,
                        "vega": 0.14,
                    }
                ],
            }
        ],
    )

    updated = trader._backfill_open_trade_reconciliation(session)

    assert updated is True
    assert session.open_trades[0]["entry_attempt_id"].startswith("recovered:spy__fast__trend_long_call_next_expiry")
    events = json.loads((trader.run_root / "2026-04-13" / "trade_reconciliation_events.json").read_text(encoding="utf-8"))
    assert len(events) == 2
    assert events[0]["decision_reason"] == "backfilled_open_trade"
    assert events[1]["status"] == "filled"


def test_fetch_today_stock_frames_returns_empty_before_rth_without_api_call(monkeypatch) -> None:
    class _BrokerStub:
        def __init__(self) -> None:
            self.called = False

        def get_stock_bars(self, *_args, **_kwargs):
            self.called = True
            raise AssertionError("get_stock_bars should not be called before the RTH open")

    broker = _BrokerStub()
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.underlyings = ["QQQ", "SPY"]
    trader.broker = broker
    trader.portfolio_config = default_portfolio_config()
    trader.settings = LabSettings()

    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._now_et",
        lambda: datetime(2026, 4, 15, 9, 20, tzinfo=ZoneInfo("America/New_York")),
    )

    frames = trader._fetch_today_stock_frames(datetime(2026, 4, 15).date())

    assert broker.called is False
    assert set(frames.keys()) == {"QQQ", "SPY"}
    assert all(frame.empty for frame in frames.values())


def test_run_rechecks_clock_after_preopen_sleep_before_fetching_stock_bars(monkeypatch, tmp_path: Path) -> None:
    class _BrokerStub:
        def __init__(self) -> None:
            self.clock_calls = 0

        def get_account(self) -> dict[str, object]:
            return {"equity": 100_000.0, "buying_power": 100_000.0}

        def get_clock(self) -> dict[str, object]:
            self.clock_calls += 1
            if self.clock_calls == 1:
                return {
                    "is_open": False,
                    "timestamp": "2026-04-15T09:20:00-04:00",
                    "next_open": "2026-04-15T09:30:00-04:00",
                    "next_close": "2026-04-15T16:00:00-04:00",
                }
            return {
                "is_open": True,
                "timestamp": "2026-04-15T09:30:05-04:00",
                "next_open": "2026-04-16T09:30:00-04:00",
                "next_close": "2026-04-15T16:00:00-04:00",
            }

    config = default_portfolio_config()
    config = config.model_copy(
        update={
            "execution": config.execution.model_copy(
                update={
                    "underlying_symbols": ("QQQ",),
                    "state_root": tmp_path / "state",
                    "run_root": tmp_path / "runs",
                    "poll_interval_seconds": 1,
                }
            ),
            "ownership": config.ownership.model_copy(update={"enabled": False}),
        }
    )
    broker = _BrokerStub()
    trader = MultiTickerPortfolioPaperTrader(
        LabSettings(),
        config,
        broker=broker,
        submit_paper_orders=False,
    )
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._now_et",
        lambda: datetime(2026, 4, 15, 9, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    monkeypatch.setattr("alpaca_lab.multi_ticker_portfolio.trader.time.sleep", lambda _seconds: None)
    trader.load_ledger = lambda: PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)
    trader.load_or_create_session = lambda *_args, **_kwargs: session
    trader.save_session = lambda *_args, **_kwargs: tmp_path / "session.json"
    trader._backfill_open_trade_reconciliation = lambda *_args, **_kwargs: False
    trader._build_symbol_snapshot = lambda **_kwargs: None
    trader._perform_startup_check = lambda **_kwargs: ("failed", {"failures": ["test failure"]})
    trader._notify_lines = lambda *_args, **_kwargs: False
    trader._alert = lambda *_args, **_kwargs: None

    def _fetch_stock_frames(_trade_date):
        assert broker.clock_calls >= 2
        return {"QQQ": pd.DataFrame()}

    trader._fetch_today_stock_frames = _fetch_stock_frames

    result = trader.run(run_once=False)

    assert result["status"] == "startup_check_failed"
    assert broker.clock_calls >= 2


def test_reconcile_and_trade_triggers_severe_loss_flatten(monkeypatch) -> None:
    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={"underlying_symbols": ("QQQ",)}
            ),
            "strategies": tuple(),
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("QQQ",)

    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

    trader.logger = _LoggerStub()
    trader._maybe_send_midday_notification = lambda **_kwargs: None
    trader._close_unexpected_broker_positions = lambda **_kwargs: []
    flattened: list[str] = []
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader.infer_symbol_regime",
        lambda _frame: "bull",
    )

    def _cleanup_known_open_trades(**kwargs) -> int:
        flattened.append(kwargs["reason"])
        kwargs["session"].open_trades = []
        return 1

    trader._cleanup_known_open_trades = _cleanup_known_open_trades
    trader._build_symbol_snapshot = lambda **_kwargs: SymbolSnapshot(
        underlying_symbol="QQQ",
        trade_date=datetime(2026, 4, 15).date(),
        stock_frame=pd.DataFrame(
            [
                {
                    "timestamp_et": datetime(2026, 4, 15, 10, 0),
                    "minute_index": 30,
                    "close": 500.0,
                }
            ]
        ),
        option_chain=pd.DataFrame(),
        mark_map={},
        latest_close=500.0,
        current_minute=30,
        latest_timestamp_et=datetime(2026, 4, 15, 10, 0, tzinfo=ZoneInfo("America/New_York")),
    )
    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=24_000.0,
        open_trades=[
            {
                "strategy_name": "qqq__fast__trend_long_call_next_expiry",
                "underlying_symbol": "QQQ",
                "regime": "bull",
                "quantity": 1,
                "max_loss_per_combo": 300.0,
            }
        ],
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._current_equity",
        lambda *_args, **_kwargs: 23_700.0,
    )

    _snapshots, current_equity = trader._reconcile_and_trade(
        session=session,
        ledger=ledger,
        stock_frames={"QQQ": pd.DataFrame([{"close": 500.0, "minute_index": 30}])},
        broker_equity=30_000.0,
    )

    assert flattened == ["severe_loss_flatten_all"]
    assert session.blocked_new_entries is True
    assert session.block_reason == "severe_loss_flatten_all triggered at equity 23700.00"
    assert current_equity == 23_700.0


def test_reconcile_and_trade_sweeps_unexpected_intraday_positions(monkeypatch) -> None:
    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={"underlying_symbols": ("QQQ",)}
            ),
            "strategies": tuple(),
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("QQQ",)

    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

    trader.logger = _LoggerStub()
    trader._maybe_send_midday_notification = lambda **_kwargs: None
    trader._apply_entry_execution_circuit_breaker = lambda *_args, **_kwargs: None
    trader._evaluate_entry = lambda **_kwargs: (_ for _ in ()).throw(AssertionError("no entry expected"))
    trader._run_exit = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no exit expected"))
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader.infer_symbol_regime",
        lambda _frame: "neutral",
    )

    cleanup_reasons: list[str] = []

    def _cleanup_unexpected(**kwargs):
        cleanup_reasons.append(kwargs["reason"])
        return [{"symbol": "QQQ260424P00645000", "status": "filled", "reason": kwargs["reason"]}]

    trader._close_unexpected_broker_positions = _cleanup_unexpected
    trader._build_symbol_snapshot = lambda **_kwargs: SymbolSnapshot(
        underlying_symbol="QQQ",
        trade_date=datetime(2026, 4, 17).date(),
        stock_frame=pd.DataFrame(
            [
                {
                    "timestamp_et": datetime(2026, 4, 17, 10, 0),
                    "minute_index": 30,
                    "close": 500.0,
                }
            ]
        ),
        option_chain=pd.DataFrame(),
        mark_map={},
        latest_close=500.0,
        current_minute=30,
        latest_timestamp_et=datetime(2026, 4, 17, 10, 0, tzinfo=ZoneInfo("America/New_York")),
    )
    session = SessionState(
        trade_date="2026-04-17",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._current_equity",
        lambda *_args, **_kwargs: 25_000.0,
    )

    _snapshots, current_equity = trader._reconcile_and_trade(
        session=session,
        ledger=ledger,
        stock_frames={"QQQ": pd.DataFrame([{"close": 500.0, "minute_index": 30}])},
        broker_equity=30_000.0,
    )

    assert cleanup_reasons == [AUTO_FLATTEN_UNEXPECTED_INTRADAY_REASON]
    assert current_equity == 25_000.0


def test_startup_check_auto_flattens_unexpected_positions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

        def info(self, *_args, **_kwargs) -> None:
            return None

    class _BrokerStub:
        def __init__(self) -> None:
            self.position_open = True
            self.submitted: list[OrderRequest] = []

        def get_account(self) -> dict[str, object]:
            return {"buying_power": 25_000.0}

        def get_positions(self) -> list[dict[str, object]]:
            if self.position_open:
                return [
                    {
                        "symbol": "QQQ260417C00600000",
                        "qty": "1",
                        "side": "long",
                        "asset_class": "us_option",
                    }
                ]
            return []

        def build_order_request(self, **kwargs) -> OrderRequest:
            return OrderRequest(**kwargs)

        def submit_order(self, request: OrderRequest, **_kwargs) -> dict[str, object]:
            self.submitted.append(request)
            self.position_open = False
            return {"id": "cleanup-startup-1", "status": "accepted"}

        def get_order(self, _order_id: str) -> dict[str, object]:
            return {
                "id": "cleanup-startup-1",
                "status": "filled",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "1.23",
            }

    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={
                    "underlying_symbols": ("QQQ",),
                    "run_root": tmp_path / "runs",
                    "state_root": tmp_path / "state",
                }
            ),
            "strategies": tuple(
                strategy
                for strategy in default_portfolio_config().strategies
                if strategy.underlying_symbol == "QQQ"
            ),
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("QQQ",)
    trader.broker = _BrokerStub()
    trader.run_root = tmp_path / "runs"
    trader.submit_paper_orders = True
    trader.logger = _LoggerStub()

    now_et = datetime(2026, 4, 15, 9, 36, tzinfo=ZoneInfo("America/New_York"))
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._now_et",
        lambda: now_et,
    )
    snapshot = SymbolSnapshot(
        underlying_symbol="QQQ",
        trade_date=now_et.date(),
        stock_frame=pd.DataFrame([{"close": 500.0}]),
        option_chain=pd.DataFrame(
            [
                {"dte": 0, "option_type": "call"},
                {"dte": 0, "option_type": "put"},
                {"dte": 1, "option_type": "call"},
                {"dte": 1, "option_type": "put"},
            ]
        ),
        mark_map={},
        latest_close=500.0,
        current_minute=6,
        latest_timestamp_et=now_et,
    )
    session = SessionState(
        trade_date=now_et.date().isoformat(),
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    status, details = trader._perform_startup_check(
        session=session,
        trade_date=now_et.date(),
        snapshots={"QQQ": snapshot},
    )

    assert status == "passed"
    assert trader.broker.submitted[0].side == "sell"
    assert details["broker_position_count"] == 0
    cleanup_entries = json.loads(
        (tmp_path / "runs" / "2026-04-15" / "broker_position_cleanup.json").read_text(
            encoding="utf-8"
        )
    )
    assert cleanup_entries[0]["reason"] == "auto_flatten_unexpected_startup_position"


def test_startup_check_respects_existing_close_orders_without_duplicate_cleanup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

        def info(self, *_args, **_kwargs) -> None:
            return None

    class _BrokerStub:
        def __init__(self) -> None:
            self.submit_count = 0

        def get_account(self) -> dict[str, object]:
            return {"buying_power": 25_000.0}

        def get_positions(self) -> list[dict[str, object]]:
            return [
                {
                    "symbol": "QQQ260417C00600000",
                    "qty": "1",
                    "side": "long",
                    "asset_class": "us_option",
                }
            ]

        def get_orders(self, *, status: str = "all", limit: int = 100) -> list[dict[str, object]]:
            assert status == "open"
            return [
                {
                    "symbol": "QQQ260417C00600000",
                    "status": "accepted",
                    "position_intent": "sell_to_close",
                    "qty": "1",
                    "filled_qty": "0",
                }
            ]

        def build_order_request(self, **kwargs) -> OrderRequest:
            return OrderRequest(**kwargs)

        def submit_order(self, request: OrderRequest, **_kwargs) -> dict[str, object]:
            self.submit_count += 1
            return {"id": "unexpected-cleanup", "status": "accepted"}

    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={
                    "underlying_symbols": ("QQQ",),
                    "run_root": tmp_path / "runs",
                    "state_root": tmp_path / "state",
                }
            ),
            "strategies": tuple(
                strategy
                for strategy in default_portfolio_config().strategies
                if strategy.underlying_symbol == "QQQ"
            ),
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("QQQ",)
    trader.broker = _BrokerStub()
    trader.run_root = tmp_path / "runs"
    trader.submit_paper_orders = True
    trader.logger = _LoggerStub()

    now_et = datetime(2026, 4, 15, 9, 33, tzinfo=ZoneInfo("America/New_York"))
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._now_et",
        lambda: now_et,
    )
    snapshot = SymbolSnapshot(
        underlying_symbol="QQQ",
        trade_date=now_et.date(),
        stock_frame=pd.DataFrame([{"close": 500.0}]),
        option_chain=pd.DataFrame(
            [
                {"dte": 0, "option_type": "call"},
                {"dte": 0, "option_type": "put"},
                {"dte": 1, "option_type": "call"},
                {"dte": 1, "option_type": "put"},
            ]
        ),
        mark_map={},
        latest_close=500.0,
        current_minute=3,
        latest_timestamp_et=now_et,
    )
    session = SessionState(
        trade_date=now_et.date().isoformat(),
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    status, details = trader._perform_startup_check(
        session=session,
        trade_date=now_et.date(),
        snapshots={"QQQ": snapshot},
    )

    assert status == "pending"
    assert trader.broker.submit_count == 0
    assert details["pending_broker_close_orders"] == ["QQQ260417C00600000"]


def test_close_unexpected_broker_positions_skips_symbols_with_open_close_orders() -> None:
    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

        def info(self, *_args, **_kwargs) -> None:
            return None

    class _BrokerStub:
        def __init__(self) -> None:
            self.submit_count = 0

        def get_positions(self) -> list[dict[str, object]]:
            return [
                {
                    "symbol": "QQQ260417C00600000",
                    "qty": "1",
                    "side": "long",
                    "asset_class": "us_option",
                }
            ]

        def get_orders(self, *, status: str = "all", limit: int = 100) -> list[dict[str, object]]:
            assert status == "open"
            return [
                {
                    "symbol": "QQQ260417C00600000",
                    "status": "accepted",
                    "position_intent": "sell_to_close",
                    "qty": "1",
                    "filled_qty": "0",
                }
            ]

        def build_order_request(self, **kwargs) -> OrderRequest:
            return OrderRequest(**kwargs)

        def submit_order(self, request: OrderRequest, **_kwargs) -> dict[str, object]:
            self.submit_count += 1
            return {"id": "cleanup-duplicate", "status": "accepted"}

    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.broker = _BrokerStub()
    trader.logger = _LoggerStub()
    trader.portfolio_config = default_portfolio_config()

    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_000.0,
    )

    cleanup_entries = trader._close_unexpected_broker_positions(
        session=session,
        trade_date=datetime(2026, 4, 15).date(),
        reason="auto_flatten_unexpected_end_of_day_position",
    )

    assert trader.broker.submit_count == 0
    assert cleanup_entries[0]["status"] == "pending_existing_close_order"


def test_force_cleanup_known_trade_books_completion(tmp_path: Path) -> None:
    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

        def info(self, *_args, **_kwargs) -> None:
            return None

    class _BrokerStub:
        def __init__(self) -> None:
            self.submitted: list[OrderRequest] = []

        def build_order_request(self, **kwargs) -> OrderRequest:
            return OrderRequest(**kwargs)

        def submit_order(self, request: OrderRequest, **_kwargs) -> dict[str, object]:
            self.submitted.append(request)
            return {"id": "cleanup-eod-1", "status": "accepted"}

        def get_order(self, _order_id: str) -> dict[str, object]:
            return {
                "id": "cleanup-eod-1",
                "status": "filled",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "1.50",
            }

    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={
                    "run_root": tmp_path / "runs",
                    "state_root": tmp_path / "state",
                }
            )
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.run_root = tmp_path / "runs"
    trader.submit_paper_orders = True
    trader.broker = _BrokerStub()
    trader.logger = _LoggerStub()

    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=24_720.0,
        open_trades=[
            {
                "strategy_name": "qqq__fast__trend_long_call_next_expiry",
                "underlying_symbol": "QQQ",
                "regime": "bull",
                "quantity": 1,
                "entry_time_et": "2026-04-15T10:15:00-04:00",
                "entry_minute": 45,
                "hard_exit_minute": 360,
                "underlying_entry": 500.0,
                "entry_debit": 2.8,
                "max_loss_per_combo": 280.0,
                "max_profit_per_combo": 120.0,
                "profit_target_dollars": 126.0,
                "stop_loss_dollars": 84.0,
                "entry_order_id": "entry-1",
                "entry_fill_price": 2.8,
                "legs": [
                    {
                        "symbol": "QQQ260417C00600000",
                        "expiration_date": "2026-04-17",
                        "option_type": "call",
                        "side": "long",
                        "strike_price": 600.0,
                        "target_delta": 0.6,
                        "entry_fill_price": 2.8,
                        "bid": 1.45,
                        "ask": 1.55,
                        "mark": 1.5,
                        "delta": 0.5,
                        "gamma": 0.04,
                        "theta": -0.08,
                        "vega": 0.11,
                    }
                ],
                "entry_attempt_id": "attempt-cleanup-1",
            }
        ],
    )
    stock_frames = {
        "QQQ": pd.DataFrame(
            [
                {
                    "timestamp_et": datetime(2026, 4, 15, 15, 59),
                    "minute_index": 389,
                    "close": 507.0,
                }
            ]
        )
    }

    cleaned = trader._cleanup_known_open_trades(
        session=session,
        trade_date=datetime(2026, 4, 15).date(),
        stock_frames=stock_frames,
        reason="auto_flatten_known_end_of_day_position",
    )

    assert cleaned == 1
    assert not session.open_trades
    assert len(session.completed_trades) == 1
    assert session.completed_trades[0]["exit_reason"] == "auto_flatten_known_end_of_day_position"
    assert session.completed_trades[0]["exit_fill_price"] == 1.5
    cleanup_entries = json.loads(
        (tmp_path / "runs" / "2026-04-15" / "broker_position_cleanup.json").read_text(
            encoding="utf-8"
        )
    )
    assert cleanup_entries[0]["reason"] == "auto_flatten_known_end_of_day_position"


def test_submit_cleanup_order_retries_after_cancelled_attempt(tmp_path: Path) -> None:
    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

        def info(self, *_args, **_kwargs) -> None:
            return None

    class _BrokerStub:
        def __init__(self) -> None:
            self.submit_count = 0
            self.submitted_ids: list[str | None] = []

        def submit_order(self, request: OrderRequest, **_kwargs) -> dict[str, object]:
            self.submit_count += 1
            self.submitted_ids.append(request.client_order_id)
            return {"id": f"cleanup-{self.submit_count}", "status": "accepted"}

        def get_order(self, order_id: str) -> dict[str, object]:
            if order_id == "cleanup-1":
                return {
                    "id": order_id,
                    "status": "canceled",
                    "qty": "1",
                    "filled_qty": "0",
                    "filled_avg_price": None,
                }
            return {
                "id": order_id,
                "status": "filled",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "1.11",
            }

        def cancel_order(self, *_args, **_kwargs) -> dict[str, object]:
            return {"status": "cancelled"}

    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={
                    "run_root": tmp_path / "runs",
                    "state_root": tmp_path / "state",
                }
            )
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.run_root = tmp_path / "runs"
    trader.submit_paper_orders = True
    trader.broker = _BrokerStub()
    trader.logger = _LoggerStub()

    request = OrderRequest(
        symbol="QQQ260417C00600000",
        side="sell",
        qty=1.0,
        order_type="market",
        time_in_force="day",
        client_order_id="cleanup-base-id",
        asset_class="option",
        strategy_name="cleanup_retry_test",
        extra={"position_intent": "sell_to_close"},
    )

    result = trader._submit_cleanup_order(
        trade_date=datetime(2026, 4, 15).date(),
        request=request,
        reason="auto_flatten_known_end_of_day_position",
        metadata={"scope": "known_trade"},
    )

    assert result["status"] == "filled"
    assert trader.broker.submit_count == 2
    cleanup_entries = json.loads(
        (tmp_path / "runs" / "2026-04-15" / "broker_position_cleanup.json").read_text(
            encoding="utf-8"
        )
    )
    assert cleanup_entries[0]["attempt_index"] == 1
    assert cleanup_entries[0]["terminal"]["status"] == "canceled"
    assert cleanup_entries[1]["attempt_index"] == 2
    assert cleanup_entries[1]["terminal"]["status"] == "filled"
    assert trader.broker.submitted_ids == ["cleanup-base-id", "cleanup-base-id-r2"]


def test_build_cleanup_order_request_uses_limit_after_option_close(monkeypatch) -> None:
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)

    class _BrokerStub:
        def build_order_request(self, **kwargs) -> OrderRequest:
            return OrderRequest(**kwargs)

    trader.broker = _BrokerStub()
    monkeypatch.setattr(
        "alpaca_lab.multi_ticker_portfolio.trader._now_et",
        lambda: datetime(2026, 4, 15, 16, 27, tzinfo=ZoneInfo("America/New_York")),
    )

    request = trader._build_cleanup_order_request(
        symbol="QQQ260417C00600000",
        side="sell",
        strategy_name="cleanup_after_close",
        asset_class="option",
        qty=1.0,
        position_intent="sell_to_close",
    )

    assert request.order_type == "limit"
    assert request.limit_price == 0.01


def test_finalize_session_retries_reconciliation_until_broker_is_flat(tmp_path: Path) -> None:
    class _LoggerStub:
        def warning(self, *_args, **_kwargs) -> None:
            return None

        def info(self, *_args, **_kwargs) -> None:
            return None

    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={
                    "run_root": tmp_path / "runs",
                    "state_root": tmp_path / "state",
                }
            )
        }
    )
    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.run_root = tmp_path / "runs"
    trader.state_root = tmp_path / "state"
    trader.submit_paper_orders = True
    trader.logger = _LoggerStub()
    trader.save_ledger = lambda _ledger: None
    trader.save_session = lambda _session: None
    trader._session_run_dir = lambda trade_date: tmp_path / "runs" / trade_date.isoformat()
    trader._flatten_all = lambda _session, _stock_frames: {
        "forced_exit_attempt_count": 1,
        "forced_exit_failure_count": 1,
        "forced_exit_cleanup_count": 0,
    }
    trader._cleanup_known_open_trades = lambda **_kwargs: 0
    close_calls = {"count": 0}

    def _close_unexpected_broker_positions(**_kwargs) -> list[dict[str, object]]:
        close_calls["count"] += 1
        if close_calls["count"] == 1:
            return [
                {
                    "symbol": "QQQ260417C00600000",
                    "reason": "auto_flatten_unexpected_end_of_day_position",
                    "status": "not_filled",
                    "order_id": "cleanup-1",
                    "filled_avg_price": None,
                }
            ]
        return []

    residual_positions = [
        [
            {
                "symbol": "QQQ260417C00600000",
                "qty": 1.0,
                "asset_class": "option",
                "raw_position": {"symbol": "QQQ260417C00600000", "qty": "1"},
            }
        ],
        [],
    ]
    trader._close_unexpected_broker_positions = _close_unexpected_broker_positions
    trader._active_broker_positions = lambda: residual_positions.pop(0)
    trader._build_trade_reconciliation_outputs = lambda **_kwargs: (
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        {},
    )
    trader._build_guardrail_scorecard_outputs = lambda **_kwargs: (
        {
            "guardrail_fire_count": 0,
            "guardrail_reason_count": 0,
            "manual_review_recommendation_count": 0,
            "already_auto_fixed_count": 0,
            "needs_manual_review": False,
        },
        {},
    )
    trader._notify_lines = lambda *_lines: True
    trader._build_end_of_day_notification_lines = lambda session, ending_equity: [
        f"{session.trade_date} {ending_equity}"
    ]

    session = SessionState(
        trade_date="2026-04-15",
        starting_equity=25_000.0,
        virtual_cash=25_250.0,
    )
    ledger = PortfolioLedger(realized_equity=25_000.0, high_watermark=25_000.0)

    summary = trader.finalize_session(session, ledger, stock_frames={})

    assert close_calls["count"] == 2
    assert summary["shutdown_reconciled"] is True
    assert summary["end_of_day_cleanup"]["reconciliation_passes"][0]["residual_broker_position_count"] == 1
    assert summary["end_of_day_cleanup"]["reconciliation_passes"][1]["residual_broker_position_count"] == 0
    assert session.notified_end_of_day is True

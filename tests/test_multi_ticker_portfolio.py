from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca_lab.brokers.alpaca import OrderRequest
from alpaca_lab.config import LabSettings
from alpaca_lab.multi_ticker_portfolio.config import default_portfolio_config, load_portfolio_config
from alpaca_lab.multi_ticker_portfolio.signals import signal_is_true
from alpaca_lab.multi_ticker_portfolio.trader import (
    MultiTickerPortfolioPaperTrader,
    PortfolioLedger,
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


def test_default_multi_ticker_portfolio_contains_all_symbols() -> None:
    config = default_portfolio_config()

    counts = Counter(strategy.underlying_symbol for strategy in config.strategies)

    assert tuple(config.execution.underlying_symbols) == (
        "QQQ",
        "SPY",
        "IWM",
        "NVDA",
        "TSLA",
        "MSFT",
        "BAC",
        "PLTR",
        "GLD",
        "ARKK",
        "XLE",
        "GDX",
        "SLV",
        "AMZN",
        "JPM",
        "XOM",
        "ORCL",
        "SHOP",
        "CRM",
    )
    assert all(counts[symbol] >= 1 for symbol in config.execution.underlying_symbols)
    assert counts["QQQ"] >= 3
    assert counts["XLE"] >= 4
    assert counts["GDX"] >= 5
    assert counts["SLV"] >= 5
    assert counts["AMZN"] >= 7
    assert counts["JPM"] >= 6
    assert counts["XOM"] >= 3
    assert counts["ORCL"] >= 4
    assert counts["SHOP"] >= 5
    assert counts["CRM"] >= 5


def test_default_multi_ticker_portfolio_includes_xle_choppy_alias() -> None:
    config = default_portfolio_config()
    alias = next(
        strategy
        for strategy in config.strategies
        if strategy.name == "xle__base__orb_long_call_same_day__choppy"
    )

    assert alias.underlying_symbol == "XLE"
    assert alias.regime == "choppy"
    assert alias.signal_name == "orb_call"


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
    jpm_strategies = tuple(
        strategy for strategy in full_config.strategies if strategy.underlying_symbol == "JPM"
    )
    config = full_config.model_copy(
        update={
            "execution": full_config.execution.model_copy(update={"underlying_symbols": ("JPM",)}),
            "strategies": jpm_strategies,
        }
    )

    class _BrokerStub:
        def get_account(self) -> dict[str, object]:
            return {"buying_power": 25_000.0}

        def get_positions(self) -> list[object]:
            return []

    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("JPM",)
    trader.broker = _BrokerStub()

    option_chain = pd.DataFrame(
        [
            {"dte": 3, "option_type": "call"},
            {"dte": 3, "option_type": "put"},
        ]
    )
    now_et = pd.Timestamp.now(tz=ZoneInfo("America/New_York")).to_pydatetime()
    snapshot = SymbolSnapshot(
        underlying_symbol="JPM",
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
        snapshots={"JPM": snapshot},
    )

    assert status == "passed"
    assert details["underlyings"]["JPM"]["required_inventory"] == {
        "same_day_calls": False,
        "same_day_puts": False,
        "next_expiry_calls": True,
        "next_expiry_puts": True,
    }


def test_startup_check_allows_symbol_when_at_least_one_strategy_is_feasible() -> None:
    full_config = default_portfolio_config()
    arkk_strategies = tuple(
        strategy for strategy in full_config.strategies if strategy.underlying_symbol == "ARKK"
    )
    config = full_config.model_copy(
        update={
            "execution": full_config.execution.model_copy(update={"underlying_symbols": ("ARKK",)}),
            "strategies": arkk_strategies,
        }
    )

    class _BrokerStub:
        def get_account(self) -> dict[str, object]:
            return {"buying_power": 25_000.0}

        def get_positions(self) -> list[object]:
            return []

    trader = MultiTickerPortfolioPaperTrader.__new__(MultiTickerPortfolioPaperTrader)
    trader.portfolio_config = config
    trader.underlyings = ("ARKK",)
    trader.broker = _BrokerStub()

    option_chain = pd.DataFrame(
        [
            {"dte": 3, "option_type": "call"},
            {"dte": 3, "option_type": "put"},
        ]
    )
    now_et = pd.Timestamp.now(tz=ZoneInfo("America/New_York")).to_pydatetime()
    snapshot = SymbolSnapshot(
        underlying_symbol="ARKK",
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
        snapshots={"ARKK": snapshot},
    )

    assert status == "passed"
    assert details["underlyings"]["ARKK"]["available_strategies"] == [
        "arkk__fast__trend_long_call_next_expiry",
        "arkk__slow__trend_long_call_next_expiry",
        "arkk__fast__trend_long_put_next_expiry",
    ]
    assert details["underlyings"]["ARKK"]["unavailable_strategies"] == [
        {
            "name": "arkk__fast__orb_long_call_same_day",
            "missing_inventory": ["same_day_calls"],
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
            )
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

    cleaned = trader._cleanup_known_open_trades_at_end_of_day(
        session=session,
        trade_date=datetime(2026, 4, 15).date(),
        stock_frames=stock_frames,
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

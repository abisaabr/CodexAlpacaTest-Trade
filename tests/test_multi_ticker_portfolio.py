from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

from alpaca_lab.multi_ticker_portfolio.config import default_portfolio_config, load_portfolio_config
from alpaca_lab.multi_ticker_portfolio.signals import signal_is_true
from alpaca_lab.multi_ticker_portfolio.trader import MultiTickerPortfolioPaperTrader, SessionState


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

    assert tuple(config.execution.underlying_symbols) == ("QQQ", "SPY", "IWM", "NVDA", "TSLA", "MSFT")
    assert all(counts[symbol] >= 1 for symbol in config.execution.underlying_symbols)
    assert counts["QQQ"] >= 3


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

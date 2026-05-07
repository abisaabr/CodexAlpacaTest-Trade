from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from alpaca_lab.qqq_portfolio.greeks import bs_price
from scripts.run_option_aware_research_backtest import (
    ENTRY_LOOKUP_AT_OR_AFTER,
    _choose_entry_delta_target_contract,
)


def test_entry_delta_target_selector_prefers_nearest_delta() -> None:
    trade_date = date(2026, 5, 6)
    entry_time = pd.Timestamp("2026-05-06T14:00:00Z")
    expiration = date(2026, 5, 7)
    years = 1.0 / 365.0
    contracts = pd.DataFrame(
        [
            {
                "underlying_symbol": "QQQ",
                "symbol": "QQQ260507C00095000",
                "option_type": "call",
                "trade_date": trade_date,
                "expiration_date": expiration,
                "strike_price": 95.0,
                "relative_strike_step": -1,
                "dte": 1,
            },
            {
                "underlying_symbol": "QQQ",
                "symbol": "QQQ260507C00100000",
                "option_type": "call",
                "trade_date": trade_date,
                "expiration_date": expiration,
                "strike_price": 100.0,
                "relative_strike_step": 0,
                "dte": 1,
            },
            {
                "underlying_symbol": "QQQ",
                "symbol": "QQQ260507C00105000",
                "option_type": "call",
                "trade_date": trade_date,
                "expiration_date": expiration,
                "strike_price": 105.0,
                "relative_strike_step": 1,
                "dte": 1,
            },
        ]
    )
    option_bars = pd.DataFrame(
        [
            {
                "symbol": row["symbol"],
                "timestamp": entry_time,
                "close": bs_price(
                    spot=100.0,
                    strike=float(row["strike_price"]),
                    years=years,
                    rate=0.04,
                    sigma=0.30,
                    option_type="call",
                ),
                "volume": 100,
            }
            for row in contracts.to_dict("records")
        ]
    )

    contract, entry_bar, status = _choose_entry_delta_target_contract(
        contracts=contracts,
        option_bars=option_bars,
        option_trades=pd.DataFrame(),
        symbol="QQQ",
        option_type="call",
        trade_date=trade_date,
        entry_time=entry_time,
        max_lag=timedelta(minutes=1),
        entry_lookup_mode=ENTRY_LOOKUP_AT_OR_AFTER,
        max_entry_staleness=timedelta(0),
        entry_spot=100.0,
        parameters={"target_delta": 0.50, "min_abs_delta": 0.05, "max_abs_delta": 0.95},
        dte_mode="next_expiry",
    )

    assert status == "selected"
    assert entry_bar is not None
    assert contract is not None
    assert contract["symbol"] == "QQQ260507C00100000"
    assert 0.40 <= contract["entry_delta"] <= 0.60
    assert contract["entry_implied_vol"] > 0.0

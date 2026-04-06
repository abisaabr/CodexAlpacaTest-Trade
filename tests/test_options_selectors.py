from __future__ import annotations

from datetime import date

import pandas as pd

from alpaca_lab.options.selectors import select_strike_window


def test_select_strike_window_keeps_plus_minus_three_steps() -> None:
    contracts = pd.DataFrame(
        [
            {
                "symbol": f"SPY260403C{strike:08d}",
                "option_type": "call",
                "strike_price": strike,
                "expiration_date": "2026-04-03",
            }
            for strike in [90, 95, 100, 105, 110, 115, 120, 125]
        ]
    )

    selected = select_strike_window(
        contracts,
        reference_price=108.0,
        trade_date=date(2026, 4, 1),
        min_dte=0,
        max_dte=14,
        strike_steps=3,
        option_types=("call",),
    )

    assert set(selected["strike_price"]) == {95, 100, 105, 110, 115, 120, 125}
    assert set(selected["relative_strike_step"]) == {-3, -2, -1, 0, 1, 2, 3}

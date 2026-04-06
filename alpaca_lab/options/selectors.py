from __future__ import annotations

from datetime import date

import pandas as pd


def select_strike_window(
    contracts: pd.DataFrame,
    *,
    reference_price: float,
    trade_date: date,
    min_dte: int = 0,
    max_dte: int = 14,
    strike_steps: int = 3,
    option_types: tuple[str, ...] = ("call", "put"),
) -> pd.DataFrame:
    if contracts.empty:
        return pd.DataFrame()
    if strike_steps < 0:
        raise ValueError("strike_steps must be non-negative.")

    frame = contracts.copy()
    frame["expiration_date"] = pd.to_datetime(
        frame["expiration_date"], errors="coerce"
    ).dt.normalize()
    frame["option_type"] = frame["option_type"].astype("string").str.lower()
    frame["strike_price"] = pd.to_numeric(frame["strike_price"], errors="coerce")
    trade_day = pd.Timestamp(trade_date).normalize()
    frame["dte"] = (frame["expiration_date"] - trade_day).dt.days
    frame = frame[
        frame["option_type"].isin(option_types)
        & frame["dte"].between(min_dte, max_dte, inclusive="both")
        & frame["strike_price"].notna()
    ].copy()
    if frame.empty:
        return frame

    selected_rows: list[pd.DataFrame] = []
    for (option_type, expiration_date), subset in frame.groupby(
        ["option_type", "expiration_date"], sort=True
    ):
        strikes = sorted(subset["strike_price"].unique().tolist())
        atm_index = min(
            range(len(strikes)),
            key=lambda index: (abs(float(strikes[index]) - reference_price), float(strikes[index])),
        )
        lower_index = max(0, atm_index - strike_steps)
        upper_index = min(len(strikes) - 1, atm_index + strike_steps)
        selected = subset[
            subset["strike_price"].isin(strikes[lower_index : upper_index + 1])
        ].copy()
        selected["atm_strike"] = float(strikes[atm_index])
        selected["relative_strike_step"] = selected["strike_price"].map(
            {float(strike): idx - atm_index for idx, strike in enumerate(strikes)}
        )
        selected["selection_reason"] = (
            f"type={option_type};expiration={pd.Timestamp(expiration_date).date().isoformat()};"
            f"atm_reference={reference_price:.4f};strike_steps={strike_steps}"
        )
        selected_rows.append(selected)
    return pd.concat(selected_rows, ignore_index=True) if selected_rows else pd.DataFrame()

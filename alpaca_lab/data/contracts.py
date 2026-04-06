from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd

from alpaca_lab.data.normalization import normalize_selected_option_contracts


def build_reference_prices(
    stock_bars: pd.DataFrame,
    *,
    reference_window_minutes: int = 5,
    timezone_name: str = "America/New_York",
) -> pd.DataFrame:
    if reference_window_minutes <= 0:
        raise ValueError("reference_window_minutes must be positive.")
    if stock_bars.empty:
        return pd.DataFrame(
            columns=["underlying_symbol", "trade_date", "reference_timestamp", "reference_price"]
        )

    frame = stock_bars.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"]).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    if frame.empty:
        return pd.DataFrame(
            columns=["underlying_symbol", "trade_date", "reference_timestamp", "reference_price"]
        )

    frame["trade_date"] = (
        frame["timestamp"].dt.tz_convert(timezone_name).dt.normalize().dt.tz_localize(None)
    )

    rows: list[dict[str, object]] = []
    for (symbol, trade_date_value), subset in frame.groupby(["symbol", "trade_date"], sort=True):
        window = subset.head(reference_window_minutes)
        rows.append(
            {
                "underlying_symbol": symbol,
                "trade_date": trade_date_value,
                "reference_timestamp": window["timestamp"].iloc[-1],
                "reference_price": float(window["close"].median()),
            }
        )
    return pd.DataFrame(rows)


def select_contracts_for_trade_date(
    contracts: pd.DataFrame,
    stock_bars: pd.DataFrame,
    *,
    trade_date: date,
    min_dte: int,
    max_dte: int,
    strike_steps: int,
    option_types: Iterable[str] = ("call", "put"),
    reference_window_minutes: int = 5,
) -> pd.DataFrame:
    if strike_steps < 0:
        raise ValueError("strike_steps must be non-negative.")

    references = build_reference_prices(stock_bars, reference_window_minutes=reference_window_minutes)
    if references.empty:
        return normalize_selected_option_contracts(pd.DataFrame())

    trade_day = pd.Timestamp(trade_date).normalize()
    day_reference = references[references["trade_date"] == trade_day]
    if day_reference.empty:
        return normalize_selected_option_contracts(pd.DataFrame())

    inventory = contracts.copy()
    if inventory.empty:
        return normalize_selected_option_contracts(pd.DataFrame())

    inventory["expiration_date"] = pd.to_datetime(inventory["expiration_date"], errors="coerce").dt.normalize()
    inventory["option_type"] = inventory["option_type"].astype("string").str.lower()
    inventory["strike_price"] = pd.to_numeric(inventory["strike_price"], errors="coerce")
    inventory = inventory.dropna(subset=["underlying_symbol", "symbol", "expiration_date", "strike_price"])
    if inventory.empty:
        return normalize_selected_option_contracts(pd.DataFrame())

    allowed_types = {value.strip().lower() for value in option_types if str(value).strip()}
    rows: list[pd.DataFrame] = []

    for reference in day_reference.itertuples():
        subset = inventory[inventory["underlying_symbol"] == reference.underlying_symbol].copy()
        if subset.empty:
            continue
        subset["dte"] = (subset["expiration_date"] - trade_day).dt.days
        subset = subset[(subset["dte"] >= min_dte) & (subset["dte"] <= max_dte)]
        if allowed_types:
            subset = subset[subset["option_type"].isin(allowed_types)]
        if subset.empty:
            continue

        for (option_type, expiration_date), contract_group in subset.groupby(
            ["option_type", "expiration_date"],
            sort=True,
        ):
            strikes = sorted(float(value) for value in contract_group["strike_price"].dropna().unique())
            if not strikes:
                continue

            atm_index = min(
                range(len(strikes)),
                key=lambda index: (abs(strikes[index] - reference.reference_price), strikes[index]),
            )
            atm_strike = strikes[atm_index]
            lower_index = max(0, atm_index - strike_steps)
            upper_index = min(len(strikes) - 1, atm_index + strike_steps)
            selected_strikes = strikes[lower_index : upper_index + 1]
            step_lookup = {
                strike: selected_index - atm_index
                for selected_index, strike in enumerate(strikes)
                if strike in selected_strikes
            }

            selected = contract_group[contract_group["strike_price"].isin(selected_strikes)].copy()
            if selected.empty:
                continue

            selected["trade_date"] = trade_day
            selected["reference_timestamp"] = reference.reference_timestamp
            selected["reference_price"] = reference.reference_price
            selected["atm_strike"] = atm_strike
            selected["relative_strike_step"] = selected["strike_price"].map(step_lookup).astype("Int64")
            selected["selection_reason"] = selected.apply(
                lambda row: (
                    f"reference=median_first_{reference_window_minutes}m_close;"
                    f" dte={int(row['dte'])};"
                    f" option_type={option_type};"
                    f" expiration_date={pd.Timestamp(expiration_date).date().isoformat()};"
                    f" atm_strike={atm_strike:.4f};"
                    f" strike_step={int(row['relative_strike_step'])};"
                    f" within_plus_minus_{strike_steps}_steps"
                ),
                axis=1,
            )
            rows.append(
                selected[
                    [
                        "trade_date",
                        "reference_timestamp",
                        "reference_price",
                        "underlying_symbol",
                        "symbol",
                        "expiration_date",
                        "option_type",
                        "strike_price",
                        "dte",
                        "atm_strike",
                        "relative_strike_step",
                        "selection_reason",
                        "inventory_status",
                    ]
                ]
            )

    if not rows:
        return normalize_selected_option_contracts(pd.DataFrame())
    return normalize_selected_option_contracts(pd.concat(rows, ignore_index=True))

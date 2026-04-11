from __future__ import annotations

import math
from statistics import NormalDist


RISK_FREE_RATE = 0.04
NORM = NormalDist()


def norm_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def bs_price(
    *,
    spot: float,
    strike: float,
    years: float,
    rate: float,
    sigma: float,
    option_type: str,
) -> float:
    if years <= 0.0 or sigma <= 0.0 or spot <= 0.0 or strike <= 0.0:
        if option_type == "call":
            return max(spot - strike, 0.0)
        return max(strike - spot, 0.0)
    sqrt_t = math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * years) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    if option_type == "call":
        return spot * NORM.cdf(d1) - strike * math.exp(-rate * years) * NORM.cdf(d2)
    return strike * math.exp(-rate * years) * NORM.cdf(-d2) - spot * NORM.cdf(-d1)


def implied_volatility(
    *,
    spot: float,
    strike: float,
    years: float,
    market_price: float,
    option_type: str,
    rate: float = RISK_FREE_RATE,
) -> float | None:
    intrinsic = max(spot - strike, 0.0) if option_type == "call" else max(strike - spot, 0.0)
    target_price = max(market_price, intrinsic + 0.01)
    if years <= 0.0 or spot <= 0.0 or strike <= 0.0:
        return None
    low = 0.01
    high = 5.0
    low_price = bs_price(
        spot=spot,
        strike=strike,
        years=years,
        rate=rate,
        sigma=low,
        option_type=option_type,
    )
    high_price = bs_price(
        spot=spot,
        strike=strike,
        years=years,
        rate=rate,
        sigma=high,
        option_type=option_type,
    )
    if target_price < low_price - 1e-6 or target_price > high_price + 1e-6:
        return None
    for _ in range(60):
        mid = 0.5 * (low + high)
        estimate = bs_price(
            spot=spot,
            strike=strike,
            years=years,
            rate=rate,
            sigma=mid,
            option_type=option_type,
        )
        if abs(estimate - target_price) <= 1e-4:
            return mid
        if estimate < target_price:
            low = mid
        else:
            high = mid
    return 0.5 * (low + high)


def bs_greeks(
    *,
    spot: float,
    strike: float,
    years: float,
    sigma: float,
    option_type: str,
    rate: float = RISK_FREE_RATE,
) -> dict[str, float]:
    if years <= 0.0 or sigma <= 0.0 or spot <= 0.0 or strike <= 0.0:
        delta = 1.0 if option_type == "call" and spot > strike else 0.0
        if option_type == "put":
            delta = -1.0 if spot < strike else 0.0
        return {"delta": delta, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    sqrt_t = math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * years) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    pdf_d1 = norm_pdf(d1)
    if option_type == "call":
        delta = NORM.cdf(d1)
        theta = (
            -(spot * pdf_d1 * sigma) / (2.0 * sqrt_t)
            - rate * strike * math.exp(-rate * years) * NORM.cdf(d2)
        ) / 365.0
    else:
        delta = NORM.cdf(d1) - 1.0
        theta = (
            -(spot * pdf_d1 * sigma) / (2.0 * sqrt_t)
            + rate * strike * math.exp(-rate * years) * NORM.cdf(-d2)
        ) / 365.0
    gamma = pdf_d1 / (spot * sigma * sqrt_t)
    vega = (spot * pdf_d1 * sqrt_t) / 100.0
    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

OptionType = Literal["call", "put"]
OptionSide = Literal["buy", "sell"]
Regime = Literal["bull", "bear", "choppy"]


@dataclass(frozen=True, slots=True)
class OptionLegTemplate:
    role: str
    option_type: OptionType
    side: OptionSide
    quantity: int = 1
    relative_strike_step: int = 0
    min_dte: int = 0
    max_dte: int = 7

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive.")
        if self.min_dte < 0 or self.max_dte < self.min_dte:
            raise ValueError("DTE window must satisfy 0 <= min_dte <= max_dte.")
        if self.option_type not in {"call", "put"}:
            raise ValueError("option_type must be call or put.")
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell.")

    @property
    def signed_quantity(self) -> int:
        return self.quantity if self.side == "buy" else -self.quantity

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class OptionStrategyTemplate:
    template_id: str
    family: str
    intended_regime: Regime
    thesis: str
    legs: tuple[OptionLegTemplate, ...]
    max_entry_lag_minutes: int = 5
    max_exit_lag_minutes: int = 10

    def __post_init__(self) -> None:
        if not self.legs:
            raise ValueError("strategy template must contain at least one leg.")
        if self.intended_regime not in {"bull", "bear", "choppy"}:
            raise ValueError("intended_regime must be bull, bear, or choppy.")
        if self.max_entry_lag_minutes < 0 or self.max_exit_lag_minutes < 0:
            raise ValueError("lag windows must be non-negative.")

    @property
    def leg_count(self) -> int:
        return len(self.legs)

    @property
    def is_multi_leg(self) -> bool:
        return self.leg_count > 1

    @property
    def required_option_types(self) -> tuple[str, ...]:
        return tuple(sorted({leg.option_type for leg in self.legs}))

    @property
    def gross_long_quantity(self) -> int:
        return sum(leg.quantity for leg in self.legs if leg.side == "buy")

    @property
    def gross_short_quantity(self) -> int:
        return sum(leg.quantity for leg in self.legs if leg.side == "sell")

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["leg_count"] = self.leg_count
        payload["is_multi_leg"] = self.is_multi_leg
        payload["required_option_types"] = list(self.required_option_types)
        payload["gross_long_quantity"] = self.gross_long_quantity
        payload["gross_short_quantity"] = self.gross_short_quantity
        return payload


def qqq_option_native_templates() -> tuple[OptionStrategyTemplate, ...]:
    return (
        OptionStrategyTemplate(
            template_id="qqq_bull_long_call_atm",
            family="long_call",
            intended_regime="bull",
            thesis="Directional upside participation with defined premium risk.",
            legs=(
                OptionLegTemplate(
                    role="long_call",
                    option_type="call",
                    side="buy",
                    relative_strike_step=0,
                    min_dte=1,
                    max_dte=7,
                ),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bull_call_debit_spread",
            family="call_debit_spread",
            intended_regime="bull",
            thesis="Upside participation with lower premium outlay and capped profit.",
            legs=(
                OptionLegTemplate("long_call", "call", "buy", relative_strike_step=0),
                OptionLegTemplate("short_call", "call", "sell", relative_strike_step=2),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bull_long_call_same_day_liquidity_first",
            family="long_call",
            intended_regime="bull",
            thesis="Liquidity-first same-day ATM upside exposure for fast RTH breakout validation.",
            legs=(
                OptionLegTemplate(
                    "long_call",
                    "call",
                    "buy",
                    relative_strike_step=0,
                    min_dte=0,
                    max_dte=0,
                ),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bull_call_debit_spread_one_step",
            family="call_debit_spread",
            intended_regime="bull",
            thesis="Liquidity-first one-step call debit spread to reduce premium outlay while preserving fill quality.",
            legs=(
                OptionLegTemplate("long_call", "call", "buy", relative_strike_step=0),
                OptionLegTemplate("short_call", "call", "sell", relative_strike_step=1),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bull_call_backspread",
            family="call_backspread",
            intended_regime="bull",
            thesis="Convex upside exposure for strong trend or breakout days.",
            legs=(
                OptionLegTemplate("short_call", "call", "sell", relative_strike_step=0),
                OptionLegTemplate(
                    "long_call_wing",
                    "call",
                    "buy",
                    quantity=2,
                    relative_strike_step=2,
                ),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bear_long_put_atm",
            family="long_put",
            intended_regime="bear",
            thesis="Directional downside participation with defined premium risk.",
            legs=(
                OptionLegTemplate(
                    role="long_put",
                    option_type="put",
                    side="buy",
                    relative_strike_step=0,
                    min_dte=1,
                    max_dte=7,
                ),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bear_put_debit_spread",
            family="put_debit_spread",
            intended_regime="bear",
            thesis="Downside participation with lower premium outlay and capped profit.",
            legs=(
                OptionLegTemplate("long_put", "put", "buy", relative_strike_step=0),
                OptionLegTemplate("short_put", "put", "sell", relative_strike_step=-2),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bear_long_put_same_day_liquidity_first",
            family="long_put",
            intended_regime="bear",
            thesis="Liquidity-first same-day ATM downside exposure for fast RTH breakdown validation.",
            legs=(
                OptionLegTemplate(
                    "long_put",
                    "put",
                    "buy",
                    relative_strike_step=0,
                    min_dte=0,
                    max_dte=0,
                ),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bear_put_debit_spread_one_step",
            family="put_debit_spread",
            intended_regime="bear",
            thesis="Liquidity-first one-step put debit spread to improve leg availability versus wider structures.",
            legs=(
                OptionLegTemplate("long_put", "put", "buy", relative_strike_step=0),
                OptionLegTemplate("short_put", "put", "sell", relative_strike_step=-1),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bear_put_backspread",
            family="put_backspread",
            intended_regime="bear",
            thesis="Convex downside exposure for selloff or breakdown days.",
            legs=(
                OptionLegTemplate("short_put", "put", "sell", relative_strike_step=0),
                OptionLegTemplate(
                    "long_put_wing",
                    "put",
                    "buy",
                    quantity=2,
                    relative_strike_step=-2,
                ),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bear_call_credit_spread",
            family="call_credit_spread",
            intended_regime="bear",
            thesis="Defined-risk premium collection when bearish regimes cap upside follow-through.",
            legs=(
                OptionLegTemplate("short_call", "call", "sell", relative_strike_step=2),
                OptionLegTemplate("long_call_wing", "call", "buy", relative_strike_step=4),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_bear_atm_call_credit_spread",
            family="call_credit_spread",
            intended_regime="bear",
            thesis="Higher-credit defined-risk bearish call spread for stronger downside regimes.",
            legs=(
                OptionLegTemplate("short_call", "call", "sell", relative_strike_step=0),
                OptionLegTemplate("long_call_wing", "call", "buy", relative_strike_step=2),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_choppy_iron_butterfly",
            family="iron_butterfly",
            intended_regime="choppy",
            thesis="Defined-risk short-volatility structure for range-bound sessions.",
            legs=(
                OptionLegTemplate("short_call_body", "call", "sell", relative_strike_step=0),
                OptionLegTemplate("short_put_body", "put", "sell", relative_strike_step=0),
                OptionLegTemplate("long_call_wing", "call", "buy", relative_strike_step=2),
                OptionLegTemplate("long_put_wing", "put", "buy", relative_strike_step=-2),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_choppy_iron_butterfly_one_step",
            family="iron_butterfly",
            intended_regime="choppy",
            thesis="Liquidity-first narrow-wing iron butterfly for range-bound sessions where wider wings reduce fill quality.",
            legs=(
                OptionLegTemplate("short_call_body", "call", "sell", relative_strike_step=0),
                OptionLegTemplate("short_put_body", "put", "sell", relative_strike_step=0),
                OptionLegTemplate("long_call_wing", "call", "buy", relative_strike_step=1),
                OptionLegTemplate("long_put_wing", "put", "buy", relative_strike_step=-1),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_choppy_iron_condor",
            family="iron_condor",
            intended_regime="choppy",
            thesis="Defined-risk premium collection for moderate range-bound sessions.",
            legs=(
                OptionLegTemplate("short_call", "call", "sell", relative_strike_step=2),
                OptionLegTemplate("long_call_wing", "call", "buy", relative_strike_step=4),
                OptionLegTemplate("short_put", "put", "sell", relative_strike_step=-2),
                OptionLegTemplate("long_put_wing", "put", "buy", relative_strike_step=-4),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_choppy_long_straddle",
            family="long_straddle",
            intended_regime="choppy",
            thesis="Long-volatility structure for choppy sessions with expansion risk.",
            legs=(
                OptionLegTemplate("long_call", "call", "buy", relative_strike_step=0),
                OptionLegTemplate("long_put", "put", "buy", relative_strike_step=0),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_choppy_long_straddle_same_day_liquidity_first",
            family="long_straddle",
            intended_regime="choppy",
            thesis="Liquidity-first same-day ATM long-volatility structure for expansion from choppy starts.",
            legs=(
                OptionLegTemplate(
                    "long_call",
                    "call",
                    "buy",
                    relative_strike_step=0,
                    min_dte=0,
                    max_dte=0,
                ),
                OptionLegTemplate(
                    "long_put",
                    "put",
                    "buy",
                    relative_strike_step=0,
                    min_dte=0,
                    max_dte=0,
                ),
            ),
        ),
        OptionStrategyTemplate(
            template_id="qqq_choppy_long_strangle",
            family="long_strangle",
            intended_regime="choppy",
            thesis="Lower-premium long-volatility structure for range breaks.",
            legs=(
                OptionLegTemplate("long_call", "call", "buy", relative_strike_step=2),
                OptionLegTemplate("long_put", "put", "buy", relative_strike_step=-2),
            ),
        ),
    )

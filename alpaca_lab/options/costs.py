from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OptionCostModel:
    commission_per_contract: float = 0.65
    slippage_bps: float = 15.0
    minimum_ticket_charge: float = 1.0

    def estimate_fill_price(self, mid_price: float, *, direction: int) -> float:
        multiplier = 1 + (self.slippage_bps / 10_000.0) * direction
        return float(mid_price * multiplier)

    def estimate_ticket_cost(self, contracts: float) -> float:
        return float(max(self.minimum_ticket_charge, abs(contracts) * self.commission_per_contract))

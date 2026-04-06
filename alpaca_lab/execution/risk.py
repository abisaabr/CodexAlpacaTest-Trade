from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from alpaca_lab.options.promotion_board import PromotionCandidate


@dataclass(slots=True)
class RiskLimits:
    max_notional_per_trade: float
    max_open_positions: int
    max_orders_per_run: int
    allowed_asset_classes: tuple[str, ...] = ("stock", "option")


@dataclass(slots=True)
class RiskDecision:
    candidate: PromotionCandidate
    approved: bool
    reasons: list[str] = field(default_factory=list)


def estimate_candidate_notional(candidate: PromotionCandidate) -> float:
    if candidate.notional is not None:
        return float(candidate.notional)
    if candidate.qty is not None and candidate.price is not None:
        return float(candidate.qty * candidate.price * max(candidate.contract_multiplier, 1.0))
    return 0.0


def evaluate_candidate_risk(
    candidate: PromotionCandidate,
    *,
    open_positions: Iterable[dict],
    open_orders: Iterable[dict],
    limits: RiskLimits,
    accepted_so_far: int,
) -> RiskDecision:
    open_positions = list(open_positions)
    open_orders = list(open_orders)
    reasons: list[str] = []
    if candidate.asset_class not in limits.allowed_asset_classes:
        reasons.append(f"asset_class={candidate.asset_class} not allowed")
    if accepted_so_far >= limits.max_orders_per_run:
        reasons.append("max_orders_per_run reached")
    if len(open_positions) >= limits.max_open_positions:
        reasons.append("max_open_positions reached")
    candidate_notional = estimate_candidate_notional(candidate)
    if candidate_notional > limits.max_notional_per_trade:
        reasons.append(
            "candidate_notional="
            f"{candidate_notional:.2f} exceeds "
            "max_notional_per_trade="
            f"{limits.max_notional_per_trade:.2f}"
        )
    if any(position.get("symbol") == candidate.symbol for position in open_positions):
        reasons.append("symbol already present in open positions")
    if any(order.get("symbol") == candidate.symbol for order in open_orders):
        reasons.append("symbol already present in open orders")
    return RiskDecision(candidate=candidate, approved=not reasons, reasons=reasons)

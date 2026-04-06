from __future__ import annotations

from dataclasses import dataclass

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter, OrderRequest
from alpaca_lab.options.promotion_board import PromotionCandidate


@dataclass(frozen=True, slots=True)
class PaperOrderTicket:
    symbol: str
    asset_class: str
    side: str
    strategy_name: str
    qty: float | None
    notional: float | None
    limit_price: float | None
    client_order_id: str


def candidate_to_order_ticket(
    broker: AlpacaBrokerAdapter,
    candidate: PromotionCandidate,
    *,
    request_key: str,
) -> PaperOrderTicket:
    order = broker.build_order_request(
        symbol=candidate.symbol,
        side=candidate.side,
        strategy_name=candidate.strategy_name,
        asset_class="option" if candidate.asset_class == "option" else "stock",
        qty=candidate.qty,
        notional=candidate.notional,
        order_type="limit" if candidate.limit_price is not None else "market",
        limit_price=candidate.limit_price,
        client_order_key=request_key,
        extra={"order_class": "simple"} if candidate.asset_class == "stock" else {},
    )
    return PaperOrderTicket(
        symbol=order.symbol,
        asset_class=order.asset_class,
        side=order.side,
        strategy_name=order.strategy_name,
        qty=order.qty,
        notional=order.notional,
        limit_price=order.limit_price,
        client_order_id=order.client_order_id or "",
    )


def ticket_to_order_request(ticket: PaperOrderTicket) -> OrderRequest:
    return OrderRequest(
        symbol=ticket.symbol,
        side=ticket.side,  # type: ignore[arg-type]
        qty=ticket.qty,
        notional=ticket.notional,
        order_type="limit" if ticket.limit_price is not None else "market",
        time_in_force="day",
        limit_price=ticket.limit_price,
        client_order_id=ticket.client_order_id,
        asset_class="option" if ticket.asset_class == "option" else "stock",
        strategy_name=ticket.strategy_name,
    )

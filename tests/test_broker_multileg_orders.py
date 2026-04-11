from __future__ import annotations

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter, OrderLeg
from alpaca_lab.config import LabSettings


def _broker() -> AlpacaBrokerAdapter:
    return AlpacaBrokerAdapter(
        LabSettings(
            default_underlyings=("QQQ",),
            alpaca_api_key="paper-key",
            alpaca_secret_key="paper-secret",
        )
    )


def test_build_multileg_order_request_formats_payload() -> None:
    broker = _broker()
    request = broker.build_multileg_order_request(
        strategy_name="iron_condor_same_day",
        qty=2,
        order_type="limit",
        time_in_force="day",
        limit_price=-1.25,
        legs=[
            OrderLeg(
                symbol="QQQ260417C00500000",
                side="sell",
                ratio_qty=1,
                position_intent="sell_to_open",
            ),
            OrderLeg(
                symbol="QQQ260417C00505000",
                side="buy",
                ratio_qty=1,
                position_intent="buy_to_open",
            ),
        ],
    )

    payload = request.to_payload()

    assert payload["order_class"] == "mleg"
    assert payload["qty"] == 2.0
    assert payload["type"] == "limit"
    assert payload["limit_price"] == -1.25
    assert "symbol" not in payload
    assert "side" not in payload
    assert len(payload["legs"]) == 2
    assert payload["legs"][0]["position_intent"] == "sell_to_open"
    assert payload["legs"][1]["side"] == "buy"

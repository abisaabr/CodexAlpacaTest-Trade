from __future__ import annotations

import pytest

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import BrokerActionBlockedError, LabSettings, LiveTradingRefusedError


def test_build_order_request_generates_stable_client_order_id() -> None:
    broker = AlpacaBrokerAdapter(LabSettings(default_underlyings=("SPY",)))

    first = broker.build_order_request(
        symbol="SPY", side="buy", strategy_name="demo", client_order_key="abc"
    )
    second = broker.build_order_request(
        symbol="SPY", side="buy", strategy_name="demo", client_order_key="abc"
    )

    assert first.client_order_id == second.client_order_id


def test_submit_order_defaults_to_dry_run_preview() -> None:
    broker = AlpacaBrokerAdapter(LabSettings(default_underlyings=("SPY",)))
    order = broker.build_order_request(symbol="SPY", side="buy", strategy_name="demo", qty=1)

    preview = broker.submit_order(order, dry_run=True)

    assert preview["status"] == "dry_run"
    assert preview["payload"]["symbol"] == "SPY"


def test_submit_order_refuses_live_path() -> None:
    broker = AlpacaBrokerAdapter(LabSettings(default_underlyings=("SPY",)))
    order = broker.build_order_request(symbol="SPY", side="buy", strategy_name="demo", qty=1)
    order.requested_live = True

    with pytest.raises(LiveTradingRefusedError):
        broker.submit_order(order, dry_run=False, explicitly_requested=True)


def test_non_dry_run_requires_explicit_request() -> None:
    broker = AlpacaBrokerAdapter(LabSettings(default_underlyings=("SPY",)))
    order = broker.build_order_request(symbol="SPY", side="buy", strategy_name="demo", qty=1)

    with pytest.raises(BrokerActionBlockedError):
        broker.submit_order(order, dry_run=False, explicitly_requested=False)

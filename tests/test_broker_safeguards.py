from __future__ import annotations

import json

import pytest

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import (
    LIVE_TRADING_BASE_URL,
    BrokerActionBlockedError,
    LabSettings,
    LiveTradingRefusedError,
)


class DummyResponse:
    def __init__(self, payload, *, status_code: int = 200, text: str | None = None) -> None:  # noqa: ANN001
        self.status_code = status_code
        self.text = text or json.dumps(payload)
        self.payload = payload

    def json(self):  # noqa: ANN201
        return self.payload


class DummySession:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def prepare_request(self, request):  # noqa: ANN001
        return request.prepare()

    def request(self, *, method, url, headers, params, json, timeout):  # noqa: ANN001
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "json": json,
                "timeout": timeout,
            }
        )
        return DummyResponse({"id": "paper-order-1", "status": "accepted"})

    def close(self) -> None:
        return None


def _paper_settings() -> LabSettings:
    return LabSettings(
        default_underlyings=("SPY",),
        alpaca_api_key="paper-key",
        alpaca_secret_key="paper-secret",
    )


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


def test_submit_order_rejects_naked_option_sell_to_open() -> None:
    broker = AlpacaBrokerAdapter(LabSettings(default_underlyings=("SPY",)))
    order = broker.build_order_request(
        symbol="SPY260417C00500000",
        side="sell",
        strategy_name="demo",
        asset_class="option",
        qty=1,
        extra={"position_intent": "sell_to_open"},
    )

    with pytest.raises(ValueError, match="sell_to_open orders are blocked"):
        broker.submit_order(order, dry_run=True)


def test_non_dry_run_requires_explicit_request() -> None:
    broker = AlpacaBrokerAdapter(LabSettings(default_underlyings=("SPY",)))
    order = broker.build_order_request(symbol="SPY", side="buy", strategy_name="demo", qty=1)

    with pytest.raises(BrokerActionBlockedError):
        broker.submit_order(order, dry_run=False, explicitly_requested=False)


def test_non_dry_run_paper_submit_is_allowed_only_on_paper_endpoint() -> None:
    session = DummySession()
    broker = AlpacaBrokerAdapter(_paper_settings(), session=session)
    order = broker.build_order_request(symbol="SPY", side="buy", strategy_name="demo", qty=1)

    response = broker.submit_order(order, dry_run=False, explicitly_requested=True)

    assert response["status"] == "accepted"
    assert len(session.calls) == 1
    assert str(session.calls[0]["url"]).startswith("https://paper-api.alpaca.markets/")


def test_submit_order_recovers_existing_order_when_client_order_id_already_exists() -> None:
    class DuplicateClientOrderSession:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.client_order_id: str | None = None

        def prepare_request(self, request):  # noqa: ANN001
            return request.prepare()

        def request(self, *, method, url, headers, params, json, timeout):  # noqa: ANN001
            self.calls.append(
                {
                    "method": method,
                    "url": url,
                    "params": params,
                    "json": json,
                    "timeout": timeout,
                }
            )
            if method == "POST" and str(url).endswith("/v2/orders"):
                self.client_order_id = str(json["client_order_id"])
                return DummyResponse(
                    {"code": 40010001, "message": "client_order_id must be unique"},
                    status_code=422,
                    text='{"code":40010001,"message":"client_order_id must be unique"}',
                )
            if method == "GET" and str(url).endswith("/v2/orders"):
                return DummyResponse(
                    [
                        {
                            "id": "existing-order-1",
                            "status": "accepted",
                            "client_order_id": self.client_order_id,
                        }
                    ]
                )
            raise AssertionError(f"unexpected request {method} {url}")

        def close(self) -> None:
            return None

    session = DuplicateClientOrderSession()
    broker = AlpacaBrokerAdapter(_paper_settings(), session=session)
    order = broker.build_order_request(symbol="SPY", side="buy", strategy_name="demo", qty=1)

    response = broker.submit_order(order, dry_run=False, explicitly_requested=True)

    assert response["id"] == "existing-order-1"
    assert [str(call["method"]) for call in session.calls] == ["POST", "GET"]


def test_submit_order_refuses_mutated_live_base_url_without_request_fallback() -> None:
    session = DummySession()
    settings = _paper_settings()
    settings.alpaca_api_base_url = LIVE_TRADING_BASE_URL
    broker = AlpacaBrokerAdapter(settings, session=session)
    order = broker.build_order_request(symbol="SPY", side="buy", strategy_name="demo", qty=1)

    with pytest.raises(LiveTradingRefusedError, match="paper-api.alpaca.markets"):
        broker.submit_order(order, dry_run=False, explicitly_requested=True)

    assert session.calls == []

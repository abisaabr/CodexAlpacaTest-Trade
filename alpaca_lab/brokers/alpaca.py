from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha1
from typing import Any, Literal

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from alpaca_lab.config import LabSettings, LiveTradingRefusedError
from alpaca_lab.logging_utils import get_logger, redact_value


def _isoformat(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def build_client_order_id(*, strategy_name: str, symbol: str, side: str, request_key: str) -> str:
    digest = sha1(f"{strategy_name}|{symbol}|{side}|{request_key}".encode()).hexdigest()[:16]
    prefix = strategy_name.lower().replace(" ", "-")[:12]
    return f"{prefix}-{digest}"


@dataclass(slots=True)
class OrderRequest:
    symbol: str
    side: Literal["buy", "sell"]
    qty: float | None = None
    notional: float | None = None
    order_type: str = "market"
    time_in_force: str = "day"
    limit_price: float | None = None
    stop_price: float | None = None
    client_order_id: str | None = None
    asset_class: Literal["stock", "option"] = "stock"
    requested_live: bool = False
    strategy_name: str = "manual"
    extra: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "symbol": self.symbol,
            "side": self.side,
            "type": self.order_type,
            "time_in_force": self.time_in_force,
        }
        if self.qty is not None:
            payload["qty"] = self.qty
        if self.notional is not None:
            payload["notional"] = self.notional
        if self.limit_price is not None:
            payload["limit_price"] = self.limit_price
        if self.stop_price is not None:
            payload["stop_price"] = self.stop_price
        if self.client_order_id is not None:
            payload["client_order_id"] = self.client_order_id
        payload.update(self.extra)
        return payload


class AlpacaBrokerAdapter:
    """Single broker gateway for all account, market-data, and paper-order workflows."""

    def __init__(
        self,
        settings: LabSettings,
        *,
        dry_run: bool | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings
        self.dry_run = settings.dry_run if dry_run is None else dry_run
        self.session = session or requests.Session()
        self.logger = get_logger("broker")

    def close(self) -> None:
        self.session.close()

    def _sanitize(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            sanitized: dict[str, Any] = {}
            for key, value in payload.items():
                lowered = key.lower()
                if any(token in lowered for token in ("key", "secret", "token", "authorization")):
                    sanitized[key] = redact_value(str(value))
                else:
                    sanitized[key] = self._sanitize(value)
            return sanitized
        if isinstance(payload, list):
            return [self._sanitize(item) for item in payload]
        return payload

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        api: Literal["trading", "data"],
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        retryable: bool = True,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        base_url = (
            self.settings.trading_api_base_url
            if api == "trading"
            else self.settings.data_api_base_url
        )
        url = f"{base_url}{path}"
        safe_request = {
            "method": method,
            "url": url,
            "params": params or {},
            "json": json_body or {},
            "mode": self.settings.trading_mode,
            "dry_run": self.dry_run,
        }
        self.logger.info("alpaca request %s", self._sanitize(safe_request))

        def do_request() -> requests.Response:
            response = self.session.request(
                method=method,
                url=url,
                headers=self.settings.auth_headers(),
                params=params,
                json=json_body,
                timeout=self.settings.request_timeout_seconds,
            )
            if retryable and response.status_code >= 500:
                response.raise_for_status()
            return response

        if retryable:
            response = retry(
                stop=stop_after_attempt(self.settings.retry_attempts),
                wait=wait_exponential(multiplier=1, min=1, max=8),
                retry=retry_if_exception_type(requests.RequestException),
                reraise=True,
            )(do_request)()
        else:
            response = do_request()

        if response.status_code >= 400:
            raise requests.HTTPError(
                f"Alpaca API error {response.status_code}: {response.text[:400]}",
                response=response,
            )

        payload = response.json() if response.content else {}
        self.logger.info(
            "alpaca response %s",
            self._sanitize(
                {
                    "status_code": response.status_code,
                    "url": url,
                    "keys": sorted(payload.keys()) if isinstance(payload, dict) else "list",
                }
            ),
        )
        return payload

    def ensure_paper_only(self, *, requested_live: bool = False) -> None:
        if requested_live:
            raise LiveTradingRefusedError("Live order routing is refused in this repository.")
        if not self.settings.alpaca_paper_trade:
            raise LiveTradingRefusedError(
                "ALPACA_PAPER_TRADE=false is refused. Use paper mode only."
            )

    def build_order_request(
        self,
        *,
        symbol: str,
        side: Literal["buy", "sell"],
        strategy_name: str,
        asset_class: Literal["stock", "option"] = "stock",
        qty: float | None = None,
        notional: float | None = None,
        order_type: str = "market",
        time_in_force: str = "day",
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
        client_order_key: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> OrderRequest:
        resolved_client_order_id = client_order_id
        if resolved_client_order_id is None:
            request_key = (
                client_order_key or f"{symbol}|{side}|{qty}|{notional}|{limit_price}|{stop_price}"
            )
            resolved_client_order_id = build_client_order_id(
                strategy_name=strategy_name,
                symbol=symbol,
                side=side,
                request_key=request_key,
            )
        return OrderRequest(
            symbol=symbol,
            side=side,
            qty=qty,
            notional=notional,
            order_type=order_type,
            time_in_force=time_in_force,
            limit_price=limit_price,
            stop_price=stop_price,
            client_order_id=resolved_client_order_id,
            asset_class=asset_class,
            strategy_name=strategy_name,
            extra=extra or {},
        )

    def get_account(self) -> dict[str, Any]:
        payload = self._request_json("GET", "/v2/account", api="trading")
        return payload if isinstance(payload, dict) else {}

    def get_positions(self) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/v2/positions", api="trading")
        return payload if isinstance(payload, list) else []

    def get_orders(self, *, status: str = "all", limit: int = 100) -> list[dict[str, Any]]:
        payload = self._request_json(
            "GET",
            "/v2/orders",
            api="trading",
            params={"status": status, "limit": limit},
        )
        return payload if isinstance(payload, list) else []

    def get_order(self, order_id: str) -> dict[str, Any]:
        payload = self._request_json("GET", f"/v2/orders/{order_id}", api="trading")
        return payload if isinstance(payload, dict) else {}

    def get_clock(self) -> dict[str, Any]:
        payload = self._request_json("GET", "/v2/clock", api="trading")
        return payload if isinstance(payload, dict) else {}

    def get_stock_latest_bars(
        self, symbols: list[str], *, feed: str | None = None
    ) -> dict[str, Any]:
        payload = self._request_json(
            "GET",
            "/v2/stocks/bars/latest",
            api="data",
            params={"symbols": ",".join(symbols), "feed": feed or self.settings.alpaca_data_feed},
        )
        return payload if isinstance(payload, dict) else {}

    def get_stock_bars(
        self,
        symbols: list[str],
        *,
        start: datetime | str,
        end: datetime | str,
        timeframe: str = "1Min",
        feed: str | None = None,
        limit: int = 10000,
    ) -> dict[str, Any]:
        params = {
            "symbols": ",".join(symbols),
            "start": _isoformat(start),
            "end": _isoformat(end),
            "timeframe": timeframe,
            "feed": feed or self.settings.alpaca_data_feed,
            "limit": limit,
        }
        aggregated: dict[str, Any] = {"bars": {symbol: [] for symbol in symbols}}
        while True:
            payload = self._request_json("GET", "/v2/stocks/bars", api="data", params=params)
            if isinstance(payload, dict):
                for symbol, rows in payload.get("bars", {}).items():
                    aggregated["bars"].setdefault(symbol, []).extend(rows)
                token = payload.get("next_page_token")
            else:
                token = None
            if not token:
                break
            params["page_token"] = token
        return aggregated

    def get_option_contracts(
        self,
        underlyings: list[str],
        *,
        expiration_date_gte: str,
        expiration_date_lte: str,
        option_type: str | None = None,
        status: str | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "underlying_symbols": ",".join(underlyings),
            "expiration_date_gte": expiration_date_gte,
            "expiration_date_lte": expiration_date_lte,
            "limit": limit,
        }
        if status:
            params["status"] = status
        if option_type and option_type != "any":
            params["type"] = option_type
        aggregated: dict[str, Any] = {"option_contracts": []}
        while True:
            payload = self._request_json(
                "GET", "/v2/options/contracts", api="trading", params=params
            )
            if isinstance(payload, dict):
                aggregated["option_contracts"].extend(payload.get("option_contracts", []))
                token = payload.get("next_page_token")
            else:
                token = None
            if not token:
                break
            params["page_token"] = token
        return aggregated

    def get_option_bars(
        self,
        symbols: list[str],
        *,
        start: datetime | str,
        end: datetime | str,
        timeframe: str = "1Min",
        limit: int = 10000,
    ) -> dict[str, Any]:
        params = {
            "symbols": ",".join(symbols),
            "start": _isoformat(start),
            "end": _isoformat(end),
            "timeframe": timeframe,
            "limit": limit,
        }
        aggregated: dict[str, Any] = {"bars": {symbol: [] for symbol in symbols}}
        while True:
            payload = self._request_json("GET", "/v1beta1/options/bars", api="data", params=params)
            if isinstance(payload, dict):
                for symbol, rows in payload.get("bars", {}).items():
                    aggregated["bars"].setdefault(symbol, []).extend(rows)
                token = payload.get("next_page_token")
            else:
                token = None
            if not token:
                break
            params["page_token"] = token
        return aggregated

    def get_option_trades(
        self,
        symbols: list[str],
        *,
        start: datetime | str,
        end: datetime | str,
        limit: int = 10000,
    ) -> dict[str, Any]:
        params = {
            "symbols": ",".join(symbols),
            "start": _isoformat(start),
            "end": _isoformat(end),
            "limit": limit,
        }
        aggregated: dict[str, Any] = {"trades": {symbol: [] for symbol in symbols}}
        while True:
            payload = self._request_json(
                "GET", "/v1beta1/options/trades", api="data", params=params
            )
            if isinstance(payload, dict):
                for symbol, rows in payload.get("trades", {}).items():
                    aggregated["trades"].setdefault(symbol, []).extend(rows)
                token = payload.get("next_page_token")
            else:
                token = None
            if not token:
                break
            params["page_token"] = token
        return aggregated

    def get_option_latest_quotes(self, symbols: list[str]) -> dict[str, Any]:
        payload = self._request_json(
            "GET",
            "/v1beta1/options/quotes/latest",
            api="data",
            params={"symbols": ",".join(symbols)},
        )
        return payload if isinstance(payload, dict) else {}

    def get_option_snapshots(self, symbols: list[str]) -> dict[str, Any]:
        payload = self._request_json(
            "GET",
            "/v1beta1/options/snapshots",
            api="data",
            params={"symbols": ",".join(symbols)},
        )
        return payload if isinstance(payload, dict) else {}

    def submit_order(
        self,
        order: OrderRequest,
        *,
        explicitly_requested: bool = False,
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        self.ensure_paper_only(requested_live=order.requested_live)
        use_dry_run = self.dry_run if dry_run is None else dry_run
        payload = order.to_payload()
        if use_dry_run:
            return {
                "status": "dry_run",
                "mode": self.settings.trading_mode,
                "payload": self._sanitize(payload),
            }
        self.settings.require_destructive_broker_action(
            action="paper order submission",
            explicitly_requested=explicitly_requested,
            requested_live=order.requested_live,
        )
        response = self._request_json(
            "POST",
            "/v2/orders",
            api="trading",
            json_body=payload,
            retryable=False,
        )
        return response if isinstance(response, dict) else {"status": "unknown"}

    def cancel_order(
        self,
        order_id: str,
        *,
        explicitly_requested: bool = False,
        requested_live: bool = False,
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        self.ensure_paper_only(requested_live=requested_live)
        use_dry_run = self.dry_run if dry_run is None else dry_run
        if use_dry_run:
            return {"status": "dry_run", "order_id": order_id, "mode": self.settings.trading_mode}
        self.settings.require_destructive_broker_action(
            action="paper order cancel",
            explicitly_requested=explicitly_requested,
            requested_live=requested_live,
        )
        response = self.session.request(
            method="DELETE",
            url=f"{self.settings.trading_api_base_url}/v2/orders/{order_id}",
            headers=self.settings.auth_headers(),
            timeout=self.settings.request_timeout_seconds,
        )
        if response.status_code not in (200, 204):
            raise requests.HTTPError(
                f"Alpaca API error {response.status_code}: {response.text[:400]}",
                response=response,
            )
        return {"status": "cancelled", "order_id": order_id}

    def read_only_connectivity_probe(self) -> dict[str, Any]:
        account = self.get_account()
        latest_bars = self.get_stock_latest_bars(["SPY"])
        return {
            "account_status": account.get("status"),
            "buying_power": account.get("buying_power"),
            "latest_bar_symbols": sorted(latest_bars.get("bars", {}).keys()),
        }

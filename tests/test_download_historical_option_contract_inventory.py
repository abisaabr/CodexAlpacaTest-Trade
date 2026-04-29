from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts.download_historical_option_contract_inventory import (
    build_inventory_coverage_diagnostics,
    download_historical_option_contract_inventory,
)


class FakeContractInventoryBroker:
    def __init__(self) -> None:
        self.calls: list[dict[str, str | None]] = []

    def get_option_contracts(
        self,
        underlyings,
        *,
        expiration_date_gte,
        expiration_date_lte,
        option_type=None,
        status=None,
    ):  # noqa: ANN001
        self.calls.append(
            {
                "underlying": underlyings[0],
                "expiration_date_gte": expiration_date_gte,
                "expiration_date_lte": expiration_date_lte,
                "option_type": option_type,
                "status": status,
            }
        )
        rows = {
            "active": [
                {
                    "id": "active-call",
                    "symbol": "GLD260424C00100000",
                    "name": "GLD Apr 24 2026 100 Call",
                    "status": "active",
                    "tradable": True,
                    "expiration_date": "2026-04-24",
                    "root_symbol": "GLD",
                    "underlying_symbol": "GLD",
                    "type": "call",
                    "style": "american",
                    "strike_price": "100",
                    "size": "100",
                }
            ],
            "inactive": [
                {
                    "id": "inactive-call-duplicate",
                    "symbol": "GLD260424C00100000",
                    "name": "GLD Apr 24 2026 100 Call",
                    "status": "inactive",
                    "tradable": False,
                    "expiration_date": "2026-04-24",
                    "root_symbol": "GLD",
                    "underlying_symbol": "GLD",
                    "type": "call",
                    "style": "american",
                    "strike_price": "100",
                    "size": "100",
                },
                {
                    "id": "inactive-put",
                    "symbol": "GLD260424P00100000",
                    "name": "GLD Apr 24 2026 100 Put",
                    "status": "inactive",
                    "tradable": False,
                    "expiration_date": "2026-04-24",
                    "root_symbol": "GLD",
                    "underlying_symbol": "GLD",
                    "type": "put",
                    "style": "american",
                    "strike_price": "100",
                    "size": "100",
                },
            ],
        }
        return {"option_contracts": rows.get(str(status), [])}


def test_downloads_active_and_inactive_inventory_with_dedupe(tmp_path: Path) -> None:
    broker = FakeContractInventoryBroker()

    packet = download_historical_option_contract_inventory(
        broker=broker,
        symbols=["GLD"],
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 23),
        build_name="contract-inventory-test",
        data_root=tmp_path / "data",
        reports_root=tmp_path / "reports",
        min_dte=0,
        max_dte=7,
        contract_chunk_days=30,
        statuses=("active", "inactive"),
    )

    assert packet["trading_effect"] == "none"
    assert packet["broker_order_effect"] == "none"
    assert packet["contract_count"] == 2
    assert packet["coverage_diagnostics"]["status"] == "ok"
    assert {call["status"] for call in broker.calls} == {"active", "inactive"}

    inventory = pd.read_parquet(packet["combined_inventory_path"])
    assert set(inventory["symbol"]) == {"GLD260424C00100000", "GLD260424P00100000"}
    duplicate_call = inventory[inventory["symbol"] == "GLD260424C00100000"].iloc[0]
    assert duplicate_call["inventory_status"] == "active"
    assert Path(packet["option_contracts_root"]).exists()
    assert (
        tmp_path
        / "reports"
        / "contract-inventory-test"
        / "historical_option_contract_inventory_packet.json"
    ).exists()


def test_inventory_coverage_diagnostics_flags_historical_gap() -> None:
    inventory = pd.DataFrame(
        {
            "symbol": ["QQQ260424C00400000"],
            "underlying_symbol": ["QQQ"],
            "expiration_date": [date(2026, 4, 24)],
            "option_type": ["call"],
            "strike_price": [400.0],
        }
    )

    diagnostics = build_inventory_coverage_diagnostics(
        inventory,
        symbols=["QQQ"],
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 23),
        min_dte=0,
        max_dte=7,
    )

    assert diagnostics["status"] == "inventory_trade_date_coverage_gap"
    assert diagnostics["low_inventory_coverage_symbols"] == ["QQQ"]
    assert diagnostics["symbol_coverage"][0]["covered_trade_date_count"] < 17
    assert diagnostics["symbol_coverage"][0]["missing_trade_dates_sample"][0] == "2026-04-01"

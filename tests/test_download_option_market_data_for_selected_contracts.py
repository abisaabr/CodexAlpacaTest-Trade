from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts.download_option_market_data_for_selected_contracts import (
    download_option_market_data_for_selected_contracts,
)


class FakeOptionDataBroker:
    def __init__(self) -> None:
        self.bar_calls = 0
        self.trade_calls = 0

    def get_option_bars(self, symbols, *, start, end, timeframe):  # noqa: ANN001
        self.bar_calls += 1
        return {
            "bars": {
                symbol: [
                    {
                        "t": "2026-04-21T14:00:00Z",
                        "o": 1.0,
                        "h": 1.2,
                        "l": 0.9,
                        "c": 1.1,
                        "v": 10,
                    }
                ]
                for symbol in symbols
            }
        }

    def get_option_trades(self, symbols, *, start, end):  # noqa: ANN001
        self.trade_calls += 1
        return {
            "trades": {
                symbol: [{"t": "2026-04-21T14:00:00Z", "p": 1.1, "s": 1}] for symbol in symbols
            }
        }


def test_downloads_option_market_data_from_arbitrary_selected_root(tmp_path: Path) -> None:
    selected_root = tmp_path / "selected_option_contracts"
    selected_path = selected_root / "underlying=GLD" / "trade_date=2026-04-21" / "part.parquet"
    selected_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-04-21"]),
            "reference_timestamp": pd.to_datetime(["2026-04-21T14:00:00Z"], utc=True),
            "reference_price": [95.2],
            "underlying_symbol": ["GLD"],
            "symbol": ["GLD260424P00095000"],
            "expiration_date": pd.to_datetime(["2026-04-24"]),
            "option_type": ["put"],
            "strike_price": [95.0],
            "dte": [3],
            "atm_strike": [95.0],
            "relative_strike_step": [0],
            "selection_reason": ["reference=strategy_entry_time"],
            "inventory_status": ["inactive"],
        }
    ).to_parquet(selected_path, index=False)

    broker = FakeOptionDataBroker()
    packet = download_option_market_data_for_selected_contracts(
        broker=broker,
        selected_contracts_root=selected_root,
        build_name="event-selected-test",
        data_root=tmp_path / "data",
        reports_root=tmp_path / "reports",
        option_batch_size=10,
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 21),
    )

    assert packet["broker_facing"] is False
    assert packet["trading_effect"] == "none"
    assert packet["selected_contract_count"] == 1
    assert packet["failed_chunk_count"] == 0
    assert packet["row_count_by_dataset"] == {"option_bars": 1, "option_trades": 1}
    assert broker.bar_calls == 1
    assert broker.trade_calls == 1
    silver_root = Path(packet["silver_root"])
    assert (silver_root / "selected_option_contracts").exists()
    assert (silver_root / "option_bars").exists()
    assert (silver_root / "option_trades").exists()


def test_downloader_rejects_disabled_datasets(tmp_path: Path) -> None:
    try:
        download_option_market_data_for_selected_contracts(
            broker=FakeOptionDataBroker(),
            selected_contracts_root=tmp_path / "missing",
            build_name="bad",
            data_root=tmp_path / "data",
            reports_root=tmp_path / "reports",
            option_batch_size=10,
            include_option_bars=False,
            include_option_trades=False,
        )
    except ValueError as exc:
        assert "At least one" in str(exc)
    else:
        raise AssertionError("Expected ValueError")

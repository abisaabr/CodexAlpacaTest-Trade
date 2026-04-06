from __future__ import annotations

from datetime import date
from pathlib import Path

from alpaca_lab.config import LabSettings
from alpaca_lab.data.historical_builder import HistoricalBuildRequest, HistoricalDatasetBuilder


class FakeHistoricalBroker:
    def __init__(self) -> None:
        self.stock_calls = 0
        self.contract_calls = 0

    def get_stock_bars(self, symbols, *, start, end, timeframe, feed):  # noqa: ANN001
        self.stock_calls += 1
        symbol = symbols[0]
        return {
            "bars": {
                symbol: [
                    {
                        "t": "2026-04-01T13:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99.5,
                        "c": 100.0,
                        "v": 1000,
                    },
                    {
                        "t": "2026-04-01T13:31:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99.5,
                        "c": 101.0,
                        "v": 900,
                    },
                    {
                        "t": "2026-04-01T13:32:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99.5,
                        "c": 102.0,
                        "v": 800,
                    },
                    {
                        "t": "2026-04-01T13:33:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99.5,
                        "c": 101.0,
                        "v": 700,
                    },
                    {
                        "t": "2026-04-01T13:34:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99.5,
                        "c": 100.0,
                        "v": 600,
                    },
                ]
            }
        }

    def get_option_contracts(
        self, underlyings, *, expiration_date_gte, expiration_date_lte, option_type, status
    ):  # noqa: ANN001
        self.contract_calls += 1
        underlying = underlyings[0]
        return {
            "option_contracts": [
                {
                    "id": f"{underlying}-100-call",
                    "symbol": f"{underlying}260403C00100000",
                    "underlying_symbol": underlying,
                    "expiration_date": "2026-04-03",
                    "type": "call",
                    "strike_price": 100,
                    "status": "inactive",
                },
                {
                    "id": f"{underlying}-105-put",
                    "symbol": f"{underlying}260403P00105000",
                    "underlying_symbol": underlying,
                    "expiration_date": "2026-04-03",
                    "type": "put",
                    "strike_price": 105,
                    "status": "inactive",
                },
            ]
        }


def test_historical_builder_resumes_completed_chunks(tmp_path: Path) -> None:
    settings = LabSettings(
        default_underlyings=("SPY",), data_root=tmp_path / "data", reports_root=tmp_path / "reports"
    )
    broker = FakeHistoricalBroker()
    builder = HistoricalDatasetBuilder(settings, broker)  # type: ignore[arg-type]
    request = HistoricalBuildRequest(
        stock_symbols=("SPY",),
        option_underlyings=("SPY",),
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 1),
        stock_chunk_days=1,
        contract_chunk_days=10,
        include_option_bars=False,
        include_option_trades=False,
        include_latest_enrichment=False,
        build_name="resume-test",
    )

    first = builder.build(request)
    second = builder.build(request)

    assert broker.stock_calls == 1
    assert broker.contract_calls == 2
    assert first.manifest_path == second.manifest_path
    assert (first.report_root / "build_summary.md").exists()

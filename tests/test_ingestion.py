from __future__ import annotations

from pathlib import Path

import pandas as pd

from alpaca_lab.config import LabSettings
from alpaca_lab.data.ingestion import DataIngestionService, StockBarIngestionRequest


class FakeIngestionBroker:
    def get_stock_bars(self, symbols, *, start, end, timeframe, feed):  # noqa: ANN001
        symbol = symbols[0]
        return {
            "bars": {
                symbol: [
                    {
                        "t": "2026-04-01T13:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    },
                    {
                        "t": "2026-04-01T13:30:00Z",
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100.5,
                        "v": 1000,
                    },
                ]
            }
        }


def test_stock_ingestion_writes_manifest_and_deduped_silver(tmp_path: Path) -> None:
    settings = LabSettings(
        default_underlyings=("SPY",), data_root=tmp_path / "data", reports_root=tmp_path / "reports"
    )
    service = DataIngestionService(settings, FakeIngestionBroker())  # type: ignore[arg-type]

    metadata = service.ingest_stock_bars(
        StockBarIngestionRequest(
            symbols=("SPY",),
            start="2026-04-01T13:30:00Z",
            end="2026-04-01T14:00:00Z",
        )
    )

    silver_path = Path(metadata.artifacts["silver"])
    manifest_path = settings.raw_data_dir / "manifests" / f"{metadata.dataset_name}.json"
    frame = pd.read_parquet(silver_path)

    assert len(frame) == 1
    assert manifest_path.exists()
    assert metadata.row_count == 1

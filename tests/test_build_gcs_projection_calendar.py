from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_gcs_projection_calendar import (
    build_gcs_projection_calendar,
    extract_trade_dates,
)


def test_extract_trade_dates_from_gcs_listing_lines() -> None:
    lines = [
        "gs://bucket/options/underlying=QQQ/trade_date=2025-01-02/part-0.parquet",
        "gs://bucket/options/underlying=QQQ/trade_date=2025-01-02/part-1.parquet",
        "gs://bucket/options/underlying=QQQ/trade_date=2025-01-03/part-0.parquet",
        "TOTAL: 3 objects",
    ]

    assert extract_trade_dates(lines) == ["2025-01-02", "2025-01-03"]


def test_build_gcs_projection_calendar_uses_union_of_symbol_dates(tmp_path: Path) -> None:
    launch_rows = [
        {
            "symbol": "QQQ",
            "dataset_id": "dense_qqq",
            "bars": "gs://data/qqq/options/",
        },
        {
            "symbol": "SPY",
            "dataset_id": "dense_spy",
            "bars": "gs://data/spy/options/",
        },
    ]
    launch_rows_path = tmp_path / "launch_rows.json"
    launch_rows_path.write_text(json.dumps(launch_rows), encoding="utf-8")

    listing_cache = {
        "gs://data/qqq/options/": [
            "gs://data/qqq/options/underlying=QQQ/trade_date=2025-01-02/part.parquet",
            "gs://data/qqq/options/underlying=QQQ/trade_date=2025-01-03/part.parquet",
        ],
        "gs://data/spy/options/": [
            "gs://data/spy/options/underlying=SPY/trade_date=2025-01-03/part.parquet",
            "gs://data/spy/options/underlying=SPY/trade_date=2025-01-06/part.parquet",
        ],
    }
    cache_path = tmp_path / "listing_cache.json"
    cache_path.write_text(json.dumps(listing_cache), encoding="utf-8")

    packet = build_gcs_projection_calendar(
        launch_rows_json=launch_rows_path,
        output_dir=tmp_path / "out",
        listing_cache_json=cache_path,
    )

    assert packet["status"] == "projection_calendar_complete"
    assert packet["trade_date_count"] == 3
    assert packet["symbols_with_dates"] == 2
    assert packet["first_trade_date"] == "2025-01-02"
    assert packet["last_trade_date"] == "2025-01-06"

    calendar = pd.read_csv(tmp_path / "out" / "projection_calendar.csv")
    assert list(calendar["trade_date"]) == ["2025-01-02", "2025-01-03", "2025-01-06"]
    assert list(calendar["symbol_count"]) == [1, 2, 1]

    summary = pd.read_csv(tmp_path / "out" / "projection_calendar_symbol_summary.csv")
    assert set(summary["symbol"]) == {"QQQ", "SPY"}

from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.build_projection_calendar import build_projection_calendar


def test_build_projection_calendar_uses_option_trade_date_partitions(tmp_path: Path) -> None:
    option_root = tmp_path / "option_bars"
    for trade_date in ["2025-01-02", "2025-01-03", "2025-01-06"]:
        partition = option_root / "option_bars" / "underlying=QQQ" / f"trade_date={trade_date}" / "batch=000"
        partition.mkdir(parents=True)
        (partition / "part.parquet").write_text("placeholder", encoding="utf-8")

    labels = tmp_path / "regime_labels.csv"
    pd.DataFrame(
        [
            {"trade_date": "2025-01-02", "regime": "bull"},
            {"trade_date": "2025-01-03", "regime": "bear"},
            {"trade_date": "2025-01-06", "regime": "choppy"},
            {"trade_date": "2025-01-07", "regime": "mixed"},
        ]
    ).to_csv(labels, index=False)

    packet = build_projection_calendar(
        option_bars_root=option_root,
        output_dir=tmp_path / "out",
        symbol="QQQ",
        regime_labels_csv=labels,
    )

    assert packet["trade_date_count"] == 3
    assert packet["first_trade_date"] == "2025-01-02"
    assert packet["last_trade_date"] == "2025-01-06"
    assert packet["regime_counts"] == {"bear": 1, "bull": 1, "choppy": 1}

    calendar = pd.read_csv(tmp_path / "out" / "projection_calendar.csv")
    assert list(calendar["trade_date"]) == ["2025-01-02", "2025-01-03", "2025-01-06"]

from __future__ import annotations

from datetime import UTC
from pathlib import Path

import pandas as pd

from scripts.build_qqq_regime_labels import build_regime_packet, label_regimes


def _synthetic_intraday_bars() -> pd.DataFrame:
    dates = pd.date_range("2025-01-02", periods=120, freq="B")
    closes: list[float] = []
    closes.extend([100.0 + (0.05 if index % 2 else -0.05) for index in range(40)])
    closes.extend([100.0 + index * 0.55 for index in range(40)])
    closes.extend([122.0 - index * 0.70 for index in range(40)])
    rows = []
    for trade_date, close in zip(dates, closes, strict=True):
        open_price = close * 0.999
        rows.append(
            {
                "timestamp": pd.Timestamp(trade_date).replace(hour=14, minute=30, tzinfo=UTC),
                "symbol": "QQQ",
                "open": open_price,
                "high": max(open_price, close) * 1.001,
                "low": min(open_price, close) * 0.999,
                "close": open_price,
                "volume": 1000,
            }
        )
        rows.append(
            {
                "timestamp": pd.Timestamp(trade_date).replace(hour=20, minute=0, tzinfo=UTC),
                "symbol": "QQQ",
                "open": open_price,
                "high": max(open_price, close) * 1.001,
                "low": min(open_price, close) * 0.999,
                "close": close,
                "volume": 2000,
            }
        )
    return pd.DataFrame(rows)


def test_label_regimes_identifies_bull_bear_and_choppy_segments() -> None:
    labels = label_regimes(_synthetic_intraday_bars())

    regimes = set(labels["regime"])
    assert {"bull", "bear", "choppy"}.issubset(regimes)
    assert labels["trade_date"].nunique() == 120
    assert labels["regime_reason"].notna().all()


def test_build_regime_packet_writes_research_artifacts(tmp_path: Path) -> None:
    stock_path = tmp_path / "stock_bars.parquet"
    _synthetic_intraday_bars().to_parquet(stock_path, index=False)

    packet = build_regime_packet(
        stock_bars_path=stock_path,
        output_dir=tmp_path / "out",
        symbol="QQQ",
    )

    assert packet["broker_facing"] is False
    assert packet["live_manifest_effect"] == "none"
    assert packet["risk_policy_effect"] == "none"
    assert packet["trade_date_count"] == 120
    assert packet["regime_counts"]["bull"] > 0
    assert packet["regime_counts"]["bear"] > 0
    assert packet["regime_counts"]["choppy"] > 0
    assert (tmp_path / "out" / "qqq_regime_labels.csv").exists()
    assert (tmp_path / "out" / "qqq_regime_packet.json").exists()
    assert (tmp_path / "out" / "qqq_regime_packet.md").exists()

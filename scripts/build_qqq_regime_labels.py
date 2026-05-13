from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_option_aware_research_backtest import _load_stock_bars

DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "qqq_regime_labels"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build research-only QQQ bull/bear/choppy regime labels from stock bars."
    )
    parser.add_argument("--stock-bars-path", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument("--trend-lookback-days", type=int, default=20)
    parser.add_argument("--vol-lookback-days", type=int, default=20)
    parser.add_argument("--bull-trend-threshold", type=float, default=0.03)
    parser.add_argument("--bear-trend-threshold", type=float, default=-0.03)
    parser.add_argument("--choppy-abs-trend-threshold", type=float, default=0.015)
    parser.add_argument("--high-vol-quantile", type=float, default=0.70)
    return parser.parse_args()


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    result = numerator / denominator.replace(0, pd.NA)
    return pd.to_numeric(result, errors="coerce")


def _daily_bars(stock_bars: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if stock_bars.empty:
        return pd.DataFrame()
    frame = stock_bars.copy()
    if "symbol" in frame.columns:
        frame = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if frame.empty:
        return pd.DataFrame()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["trade_date"] = frame["timestamp"].dt.date
    aggregations: dict[str, Any] = {
        "timestamp": ["min", "max"],
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }
    if "volume" in frame.columns:
        aggregations["volume"] = "sum"
    daily = frame.sort_values("timestamp").groupby("trade_date").agg(aggregations)
    daily.columns = [
        "_".join(item for item in column if item).rstrip("_")
        for column in daily.columns.to_flat_index()
    ]
    daily = daily.reset_index().rename(
        columns={
            "timestamp_min": "session_start",
            "timestamp_max": "session_end",
            "open_first": "open",
            "high_max": "high",
            "low_min": "low",
            "close_last": "close",
            "volume_sum": "volume",
        }
    )
    daily["symbol"] = symbol.upper()
    return daily


def label_regimes(
    stock_bars: pd.DataFrame,
    *,
    symbol: str = "QQQ",
    trend_lookback_days: int = 20,
    vol_lookback_days: int = 20,
    bull_trend_threshold: float = 0.03,
    bear_trend_threshold: float = -0.03,
    choppy_abs_trend_threshold: float = 0.015,
    high_vol_quantile: float = 0.70,
) -> pd.DataFrame:
    daily = _daily_bars(stock_bars, symbol)
    if daily.empty:
        return daily

    daily["daily_return"] = daily["close"].pct_change()
    daily["trend_return"] = daily["close"].pct_change(trend_lookback_days)
    daily["ma_20"] = daily["close"].rolling(trend_lookback_days, min_periods=5).mean()
    daily["realized_vol"] = daily["daily_return"].rolling(
        vol_lookback_days, min_periods=5
    ).std()
    daily["intraday_range_pct"] = _safe_divide(daily["high"] - daily["low"], daily["open"])
    vol_threshold = daily["realized_vol"].quantile(high_vol_quantile)

    labels = []
    reasons = []
    for row in daily.to_dict("records"):
        trend_return = row.get("trend_return")
        realized_vol = row.get("realized_vol")
        close = row.get("close")
        ma_20 = row.get("ma_20")
        if pd.isna(trend_return) or pd.isna(realized_vol) or pd.isna(ma_20):
            labels.append("warmup_unclassified")
            reasons.append("insufficient_rolling_history")
            continue
        if trend_return >= bull_trend_threshold and close >= ma_20:
            labels.append("bull")
            reasons.append("positive_20d_trend_above_ma")
            continue
        if trend_return <= bear_trend_threshold and close <= ma_20:
            labels.append("bear")
            reasons.append("negative_20d_trend_below_ma")
            continue
        if abs(trend_return) <= choppy_abs_trend_threshold and realized_vol <= vol_threshold:
            labels.append("choppy")
            reasons.append("low_abs_trend_and_contained_realized_vol")
            continue
        labels.append("mixed")
        reasons.append("trend_or_vol_not_cleanly_classified")

    daily["regime"] = labels
    daily["regime_reason"] = reasons
    daily["vol_threshold"] = float(vol_threshold) if not pd.isna(vol_threshold) else None
    return daily


def _regime_counts(labels: pd.DataFrame) -> dict[str, int]:
    return dict(sorted(Counter(labels.get("regime", [])).items()))


def build_regime_packet(
    *,
    stock_bars_path: Path,
    output_dir: Path,
    symbol: str = "QQQ",
    trend_lookback_days: int = 20,
    vol_lookback_days: int = 20,
    bull_trend_threshold: float = 0.03,
    bear_trend_threshold: float = -0.03,
    choppy_abs_trend_threshold: float = 0.015,
    high_vol_quantile: float = 0.70,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stock_bars = _load_stock_bars(stock_bars_path, symbol_filter={symbol.upper()})
    labels = label_regimes(
        stock_bars,
        symbol=symbol,
        trend_lookback_days=trend_lookback_days,
        vol_lookback_days=vol_lookback_days,
        bull_trend_threshold=bull_trend_threshold,
        bear_trend_threshold=bear_trend_threshold,
        choppy_abs_trend_threshold=choppy_abs_trend_threshold,
        high_vol_quantile=high_vol_quantile,
    )
    csv_path = output_dir / f"{symbol.lower()}_regime_labels.csv"
    json_path = output_dir / f"{symbol.lower()}_regime_labels.json"
    packet_path = output_dir / f"{symbol.lower()}_regime_packet.json"
    md_path = output_dir / f"{symbol.lower()}_regime_packet.md"
    labels.to_csv(csv_path, index=False)
    labels.to_json(json_path, orient="records", indent=2, date_format="iso")
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "regime_labels_complete",
        "mode": "research_only",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "symbol": symbol.upper(),
        "stock_bars_path": str(stock_bars_path),
        "trade_date_count": int(len(labels)),
        "regime_counts": _regime_counts(labels),
        "parameters": {
            "trend_lookback_days": trend_lookback_days,
            "vol_lookback_days": vol_lookback_days,
            "bull_trend_threshold": bull_trend_threshold,
            "bear_trend_threshold": bear_trend_threshold,
            "choppy_abs_trend_threshold": choppy_abs_trend_threshold,
            "high_vol_quantile": high_vol_quantile,
        },
        "artifacts": {
            "labels_csv": str(csv_path),
            "labels_json": str(json_path),
            "packet_json": str(packet_path),
            "packet_md": str(md_path),
        },
        "next_step_contract": [
            "Use these labels to stratify bull, bear, and choppy QQQ tournaments.",
            "Do not promote a strategy across regimes unless each intended-regime packet passes promotion gates.",
            "Treat warmup_unclassified and mixed days as diagnostics unless a tournament explicitly targets them.",
        ],
    }
    packet_path.write_text(json.dumps(packet, indent=2), encoding="utf-8")
    _write_markdown(md_path, packet)
    return packet


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# QQQ Regime Labels",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Symbol: `{packet['symbol']}`",
        f"- Mode: `{packet['mode']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Trade dates: `{packet['trade_date_count']}`",
        "",
        "## Regime Counts",
        "",
    ]
    for regime, count in packet["regime_counts"].items():
        lines.append(f"- `{regime}`: `{count}`")
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    packet = build_regime_packet(
        stock_bars_path=Path(args.stock_bars_path),
        output_dir=Path(args.output_dir),
        symbol=args.symbol,
        trend_lookback_days=args.trend_lookback_days,
        vol_lookback_days=args.vol_lookback_days,
        bull_trend_threshold=args.bull_trend_threshold,
        bear_trend_threshold=args.bear_trend_threshold,
        choppy_abs_trend_threshold=args.choppy_abs_trend_threshold,
        high_vol_quantile=args.high_vol_quantile,
    )
    print(json.dumps(packet, indent=2))


if __name__ == "__main__":
    main()

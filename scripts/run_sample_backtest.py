from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.backtest.engine import FixedFractionSizer, LinearCostModel, run_backtest
from alpaca_lab.config import load_settings
from alpaca_lab.data.storage import latest_file
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.reporting import write_summary_bundle
from alpaca_lab.strategies.stock_momentum import ConservativeBreakoutStockStrategy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a safe sample backtest using local or synthetic stock bars."
    )
    parser.add_argument("--bars-path", default=None, help="Optional parquet input path.")
    parser.add_argument(
        "--symbols", default="SPY,QQQ", help="Comma-separated symbol list for synthetic fallback."
    )
    parser.add_argument("--initial-cash", type=float, default=100_000.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-per-unit", type=float, default=0.01)
    parser.add_argument("--allocation-fraction", type=float, default=0.10)
    parser.add_argument("--config", default=None, help="Optional YAML config path.")
    parser.add_argument("--synthetic", action="store_true", help="Force synthetic sample data.")
    return parser.parse_args()


def _synthetic_bars(symbols: list[str], periods: int = 240) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    base_index = pd.date_range("2026-01-05 14:30:00+00:00", periods=periods, freq="1min", tz="UTC")
    for offset, symbol in enumerate(symbols):
        base_price = 100 + offset * 25
        wave = np.sin(np.linspace(0, 10, periods))
        trend = np.linspace(0, 4 + offset, periods)
        close = base_price + trend + wave
        open_prices = close - 0.15
        high = close + 0.25
        low = close - 0.25
        volume = 10_000 + (wave * 1000).astype(int) + offset * 500
        for timestamp, open_price, high_price, low_price, close_price, volume_value in zip(
            base_index,
            open_prices,
            high,
            low,
            close,
            volume,
            strict=True,
        ):
            rows.append(
                {
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": int(max(volume_value, 1)),
                }
            )
    return pd.DataFrame(rows)


def _resolve_bars(args: argparse.Namespace, data_root: Path) -> tuple[pd.DataFrame, str]:
    if not args.synthetic:
        if args.bars_path:
            path = Path(args.bars_path)
            return pd.read_parquet(path), str(path)
        latest = latest_file(data_root / "silver" / "stocks", "*.parquet")
        if latest is not None:
            return pd.read_parquet(latest), str(latest)
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    return _synthetic_bars(symbols), "synthetic"


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)

    bars, source_name = _resolve_bars(args, settings.data_root)
    strategy = ConservativeBreakoutStockStrategy()
    result = run_backtest(
        bars,
        strategy,
        initial_cash=args.initial_cash,
        cost_model=LinearCostModel(slippage_bps=args.slippage_bps, fee_per_unit=args.fee_per_unit),
        position_sizer=FixedFractionSizer(base_allocation_fraction=args.allocation_fraction),
    )

    output_root = settings.reports_root / "sample_backtest"
    output_root.mkdir(parents=True, exist_ok=True)
    paths = write_summary_bundle(
        output_root,
        name="sample_backtest",
        summary={"bars_source": source_name, **result.summary},
        table_map={"trades": result.trades, "equity": result.equity_curve},
    )
    print(
        json.dumps(
            {
                "summary": result.summary,
                "bars_source": source_name,
                "artifacts": {k: str(v) for k, v in paths.items()},
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()

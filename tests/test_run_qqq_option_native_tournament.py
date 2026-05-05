from __future__ import annotations

from datetime import UTC
from pathlib import Path

import pandas as pd

from scripts.run_qqq_option_native_tournament import run_tournament


def _write_parquet_tree(root: Path, frame: pd.DataFrame, *, trade_date: str) -> None:
    path = root / "underlying=QQQ" / f"trade_date={trade_date}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _stock_bars() -> pd.DataFrame:
    rows = []
    for trade_date in ["2026-04-27", "2026-04-28", "2026-04-29"]:
        for minute, close in [(0, 500.0), (30, 501.0), (210, 503.0), (390, 502.0)]:
            timestamp = pd.Timestamp(trade_date).replace(
                hour=13, minute=30, tzinfo=UTC
            ) + pd.Timedelta(minutes=minute)
            rows.append(
                {
                    "symbol": "QQQ",
                    "timestamp": timestamp,
                    "open": close,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1000,
                }
            )
    return pd.DataFrame(rows)


def _contracts_and_bars() -> tuple[pd.DataFrame, pd.DataFrame]:
    contracts = []
    bars = []
    for trade_date in ["2026-04-27", "2026-04-28", "2026-04-29"]:
        for option_type in ["call", "put"]:
            for step in [-4, -2, 0, 2, 4]:
                strike = 500 + step
                symbol = f"QQQ260430{option_type[0].upper()}{strike:08d}"
                contracts.append(
                    {
                        "trade_date": pd.Timestamp(trade_date),
                        "reference_timestamp": pd.Timestamp(trade_date).replace(
                            hour=13, minute=30, tzinfo=UTC
                        ),
                        "reference_price": 500.0,
                        "underlying_symbol": "QQQ",
                        "symbol": symbol,
                        "expiration_date": pd.Timestamp("2026-04-30"),
                        "option_type": option_type,
                        "strike_price": float(strike),
                        "dte": 1,
                        "atm_strike": 500.0,
                        "relative_strike_step": step,
                        "selection_reason": "test",
                        "inventory_status": "active",
                    }
                )
                for minute, close in [(30, 2.0 + abs(step) * 0.1), (210, 2.3 + abs(step) * 0.1)]:
                    bars.append(
                        {
                            "symbol": symbol,
                            "underlying_symbol": "QQQ",
                            "trade_date": pd.Timestamp(trade_date),
                            "timestamp": pd.Timestamp(trade_date).replace(
                                hour=13, minute=30, tzinfo=UTC
                            )
                            + pd.Timedelta(minutes=minute),
                            "open": close,
                            "high": close,
                            "low": close,
                            "close": close,
                            "volume": 10,
                            "trade_count": 1,
                            "vwap": close,
                        }
                    )
    return pd.DataFrame(contracts), pd.DataFrame(bars)


def test_option_native_tournament_emits_multileg_strategy_fill(tmp_path: Path) -> None:
    stock_path = tmp_path / "stock_bars.parquet"
    _stock_bars().to_parquet(stock_path, index=False)
    contracts, bars = _contracts_and_bars()
    contracts_root = tmp_path / "selected_contracts"
    bars_root = tmp_path / "option_bars"
    for trade_date, group in contracts.groupby(contracts["trade_date"].dt.date):
        _write_parquet_tree(contracts_root, group, trade_date=str(trade_date))
    for trade_date, group in bars.groupby(bars["trade_date"].dt.date):
        _write_parquet_tree(bars_root, group, trade_date=str(trade_date))
    regime_path = tmp_path / "qqq_regime_labels.csv"
    pd.DataFrame(
        [
            {"trade_date": "2026-04-27", "symbol": "QQQ", "regime": "bull"},
            {"trade_date": "2026-04-28", "symbol": "QQQ", "regime": "bear"},
            {"trade_date": "2026-04-29", "symbol": "QQQ", "regime": "choppy"},
        ]
    ).to_csv(regime_path, index=False)

    packet = run_tournament(
        stock_bars_path=stock_path,
        selected_contracts_root=contracts_root,
        option_bars_root=bars_root,
        option_trades_root=None,
        regime_labels_csv=regime_path,
        output_dir=tmp_path / "out",
        run_id="unit_profile",
        test_date_count=1,
        slippage_bps=0,
        fee_per_contract=0,
    )

    assert packet["broker_facing"] is False
    assert packet["template_count"] == 18
    summaries = {row["source_strategy_id"]: row for row in packet["candidate_summaries"]}
    assert summaries["qqq_bull_long_call_atm"]["strategy_fill_coverage"] == 1.0
    assert summaries["qqq_bull_long_call_atm"]["candidate_variant_id"].startswith(
        "qqq_bull_long_call_atm__fixed_offset"
    )
    assert summaries["qqq_choppy_iron_butterfly"]["strategy_fill_coverage"] == 1.0
    assert summaries["qqq_choppy_iron_butterfly"]["fill_coverage_unit"] == (
        "filled_multi_leg_strategy_orders_per_intended_regime_day"
    )
    assert (tmp_path / "out" / "unit_profile" / "option_aware_candidate_summary.csv").exists()
    assert (tmp_path / "out" / "unit_profile" / "option_aware_trade_economics.csv").exists()

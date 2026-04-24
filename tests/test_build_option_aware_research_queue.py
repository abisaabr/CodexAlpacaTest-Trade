from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_option_aware_research_queue import build_queue


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_option_aware_queue_maps_put_candidate_to_selected_contracts(tmp_path: Path) -> None:
    summary_json = tmp_path / "summary.json"
    _write_json(
        summary_json,
        {
            "top_candidates": [
                {
                    "variant_id": "rq002__gld__put",
                    "symbol": "GLD",
                    "source_strategy_id": "gld__base__trend_long_put_next_expiry",
                    "expectancy_after_cost": 12.5,
                    "net_pnl": 125.0,
                    "actual_trade_count": 10,
                }
            ]
        },
    )
    contracts_path = (
        tmp_path
        / "contracts"
        / "underlying=GLD"
        / "trade_date=2026-04-23"
        / "part.parquet"
    )
    contracts_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-04-23", "2026-04-23"]),
            "reference_timestamp": pd.to_datetime(
                ["2026-04-23T13:34:00Z", "2026-04-23T13:34:00Z"], utc=True
            ),
            "reference_price": [433.5, 433.5],
            "underlying_symbol": ["GLD", "GLD"],
            "symbol": ["GLD260424P00433000", "GLD260424C00433000"],
            "expiration_date": pd.to_datetime(["2026-04-24", "2026-04-24"]),
            "option_type": ["put", "call"],
            "strike_price": [433.0, 433.0],
            "dte": [1, 1],
            "relative_strike_step": [0, 0],
        }
    ).to_parquet(contracts_path, index=False)

    payload = build_queue(
        summary_json=summary_json,
        selected_contracts_root=tmp_path / "contracts",
        option_bars_root=tmp_path / "missing_bars",
        option_trades_root=tmp_path / "missing_trades",
        top_n=10,
        contracts_per_candidate=3,
    )

    assert payload["status"] == "blocked_missing_option_market_data"
    assert payload["promotion_allowed"] is False
    item = payload["queue_items"][0]
    assert item["directional_option_type"] == "put"
    assert item["representative_contract_count"] == 1
    assert item["representative_contracts"][0]["symbol"] == "GLD260424P00433000"
    assert item["blockers"] == ["missing_historical_option_bars", "missing_historical_option_trades"]


def test_option_aware_queue_is_ready_when_market_data_roots_present(tmp_path: Path) -> None:
    summary_json = tmp_path / "summary.json"
    _write_json(
        summary_json,
        {
            "top_candidates": [
                {
                    "variant_id": "rq002__qqq__call",
                    "symbol": "QQQ",
                    "source_strategy_id": "qqq__base__trend_long_call_next_expiry",
                }
            ]
        },
    )
    contracts_path = tmp_path / "contracts" / "underlying=QQQ" / "trade_date=2026-04-23" / "part.parquet"
    contracts_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-04-23"]),
            "reference_timestamp": pd.to_datetime(["2026-04-23T13:34:00Z"], utc=True),
            "reference_price": [430.0],
            "underlying_symbol": ["QQQ"],
            "symbol": ["QQQ260424C00430000"],
            "expiration_date": pd.to_datetime(["2026-04-24"]),
            "option_type": ["call"],
            "strike_price": [430.0],
            "dte": [1],
            "relative_strike_step": [0],
        }
    ).to_parquet(contracts_path, index=False)
    bars_file = tmp_path / "option_bars" / "bars.parquet"
    trades_file = tmp_path / "option_trades" / "trades.parquet"
    bars_file.parent.mkdir(parents=True, exist_ok=True)
    trades_file.parent.mkdir(parents=True, exist_ok=True)
    bars_file.write_text("placeholder", encoding="utf-8")
    trades_file.write_text("placeholder", encoding="utf-8")

    payload = build_queue(
        summary_json=summary_json,
        selected_contracts_root=tmp_path / "contracts",
        option_bars_root=tmp_path / "option_bars",
        option_trades_root=tmp_path / "option_trades",
        top_n=10,
        contracts_per_candidate=3,
    )

    assert payload["status"] == "ready_for_option_aware_backtest"
    assert payload["queue_items"][0]["blockers"] == []

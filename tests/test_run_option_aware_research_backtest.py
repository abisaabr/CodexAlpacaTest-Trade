from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pandas as pd

from scripts.run_option_aware_research_backtest import (
    CONTRACT_SELECTION_LIQUIDITY_FIRST,
    CONTRACT_SELECTION_NEAREST,
    EXIT_LOOKUP_AT_OR_AFTER,
    EXIT_LOOKUP_AT_OR_AFTER_OR_PRIOR,
    STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY,
    STRATEGY_FILL_COVERAGE_GATE,
    _build_option_research_index,
    _candidate_window,
    _exit_option_bar,
    _filter_stock_trades_for_option_session,
    _option_structure_legs,
    _path_matches_symbol_filter,
    _recommendation,
    build_option_aware_backtest,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_variants(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_candidate_window_supports_non_overlapping_gcp_shards() -> None:
    rows = [{"candidate_variant_id": f"v{index}"} for index in range(1, 8)]

    selected, start_offset, end_index, scope_count = _candidate_window(
        rows,
        top_n=6,
        candidate_start_index=3,
        candidate_count=2,
    )

    assert [row["candidate_variant_id"] for row in selected] == ["v3", "v4"]
    assert start_offset == 2
    assert end_index == 4
    assert scope_count == 6


def test_option_aware_backtest_prices_stock_signal_windows_against_options(
    tmp_path: Path,
) -> None:
    variant_id = "rq002__gld_base_trend_long_put_next_expiry__test"
    queue_json = tmp_path / "queue.json"
    variants_jsonl = tmp_path / "variants.jsonl"
    stock_bars_path = tmp_path / "stock_bars.parquet"
    contracts_root = tmp_path / "selected_option_contracts"
    option_bars_root = tmp_path / "option_bars"
    option_trades_root = tmp_path / "option_trades"

    _write_json(
        queue_json,
        {
            "status": "ready_for_option_aware_backtest",
            "selected_contracts_root": str(contracts_root),
            "option_bars_root": str(option_bars_root),
            "option_trades_root": str(option_trades_root),
            "promotion_allowed": False,
            "queue_items": [
                {
                    "candidate_variant_id": variant_id,
                    "symbol": "GLD",
                    "directional_option_type": "put",
                    "family": "Single-leg long put",
                    "source_strategy_id": "gld__base__trend_long_put_next_expiry",
                }
            ],
        },
    )
    _write_variants(
        variants_jsonl,
        [
            {
                "variant_id": variant_id,
                "queue_id": "RQ-002-single-leg-repair-and-loss-filter",
                "symbol": "GLD",
                "variant_type": "single_leg_repair",
                "family": "Single-leg long put",
                "source_strategy_id": "gld__base__trend_long_put_next_expiry",
                "parameters": {
                    "hard_exit_minute": 5,
                    "liquidity_gate": "baseline",
                    "profit_target_multiple": 0.45,
                    "stop_loss_multiple": 0.24,
                },
            }
        ],
    )

    timestamps = pd.date_range("2026-04-21T13:30:00Z", periods=60, freq="min")
    prices = [100.0 - index * 0.2 for index in range(len(timestamps))]
    pd.DataFrame(
        {
            "symbol": ["GLD"] * len(timestamps),
            "timestamp": timestamps,
            "open": prices,
            "high": [price + 0.05 for price in prices],
            "low": [price - 0.05 for price in prices],
            "close": prices,
            "volume": [1000] * len(timestamps),
        }
    ).to_parquet(stock_bars_path, index=False)

    contracts_path = contracts_root / "underlying=GLD" / "trade_date=2026-04-21" / "part.parquet"
    contracts_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-04-21"]),
            "reference_timestamp": pd.to_datetime(["2026-04-21T13:34:00Z"], utc=True),
            "reference_price": [100.0],
            "underlying_symbol": ["GLD"],
            "symbol": ["GLD260424P00100000"],
            "expiration_date": pd.to_datetime(["2026-04-24"]),
            "option_type": ["put"],
            "strike_price": [100.0],
            "dte": [3],
            "relative_strike_step": [0],
        }
    ).to_parquet(contracts_path, index=False)

    option_prices = [2.0 + index * 0.03 for index in range(len(timestamps))]
    option_bars_path = (
        option_bars_root / "underlying=GLD" / "trade_date=2026-04-21" / "batch=000" / "part.parquet"
    )
    option_bars_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": ["GLD260424P00100000"] * len(timestamps),
            "underlying_symbol": ["GLD"] * len(timestamps),
            "trade_date": pd.to_datetime(["2026-04-21"] * len(timestamps)),
            "timestamp": timestamps,
            "open": option_prices,
            "high": [price + 0.01 for price in option_prices],
            "low": [price - 0.01 for price in option_prices],
            "close": option_prices,
            "volume": [10] * len(timestamps),
        }
    ).to_parquet(option_bars_path, index=False)
    option_trades_path = (
        option_trades_root
        / "underlying=GLD"
        / "trade_date=2026-04-21"
        / "batch=000"
        / "part.parquet"
    )
    option_trades_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": ["GLD260424P00100000"] * len(timestamps),
            "underlying_symbol": ["GLD"] * len(timestamps),
            "trade_date": pd.to_datetime(["2026-04-21"] * len(timestamps)),
            "timestamp": timestamps,
            "price": option_prices,
            "size": [1] * len(timestamps),
        }
    ).to_parquet(option_trades_path, index=False)

    payload = build_option_aware_backtest(
        queue_json=queue_json,
        variants_jsonl=variants_jsonl,
        stock_bars_path=stock_bars_path,
        selected_contracts_root=None,
        option_bars_root=None,
        option_trades_root=None,
        top_n=1,
        initial_cash=100_000.0,
        allocation_fraction=0.10,
        slippage_bps=1.0,
        fee_per_contract=0.0,
        max_entry_lag=timedelta(minutes=1),
        max_exit_lag=timedelta(minutes=1),
    )

    assert payload["broker_facing"] is False
    assert payload["promotion_allowed"] is False
    assert payload["option_lookup_mode"] == "indexed_by_contract_and_symbol"
    assert payload["option_index_counts"] == {
        "contract_keys": 1,
        "bar_symbols": 1,
        "trade_symbols": 1,
    }
    assert payload["candidate_count"] == 1
    assert payload["option_trade_count"] > 0
    summary = payload["candidate_summaries"][0]
    assert summary["strategy_id"] == "gld__base__trend_long_put_next_expiry"
    assert summary["family"] == "Single-leg long put"
    assert '"hard_exit_minute":5' in summary["parameter_set"]
    assert payload["trade_rows"][0]["family"] == "Single-leg long put"
    assert summary["fill_coverage"] == 1.0
    assert summary["fill_failure_reason"] == "fill_gate_clear"
    assert summary["net_pnl"] > 0
    assert summary["recommendation"] in {
        "candidate_for_walk_forward_review",
        "hold_option_economics",
    }


def test_recommendation_holds_until_strategy_fill_gate() -> None:
    summary = {
        "source_stock_trade_count": 100,
        "option_trade_count": 89,
        "strategy_fill_coverage": STRATEGY_FILL_COVERAGE_GATE - 0.01,
        "fill_coverage": STRATEGY_FILL_COVERAGE_GATE - 0.01,
        "train_net_pnl": 100.0,
        "test_net_pnl": 100.0,
        "net_pnl": 200.0,
        "expectancy": 2.0,
    }

    assert _recommendation(summary) == "hold_option_fill_coverage"


def test_family_aware_option_structure_builds_vertical_and_iron_butterfly_legs() -> None:
    trade_date = pd.Timestamp("2026-04-21").date()
    entry_time = pd.Timestamp("2026-04-21T13:35:00Z")
    contracts = pd.DataFrame(
        {
            "trade_date": [trade_date] * 4,
            "underlying_symbol": ["QQQ"] * 4,
            "symbol": [
                "QQQ260424C00100000",
                "QQQ260424C00101000",
                "QQQ260424P00100000",
                "QQQ260424P00099000",
            ],
            "option_type": ["call", "call", "put", "put"],
            "strike_price": [100.0, 101.0, 100.0, 99.0],
            "dte": [3, 3, 3, 3],
            "relative_strike_step": [0, 1, 0, -1],
        }
    )
    option_bars = pd.DataFrame(
        {
            "symbol": [
                "QQQ260424C00100000",
                "QQQ260424C00101000",
                "QQQ260424P00100000",
                "QQQ260424P00099000",
            ],
            "timestamp": [entry_time] * 4,
            "close": [2.0, 1.1, 2.2, 1.0],
            "volume": [10, 10, 10, 10],
        }
    )
    option_index = _build_option_research_index(
        contracts=contracts,
        option_bars=option_bars,
        option_trades=pd.DataFrame(),
    )

    vertical_queue = {
        "candidate_variant_id": "qqq_vertical",
        "symbol": "QQQ",
        "directional_option_type": "call",
        "family": "debit_call_vertical",
    }
    vertical_legs, vertical_structure, vertical_status = _option_structure_legs(
        queue_item=vertical_queue,
        variant={"parameters": {"family_template": "debit_call_vertical"}},
        contracts=contracts,
        option_bars=option_bars,
        option_trades=pd.DataFrame(),
        option_index=option_index,
        symbol="QQQ",
        trade_date=trade_date,
        entry_time=entry_time,
        max_entry_lag=timedelta(minutes=1),
        entry_lookup_mode="first_bar_at_or_after_entry_within_lag",
        max_entry_staleness=timedelta(minutes=0),
        contract_selection_method=CONTRACT_SELECTION_NEAREST,
    )

    assert vertical_status == "selected"
    assert vertical_structure == "debit_call_vertical"
    assert [(leg["role"], leg["side"]) for leg in vertical_legs] == [
        ("long_call", 1),
        ("short_call_wing", -1),
    ]

    iron_queue = {
        "candidate_variant_id": "qqq_iron",
        "symbol": "QQQ",
        "directional_option_type": "call",
        "family": "iron_butterfly",
    }
    iron_legs, iron_structure, iron_status = _option_structure_legs(
        queue_item=iron_queue,
        variant={"parameters": {"family_template": "iron_butterfly"}},
        contracts=contracts,
        option_bars=option_bars,
        option_trades=pd.DataFrame(),
        option_index=option_index,
        symbol="QQQ",
        trade_date=trade_date,
        entry_time=entry_time,
        max_entry_lag=timedelta(minutes=1),
        entry_lookup_mode="first_bar_at_or_after_entry_within_lag",
        max_entry_staleness=timedelta(minutes=0),
        contract_selection_method=CONTRACT_SELECTION_NEAREST,
    )

    assert iron_status == "selected"
    assert iron_structure == "iron_butterfly"
    assert [(leg["role"], leg["side"]) for leg in iron_legs] == [
        ("short_call_body", -1),
        ("short_put_body", -1),
        ("long_call_wing", 1),
        ("long_put_wing", 1),
    ]


def test_exit_lookup_defaults_to_strict_at_or_after() -> None:
    bars = pd.DataFrame(
        {
            "symbol": ["QQQ260515C00500000"],
            "timestamp": pd.to_datetime(["2026-05-01T15:58:00Z"], utc=True),
            "close": [2.50],
        }
    )
    exit_time = pd.Timestamp("2026-05-01T16:00:00Z")

    assert (
        _exit_option_bar(
            option_bars=bars,
            contract_symbol="QQQ260515C00500000",
            timestamp=exit_time,
            max_lag=timedelta(minutes=5),
            lookup_mode=EXIT_LOOKUP_AT_OR_AFTER,
        )
        is None
    )
    assert _exit_option_bar(
        option_bars=bars,
        contract_symbol="QQQ260515C00500000",
        timestamp=exit_time,
        max_lag=timedelta(minutes=5),
        lookup_mode=EXIT_LOOKUP_AT_OR_AFTER_OR_PRIOR,
    )["close"] == 2.50


def test_stock_session_filter_keeps_only_same_day_option_rth_trades() -> None:
    trades = pd.DataFrame(
        {
            "entry_time": pd.to_datetime(
                [
                    "2026-05-01T13:40:00Z",
                    "2026-05-01T12:00:00Z",
                    "2026-05-01T19:50:00Z",
                    "2026-05-01T19:50:00Z",
                ],
                utc=True,
            ),
            "exit_time": pd.to_datetime(
                [
                    "2026-05-01T15:00:00Z",
                    "2026-05-01T15:00:00Z",
                    "2026-05-01T20:30:00Z",
                    "2026-05-04T14:00:00Z",
                ],
                utc=True,
            ),
        }
    )

    filtered = _filter_stock_trades_for_option_session(
        trades,
        stock_session_filter=STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY,
    )

    assert len(filtered) == 1
    assert filtered.attrs["raw_source_stock_trade_count"] == 4
    assert filtered.attrs["source_session_dropped_count"] == 3


def test_liquidity_first_selector_uses_entry_window_without_future_bars(
    tmp_path: Path,
) -> None:
    variant_id = "rq002__gld_liquidity_first__test"
    queue_json = tmp_path / "queue.json"
    variants_jsonl = tmp_path / "variants.jsonl"
    stock_bars_path = tmp_path / "stock_bars.parquet"
    contracts_root = tmp_path / "selected_option_contracts"
    option_bars_root = tmp_path / "option_bars"
    option_trades_root = tmp_path / "option_trades"

    _write_json(
        queue_json,
        {
            "status": "ready_for_option_aware_backtest",
            "selected_contracts_root": str(contracts_root),
            "option_bars_root": str(option_bars_root),
            "option_trades_root": str(option_trades_root),
            "promotion_allowed": False,
            "queue_items": [
                {
                    "candidate_variant_id": variant_id,
                    "symbol": "GLD",
                    "directional_option_type": "put",
                    "source_strategy_id": "gld__base__trend_long_put_next_expiry",
                }
            ],
        },
    )
    _write_variants(
        variants_jsonl,
        [
            {
                "variant_id": variant_id,
                "queue_id": "RQ-002-single-leg-repair-and-loss-filter",
                "symbol": "GLD",
                "variant_type": "single_leg_repair",
                "source_strategy_id": "gld__base__trend_long_put_next_expiry",
                "parameters": {
                    "hard_exit_minute": 5,
                    "liquidity_gate": "baseline",
                    "profit_target_multiple": 0.45,
                    "stop_loss_multiple": 0.24,
                },
            }
        ],
    )

    timestamps = pd.date_range("2026-04-21T13:30:00Z", periods=60, freq="min")
    prices = [100.0 - index * 0.2 for index in range(len(timestamps))]
    pd.DataFrame(
        {
            "symbol": ["GLD"] * len(timestamps),
            "timestamp": timestamps,
            "open": prices,
            "high": [price + 0.05 for price in prices],
            "low": [price - 0.05 for price in prices],
            "close": prices,
            "volume": [1000] * len(timestamps),
        }
    ).to_parquet(stock_bars_path, index=False)

    contracts_path = contracts_root / "underlying=GLD" / "trade_date=2026-04-21" / "part.parquet"
    contracts_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-04-21", "2026-04-21"]),
            "reference_timestamp": pd.to_datetime(
                ["2026-04-21T13:34:00Z", "2026-04-21T13:34:00Z"], utc=True
            ),
            "reference_price": [100.0, 100.0],
            "underlying_symbol": ["GLD", "GLD"],
            "symbol": ["GLD260424P00100000", "GLD260424P00101000"],
            "expiration_date": pd.to_datetime(["2026-04-24", "2026-04-24"]),
            "option_type": ["put", "put"],
            "strike_price": [100.0, 101.0],
            "dte": [3, 3],
            "relative_strike_step": [0, 1],
        }
    ).to_parquet(contracts_path, index=False)

    option_prices = [2.0 + index * 0.03 for index in range(len(timestamps))]
    option_bars_path = (
        option_bars_root / "underlying=GLD" / "trade_date=2026-04-21" / "batch=000" / "part.parquet"
    )
    option_bars_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": ["GLD260424P00101000"] * len(timestamps),
            "underlying_symbol": ["GLD"] * len(timestamps),
            "trade_date": pd.to_datetime(["2026-04-21"] * len(timestamps)),
            "timestamp": timestamps,
            "open": option_prices,
            "high": [price + 0.01 for price in option_prices],
            "low": [price - 0.01 for price in option_prices],
            "close": option_prices,
            "volume": [25] * len(timestamps),
        }
    ).to_parquet(option_bars_path, index=False)
    option_trades_path = (
        option_trades_root
        / "underlying=GLD"
        / "trade_date=2026-04-21"
        / "batch=000"
        / "part.parquet"
    )
    option_trades_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": ["GLD260424P00101000"] * len(timestamps),
            "underlying_symbol": ["GLD"] * len(timestamps),
            "trade_date": pd.to_datetime(["2026-04-21"] * len(timestamps)),
            "timestamp": timestamps,
            "price": option_prices,
            "size": [1] * len(timestamps),
        }
    ).to_parquet(option_trades_path, index=False)

    payload = build_option_aware_backtest(
        queue_json=queue_json,
        variants_jsonl=variants_jsonl,
        stock_bars_path=stock_bars_path,
        selected_contracts_root=None,
        option_bars_root=None,
        option_trades_root=None,
        top_n=1,
        initial_cash=100_000.0,
        allocation_fraction=0.10,
        slippage_bps=1.0,
        fee_per_contract=0.0,
        max_entry_lag=timedelta(minutes=1),
        max_exit_lag=timedelta(minutes=1),
        contract_selection_method=CONTRACT_SELECTION_LIQUIDITY_FIRST,
    )

    assert payload["broker_facing"] is False
    assert payload["promotion_allowed"] is False
    assert payload["contract_selection_method"] == CONTRACT_SELECTION_LIQUIDITY_FIRST
    assert payload["contract_selection_lookahead"] == "entry_window_only"
    assert payload["option_lookup_mode"] == "indexed_by_contract_and_symbol"
    assert payload["option_trade_count"] > 0
    assert {row["contract_symbol"] for row in payload["trade_rows"]} == {"GLD260424P00101000"}
    summary = payload["candidate_summaries"][0]
    assert summary["missing_no_entry_bar"] == 0
    assert summary["missing_no_selected_contract"] == 0
    assert summary["fill_failure_reason"] == "fill_gate_clear"
    assert summary["contract_selection_method"] == CONTRACT_SELECTION_LIQUIDITY_FIRST


def test_option_aware_parquet_path_pruning_skips_other_symbol_partitions() -> None:
    assert _path_matches_symbol_filter(
        Path("underlying=QQQ") / "trade_date=2026-04-21" / "part.parquet",
        {"QQQ"},
    )
    assert not _path_matches_symbol_filter(
        Path("underlying=AAPL") / "trade_date=2026-04-21" / "part.parquet",
        {"QQQ"},
    )
    assert _path_matches_symbol_filter(
        Path("QQQ") / "365d_5x5" / "option_bars_silver" / "part.parquet",
        {"QQQ"},
    )
    assert not _path_matches_symbol_filter(
        Path("AAPL") / "365d_5x5" / "option_bars_silver" / "part.parquet",
        {"QQQ"},
    )

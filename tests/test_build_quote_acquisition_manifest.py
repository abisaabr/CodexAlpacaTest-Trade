from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_quote_acquisition_manifest import build_quote_acquisition_manifest


def test_build_quote_acquisition_manifest_marks_missing_quote_windows(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay" / "profile"
    replay_root.mkdir(parents=True)
    trade_csv = replay_root / "option_aware_trade_economics.csv"
    leg_details = [
        {
            "contract_symbol": "QQQ260515C00450000",
            "role": "long_call",
            "side": 1,
            "ratio": 1,
            "entry_time": "2026-05-13T14:30:15+00:00",
            "entry_quote_source": "option_bar_close_no_bid_ask",
            "exit_time": "2026-05-13T14:45:00+00:00",
            "exit_quote_source": "option_bar_close_no_bid_ask",
            "relative_strike_step": 0,
        },
        {
            "contract_symbol": "QQQ260515C00451000",
            "role": "short_call_wing",
            "side": -1,
            "ratio": 1,
            "entry_time": "2026-05-13T14:30:15+00:00",
            "entry_quote_source": "option_bar_close_no_bid_ask",
            "exit_time": "2026-05-13T14:45:00+00:00",
            "exit_quote_source": "option_bar_close_no_bid_ask",
            "relative_strike_step": 1,
        },
    ]
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "portfolio12h__qqq__choppy__call__debit_call_vertical__abc",
                "strategy_id": "qqq__choppy__call__debit_call_vertical",
                "source_strategy_id": "qqq__choppy__call__debit_call_vertical",
                "symbol": "QQQ",
                "family": "debit_call_vertical",
                "intended_regime": "choppy",
                "option_structure": "debit_call_vertical",
                "contract_symbol": "QQQ260515C00450000;QQQ260515C00451000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
                "leg_details_json": json.dumps(leg_details),
            }
        ]
    ).to_csv(trade_csv, index=False)

    quote_sidecar = tmp_path / "option_quote_sidecar.csv"
    pd.DataFrame(
        [
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
                "bid": 1.00,
                "ask": 1.02,
            },
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:44:55+00:00",
                "bid": 1.04,
                "ask": 1.06,
            },
        ]
    ).to_csv(quote_sidecar, index=False)

    summary = build_quote_acquisition_manifest(
        trade_economics_roots=[tmp_path / "replay"],
        trade_economics_csvs=[],
        quote_sidecar_csv=quote_sidecar,
        output_dir=tmp_path / "out",
        underlyings={"QQQ"},
        window_before_seconds=300,
        window_after_seconds=60,
        max_quote_age_seconds=60,
        only_missing=False,
    )

    assert summary["manifest_event_count"] == 4
    assert summary["contract_date_count"] == 2
    assert summary["coverage_status_counts"] == {
        "contract_not_in_sidecar_on_date": 2,
        "covered_fresh": 2,
    }

    rows = pd.read_csv(tmp_path / "out" / "quote_acquisition_manifest.csv")
    assert set(rows["event_side"]) == {"entry", "exit"}
    assert set(rows["contract_symbol"]) == {
        "QQQ260515C00450000",
        "QQQ260515C00451000",
    }
    missing = rows[rows["sidecar_coverage_status"] == "contract_not_in_sidecar_on_date"]
    assert set(missing["contract_symbol"]) == {"QQQ260515C00451000"}
    assert set(missing["requested_window_start_utc"]) == {
        "2026-05-13T14:25:15+00:00",
        "2026-05-13T14:40:00+00:00",
    }


def test_build_quote_acquisition_manifest_only_missing_filters_fresh_sidecar_events(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay" / "profile"
    replay_root.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "covered",
                "strategy_id": "qqq__bull__call__single_leg_repair",
                "symbol": "QQQ",
                "family": "single_leg_repair",
                "intended_regime": "bull",
                "contract_symbol": "QQQ260515C00450000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
            },
            {
                "candidate_variant_id": "missing",
                "strategy_id": "qqq__bull__call__single_leg_repair",
                "symbol": "QQQ",
                "family": "single_leg_repair",
                "intended_regime": "bull",
                "contract_symbol": "QQQ260516C00450000",
                "stock_entry_time": "2026-05-14T14:30:15+00:00",
                "stock_exit_time": "2026-05-14T14:45:00+00:00",
            },
        ]
    ).to_csv(replay_root / "option_aware_trade_economics.csv", index=False)
    quote_sidecar = tmp_path / "option_quote_sidecar.csv"
    pd.DataFrame(
        [
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
            },
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:44:50+00:00",
            },
        ]
    ).to_csv(quote_sidecar, index=False)

    summary = build_quote_acquisition_manifest(
        trade_economics_roots=[tmp_path / "replay"],
        trade_economics_csvs=[],
        quote_sidecar_csv=quote_sidecar,
        output_dir=tmp_path / "out",
        underlyings={"QQQ"},
        window_before_seconds=300,
        window_after_seconds=60,
        max_quote_age_seconds=60,
        only_missing=True,
    )

    rows = pd.read_csv(tmp_path / "out" / "quote_acquisition_manifest.csv")
    assert summary["manifest_event_count"] == 2
    assert set(rows["candidate_variant_id"]) == {"missing"}
    assert set(rows["sidecar_coverage_status"]) == {"trade_date_not_in_sidecar"}

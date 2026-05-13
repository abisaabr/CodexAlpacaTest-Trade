from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.diagnose_option_trade_print_coverage import diagnose_option_trade_print_coverage


def test_diagnoses_trade_print_coverage_near_decision(tmp_path: Path) -> None:
    manifest = tmp_path / "quote_acquisition_manifest.csv"
    pd.DataFrame(
        {
            "underlying": ["QQQ", "QQQ"],
            "trade_date": ["2026-04-21", "2026-04-21"],
            "contract_symbol": ["QQQ260424C00420000", "QQQ260424C00425000"],
            "event_side": ["entry", "exit"],
            "decision_time_utc": ["2026-04-21T14:00:00Z", "2026-04-21T14:00:00Z"],
            "requested_window_start_utc": ["2026-04-21T13:55:00Z", "2026-04-21T13:55:00Z"],
            "requested_window_end_utc": ["2026-04-21T14:01:00Z", "2026-04-21T14:01:00Z"],
            "family": ["debit_call_vertical", "debit_call_vertical"],
            "intended_regime": ["choppy", "choppy"],
            "strategy_id": ["s1", "s2"],
            "candidate_variant_id": ["c1", "c2"],
        }
    ).to_csv(manifest, index=False)
    prints = tmp_path / "option_trade_prints.csv"
    pd.DataFrame(
        {
            "option_symbol": ["QQQ260424C00420000", "QQQ260424C00420000"],
            "underlying_symbol": ["QQQ", "QQQ"],
            "event_time_utc": ["2026-04-21T13:59:45Z", "2026-04-21T14:00:05Z"],
            "observed_at_utc": ["2026-04-21T14:00:05Z", "2026-04-21T14:00:05Z"],
            "trade_age_seconds": ["", ""],
            "price": [1.1, 1.12],
            "size": [1, 1],
        }
    ).to_csv(prints, index=False)

    summary = diagnose_option_trade_print_coverage(
        quote_acquisition_manifest_csv=manifest,
        option_trades_csv=prints,
        output_dir=tmp_path / "out",
        underlyings={"QQQ"},
        forward_window_seconds=60,
        prior_window_seconds=60,
        write_event_detail=True,
    )

    assert summary["broker_facing"] is False
    assert summary["manifest_event_count"] == 2
    assert summary["events_with_requested_window_trade"] == 1
    assert summary["events_with_forward_trade"] == 1
    assert summary["events_with_recent_prior_trade"] == 1
    assert summary["coverage_status_counts"]["forward_and_prior_prints"] == 1
    assert summary["coverage_status_counts"]["no_trade_print_near_decision"] == 1
    assert Path(summary["event_detail_csv"]).exists()

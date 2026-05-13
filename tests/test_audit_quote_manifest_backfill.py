from pathlib import Path

import pandas as pd

from scripts.audit_quote_manifest_backfill import audit_quote_manifest_backfill


def test_audit_quote_manifest_backfill_requires_fresh_valid_bid_ask(tmp_path: Path) -> None:
    manifest = tmp_path / "quote_manifest.csv"
    pd.DataFrame(
        [
            {
                "underlying": "QQQ",
                "trade_date": "2026-05-13",
                "contract_symbol": "QQQ260515C00450000",
                "event_side": "entry",
                "decision_time_utc": "2026-05-13T14:30:15Z",
                "requested_window_start_utc": "2026-05-13T14:25:15Z",
                "requested_window_end_utc": "2026-05-13T14:31:15Z",
                "candidate_variant_id": "covered",
                "strategy_id": "qqq__bull__call__single_leg_repair",
                "family": "single_leg_repair",
                "intended_regime": "bull",
                "leg_index": 0,
            },
            {
                "underlying": "QQQ",
                "trade_date": "2026-05-13",
                "contract_symbol": "QQQ260515P00440000",
                "event_side": "entry",
                "decision_time_utc": "2026-05-13T14:30:15Z",
                "requested_window_start_utc": "2026-05-13T14:25:15Z",
                "requested_window_end_utc": "2026-05-13T14:31:15Z",
                "candidate_variant_id": "missing",
                "strategy_id": "qqq__bear__put__single_leg_repair",
                "family": "single_leg_repair",
                "intended_regime": "bear",
                "leg_index": 0,
            },
            {
                "underlying": "QQQ",
                "trade_date": "2026-05-13",
                "contract_symbol": "QQQ260515C00451000",
                "event_side": "entry",
                "decision_time_utc": "2026-05-13T14:30:15Z",
                "requested_window_start_utc": "2026-05-13T14:25:15Z",
                "requested_window_end_utc": "2026-05-13T14:31:15Z",
                "candidate_variant_id": "wide",
                "strategy_id": "qqq__choppy__call__debit_call_vertical",
                "family": "debit_call_vertical",
                "intended_regime": "choppy",
                "leg_index": 1,
            },
        ]
    ).to_csv(manifest, index=False)

    sidecar = tmp_path / "option_quote_sidecar.csv"
    pd.DataFrame(
        [
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:00Z",
                "bid": 1.00,
                "ask": 1.02,
            },
            {
                "option_symbol": "QQQ260515C00451000",
                "event_time_utc": "2026-05-13T14:30:00Z",
                "bid": 0.50,
                "ask": 0.90,
            },
        ]
    ).to_csv(sidecar, index=False)

    summary = audit_quote_manifest_backfill(
        quote_manifest_csv=manifest,
        quote_sidecar_csv=sidecar,
        output_dir=tmp_path / "out",
        max_quote_age_seconds=60,
        max_relative_spread=0.25,
    )

    assert summary["manifest_event_count"] == 3
    assert summary["quote_sidecar_contract_count"] == 2
    assert summary["strict_all_events_quote_backed"] is False
    assert summary["audit_status_counts"] == {
        "contract_not_in_sidecar": 1,
        "quote_backed_event": 1,
        "wide_spread": 1,
    }
    assert summary["coverage_diagnostics"]["contract_overlap_count"] == 2
    assert summary["coverage_diagnostics"]["contract_date_overlap_count"] == 2
    assert summary["coverage_diagnostics"]["likely_gap_category"] == "event_time_or_quote_quality_gap"

    rows = pd.read_csv(tmp_path / "out" / "quote_manifest_backfill_audit_rows.csv")
    covered = rows[rows["audit_status"] == "quote_backed_event"].iloc[0]
    assert covered["matched_quote_time_utc"] == "2026-05-13T14:30:00+00:00"
    assert covered["matched_quote_age_seconds"] == 15.0

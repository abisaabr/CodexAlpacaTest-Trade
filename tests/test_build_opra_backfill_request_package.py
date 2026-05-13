from pathlib import Path

import pandas as pd

from scripts.build_opra_backfill_request_package import build_opra_backfill_request_package


def test_build_opra_backfill_request_package_chunks_contract_date_windows(tmp_path: Path) -> None:
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
                "candidate_variant_id": "a",
                "strategy_id": "s1",
                "family": "single_leg_repair",
                "intended_regime": "bull",
            },
            {
                "underlying": "QQQ",
                "trade_date": "2026-05-13",
                "contract_symbol": "QQQ260515C00450000",
                "event_side": "exit",
                "decision_time_utc": "2026-05-13T14:45:00Z",
                "requested_window_start_utc": "2026-05-13T14:40:00Z",
                "requested_window_end_utc": "2026-05-13T14:46:00Z",
                "candidate_variant_id": "a",
                "strategy_id": "s1",
                "family": "single_leg_repair",
                "intended_regime": "bull",
            },
            {
                "underlying": "QQQ",
                "trade_date": "2026-05-14",
                "contract_symbol": "QQQ260515P00440000",
                "event_side": "entry",
                "decision_time_utc": "2026-05-14T14:30:15Z",
                "requested_window_start_utc": "2026-05-14T14:25:15Z",
                "requested_window_end_utc": "2026-05-14T14:31:15Z",
                "candidate_variant_id": "b",
                "strategy_id": "s2",
                "family": "debit_put_vertical",
                "intended_regime": "bear",
            },
        ]
    ).to_csv(manifest, index=False)

    summary = build_opra_backfill_request_package(
        quote_manifest_csv=manifest,
        output_dir=tmp_path / "out",
        max_windows_per_chunk=1,
        window_padding_seconds=5,
    )

    assert summary["request_window_count"] == 2
    assert summary["chunk_count"] == 2
    windows = pd.read_csv(tmp_path / "out" / "opra_backfill_request_windows.csv")
    first = windows[windows["contract_symbol"] == "QQQ260515C00450000"].iloc[0]
    assert first["event_count"] == 2
    assert first["request_window_start_utc"] == "2026-05-13T14:25:10+00:00"
    assert first["request_window_end_utc"] == "2026-05-13T14:46:05+00:00"
    assert first["required_fields"] == "option_symbol,event_time_utc,bid,ask,bid_size,ask_size"
    assert (tmp_path / "out" / "chunks" / "opra_backfill_request_chunk_0001.csv").exists()

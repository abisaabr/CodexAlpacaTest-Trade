from pathlib import Path

import pandas as pd

from scripts.build_external_opra_quote_sidecar import build_external_opra_quote_sidecar


def test_build_external_opra_quote_sidecar_filters_manifest_windows(tmp_path: Path) -> None:
    vendor_csv = tmp_path / "vendor_quotes.csv"
    pd.DataFrame(
        [
            {
                "symbol": "QQQ250430C00470000",
                "timestamp": "2025-04-29T15:34:10Z",
                "bid_price": 1.0,
                "ask_price": 1.1,
                "bid_size": 10,
                "ask_size": 12,
            },
            {
                "symbol": "QQQ250430C00470000",
                "timestamp": "2025-04-29T16:34:10Z",
                "bid_price": 1.0,
                "ask_price": 1.1,
            },
            {
                "symbol": "SPY250430C00470000",
                "timestamp": "2025-04-29T15:34:10Z",
                "bid_price": 1.0,
                "ask_price": 1.1,
            },
            {
                "symbol": "QQQ250430C00470000",
                "timestamp": "2025-04-29T15:34:11Z",
                "bid_price": 1.2,
                "ask_price": 1.1,
            },
        ]
    ).to_csv(vendor_csv, index=False)

    manifest_csv = tmp_path / "quote_acquisition_manifest.csv"
    pd.DataFrame(
        [
            {
                "contract_symbol": "QQQ250430C00470000",
                "requested_window_start_utc": "2025-04-29T15:29:00Z",
                "requested_window_end_utc": "2025-04-29T15:35:00Z",
            }
        ]
    ).to_csv(manifest_csv, index=False)

    summary = build_external_opra_quote_sidecar(
        input_path=vendor_csv,
        output_dir=tmp_path / "out",
        acquisition_manifest_csv=manifest_csv,
    )

    assert summary["status"] == "external_opra_quote_sidecar_complete"
    assert summary["broker_facing"] is False
    assert summary["sidecar_quote_rows"] == 1
    assert summary["rejected_rows"] == 3
    assert summary["manifest_symbols_with_quotes"] == 1

    sidecar = pd.read_csv(tmp_path / "out" / "option_quote_sidecar.csv")
    row = sidecar.iloc[0]
    assert row["option_symbol"] == "QQQ250430C00470000"
    assert row["underlying_symbol"] == "QQQ"
    assert row["bid"] == 1.0
    assert row["ask"] == 1.1
    assert round(row["relative_spread"], 8) == round(0.1 / 1.05, 8)
    assert row["quote_source"] == "option_quote_bid_ask_external_opra"

    rejected = pd.read_csv(tmp_path / "out" / "external_opra_rejected_rows.csv")
    assert set(rejected["rejection_reason"]) == {
        "outside_requested_window",
        "symbol_not_requested",
        "invalid_bid_ask",
    }


def test_build_external_opra_quote_sidecar_accepts_alias_columns_without_manifest(tmp_path: Path) -> None:
    vendor_dir = tmp_path / "vendor"
    vendor_dir.mkdir()
    pd.DataFrame(
        [
            {
                "contract_symbol": "IWM250430P00195000",
                "sip_timestamp": "2025-04-29T15:34:10Z",
                "best_bid": 2.0,
                "best_ask": 2.2,
            }
        ]
    ).to_csv(vendor_dir / "part1.csv", index=False)

    summary = build_external_opra_quote_sidecar(
        input_path=vendor_dir,
        output_dir=tmp_path / "out",
    )

    assert summary["input_file_count"] == 1
    assert summary["sidecar_quote_rows"] == 1
    sidecar = pd.read_csv(tmp_path / "out" / "option_quote_sidecar.csv")
    assert sidecar["option_symbol"].tolist() == ["IWM250430P00195000"]
    assert sidecar["underlying_symbol"].tolist() == ["IWM"]

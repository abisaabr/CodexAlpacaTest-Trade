from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.run_quote_backed_evidence_repair_sweep import _apply_quote_sidecar_tree


def test_apply_quote_sidecar_tree_writes_side_aware_replay_fields(tmp_path: Path) -> None:
    source = tmp_path / "source" / "profile_a"
    source.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "qqq_bull",
                "symbol": "QQQ",
                "contract_symbol": "QQQ260515C00450000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
                "risk_per_unit": 100.0,
                "quantity": 1,
                "fees": 0.0,
                "option_pnl": 0.0,
            }
        ]
    ).to_csv(source / "option_aware_trade_economics.csv", index=False)

    quote_sidecar = tmp_path / "option_quote_sidecar.csv"
    pd.DataFrame(
        [
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
                "bid": 1.00,
                "ask": 1.05,
            },
            {
                "option_symbol": "QQQ260515C00450000",
                "event_time_utc": "2026-05-13T14:44:59+00:00",
                "bid": 1.20,
                "ask": 1.25,
            },
        ]
    ).to_csv(quote_sidecar, index=False)

    summary = _apply_quote_sidecar_tree(
        source_root=tmp_path / "source",
        output_root=tmp_path / "out",
        quote_sidecar_csv=quote_sidecar,
        option_trades_csv=None,
        entry_selection_window_seconds=60.0,
    )

    assert summary["quote_backed_replay_status_counts"] == {"quote_backed_replay": 1}
    output = pd.read_csv(tmp_path / "out" / "profile_a" / "option_aware_trade_economics.csv")
    row = output.iloc[0]
    assert row["quote_backed_replay_status"] == "quote_backed_replay"
    assert row["quote_backed_option_pnl"] == 15.0
    assert row["quote_backed_max_quote_age_seconds"] == 15.0

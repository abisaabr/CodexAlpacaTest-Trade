from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from scripts.build_option_data_repair_plan import build_option_data_repair_plan


def _write_selected(root: Path, symbols: list[str]) -> None:
    path = root / "underlying=AMZN" / "trade_date=2026-04-17" / "part.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-04-17"] * len(symbols)),
            "reference_timestamp": pd.to_datetime(
                ["2026-04-17T14:00:00Z"] * len(symbols), utc=True
            ),
            "reference_price": [180.0] * len(symbols),
            "underlying_symbol": ["AMZN"] * len(symbols),
            "symbol": symbols,
            "expiration_date": pd.to_datetime(["2026-04-24"] * len(symbols)),
            "option_type": ["call"] * len(symbols),
            "strike_price": [180.0 + index for index, _ in enumerate(symbols)],
            "dte": [7] * len(symbols),
            "atm_strike": [180.0] * len(symbols),
            "relative_strike_step": list(range(len(symbols))),
            "selection_reason": ["reference=strategy_entry_time"] * len(symbols),
            "inventory_status": ["active"] * len(symbols),
        }
    ).to_parquet(path, index=False)


def test_repair_plan_builds_missing_trade_subset(tmp_path: Path) -> None:
    selected_root = tmp_path / "selected_option_contracts"
    symbols = ["AMZN1", "AMZN2", "AMZN3", "AMZN4", "AMZN5"]
    _write_selected(selected_root, symbols)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        """
{
  "datasets": {
    "option_trades": {
      "chunks": {
        "AMZN__2026-04-17__batch000": {
          "status": "completed",
          "metadata": {
            "underlying": "AMZN",
            "trade_date": "2026-04-17",
            "symbols": ["AMZN1", "AMZN2"]
          }
        }
      }
    }
  }
}
""",
        encoding="utf-8",
    )

    packet = build_option_data_repair_plan(
        manifest_json=manifest_path,
        selected_contracts_root=selected_root,
        output_dir=tmp_path / "repair",
        plan_id="repair-test",
        datasets=("option_trades",),
        option_batch_size=2,
        start_date=date(2026, 4, 17),
        end_date=date(2026, 4, 17),
    )

    assert packet["status"] == "ready_repair_download"
    assert packet["selected_subset_rows"] == 3
    assert packet["repair_chunk_count"] == 2
    assert packet["dataset_summaries"]["option_trades"] == {
        "expected_symbol_days": 5,
        "completed_symbol_days": 2,
        "remaining_symbol_days": 3,
        "repair_chunks": 2,
    }
    subset_path = (
        tmp_path
        / "repair"
        / "selected_option_contracts"
        / "underlying=AMZN"
        / "trade_date=2026-04-17"
        / "part.parquet"
    )
    assert set(pd.read_parquet(subset_path)["symbol"]) == {"AMZN3", "AMZN4", "AMZN5"}
    assert "--no-include-option-bars" in packet["recommended_download_command"]


def test_repair_plan_reports_no_remaining_work(tmp_path: Path) -> None:
    selected_root = tmp_path / "selected_option_contracts"
    symbols = ["AMZN1", "AMZN2"]
    _write_selected(selected_root, symbols)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        """
{
  "datasets": {
    "option_trades": {
      "chunks": {
        "AMZN__2026-04-17__batch000": {
          "status": "completed",
          "metadata": {
            "underlying": "AMZN",
            "trade_date": "2026-04-17",
            "symbols": ["AMZN1", "AMZN2"]
          }
        }
      }
    }
  }
}
""",
        encoding="utf-8",
    )

    packet = build_option_data_repair_plan(
        manifest_json=manifest_path,
        selected_contracts_root=selected_root,
        output_dir=tmp_path / "repair",
        plan_id="repair-test",
        datasets=("option_trades",),
        option_batch_size=20,
    )

    assert packet["status"] == "no_repair_needed"
    assert packet["selected_subset_rows"] == 0
    assert packet["repair_chunk_count"] == 0

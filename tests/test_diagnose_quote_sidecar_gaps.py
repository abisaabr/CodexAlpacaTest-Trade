from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.diagnose_quote_sidecar_gaps import diagnose_quote_sidecar_gaps


def test_diagnose_quote_sidecar_gaps_classifies_qqq_failures(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay" / "profile"
    replay_root.mkdir(parents=True)
    trades = replay_root / "option_aware_trade_economics.csv"
    pd.DataFrame(
        [
            {
                "candidate_variant_id": "matched",
                "symbol": "QQQ",
                "contract_symbol": "QQQ260515C00450000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
            },
            {
                "candidate_variant_id": "missing_date",
                "symbol": "QQQ",
                "contract_symbol": "QQQ260516C00450000",
                "stock_entry_time": "2026-05-14T14:30:15+00:00",
                "stock_exit_time": "2026-05-14T14:45:00+00:00",
            },
            {
                "candidate_variant_id": "missing_contract",
                "symbol": "QQQ",
                "contract_symbol": "QQQ260515C00460000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
            },
            {
                "candidate_variant_id": "normalization",
                "symbol": "QQQ",
                "contract_symbol": "QQQ260515C00470000",
                "stock_entry_time": "2026-05-13T14:30:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
            },
            {
                "candidate_variant_id": "before_first_quote",
                "symbol": "QQQ",
                "contract_symbol": "QQQ260515C00480000",
                "stock_entry_time": "2026-05-13T14:29:15+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
            },
            {
                "candidate_variant_id": "stale",
                "symbol": "QQQ",
                "contract_symbol": "QQQ260515C00490000",
                "stock_entry_time": "2026-05-13T14:32:30+00:00",
                "stock_exit_time": "2026-05-13T14:45:00+00:00",
            },
        ]
    ).to_csv(trades, index=False)

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
                "event_time_utc": "2026-05-13T14:44:59+00:00",
                "bid": 1.05,
                "ask": 1.07,
            },
            {
                "option_symbol": "QQQ-260515-C-00470000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
                "bid": 1.00,
                "ask": 1.02,
            },
            {
                "option_symbol": "QQQ260515C00480000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
                "bid": 1.00,
                "ask": 1.02,
            },
            {
                "option_symbol": "QQQ260515C00490000",
                "event_time_utc": "2026-05-13T14:30:00+00:00",
                "bid": 1.00,
                "ask": 1.02,
            },
        ]
    ).to_csv(quote_sidecar, index=False)

    summary = diagnose_quote_sidecar_gaps(
        trade_economics_roots=[tmp_path / "replay"],
        trade_economics_csvs=[],
        quote_sidecar_csv=quote_sidecar,
        output_dir=tmp_path / "out",
        underlyings={"QQQ"},
        max_quote_age_seconds=60,
    )

    assert summary["diagnosed_trade_rows"] == 6
    assert summary["strict_quote_backed_trade_rows"] == 1
    assert summary["strict_quote_backed_trade_row_rate"] == round(1 / 6, 6)
    assert summary["entry_gap_reason_counts"]["matched_quote"] == 1
    assert summary["entry_gap_reason_counts"]["trade_date_not_in_sidecar"] == 1
    assert summary["entry_gap_reason_counts"]["contract_not_in_sidecar"] == 1
    assert summary["entry_gap_reason_counts"]["symbol_normalization_mismatch"] == 1
    assert summary["entry_gap_reason_counts"]["no_quote_before_decision"] == 1
    assert summary["entry_gap_reason_counts"]["stale_quote"] == 1

    rows = pd.read_csv(tmp_path / "out" / "quote_gap_rows.csv")
    by_id = {row["candidate_variant_id"]: row["entry_gap_reason"] for _, row in rows.iterrows()}
    assert by_id["matched"] == "matched_quote"
    assert by_id["missing_date"] == "trade_date_not_in_sidecar"
    assert by_id["missing_contract"] == "contract_not_in_sidecar"
    assert by_id["normalization"] == "symbol_normalization_mismatch"
    assert by_id["before_first_quote"] == "no_quote_before_decision"
    assert by_id["stale"] == "stale_quote"

    universe = pd.read_csv(tmp_path / "out" / "replay_contract_universe.csv")
    assert set(universe["underlying"]) == {"QQQ"}
    symbols_txt = (tmp_path / "out" / "replay_contract_symbols.txt").read_text(encoding="utf-8")
    assert "QQQ260515C00450000" in symbols_txt
    assert "QQQ260515C00490000" in symbols_txt
    sidecar = pd.read_csv(tmp_path / "out" / "sidecar_symbol_coverage.csv")
    assert set(sidecar["underlying"]) == {"QQQ"}

    action_plan = pd.read_csv(tmp_path / "out" / "quote_gap_root_cause_action_plan.csv")
    contract_gap = action_plan[
        (action_plan["side"] == "entry")
        & (action_plan["gap_reason"] == "contract_not_in_sidecar")
    ].iloc[0]
    assert contract_gap["root_cause_class"] == "quote_capture_universe"
    assert contract_gap["recommended_action"] == "expand_runtime_leg_or_replay_contract_quote_capture_universe"
    assert bool(contract_gap["blocks_quote_backed_replay"])
    matched = action_plan[
        (action_plan["side"] == "entry")
        & (action_plan["gap_reason"] == "matched_quote")
    ].iloc[0]
    assert matched["recommended_action"] == "none"

from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.build_greek_selector_diagnostic import build_diagnostic


def _write_summary(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "candidate_variant_id",
        "source_strategy_id",
        "symbol",
        "intended_regime",
        "family",
        "directional_option_type",
        "parameter_set",
        "fill_coverage",
        "option_trade_count",
        "source_stock_trade_count",
        "net_pnl",
        "test_net_pnl",
        "entry_bar_coverage",
        "exit_bar_coverage",
        "data_foundation_coverage",
        "missing_no_selected_contract",
        "missing_no_greek_snapshot",
        "missing_no_entry_bar",
        "missing_no_exit_bar",
        "missing_option_price_count",
        "recommendation",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _row(
    candidate_id: str,
    *,
    params: dict[str, object],
    fill_coverage: float = 0.0,
    trades: int = 0,
    source_trades: int = 100,
    net_pnl: float = 0.0,
    test_net_pnl: float = 0.0,
    selected_missing: int = 0,
    entry_missing: int = 0,
    exit_missing: int = 0,
    family: str = "single_leg_repair",
) -> dict[str, object]:
    return {
        "candidate_variant_id": candidate_id,
        "source_strategy_id": candidate_id,
        "symbol": "QQQ",
        "intended_regime": "bull",
        "family": family,
        "directional_option_type": "call",
        "parameter_set": json.dumps(params),
        "fill_coverage": fill_coverage,
        "option_trade_count": trades,
        "source_stock_trade_count": source_trades,
        "net_pnl": net_pnl,
        "test_net_pnl": test_net_pnl,
        "entry_bar_coverage": 1.0 if not entry_missing else 0.2,
        "exit_bar_coverage": 1.0 if not exit_missing else 0.2,
        "data_foundation_coverage": fill_coverage,
        "missing_no_selected_contract": selected_missing,
        "missing_no_greek_snapshot": 0,
        "missing_no_entry_bar": entry_missing,
        "missing_no_exit_bar": exit_missing,
        "missing_option_price_count": 0,
        "recommendation": "research",
    }


def test_greek_selector_diagnostic_classifies_selector_and_exit_failures(tmp_path: Path) -> None:
    summary_path = tmp_path / "wave" / "worker" / "option_aware_candidate_summary.csv"
    _write_summary(
        summary_path,
        [
            _row(
                "same_day_gap",
                params={"dte_mode": "same_day", "target_delta": 0.25, "min_abs_delta": 0.18, "max_abs_delta": 0.34},
                selected_missing=100,
            ),
            _row(
                "next_expiry_strict_delta",
                params={"dte_mode": "next_expiry", "target_delta": 0.80, "min_abs_delta": 0.70, "max_abs_delta": 0.90},
                selected_missing=100,
            ),
            _row(
                "too_few_trades",
                params={"dte_mode": "next_expiry", "target_delta": 0.50, "min_abs_delta": 0.40, "max_abs_delta": 0.60},
                fill_coverage=1.0,
                trades=4,
                net_pnl=50,
                test_net_pnl=5,
            ),
            _row(
                "bad_exit",
                params={"dte_mode": "next_expiry", "target_delta": 0.50, "min_abs_delta": 0.40, "max_abs_delta": 0.60},
                fill_coverage=1.0,
                trades=50,
                net_pnl=-10,
                test_net_pnl=-3,
            ),
        ],
    )

    summary = build_diagnostic(
        input_roots=[tmp_path / "wave"],
        output_dir=tmp_path / "out",
        fill_coverage_gate=0.90,
        min_trades=20,
    )

    assert summary["candidate_summary_file_count"] == 1
    assert summary["candidate_count"] == 4
    assert summary["dominant_cause_counts"]["bad_dte_or_strike_availability"] == 1
    assert summary["dominant_cause_counts"]["too_strict_delta_targeting"] == 1
    assert summary["dominant_cause_counts"]["too_few_trades"] == 1
    assert summary["dominant_cause_counts"]["bad_exits"] == 1
    assert (tmp_path / "out" / "greek_selector_diagnostic_rows.csv").exists()
    assert (tmp_path / "out" / "greek_selector_diagnostic_groups.csv").exists()
    assert (tmp_path / "out" / "greek_selector_diagnostic.md").exists()

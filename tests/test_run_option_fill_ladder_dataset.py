from __future__ import annotations

from datetime import date

from scripts.run_option_fill_ladder_dataset import STAGES, dataset_id, date_window


def test_fill_ladder_stage_windows_are_calendar_day_inclusive() -> None:
    end = date(2026, 4, 28)

    assert date_window("7d_atm", end) == (date(2026, 4, 22), end)
    assert date_window("30d_atm", end) == (date(2026, 3, 30), end)
    assert date_window("30d_5x5", end) == (date(2026, 3, 30), end)
    assert date_window("365d_5x5", end) == (date(2025, 4, 29), end)


def test_fill_ladder_stage_strike_widths_match_campaign_contract() -> None:
    assert STAGES["7d_atm"].strike_steps == 0
    assert STAGES["30d_atm"].strike_steps == 0
    assert STAGES["30d_5x5"].strike_steps == 5
    assert STAGES["365d_5x5"].strike_steps == 5


def test_dataset_id_normalizes_symbol() -> None:
    assert dataset_id("option_fill_ladder_20260429", "SPY", "7d_atm") == (
        "option_fill_ladder_20260429_spy_7d_atm"
    )

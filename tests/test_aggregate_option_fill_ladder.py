from scripts.aggregate_option_fill_ladder import packet_row, summarize


def packet(symbol: str, stage: str, coverage: float, selected: int = 10) -> dict:
    covered = int(round(selected * coverage))
    return {
        "campaign_id": "option_fill_ladder_20260429",
        "symbol": symbol,
        "stage": stage,
        "status": "complete",
        "date_window": {"start": "2026-04-22", "end": "2026-04-28"},
        "strike_steps_each_side": 0,
        "coverage": {
            "selected_contract_day_count": selected,
            "contract_days_with_any_bar": covered,
            "contract_day_coverage": coverage,
            "missing_contract_day_count": selected - covered,
            "option_bar_row_count": 1234,
        },
    }


def test_packet_row_enforces_fill_gate() -> None:
    passing = packet_row(packet("SPY", "7d_atm", 1.0), 0.90)
    blocked = packet_row(packet("AMZN", "7d_atm", 0.80), 0.90)
    assert passing["passes_fill_gate"] is True
    assert blocked["passes_fill_gate"] is False


def test_empty_selection_does_not_pass_gate() -> None:
    row = packet_row(packet("SPY", "7d_atm", 1.0, selected=0), 0.90)
    assert row["passes_fill_gate"] is False


def test_summary_tracks_stage_and_symbol_blockers() -> None:
    rows = [
        packet_row(packet("SPY", "7d_atm", 1.0), 0.90),
        packet_row(packet("AMZN", "7d_atm", 0.80), 0.90),
    ]
    summary = summarize(rows, 0.90)
    assert summary["stage_summary"]["7d_atm"]["pass_count"] == 1
    assert summary["stage_summary"]["7d_atm"]["failing_symbols"] == ["AMZN"]
    assert summary["symbol_summary"]["SPY"]["ready_for_replay_stages"] == ["7d_atm"]
    assert summary["symbol_summary"]["AMZN"]["failed_stages"] == ["7d_atm"]

from __future__ import annotations

from collections import Counter

from scripts.build_runtime_quote_capture_gap_report import build_gap_report


def test_gap_report_recommends_restart_when_runtime_symbol_missing_from_plan() -> None:
    report = build_gap_report(
        runtime_rows=[
            {
                "option_symbol": "QQQ260514P00700000",
                "underlying_symbol": "QQQ",
                "strategy_name": "qqq_test",
                "regime": "bear",
                "family": "broken_wing_put_butterfly",
            },
            {
                "option_symbol": "AMD260515P00450000",
                "underlying_symbol": "AMD",
                "strategy_name": "amd_test",
                "regime": "bear",
                "family": "broken_wing_put_butterfly",
            },
        ],
        runtime_errors=[],
        plan_payload={"option_symbols": ["QQQ260514P00700000"]},
        session_payload={"completed_trades": []},
        observed_counts=Counter({"QQQ260514P00700000": 5}),
    )

    assert report["decision"] == "quote_capture_plan_current_gap"
    assert report["restart_quote_shadow_recommended"] is True
    assert report["runtime_symbols_covered_pct"] == 50.0
    assert report["runtime_symbols_missing_from_plan"] == ["AMD260515P00450000"]
    assert report["restart_relevant_symbols_missing_from_plan"] == ["AMD260515P00450000"]


def test_gap_report_treats_session_trade_symbols_as_material() -> None:
    report = build_gap_report(
        runtime_rows=[],
        runtime_errors=[],
        plan_payload={"option_symbols": ["QQQ260514P00700000"]},
        session_payload={
            "open_trades": [
                {
                    "strategy_name": "amd_open",
                    "underlying_symbol": "AMD",
                    "regime": "bear",
                    "legs": [{"symbol": "AMD260515P00450000"}],
                }
            ]
        },
        observed_counts=Counter({"QQQ260514P00700000": 5}),
    )

    assert report["decision"] == "quote_capture_plan_current_gap"
    assert report["session_symbols_covered_pct"] == 0.0
    assert report["open_session_symbols_covered_pct"] == 0.0
    assert report["session_symbols_missing_from_plan"] == ["AMD260515P00450000"]
    assert report["open_session_symbols_missing_from_plan"] == ["AMD260515P00450000"]
    assert report["restart_relevant_symbols_missing_from_plan"] == ["AMD260515P00450000"]


def test_gap_report_passes_when_plan_covers_runtime_and_session_symbols() -> None:
    report = build_gap_report(
        runtime_rows=[
            {
                "option_symbol": "QQQ260514P00700000",
                "underlying_symbol": "QQQ",
                "strategy_name": "qqq_test",
                "regime": "bear",
                "family": "single_leg_repair",
            }
        ],
        runtime_errors=[],
        plan_payload={"option_symbols": ["QQQ260514P00700000"]},
        session_payload={
            "completed_trades": [
                {
                    "strategy_name": "qqq_test",
                    "underlying_symbol": "QQQ",
                    "regime": "bear",
                    "legs": [{"symbol": "QQQ260514P00700000"}],
                }
            ]
        },
        observed_counts=Counter({"QQQ260514P00700000": 12}),
    )

    assert report["decision"] == "quote_capture_plan_covers_runtime_and_session_symbols"
    assert report["restart_quote_shadow_recommended"] is False
    assert report["runtime_symbols_covered_pct"] == 100.0
    assert report["session_symbols_covered_pct"] == 100.0


def test_gap_report_does_not_restart_for_completed_only_historical_gap() -> None:
    report = build_gap_report(
        runtime_rows=[
            {
                "option_symbol": "QQQ260514P00700000",
                "underlying_symbol": "QQQ",
                "strategy_name": "qqq_current",
                "regime": "bear",
                "family": "broken_wing_put_butterfly",
            }
        ],
        runtime_errors=[],
        plan_payload={"option_symbols": ["QQQ260514P00700000"]},
        session_payload={
            "completed_trades": [
                {
                    "strategy_name": "amd_completed",
                    "underlying_symbol": "AMD",
                    "regime": "bear",
                    "legs": [{"symbol": "AMD260515P00450000"}],
                }
            ]
        },
        observed_counts=Counter({"QQQ260514P00700000": 8}),
    )

    assert report["decision"] == "quote_capture_plan_currently_covered_with_historical_session_gaps"
    assert report["restart_quote_shadow_recommended"] is False
    assert report["restart_relevant_symbols_missing_from_plan"] == []
    assert report["runtime_symbols_missing_from_plan"] == []
    assert report["open_session_symbols_missing_from_plan"] == []
    assert report["completed_session_symbols_missing_from_plan"] == ["AMD260515P00450000"]
    assert "historical OPRA evidence gaps" in report["restart_guidance"]


def test_gap_report_defers_runtime_only_gap_to_dynamic_refresh() -> None:
    report = build_gap_report(
        runtime_rows=[
            {
                "option_symbol": "QQQ260514P00718000",
                "underlying_symbol": "QQQ",
                "strategy_name": "qqq_current",
                "regime": "bear",
                "family": "broken_wing_put_butterfly",
            }
        ],
        runtime_errors=[],
        plan_payload={
            "option_symbols": ["QQQ260514P00717000"],
            "notes": ["runtime refresh added 3 OPRA symbols at 2026-05-13T15:01:17Z"],
        },
        session_payload={"open_trades": [], "completed_trades": []},
        observed_counts=Counter({"QQQ260514P00717000": 8}),
    )

    assert report["decision"] == "quote_capture_runtime_gap_dynamic_refresh_active"
    assert report["dynamic_runtime_refresh_active"] is True
    assert report["restart_quote_shadow_recommended"] is False
    assert report["runtime_symbols_missing_from_plan"] == ["QQQ260514P00718000"]
    assert "dynamic runtime refresh is active" in report["restart_guidance"]

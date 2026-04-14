from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from scripts.run_multi_ticker_watchdog import _build_report

ET = ZoneInfo("America/New_York")


def test_watchdog_flags_missing_open_session(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.run_multi_ticker_watchdog._now_et",
        lambda: datetime(2026, 4, 15, 10, 5, tzinfo=ET),
    )

    report = _build_report(
        clock={
            "is_open": True,
            "timestamp": "2026-04-15T14:05:00+00:00",
            "next_open": "2026-04-16T13:30:00+00:00",
            "next_close": "2026-04-15T20:00:00+00:00",
        },
        session_payload=None,
        stale_seconds=900,
        morning_grace_minutes=20,
        midday_minute=12 * 60 + 30,
        close_grace_minutes=15,
    )

    assert report["status"] == "error"
    assert any(issue["code"] == "session_missing" for issue in report["issues"])


def test_watchdog_flags_stale_open_session_and_missing_morning_notice(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.run_multi_ticker_watchdog._now_et",
        lambda: datetime(2026, 4, 15, 10, 5, tzinfo=ET),
    )

    report = _build_report(
        clock={
            "is_open": True,
            "timestamp": "2026-04-15T14:05:00+00:00",
            "next_open": "2026-04-16T13:30:00+00:00",
            "next_close": "2026-04-15T20:00:00+00:00",
        },
        session_payload={
            "session_path": "reports/session_2026-04-15.json",
            "startup_check_status": "passed",
            "blocked_new_entries": False,
            "last_updated_at": "2026-04-15T09:40:00-04:00",
            "notified_morning": False,
            "notified_midday": False,
            "notified_end_of_day": False,
            "open_trades": [],
            "completed_trades": [],
        },
        stale_seconds=900,
        morning_grace_minutes=20,
        midday_minute=12 * 60 + 30,
        close_grace_minutes=15,
    )

    assert report["status"] == "error"
    codes = {issue["code"] for issue in report["issues"]}
    assert "session_stale" in codes
    assert "morning_notification_missing" in codes


def test_watchdog_flags_missing_end_of_day_notice_after_close(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.run_multi_ticker_watchdog._now_et",
        lambda: datetime(2026, 4, 15, 16, 30, tzinfo=ET),
    )

    report = _build_report(
        clock={
            "is_open": False,
            "timestamp": "2026-04-15T20:30:00+00:00",
            "next_open": "2026-04-16T13:30:00+00:00",
            "next_close": "2026-04-16T20:00:00+00:00",
        },
        session_payload={
            "session_path": "reports/session_2026-04-15.json",
            "startup_check_status": "passed",
            "blocked_new_entries": False,
            "last_updated_at": "2026-04-15T16:01:00-04:00",
            "notified_morning": True,
            "notified_midday": True,
            "notified_end_of_day": False,
            "open_trades": [],
            "completed_trades": [],
        },
        stale_seconds=900,
        morning_grace_minutes=20,
        midday_minute=12 * 60 + 30,
        close_grace_minutes=15,
    )

    assert report["status"] == "warning"
    assert any(issue["code"] == "end_of_day_notification_missing" for issue in report["issues"])

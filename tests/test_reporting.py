from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from alpaca_lab.reporting import append_journal_entry, write_alert_queue, write_summary_bundle


def test_reporting_writes_summary_journal_and_alerts(tmp_path: Path) -> None:
    summary_paths = write_summary_bundle(
        tmp_path / "reports",
        name="unit_test_run",
        summary={"approved_count": 1, "blocked_count": 0},
        table_map={"approved": pd.DataFrame([{"symbol": "SPY"}])},
    )
    alert_path = write_alert_queue(
        tmp_path / "reports" / "alerts.json", [{"level": "info", "message": "ok"}]
    )
    journal_path = append_journal_entry(tmp_path / "reports" / "journal.json", {"run_id": "abc"})

    assert Path(summary_paths["json"]).exists()
    assert Path(summary_paths["markdown"]).exists()
    assert Path(summary_paths["approved"]).exists()
    assert alert_path.exists()
    assert journal_path.exists()
    assert json.loads(journal_path.read_text(encoding="utf-8"))[0]["run_id"] == "abc"

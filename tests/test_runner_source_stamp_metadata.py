from __future__ import annotations

import json
from pathlib import Path

from alpaca_lab.multi_ticker_portfolio.trader import _git_metadata


def test_git_metadata_falls_back_to_source_stamp_when_git_is_absent(tmp_path: Path) -> None:
    stamp = {
        "runner_branch": "codex/qqq-paper-portfolio",
        "runner_commit": "f2b9bae7b2af26eefc086189a244e4d5a6c81a83",
        "deployed_at": "2026-04-24T10:15:16-04:00",
        "archive_sha256": "a" * 64,
        "deploy_method": "git_archive_overlay_with_backup",
        "broker_facing": False,
        "live_manifest_effect": "none_intended",
        "risk_policy_effect": "none_intended",
    }
    (tmp_path / ".codexalpaca_source_stamp.json").write_text(json.dumps(stamp), encoding="utf-8")

    metadata = _git_metadata(tmp_path)

    assert metadata["runner_repo_metadata_available"] is False
    assert metadata["runner_source_stamp_available"] is True
    assert metadata["runner_repo_commit"] == "f2b9bae7b2af"
    assert metadata["runner_repo_branch"] == "codex/qqq-paper-portfolio"
    assert metadata["runner_repo_dirty"] is False
    assert metadata["runner_source_stamp_archive_sha256"] == "a" * 64


def test_git_metadata_reports_missing_source_stamp_when_git_is_absent(tmp_path: Path) -> None:
    metadata = _git_metadata(tmp_path)

    assert metadata["runner_repo_metadata_available"] is False
    assert metadata["runner_source_stamp_available"] is False
    assert metadata["runner_repo_commit"] is None
    assert metadata["runner_repo_branch"] is None

from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

_decide_sync_action = importlib.import_module("run_multi_ticker_github_sync")._decide_sync_action


def test_sync_decision_is_up_to_date_when_shas_match() -> None:
    result = _decide_sync_action(
        local_sha="abc",
        remote_sha="abc",
        dirty=False,
        market_open=False,
        trader_process_count=0,
        broker_position_count=0,
    )

    assert result.status == "up_to_date"
    assert result.should_apply is False


def test_sync_decision_blocks_dirty_worktree() -> None:
    result = _decide_sync_action(
        local_sha="abc",
        remote_sha="def",
        dirty=True,
        market_open=False,
        trader_process_count=0,
        broker_position_count=0,
    )

    assert result.status == "blocked_dirty_worktree"
    assert result.should_apply is False


def test_sync_decision_waits_for_safe_window_when_market_open() -> None:
    result = _decide_sync_action(
        local_sha="abc",
        remote_sha="def",
        dirty=False,
        market_open=True,
        trader_process_count=1,
        broker_position_count=2,
    )

    assert result.status == "pending_update"
    assert result.should_apply is False
    assert "market_open" in result.message
    assert "trader_running" in result.message
    assert "broker_positions" in result.message


def test_sync_decision_allows_pull_when_flat_and_closed() -> None:
    result = _decide_sync_action(
        local_sha="abc",
        remote_sha="def",
        dirty=False,
        market_open=False,
        trader_process_count=0,
        broker_position_count=0,
    )

    assert result.status == "update_ready"
    assert result.should_apply is True

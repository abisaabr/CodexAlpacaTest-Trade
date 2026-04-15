from __future__ import annotations

from pathlib import Path

from alpaca_lab.execution.failover import evaluate_standby_failover_readiness
from alpaca_lab.execution.ownership import FileOwnershipLease


def test_failover_check_ready_when_shared_lease_is_visible(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    lease_path = tmp_path / "OneDrive" / "CodexAlpaca" / "leases" / "multi_ticker_portfolio.json"
    owner = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-a",
        owner_label="desktop-a",
        ttl_seconds=180,
    )
    owner.acquire(role="portfolio_trader")

    result = evaluate_standby_failover_readiness(
        ownership_enabled=True,
        lease_path=lease_path,
        machine_label="desktop-b",
        lease_ttl_seconds=180,
        repo_root=repo_root,
        require_existing_lease=True,
    )

    assert result.ready is True
    assert result.current_owner_label == "desktop-a"
    assert any("expected standby state" in note for note in result.notes)


def test_failover_check_rejects_repo_local_lease_path(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    lease_path = repo_root / "reports" / "multi_ticker_portfolio" / "state" / "ownership_lease.json"
    lease_path.parent.mkdir(parents=True)
    repo_root.mkdir(exist_ok=True)

    result = evaluate_standby_failover_readiness(
        ownership_enabled=True,
        lease_path=lease_path,
        machine_label="desktop-b",
        lease_ttl_seconds=180,
        repo_root=repo_root,
        require_existing_lease=False,
    )

    assert result.ready is False
    assert any(issue.code == "lease_path_repo_local" for issue in result.issues)


def test_failover_check_rejects_expected_path_mismatch(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    actual_path = tmp_path / "OneDrive" / "CodexAlpaca" / "leases" / "multi_ticker_portfolio.json"
    expected_path = tmp_path / "OtherDrive" / "CodexAlpaca" / "leases" / "multi_ticker_portfolio.json"

    result = evaluate_standby_failover_readiness(
        ownership_enabled=True,
        lease_path=actual_path,
        machine_label="desktop-b",
        lease_ttl_seconds=180,
        repo_root=repo_root,
        expected_lease_path=expected_path,
        require_existing_lease=False,
    )

    assert result.ready is False
    assert any(issue.code == "expected_lease_path_mismatch" for issue in result.issues)


def test_failover_check_allows_missing_file_when_expected_path_matches(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    expected_path = tmp_path / "OneDrive" / "CodexAlpaca" / "leases" / "multi_ticker_portfolio.json"

    result = evaluate_standby_failover_readiness(
        ownership_enabled=True,
        lease_path=expected_path,
        machine_label="desktop-b",
        lease_ttl_seconds=180,
        repo_root=repo_root,
        expected_lease_path=expected_path,
        require_existing_lease=True,
    )

    assert result.ready is True
    assert any("matches the expected shared lease path" in note for note in result.notes)

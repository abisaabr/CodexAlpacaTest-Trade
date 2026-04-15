from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from alpaca_lab.config import LabSettings
from alpaca_lab.execution.ownership import FileOwnershipLease
from alpaca_lab.multi_ticker_portfolio import (
    MultiTickerPortfolioPaperTrader,
    default_portfolio_config,
)


def test_file_ownership_lease_blocks_other_owner(tmp_path: Path) -> None:
    lease_path = tmp_path / "shared_lease.json"
    owner_a = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )
    owner_b = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-b",
        owner_label="machine-b",
        ttl_seconds=180,
    )

    acquired = owner_a.acquire(role="portfolio_trader")
    blocked = owner_b.acquire(role="portfolio_trader")

    assert acquired.acquired is True
    assert blocked.blocked is True
    assert blocked.blocked_by_owner_id == "owner-a"
    assert blocked.blocked_by_owner_label == "machine-a"


def test_file_ownership_lease_allows_same_owner_multiple_roles(tmp_path: Path) -> None:
    lease = FileOwnershipLease(
        path=tmp_path / "shared_lease.json",
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )

    trader_role = lease.acquire(role="portfolio_trader")
    close_role = lease.acquire(role="eod_close_guard")

    assert trader_role.acquired is True
    assert close_role.acquired is True
    assert set((close_role.roles or {}).keys()) == {"portfolio_trader", "eod_close_guard"}


def test_trader_run_returns_ownership_blocked_when_other_owner_holds_lease(tmp_path: Path) -> None:
    lease_path = tmp_path / "shared_lease.json"
    other_owner = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )
    other_owner.acquire(role="portfolio_trader")

    class _BrokerStub:
        def get_clock(self) -> dict[str, object]:
            return {
                "is_open": False,
                "timestamp": datetime(2026, 4, 15, 12, 0, tzinfo=UTC).isoformat(),
            }

    config = default_portfolio_config().model_copy(
        update={
            "execution": default_portfolio_config().execution.model_copy(
                update={
                    "state_root": tmp_path / "state",
                    "run_root": tmp_path / "runs",
                }
            ),
            "ownership": default_portfolio_config().ownership.model_copy(
                update={
                    "lease_path": lease_path,
                    "machine_label": "machine-b",
                }
            ),
        }
    )
    trader = MultiTickerPortfolioPaperTrader(
        LabSettings(),
        config,
        broker=_BrokerStub(),
        submit_paper_orders=False,
    )

    result = trader.run(run_once=True)

    assert result["status"] == "ownership_blocked"
    assert result["lease"]["blocked_by_owner_id"] == "owner-a"


def test_file_ownership_lease_without_existing_owner_is_not_blocked(tmp_path: Path) -> None:
    lease = FileOwnershipLease(
        path=tmp_path / "shared_lease.json",
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )

    status = lease.inspect()

    assert status.owner_id is None
    assert status.blocked is False

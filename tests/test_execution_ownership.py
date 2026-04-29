from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from alpaca_lab.config import LabSettings
from alpaca_lab.execution.ownership import (
    FileOwnershipLease,
    GCSGenerationMatchLeaseStore,
    GenerationMatchOwnershipLease,
    LeaseConflictError,
    ObjectLeaseRecord,
)
from alpaca_lab.multi_ticker_portfolio import (
    MultiTickerPortfolioPaperTrader,
    default_portfolio_config,
)


class _InMemoryObjectLeaseStore:
    def __init__(self) -> None:
        self.payload: dict[str, object] | None = None
        self.generation: int = 0

    def read(self) -> ObjectLeaseRecord | None:
        if self.payload is None:
            return None
        return ObjectLeaseRecord(payload=dict(self.payload), generation=str(self.generation))

    def create_if_absent(self, payload: dict[str, object]) -> str:
        if self.payload is not None:
            raise LeaseConflictError("object already exists")
        self.generation += 1
        self.payload = dict(payload)
        return str(self.generation)

    def replace_if_generation(self, *, generation: str, payload: dict[str, object]) -> str:
        if self.payload is None or str(self.generation) != str(generation):
            raise LeaseConflictError("generation mismatch")
        self.generation += 1
        self.payload = dict(payload)
        return str(self.generation)

    def delete_if_generation(self, *, generation: str) -> None:
        if self.payload is None or str(self.generation) != str(generation):
            raise LeaseConflictError("generation mismatch")
        self.generation += 1
        self.payload = None


class _FakeGcsError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.code = status_code
        self.status_code = status_code


class _FakeBlob:
    def __init__(self) -> None:
        self._data: str | None = None
        self.generation: int | None = None

    def reload(self) -> None:
        if self._data is None:
            raise _FakeGcsError(404, "not found")

    def download_as_text(self, *, encoding: str = "utf-8") -> str:
        assert encoding == "utf-8"
        if self._data is None:
            raise _FakeGcsError(404, "not found")
        return self._data

    def upload_from_string(
        self,
        data: str,
        *,
        content_type: str,
        if_generation_match: int | None = None,
    ) -> None:
        assert content_type == "application/json"
        current_generation = self.generation
        if if_generation_match == 0 and self._data is not None:
            raise _FakeGcsError(412, "precondition failed")
        if if_generation_match not in (None, 0) and current_generation != if_generation_match:
            raise _FakeGcsError(412, "precondition failed")
        self.generation = 1 if current_generation is None else current_generation + 1
        self._data = data

    def delete(self, *, if_generation_match: int | None = None) -> None:
        if self._data is None:
            raise _FakeGcsError(404, "not found")
        if if_generation_match is not None and self.generation != if_generation_match:
            raise _FakeGcsError(412, "precondition failed")
        self.generation = None
        self._data = None


class _FakeBucket:
    def __init__(self, blob: _FakeBlob) -> None:
        self._blob = blob

    def blob(self, object_name: str) -> _FakeBlob:
        assert object_name == "leases/paper-execution/lease.json"
        return self._blob


class _FakeStorageClient:
    def __init__(self, blob: _FakeBlob) -> None:
        self._blob = blob

    def bucket(self, bucket_name: str) -> _FakeBucket:
        assert bucket_name == "codexalpaca-control-us"
        return _FakeBucket(self._blob)


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


def test_file_ownership_lease_blocks_same_owner_same_role_from_other_process(
    tmp_path: Path,
    monkeypatch,
) -> None:
    lease_path = tmp_path / "shared_lease.json"
    monkeypatch.setattr("alpaca_lab.execution.ownership._pid_is_running", lambda _pid: True)
    monkeypatch.setattr("alpaca_lab.execution.ownership.os.getpid", lambda: 101)
    owner_a = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )
    acquired = owner_a.acquire(role="portfolio_trader")

    monkeypatch.setattr("alpaca_lab.execution.ownership.os.getpid", lambda: 202)
    owner_b = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )
    blocked = owner_b.acquire(role="portfolio_trader")

    assert acquired.acquired is True
    assert blocked.blocked is True
    assert blocked.blocked_by_owner_id == "owner-a"
    assert blocked.blocked_by_owner_label == "machine-a"


def test_file_ownership_lease_allows_same_owner_same_role_when_old_pid_is_dead(
    tmp_path: Path,
    monkeypatch,
) -> None:
    lease_path = tmp_path / "shared_lease.json"
    monkeypatch.setattr("alpaca_lab.execution.ownership.os.getpid", lambda: 101)
    owner_a = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )
    acquired = owner_a.acquire(role="portfolio_trader")

    monkeypatch.setattr("alpaca_lab.execution.ownership.os.getpid", lambda: 202)
    monkeypatch.setattr("alpaca_lab.execution.ownership._pid_is_running", lambda _pid: False)
    owner_b = FileOwnershipLease(
        path=lease_path,
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )
    reclaimed = owner_b.acquire(role="portfolio_trader")

    assert acquired.acquired is True
    assert reclaimed.acquired is True
    assert reclaimed.blocked is False


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
                    "enabled": True,
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


def test_file_ownership_lease_release_removes_last_role(tmp_path: Path) -> None:
    lease = FileOwnershipLease(
        path=tmp_path / "shared_lease.json",
        owner_id="owner-a",
        owner_label="machine-a",
        ttl_seconds=180,
    )
    lease.acquire(role="portfolio_trader")

    released = lease.release(role="portfolio_trader")

    assert released.owner_id is None
    assert released.roles == {}
    assert (tmp_path / "shared_lease.json").exists() is False


def test_generation_match_ownership_lease_blocks_other_owner() -> None:
    store = _InMemoryObjectLeaseStore()
    owner_a = GenerationMatchOwnershipLease(
        store=store,
        lease_path="gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        owner_id="owner-a",
        owner_label="machine-a",
        machine_label="machine-a",
        runner_path="vm-execution-paper-01",
        git_commit="abc123",
        audit_context={"plane": "execution", "environment": "paper", "source": "vm"},
    )
    owner_b = GenerationMatchOwnershipLease(
        store=store,
        lease_path="gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        owner_id="owner-b",
        owner_label="machine-b",
        machine_label="machine-b",
        runner_path="desktop-b",
        git_commit="def456",
        audit_context={"plane": "execution", "environment": "paper", "source": "workstation"},
    )

    acquired = owner_a.acquire(role="portfolio_trader")
    blocked = owner_b.acquire(role="portfolio_trader")

    assert acquired.acquired is True
    assert acquired.generation == "1"
    assert blocked.blocked is True
    assert blocked.blocked_by_owner_id == "owner-a"
    assert blocked.blocked_by_owner_label == "machine-a"


def test_generation_match_ownership_lease_allows_same_owner_multiple_roles() -> None:
    store = _InMemoryObjectLeaseStore()
    lease = GenerationMatchOwnershipLease(
        store=store,
        lease_path="gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        owner_id="owner-a",
        owner_label="machine-a",
        machine_label="machine-a",
        runner_path="vm-execution-paper-01",
        git_commit="abc123",
        audit_context={"plane": "execution", "environment": "paper", "source": "vm"},
    )

    trader_role = lease.acquire(role="portfolio_trader")
    close_role = lease.acquire(role="eod_close_guard")

    assert trader_role.acquired is True
    assert close_role.acquired is True
    assert set((close_role.roles or {}).keys()) == {"portfolio_trader", "eod_close_guard"}
    assert close_role.generation == "2"


def test_generation_match_ownership_lease_can_take_over_expired_lease(monkeypatch) -> None:
    store = _InMemoryObjectLeaseStore()
    base_time = datetime(2026, 4, 23, 15, 0, tzinfo=UTC)
    monkeypatch.setattr("alpaca_lab.execution.ownership._now_utc", lambda: base_time)
    owner_a = GenerationMatchOwnershipLease(
        store=store,
        lease_path="gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        owner_id="owner-a",
        owner_label="machine-a",
        machine_label="machine-a",
        runner_path="vm-execution-paper-01",
        git_commit="abc123",
    )
    acquired = owner_a.acquire(role="portfolio_trader")

    monkeypatch.setattr(
        "alpaca_lab.execution.ownership._now_utc",
        lambda: base_time + timedelta(minutes=10),
    )
    owner_b = GenerationMatchOwnershipLease(
        store=store,
        lease_path="gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        owner_id="owner-b",
        owner_label="machine-b",
        machine_label="machine-b",
        runner_path="desktop-b",
        git_commit="def456",
    )
    takeover = owner_b.acquire(role="portfolio_trader")

    assert acquired.acquired is True
    assert takeover.acquired is True
    assert takeover.owner_id == "owner-b"
    assert takeover.owner_label == "machine-b"


def test_generation_match_ownership_lease_release_removes_last_role() -> None:
    store = _InMemoryObjectLeaseStore()
    lease = GenerationMatchOwnershipLease(
        store=store,
        lease_path="gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        owner_id="owner-a",
        owner_label="machine-a",
        machine_label="machine-a",
        runner_path="vm-execution-paper-01",
        git_commit="abc123",
    )
    lease.acquire(role="portfolio_trader")

    released = lease.release(role="portfolio_trader")

    assert released.owner_id is None
    assert released.roles == {}
    assert store.read() is None


def test_gcs_generation_match_store_round_trips_payload() -> None:
    blob = _FakeBlob()
    store = GCSGenerationMatchLeaseStore.from_gcs_uri(
        "gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        client=_FakeStorageClient(blob),
    )
    payload = {"owner_id": "owner-a", "roles": {"portfolio_trader": {"expires_at": "2026-04-23T20:00:00+00:00"}}}

    generation = store.create_if_absent(payload)
    record = store.read()

    assert generation == "1"
    assert record is not None
    assert record.generation == "1"
    assert record.payload["owner_id"] == "owner-a"


def test_gcs_generation_match_store_detects_generation_conflict() -> None:
    blob = _FakeBlob()
    store = GCSGenerationMatchLeaseStore.from_gcs_uri(
        "gs://codexalpaca-control-us/leases/paper-execution/lease.json",
        client=_FakeStorageClient(blob),
    )
    store.create_if_absent({"owner_id": "owner-a", "roles": {}})

    try:
        store.replace_if_generation(generation="999", payload={"owner_id": "owner-b", "roles": {}})
    except LeaseConflictError:
        pass
    else:
        raise AssertionError("expected generation mismatch to raise LeaseConflictError")


def test_gcs_generation_match_store_rejects_invalid_uri() -> None:
    try:
        GCSGenerationMatchLeaseStore.from_gcs_uri("not-a-gs-uri", client=_FakeStorageClient(_FakeBlob()))
    except ValueError as exc:
        assert "gs://" in str(exc)
    else:
        raise AssertionError("expected invalid GCS URI to raise ValueError")

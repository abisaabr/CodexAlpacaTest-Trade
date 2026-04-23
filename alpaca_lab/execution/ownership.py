from __future__ import annotations

import getpass
import hashlib
import json
import os
import platform
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _pid_is_running(pid: str | None) -> bool:
    if pid in (None, ""):
        return False
    try:
        os.kill(int(pid), 0)
    except (OSError, ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True
    return True


def _iso_or_none(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _filtered_roles(payload: dict[str, Any], now_utc: datetime) -> dict[str, dict[str, Any]]:
    roles_payload = payload.get("roles", {})
    if not isinstance(roles_payload, dict):
        return {}
    fresh_roles: dict[str, dict[str, Any]] = {}
    for role_name, role_payload in roles_payload.items():
        if not isinstance(role_payload, dict):
            continue
        expires_at_raw = _iso_or_none(role_payload.get("expires_at"))
        if expires_at_raw is None:
            continue
        try:
            expires_at = datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if expires_at < now_utc:
            continue
        fresh_roles[str(role_name)] = dict(role_payload)
    return fresh_roles


def default_owner_id() -> str:
    fingerprint = f"{getpass.getuser()}|{platform.node()}|{uuid.getnode()}"
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:24]


def default_owner_label() -> str:
    return f"{getpass.getuser()}@{platform.node()}"


@dataclass(slots=True)
class OwnershipLeaseStatus:
    enabled: bool
    acquired: bool
    held_by_self: bool
    owner_id: str | None
    owner_label: str | None
    blocked_by_owner_id: str | None = None
    blocked_by_owner_label: str | None = None
    lease_path: str | None = None
    heartbeat_at: str | None = None
    expires_at: str | None = None
    roles: dict[str, Any] | None = None
    generation: str | None = None

    @property
    def blocked(self) -> bool:
        return self.enabled and not self.acquired and (
            self.blocked_by_owner_id is not None or self.blocked_by_owner_label is not None
        )


class FileOwnershipLease:
    def __init__(
        self,
        *,
        path: Path,
        owner_id: str | None = None,
        owner_label: str | None = None,
        ttl_seconds: int = 180,
    ) -> None:
        self.path = path
        self.owner_id = owner_id or default_owner_id()
        self.owner_label = owner_label or default_owner_label()
        self.ttl_seconds = max(30, int(ttl_seconds))

    def _read_payload(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _write_payload(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp_path.replace(self.path)

    def inspect(self) -> OwnershipLeaseStatus:
        now_utc = _now_utc()
        payload = self._read_payload()
        roles = _filtered_roles(payload, now_utc)
        owner_id = str(payload.get("owner_id") or "") or None
        owner_label = str(payload.get("owner_label") or "") or None
        if not roles or owner_id is None:
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=None,
                owner_label=None,
                lease_path=str(self.path),
                roles={},
            )
        held_by_self = owner_id == self.owner_id
        heartbeat_at = payload.get("heartbeat_at")
        expires_at = payload.get("expires_at")
        return OwnershipLeaseStatus(
            enabled=True,
            acquired=held_by_self,
            held_by_self=held_by_self,
            owner_id=owner_id,
            owner_label=owner_label,
            blocked_by_owner_id=None if held_by_self else owner_id,
            blocked_by_owner_label=None if held_by_self else owner_label,
            lease_path=str(self.path),
            heartbeat_at=_iso_or_none(heartbeat_at),
            expires_at=_iso_or_none(expires_at),
            roles=roles,
        )

    def acquire(self, *, role: str, metadata: dict[str, Any] | None = None) -> OwnershipLeaseStatus:
        now_utc = _now_utc()
        payload = self._read_payload()
        roles = _filtered_roles(payload, now_utc)
        owner_id = str(payload.get("owner_id") or "") or None
        owner_label = str(payload.get("owner_label") or "") or None
        existing_role = roles.get(str(role))
        current_pid = str(os.getpid())
        if owner_id == self.owner_id and isinstance(existing_role, dict):
            existing_pid = str(existing_role.get("pid") or "") or None
            if existing_pid != current_pid and _pid_is_running(existing_pid):
                return OwnershipLeaseStatus(
                    enabled=True,
                    acquired=False,
                    held_by_self=False,
                    owner_id=owner_id,
                    owner_label=owner_label or self.owner_label,
                    blocked_by_owner_id=owner_id,
                    blocked_by_owner_label=owner_label or self.owner_label,
                    lease_path=str(self.path),
                    heartbeat_at=_iso_or_none(payload.get("heartbeat_at")),
                    expires_at=_iso_or_none(payload.get("expires_at")),
                    roles=roles,
                )
        if owner_id and owner_id != self.owner_id and roles:
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=owner_id,
                owner_label=owner_label,
                blocked_by_owner_id=owner_id,
                blocked_by_owner_label=owner_label,
                lease_path=str(self.path),
                heartbeat_at=_iso_or_none(payload.get("heartbeat_at")),
                expires_at=_iso_or_none(payload.get("expires_at")),
                roles=roles,
            )
        expires_at = now_utc + timedelta(seconds=self.ttl_seconds)
        role_payload = {
            "heartbeat_at": now_utc.isoformat(),
            "expires_at": expires_at.isoformat(),
            "pid": str(os.getpid()),
        }
        if metadata:
            role_payload.update(metadata)
        roles[str(role)] = role_payload
        lease_payload = {
            "version": 1,
            "owner_id": self.owner_id,
            "owner_label": self.owner_label,
            "heartbeat_at": now_utc.isoformat(),
            "expires_at": max(
                datetime.fromisoformat(str(role_data["expires_at"]).replace("Z", "+00:00"))
                for role_data in roles.values()
            ).isoformat(),
            "roles": roles,
        }
        self._write_payload(lease_payload)
        return OwnershipLeaseStatus(
            enabled=True,
            acquired=True,
            held_by_self=True,
            owner_id=self.owner_id,
            owner_label=self.owner_label,
            lease_path=str(self.path),
            heartbeat_at=lease_payload["heartbeat_at"],
            expires_at=lease_payload["expires_at"],
            roles=roles,
        )

    def renew(self, *, role: str, metadata: dict[str, Any] | None = None) -> OwnershipLeaseStatus:
        return self.acquire(role=role, metadata=metadata)

    def release(self, *, role: str) -> OwnershipLeaseStatus:
        now_utc = _now_utc()
        payload = self._read_payload()
        roles = _filtered_roles(payload, now_utc)
        owner_id = str(payload.get("owner_id") or "") or None
        owner_label = str(payload.get("owner_label") or "") or None
        if owner_id != self.owner_id:
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=owner_id,
                owner_label=owner_label,
                blocked_by_owner_id=owner_id,
                blocked_by_owner_label=owner_label,
                lease_path=str(self.path),
                heartbeat_at=_iso_or_none(payload.get("heartbeat_at")),
                expires_at=_iso_or_none(payload.get("expires_at")),
                roles=roles,
            )
        if str(role) not in roles:
            return self.inspect()
        roles.pop(str(role), None)
        if not roles:
            try:
                self.path.unlink(missing_ok=True)
            except TypeError:
                if self.path.exists():
                    self.path.unlink()
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=None,
                owner_label=None,
                lease_path=str(self.path),
                roles={},
            )
        expires_at = max(
            datetime.fromisoformat(str(role_data["expires_at"]).replace("Z", "+00:00"))
            for role_data in roles.values()
        ).isoformat()
        lease_payload = {
            "version": 1,
            "owner_id": self.owner_id,
            "owner_label": self.owner_label,
            "heartbeat_at": now_utc.isoformat(),
            "expires_at": expires_at,
            "roles": roles,
        }
        self._write_payload(lease_payload)
        return OwnershipLeaseStatus(
            enabled=True,
            acquired=True,
            held_by_self=True,
            owner_id=self.owner_id,
            owner_label=self.owner_label,
            lease_path=str(self.path),
            heartbeat_at=lease_payload["heartbeat_at"],
            expires_at=lease_payload["expires_at"],
            roles=roles,
        )


@dataclass(slots=True)
class ObjectLeaseRecord:
    payload: dict[str, Any]
    generation: str


class LeaseConflictError(RuntimeError):
    pass


class ObjectLeaseStore(Protocol):
    def read(self) -> ObjectLeaseRecord | None:
        ...

    def create_if_absent(self, payload: dict[str, Any]) -> str:
        ...

    def replace_if_generation(self, *, generation: str, payload: dict[str, Any]) -> str:
        ...

    def delete_if_generation(self, *, generation: str) -> None:
        ...


class GenerationMatchOwnershipLease:
    def __init__(
        self,
        *,
        store: ObjectLeaseStore,
        lease_path: str,
        owner_id: str | None = None,
        owner_label: str | None = None,
        ttl_seconds: int = 180,
        machine_label: str | None = None,
        runner_path: str | None = None,
        git_commit: str | None = None,
        audit_context: dict[str, Any] | None = None,
    ) -> None:
        self.store = store
        self.lease_path = lease_path
        self.owner_id = owner_id or default_owner_id()
        self.owner_label = owner_label or default_owner_label()
        self.ttl_seconds = max(30, int(ttl_seconds))
        self.machine_label = machine_label or self.owner_label
        self.runner_path = runner_path
        self.git_commit = git_commit
        self.audit_context = dict(audit_context or {})

    def _record_to_status(self, record: ObjectLeaseRecord | None) -> OwnershipLeaseStatus:
        if record is None:
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=None,
                owner_label=None,
                lease_path=self.lease_path,
                roles={},
                generation=None,
            )
        now_utc = _now_utc()
        payload = record.payload
        roles = _filtered_roles(payload, now_utc)
        owner_id = str(payload.get("owner_id") or "") or None
        owner_label = str(payload.get("owner_label") or "") or None
        if not roles or owner_id is None:
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=None,
                owner_label=None,
                lease_path=self.lease_path,
                heartbeat_at=_iso_or_none(payload.get("heartbeat_at")),
                expires_at=_iso_or_none(payload.get("expires_at")),
                roles={},
                generation=record.generation,
            )
        held_by_self = owner_id == self.owner_id
        return OwnershipLeaseStatus(
            enabled=True,
            acquired=held_by_self,
            held_by_self=held_by_self,
            owner_id=owner_id,
            owner_label=owner_label,
            blocked_by_owner_id=None if held_by_self else owner_id,
            blocked_by_owner_label=None if held_by_self else owner_label,
            lease_path=self.lease_path,
            heartbeat_at=_iso_or_none(payload.get("heartbeat_at")),
            expires_at=_iso_or_none(payload.get("expires_at")),
            roles=roles,
            generation=record.generation,
        )

    def _build_payload(self, *, now_utc: datetime, roles: dict[str, dict[str, Any]]) -> dict[str, Any]:
        expires_at = max(
            datetime.fromisoformat(str(role_data["expires_at"]).replace("Z", "+00:00"))
            for role_data in roles.values()
        )
        payload: dict[str, Any] = {
            "version": 1,
            "lease_kind": "paper_execution",
            "owner_id": self.owner_id,
            "owner_label": self.owner_label,
            "machine_label": self.machine_label,
            "runner_path": self.runner_path,
            "git_commit": self.git_commit,
            "acquired_at": now_utc.isoformat(),
            "heartbeat_at": now_utc.isoformat(),
            "expires_at": expires_at.isoformat(),
            "roles": roles,
            "audit_context": dict(self.audit_context),
        }
        return payload

    def inspect(self) -> OwnershipLeaseStatus:
        return self._record_to_status(self.store.read())

    def acquire(self, *, role: str, metadata: dict[str, Any] | None = None) -> OwnershipLeaseStatus:
        now_utc = _now_utc()
        record = self.store.read()
        status = self._record_to_status(record)
        roles = dict(status.roles or {})
        current_pid = str(os.getpid())
        existing_role = roles.get(str(role))
        if status.owner_id == self.owner_id and isinstance(existing_role, dict):
            existing_pid = str(existing_role.get("pid") or "") or None
            if existing_pid != current_pid and _pid_is_running(existing_pid):
                return OwnershipLeaseStatus(
                    enabled=True,
                    acquired=False,
                    held_by_self=False,
                    owner_id=status.owner_id,
                    owner_label=status.owner_label or self.owner_label,
                    blocked_by_owner_id=status.owner_id,
                    blocked_by_owner_label=status.owner_label or self.owner_label,
                    lease_path=self.lease_path,
                    heartbeat_at=status.heartbeat_at,
                    expires_at=status.expires_at,
                    roles=roles,
                    generation=status.generation,
                )
        if status.owner_id and status.owner_id != self.owner_id and roles:
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=status.owner_id,
                owner_label=status.owner_label,
                blocked_by_owner_id=status.owner_id,
                blocked_by_owner_label=status.owner_label,
                lease_path=self.lease_path,
                heartbeat_at=status.heartbeat_at,
                expires_at=status.expires_at,
                roles=roles,
                generation=status.generation,
            )
        expires_at = now_utc + timedelta(seconds=self.ttl_seconds)
        role_payload = {
            "heartbeat_at": now_utc.isoformat(),
            "expires_at": expires_at.isoformat(),
            "pid": str(os.getpid()),
        }
        if metadata:
            role_payload.update(metadata)
        roles[str(role)] = role_payload
        payload = self._build_payload(now_utc=now_utc, roles=roles)
        try:
            if record is None:
                generation = self.store.create_if_absent(payload)
            else:
                generation = self.store.replace_if_generation(
                    generation=record.generation,
                    payload=payload,
                )
        except LeaseConflictError:
            return self.inspect()
        refreshed = self.inspect()
        refreshed.generation = generation
        return refreshed

    def renew(self, *, role: str, metadata: dict[str, Any] | None = None) -> OwnershipLeaseStatus:
        return self.acquire(role=role, metadata=metadata)

    def release(self, *, role: str) -> OwnershipLeaseStatus:
        now_utc = _now_utc()
        record = self.store.read()
        status = self._record_to_status(record)
        roles = dict(status.roles or {})
        if record is None:
            return status
        if status.owner_id != self.owner_id:
            return OwnershipLeaseStatus(
                enabled=True,
                acquired=False,
                held_by_self=False,
                owner_id=status.owner_id,
                owner_label=status.owner_label,
                blocked_by_owner_id=status.owner_id,
                blocked_by_owner_label=status.owner_label,
                lease_path=self.lease_path,
                heartbeat_at=status.heartbeat_at,
                expires_at=status.expires_at,
                roles=roles,
                generation=record.generation,
            )
        if str(role) not in roles:
            return status
        roles.pop(str(role), None)
        try:
            if not roles:
                self.store.delete_if_generation(generation=record.generation)
                return OwnershipLeaseStatus(
                    enabled=True,
                    acquired=False,
                    held_by_self=False,
                    owner_id=None,
                    owner_label=None,
                    lease_path=self.lease_path,
                    roles={},
                    generation=None,
                )
            payload = self._build_payload(now_utc=now_utc, roles=roles)
            generation = self.store.replace_if_generation(
                generation=record.generation,
                payload=payload,
            )
        except LeaseConflictError:
            return self.inspect()
        refreshed = self.inspect()
        refreshed.generation = generation
        return refreshed


class NoopOwnershipLease:
    def inspect(self) -> OwnershipLeaseStatus:
        return OwnershipLeaseStatus(
            enabled=False,
            acquired=True,
            held_by_self=True,
            owner_id=None,
            owner_label=None,
            lease_path=None,
            roles={},
        )

    def acquire(self, *, role: str, metadata: dict[str, Any] | None = None) -> OwnershipLeaseStatus:
        return self.inspect()

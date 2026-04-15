from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from alpaca_lab.execution.ownership import FileOwnershipLease


def _normalize_path(path: Path) -> str:
    return os.path.normcase(str(path.expanduser().resolve(strict=False)))


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(base.resolve(strict=False))
        return True
    except ValueError:
        return False


def _shared_path_hint(path: Path, repo_root: Path | None) -> str:
    if not path.is_absolute():
        return "relative_path"
    if repo_root is not None and _is_relative_to(path, repo_root):
        return "repo_local"
    lower_path = _normalize_path(path)
    if lower_path.startswith("\\\\"):
        return "network_share"
    shared_markers = (
        "onedrive",
        "dropbox",
        "google drive",
        "googledrive",
        "icloud",
        "box",
    )
    if any(marker in lower_path for marker in shared_markers):
        return "cloud_synced"
    return "absolute_external"


@dataclass(slots=True)
class FailoverCheckIssue:
    severity: str
    code: str
    message: str


@dataclass(slots=True)
class FailoverCheckResult:
    ready: bool
    status: str
    ownership_enabled: bool
    machine_label: str | None
    lease_path: str | None
    expected_lease_path: str | None
    lease_exists: bool
    shared_path_hint: str | None
    current_owner_id: str | None = None
    current_owner_label: str | None = None
    current_owner_expires_at: str | None = None
    issues: list[FailoverCheckIssue] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["issues"] = [asdict(issue) for issue in self.issues]
        return payload


def evaluate_standby_failover_readiness(
    *,
    ownership_enabled: bool,
    lease_path: Path,
    machine_label: str | None,
    lease_ttl_seconds: int,
    repo_root: Path | None = None,
    expected_lease_path: Path | None = None,
    require_existing_lease: bool = True,
) -> FailoverCheckResult:
    issues: list[FailoverCheckIssue] = []
    notes: list[str] = []
    resolved_lease_path = lease_path.expanduser().resolve(strict=False)
    resolved_expected_path = (
        expected_lease_path.expanduser().resolve(strict=False)
        if expected_lease_path is not None
        else None
    )
    shared_hint = _shared_path_hint(resolved_lease_path, repo_root)

    if not ownership_enabled:
        issues.append(
            FailoverCheckIssue(
                severity="error",
                code="ownership_disabled",
                message="Ownership lease is disabled, so a standby machine cannot coordinate safely.",
            )
        )
    if not machine_label:
        issues.append(
            FailoverCheckIssue(
                severity="error",
                code="machine_label_missing",
                message="MULTI_TICKER_MACHINE_LABEL is not set. Give each machine a distinct label.",
            )
        )
    if not lease_path.is_absolute():
        issues.append(
            FailoverCheckIssue(
                severity="error",
                code="lease_path_not_absolute",
                message="Lease path is not absolute. Use a fully qualified shared path for failover safety.",
            )
        )
    if shared_hint == "repo_local":
        issues.append(
            FailoverCheckIssue(
                severity="error",
                code="lease_path_repo_local",
                message="Lease path resolves inside the repo, so another machine will not see the same lease file.",
            )
        )
    elif shared_hint == "relative_path":
        issues.append(
            FailoverCheckIssue(
                severity="error",
                code="lease_path_relative",
                message="Lease path is relative. Point it at a shared absolute path before enabling standby failover.",
            )
        )
    elif shared_hint in {"cloud_synced", "network_share"}:
        notes.append(f"Lease path looks shared-friendly ({shared_hint}).")
    else:
        notes.append(
            "Lease path is absolute and outside the repo. Make sure the same path really exists on the standby machine."
        )

    if resolved_expected_path is not None:
        if _normalize_path(resolved_lease_path) != _normalize_path(resolved_expected_path):
            issues.append(
                FailoverCheckIssue(
                    severity="error",
                    code="expected_lease_path_mismatch",
                    message=(
                        "Configured lease path does not match the expected lease path. "
                        "Both machines should point at the same shared file."
                    ),
                )
            )
        else:
            notes.append("Configured lease path matches the expected shared lease path.")

    lease_exists = resolved_lease_path.exists()
    if require_existing_lease and not lease_exists and resolved_expected_path is None:
        issues.append(
            FailoverCheckIssue(
                severity="error",
                code="lease_file_missing",
                message=(
                    "The shared lease file does not exist yet. On a standby machine, that usually means you are "
                    "not looking at the same synced file as the active machine."
                ),
            )
        )

    owner_id = None
    owner_label = None
    owner_expires_at = None
    if lease_exists:
        lease = FileOwnershipLease(
            path=resolved_lease_path,
            owner_label=machine_label,
            ttl_seconds=lease_ttl_seconds,
        )
        status = lease.inspect()
        owner_id = status.owner_id
        owner_label = status.owner_label
        owner_expires_at = status.expires_at
        if status.blocked:
            notes.append(
                f"Another machine currently owns the lease ({status.blocked_by_owner_label or status.blocked_by_owner_id}). "
                "This is the expected standby state."
            )
        elif status.held_by_self:
            notes.append("This machine already owns the lease. That is fine for a migration, but not a standby validation.")
        else:
            notes.append("Lease file is visible and currently unowned.")
        if machine_label and owner_label and machine_label == owner_label and status.blocked:
            issues.append(
                FailoverCheckIssue(
                    severity="warning",
                    code="machine_label_duplicate",
                    message=(
                        "This machine label matches the current lease owner label. The runtime will still be safe, "
                        "but duplicate labels make failover debugging harder."
                    ),
                )
            )
    else:
        if resolved_expected_path is not None:
            notes.append(
                "Lease file is not present yet, but the configured path matches the expected shared lease path."
            )
        else:
            notes.append("Lease file is not present yet.")

    ready = not any(issue.severity == "error" for issue in issues)
    status = "ready" if ready else "not_ready"
    return FailoverCheckResult(
        ready=ready,
        status=status,
        ownership_enabled=ownership_enabled,
        machine_label=machine_label,
        lease_path=str(resolved_lease_path),
        expected_lease_path=str(resolved_expected_path) if resolved_expected_path is not None else None,
        lease_exists=lease_exists,
        shared_path_hint=shared_hint,
        current_owner_id=owner_id,
        current_owner_label=owner_label,
        current_owner_expires_at=owner_expires_at,
        issues=issues,
        notes=notes,
    )

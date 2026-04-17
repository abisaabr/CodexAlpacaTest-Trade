from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from dotenv import dotenv_values

STATE_RELATIVE_PATH = Path("reports") / "multi_ticker_portfolio" / "state"
RUNS_RELATIVE_PATH = Path("reports") / "multi_ticker_portfolio" / "runs"
HEALTH_RELATIVE_PATH = Path("reports") / "multi_ticker_portfolio" / "health"
ENV_RELATIVE_PATH = Path(".env")
LATEST_HEALTH_FILENAME = "latest_health_check.json"


def _utc_timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def _copy_tree(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_value(repo_root: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    value = result.stdout.strip()
    return value or None


def _resolve_latest_session_file(repo_root: Path) -> Path | None:
    state_dir = repo_root / STATE_RELATIVE_PATH
    if not state_dir.exists():
        return None
    session_files = sorted(state_dir.glob("session_*.json"))
    return session_files[-1] if session_files else None


def _build_restore_notes(
    *,
    trade_date: str | None,
    lease_path: str | None,
    machine_label: str | None,
    repo_branch: str | None,
    repo_commit: str | None,
) -> list[str]:
    notes = [
        "Clone the same repo branch on the destination machine before restoring this bundle.",
        "Run the standby failover check before starting the trader.",
        "Do not let both machines actively trade unless they share the same ownership lease path.",
    ]
    if trade_date:
        notes.append(
            f"This bundle includes live runtime state for trade date {trade_date}. Restore it before starting the runner."
        )
    if lease_path:
        notes.append(f"Configured shared lease path: {lease_path}")
    if machine_label:
        notes.append(
            f"Source machine label was '{machine_label}'. Override it on the destination machine so the labels stay distinct."
        )
    if repo_branch or repo_commit:
        notes.append(
            "Checkout the same code before restoring: "
            f"branch={repo_branch or 'unknown'} commit={repo_commit or 'unknown'}."
        )
    return notes


@dataclass(slots=True)
class MigrationBundleManifest:
    created_at_utc: str
    bundle_name: str
    trade_date: str | None
    repo_branch: str | None
    repo_commit: str | None
    session_file: str | None
    run_directory: str | None
    latest_health_file: str | None
    ownership_lease_path: str | None
    source_machine_label: str | None
    included_env_file: bool
    included_state_dir: bool
    included_run_dir: bool
    included_health_file: bool
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def create_runtime_migration_bundle(
    *,
    repo_root: Path,
    output_root: Path,
    bundle_name: str | None = None,
    include_env: bool = True,
    zip_bundle: bool = True,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    session_file = _resolve_latest_session_file(repo_root)
    trade_date = None
    run_relative_path: Path | None = None
    if session_file is not None:
        trade_date = session_file.stem.removeprefix("session_")
        run_relative_path = RUNS_RELATIVE_PATH / trade_date

    env_values = dotenv_values(repo_root / ENV_RELATIVE_PATH)
    lease_path = env_values.get("MULTI_TICKER_OWNERSHIP_LEASE_PATH")
    machine_label = env_values.get("MULTI_TICKER_MACHINE_LABEL")

    final_bundle_name = bundle_name or f"multi_ticker_migration_{_utc_timestamp_slug()}"
    bundle_dir = output_root / final_bundle_name
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    payload_dir = bundle_dir / "payload"
    payload_dir.mkdir(parents=True, exist_ok=True)

    included_env = False
    env_path = repo_root / ENV_RELATIVE_PATH
    if include_env and env_path.exists():
        _copy_file(env_path, payload_dir / ENV_RELATIVE_PATH)
        included_env = True

    included_state_dir = False
    state_dir = repo_root / STATE_RELATIVE_PATH
    if state_dir.exists():
        _copy_tree(state_dir, payload_dir / STATE_RELATIVE_PATH)
        included_state_dir = True

    included_run_dir = False
    if run_relative_path is not None and (repo_root / run_relative_path).exists():
        _copy_tree(repo_root / run_relative_path, payload_dir / run_relative_path)
        included_run_dir = True

    included_health_file = False
    latest_health_relative = HEALTH_RELATIVE_PATH / LATEST_HEALTH_FILENAME
    latest_health_path = repo_root / latest_health_relative
    if latest_health_path.exists():
        _copy_file(latest_health_path, payload_dir / latest_health_relative)
        included_health_file = True

    repo_branch = _git_value(repo_root, "rev-parse", "--abbrev-ref", "HEAD")
    repo_commit = _git_value(repo_root, "rev-parse", "HEAD")

    notes = _build_restore_notes(
        trade_date=trade_date,
        lease_path=lease_path,
        machine_label=machine_label,
        repo_branch=repo_branch,
        repo_commit=repo_commit,
    )

    manifest = MigrationBundleManifest(
        created_at_utc=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        bundle_name=final_bundle_name,
        trade_date=trade_date,
        repo_branch=repo_branch,
        repo_commit=repo_commit,
        session_file=(
            str(session_file.relative_to(repo_root)) if session_file is not None else None
        ),
        run_directory=str(run_relative_path) if run_relative_path is not None else None,
        latest_health_file=str(latest_health_relative) if latest_health_path.exists() else None,
        ownership_lease_path=lease_path,
        source_machine_label=machine_label,
        included_env_file=included_env,
        included_state_dir=included_state_dir,
        included_run_dir=included_run_dir,
        included_health_file=included_health_file,
        notes=notes,
    )

    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), indent=2),
        encoding="utf-8",
    )

    instructions_path = bundle_dir / "RESTORE_INSTRUCTIONS.md"
    instructions_path.write_text(
        "\n".join(
            [
                "# Runtime Migration Bundle",
                "",
                "Use this bundle on the destination machine after cloning the repo.",
                "",
                "1. Check out the same branch/commit as the source machine.",
                "2. Run the restore script against this bundle.",
                "3. Override the machine label on the destination machine.",
                "4. Run the standby failover check.",
                "5. Start the trader only after the failover check passes.",
                "",
                "Suggested commands:",
                "",
                "```powershell",
                (
                    f"python scripts\\restore_multi_ticker_migration_bundle.py "
                    f"\"{bundle_dir}\" --target-repo \"{repo_root}\" --machine-label <new-machine-label>"
                ),
                "python scripts\\run_multi_ticker_standby_failover_check.py",
                "```",
                "",
                "Notes:",
                *[f"- {note}" for note in notes],
            ]
        ),
        encoding="utf-8",
    )

    zip_path: Path | None = None
    if zip_bundle:
        zip_path = bundle_dir.with_suffix(".zip")
        if zip_path.exists():
            zip_path.unlink()
        with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
            for path in bundle_dir.rglob("*"):
                archive.write(path, path.relative_to(bundle_dir.parent))

    return {
        "bundle_dir": str(bundle_dir),
        "zip_path": str(zip_path) if zip_path is not None else None,
        "manifest": manifest.to_dict(),
    }


def restore_runtime_migration_bundle(
    *,
    bundle_path: Path,
    target_repo_root: Path,
    machine_label: str | None = None,
    lease_path_override: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    bundle_path = bundle_path.resolve()
    target_repo_root = target_repo_root.resolve()
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Bundle manifest not found: {manifest_path}")

    manifest = _read_json(manifest_path)
    payload_dir = bundle_path / "payload"
    if not payload_dir.exists():
        raise FileNotFoundError(f"Bundle payload directory not found: {payload_dir}")

    restored: list[str] = []
    for source in payload_dir.rglob("*"):
        if source.is_dir():
            continue
        relative_path = source.relative_to(payload_dir)
        destination = target_repo_root / relative_path
        if destination.exists():
            if not force:
                raise FileExistsError(
                    f"Destination file already exists: {destination}. Pass force=True to overwrite it."
                )
            destination.unlink()
        _copy_file(source, destination)
        restored.append(str(relative_path))

    env_path = target_repo_root / ENV_RELATIVE_PATH
    if env_path.exists() and (machine_label or lease_path_override):
        env_lines = env_path.read_text(encoding="utf-8").splitlines()

        def upsert(name: str, value: str) -> None:
            prefix = f"{name}="
            for index, line in enumerate(env_lines):
                if line.startswith(prefix):
                    env_lines[index] = f"{prefix}{value}"
                    break
            else:
                env_lines.append(f"{prefix}{value}")

        if machine_label:
            upsert("MULTI_TICKER_MACHINE_LABEL", machine_label)
        if lease_path_override:
            upsert("MULTI_TICKER_OWNERSHIP_LEASE_PATH", lease_path_override)
            upsert("MULTI_TICKER_OWNERSHIP_ENABLED", "true")
        env_path.write_text("\n".join(env_lines) + "\n", encoding="utf-8")

    return {
        "bundle_path": str(bundle_path),
        "target_repo_root": str(target_repo_root),
        "restored_files": restored,
        "manifest": manifest,
    }

from __future__ import annotations

import json
from pathlib import Path

from alpaca_lab.execution.migration import (
    ENV_RELATIVE_PATH,
    HEALTH_RELATIVE_PATH,
    LATEST_HEALTH_FILENAME,
    RUNS_RELATIVE_PATH,
    STATE_RELATIVE_PATH,
    create_runtime_migration_bundle,
    restore_runtime_migration_bundle,
)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_create_runtime_migration_bundle_includes_expected_runtime_files(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _write_text(repo_root / ENV_RELATIVE_PATH, "MULTI_TICKER_MACHINE_LABEL=source-box\n")
    _write_text(
        repo_root / STATE_RELATIVE_PATH / "session_2026-04-17.json",
        json.dumps({"trade_date": "2026-04-17"}),
    )
    _write_text(
        repo_root / RUNS_RELATIVE_PATH / "2026-04-17" / "order_journal.json",
        json.dumps({"orders": []}),
    )
    _write_text(
        repo_root / HEALTH_RELATIVE_PATH / LATEST_HEALTH_FILENAME,
        json.dumps({"status": "ok"}),
    )
    git_dir = repo_root / ".git"
    git_dir.mkdir()

    output_root = repo_root / "reports" / "multi_ticker_portfolio" / "migration_bundles"
    result = create_runtime_migration_bundle(
        repo_root=repo_root,
        output_root=output_root,
        bundle_name="bundle",
    )

    bundle_dir = Path(result["bundle_dir"])
    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "payload" / ENV_RELATIVE_PATH).exists()
    assert (bundle_dir / "payload" / STATE_RELATIVE_PATH / "session_2026-04-17.json").exists()
    assert (bundle_dir / "payload" / RUNS_RELATIVE_PATH / "2026-04-17" / "order_journal.json").exists()
    assert (bundle_dir / "payload" / HEALTH_RELATIVE_PATH / LATEST_HEALTH_FILENAME).exists()
    assert Path(result["zip_path"]).exists()
    assert result["manifest"]["trade_date"] == "2026-04-17"


def test_restore_runtime_migration_bundle_overrides_env_values(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _write_text(repo_root / ENV_RELATIVE_PATH, "MULTI_TICKER_MACHINE_LABEL=source-box\n")
    _write_text(
        repo_root / STATE_RELATIVE_PATH / "session_2026-04-17.json",
        json.dumps({"trade_date": "2026-04-17"}),
    )

    output_root = repo_root / "reports" / "multi_ticker_portfolio" / "migration_bundles"
    result = create_runtime_migration_bundle(
        repo_root=repo_root,
        output_root=output_root,
        bundle_name="bundle",
        zip_bundle=False,
    )

    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()
    restored = restore_runtime_migration_bundle(
        bundle_path=Path(result["bundle_dir"]),
        target_repo_root=target_repo,
        machine_label="standby-box",
        lease_path_override=r"C:\Shared\leases\multi_ticker_portfolio.json",
    )

    restored_env = (target_repo / ENV_RELATIVE_PATH).read_text(encoding="utf-8")
    assert "MULTI_TICKER_MACHINE_LABEL=standby-box" in restored_env
    assert r"MULTI_TICKER_OWNERSHIP_LEASE_PATH=C:\Shared\leases\multi_ticker_portfolio.json" in restored_env
    assert "MULTI_TICKER_OWNERSHIP_ENABLED=true" in restored_env
    assert (target_repo / STATE_RELATIVE_PATH / "session_2026-04-17.json").exists()
    assert restored["manifest"]["bundle_name"] == "bundle"

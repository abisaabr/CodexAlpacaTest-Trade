from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

EXCLUDED_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
}
EXCLUDED_SUFFIXES = {
    ".log",
    ".err",
    ".pyc",
    ".pyo",
}
STORED_SUFFIXES = {
    ".zip",
    ".parquet",
    ".gz",
    ".feather",
}


def _utc_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _iter_workspace_files(workspace_root: Path) -> list[Path]:
    files: list[Path] = []
    for path in workspace_root.rglob("*"):
        if path.is_dir():
            if path.name in EXCLUDED_NAMES:
                continue
            continue
        if any(part in EXCLUDED_NAMES for part in path.parts):
            continue
        if path.suffix.lower() in EXCLUDED_SUFFIXES:
            continue
        files.append(path)
    return sorted(files)


def _archive_candidates(archive_root: Path, workspace_name: str) -> list[str]:
    if not archive_root.exists():
        return []
    prefixes = (
        workspace_name,
        "qqq_options_30d_cleanroom",
        "qqq_alpaca_atm_0_7dte_backfill",
        "qqq_direct_greeks",
        "alpaca-stock-strategy-research",
    )
    names = []
    for path in sorted(archive_root.glob("*.zip")):
        lower_name = path.name.lower()
        if any(prefix.lower() in lower_name for prefix in prefixes):
            names.append(path.name)
    return names


def _write_restore_script(bundle_dir: Path, workspace_zip_name: str, workspace_name: str) -> None:
    script = f"""param(
    [string]$TargetParent = (Split-Path -Parent $PSScriptRoot),
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$zipPath = Join-Path $PSScriptRoot "{workspace_zip_name}"
$destination = Join-Path $TargetParent "{workspace_name}"
$repoSibling = Join-Path $TargetParent "codexalpaca_repo"

if (-not (Test-Path $zipPath)) {{
    throw "Missing workspace archive at $zipPath"
}}

if ((Test-Path $destination) -and -not $Force) {{
    throw "Destination already exists at $destination. Rerun with -Force to overwrite."
}}

if (Test-Path $destination) {{
    Remove-Item -LiteralPath $destination -Recurse -Force
}}

Expand-Archive -LiteralPath $zipPath -DestinationPath $TargetParent -Force

Write-Host "Restored research workspace to $destination"
if (-not (Test-Path $repoSibling)) {{
    Write-Warning "Expected sibling repo missing at $repoSibling. The cleanroom research scripts assume qqq_options_30d_cleanroom and codexalpaca_repo share the same parent folder."
}} else {{
    Write-Host "Detected sibling repo at $repoSibling"
}}

Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Verify codexalpaca_repo exists beside the restored workspace."
Write-Host "  2. Use the repo virtualenv when running the cleanroom scripts."
Write-Host "  3. Example:"
Write-Host "     cd $repoSibling"
Write-Host "     .\\.venv\\Scripts\\python.exe ..\\{workspace_name}\\research_candidate_ticker_batch.py --tickers aapl,amzn"
"""
    (bundle_dir / "RESTORE_RESEARCH_WORKSPACE.ps1").write_text(script, encoding="utf-8")


@dataclass(slots=True)
class ResearchBundleManifest:
    created_at_utc: str
    bundle_name: str
    workspace_name: str
    source_workspace: str
    workspace_zip_name: str
    sibling_repo_required: str
    included_file_count: int
    related_archives_detected: list[str]
    notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def create_cleanroom_research_bundle(
    *,
    workspace_root: Path,
    output_root: Path,
    archive_root: Path | None = None,
    bundle_name: str | None = None,
) -> dict[str, Any]:
    workspace_root = workspace_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    if not workspace_root.exists():
        raise FileNotFoundError(f"Workspace not found: {workspace_root}")

    final_bundle_name = bundle_name or f"cleanroom_research_handoff_{_utc_slug()}"
    bundle_dir = output_root / final_bundle_name
    if bundle_dir.exists():
        raise FileExistsError(f"Bundle directory already exists: {bundle_dir}")
    bundle_dir.mkdir(parents=True, exist_ok=False)

    workspace_name = workspace_root.name
    workspace_zip_name = f"{workspace_name}_snapshot.zip"
    workspace_zip_path = bundle_dir / workspace_zip_name

    files = _iter_workspace_files(workspace_root)
    with ZipFile(workspace_zip_path, "w", compression=ZIP_DEFLATED, allowZip64=True) as archive:
        for path in files:
            relative = path.relative_to(workspace_root.parent)
            compress_type = ZIP_STORED if path.suffix.lower() in STORED_SUFFIXES else ZIP_DEFLATED
            archive.write(path, arcname=str(relative), compress_type=compress_type)

    related_archives = _archive_candidates(archive_root.resolve(), workspace_name) if archive_root else []
    notes = [
        "Restore this workspace into the same parent folder as codexalpaca_repo.",
        "Use the codexalpaca_repo virtualenv when running the cleanroom research scripts.",
        "The current cleanroom research scripts assume qqq_options_30d_cleanroom and codexalpaca_repo are sibling folders.",
    ]
    if related_archives:
        notes.append(
            "Optional archive backups detected in the source archive folder. Copy them too if you want raw backup parity."
        )

    manifest = ResearchBundleManifest(
        created_at_utc=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        bundle_name=final_bundle_name,
        workspace_name=workspace_name,
        source_workspace=str(workspace_root),
        workspace_zip_name=workspace_zip_name,
        sibling_repo_required="codexalpaca_repo",
        included_file_count=len(files),
        related_archives_detected=related_archives,
        notes=notes,
    )
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")
    (bundle_dir / "README.txt").write_text(
        "\n".join(
            [
                "Cleanroom Research Bundle",
                "",
                f"Workspace: {workspace_name}",
                "Restore it into the same parent folder as codexalpaca_repo.",
                "Then run RESTORE_RESEARCH_WORKSPACE.ps1 from this bundle.",
                "",
                "Example:",
                r'  powershell -ExecutionPolicy Bypass -File .\RESTORE_RESEARCH_WORKSPACE.ps1 -TargetParent "C:\Users\<you>\Downloads"',
            ]
        ),
        encoding="utf-8",
    )
    _write_restore_script(bundle_dir, workspace_zip_name, workspace_name)

    outer_zip = bundle_dir.with_suffix(".zip")
    with ZipFile(outer_zip, "w", compression=ZIP_DEFLATED, allowZip64=True) as archive:
        for path in bundle_dir.rglob("*"):
            archive.write(path, arcname=str(path.relative_to(bundle_dir.parent)))

    return {
        "bundle_dir": str(bundle_dir),
        "zip_path": str(outer_zip),
        "manifest": manifest.to_dict(),
    }

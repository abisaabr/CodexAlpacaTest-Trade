from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

from alpaca_lab.research_bundle import create_cleanroom_research_bundle


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_create_cleanroom_research_bundle_excludes_logs_and_writes_restore_script(tmp_path: Path) -> None:
    workspace = tmp_path / "qqq_options_30d_cleanroom"
    _write(workspace / "run_symbol_batch_365.py", "print('ok')\n")
    _write(workspace / "output" / "sample.parquet", "parquet-placeholder")
    _write(workspace / "debug.log", "ignore me")
    _write(workspace / "__pycache__" / "junk.pyc", "ignore me too")

    archives = tmp_path / "repo_archives"
    archives.mkdir()
    (archives / "qqq_options_30d_cleanroom.zip").write_text("zip-placeholder", encoding="utf-8")

    result = create_cleanroom_research_bundle(
        workspace_root=workspace,
        output_root=tmp_path / "out",
        archive_root=archives,
        bundle_name="bundle",
    )

    bundle_dir = Path(result["bundle_dir"])
    manifest = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["workspace_name"] == "qqq_options_30d_cleanroom"
    assert manifest["related_archives_detected"] == ["qqq_options_30d_cleanroom.zip"]
    assert (bundle_dir / "RESTORE_RESEARCH_WORKSPACE.ps1").exists()
    assert Path(result["zip_path"]).exists()

    with ZipFile(bundle_dir / "qqq_options_30d_cleanroom_snapshot.zip") as archive:
        names = set(archive.namelist())
    assert "qqq_options_30d_cleanroom/run_symbol_batch_365.py" in names
    assert "qqq_options_30d_cleanroom/output/sample.parquet" in names
    assert "qqq_options_30d_cleanroom/debug.log" not in names
    assert "qqq_options_30d_cleanroom/__pycache__/junk.pyc" not in names

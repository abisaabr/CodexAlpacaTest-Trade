from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_summary_bundle(
    root: str | Path,
    *,
    name: str,
    summary: dict[str, Any],
    table_map: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Path]:
    output_root = Path(root)
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / f"{name}.json"
    md_path = output_root / f"{name}.md"
    json_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    lines = [f"# {name}", ""]
    for key, value in summary.items():
        lines.append(f"- {key}: {value}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    paths = {"json": json_path, "markdown": md_path}
    for table_name, frame in (table_map or {}).items():
        csv_path = output_root / f"{name}_{table_name}.csv"
        frame.to_csv(csv_path, index=False)
        paths[table_name] = csv_path
    return paths

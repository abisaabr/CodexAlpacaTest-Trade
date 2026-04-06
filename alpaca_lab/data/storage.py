from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def timestamp_slug(moment: datetime | None = None) -> str:
    current = moment or datetime.now(timezone.utc)
    return current.strftime("%Y%m%d_%H%M%S")


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def write_json(path: Path, payload: Any) -> Path:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def write_text(path: Path, text: str) -> Path:
    ensure_directory(path.parent)
    path.write_text(text, encoding="utf-8")
    return path


def write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    ensure_directory(path.parent)
    frame.to_parquet(path, index=False)
    return path


def latest_file(root: Path, pattern: str) -> Path | None:
    if not root.exists():
        return None
    candidates = sorted(root.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None

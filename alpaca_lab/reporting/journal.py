from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def append_journal_entry(path: str | Path, entry: dict[str, Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        payload = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            payload = [payload]
    else:
        payload = []
    payload.append(entry)
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target

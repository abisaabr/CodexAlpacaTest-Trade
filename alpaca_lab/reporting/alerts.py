from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_alert_queue(path: str | Path, alerts: list[dict[str, Any]]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(alerts, indent=2), encoding="utf-8")
    return target

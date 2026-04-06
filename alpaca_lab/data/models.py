from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class IngestionMetadata(BaseModel):
    dataset_name: str
    row_count: int
    request_params: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    extra_counts: dict[str, int] = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    restart_safe: bool = True
    skipped: bool = False

    @classmethod
    def from_paths(
        cls,
        *,
        dataset_name: str,
        row_count: int,
        request_params: dict[str, Any],
        artifacts: dict[str, Path],
        extra_counts: dict[str, int] | None = None,
        restart_safe: bool = True,
        skipped: bool = False,
    ) -> "IngestionMetadata":
        return cls(
            dataset_name=dataset_name,
            row_count=row_count,
            request_params=request_params,
            artifacts={name: str(path) for name, path in artifacts.items()},
            extra_counts=extra_counts or {},
            restart_safe=restart_safe,
            skipped=skipped,
        )

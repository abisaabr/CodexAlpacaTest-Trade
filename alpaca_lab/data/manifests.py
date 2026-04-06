from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from alpaca_lab.data.storage import ensure_directory


@dataclass(slots=True)
class ChunkArtifact:
    dataset: str
    chunk_id: str
    status: str
    attempts: int
    row_count: int
    artifacts: dict[str, str]
    quality: list[dict[str, Any]]
    warnings: list[str]
    error: str | None = None


class BuildManifestStore:
    def __init__(self, path: Path, *, request_payload: dict[str, Any]) -> None:
        self.path = path
        self.request_payload = request_payload
        ensure_directory(path.parent)
        if not self.path.exists():
            self._write(
                {
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "request": request_payload,
                    "datasets": {},
                    "summaries": {},
                }
            )

    def _read(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, payload: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    def chunk_record(self, dataset: str, chunk_id: str) -> dict[str, Any] | None:
        payload = self._read()
        return payload.get("datasets", {}).get(dataset, {}).get("chunks", {}).get(chunk_id)

    def is_completed(self, dataset: str, chunk_id: str) -> bool:
        record = self.chunk_record(dataset, chunk_id)
        if record is None:
            return False
        if record.get("status") != "completed":
            return False
        artifacts = record.get("artifacts", {})
        return all(Path(path).exists() for path in artifacts.values())

    def start_chunk(self, dataset: str, chunk_id: str, *, metadata: dict[str, Any]) -> None:
        payload = self._read()
        dataset_bucket = payload.setdefault("datasets", {}).setdefault(dataset, {"chunks": {}})
        record = dataset_bucket["chunks"].get(chunk_id, {})
        attempts = int(record.get("attempts", 0)) + 1
        dataset_bucket["chunks"][chunk_id] = {
            **record,
            "status": "running",
            "attempts": attempts,
            "metadata": metadata,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "error": None,
        }
        self._write(payload)

    def complete_chunk(
        self,
        dataset: str,
        chunk_id: str,
        *,
        row_count: int,
        artifacts: dict[str, Path],
        quality: list[dict[str, Any]] | None = None,
        warnings: list[str] | None = None,
    ) -> None:
        payload = self._read()
        dataset_bucket = payload.setdefault("datasets", {}).setdefault(dataset, {"chunks": {}})
        record = dataset_bucket["chunks"].setdefault(chunk_id, {})
        record.update(
            {
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "row_count": row_count,
                "artifacts": {name: str(path) for name, path in artifacts.items()},
                "quality": quality or [],
                "warnings": warnings or [],
                "error": None,
            }
        )
        self._write(payload)

    def fail_chunk(self, dataset: str, chunk_id: str, *, error: str) -> None:
        payload = self._read()
        dataset_bucket = payload.setdefault("datasets", {}).setdefault(dataset, {"chunks": {}})
        record = dataset_bucket["chunks"].setdefault(chunk_id, {})
        record.update(
            {
                "status": "failed",
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "error": error,
            }
        )
        self._write(payload)

    def write_summary(self, name: str, payload_fragment: dict[str, Any]) -> None:
        payload = self._read()
        payload.setdefault("summaries", {})[name] = payload_fragment
        self._write(payload)

    def all_chunks_frame(self) -> pd.DataFrame:
        payload = self._read()
        rows: list[dict[str, Any]] = []
        for dataset, dataset_payload in payload.get("datasets", {}).items():
            for chunk_id, record in dataset_payload.get("chunks", {}).items():
                rows.append(
                    {
                        "dataset": dataset,
                        "chunk_id": chunk_id,
                        "status": record.get("status"),
                        "attempts": record.get("attempts", 0),
                        "row_count": record.get("row_count", 0),
                        "error": record.get("error"),
                    }
                )
        return pd.DataFrame(
            rows,
            columns=["dataset", "chunk_id", "status", "attempts", "row_count", "error"],
        )

    def quality_frame(self) -> pd.DataFrame:
        payload = self._read()
        rows: list[dict[str, Any]] = []
        for dataset, dataset_payload in payload.get("datasets", {}).items():
            for chunk_id, record in dataset_payload.get("chunks", {}).items():
                for quality_row in record.get("quality", []):
                    rows.append({"dataset": dataset, "chunk_id": chunk_id, **quality_row})
        return pd.DataFrame(
            rows,
            columns=[
                "dataset",
                "chunk_id",
                "underlying_symbol",
                "symbol",
                "trade_date",
                "row_count",
                "duplicate_rows",
                "missing_intervals",
                "empty_response",
                "schema_missing_columns",
                "schema_extra_columns",
            ],
        )

    def failed_chunks_frame(self) -> pd.DataFrame:
        frame = self.all_chunks_frame()
        if frame.empty:
            return frame
        return frame[frame["status"] == "failed"].reset_index(drop=True)

    def retry_summary(self) -> dict[str, Any]:
        frame = self.all_chunks_frame()
        if frame.empty:
            return {"total_chunks": 0, "retried_chunks": 0, "failed_chunks": 0}
        return {
            "total_chunks": int(len(frame)),
            "retried_chunks": int((frame["attempts"] > 1).sum()),
            "failed_chunks": int((frame["status"] == "failed").sum()),
        }

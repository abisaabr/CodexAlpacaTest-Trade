from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import pandas as pd


@dataclass(slots=True)
class PromotionCandidate:
    symbol: str
    asset_class: Literal["stock", "option"]
    side: Literal["buy", "sell"] = "buy"
    strategy_name: str = "unknown"
    thesis: str = ""
    score: float = 0.0
    price: float | None = None
    limit_price: float | None = None
    qty: float | None = None
    notional: float | None = None
    underlying_symbol: str | None = None
    contract_multiplier: float = 1.0
    tags: tuple[str, ...] = field(default_factory=tuple)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["tags"] = list(self.tags)
        return record


def load_promotion_board(path: str | Path) -> list[PromotionCandidate]:
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"Promotion board not found: {source}")
    if source.suffix.lower() == ".json":
        payload = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Promotion board JSON must contain a list of candidate objects.")
        return [PromotionCandidate(**record) for record in payload]
    if source.suffix.lower() in {".csv", ".tsv"}:
        separator = "\t" if source.suffix.lower() == ".tsv" else ","
        frame = pd.read_csv(source, sep=separator)
        return [PromotionCandidate(**record) for record in frame.to_dict(orient="records")]
    raise ValueError("Promotion board must be a .json, .csv, or .tsv file.")


def save_promotion_board(candidates: list[PromotionCandidate], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    records = [candidate.to_record() for candidate in candidates]
    if target.suffix.lower() == ".json":
        target.write_text(json.dumps(records, indent=2), encoding="utf-8")
        return target
    frame = pd.DataFrame(records)
    if target.suffix.lower() == ".tsv":
        frame.to_csv(target, sep="\t", index=False)
    else:
        frame.to_csv(target, index=False)
    return target

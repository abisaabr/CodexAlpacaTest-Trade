from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import LabSettings
from alpaca_lab.data.models import IngestionMetadata
from alpaca_lab.data.normalization import normalize_option_bars, normalize_option_contracts, normalize_stock_bars
from alpaca_lab.data.storage import ensure_directory, slugify, timestamp_slug, write_json, write_parquet, write_text
from alpaca_lab.logging_utils import get_logger


def _coerce_symbols(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(item.strip().upper() for item in value.split(",") if item.strip())
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item).strip().upper() for item in value if str(item).strip())
    raise TypeError("Expected a symbol sequence or comma-separated string.")


class StockBarIngestionRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    symbols: tuple[str, ...]
    start: datetime
    end: datetime
    timeframe: str = "1Min"
    feed: str | None = None
    overwrite: bool = False

    @field_validator("symbols", mode="before")
    @classmethod
    def parse_symbols(cls, value: Any) -> tuple[str, ...]:
        return _coerce_symbols(value)

    @model_validator(mode="after")
    def validate_window(self) -> "StockBarIngestionRequest":
        if self.end <= self.start:
            raise ValueError("end must be after start")
        return self


class OptionsIngestionRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    underlyings: tuple[str, ...]
    min_dte: int = 7
    max_dte: int = 30
    strike_distance_pct: float = 0.05
    option_type: Literal["call", "put", "any"] = "call"
    include_bars: bool = True
    start: datetime | None = None
    end: datetime | None = None
    timeframe: str = "1Min"
    overwrite: bool = False
    as_of: date | None = None

    @field_validator("underlyings", mode="before")
    @classmethod
    def parse_underlyings(cls, value: Any) -> tuple[str, ...]:
        return _coerce_symbols(value)

    @model_validator(mode="after")
    def validate_parameters(self) -> "OptionsIngestionRequest":
        if self.min_dte < 0 or self.max_dte < self.min_dte:
            raise ValueError("DTE window must be non-negative and ordered.")
        if not 0 < self.strike_distance_pct <= 1:
            raise ValueError("strike_distance_pct must be between 0 and 1.")
        if self.include_bars and (self.start is None or self.end is None):
            raise ValueError("start and end are required when include_bars=true.")
        if self.start is not None and self.end is not None and self.end <= self.start:
            raise ValueError("end must be after start.")
        return self


class DataIngestionService:
    def __init__(self, settings: LabSettings, broker: AlpacaBrokerAdapter) -> None:
        self.settings = settings
        self.broker = broker
        self.logger = get_logger("ingestion")

    def _report_path(self, name: str) -> Path:
        return self.settings.reports_root / f"{name}_{timestamp_slug()}.md"

    def _manifest_path(self, name: str) -> Path:
        return self.settings.raw_data_dir / "manifests" / f"{name}.json"

    def ingest_stock_bars(self, request: StockBarIngestionRequest) -> IngestionMetadata:
        dataset_name = slugify(
            f"stock-bars-{'-'.join(request.symbols)}-{request.start:%Y%m%d}-{request.end:%Y%m%d}-{request.timeframe}"
        )
        raw_path = self.settings.raw_data_dir / "stocks" / f"{dataset_name}.json"
        bronze_path = self.settings.bronze_data_dir / "stocks" / f"{dataset_name}.parquet"
        silver_path = self.settings.silver_data_dir / "stocks" / f"{dataset_name}.parquet"
        report_path = self._report_path(dataset_name)
        ensure_directory(raw_path.parent)

        if silver_path.exists() and not request.overwrite:
            frame = pd.read_parquet(silver_path)
            metadata = IngestionMetadata.from_paths(
                dataset_name=dataset_name,
                row_count=int(len(frame)),
                request_params=request.model_dump(mode="json"),
                artifacts={"silver": silver_path, "report": report_path, "raw": raw_path, "bronze": bronze_path},
                restart_safe=True,
                skipped=True,
            )
            write_json(self._manifest_path(dataset_name), metadata.model_dump(mode="json"))
            write_text(report_path, f"# {dataset_name}\n\nSkipped existing dataset at `{silver_path}`.\n")
            return metadata

        payload = self.broker.get_stock_bars(
            list(request.symbols),
            start=request.start,
            end=request.end,
            timeframe=request.timeframe,
            feed=request.feed,
        )
        bronze_frame = normalize_stock_bars(payload)
        silver_frame = bronze_frame.drop_duplicates(subset=["symbol", "timestamp"]).reset_index(drop=True)

        write_json(raw_path, payload)
        write_parquet(bronze_path, bronze_frame)
        write_parquet(silver_path, silver_frame)

        metadata = IngestionMetadata.from_paths(
            dataset_name=dataset_name,
            row_count=int(len(silver_frame)),
            request_params=request.model_dump(mode="json"),
            artifacts={"raw": raw_path, "bronze": bronze_path, "silver": silver_path, "report": report_path},
            extra_counts={"symbols": len(request.symbols)},
            restart_safe=True,
        )
        write_json(self._manifest_path(dataset_name), metadata.model_dump(mode="json"))
        report = (
            f"# {dataset_name}\n\n"
            f"- Rows: {len(silver_frame)}\n"
            f"- Symbols: {', '.join(request.symbols)}\n"
            f"- Raw payload: `{raw_path}`\n"
            f"- Bronze parquet: `{bronze_path}`\n"
            f"- Silver parquet: `{silver_path}`\n"
        )
        write_text(report_path, report)
        return metadata

    def ingest_options_data(self, request: OptionsIngestionRequest) -> IngestionMetadata:
        anchor_date = request.as_of or datetime.now(timezone.utc).date()
        expiry_start = anchor_date + timedelta(days=request.min_dte)
        expiry_end = anchor_date + timedelta(days=request.max_dte)
        dataset_name = slugify(
            f"options-{'-'.join(request.underlyings)}-{expiry_start.isoformat()}-{expiry_end.isoformat()}-{request.option_type}"
        )
        raw_path = self.settings.raw_data_dir / "options" / f"{dataset_name}.json"
        bronze_contracts_path = self.settings.bronze_data_dir / "options" / f"{dataset_name}_contracts.parquet"
        silver_contracts_path = self.settings.silver_data_dir / "options" / f"{dataset_name}_contracts.parquet"
        silver_bars_path = self.settings.silver_data_dir / "options" / f"{dataset_name}_bars.parquet"
        report_path = self._report_path(dataset_name)
        ensure_directory(raw_path.parent)

        latest_stock_bars = self.broker.get_stock_latest_bars(list(request.underlyings), feed=self.settings.alpaca_data_feed)
        reference_prices = {
            symbol: payload.get("c")
            for symbol, payload in latest_stock_bars.get("bars", {}).items()
            if isinstance(payload, dict)
        }
        contracts_payload = self.broker.get_option_contracts(
            list(request.underlyings),
            expiration_date_gte=expiry_start.isoformat(),
            expiration_date_lte=expiry_end.isoformat(),
            option_type=request.option_type,
        )
        contracts_frame = normalize_option_contracts(contracts_payload)
        if not contracts_frame.empty:
            contracts_frame["reference_price"] = contracts_frame["underlying_symbol"].map(reference_prices)
            contracts_frame["strike_distance_pct"] = (
                (contracts_frame["strike_price"] - contracts_frame["reference_price"]).abs()
                / contracts_frame["reference_price"]
            )
            contracts_frame = contracts_frame[
                contracts_frame["strike_distance_pct"].fillna(1.0) <= request.strike_distance_pct
            ].reset_index(drop=True)

        bar_payload: dict[str, Any] = {"bars": {}}
        bars_frame = pd.DataFrame()
        if request.include_bars and not contracts_frame.empty:
            option_symbols = contracts_frame["symbol"].dropna().astype(str).tolist()
            bar_payload = self.broker.get_option_bars(
                option_symbols,
                start=request.start,
                end=request.end,
                timeframe=request.timeframe,
            )
            bars_frame = normalize_option_bars(bar_payload)

        combined_payload = {
            "reference_prices": reference_prices,
            "contracts": contracts_payload,
            "bars": bar_payload,
        }
        write_json(raw_path, combined_payload)
        write_parquet(bronze_contracts_path, contracts_frame)
        write_parquet(silver_contracts_path, contracts_frame)
        if not bars_frame.empty:
            write_parquet(silver_bars_path, bars_frame)

        metadata = IngestionMetadata.from_paths(
            dataset_name=dataset_name,
            row_count=int(len(contracts_frame) + len(bars_frame)),
            request_params=request.model_dump(mode="json"),
            artifacts={
                "raw": raw_path,
                "contracts": silver_contracts_path,
                "bars": silver_bars_path,
                "report": report_path,
            },
            extra_counts={
                "contract_rows": int(len(contracts_frame)),
                "bar_rows": int(len(bars_frame)),
                "underlyings": len(request.underlyings),
            },
            restart_safe=True,
            skipped=False,
        )
        write_json(self._manifest_path(dataset_name), metadata.model_dump(mode="json"))
        report = (
            f"# {dataset_name}\n\n"
            f"- Underlyings: {', '.join(request.underlyings)}\n"
            f"- DTE window: {request.min_dte}-{request.max_dte}\n"
            f"- Strike distance pct: {request.strike_distance_pct:.2%}\n"
            f"- Reference prices: {reference_prices}\n"
            f"- Contract rows: {len(contracts_frame)}\n"
            f"- Option bar rows: {len(bars_frame)}\n"
            f"- Raw payload: `{raw_path}`\n"
            f"- Contracts parquet: `{silver_contracts_path}`\n"
            f"- Bars parquet: `{silver_bars_path}`\n"
        )
        write_text(report_path, report)
        return metadata

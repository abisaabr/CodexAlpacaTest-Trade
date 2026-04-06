from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import LabSettings
from alpaca_lab.data.chunking import batched, iter_date_chunks, iter_dates, market_session_bounds
from alpaca_lab.data.contracts import select_contracts_for_trade_date
from alpaca_lab.data.manifests import BuildManifestStore
from alpaca_lab.data.normalization import (
    normalize_option_bar_chunk,
    normalize_option_contract_inventory,
    normalize_option_latest_quotes,
    normalize_option_snapshots,
    normalize_option_trade_chunk,
    normalize_stock_bar_chunk,
)
from alpaca_lab.data.quality import aggregate_quality_reports, build_quality_rows, selected_contract_report
from alpaca_lab.data.schemas import (
    OPTION_BAR_SCHEMA,
    OPTION_CONTRACT_SCHEMA,
    OPTION_QUOTE_SCHEMA,
    OPTION_SNAPSHOT_SCHEMA,
    OPTION_TRADE_SCHEMA,
    SELECTED_OPTION_SCHEMA,
    STOCK_BAR_SCHEMA,
)
from alpaca_lab.data.storage import ensure_directory, slugify, write_json, write_text
from alpaca_lab.logging_utils import get_logger


def _coerce_symbols(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(item.strip().upper() for item in value.split(",") if item.strip())
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item).strip().upper() for item in value if str(item).strip())
    raise TypeError("Expected a symbol sequence or comma-separated string.")


class HistoricalBuildRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    stock_symbols: tuple[str, ...]
    option_underlyings: tuple[str, ...] | None = None
    start_date: date
    end_date: date
    stock_timeframe: str = "1Min"
    option_timeframe: str = "1Min"
    min_dte: int = 0
    max_dte: int = 14
    strike_steps: int = 3
    option_types: tuple[str, ...] = ("call", "put")
    stock_chunk_days: int = 5
    contract_chunk_days: int = 30
    option_batch_size: int = 25
    include_option_bars: bool = True
    include_option_trades: bool = True
    include_latest_enrichment: bool = True
    reference_window_minutes: int = 5
    feed: str | None = None
    build_name: str | None = None
    contract_status: str | None = None
    overwrite: bool = False

    @field_validator("stock_symbols", "option_underlyings", mode="before")
    @classmethod
    def parse_symbols(cls, value: Any) -> tuple[str, ...] | None:
        if value is None:
            return None
        return _coerce_symbols(value)

    @field_validator("option_types", mode="before")
    @classmethod
    def parse_option_types(cls, value: Any) -> tuple[str, ...]:
        if isinstance(value, str):
            return tuple(item.strip().lower() for item in value.split(",") if item.strip())
        if isinstance(value, (list, tuple, set)):
            return tuple(str(item).strip().lower() for item in value if str(item).strip())
        raise TypeError("option_types must be a sequence or comma-separated string.")

    @model_validator(mode="after")
    def validate_request(self) -> "HistoricalBuildRequest":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date.")
        if self.option_underlyings is None:
            self.option_underlyings = self.stock_symbols
        if self.min_dte < 0 or self.max_dte < self.min_dte:
            raise ValueError("DTE window must be non-negative and ordered.")
        if self.strike_steps < 0:
            raise ValueError("strike_steps must be non-negative.")
        if self.stock_chunk_days <= 0 or self.contract_chunk_days <= 0:
            raise ValueError("Chunk sizes must be positive.")
        if self.option_batch_size <= 0:
            raise ValueError("option_batch_size must be positive.")
        if self.reference_window_minutes <= 0:
            raise ValueError("reference_window_minutes must be positive.")
        return self


@dataclass(frozen=True, slots=True)
class BuildLayout:
    build_name: str
    manifest_path: Path
    raw_root: Path
    silver_root: Path
    report_root: Path


@dataclass(frozen=True, slots=True)
class HistoricalBuildResult:
    build_name: str
    manifest_path: Path
    report_root: Path
    silver_root: Path
    raw_root: Path
    quality_report_path: Path
    summary_report_path: Path


class HistoricalDatasetBuilder:
    def __init__(self, settings: LabSettings, broker: AlpacaBrokerAdapter) -> None:
        self.settings = settings
        self.broker = broker
        self.logger = get_logger("historical_builder")

    def build(self, request: HistoricalBuildRequest) -> HistoricalBuildResult:
        layout = self._build_layout(request)
        manifest = BuildManifestStore(layout.manifest_path, request_payload=request.model_dump(mode="json"))

        self._build_stock_bars(request, layout, manifest)
        self._build_option_contract_inventory(request, layout, manifest)
        self._build_selected_contracts(request, layout, manifest)
        if request.include_option_bars:
            self._build_option_market_data(request, layout, manifest, dataset="option_bars")
        if request.include_option_trades:
            self._build_option_market_data(request, layout, manifest, dataset="option_trades")
        if request.include_latest_enrichment:
            self._build_latest_enrichment(request, layout, manifest)

        quality_report_path, summary_report_path = self._write_reports(request, layout, manifest)
        manifest.write_summary(
            "artifacts",
            {
                "quality_report_path": str(quality_report_path),
                "summary_report_path": str(summary_report_path),
                "report_root": str(layout.report_root),
                "silver_root": str(layout.silver_root),
                "raw_root": str(layout.raw_root),
            },
        )
        manifest.write_summary("retry_summary", manifest.retry_summary())

        return HistoricalBuildResult(
            build_name=layout.build_name,
            manifest_path=layout.manifest_path,
            report_root=layout.report_root,
            silver_root=layout.silver_root,
            raw_root=layout.raw_root,
            quality_report_path=quality_report_path,
            summary_report_path=summary_report_path,
        )

    def _build_layout(self, request: HistoricalBuildRequest) -> BuildLayout:
        request_payload = request.model_dump(mode="json")
        digest = hashlib.sha1(json.dumps(request_payload, sort_keys=True).encode("utf-8")).hexdigest()[:8]
        build_name = request.build_name or slugify(
            f"historical-1min-{request.start_date.isoformat()}-{request.end_date.isoformat()}-"
            f"{len(request.stock_symbols)}sym-dte{request.min_dte}-{request.max_dte}-{digest}"
        )
        raw_root = self.settings.raw_data_dir / "historical" / build_name
        silver_root = self.settings.silver_data_dir / "historical" / build_name
        report_root = self.settings.reports_root / build_name
        manifest_path = self.settings.raw_data_dir / "manifests" / f"{build_name}.json"
        ensure_directory(raw_root)
        ensure_directory(silver_root)
        ensure_directory(report_root)
        return BuildLayout(
            build_name=build_name,
            manifest_path=manifest_path,
            raw_root=raw_root,
            silver_root=silver_root,
            report_root=report_root,
        )

    def _stock_chunk_paths(self, layout: BuildLayout, symbol: str, chunk_id: str) -> tuple[Path, Path]:
        raw_path = layout.raw_root / "stock_bars" / f"symbol={symbol}" / f"chunk={chunk_id}" / "payload.json"
        silver_path = (
            layout.silver_root / "stock_bars" / f"symbol={symbol}" / f"chunk={chunk_id}" / "part.parquet"
        )
        return raw_path, silver_path

    def _inventory_chunk_paths(self, layout: BuildLayout, underlying: str, chunk_id: str) -> tuple[Path, Path]:
        raw_path = (
            layout.raw_root
            / "option_contract_inventory"
            / f"underlying={underlying}"
            / f"chunk={chunk_id}"
            / "payload.json"
        )
        silver_path = (
            layout.silver_root
            / "option_contract_inventory"
            / f"underlying={underlying}"
            / f"chunk={chunk_id}"
            / "part.parquet"
        )
        return raw_path, silver_path

    def _selected_paths(self, layout: BuildLayout, underlying: str, trade_date_value: date) -> Path:
        return (
            layout.silver_root
            / "selected_option_contracts"
            / f"underlying={underlying}"
            / f"trade_date={trade_date_value.isoformat()}"
            / "part.parquet"
        )

    def _market_data_paths(
        self,
        layout: BuildLayout,
        dataset: str,
        underlying: str,
        trade_date_value: date,
        batch_index: int,
    ) -> tuple[Path, Path]:
        raw_path = (
            layout.raw_root
            / dataset
            / f"underlying={underlying}"
            / f"trade_date={trade_date_value.isoformat()}"
            / f"batch={batch_index:03d}"
            / "payload.json"
        )
        silver_path = (
            layout.silver_root
            / dataset
            / f"underlying={underlying}"
            / f"trade_date={trade_date_value.isoformat()}"
            / f"batch={batch_index:03d}"
            / "part.parquet"
        )
        return raw_path, silver_path

    def _enrichment_paths(
        self,
        layout: BuildLayout,
        dataset: str,
        collected_at_slug: str,
        batch_index: int,
    ) -> tuple[Path, Path]:
        raw_path = (
            layout.raw_root / dataset / f"collected_at={collected_at_slug}" / f"batch={batch_index:03d}" / "payload.json"
        )
        silver_path = (
            layout.silver_root
            / dataset
            / f"collected_at={collected_at_slug}"
            / f"batch={batch_index:03d}"
            / "part.parquet"
        )
        return raw_path, silver_path

    def _build_stock_bars(
        self,
        request: HistoricalBuildRequest,
        layout: BuildLayout,
        manifest: BuildManifestStore,
    ) -> None:
        for symbol in request.stock_symbols:
            for chunk in iter_date_chunks(request.start_date, request.end_date, chunk_days=request.stock_chunk_days):
                chunk_id = f"{symbol}__{chunk.label}"
                raw_path, silver_path = self._stock_chunk_paths(layout, symbol, chunk_id)
                if not request.overwrite and manifest.is_completed("stock_bars", chunk_id):
                    continue

                session_start, _ = market_session_bounds(chunk.start_date)
                _, session_end = market_session_bounds(chunk.end_date)
                manifest.start_chunk(
                    "stock_bars",
                    chunk_id,
                    metadata={
                        "symbol": symbol,
                        "chunk_start": chunk.start_date.isoformat(),
                        "chunk_end": chunk.end_date.isoformat(),
                    },
                )
                try:
                    payload = self.broker.get_stock_bars(
                        [symbol],
                        start=session_start,
                        end=session_end,
                        timeframe=request.stock_timeframe,
                        feed=request.feed or self.settings.alpaca_data_feed,
                    )
                    frame = normalize_stock_bar_chunk(
                        payload,
                        source_feed=request.feed or self.settings.alpaca_data_feed,
                        chunk_start=session_start,
                        chunk_end=session_end,
                    )
                    quality_rows = build_quality_rows(
                        "stock_bars",
                        frame,
                        STOCK_BAR_SCHEMA,
                        chunk_id,
                        group_columns=("symbol", "trade_date"),
                        check_missing_intervals=True,
                    )
                    warnings = ["empty stock bar response"] if frame.empty else []
                    write_json(raw_path, payload)
                    ensure_directory(silver_path.parent)
                    frame.to_parquet(silver_path, index=False)
                    manifest.complete_chunk(
                        "stock_bars",
                        chunk_id,
                        row_count=int(len(frame)),
                        artifacts={"raw": raw_path, "silver": silver_path},
                        quality=quality_rows,
                        warnings=warnings,
                    )
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("stock chunk failed: %s", chunk_id)
                    manifest.fail_chunk("stock_bars", chunk_id, error=str(exc))

    def _build_option_contract_inventory(
        self,
        request: HistoricalBuildRequest,
        layout: BuildLayout,
        manifest: BuildManifestStore,
    ) -> None:
        inventory_end = request.end_date + timedelta(days=request.max_dte)
        collected_at = datetime.now(timezone.utc)
        for underlying in request.option_underlyings or ():
            for chunk in iter_date_chunks(request.start_date, inventory_end, chunk_days=request.contract_chunk_days):
                chunk_id = f"{underlying}__{chunk.label}"
                raw_path, silver_path = self._inventory_chunk_paths(layout, underlying, chunk_id)
                if not request.overwrite and manifest.is_completed("option_contract_inventory", chunk_id):
                    continue

                manifest.start_chunk(
                    "option_contract_inventory",
                    chunk_id,
                    metadata={
                        "underlying": underlying,
                        "expiration_window_start": chunk.start_date.isoformat(),
                        "expiration_window_end": chunk.end_date.isoformat(),
                        "status": request.contract_status,
                    },
                )
                try:
                    payload = self.broker.get_option_contracts(
                        [underlying],
                        expiration_date_gte=chunk.start_date.isoformat(),
                        expiration_date_lte=chunk.end_date.isoformat(),
                        option_type=None,
                        status=request.contract_status,
                    )
                    frame = normalize_option_contract_inventory(
                        payload,
                        inventory_status=request.contract_status,
                        inventory_collected_at=collected_at,
                        expiration_window_start=chunk.start_date,
                        expiration_window_end=chunk.end_date,
                    )
                    quality_rows = build_quality_rows(
                        "option_contract_inventory",
                        frame,
                        OPTION_CONTRACT_SCHEMA,
                        chunk_id,
                        group_columns=("underlying_symbol",),
                    )
                    warnings = ["empty option contract inventory response"] if frame.empty else []
                    write_json(raw_path, payload)
                    ensure_directory(silver_path.parent)
                    frame.to_parquet(silver_path, index=False)
                    manifest.complete_chunk(
                        "option_contract_inventory",
                        chunk_id,
                        row_count=int(len(frame)),
                        artifacts={"raw": raw_path, "silver": silver_path},
                        quality=quality_rows,
                        warnings=warnings,
                    )
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("option contract inventory chunk failed: %s", chunk_id)
                    manifest.fail_chunk("option_contract_inventory", chunk_id, error=str(exc))

    def _build_selected_contracts(
        self,
        request: HistoricalBuildRequest,
        layout: BuildLayout,
        manifest: BuildManifestStore,
    ) -> None:
        stock_history = self._load_dataset(layout.silver_root / "stock_bars")
        inventory_history = self._load_dataset(layout.silver_root / "option_contract_inventory")
        if stock_history.empty or inventory_history.empty:
            self.logger.warning("Skipping selected contract build because stock or inventory history is empty.")
            return

        stock_history["trade_date"] = (
            pd.to_datetime(stock_history["timestamp"], utc=True)
            .dt.tz_convert("America/New_York")
            .dt.normalize()
            .dt.tz_localize(None)
        )
        inventory_history["expiration_date"] = pd.to_datetime(
            inventory_history["expiration_date"], errors="coerce"
        ).dt.normalize()

        for underlying in request.option_underlyings or ():
            underlying_stock = stock_history[stock_history["symbol"] == underlying].copy()
            underlying_inventory = inventory_history[
                inventory_history["underlying_symbol"] == underlying
            ].copy()

            for trade_date_value in iter_dates(request.start_date, request.end_date):
                chunk_id = f"{underlying}__{trade_date_value.isoformat()}"
                silver_path = self._selected_paths(layout, underlying, trade_date_value)
                if not request.overwrite and manifest.is_completed("selected_option_contracts", chunk_id):
                    continue

                manifest.start_chunk(
                    "selected_option_contracts",
                    chunk_id,
                    metadata={"underlying": underlying, "trade_date": trade_date_value.isoformat()},
                )
                try:
                    day_stock = underlying_stock[
                        underlying_stock["trade_date"] == pd.Timestamp(trade_date_value).normalize()
                    ].copy()
                    selected = select_contracts_for_trade_date(
                        underlying_inventory,
                        day_stock,
                        trade_date=trade_date_value,
                        min_dte=request.min_dte,
                        max_dte=request.max_dte,
                        strike_steps=request.strike_steps,
                        option_types=request.option_types,
                        reference_window_minutes=request.reference_window_minutes,
                    )
                    quality_rows = build_quality_rows(
                        "selected_option_contracts",
                        selected,
                        SELECTED_OPTION_SCHEMA,
                        chunk_id,
                        group_columns=("underlying_symbol", "trade_date"),
                    )
                    warnings = ["no contracts selected for day"] if selected.empty else []
                    ensure_directory(silver_path.parent)
                    selected.to_parquet(silver_path, index=False)
                    manifest.complete_chunk(
                        "selected_option_contracts",
                        chunk_id,
                        row_count=int(len(selected)),
                        artifacts={"silver": silver_path},
                        quality=quality_rows,
                        warnings=warnings,
                    )
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("selected contract chunk failed: %s", chunk_id)
                    manifest.fail_chunk("selected_option_contracts", chunk_id, error=str(exc))

    def _build_option_market_data(
        self,
        request: HistoricalBuildRequest,
        layout: BuildLayout,
        manifest: BuildManifestStore,
        *,
        dataset: str,
    ) -> None:
        if dataset not in {"option_bars", "option_trades"}:
            raise ValueError(f"Unsupported dataset: {dataset}")

        for underlying in request.option_underlyings or ():
            for trade_date_value in iter_dates(request.start_date, request.end_date):
                selected_path = self._selected_paths(layout, underlying, trade_date_value)
                if not selected_path.exists():
                    continue

                selected = pd.read_parquet(selected_path)
                if selected.empty:
                    continue

                underlying_lookup = {
                    row.symbol: row.underlying_symbol
                    for row in selected[["symbol", "underlying_symbol"]].drop_duplicates().itertuples()
                }
                session_start, session_end = market_session_bounds(trade_date_value)
                symbols = sorted(selected["symbol"].dropna().astype(str).unique().tolist())

                for batch_index, symbol_batch in enumerate(batched(symbols, request.option_batch_size)):
                    chunk_id = f"{underlying}__{trade_date_value.isoformat()}__batch{batch_index:03d}"
                    raw_path, silver_path = self._market_data_paths(
                        layout,
                        dataset,
                        underlying,
                        trade_date_value,
                        batch_index,
                    )
                    if not request.overwrite and manifest.is_completed(dataset, chunk_id):
                        continue

                    manifest.start_chunk(
                        dataset,
                        chunk_id,
                        metadata={
                            "underlying": underlying,
                            "trade_date": trade_date_value.isoformat(),
                            "symbols": symbol_batch,
                        },
                    )
                    try:
                        if dataset == "option_bars":
                            payload = self.broker.get_option_bars(
                                symbol_batch,
                                start=session_start,
                                end=session_end,
                                timeframe=request.option_timeframe,
                            )
                            frame = normalize_option_bar_chunk(
                                payload,
                                trade_date=trade_date_value,
                                underlying_lookup=underlying_lookup,
                                chunk_id=chunk_id,
                            )
                            schema = OPTION_BAR_SCHEMA
                            check_missing = True
                        else:
                            payload = self.broker.get_option_trades(
                                symbol_batch,
                                start=session_start,
                                end=session_end,
                            )
                            frame = normalize_option_trade_chunk(
                                payload,
                                trade_date=trade_date_value,
                                underlying_lookup=underlying_lookup,
                                chunk_id=chunk_id,
                            )
                            schema = OPTION_TRADE_SCHEMA
                            check_missing = False

                        quality_rows = build_quality_rows(
                            dataset,
                            frame,
                            schema,
                            chunk_id,
                            group_columns=("underlying_symbol", "symbol", "trade_date"),
                            check_missing_intervals=check_missing,
                        )
                        warnings = [f"empty {dataset} response"] if frame.empty else []
                        write_json(raw_path, payload)
                        ensure_directory(silver_path.parent)
                        frame.to_parquet(silver_path, index=False)
                        manifest.complete_chunk(
                            dataset,
                            chunk_id,
                            row_count=int(len(frame)),
                            artifacts={"raw": raw_path, "silver": silver_path},
                            quality=quality_rows,
                            warnings=warnings,
                        )
                    except Exception as exc:  # noqa: BLE001
                        self.logger.exception("%s chunk failed: %s", dataset, chunk_id)
                        manifest.fail_chunk(dataset, chunk_id, error=str(exc))

    def _build_latest_enrichment(
        self,
        request: HistoricalBuildRequest,
        layout: BuildLayout,
        manifest: BuildManifestStore,
    ) -> None:
        selected_history = self._load_dataset(layout.silver_root / "selected_option_contracts")
        if selected_history.empty:
            return

        today = datetime.now(timezone.utc).date()
        selected_history["expiration_date"] = pd.to_datetime(
            selected_history["expiration_date"], errors="coerce"
        ).dt.date
        active_selected = selected_history[selected_history["expiration_date"] >= today].copy()
        if active_selected.empty:
            manifest.write_summary(
                "latest_enrichment",
                {
                    "status": "skipped",
                    "reason": "No non-expired selected contracts remained available for current quote/snapshot enrichment.",
                },
            )
            return

        active_selected = active_selected.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
        collected_at = datetime.now(timezone.utc)
        collected_at_slug = collected_at.strftime("%Y%m%d_%H%M%S")
        underlying_lookup = {
            row.symbol: row.underlying_symbol
            for row in active_selected[["symbol", "underlying_symbol"]].drop_duplicates().itertuples()
        }

        for batch_index, symbol_batch in enumerate(
            batched(active_selected["symbol"].astype(str).tolist(), request.option_batch_size)
        ):
            batch_id = f"{collected_at_slug}__batch{batch_index:03d}"

            quote_raw_path, quote_silver_path = self._enrichment_paths(
                layout,
                "option_latest_quotes",
                collected_at_slug,
                batch_index,
            )
            if request.overwrite or not manifest.is_completed("option_latest_quotes", batch_id):
                manifest.start_chunk(
                    "option_latest_quotes",
                    batch_id,
                    metadata={"symbols": symbol_batch, "collected_at": collected_at.isoformat()},
                )
                try:
                    quote_payload = self.broker.get_option_latest_quotes(symbol_batch)
                    quote_frame = normalize_option_latest_quotes(
                        quote_payload,
                        collected_at=collected_at,
                        underlying_lookup=underlying_lookup,
                    )
                    quote_quality = build_quality_rows(
                        "option_latest_quotes",
                        quote_frame,
                        OPTION_QUOTE_SCHEMA,
                        batch_id,
                        group_columns=("underlying_symbol", "symbol"),
                    )
                    quote_warnings = ["empty latest quote response"] if quote_frame.empty else []
                    write_json(quote_raw_path, quote_payload)
                    ensure_directory(quote_silver_path.parent)
                    quote_frame.to_parquet(quote_silver_path, index=False)
                    manifest.complete_chunk(
                        "option_latest_quotes",
                        batch_id,
                        row_count=int(len(quote_frame)),
                        artifacts={"raw": quote_raw_path, "silver": quote_silver_path},
                        quality=quote_quality,
                        warnings=quote_warnings,
                    )
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("option_latest_quotes chunk failed: %s", batch_id)
                    manifest.fail_chunk("option_latest_quotes", batch_id, error=str(exc))

            snapshot_raw_path, snapshot_silver_path = self._enrichment_paths(
                layout,
                "option_snapshots",
                collected_at_slug,
                batch_index,
            )
            if request.overwrite or not manifest.is_completed("option_snapshots", batch_id):
                manifest.start_chunk(
                    "option_snapshots",
                    batch_id,
                    metadata={"symbols": symbol_batch, "collected_at": collected_at.isoformat()},
                )
                try:
                    snapshot_payload = self.broker.get_option_snapshots(symbol_batch)
                    snapshot_frame = normalize_option_snapshots(
                        snapshot_payload,
                        collected_at=collected_at,
                        underlying_lookup=underlying_lookup,
                    )
                    snapshot_quality = build_quality_rows(
                        "option_snapshots",
                        snapshot_frame,
                        OPTION_SNAPSHOT_SCHEMA,
                        batch_id,
                        group_columns=("underlying_symbol", "symbol"),
                    )
                    snapshot_warnings = ["empty snapshot response"] if snapshot_frame.empty else []
                    write_json(snapshot_raw_path, snapshot_payload)
                    ensure_directory(snapshot_silver_path.parent)
                    snapshot_frame.to_parquet(snapshot_silver_path, index=False)
                    manifest.complete_chunk(
                        "option_snapshots",
                        batch_id,
                        row_count=int(len(snapshot_frame)),
                        artifacts={"raw": snapshot_raw_path, "silver": snapshot_silver_path},
                        quality=snapshot_quality,
                        warnings=snapshot_warnings,
                    )
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("option_snapshots chunk failed: %s", batch_id)
                    manifest.fail_chunk("option_snapshots", batch_id, error=str(exc))

    def _write_reports(
        self,
        request: HistoricalBuildRequest,
        layout: BuildLayout,
        manifest: BuildManifestStore,
    ) -> tuple[Path, Path]:
        quality_frame = manifest.quality_frame()
        all_chunks = manifest.all_chunks_frame()
        failed_chunks = manifest.failed_chunks_frame()
        coverage_by_symbol, coverage_by_date = aggregate_quality_reports(quality_frame)
        selected_contracts = selected_contract_report(quality_frame)
        retry_summary = manifest.retry_summary()

        quality_parquet_path = layout.report_root / "quality_audit.parquet"
        coverage_symbol_path = layout.report_root / "coverage_by_symbol.parquet"
        coverage_date_path = layout.report_root / "coverage_by_date.parquet"
        selected_path = layout.report_root / "selected_contracts_by_day.parquet"
        failed_path = layout.report_root / "failed_chunks.csv"
        summary_path = layout.report_root / "build_summary.md"

        self._write_frame_bundle(quality_frame, quality_parquet_path, layout.report_root / "quality_audit.csv")
        self._write_frame_bundle(
            coverage_by_symbol,
            coverage_symbol_path,
            layout.report_root / "coverage_by_symbol.csv",
        )
        self._write_frame_bundle(
            coverage_by_date,
            coverage_date_path,
            layout.report_root / "coverage_by_date.csv",
        )
        self._write_frame_bundle(
            selected_contracts,
            selected_path,
            layout.report_root / "selected_contracts_by_day.csv",
        )
        self._write_frame_bundle(
            all_chunks,
            layout.report_root / "chunk_summary.parquet",
            layout.report_root / "chunk_summary.csv",
        )
        failed_chunks.to_csv(failed_path, index=False)

        summary_text = "\n".join(
            [
                f"# Historical Dataset Build: {layout.build_name}",
                "",
                "## Request",
                f"- Start date: {request.start_date.isoformat()}",
                f"- End date: {request.end_date.isoformat()}",
                f"- Stock symbols: {', '.join(request.stock_symbols)}",
                f"- Option underlyings: {', '.join(request.option_underlyings or ())}",
                f"- DTE window: {request.min_dte}-{request.max_dte}",
                f"- Strike steps around ATM: +/-{request.strike_steps}",
                f"- Stock chunk days: {request.stock_chunk_days}",
                f"- Contract chunk days: {request.contract_chunk_days}",
                f"- Option batch size: {request.option_batch_size}",
                "",
                "## Chunk Summary",
                f"- Total chunks: {retry_summary['total_chunks']}",
                f"- Retried chunks: {retry_summary['retried_chunks']}",
                f"- Failed chunks: {retry_summary['failed_chunks']}",
                "",
                "## Report Artifacts",
                f"- Quality audit: `{quality_parquet_path}`",
                f"- Coverage by symbol: `{coverage_symbol_path}`",
                f"- Coverage by date: `{coverage_date_path}`",
                f"- Selected contracts by day: `{selected_path}`",
                f"- Failed chunks: `{failed_path}`",
                "",
                "## Limitations",
                "- Historical stock bars, option bars, and option trades are collected from Alpaca.",
                "- Current latest option quotes and snapshots are collected only for non-expired selected contracts because Alpaca does not expose a historical options quotes surface in this build path.",
                "- Options minute bars may contain missing intervals when no trades were recorded for a contract.",
            ]
        )
        write_text(summary_path, summary_text)
        return quality_parquet_path, summary_path

    def _write_frame_bundle(self, frame: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
        ensure_directory(parquet_path.parent)
        frame.to_parquet(parquet_path, index=False)
        frame.to_csv(csv_path, index=False)

    def _load_dataset(self, root: Path) -> pd.DataFrame:
        if not root.exists():
            return pd.DataFrame()
        parts: list[pd.DataFrame] = []
        for path in sorted(root.rglob("*.parquet")):
            parts.append(pd.read_parquet(path))
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts, ignore_index=True)

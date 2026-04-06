from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd


FieldKind = Literal["string", "float", "int", "bool", "datetime", "date", "json"]


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    kind: FieldKind
    required: bool = True


@dataclass(frozen=True, slots=True)
class SchemaValidationResult:
    missing_columns: tuple[str, ...]
    extra_columns: tuple[str, ...]
    duplicate_rows: int
    row_count: int


@dataclass(frozen=True, slots=True)
class DatasetSchema:
    name: str
    fields: tuple[FieldSpec, ...]
    primary_key: tuple[str, ...] = field(default_factory=tuple)

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields if field.required)

    @property
    def allowed_columns(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields)

    def apply(self, frame: pd.DataFrame) -> pd.DataFrame:
        normalized = frame.copy()
        for spec in self.fields:
            if spec.name not in normalized.columns:
                normalized[spec.name] = pd.NA
            normalized[spec.name] = _coerce_series(normalized[spec.name], spec.kind)
        return normalized.loc[:, self.allowed_columns]

    def validate(self, frame: pd.DataFrame) -> SchemaValidationResult:
        missing = tuple(column for column in self.required_columns if column not in frame.columns)
        extra = tuple(column for column in frame.columns if column not in self.allowed_columns)
        duplicate_rows = 0
        if self.primary_key and all(column in frame.columns for column in self.primary_key):
            duplicate_rows = int(frame.duplicated(subset=list(self.primary_key)).sum())
        return SchemaValidationResult(
            missing_columns=missing,
            extra_columns=extra,
            duplicate_rows=duplicate_rows,
            row_count=int(len(frame)),
        )


def _coerce_series(series: pd.Series, kind: FieldKind) -> pd.Series:
    if kind == "string":
        return series.astype("string")
    if kind == "float":
        return pd.to_numeric(series, errors="coerce").astype("Float64")
    if kind == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if kind == "bool":
        if series.dtype == "boolean":
            return series
        def to_boolean(value: object) -> object:
            if pd.isna(value):
                return pd.NA
            if isinstance(value, bool):
                return value
            normalized = str(value).strip().lower()
            if normalized in {"true", "1", "yes", "y"}:
                return True
            if normalized in {"false", "0", "no", "n"}:
                return False
            return pd.NA

        mapped = series.map(to_boolean)
        return mapped.astype("boolean")
    if kind == "datetime":
        return pd.to_datetime(series, utc=True, errors="coerce")
    if kind == "date":
        parsed = pd.to_datetime(series, errors="coerce")
        if getattr(parsed.dt, "tz", None) is not None:
            parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
        return parsed
    if kind == "json":
        return series.astype("string")
    raise ValueError(f"Unsupported field kind: {kind}")


STOCK_BAR_SCHEMA = DatasetSchema(
    name="stock_bars",
    fields=(
        FieldSpec("symbol", "string"),
        FieldSpec("timestamp", "datetime"),
        FieldSpec("open", "float"),
        FieldSpec("high", "float"),
        FieldSpec("low", "float"),
        FieldSpec("close", "float"),
        FieldSpec("volume", "int"),
        FieldSpec("trade_count", "int", required=False),
        FieldSpec("vwap", "float", required=False),
        FieldSpec("source_feed", "string", required=False),
        FieldSpec("chunk_start", "datetime", required=False),
        FieldSpec("chunk_end", "datetime", required=False),
    ),
    primary_key=("symbol", "timestamp"),
)

OPTION_CONTRACT_SCHEMA = DatasetSchema(
    name="option_contracts",
    fields=(
        FieldSpec("contract_id", "string", required=False),
        FieldSpec("symbol", "string"),
        FieldSpec("name", "string", required=False),
        FieldSpec("status", "string", required=False),
        FieldSpec("tradable", "bool", required=False),
        FieldSpec("expiration_date", "date"),
        FieldSpec("root_symbol", "string", required=False),
        FieldSpec("underlying_symbol", "string"),
        FieldSpec("underlying_asset_id", "string", required=False),
        FieldSpec("option_type", "string"),
        FieldSpec("style", "string", required=False),
        FieldSpec("strike_price", "float"),
        FieldSpec("multiplier", "int", required=False),
        FieldSpec("contract_size", "int", required=False),
        FieldSpec("open_interest", "int", required=False),
        FieldSpec("open_interest_date", "date", required=False),
        FieldSpec("close_price", "float", required=False),
        FieldSpec("close_price_date", "date", required=False),
        FieldSpec("ppind", "bool", required=False),
        FieldSpec("inventory_status", "string", required=False),
        FieldSpec("inventory_collected_at", "datetime", required=False),
        FieldSpec("expiration_window_start", "date", required=False),
        FieldSpec("expiration_window_end", "date", required=False),
    ),
    primary_key=("symbol",),
)

SELECTED_OPTION_SCHEMA = DatasetSchema(
    name="selected_option_contracts",
    fields=(
        FieldSpec("trade_date", "date"),
        FieldSpec("reference_timestamp", "datetime"),
        FieldSpec("reference_price", "float"),
        FieldSpec("underlying_symbol", "string"),
        FieldSpec("symbol", "string"),
        FieldSpec("expiration_date", "date"),
        FieldSpec("option_type", "string"),
        FieldSpec("strike_price", "float"),
        FieldSpec("dte", "int"),
        FieldSpec("atm_strike", "float"),
        FieldSpec("relative_strike_step", "int"),
        FieldSpec("selection_reason", "string"),
        FieldSpec("inventory_status", "string", required=False),
    ),
    primary_key=("trade_date", "symbol"),
)

OPTION_BAR_SCHEMA = DatasetSchema(
    name="option_bars",
    fields=(
        FieldSpec("symbol", "string"),
        FieldSpec("underlying_symbol", "string", required=False),
        FieldSpec("trade_date", "date"),
        FieldSpec("timestamp", "datetime"),
        FieldSpec("open", "float"),
        FieldSpec("high", "float"),
        FieldSpec("low", "float"),
        FieldSpec("close", "float"),
        FieldSpec("volume", "int"),
        FieldSpec("trade_count", "int", required=False),
        FieldSpec("vwap", "float", required=False),
        FieldSpec("chunk_id", "string", required=False),
    ),
    primary_key=("symbol", "timestamp"),
)

OPTION_TRADE_SCHEMA = DatasetSchema(
    name="option_trades",
    fields=(
        FieldSpec("symbol", "string"),
        FieldSpec("underlying_symbol", "string", required=False),
        FieldSpec("trade_date", "date"),
        FieldSpec("timestamp", "datetime"),
        FieldSpec("price", "float"),
        FieldSpec("size", "int"),
        FieldSpec("exchange", "string", required=False),
        FieldSpec("conditions", "string", required=False),
        FieldSpec("chunk_id", "string", required=False),
    ),
    primary_key=("symbol", "timestamp", "price", "size"),
)

OPTION_QUOTE_SCHEMA = DatasetSchema(
    name="option_latest_quotes",
    fields=(
        FieldSpec("symbol", "string"),
        FieldSpec("underlying_symbol", "string", required=False),
        FieldSpec("quote_timestamp", "datetime"),
        FieldSpec("bid_price", "float", required=False),
        FieldSpec("bid_size", "int", required=False),
        FieldSpec("bid_exchange", "string", required=False),
        FieldSpec("ask_price", "float", required=False),
        FieldSpec("ask_size", "int", required=False),
        FieldSpec("ask_exchange", "string", required=False),
        FieldSpec("quote_condition", "string", required=False),
        FieldSpec("collected_at", "datetime"),
    ),
    primary_key=("symbol", "quote_timestamp"),
)

OPTION_SNAPSHOT_SCHEMA = DatasetSchema(
    name="option_snapshots",
    fields=(
        FieldSpec("symbol", "string"),
        FieldSpec("underlying_symbol", "string", required=False),
        FieldSpec("collected_at", "datetime"),
        FieldSpec("implied_volatility", "float", required=False),
        FieldSpec("delta", "float", required=False),
        FieldSpec("gamma", "float", required=False),
        FieldSpec("theta", "float", required=False),
        FieldSpec("vega", "float", required=False),
        FieldSpec("rho", "float", required=False),
        FieldSpec("latest_quote_timestamp", "datetime", required=False),
        FieldSpec("latest_bid_price", "float", required=False),
        FieldSpec("latest_bid_size", "int", required=False),
        FieldSpec("latest_bid_exchange", "string", required=False),
        FieldSpec("latest_ask_price", "float", required=False),
        FieldSpec("latest_ask_size", "int", required=False),
        FieldSpec("latest_ask_exchange", "string", required=False),
        FieldSpec("latest_quote_condition", "string", required=False),
        FieldSpec("latest_trade_timestamp", "datetime", required=False),
        FieldSpec("latest_trade_price", "float", required=False),
        FieldSpec("latest_trade_size", "int", required=False),
        FieldSpec("latest_trade_exchange", "string", required=False),
        FieldSpec("latest_trade_condition", "string", required=False),
        FieldSpec("minute_bar_timestamp", "datetime", required=False),
        FieldSpec("minute_bar_open", "float", required=False),
        FieldSpec("minute_bar_high", "float", required=False),
        FieldSpec("minute_bar_low", "float", required=False),
        FieldSpec("minute_bar_close", "float", required=False),
        FieldSpec("minute_bar_volume", "int", required=False),
        FieldSpec("minute_bar_trade_count", "int", required=False),
        FieldSpec("minute_bar_vwap", "float", required=False),
        FieldSpec("daily_bar_timestamp", "datetime", required=False),
        FieldSpec("daily_bar_open", "float", required=False),
        FieldSpec("daily_bar_high", "float", required=False),
        FieldSpec("daily_bar_low", "float", required=False),
        FieldSpec("daily_bar_close", "float", required=False),
        FieldSpec("daily_bar_volume", "int", required=False),
        FieldSpec("daily_bar_trade_count", "int", required=False),
        FieldSpec("daily_bar_vwap", "float", required=False),
        FieldSpec("prev_daily_bar_timestamp", "datetime", required=False),
        FieldSpec("prev_daily_bar_open", "float", required=False),
        FieldSpec("prev_daily_bar_high", "float", required=False),
        FieldSpec("prev_daily_bar_low", "float", required=False),
        FieldSpec("prev_daily_bar_close", "float", required=False),
        FieldSpec("prev_daily_bar_volume", "int", required=False),
        FieldSpec("prev_daily_bar_trade_count", "int", required=False),
        FieldSpec("prev_daily_bar_vwap", "float", required=False),
    ),
    primary_key=("symbol", "collected_at"),
)

QUALITY_AUDIT_SCHEMA = DatasetSchema(
    name="quality_audit",
    fields=(
        FieldSpec("dataset", "string"),
        FieldSpec("chunk_id", "string"),
        FieldSpec("underlying_symbol", "string", required=False),
        FieldSpec("symbol", "string", required=False),
        FieldSpec("trade_date", "date", required=False),
        FieldSpec("row_count", "int"),
        FieldSpec("duplicate_rows", "int"),
        FieldSpec("missing_intervals", "int", required=False),
        FieldSpec("empty_response", "bool"),
        FieldSpec("schema_missing_columns", "string", required=False),
        FieldSpec("schema_extra_columns", "string", required=False),
    ),
)

SCHEMAS = {
    "stock_bars": STOCK_BAR_SCHEMA,
    "option_contracts": OPTION_CONTRACT_SCHEMA,
    "selected_option_contracts": SELECTED_OPTION_SCHEMA,
    "option_bars": OPTION_BAR_SCHEMA,
    "option_trades": OPTION_TRADE_SCHEMA,
    "option_latest_quotes": OPTION_QUOTE_SCHEMA,
    "option_snapshots": OPTION_SNAPSHOT_SCHEMA,
    "quality_audit": QUALITY_AUDIT_SCHEMA,
}

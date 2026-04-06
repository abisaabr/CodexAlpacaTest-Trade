from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from alpaca_lab.data.chunking import market_session_bounds
from alpaca_lab.data.schemas import DatasetSchema, QUALITY_AUDIT_SCHEMA


def add_trade_date(
    frame: pd.DataFrame,
    *,
    timestamp_column: str = "timestamp",
    timezone_name: str = "America/New_York",
) -> pd.DataFrame:
    if frame.empty:
        if "trade_date" in frame.columns:
            return frame
        return frame.assign(trade_date=pd.Series(dtype="datetime64[ns]"))
    if "trade_date" in frame.columns:
        result = frame.copy()
        result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.normalize()
        return result
    if timestamp_column not in frame.columns:
        return frame.copy()

    result = frame.copy()
    result[timestamp_column] = pd.to_datetime(result[timestamp_column], utc=True, errors="coerce")
    result["trade_date"] = (
        result[timestamp_column].dt.tz_convert(timezone_name).dt.normalize().dt.tz_localize(None)
    )
    return result


def calculate_missing_bar_intervals(
    frame: pd.DataFrame,
    *,
    group_columns: Iterable[str],
    timestamp_column: str = "timestamp",
) -> dict[tuple[Any, ...], int]:
    if frame.empty or timestamp_column not in frame.columns:
        return {}

    working = add_trade_date(frame, timestamp_column=timestamp_column)
    if "trade_date" not in working.columns:
        return {}

    group_columns = tuple(group_columns)
    missing_map: dict[tuple[Any, ...], int] = {}

    for group_key, subset in working.groupby(list(group_columns), dropna=False, sort=True):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        trade_date_value = subset["trade_date"].iloc[0]
        if pd.isna(trade_date_value):
            missing_map[group_key] = 0
            continue

        session_start, session_end = market_session_bounds(pd.Timestamp(trade_date_value).date())
        expected_index = pd.date_range(session_start, session_end, freq="1min", inclusive="left")
        observed = (
            pd.to_datetime(subset[timestamp_column], utc=True, errors="coerce")
            .dropna()
            .dt.floor("min")
            .drop_duplicates()
            .sort_values()
        )
        missing_map[group_key] = int(len(expected_index.difference(pd.DatetimeIndex(observed))))
    return missing_map


def build_quality_rows(
    dataset: str,
    frame: pd.DataFrame,
    schema: DatasetSchema,
    chunk_id: str,
    *,
    group_columns: Iterable[str] = (),
    timestamp_column: str = "timestamp",
    check_missing_intervals: bool = False,
) -> list[dict[str, Any]]:
    group_columns = tuple(group_columns)
    schema_result = schema.validate(frame)

    if frame.empty:
        quality_frame = QUALITY_AUDIT_SCHEMA.apply(
            pd.DataFrame(
                [
                    {
                        "dataset": dataset,
                        "chunk_id": chunk_id,
                        "underlying_symbol": pd.NA,
                        "symbol": pd.NA,
                        "trade_date": pd.NaT,
                        "row_count": 0,
                        "duplicate_rows": 0,
                        "missing_intervals": pd.NA,
                        "empty_response": True,
                        "schema_missing_columns": ",".join(schema_result.missing_columns) or pd.NA,
                        "schema_extra_columns": ",".join(schema_result.extra_columns) or pd.NA,
                    }
                ]
            )
        )
        return quality_frame.where(pd.notna(quality_frame), None).to_dict(orient="records")

    working = add_trade_date(frame, timestamp_column=timestamp_column)
    group_frame = working if group_columns else working.assign(__all__="all")
    effective_groups = group_columns if group_columns else ("__all__",)
    missing_map = (
        calculate_missing_bar_intervals(group_frame, group_columns=effective_groups, timestamp_column=timestamp_column)
        if check_missing_intervals
        else {}
    )

    rows: list[dict[str, Any]] = []
    for group_key, subset in group_frame.groupby(list(effective_groups), dropna=False, sort=True):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        group_values = dict(zip(effective_groups, group_key))
        duplicate_rows = (
            int(subset.duplicated(subset=list(schema.primary_key)).sum()) if schema.primary_key else 0
        )
        rows.append(
            {
                "dataset": dataset,
                "chunk_id": chunk_id,
                "underlying_symbol": (
                    group_values.get("underlying_symbol")
                    if "underlying_symbol" in effective_groups
                    else subset["underlying_symbol"].iloc[0]
                    if "underlying_symbol" in subset.columns and subset["underlying_symbol"].nunique(dropna=False) == 1
                    else pd.NA
                ),
                "symbol": (
                    group_values.get("symbol")
                    if "symbol" in effective_groups
                    else subset["symbol"].iloc[0]
                    if "symbol" in subset.columns and subset["symbol"].nunique(dropna=False) == 1
                    else pd.NA
                ),
                "trade_date": (
                    group_values.get("trade_date")
                    if "trade_date" in effective_groups
                    else subset["trade_date"].iloc[0]
                    if "trade_date" in subset.columns and subset["trade_date"].nunique(dropna=False) == 1
                    else pd.NaT
                ),
                "row_count": int(len(subset)),
                "duplicate_rows": duplicate_rows,
                "missing_intervals": missing_map.get(group_key, pd.NA) if check_missing_intervals else pd.NA,
                "empty_response": False,
                "schema_missing_columns": ",".join(schema_result.missing_columns) or pd.NA,
                "schema_extra_columns": ",".join(schema_result.extra_columns) or pd.NA,
            }
        )

    quality_frame = QUALITY_AUDIT_SCHEMA.apply(pd.DataFrame(rows))
    return quality_frame.where(pd.notna(quality_frame), None).to_dict(orient="records")


def aggregate_quality_reports(quality_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if quality_frame.empty:
        return (
            pd.DataFrame(
                columns=[
                    "dataset",
                    "coverage_symbol",
                    "row_count",
                    "chunk_count",
                    "trade_days",
                    "missing_intervals",
                    "duplicate_rows",
                    "empty_responses",
                ]
            ),
            pd.DataFrame(
                columns=[
                    "dataset",
                    "trade_date",
                    "row_count",
                    "chunk_count",
                    "symbols",
                    "missing_intervals",
                    "duplicate_rows",
                    "empty_responses",
                ]
            ),
        )

    working = quality_frame.copy()
    working["coverage_symbol"] = working["underlying_symbol"].fillna(working["symbol"])

    by_symbol = (
        working.dropna(subset=["coverage_symbol"])
        .groupby(["dataset", "coverage_symbol"], dropna=False)
        .agg(
            row_count=("row_count", "sum"),
            chunk_count=("chunk_id", "nunique"),
            trade_days=("trade_date", "nunique"),
            missing_intervals=("missing_intervals", "sum"),
            duplicate_rows=("duplicate_rows", "sum"),
            empty_responses=("empty_response", "sum"),
        )
        .reset_index()
        .sort_values(["dataset", "coverage_symbol"])
        .reset_index(drop=True)
    )

    by_date = (
        working.dropna(subset=["trade_date"])
        .groupby(["dataset", "trade_date"], dropna=False)
        .agg(
            row_count=("row_count", "sum"),
            chunk_count=("chunk_id", "nunique"),
            symbols=("symbol", "nunique"),
            missing_intervals=("missing_intervals", "sum"),
            duplicate_rows=("duplicate_rows", "sum"),
            empty_responses=("empty_response", "sum"),
        )
        .reset_index()
        .sort_values(["dataset", "trade_date"])
        .reset_index(drop=True)
    )

    return by_symbol, by_date


def selected_contract_report(quality_frame: pd.DataFrame) -> pd.DataFrame:
    if quality_frame.empty:
        return pd.DataFrame(columns=["underlying_symbol", "trade_date", "selected_contracts"])

    filtered = quality_frame[quality_frame["dataset"] == "selected_option_contracts"].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["underlying_symbol", "trade_date", "selected_contracts"])

    return (
        filtered.groupby(["underlying_symbol", "trade_date"], dropna=False)["row_count"]
        .sum()
        .rename("selected_contracts")
        .reset_index()
        .sort_values(["underlying_symbol", "trade_date"])
        .reset_index(drop=True)
    )

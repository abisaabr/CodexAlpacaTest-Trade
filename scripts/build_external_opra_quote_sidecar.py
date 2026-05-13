from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd


OPTION_SYMBOL_RE = re.compile(r"^([A-Z]+)\d{6}[CP]\d{8}$")

OPTION_QUOTE_COLUMNS = [
    "symbol",
    "option_symbol",
    "underlying_symbol",
    "event_time_utc",
    "observed_at_utc",
    "quote_age_seconds",
    "latency_seconds",
    "bid",
    "ask",
    "bid_price",
    "ask_price",
    "bid_size",
    "ask_size",
    "mid",
    "absolute_spread",
    "relative_spread",
    "spread_pct",
    "quote_source",
    "source_file",
]

ALIASES = {
    "symbol": ("option_symbol", "symbol", "contract_symbol", "contract", "root_symbol"),
    "timestamp": (
        "event_time_utc",
        "quote_time",
        "timestamp",
        "sip_timestamp",
        "participant_timestamp",
        "t",
    ),
    "observed_at": ("observed_at_utc", "received_at", "capture_time", "ingested_at"),
    "bid": ("bid", "bid_price", "best_bid", "bp"),
    "ask": ("ask", "ask_price", "best_ask", "ap"),
    "bid_size": ("bid_size", "best_bid_size", "bs"),
    "ask_size": ("ask_size", "best_ask_size", "as"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize external historical OPRA BBO quote exports into the "
            "option_quote_sidecar.csv schema used by quote-backed replay. "
            "This is research-only and never submits orders."
        )
    )
    parser.add_argument("--input-path", required=True, help="CSV file or directory of CSV files.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--input-glob", default="*.csv", help="Glob used when input-path is a directory.")
    parser.add_argument("--acquisition-manifest-csv", default=None)
    parser.add_argument("--symbol-column", default=None)
    parser.add_argument("--timestamp-column", default=None)
    parser.add_argument("--observed-at-column", default=None)
    parser.add_argument("--bid-column", default=None)
    parser.add_argument("--ask-column", default=None)
    parser.add_argument("--bid-size-column", default=None)
    parser.add_argument("--ask-size-column", default=None)
    parser.add_argument("--quote-source", default="option_quote_bid_ask_external_opra")
    parser.add_argument(
        "--manifest-window-padding-seconds",
        type=float,
        default=0.0,
        help="Optional extra seconds before/after acquisition manifest windows.",
    )
    return parser.parse_args()


def _input_paths(input_path: Path, input_glob: str) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    if not input_path.exists():
        raise FileNotFoundError(input_path)
    paths = sorted(path for path in input_path.glob(input_glob) if path.is_file())
    if not paths:
        raise ValueError(f"no input CSV files found under {input_path} with glob {input_glob!r}")
    return paths


def _resolve_column(frame: pd.DataFrame, explicit: str | None, aliases: tuple[str, ...], *, required: bool) -> str | None:
    if explicit:
        if explicit not in frame.columns:
            raise ValueError(f"requested column {explicit!r} not found in input columns")
        return explicit
    lower_to_column = {str(column).lower(): str(column) for column in frame.columns}
    for alias in aliases:
        column = lower_to_column.get(alias.lower())
        if column is not None:
            return column
    if required:
        raise ValueError(f"none of columns {aliases!r} found in input columns")
    return None


def _timestamp_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _underlying_from_option_symbol(symbol: str) -> str:
    match = OPTION_SYMBOL_RE.match(symbol.upper())
    return match.group(1) if match else "UNKNOWN"


def _safe_relative_spread(bid: float, ask: float) -> float | None:
    mid = (bid + ask) / 2.0
    if mid <= 0.0:
        return None
    return (ask - bid) / mid


def _load_manifest_windows(
    acquisition_manifest_csv: Path | None,
    *,
    padding_seconds: float,
) -> tuple[dict[str, tuple[pd.Timestamp, pd.Timestamp]], dict[str, set[str]]]:
    if acquisition_manifest_csv is None:
        return {}, {}
    manifest = pd.read_csv(acquisition_manifest_csv, low_memory=False)
    required = {"contract_symbol", "requested_window_start_utc", "requested_window_end_utc"}
    missing = sorted(required - set(manifest.columns))
    if missing:
        raise ValueError(f"acquisition manifest missing required columns: {missing}")
    frame = manifest.copy()
    frame["contract_symbol"] = frame["contract_symbol"].astype(str).str.upper()
    frame["_start"] = _timestamp_series(frame["requested_window_start_utc"])
    frame["_end"] = _timestamp_series(frame["requested_window_end_utc"])
    frame = frame.dropna(subset=["_start", "_end"])
    if padding_seconds:
        padding = pd.Timedelta(seconds=padding_seconds)
        frame["_start"] = frame["_start"] - padding
        frame["_end"] = frame["_end"] + padding
    windows: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    dates_by_symbol: dict[str, set[str]] = {}
    for symbol, group in frame.groupby("contract_symbol"):
        windows[str(symbol)] = (group["_start"].min(), group["_end"].max())
        dates_by_symbol[str(symbol)] = set(pd.to_datetime(group["_start"], utc=True).dt.date.astype(str))
    return windows, dates_by_symbol


def _normalize_one_file(
    path: Path,
    *,
    args: argparse.Namespace,
    manifest_windows: dict[str, tuple[pd.Timestamp, pd.Timestamp]],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    frame = pd.read_csv(path, low_memory=False)
    if frame.empty:
        return pd.DataFrame(columns=OPTION_QUOTE_COLUMNS), pd.DataFrame(), {
            "source_file": str(path),
            "input_rows": 0,
            "kept_rows": 0,
            "rejected_rows": 0,
        }

    symbol_column = _resolve_column(frame, args.symbol_column, ALIASES["symbol"], required=True)
    timestamp_column = _resolve_column(frame, args.timestamp_column, ALIASES["timestamp"], required=True)
    observed_column = _resolve_column(frame, args.observed_at_column, ALIASES["observed_at"], required=False)
    bid_column = _resolve_column(frame, args.bid_column, ALIASES["bid"], required=True)
    ask_column = _resolve_column(frame, args.ask_column, ALIASES["ask"], required=True)
    bid_size_column = _resolve_column(frame, args.bid_size_column, ALIASES["bid_size"], required=False)
    ask_size_column = _resolve_column(frame, args.ask_size_column, ALIASES["ask_size"], required=False)

    work = pd.DataFrame()
    work["symbol"] = frame[symbol_column].astype(str).str.upper().str.strip()
    work["option_symbol"] = work["symbol"]
    work["_event_ts"] = _timestamp_series(frame[timestamp_column])
    work["_observed_ts"] = _timestamp_series(frame[observed_column]) if observed_column else work["_event_ts"]
    work["bid"] = pd.to_numeric(frame[bid_column], errors="coerce")
    work["ask"] = pd.to_numeric(frame[ask_column], errors="coerce")
    work["bid_size"] = pd.to_numeric(frame[bid_size_column], errors="coerce") if bid_size_column else 0.0
    work["ask_size"] = pd.to_numeric(frame[ask_size_column], errors="coerce") if ask_size_column else 0.0
    work["source_file"] = str(path)

    rejection_reasons: list[str] = []
    kept_mask: list[bool] = []
    for _, row in work.iterrows():
        reasons: list[str] = []
        if not row["symbol"]:
            reasons.append("missing_symbol")
        if pd.isna(row["_event_ts"]):
            reasons.append("missing_event_time")
        bid = row["bid"]
        ask = row["ask"]
        if pd.isna(bid) or pd.isna(ask) or not (float(bid) > 0.0 and float(ask) >= float(bid)):
            reasons.append("invalid_bid_ask")
        if manifest_windows:
            window = manifest_windows.get(row["symbol"])
            if window is None:
                reasons.append("symbol_not_requested")
            elif not pd.isna(row["_event_ts"]):
                start, end = window
                if not (start <= row["_event_ts"] <= end):
                    reasons.append("outside_requested_window")
        kept_mask.append(not reasons)
        rejection_reasons.append(";".join(reasons))

    rejected = work.loc[[not value for value in kept_mask]].copy()
    if not rejected.empty:
        rejected["rejection_reason"] = [reason for keep, reason in zip(kept_mask, rejection_reasons) if not keep]
    kept = work.loc[kept_mask].copy()
    if kept.empty:
        return pd.DataFrame(columns=OPTION_QUOTE_COLUMNS), rejected, {
            "source_file": str(path),
            "input_rows": int(len(frame)),
            "kept_rows": 0,
            "rejected_rows": int(len(rejected)),
        }

    kept = kept.sort_values(["option_symbol", "_event_ts", "_observed_ts"]).drop_duplicates(
        subset=["option_symbol", "_event_ts", "bid", "ask"],
        keep="last",
    )
    kept["underlying_symbol"] = kept["option_symbol"].map(_underlying_from_option_symbol)
    kept["event_time_utc"] = kept["_event_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    kept["observed_at_utc"] = kept["_observed_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    kept["quote_age_seconds"] = (
        (kept["_observed_ts"] - kept["_event_ts"]).dt.total_seconds().clip(lower=0).round(6)
    )
    kept["latency_seconds"] = kept["quote_age_seconds"]
    kept["bid_price"] = kept["bid"]
    kept["ask_price"] = kept["ask"]
    kept["mid"] = ((kept["bid"] + kept["ask"]) / 2.0).round(6)
    kept["absolute_spread"] = (kept["ask"] - kept["bid"]).round(6)
    kept["relative_spread"] = [
        round(value, 8) if value is not None and math.isfinite(value) else None
        for value in (_safe_relative_spread(float(bid), float(ask)) for bid, ask in zip(kept["bid"], kept["ask"]))
    ]
    kept["spread_pct"] = kept["relative_spread"]
    kept["quote_source"] = args.quote_source
    output = kept[OPTION_QUOTE_COLUMNS].copy()
    return output, rejected, {
        "source_file": str(path),
        "input_rows": int(len(frame)),
        "kept_rows": int(len(output)),
        "rejected_rows": int(len(rejected)),
    }


def build_external_opra_quote_sidecar(
    *,
    input_path: Path,
    output_dir: Path,
    input_glob: str = "*.csv",
    acquisition_manifest_csv: Path | None = None,
    manifest_window_padding_seconds: float = 0.0,
    quote_source: str = "option_quote_bid_ask_external_opra",
    symbol_column: str | None = None,
    timestamp_column: str | None = None,
    observed_at_column: str | None = None,
    bid_column: str | None = None,
    ask_column: str | None = None,
    bid_size_column: str | None = None,
    ask_size_column: str | None = None,
) -> dict[str, Any]:
    args = argparse.Namespace(
        symbol_column=symbol_column,
        timestamp_column=timestamp_column,
        observed_at_column=observed_at_column,
        bid_column=bid_column,
        ask_column=ask_column,
        bid_size_column=bid_size_column,
        ask_size_column=ask_size_column,
        quote_source=quote_source,
    )
    paths = _input_paths(input_path, input_glob)
    manifest_windows, dates_by_symbol = _load_manifest_windows(
        acquisition_manifest_csv,
        padding_seconds=manifest_window_padding_seconds,
    )
    outputs: list[pd.DataFrame] = []
    rejections: list[pd.DataFrame] = []
    source_summaries: list[dict[str, Any]] = []
    for path in paths:
        normalized, rejected, source_summary = _normalize_one_file(
            path,
            args=args,
            manifest_windows=manifest_windows,
        )
        outputs.append(normalized)
        if not rejected.empty:
            rejections.append(rejected)
        source_summaries.append(source_summary)

    sidecar = pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame(columns=OPTION_QUOTE_COLUMNS)
    if not sidecar.empty:
        sidecar = sidecar.sort_values(["option_symbol", "event_time_utc", "observed_at_utc"]).reset_index(drop=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    sidecar_path = output_dir / "option_quote_sidecar.csv"
    sidecar.to_csv(sidecar_path, index=False)

    rejected_frame = pd.concat(rejections, ignore_index=True) if rejections else pd.DataFrame()
    rejected_path = output_dir / "external_opra_rejected_rows.csv"
    rejected_frame.to_csv(rejected_path, index=False)

    requested_symbols = set(manifest_windows)
    covered_symbols = set(sidecar["option_symbol"].astype(str).str.upper()) if not sidecar.empty else set()
    summary = {
        "status": "external_opra_quote_sidecar_complete",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "input_path": str(input_path),
        "input_file_count": len(paths),
        "acquisition_manifest_csv": str(acquisition_manifest_csv) if acquisition_manifest_csv else None,
        "output_sidecar_csv": str(sidecar_path),
        "rejected_rows_csv": str(rejected_path),
        "input_rows": int(sum(item["input_rows"] for item in source_summaries)),
        "sidecar_quote_rows": int(len(sidecar)),
        "rejected_rows": int(len(rejected_frame)),
        "unique_option_symbols": int(len(covered_symbols)),
        "manifest_requested_symbol_count": int(len(requested_symbols)),
        "manifest_symbols_with_quotes": int(len(requested_symbols & covered_symbols)),
        "manifest_symbols_without_quotes": int(len(requested_symbols - covered_symbols)),
        "manifest_dates_by_symbol_count": int(sum(len(value) for value in dates_by_symbol.values())),
        "quote_source": quote_source,
        "source_files": source_summaries,
    }
    (output_dir / "external_opra_sidecar_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    summary = build_external_opra_quote_sidecar(
        input_path=Path(args.input_path),
        output_dir=Path(args.output_dir),
        input_glob=args.input_glob,
        acquisition_manifest_csv=Path(args.acquisition_manifest_csv) if args.acquisition_manifest_csv else None,
        manifest_window_padding_seconds=args.manifest_window_padding_seconds,
        quote_source=args.quote_source,
        symbol_column=args.symbol_column,
        timestamp_column=args.timestamp_column,
        observed_at_column=args.observed_at_column,
        bid_column=args.bid_column,
        ask_column=args.ask_column,
        bid_size_column=args.bid_size_column,
        ask_size_column=args.ask_size_column,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

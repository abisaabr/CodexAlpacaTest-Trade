from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a vendor-ready historical OPRA BBO backfill request package "
            "from quote_manifest.csv. This is research-only and never submits orders."
        )
    )
    parser.add_argument("--quote-manifest-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-windows-per-chunk", type=int, default=250)
    parser.add_argument(
        "--window-padding-seconds",
        type=float,
        default=0.0,
        help="Optional extra seconds added to each consolidated request window.",
    )
    return parser.parse_args()


def _timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        timestamp = pd.Timestamp(text)
    except (TypeError, ValueError):
        return None
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _clean_symbol(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip().upper()


def _iso(timestamp: pd.Timestamp | None) -> str:
    return "" if timestamp is None else timestamp.isoformat()


def _collapse_values(series: pd.Series, *, limit: int = 20) -> str:
    values = sorted({str(value) for value in series.fillna("") if str(value)})
    if len(values) > limit:
        return ";".join(values[:limit]) + f";+{len(values) - limit}_more"
    return ";".join(values)


def _window_rows(manifest: pd.DataFrame, *, window_padding_seconds: float) -> pd.DataFrame:
    frame = manifest.copy()
    frame["_contract"] = frame["contract_symbol"].map(_clean_symbol)
    frame["_start"] = pd.to_datetime(frame["requested_window_start_utc"], utc=True, errors="coerce")
    frame["_end"] = pd.to_datetime(frame["requested_window_end_utc"], utc=True, errors="coerce")
    frame["_decision"] = pd.to_datetime(frame["decision_time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["_start", "_end"])
    frame = frame[frame["_contract"] != ""]
    if window_padding_seconds:
        padding = pd.Timedelta(seconds=window_padding_seconds)
        frame["_start"] = frame["_start"] - padding
        frame["_end"] = frame["_end"] + padding
    grouped = frame.groupby(["underlying", "trade_date", "_contract"], dropna=False)
    rows = grouped.agg(
        request_window_start_utc=("_start", "min"),
        request_window_end_utc=("_end", "max"),
        first_decision_time_utc=("_decision", "min"),
        last_decision_time_utc=("_decision", "max"),
        event_count=("event_side", "size"),
        candidate_count=("candidate_variant_id", "nunique"),
        strategy_count=("strategy_id", "nunique"),
        families=("family", _collapse_values),
        intended_regimes=("intended_regime", _collapse_values),
        event_sides=("event_side", _collapse_values),
    ).reset_index()
    rows = rows.rename(columns={"_contract": "contract_symbol"})
    rows["request_window_start_utc"] = rows["request_window_start_utc"].map(_iso)
    rows["request_window_end_utc"] = rows["request_window_end_utc"].map(_iso)
    rows["first_decision_time_utc"] = rows["first_decision_time_utc"].map(_iso)
    rows["last_decision_time_utc"] = rows["last_decision_time_utc"].map(_iso)
    rows["requested_dataset"] = "historical_opra_bbo_nbbo"
    rows["required_fields"] = "option_symbol,event_time_utc,bid,ask,bid_size,ask_size"
    rows["normalizer_script"] = "scripts/build_external_opra_quote_sidecar.py"
    rows["audit_script"] = "scripts/audit_quote_manifest_backfill.py"
    return rows.sort_values(["underlying", "trade_date", "contract_symbol"]).reset_index(drop=True)


def _write_chunks(windows: pd.DataFrame, output_dir: Path, max_windows_per_chunk: int) -> list[dict[str, Any]]:
    chunk_dir = output_dir / "chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunks: list[dict[str, Any]] = []
    if max_windows_per_chunk <= 0:
        raise ValueError("max_windows_per_chunk must be positive")
    for chunk_index, start in enumerate(range(0, len(windows), max_windows_per_chunk), start=1):
        chunk = windows.iloc[start : start + max_windows_per_chunk].copy()
        path = chunk_dir / f"opra_backfill_request_chunk_{chunk_index:04d}.csv"
        chunk.to_csv(path, index=False)
        chunks.append(
            {
                "chunk_index": chunk_index,
                "path": str(path),
                "window_count": int(len(chunk)),
                "event_count": int(chunk["event_count"].sum()) if not chunk.empty else 0,
                "trade_date_min": str(chunk["trade_date"].min()) if not chunk.empty else "",
                "trade_date_max": str(chunk["trade_date"].max()) if not chunk.empty else "",
                "contract_count": int(chunk["contract_symbol"].nunique()) if not chunk.empty else 0,
            }
        )
    return chunks


def build_opra_backfill_request_package(
    *,
    quote_manifest_csv: Path,
    output_dir: Path,
    max_windows_per_chunk: int,
    window_padding_seconds: float,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = pd.read_csv(quote_manifest_csv, low_memory=False)
    required = {
        "underlying",
        "trade_date",
        "contract_symbol",
        "decision_time_utc",
        "requested_window_start_utc",
        "requested_window_end_utc",
    }
    missing = sorted(required - set(manifest.columns))
    if missing:
        raise ValueError(f"quote manifest missing required columns: {missing}")

    windows = _window_rows(manifest, window_padding_seconds=window_padding_seconds)
    windows_csv = output_dir / "opra_backfill_request_windows.csv"
    windows.to_csv(windows_csv, index=False)
    chunks = _write_chunks(windows, output_dir, max_windows_per_chunk)
    chunk_manifest_csv = output_dir / "opra_backfill_request_chunks.csv"
    pd.DataFrame(chunks).to_csv(chunk_manifest_csv, index=False)

    vendor_readme = output_dir / "README_external_opra_backfill.md"
    vendor_readme.write_text(
        "\n".join(
            [
                "# External OPRA BBO Backfill Request",
                "",
                "Use `opra_backfill_request_windows.csv` or the files under `chunks/` to request historical OPRA/NBBO quotes.",
                "Required output columns for normalization: `option_symbol`, `event_time_utc`, `bid`, `ask`, `bid_size`, `ask_size`.",
                "Normalize vendor CSVs with `scripts/build_external_opra_quote_sidecar.py --acquisition-manifest-csv <quote_manifest.csv>`.",
                "Then run `scripts/audit_quote_manifest_backfill.py` and require quote-backed coverage before optimizer or promotion use.",
                "",
            ]
        ),
        encoding="utf-8",
    )

    summary = {
        "status": "opra_backfill_request_package_built",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "quote_manifest_csv": str(quote_manifest_csv),
        "request_windows_csv": str(windows_csv),
        "chunk_manifest_csv": str(chunk_manifest_csv),
        "vendor_readme": str(vendor_readme),
        "manifest_event_count": int(len(manifest)),
        "request_window_count": int(len(windows)),
        "contract_count": int(windows["contract_symbol"].nunique()) if not windows.empty else 0,
        "trade_date_count": int(windows["trade_date"].nunique()) if not windows.empty else 0,
        "trade_date_min": str(windows["trade_date"].min()) if not windows.empty else "",
        "trade_date_max": str(windows["trade_date"].max()) if not windows.empty else "",
        "max_windows_per_chunk": max_windows_per_chunk,
        "chunk_count": int(len(chunks)),
        "chunks": chunks,
        "window_padding_seconds": window_padding_seconds,
    }
    summary_json = output_dir / "opra_backfill_request_summary.json"
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> int:
    args = parse_args()
    summary = build_opra_backfill_request_package(
        quote_manifest_csv=Path(args.quote_manifest_csv),
        output_dir=Path(args.output_dir),
        max_windows_per_chunk=args.max_windows_per_chunk,
        window_padding_seconds=args.window_padding_seconds,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

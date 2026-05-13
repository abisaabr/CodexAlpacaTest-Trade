from __future__ import annotations

import argparse
import json
import math
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Strictly audit OPRA quote sidecar coverage against quote_manifest.csv. "
            "This is research-only and never submits orders."
        )
    )
    parser.add_argument("--quote-manifest-csv", required=True)
    parser.add_argument("--quote-sidecar-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-quote-age-seconds", type=float, default=60.0)
    parser.add_argument("--max-relative-spread", type=float, default=0.25)
    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class QuoteIndex:
    frame: pd.DataFrame
    times: list[pd.Timestamp]


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


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _iso(timestamp: pd.Timestamp | None) -> str:
    return "" if timestamp is None else timestamp.isoformat()


def _load_quotes(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        quotes = pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    return quotes


def _build_quote_index(quotes: pd.DataFrame) -> dict[str, QuoteIndex]:
    if quotes.empty:
        return {}
    symbol_column = "option_symbol" if "option_symbol" in quotes.columns else "symbol"
    if symbol_column not in quotes.columns or "event_time_utc" not in quotes.columns:
        raise ValueError("quote sidecar must contain option_symbol or symbol plus event_time_utc")
    frame = quotes.copy()
    frame["_symbol"] = frame[symbol_column].map(_clean_symbol)
    frame["_event_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["_event_ts"])
    frame = frame[frame["_symbol"] != ""]
    output: dict[str, QuoteIndex] = {}
    for symbol, group in frame.sort_values(["_symbol", "_event_ts"]).groupby("_symbol"):
        group = group.reset_index(drop=True)
        output[str(symbol)] = QuoteIndex(frame=group, times=list(group["_event_ts"]))
    return output


def _quote_prices(quote: pd.Series) -> tuple[float | None, float | None]:
    bid = _safe_float(quote.get("bid", quote.get("bid_price")))
    ask = _safe_float(quote.get("ask", quote.get("ask_price")))
    return bid, ask


def _audit_row(
    row: pd.Series,
    *,
    quote_index: dict[str, QuoteIndex],
    max_quote_age_seconds: float,
    max_relative_spread: float,
) -> dict[str, Any]:
    contract_symbol = _clean_symbol(row.get("contract_symbol"))
    decision_time = _timestamp(row.get("decision_time_utc"))
    window_start = _timestamp(row.get("requested_window_start_utc"))
    window_end = _timestamp(row.get("requested_window_end_utc"))
    base = {
        "underlying": _clean_symbol(row.get("underlying")),
        "trade_date": row.get("trade_date", ""),
        "contract_symbol": contract_symbol,
        "event_side": row.get("event_side", ""),
        "decision_time_utc": _iso(decision_time),
        "requested_window_start_utc": _iso(window_start),
        "requested_window_end_utc": _iso(window_end),
        "candidate_variant_id": row.get("candidate_variant_id", ""),
        "strategy_id": row.get("strategy_id", ""),
        "family": row.get("family", ""),
        "intended_regime": row.get("intended_regime", ""),
        "leg_index": row.get("leg_index", ""),
        "matched_quote_time_utc": "",
        "matched_bid": "",
        "matched_ask": "",
        "matched_mid": "",
        "matched_absolute_spread": "",
        "matched_relative_spread": "",
        "matched_quote_age_seconds": "",
    }
    if not contract_symbol:
        return {**base, "audit_status": "missing_contract_symbol"}
    if decision_time is None:
        return {**base, "audit_status": "missing_decision_time"}
    symbol_quotes = quote_index.get(contract_symbol)
    if symbol_quotes is None:
        return {**base, "audit_status": "contract_not_in_sidecar"}

    position = bisect_right(symbol_quotes.times, decision_time) - 1
    if position < 0:
        return {**base, "audit_status": "no_quote_before_decision"}
    quote = symbol_quotes.frame.iloc[position]
    quote_time = _timestamp(quote.get("event_time_utc"))
    if quote_time is None:
        return {**base, "audit_status": "missing_quote_time"}
    if window_start is not None and quote_time < window_start:
        return {**base, "matched_quote_time_utc": _iso(quote_time), "audit_status": "no_quote_in_requested_window"}
    if window_end is not None and quote_time > window_end:
        return {**base, "matched_quote_time_utc": _iso(quote_time), "audit_status": "quote_after_requested_window"}

    bid, ask = _quote_prices(quote)
    if bid is None or ask is None or not (bid > 0.0 and ask >= bid):
        return {**base, "matched_quote_time_utc": _iso(quote_time), "audit_status": "invalid_bid_ask"}
    mid = (bid + ask) / 2.0
    if mid <= 0.0:
        return {**base, "matched_quote_time_utc": _iso(quote_time), "audit_status": "invalid_mid"}
    age_seconds = max((decision_time - quote_time).total_seconds(), 0.0)
    absolute_spread = ask - bid
    relative_spread = absolute_spread / mid
    output = {
        **base,
        "matched_quote_time_utc": _iso(quote_time),
        "matched_bid": bid,
        "matched_ask": ask,
        "matched_mid": round(mid, 8),
        "matched_absolute_spread": round(absolute_spread, 8),
        "matched_relative_spread": round(relative_spread, 8),
        "matched_quote_age_seconds": round(age_seconds, 6),
    }
    if age_seconds > max_quote_age_seconds:
        return {**output, "audit_status": "stale_quote"}
    if relative_spread > max_relative_spread:
        return {**output, "audit_status": "wide_spread"}
    return {**output, "audit_status": "quote_backed_event"}


def audit_quote_manifest_backfill(
    *,
    quote_manifest_csv: Path,
    quote_sidecar_csv: Path,
    output_dir: Path,
    max_quote_age_seconds: float,
    max_relative_spread: float,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = pd.read_csv(quote_manifest_csv, low_memory=False)
    required = {
        "contract_symbol",
        "decision_time_utc",
        "requested_window_start_utc",
        "requested_window_end_utc",
    }
    missing = sorted(required - set(manifest.columns))
    if missing:
        raise ValueError(f"quote manifest missing required columns: {missing}")

    quotes = _load_quotes(quote_sidecar_csv)
    quote_index = _build_quote_index(quotes)
    audit_rows = [
        _audit_row(
            row,
            quote_index=quote_index,
            max_quote_age_seconds=max_quote_age_seconds,
            max_relative_spread=max_relative_spread,
        )
        for _, row in manifest.iterrows()
    ]
    audit_frame = pd.DataFrame(audit_rows)
    audit_csv = output_dir / "quote_manifest_backfill_audit_rows.csv"
    audit_frame.to_csv(audit_csv, index=False)

    status_counts = Counter()
    symbol_counts = Counter()
    family_counts = Counter()
    if not audit_frame.empty:
        status_counts.update(audit_frame["audit_status"].fillna("unknown").astype(str))
        symbol_counts.update(audit_frame["underlying"].fillna("unknown").astype(str))
        family_counts.update(audit_frame["family"].fillna("unknown").astype(str))

    quote_backed_count = int(status_counts.get("quote_backed_event", 0))
    manifest_event_count = int(len(audit_frame))
    summary = {
        "status": "quote_manifest_backfill_audit_complete",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "quote_manifest_csv": str(quote_manifest_csv),
        "quote_sidecar_csv": str(quote_sidecar_csv),
        "audit_rows_csv": str(audit_csv),
        "manifest_event_count": manifest_event_count,
        "quote_sidecar_row_count": int(len(quotes)),
        "quote_sidecar_contract_count": int(len(quote_index)),
        "quote_backed_event_count": quote_backed_count,
        "quote_backed_event_rate": round(quote_backed_count / manifest_event_count, 8)
        if manifest_event_count
        else 0.0,
        "strict_all_events_quote_backed": bool(manifest_event_count > 0 and quote_backed_count == manifest_event_count),
        "max_quote_age_seconds": max_quote_age_seconds,
        "max_relative_spread": max_relative_spread,
        "audit_status_counts": dict(sorted(status_counts.items())),
        "underlying_counts": dict(sorted(symbol_counts.items())),
        "family_counts": dict(sorted(family_counts.items())),
    }
    summary_json = output_dir / "quote_manifest_backfill_audit_summary.json"
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> int:
    args = parse_args()
    summary = audit_quote_manifest_backfill(
        quote_manifest_csv=Path(args.quote_manifest_csv),
        quote_sidecar_csv=Path(args.quote_sidecar_csv),
        output_dir=Path(args.output_dir),
        max_quote_age_seconds=args.max_quote_age_seconds,
        max_relative_spread=args.max_relative_spread,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

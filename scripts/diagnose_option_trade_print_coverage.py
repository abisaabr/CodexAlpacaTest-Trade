from __future__ import annotations

import argparse
import json
import math
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Diagnose historical option trade-print coverage for quote acquisition "
            "manifest events. Research-only; never submits orders."
        )
    )
    parser.add_argument("--quote-acquisition-manifest-csv", required=True)
    parser.add_argument("--option-trades-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--underlying", action="append", default=[])
    parser.add_argument("--forward-window-seconds", type=float, default=60.0)
    parser.add_argument("--prior-window-seconds", type=float, default=60.0)
    parser.add_argument("--write-event-detail", action="store_true")
    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class TradeIndex:
    frame: pd.DataFrame
    times: list[pd.Timestamp]


def _timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    try:
        timestamp = pd.Timestamp(value)
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


def _load_trade_index(path: Path) -> dict[str, TradeIndex]:
    trades = pd.read_csv(path, low_memory=False)
    if trades.empty:
        return {}
    required = {"option_symbol", "event_time_utc"}
    missing = sorted(required.difference(trades.columns))
    if missing:
        raise ValueError(f"option trades CSV missing required columns: {missing}")
    frame = trades.copy()
    frame["option_symbol"] = frame["option_symbol"].map(_clean_symbol)
    frame["_event_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["_event_ts"])
    output: dict[str, TradeIndex] = {}
    for symbol, group in frame.sort_values(["option_symbol", "_event_ts"]).groupby("option_symbol"):
        group = group.reset_index(drop=True)
        output[str(symbol)] = TradeIndex(frame=group, times=list(group["_event_ts"]))
    return output


def _count_between(times: list[pd.Timestamp], start: pd.Timestamp | None, end: pd.Timestamp | None) -> int:
    if start is None or end is None:
        return 0
    return max(0, bisect_right(times, end) - bisect_left(times, start))


def _latest_before(index: TradeIndex | None, decision_time: pd.Timestamp | None) -> tuple[pd.Timestamp | None, float | None]:
    if index is None or decision_time is None:
        return None, None
    position = bisect_right(index.times, decision_time) - 1
    if position < 0:
        return None, None
    timestamp = index.times[position]
    return timestamp, max((decision_time - timestamp).total_seconds(), 0.0)


def diagnose_option_trade_print_coverage(
    *,
    quote_acquisition_manifest_csv: Path,
    option_trades_csv: Path,
    output_dir: Path,
    underlyings: set[str],
    forward_window_seconds: float,
    prior_window_seconds: float,
    write_event_detail: bool = False,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = pd.read_csv(quote_acquisition_manifest_csv, low_memory=False)
    if underlyings and "underlying" in manifest.columns:
        manifest = manifest[manifest["underlying"].map(_clean_symbol).isin(underlyings)]
    trade_index = _load_trade_index(option_trades_csv)

    rows: list[dict[str, Any]] = []
    for _, row in manifest.iterrows():
        symbol = _clean_symbol(row.get("contract_symbol"))
        decision_time = _timestamp(row.get("decision_time_utc"))
        requested_start = _timestamp(row.get("requested_window_start_utc"))
        requested_end = _timestamp(row.get("requested_window_end_utc"))
        forward_end = (
            decision_time + pd.Timedelta(seconds=forward_window_seconds)
            if decision_time is not None
            else None
        )
        prior_start = (
            decision_time - pd.Timedelta(seconds=prior_window_seconds)
            if decision_time is not None
            else None
        )
        index = trade_index.get(symbol)
        times = index.times if index else []
        latest_before, latest_before_age = _latest_before(index, decision_time)
        requested_count = _count_between(times, requested_start, requested_end)
        forward_count = _count_between(times, decision_time, forward_end)
        prior_count = _count_between(times, prior_start, decision_time)
        rows.append(
            {
                "underlying": _clean_symbol(row.get("underlying")),
                "trade_date": row.get("trade_date"),
                "contract_symbol": symbol,
                "event_side": row.get("event_side"),
                "decision_time_utc": row.get("decision_time_utc"),
                "family": row.get("family"),
                "intended_regime": row.get("intended_regime"),
                "strategy_id": row.get("strategy_id"),
                "candidate_variant_id": row.get("candidate_variant_id"),
                "requested_window_trade_print_count": requested_count,
                "forward_trade_print_count": forward_count,
                "prior_trade_print_count": prior_count,
                "latest_trade_before_decision_utc": latest_before.isoformat() if latest_before is not None else "",
                "latest_trade_before_decision_age_seconds": latest_before_age,
                "has_requested_window_trade": requested_count > 0,
                "has_forward_trade": forward_count > 0,
                "has_recent_prior_trade": latest_before_age is not None and latest_before_age <= prior_window_seconds,
                "trade_print_coverage_status": (
                    "forward_and_prior_prints"
                    if forward_count > 0 and latest_before_age is not None and latest_before_age <= prior_window_seconds
                    else "forward_print_only"
                    if forward_count > 0
                    else "recent_prior_print_only"
                    if latest_before_age is not None and latest_before_age <= prior_window_seconds
                    else "requested_window_print_only"
                    if requested_count > 0
                    else "no_trade_print_near_decision"
                ),
            }
        )

    event_frame = pd.DataFrame(rows)
    event_detail_path = output_dir / "option_trade_print_event_coverage.csv"
    if write_event_detail:
        event_frame.to_csv(event_detail_path, index=False)

    group_columns = ["underlying", "event_side", "family", "trade_print_coverage_status"]
    status_summary = (
        event_frame.groupby(group_columns, dropna=False)
        .size()
        .reset_index(name="event_count")
        if not event_frame.empty
        else pd.DataFrame(columns=[*group_columns, "event_count"])
    )
    status_summary_path = output_dir / "option_trade_print_coverage_by_family.csv"
    status_summary.to_csv(status_summary_path, index=False)

    contract_summary = (
        event_frame.groupby(["underlying", "trade_date", "contract_symbol"], dropna=False)
        .agg(
            event_count=("contract_symbol", "size"),
            requested_window_trade_print_count=("requested_window_trade_print_count", "sum"),
            forward_trade_print_count=("forward_trade_print_count", "sum"),
            prior_trade_print_count=("prior_trade_print_count", "sum"),
            events_with_forward_trade=("has_forward_trade", "sum"),
            events_with_recent_prior_trade=("has_recent_prior_trade", "sum"),
        )
        .reset_index()
        if not event_frame.empty
        else pd.DataFrame()
    )
    contract_summary_path = output_dir / "option_trade_print_coverage_by_contract_date.csv"
    contract_summary.to_csv(contract_summary_path, index=False)

    status_counts = Counter()
    if not event_frame.empty:
        status_counts.update(event_frame["trade_print_coverage_status"].astype(str))
    summary = {
        "status": "option_trade_print_coverage_diagnostic_complete",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "quote_acquisition_manifest_csv": str(quote_acquisition_manifest_csv),
        "option_trades_csv": str(option_trades_csv),
        "output_dir": str(output_dir),
        "underlyings": sorted(underlyings) if underlyings else "all",
        "manifest_event_count": int(len(event_frame)),
        "trade_print_contract_count": int(len(trade_index)),
        "events_with_requested_window_trade": int(event_frame["has_requested_window_trade"].sum())
        if not event_frame.empty
        else 0,
        "events_with_forward_trade": int(event_frame["has_forward_trade"].sum()) if not event_frame.empty else 0,
        "events_with_recent_prior_trade": int(event_frame["has_recent_prior_trade"].sum())
        if not event_frame.empty
        else 0,
        "coverage_status_counts": dict(sorted(status_counts.items())),
        "family_summary_csv": str(status_summary_path),
        "contract_date_summary_csv": str(contract_summary_path),
        "event_detail_csv": str(event_detail_path) if write_event_detail else "",
        "lineage_note": (
            "Trade prints diagnose contract liquidity around replay decisions. They do not "
            "supply OPRA bid/ask spread or quote-age evidence."
        ),
    }
    summary_path = output_dir / "option_trade_print_coverage_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> int:
    args = parse_args()
    summary = diagnose_option_trade_print_coverage(
        quote_acquisition_manifest_csv=Path(args.quote_acquisition_manifest_csv),
        option_trades_csv=Path(args.option_trades_csv),
        output_dir=Path(args.output_dir),
        underlyings={_clean_symbol(value) for value in args.underlying if _clean_symbol(value)},
        forward_window_seconds=args.forward_window_seconds,
        prior_window_seconds=args.prior_window_seconds,
        write_event_detail=args.write_event_detail,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

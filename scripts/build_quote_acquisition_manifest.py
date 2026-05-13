from __future__ import annotations

import argparse
import json
import math
import re
import sys
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


OCC_SYMBOL_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an OPRA/SIP quote acquisition manifest from option-aware replay "
            "trade economics. This is research-only and never submits orders."
        )
    )
    parser.add_argument("--trade-economics-root", action="append", default=[])
    parser.add_argument("--trade-economics-csv", action="append", default=[])
    parser.add_argument("--quote-sidecar-csv", default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--underlying", action="append", default=[])
    parser.add_argument("--window-before-seconds", type=float, default=300.0)
    parser.add_argument("--window-after-seconds", type=float, default=60.0)
    parser.add_argument("--max-quote-age-seconds", type=float, default=60.0)
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Drop leg events already covered by a fresh sidecar quote.",
    )
    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class QuoteSidecarIndex:
    by_symbol: dict[str, list[pd.Timestamp]]
    contract_dates: set[tuple[str, str]]
    underlying_dates: set[tuple[str, str]]
    min_time: pd.Timestamp | None
    max_time: pd.Timestamp | None


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


def _compact_symbol(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", _clean_symbol(value))


def _occ_parts(symbol: str) -> dict[str, Any]:
    compact = _compact_symbol(symbol)
    match = OCC_SYMBOL_RE.match(compact)
    if not match:
        return {
            "underlying": "",
            "expiration": "",
            "option_type": "",
            "strike": None,
            "compact_symbol": compact,
        }
    underlying, expiry_raw, option_type, strike_raw = match.groups()
    return {
        "underlying": underlying,
        "expiration": f"20{expiry_raw[:2]}-{expiry_raw[2:4]}-{expiry_raw[4:6]}",
        "option_type": option_type,
        "strike": int(strike_raw) / 1000.0,
        "compact_symbol": compact,
    }


def _contract_symbols(row: pd.Series) -> list[str]:
    raw = _clean_symbol(row.get("contract_symbol"))
    if not raw:
        return []
    return [_clean_symbol(item) for item in raw.replace(",", ";").split(";") if _clean_symbol(item)]


def _input_csvs(roots: list[Path], csvs: list[Path]) -> list[Path]:
    output = []
    output.extend(path for path in csvs if path.exists())
    for root in roots:
        if root.exists():
            output.extend(sorted(root.rglob("option_aware_trade_economics.csv")))
    return sorted(dict.fromkeys(output))


def _load_quote_sidecar(path: Path | None) -> QuoteSidecarIndex | None:
    if path is None or not path.exists():
        return None
    quotes = pd.read_csv(path, low_memory=False)
    if quotes.empty:
        return QuoteSidecarIndex({}, set(), set(), None, None)
    symbol_column = "option_symbol" if "option_symbol" in quotes.columns else "symbol"
    if symbol_column not in quotes.columns or "event_time_utc" not in quotes.columns:
        raise ValueError("quote sidecar must contain option_symbol or symbol plus event_time_utc")

    frame = quotes[[symbol_column, "event_time_utc"]].copy()
    frame["_symbol"] = frame[symbol_column].map(_clean_symbol)
    frame["_event_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["_event_ts"])
    frame = frame[frame["_symbol"] != ""]

    by_symbol: dict[str, list[pd.Timestamp]] = {}
    contract_dates: set[tuple[str, str]] = set()
    underlying_dates: set[tuple[str, str]] = set()
    for symbol, group in frame.sort_values(["_symbol", "_event_ts"]).groupby("_symbol"):
        times = list(group["_event_ts"])
        by_symbol[str(symbol)] = times
        parts = _occ_parts(str(symbol))
        underlying = str(parts.get("underlying") or "")
        for timestamp in times:
            date = str(timestamp.date())
            contract_dates.add((str(symbol), date))
            if underlying:
                underlying_dates.add((underlying, date))
    all_times = frame["_event_ts"]
    return QuoteSidecarIndex(
        by_symbol=by_symbol,
        contract_dates=contract_dates,
        underlying_dates=underlying_dates,
        min_time=all_times.min(),
        max_time=all_times.max(),
    )


def _sidecar_status(
    *,
    sidecar: QuoteSidecarIndex | None,
    contract_symbol: str,
    underlying: str,
    decision_time: pd.Timestamp | None,
    max_quote_age_seconds: float,
) -> dict[str, Any]:
    if sidecar is None:
        return {"sidecar_coverage_status": "not_checked"}
    if decision_time is None:
        return {"sidecar_coverage_status": "missing_decision_time"}
    decision_date = str(decision_time.date())
    if (underlying, decision_date) not in sidecar.underlying_dates:
        return {"sidecar_coverage_status": "trade_date_not_in_sidecar"}
    if (contract_symbol, decision_date) not in sidecar.contract_dates:
        return {"sidecar_coverage_status": "contract_not_in_sidecar_on_date"}
    times = sidecar.by_symbol.get(contract_symbol, [])
    position = bisect_right(times, decision_time) - 1
    if position < 0:
        return {"sidecar_coverage_status": "no_quote_before_decision"}
    latest = times[position]
    age_seconds = (decision_time - latest).total_seconds()
    status = "covered_fresh" if age_seconds <= max_quote_age_seconds else "quote_before_decision_stale"
    return {
        "sidecar_coverage_status": status,
        "sidecar_latest_quote_time_utc": latest.isoformat(),
        "sidecar_quote_age_seconds": round(age_seconds, 6),
    }


def _row_value(row: pd.Series, key: str) -> Any:
    value = row.get(key)
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return value


def _load_leg_details(row: pd.Series) -> list[dict[str, Any]]:
    raw = _row_value(row, "leg_details_json")
    if raw:
        try:
            parsed = json.loads(str(raw))
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list) and parsed:
            return [leg for leg in parsed if isinstance(leg, dict)]

    legs = []
    for index, contract in enumerate(_contract_symbols(row)):
        legs.append(
            {
                "contract_symbol": contract,
                "role": "",
                "side": "",
                "ratio": "",
                "relative_strike_step": str(index),
            }
        )
    return legs


def _event_time(row: pd.Series, leg: dict[str, Any], event_side: str) -> pd.Timestamp | None:
    if event_side == "entry":
        return _timestamp(
            leg.get("entry_time")
            or leg.get("entry_quote_time")
            or row.get("option_entry_time")
            or row.get("stock_entry_time")
        )
    if event_side == "exit":
        return _timestamp(
            leg.get("exit_time")
            or leg.get("exit_quote_time")
            or row.get("option_exit_time")
            or row.get("stock_exit_time")
        )
    raise ValueError(f"unknown event side: {event_side}")


def _iso(timestamp: pd.Timestamp | None) -> str:
    return "" if timestamp is None else timestamp.isoformat()


def _build_event_rows(
    *,
    csv_path: Path,
    trades: pd.DataFrame,
    underlyings: set[str],
    sidecar: QuoteSidecarIndex | None,
    window_before_seconds: float,
    window_after_seconds: float,
    max_quote_age_seconds: float,
    only_missing: bool,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row_index, row in trades.iterrows():
        row_symbol = _clean_symbol(row.get("symbol"))
        if underlyings and row_symbol not in underlyings:
            continue
        for leg_index, leg in enumerate(_load_leg_details(row)):
            contract_symbol = _clean_symbol(leg.get("contract_symbol"))
            if not contract_symbol:
                continue
            parts = _occ_parts(contract_symbol)
            underlying = str(parts.get("underlying") or row_symbol)
            if underlyings and underlying not in underlyings:
                continue
            for event_side in ("entry", "exit"):
                decision_time = _event_time(row, leg, event_side)
                coverage = _sidecar_status(
                    sidecar=sidecar,
                    contract_symbol=contract_symbol,
                    underlying=underlying,
                    decision_time=decision_time,
                    max_quote_age_seconds=max_quote_age_seconds,
                )
                if only_missing and coverage.get("sidecar_coverage_status") == "covered_fresh":
                    continue
                window_start = (
                    decision_time - pd.Timedelta(seconds=window_before_seconds)
                    if decision_time is not None
                    else None
                )
                window_end = (
                    decision_time + pd.Timedelta(seconds=window_after_seconds)
                    if decision_time is not None
                    else None
                )
                output.append(
                    {
                        "underlying": underlying,
                        "trade_date": str(decision_time.date()) if decision_time is not None else "",
                        "contract_symbol": contract_symbol,
                        "contract_compact_symbol": str(parts.get("compact_symbol") or ""),
                        "expiration": str(parts.get("expiration") or ""),
                        "option_type": str(parts.get("option_type") or ""),
                        "strike": parts.get("strike"),
                        "event_side": event_side,
                        "decision_time_utc": _iso(decision_time),
                        "requested_window_start_utc": _iso(window_start),
                        "requested_window_end_utc": _iso(window_end),
                        "candidate_variant_id": _row_value(row, "candidate_variant_id"),
                        "strategy_id": _row_value(row, "strategy_id"),
                        "source_strategy_id": _row_value(row, "source_strategy_id"),
                        "family": _row_value(row, "family"),
                        "intended_regime": _row_value(row, "intended_regime"),
                        "option_structure": _row_value(row, "option_structure"),
                        "leg_index": leg_index,
                        "leg_role": leg.get("role", ""),
                        "leg_side": leg.get("side", ""),
                        "leg_ratio": leg.get("ratio", ""),
                        "leg_relative_strike_step": leg.get("relative_strike_step", ""),
                        "replay_quote_source": leg.get(f"{event_side}_quote_source")
                        or _row_value(row, f"{event_side}_quote_source"),
                        "replay_quote_age_seconds": leg.get(f"{event_side}_quote_age_seconds")
                        or _row_value(row, f"{event_side}_quote_age_seconds"),
                        "replay_option_bar_lag_seconds": leg.get(f"{event_side}_option_bar_lag_seconds")
                        or _row_value(row, f"{event_side}_option_bar_lag_seconds"),
                        "source_csv": str(csv_path),
                        "source_row_index": int(row_index),
                        **coverage,
                    }
                )
    return output


def _write_aggregates(events: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    if events.empty:
        contract_dates = pd.DataFrame()
        date_summary = pd.DataFrame()
    else:
        grouped = events.groupby(["underlying", "trade_date", "contract_symbol"], dropna=False)
        contract_dates = grouped.agg(
            event_count=("event_side", "size"),
            first_decision_time_utc=("decision_time_utc", "min"),
            last_decision_time_utc=("decision_time_utc", "max"),
            requested_window_start_utc=("requested_window_start_utc", "min"),
            requested_window_end_utc=("requested_window_end_utc", "max"),
            strategy_count=("strategy_id", "nunique"),
            candidate_count=("candidate_variant_id", "nunique"),
        ).reset_index()
        date_summary = events.groupby(["underlying", "trade_date"], dropna=False).agg(
            event_count=("event_side", "size"),
            contract_count=("contract_symbol", "nunique"),
            strategy_count=("strategy_id", "nunique"),
            candidate_count=("candidate_variant_id", "nunique"),
            requested_window_start_utc=("requested_window_start_utc", "min"),
            requested_window_end_utc=("requested_window_end_utc", "max"),
        ).reset_index()

    contract_dates_path = output_dir / "quote_acquisition_contract_dates.csv"
    date_summary_path = output_dir / "quote_acquisition_date_summary.csv"
    contract_dates.to_csv(contract_dates_path, index=False)
    date_summary.to_csv(date_summary_path, index=False)
    return {
        "contract_dates_csv": str(contract_dates_path),
        "date_summary_csv": str(date_summary_path),
        "contract_date_count": int(len(contract_dates)),
        "date_summary_count": int(len(date_summary)),
    }


def build_quote_acquisition_manifest(
    *,
    trade_economics_roots: list[Path],
    trade_economics_csvs: list[Path],
    quote_sidecar_csv: Path | None,
    output_dir: Path,
    underlyings: set[str],
    window_before_seconds: float,
    window_after_seconds: float,
    max_quote_age_seconds: float,
    only_missing: bool,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csvs = _input_csvs(trade_economics_roots, trade_economics_csvs)
    sidecar = _load_quote_sidecar(quote_sidecar_csv)
    events: list[dict[str, Any]] = []
    input_row_count = 0
    skipped_csvs = []
    for csv_path in csvs:
        try:
            trades = pd.read_csv(csv_path, low_memory=False)
        except (pd.errors.EmptyDataError, UnicodeDecodeError) as exc:
            skipped_csvs.append({"path": str(csv_path), "reason": type(exc).__name__})
            continue
        input_row_count += int(len(trades))
        events.extend(
            _build_event_rows(
                csv_path=csv_path,
                trades=trades,
                underlyings=underlyings,
                sidecar=sidecar,
                window_before_seconds=window_before_seconds,
                window_after_seconds=window_after_seconds,
                max_quote_age_seconds=max_quote_age_seconds,
                only_missing=only_missing,
            )
        )

    event_frame = pd.DataFrame(events)
    manifest_csv = output_dir / "quote_acquisition_manifest.csv"
    event_frame.to_csv(manifest_csv, index=False)
    aggregate_paths = _write_aggregates(event_frame, output_dir)

    coverage_counts = Counter()
    quote_source_counts = Counter()
    family_counts = Counter()
    symbol_counts = Counter()
    if not event_frame.empty:
        coverage_counts.update(event_frame["sidecar_coverage_status"].fillna("unknown").astype(str))
        quote_source_counts.update(event_frame["replay_quote_source"].fillna("unknown").astype(str))
        family_counts.update(event_frame["family"].fillna("unknown").astype(str))
        symbol_counts.update(event_frame["underlying"].fillna("unknown").astype(str))

    summary = {
        "status": "quote_acquisition_manifest_built",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "trade_economics_csv_count": len(csvs),
        "skipped_csvs": skipped_csvs,
        "input_trade_row_count": input_row_count,
        "manifest_event_count": int(len(event_frame)),
        "underlyings": sorted(underlyings) if underlyings else "all",
        "window_before_seconds": window_before_seconds,
        "window_after_seconds": window_after_seconds,
        "max_quote_age_seconds": max_quote_age_seconds,
        "only_missing": only_missing,
        "quote_sidecar_csv": str(quote_sidecar_csv) if quote_sidecar_csv else "",
        "quote_sidecar_min_time_utc": _iso(sidecar.min_time) if sidecar else "",
        "quote_sidecar_max_time_utc": _iso(sidecar.max_time) if sidecar else "",
        "manifest_csv": str(manifest_csv),
        "coverage_status_counts": dict(sorted(coverage_counts.items())),
        "replay_quote_source_counts": dict(sorted(quote_source_counts.items())),
        "family_counts": dict(sorted(family_counts.items())),
        "underlying_counts": dict(sorted(symbol_counts.items())),
        **aggregate_paths,
    }
    summary_path = output_dir / "quote_acquisition_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> int:
    args = parse_args()
    underlyings = {_clean_symbol(value) for value in args.underlying if _clean_symbol(value)}
    summary = build_quote_acquisition_manifest(
        trade_economics_roots=[Path(value) for value in args.trade_economics_root],
        trade_economics_csvs=[Path(value) for value in args.trade_economics_csv],
        quote_sidecar_csv=Path(args.quote_sidecar_csv) if args.quote_sidecar_csv else None,
        output_dir=Path(args.output_dir),
        underlyings=underlyings,
        window_before_seconds=args.window_before_seconds,
        window_after_seconds=args.window_after_seconds,
        max_quote_age_seconds=args.max_quote_age_seconds,
        only_missing=args.only_missing,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

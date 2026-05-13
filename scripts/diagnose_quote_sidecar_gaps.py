from __future__ import annotations

import argparse
import json
import math
import re
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


OCC_SYMBOL_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


ROOT_CAUSE_GUIDANCE: dict[str, dict[str, Any]] = {
    "matched_quote": {
        "root_cause_class": "matched",
        "recommended_action": "none",
        "blocks_quote_backed_replay": False,
    },
    "missing_contract_symbol": {
        "root_cause_class": "replay_generation",
        "recommended_action": "repair_replay_contract_symbol_or_leg_details",
        "blocks_quote_backed_replay": True,
    },
    "missing_decision_time": {
        "root_cause_class": "replay_generation",
        "recommended_action": "repair_replay_entry_exit_timestamps",
        "blocks_quote_backed_replay": True,
    },
    "trade_date_not_in_sidecar": {
        "root_cause_class": "quote_capture_window",
        "recommended_action": "capture_or_fetch_opra_quotes_for_trade_date",
        "blocks_quote_backed_replay": True,
    },
    "symbol_normalization_mismatch": {
        "root_cause_class": "symbol_normalization",
        "recommended_action": "normalize_occ_symbol_format_before_join",
        "blocks_quote_backed_replay": True,
    },
    "contract_not_in_sidecar": {
        "root_cause_class": "quote_capture_universe",
        "recommended_action": "expand_runtime_leg_or_replay_contract_quote_capture_universe",
        "blocks_quote_backed_replay": True,
    },
    "contract_trade_date_not_in_sidecar": {
        "root_cause_class": "quote_capture_contract_date",
        "recommended_action": "backfill_contract_quotes_for_trade_date",
        "blocks_quote_backed_replay": True,
    },
    "no_quote_before_decision": {
        "root_cause_class": "quote_capture_start_or_decision_time",
        "recommended_action": "capture_earlier_quotes_or_backfill_pre_decision_opra_quotes",
        "blocks_quote_backed_replay": True,
    },
    "invalid_bid_ask": {
        "root_cause_class": "quote_data_quality",
        "recommended_action": "reject_bad_quotes_or_rebuild_sidecar_with_valid_bid_ask",
        "blocks_quote_backed_replay": True,
    },
    "stale_quote": {
        "root_cause_class": "quote_freshness",
        "recommended_action": "tighten_capture_refresh_or_backfill_dense_opra_quotes",
        "blocks_quote_backed_replay": True,
    },
    "partial_multileg_quote": {
        "root_cause_class": "partial_multileg_coverage",
        "recommended_action": "backfill_missing_multileg_legs_before_replay",
        "blocks_quote_backed_replay": True,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Diagnose why replay trade economics rows fail to match OPRA/SIP "
            "quote sidecars. This is research-only and never submits orders."
        )
    )
    parser.add_argument("--trade-economics-root", action="append", default=[])
    parser.add_argument("--trade-economics-csv", action="append", default=[])
    parser.add_argument("--quote-sidecar-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--underlying", action="append", default=[])
    parser.add_argument("--max-quote-age-seconds", type=float, default=60.0)
    parser.add_argument("--max-examples-per-reason", type=int, default=20)
    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class QuoteIndex:
    frame: pd.DataFrame
    times: list[pd.Timestamp]
    dates: set[str]
    min_time: pd.Timestamp
    max_time: pd.Timestamp


def _timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if str(value).strip() == "":
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


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


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
    expiration = f"20{expiry_raw[:2]}-{expiry_raw[2:4]}-{expiry_raw[4:6]}"
    return {
        "underlying": underlying,
        "expiration": expiration,
        "option_type": option_type,
        "strike": int(strike_raw) / 1000.0,
        "compact_symbol": compact,
    }


def _contract_symbols(row: pd.Series) -> list[str]:
    raw = _clean_symbol(row.get("contract_symbol"))
    if not raw:
        return []
    return [_clean_symbol(item) for item in raw.replace(",", ";").split(";") if _clean_symbol(item)]


def _row_underlyings(row: pd.Series) -> set[str]:
    values = {_clean_symbol(row.get("symbol"))}
    for contract in _contract_symbols(row):
        underlying = str(_occ_parts(contract).get("underlying") or "")
        if underlying:
            values.add(underlying)
    return {value for value in values if value}


def _decision_time(row: pd.Series, side: str) -> pd.Timestamp | None:
    if side == "entry":
        return _timestamp(row.get("stock_entry_time") or row.get("option_entry_time"))
    if side == "exit":
        return _timestamp(row.get("stock_exit_time") or row.get("option_exit_time"))
    raise ValueError(f"unknown side: {side}")


def _input_csvs(roots: list[Path], csvs: list[Path]) -> list[Path]:
    output = []
    output.extend(path for path in csvs if path.exists())
    for root in roots:
        if root.exists():
            output.extend(sorted(root.rglob("option_aware_trade_economics.csv")))
    return sorted(dict.fromkeys(output))


def _build_quote_indexes(quote_sidecar_csv: Path) -> tuple[dict[str, QuoteIndex], dict[str, set[str]], dict[str, set[str]], pd.DataFrame]:
    quotes = pd.read_csv(quote_sidecar_csv, low_memory=False)
    if quotes.empty:
        return {}, {}, {}, quotes
    symbol_column = "option_symbol" if "option_symbol" in quotes.columns else "symbol"
    if symbol_column not in quotes.columns or "event_time_utc" not in quotes.columns:
        raise ValueError("quote sidecar must contain option_symbol or symbol plus event_time_utc")
    frame = quotes.copy()
    frame["_event_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True, errors="coerce")
    frame["_symbol_exact"] = frame[symbol_column].map(_clean_symbol)
    frame["_symbol_compact"] = frame[symbol_column].map(_compact_symbol)
    frame["_underlying"] = frame["_symbol_compact"].map(lambda value: str(_occ_parts(value).get("underlying") or ""))
    frame = frame.dropna(subset=["_event_ts"])
    frame = frame[frame["_symbol_exact"] != ""]
    quote_index: dict[str, QuoteIndex] = {}
    compact_to_exact: dict[str, set[str]] = defaultdict(set)
    underlying_dates: dict[str, set[str]] = defaultdict(set)
    for compact, exact in zip(frame["_symbol_compact"], frame["_symbol_exact"], strict=False):
        compact_to_exact[str(compact)].add(str(exact))
    for underlying, group in frame.groupby("_underlying"):
        if underlying:
            underlying_dates[str(underlying)].update(str(ts.date()) for ts in group["_event_ts"])
    for symbol, group in frame.sort_values(["_symbol_exact", "_event_ts"]).groupby("_symbol_exact"):
        group = group.reset_index(drop=True)
        times = list(group["_event_ts"])
        quote_index[str(symbol)] = QuoteIndex(
            frame=group,
            times=times,
            dates={str(ts.date()) for ts in times},
            min_time=times[0],
            max_time=times[-1],
        )
    return quote_index, compact_to_exact, underlying_dates, frame


def _latest_quote(index: QuoteIndex, decision_time: pd.Timestamp) -> pd.Series | None:
    position = bisect_right(index.times, decision_time) - 1
    if position < 0:
        return None
    return index.frame.iloc[position]


def _diagnose_leg(
    *,
    contract_symbol: str,
    decision_time: pd.Timestamp | None,
    quote_index: dict[str, QuoteIndex],
    compact_to_exact: dict[str, set[str]],
    underlying_dates: dict[str, set[str]],
    max_quote_age_seconds: float,
) -> dict[str, Any]:
    symbol = _clean_symbol(contract_symbol)
    compact = _compact_symbol(symbol)
    parts = _occ_parts(symbol)
    underlying = str(parts.get("underlying") or "")
    if not symbol:
        return {"reason": "missing_contract_symbol", "matched": False}
    if decision_time is None:
        return {"reason": "missing_decision_time", "matched": False, "contract_symbol": symbol}
    decision_date = str(decision_time.date())
    if underlying and decision_date not in underlying_dates.get(underlying, set()):
        return {
            "reason": "trade_date_not_in_sidecar",
            "matched": False,
            "contract_symbol": symbol,
            "underlying": underlying,
            "decision_time_utc": decision_time.isoformat(),
        }
    if symbol not in quote_index:
        exact_matches = sorted(compact_to_exact.get(compact, set()))
        if exact_matches:
            reason = "symbol_normalization_mismatch"
        else:
            reason = "contract_not_in_sidecar"
        return {
            "reason": reason,
            "matched": False,
            "contract_symbol": symbol,
            "underlying": underlying,
            "decision_time_utc": decision_time.isoformat(),
            "normalized_matches": ";".join(exact_matches),
        }
    index = quote_index[symbol]
    if decision_date not in index.dates:
        return {
            "reason": "contract_trade_date_not_in_sidecar",
            "matched": False,
            "contract_symbol": symbol,
            "underlying": underlying,
            "decision_time_utc": decision_time.isoformat(),
            "sidecar_min_time_utc": index.min_time.isoformat(),
            "sidecar_max_time_utc": index.max_time.isoformat(),
        }
    quote = _latest_quote(index, decision_time)
    if quote is None:
        return {
            "reason": "no_quote_before_decision",
            "matched": False,
            "contract_symbol": symbol,
            "underlying": underlying,
            "decision_time_utc": decision_time.isoformat(),
            "sidecar_min_time_utc": index.min_time.isoformat(),
        }
    quote_time = _timestamp(quote.get("event_time_utc"))
    bid = _safe_float(quote.get("bid", quote.get("bid_price")))
    ask = _safe_float(quote.get("ask", quote.get("ask_price")))
    if quote_time is None or bid is None or ask is None or bid <= 0.0 or ask < bid:
        return {
            "reason": "invalid_bid_ask",
            "matched": False,
            "contract_symbol": symbol,
            "underlying": underlying,
            "decision_time_utc": decision_time.isoformat(),
        }
    age = max((decision_time - quote_time).total_seconds(), 0.0)
    if age > max_quote_age_seconds:
        return {
            "reason": "stale_quote",
            "matched": False,
            "contract_symbol": symbol,
            "underlying": underlying,
            "decision_time_utc": decision_time.isoformat(),
            "quote_time_utc": quote_time.isoformat(),
            "quote_age_seconds": round(age, 6),
        }
    return {
        "reason": "matched_quote",
        "matched": True,
        "contract_symbol": symbol,
        "underlying": underlying,
        "decision_time_utc": decision_time.isoformat(),
        "quote_time_utc": quote_time.isoformat(),
        "quote_age_seconds": round(age, 6),
        "bid": bid,
        "ask": ask,
    }


def _collapse_side(leg_results: list[dict[str, Any]]) -> dict[str, Any]:
    if not leg_results:
        return {
            "reason": "missing_contract_symbol",
            "leg_count": 0,
            "matched_legs": 0,
            "missing_legs": 0,
            "max_quote_age_seconds": None,
            "leg_reasons": "",
        }
    matched_legs = sum(1 for item in leg_results if item.get("matched"))
    reasons = [str(item.get("reason") or "unknown") for item in leg_results]
    if matched_legs == len(leg_results):
        reason = "matched_quote"
    elif matched_legs:
        reason = "partial_multileg_quote"
    else:
        reason = Counter(reasons).most_common(1)[0][0]
    ages = [
        _safe_float(item.get("quote_age_seconds"))
        for item in leg_results
        if _safe_float(item.get("quote_age_seconds")) is not None
    ]
    return {
        "reason": reason,
        "leg_count": len(leg_results),
        "matched_legs": matched_legs,
        "missing_legs": len(leg_results) - matched_legs,
        "max_quote_age_seconds": round(max(ages), 6) if ages else None,
        "leg_reasons": ";".join(reasons),
    }


def _contract_universe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        for contract in str(row.get("contract_symbol") or "").replace(",", ";").split(";"):
            symbol = _clean_symbol(contract)
            if not symbol:
                continue
            parts = _occ_parts(symbol)
            item = grouped.setdefault(
                symbol,
                {
                    "contract_symbol": symbol,
                    "underlying": parts.get("underlying"),
                    "expiration": parts.get("expiration"),
                    "option_type": parts.get("option_type"),
                    "strike": parts.get("strike"),
                    "row_count": 0,
                    "trade_dates": set(),
                    "first_decision_time_utc": None,
                    "last_decision_time_utc": None,
                },
            )
            item["row_count"] += 1
            for field in ("entry_time_utc", "exit_time_utc"):
                timestamp = _timestamp(row.get(field))
                if timestamp is None:
                    continue
                item["trade_dates"].add(str(timestamp.date()))
                if item["first_decision_time_utc"] is None or timestamp < item["first_decision_time_utc"]:
                    item["first_decision_time_utc"] = timestamp
                if item["last_decision_time_utc"] is None or timestamp > item["last_decision_time_utc"]:
                    item["last_decision_time_utc"] = timestamp
    output = []
    for item in grouped.values():
        output.append(
            {
                **{
                    key: value
                    for key, value in item.items()
                    if key not in {"trade_dates", "first_decision_time_utc", "last_decision_time_utc"}
                },
                "trade_dates": ";".join(sorted(item["trade_dates"])),
                "first_decision_time_utc": item["first_decision_time_utc"].isoformat()
                if item["first_decision_time_utc"] is not None
                else "",
                "last_decision_time_utc": item["last_decision_time_utc"].isoformat()
                if item["last_decision_time_utc"] is not None
                else "",
            }
        )
    return sorted(output, key=lambda item: (str(item.get("underlying") or ""), str(item["contract_symbol"])))


def _sidecar_coverage_rows(quote_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if quote_frame.empty:
        return []
    rows = []
    for symbol, group in quote_frame.groupby("_symbol_exact"):
        parts = _occ_parts(str(symbol))
        rows.append(
            {
                "option_symbol": symbol,
                "underlying": parts.get("underlying"),
                "expiration": parts.get("expiration"),
                "option_type": parts.get("option_type"),
                "strike": parts.get("strike"),
                "quote_count": int(len(group)),
                "quote_dates": ";".join(sorted({str(ts.date()) for ts in group["_event_ts"]})),
                "first_quote_time_utc": group["_event_ts"].min().isoformat(),
                "last_quote_time_utc": group["_event_ts"].max().isoformat(),
            }
        )
    return sorted(rows, key=lambda item: (str(item.get("underlying") or ""), str(item["option_symbol"])))


def _guidance_for_reason(reason: str) -> dict[str, Any]:
    return ROOT_CAUSE_GUIDANCE.get(
        reason,
        {
            "root_cause_class": "unknown",
            "recommended_action": "inspect_quote_gap_examples",
            "blocks_quote_backed_replay": True,
        },
    )


def _unique_contract_examples(group: pd.DataFrame, limit: int = 8) -> str:
    values: list[str] = []
    seen = set()
    for raw in group.get("contract_symbol", pd.Series(dtype=str)).dropna():
        for symbol in str(raw).replace(",", ";").split(";"):
            cleaned = _clean_symbol(symbol)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                values.append(cleaned)
            if len(values) >= limit:
                return ";".join(values)
    return ";".join(values)


def _unique_examples(group: pd.DataFrame, column: str, limit: int = 8) -> str:
    if column not in group.columns:
        return ""
    values = []
    seen = set()
    for raw in group[column].dropna():
        value = str(raw).strip()
        if value and value not in seen:
            seen.add(value)
            values.append(value)
        if len(values) >= limit:
            break
    return ";".join(values)


def _root_cause_action_rows(diagnostics: pd.DataFrame) -> list[dict[str, Any]]:
    if diagnostics.empty:
        return []
    rows: list[dict[str, Any]] = []
    total_rows = max(len(diagnostics), 1)
    for side in ("entry", "exit"):
        reason_column = f"{side}_gap_reason"
        if reason_column not in diagnostics.columns:
            continue
        for reason, group in diagnostics.groupby(reason_column, dropna=False):
            reason_value = str(reason or "unknown")
            guidance = _guidance_for_reason(reason_value)
            rows.append(
                {
                    "side": side,
                    "gap_reason": reason_value,
                    "row_count": int(len(group)),
                    "pct_of_diagnosed_rows": round(float(len(group) / total_rows), 6),
                    "root_cause_class": guidance["root_cause_class"],
                    "recommended_action": guidance["recommended_action"],
                    "blocks_quote_backed_replay": bool(guidance["blocks_quote_backed_replay"]),
                    "example_candidate_variant_ids": _unique_examples(group, "candidate_variant_id"),
                    "example_symbols": _unique_examples(group, "symbol"),
                    "example_contract_symbols": _unique_contract_examples(group),
                    "example_trade_dates": _unique_examples(group, "trade_date"),
                }
            )
    return sorted(
        rows,
        key=lambda item: (
            bool(item["blocks_quote_backed_replay"]) is False,
            -int(item["row_count"]),
            str(item["side"]),
            str(item["gap_reason"]),
        ),
    )


def diagnose_quote_sidecar_gaps(
    *,
    trade_economics_roots: list[Path],
    trade_economics_csvs: list[Path],
    quote_sidecar_csv: Path,
    output_dir: Path,
    underlyings: set[str] | None = None,
    max_quote_age_seconds: float = 60.0,
    max_examples_per_reason: int = 20,
) -> dict[str, Any]:
    underlyings = {item.upper() for item in underlyings or set() if item}
    output_dir.mkdir(parents=True, exist_ok=True)
    quote_index, compact_to_exact, underlying_dates, quote_frame = _build_quote_indexes(quote_sidecar_csv)
    rows: list[dict[str, Any]] = []
    csvs = _input_csvs(trade_economics_roots, trade_economics_csvs)
    for csv_path in csvs:
        try:
            frame = pd.read_csv(csv_path, low_memory=False)
        except (pd.errors.EmptyDataError, UnicodeDecodeError):
            continue
        for _, row in frame.iterrows():
            row_underlyings = _row_underlyings(row)
            if underlyings and not row_underlyings.intersection(underlyings):
                continue
            contracts = _contract_symbols(row)
            entry_time = _decision_time(row, "entry")
            exit_time = _decision_time(row, "exit")
            side_outputs: dict[str, dict[str, Any]] = {}
            for side, decision_time in (("entry", entry_time), ("exit", exit_time)):
                leg_results = [
                    _diagnose_leg(
                        contract_symbol=contract,
                        decision_time=decision_time,
                        quote_index=quote_index,
                        compact_to_exact=compact_to_exact,
                        underlying_dates=underlying_dates,
                        max_quote_age_seconds=max_quote_age_seconds,
                    )
                    for contract in contracts
                ]
                side_outputs[side] = _collapse_side(leg_results)
            rows.append(
                {
                    "source_csv": str(csv_path),
                    "profile": csv_path.parent.name,
                    "candidate_variant_id": row.get("candidate_variant_id"),
                    "symbol": row.get("symbol"),
                    "family": row.get("family"),
                    "intended_regime": row.get("intended_regime"),
                    "contract_symbol": row.get("contract_symbol"),
                    "trade_date": row.get("trade_date"),
                    "entry_time_utc": entry_time.isoformat() if entry_time is not None else "",
                    "exit_time_utc": exit_time.isoformat() if exit_time is not None else "",
                    "entry_gap_reason": side_outputs["entry"]["reason"],
                    "entry_leg_count": side_outputs["entry"]["leg_count"],
                    "entry_matched_legs": side_outputs["entry"]["matched_legs"],
                    "entry_missing_legs": side_outputs["entry"]["missing_legs"],
                    "entry_max_quote_age_seconds": side_outputs["entry"]["max_quote_age_seconds"],
                    "entry_leg_reasons": side_outputs["entry"]["leg_reasons"],
                    "exit_gap_reason": side_outputs["exit"]["reason"],
                    "exit_leg_count": side_outputs["exit"]["leg_count"],
                    "exit_matched_legs": side_outputs["exit"]["matched_legs"],
                    "exit_missing_legs": side_outputs["exit"]["missing_legs"],
                    "exit_max_quote_age_seconds": side_outputs["exit"]["max_quote_age_seconds"],
                    "exit_leg_reasons": side_outputs["exit"]["leg_reasons"],
                }
            )

    diagnostics = pd.DataFrame(rows)
    diagnostics.to_csv(output_dir / "quote_gap_rows.csv", index=False)
    universe = _contract_universe_rows(rows)
    pd.DataFrame(universe).to_csv(output_dir / "replay_contract_universe.csv", index=False)
    sidecar_coverage = _sidecar_coverage_rows(quote_frame)
    pd.DataFrame(sidecar_coverage).to_csv(output_dir / "sidecar_symbol_coverage.csv", index=False)
    root_cause_action_rows = _root_cause_action_rows(diagnostics)
    pd.DataFrame(root_cause_action_rows).to_csv(output_dir / "quote_gap_root_cause_action_plan.csv", index=False)

    examples = []
    if not diagnostics.empty:
        for side in ("entry", "exit"):
            reason_column = f"{side}_gap_reason"
            for reason, group in diagnostics.groupby(reason_column):
                examples.extend(group.head(max_examples_per_reason).to_dict("records"))
    pd.DataFrame(examples).drop_duplicates().to_csv(output_dir / "quote_gap_examples.csv", index=False)

    entry_counts = Counter(diagnostics["entry_gap_reason"]) if not diagnostics.empty else Counter()
    exit_counts = Counter(diagnostics["exit_gap_reason"]) if not diagnostics.empty else Counter()
    replay_contracts = {str(item["contract_symbol"]) for item in universe}
    sidecar_contracts = {str(item["option_symbol"]) for item in sidecar_coverage}
    replay_dates = {
        date
        for item in universe
        for date in str(item.get("trade_dates") or "").split(";")
        if date
    }
    sidecar_dates = {
        date
        for item in sidecar_coverage
        for date in str(item.get("quote_dates") or "").split(";")
        if date
    }
    sidecar_by_underlying = Counter(str(item.get("underlying") or "UNKNOWN") for item in sidecar_coverage)
    replay_by_underlying = Counter(str(item.get("underlying") or "UNKNOWN") for item in universe)
    if diagnostics.empty:
        strict_quote_backed_rows = 0
    else:
        strict_mask = (
            diagnostics["entry_gap_reason"].astype(str).eq("matched_quote")
            & diagnostics["exit_gap_reason"].astype(str).eq("matched_quote")
            & pd.to_numeric(diagnostics["entry_missing_legs"], errors="coerce").fillna(1).eq(0)
            & pd.to_numeric(diagnostics["exit_missing_legs"], errors="coerce").fillna(1).eq(0)
        )
        strict_quote_backed_rows = int(strict_mask.sum())
    summary = {
        "status": "quote_sidecar_gap_diagnostic_complete",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "quote_sidecar_csv": str(quote_sidecar_csv),
        "trade_economics_roots": [str(path) for path in trade_economics_roots],
        "trade_economics_csvs": [str(path) for path in trade_economics_csvs],
        "underlying_filter": sorted(underlyings),
        "max_quote_age_seconds": max_quote_age_seconds,
        "input_csv_count": len(csvs),
        "diagnosed_trade_rows": int(len(diagnostics)),
        "strict_quote_backed_trade_rows": strict_quote_backed_rows,
        "strict_quote_backed_trade_row_rate": round(strict_quote_backed_rows / len(diagnostics), 6)
        if len(diagnostics)
        else 0.0,
        "entry_gap_reason_counts": {str(key): int(value) for key, value in entry_counts.items()},
        "exit_gap_reason_counts": {str(key): int(value) for key, value in exit_counts.items()},
        "root_cause_action_counts": {
            str(key): int(value)
            for key, value in Counter(
                str(row.get("recommended_action") or "unknown") for row in root_cause_action_rows
            ).items()
        },
        "replay_contract_count": len(replay_contracts),
        "sidecar_contract_count": len(sidecar_contracts),
        "replay_contracts_present_in_sidecar": len(replay_contracts.intersection(sidecar_contracts)),
        "replay_trade_dates": sorted(replay_dates),
        "sidecar_quote_dates": sorted(sidecar_dates),
        "replay_contract_count_by_underlying": {str(key): int(value) for key, value in replay_by_underlying.items()},
        "sidecar_contract_count_by_underlying": {str(key): int(value) for key, value in sidecar_by_underlying.items()},
        "outputs": {
            "quote_gap_rows_csv": str(output_dir / "quote_gap_rows.csv"),
            "quote_gap_examples_csv": str(output_dir / "quote_gap_examples.csv"),
            "quote_gap_root_cause_action_plan_csv": str(output_dir / "quote_gap_root_cause_action_plan.csv"),
            "replay_contract_universe_csv": str(output_dir / "replay_contract_universe.csv"),
            "sidecar_symbol_coverage_csv": str(output_dir / "sidecar_symbol_coverage.csv"),
            "summary_json": str(output_dir / "quote_gap_diagnostic_summary.json"),
        },
    }
    (output_dir / "quote_gap_diagnostic_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def main() -> None:
    args = parse_args()
    summary = diagnose_quote_sidecar_gaps(
        trade_economics_roots=[Path(path) for path in args.trade_economics_root],
        trade_economics_csvs=[Path(path) for path in args.trade_economics_csv],
        quote_sidecar_csv=Path(args.quote_sidecar_csv),
        output_dir=Path(args.output_dir),
        underlyings={item.upper() for item in args.underlying},
        max_quote_age_seconds=args.max_quote_age_seconds,
        max_examples_per_reason=args.max_examples_per_reason,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

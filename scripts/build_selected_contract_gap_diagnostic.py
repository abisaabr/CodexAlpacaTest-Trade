from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


CLASS_MISSING_SELECTED_PARTITION = "missing_selected_contract_partition"
CLASS_MISSING_OPTION_BARS_PARTITION = "missing_option_bars_partition"
CLASS_NO_OPTION_TYPE_CONTRACTS = "no_option_type_contracts"
CLASS_NO_DTE_ELIGIBLE_CONTRACTS = "no_dte_eligible_contracts"
CLASS_BASE_ENTRY_BAR_MISSING = "base_entry_bar_missing"
CLASS_NO_VALID_WING_CONTRACT = "no_valid_wing_contract"
CLASS_WING_ENTRY_BAR_MISSING = "wing_entry_bar_missing"
CLASS_CHAIN_BUILDS = "chain_builds_with_source_data"
CLASS_UNSUPPORTED_FAMILY = "unsupported_family"


def _float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _selected_path(source_root: Path, symbol: str, trade_date: str) -> Path:
    return (
        source_root
        / "selected_contracts"
        / f"underlying={symbol}"
        / f"trade_date={trade_date}"
        / "part.parquet"
    )


def _option_bars_path(source_root: Path, symbol: str, trade_date: str) -> Path:
    return (
        source_root
        / "option_bars"
        / f"underlying={symbol}"
        / f"trade_date={trade_date}"
        / "batch=000"
        / "part.parquet"
    )


def _load_selected_contracts(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if "dte" in frame.columns:
        frame["dte"] = pd.to_numeric(frame["dte"], errors="coerce")
    if "strike_price" in frame.columns:
        frame["strike_price"] = pd.to_numeric(frame["strike_price"], errors="coerce")
    if "relative_strike_step" in frame.columns:
        frame["relative_strike_step"] = pd.to_numeric(
            frame["relative_strike_step"], errors="coerce"
        )
    if "option_type" in frame.columns:
        frame["option_type"] = frame["option_type"].astype(str).str.lower()
    return frame


def _load_option_bars(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame


def _contracts_for_type(
    contracts: pd.DataFrame,
    *,
    symbol: str,
    option_type: str,
) -> pd.DataFrame:
    frame = contracts.copy()
    if "underlying_symbol" in frame.columns:
        frame = frame[frame["underlying_symbol"].astype(str) == symbol]
    return frame[frame["option_type"].astype(str).str.lower() == option_type].copy()


def _filter_dte(frame: pd.DataFrame, dte_mode: str) -> pd.DataFrame:
    if frame.empty or "dte" not in frame.columns:
        return frame.copy()
    dte = pd.to_numeric(frame["dte"], errors="coerce")
    mode = dte_mode.lower()
    if mode == "same_day":
        return frame[dte == 0].copy()
    if mode == "next_expiry":
        positive = frame[dte > 0].copy()
        if positive.empty:
            return positive
        min_dte = pd.to_numeric(positive["dte"], errors="coerce").min()
        return positive[pd.to_numeric(positive["dte"], errors="coerce") == min_dte].copy()
    return frame.copy()


def _bars_in_entry_window(
    bars: pd.DataFrame,
    contract_symbol: str,
    entry_time: pd.Timestamp,
    max_lag_minutes: float,
) -> pd.DataFrame:
    if bars.empty or "timestamp" not in bars.columns:
        return bars.iloc[0:0].copy()
    end = entry_time + pd.Timedelta(minutes=max_lag_minutes)
    frame = bars[bars["symbol"].astype(str) == contract_symbol].copy()
    return frame[(frame["timestamp"] >= entry_time) & (frame["timestamp"] <= end)].copy()


def _entry_bar(
    bars: pd.DataFrame,
    contract_symbol: str,
    entry_time: pd.Timestamp,
    max_lag_minutes: float,
) -> dict[str, Any] | None:
    frame = _bars_in_entry_window(bars, contract_symbol, entry_time, max_lag_minutes)
    if frame.empty:
        return None
    return frame.sort_values(["timestamp"]).iloc[0].to_dict()


def _entry_print_count(
    bars: pd.DataFrame,
    contract_symbol: str,
    entry_time: pd.Timestamp,
    max_lag_minutes: float,
) -> float:
    # Match current GCP replays: option_trades_root is intentionally empty, so the
    # backtester ranks entry-liquidity candidates with prints=0 rather than using
    # option-bar trade_count as a proxy.
    _ = (bars, contract_symbol, entry_time, max_lag_minutes)
    return 0.0


def _select_base_contract(
    contracts: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    symbol: str,
    option_type: str,
    dte_mode: str,
    entry_time: pd.Timestamp,
    max_lag_minutes: float,
) -> tuple[dict[str, Any] | None, str, int, int, int]:
    type_frame = _contracts_for_type(contracts, symbol=symbol, option_type=option_type)
    dte_frame = _filter_dte(type_frame, dte_mode)
    if type_frame.empty:
        return None, "no_selected_contract", len(type_frame), len(dte_frame), 0
    if dte_frame.empty:
        return None, "no_selected_contract", len(type_frame), len(dte_frame), 0

    choices: list[tuple[tuple[float, ...], str, dict[str, Any]]] = []
    for contract in dte_frame.to_dict("records"):
        contract_symbol = str(contract["symbol"])
        entry_bar = _entry_bar(bars, contract_symbol, entry_time, max_lag_minutes)
        if not entry_bar:
            continue
        prints = _entry_print_count(bars, contract_symbol, entry_time, max_lag_minutes)
        volume = _float(entry_bar.get("volume"))
        abs_step = abs(_float(contract.get("relative_strike_step")))
        dte = _float(contract.get("dte"), 999.0)
        rank_key = (-prints, -volume, abs_step, dte)
        choices.append((rank_key, contract_symbol, contract))
    if not choices:
        return None, "no_entry_bar", len(type_frame), len(dte_frame), 0
    choices.sort(key=lambda item: (item[0], item[1]))
    return choices[0][2], "selected", len(type_frame), len(dte_frame), len(choices)


def _select_wing_contract(
    contracts: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    symbol: str,
    option_type: str,
    base_contract: dict[str, Any],
    higher: bool,
    width_steps: int,
    entry_time: pd.Timestamp,
    max_lag_minutes: float,
) -> tuple[dict[str, Any] | None, str, int, int]:
    frame = _contracts_for_type(contracts, symbol=symbol, option_type=option_type)
    if frame.empty:
        return None, "no_selected_contract", 0, 0

    base_dte = base_contract.get("dte")
    if base_dte is not None and "dte" in frame.columns:
        same_dte = frame[pd.to_numeric(frame["dte"], errors="coerce") == _float(base_dte)]
        if not same_dte.empty:
            frame = same_dte.copy()

    base_strike = _float(base_contract.get("strike_price") or base_contract.get("strike"))
    strikes = pd.to_numeric(frame.get("strike_price", frame.get("strike")), errors="coerce")
    if higher:
        candidates = frame[strikes > base_strike].copy()
        candidates["_strike_sort"] = pd.to_numeric(
            candidates.get("strike_price", candidates.get("strike")), errors="coerce"
        )
        candidates = candidates.sort_values(["_strike_sort", "symbol"])
    else:
        candidates = frame[strikes < base_strike].copy()
        candidates["_strike_sort"] = pd.to_numeric(
            candidates.get("strike_price", candidates.get("strike")), errors="coerce"
        )
        candidates = candidates.sort_values(["_strike_sort", "symbol"], ascending=[False, True])

    if candidates.empty:
        return None, "no_selected_contract", 0, 0

    width_steps = max(1, int(width_steps))
    candidate_rows = candidates.to_dict("records")
    ordered = candidate_rows[width_steps - 1 :] + candidate_rows[: width_steps - 1]
    entry_candidate_count = 0
    for contract in ordered:
        contract_symbol = str(contract["symbol"])
        if _entry_bar(bars, contract_symbol, entry_time, max_lag_minutes):
            entry_candidate_count += 1
            return contract, "selected", len(candidate_rows), entry_candidate_count
    return candidate_rows[0], "no_entry_bar", len(candidate_rows), entry_candidate_count


def _analyze_structure(
    row: dict[str, Any],
    contracts: pd.DataFrame,
    bars: pd.DataFrame,
) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "")
    family = str(row.get("family") or row.get("option_structure") or "").lower()
    option_type = str(row.get("option_type") or "").lower()
    dte_mode = str(row.get("dte_mode") or "").lower()
    entry_time = _timestamp(row.get("stock_entry_time") or row.get("lookup_time"))
    max_lag_minutes = _float(row.get("max_entry_lag_minutes"), 0.0)
    vertical_width = _int(row.get("wing_width_steps"), 1)
    short_width = _int(row.get("short_width_steps"), 1)

    base, base_status, option_type_rows, dte_rows, base_entry_choices = _select_base_contract(
        contracts,
        bars,
        symbol=symbol,
        option_type=option_type,
        dte_mode=dte_mode,
        entry_time=entry_time,
        max_lag_minutes=max_lag_minutes,
    )
    result: dict[str, Any] = {
        "option_type_rows": option_type_rows,
        "dte_filtered_rows": dte_rows,
        "base_entry_candidate_count": base_entry_choices,
        "base_status": base_status,
        "selected_base_symbol": "",
        "selected_base_strike": "",
        "leg_status": base_status,
        "selected_leg_symbols": "",
        "selected_leg_strikes": "",
        "wing_candidate_count": 0,
        "wing_entry_candidate_count": 0,
    }

    if option_type_rows == 0:
        result["classification"] = CLASS_NO_OPTION_TYPE_CONTRACTS
        return result
    if dte_rows == 0:
        result["classification"] = CLASS_NO_DTE_ELIGIBLE_CONTRACTS
        return result
    if base_status != "selected" or base is None:
        result["classification"] = CLASS_BASE_ENTRY_BAR_MISSING
        return result

    selected_legs = [base]
    result["selected_base_symbol"] = str(base.get("symbol") or "")
    result["selected_base_strike"] = base.get("strike_price") or base.get("strike") or ""

    def add_wing(
        base_leg: dict[str, Any],
        *,
        wing_option_type: str,
        higher: bool,
        width_steps: int,
    ) -> tuple[dict[str, Any] | None, str]:
        wing, status, candidate_count, entry_count = _select_wing_contract(
            contracts,
            bars,
            symbol=symbol,
            option_type=wing_option_type,
            base_contract=base_leg,
            higher=higher,
            width_steps=width_steps,
            entry_time=entry_time,
            max_lag_minutes=max_lag_minutes,
        )
        result["wing_candidate_count"] = _int(result["wing_candidate_count"]) + candidate_count
        result["wing_entry_candidate_count"] = (
            _int(result["wing_entry_candidate_count"]) + entry_count
        )
        return wing, status

    if family == "debit_call_vertical":
        wing, status = add_wing(
            base,
            wing_option_type="call",
            higher=True,
            width_steps=vertical_width,
        )
        if status == "selected" and wing is not None:
            selected_legs.append(wing)
        result["leg_status"] = status
    elif family == "debit_put_vertical":
        wing, status = add_wing(
            base,
            wing_option_type="put",
            higher=False,
            width_steps=vertical_width,
        )
        if status == "selected" and wing is not None:
            selected_legs.append(wing)
        result["leg_status"] = status
    elif family == "bear_call_credit_spread":
        short_call, status = add_wing(
            base,
            wing_option_type="call",
            higher=True,
            width_steps=short_width,
        )
        if status == "selected" and short_call is not None:
            selected_legs.append(short_call)
            long_call, status = add_wing(
                short_call,
                wing_option_type="call",
                higher=True,
                width_steps=vertical_width,
            )
            if status == "selected" and long_call is not None:
                selected_legs.append(long_call)
        result["leg_status"] = status
    else:
        result["classification"] = CLASS_UNSUPPORTED_FAMILY
        return result

    result["selected_leg_symbols"] = ";".join(str(leg.get("symbol") or "") for leg in selected_legs)
    result["selected_leg_strikes"] = ";".join(
        str(leg.get("strike_price") or leg.get("strike") or "") for leg in selected_legs
    )
    if result["leg_status"] == "selected":
        result["classification"] = CLASS_CHAIN_BUILDS
    elif result["leg_status"] == "no_entry_bar":
        result["classification"] = CLASS_WING_ENTRY_BAR_MISSING
    else:
        result["classification"] = CLASS_NO_VALID_WING_CONTRACT
    return result


def build_gap_diagnostic(
    *,
    requests_csv: Path,
    source_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    rows = _read_csv(requests_csv)
    output_dir.mkdir(parents=True, exist_ok=True)

    diagnostic_rows: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        trade_date = str(row.get("trade_date") or "")
        selected_file = _selected_path(source_root, symbol, trade_date)
        bars_file = _option_bars_path(source_root, symbol, trade_date)
        diagnostic: dict[str, Any] = {
            **row,
            "selected_contract_file": selected_file.as_posix(),
            "option_bars_file": bars_file.as_posix(),
            "selected_contract_file_status": "available" if selected_file.exists() else "missing",
            "option_bars_file_status": "available" if bars_file.exists() else "missing",
            "contract_rows": 0,
            "option_bar_rows": 0,
            "classification": "",
        }
        if not selected_file.exists():
            diagnostic["classification"] = CLASS_MISSING_SELECTED_PARTITION
            diagnostic_rows.append(diagnostic)
            continue
        if not bars_file.exists():
            diagnostic["classification"] = CLASS_MISSING_OPTION_BARS_PARTITION
            diagnostic["contract_rows"] = len(_load_selected_contracts(selected_file))
            diagnostic_rows.append(diagnostic)
            continue

        contracts = _load_selected_contracts(selected_file)
        bars = _load_option_bars(bars_file)
        diagnostic["contract_rows"] = len(contracts)
        diagnostic["option_bar_rows"] = len(bars)
        diagnostic.update(_analyze_structure(row, contracts, bars))
        diagnostic_rows.append(diagnostic)

    class_counts = Counter(str(row["classification"]) for row in diagnostic_rows)
    case_counts = Counter(str(row.get("case_id") or "") for row in diagnostic_rows)
    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "requests_csv": requests_csv.as_posix(),
        "source_root": source_root.as_posix(),
        "request_count": len(diagnostic_rows),
        "case_count": len(case_counts),
        "classification_counts": dict(sorted(class_counts.items())),
        "case_counts": dict(sorted(case_counts.items())),
        "output_files": {
            "rows_csv": (output_dir / "selected_contract_gap_diagnostic_rows.csv").as_posix(),
            "summary_json": (
                output_dir / "selected_contract_gap_diagnostic_summary.json"
            ).as_posix(),
            "markdown": (output_dir / "selected_contract_gap_diagnostic.md").as_posix(),
        },
    }

    rows_csv = output_dir / "selected_contract_gap_diagnostic_rows.csv"
    summary_json = output_dir / "selected_contract_gap_diagnostic_summary.json"
    markdown = output_dir / "selected_contract_gap_diagnostic.md"
    _write_csv(rows_csv, diagnostic_rows)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    markdown.write_text(_markdown(summary, diagnostic_rows), encoding="utf-8")
    return summary


def _markdown(summary: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Selected Contract Gap Diagnostic",
        "",
        f"Generated: `{summary['generated_at']}`",
        f"Requests: `{summary['request_count']}` across `{summary['case_count']}` cases.",
        "",
        "## Classification Counts",
        "",
    ]
    for key, value in summary["classification_counts"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Case Breakdown", ""])
    grouped: dict[str, Counter[str]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("case_id") or ""), Counter()).update(
            [str(row.get("classification") or "")]
        )
    for case_id, counts in sorted(grouped.items()):
        rendered = ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
        lines.append(f"- `{case_id}`: {rendered}")
    lines.extend(
        [
            "",
            "## Repair Interpretation",
            "",
            "- `missing_selected_contract_partition`: repair or redownload the date partition before retesting.",
            "- `no_valid_wing_contract`: selected-contract universe is too narrow for the multi-leg structure at the selected base/width.",
            "- `base_entry_bar_missing` or `wing_entry_bar_missing`: source contracts exist, but executable entry bars are absent inside the configured lag window.",
            "- `chain_builds_with_source_data`: the local source data can build the legs, so inspect runtime path, parameter, or replay lineage mismatches.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests-csv", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_gap_diagnostic(
        requests_csv=args.requests_csv,
        source_root=args.source_root,
        output_dir=args.output_dir,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

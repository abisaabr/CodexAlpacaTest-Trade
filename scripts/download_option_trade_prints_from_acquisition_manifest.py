from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.data.chunking import batched
from alpaca_lab.logging_utils import configure_logging


OPTION_TRADE_COLUMNS = [
    "option_symbol",
    "underlying_symbol",
    "event_time_utc",
    "observed_at_utc",
    "trade_age_seconds",
    "price",
    "size",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download historical option trade prints for the exact contract/date "
            "windows listed in a quote acquisition manifest. Research-only; never "
            "submits orders."
        )
    )
    parser.add_argument("--contract-dates-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--underlying", action="append", default=[])
    parser.add_argument("--option-batch-size", type=int, default=50)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--config", default=None)
    parser.add_argument(
        "--max-date-count",
        type=int,
        default=0,
        help="Optional smoke-test cap on distinct trade_date values. 0 means no cap.",
    )
    return parser.parse_args()


def _clean_symbol(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip().upper()


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


def _iso(timestamp: pd.Timestamp | None) -> str:
    return "" if timestamp is None else timestamp.isoformat()


def _load_contract_dates(
    path: Path,
    *,
    underlyings: set[str],
    max_date_count: int,
) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    required = {
        "underlying",
        "trade_date",
        "contract_symbol",
        "requested_window_start_utc",
        "requested_window_end_utc",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"contract dates CSV missing required columns: {missing}")
    frame = frame.copy()
    frame["underlying"] = frame["underlying"].map(_clean_symbol)
    frame["contract_symbol"] = frame["contract_symbol"].map(_clean_symbol)
    frame = frame[(frame["underlying"] != "") & (frame["contract_symbol"] != "")]
    if underlyings:
        frame = frame[frame["underlying"].isin(underlyings)]
    frame["_start"] = frame["requested_window_start_utc"].map(_timestamp)
    frame["_end"] = frame["requested_window_end_utc"].map(_timestamp)
    frame = frame.dropna(subset=["_start", "_end"])
    frame = frame[frame["_end"] >= frame["_start"]]
    if max_date_count > 0 and not frame.empty:
        dates = sorted(frame["trade_date"].dropna().astype(str).unique().tolist())[:max_date_count]
        frame = frame[frame["trade_date"].astype(str).isin(dates)]
    return frame.reset_index(drop=True)


def _extract_trade_rows(
    payload: dict[str, Any],
    *,
    underlying_lookup: dict[str, str],
    symbol_windows: dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]],
    observed_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    trades = payload.get("trades") if isinstance(payload, dict) else {}
    if not isinstance(trades, dict):
        return rows
    observed_at_text = observed_at.isoformat()
    for symbol, symbol_trades in trades.items():
        option_symbol = _clean_symbol(symbol)
        if not isinstance(symbol_trades, list):
            continue
        for trade in symbol_trades:
            if not isinstance(trade, dict):
                continue
            event_time = _timestamp(trade.get("t") or trade.get("timestamp"))
            if event_time is None:
                continue
            windows = symbol_windows.get(option_symbol, [])
            if windows and not any(start <= event_time <= end for start, end in windows):
                continue
            price = trade.get("p", trade.get("price"))
            size = trade.get("s", trade.get("size"))
            rows.append(
                {
                    "option_symbol": option_symbol,
                    "underlying_symbol": underlying_lookup.get(option_symbol, ""),
                    "event_time_utc": event_time.isoformat(),
                    "observed_at_utc": observed_at_text,
                    "trade_age_seconds": "",
                    "price": price,
                    "size": size,
                }
            )
    return rows


def download_option_trade_prints_from_acquisition_manifest(
    *,
    broker: Any,
    contract_dates_csv: Path,
    output_dir: Path,
    underlyings: set[str],
    option_batch_size: int,
    sleep_seconds: float = 0.0,
    max_date_count: int = 0,
) -> dict[str, Any]:
    if option_batch_size <= 0 or option_batch_size > 100:
        raise ValueError("option_batch_size must be between 1 and 100.")
    output_dir.mkdir(parents=True, exist_ok=True)
    contract_dates = _load_contract_dates(
        contract_dates_csv,
        underlyings=underlyings,
        max_date_count=max_date_count,
    )
    generated_at = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    request_count = 0
    requested_symbol_dates = 0

    for (underlying, trade_date), day_frame in contract_dates.groupby(
        ["underlying", "trade_date"], sort=True
    ):
        day_start = day_frame["_start"].min()
        day_end = day_frame["_end"].max()
        symbols = sorted(day_frame["contract_symbol"].dropna().astype(str).unique().tolist())
        requested_symbol_dates += len(symbols)
        underlying_lookup = {symbol: str(underlying) for symbol in symbols}
        symbol_windows: dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
        for symbol, symbol_frame in day_frame.groupby("contract_symbol", sort=True):
            windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
            for _, window_row in symbol_frame[["_start", "_end"]].iterrows():
                start = window_row["_start"]
                end = window_row["_end"]
                if start is not None and end is not None:
                    windows.append((start, end))
            symbol_windows[str(symbol)] = windows
        for batch_index, symbol_batch in enumerate(batched(symbols, option_batch_size)):
            request_count += 1
            started_at = datetime.now(UTC)
            audit: dict[str, Any] = {
                "underlying": underlying,
                "trade_date": trade_date,
                "batch_index": batch_index,
                "symbol_count": len(symbol_batch),
                "start_utc": _iso(day_start),
                "end_utc": _iso(day_end),
                "requested_at_utc": started_at.isoformat(),
            }
            try:
                payload = broker.get_option_trades(
                    list(symbol_batch),
                    start=day_start.to_pydatetime(),
                    end=day_end.to_pydatetime(),
                )
                batch_rows = _extract_trade_rows(
                    payload,
                    underlying_lookup=underlying_lookup,
                    symbol_windows=symbol_windows,
                    observed_at=generated_at,
                )
                rows.extend(batch_rows)
                audit["status"] = "ok"
                audit["trade_print_count"] = len(batch_rows)
                request_audit = payload.get("_request_audit") if isinstance(payload, dict) else None
                if isinstance(request_audit, list):
                    audit["page_count"] = len(request_audit)
                    audit["http_status_codes"] = ",".join(
                        str(item.get("status_code", "")) for item in request_audit
                    )
                else:
                    audit["page_count"] = ""
                    audit["http_status_codes"] = ""
            except Exception as exc:  # noqa: BLE001
                audit["status"] = "failed"
                audit["error"] = str(exc)
                failures.append(dict(audit))
            audit_rows.append(audit)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    trade_frame = pd.DataFrame(rows, columns=OPTION_TRADE_COLUMNS)
    audit_frame = pd.DataFrame(audit_rows)
    failures_frame = pd.DataFrame(failures)
    trade_path = output_dir / "option_trade_prints.csv"
    audit_path = output_dir / "option_trade_print_download_audit.csv"
    failures_path = output_dir / "option_trade_print_download_failures.csv"
    trade_frame.to_csv(trade_path, index=False)
    audit_frame.to_csv(audit_path, index=False)
    failures_frame.to_csv(failures_path, index=False)

    coverage_counts = Counter()
    if not audit_frame.empty:
        for status, count in audit_frame["status"].value_counts().items():
            coverage_counts[str(status)] = int(count)
    summary = {
        "status": "completed" if not failures else "completed_with_failures",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "contract_dates_csv": str(contract_dates_csv),
        "output_dir": str(output_dir),
        "underlyings": sorted(underlyings) if underlyings else "all",
        "contract_date_rows": int(len(contract_dates)),
        "requested_symbol_dates": int(requested_symbol_dates),
        "request_count": int(request_count),
        "trade_print_rows": int(len(trade_frame)),
        "unique_trade_contracts": int(trade_frame["option_symbol"].nunique()) if not trade_frame.empty else 0,
        "failed_request_count": int(len(failures)),
        "request_status_counts": dict(sorted(coverage_counts.items())),
        "option_trade_prints_csv": str(trade_path),
        "download_audit_csv": str(audit_path),
        "failures_csv": str(failures_path),
        "generated_at_utc": generated_at.isoformat(),
        "lineage_note": (
            "Historical option trade prints repair liquidity/fill evidence only. "
            "They are not OPRA bid/ask quotes and must not be treated as quote-backed spread evidence."
        ),
    }
    summary_path = output_dir / "option_trade_print_download_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> int:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    broker = AlpacaBrokerAdapter(settings, dry_run=True)
    try:
        summary = download_option_trade_prints_from_acquisition_manifest(
            broker=broker,
            contract_dates_csv=Path(args.contract_dates_csv),
            output_dir=Path(args.output_dir),
            underlyings={_clean_symbol(value) for value in args.underlying if _clean_symbol(value)},
            option_batch_size=args.option_batch_size,
            sleep_seconds=args.sleep_seconds,
            max_date_count=args.max_date_count,
        )
    finally:
        broker.close()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 2 if summary["failed_request_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

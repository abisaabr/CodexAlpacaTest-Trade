from __future__ import annotations

import argparse
import json
import math
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import pandas as pd
import requests


VENDOR_COLUMNS = [
    "option_symbol",
    "event_time_utc",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "source_provider",
    "source_ticker",
    "source_request_start_utc",
    "source_request_end_utc",
    "sequence_number",
    "sip_timestamp",
    "bid_exchange",
    "ask_exchange",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download historical OPRA/NBBO option quotes from Polygon/Massive "
            "for windows emitted by build_opra_backfill_request_package.py. "
            "This is research-only and never submits orders."
        )
    )
    parser.add_argument("--request-windows-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--api-key-env", default="POLYGON_API_KEY,MASSIVE_API_KEY")
    parser.add_argument("--base-url", default="https://api.polygon.io")
    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument("--max-windows", type=int, default=None)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def _clean_symbol(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip().upper()


def _polygon_option_ticker(contract_symbol: str) -> str:
    symbol = _clean_symbol(contract_symbol)
    if not symbol:
        return ""
    return symbol if symbol.startswith("O:") else f"O:{symbol}"


def _timestamp_ns(value: Any) -> int | None:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    timestamp = timestamp.tz_convert("UTC")
    return int(timestamp.value)


def _ns_to_iso(value: Any) -> str:
    try:
        timestamp = pd.to_datetime(int(value), unit="ns", utc=True)
    except (TypeError, ValueError, OverflowError):
        return ""
    if pd.isna(timestamp):
        return ""
    return timestamp.isoformat()


def _api_key_from_env(names_csv: str) -> tuple[str, str]:
    for name in (item.strip() for item in names_csv.split(",") if item.strip()):
        value = os.environ.get(name)
        if value:
            return name, value
    return "", ""


def _append_api_key(url: str, api_key: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("apiKey", api_key)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _fetch_window(
    *,
    session: requests.Session,
    base_url: str,
    api_key: str,
    contract_symbol: str,
    start_ns: int,
    end_ns: int,
    limit: int,
    timeout_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ticker = _polygon_option_ticker(contract_symbol)
    url = f"{base_url.rstrip('/')}/v3/quotes/{ticker}"
    params: dict[str, Any] = {
        "timestamp.gte": start_ns,
        "timestamp.lte": end_ns,
        "order": "asc",
        "sort": "timestamp",
        "limit": limit,
        "apiKey": api_key,
    }
    rows: list[dict[str, Any]] = []
    requests_made: list[dict[str, Any]] = []
    page_index = 1
    while url:
        response = session.get(url, params=params if page_index == 1 else None, timeout=timeout_seconds)
        request_record = {
            "contract_symbol": contract_symbol,
            "source_ticker": ticker,
            "page_index": page_index,
            "url": response.url.split("apiKey=")[0] + "apiKey=<redacted>",
            "status_code": int(response.status_code),
            "row_count": 0,
            "error": "",
        }
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if response.status_code >= 400:
            request_record["error"] = str(payload.get("message") or response.text[:300])
            requests_made.append(request_record)
            break
        results = payload.get("results") if isinstance(payload, dict) else []
        if not isinstance(results, list):
            results = []
        for result in results:
            if not isinstance(result, dict):
                continue
            bid = result.get("bid_price")
            ask = result.get("ask_price")
            timestamp = result.get("sip_timestamp") or result.get("participant_timestamp")
            rows.append(
                {
                    "option_symbol": contract_symbol,
                    "event_time_utc": _ns_to_iso(timestamp),
                    "bid": bid,
                    "ask": ask,
                    "bid_size": result.get("bid_size", 0),
                    "ask_size": result.get("ask_size", 0),
                    "source_provider": "polygon_options_quotes",
                    "source_ticker": ticker,
                    "source_request_start_utc": pd.to_datetime(start_ns, unit="ns", utc=True).isoformat(),
                    "source_request_end_utc": pd.to_datetime(end_ns, unit="ns", utc=True).isoformat(),
                    "sequence_number": result.get("sequence_number"),
                    "sip_timestamp": result.get("sip_timestamp"),
                    "bid_exchange": result.get("bid_exchange"),
                    "ask_exchange": result.get("ask_exchange"),
                }
            )
        request_record["row_count"] = len(results)
        requests_made.append(request_record)
        next_url = payload.get("next_url") if isinstance(payload, dict) else None
        url = _append_api_key(str(next_url), api_key) if next_url else ""
        params = None
        page_index += 1
    return rows, requests_made


def download_polygon_opra_quotes_from_request_package(
    *,
    request_windows_csv: Path,
    output_dir: Path,
    api_key_env: str,
    base_url: str,
    limit: int,
    max_windows: int | None,
    sleep_seconds: float,
    timeout_seconds: float,
    resume: bool,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    vendor_csv = output_dir / "polygon_opra_quotes.csv"
    request_audit_csv = output_dir / "polygon_opra_quote_requests.csv"
    failure_csv = output_dir / "polygon_opra_quote_failures.csv"
    summary_json = output_dir / "polygon_opra_download_summary.json"
    try:
        requested_windows = pd.read_csv(request_windows_csv, low_memory=False)
    except (FileNotFoundError, pd.errors.EmptyDataError):
        requested_windows = pd.DataFrame()
    if max_windows is not None and not requested_windows.empty:
        requested_windows = requested_windows.head(max_windows).copy()

    key_name, api_key = _api_key_from_env(api_key_env)
    if not api_key:
        empty_quotes = pd.DataFrame(columns=VENDOR_COLUMNS)
        empty_quotes.to_csv(vendor_csv, index=False)
        pd.DataFrame().to_csv(request_audit_csv, index=False)
        pd.DataFrame().to_csv(failure_csv, index=False)
        summary = {
            "status": "blocked_missing_polygon_api_key",
            "broker_facing": False,
            "paper_runner_state_changed": False,
            "live_manifest_effect": "none",
            "risk_policy_effect": "none",
            "request_windows_csv": str(request_windows_csv),
            "output_vendor_csv": str(vendor_csv),
            "request_audit_csv": str(request_audit_csv),
            "failure_csv": str(failure_csv),
            "api_key_env_checked": [item.strip() for item in api_key_env.split(",") if item.strip()],
            "downloaded_quote_rows": 0,
            "requested_window_count": int(len(requested_windows)),
            "completed_window_count": 0,
            "failed_window_count": 0,
        }
        summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        return summary

    windows = requested_windows

    completed_symbols: set[tuple[str, str, str]] = set()
    existing_rows: list[pd.DataFrame] = []
    if resume and vendor_csv.exists():
        try:
            existing = pd.read_csv(vendor_csv, low_memory=False)
        except pd.errors.EmptyDataError:
            existing = pd.DataFrame(columns=VENDOR_COLUMNS)
        if not existing.empty:
            existing_rows.append(existing)
            for _, row in existing.iterrows():
                completed_symbols.add(
                    (
                        _clean_symbol(row.get("option_symbol")),
                        str(row.get("source_request_start_utc") or ""),
                        str(row.get("source_request_end_utc") or ""),
                    )
                )

    session = session or requests.Session()
    quote_rows: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    completed_window_count = 0
    for _, window in windows.iterrows():
        contract = _clean_symbol(window.get("contract_symbol"))
        start_ns = _timestamp_ns(window.get("request_window_start_utc"))
        end_ns = _timestamp_ns(window.get("request_window_end_utc"))
        if not contract or start_ns is None or end_ns is None:
            failures.append(
                {
                    "contract_symbol": contract,
                    "request_window_start_utc": window.get("request_window_start_utc", ""),
                    "request_window_end_utc": window.get("request_window_end_utc", ""),
                    "reason": "invalid_request_window",
                }
            )
            continue
        start_iso = pd.to_datetime(start_ns, unit="ns", utc=True).isoformat()
        end_iso = pd.to_datetime(end_ns, unit="ns", utc=True).isoformat()
        if resume and (contract, start_iso, end_iso) in completed_symbols:
            completed_window_count += 1
            continue
        rows, requests_made = _fetch_window(
            session=session,
            base_url=base_url,
            api_key=api_key,
            contract_symbol=contract,
            start_ns=start_ns,
            end_ns=end_ns,
            limit=limit,
            timeout_seconds=timeout_seconds,
        )
        quote_rows.extend(rows)
        request_rows.extend(requests_made)
        if any(item.get("error") for item in requests_made):
            failures.append(
                {
                    "contract_symbol": contract,
                    "request_window_start_utc": start_iso,
                    "request_window_end_utc": end_iso,
                    "reason": ";".join(item["error"] for item in requests_made if item.get("error")),
                }
            )
        else:
            completed_window_count += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)

    new_frame = pd.DataFrame(quote_rows, columns=VENDOR_COLUMNS)
    output_frame = pd.concat(existing_rows + [new_frame], ignore_index=True) if existing_rows else new_frame
    if not output_frame.empty:
        output_frame = output_frame.drop_duplicates(
            subset=["option_symbol", "event_time_utc", "bid", "ask", "source_request_start_utc", "source_request_end_utc"],
            keep="last",
        ).sort_values(["option_symbol", "event_time_utc"])
    output_frame.to_csv(vendor_csv, index=False)
    pd.DataFrame(request_rows).to_csv(request_audit_csv, index=False)
    pd.DataFrame(failures).to_csv(failure_csv, index=False)

    summary = {
        "status": "polygon_opra_download_complete" if not failures else "polygon_opra_download_completed_with_failures",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "request_windows_csv": str(request_windows_csv),
        "output_vendor_csv": str(vendor_csv),
        "request_audit_csv": str(request_audit_csv),
        "failure_csv": str(failure_csv),
        "api_key_env_used": key_name,
        "base_url": base_url,
        "requested_window_count": int(len(windows)),
        "completed_window_count": int(completed_window_count),
        "failed_window_count": int(len(failures)),
        "downloaded_quote_rows": int(len(output_frame)),
        "unique_option_symbols": int(output_frame["option_symbol"].nunique()) if not output_frame.empty else 0,
        "limit": limit,
        "max_windows": max_windows,
        "resume": resume,
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> int:
    args = parse_args()
    summary = download_polygon_opra_quotes_from_request_package(
        request_windows_csv=Path(args.request_windows_csv),
        output_dir=Path(args.output_dir),
        api_key_env=args.api_key_env,
        base_url=args.base_url,
        limit=args.limit,
        max_windows=args.max_windows,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        resume=args.resume,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 2 if summary["status"].startswith("blocked_") else 0


if __name__ == "__main__":
    raise SystemExit(main())

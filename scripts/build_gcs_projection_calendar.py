from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TRADE_DATE_RE = re.compile(r"trade_date=(\d{4}-\d{2}-\d{2})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a research-only full-period projection calendar from GCS "
            "option-bar trade_date partitions in ticker launch rows."
        )
    )
    parser.add_argument("--launch-rows-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--gcloud", default="gcloud")
    parser.add_argument(
        "--listing-cache-json",
        default=None,
        help=(
            "Optional test/offline cache mapping bars URIs to gcloud storage ls "
            "lines. Production aggregate VMs should omit this."
        ),
    )
    return parser.parse_args()


def extract_trade_dates(listing_lines: list[str]) -> list[str]:
    dates: set[str] = set()
    for line in listing_lines:
        for match in TRADE_DATE_RE.finditer(str(line)):
            dates.add(match.group(1))
    return sorted(dates)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_launch_rows(path: Path) -> list[dict[str, Any]]:
    payload = _load_json(path)
    if isinstance(payload, dict):
        for key in ["launch_rows", "rows", "tickers"]:
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
    if not isinstance(payload, list):
        raise ValueError(f"Expected launch rows list in {path}")
    rows = [row for row in payload if isinstance(row, dict)]
    if not rows:
        raise ValueError(f"No launch row objects found in {path}")
    return rows


def _load_listing_cache(path: Path | None) -> dict[str, list[str]]:
    if path is None:
        return {}
    payload = _load_json(path)
    if not isinstance(payload, dict):
        raise ValueError("--listing-cache-json must be a JSON object keyed by bars URI")
    cache: dict[str, list[str]] = {}
    for uri, lines in payload.items():
        if isinstance(lines, list):
            cache[str(uri)] = [str(line) for line in lines]
        elif isinstance(lines, str):
            cache[str(uri)] = [line for line in lines.splitlines() if line.strip()]
        else:
            raise ValueError(f"Listing cache value for {uri!r} must be a list or string")
    return cache


def _gcs_listing(*, gcloud: str, uri: str) -> list[str]:
    command = [gcloud, "storage", "ls", "--recursive", uri]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        detail = stderr or stdout or f"exit code {completed.returncode}"
        raise RuntimeError(f"gcloud storage ls failed for {uri}: {detail}")
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_gcs_projection_calendar(
    *,
    launch_rows_json: Path,
    output_dir: Path,
    gcloud: str = "gcloud",
    listing_cache_json: Path | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    launch_rows = _load_launch_rows(launch_rows_json)
    listing_cache = _load_listing_cache(listing_cache_json)

    dates_by_symbol: dict[str, set[str]] = {}
    row_summaries: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for row in launch_rows:
        symbol = str(row.get("symbol") or "").upper()
        bars_uri = str(row.get("bars") or row.get("bars_uri") or "")
        dataset_id = str(row.get("dataset_id") or "unknown")
        if not symbol or not bars_uri:
            errors.append(
                {
                    "symbol": symbol or "unknown",
                    "bars_uri": bars_uri,
                    "error": "missing_symbol_or_bars_uri",
                }
            )
            continue
        try:
            listing_lines = listing_cache.get(bars_uri)
            if listing_lines is None:
                listing_lines = _gcs_listing(gcloud=gcloud, uri=bars_uri)
            trade_dates = extract_trade_dates(listing_lines)
            dates_by_symbol[symbol] = set(trade_dates)
            row_summaries.append(
                {
                    "symbol": symbol,
                    "dataset_id": dataset_id,
                    "bars_uri": bars_uri,
                    "listing_line_count": len(listing_lines),
                    "trade_date_count": len(trade_dates),
                    "first_trade_date": trade_dates[0] if trade_dates else "",
                    "last_trade_date": trade_dates[-1] if trade_dates else "",
                    "status": "ok" if trade_dates else "no_trade_dates_found",
                }
            )
            if not trade_dates:
                errors.append(
                    {
                        "symbol": symbol,
                        "bars_uri": bars_uri,
                        "error": "no_trade_dates_found",
                    }
                )
        except Exception as exc:
            errors.append({"symbol": symbol, "bars_uri": bars_uri, "error": str(exc)})
            row_summaries.append(
                {
                    "symbol": symbol,
                    "dataset_id": dataset_id,
                    "bars_uri": bars_uri,
                    "listing_line_count": 0,
                    "trade_date_count": 0,
                    "first_trade_date": "",
                    "last_trade_date": "",
                    "status": "listing_failed",
                }
            )

    union_dates = sorted({date for dates in dates_by_symbol.values() for date in dates})
    if not union_dates:
        raise ValueError("No trade_date partitions were found in any launch-row bars URI")

    calendar_rows: list[dict[str, Any]] = []
    for trade_date in union_dates:
        symbols = sorted(symbol for symbol, dates in dates_by_symbol.items() if trade_date in dates)
        calendar_rows.append(
            {
                "trade_date": trade_date,
                "calendar_regime": "unknown",
                "calendar_source": "gcs_option_bar_union",
                "symbol_count": len(symbols),
                "symbols": ",".join(symbols),
            }
        )

    calendar_csv = output_dir / "projection_calendar.csv"
    symbol_summary_csv = output_dir / "projection_calendar_symbol_summary.csv"
    packet_json = output_dir / "projection_calendar_packet.json"
    packet_md = output_dir / "projection_calendar_packet.md"
    _write_csv(
        calendar_csv,
        calendar_rows,
        ["trade_date", "calendar_regime", "calendar_source", "symbol_count", "symbols"],
    )
    _write_csv(
        symbol_summary_csv,
        row_summaries,
        [
            "symbol",
            "dataset_id",
            "bars_uri",
            "listing_line_count",
            "trade_date_count",
            "first_trade_date",
            "last_trade_date",
            "status",
        ],
    )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "projection_calendar_complete" if not errors else "projection_calendar_partial",
        "mode": "research_only_gcs_option_bar_union",
        "broker_facing": False,
        "paper_orders": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "launch_rows_json": str(launch_rows_json),
        "launch_row_count": len(launch_rows),
        "symbols_with_dates": len([symbol for symbol, dates in dates_by_symbol.items() if dates]),
        "symbols_with_errors": len({item["symbol"] for item in errors}),
        "trade_date_count": len(union_dates),
        "first_trade_date": union_dates[0],
        "last_trade_date": union_dates[-1],
        "calendar_regime_label_source": "not_provided",
        "errors": errors,
        "symbols": row_summaries,
        "artifacts": {
            "calendar_csv": str(calendar_csv),
            "symbol_summary_csv": str(symbol_summary_csv),
            "packet_json": str(packet_json),
            "packet_md": str(packet_md),
        },
    }
    packet_json.write_text(json.dumps(packet, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(packet_md, packet)
    return packet


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# GCS Projection Calendar",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Status: `{packet['status']}`",
        f"- Launch rows: `{packet['launch_row_count']}`",
        f"- Symbols with dates: `{packet['symbols_with_dates']}`",
        f"- Symbols with errors: `{packet['symbols_with_errors']}`",
        f"- Trade dates: `{packet['trade_date_count']}`",
        f"- First trade date: `{packet['first_trade_date']}`",
        f"- Last trade date: `{packet['last_trade_date']}`",
        f"- Calendar regime labels: `{packet['calendar_regime_label_source']}`",
        f"- Broker facing: `{str(packet['broker_facing']).lower()}`",
        "",
        "## Symbol Coverage",
        "",
        "| Symbol | Dataset | Trade Dates | First | Last | Status |",
        "| --- | --- | ---: | --- | --- | --- |",
    ]
    for row in packet["symbols"]:
        lines.append(
            f"| `{row['symbol']}` | `{row['dataset_id']}` | {row['trade_date_count']} | "
            f"`{row['first_trade_date']}` | `{row['last_trade_date']}` | `{row['status']}` |"
        )
    if packet["errors"]:
        lines.extend(["", "## Errors", ""])
        for error in packet["errors"]:
            lines.append(f"- `{error['symbol']}` `{error['bars_uri']}`: {error['error']}")
    lines.extend(
        [
            "",
            "Hard rule: this calendar is research-only. It does not authorize trading, paper orders, live-manifest edits, or risk-policy edits.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    packet = build_gcs_projection_calendar(
        launch_rows_json=Path(args.launch_rows_json),
        output_dir=Path(args.output_dir),
        gcloud=args.gcloud,
        listing_cache_json=Path(args.listing_cache_json) if args.listing_cache_json else None,
    )
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

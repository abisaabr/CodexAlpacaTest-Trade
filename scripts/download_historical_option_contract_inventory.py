from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - used when imported as scripts.*
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.data.chunking import iter_date_chunks
from alpaca_lab.data.manifests import BuildManifestStore
from alpaca_lab.data.normalization import normalize_option_contract_inventory
from alpaca_lab.data.quality import build_quality_rows
from alpaca_lab.data.schemas import OPTION_CONTRACT_SCHEMA
from alpaca_lab.data.storage import ensure_directory, write_json
from alpaca_lab.logging_utils import configure_logging

DEFAULT_STATUSES = ("active", "inactive")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Research-only historical option-contract inventory downloader. "
            "This ports the QQQ cleanroom active+inactive contract-discovery "
            "pattern into the governed research runner."
        )
    )
    parser.add_argument("--symbols", required=True, help="Comma-separated underlyings.")
    parser.add_argument("--start-date", required=True, help="Inclusive YYYY-MM-DD trade start.")
    parser.add_argument("--end-date", required=True, help="Inclusive YYYY-MM-DD trade end.")
    parser.add_argument("--build-name", required=True)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--reports-root", default="reports")
    parser.add_argument("--min-dte", type=int, default=0)
    parser.add_argument("--max-dte", type=int, default=7)
    parser.add_argument("--contract-chunk-days", type=int, default=14)
    parser.add_argument(
        "--statuses",
        default="active,inactive",
        help="Comma-separated contract statuses to fetch. Keep active,inactive for historical work.",
    )
    parser.add_argument("--option-type", default=None, help="Optional call/put filter.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def _parse_csv(value: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = list(value)
    return tuple(str(item).strip().upper() for item in items if str(item).strip())


def _parse_statuses(value: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = list(value)
    statuses = tuple(str(item).strip().lower() for item in items if str(item).strip())
    return statuses or DEFAULT_STATUSES


def _raw_payload_path(raw_root: Path, *, underlying: str, status: str, chunk_id: str) -> Path:
    return (
        raw_root
        / "option_contract_inventory"
        / f"underlying={underlying}"
        / f"status={status}"
        / f"chunk={chunk_id}"
        / "payload.json"
    )


def _silver_inventory_path(silver_root: Path, *, underlying: str, chunk_id: str) -> Path:
    return (
        silver_root
        / "option_contract_inventory"
        / f"underlying={underlying}"
        / f"chunk={chunk_id}"
        / "part.parquet"
    )


def _weekday_dates(start_date: date, end_date: date) -> list[date]:
    if end_date < start_date:
        return []
    return [item.date() for item in pd.date_range(start=start_date, end=end_date, freq="B")]


def _dedupe_contract_inventory(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return OPTION_CONTRACT_SCHEMA.apply(pd.DataFrame())

    frame = pd.concat(non_empty, ignore_index=True)
    if frame.empty:
        return OPTION_CONTRACT_SCHEMA.apply(frame)

    frame["symbol"] = frame["symbol"].astype(str)
    frame["underlying_symbol"] = frame["underlying_symbol"].astype(str).str.upper()
    frame["expiration_date"] = pd.to_datetime(frame["expiration_date"], errors="coerce").dt.date
    frame["strike_price"] = pd.to_numeric(frame["strike_price"], errors="coerce")
    frame["option_type"] = frame["option_type"].astype(str).str.lower()
    frame["_status_rank"] = frame["inventory_status"].map({"active": 0, "inactive": 1}).fillna(9)
    frame = frame.sort_values(
        ["symbol", "_status_rank", "inventory_collected_at"], ascending=[True, True, False]
    )
    frame = frame.drop_duplicates(subset=["symbol"], keep="first").drop(columns=["_status_rank"])
    return OPTION_CONTRACT_SCHEMA.apply(frame.reset_index(drop=True))


def build_inventory_coverage_diagnostics(
    inventory: pd.DataFrame,
    *,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    min_dte: int,
    max_dte: int,
) -> dict[str, Any]:
    requested_dates = _weekday_dates(start_date, end_date)
    requested_count = len(requested_dates)
    symbol_rows = []
    low_symbols = []

    frame = inventory.copy()
    if not frame.empty:
        frame["underlying_symbol"] = frame["underlying_symbol"].astype(str).str.upper()
        frame["expiration_date"] = pd.to_datetime(frame["expiration_date"], errors="coerce").dt.date
        frame["option_type"] = frame["option_type"].astype(str).str.lower()

    for symbol in sorted(set(str(symbol).upper() for symbol in symbols)):
        symbol_frame = frame[frame["underlying_symbol"] == symbol] if not frame.empty else frame
        covered_dates: list[str] = []
        call_covered = 0
        put_covered = 0
        for trade_date in requested_dates:
            window_start = trade_date + timedelta(days=min_dte)
            window_end = trade_date + timedelta(days=max_dte)
            eligible = symbol_frame[
                (symbol_frame["expiration_date"] >= window_start)
                & (symbol_frame["expiration_date"] <= window_end)
            ]
            if not eligible.empty:
                covered_dates.append(trade_date.isoformat())
                option_types = set(eligible["option_type"].dropna().astype(str).str.lower())
                call_covered += int("call" in option_types)
                put_covered += int("put" in option_types)
        coverage_ratio = round(len(covered_dates) / requested_count, 6) if requested_count else 0.0
        if requested_count and coverage_ratio < 0.90:
            low_symbols.append(symbol)
        expirations = sorted(
            {
                value.isoformat()
                for value in symbol_frame["expiration_date"].dropna().tolist()
                if isinstance(value, date)
            }
        )
        missing_dates = sorted({item.isoformat() for item in requested_dates} - set(covered_dates))
        symbol_rows.append(
            {
                "symbol": symbol,
                "requested_weekday_trade_date_count": requested_count,
                "covered_trade_date_count": len(covered_dates),
                "coverage_ratio": coverage_ratio,
                "call_covered_trade_date_count": call_covered,
                "put_covered_trade_date_count": put_covered,
                "contract_count": int(len(symbol_frame)),
                "expiration_date_count": len(expirations),
                "first_expiration_date": expirations[0] if expirations else None,
                "last_expiration_date": expirations[-1] if expirations else None,
                "missing_trade_dates_sample": missing_dates[:10],
                "coverage_status": "ok" if coverage_ratio >= 0.90 else "coverage_gap",
            }
        )

    return {
        "status": "ok" if symbol_rows and not low_symbols else "inventory_trade_date_coverage_gap",
        "requested_weekday_trade_dates": [item.isoformat() for item in requested_dates],
        "requested_weekday_trade_date_count": requested_count,
        "min_dte": min_dte,
        "max_dte": max_dte,
        "low_inventory_coverage_symbols": low_symbols,
        "symbol_coverage": symbol_rows,
    }


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Historical Option Contract Inventory Packet",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Status: `{packet['status']}`",
        f"- Build name: `{packet['build_name']}`",
        f"- Broker order effect: `{packet['broker_order_effect']}`",
        f"- Trading effect: `{packet['trading_effect']}`",
        f"- Symbols: `{', '.join(packet['symbols'])}`",
        f"- Contract count: `{packet['contract_count']}`",
        f"- Failed chunk count: `{packet['failed_chunk_count']}`",
        f"- Coverage status: `{packet['coverage_diagnostics']['status']}`",
        "",
        "## Symbol Coverage",
        "",
    ]
    for row in packet["coverage_diagnostics"]["symbol_coverage"]:
        lines.append(
            "- "
            f"`{row['symbol']}` contracts `{row['contract_count']}` "
            f"covered days `{row['covered_trade_date_count']}/"
            f"{row['requested_weekday_trade_date_count']}` "
            f"ratio `{row['coverage_ratio']}` status `{row['coverage_status']}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def download_historical_option_contract_inventory(
    *,
    broker: Any,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    build_name: str,
    data_root: Path,
    reports_root: Path,
    min_dte: int = 0,
    max_dte: int = 7,
    contract_chunk_days: int = 14,
    statuses: Sequence[str] = DEFAULT_STATUSES,
    option_type: str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date.")
    if min_dte < 0 or max_dte < min_dte:
        raise ValueError("DTE window must be non-negative and ordered.")
    if contract_chunk_days <= 0:
        raise ValueError("contract_chunk_days must be positive.")

    normalized_symbols = tuple(
        sorted(set(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    )
    normalized_statuses = _parse_statuses(statuses)
    if not normalized_symbols:
        raise ValueError("At least one symbol is required.")

    raw_root = data_root / "raw" / "historical" / build_name
    silver_root = data_root / "silver" / "historical" / build_name
    report_root = reports_root / build_name
    manifest_path = data_root / "raw" / "manifests" / f"{build_name}.json"
    ensure_directory(raw_root)
    ensure_directory(silver_root)
    ensure_directory(report_root)
    manifest = BuildManifestStore(
        manifest_path,
        request_payload={
            "mode": "research_only_active_inactive_contract_inventory",
            "symbols": normalized_symbols,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "build_name": build_name,
            "data_root": str(data_root),
            "reports_root": str(reports_root),
            "min_dte": min_dte,
            "max_dte": max_dte,
            "contract_chunk_days": contract_chunk_days,
            "statuses": normalized_statuses,
            "option_type": option_type,
            "overwrite": overwrite,
            "broker_order_effect": "none",
            "trading_effect": "none",
        },
    )

    inventory_end = end_date + timedelta(days=max_dte)
    collected_at = datetime.now(UTC)
    chunk_summaries: list[dict[str, Any]] = []
    all_frames: list[pd.DataFrame] = []

    for underlying in normalized_symbols:
        for chunk in iter_date_chunks(start_date, inventory_end, chunk_days=contract_chunk_days):
            chunk_id = f"{underlying}__{chunk.label}"
            silver_path = _silver_inventory_path(
                silver_root, underlying=underlying, chunk_id=chunk_id
            )
            if not overwrite and manifest.is_completed("option_contract_inventory", chunk_id):
                if silver_path.exists():
                    all_frames.append(pd.read_parquet(silver_path))
                continue

            manifest.start_chunk(
                "option_contract_inventory",
                chunk_id,
                metadata={
                    "underlying": underlying,
                    "expiration_window_start": chunk.start_date.isoformat(),
                    "expiration_window_end": chunk.end_date.isoformat(),
                    "statuses": normalized_statuses,
                    "option_type": option_type,
                },
            )
            try:
                status_frames: list[pd.DataFrame] = []
                raw_artifacts: dict[str, Path] = {}
                status_counts: dict[str, int] = {}
                for status in normalized_statuses:
                    payload = broker.get_option_contracts(
                        [underlying],
                        expiration_date_gte=chunk.start_date.isoformat(),
                        expiration_date_lte=chunk.end_date.isoformat(),
                        option_type=option_type,
                        status=status,
                    )
                    raw_path = _raw_payload_path(
                        raw_root, underlying=underlying, status=status, chunk_id=chunk_id
                    )
                    write_json(raw_path, payload)
                    raw_artifacts[f"raw_{status}"] = raw_path
                    frame = normalize_option_contract_inventory(
                        payload,
                        inventory_status=status,
                        inventory_collected_at=collected_at,
                        expiration_window_start=chunk.start_date,
                        expiration_window_end=chunk.end_date,
                    )
                    status_counts[status] = int(len(frame))
                    status_frames.append(frame)

                frame = _dedupe_contract_inventory(status_frames)
                quality_rows = build_quality_rows(
                    "option_contract_inventory",
                    frame,
                    OPTION_CONTRACT_SCHEMA,
                    chunk_id,
                    group_columns=("underlying_symbol",),
                )
                warnings = (
                    ["empty active+inactive option contract inventory response"]
                    if frame.empty
                    else []
                )
                ensure_directory(silver_path.parent)
                frame.to_parquet(silver_path, index=False)
                all_frames.append(frame)
                manifest.complete_chunk(
                    "option_contract_inventory",
                    chunk_id,
                    row_count=int(len(frame)),
                    artifacts={**raw_artifacts, "silver": silver_path},
                    quality=quality_rows,
                    warnings=warnings,
                )
                chunk_summaries.append(
                    {
                        "underlying": underlying,
                        "chunk_id": chunk_id,
                        "expiration_window_start": chunk.start_date.isoformat(),
                        "expiration_window_end": chunk.end_date.isoformat(),
                        "status_counts": status_counts,
                        "deduped_contract_count": int(len(frame)),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                manifest.fail_chunk("option_contract_inventory", chunk_id, error=str(exc))
                raise

    inventory = _dedupe_contract_inventory(all_frames)
    combined_path = silver_root / "option_contract_inventory_combined.parquet"
    ensure_directory(combined_path.parent)
    inventory.to_parquet(combined_path, index=False)
    coverage_diagnostics = build_inventory_coverage_diagnostics(
        inventory,
        symbols=normalized_symbols,
        start_date=start_date,
        end_date=end_date,
        min_dte=min_dte,
        max_dte=max_dte,
    )
    failed_frame = manifest.failed_chunks_frame()
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": (
            "contract_inventory_complete"
            if failed_frame.empty
            else "contract_inventory_completed_with_failures"
        ),
        "mode": "research_only_active_inactive_contract_inventory",
        "build_name": build_name,
        "symbols": list(normalized_symbols),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "min_dte": min_dte,
        "max_dte": max_dte,
        "contract_chunk_days": contract_chunk_days,
        "statuses": list(normalized_statuses),
        "option_type": option_type,
        "raw_root": str(raw_root),
        "silver_root": str(silver_root),
        "option_contracts_root": str(silver_root / "option_contract_inventory"),
        "combined_inventory_path": str(combined_path),
        "report_root": str(report_root),
        "manifest_path": str(manifest_path),
        "contract_count": int(len(inventory)),
        "chunk_count": len(chunk_summaries),
        "failed_chunk_count": int(len(failed_frame)),
        "chunk_summaries": chunk_summaries,
        "coverage_diagnostics": coverage_diagnostics,
        "broker_metadata_endpoint_used": True,
        "broker_order_effect": "none",
        "trading_effect": "none",
        "promotion_allowed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "next_step_contract": [
            "Use option_contracts_root with build_dense_option_universe.py.",
            "Proceed to option bar/trade download only if dense coverage diagnostics reach the 0.90 gate.",
            "Treat coverage gaps as data-foundation blockers, not strategy promotion evidence.",
            "Do not relax the governed 0.90 fill gate for promotion.",
        ],
    }
    write_json(report_root / "historical_option_contract_inventory_packet.json", packet)
    _write_markdown(report_root / "historical_option_contract_inventory_packet.md", packet)
    manifest.write_summary(
        "artifacts",
        {
            "packet_json": str(report_root / "historical_option_contract_inventory_packet.json"),
            "packet_md": str(report_root / "historical_option_contract_inventory_packet.md"),
            "silver_root": str(silver_root),
            "option_contracts_root": str(silver_root / "option_contract_inventory"),
            "combined_inventory_path": str(combined_path),
        },
    )
    manifest.write_summary("coverage_diagnostics", coverage_diagnostics)
    manifest.write_summary("retry_summary", manifest.retry_summary())
    return packet


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    broker = AlpacaBrokerAdapter(settings, dry_run=True)
    try:
        packet = download_historical_option_contract_inventory(
            broker=broker,
            symbols=_parse_csv(args.symbols),
            start_date=date.fromisoformat(args.start_date),
            end_date=date.fromisoformat(args.end_date),
            build_name=args.build_name,
            data_root=Path(args.data_root),
            reports_root=Path(args.reports_root),
            min_dte=args.min_dte,
            max_dte=args.max_dte,
            contract_chunk_days=args.contract_chunk_days,
            statuses=_parse_statuses(args.statuses),
            option_type=args.option_type,
            overwrite=args.overwrite,
        )
    finally:
        broker.close()
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

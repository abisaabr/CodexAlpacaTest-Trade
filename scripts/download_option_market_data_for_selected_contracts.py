from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.data.chunking import batched, market_session_bounds
from alpaca_lab.data.manifests import BuildManifestStore
from alpaca_lab.data.normalization import normalize_option_bar_chunk, normalize_option_trade_chunk
from alpaca_lab.data.quality import build_quality_rows
from alpaca_lab.data.schemas import OPTION_BAR_SCHEMA, OPTION_TRADE_SCHEMA, SELECTED_OPTION_SCHEMA
from alpaca_lab.data.storage import ensure_directory, write_json
from alpaca_lab.logging_utils import configure_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Research-only downloader for option bars/trades from an arbitrary "
            "selected_option_contracts root."
        )
    )
    parser.add_argument("--selected-contracts-root", required=True)
    parser.add_argument("--build-name", required=True)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--reports-root", default="reports")
    parser.add_argument("--option-batch-size", type=int, default=20)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument(
        "--include-option-bars", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--include-option-trades", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--config", default=None)
    return parser.parse_args()


def _load_selected_contracts(root: Path) -> pd.DataFrame:
    if not root.exists():
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())
    frames = [pd.read_parquet(path) for path in sorted(root.rglob("*.parquet"))]
    if not frames:
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())
    frame = SELECTED_OPTION_SCHEMA.apply(pd.concat(frames, ignore_index=True))
    frame = frame.dropna(subset=["trade_date", "underlying_symbol", "symbol"])
    return frame.drop_duplicates(subset=["trade_date", "symbol"]).reset_index(drop=True)


def _filter_dates(
    frame: pd.DataFrame, *, start_date: date | None, end_date: date | None
) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.date
    if start_date is not None:
        result = result[result["trade_date"] >= start_date]
    if end_date is not None:
        result = result[result["trade_date"] <= end_date]
    return result.reset_index(drop=True)


def _selected_path(root: Path, underlying: str, trade_date_value: date) -> Path:
    return (
        root
        / f"underlying={underlying}"
        / f"trade_date={trade_date_value.isoformat()}"
        / "part.parquet"
    )


def _market_data_paths(
    *,
    raw_root: Path,
    silver_root: Path,
    dataset: str,
    underlying: str,
    trade_date_value: date,
    batch_index: int,
) -> tuple[Path, Path]:
    raw_path = (
        raw_root
        / dataset
        / f"underlying={underlying}"
        / f"trade_date={trade_date_value.isoformat()}"
        / f"batch={batch_index:03d}"
        / "payload.json"
    )
    silver_path = (
        silver_root
        / dataset
        / f"underlying={underlying}"
        / f"trade_date={trade_date_value.isoformat()}"
        / f"batch={batch_index:03d}"
        / "part.parquet"
    )
    return raw_path, silver_path


def _write_selected_tree(root: Path, selected: pd.DataFrame) -> int:
    if selected.empty:
        return 0
    count = 0
    for (underlying, trade_date_value), subset in selected.groupby(
        ["underlying_symbol", "trade_date"], sort=True
    ):
        path = _selected_path(root, str(underlying), trade_date_value)
        ensure_directory(path.parent)
        SELECTED_OPTION_SCHEMA.apply(subset).to_parquet(path, index=False)
        count += 1
    return count


def download_option_market_data_for_selected_contracts(
    *,
    broker: Any,
    selected_contracts_root: Path,
    build_name: str,
    data_root: Path,
    reports_root: Path,
    option_batch_size: int,
    start_date: date | None = None,
    end_date: date | None = None,
    include_option_bars: bool = True,
    include_option_trades: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    if option_batch_size <= 0:
        raise ValueError("option_batch_size must be positive.")
    if not include_option_bars and not include_option_trades:
        raise ValueError("At least one option market data dataset must be enabled.")

    raw_root = data_root / "raw" / "historical" / build_name
    silver_root = data_root / "silver" / "historical" / build_name
    report_root = reports_root / build_name
    manifest_path = data_root / "raw" / "manifests" / f"{build_name}.json"
    ensure_directory(raw_root)
    ensure_directory(silver_root)
    ensure_directory(report_root)

    selected = _filter_dates(
        _load_selected_contracts(selected_contracts_root),
        start_date=start_date,
        end_date=end_date,
    )
    copied_selected_partitions = _write_selected_tree(
        silver_root / "selected_option_contracts", selected
    )
    request_payload = {
        "mode": "research_only_selected_contract_market_data_download",
        "selected_contracts_root": str(selected_contracts_root),
        "build_name": build_name,
        "data_root": str(data_root),
        "reports_root": str(reports_root),
        "option_batch_size": option_batch_size,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "include_option_bars": include_option_bars,
        "include_option_trades": include_option_trades,
        "overwrite": overwrite,
        "broker_facing": False,
        "trading_effect": "none",
    }
    manifest = BuildManifestStore(manifest_path, request_payload=request_payload)

    datasets = []
    if include_option_bars:
        datasets.append("option_bars")
    if include_option_trades:
        datasets.append("option_trades")

    for dataset in datasets:
        for (underlying, trade_date_value), day_selected in selected.groupby(
            ["underlying_symbol", "trade_date"], sort=True
        ):
            underlying = str(underlying)
            trade_date_value = pd.Timestamp(trade_date_value).date()
            symbols = sorted(day_selected["symbol"].dropna().astype(str).unique().tolist())
            underlying_lookup = {
                row.symbol: row.underlying_symbol
                for row in day_selected[["symbol", "underlying_symbol"]]
                .drop_duplicates()
                .itertuples()
            }
            session_start, session_end = market_session_bounds(trade_date_value)
            for batch_index, symbol_batch in enumerate(batched(symbols, option_batch_size)):
                chunk_id = f"{underlying}__{trade_date_value.isoformat()}__batch{batch_index:03d}"
                raw_path, silver_path = _market_data_paths(
                    raw_root=raw_root,
                    silver_root=silver_root,
                    dataset=dataset,
                    underlying=underlying,
                    trade_date_value=trade_date_value,
                    batch_index=batch_index,
                )
                if not overwrite and manifest.is_completed(dataset, chunk_id):
                    continue

                manifest.start_chunk(
                    dataset,
                    chunk_id,
                    metadata={
                        "underlying": underlying,
                        "trade_date": trade_date_value.isoformat(),
                        "symbols": symbol_batch,
                    },
                )
                try:
                    if dataset == "option_bars":
                        payload = broker.get_option_bars(
                            symbol_batch,
                            start=session_start,
                            end=session_end,
                            timeframe="1Min",
                        )
                        frame = normalize_option_bar_chunk(
                            payload,
                            trade_date=trade_date_value,
                            underlying_lookup=underlying_lookup,
                            chunk_id=chunk_id,
                        )
                        schema = OPTION_BAR_SCHEMA
                        check_missing = True
                    else:
                        payload = broker.get_option_trades(
                            symbol_batch,
                            start=session_start,
                            end=session_end,
                        )
                        frame = normalize_option_trade_chunk(
                            payload,
                            trade_date=trade_date_value,
                            underlying_lookup=underlying_lookup,
                            chunk_id=chunk_id,
                        )
                        schema = OPTION_TRADE_SCHEMA
                        check_missing = False

                    quality_rows = build_quality_rows(
                        dataset,
                        frame,
                        schema,
                        chunk_id,
                        group_columns=("underlying_symbol", "symbol", "trade_date"),
                        check_missing_intervals=check_missing,
                    )
                    write_json(raw_path, payload)
                    ensure_directory(silver_path.parent)
                    frame.to_parquet(silver_path, index=False)
                    manifest.complete_chunk(
                        dataset,
                        chunk_id,
                        row_count=int(len(frame)),
                        artifacts={"raw": raw_path, "silver": silver_path},
                        quality=quality_rows,
                        warnings=[f"empty {dataset} response"] if frame.empty else [],
                    )
                except Exception as exc:  # noqa: BLE001
                    manifest.fail_chunk(dataset, chunk_id, error=str(exc))

    chunk_frame = manifest.all_chunks_frame()
    failed_frame = manifest.failed_chunks_frame()
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "completed" if failed_frame.empty else "completed_with_failures",
        "mode": "research_only_selected_contract_market_data_download",
        "broker_facing": False,
        "trading_effect": "none",
        "promotion_allowed": False,
        "build_name": build_name,
        "selected_contracts_root": str(selected_contracts_root),
        "silver_root": str(silver_root),
        "raw_root": str(raw_root),
        "manifest_path": str(manifest_path),
        "report_root": str(report_root),
        "selected_contract_count": int(len(selected)),
        "selected_contract_partitions": copied_selected_partitions,
        "option_batch_size": option_batch_size,
        "dataset_count": len(datasets),
        "chunk_count": int(len(chunk_frame)),
        "failed_chunk_count": int(len(failed_frame)),
        "row_count_by_dataset": (
            {
                str(dataset): int(row_count)
                for dataset, row_count in chunk_frame.groupby("dataset")["row_count"].sum().items()
            }
            if not chunk_frame.empty
            else {}
        ),
        "next_step_contract": [
            "Use the emitted selected_contracts_root, option_bars, and option_trades roots for research-only replay.",
            "Require >=0.90 fill coverage before any promotion packet can advance.",
            "Do not change live manifests, strategy selection, or risk policy from this downloader output.",
        ],
    }
    write_json(report_root / "selected_contract_market_data_download_packet.json", packet)
    return packet


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    broker = AlpacaBrokerAdapter(settings, dry_run=True)
    packet = download_option_market_data_for_selected_contracts(
        broker=broker,
        selected_contracts_root=Path(args.selected_contracts_root),
        build_name=args.build_name,
        data_root=Path(args.data_root),
        reports_root=Path(args.reports_root),
        option_batch_size=args.option_batch_size,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        include_option_bars=args.include_option_bars,
        include_option_trades=args.include_option_trades,
        overwrite=args.overwrite,
    )
    print(json.dumps(packet, indent=2, default=str))
    if packet["failed_chunk_count"]:
        sys.exit(2)


if __name__ == "__main__":
    main()

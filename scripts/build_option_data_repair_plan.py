from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.data.chunking import batched
from alpaca_lab.data.schemas import SELECTED_OPTION_SCHEMA


DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "option_data_repair_plan"
DATASETS = ("option_bars", "option_trades")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a research-only option-data repair plan from a selected-contract root "
            "and an existing downloader manifest."
        )
    )
    parser.add_argument("--manifest-json", required=True)
    parser.add_argument("--selected-contracts-root", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--plan-id", default=None)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=DATASETS,
        default=None,
        help="Dataset to repair. Defaults to option_trades only.",
    )
    parser.add_argument("--option-batch-size", type=int, default=20)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--underlying-filter", default=None)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _symbol_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {item.strip().upper() for item in value.split(",") if item.strip()}
    return symbols or None


def _load_selected_contracts(root: Path) -> pd.DataFrame:
    if not root.exists():
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())
    frames = [pd.read_parquet(path) for path in sorted(root.rglob("*.parquet"))]
    if not frames:
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())
    frame = SELECTED_OPTION_SCHEMA.apply(pd.concat(frames, ignore_index=True))
    frame = frame.dropna(subset=["trade_date", "underlying_symbol", "symbol"])
    frame["underlying_symbol"] = frame["underlying_symbol"].astype(str).str.upper()
    frame["symbol"] = frame["symbol"].astype(str)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    return frame.dropna(subset=["trade_date"]).drop_duplicates(
        subset=["trade_date", "symbol"]
    )


def _filter_selected(
    selected: pd.DataFrame,
    *,
    start_date: date | None,
    end_date: date | None,
    underlying_filter: set[str] | None,
) -> pd.DataFrame:
    if selected.empty:
        return selected
    frame = selected.copy()
    if start_date is not None:
        frame = frame[frame["trade_date"] >= start_date]
    if end_date is not None:
        frame = frame[frame["trade_date"] <= end_date]
    if underlying_filter:
        frame = frame[frame["underlying_symbol"].isin(underlying_filter)]
    return frame.reset_index(drop=True)


def _completed_symbols_by_key(
    manifest: dict[str, Any], datasets: set[str]
) -> dict[tuple[str, str, date], set[str]]:
    completed: dict[tuple[str, str, date], set[str]] = defaultdict(set)
    dataset_payloads = manifest.get("datasets")
    if not isinstance(dataset_payloads, dict):
        return completed

    for dataset in datasets:
        chunks = (dataset_payloads.get(dataset) or {}).get("chunks") or {}
        if not isinstance(chunks, dict):
            continue
        for chunk_id, record in chunks.items():
            if not isinstance(record, dict) or record.get("status") != "completed":
                continue
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            underlying = str(metadata.get("underlying") or str(chunk_id).split("__")[0]).upper()
            trade_date_text = str(metadata.get("trade_date") or "")
            if not trade_date_text and "__" in str(chunk_id):
                trade_date_text = str(chunk_id).split("__")[1]
            try:
                trade_date_value = date.fromisoformat(trade_date_text)
            except ValueError:
                continue
            symbols = metadata.get("symbols")
            if not isinstance(symbols, list):
                symbols = []
            completed[(dataset, underlying, trade_date_value)].update(str(symbol) for symbol in symbols)
    return completed


def _write_partitioned_subset(root: Path, selected: pd.DataFrame) -> int:
    if selected.empty:
        return 0
    count = 0
    for (underlying, trade_date_value), subset in selected.groupby(
        ["underlying_symbol", "trade_date"], sort=True
    ):
        path = (
            root
            / f"underlying={underlying}"
            / f"trade_date={pd.Timestamp(trade_date_value).date().isoformat()}"
            / "part.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        SELECTED_OPTION_SCHEMA.apply(subset).to_parquet(path, index=False)
        count += 1
    return count


def build_option_data_repair_plan(
    *,
    manifest_json: Path,
    selected_contracts_root: Path,
    output_dir: Path,
    plan_id: str | None,
    datasets: tuple[str, ...],
    option_batch_size: int,
    start_date: date | None = None,
    end_date: date | None = None,
    underlying_filter: set[str] | None = None,
) -> dict[str, Any]:
    if option_batch_size <= 0:
        raise ValueError("option_batch_size must be positive.")
    if not datasets:
        raise ValueError("At least one dataset is required.")
    invalid = sorted(set(datasets).difference(DATASETS))
    if invalid:
        raise ValueError(f"Unsupported datasets: {invalid}")

    manifest = _load_json(manifest_json)
    selected = _filter_selected(
        _load_selected_contracts(selected_contracts_root),
        start_date=start_date,
        end_date=end_date,
        underlying_filter=underlying_filter,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    resolved_plan_id = plan_id or f"option_data_repair_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    selected_subset_root = output_dir / "selected_option_contracts"

    completed = _completed_symbols_by_key(manifest, set(datasets))
    batches: list[dict[str, Any]] = []
    dataset_summaries: dict[str, dict[str, int]] = {
        dataset: {
            "expected_symbol_days": 0,
            "completed_symbol_days": 0,
            "remaining_symbol_days": 0,
            "repair_chunks": 0,
        }
        for dataset in datasets
    }
    missing_symbol_keys: set[tuple[str, date, str]] = set()

    for (underlying, trade_date_value), group in selected.groupby(
        ["underlying_symbol", "trade_date"], sort=True
    ):
        underlying = str(underlying).upper()
        trade_date_value = pd.Timestamp(trade_date_value).date()
        expected_symbols = sorted(set(group["symbol"].astype(str)))
        for dataset in datasets:
            completed_symbols = completed.get((dataset, underlying, trade_date_value), set())
            remaining_symbols = [
                symbol for symbol in expected_symbols if symbol not in completed_symbols
            ]
            summary = dataset_summaries[dataset]
            summary["expected_symbol_days"] += len(expected_symbols)
            summary["completed_symbol_days"] += len(expected_symbols) - len(remaining_symbols)
            summary["remaining_symbol_days"] += len(remaining_symbols)
            for symbol in remaining_symbols:
                missing_symbol_keys.add((underlying, trade_date_value, symbol))
            for batch_index, symbol_batch in enumerate(batched(remaining_symbols, option_batch_size)):
                summary["repair_chunks"] += 1
                batches.append(
                    {
                        "dataset": dataset,
                        "repair_chunk_id": (
                            f"{dataset}__{underlying}__{trade_date_value.isoformat()}"
                            f"__repair_batch{batch_index:03d}"
                        ),
                        "underlying": underlying,
                        "trade_date": trade_date_value.isoformat(),
                        "symbols": symbol_batch,
                        "symbol_count": len(symbol_batch),
                    }
                )

    if missing_symbol_keys:
        subset_mask = selected.apply(
            lambda row: (
                str(row["underlying_symbol"]).upper(),
                pd.Timestamp(row["trade_date"]).date(),
                str(row["symbol"]),
            )
            in missing_symbol_keys,
            axis=1,
        )
        selected_subset = selected[subset_mask].copy()
    else:
        selected_subset = selected.iloc[0:0].copy()
    subset_partition_count = _write_partitioned_subset(selected_subset_root, selected_subset)

    recommended_args = [
        "python",
        "scripts/download_option_market_data_for_selected_contracts.py",
        "--selected-contracts-root",
        str(selected_subset_root),
        "--build-name",
        f"{resolved_plan_id}_download",
        "--option-batch-size",
        str(option_batch_size),
    ]
    if "option_bars" not in datasets:
        recommended_args.append("--no-include-option-bars")
    if "option_trades" not in datasets:
        recommended_args.append("--no-include-option-trades")

    batch_frame = pd.DataFrame(batches)
    if not batch_frame.empty:
        batch_frame.to_csv(output_dir / "repair_batches.csv", index=False)
    selected_subset.to_csv(output_dir / "selected_contracts_subset.csv", index=False)

    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "plan_id": resolved_plan_id,
        "status": "ready_repair_download" if batches else "no_repair_needed",
        "mode": "research_only_option_data_repair_plan",
        "broker_facing": False,
        "trading_effect": "none",
        "promotion_allowed": False,
        "manifest_json": str(manifest_json),
        "selected_contracts_root": str(selected_contracts_root),
        "output_dir": str(output_dir),
        "selected_subset_root": str(selected_subset_root),
        "selected_source_rows": int(len(selected)),
        "selected_subset_rows": int(len(selected_subset)),
        "selected_subset_partitions": subset_partition_count,
        "datasets": list(datasets),
        "option_batch_size": option_batch_size,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "underlying_filter": sorted(underlying_filter) if underlying_filter else [],
        "repair_chunk_count": len(batches),
        "dataset_summaries": dataset_summaries,
        "repair_batches": batches,
        "recommended_download_command": " ".join(recommended_args),
        "operator_contract": [
            "Use this plan for research-only repair downloads after the active downloader finishes or stalls.",
            "Prefer trades-only repair when option bars are already complete.",
            "Do not run broad competing Alpaca downloaders while another long-running options downloader is active.",
            "Do not change live manifests, strategy selection, or risk policy from this packet.",
        ],
    }
    (output_dir / "option_data_repair_plan.json").write_text(
        json.dumps(packet, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Option Data Repair Plan",
        "",
        f"- Status: `{packet['status']}`",
        f"- Plan id: `{resolved_plan_id}`",
        f"- Datasets: `{', '.join(datasets)}`",
        f"- Option batch size: `{option_batch_size}`",
        f"- Selected source rows: `{packet['selected_source_rows']}`",
        f"- Selected repair subset rows: `{packet['selected_subset_rows']}`",
        f"- Repair chunks: `{packet['repair_chunk_count']}`",
        f"- Selected repair root: `{selected_subset_root}`",
        "",
        "## Dataset Summary",
        "",
        "| Dataset | Expected symbol-days | Completed symbol-days | Remaining symbol-days | Repair chunks |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for dataset, summary in dataset_summaries.items():
        lines.append(
            f"| {dataset} | {summary['expected_symbol_days']} | "
            f"{summary['completed_symbol_days']} | {summary['remaining_symbol_days']} | "
            f"{summary['repair_chunks']} |"
        )
    lines.extend(
        [
            "",
            "## Recommended Command",
            "",
            f"`{packet['recommended_download_command']}`",
            "",
            "## Operator Contract",
            "",
        ]
    )
    for row in packet["operator_contract"]:
        lines.append(f"- {row}")
    (output_dir / "option_data_repair_plan.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    return packet


def main() -> None:
    args = parse_args()
    packet = build_option_data_repair_plan(
        manifest_json=Path(args.manifest_json),
        selected_contracts_root=Path(args.selected_contracts_root),
        output_dir=Path(args.output_dir),
        plan_id=args.plan_id,
        datasets=tuple(args.dataset or ["option_trades"]),
        option_batch_size=args.option_batch_size,
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        underlying_filter=_symbol_filter(args.underlying_filter),
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True, slots=True)
class StageConfig:
    stage_id: str
    calendar_days: int
    strike_steps: int
    label: str


STAGES: dict[str, StageConfig] = {
    "7d_atm": StageConfig(
        stage_id="7d_atm",
        calendar_days=7,
        strike_steps=0,
        label="7 calendar days, next listed expiration, ATM only",
    ),
    "30d_atm": StageConfig(
        stage_id="30d_atm",
        calendar_days=30,
        strike_steps=0,
        label="30 calendar days, next listed expiration, ATM only",
    ),
    "30d_5x5": StageConfig(
        stage_id="30d_5x5",
        calendar_days=30,
        strike_steps=5,
        label="30 calendar days, next listed expiration, 5 strikes each side",
    ),
    "365d_5x5": StageConfig(
        stage_id="365d_5x5",
        calendar_days=365,
        strike_steps=5,
        label="365 calendar days, next listed expiration, 5 strikes each side",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a research-only option fill ladder dataset for one symbol and stage."
    )
    parser.add_argument("--symbol", required=True, help="Underlying symbol, e.g. QQQ.")
    parser.add_argument("--stage", required=True, choices=sorted(STAGES))
    parser.add_argument("--campaign-id", default="option_fill_ladder_20260429")
    parser.add_argument("--end-date", default="2026-04-28")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--reports-root", default="reports")
    parser.add_argument("--stock-chunk-days", type=int, default=5)
    parser.add_argument("--contract-chunk-days", type=int, default=14)
    parser.add_argument("--option-batch-size", type=int, default=25)
    parser.add_argument("--feed", default="sip")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--upload-gcs", action="store_true")
    parser.add_argument("--gcs-control-root", default="")
    parser.add_argument("--gcs-option-data-root", default="")
    parser.add_argument("--gcs-stock-data-root", default="")
    return parser.parse_args()


def date_window(stage: str, end_date: date) -> tuple[date, date]:
    config = STAGES[stage]
    return end_date - timedelta(days=config.calendar_days - 1), end_date


def dataset_id(campaign_id: str, symbol: str, stage: str) -> str:
    return f"{campaign_id}_{symbol.lower()}_{stage}"


def run_command(command: list[str], *, cwd: Path) -> None:
    print("running", " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def write_stock_only_config(
    path: Path,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    build_name: str,
    stock_chunk_days: int,
    feed: str,
    overwrite: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(
        [
            f"build_name: {build_name}",
            f"stock_symbols: {symbol}",
            "option_underlyings: []",
            f"start_date: {start_date.isoformat()}",
            f"end_date: {end_date.isoformat()}",
            f"stock_chunk_days: {stock_chunk_days}",
            "include_option_bars: false",
            "include_option_trades: false",
            "include_latest_enrichment: false",
            f"feed: {feed}",
            "alpaca_paper_trade: true",
            "alpaca_api_base_url: https://paper-api.alpaca.markets",
            f"alpaca_data_feed: {feed}",
            "live_trading: false",
            "dry_run: true",
            "data_root: data",
            "reports_root: reports",
            "request_timeout_seconds: 60",
            "retry_attempts: 5",
            "log_level: INFO",
            f"overwrite: {str(overwrite).lower()}",
            "",
        ]
    )
    path.write_text(payload, encoding="utf-8")


def _read_parquet_tree(root: Path) -> pd.DataFrame:
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(path) for path in files), ignore_index=True)


def build_coverage_summary(
    *,
    dataset: str,
    symbol: str,
    start_date: date,
    end_date: date,
    selected_root: Path,
    option_bars_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = _read_parquet_tree(selected_root)
    bars = _read_parquet_tree(option_bars_root)
    if selected.empty:
        raise RuntimeError(f"No selected contracts found under {selected_root}")
    if bars.empty:
        raise RuntimeError(f"No option bars found under {option_bars_root}")

    selected["trade_date"] = pd.to_datetime(selected["trade_date"]).dt.date.astype(str)
    selected["expiration_date"] = pd.to_datetime(selected["expiration_date"]).dt.date.astype(str)
    bars["trade_date"] = pd.to_datetime(bars["trade_date"]).dt.date.astype(str)

    selected_cd = (
        selected.loc[:, ["trade_date", "symbol"]]
        .drop_duplicates()
        .rename(columns={"symbol": "contract"})
    )
    bar_cd = (
        bars.loc[:, ["trade_date", "symbol"]]
        .drop_duplicates()
        .rename(columns={"symbol": "contract"})
    )
    merged = selected_cd.merge(
        bar_cd.assign(has_bar=True),
        on=["trade_date", "contract"],
        how="left",
    )
    merged["has_bar"] = merged["has_bar"].fillna(False).astype(bool)
    missing = merged.loc[~merged["has_bar"]].sort_values(["trade_date", "contract"])

    bar_counts = (
        bars.groupby(["trade_date", "symbol"])
        .size()
        .reset_index(name="bar_rows")
        .rename(columns={"symbol": "contract"})
    )
    selected_with_counts = selected_cd.merge(bar_counts, on=["trade_date", "contract"], how="left")
    selected_with_counts["bar_rows"] = selected_with_counts["bar_rows"].fillna(0).astype(int)
    by_day = (
        selected_with_counts.groupby("trade_date")
        .agg(
            selected_contract_days=("contract", "count"),
            contracts_with_bars=("bar_rows", lambda rows: int((rows > 0).sum())),
            option_bar_rows=("bar_rows", "sum"),
            min_bars_per_contract_day=("bar_rows", "min"),
            median_bars_per_contract_day=("bar_rows", "median"),
            max_bars_per_contract_day=("bar_rows", "max"),
        )
        .reset_index()
    )
    by_day["contract_day_coverage"] = (
        by_day["contracts_with_bars"] / by_day["selected_contract_days"]
    ).round(6)

    summary = {
        "status": "complete",
        "dataset_id": dataset,
        "symbol": symbol,
        "date_window": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "expiration_selection": "nearest_listed_expiration_after_trade_date",
        "selected_contract_count": int(len(selected)),
        "selected_contract_day_count": int(len(selected_cd)),
        "selected_trade_date_count": int(selected_cd["trade_date"].nunique()),
        "unique_selected_contract_count": int(selected_cd["contract"].nunique()),
        "option_bar_row_count": int(len(bars)),
        "option_bar_trade_date_count": int(bars["trade_date"].nunique()),
        "contracts_with_any_bar": int(bar_cd["contract"].nunique()),
        "contract_days_with_any_bar": int(merged["has_bar"].sum()),
        "contract_day_coverage": round(float(merged["has_bar"].mean()), 6),
        "missing_contract_day_count": int(len(missing)),
        "bar_rows_per_selected_contract_day": {
            "min": int(selected_with_counts["bar_rows"].min()),
            "median": float(selected_with_counts["bar_rows"].median()),
            "max": int(selected_with_counts["bar_rows"].max()),
        },
        "missing_contract_days_sample": missing.head(50).to_dict(orient="records"),
    }
    (output_dir / "coverage_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    by_day.to_csv(output_dir / "coverage_by_day.csv", index=False)
    selected_with_counts.to_csv(output_dir / "coverage_by_contract_day.csv", index=False)
    return summary


def _split_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Expected GCS URI: {uri}")
    bucket, _, prefix = uri[5:].partition("/")
    return bucket, prefix


def _join_gcs(root: str, *parts: str) -> str:
    return "/".join([root.rstrip("/"), *(part.strip("/") for part in parts if part)])


def upload_path(source: Path, dest_uri: str) -> None:
    from google.cloud import storage

    client = storage.Client()
    bucket_name, prefix = _split_gcs_uri(dest_uri)
    bucket = client.bucket(bucket_name)
    if source.is_dir():
        for path in source.rglob("*"):
            if path.is_file():
                relative = path.relative_to(source).as_posix()
                bucket.blob(f"{prefix.rstrip('/')}/{relative}").upload_from_filename(str(path))
    elif source.exists():
        bucket.blob(prefix).upload_from_filename(str(source))


def maybe_upload_artifacts(
    *,
    args: argparse.Namespace,
    symbol: str,
    stage: str,
    stock_bars_root: Path,
    inventory_root: Path,
    combined_inventory_path: Path,
    option_bars_root: Path,
    wave_root: Path,
    download_report_root: Path,
    manifests: list[Path],
) -> dict[str, str]:
    if not args.upload_gcs:
        return {}
    if not args.gcs_control_root or not args.gcs_option_data_root or not args.gcs_stock_data_root:
        raise ValueError("GCS upload requires control, option-data, and stock-data roots.")

    symbol_stage = [symbol, stage]
    roots = {
        "stock_bars": _join_gcs(args.gcs_stock_data_root, *symbol_stage, "stock_ref_silver", "stock_bars"),
        "contract_inventory": _join_gcs(
            args.gcs_option_data_root, *symbol_stage, "contract_inventory_silver", "option_contract_inventory"
        ),
        "option_bars": _join_gcs(args.gcs_option_data_root, *symbol_stage, "option_bars_silver", "option_bars"),
        "research_wave": _join_gcs(args.gcs_control_root, *symbol_stage, "research_wave"),
        "download_report": _join_gcs(args.gcs_control_root, *symbol_stage, "download_report"),
        "manifests": _join_gcs(args.gcs_control_root, *symbol_stage, "manifests"),
    }
    upload_path(stock_bars_root, roots["stock_bars"])
    upload_path(inventory_root, roots["contract_inventory"])
    if combined_inventory_path.exists():
        upload_path(
            combined_inventory_path,
            _join_gcs(args.gcs_option_data_root, *symbol_stage, "option_contract_inventory_combined.parquet"),
        )
    upload_path(option_bars_root, roots["option_bars"])
    upload_path(wave_root, roots["research_wave"])
    upload_path(download_report_root, roots["download_report"])
    for manifest in manifests:
        if manifest.exists():
            upload_path(manifest, _join_gcs(roots["manifests"], manifest.name))
    return roots


def write_status_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        f"# {packet['symbol']} {packet['stage']} Fill Ladder Dataset",
        "",
        f"- Status: `{packet['status']}`",
        f"- Dataset id: `{packet['dataset_id']}`",
        f"- Stage label: `{packet['stage_label']}`",
        f"- Window: `{packet['date_window']['start']}` through `{packet['date_window']['end']}`",
        f"- Strike steps: `{packet['strike_steps_each_side']}`",
        f"- Selected contract-days: `{packet['coverage']['selected_contract_day_count']}`",
        f"- Option-bar rows: `{packet['coverage']['option_bar_row_count']}`",
        f"- Contract-day coverage: `{packet['coverage']['contract_day_coverage']}`",
        f"- Missing contract-days: `{packet['coverage']['missing_contract_day_count']}`",
        f"- Broker-facing: `{packet['broker_facing']}`",
        f"- Trading effect: `{packet['trading_effect']}`",
        f"- Promotion allowed: `{packet['promotion_allowed']}`",
        "",
        "## Guardrails",
        "",
    ]
    for item in packet["guardrails"]:
        lines.append(f"- {item}")
    if packet.get("gcs_roots"):
        lines.extend(["", "## GCS Roots", ""])
        for key, value in sorted(packet["gcs_roots"].items()):
            lines.append(f"- {key}: `{value}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    symbol = args.symbol.upper()
    stage = STAGES[args.stage]
    end = date.fromisoformat(args.end_date)
    start, end = date_window(args.stage, end)
    base_id = dataset_id(args.campaign_id, symbol, args.stage)

    data_root = Path(args.data_root)
    reports_root = Path(args.reports_root)
    wave_root = reports_root / "research_wave" / args.campaign_id / symbol / args.stage
    dense_root = wave_root / "dense_universe"
    coverage_root = wave_root / "coverage"
    wave_root.mkdir(parents=True, exist_ok=True)

    stock_build = f"{base_id}_stock_ref_{start:%Y%m%d}_{end:%Y%m%d}"
    inventory_build = f"{base_id}_inventory_{start:%Y%m%d}_{end:%Y%m%d}"
    bars_build = f"{base_id}_option_bars_{start:%Y%m%d}_{end:%Y%m%d}"

    stock_config_path = wave_root / "stock_only_config.yaml"
    write_stock_only_config(
        stock_config_path,
        symbol=symbol,
        start_date=start,
        end_date=end,
        build_name=stock_build,
        stock_chunk_days=args.stock_chunk_days,
        feed=args.feed,
        overwrite=args.overwrite,
    )

    run_command(
        [sys.executable, "scripts/build_historical_dataset.py", "--config", str(stock_config_path)],
        cwd=repo_root,
    )

    inventory_command = [
        sys.executable,
        "scripts/download_historical_option_contract_inventory.py",
        "--symbols",
        symbol,
        "--start-date",
        start.isoformat(),
        "--end-date",
        end.isoformat(),
        "--build-name",
        inventory_build,
        "--data-root",
        str(data_root),
        "--reports-root",
        str(reports_root),
        "--min-dte",
        "1",
        "--max-dte",
        "7",
        "--contract-chunk-days",
        str(args.contract_chunk_days),
        "--statuses",
        "active,inactive",
    ]
    if args.overwrite:
        inventory_command.append("--overwrite")
    run_command(inventory_command, cwd=repo_root)

    stock_bars_root = data_root / "silver" / "historical" / stock_build / "stock_bars"
    inventory_root = data_root / "silver" / "historical" / inventory_build / "option_contract_inventory"
    selected_root = dense_root / "selected_option_contracts"
    dense_command = [
        sys.executable,
        "scripts/build_dense_option_universe.py",
        "--stock-bars-path",
        str(stock_bars_root),
        "--option-contracts-root",
        str(inventory_root),
        "--output-dir",
        str(dense_root),
        "--symbol-filter",
        symbol,
        "--start-date",
        start.isoformat(),
        "--end-date",
        end.isoformat(),
        "--min-dte",
        "1",
        "--max-dte",
        "7",
        "--strike-steps",
        str(stage.strike_steps),
        "--expiration-selection",
        "next_after_trade_date",
        "--reference-bar",
        "first",
    ]
    run_command(dense_command, cwd=repo_root)

    download_command = [
        sys.executable,
        "scripts/download_option_market_data_for_selected_contracts.py",
        "--selected-contracts-root",
        str(selected_root),
        "--build-name",
        bars_build,
        "--data-root",
        str(data_root),
        "--reports-root",
        str(reports_root),
        "--option-batch-size",
        str(args.option_batch_size),
        "--start-date",
        start.isoformat(),
        "--end-date",
        end.isoformat(),
        "--include-option-bars",
        "--no-include-option-trades",
    ]
    if args.overwrite:
        download_command.append("--overwrite")
    run_command(download_command, cwd=repo_root)

    option_bars_root = data_root / "silver" / "historical" / bars_build / "option_bars"
    coverage = build_coverage_summary(
        dataset=base_id,
        symbol=symbol,
        start_date=start,
        end_date=end,
        selected_root=selected_root,
        option_bars_root=option_bars_root,
        output_dir=coverage_root,
    )

    manifests = [
        data_root / "raw" / "manifests" / f"{stock_build}.json",
        data_root / "raw" / "manifests" / f"{inventory_build}.json",
        data_root / "raw" / "manifests" / f"{bars_build}.json",
    ]
    gcs_roots = maybe_upload_artifacts(
        args=args,
        symbol=symbol,
        stage=args.stage,
        stock_bars_root=stock_bars_root,
        inventory_root=inventory_root,
        combined_inventory_path=data_root
        / "silver"
        / "historical"
        / inventory_build
        / "option_contract_inventory_combined.parquet",
        option_bars_root=option_bars_root,
        wave_root=wave_root,
        download_report_root=reports_root / bars_build,
        manifests=manifests,
    )

    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "complete",
        "mode": "research_only_option_fill_ladder_dataset",
        "broker_facing": False,
        "trading_effect": "none",
        "promotion_allowed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "campaign_id": args.campaign_id,
        "dataset_id": base_id,
        "symbol": symbol,
        "stage": args.stage,
        "stage_label": stage.label,
        "date_window": {"start": start.isoformat(), "end": end.isoformat()},
        "expiration_selection": "nearest_listed_expiration_after_trade_date",
        "dte_search_window_calendar_days": {"min": 1, "max": 7},
        "strike_steps_each_side": stage.strike_steps,
        "option_types": ["call", "put"],
        "build_names": {
            "stock_ref": stock_build,
            "contract_inventory": inventory_build,
            "option_bars": bars_build,
        },
        "coverage": coverage,
        "local_roots": {
            "stock_bars": str(stock_bars_root),
            "contract_inventory": str(inventory_root),
            "selected_contracts": str(selected_root),
            "option_bars": str(option_bars_root),
            "coverage_report": str(coverage_root),
        },
        "gcs_roots": gcs_roots,
        "guardrails": [
            "Research-only market data artifact.",
            "Do not trade from this packet.",
            "Do not arm an execution window from this packet.",
            "Do not modify live manifests, strategy selection, or risk policy from this packet.",
            "Keep the 0.90 fill gate for promotion review.",
        ],
        "next_safe_step": "Aggregate this fill ladder with sibling symbol/stage datasets before strategy replay.",
    }
    (wave_root / "fill_ladder_status.json").write_text(
        json.dumps(packet, indent=2) + "\n",
        encoding="utf-8",
    )
    write_status_markdown(wave_root / "fill_ladder_status.md", packet)

    if args.upload_gcs and args.gcs_control_root:
        upload_path(wave_root / "fill_ladder_status.json", _join_gcs(args.gcs_control_root, symbol, args.stage, "fill_ladder_status.json"))
        upload_path(wave_root / "fill_ladder_status.md", _join_gcs(args.gcs_control_root, symbol, args.stage, "fill_ladder_status.md"))

    print(json.dumps(packet, indent=2))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


STAGE_ORDER = ["7d_atm", "30d_atm", "30d_5x5", "365d_5x5"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate option fill ladder status packets into a fill-gate matrix.",
    )
    parser.add_argument("--campaign-id", default="option_fill_ladder_20260429")
    parser.add_argument("--reports-root", default="reports")
    parser.add_argument("--output-dir", default="")
    parser.add_argument(
        "--gcs-control-root",
        default="",
        help="Optional GCS root containing per-symbol fill_ladder_status.json packets.",
    )
    parser.add_argument("--upload-gcs", action="store_true")
    parser.add_argument("--fill-gate", type=float, default=0.90)
    return parser.parse_args()


def split_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"expected gs:// URI, got {uri!r}")
    rest = uri[5:]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix.strip("/")


def gcloud_executable() -> str:
    executable = shutil.which("gcloud") or shutil.which("gcloud.cmd")
    if not executable:
        raise SystemExit("gcloud CLI is required for GCS fallback but was not found on PATH")
    return executable


def load_local_packets(root: Path) -> list[dict[str, Any]]:
    packets = []
    for path in sorted(root.glob("*/*/fill_ladder_status.json")):
        packets.append(json.loads(path.read_text(encoding="utf-8")))
    return packets


def load_gcs_packets(root: str) -> list[dict[str, Any]]:
    try:
        from google.cloud import storage
    except ImportError as exc:  # pragma: no cover - exercised only when dependency missing.
        return load_gcs_packets_with_cli(root)

    bucket_name, prefix = split_gcs_uri(root.rstrip("/") + "/")
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        packets = []
        for blob in client.list_blobs(bucket, prefix=prefix):
            if not blob.name.endswith("/fill_ladder_status.json"):
                continue
            packets.append(json.loads(blob.download_as_text(encoding="utf-8")))
        return packets
    except Exception:
        return load_gcs_packets_with_cli(root)


def load_gcs_packets_with_cli(root: str) -> list[dict[str, Any]]:
    list_uri = root.rstrip("/") + "/**/fill_ladder_status.json"
    gcloud = gcloud_executable()
    listed = subprocess.run(
        [gcloud, "storage", "ls", list_uri],
        check=True,
        capture_output=True,
        text=True,
    )
    packets = []
    for uri in [line.strip() for line in listed.stdout.splitlines() if line.strip()]:
        packet = subprocess.run(
            [gcloud, "storage", "cat", uri],
            check=True,
            capture_output=True,
            text=True,
        )
        packets.append(json.loads(packet.stdout))
    return packets


def upload_file(path: Path, destination: str) -> None:
    try:
        from google.cloud import storage
    except ImportError as exc:  # pragma: no cover - exercised only when dependency missing.
        subprocess.run(["gcloud", "storage", "cp", str(path), destination], check=True)
        return

    bucket_name, blob_name = split_gcs_uri(destination)
    try:
        storage.Client().bucket(bucket_name).blob(blob_name).upload_from_filename(str(path))
    except Exception:
        subprocess.run([gcloud_executable(), "storage", "cp", str(path), destination], check=True)


def packet_row(packet: dict[str, Any], fill_gate: float) -> dict[str, Any]:
    coverage = packet.get("coverage", {})
    selected_days = int(coverage.get("selected_contract_day_count") or 0)
    covered_days = int(coverage.get("contract_days_with_any_bar") or 0)
    coverage_ratio = float(coverage.get("contract_day_coverage") or 0.0)
    missing_days = int(coverage.get("missing_contract_day_count") or 0)
    row_count = int(coverage.get("option_bar_row_count") or 0)
    passes = selected_days > 0 and coverage_ratio >= fill_gate
    return {
        "campaign_id": packet.get("campaign_id", ""),
        "symbol": packet.get("symbol", ""),
        "stage": packet.get("stage", ""),
        "status": packet.get("status", ""),
        "start_date": packet.get("date_window", {}).get("start", ""),
        "end_date": packet.get("date_window", {}).get("end", ""),
        "strike_steps_each_side": packet.get("strike_steps_each_side", ""),
        "selected_contract_day_count": selected_days,
        "covered_contract_day_count": covered_days,
        "missing_contract_day_count": missing_days,
        "contract_day_coverage": coverage_ratio,
        "option_bar_row_count": row_count,
        "passes_fill_gate": passes,
    }


def summarize(rows: list[dict[str, Any]], fill_gate: float) -> dict[str, Any]:
    by_stage: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_stage[str(row["stage"])].append(row)
        by_symbol[str(row["symbol"])].append(row)

    stage_summary = {}
    for stage, stage_rows in sorted(by_stage.items()):
        pass_count = sum(1 for row in stage_rows if row["passes_fill_gate"])
        coverage_values = [float(row["contract_day_coverage"]) for row in stage_rows]
        stage_summary[stage] = {
            "packet_count": len(stage_rows),
            "pass_count": pass_count,
            "fail_count": len(stage_rows) - pass_count,
            "min_coverage": min(coverage_values) if coverage_values else 0.0,
            "max_coverage": max(coverage_values) if coverage_values else 0.0,
            "avg_coverage": sum(coverage_values) / len(coverage_values) if coverage_values else 0.0,
            "failing_symbols": sorted(
                row["symbol"] for row in stage_rows if not row["passes_fill_gate"]
            ),
        }

    symbol_summary = {}
    for symbol, symbol_rows in sorted(by_symbol.items()):
        passed_stages = sorted(
            (row["stage"] for row in symbol_rows if row["passes_fill_gate"]),
            key=lambda value: STAGE_ORDER.index(value) if value in STAGE_ORDER else 99,
        )
        failed_stages = sorted(
            (row["stage"] for row in symbol_rows if not row["passes_fill_gate"]),
            key=lambda value: STAGE_ORDER.index(value) if value in STAGE_ORDER else 99,
        )
        symbol_summary[symbol] = {
            "packet_count": len(symbol_rows),
            "passed_stages": passed_stages,
            "failed_stages": failed_stages,
            "ready_for_replay_stages": passed_stages,
        }

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "fill_gate": fill_gate,
        "packet_count": len(rows),
        "stage_summary": stage_summary,
        "symbol_summary": symbol_summary,
        "ready_for_strategy_replay": [
            {"symbol": row["symbol"], "stage": row["stage"], "coverage": row["contract_day_coverage"]}
            for row in sorted(rows, key=lambda item: (item["symbol"], item["stage"]))
            if row["passes_fill_gate"]
        ],
        "blocked_for_strategy_replay": [
            {"symbol": row["symbol"], "stage": row["stage"], "coverage": row["contract_day_coverage"]}
            for row in sorted(rows, key=lambda item: (item["symbol"], item["stage"]))
            if not row["passes_fill_gate"]
        ],
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "campaign_id",
        "symbol",
        "stage",
        "status",
        "start_date",
        "end_date",
        "strike_steps_each_side",
        "selected_contract_day_count",
        "covered_contract_day_count",
        "missing_contract_day_count",
        "contract_day_coverage",
        "option_bar_row_count",
        "passes_fill_gate",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Option Fill Ladder Aggregate Summary",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Fill gate: `{summary['fill_gate']}`",
        f"- Packets aggregated: `{summary['packet_count']}`",
        "",
        "## Stage Summary",
        "",
        "| Stage | Packets | Pass | Fail | Min Coverage | Avg Coverage | Failing Symbols |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for stage in sorted(summary["stage_summary"], key=lambda value: STAGE_ORDER.index(value) if value in STAGE_ORDER else 99):
        item = summary["stage_summary"][stage]
        failing = ", ".join(item["failing_symbols"]) if item["failing_symbols"] else "none"
        lines.append(
            f"| `{stage}` | {item['packet_count']} | {item['pass_count']} | {item['fail_count']} | "
            f"{item['min_coverage']:.6f} | {item['avg_coverage']:.6f} | {failing} |"
        )

    lines.extend(
        [
            "",
            "## Symbol/Stage Matrix",
            "",
            "| Symbol | Stage | Coverage | Selected Days | Covered Days | Missing Days | Gate |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    rows_sorted = sorted(
        rows,
        key=lambda row: (
            row["symbol"],
            STAGE_ORDER.index(row["stage"]) if row["stage"] in STAGE_ORDER else 99,
        ),
    )
    for row in rows_sorted:
        gate = "pass" if row["passes_fill_gate"] else "blocked"
        lines.append(
            f"| `{row['symbol']}` | `{row['stage']}` | {row['contract_day_coverage']:.6f} | "
            f"{row['selected_contract_day_count']} | {row['covered_contract_day_count']} | "
            f"{row['missing_contract_day_count']} | `{gate}` |"
        )

    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Research-only aggregate.",
            "- Do not trade from this packet.",
            "- Do not modify live manifests, strategy selection, or risk policy from this packet.",
            "- Keep the `0.90` fill gate for promotion review.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    reports_root = Path(args.reports_root)
    output_dir = Path(args.output_dir) if args.output_dir else reports_root / "research_wave" / args.campaign_id / "summary"
    output_dir.mkdir(parents=True, exist_ok=True)

    packets = load_gcs_packets(args.gcs_control_root) if args.gcs_control_root else load_local_packets(reports_root / "research_wave" / args.campaign_id)
    rows = [packet_row(packet, args.fill_gate) for packet in packets]
    rows.sort(key=lambda row: (row["symbol"], row["stage"]))
    summary = summarize(rows, args.fill_gate)

    json_path = output_dir / "fill_ladder_aggregate_summary.json"
    csv_path = output_dir / "fill_ladder_aggregate_rows.csv"
    md_path = output_dir / "fill_ladder_aggregate_summary.md"
    json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_csv(csv_path, rows)
    write_markdown(md_path, summary, rows)

    if args.upload_gcs and args.gcs_control_root:
        upload_file(json_path, args.gcs_control_root.rstrip("/") + "/summary/fill_ladder_aggregate_summary.json")
        upload_file(csv_path, args.gcs_control_root.rstrip("/") + "/summary/fill_ladder_aggregate_rows.csv")
        upload_file(md_path, args.gcs_control_root.rstrip("/") + "/summary/fill_ladder_aggregate_summary.md")

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

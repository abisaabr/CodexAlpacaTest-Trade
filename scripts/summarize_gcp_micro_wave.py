from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Harvest option-aware candidate summaries from a GCP micro-shard wave."
    )
    parser.add_argument("--wave-id", required=True)
    parser.add_argument("--candidate-start", type=int, default=1)
    parser.add_argument("--candidate-end", type=int, required=True)
    parser.add_argument("--bucket", default="codexalpaca-control-us")
    parser.add_argument("--gcloud-bin", default=os.environ.get("GCLOUD_BIN", "gcloud"))
    parser.add_argument("--project", default="codexalpaca")
    parser.add_argument("--output-json", default="")
    return parser.parse_args()


def _gcloud_cat(uri: str, *, project: str, gcloud_bin: str) -> str | None:
    command = [gcloud_bin, "storage", "cat", uri, "--project", project]
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return result.stdout


def _summary_uri(*, bucket: str, wave_id: str, candidate_index: int) -> str:
    candidate = f"{candidate_index:03d}"
    worker_id = f"qqqfamilymicro_qqq_e0x60_c{candidate}"
    run_id = f"{worker_id}_qqq_e0_x60_entry_liquidity_first_research_only"
    return (
        f"gs://{bucket}/research_results/{wave_id}/workers/{worker_id}/"
        f"reports/research_wave/{run_id}/{run_id}/option_aware_candidate_summary.json"
    )


def _load_rows(payload: str) -> list[dict[str, Any]]:
    value = json.loads(payload)
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _eligible_like(row: dict[str, Any]) -> bool:
    return (
        float(row.get("fill_coverage") or 0.0) >= 0.90
        and int(row.get("option_trade_count") or 0) >= 20
        and float(row.get("net_pnl") or 0.0) > 0
        and float(row.get("test_net_pnl") or 0.0) > 0
    )


def main() -> int:
    args = parse_args()
    rows: list[dict[str, Any]] = []
    missing: list[int] = []
    for candidate_index in range(args.candidate_start, args.candidate_end + 1):
        payload = _gcloud_cat(
            _summary_uri(
                bucket=args.bucket,
                wave_id=args.wave_id,
                candidate_index=candidate_index,
            ),
            project=args.project,
            gcloud_bin=args.gcloud_bin,
        )
        if payload is None:
            missing.append(candidate_index)
            continue
        rows.extend(_load_rows(payload))

    compact = []
    for row in rows:
        compact.append(
            {
                "candidate_variant_id": row.get("candidate_variant_id"),
                "eligible_like": _eligible_like(row),
                "family": row.get("family"),
                "fill_coverage": row.get("fill_coverage"),
                "intended_regime": row.get("intended_regime"),
                "net_pnl": row.get("net_pnl"),
                "option_trade_count": row.get("option_trade_count"),
                "profit_factor": row.get("profit_factor"),
                "recommendation": row.get("recommendation"),
                "test_net_pnl": row.get("test_net_pnl"),
            }
        )
    compact.sort(
        key=lambda row: (
            not bool(row["eligible_like"]),
            str(row.get("intended_regime") or ""),
            -(float(row.get("net_pnl") or 0.0)),
        )
    )
    result = {
        "completed": len(rows),
        "eligible_like": sum(1 for row in compact if row["eligible_like"]),
        "missing_candidate_indices": missing,
        "rows": compact,
        "wave_id": args.wave_id,
    }
    if args.output_json:
        path = Path(args.output_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

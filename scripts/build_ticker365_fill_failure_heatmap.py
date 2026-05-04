from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GCLOUD = (
    r"C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
)
DEFAULT_WAVE_GCS_PREFIX = (
    "gs://codexalpaca-control-us/research_results/"
    "ticker365_entry_asof_rescue_20260503T2352Z"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "reports"
    / "gcp_research"
    / "ticker365_entry_asof_rescue_paper_readiness_20260503"
    / "fill_failure_heatmap"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a fill-failure heatmap and strategy timing-redesign target list "
            "from ticker365 option-aware worker artifacts."
        )
    )
    parser.add_argument("--wave-gcs-prefix", default=DEFAULT_WAVE_GCS_PREFIX)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--gcloud-bin", default=os.environ.get("GCLOUD", DEFAULT_GCLOUD))
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument(
        "--symbol-filter",
        default=None,
        help="Optional comma-separated symbol allowlist, e.g. QQQ.",
    )
    parser.add_argument("--skip-download", action="store_true")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_command(command: list[str]) -> str:
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=900,
    )
    if result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(command)}\n{result.stdout}")
    return result.stdout or ""


def gcloud_ls(gcloud_bin: str, pattern: str) -> list[str]:
    output = run_command([gcloud_bin, "storage", "ls", pattern])
    return [line.strip() for line in output.splitlines() if line.strip().startswith("gs://")]


def gcloud_cat(gcloud_bin: str, uri: str) -> str:
    return run_command([gcloud_bin, "storage", "cat", uri])


def local_name(uri: str) -> str:
    digest = hashlib.sha256(uri.encode("utf-8")).hexdigest()[:16]
    return f"{digest}_{Path(uri).name}"


def download_artifacts(
    *,
    gcloud_bin: str,
    wave_gcs_prefix: str,
    output_dir: Path,
    skip_download: bool,
    symbol_filter: set[str] | None,
) -> dict[str, list[Path]]:
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    worker_glob = "**"
    if symbol_filter and len(symbol_filter) == 1:
        worker_glob = f"ticker365fillrepair_{next(iter(symbol_filter)).lower()}"
    patterns = {
        "candidate_summary": (
            f"{wave_gcs_prefix.rstrip('/')}/workers/{worker_glob}/**/"
            "option_aware_candidate_summary.json"
        ),
        "fill_failures": (
            f"{wave_gcs_prefix.rstrip('/')}/workers/{worker_glob}/**/"
            "option_aware_fill_failures.json"
        ),
    }
    downloaded: dict[str, list[Path]] = {"candidate_summary": [], "fill_failures": []}
    if skip_download:
        for key in downloaded:
            downloaded[key] = sorted((raw_dir / key).glob("*.json"))
        return downloaded
    for key, pattern in patterns.items():
        target_dir = raw_dir / key
        target_dir.mkdir(parents=True, exist_ok=True)
        for uri in gcloud_ls(gcloud_bin, pattern):
            path = target_dir / local_name(uri)
            path.write_text(gcloud_cat(gcloud_bin, uri), encoding="utf-8")
            sidecar = path.with_suffix(path.suffix + ".uri")
            sidecar.write_text(uri + "\n", encoding="utf-8")
            downloaded[key].append(path)
    return downloaded


def artifact_uri(path: Path) -> str:
    sidecar = path.with_suffix(path.suffix + ".uri")
    if sidecar.exists():
        return sidecar.read_text(encoding="utf-8").strip()
    return str(path)


def run_profile_from_uri(uri: str) -> str:
    parts = [part for part in uri.replace("\\", "/").split("/") if part]
    if len(parts) >= 2:
        return parts[-2]
    return "unknown_profile"


def load_json_rows(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8") or "[]")
        uri = artifact_uri(path)
        profile = run_profile_from_uri(uri)
        if isinstance(payload, dict):
            values = payload.get("candidate_summaries") or payload.get("fill_failure_rows") or []
        elif isinstance(payload, list):
            values = payload
        else:
            values = []
        for row in values:
            if isinstance(row, dict):
                item = dict(row)
                item["artifact_uri"] = uri
                item["aggregate_profile"] = profile
                rows.append(item)
    return rows


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def parameter_timing_profile(row: dict[str, Any]) -> str:
    raw = row.get("parameter_set")
    if isinstance(raw, dict):
        return str(raw.get("timing_profile") or "")
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            return str(json.loads(raw).get("timing_profile") or "")
        except json.JSONDecodeError:
            return ""
    return ""


def counter_rows(counter: Counter[tuple[Any, ...] | str], columns: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, count in counter.most_common():
        values = key if isinstance(key, tuple) else (key,)
        row = {column: values[index] if index < len(values) else "" for index, column in enumerate(columns)}
        row["count"] = count
        rows.append(row)
    return rows


def bucket_minutes(value: Any) -> str:
    if value in (None, ""):
        return "missing"
    minutes = as_float(value, -1.0)
    if minutes < 0:
        return "missing"
    if minutes <= 1:
        return "0_to_1m"
    if minutes <= 5:
        return "1_to_5m"
    if minutes <= 15:
        return "5_to_15m"
    if minutes <= 60:
        return "15_to_60m"
    return "over_60m"


def dominant_reason(row: dict[str, Any]) -> str:
    missing = {
        "entry_bar_gap_or_entry_timing_mismatch": as_int(row.get("missing_no_entry_bar")),
        "exit_bar_gap_or_exit_policy_mismatch": as_int(row.get("missing_no_exit_bar")),
        "position_sizing_too_expensive": as_int(row.get("missing_too_expensive")),
        "selected_contract_universe_gap": as_int(row.get("missing_no_selected_contract")),
    }
    reason, count = max(missing.items(), key=lambda item: item[1])
    return reason if count > 0 else str(row.get("fill_failure_reason") or "unknown")


def redesign_action(row: dict[str, Any]) -> str:
    entry = as_float(row.get("entry_bar_coverage"))
    exit_ = as_float(row.get("exit_bar_coverage"))
    reason = dominant_reason(row)
    if reason == "position_sizing_too_expensive":
        return "position_sizing_or_contract_moneyness_redesign"
    if reason == "selected_contract_universe_gap":
        return "selected_contract_universe_repair"
    if entry + 0.05 < exit_:
        return "entry_timing_redesign"
    if exit_ + 0.05 < entry:
        return "exit_policy_redesign"
    if "entry" in reason:
        return "entry_timing_redesign"
    if "exit" in reason:
        return "exit_policy_redesign"
    return "entry_exit_joint_redesign"


def target_score(row: dict[str, Any]) -> tuple[float, float, float, float]:
    net = as_float(row.get("net_pnl"))
    test = as_float(row.get("test_net_pnl"))
    fill = as_float(row.get("strategy_fill_coverage") or row.get("fill_coverage"))
    data = as_float(row.get("data_foundation_coverage"))
    positive = 1.0 if net > 0 and test > 0 else 0.0
    return positive, data, fill, test


def build_redesign_targets(summary_rows: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in summary_rows:
        fill = as_float(row.get("strategy_fill_coverage") or row.get("fill_coverage"))
        data = as_float(row.get("data_foundation_coverage"))
        trades = as_int(row.get("option_trade_count"))
        if fill >= 0.90 or data < 0.95 or trades < 20:
            continue
        key = (str(row.get("candidate_variant_id") or ""), str(row.get("aggregate_profile") or ""))
        if key[0] == "":
            continue
        current = dedup.get(key)
        if current is None or target_score(row) > target_score(current):
            dedup[key] = row

    targets: list[dict[str, Any]] = []
    for row in sorted(dedup.values(), key=target_score, reverse=True)[:top_n]:
        targets.append(
            {
                "candidate_variant_id": row.get("candidate_variant_id"),
                "symbol": str(row.get("symbol") or "").upper(),
                "source_strategy_id": row.get("source_strategy_id"),
                "family": row.get("family"),
                "intended_regime": row.get("intended_regime"),
                "timing_profile": parameter_timing_profile(row),
                "aggregate_profile": row.get("aggregate_profile"),
                "contract_selection_method": row.get("contract_selection_method"),
                "max_entry_lag_minutes": row.get("max_entry_lag_minutes"),
                "max_exit_lag_minutes": row.get("max_exit_lag_minutes"),
                "strategy_fill_coverage": row.get("strategy_fill_coverage") or row.get("fill_coverage"),
                "data_foundation_coverage": row.get("data_foundation_coverage"),
                "entry_bar_coverage": row.get("entry_bar_coverage"),
                "exit_bar_coverage": row.get("exit_bar_coverage"),
                "option_trade_count": row.get("option_trade_count"),
                "net_pnl": row.get("net_pnl"),
                "test_net_pnl": row.get("test_net_pnl"),
                "dominant_failure_reason": dominant_reason(row),
                "recommended_redesign_action": redesign_action(row),
            }
        )
    return targets


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Ticker365 Fill-Failure Heatmap",
        "",
        f"- Generated UTC: `{packet['generated_at_utc']}`",
        f"- Source wave: `{packet['wave_gcs_prefix']}`",
        f"- Candidate summary files: `{packet['artifact_counts']['candidate_summary_files']}`",
        f"- Fill failure files: `{packet['artifact_counts']['fill_failure_files']}`",
        f"- Candidate rows: `{packet['artifact_counts']['candidate_summary_rows']}`",
        f"- Fill failure rows: `{packet['artifact_counts']['fill_failure_rows']}`",
        f"- Decision: `{packet['decision']}`",
        "",
        "## Failure Reasons",
        "",
    ]
    for row in packet["heatmaps"]["failure_by_reason"][:10]:
        lines.append(f"- `{row['failure_reason']}`: `{row['count']}`")
    lines.extend(["", "## Top Symbol/Reason Hotspots", ""])
    for row in packet["heatmaps"]["failure_by_symbol_reason"][:15]:
        lines.append(f"- `{row['symbol']}` `{row['failure_reason']}`: `{row['count']}`")
    lines.extend(["", "## Top Redesign Targets", ""])
    for row in packet["redesign_targets"][:20]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['source_strategy_id']}` `{row['timing_profile']}` "
            f"action `{row['recommended_redesign_action']}` "
            f"fill `{row['strategy_fill_coverage']}` "
            f"entry `{row['entry_bar_coverage']}` exit `{row['exit_bar_coverage']}` "
            f"test_pnl `{row['test_net_pnl']}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_heatmap(
    *,
    wave_gcs_prefix: str,
    output_dir: Path,
    gcloud_bin: str,
    top_n: int,
    skip_download: bool,
    symbol_filter: set[str] | None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded = download_artifacts(
        gcloud_bin=gcloud_bin,
        wave_gcs_prefix=wave_gcs_prefix,
        output_dir=output_dir,
        skip_download=skip_download,
        symbol_filter=symbol_filter,
    )
    summary_rows = load_json_rows(downloaded["candidate_summary"])
    failure_rows = load_json_rows(downloaded["fill_failures"])
    if symbol_filter:
        summary_rows = [
            row for row in summary_rows if str(row.get("symbol") or "").upper() in symbol_filter
        ]
        failure_rows = [
            row for row in failure_rows if str(row.get("symbol") or "").upper() in symbol_filter
        ]

    reason_counts: Counter[str] = Counter()
    symbol_reason: Counter[tuple[str, str]] = Counter()
    symbol_family_reason: Counter[tuple[str, str, str]] = Counter()
    profile_reason: Counter[tuple[str, str, str, str]] = Counter()
    context_buckets: Counter[tuple[str, str, str]] = Counter()
    for row in failure_rows:
        reason = str(row.get("failure_reason") or "unknown")
        symbol = str(row.get("symbol") or "").upper() or "UNKNOWN"
        family = str(row.get("family") or "unknown")
        selector = str(row.get("contract_selection_method") or "unknown")
        entry_lag = str(row.get("max_entry_lag_minutes") or "")
        exit_lag = str(row.get("max_exit_lag_minutes") or "")
        reason_counts[reason] += 1
        symbol_reason[(symbol, reason)] += 1
        symbol_family_reason[(symbol, family, reason)] += 1
        profile_reason[(selector, entry_lag, exit_lag, reason)] += 1
        if reason == "no_entry_bar":
            context_buckets[(symbol, "previous_bar_age_minutes", bucket_minutes(row.get("previous_bar_age_minutes")))] += 1
            context_buckets[(symbol, "next_bar_lag_minutes", bucket_minutes(row.get("next_bar_lag_minutes")))] += 1
        if reason == "no_exit_bar":
            context_buckets[(symbol, "exit_previous_bar_age_minutes", bucket_minutes(row.get("previous_bar_age_minutes")))] += 1
            context_buckets[(symbol, "exit_next_bar_lag_minutes", bucket_minutes(row.get("next_bar_lag_minutes")))] += 1

    redesign_targets = build_redesign_targets(summary_rows, top_n=top_n)
    decision = (
        "strategy_entry_exit_redesign_wave_required"
        if redesign_targets
        else "insufficient_redesign_targets"
    )
    packet = {
        "generated_at_utc": utc_now(),
        "wave_gcs_prefix": wave_gcs_prefix,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "symbol_filter": sorted(symbol_filter) if symbol_filter else [],
        "decision": decision,
        "artifact_counts": {
            "candidate_summary_files": len(downloaded["candidate_summary"]),
            "fill_failure_files": len(downloaded["fill_failures"]),
            "candidate_summary_rows": len(summary_rows),
            "fill_failure_rows": len(failure_rows),
        },
        "heatmaps": {
            "failure_by_reason": counter_rows(reason_counts, ["failure_reason"]),
            "failure_by_symbol_reason": counter_rows(symbol_reason, ["symbol", "failure_reason"]),
            "failure_by_symbol_family_reason": counter_rows(
                symbol_family_reason, ["symbol", "family", "failure_reason"]
            ),
            "failure_by_profile_reason": counter_rows(
                profile_reason,
                [
                    "contract_selection_method",
                    "max_entry_lag_minutes",
                    "max_exit_lag_minutes",
                    "failure_reason",
                ],
            ),
            "nearest_bar_context_buckets": counter_rows(
                context_buckets, ["symbol", "context_metric", "bucket"]
            ),
        },
        "redesign_targets": redesign_targets,
        "next_step_contract": [
            "Do not promote any strategy from this heatmap.",
            "Patch timing-profile semantics before rerunning redesign candidates.",
            "Run a strict at-or-after entry replay first; use as-of only as a research diagnostic.",
            "Keep fill_coverage >= 0.90 and all paper/live/risk manifests unchanged.",
            "If the strict redesign wave remains below the fill gate, redesign source signal timing rather than widening the gate.",
        ],
    }

    json_path = output_dir / "ticker365_fill_failure_heatmap.json"
    md_path = output_dir / "ticker365_fill_failure_heatmap.md"
    json_path.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown(md_path, packet)
    write_csv(output_dir / "failure_by_reason.csv", packet["heatmaps"]["failure_by_reason"])
    write_csv(output_dir / "failure_by_symbol_reason.csv", packet["heatmaps"]["failure_by_symbol_reason"])
    write_csv(
        output_dir / "failure_by_symbol_family_reason.csv",
        packet["heatmaps"]["failure_by_symbol_family_reason"],
    )
    write_csv(output_dir / "failure_by_profile_reason.csv", packet["heatmaps"]["failure_by_profile_reason"])
    write_csv(output_dir / "nearest_bar_context_buckets.csv", packet["heatmaps"]["nearest_bar_context_buckets"])
    write_csv(output_dir / "strategy_redesign_targets.csv", redesign_targets)
    (output_dir / "strategy_redesign_targets.json").write_text(
        json.dumps(redesign_targets, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return packet


def main() -> None:
    args = parse_args()
    symbol_filter = (
        {item.strip().upper() for item in args.symbol_filter.split(",") if item.strip()}
        if args.symbol_filter
        else None
    )
    packet = build_heatmap(
        wave_gcs_prefix=args.wave_gcs_prefix,
        output_dir=Path(args.output_dir),
        gcloud_bin=args.gcloud_bin,
        top_n=args.top_n,
        skip_download=args.skip_download,
        symbol_filter=symbol_filter,
    )
    print(json.dumps({key: value for key, value in packet.items() if key != "heatmaps"}, indent=2))


if __name__ == "__main__":
    main()

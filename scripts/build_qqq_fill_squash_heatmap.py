from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_GCLOUD = (
    r"C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a QQQ fill-squash heatmap from option-aware candidate summaries."
    )
    parser.add_argument("--gcloud", default=DEFAULT_GCLOUD)
    parser.add_argument("--gcs-prefix", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fill-coverage-gate", type=float, default=0.90)
    return parser.parse_args()


def run_command(command: list[str]) -> str:
    result = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0:
        output = result.stdout or ""
        if "matched no objects" in output:
            return ""
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(command)}\n{output}")
    return result.stdout or ""


def storage_ls(gcloud: str, uri: str) -> list[str]:
    output = run_command([gcloud, "storage", "ls", "--recursive", uri])
    return [line.strip() for line in output.splitlines() if line.strip().startswith("gs://")]


def storage_cat(gcloud: str, uri: str) -> str:
    return run_command([gcloud, "storage", "cat", uri])


def _profile_from_uri(uri: str) -> str:
    match = re.search(r"/workers/([^/]+)/", uri)
    if not match:
        return "unknown"
    worker_id = match.group(1)
    if worker_id.startswith("qqqfillsquash_qqq_"):
        profile_chunk = worker_id.removeprefix("qqqfillsquash_qqq_")
        parts = profile_chunk.rsplit("_c", 1)
        return parts[0] if parts else profile_chunk
    return worker_id


def _chunk_from_uri(uri: str) -> str:
    match = re.search(r"/workers/([^/]+)/", uri)
    if not match:
        return "unknown"
    worker_id = match.group(1)
    chunk = re.search(r"_c(\d{3})_(\d{3})$", worker_id)
    if chunk:
        return f"c{chunk.group(1)}-{chunk.group(2)}"
    return "unknown"


def load_summaries(gcloud: str, gcs_prefix: str) -> pd.DataFrame:
    uris = storage_ls(gcloud, f"{gcs_prefix.rstrip('/')}/**/option_aware_candidate_summary.json")
    rows: list[dict[str, Any]] = []
    for uri in uris:
        payload = json.loads(storage_cat(gcloud, uri))
        if isinstance(payload, dict):
            payload = [payload]
        for row in payload:
            if not isinstance(row, dict):
                continue
            enriched = dict(row)
            enriched["source_uri"] = uri
            enriched["artifact_type"] = "final_summary"
            enriched["profile"] = _profile_from_uri(uri)
            enriched["chunk"] = _chunk_from_uri(uri)
            rows.append(enriched)
    progress_uris = storage_ls(
        gcloud, f"{gcs_prefix.rstrip('/')}/**/candidate_summary_progress.jsonl"
    )
    for uri in progress_uris:
        for line in storage_cat(gcloud, uri).splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            enriched = dict(row)
            enriched["source_uri"] = uri
            enriched["artifact_type"] = "progress_jsonl"
            enriched["profile"] = _profile_from_uri(uri)
            enriched["chunk"] = _chunk_from_uri(uri)
            rows.append(enriched)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["_artifact_priority"] = frame["artifact_type"].map(
        {"final_summary": 1, "progress_jsonl": 0}
    ).fillna(0)
    dedupe_keys = [
        "profile",
        "chunk",
        "candidate_variant_id",
        "contract_selection_method",
        "entry_lookup_mode",
        "max_entry_lag_minutes",
        "max_entry_staleness_minutes",
        "exit_lookup_mode",
        "max_exit_lag_minutes",
        "source_session_filter",
    ]
    for column in dedupe_keys:
        if column not in frame.columns:
            frame[column] = ""
    return (
        frame.sort_values("_artifact_priority")
        .drop_duplicates(dedupe_keys, keep="last")
        .drop(columns=["_artifact_priority"])
    )


def _safe_numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([0.0] * len(frame), index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)


def build_outputs(frame: pd.DataFrame, output_dir: Path, fill_gate: float) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if frame.empty:
        (output_dir / "qqq_fill_squash_heatmap.md").write_text(
            "# QQQ Fill-Squash Heatmap\n\nNo candidate summary rows found.\n",
            encoding="utf-8",
        )
        return

    frame = frame.copy()
    frame["strategy_fill_coverage"] = _safe_numeric(frame, "strategy_fill_coverage")
    frame["test_net_pnl"] = _safe_numeric(frame, "test_net_pnl")
    frame["net_pnl"] = _safe_numeric(frame, "net_pnl")
    frame["option_trade_count"] = _safe_numeric(frame, "option_trade_count")
    frame["fill_gate_pass"] = frame["strategy_fill_coverage"] >= fill_gate
    frame["paper_ready_research_gate_pass"] = (
        frame["fill_gate_pass"]
        & (frame["option_trade_count"] >= 20)
        & (frame["test_net_pnl"] > 0)
        & (frame["net_pnl"] > 0)
    )

    group_columns = [
        "profile",
        "contract_selection_method",
        "entry_lookup_mode",
        "max_entry_lag_minutes",
        "max_entry_staleness_minutes",
        "exit_lookup_mode",
        "max_exit_lag_minutes",
        "source_session_filter",
    ]
    for column in group_columns:
        if column not in frame.columns:
            frame[column] = "unknown"

    heatmap = (
        frame.groupby(group_columns, dropna=False)
        .agg(
            candidate_rows=("candidate_variant_id", "count"),
            unique_candidates=("candidate_variant_id", "nunique"),
            chunks=("chunk", "nunique"),
            mean_fill=("strategy_fill_coverage", "mean"),
            min_fill=("strategy_fill_coverage", "min"),
            max_fill=("strategy_fill_coverage", "max"),
            fill_gate_passes=("fill_gate_pass", "sum"),
            research_gate_passes=("paper_ready_research_gate_pass", "sum"),
            mean_test_net_pnl=("test_net_pnl", "mean"),
            max_test_net_pnl=("test_net_pnl", "max"),
            mean_net_pnl=("net_pnl", "mean"),
            max_net_pnl=("net_pnl", "max"),
        )
        .reset_index()
        .sort_values(
            ["research_gate_passes", "fill_gate_passes", "mean_fill", "max_test_net_pnl"],
            ascending=[False, False, False, False],
        )
    )
    heatmap["mean_fill"] = heatmap["mean_fill"].round(4)
    heatmap["mean_test_net_pnl"] = heatmap["mean_test_net_pnl"].round(2)
    heatmap["mean_net_pnl"] = heatmap["mean_net_pnl"].round(2)

    failure_columns = group_columns + ["fill_failure_reason"]
    if "fill_failure_reason" not in frame.columns:
        frame["fill_failure_reason"] = "unknown"
    failures = (
        frame.groupby(failure_columns, dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["profile", "count"], ascending=[True, False])
    )

    top_columns = [
        "profile",
        "chunk",
        "candidate_variant_id",
        "source_strategy_id",
        "symbol",
        "family",
        "parameter_set",
        "contract_selection_method",
        "strategy_fill_coverage",
        "data_foundation_coverage",
        "entry_bar_coverage",
        "exit_bar_coverage",
        "option_trade_count",
        "test_net_pnl",
        "net_pnl",
        "fill_failure_reason",
        "recommendation",
        "source_uri",
    ]
    for column in top_columns:
        if column not in frame.columns:
            frame[column] = ""
    top = frame.sort_values(
        ["paper_ready_research_gate_pass", "strategy_fill_coverage", "test_net_pnl"],
        ascending=[False, False, False],
    )[top_columns].head(100)

    frame.to_csv(output_dir / "qqq_fill_squash_candidate_rows.csv", index=False)
    heatmap.to_csv(output_dir / "qqq_fill_squash_heatmap.csv", index=False)
    failures.to_csv(output_dir / "qqq_fill_squash_failure_reasons.csv", index=False)
    top.to_csv(output_dir / "qqq_fill_squash_top_candidates.csv", index=False)

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    lines = [
        "# QQQ Fill-Squash Heatmap",
        "",
        f"- Generated: `{generated_at}`",
        f"- Candidate rows: `{len(frame)}`",
        f"- Fill coverage gate: `{fill_gate}`",
        f"- Research-gate rows: `{int(frame['paper_ready_research_gate_pass'].sum())}`",
        "",
        "## Top Profiles",
        "",
        heatmap.head(20).to_markdown(index=False),
        "",
        "## Top Candidates",
        "",
        top.head(20).to_markdown(index=False),
        "",
        "## Dominant Failure Reasons",
        "",
        failures.head(40).to_markdown(index=False),
        "",
        "Hard rules: this report is research-only, starts no trading, submits no paper orders, "
        "and does not change live manifests or risk policy.",
        "",
    ]
    (output_dir / "qqq_fill_squash_heatmap.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    frame = load_summaries(args.gcloud, args.gcs_prefix)
    build_outputs(frame, Path(args.output_dir), args.fill_coverage_gate)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.apply_quote_sidecar_to_trade_economics import (
    _build_quote_index,
    _build_trade_index,
    _contract_symbols,
    _count_prints,
    _quote_backed_replay_for_row,
    _quote_quality_for_side,
    _timestamp,
)
from scripts.repair_projection_replay_lineage import build_lineage_repair


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a research-only quote-backed evidence repair sweep. The sweep "
            "audits current paper and review-candidate replay lineage, applies "
            "OPRA/SIP realtime quote sidecars with causal as-of joins, rejects "
            "incomplete quote lineage, and emits a survivor report for downstream "
            "projection/optimization only when fully quote-backed candidates exist."
        )
    )
    parser.add_argument("--paper-report-json", required=True)
    parser.add_argument("--paper-replay-root", action="append", default=[])
    parser.add_argument("--review-report-json", action="append", default=[])
    parser.add_argument("--promotion-packet-glob", action="append", default=[])
    parser.add_argument("--review-replay-root", action="append", default=[])
    parser.add_argument("--search-root", action="append", default=[])
    parser.add_argument("--quote-sidecar-csv", required=True)
    parser.add_argument("--option-trades-csv", default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--initial-cash", type=float, default=25000.0)
    parser.add_argument("--entry-selection-window-seconds", type=float, default=60.0)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _candidate_key(row: dict[str, Any]) -> tuple[str, str]:
    candidate_id = str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or "")
    candidate_id = candidate_id.split("__profile_", 1)[0]
    return candidate_id, str(row.get("aggregate_profile") or "")


def _normalise_plan_rows(rows: list[dict[str, Any]], *, initial_cash: float) -> list[dict[str, Any]]:
    if not rows:
        return []
    explicit_weight_sum = sum(_float(row.get("research_only_weight")) for row in rows)
    if explicit_weight_sum > 0:
        return rows
    weight = round(1.0 / len(rows), 8)
    dollars = round(initial_cash * weight, 2)
    output = []
    for row in rows:
        normalised = dict(row)
        normalised["research_only_weight"] = weight
        normalised["research_only_dollars"] = dollars
        output.append(normalised)
    return output


def _load_plan_rows(path: Path, source_scope: str) -> list[dict[str, Any]]:
    payload = _load_json(path)
    rows = payload.get("capital_plan") or []
    output = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not _candidate_key(row)[0] or not _candidate_key(row)[1]:
            continue
        enriched = dict(row)
        enriched.setdefault("candidate_variant_id", enriched.get("base_candidate_variant_id"))
        enriched["evidence_source_scope"] = source_scope
        enriched["evidence_source_json"] = str(path)
        output.append(enriched)
    return output


def _load_review_candidates_from_packet(path: Path) -> list[dict[str, Any]]:
    payload = _load_json(path)
    rows = payload.get("review_candidates") or []
    output = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not _candidate_key(row)[0] or not _candidate_key(row)[1]:
            continue
        enriched = dict(row)
        enriched.setdefault("candidate_variant_id", enriched.get("base_candidate_variant_id"))
        enriched["evidence_source_scope"] = "promotion_review_candidate"
        enriched["evidence_source_json"] = str(path)
        enriched["evidence_source_decision"] = payload.get("decision")
        enriched["evidence_source_packet_status"] = payload.get("status")
        output.append(enriched)
    return output


def _build_review_report(
    *,
    review_report_jsons: list[Path],
    promotion_packet_globs: list[str],
    output_path: Path,
    initial_cash: float,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for path in review_report_jsons:
        if path.exists():
            rows.extend(_load_plan_rows(path, "review_report_capital_plan"))
    for pattern in promotion_packet_globs:
        for path in sorted(Path().glob(pattern)):
            if path.is_file():
                rows.extend(_load_review_candidates_from_packet(path))

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    for row in rows:
        key = _candidate_key(row)
        source_scope = str(row.get("evidence_source_scope") or "unknown")
        source_counts[source_scope] = source_counts.get(source_scope, 0) + 1
        if key not in deduped:
            deduped[key] = row
            continue
        existing = deduped[key]
        scopes = {
            str(existing.get("evidence_source_scope") or ""),
            source_scope,
        }
        existing["evidence_source_scope"] = ",".join(sorted(scope for scope in scopes if scope))

    capital_plan = _normalise_plan_rows(list(deduped.values()), initial_cash=initial_cash)
    report = {
        "status": "combined_review_candidate_report",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "candidate_count_before_dedupe": len(rows),
        "capital_plan_count": len(capital_plan),
        "source_counts": source_counts,
        "capital_plan": capital_plan,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def _apply_quote_sidecar_tree(
    *,
    source_root: Path,
    output_root: Path,
    quote_sidecar_csv: Path,
    option_trades_csv: Path | None,
    entry_selection_window_seconds: float,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    quote_sidecar = pd.read_csv(quote_sidecar_csv, low_memory=False)
    quote_index = _build_quote_index(quote_sidecar)
    trade_index = (
        _build_trade_index(pd.read_csv(option_trades_csv, low_memory=False))
        if option_trades_csv and option_trades_csv.exists()
        else {}
    )
    input_csvs = sorted(source_root.rglob("option_aware_trade_economics.csv"))
    summaries = []
    total_rows = 0
    aggregate_status_counts: dict[str, int] = {}
    aggregate_replay_status_counts: dict[str, int] = {}
    for csv_path in input_csvs:
        try:
            trades = pd.read_csv(csv_path, low_memory=False)
        except (pd.errors.EmptyDataError, UnicodeDecodeError):
            continue
        enriched_rows: list[dict[str, Any]] = []
        status_counts: dict[str, int] = {}
        replay_status_counts: dict[str, int] = {}
        for _, row in trades.iterrows():
            entry_time = _timestamp(row.get("stock_entry_time") or row.get("option_entry_time"))
            exit_time = _timestamp(row.get("stock_exit_time") or row.get("option_exit_time"))
            output = row.to_dict()
            for prefix, decision_time in (("entry", entry_time), ("exit", exit_time)):
                quality = _quote_quality_for_side(
                    row=row,
                    quote_index=quote_index,
                    prefix=prefix,
                    decision_time=decision_time,
                )
                output.update(quality)
                status = str(quality.get(f"{prefix}_quote_source") or "unknown")
                status_counts[f"{prefix}:{status}"] = status_counts.get(f"{prefix}:{status}", 0) + 1
            replay = _quote_backed_replay_for_row(
                row=row,
                quote_index=quote_index,
                entry_time=entry_time,
                exit_time=exit_time,
            )
            output.update(replay)
            replay_status = str(replay.get("quote_backed_replay_status") or "unknown")
            replay_status_counts[replay_status] = replay_status_counts.get(replay_status, 0) + 1
            if trade_index:
                symbols = _contract_symbols(row)
                entry_window_end = (
                    entry_time + pd.Timedelta(seconds=entry_selection_window_seconds)
                    if entry_time is not None
                    else None
                )
                entry_prints = _count_prints(trade_index, symbols, entry_time, entry_window_end)
                full_prints = _count_prints(trade_index, symbols, entry_time, exit_time)
                if entry_prints is not None:
                    output["entry_selection_trade_print_count"] = entry_prints
                if full_prints is not None:
                    output["option_trade_print_count"] = full_prints
            enriched_rows.append(output)
        relative = csv_path.relative_to(source_root)
        output_csv = output_root / relative
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(enriched_rows).to_csv(output_csv, index=False)
        total_rows += len(enriched_rows)
        for key, value in status_counts.items():
            aggregate_status_counts[key] = aggregate_status_counts.get(key, 0) + int(value)
        for key, value in replay_status_counts.items():
            aggregate_replay_status_counts[key] = aggregate_replay_status_counts.get(key, 0) + int(value)
        summaries.append(
            {
                "input_csv": str(csv_path),
                "output_csv": str(output_csv),
                "trade_rows": int(len(enriched_rows)),
                "quote_source_status_counts": status_counts,
                "quote_backed_replay_status_counts": replay_status_counts,
            }
        )
    summary = {
        "status": "quote_sidecar_tree_apply_complete",
        "source_root": str(source_root),
        "output_root": str(output_root),
        "quote_sidecar_csv": str(quote_sidecar_csv),
        "option_trades_csv": str(option_trades_csv) if option_trades_csv else None,
        "input_csv_count": len(input_csvs),
        "output_csv_count": len(summaries),
        "trade_rows": total_rows,
        "quote_symbol_count": int(len(quote_index)),
        "quote_source_status_counts": aggregate_status_counts,
        "quote_backed_replay_status_counts": aggregate_replay_status_counts,
        "files": summaries,
    }
    (output_root / "quote_sidecar_apply_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def _quote_backed_keys(lineage_csv: Path) -> set[tuple[str, str]]:
    if not lineage_csv.exists():
        return set()
    frame = pd.read_csv(lineage_csv, low_memory=False)
    if frame.empty or "quote_quality_status" not in frame.columns:
        return set()
    backed = frame[frame["quote_quality_status"].astype(str).eq("quote_backed_replay")]
    return {
        (
            str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or ""),
            str(row.get("aggregate_profile") or ""),
        )
        for _, row in backed.iterrows()
    }


def _all_lineage_rows(paths: list[Path]) -> pd.DataFrame:
    frames = []
    for path in paths:
        if path.exists():
            frames.append(pd.read_csv(path, low_memory=False))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _build_survivor_report(
    *,
    source_reports: list[Path],
    backed_keys: set[tuple[str, str]],
    output_path: Path,
    initial_cash: float,
) -> dict[str, Any]:
    source_rows: list[dict[str, Any]] = []
    for report_path in source_reports:
        if report_path.exists():
            source_rows.extend(_load_plan_rows(report_path, "survivor_source_report"))
    selected = []
    rejected = []
    seen = set()
    for row in source_rows:
        key = _candidate_key(row)
        if key in backed_keys:
            if key not in seen:
                selected.append(row)
                seen.add(key)
        else:
            rejected.append(
                {
                    "base_candidate_variant_id": key[0],
                    "aggregate_profile": key[1],
                    "symbol": row.get("symbol"),
                    "family": row.get("family"),
                    "intended_regime": row.get("intended_regime"),
                    "rejection_reason": "incomplete_quote_backed_replay_lineage",
                }
            )
    selected = _normalise_plan_rows(selected, initial_cash=initial_cash)
    report = {
        "status": "quote_backed_survivor_report",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "source_capital_plan_count": len(source_rows),
        "quote_backed_survivor_count": len(selected),
        "rejected_count": len(rejected),
        "rejection_policy": "reject anything without quote_backed_replay status",
        "capital_plan": selected,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    pd.DataFrame(rejected).to_csv(output_path.with_name("quote_backed_rejections.csv"), index=False)
    return report


def _copy_if_exists(source: Path, target: Path) -> None:
    if source.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    quote_sidecar_csv = Path(args.quote_sidecar_csv)
    option_trades_csv = Path(args.option_trades_csv) if args.option_trades_csv else None

    paper_report = Path(args.paper_report_json)
    review_report = output_dir / "review_candidates_combined_report.json"
    _build_review_report(
        review_report_jsons=[Path(path) for path in args.review_report_json],
        promotion_packet_globs=args.promotion_packet_glob,
        output_path=review_report,
        initial_cash=args.initial_cash,
    )

    search_roots = [Path(path) for path in args.search_root]
    paper_repaired_root = output_dir / "paper" / "repaired_replay"
    review_repaired_root = output_dir / "review" / "repaired_replay"
    paper_lineage = build_lineage_repair(
        portfolio_report_json=paper_report,
        replay_roots=[Path(path) for path in args.paper_replay_root],
        search_roots=search_roots,
        output_dir=output_dir / "paper" / "lineage_before_sidecar",
        repaired_replay_root=paper_repaired_root,
        copy_existing_replay_roots=True,
    )
    review_lineage = build_lineage_repair(
        portfolio_report_json=review_report,
        replay_roots=[Path(path) for path in args.review_replay_root],
        search_roots=search_roots,
        output_dir=output_dir / "review" / "lineage_before_sidecar",
        repaired_replay_root=review_repaired_root,
        copy_existing_replay_roots=True,
    )

    paper_quote_root = output_dir / "paper" / "quote_sidecar_replay"
    review_quote_root = output_dir / "review" / "quote_sidecar_replay"
    paper_apply = _apply_quote_sidecar_tree(
        source_root=paper_repaired_root,
        output_root=paper_quote_root,
        quote_sidecar_csv=quote_sidecar_csv,
        option_trades_csv=option_trades_csv,
        entry_selection_window_seconds=args.entry_selection_window_seconds,
    )
    review_apply = _apply_quote_sidecar_tree(
        source_root=review_repaired_root,
        output_root=review_quote_root,
        quote_sidecar_csv=quote_sidecar_csv,
        option_trades_csv=option_trades_csv,
        entry_selection_window_seconds=args.entry_selection_window_seconds,
    )

    paper_after = build_lineage_repair(
        portfolio_report_json=paper_report,
        replay_roots=[paper_quote_root],
        search_roots=[],
        output_dir=output_dir / "paper" / "lineage_after_sidecar",
    )
    review_after = build_lineage_repair(
        portfolio_report_json=review_report,
        replay_roots=[review_quote_root],
        search_roots=[],
        output_dir=output_dir / "review" / "lineage_after_sidecar",
    )

    backed_keys = set()
    backed_keys.update(_quote_backed_keys(output_dir / "paper" / "lineage_after_sidecar" / "quote_quality_lineage.csv"))
    backed_keys.update(_quote_backed_keys(output_dir / "review" / "lineage_after_sidecar" / "quote_quality_lineage.csv"))
    survivor_report = _build_survivor_report(
        source_reports=[paper_report, review_report],
        backed_keys=backed_keys,
        output_path=output_dir / "quote_backed_survivor_report.json",
        initial_cash=args.initial_cash,
    )

    lineage_rows = _all_lineage_rows(
        [
            output_dir / "paper" / "lineage_after_sidecar" / "quote_quality_lineage.csv",
            output_dir / "review" / "lineage_after_sidecar" / "quote_quality_lineage.csv",
        ]
    )
    if not lineage_rows.empty:
        lineage_rows.to_csv(output_dir / "combined_quote_quality_lineage_after_sidecar.csv", index=False)

    optimizer_status = (
        "ready_for_quote_backed_projection"
        if survivor_report["quote_backed_survivor_count"] > 0
        else "skipped_no_quote_backed_survivors"
    )
    summary = {
        "status": "quote_backed_evidence_repair_sweep_complete",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "paper_lineage_before_sidecar": paper_lineage,
        "review_lineage_before_sidecar": review_lineage,
        "paper_quote_sidecar_apply": paper_apply,
        "review_quote_sidecar_apply": review_apply,
        "paper_lineage_after_sidecar": paper_after,
        "review_lineage_after_sidecar": review_after,
        "quote_backed_survivor_count": survivor_report["quote_backed_survivor_count"],
        "optimizer_status": optimizer_status,
        "next_projection_input": str(output_dir / "quote_backed_survivor_report.json")
        if survivor_report["quote_backed_survivor_count"] > 0
        else None,
        "rejection_policy": survivor_report["rejection_policy"],
    }
    _copy_if_exists(quote_sidecar_csv, output_dir / "inputs" / quote_sidecar_csv.name)
    if option_trades_csv:
        _copy_if_exists(option_trades_csv, output_dir / "inputs" / option_trades_csv.name)
    (output_dir / "evidence_repair_sweep_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

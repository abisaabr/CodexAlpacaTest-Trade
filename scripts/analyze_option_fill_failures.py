from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize option-aware fill failures for a candidate or shard. "
            "This is research-only diagnostic tooling; it does not change promotion gates."
        )
    )
    parser.add_argument("--candidate-summary-json", required=True)
    parser.add_argument("--fill-failures-json", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-variant-id", default=None)
    parser.add_argument("--report-id", default=None)
    return parser.parse_args()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        return [payload]
    return []


def _counter(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts = Counter(str(row.get(key) or "missing") for row in rows)
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _top_dates(rows: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    by_date: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        trade_date = str(row.get("trade_date") or "missing")
        reason = str(row.get("failure_reason") or "missing")
        by_date[trade_date][reason] += 1
    ranked = sorted(
        by_date.items(),
        key=lambda item: (-sum(item[1].values()), item[0]),
    )
    return [
        {
            "trade_date": trade_date,
            "failure_count": sum(counter.values()),
            "failure_reasons": dict(sorted(counter.items())),
        }
        for trade_date, counter in ranked[:limit]
    ]


def _dominant_classification(failure_rows: list[dict[str, Any]]) -> list[str]:
    reasons = Counter(str(row.get("failure_reason") or "") for row in failure_rows)
    classifications: list[str] = []
    if reasons.get("no_selected_contract", 0):
        classifications.append("selected_contract_universe_gap")
    if reasons.get("no_entry_bar", 0):
        classifications.append("entry_bar_gap_or_entry_timing_mismatch")
    if reasons.get("no_exit_bar", 0):
        classifications.append("exit_bar_gap_or_exit_policy_mismatch")
    if reasons.get("missing_option_price", 0):
        classifications.append("missing_option_price_count")
    if not classifications and failure_rows:
        classifications.append("unclassified_fill_failure")
    return classifications


def _candidate_lookup(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("candidate_variant_id")): row
        for row in candidates
        if row.get("candidate_variant_id")
    }


def _write_markdown(path: Path, report: dict[str, Any]) -> None:
    candidate = report["candidate"]
    lines = [
        "# Option Fill Failure Diagnostic",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Report ID: `{report['report_id']}`",
        f"- Candidate: `{report['candidate_variant_id'] or 'all_candidates'}`",
        f"- Strategy: `{candidate.get('strategy_id') or candidate.get('source_strategy_id') or 'mixed'}`",
        f"- Symbol: `{candidate.get('symbol') or 'mixed'}`",
        f"- Family: `{candidate.get('family') or 'mixed'}`",
        f"- Intended regime: `{candidate.get('intended_regime') or 'mixed'}`",
        "",
        "## Candidate Metrics",
        "",
        f"- Net PnL: `{candidate.get('net_pnl')}`",
        f"- Test net PnL: `{candidate.get('test_net_pnl')}`",
        f"- Strategy fill coverage: `{candidate.get('strategy_fill_coverage', candidate.get('fill_coverage'))}`",
        f"- Data foundation coverage: `{candidate.get('data_foundation_coverage')}`",
        f"- Entry bar coverage: `{candidate.get('entry_bar_coverage')}`",
        f"- Exit bar coverage: `{candidate.get('exit_bar_coverage')}`",
        f"- Option trade count: `{candidate.get('option_trade_count')}`",
        "",
        "## Failure Classification",
        "",
    ]
    for item in report["dominant_classification"]:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Failure Reason Counts", ""])
    for reason, count in report["failure_reason_counts"].items():
        lines.append(f"- `{reason}`: `{count}`")
    lines.extend(["", "## Option Structure Counts", ""])
    for structure, count in report["option_structure_counts"].items():
        lines.append(f"- `{structure}`: `{count}`")
    lines.extend(["", "## Top Failure Dates", ""])
    for row in report["top_failure_dates"]:
        reasons = ", ".join(f"{key}={value}" for key, value in row["failure_reasons"].items())
        lines.append(f"- `{row['trade_date']}`: `{row['failure_count']}` failures ({reasons})")
    lines.extend(["", "## Recommended Next Research Step", ""])
    lines.append(
        "- Run a bounded research-only micro-wave that separates data repair from realistic entry-semantics diagnostics: "
        "first repair/reselect missing vertical legs for `no_selected_contract`, then compare strict `0` minute entry lag "
        "against small, explicitly modeled entry-lag profiles without changing the `fill_coverage >= 0.90` promotion gate."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_report(
    *,
    candidate_summary_json: Path,
    fill_failures_json: Path,
    output_dir: Path,
    candidate_variant_id: str | None,
    report_id: str | None,
) -> dict[str, Any]:
    candidates = _rows(_load_json(candidate_summary_json), "candidates", "candidate_summaries")
    failures = _rows(_load_json(fill_failures_json), "fill_failures", "failures")
    if candidate_variant_id:
        failures = [
            row for row in failures if str(row.get("candidate_variant_id")) == candidate_variant_id
        ]
    lookup = _candidate_lookup(candidates)
    candidate = lookup.get(candidate_variant_id or "", {})
    if not candidate and candidate_variant_id is None and candidates:
        candidate = {
            "strategy_id": "mixed",
            "source_strategy_id": "mixed",
            "symbol": "mixed",
            "family": "mixed",
            "intended_regime": "mixed",
        }

    resolved_report_id = report_id or f"fill_failure_diagnostic_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "report_id": resolved_report_id,
        "candidate_variant_id": candidate_variant_id,
        "candidate": candidate,
        "failure_count": len(failures),
        "failure_reason_counts": _counter(failures, "failure_reason"),
        "option_structure_counts": _counter(failures, "option_structure"),
        "contract_symbol_presence_counts": {
            "has_contract_symbol": sum(1 for row in failures if row.get("contract_symbol")),
            "missing_contract_symbol": sum(1 for row in failures if not row.get("contract_symbol")),
        },
        "entry_lookup_mode_counts": _counter(failures, "entry_lookup_mode"),
        "exit_lookup_mode_counts": _counter(failures, "exit_lookup_mode"),
        "contract_selection_method_counts": _counter(failures, "contract_selection_method"),
        "dominant_classification": _dominant_classification(failures),
        "top_failure_dates": _top_dates(failures),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{resolved_report_id}.json"
    md_path = output_dir / f"{resolved_report_id}.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(md_path, report)
    return report


def main() -> None:
    args = parse_args()
    report = build_report(
        candidate_summary_json=Path(args.candidate_summary_json),
        fill_failures_json=Path(args.fill_failures_json),
        output_dir=Path(args.output_dir),
        candidate_variant_id=args.candidate_variant_id,
        report_id=args.report_id,
    )
    print(json.dumps({"report_id": report["report_id"], "failure_count": report["failure_count"]}, indent=2))


if __name__ == "__main__":
    main()

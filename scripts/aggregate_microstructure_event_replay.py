from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate microstructure event replay shard summaries."
    )
    parser.add_argument("--workers-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="")
    parser.add_argument("--min-fill-coverage", type=float, default=0.90)
    parser.add_argument("--min-trades", type=int, default=20)
    parser.add_argument("--min-net-pnl", type=float, default=0.0)
    parser.add_argument("--max-avg-spread-cost-to-target", type=float, default=0.65)
    parser.add_argument("--max-review-candidates", type=int, default=50)
    return parser.parse_args()


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _to_float(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _to_int(row: dict[str, Any], key: str) -> int:
    try:
        return int(float(row.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def _eligible(
    row: dict[str, Any],
    *,
    min_fill_coverage: float,
    min_trades: int,
    min_net_pnl: float,
    max_avg_spread_cost_to_target: float,
) -> bool:
    return (
        _to_float(row, "fill_coverage") >= min_fill_coverage
        and _to_int(row, "filled_trade_count") >= min_trades
        and _to_float(row, "net_pnl_total") > min_net_pnl
        and _to_float(row, "avg_net_pnl") > 0.0
        and (
            "avg_spread_cost_to_target" not in row
            or _to_float(row, "avg_spread_cost_to_target") <= max_avg_spread_cost_to_target
        )
    )


def _blockers(
    row: dict[str, Any],
    *,
    min_fill_coverage: float,
    min_trades: int,
    min_net_pnl: float,
    max_avg_spread_cost_to_target: float,
) -> list[str]:
    result: list[str] = []
    if _to_float(row, "fill_coverage") < min_fill_coverage:
        result.append("fill_coverage_below_gate")
    if _to_int(row, "filled_trade_count") < min_trades:
        result.append("trade_count_below_gate")
    if _to_float(row, "net_pnl_total") <= min_net_pnl:
        result.append("net_pnl_not_positive")
    if _to_float(row, "avg_net_pnl") <= 0.0:
        result.append("avg_net_pnl_not_positive")
    if (
        "avg_spread_cost_to_target" in row
        and _to_float(row, "avg_spread_cost_to_target") > max_avg_spread_cost_to_target
    ):
        result.append("avg_spread_cost_to_target_above_gate")
    return result


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    workers_root = Path(args.workers_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    source_files = sorted(workers_root.rglob("microstructure_event_replay_summary.csv"))
    for path in source_files:
        for row in _read_csv(path):
            row["source_summary_path"] = str(path)
            row["eligible_for_microstructure_review"] = _eligible(
                row,
                min_fill_coverage=args.min_fill_coverage,
                min_trades=args.min_trades,
                min_net_pnl=args.min_net_pnl,
                max_avg_spread_cost_to_target=args.max_avg_spread_cost_to_target,
            )
            row["microstructure_blockers"] = ";".join(
                _blockers(
                    row,
                    min_fill_coverage=args.min_fill_coverage,
                    min_trades=args.min_trades,
                    min_net_pnl=args.min_net_pnl,
                    max_avg_spread_cost_to_target=args.max_avg_spread_cost_to_target,
                )
            )
            rows.append(row)
    rows.sort(
        key=lambda row: (
            not bool(row["eligible_for_microstructure_review"]),
            -_to_float(row, "net_pnl_total"),
            -_to_float(row, "avg_net_pnl"),
        )
    )
    blocker_counts: Counter[str] = Counter()
    for row in rows:
        for blocker in str(row.get("microstructure_blockers") or "").split(";"):
            if blocker:
                blocker_counts[blocker] += 1
    review_candidates = [
        row for row in rows if bool(row["eligible_for_microstructure_review"])
    ][: args.max_review_candidates]
    packet = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "wave_id": args.wave_id,
        "workers_root": str(workers_root),
        "source_summary_file_count": len(source_files),
        "grid_result_count": len(rows),
        "gates": {
            "min_fill_coverage": args.min_fill_coverage,
            "min_trades": args.min_trades,
            "min_net_pnl": args.min_net_pnl,
            "max_avg_spread_cost_to_target": args.max_avg_spread_cost_to_target,
        },
        "eligible_for_microstructure_review_count": len(
            [row for row in rows if bool(row["eligible_for_microstructure_review"])]
        ),
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "review_candidates": review_candidates,
        "decision": "microstructure_review_ready"
        if review_candidates
        else "research_only_blocked",
        "promotion_status": "not_paper_eligible_without_longer_tick_quote_history_and_governed_packet",
        "research_only": True,
        "broker_facing": False,
        "paper_orders": False,
    }
    (output_dir / "microstructure_event_replay_aggregate_packet.json").write_text(
        json.dumps(packet, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_csv(output_dir / "microstructure_event_replay_aggregate_summary.csv", rows)
    _write_csv(output_dir / "microstructure_event_replay_review_candidates.csv", review_candidates)
    md_lines = [
        "# Microstructure Event Replay Aggregate",
        "",
        f"Generated: `{packet['generated_at_utc']}`",
        "",
        f"- Wave ID: `{args.wave_id}`",
        f"- Source shard summaries: `{len(source_files)}`",
        f"- Grid results: `{len(rows)}`",
        f"- Eligible for microstructure review: `{packet['eligible_for_microstructure_review_count']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Max average spread-cost-to-target gate: `{args.max_avg_spread_cost_to_target}`",
        "",
        "## Blockers",
        "",
    ]
    for blocker, count in sorted(blocker_counts.items()):
        md_lines.append(f"- `{blocker}`: `{count}`")
    md_lines.extend(["", "## Top Candidates", ""])
    for row in review_candidates[:20]:
        md_lines.append(
            f"- `{row.get('grid_id')}` mode={row.get('signal_mode')} trades={row.get('filled_trade_count')} "
            f"fill={row.get('fill_coverage')} net={row.get('net_pnl_total')} avg={row.get('avg_net_pnl')}"
        )
    if not review_candidates:
        md_lines.append("- None.")
    md_lines.extend(
        [
            "",
            "## Promotion Posture",
            "",
            "This packet can identify microstructure review leads, but it does not authorize PAPER activation. "
            "The current input is a short websocket shadow capture, not a long historical tick/quote data set.",
            "",
        ]
    )
    (output_dir / "microstructure_event_replay_aggregate_packet.md").write_text(
        "\n".join(md_lines),
        encoding="utf-8",
    )
    print(f"packet_json={output_dir / 'microstructure_event_replay_aggregate_packet.json'}")
    print(f"summary_csv={output_dir / 'microstructure_event_replay_aggregate_summary.csv'}")
    print(f"review_csv={output_dir / 'microstructure_event_replay_review_candidates.csv'}")


if __name__ == "__main__":
    main()

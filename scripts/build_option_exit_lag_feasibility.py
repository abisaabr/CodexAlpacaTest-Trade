from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_option_aware_research_backtest import (  # noqa: E402
    _build_option_research_index,
    _choose_entry_liquidity_first_contract,
    _first_option_bar,
    _load_json,
    _load_option_inputs,
    _load_stock_bars,
    _stock_trades_for_variant,
    _trade_print_count,
    _variant_map,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a non-broker-facing exit-lag feasibility packet for option-aware "
            "research candidates blocked by missing exit bars."
        )
    )
    parser.add_argument("--queue-json", required=True)
    parser.add_argument("--variants-jsonl", required=True)
    parser.add_argument("--stock-bars-path", required=True)
    parser.add_argument("--selected-contracts-root", required=True)
    parser.add_argument("--option-bars-root", required=True)
    parser.add_argument("--option-trades-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-ids", default=None)
    parser.add_argument("--symbol-filter", default=None)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--entry-lag-minutes", type=int, default=60)
    parser.add_argument("--exit-lags-minutes", default="10,30,60,90,120,180")
    parser.add_argument("--max-probe-lag-minutes", type=int, default=240)
    parser.add_argument("--fill-coverage-gate", type=float, default=0.90)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--allocation-fraction", type=float, default=0.10)
    return parser.parse_args()


def _parse_csv_set(value: str | None) -> set[str] | None:
    if not value:
        return None
    parsed = {item.strip() for item in value.split(",") if item.strip()}
    return parsed or None


def _parse_int_csv(value: str) -> list[int]:
    parsed = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not parsed:
        raise ValueError("at least one exit lag is required")
    return parsed


def _json_default(value: Any) -> str:
    return str(value)


def _first_bar_lag_minutes(
    *,
    option_index: Any,
    contract_symbol: str,
    timestamp: pd.Timestamp,
    max_lag: timedelta,
) -> float | None:
    bar = _first_option_bar(
        option_bars=pd.DataFrame(),
        option_index=option_index,
        contract_symbol=contract_symbol,
        timestamp=timestamp,
        max_lag=max_lag,
    )
    if not bar:
        return None
    return round((pd.Timestamp(bar["timestamp"]) - timestamp).total_seconds() / 60.0, 4)


def classify_missing_exit(
    *,
    trade_print_count: int,
    first_available_lag_minutes: float | None,
    lag_minutes: int,
    max_probe_lag_minutes: int,
) -> str:
    if trade_print_count > 0:
        return "bar_gap_trade_print_available"
    if first_available_lag_minutes is None:
        return "illiquid_or_missing_exit_data"
    if first_available_lag_minutes <= max_probe_lag_minutes:
        if first_available_lag_minutes <= max(lag_minutes * 3, lag_minutes + 30):
            return "short_lag_execution_timing_mismatch"
        return "late_exit_liquidity_window"
    return "illiquid_or_missing_exit_data"


def _candidate_queue_items(
    queue: dict[str, Any],
    *,
    candidate_ids: set[str] | None,
    symbol_filter: set[str] | None,
    top_n: int,
) -> list[dict[str, Any]]:
    rows = [item for item in queue.get("queue_items", []) if isinstance(item, dict)]
    filtered: list[dict[str, Any]] = []
    for item in rows:
        candidate_id = str(item.get("candidate_variant_id") or "")
        symbol = str(item.get("symbol") or "").upper()
        if candidate_ids and candidate_id not in candidate_ids:
            continue
        if symbol_filter and symbol not in {value.upper() for value in symbol_filter}:
            continue
        filtered.append(item)
    return filtered[:top_n]


def _profile_key(candidate_id: str, lag: int) -> str:
    return f"{candidate_id}__lag{lag}"


def _summarize_lag_rows(
    trade_rows: list[dict[str, Any]],
    *,
    candidate: dict[str, Any],
    exit_lags: list[int],
    fill_coverage_gate: float,
) -> list[dict[str, Any]]:
    summaries = []
    candidate_id = str(candidate.get("candidate_variant_id") or "")
    source_rows = [row for row in trade_rows if row["candidate_variant_id"] == candidate_id]
    source_trade_count = len({row["source_trade_index"] for row in source_rows})
    for lag in exit_lags:
        lag_rows = [row for row in source_rows if row["exit_lag_minutes"] == lag]
        filled = [row for row in lag_rows if row["exit_status"] == "filled"]
        missing = [row for row in lag_rows if row["exit_status"] != "filled"]
        classification_counts: dict[str, int] = {}
        for row in missing:
            key = str(row["exit_missing_classification"])
            classification_counts[key] = classification_counts.get(key, 0) + 1
        first_available_lags = [
            float(row["first_available_exit_lag_minutes"])
            for row in lag_rows
            if row.get("first_available_exit_lag_minutes") is not None
        ]
        fill_coverage = round(len(filled) / source_trade_count, 4) if source_trade_count else 0.0
        summaries.append(
            {
                "profile_key": _profile_key(candidate_id, lag),
                "candidate_variant_id": candidate_id,
                "symbol": candidate.get("symbol"),
                "source_strategy_id": candidate.get("source_strategy_id"),
                "directional_option_type": candidate.get("directional_option_type"),
                "exit_lag_minutes": lag,
                "source_trade_count": source_trade_count,
                "filled_exit_count": len(filled),
                "missing_exit_count": len(missing),
                "fill_coverage": fill_coverage,
                "fill_gate_pass": fill_coverage >= fill_coverage_gate,
                "missing_with_later_bar_count": sum(
                    1 for row in missing if row.get("first_available_exit_lag_minutes") is not None
                ),
                "missing_with_trade_print_count": sum(
                    1 for row in missing if int(row.get("exit_trade_print_count") or 0) > 0
                ),
                "classification_counts": classification_counts,
                "median_first_available_exit_lag_minutes": (
                    round(float(median(first_available_lags)), 4)
                    if first_available_lags
                    else None
                ),
            }
        )
    return summaries


def _candidate_summaries(
    profile_rows: list[dict[str, Any]],
    *,
    exit_lags: list[int],
    fill_coverage_gate: float,
) -> list[dict[str, Any]]:
    by_candidate: dict[str, list[dict[str, Any]]] = {}
    for row in profile_rows:
        by_candidate.setdefault(str(row["candidate_variant_id"]), []).append(row)
    summaries = []
    for candidate_id, rows in sorted(by_candidate.items()):
        rows_by_lag = {int(row["exit_lag_minutes"]): row for row in rows}
        fill_curve = {
            str(lag): rows_by_lag.get(lag, {}).get("fill_coverage", 0.0) for lag in exit_lags
        }
        passing_lags = [
            lag
            for lag in exit_lags
            if float(rows_by_lag.get(lag, {}).get("fill_coverage", 0.0)) >= fill_coverage_gate
        ]
        short_lag_pass = any(lag <= 30 for lag in passing_lags)
        wide_lag_pass = any(lag >= 60 for lag in passing_lags)
        min_fill = min(float(value) for value in fill_curve.values()) if fill_curve else 0.0
        max_fill = max(float(value) for value in fill_curve.values()) if fill_curve else 0.0
        first_row = rows[0]
        if short_lag_pass:
            recommendation = "fill_feasible_under_short_lag_review"
        elif wide_lag_pass:
            recommendation = "exit_policy_research_candidate_not_promotion"
        else:
            recommendation = "do_not_promote_exit_fill_not_feasible"
        summaries.append(
            {
                "candidate_variant_id": candidate_id,
                "symbol": first_row.get("symbol"),
                "source_strategy_id": first_row.get("source_strategy_id"),
                "directional_option_type": first_row.get("directional_option_type"),
                "fill_coverage_gate": fill_coverage_gate,
                "min_fill_coverage": round(min_fill, 4),
                "max_fill_coverage": round(max_fill, 4),
                "shortest_passing_exit_lag_minutes": min(passing_lags) if passing_lags else None,
                "full_stack_fill_gate_pass": min_fill >= fill_coverage_gate,
                "wide_lag_fill_gate_pass": wide_lag_pass,
                "short_lag_fill_gate_pass": short_lag_pass,
                "fill_curve": fill_curve,
                "recommendation": recommendation,
            }
        )
    return sorted(
        summaries,
        key=lambda row: (
            row["full_stack_fill_gate_pass"],
            row["wide_lag_fill_gate_pass"],
            row["max_fill_coverage"],
            row["min_fill_coverage"],
        ),
        reverse=True,
    )


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            {
                key: json.dumps(value, sort_keys=True) if isinstance(value, dict) else value
                for key, value in row.items()
            }
            for row in rows
        )


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Option Exit-Lag Feasibility Packet",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Status: `{packet['status']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Candidate count: `{packet['candidate_count']}`",
        f"- Exit lags: `{packet['exit_lags_minutes']}`",
        f"- Fill coverage gate: `{packet['fill_coverage_gate']}`",
        "",
        "## Candidate Summary",
        "",
    ]
    for row in packet["candidate_summaries"]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"min_fill `{row['min_fill_coverage']}` max_fill `{row['max_fill_coverage']}` "
            f"shortest_pass `{row['shortest_passing_exit_lag_minutes']}` "
            f"recommendation `{row['recommendation']}`"
        )
    lines.extend(["", "## Next Actions", ""])
    for item in packet["next_actions"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_option_exit_lag_feasibility(
    *,
    queue_json: Path,
    variants_jsonl: Path,
    stock_bars_path: Path,
    selected_contracts_root: Path,
    option_bars_root: Path,
    option_trades_root: Path,
    output_dir: Path,
    candidate_ids: set[str] | None = None,
    symbol_filter: set[str] | None = None,
    top_n: int = 50,
    entry_lag_minutes: int = 60,
    exit_lags_minutes: list[int] | None = None,
    max_probe_lag_minutes: int = 240,
    fill_coverage_gate: float = 0.90,
    initial_cash: float = 25_000.0,
    allocation_fraction: float = 0.10,
) -> dict[str, Any]:
    exit_lags = exit_lags_minutes or [10, 30, 60, 90, 120, 180]
    output_dir.mkdir(parents=True, exist_ok=True)
    queue = _load_json(queue_json)
    variants = _variant_map(variants_jsonl)
    stock_bars = _load_stock_bars(stock_bars_path)
    contracts, option_bars, option_trades = _load_option_inputs(
        queue=queue,
        selected_contracts_root=selected_contracts_root,
        option_bars_root=option_bars_root,
        option_trades_root=option_trades_root,
    )
    option_index = _build_option_research_index(
        contracts=contracts,
        option_bars=option_bars,
        option_trades=option_trades,
    )
    queue_items = _candidate_queue_items(
        queue,
        candidate_ids=candidate_ids,
        symbol_filter=symbol_filter,
        top_n=top_n,
    )

    trade_rows: list[dict[str, Any]] = []
    entry_lag = timedelta(minutes=entry_lag_minutes)
    max_probe_lag = timedelta(minutes=max_probe_lag_minutes)
    for queue_item in queue_items:
        candidate_id = str(queue_item.get("candidate_variant_id") or "")
        variant = variants.get(candidate_id)
        if not variant:
            continue
        source_trades = _stock_trades_for_variant(
            variant=variant,
            stock_bars=stock_bars,
            initial_cash=initial_cash,
            allocation_fraction=allocation_fraction,
        )
        for trade_index, trade in enumerate(source_trades.to_dict("records")):
            entry_time = pd.Timestamp(trade["entry_time"])
            exit_time = pd.Timestamp(trade["exit_time"])
            trade_date = entry_time.date()
            contract, entry_bar, entry_status = _choose_entry_liquidity_first_contract(
                contracts=contracts,
                option_bars=option_bars,
                option_trades=option_trades,
                option_index=option_index,
                symbol=str(queue_item.get("symbol") or ""),
                option_type=str(queue_item.get("directional_option_type") or ""),
                trade_date=trade_date,
                entry_time=entry_time,
                max_lag=entry_lag,
            )
            contract_symbol = str(contract["symbol"]) if contract else None
            first_available_lag = (
                _first_bar_lag_minutes(
                    option_index=option_index,
                    contract_symbol=contract_symbol,
                    timestamp=exit_time,
                    max_lag=max_probe_lag,
                )
                if contract_symbol
                else None
            )
            for lag in exit_lags:
                exit_lag = timedelta(minutes=lag)
                exit_bar = (
                    _first_option_bar(
                        option_bars=option_bars,
                        option_index=option_index,
                        contract_symbol=contract_symbol,
                        timestamp=exit_time,
                        max_lag=exit_lag,
                    )
                    if contract_symbol
                    else None
                )
                trade_print_count = (
                    _trade_print_count(
                        option_trades=option_trades,
                        option_index=option_index,
                        contract_symbol=contract_symbol,
                        start=exit_time,
                        end=exit_time + exit_lag,
                    )
                    if contract_symbol
                    else 0
                )
                if entry_status != "selected":
                    exit_status = entry_status
                    classification = entry_status
                elif exit_bar:
                    exit_status = "filled"
                    classification = "filled"
                else:
                    exit_status = "missing_exit_bar"
                    classification = classify_missing_exit(
                        trade_print_count=trade_print_count,
                        first_available_lag_minutes=first_available_lag,
                        lag_minutes=lag,
                        max_probe_lag_minutes=max_probe_lag_minutes,
                    )
                trade_rows.append(
                    {
                        "candidate_variant_id": candidate_id,
                        "symbol": queue_item.get("symbol"),
                        "source_strategy_id": queue_item.get("source_strategy_id"),
                        "directional_option_type": queue_item.get("directional_option_type"),
                        "source_trade_index": trade_index,
                        "trade_date": str(trade_date),
                        "stock_entry_time": str(entry_time),
                        "stock_exit_time": str(exit_time),
                        "contract_symbol": contract_symbol,
                        "entry_status": entry_status,
                        "exit_lag_minutes": lag,
                        "exit_status": exit_status,
                        "exit_missing_classification": classification,
                        "exit_trade_print_count": trade_print_count,
                        "first_available_exit_lag_minutes": first_available_lag,
                        "filled_exit_lag_minutes": (
                            round(
                                (
                                    pd.Timestamp(exit_bar["timestamp"]) - exit_time
                                ).total_seconds()
                                / 60.0,
                                4,
                            )
                            if exit_bar
                            else None
                        ),
                    }
                )

    profile_rows: list[dict[str, Any]] = []
    for item in queue_items:
        profile_rows.extend(
            _summarize_lag_rows(
                trade_rows,
                candidate=item,
                exit_lags=exit_lags,
                fill_coverage_gate=fill_coverage_gate,
            )
        )
    candidate_summaries = _candidate_summaries(
        profile_rows,
        exit_lags=exit_lags,
        fill_coverage_gate=fill_coverage_gate,
    )
    full_stack_pass_count = sum(1 for row in candidate_summaries if row["full_stack_fill_gate_pass"])
    wide_lag_candidate_count = sum(
        1
        for row in candidate_summaries
        if row["wide_lag_fill_gate_pass"] and not row["full_stack_fill_gate_pass"]
    )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "option_exit_lag_feasibility_complete",
        "decision": (
            "fill_feasible_under_full_stack"
            if full_stack_pass_count
            else "research_only_blocked_exit_lag"
        ),
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "queue_json": str(queue_json),
        "variants_jsonl": str(variants_jsonl),
        "stock_bars_path": str(stock_bars_path),
        "selected_contracts_root": str(selected_contracts_root),
        "option_bars_root": str(option_bars_root),
        "option_trades_root": str(option_trades_root),
        "candidate_count": len(candidate_summaries),
        "full_stack_fill_gate_pass_count": full_stack_pass_count,
        "wide_lag_only_candidate_count": wide_lag_candidate_count,
        "entry_lag_minutes": entry_lag_minutes,
        "exit_lags_minutes": exit_lags,
        "max_probe_lag_minutes": max_probe_lag_minutes,
        "fill_coverage_gate": fill_coverage_gate,
        "candidate_summaries": candidate_summaries,
        "profile_summaries": profile_rows,
        "artifacts": {
            "packet_json": str(output_dir / "exit_lag_feasibility_packet.json"),
            "packet_md": str(output_dir / "exit_lag_feasibility_packet.md"),
            "candidate_summary_json": str(output_dir / "exit_lag_candidate_summary.json"),
            "candidate_summary_csv": str(output_dir / "exit_lag_candidate_summary.csv"),
            "profile_summary_json": str(output_dir / "exit_lag_profile_summary.json"),
            "profile_summary_csv": str(output_dir / "exit_lag_profile_summary.csv"),
            "trade_diagnostics_json": str(output_dir / "exit_lag_trade_diagnostics.json"),
            "trade_diagnostics_csv": str(output_dir / "exit_lag_trade_diagnostics.csv"),
        },
        "hard_rules": [
            "This packet is not broker-facing.",
            "This packet does not authorize live manifest changes.",
            "This packet does not change strategy selection or risk policy.",
            "Do not relax the fill-coverage gate from this packet alone.",
        ],
        "next_actions": [
            "Use this packet to decide whether gaps are data repair, execution timing, or strategy design issues.",
            "If candidates only pass at wider exit lags, treat them as exit-policy research candidates, not promotions.",
            "Require governed strategy review and broker-audited paper evidence before any activation discussion.",
        ],
    }

    _write_json(output_dir / "exit_lag_feasibility_packet.json", packet)
    _write_markdown(output_dir / "exit_lag_feasibility_packet.md", packet)
    _write_json(output_dir / "exit_lag_candidate_summary.json", candidate_summaries)
    _write_csv(output_dir / "exit_lag_candidate_summary.csv", candidate_summaries)
    _write_json(output_dir / "exit_lag_profile_summary.json", profile_rows)
    _write_csv(output_dir / "exit_lag_profile_summary.csv", profile_rows)
    _write_json(output_dir / "exit_lag_trade_diagnostics.json", trade_rows)
    _write_csv(output_dir / "exit_lag_trade_diagnostics.csv", trade_rows)
    return packet


def main() -> None:
    args = parse_args()
    packet = build_option_exit_lag_feasibility(
        queue_json=Path(args.queue_json),
        variants_jsonl=Path(args.variants_jsonl),
        stock_bars_path=Path(args.stock_bars_path),
        selected_contracts_root=Path(args.selected_contracts_root),
        option_bars_root=Path(args.option_bars_root),
        option_trades_root=Path(args.option_trades_root),
        output_dir=Path(args.output_dir),
        candidate_ids=_parse_csv_set(args.candidate_ids),
        symbol_filter=_parse_csv_set(args.symbol_filter),
        top_n=args.top_n,
        entry_lag_minutes=args.entry_lag_minutes,
        exit_lags_minutes=_parse_int_csv(args.exit_lags_minutes),
        max_probe_lag_minutes=args.max_probe_lag_minutes,
        fill_coverage_gate=args.fill_coverage_gate,
        initial_cash=args.initial_cash,
        allocation_fraction=args.allocation_fraction,
    )
    print(json.dumps(packet, indent=2, default=_json_default))


if __name__ == "__main__":
    main()

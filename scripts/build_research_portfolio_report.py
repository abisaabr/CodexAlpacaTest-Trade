from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a research-only portfolio construction report from option-aware replays."
    )
    parser.add_argument("--replay-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fill-coverage-gate", type=float, default=0.90)
    parser.add_argument("--min-option-trades", type=int, default=20)
    parser.add_argument("--min-test-net-pnl", type=float, default=0.0)
    parser.add_argument("--max-positions", type=int, default=5)
    parser.add_argument("--max-symbol-weight", type=float, default=0.50)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    return parser.parse_args()


def _profile_name(path: Path) -> str:
    return path.parent.name


def _load_candidate_summaries(replay_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(replay_root.rglob("option_aware_candidate_summary.csv")):
        frame = pd.read_csv(path)
        if frame.empty:
            continue
        frame["profile"] = _profile_name(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _float(value: object, default: float = 0.0) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return default if pd.isna(parsed) else float(parsed)


def _candidate_score(row: dict[str, Any]) -> float:
    drawdown_penalty = abs(min(_float(row["worst_drawdown"]), 0.0))
    fill_bonus = 1_000.0 * _float(row["min_fill_coverage"])
    test_bonus = 2.0 * max(_float(row["min_test_net_pnl"]), 0.0)
    trade_bonus = 20.0 * min(_float(row["min_option_trade_count"]), 50.0)
    return (
        _float(row["min_net_pnl"]) + test_bonus + fill_bonus + trade_bonus - 0.5 * drawdown_penalty
    )


def summarize_candidates(
    candidates: pd.DataFrame,
    *,
    fill_coverage_gate: float,
    min_option_trades: int,
    min_test_net_pnl: float,
) -> list[dict[str, Any]]:
    if candidates.empty:
        return []

    numeric_columns = [
        "net_pnl",
        "test_net_pnl",
        "fill_coverage",
        "option_trade_count",
        "max_drawdown",
        "win_rate",
        "profit_factor",
        "missing_option_price_count",
        "missing_no_selected_contract",
        "missing_no_entry_bar",
        "missing_no_exit_bar",
    ]
    frame = candidates.copy()
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    rows: list[dict[str, Any]] = []
    for candidate_id, group in frame.groupby("candidate_variant_id", sort=False):
        first = group.iloc[0]
        min_fill = _float(group["fill_coverage"].min())
        min_trades = int(_float(group["option_trade_count"].min()))
        min_test = _float(group["test_net_pnl"].min())
        row = {
            "candidate_variant_id": candidate_id,
            "symbol": str(first.get("symbol")),
            "source_strategy_id": str(first.get("source_strategy_id")),
            "directional_option_type": str(first.get("directional_option_type")),
            "profile_count": int(group["profile"].nunique()),
            "profiles": sorted(str(value) for value in group["profile"].dropna().unique()),
            "min_net_pnl": _float(group["net_pnl"].min()),
            "median_net_pnl": _float(group["net_pnl"].median()),
            "min_test_net_pnl": min_test,
            "median_test_net_pnl": _float(group["test_net_pnl"].median()),
            "min_fill_coverage": min_fill,
            "max_fill_coverage": _float(group["fill_coverage"].max()),
            "min_option_trade_count": min_trades,
            "max_missing_option_price_count": int(
                _float(group["missing_option_price_count"].max())
            ),
            "max_missing_no_selected_contract": int(
                _float(group["missing_no_selected_contract"].max())
            ),
            "max_missing_no_entry_bar": int(_float(group["missing_no_entry_bar"].max())),
            "max_missing_no_exit_bar": int(_float(group["missing_no_exit_bar"].max())),
            "worst_drawdown": _float(group["max_drawdown"].min()),
            "min_win_rate": _float(group["win_rate"].min()),
            "positive_net_profile_count": int((group["net_pnl"] > 0).sum()),
            "positive_test_profile_count": int((group["test_net_pnl"] > min_test_net_pnl).sum()),
        }
        row["research_score"] = round(_candidate_score(row), 6)
        blockers = []
        if min_fill < fill_coverage_gate:
            blockers.append(f"fill_coverage_below_{fill_coverage_gate:.2f}")
        if min_trades < min_option_trades:
            blockers.append(f"option_trades_below_{min_option_trades}")
        if min_test <= min_test_net_pnl:
            blockers.append(f"test_net_pnl_not_above_{min_test_net_pnl:g}")
        if row["min_net_pnl"] <= 0:
            blockers.append("min_net_pnl_not_positive")
        row["promotion_status"] = (
            "eligible_for_promotion_review" if not blockers else "research_only_blocked"
        )
        row["promotion_blockers"] = blockers
        rows.append(row)

    return sorted(
        rows,
        key=lambda item: (
            item["promotion_status"] == "eligible_for_promotion_review",
            item["research_score"],
            item["min_test_net_pnl"],
            item["min_fill_coverage"],
        ),
        reverse=True,
    )


def _cap_weights(raw_weights: dict[str, float], max_weight: float) -> dict[str, float]:
    if not raw_weights:
        return {}
    weights = {key: max(float(value), 0.0) for key, value in raw_weights.items()}
    total = sum(weights.values())
    if total <= 0:
        equal = min(1.0 / len(weights), max_weight)
        return {key: equal for key in weights}
    weights = {key: value / total for key, value in weights.items()}
    capped: dict[str, float] = {}
    remaining = dict(weights)
    remaining_weight = 1.0
    while remaining:
        total_remaining = sum(remaining.values())
        if total_remaining <= 0:
            equal = remaining_weight / len(remaining)
            capped.update({key: equal for key in remaining})
            break
        progress = False
        for key, value in list(remaining.items()):
            proposed = remaining_weight * value / total_remaining
            if proposed > max_weight:
                capped[key] = max_weight
                remaining_weight -= max_weight
                remaining.pop(key)
                progress = True
        if not progress:
            capped.update(
                {
                    key: remaining_weight * value / total_remaining
                    for key, value in remaining.items()
                }
            )
            break
    return {key: round(value, 6) for key, value in capped.items()}


def build_capital_plan(
    candidate_rows: list[dict[str, Any]],
    *,
    max_positions: int,
    max_symbol_weight: float,
    initial_cash: float,
    min_option_trades: int,
) -> list[dict[str, Any]]:
    eligible_rows = [
        row
        for row in candidate_rows
        if row["promotion_status"] == "eligible_for_promotion_review"
    ]
    plan_pool = eligible_rows if eligible_rows else candidate_rows

    by_symbol: dict[str, dict[str, Any]] = {}
    for row in plan_pool:
        if row["min_net_pnl"] <= 0 or row["min_test_net_pnl"] <= 0:
            continue
        if row["min_option_trade_count"] < min_option_trades:
            continue
        symbol = str(row["symbol"]).upper()
        current = by_symbol.get(symbol)
        if current is None or row["research_score"] > current["research_score"]:
            by_symbol[symbol] = row

    selected = sorted(by_symbol.values(), key=lambda item: item["research_score"], reverse=True)[
        :max_positions
    ]
    raw_weights = {}
    for row in selected:
        drawdown = max(abs(min(_float(row["worst_drawdown"]), 0.0)), 250.0)
        raw_weights[row["candidate_variant_id"]] = (
            max(_float(row["research_score"]), 1.0) / drawdown
        )
    weights = _cap_weights(raw_weights, max_symbol_weight)

    plan = []
    for row in selected:
        weight = weights.get(row["candidate_variant_id"], 0.0)
        plan.append(
            {
                "candidate_variant_id": row["candidate_variant_id"],
                "symbol": row["symbol"],
                "source_strategy_id": row["source_strategy_id"],
                "directional_option_type": row["directional_option_type"],
                "research_only_weight": weight,
                "research_only_dollars": round(weight * initial_cash, 2),
                "min_net_pnl": row["min_net_pnl"],
                "min_test_net_pnl": row["min_test_net_pnl"],
                "min_fill_coverage": row["min_fill_coverage"],
                "min_option_trade_count": row["min_option_trade_count"],
                "worst_drawdown": row["worst_drawdown"],
                "promotion_status": row["promotion_status"],
                "promotion_blockers": row["promotion_blockers"],
            }
        )
    return plan


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Research Portfolio Report",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Status: `{packet['status']}`",
        f"- Promotion allowed: `{packet['promotion_allowed']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Fill coverage gate: `{packet['fill_coverage_gate']}`",
        f"- Minimum option trades: `{packet['min_option_trades']}`",
        f"- Capital plan allocated weight: `{packet['capital_plan_allocated_weight']}`",
        f"- Capital plan unallocated dollars: `${packet['capital_plan_unallocated_dollars']}`",
        "",
        "## Capital Plan",
        "",
    ]
    if not packet["capital_plan"]:
        lines.append(
            "- No research-only capital plan candidates passed the interim positive-PnL filters."
        )
    for row in packet["capital_plan"]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` weight "
            f"`{row['research_only_weight']:.2%}` dollars `${row['research_only_dollars']}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"fill `{row['min_fill_coverage']}` status `{row['promotion_status']}`"
        )
    lines.extend(["", "## Top Candidates", ""])
    for row in packet["top_candidates"][:20]:
        blockers = ", ".join(row["promotion_blockers"]) or "none"
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` score `{row['research_score']}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"fill `{row['min_fill_coverage']}-{row['max_fill_coverage']}` "
            f"trades `{row['min_option_trade_count']}` blockers `{blockers}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_research_portfolio_report(
    *,
    replay_root: Path,
    output_dir: Path,
    fill_coverage_gate: float,
    min_option_trades: int,
    min_test_net_pnl: float,
    max_positions: int,
    max_symbol_weight: float,
    initial_cash: float,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates = _load_candidate_summaries(replay_root)
    candidate_rows = summarize_candidates(
        candidates,
        fill_coverage_gate=fill_coverage_gate,
        min_option_trades=min_option_trades,
        min_test_net_pnl=min_test_net_pnl,
    )
    capital_plan = build_capital_plan(
        candidate_rows,
        max_positions=max_positions,
        max_symbol_weight=max_symbol_weight,
        initial_cash=initial_cash,
        min_option_trades=min_option_trades,
    )
    allocated_weight = round(sum(row["research_only_weight"] for row in capital_plan), 6)
    eligible_count = sum(
        1 for row in candidate_rows if row["promotion_status"] == "eligible_for_promotion_review"
    )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "research_portfolio_report_complete",
        "promotion_allowed": eligible_count > 0,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "replay_root": str(replay_root),
        "candidate_count": len(candidate_rows),
        "eligible_for_promotion_review_count": eligible_count,
        "fill_coverage_gate": fill_coverage_gate,
        "min_option_trades": min_option_trades,
        "min_test_net_pnl": min_test_net_pnl,
        "max_positions": max_positions,
        "max_symbol_weight": max_symbol_weight,
        "initial_cash": initial_cash,
        "capital_plan": capital_plan,
        "capital_plan_allocated_weight": allocated_weight,
        "capital_plan_unallocated_weight": round(max(1.0 - allocated_weight, 0.0), 6),
        "capital_plan_unallocated_dollars": round(max(1.0 - allocated_weight, 0.0) * initial_cash, 2),
        "top_candidates": candidate_rows[:50],
        "next_step_contract": [
            "Treat the capital plan as research-only until fill coverage reaches the configured gate.",
            "Use the event-driven selected-contract downloader to repair no_entry_bar/no_selected_contract gaps.",
            "Do not modify live manifests, strategy selection, or risk policy from this report.",
        ],
    }
    json_path = output_dir / "research_portfolio_report.json"
    md_path = output_dir / "research_portfolio_report.md"
    json_path.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")
    _write_markdown(md_path, packet)
    return packet


def main() -> None:
    args = parse_args()
    packet = build_research_portfolio_report(
        replay_root=Path(args.replay_root),
        output_dir=Path(args.output_dir),
        fill_coverage_gate=args.fill_coverage_gate,
        min_option_trades=args.min_option_trades,
        min_test_net_pnl=args.min_test_net_pnl,
        max_positions=args.max_positions,
        max_symbol_weight=args.max_symbol_weight,
        initial_cash=args.initial_cash,
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

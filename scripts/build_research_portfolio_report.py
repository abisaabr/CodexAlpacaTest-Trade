from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

UTC = timezone.utc

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
    parser.add_argument("--max-strategies-per-symbol", type=int, default=2)
    parser.add_argument("--max-symbol-weight", type=float, default=0.50)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument(
        "--required-regimes",
        default="bull,bear,choppy",
        help=(
            "Comma-separated regime set required for regime-complete paper-readiness "
            "summary. This does not change candidate-level promotion gates."
        ),
    )
    parser.add_argument(
        "--candidate-identity-mode",
        choices=["variant", "variant_profile"],
        default="variant",
        help=(
            "Use variant_profile for portfolio-wide aggregation so rescue/stress "
            "runs cannot demote a candidate from a different replay profile."
        ),
    )
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


def _first_text(row: pd.Series, key: str) -> str:
    value = row.get(key)
    if pd.isna(value):
        return ""
    return str(value)


def _identity_slug(value: object) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", str(value).lower()).strip("-")
    return slug or "unknown"


def _infer_intended_regime(*values: object) -> str:
    for value in values:
        tokens = re.split(r"[^a-z0-9]+", str(value or "").lower())
        for token in tokens:
            if token in {"bull", "bear", "choppy"}:
                return token
    return ""


def _candidate_score(row: dict[str, Any]) -> float:
    drawdown_penalty = abs(min(_float(row["worst_drawdown"]), 0.0))
    fill_bonus = 1_000.0 * _float(row["min_fill_coverage"])
    test_bonus = 2.0 * max(_float(row["min_test_net_pnl"]), 0.0)
    trade_bonus = 20.0 * min(_float(row["min_option_trade_count"]), 50.0)
    return (
        _float(row["min_net_pnl"]) + test_bonus + fill_bonus + trade_bonus - 0.5 * drawdown_penalty
    )


def _fill_failure_reason(row: dict[str, Any], fill_coverage_gate: float) -> str:
    if _float(row.get("min_fill_coverage")) >= fill_coverage_gate:
        return "fill_gate_clear"
    existing = str(row.get("fill_failure_reason") or "")
    if existing and existing != "nan":
        return existing
    missing = {
        "selected_contract_universe_gap": int(_float(row.get("max_missing_no_selected_contract"))),
        "entry_bar_gap_or_entry_timing_mismatch": int(_float(row.get("max_missing_no_entry_bar"))),
        "exit_bar_gap_or_exit_policy_mismatch": int(_float(row.get("max_missing_no_exit_bar"))),
    }
    dominant_reason, dominant_count = max(missing.items(), key=lambda item: item[1])
    if dominant_count <= 0:
        return "mixed_low_fill_gap"
    tied = [reason for reason, count in missing.items() if count == dominant_count]
    return dominant_reason if len(tied) == 1 else "mixed_low_fill_gap"


def summarize_candidates(
    candidates: pd.DataFrame,
    *,
    fill_coverage_gate: float,
    min_option_trades: int,
    min_test_net_pnl: float,
    candidate_identity_mode: str = "variant",
) -> list[dict[str, Any]]:
    if candidates.empty:
        return []
    if candidate_identity_mode not in {"variant", "variant_profile"}:
        raise ValueError(f"Unsupported candidate identity mode: {candidate_identity_mode}")

    numeric_columns = [
        "net_pnl",
        "test_net_pnl",
        "fill_coverage",
        "strategy_fill_coverage",
        "data_foundation_coverage",
        "entry_bar_coverage",
        "exit_bar_coverage",
        "intended_order_count",
        "filled_order_count",
        "skipped_order_count",
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
    frame["base_candidate_variant_id"] = frame["candidate_variant_id"].astype(str)
    if candidate_identity_mode == "variant_profile":
        frame["aggregate_candidate_key"] = frame.apply(
            lambda row: (
                f"{row['base_candidate_variant_id']}__profile_{_identity_slug(row.get('profile'))}"
            ),
            axis=1,
        )
    else:
        frame["aggregate_candidate_key"] = frame["base_candidate_variant_id"]

    rows: list[dict[str, Any]] = []
    for candidate_key, group in frame.groupby("aggregate_candidate_key", sort=False):
        first = group.iloc[0]
        base_candidate_id = str(first.get("base_candidate_variant_id"))
        fill_column = "strategy_fill_coverage" if "strategy_fill_coverage" in group else "fill_coverage"
        min_fill = _float(group[fill_column].min())
        min_trades = int(_float(group["option_trade_count"].min()))
        min_test = _float(group["test_net_pnl"].min())
        min_data_foundation = (
            _float(group["data_foundation_coverage"].min())
            if "data_foundation_coverage" in group
            else None
        )
        row = {
            "candidate_variant_id": str(candidate_key),
            "base_candidate_variant_id": base_candidate_id,
            "candidate_identity_mode": candidate_identity_mode,
            "aggregate_profile": (
                _first_text(first, "profile") if candidate_identity_mode == "variant_profile" else None
            ),
            "symbol": str(first.get("symbol")),
            "strategy_id": _first_text(first, "strategy_id")
            or _first_text(first, "source_strategy_id"),
            "source_strategy_id": str(first.get("source_strategy_id")),
            "family": _first_text(first, "family"),
            "intended_regime": _first_text(first, "intended_regime")
            or _infer_intended_regime(
                first.get("strategy_id"),
                first.get("source_strategy_id"),
                first.get("candidate_variant_id"),
                base_candidate_id,
            ),
            "parameter_set": _first_text(first, "parameter_set"),
            "directional_option_type": str(first.get("directional_option_type")),
            "profile_count": int(group["profile"].nunique()),
            "profiles": sorted(str(value) for value in group["profile"].dropna().unique()),
            "min_net_pnl": _float(group["net_pnl"].min()),
            "median_net_pnl": _float(group["net_pnl"].median()),
            "min_test_net_pnl": min_test,
            "median_test_net_pnl": _float(group["test_net_pnl"].median()),
            "min_fill_coverage": min_fill,
            "max_fill_coverage": _float(group[fill_column].max()),
            "min_strategy_fill_coverage": min_fill,
            "max_strategy_fill_coverage": _float(group[fill_column].max()),
            "min_data_foundation_coverage": min_data_foundation,
            "min_entry_bar_coverage": (
                _float(group["entry_bar_coverage"].min())
                if "entry_bar_coverage" in group
                else None
            ),
            "min_exit_bar_coverage": (
                _float(group["exit_bar_coverage"].min())
                if "exit_bar_coverage" in group
                else None
            ),
            "min_intended_order_count": (
                int(_float(group["intended_order_count"].min()))
                if "intended_order_count" in group
                else (
                    int(_float(group["source_stock_trade_count"].min()))
                    if "source_stock_trade_count" in group
                    else min_trades + int(_float(group["missing_option_price_count"].max()))
                )
            ),
            "min_filled_order_count": (
                int(_float(group["filled_order_count"].min()))
                if "filled_order_count" in group
                else min_trades
            ),
            "max_skipped_order_count": (
                int(_float(group["skipped_order_count"].max()))
                if "skipped_order_count" in group
                else int(_float(group["missing_option_price_count"].max()))
            ),
            "fill_coverage_unit": _first_text(first, "fill_coverage_unit")
            or "filled_multi_leg_strategy_orders_per_intended_signal",
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
        if "fill_failure_reason" in group.columns:
            reasons = [
                str(value)
                for value in group["fill_failure_reason"].dropna().tolist()
                if str(value) and str(value) != "nan"
            ]
            if reasons:
                row["fill_failure_reason"] = Counter(reasons).most_common(1)[0][0]
        row["fill_failure_reason"] = _fill_failure_reason(row, fill_coverage_gate)
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
    max_strategies_per_symbol: int,
    max_symbol_weight: float,
    initial_cash: float,
    min_option_trades: int,
) -> list[dict[str, Any]]:
    eligible_rows = [
        row for row in candidate_rows if row["promotion_status"] == "eligible_for_promotion_review"
    ]
    plan_pool = eligible_rows if eligible_rows else candidate_rows

    selected: list[dict[str, Any]] = []
    selected_per_symbol: dict[str, int] = {}
    selected_base_candidate_ids: set[str] = set()
    for row in sorted(plan_pool, key=lambda item: item["research_score"], reverse=True):
        if row["min_net_pnl"] <= 0 or row["min_test_net_pnl"] <= 0:
            continue
        if row["min_option_trade_count"] < min_option_trades:
            continue
        base_candidate_id = str(row.get("base_candidate_variant_id") or row["candidate_variant_id"])
        if base_candidate_id in selected_base_candidate_ids:
            continue
        symbol = str(row["symbol"]).upper()
        if selected_per_symbol.get(symbol, 0) >= max_strategies_per_symbol:
            continue
        selected.append(row)
        selected_base_candidate_ids.add(base_candidate_id)
        selected_per_symbol[symbol] = selected_per_symbol.get(symbol, 0) + 1
        if len(selected) >= max_positions:
            break

    candidate_raw_weights: dict[str, float] = {}
    symbol_raw_weights: dict[str, float] = {}
    for row in selected:
        drawdown = max(abs(min(_float(row["worst_drawdown"]), 0.0)), 250.0)
        raw_weight = max(_float(row["research_score"]), 1.0) / drawdown
        candidate_raw_weights[row["candidate_variant_id"]] = raw_weight
        symbol = str(row["symbol"]).upper()
        symbol_raw_weights[symbol] = symbol_raw_weights.get(symbol, 0.0) + raw_weight

    symbol_weights = _cap_weights(symbol_raw_weights, max_symbol_weight)
    weights: dict[str, float] = {}
    for row in selected:
        symbol = str(row["symbol"]).upper()
        symbol_raw_weight = symbol_raw_weights.get(symbol, 0.0)
        if symbol_raw_weight <= 0:
            weights[row["candidate_variant_id"]] = 0.0
            continue
        weights[row["candidate_variant_id"]] = round(
            symbol_weights.get(symbol, 0.0)
            * candidate_raw_weights[row["candidate_variant_id"]]
            / symbol_raw_weight,
            6,
        )

    plan = []
    for row in selected:
        weight = weights.get(row["candidate_variant_id"], 0.0)
        plan.append(
            {
                "candidate_variant_id": row["candidate_variant_id"],
                "base_candidate_variant_id": row.get("base_candidate_variant_id"),
                "candidate_identity_mode": row.get("candidate_identity_mode"),
                "aggregate_profile": row.get("aggregate_profile"),
                "symbol": row["symbol"],
                "strategy_id": row.get("strategy_id"),
                "source_strategy_id": row["source_strategy_id"],
                "family": row.get("family"),
                "intended_regime": row.get("intended_regime"),
                "parameter_set": row.get("parameter_set"),
                "directional_option_type": row["directional_option_type"],
                "research_only_weight": weight,
                "research_only_dollars": round(weight * initial_cash, 2),
                "min_net_pnl": row["min_net_pnl"],
                "min_test_net_pnl": row["min_test_net_pnl"],
                "min_fill_coverage": row["min_fill_coverage"],
                "min_strategy_fill_coverage": row.get("min_strategy_fill_coverage"),
                "min_data_foundation_coverage": row.get("min_data_foundation_coverage"),
                "min_option_trade_count": row["min_option_trade_count"],
                "worst_drawdown": row["worst_drawdown"],
                "promotion_status": row["promotion_status"],
                "promotion_blockers": row["promotion_blockers"],
            }
        )
    return plan


def _blocker_counts(candidate_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in candidate_rows:
        counts.update(str(item) for item in row.get("promotion_blockers", []))
    return dict(sorted(counts.items()))


def _fill_failure_counts(candidate_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in candidate_rows:
        counts[str(row.get("fill_failure_reason") or "unknown")] += 1
    return dict(sorted(counts.items()))


def _normalize_required_regimes(required_regimes: list[str] | tuple[str, ...] | None) -> list[str]:
    regimes = [
        str(regime).strip().lower()
        for regime in (required_regimes or ["bull", "bear", "choppy"])
        if str(regime).strip()
    ]
    return list(dict.fromkeys(regimes))


def _regime_summary(
    candidate_rows: list[dict[str, Any]],
    *,
    required_regimes: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    normalized_required = _normalize_required_regimes(required_regimes)
    by_regime: dict[str, dict[str, Any]] = {
        regime: {
            "intended_regime": regime,
            "candidate_count": 0,
            "eligible_for_promotion_review_count": 0,
            "blocked_count": 0,
            "best_candidate_variant_id": None,
            "best_research_score": None,
            "best_min_net_pnl": None,
            "best_min_test_net_pnl": None,
            "best_min_fill_coverage": None,
            "best_promotion_status": None,
            "blocker_counts": Counter(),
            "fill_failure_counts": Counter(),
        }
        for regime in normalized_required
    }
    for row in candidate_rows:
        regime = str(row.get("intended_regime") or "unknown").lower()
        item = by_regime.setdefault(
            regime,
            {
                "intended_regime": regime,
                "candidate_count": 0,
                "eligible_for_promotion_review_count": 0,
                "blocked_count": 0,
                "best_candidate_variant_id": None,
                "best_research_score": None,
                "best_min_net_pnl": None,
                "best_min_test_net_pnl": None,
                "best_min_fill_coverage": None,
                "best_promotion_status": None,
                "blocker_counts": Counter(),
                "fill_failure_counts": Counter(),
            },
        )
        item["candidate_count"] += 1
        if row.get("promotion_status") == "eligible_for_promotion_review":
            item["eligible_for_promotion_review_count"] += 1
        else:
            item["blocked_count"] += 1
        for blocker in row.get("promotion_blockers", []):
            item["blocker_counts"][str(blocker)] += 1
        item["fill_failure_counts"][str(row.get("fill_failure_reason") or "unknown")] += 1
        if item["best_research_score"] is None or _float(row.get("research_score")) > _float(
            item["best_research_score"]
        ):
            item["best_candidate_variant_id"] = row.get("candidate_variant_id")
            item["best_research_score"] = row.get("research_score")
            item["best_min_net_pnl"] = row.get("min_net_pnl")
            item["best_min_test_net_pnl"] = row.get("min_test_net_pnl")
            item["best_min_fill_coverage"] = row.get("min_fill_coverage")
            item["best_promotion_status"] = row.get("promotion_status")

    order = {regime: index for index, regime in enumerate(normalized_required)}
    result: list[dict[str, Any]] = []
    for item in by_regime.values():
        item["blocker_counts"] = dict(sorted(item["blocker_counts"].items()))
        item["fill_failure_counts"] = dict(sorted(item["fill_failure_counts"].items()))
        result.append(item)
    return sorted(
        result,
        key=lambda row: (
            order.get(str(row["intended_regime"]), len(order)),
            str(row["intended_regime"]),
        ),
    )


def _eligible_regimes(regime_summary: list[dict[str, Any]]) -> list[str]:
    return [
        str(row["intended_regime"])
        for row in regime_summary
        if int(row.get("eligible_for_promotion_review_count") or 0) > 0
    ]


def _has_strong_data_foundation(row: dict[str, Any]) -> bool:
    value = row.get("min_data_foundation_coverage")
    if value in (None, "", "nan"):
        return False
    return _float(value) >= 0.90


def _is_data_repair_candidate(row: dict[str, Any]) -> bool:
    reason = str(row.get("fill_failure_reason") or "")
    if reason == "selected_contract_universe_gap":
        return True
    if reason in {
        "entry_bar_gap_or_entry_timing_mismatch",
        "exit_bar_gap_or_exit_policy_mismatch",
        "mixed_low_fill_gap",
    }:
        # If selected-contract coverage is already strong, timing misses are a strategy/replay
        # design problem rather than a raw-data repair target.
        return not _has_strong_data_foundation(row)
    return False


def _data_repair_candidates(
    candidate_rows: list[dict[str, Any]], max_items: int = 12
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in candidate_rows
        if row.get("promotion_status") != "eligible_for_promotion_review"
        and _is_data_repair_candidate(row)
        and _float(row.get("min_net_pnl")) > 0
        and _float(row.get("min_test_net_pnl")) > 0
    ]
    rows = sorted(rows, key=lambda item: item["research_score"], reverse=True)
    keys = [
        "candidate_variant_id",
        "symbol",
        "strategy_id",
        "source_strategy_id",
        "family",
        "intended_regime",
        "parameter_set",
        "directional_option_type",
        "min_net_pnl",
        "min_test_net_pnl",
        "min_fill_coverage",
        "max_fill_coverage",
        "min_strategy_fill_coverage",
        "max_strategy_fill_coverage",
        "min_data_foundation_coverage",
        "min_entry_bar_coverage",
        "min_exit_bar_coverage",
        "min_option_trade_count",
        "worst_drawdown",
        "promotion_status",
        "fill_coverage_unit",
        "fill_failure_reason",
        "promotion_blockers",
        "research_score",
    ]
    return [{key: row.get(key) for key in keys} for row in rows[:max_items]]


def _strategy_redesign_candidates(
    candidate_rows: list[dict[str, Any]], max_items: int = 12
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in candidate_rows
        if row.get("promotion_status") != "eligible_for_promotion_review"
        and (
            _float(row.get("min_net_pnl")) <= 0
            or _float(row.get("min_test_net_pnl")) <= 0
            or (
                _has_strong_data_foundation(row)
                and _float(row.get("min_fill_coverage")) < 0.90
            )
        )
    ]
    rows = sorted(rows, key=lambda item: item["research_score"], reverse=True)
    keys = [
        "candidate_variant_id",
        "symbol",
        "strategy_id",
        "source_strategy_id",
        "family",
        "intended_regime",
        "parameter_set",
        "directional_option_type",
        "min_net_pnl",
        "min_test_net_pnl",
        "min_fill_coverage",
        "max_fill_coverage",
        "min_strategy_fill_coverage",
        "max_strategy_fill_coverage",
        "min_data_foundation_coverage",
        "min_entry_bar_coverage",
        "min_exit_bar_coverage",
        "min_option_trade_count",
        "worst_drawdown",
        "promotion_status",
        "fill_coverage_unit",
        "fill_failure_reason",
        "promotion_blockers",
        "research_score",
    ]
    return [{key: row.get(key) for key in keys} for row in rows[:max_items]]


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Research Portfolio Report",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Status: `{packet['status']}`",
        f"- Promotion allowed: `{packet['promotion_allowed']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Fill coverage gate: `{packet['fill_coverage_gate']}`",
        f"- Fill coverage unit: `{packet.get('fill_coverage_unit')}`",
        f"- Fill coverage semantics: {packet.get('fill_coverage_semantics')}",
        f"- Minimum option trades: `{packet['min_option_trades']}`",
        f"- Maximum strategies per symbol: `{packet['max_strategies_per_symbol']}`",
        f"- Maximum symbol weight: `{packet['max_symbol_weight']}`",
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
            f"family `{row.get('family') or 'unknown'}` "
            f"regime `{row.get('intended_regime') or 'unknown'}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"strategy_fill `{row['min_fill_coverage']}` "
            f"data_foundation `{row.get('min_data_foundation_coverage')}` "
            f"status `{row['promotion_status']}`"
        )
    lines.extend(["", "## Top Candidates", ""])
    for row in packet["top_candidates"][:20]:
        blockers = ", ".join(row["promotion_blockers"]) or "none"
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` score `{row['research_score']}` "
            f"family `{row.get('family') or 'unknown'}` "
            f"regime `{row.get('intended_regime') or 'unknown'}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"strategy_fill `{row['min_fill_coverage']}-{row['max_fill_coverage']}` "
            f"data_foundation `{row.get('min_data_foundation_coverage')}` "
            f"trades `{row['min_option_trade_count']}` blockers `{blockers}`"
        )
    lines.extend(["", "## Fill Failure Counts", ""])
    for reason, count in packet["fill_failure_counts"].items():
        lines.append(f"- `{reason}`: `{count}`")
    lines.extend(["", "## Regime Completeness", ""])
    lines.append(f"- Required regimes: `{', '.join(packet['required_regimes'])}`")
    lines.append(f"- Eligible regimes: `{', '.join(packet['eligible_regimes']) or 'none'}`")
    lines.append(
        f"- Missing eligible regimes: `{', '.join(packet['missing_eligible_regimes']) or 'none'}`"
    )
    lines.append(
        f"- Regime complete for promotion review: `{packet['regime_complete_for_promotion_review']}`"
    )
    for row in packet["regime_summary"]:
        lines.append(
            "- "
            f"`{row['intended_regime']}` candidates `{row['candidate_count']}` "
            f"eligible `{row['eligible_for_promotion_review_count']}` "
            f"best `{row.get('best_candidate_variant_id')}` "
            f"best_fill `{row.get('best_min_fill_coverage')}` "
            f"best_status `{row.get('best_promotion_status')}`"
        )
    lines.extend(["", "## Data Repair Priority", ""])
    if not packet["data_repair_priority_candidates"]:
        lines.append("- No positive-economics data-repair candidates selected.")
    for row in packet["data_repair_priority_candidates"]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"fill `{row['min_fill_coverage']}-{row['max_fill_coverage']}` "
            f"reason `{row['fill_failure_reason']}` score `{row['research_score']}`"
        )
    lines.extend(["", "## Strategy Redesign Priority", ""])
    if not packet["strategy_redesign_candidates"]:
        lines.append("- No strategy-redesign candidates selected.")
    for row in packet["strategy_redesign_candidates"]:
        lines.append(
            "- "
            f"`{row['symbol']}` `{row['candidate_variant_id']}` "
            f"min_net `{row['min_net_pnl']}` min_test `{row['min_test_net_pnl']}` "
            f"reason `{row['fill_failure_reason']}`"
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
    max_strategies_per_symbol: int,
    max_symbol_weight: float,
    initial_cash: float,
    required_regimes: list[str] | tuple[str, ...] | None = None,
    candidate_identity_mode: str = "variant",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates = _load_candidate_summaries(replay_root)
    candidate_rows = summarize_candidates(
        candidates,
        fill_coverage_gate=fill_coverage_gate,
        min_option_trades=min_option_trades,
        min_test_net_pnl=min_test_net_pnl,
        candidate_identity_mode=candidate_identity_mode,
    )
    capital_plan = build_capital_plan(
        candidate_rows,
        max_positions=max_positions,
        max_strategies_per_symbol=max_strategies_per_symbol,
        max_symbol_weight=max_symbol_weight,
        initial_cash=initial_cash,
        min_option_trades=min_option_trades,
    )
    allocated_weight = round(sum(row["research_only_weight"] for row in capital_plan), 6)
    eligible_count = sum(
        1 for row in candidate_rows if row["promotion_status"] == "eligible_for_promotion_review"
    )
    normalized_required_regimes = _normalize_required_regimes(required_regimes)
    regime_summary = _regime_summary(
        candidate_rows, required_regimes=normalized_required_regimes
    )
    eligible_regimes = _eligible_regimes(regime_summary)
    missing_eligible_regimes = [
        regime for regime in normalized_required_regimes if regime not in set(eligible_regimes)
    ]
    fill_coverage_unit = next(
        (
            row.get("fill_coverage_unit")
            for row in candidate_rows
            if row.get("fill_coverage_unit")
        ),
        "filled_multi_leg_strategy_orders_per_intended_signal",
    )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "research_portfolio_report_complete",
        "promotion_allowed": eligible_count > 0,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "replay_root": str(replay_root),
        "candidate_identity_mode": candidate_identity_mode,
        "candidate_count": len(candidate_rows),
        "eligible_for_promotion_review_count": eligible_count,
        "fill_coverage_gate": fill_coverage_gate,
        "strategy_fill_coverage_gate": fill_coverage_gate,
        "fill_coverage_unit": fill_coverage_unit,
        "fill_coverage_semantics": (
            "fill_coverage is an alias for strategy_fill_coverage. "
            "min_data_foundation_coverage separates selected-contract availability from "
            "entry/exit timing and strategy execution misses."
        ),
        "min_option_trades": min_option_trades,
        "min_test_net_pnl": min_test_net_pnl,
        "max_positions": max_positions,
        "max_strategies_per_symbol": max_strategies_per_symbol,
        "max_symbol_weight": max_symbol_weight,
        "initial_cash": initial_cash,
        "capital_plan": capital_plan,
        "capital_plan_allocated_weight": allocated_weight,
        "capital_plan_unallocated_weight": round(max(1.0 - allocated_weight, 0.0), 6),
        "capital_plan_unallocated_dollars": round(
            max(1.0 - allocated_weight, 0.0) * initial_cash, 2
        ),
        "top_candidates": candidate_rows[:50],
        "blocker_counts": _blocker_counts(candidate_rows),
        "fill_failure_counts": _fill_failure_counts(candidate_rows),
        "required_regimes": normalized_required_regimes,
        "regime_summary": regime_summary,
        "eligible_regimes": eligible_regimes,
        "missing_eligible_regimes": missing_eligible_regimes,
        "regime_complete_for_promotion_review": not missing_eligible_regimes,
        "promotion_allowed_regime_complete": eligible_count > 0
        and not missing_eligible_regimes,
        "data_repair_priority_candidates": _data_repair_candidates(candidate_rows),
        "strategy_redesign_candidates": _strategy_redesign_candidates(candidate_rows),
        "next_step_contract": [
            "Treat the capital plan as research-only until fill coverage reaches the configured gate.",
            "Treat a symbol as regime-complete only when bull, bear, and choppy required regimes each have at least one eligible governed-review candidate.",
            "Use dense daily option-universe builds when selected-contract gaps dominate otherwise positive candidates.",
            "Use strategy redesign when data foundation is strong but entry/exit bar timing still blocks fills.",
            "Use event-driven selected-contract repairs only for isolated missing entry/exit bars with weak data foundation.",
            "Redesign or quarantine candidates whose economics fail after fill repair.",
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
        max_strategies_per_symbol=args.max_strategies_per_symbol,
        max_symbol_weight=args.max_symbol_weight,
        initial_cash=args.initial_cash,
        required_regimes=args.required_regimes.split(","),
        candidate_identity_mode=args.candidate_identity_mode,
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

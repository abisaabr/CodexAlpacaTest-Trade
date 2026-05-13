from __future__ import annotations

import argparse
import itertools
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Select a constrained research portfolio from projector scaled trades. "
            "This creates candidate reports only; it does not edit paper manifests."
        )
    )
    parser.add_argument("--portfolio-report-json", required=True)
    parser.add_argument("--scaled-trades-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--backtest-allocation-fraction", type=float, default=0.05)
    parser.add_argument("--train-end-date", default=None)
    parser.add_argument("--min-train-trades", type=int, default=5)
    parser.add_argument("--min-test-trades", type=int, default=5)
    parser.add_argument("--max-candidates", type=int, default=40)
    parser.add_argument("--max-per-symbol", type=int, default=4)
    parser.add_argument("--max-per-regime", type=int, default=8)
    parser.add_argument("--max-per-family", type=int, default=20)
    parser.add_argument("--min-symbols", type=int, default=3)
    parser.add_argument("--min-regimes", type=int, default=2)
    parser.add_argument("--min-families", type=int, default=2)
    parser.add_argument("--max-drawdown-pct", type=float, default=25.0)
    parser.add_argument(
        "--min-average-daily-pnl",
        type=float,
        default=None,
        help="Reject selected portfolios whose simulated average daily PnL is below this target.",
    )
    parser.add_argument("--max-symbol-trade-share", type=float, default=None)
    parser.add_argument("--max-regime-trade-share", type=float, default=None)
    parser.add_argument("--max-family-trade-share", type=float, default=None)
    parser.add_argument("--max-candidate-trade-share", type=float, default=None)
    parser.add_argument("--max-symbol-pnl-share", type=float, default=None)
    parser.add_argument("--max-regime-pnl-share", type=float, default=None)
    parser.add_argument("--max-family-pnl-share", type=float, default=None)
    parser.add_argument("--max-candidate-pnl-share", type=float, default=None)
    parser.add_argument(
        "--objective",
        choices=["test_pnl", "total_pnl", "risk_adjusted"],
        default="risk_adjusted",
    )
    parser.add_argument("--max-exact-candidates", type=int, default=20)
    return parser.parse_args()


def _load_plan(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [dict(row) for row in payload.get("capital_plan") or [] if isinstance(row, dict)]


def _key(row: dict[str, Any]) -> tuple[str, str]:
    base_id = str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or "")
    return base_id.split("__profile_", 1)[0], str(row.get("aggregate_profile") or "")


def _trade_key(row: pd.Series) -> tuple[str, str]:
    base_id = str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or "")
    return base_id.split("__profile_", 1)[0], str(row.get("aggregate_profile") or "")


def _pnl_column(frame: pd.DataFrame) -> str:
    if "scaled_projection_pnl" in frame.columns:
        return "scaled_projection_pnl"
    if "scaled_option_pnl" in frame.columns:
        return "scaled_option_pnl"
    if "source_pnl_per_combo" in frame.columns:
        return "source_pnl_per_combo"
    if "source_option_pnl" in frame.columns:
        return "source_option_pnl"
    raise ValueError("Scaled trades are missing a supported PnL column")


def _max_drawdown(values: list[float]) -> tuple[float, float]:
    peak = values[0] if values else 0.0
    max_drawdown = 0.0
    max_drawdown_pct = 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = value - peak
        drawdown_pct = drawdown / peak * 100.0 if peak > 0 else 0.0
        max_drawdown = min(max_drawdown, drawdown)
        max_drawdown_pct = min(max_drawdown_pct, drawdown_pct)
    return max_drawdown, max_drawdown_pct


def _simulate_selected(
    trades: pd.DataFrame,
    selected_keys: set[tuple[str, str]],
    *,
    initial_cash: float,
    backtest_allocation_fraction: float,
) -> dict[str, Any]:
    if not selected_keys:
        return {
            "ending_equity": initial_cash,
            "net_pnl": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": 0.0,
            "trading_days": 0,
        }
    frame = trades[trades["_optimizer_key"].isin(selected_keys)].copy()
    if frame.empty:
        return {
            "ending_equity": initial_cash,
            "net_pnl": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": 0.0,
            "trading_days": 0,
        }
    pnl_col = _pnl_column(frame)
    weight = 1.0 / len(selected_keys)
    backtest_budget = initial_cash * backtest_allocation_fraction
    equity = initial_cash
    values = [equity]
    daily_rows = []
    for trade_date, group in frame.sort_values("trade_date").groupby("trade_date"):
        start = equity
        scale = (start * weight) / backtest_budget if backtest_budget > 0 else 0.0
        pnl = float(pd.to_numeric(group[pnl_col], errors="coerce").fillna(0.0).sum()) * scale
        equity = max(start + pnl, 0.0)
        values.append(equity)
        daily_rows.append({"trade_date": str(trade_date), "daily_pnl": round(pnl, 6), "ending_equity": round(equity, 6)})
    max_dd, max_dd_pct = _max_drawdown(values)
    return {
        "ending_equity": round(equity, 2),
        "net_pnl": round(equity - initial_cash, 2),
        "max_drawdown": round(max_dd, 2),
        "max_drawdown_pct": round(max_dd_pct, 4),
        "trading_days": len(daily_rows),
        "average_daily_pnl": round((equity - initial_cash) / len(daily_rows), 4)
        if daily_rows
        else 0.0,
        "daily_rows": daily_rows,
    }


def _prepare_daily_vectors(trades: pd.DataFrame) -> dict[str, Any]:
    frame = trades.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date.astype(str)
    pnl_col = _pnl_column(frame)
    frame["_pnl_for_optimizer"] = pd.to_numeric(frame[pnl_col], errors="coerce").fillna(0.0)
    trade_dates = sorted(frame["trade_date"].dropna().unique().tolist())
    date_index = {date: idx for idx, date in enumerate(trade_dates)}
    pnl_vectors: dict[tuple[str, str], np.ndarray] = {}
    count_vectors: dict[tuple[str, str], np.ndarray] = {}
    grouped = (
        frame.groupby(["_optimizer_key", "trade_date"], dropna=False)
        .agg(pnl=("_pnl_for_optimizer", "sum"), count=("_pnl_for_optimizer", "size"))
        .reset_index()
    )
    for key, group in grouped.groupby("_optimizer_key", dropna=False):
        pnl_vector = np.zeros(len(trade_dates), dtype=float)
        count_vector = np.zeros(len(trade_dates), dtype=np.int32)
        for _, row in group.iterrows():
            idx = date_index.get(str(row["trade_date"]))
            if idx is None:
                continue
            pnl_vector[idx] = float(row["pnl"])
            count_vector[idx] = int(row["count"])
        pnl_vectors[key] = pnl_vector
        count_vectors[key] = count_vector
    return {
        "trade_dates": trade_dates,
        "pnl_vectors": pnl_vectors,
        "count_vectors": count_vectors,
    }


def _simulate_selected_fast(
    prepared: dict[str, Any],
    selected_keys: set[tuple[str, str]],
    *,
    initial_cash: float,
    backtest_allocation_fraction: float,
) -> dict[str, Any]:
    if not selected_keys:
        return {
            "ending_equity": initial_cash,
            "net_pnl": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": 0.0,
            "trading_days": 0,
            "average_daily_pnl": 0.0,
        }
    trade_dates = prepared["trade_dates"]
    if not trade_dates:
        return {
            "ending_equity": initial_cash,
            "net_pnl": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": 0.0,
            "trading_days": 0,
            "average_daily_pnl": 0.0,
        }
    pnl_sum = np.zeros(len(trade_dates), dtype=float)
    count_sum = np.zeros(len(trade_dates), dtype=np.int32)
    for key in selected_keys:
        pnl_sum += prepared["pnl_vectors"].get(key, 0.0)
        count_sum += prepared["count_vectors"].get(key, 0)
    active_indexes = np.flatnonzero(count_sum > 0)
    if len(active_indexes) == 0:
        return {
            "ending_equity": initial_cash,
            "net_pnl": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": 0.0,
            "trading_days": 0,
            "average_daily_pnl": 0.0,
        }
    weight = 1.0 / len(selected_keys)
    backtest_budget = initial_cash * backtest_allocation_fraction
    equity = initial_cash
    values = [equity]
    for idx in active_indexes:
        start = equity
        scale = (start * weight) / backtest_budget if backtest_budget > 0 else 0.0
        pnl = float(pnl_sum[idx]) * scale
        equity = max(start + pnl, 0.0)
        values.append(equity)
    max_dd, max_dd_pct = _max_drawdown(values)
    return {
        "ending_equity": round(equity, 2),
        "net_pnl": round(equity - initial_cash, 2),
        "max_drawdown": round(max_dd, 2),
        "max_drawdown_pct": round(max_dd_pct, 4),
        "trading_days": int(len(active_indexes)),
        "average_daily_pnl": round((equity - initial_cash) / len(active_indexes), 4),
    }


def _candidate_stats(
    trades: pd.DataFrame,
    *,
    train_end_date: str | None,
    min_train_trades: int,
    min_test_trades: int,
) -> pd.DataFrame:
    frame = trades.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    frame["_optimizer_key"] = frame.apply(_trade_key, axis=1)
    pnl_col = _pnl_column(frame)
    split_date = pd.to_datetime(train_end_date).date() if train_end_date else None
    rows = []
    for key, group in frame.groupby("_optimizer_key", dropna=False):
        if split_date:
            train = group[group["trade_date"] <= split_date]
            test = group[group["trade_date"] > split_date]
        else:
            midpoint = max(int(len(group) * 0.7), 1)
            ordered = group.sort_values("trade_date")
            train = ordered.iloc[:midpoint]
            test = ordered.iloc[midpoint:]
        train_pnl = float(pd.to_numeric(train[pnl_col], errors="coerce").fillna(0.0).sum())
        test_pnl = float(pd.to_numeric(test[pnl_col], errors="coerce").fillna(0.0).sum())
        total_pnl = train_pnl + test_pnl
        blockers = []
        if len(train) < min_train_trades:
            blockers.append("min_train_trades")
        if len(test) < min_test_trades:
            blockers.append("min_test_trades")
        if train_pnl <= 0:
            blockers.append("train_pnl_not_positive")
        if test_pnl <= 0:
            blockers.append("test_pnl_not_positive")
        first = group.iloc[0]
        drawdown = _max_drawdown([0.0] + pd.to_numeric(group[pnl_col], errors="coerce").fillna(0.0).cumsum().tolist())[0]
        rows.append(
            {
                "base_candidate_variant_id": key[0],
                "aggregate_profile": key[1],
                "symbol": str(first.get("symbol") or "").upper(),
                "family": str(first.get("family") or ""),
                "intended_regime": str(first.get("intended_regime") or ""),
                "train_trades": int(len(train)),
                "test_trades": int(len(test)),
                "total_trades": int(len(group)),
                "train_pnl": round(train_pnl, 6),
                "test_pnl": round(test_pnl, 6),
                "total_pnl": round(total_pnl, 6),
                "candidate_drawdown": round(drawdown, 6),
                "eligible": not blockers,
                "blockers": ",".join(blockers),
                "_optimizer_key": key,
            }
        )
    return pd.DataFrame(rows)


def _objective_value(row: pd.Series, objective: str) -> float:
    if objective == "test_pnl":
        return float(row["test_pnl"])
    if objective == "total_pnl":
        return float(row["total_pnl"])
    drawdown = abs(float(row.get("candidate_drawdown") or 0.0))
    return float(row["test_pnl"]) / (1.0 + drawdown)


def _portfolio_objective(simulation: dict[str, Any], subset: pd.DataFrame, objective: str) -> float:
    if objective == "test_pnl":
        return float(pd.to_numeric(subset["test_pnl"], errors="coerce").fillna(0.0).sum())
    if objective == "total_pnl":
        return float(simulation.get("net_pnl") or 0.0)
    drawdown = abs(float(simulation.get("max_drawdown") or 0.0))
    if drawdown <= 0:
        drawdown = 1.0
    return float(simulation.get("net_pnl") or 0.0) / drawdown


def _counter_from_frame(rows: pd.DataFrame, column: str) -> Counter[str]:
    return Counter(str(value) for value in rows[column].tolist())


def _subset_cap_failure(
    rows: pd.DataFrame,
    *,
    max_per_symbol: int,
    max_per_regime: int,
    max_per_family: int,
) -> str | None:
    if any(count > max_per_symbol for count in _counter_from_frame(rows, "symbol").values()):
        return "max_per_symbol"
    if any(count > max_per_regime for count in _counter_from_frame(rows, "intended_regime").values()):
        return "max_per_regime"
    if any(count > max_per_family for count in _counter_from_frame(rows, "family").values()):
        return "max_per_family"
    return None


def _required_failures(
    rows: pd.DataFrame,
    *,
    min_symbols: int,
    min_regimes: int,
    min_families: int,
) -> list[str]:
    failures = []
    if rows["symbol"].astype(str).nunique() < min_symbols:
        failures.append("min_symbols")
    if rows["intended_regime"].astype(str).nunique() < min_regimes:
        failures.append("min_regimes")
    if rows["family"].astype(str).nunique() < min_families:
        failures.append("min_families")
    return failures


def _concentration_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "symbol_trade_share": {},
            "regime_trade_share": {},
            "family_trade_share": {},
            "candidate_trade_share": {},
            "symbol_abs_pnl_share": {},
            "regime_abs_pnl_share": {},
            "family_abs_pnl_share": {},
            "candidate_abs_pnl_share": {},
        }

    frame = rows.copy()
    frame["total_trades"] = pd.to_numeric(frame.get("total_trades"), errors="coerce").fillna(0.0)
    frame["abs_total_pnl"] = pd.to_numeric(frame.get("total_pnl"), errors="coerce").fillna(0.0).abs()

    def shares(column: str, metric: str) -> dict[str, float]:
        total = float(frame[metric].sum())
        if total <= 0:
            return {}
        grouped = frame.groupby(column, dropna=False)[metric].sum()
        return {
            str(key): round(float(value) / total, 8)
            for key, value in grouped.sort_values(ascending=False).items()
        }

    return {
        "symbol_trade_share": shares("symbol", "total_trades"),
        "regime_trade_share": shares("intended_regime", "total_trades"),
        "family_trade_share": shares("family", "total_trades"),
        "candidate_trade_share": shares("base_candidate_variant_id", "total_trades"),
        "symbol_abs_pnl_share": shares("symbol", "abs_total_pnl"),
        "regime_abs_pnl_share": shares("intended_regime", "abs_total_pnl"),
        "family_abs_pnl_share": shares("family", "abs_total_pnl"),
        "candidate_abs_pnl_share": shares("base_candidate_variant_id", "abs_total_pnl"),
    }


def _max_share(values: dict[str, float]) -> float:
    return max(values.values()) if values else 0.0


def _concentration_failures(
    rows: pd.DataFrame,
    *,
    max_symbol_trade_share: float | None,
    max_regime_trade_share: float | None,
    max_family_trade_share: float | None,
    max_candidate_trade_share: float | None,
    max_symbol_pnl_share: float | None,
    max_regime_pnl_share: float | None,
    max_family_pnl_share: float | None,
    max_candidate_pnl_share: float | None,
) -> list[str]:
    metrics = _concentration_metrics(rows)
    checks = [
        ("max_symbol_trade_share", max_symbol_trade_share, "symbol_trade_share"),
        ("max_regime_trade_share", max_regime_trade_share, "regime_trade_share"),
        ("max_family_trade_share", max_family_trade_share, "family_trade_share"),
        ("max_candidate_trade_share", max_candidate_trade_share, "candidate_trade_share"),
        ("max_symbol_pnl_share", max_symbol_pnl_share, "symbol_abs_pnl_share"),
        ("max_regime_pnl_share", max_regime_pnl_share, "regime_abs_pnl_share"),
        ("max_family_pnl_share", max_family_pnl_share, "family_abs_pnl_share"),
        ("max_candidate_pnl_share", max_candidate_pnl_share, "candidate_abs_pnl_share"),
    ]
    failures: list[str] = []
    for reason, limit, metric_key in checks:
        if limit is None:
            continue
        if _max_share(metrics[metric_key]) > float(limit):
            failures.append(reason)
    return failures


def _select_exact_subset(
    *,
    trades: pd.DataFrame,
    eligible: pd.DataFrame,
    initial_cash: float,
    backtest_allocation_fraction: float,
    max_candidates: int,
    max_per_symbol: int,
    max_per_regime: int,
    max_per_family: int,
    min_symbols: int,
    min_regimes: int,
    min_families: int,
    max_drawdown_pct: float,
    min_average_daily_pnl: float | None,
    max_symbol_trade_share: float | None,
    max_regime_trade_share: float | None,
    max_family_trade_share: float | None,
    max_candidate_trade_share: float | None,
    max_symbol_pnl_share: float | None,
    max_regime_pnl_share: float | None,
    max_family_pnl_share: float | None,
    max_candidate_pnl_share: float | None,
    objective: str = "risk_adjusted",
) -> tuple[set[tuple[str, str]], list[dict[str, Any]], dict[str, Any]]:
    rows = eligible.reset_index(drop=True)
    records = rows.to_dict("records")
    max_size = min(max_candidates, len(rows))
    best_keys: set[tuple[str, str]] = set()
    best_score: float | None = None
    best_simulation: dict[str, Any] = {}
    evaluated = 0
    infeasible_reasons: Counter[str] = Counter()
    prepared = _prepare_daily_vectors(trades)
    keys = [record["_optimizer_key"] for record in records]
    symbols = [str(record["symbol"]) for record in records]
    regimes = [str(record["intended_regime"]) for record in records]
    families = [str(record["family"]) for record in records]
    candidate_ids = [str(record["base_candidate_variant_id"]) for record in records]
    total_trades = [float(record.get("total_trades") or 0.0) for record in records]
    abs_total_pnls = [abs(float(record.get("total_pnl") or 0.0)) for record in records]
    test_pnls = [float(record.get("test_pnl") or 0.0) for record in records]

    def cap_failure(indexes: tuple[int, ...]) -> str | None:
        symbol_counts = Counter(symbols[index] for index in indexes)
        if any(count > max_per_symbol for count in symbol_counts.values()):
            return "max_per_symbol"
        regime_counts = Counter(regimes[index] for index in indexes)
        if any(count > max_per_regime for count in regime_counts.values()):
            return "max_per_regime"
        family_counts = Counter(families[index] for index in indexes)
        if any(count > max_per_family for count in family_counts.values()):
            return "max_per_family"
        return None

    def diversity_failures(indexes: tuple[int, ...]) -> list[str]:
        failures = []
        if len({symbols[index] for index in indexes}) < min_symbols:
            failures.append("min_symbols")
        if len({regimes[index] for index in indexes}) < min_regimes:
            failures.append("min_regimes")
        if len({families[index] for index in indexes}) < min_families:
            failures.append("min_families")
        return failures

    def share_failures(indexes: tuple[int, ...]) -> list[str]:
        failures = []
        size = len(indexes)
        if size <= 0:
            return failures
        share_specs = [
            ("symbol", symbols, max_symbol_trade_share, max_symbol_pnl_share),
            ("regime", regimes, max_regime_trade_share, max_regime_pnl_share),
            ("family", families, max_family_trade_share, max_family_pnl_share),
            ("candidate", candidate_ids, max_candidate_trade_share, max_candidate_pnl_share),
        ]
        for name, labels, max_trade_share, max_pnl_share in share_specs:
            if max_trade_share is not None:
                trade_totals: Counter[str] = Counter()
                for index in indexes:
                    trade_totals[labels[index]] += total_trades[index]
                total = sum(trade_totals.values())
                if total > 0 and max(trade_totals.values(), default=0.0) / total > float(max_trade_share):
                    failures.append(f"max_{name}_trade_share")
            if max_pnl_share is not None:
                totals: Counter[str] = Counter()
                for index in indexes:
                    totals[labels[index]] += abs_total_pnls[index]
                total = sum(totals.values())
                if total > 0 and max(totals.values(), default=0.0) / total > float(max_pnl_share):
                    failures.append(f"max_{name}_pnl_share")
        return failures

    for size in range(1, max_size + 1):
        for indexes in itertools.combinations(range(len(rows)), size):
            cap_reason = cap_failure(indexes)
            if cap_reason:
                infeasible_reasons[cap_reason] += 1
                continue
            required = diversity_failures(indexes)
            if required:
                for reason in required:
                    infeasible_reasons[reason] += 1
                continue
            concentration = share_failures(indexes)
            if concentration:
                for reason in concentration:
                    infeasible_reasons[reason] += 1
                continue
            selected_candidate_keys = {keys[index] for index in indexes}
            simulated = _simulate_selected_fast(
                prepared,
                selected_candidate_keys,
                initial_cash=initial_cash,
                backtest_allocation_fraction=backtest_allocation_fraction,
            )
            evaluated += 1
            if abs(float(simulated["max_drawdown_pct"])) > max_drawdown_pct:
                infeasible_reasons["max_drawdown_pct"] += 1
                continue
            if (
                min_average_daily_pnl is not None
                and float(simulated.get("average_daily_pnl") or 0.0) < float(min_average_daily_pnl)
            ):
                infeasible_reasons["min_average_daily_pnl"] += 1
                continue
            if objective == "test_pnl":
                score = sum(test_pnls[index] for index in indexes)
            elif objective == "total_pnl":
                score = float(simulated.get("net_pnl") or 0.0)
            else:
                drawdown = abs(float(simulated.get("max_drawdown") or 0.0)) or 1.0
                score = float(simulated.get("net_pnl") or 0.0) / drawdown
            if best_score is None or score > best_score:
                best_score = score
                best_keys = selected_candidate_keys
                best_simulation = simulated
    rejected = []
    for _, row in eligible.iterrows():
        if row["_optimizer_key"] not in best_keys:
            rejected.append(
                {
                    **row.drop(labels=["_optimizer_key"]).to_dict(),
                    "reject_reason": "not_selected_by_exact_optimizer",
                }
            )
    compact_best_simulation = dict(best_simulation)
    compact_best_simulation.pop("daily_rows", None)
    return best_keys, rejected, {
        "search_mode": "exact_subset",
        "evaluated_portfolios": evaluated,
        "infeasible_reason_counts": dict(infeasible_reasons),
        "best_objective_score": best_score,
        "best_simulation": compact_best_simulation,
    }


def _select_greedy(
    *,
    trades: pd.DataFrame,
    eligible: pd.DataFrame,
    initial_cash: float,
    backtest_allocation_fraction: float,
    max_candidates: int,
    max_per_symbol: int,
    max_per_regime: int,
    max_per_family: int,
    min_symbols: int,
    min_regimes: int,
    min_families: int,
    max_drawdown_pct: float,
    min_average_daily_pnl: float | None,
    max_symbol_trade_share: float | None,
    max_regime_trade_share: float | None,
    max_family_trade_share: float | None,
    max_candidate_trade_share: float | None,
    max_symbol_pnl_share: float | None,
    max_regime_pnl_share: float | None,
    max_family_pnl_share: float | None,
    max_candidate_pnl_share: float | None,
) -> tuple[set[tuple[str, str]], list[dict[str, Any]], dict[str, Any]]:
    selected_keys: set[tuple[str, str]] = set()
    symbol_counts: Counter[str] = Counter()
    regime_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    rejected = []
    for _, row in eligible.iterrows():
        key = row["_optimizer_key"]
        symbol = str(row["symbol"])
        regime = str(row["intended_regime"])
        family = str(row["family"])
        if len(selected_keys) >= max_candidates:
            break
        if symbol_counts[symbol] >= max_per_symbol:
            rejected.append({**row.drop(labels=["_optimizer_key"]).to_dict(), "reject_reason": "max_per_symbol"})
            continue
        if regime_counts[regime] >= max_per_regime:
            rejected.append({**row.drop(labels=["_optimizer_key"]).to_dict(), "reject_reason": "max_per_regime"})
            continue
        if family_counts[family] >= max_per_family:
            rejected.append({**row.drop(labels=["_optimizer_key"]).to_dict(), "reject_reason": "max_per_family"})
            continue
        candidate_selection = set(selected_keys)
        candidate_selection.add(key)
        candidate_rows = eligible[eligible["_optimizer_key"].isin(candidate_selection)]
        minimum_portfolio_size = max(2, min_symbols, min_regimes, min_families)
        if len(candidate_rows) >= minimum_portfolio_size:
            concentration = _concentration_failures(
                candidate_rows,
                max_symbol_trade_share=max_symbol_trade_share,
                max_regime_trade_share=max_regime_trade_share,
                max_family_trade_share=max_family_trade_share,
                max_candidate_trade_share=max_candidate_trade_share,
                max_symbol_pnl_share=max_symbol_pnl_share,
                max_regime_pnl_share=max_regime_pnl_share,
                max_family_pnl_share=max_family_pnl_share,
                max_candidate_pnl_share=max_candidate_pnl_share,
            )
            if concentration:
                rejected.append(
                    {
                        **row.drop(labels=["_optimizer_key"]).to_dict(),
                        "reject_reason": ";".join(concentration),
                    }
                )
                continue
        simulated = _simulate_selected(
            trades,
            candidate_selection,
            initial_cash=initial_cash,
            backtest_allocation_fraction=backtest_allocation_fraction,
        )
        if abs(float(simulated["max_drawdown_pct"])) > max_drawdown_pct:
            rejected.append({**row.drop(labels=["_optimizer_key"]).to_dict(), "reject_reason": "max_drawdown_pct"})
            continue
        if (
            min_average_daily_pnl is not None
            and len(candidate_rows) >= minimum_portfolio_size
            and float(simulated.get("average_daily_pnl") or 0.0) < float(min_average_daily_pnl)
        ):
            rejected.append(
                {**row.drop(labels=["_optimizer_key"]).to_dict(), "reject_reason": "min_average_daily_pnl"}
            )
            continue
        selected_keys.add(key)
        symbol_counts[symbol] += 1
        regime_counts[regime] += 1
        family_counts[family] += 1
    return selected_keys, rejected, {"search_mode": "greedy_incremental"}


def optimize_portfolio_candidates(
    *,
    portfolio_report_json: Path,
    scaled_trades_csv: Path,
    output_dir: Path,
    initial_cash: float,
    backtest_allocation_fraction: float,
    train_end_date: str | None,
    min_train_trades: int,
    min_test_trades: int,
    max_candidates: int,
    max_per_symbol: int,
    max_per_regime: int,
    max_per_family: int,
    min_symbols: int,
    min_regimes: int,
    min_families: int,
    max_drawdown_pct: float,
    min_average_daily_pnl: float | None = None,
    max_symbol_trade_share: float | None = None,
    max_regime_trade_share: float | None = None,
    max_family_trade_share: float | None = None,
    max_candidate_trade_share: float | None = None,
    max_symbol_pnl_share: float | None = None,
    max_regime_pnl_share: float | None = None,
    max_family_pnl_share: float | None = None,
    max_candidate_pnl_share: float | None = None,
    objective: str = "risk_adjusted",
    max_exact_candidates: int = 20,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    plan = _load_plan(portfolio_report_json)
    plan_by_key = {_key(row): row for row in plan}
    trades = pd.read_csv(scaled_trades_csv, low_memory=False)
    trades["_optimizer_key"] = trades.apply(_trade_key, axis=1)
    stats = _candidate_stats(
        trades,
        train_end_date=train_end_date,
        min_train_trades=min_train_trades,
        min_test_trades=min_test_trades,
    )
    stats["_objective"] = stats.apply(lambda row: _objective_value(row, objective), axis=1)
    eligible = stats[stats["eligible"].astype(bool)].sort_values("_objective", ascending=False)
    if len(eligible) <= max_exact_candidates:
        selected_keys, rejected, optimizer_metadata = _select_exact_subset(
            trades=trades,
            eligible=eligible,
            initial_cash=initial_cash,
            backtest_allocation_fraction=backtest_allocation_fraction,
            max_candidates=max_candidates,
            max_per_symbol=max_per_symbol,
            max_per_regime=max_per_regime,
            max_per_family=max_per_family,
            min_symbols=min_symbols,
            min_regimes=min_regimes,
            min_families=min_families,
            max_drawdown_pct=max_drawdown_pct,
            min_average_daily_pnl=min_average_daily_pnl,
            max_symbol_trade_share=max_symbol_trade_share,
            max_regime_trade_share=max_regime_trade_share,
            max_family_trade_share=max_family_trade_share,
            max_candidate_trade_share=max_candidate_trade_share,
            max_symbol_pnl_share=max_symbol_pnl_share,
            max_regime_pnl_share=max_regime_pnl_share,
            max_family_pnl_share=max_family_pnl_share,
            max_candidate_pnl_share=max_candidate_pnl_share,
            objective=objective,
        )
    else:
        selected_keys, rejected, optimizer_metadata = _select_greedy(
            trades=trades,
            eligible=eligible,
            initial_cash=initial_cash,
            backtest_allocation_fraction=backtest_allocation_fraction,
            max_candidates=max_candidates,
            max_per_symbol=max_per_symbol,
            max_per_regime=max_per_regime,
            max_per_family=max_per_family,
            min_symbols=min_symbols,
            min_regimes=min_regimes,
            min_families=min_families,
            max_drawdown_pct=max_drawdown_pct,
            min_average_daily_pnl=min_average_daily_pnl,
            max_symbol_trade_share=max_symbol_trade_share,
            max_regime_trade_share=max_regime_trade_share,
            max_family_trade_share=max_family_trade_share,
            max_candidate_trade_share=max_candidate_trade_share,
            max_symbol_pnl_share=max_symbol_pnl_share,
            max_regime_pnl_share=max_regime_pnl_share,
            max_family_pnl_share=max_family_pnl_share,
            max_candidate_pnl_share=max_candidate_pnl_share,
        )
    selected_stats = stats[stats["_optimizer_key"].isin(selected_keys)].copy()
    selected_stats = selected_stats.sort_values("_objective", ascending=False)
    symbol_counts = _counter_from_frame(selected_stats, "symbol")
    regime_counts = _counter_from_frame(selected_stats, "intended_regime")
    family_counts = _counter_from_frame(selected_stats, "family")
    final_simulation = _simulate_selected(
        trades,
        selected_keys,
        initial_cash=initial_cash,
        backtest_allocation_fraction=backtest_allocation_fraction,
    )
    failures = []
    if len(symbol_counts) < min_symbols:
        failures.append("min_symbols")
    if len(regime_counts) < min_regimes:
        failures.append("min_regimes")
    if len(family_counts) < min_families:
        failures.append("min_families")
    if (
        min_average_daily_pnl is not None
        and float(final_simulation.get("average_daily_pnl") or 0.0) < float(min_average_daily_pnl)
    ):
        failures.append("min_average_daily_pnl")
    concentration_failures = _concentration_failures(
        selected_stats,
        max_symbol_trade_share=max_symbol_trade_share,
        max_regime_trade_share=max_regime_trade_share,
        max_family_trade_share=max_family_trade_share,
        max_candidate_trade_share=max_candidate_trade_share,
        max_symbol_pnl_share=max_symbol_pnl_share,
        max_regime_pnl_share=max_regime_pnl_share,
        max_family_pnl_share=max_family_pnl_share,
        max_candidate_pnl_share=max_candidate_pnl_share,
    )
    failures.extend(reason for reason in concentration_failures if reason not in failures)
    concentration_metrics = _concentration_metrics(selected_stats)
    selected_plan = [
        dict(plan_by_key[key])
        for key in selected_stats["_optimizer_key"].tolist()
        if key in plan_by_key
    ]
    for row in selected_plan:
        weight = 1.0 / len(selected_plan) if selected_plan else 0.0
        row["source_research_only_weight_before_optimizer"] = row.get("research_only_weight")
        row["research_only_weight"] = round(weight, 8)
        row["research_only_dollars"] = round(weight * initial_cash, 2)
    optimized_report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_portfolio_report_json": str(portfolio_report_json),
        "source_scaled_trades_csv": str(scaled_trades_csv),
        "optimizer_constraints": {
            "train_end_date": train_end_date,
            "min_train_trades": min_train_trades,
            "min_test_trades": min_test_trades,
            "max_candidates": max_candidates,
            "max_per_symbol": max_per_symbol,
            "max_per_regime": max_per_regime,
            "max_per_family": max_per_family,
            "min_symbols": min_symbols,
            "min_regimes": min_regimes,
            "min_families": min_families,
            "max_drawdown_pct": max_drawdown_pct,
            "min_average_daily_pnl": min_average_daily_pnl,
            "max_symbol_trade_share": max_symbol_trade_share,
            "max_regime_trade_share": max_regime_trade_share,
            "max_family_trade_share": max_family_trade_share,
            "max_candidate_trade_share": max_candidate_trade_share,
            "max_symbol_pnl_share": max_symbol_pnl_share,
            "max_regime_pnl_share": max_regime_pnl_share,
            "max_family_pnl_share": max_family_pnl_share,
            "max_candidate_pnl_share": max_candidate_pnl_share,
            "objective": objective,
            "max_exact_candidates": max_exact_candidates,
        },
        "optimizer_metadata": optimizer_metadata,
        "selected_concentration": concentration_metrics,
        "capital_plan": selected_plan,
    }
    report_path = output_dir / "optimized_portfolio_report.json"
    report_path.write_text(json.dumps(optimized_report, indent=2, sort_keys=True), encoding="utf-8")
    selected_stats.drop(columns=["_optimizer_key"], errors="ignore").to_csv(
        output_dir / "selected_candidates.csv",
        index=False,
    )
    stats.drop(columns=["_optimizer_key"], errors="ignore").to_csv(
        output_dir / "candidate_stats.csv",
        index=False,
    )
    pd.DataFrame(rejected).to_csv(output_dir / "optimizer_rejections.csv", index=False)
    pd.DataFrame(final_simulation.pop("daily_rows", [])).to_csv(
        output_dir / "optimized_daily_curve.csv",
        index=False,
    )
    summary = {
        "status": "passed" if not failures else "failed",
        "failure_reasons": failures,
        "optimized_portfolio_report_json": str(report_path),
        "candidate_count": int(len(stats)),
        "eligible_candidate_count": int(len(eligible)),
        "selected_candidate_count": int(len(selected_plan)),
        "selected_symbol_counts": dict(symbol_counts),
        "selected_regime_counts": dict(regime_counts),
        "selected_family_counts": dict(family_counts),
        "selected_concentration": concentration_metrics,
        "optimizer_metadata": optimizer_metadata,
        "simulation": final_simulation,
    }
    (output_dir / "optimizer_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def main() -> None:
    args = parse_args()
    summary = optimize_portfolio_candidates(
        portfolio_report_json=Path(args.portfolio_report_json),
        scaled_trades_csv=Path(args.scaled_trades_csv),
        output_dir=Path(args.output_dir),
        initial_cash=args.initial_cash,
        backtest_allocation_fraction=args.backtest_allocation_fraction,
        train_end_date=args.train_end_date,
        min_train_trades=args.min_train_trades,
        min_test_trades=args.min_test_trades,
        max_candidates=args.max_candidates,
        max_per_symbol=args.max_per_symbol,
        max_per_regime=args.max_per_regime,
        max_per_family=args.max_per_family,
        min_symbols=args.min_symbols,
        min_regimes=args.min_regimes,
        min_families=args.min_families,
        max_drawdown_pct=args.max_drawdown_pct,
        min_average_daily_pnl=args.min_average_daily_pnl,
        max_symbol_trade_share=args.max_symbol_trade_share,
        max_regime_trade_share=args.max_regime_trade_share,
        max_family_trade_share=args.max_family_trade_share,
        max_candidate_trade_share=args.max_candidate_trade_share,
        max_symbol_pnl_share=args.max_symbol_pnl_share,
        max_regime_pnl_share=args.max_regime_pnl_share,
        max_family_pnl_share=args.max_family_pnl_share,
        max_candidate_pnl_share=args.max_candidate_pnl_share,
        objective=args.objective,
        max_exact_candidates=args.max_exact_candidates,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

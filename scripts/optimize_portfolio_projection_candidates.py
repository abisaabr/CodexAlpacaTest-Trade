from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
        "--objective",
        choices=["test_pnl", "total_pnl", "risk_adjusted"],
        default="risk_adjusted",
    )
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
    if "source_pnl_per_combo" in frame.columns:
        return "source_pnl_per_combo"
    if "source_option_pnl" in frame.columns:
        return "source_option_pnl"
    return "scaled_option_pnl"


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
    objective: str,
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
        simulated = _simulate_selected(
            trades,
            candidate_selection,
            initial_cash=initial_cash,
            backtest_allocation_fraction=backtest_allocation_fraction,
        )
        if abs(float(simulated["max_drawdown_pct"])) > max_drawdown_pct:
            rejected.append({**row.drop(labels=["_optimizer_key"]).to_dict(), "reject_reason": "max_drawdown_pct"})
            continue
        selected_keys.add(key)
        symbol_counts[symbol] += 1
        regime_counts[regime] += 1
        family_counts[family] += 1
    selected_stats = stats[stats["_optimizer_key"].isin(selected_keys)].copy()
    selected_stats = selected_stats.sort_values("_objective", ascending=False)
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
            "objective": objective,
        },
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
        objective=args.objective,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a research-only portfolio growth projection from a portfolio "
            "report and option-aware trade economics."
        )
    )
    parser.add_argument("--portfolio-report-json", required=True)
    parser.add_argument("--replay-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--target-equity", type=float, default=300_000.0)
    parser.add_argument("--backtest-allocation-fraction", type=float, default=0.05)
    parser.add_argument("--annual-trading-days", type=int, default=252)
    parser.add_argument("--projection-years", type=int, default=5)
    parser.add_argument("--bootstrap-runs", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=7)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _float(value: object, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _profile_name(path: Path) -> str:
    return path.parent.name


def _load_trade_economics(replay_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(replay_root.rglob("option_aware_trade_economics.csv")):
        frame = pd.read_csv(path)
        if frame.empty:
            continue
        frame["aggregate_profile"] = _profile_name(path)
        frame["source_file"] = str(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    trades = pd.concat(frames, ignore_index=True)
    for column in [
        "option_pnl",
        "entry_price_after_slippage",
        "exit_price_after_slippage",
        "quantity",
        "fees",
    ]:
        if column in trades.columns:
            trades[column] = pd.to_numeric(trades[column], errors="coerce")
    if "trade_date" not in trades.columns:
        raise ValueError("Trade economics files must include trade_date")
    trades["trade_date"] = pd.to_datetime(trades["trade_date"], errors="coerce").dt.date
    trades = trades.dropna(subset=["trade_date", "candidate_variant_id", "option_pnl"])
    dedupe_columns = [
        column
        for column in [
            "candidate_variant_id",
            "aggregate_profile",
            "symbol",
            "contract_symbol",
            "option_entry_time",
            "option_exit_time",
            "option_pnl",
            "quantity",
        ]
        if column in trades.columns
    ]
    if dedupe_columns:
        trades = trades.drop_duplicates(subset=dedupe_columns)
    return trades


def _capital_plan(portfolio_report: dict[str, Any]) -> list[dict[str, Any]]:
    plan = portfolio_report.get("capital_plan") or []
    if not isinstance(plan, list):
        return []
    return [row for row in plan if isinstance(row, dict)]


def _select_capital_plan_trades(
    *,
    trades: pd.DataFrame,
    capital_plan: list[dict[str, Any]],
    initial_cash: float,
    backtest_allocation_fraction: float,
) -> pd.DataFrame:
    selected_frames: list[pd.DataFrame] = []
    backtest_budget = initial_cash * backtest_allocation_fraction
    if backtest_budget <= 0:
        raise ValueError("Backtest allocation budget must be positive")
    if trades.empty or "candidate_variant_id" not in trades.columns:
        return pd.DataFrame()
    for row in capital_plan:
        base_id = str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or "")
        profile = str(row.get("aggregate_profile") or "")
        weight = _float(row.get("research_only_weight"))
        dollars = _float(row.get("research_only_dollars"), initial_cash * weight)
        if not base_id or weight <= 0:
            continue
        mask = trades["candidate_variant_id"].astype(str).eq(base_id)
        if profile and "aggregate_profile" in trades.columns:
            mask &= trades["aggregate_profile"].astype(str).eq(profile)
        frame = trades.loc[mask].copy()
        if frame.empty:
            continue
        frame["portfolio_candidate_variant_id"] = row.get("candidate_variant_id")
        frame["base_candidate_variant_id"] = base_id
        frame["portfolio_weight"] = weight
        frame["portfolio_allocated_dollars"] = dollars
        frame["capital_plan_family"] = row.get("family")
        frame["capital_plan_regime"] = row.get("intended_regime")
        frame["capital_plan_research_score"] = row.get("research_score")
        frame["static_scale_factor"] = dollars / backtest_budget
        selected_frames.append(frame)
    if not selected_frames:
        return pd.DataFrame()
    return pd.concat(selected_frames, ignore_index=True)


def _build_daily_equity(
    *,
    selected_trades: pd.DataFrame,
    initial_cash: float,
    backtest_allocation_fraction: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if selected_trades.empty:
        return pd.DataFrame(), pd.DataFrame()
    backtest_budget = initial_cash * backtest_allocation_fraction
    trades = selected_trades.copy().sort_values(["trade_date", "portfolio_candidate_variant_id"])
    trade_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    equity = initial_cash
    peak = initial_cash
    for trade_date, group in trades.groupby("trade_date", sort=True):
        start_equity = equity
        daily_pnl = 0.0
        for _, row in group.iterrows():
            weight = _float(row.get("portfolio_weight"))
            dynamic_budget = start_equity * weight
            scale = dynamic_budget / backtest_budget if backtest_budget > 0 else 0.0
            scaled_pnl = _float(row.get("option_pnl")) * scale
            daily_pnl += scaled_pnl
            trade_rows.append(
                {
                    "trade_date": str(trade_date),
                    "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
                    "base_candidate_variant_id": row.get("base_candidate_variant_id"),
                    "aggregate_profile": row.get("aggregate_profile"),
                    "symbol": row.get("symbol"),
                    "family": row.get("capital_plan_family") or row.get("family"),
                    "intended_regime": row.get("capital_plan_regime"),
                    "portfolio_weight": weight,
                    "source_option_pnl": round(_float(row.get("option_pnl")), 6),
                    "dynamic_scale_factor": round(scale, 8),
                    "scaled_option_pnl": round(scaled_pnl, 6),
                }
            )
        equity = max(start_equity + daily_pnl, 0.0)
        peak = max(peak, equity)
        drawdown = equity - peak
        daily_return = daily_pnl / start_equity if start_equity > 0 else -1.0
        daily_rows.append(
            {
                "trade_date": str(trade_date),
                "starting_equity": round(start_equity, 6),
                "daily_pnl": round(daily_pnl, 6),
                "daily_return": round(daily_return, 10),
                "ending_equity": round(equity, 6),
                "peak_equity": round(peak, 6),
                "drawdown": round(drawdown, 6),
                "drawdown_pct": round(drawdown / peak, 10) if peak > 0 else -1.0,
                "trade_count": int(len(group)),
            }
        )
    return pd.DataFrame(daily_rows), pd.DataFrame(trade_rows)


def _max_drawdown_from_equity(equity_values: np.ndarray) -> float:
    if equity_values.size == 0:
        return 0.0
    peaks = np.maximum.accumulate(equity_values)
    drawdowns = equity_values - peaks
    return float(drawdowns.min())


def _historical_metrics(
    *,
    daily_curve: pd.DataFrame,
    initial_cash: float,
    target_equity: float,
    annual_trading_days: int,
) -> dict[str, Any]:
    if daily_curve.empty:
        return {
            "status": "no_equity_curve",
            "reason": "No capital-plan trades matched replay trade economics.",
        }
    ending_equity = _float(daily_curve["ending_equity"].iloc[-1])
    days = int(len(daily_curve))
    years = days / annual_trading_days if annual_trading_days > 0 else 0.0
    total_return = ending_equity / initial_cash - 1.0 if initial_cash > 0 else 0.0
    cagr = (ending_equity / initial_cash) ** (1.0 / years) - 1.0 if years > 0 and ending_equity > 0 else 0.0
    daily_returns = pd.to_numeric(daily_curve["daily_return"], errors="coerce").fillna(0.0)
    vol = float(daily_returns.std(ddof=0) * math.sqrt(annual_trading_days))
    sharpe = float((daily_returns.mean() * annual_trading_days) / vol) if vol > 0 else None
    max_drawdown = _float(daily_curve["drawdown"].min())
    max_drawdown_pct = _float(daily_curve["drawdown_pct"].min())
    hit_rows = daily_curve[daily_curve["ending_equity"] >= target_equity]
    target_hit = not hit_rows.empty
    return {
        "status": "historical_compounded_curve_complete",
        "initial_cash": round(initial_cash, 2),
        "target_equity": round(target_equity, 2),
        "starting_date": str(daily_curve["trade_date"].iloc[0]),
        "ending_date": str(daily_curve["trade_date"].iloc[-1]),
        "trading_days": days,
        "ending_equity": round(ending_equity, 2),
        "total_return_pct": round(total_return * 100.0, 4),
        "cagr_pct": round(cagr * 100.0, 4),
        "annualized_volatility_pct": round(vol * 100.0, 4),
        "sharpe_like_ratio": round(sharpe, 4) if sharpe is not None else None,
        "max_drawdown": round(max_drawdown, 2),
        "max_drawdown_pct": round(max_drawdown_pct * 100.0, 4),
        "target_hit_in_historical_curve": target_hit,
        "first_target_hit_date": str(hit_rows["trade_date"].iloc[0]) if target_hit else None,
    }


def _bootstrap_projection(
    *,
    daily_curve: pd.DataFrame,
    initial_cash: float,
    target_equity: float,
    annual_trading_days: int,
    projection_years: int,
    bootstrap_runs: int,
    seed: int,
) -> dict[str, Any]:
    if daily_curve.empty:
        return {"status": "no_projection", "reason": "No daily returns available."}
    returns = pd.to_numeric(daily_curve["daily_return"], errors="coerce").fillna(0.0).to_numpy()
    if returns.size < 20:
        return {
            "status": "projection_directional_only",
            "reason": "Fewer than 20 daily return observations.",
        }
    rng = np.random.default_rng(seed)
    horizon_days = int(max(1, projection_years) * annual_trading_days)
    horizons = sorted(
        {
            annual_trading_days // 4,
            annual_trading_days // 2,
            annual_trading_days,
            2 * annual_trading_days,
            3 * annual_trading_days,
            horizon_days,
        }
    )
    endings_by_horizon: dict[int, list[float]] = {day: [] for day in horizons}
    target_hit_days: list[int] = []
    min_equities: list[float] = []
    max_drawdowns: list[float] = []
    max_drawdown_pcts: list[float] = []
    ruin_count = 0
    half_drawdown_count = 0
    for _ in range(bootstrap_runs):
        sampled = rng.choice(returns, size=horizon_days, replace=True)
        equity = initial_cash
        values = []
        first_hit: int | None = None
        for index, daily_return in enumerate(sampled, start=1):
            equity = max(equity * (1.0 + float(daily_return)), 0.0)
            values.append(equity)
            if first_hit is None and equity >= target_equity:
                first_hit = index
            if index in endings_by_horizon:
                endings_by_horizon[index].append(equity)
        values_array = np.array(values, dtype=float)
        if first_hit is not None:
            target_hit_days.append(first_hit)
        if values_array.size:
            min_equity = float(values_array.min())
            min_equities.append(min_equity)
            max_dd = _max_drawdown_from_equity(values_array)
            max_drawdowns.append(max_dd)
            peaks = np.maximum.accumulate(values_array)
            drawdown_pcts = np.divide(
                values_array - peaks,
                peaks,
                out=np.zeros_like(values_array),
                where=peaks > 0,
            )
            max_drawdown_pcts.append(float(drawdown_pcts.min()))
            if min_equity <= 0:
                ruin_count += 1
            if min_equity <= initial_cash * 0.5:
                half_drawdown_count += 1
    horizon_stats = []
    for day, values in endings_by_horizon.items():
        arr = np.array(values, dtype=float)
        if arr.size == 0:
            continue
        horizon_stats.append(
            {
                "trading_days": day,
                "approx_years": round(day / annual_trading_days, 4),
                "p10_ending_equity": round(float(np.percentile(arr, 10)), 2),
                "p25_ending_equity": round(float(np.percentile(arr, 25)), 2),
                "median_ending_equity": round(float(np.percentile(arr, 50)), 2),
                "p75_ending_equity": round(float(np.percentile(arr, 75)), 2),
                "p90_ending_equity": round(float(np.percentile(arr, 90)), 2),
                "target_hit_probability_pct": round(float(np.mean(arr >= target_equity)) * 100.0, 4),
            }
        )
    target_hit_probability = len(target_hit_days) / bootstrap_runs if bootstrap_runs else 0.0
    return {
        "status": "bootstrap_projection_complete",
        "bootstrap_runs": bootstrap_runs,
        "projection_years": projection_years,
        "annual_trading_days": annual_trading_days,
        "target_hit_probability_pct": round(target_hit_probability * 100.0, 4),
        "median_trading_days_to_target_if_hit": (
            round(float(np.median(target_hit_days)), 2) if target_hit_days else None
        ),
        "median_years_to_target_if_hit": (
            round(float(np.median(target_hit_days)) / annual_trading_days, 4)
            if target_hit_days
            else None
        ),
        "probability_equity_drawdown_below_50pct_start_pct": round(
            half_drawdown_count / bootstrap_runs * 100.0, 4
        )
        if bootstrap_runs
        else None,
        "risk_of_ruin_pct": round(ruin_count / bootstrap_runs * 100.0, 4)
        if bootstrap_runs
        else None,
        "median_worst_drawdown": round(float(np.median(max_drawdowns)), 2)
        if max_drawdowns
        else None,
        "median_worst_drawdown_pct": round(float(np.median(max_drawdown_pcts)) * 100.0, 4)
        if max_drawdown_pcts
        else None,
        "p10_min_equity": round(float(np.percentile(min_equities, 10)), 2)
        if min_equities
        else None,
        "horizon_stats": horizon_stats,
    }


def _evidence_grade(
    *,
    daily_curve: pd.DataFrame,
    historical: dict[str, Any],
    projection: dict[str, Any],
    capital_plan: list[dict[str, Any]],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not capital_plan:
        blockers.append("no_capital_plan")
    if daily_curve.empty:
        blockers.append("no_matched_trade_economics")
    elif len(daily_curve) < 252:
        warnings.append("less_than_252_trading_days")
    if _float(historical.get("max_drawdown_pct")) <= -50:
        warnings.append("historical_drawdown_exceeds_50pct")
    if _float(historical.get("annualized_volatility_pct")) >= 80:
        warnings.append("annualized_volatility_above_80pct")
    if _float(historical.get("cagr_pct")) >= 150:
        warnings.append("high_cagr_extrapolation_requires_walk_forward_confirmation")
    if _float(projection.get("target_hit_probability_pct")) < 50:
        warnings.append("target_hit_probability_below_50pct")
    if _float(projection.get("probability_equity_drawdown_below_50pct_start_pct")) >= 5:
        warnings.append("bootstrap_probability_of_50pct_drawdown_above_5pct")
    if _float(projection.get("risk_of_ruin_pct")) > 0:
        blockers.append("nonzero_bootstrap_risk_of_ruin")
    if blockers:
        grade = "not_institutional_expectation"
    elif warnings:
        grade = "directional_expectation_only"
    else:
        grade = "research_projection_candidate"
    return {
        "grade": grade,
        "blockers": blockers,
        "warnings": warnings,
        "interpretation": (
            "Use this as a governed research projection, not a promise of future returns. "
            "Paper validation and broker-audited live-like slippage evidence are still required."
        ),
    }


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    historical = packet["historical_metrics"]
    projection = packet["bootstrap_projection"]
    grade = packet["evidence_grade"]
    lines = [
        "# Portfolio Growth Projection",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Scope: `{packet['scope']}`",
        f"- Broker facing: `{str(packet['broker_facing']).lower()}`",
        f"- Live manifest effect: `{packet['live_manifest_effect']}`",
        f"- Risk policy effect: `{packet['risk_policy_effect']}`",
        f"- Initial cash: `${packet['initial_cash']}`",
        f"- Target equity: `${packet['target_equity']}`",
        f"- Evidence grade: `{grade['grade']}`",
        "",
        "## Historical Compounded Curve",
        "",
    ]
    for key in [
        "starting_date",
        "ending_date",
        "trading_days",
        "ending_equity",
        "total_return_pct",
        "cagr_pct",
        "annualized_volatility_pct",
        "sharpe_like_ratio",
        "max_drawdown",
        "max_drawdown_pct",
        "target_hit_in_historical_curve",
        "first_target_hit_date",
    ]:
        lines.append(f"- `{key}`: `{historical.get(key)}`")
    lines.extend(["", "## Bootstrap Projection", ""])
    for key in [
        "status",
        "bootstrap_runs",
        "projection_years",
        "target_hit_probability_pct",
        "median_trading_days_to_target_if_hit",
        "median_years_to_target_if_hit",
        "probability_equity_drawdown_below_50pct_start_pct",
        "risk_of_ruin_pct",
        "median_worst_drawdown",
        "median_worst_drawdown_pct",
        "p10_min_equity",
    ]:
        lines.append(f"- `{key}`: `{projection.get(key)}`")
    lines.extend(["", "## Horizon Stats", ""])
    horizon_stats = projection.get("horizon_stats") or []
    if not horizon_stats:
        lines.append("- No horizon stats available.")
    else:
        lines.append(
            "| Approx Years | P10 Ending | Median Ending | P90 Ending | Target Hit Probability |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        for row in horizon_stats:
            lines.append(
                f"| `{row['approx_years']}` | `${row['p10_ending_equity']}` | "
                f"`${row['median_ending_equity']}` | `${row['p90_ending_equity']}` | "
                f"`{row['target_hit_probability_pct']}%` |"
            )
    lines.extend(["", "## Evidence Warnings", ""])
    if not grade["blockers"] and not grade["warnings"]:
        lines.append("- No projection-grade blockers or warnings were generated.")
    for item in grade["blockers"]:
        lines.append(f"- Blocker: `{item}`")
    for item in grade["warnings"]:
        lines.append(f"- Warning: `{item}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            grade["interpretation"],
            "",
            "Hard rule: this packet is research-only. It does not authorize trading, paper orders, live-manifest edits, risk-policy edits, or lowering promotion gates.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_growth_projection(
    *,
    portfolio_report_json: Path,
    replay_root: Path,
    output_dir: Path,
    initial_cash: float,
    target_equity: float,
    backtest_allocation_fraction: float,
    annual_trading_days: int,
    projection_years: int,
    bootstrap_runs: int,
    seed: int,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    portfolio_report = _load_json(portfolio_report_json)
    capital_plan = _capital_plan(portfolio_report)
    trades = _load_trade_economics(replay_root)
    selected_trades = _select_capital_plan_trades(
        trades=trades,
        capital_plan=capital_plan,
        initial_cash=initial_cash,
        backtest_allocation_fraction=backtest_allocation_fraction,
    )
    daily_curve, scaled_trades = _build_daily_equity(
        selected_trades=selected_trades,
        initial_cash=initial_cash,
        backtest_allocation_fraction=backtest_allocation_fraction,
    )
    historical = _historical_metrics(
        daily_curve=daily_curve,
        initial_cash=initial_cash,
        target_equity=target_equity,
        annual_trading_days=annual_trading_days,
    )
    projection = _bootstrap_projection(
        daily_curve=daily_curve,
        initial_cash=initial_cash,
        target_equity=target_equity,
        annual_trading_days=annual_trading_days,
        projection_years=projection_years,
        bootstrap_runs=bootstrap_runs,
        seed=seed,
    )
    evidence_grade = _evidence_grade(
        daily_curve=daily_curve,
        historical=historical,
        projection=projection,
        capital_plan=capital_plan,
    )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "portfolio_growth_projection_complete",
        "scope": "research_only_compounded_portfolio_projection",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "portfolio_report_json": str(portfolio_report_json),
        "replay_root": str(replay_root),
        "initial_cash": initial_cash,
        "target_equity": target_equity,
        "backtest_allocation_fraction": backtest_allocation_fraction,
        "capital_plan_count": len(capital_plan),
        "matched_trade_count": int(len(selected_trades)),
        "matched_daily_count": int(len(daily_curve)),
        "historical_metrics": historical,
        "bootstrap_projection": projection,
        "evidence_grade": evidence_grade,
        "capital_plan": capital_plan,
    }
    (output_dir / "portfolio_growth_projection.json").write_text(
        json.dumps(packet, indent=2, sort_keys=True), encoding="utf-8"
    )
    if not daily_curve.empty:
        daily_curve.to_csv(output_dir / "portfolio_growth_equity_curve.csv", index=False)
    if not scaled_trades.empty:
        scaled_trades.to_csv(output_dir / "portfolio_growth_scaled_trades.csv", index=False)
    _write_markdown(output_dir / "portfolio_growth_projection.md", packet)
    return packet


def main() -> None:
    args = parse_args()
    packet = build_growth_projection(
        portfolio_report_json=Path(args.portfolio_report_json),
        replay_root=Path(args.replay_root),
        output_dir=Path(args.output_dir),
        initial_cash=args.initial_cash,
        target_equity=args.target_equity,
        backtest_allocation_fraction=args.backtest_allocation_fraction,
        annual_trading_days=args.annual_trading_days,
        projection_years=args.projection_years,
        bootstrap_runs=args.bootstrap_runs,
        seed=args.seed,
    )
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

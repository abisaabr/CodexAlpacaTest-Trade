from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from scripts.optimize_portfolio_projection_candidates import optimize_portfolio_candidates


TIER_CONFIGS: dict[str, dict[str, Any]] = {
    "unconstrained_max_profit": {
        "max_candidates": 50,
        "max_per_symbol": 50,
        "max_per_regime": 50,
        "max_per_family": 50,
        "min_symbols": 1,
        "min_regimes": 1,
        "min_families": 1,
        "max_drawdown_pct": 100.0,
        "objective": "total_pnl",
        "max_exact_candidates": 18,
    },
    "current_paper_risk": {
        "max_candidates": 20,
        "max_per_symbol": 4,
        "max_per_regime": 10,
        "max_per_family": 10,
        "min_symbols": 3,
        "min_regimes": 2,
        "min_families": 2,
        "max_drawdown_pct": 35.0,
        "objective": "risk_adjusted",
        "max_symbol_trade_share": 0.70,
        "max_family_trade_share": 0.85,
        "max_symbol_pnl_share": 0.85,
        "max_family_pnl_share": 0.85,
    },
    "strict_institutional": {
        "max_candidates": 12,
        "max_per_symbol": 2,
        "max_per_regime": 8,
        "max_per_family": 4,
        "min_symbols": 3,
        "min_regimes": 2,
        "min_families": 2,
        "max_drawdown_pct": 22.0,
        "min_average_daily_pnl": 50.0,
        "objective": "risk_adjusted",
        "max_symbol_trade_share": 0.60,
        "max_regime_trade_share": 0.80,
        "max_family_trade_share": 0.85,
        "max_candidate_trade_share": 0.45,
        "max_symbol_pnl_share": 0.80,
        "max_regime_pnl_share": 0.80,
        "max_family_pnl_share": 0.85,
        "max_candidate_pnl_share": 0.50,
    },
    "target_200_day_relaxed_controlled": {
        "max_candidates": 20,
        "max_per_symbol": 4,
        "max_per_regime": 12,
        "max_per_family": 8,
        "min_symbols": 3,
        "min_regimes": 2,
        "min_families": 2,
        "max_drawdown_pct": 35.0,
        "min_average_daily_pnl": 200.0,
        "objective": "risk_adjusted",
        "max_symbol_trade_share": 0.70,
        "max_regime_trade_share": 0.85,
        "max_family_trade_share": 0.85,
        "max_candidate_trade_share": 0.55,
        "max_symbol_pnl_share": 0.85,
        "max_regime_pnl_share": 0.90,
        "max_family_pnl_share": 0.85,
        "max_candidate_pnl_share": 0.60,
    },
    "drawdown_minimized_benchmark": {
        "max_candidates": 8,
        "max_per_symbol": 2,
        "max_per_regime": 4,
        "max_per_family": 3,
        "min_symbols": 2,
        "min_regimes": 1,
        "min_families": 2,
        "max_drawdown_pct": 15.0,
        "objective": "risk_adjusted",
        "max_symbol_trade_share": 0.60,
        "max_family_trade_share": 0.75,
        "max_candidate_trade_share": 0.40,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a reproducible constrained optimizer tier sweep from projector "
            "scaled trades. This is broker-free and does not edit paper manifests."
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
    parser.add_argument(
        "--tier",
        action="append",
        choices=sorted(TIER_CONFIGS),
        default=[],
        help="Tier to run. May be repeated. Default runs all tiers.",
    )
    return parser.parse_args()


def _compact_simulation(summary: dict[str, Any]) -> dict[str, Any]:
    simulation = dict(summary.get("simulation") or {})
    simulation.pop("daily_rows", None)
    return simulation


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Portfolio Optimizer Tier Sweep",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Source report: `{summary['source_portfolio_report_json']}`",
        f"- Source trades: `{summary['source_scaled_trades_csv']}`",
        "",
        "| Tier | Status | Selected | Avg Daily PnL | Net PnL | Max DD % | Failures |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for tier in summary["tiers"]:
        simulation = tier.get("simulation") or {}
        failures = ",".join(tier.get("failure_reasons") or [])
        lines.append(
            "| "
            f"{tier['tier']} | "
            f"{tier['status']} | "
            f"{tier['selected_candidate_count']} | "
            f"{simulation.get('average_daily_pnl', 0.0)} | "
            f"{simulation.get('net_pnl', 0.0)} | "
            f"{simulation.get('max_drawdown_pct', 0.0)} | "
            f"{failures} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_optimizer_tier_sweep(
    *,
    portfolio_report_json: Path,
    scaled_trades_csv: Path,
    output_dir: Path,
    initial_cash: float = 25_000.0,
    backtest_allocation_fraction: float = 0.05,
    train_end_date: str | None = None,
    min_train_trades: int = 5,
    min_test_trades: int = 5,
    tiers: list[str] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_tiers = tiers or list(TIER_CONFIGS)
    tier_rows: list[dict[str, Any]] = []
    for tier in selected_tiers:
        config = dict(TIER_CONFIGS[tier])
        tier_output = output_dir / tier
        summary = optimize_portfolio_candidates(
            portfolio_report_json=portfolio_report_json,
            scaled_trades_csv=scaled_trades_csv,
            output_dir=tier_output,
            initial_cash=initial_cash,
            backtest_allocation_fraction=backtest_allocation_fraction,
            train_end_date=train_end_date,
            min_train_trades=min_train_trades,
            min_test_trades=min_test_trades,
            max_candidates=int(config["max_candidates"]),
            max_per_symbol=int(config["max_per_symbol"]),
            max_per_regime=int(config["max_per_regime"]),
            max_per_family=int(config["max_per_family"]),
            min_symbols=int(config["min_symbols"]),
            min_regimes=int(config["min_regimes"]),
            min_families=int(config["min_families"]),
            max_drawdown_pct=float(config["max_drawdown_pct"]),
            min_average_daily_pnl=config.get("min_average_daily_pnl"),
            max_symbol_trade_share=config.get("max_symbol_trade_share"),
            max_regime_trade_share=config.get("max_regime_trade_share"),
            max_family_trade_share=config.get("max_family_trade_share"),
            max_candidate_trade_share=config.get("max_candidate_trade_share"),
            max_symbol_pnl_share=config.get("max_symbol_pnl_share"),
            max_regime_pnl_share=config.get("max_regime_pnl_share"),
            max_family_pnl_share=config.get("max_family_pnl_share"),
            max_candidate_pnl_share=config.get("max_candidate_pnl_share"),
            objective=str(config["objective"]),
            max_exact_candidates=int(config.get("max_exact_candidates", 20)),
        )
        tier_rows.append(
            {
                "tier": tier,
                "status": summary.get("status"),
                "failure_reasons": summary.get("failure_reasons") or [],
                "selected_candidate_count": summary.get("selected_candidate_count", 0),
                "eligible_candidate_count": summary.get("eligible_candidate_count", 0),
                "candidate_count": summary.get("candidate_count", 0),
                "simulation": _compact_simulation(summary),
                "optimizer_summary_json": str(tier_output / "optimizer_summary.json"),
                "optimized_portfolio_report_json": str(tier_output / "optimized_portfolio_report.json"),
                "constraints": config,
            }
        )
    passed = [row for row in tier_rows if row.get("status") == "passed"]
    best_by_average_daily = sorted(
        passed,
        key=lambda row: float((row.get("simulation") or {}).get("average_daily_pnl") or 0.0),
        reverse=True,
    )
    summary = {
        "status": "optimizer_tier_sweep_complete",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_portfolio_report_json": str(portfolio_report_json),
        "source_scaled_trades_csv": str(scaled_trades_csv),
        "initial_cash": initial_cash,
        "backtest_allocation_fraction": backtest_allocation_fraction,
        "train_end_date": train_end_date,
        "min_train_trades": min_train_trades,
        "min_test_trades": min_test_trades,
        "tier_count": len(tier_rows),
        "passed_tier_count": len(passed),
        "best_passed_tier_by_average_daily_pnl": best_by_average_daily[0]["tier"] if best_by_average_daily else None,
        "tiers": tier_rows,
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }
    summary_json = output_dir / "optimizer_tier_sweep_summary.json"
    summary_md = output_dir / "optimizer_tier_sweep_summary.md"
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(summary_md, summary)
    return summary


def main() -> None:
    args = parse_args()
    summary = run_optimizer_tier_sweep(
        portfolio_report_json=Path(args.portfolio_report_json),
        scaled_trades_csv=Path(args.scaled_trades_csv),
        output_dir=Path(args.output_dir),
        initial_cash=args.initial_cash,
        backtest_allocation_fraction=args.backtest_allocation_fraction,
        train_end_date=args.train_end_date,
        min_train_trades=args.min_train_trades,
        min_test_trades=args.min_test_trades,
        tiers=list(args.tier) or None,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

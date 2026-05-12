from __future__ import annotations

import argparse
from collections import Counter
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_PRODUCTION_RISK_CONFIG = REPO_ROOT / "config" / "risk_controls" / "multi_ticker_portfolio.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a research-only portfolio growth projection from a portfolio "
            "report and option-aware trade economics."
        )
    )
    parser.add_argument("--portfolio-report-json", required=True)
    parser.add_argument(
        "--additional-portfolio-report-json",
        action="append",
        default=[],
        help=(
            "Additional portfolio report, promotion packet, or rollup JSON carrying "
            "a capital_plan. May be repeated for combined portfolio projections."
        ),
    )
    parser.add_argument("--replay-root", required=True)
    parser.add_argument(
        "--additional-replay-root",
        action="append",
        default=[],
        help=(
            "Additional replay root containing option_aware_trade_economics.csv files. "
            "May be repeated for combined portfolio projections."
        ),
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--target-equity", type=float, default=300_000.0)
    parser.add_argument("--backtest-allocation-fraction", type=float, default=0.05)
    parser.add_argument(
        "--max-symbol-weight",
        type=float,
        default=None,
        help=(
            "Optional cap applied after merging capital plans. Candidate weights are "
            "rescaled within each symbol and research_only_dollars are recomputed."
        ),
    )
    parser.add_argument(
        "--risk-simulation-mode",
        choices=["capital_plan", "production_runtime"],
        default="capital_plan",
        help=(
            "capital_plan preserves the historical projection behavior. production_runtime "
            "replays option entries/exits through runtime-style risk gates instead of using "
            "a portfolio-level symbol cap as a proxy."
        ),
    )
    parser.add_argument(
        "--production-risk-config-yaml",
        default=str(DEFAULT_PRODUCTION_RISK_CONFIG),
        help=(
            "Risk-control YAML used when --risk-simulation-mode production_runtime is active. "
            "The file may be a standalone risk config or a paper portfolio config with a risk section."
        ),
    )
    parser.add_argument(
        "--production-strategy-manifest-yaml",
        action="append",
        default=[],
        help=(
            "Optional strategy or promotion manifest YAML carrying risk_fraction and max_contracts. "
            "May be repeated. When absent or unmatched, production defaults are used."
        ),
    )
    parser.add_argument("--production-default-risk-fraction", type=float, default=0.05)
    parser.add_argument("--production-default-max-contracts", type=int, default=6)
    parser.add_argument(
        "--production-enforce-broker-equity-floor",
        action="store_true",
        help=(
            "When set, enforce broker_min_equity_to_trade using the simulated sleeve equity. "
            "Keep disabled for sleeve-only projections where broker account equity is external."
        ),
    )
    parser.add_argument("--annual-trading-days", type=int, default=252)
    parser.add_argument("--projection-years", type=int, default=5)
    parser.add_argument("--bootstrap-runs", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--calendar-csv",
        default=None,
        help=(
            "Optional full-period trading calendar CSV. When provided, the equity "
            "curve carries cash through calendar days with no selected strategy trades."
        ),
    )
    parser.add_argument(
        "--calendar-date-column",
        default="trade_date",
        help="Date column in --calendar-csv.",
    )
    parser.add_argument(
        "--optimization-train-end-date",
        default=None,
        help=(
            "Optional YYYY-MM-DD split date. When set, emit train/test candidate "
            "stability and a recommended train-positive/test-positive subset."
        ),
    )
    parser.add_argument("--optimization-min-train-trades", type=int, default=5)
    parser.add_argument("--optimization-min-test-trades", type=int, default=5)
    parser.add_argument(
        "--optimization-max-per-symbol-regime",
        type=int,
        default=None,
        help=(
            "Optional cap for the train/test recommended subset. Candidates are "
            "ranked by train PnL within each symbol/regime bucket."
        ),
    )
    parser.add_argument("--stress-max-entry-spread-pct", type=float, default=None)
    parser.add_argument("--stress-max-exit-spread-pct", type=float, default=None)
    parser.add_argument("--stress-max-entry-quote-age-seconds", type=float, default=None)
    parser.add_argument("--stress-max-exit-quote-age-seconds", type=float, default=None)
    parser.add_argument(
        "--market-quality-cost-model-enabled",
        action="store_true",
        help=(
            "Subtract an execution-quality cost from option PnL using observed or "
            "proxy bid/ask spread, quote-age, and trade-print evidence."
        ),
    )
    parser.add_argument("--market-quality-no-bid-ask-spread-pct", type=float, default=0.12)
    parser.add_argument("--market-quality-zero-print-spread-pct", type=float, default=0.08)
    parser.add_argument("--market-quality-unknown-spread-pct", type=float, default=0.10)
    parser.add_argument("--market-quality-max-spread-pct", type=float, default=0.50)
    parser.add_argument("--market-quality-quote-age-grace-seconds", type=float, default=5.0)
    parser.add_argument("--market-quality-quote-age-spread-pct-per-minute", type=float, default=0.02)
    parser.add_argument(
        "--fill-model-enabled",
        action="store_true",
        help="Annotate rows with a simple market-quality fill-probability estimate.",
    )
    parser.add_argument(
        "--min-fill-probability",
        type=float,
        default=None,
        help="Reject rows with projected_fill_probability below this threshold.",
    )
    parser.add_argument(
        "--unknown-fill-probability",
        type=float,
        default=1.0,
        help="Fill probability assigned when no spread, age, or trade-print inputs are present.",
    )
    parser.add_argument(
        "--fill-model-haircut-positive-pnl",
        action="store_true",
        help=(
            "Conservatively multiply positive option PnL by projected_fill_probability "
            "before equity, train/test, and optimizer calculations. Losses are left "
            "unreduced so low-quality quote inputs cannot make losing trades look safer."
        ),
    )
    parser.add_argument("--diversification-min-symbols", type=int, default=None)
    parser.add_argument("--diversification-min-regimes", type=int, default=None)
    parser.add_argument("--diversification-min-families", type=int, default=None)
    parser.add_argument("--diversification-max-symbol-trade-share", type=float, default=None)
    parser.add_argument("--diversification-max-family-trade-share", type=float, default=None)
    parser.add_argument("--diversification-max-regime-trade-share", type=float, default=None)
    parser.add_argument("--diversification-max-symbol-pnl-share", type=float, default=None)
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


def _int(value: object, default: int = 0) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return default
    return parsed


def _profile_name(path: Path) -> str:
    return path.parent.name


def _capital_plan_trade_filters(
    capital_plan: list[dict[str, Any]],
) -> dict[str, set[str]] | None:
    if not capital_plan:
        return None
    filters: dict[str, set[str]] = {}
    for row in capital_plan:
        profile = str(row.get("aggregate_profile") or "")
        candidate_id = str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or "")
        candidate_id = candidate_id.split("__profile_", 1)[0]
        if not profile or not candidate_id:
            return None
        filters.setdefault(profile, set()).add(candidate_id)
    return filters


def _load_trade_economics(
    replay_root: Path,
    *,
    candidate_filters: dict[str, set[str]] | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(replay_root.rglob("option_aware_trade_economics.csv")):
        profile = _profile_name(path)
        wanted_candidates = candidate_filters.get(profile) if candidate_filters else None
        if candidate_filters is not None and not wanted_candidates:
            continue
        try:
            frame = pd.read_csv(path, low_memory=False)
        except pd.errors.EmptyDataError:
            continue
        if frame.empty:
            continue
        if wanted_candidates is not None:
            if "candidate_variant_id" not in frame.columns:
                continue
            frame = frame[frame["candidate_variant_id"].astype(str).isin(wanted_candidates)].copy()
            if frame.empty:
                continue
        frame["aggregate_profile"] = profile
        frame["source_file"] = str(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    trades = pd.concat(frames, ignore_index=True)
    for column in [
        "option_pnl",
        "entry_price_after_slippage",
        "exit_price_after_slippage",
        "entry_debit_per_unit",
        "risk_per_unit",
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


def _load_trade_economics_roots(
    replay_roots: list[Path],
    *,
    candidate_filters: dict[str, set[str]] | None = None,
) -> pd.DataFrame:
    frames = []
    for replay_root in replay_roots:
        frame = _load_trade_economics(replay_root, candidate_filters=candidate_filters)
        if frame.empty:
            continue
        frame["replay_root"] = str(replay_root)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    trades = pd.concat(frames, ignore_index=True)
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


def _load_projection_calendar(
    *,
    calendar_csv: Path | None,
    date_column: str,
) -> pd.DataFrame:
    if calendar_csv is None:
        return pd.DataFrame()
    if not calendar_csv.exists():
        raise FileNotFoundError(f"Calendar CSV not found: {calendar_csv}")
    calendar = pd.read_csv(calendar_csv)
    if date_column not in calendar.columns:
        raise ValueError(f"Calendar CSV must include date column {date_column!r}")
    calendar = calendar.copy()
    calendar["trade_date"] = pd.to_datetime(calendar[date_column], errors="coerce").dt.date
    calendar = calendar.dropna(subset=["trade_date"]).drop_duplicates(subset=["trade_date"])
    calendar = calendar.sort_values("trade_date").reset_index(drop=True)
    regime_column = next(
        (
            column
            for column in [
                "market_regime",
                "calendar_regime",
                "regime",
                "intended_regime",
            ]
            if column in calendar.columns
        ),
        None,
    )
    if regime_column:
        calendar["calendar_regime"] = calendar[regime_column].fillna("unknown").astype(str)
    else:
        calendar["calendar_regime"] = "unknown"
    return calendar[["trade_date", "calendar_regime"]]


def _capital_plan(
    portfolio_report: dict[str, Any], *, source_path: Path | None = None
) -> list[dict[str, Any]]:
    plan = portfolio_report.get("capital_plan") or []
    if not isinstance(plan, list):
        return []
    rows = []
    for row in plan:
        if not isinstance(row, dict):
            continue
        normalized = dict(row)
        if source_path is not None:
            normalized["source_portfolio_report_json"] = str(source_path)
        rows.append(normalized)
    return rows


def _load_yaml_object(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML object at {path}")
    return payload


def _production_risk_defaults() -> dict[str, Any]:
    return {
        "sleeve_starting_equity": 25_000.0,
        "max_open_risk_fraction": 0.15,
        "daily_loss_gate_pct": None,
        "delever_drawdown_pct": 8.0,
        "delever_risk_scale": 0.5,
        "max_open_positions": 10,
        "max_positions_per_regime": 10,
        "max_positions_per_symbol": 3,
        "entry_cluster_window_minutes": 15,
        "max_positions_per_regime_window": 3,
        "max_positions_per_bucket_regime_window": 2,
        "max_open_risk_fraction_per_symbol": 0.05,
        "regime_risk_scales": {},
        "bucket_caps": [
            {
                "name": "index_beta",
                "symbols": ["QQQ", "SPY", "IWM"],
                "max_open_risk_fraction": 0.08,
            },
            {
                "name": "growth_tech",
                "symbols": ["NVDA", "TSLA", "MSFT", "AMZN", "ORCL", "SHOP", "CRM", "PLTR", "ARKK"],
                "max_open_risk_fraction": 0.09,
            },
            {
                "name": "metals_energy",
                "symbols": ["GLD", "GDX", "SLV", "XLE", "XOM"],
                "max_open_risk_fraction": 0.08,
            },
            {
                "name": "financials",
                "symbols": ["BAC", "JPM", "SCHW"],
                "max_open_risk_fraction": 0.06,
            },
        ],
        "min_required_buying_power": 7_500.0,
        "broker_min_equity_to_trade": 26_000.0,
        "broker_equity_emergency_stop": 25_500.0,
    }


def _normalize_bucket_caps(items: object) -> list[dict[str, Any]]:
    buckets: list[dict[str, Any]] = []
    if not isinstance(items, list):
        return buckets
    for item in items:
        if not isinstance(item, dict):
            continue
        symbols = item.get("symbols") or []
        if isinstance(symbols, str):
            symbols = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]
        buckets.append(
            {
                "name": str(item.get("name") or "unnamed_bucket"),
                "symbols": [str(symbol).upper() for symbol in symbols],
                "max_open_risk_fraction": _float(item.get("max_open_risk_fraction")),
            }
        )
    return buckets


def _load_production_risk_config(path: Path | None) -> dict[str, Any]:
    config = _production_risk_defaults()
    source = "built_in_defaults"
    if path is not None and path.exists():
        payload = _load_yaml_object(path)
        risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else payload
        config.update({key: value for key, value in risk.items() if key in config})
        source = str(path)
    config["bucket_caps"] = _normalize_bucket_caps(config.get("bucket_caps"))
    config["_source"] = source
    return config


def _strategy_identity_keys(row: dict[str, Any]) -> list[str]:
    keys = []
    for key in [
        "candidate_variant_id",
        "base_candidate_variant_id",
        "source_strategy_id",
        "strategy_id",
        "name",
    ]:
        value = row.get(key)
        if value is None:
            continue
        text = str(value)
        if text and text not in keys:
            keys.append(text)
        base_text = text.split("__profile_", 1)[0]
        if base_text and base_text not in keys:
            keys.append(base_text)
    return keys


def _load_strategy_sizing(paths: list[Path]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    sizing_by_key: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    for path in paths:
        if not path.exists():
            source_counts[str(path)] = 0
            continue
        payload = _load_yaml_object(path)
        strategies = payload.get("strategies") or []
        if not isinstance(strategies, list):
            source_counts[str(path)] = 0
            continue
        source_counts[str(path)] = len(strategies)
        for strategy in strategies:
            if not isinstance(strategy, dict):
                continue
            sizing = {
                "risk_fraction": _float(strategy.get("risk_fraction")),
                "max_contracts": _int(strategy.get("max_contracts")),
                "source_strategy_manifest_yaml": str(path),
                "source_strategy_name": strategy.get("name"),
            }
            if sizing["risk_fraction"] <= 0 or sizing["max_contracts"] <= 0:
                continue
            for key in _strategy_identity_keys(strategy):
                sizing_by_key[key] = sizing
    return sizing_by_key, {
        "source_strategy_manifest_counts": source_counts,
        "sizing_key_count": len(sizing_by_key),
    }


def _resolve_strategy_sizing(
    *,
    row: dict[str, Any],
    sizing_by_key: dict[str, dict[str, Any]],
    default_risk_fraction: float,
    default_max_contracts: int,
) -> dict[str, Any]:
    for key in _strategy_identity_keys(row):
        sizing = sizing_by_key.get(key)
        if sizing:
            return {
                "risk_fraction": sizing["risk_fraction"],
                "max_contracts": sizing["max_contracts"],
                "strategy_sizing_mode": "strategy_manifest",
                "source_strategy_manifest_yaml": sizing.get("source_strategy_manifest_yaml"),
                "source_strategy_name": sizing.get("source_strategy_name"),
            }
    risk_fraction = _float(row.get("risk_fraction"), default_risk_fraction)
    max_contracts = _int(row.get("max_contracts"), default_max_contracts)
    if risk_fraction <= 0:
        risk_fraction = default_risk_fraction
    if max_contracts <= 0:
        max_contracts = default_max_contracts
    return {
        "risk_fraction": risk_fraction,
        "max_contracts": max_contracts,
        "strategy_sizing_mode": "projection_default",
        "source_strategy_manifest_yaml": None,
        "source_strategy_name": None,
    }


def _cap_weights(raw_weights: dict[str, float], max_weight: float) -> dict[str, float]:
    if not raw_weights:
        return {}
    if max_weight <= 0:
        raise ValueError("--max-symbol-weight must be positive when provided")
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


def _merge_capital_plans(
    *,
    portfolio_report_jsons: list[Path],
    initial_cash: float,
    max_symbol_weight: float | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    capital_plan: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    for path in portfolio_report_jsons:
        report = _load_json(path)
        rows = _capital_plan(report, source_path=path)
        capital_plan.extend(rows)
        source_counts[str(path)] = len(rows)
    if not capital_plan:
        return [], {
            "mode": "no_capital_plan",
            "source_capital_plan_counts": source_counts,
            "max_symbol_weight": max_symbol_weight,
        }
    original_weight = sum(_float(row.get("research_only_weight")) for row in capital_plan)
    if max_symbol_weight is None:
        return capital_plan, {
            "mode": "source_capital_plan_weights",
            "source_capital_plan_counts": source_counts,
            "original_allocated_weight": round(original_weight, 6),
            "max_symbol_weight": None,
        }
    symbol_raw_weights: dict[str, float] = {}
    for row in capital_plan:
        symbol = str(row.get("symbol") or "UNKNOWN").upper()
        symbol_raw_weights[symbol] = symbol_raw_weights.get(symbol, 0.0) + _float(
            row.get("research_only_weight")
        )
    symbol_weights = _cap_weights(symbol_raw_weights, max_symbol_weight)
    symbol_original_weights = dict(symbol_raw_weights)
    reweighted: list[dict[str, Any]] = []
    for row in capital_plan:
        symbol = str(row.get("symbol") or "UNKNOWN").upper()
        symbol_original = symbol_original_weights.get(symbol, 0.0)
        original_candidate_weight = _float(row.get("research_only_weight"))
        candidate_weight = (
            symbol_weights.get(symbol, 0.0) * original_candidate_weight / symbol_original
            if symbol_original > 0
            else 0.0
        )
        normalized = dict(row)
        normalized["source_research_only_weight"] = original_candidate_weight
        normalized["source_research_only_dollars"] = _float(row.get("research_only_dollars"))
        normalized["research_only_weight"] = round(candidate_weight, 6)
        normalized["research_only_dollars"] = round(candidate_weight * initial_cash, 2)
        reweighted.append(normalized)
    return reweighted, {
        "mode": "portfolio_level_symbol_cap_reweight",
        "source_capital_plan_counts": source_counts,
        "original_allocated_weight": round(original_weight, 6),
        "original_symbol_weights": {
            key: round(value, 6) for key, value in sorted(symbol_original_weights.items())
        },
        "reweighted_symbol_weights": {
            key: round(value, 6) for key, value in sorted(symbol_weights.items())
        },
        "reweighted_allocated_weight": round(
            sum(_float(row.get("research_only_weight")) for row in reweighted), 6
        ),
        "max_symbol_weight": max_symbol_weight,
    }




def _select_capital_plan_trades(
    *,
    trades: pd.DataFrame,
    capital_plan: list[dict[str, Any]],
    initial_cash: float,
    backtest_allocation_fraction: float,
    strategy_sizing_by_key: dict[str, dict[str, Any]] | None = None,
    default_risk_fraction: float = 0.05,
    default_max_contracts: int = 6,
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
        sizing = _resolve_strategy_sizing(
            row=row,
            sizing_by_key=strategy_sizing_by_key or {},
            default_risk_fraction=default_risk_fraction,
            default_max_contracts=default_max_contracts,
        )
        frame["portfolio_candidate_variant_id"] = row.get("candidate_variant_id")
        frame["base_candidate_variant_id"] = base_id
        frame["portfolio_weight"] = weight
        frame["portfolio_allocated_dollars"] = dollars
        frame["capital_plan_family"] = row.get("family")
        frame["capital_plan_regime"] = row.get("intended_regime")
        frame["capital_plan_research_score"] = row.get("research_score")
        frame["production_risk_fraction"] = sizing["risk_fraction"]
        frame["production_max_contracts"] = sizing["max_contracts"]
        frame["production_strategy_sizing_mode"] = sizing["strategy_sizing_mode"]
        frame["production_strategy_manifest_yaml"] = sizing["source_strategy_manifest_yaml"]
        frame["production_strategy_name"] = sizing["source_strategy_name"]
        frame["static_scale_factor"] = dollars / backtest_budget
        selected_frames.append(frame)
    if not selected_frames:
        return pd.DataFrame()
    return pd.concat(selected_frames, ignore_index=True)


def _capital_plan_match_key(row: dict[str, Any]) -> tuple[str, str]:
    candidate_id = str(row.get("base_candidate_variant_id") or row.get("candidate_variant_id") or "")
    candidate_id = candidate_id.split("__profile_", 1)[0]
    return candidate_id, str(row.get("aggregate_profile") or "")


def _selected_trade_match_key(row: pd.Series) -> tuple[str, str]:
    candidate_id = str(
        row.get("base_candidate_variant_id")
        or row.get("candidate_variant_id")
        or row.get("portfolio_candidate_variant_id")
        or ""
    )
    candidate_id = candidate_id.split("__profile_", 1)[0]
    return candidate_id, str(row.get("aggregate_profile") or "")


def _capital_plan_match_coverage(
    *, capital_plan: list[dict[str, Any]], selected_trades: pd.DataFrame
) -> dict[str, Any]:
    matched_keys = (
        {_selected_trade_match_key(row) for _, row in selected_trades.iterrows()}
        if not selected_trades.empty
        else set()
    )
    matched_rows = []
    unmatched_rows = []
    for row in capital_plan:
        normalized = {
            "candidate_variant_id": row.get("candidate_variant_id"),
            "base_candidate_variant_id": row.get("base_candidate_variant_id"),
            "aggregate_profile": row.get("aggregate_profile"),
            "symbol": row.get("symbol"),
            "family": row.get("family"),
            "intended_regime": row.get("intended_regime"),
        }
        if _capital_plan_match_key(row) in matched_keys:
            matched_rows.append(normalized)
        else:
            unmatched_rows.append(normalized)
    unmatched_frame = pd.DataFrame(unmatched_rows)
    unmatched_by_symbol = (
        unmatched_frame.get("symbol", pd.Series(dtype=object))
        .fillna("UNKNOWN")
        .astype(str)
        .str.upper()
        .value_counts()
        .to_dict()
        if not unmatched_frame.empty
        else {}
    )
    unmatched_by_regime = (
        unmatched_frame.get("intended_regime", pd.Series(dtype=object))
        .fillna("unknown")
        .astype(str)
        .value_counts()
        .to_dict()
        if not unmatched_frame.empty
        else {}
    )
    unmatched_by_family = (
        unmatched_frame.get("family", pd.Series(dtype=object))
        .fillna("unknown")
        .astype(str)
        .value_counts()
        .to_dict()
        if not unmatched_frame.empty
        else {}
    )
    return {
        "status": "complete",
        "capital_plan_count": int(len(capital_plan)),
        "matched_capital_plan_count": int(len(matched_rows)),
        "unmatched_capital_plan_count": int(len(unmatched_rows)),
        "selected_trade_count_before_hardening": int(len(selected_trades)),
        "match_rate_pct": (
            round(len(matched_rows) / len(capital_plan) * 100.0, 4) if capital_plan else 0.0
        ),
        "unmatched_by_symbol": {str(key): int(value) for key, value in unmatched_by_symbol.items()},
        "unmatched_by_regime": {str(key): int(value) for key, value in unmatched_by_regime.items()},
        "unmatched_by_family": {str(key): int(value) for key, value in unmatched_by_family.items()},
        "unmatched_rows": unmatched_rows,
        "unmatched_sample": unmatched_rows[:200],
    }


def _first_existing_column(frame: pd.DataFrame, names: list[str]) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    return None


def _normalize_share(value: object) -> float:
    parsed = _float(value, default=math.nan)
    if math.isnan(parsed):
        return math.nan
    parsed = abs(parsed)
    if parsed > 1.0:
        parsed /= 100.0
    return parsed


def _threshold_share(value: float | None) -> float | None:
    if value is None:
        return None
    return _normalize_share(value)


def _quality_column_map(frame: pd.DataFrame) -> dict[str, str | None]:
    return {
        "entry_spread_pct": _first_existing_column(
            frame,
            [
                "entry_spread_pct",
                "entry_relative_spread",
                "entry_relative_spread_pct",
                "entry_average_relative_spread",
                "entry_max_relative_spread",
                "entry_bid_ask_spread_pct",
                "entry_option_spread_pct",
                "spread_pct",
            ],
        ),
        "exit_spread_pct": _first_existing_column(
            frame,
            [
                "exit_spread_pct",
                "exit_relative_spread",
                "exit_relative_spread_pct",
                "exit_average_relative_spread",
                "exit_max_relative_spread",
                "exit_bid_ask_spread_pct",
                "exit_option_spread_pct",
            ],
        ),
        "entry_quote_age_seconds": _first_existing_column(
            frame,
            [
                "entry_quote_age_seconds",
                "entry_freshness_seconds",
                "entry_option_quote_age_seconds",
                "freshness_seconds",
            ],
        ),
        "exit_quote_age_seconds": _first_existing_column(
            frame,
            [
                "exit_quote_age_seconds",
                "exit_freshness_seconds",
                "exit_option_quote_age_seconds",
            ],
        ),
    }


def _market_quality_output_fields(row: pd.Series) -> dict[str, Any]:
    column_map = _quality_column_map(pd.DataFrame(columns=list(row.index)))
    output: dict[str, Any] = {}
    for field, source_column in column_map.items():
        if source_column and source_column in row.index:
            output[field] = row.get(source_column)
            output[f"{field}_source_column"] = source_column
    for source_field in ["entry_quote_source", "exit_quote_source"]:
        if source_field in row.index:
            output[source_field] = row.get(source_field)
    return output


def _market_quality_diagnostics(selected_trades: pd.DataFrame) -> dict[str, Any]:
    if selected_trades.empty:
        return {
            "status": "no_trades",
            "input_trade_count": 0,
        }
    column_map = {
        key: value
        for key, value in _quality_column_map(selected_trades).items()
        if value
    }
    diagnostics: dict[str, Any] = {
        "status": "complete",
        "input_trade_count": int(len(selected_trades)),
        "source_columns": column_map,
        "quote_source_counts": {},
        "spread_coverage": {},
        "quote_age_seconds": {},
        "trade_print_coverage": {},
    }
    for source_field in ["entry_quote_source", "exit_quote_source"]:
        if source_field in selected_trades.columns:
            diagnostics["quote_source_counts"][source_field] = {
                str(key): int(value)
                for key, value in selected_trades[source_field]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", "missing")
                .value_counts()
                .sort_index()
                .items()
            }
    for canonical_field in ["entry_spread_pct", "exit_spread_pct"]:
        source_column = column_map.get(canonical_field)
        if not source_column:
            continue
        values = pd.to_numeric(selected_trades[source_column], errors="coerce")
        present = values.dropna()
        diagnostics["spread_coverage"][canonical_field] = {
            "source_column": source_column,
            "present_count": int(present.size),
            "coverage_pct": round(float(present.size) / float(len(selected_trades)) * 100.0, 4),
            "p50": round(float(present.quantile(0.50)), 8) if not present.empty else None,
            "p95": round(float(present.quantile(0.95)), 8) if not present.empty else None,
        }
    for canonical_field in ["entry_quote_age_seconds", "exit_quote_age_seconds"]:
        source_column = column_map.get(canonical_field)
        if not source_column:
            continue
        values = pd.to_numeric(selected_trades[source_column], errors="coerce")
        present = values.dropna()
        diagnostics["quote_age_seconds"][canonical_field] = {
            "source_column": source_column,
            "present_count": int(present.size),
            "coverage_pct": round(float(present.size) / float(len(selected_trades)) * 100.0, 4),
            "p50": round(float(present.quantile(0.50)), 6) if not present.empty else None,
            "p95": round(float(present.quantile(0.95)), 6) if not present.empty else None,
            "max": round(float(present.max()), 6) if not present.empty else None,
        }
    source_frame = pd.DataFrame(
        {
            field: selected_trades[field].fillna("").astype(str).str.lower()
            for field in ["entry_quote_source", "exit_quote_source"]
            if field in selected_trades.columns
        }
    )
    if not source_frame.empty:
        no_bid_ask_mask = source_frame.apply(
            lambda row: any("no_bid_ask" in str(value) for value in row),
            axis=1,
        )
        diagnostics["rows_with_any_no_bid_ask_quote_source"] = int(no_bid_ask_mask.sum())
        diagnostics["rows_with_any_no_bid_ask_quote_source_pct"] = round(
            float(no_bid_ask_mask.sum()) / float(len(selected_trades)) * 100.0,
            4,
        )
    print_column = _first_existing_column(
        selected_trades,
        ["entry_selection_trade_print_count", "option_trade_print_count", "trade_print_count"],
    )
    if print_column:
        values = pd.to_numeric(selected_trades[print_column], errors="coerce")
        present = values.dropna()
        diagnostics["trade_print_coverage"] = {
            "source_column": print_column,
            "present_count": int(present.size),
            "coverage_pct": round(float(present.size) / float(len(selected_trades)) * 100.0, 4),
            "zero_or_missing_count": int((values.fillna(0.0) <= 0.0).sum()),
            "p50": round(float(present.quantile(0.50)), 6) if not present.empty else None,
            "p95": round(float(present.quantile(0.95)), 6) if not present.empty else None,
        }
    return diagnostics


def _fill_probability_haircut_output_fields(row: pd.Series) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for field in [
        "source_option_pnl_before_fill_haircut",
        "fill_probability_pnl_multiplier",
        "fill_probability_pnl_haircut_amount",
    ]:
        if field in row.index:
            output[field] = row.get(field)
    return output


def _market_quality_cost_output_fields(row: pd.Series) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for field in [
        "source_option_pnl_before_market_quality_cost",
        "market_quality_pnl_cost",
        "market_quality_entry_spread_pct_applied",
        "market_quality_exit_spread_pct_applied",
        "market_quality_entry_spread_reason",
        "market_quality_exit_spread_reason",
        "market_quality_entry_notional",
        "market_quality_exit_notional",
    ]:
        if field in row.index:
            output[field] = row.get(field)
    return output


def _hardening_rejection_row(
    *,
    row_id: int,
    row: pd.Series,
    reason: str,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        "row_id": int(row_id),
        "decision": "rejected",
        "decision_reason": reason,
        "trade_date": str(row.get("trade_date")),
        "candidate_variant_id": row.get("portfolio_candidate_variant_id")
        or row.get("candidate_variant_id"),
        "base_candidate_variant_id": row.get("base_candidate_variant_id"),
        "aggregate_profile": row.get("aggregate_profile"),
        "symbol": row.get("symbol"),
        "family": row.get("capital_plan_family") or row.get("family"),
        "intended_regime": row.get("capital_plan_regime") or row.get("intended_regime"),
    }
    if detail:
        event.update(detail)
    return event


def _apply_market_quality_stress(
    *,
    selected_trades: pd.DataFrame,
    max_entry_spread_pct: float | None,
    max_exit_spread_pct: float | None,
    max_entry_quote_age_seconds: float | None,
    max_exit_quote_age_seconds: float | None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    limits = {
        "entry_spread_pct": _threshold_share(max_entry_spread_pct),
        "exit_spread_pct": _threshold_share(max_exit_spread_pct),
        "entry_quote_age_seconds": max_entry_quote_age_seconds,
        "exit_quote_age_seconds": max_exit_quote_age_seconds,
    }
    configured = {key: value for key, value in limits.items() if value is not None}
    if not configured:
        return selected_trades, pd.DataFrame(), {
            "status": "not_configured",
            "input_trade_count": int(len(selected_trades)),
            "kept_trade_count": int(len(selected_trades)),
            "rejected_trade_count": 0,
        }
    if selected_trades.empty:
        return selected_trades, pd.DataFrame(), {
            "status": "enabled",
            "input_trade_count": 0,
            "kept_trade_count": 0,
            "rejected_trade_count": 0,
            "limits": configured,
        }
    column_map = _quality_column_map(selected_trades)
    keep_mask = pd.Series(True, index=selected_trades.index)
    rejections: list[dict[str, Any]] = []
    for row_id, row in selected_trades.iterrows():
        row_reasons: list[str] = []
        details: dict[str, Any] = {}
        for field, limit in configured.items():
            source_column = column_map.get(field)
            details[f"{field}_limit"] = limit
            details[f"{field}_source_column"] = source_column
            if not source_column:
                row_reasons.append(f"missing_{field}")
                continue
            if field.endswith("_spread_pct"):
                value = _normalize_share(row.get(source_column))
            else:
                value = _float(row.get(source_column), default=math.nan)
            details[field] = None if math.isnan(value) else round(value, 8)
            if math.isnan(value):
                row_reasons.append(f"missing_{field}")
            elif value > float(limit):
                row_reasons.append(f"{field}_above_limit")
        if row_reasons:
            keep_mask.loc[row_id] = False
            rejections.append(
                _hardening_rejection_row(
                    row_id=int(row_id),
                    row=row,
                    reason=";".join(row_reasons),
                    detail=details,
                )
            )
    rejection_frame = pd.DataFrame(rejections)
    kept = selected_trades.loc[keep_mask].copy()
    reason_counts = Counter()
    for rejection in rejections:
        for reason in str(rejection["decision_reason"]).split(";"):
            reason_counts[reason] += 1
    return kept, rejection_frame, {
        "status": "enabled",
        "input_trade_count": int(len(selected_trades)),
        "kept_trade_count": int(len(kept)),
        "rejected_trade_count": int(len(rejection_frame)),
        "limits": configured,
        "source_columns": column_map,
        "rejection_reason_counts": dict(sorted(reason_counts.items())),
    }


def _quote_source_has_no_bid_ask(row: pd.Series, field: str) -> bool:
    source = str(row.get(field) or "").strip().lower()
    if not source:
        return False
    return "no_bid_ask" in source or source in {"unknown", "missing", "none"}


def _quality_trade_print_column(frame: pd.DataFrame) -> str | None:
    return _first_existing_column(
        frame,
        ["entry_selection_trade_print_count", "option_trade_print_count", "trade_print_count"],
    )


def _side_notional(row: pd.Series, *, side: str) -> float:
    quantity = max(_int(row.get("quantity"), 1), 1)
    if side == "entry":
        for field in ["entry_debit_per_unit", "risk_per_unit"]:
            value = abs(_float(row.get(field), default=math.nan))
            if not math.isnan(value) and value > 0.0:
                return value * quantity
        price = abs(_float(row.get("entry_price_after_slippage"), default=math.nan))
        if not math.isnan(price) and price > 0.0:
            return price * 100.0 * quantity
        return 0.0
    for field in ["exit_value_per_unit", "entry_debit_per_unit", "risk_per_unit"]:
        value = abs(_float(row.get(field), default=math.nan))
        if not math.isnan(value) and value > 0.0:
            return value * quantity
    price = abs(_float(row.get("exit_price_after_slippage"), default=math.nan))
    if not math.isnan(price) and price > 0.0:
        return price * 100.0 * quantity
    return 0.0


def _applied_quality_spread(
    *,
    row: pd.Series,
    side: str,
    column_map: dict[str, str | None],
    trade_print_column: str | None,
    no_bid_ask_spread_pct: float,
    zero_print_spread_pct: float,
    unknown_spread_pct: float,
    max_spread_pct: float,
    quote_age_grace_seconds: float,
    quote_age_spread_pct_per_minute: float,
) -> tuple[float, str]:
    reasons: list[str] = []
    spread_column = column_map.get(f"{side}_spread_pct")
    spread = _normalize_share(row.get(spread_column)) if spread_column else math.nan
    if not math.isnan(spread):
        applied = spread
        reasons.append(f"observed_{spread_column}")
    else:
        applied = 0.0
        reasons.append("missing_observed_spread")

    source_field = f"{side}_quote_source"
    if source_field in row.index and _quote_source_has_no_bid_ask(row, source_field):
        applied = max(applied, no_bid_ask_spread_pct)
        reasons.append("no_bid_ask_proxy")

    if trade_print_column:
        prints = _float(row.get(trade_print_column), default=math.nan)
        if math.isnan(prints) or prints <= 0.0:
            applied = max(applied, zero_print_spread_pct)
            reasons.append("zero_trade_print_proxy")

    if applied <= 0.0:
        applied = unknown_spread_pct
        reasons.append("unknown_spread_proxy")

    age_column = column_map.get(f"{side}_quote_age_seconds")
    if age_column:
        age = _float(row.get(age_column), default=math.nan)
        if not math.isnan(age) and age > quote_age_grace_seconds:
            extra = ((age - quote_age_grace_seconds) / 60.0) * quote_age_spread_pct_per_minute
            applied += max(extra, 0.0)
            reasons.append("quote_age_penalty")

    return min(max(applied, 0.0), max_spread_pct), "|".join(reasons)


def _apply_market_quality_pnl_cost_model(
    *,
    selected_trades: pd.DataFrame,
    enabled: bool,
    no_bid_ask_spread_pct: float,
    zero_print_spread_pct: float,
    unknown_spread_pct: float,
    max_spread_pct: float,
    quote_age_grace_seconds: float,
    quote_age_spread_pct_per_minute: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not enabled:
        return selected_trades, {
            "status": "not_configured",
            "input_trade_count": int(len(selected_trades)),
            "adjusted_trade_count": 0,
        }
    if selected_trades.empty:
        return selected_trades, {
            "status": "enabled",
            "input_trade_count": 0,
            "adjusted_trade_count": 0,
            "total_market_quality_pnl_cost": 0.0,
        }

    adjusted = selected_trades.copy()
    column_map = _quality_column_map(adjusted)
    trade_print_column = _quality_trade_print_column(adjusted)
    no_bid_ask_spread_pct = _threshold_share(no_bid_ask_spread_pct) or 0.0
    zero_print_spread_pct = _threshold_share(zero_print_spread_pct) or 0.0
    unknown_spread_pct = _threshold_share(unknown_spread_pct) or 0.0
    max_spread_pct = _threshold_share(max_spread_pct) or 1.0
    cost_rows: list[dict[str, Any]] = []
    adjusted_pnl: list[float] = []
    for _, row in adjusted.iterrows():
        entry_spread, entry_reason = _applied_quality_spread(
            row=row,
            side="entry",
            column_map=column_map,
            trade_print_column=trade_print_column,
            no_bid_ask_spread_pct=no_bid_ask_spread_pct,
            zero_print_spread_pct=zero_print_spread_pct,
            unknown_spread_pct=unknown_spread_pct,
            max_spread_pct=max_spread_pct,
            quote_age_grace_seconds=quote_age_grace_seconds,
            quote_age_spread_pct_per_minute=quote_age_spread_pct_per_minute,
        )
        exit_spread, exit_reason = _applied_quality_spread(
            row=row,
            side="exit",
            column_map=column_map,
            trade_print_column=trade_print_column,
            no_bid_ask_spread_pct=no_bid_ask_spread_pct,
            zero_print_spread_pct=zero_print_spread_pct,
            unknown_spread_pct=unknown_spread_pct,
            max_spread_pct=max_spread_pct,
            quote_age_grace_seconds=quote_age_grace_seconds,
            quote_age_spread_pct_per_minute=quote_age_spread_pct_per_minute,
        )
        entry_notional = _side_notional(row, side="entry")
        exit_notional = _side_notional(row, side="exit")
        cost = (entry_notional * entry_spread * 0.5) + (exit_notional * exit_spread * 0.5)
        raw_pnl = _float(row.get("option_pnl"))
        adjusted_pnl.append(raw_pnl - cost)
        cost_rows.append(
            {
                "source_option_pnl_before_market_quality_cost": raw_pnl,
                "market_quality_pnl_cost": cost,
                "market_quality_entry_spread_pct_applied": entry_spread,
                "market_quality_exit_spread_pct_applied": exit_spread,
                "market_quality_entry_spread_reason": entry_reason,
                "market_quality_exit_spread_reason": exit_reason,
                "market_quality_entry_notional": entry_notional,
                "market_quality_exit_notional": exit_notional,
            }
        )

    cost_frame = pd.DataFrame(cost_rows, index=adjusted.index)
    for column in cost_frame.columns:
        adjusted[column] = cost_frame[column]
    adjusted["option_pnl"] = adjusted_pnl
    total_cost = float(cost_frame["market_quality_pnl_cost"].sum()) if not cost_frame.empty else 0.0
    return adjusted, {
        "status": "enabled",
        "input_trade_count": int(len(selected_trades)),
        "adjusted_trade_count": int((cost_frame["market_quality_pnl_cost"] > 0.0).sum()),
        "total_market_quality_pnl_cost": round(total_cost, 6),
        "average_market_quality_pnl_cost": round(float(cost_frame["market_quality_pnl_cost"].mean()), 6)
        if not cost_frame.empty
        else None,
        "source_columns": column_map,
        "trade_print_source_column": trade_print_column,
        "parameters": {
            "no_bid_ask_spread_pct": no_bid_ask_spread_pct,
            "zero_print_spread_pct": zero_print_spread_pct,
            "unknown_spread_pct": unknown_spread_pct,
            "max_spread_pct": max_spread_pct,
            "quote_age_grace_seconds": quote_age_grace_seconds,
            "quote_age_spread_pct_per_minute": quote_age_spread_pct_per_minute,
        },
    }


def _estimate_fill_probability(row: pd.Series, *, unknown_fill_probability: float) -> tuple[float, list[str]]:
    probability = 1.0
    components: list[str] = []
    frame_columns = set(row.index)
    column_map = {
        key: value
        for key, value in _quality_column_map(pd.DataFrame(columns=list(frame_columns))).items()
        if value
    }
    entry_spread_column = column_map.get("entry_spread_pct")
    if entry_spread_column:
        spread = _normalize_share(row.get(entry_spread_column))
        if not math.isnan(spread):
            probability *= max(0.15, 1.0 - min(spread, 0.5) * 2.0)
            components.append(f"entry_spread={round(spread, 6)}")
    entry_age_column = column_map.get("entry_quote_age_seconds")
    if entry_age_column:
        age = _float(row.get(entry_age_column), default=math.nan)
        if not math.isnan(age):
            probability *= 1.0 if age <= 5.0 else max(0.2, 1.0 - (age - 5.0) / 120.0)
            components.append(f"entry_quote_age_seconds={round(age, 3)}")
    for source_field, multiplier in [
        ("entry_quote_source", 0.55),
        ("exit_quote_source", 0.75),
    ]:
        if source_field in frame_columns:
            source = str(row.get(source_field) or "").strip().lower()
            if source:
                components.append(f"{source_field}={source}")
                if "no_bid_ask" in source or source in {"unknown", "missing", "none"}:
                    probability *= multiplier
    print_column = _first_existing_column(
        pd.DataFrame(columns=list(frame_columns)),
        ["entry_selection_trade_print_count", "option_trade_print_count", "trade_print_count"],
    )
    if print_column:
        prints = _float(row.get(print_column), default=math.nan)
        if not math.isnan(prints):
            if prints <= 0:
                multiplier = 0.35
            elif prints <= 2:
                multiplier = 0.65
            elif prints <= 5:
                multiplier = 0.85
            else:
                multiplier = 1.0
            probability *= multiplier
            components.append(f"{print_column}={round(prints, 3)}")
    if not components:
        unknown = min(max(_float(unknown_fill_probability, 1.0), 0.0), 1.0)
        return unknown, ["unknown_market_quality_inputs"]
    return min(max(probability, 0.0), 1.0), components


def _apply_fill_probability_model(
    *,
    selected_trades: pd.DataFrame,
    enabled: bool,
    min_fill_probability: float | None,
    unknown_fill_probability: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    configured = enabled or min_fill_probability is not None
    if not configured:
        return selected_trades, pd.DataFrame(), {
            "status": "not_configured",
            "input_trade_count": int(len(selected_trades)),
            "kept_trade_count": int(len(selected_trades)),
            "rejected_trade_count": 0,
        }
    if selected_trades.empty:
        return selected_trades, pd.DataFrame(), {
            "status": "enabled",
            "input_trade_count": 0,
            "kept_trade_count": 0,
            "rejected_trade_count": 0,
            "min_fill_probability": min_fill_probability,
            "unknown_fill_probability": unknown_fill_probability,
        }
    modeled = selected_trades.copy()
    probabilities: list[float] = []
    components: list[str] = []
    for _, row in modeled.iterrows():
        probability, row_components = _estimate_fill_probability(
            row,
            unknown_fill_probability=unknown_fill_probability,
        )
        probabilities.append(probability)
        components.append("|".join(row_components))
    modeled["projected_fill_probability"] = probabilities
    modeled["fill_probability_components"] = components
    if min_fill_probability is None:
        return modeled, pd.DataFrame(), {
            "status": "enabled",
            "input_trade_count": int(len(selected_trades)),
            "kept_trade_count": int(len(modeled)),
            "rejected_trade_count": 0,
            "min_fill_probability": None,
            "unknown_fill_probability": unknown_fill_probability,
            "average_fill_probability": round(float(np.mean(probabilities)), 6)
            if probabilities
            else None,
        }
    threshold = min(max(_float(min_fill_probability), 0.0), 1.0)
    keep_mask = modeled["projected_fill_probability"] >= threshold
    rejections = [
        _hardening_rejection_row(
            row_id=int(row_id),
            row=row,
            reason="fill_probability_below_min",
            detail={
                "projected_fill_probability": round(_float(row.get("projected_fill_probability")), 8),
                "min_fill_probability": threshold,
                "fill_probability_components": row.get("fill_probability_components"),
            },
        )
        for row_id, row in modeled.loc[~keep_mask].iterrows()
    ]
    kept = modeled.loc[keep_mask].copy()
    probability_array = np.array(probabilities, dtype=float)
    return kept, pd.DataFrame(rejections), {
        "status": "enabled",
        "input_trade_count": int(len(selected_trades)),
        "kept_trade_count": int(len(kept)),
        "rejected_trade_count": int(len(rejections)),
        "min_fill_probability": threshold,
        "unknown_fill_probability": unknown_fill_probability,
        "average_fill_probability": round(float(np.mean(probability_array)), 6)
        if probability_array.size
        else None,
        "p10_fill_probability": round(float(np.percentile(probability_array, 10)), 6)
        if probability_array.size
        else None,
        "p50_fill_probability": round(float(np.percentile(probability_array, 50)), 6)
        if probability_array.size
        else None,
        "p90_fill_probability": round(float(np.percentile(probability_array, 90)), 6)
        if probability_array.size
        else None,
        "rejection_reason_counts": {"fill_probability_below_min": int(len(rejections))}
        if rejections
        else {},
    }


def _apply_fill_probability_pnl_haircut(
    *,
    selected_trades: pd.DataFrame,
    enabled: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not enabled:
        return selected_trades, {
            "status": "not_configured",
            "input_trade_count": int(len(selected_trades)),
            "adjusted_trade_count": 0,
        }
    if selected_trades.empty:
        return selected_trades, {
            "status": "enabled",
            "input_trade_count": 0,
            "adjusted_trade_count": 0,
            "total_positive_pnl_before_haircut": 0.0,
            "total_positive_pnl_after_haircut": 0.0,
            "total_positive_pnl_haircut": 0.0,
        }
    if "projected_fill_probability" not in selected_trades.columns:
        return selected_trades, {
            "status": "not_applied_missing_projected_fill_probability",
            "input_trade_count": int(len(selected_trades)),
            "adjusted_trade_count": 0,
        }

    adjusted = selected_trades.copy()
    raw_pnl = pd.to_numeric(adjusted.get("option_pnl"), errors="coerce").fillna(0.0)
    probabilities = (
        pd.to_numeric(adjusted.get("projected_fill_probability"), errors="coerce")
        .fillna(1.0)
        .clip(lower=0.0, upper=1.0)
    )
    positive_mask = raw_pnl > 0.0
    multipliers = pd.Series(1.0, index=adjusted.index, dtype=float)
    multipliers.loc[positive_mask] = probabilities.loc[positive_mask]
    adjusted_pnl = raw_pnl * multipliers
    haircut_amount = raw_pnl - adjusted_pnl

    adjusted["source_option_pnl_before_fill_haircut"] = raw_pnl
    adjusted["fill_probability_pnl_multiplier"] = multipliers
    adjusted["fill_probability_pnl_haircut_amount"] = haircut_amount
    adjusted["option_pnl"] = adjusted_pnl

    positive_before = float(raw_pnl.loc[positive_mask].sum())
    positive_after = float(adjusted_pnl.loc[positive_mask].sum())
    return adjusted, {
        "status": "enabled",
        "mode": "positive_pnl_only",
        "input_trade_count": int(len(selected_trades)),
        "adjusted_trade_count": int(positive_mask.sum()),
        "total_positive_pnl_before_haircut": round(positive_before, 6),
        "total_positive_pnl_after_haircut": round(positive_after, 6),
        "total_positive_pnl_haircut": round(positive_before - positive_after, 6),
        "average_positive_pnl_multiplier": round(float(multipliers.loc[positive_mask].mean()), 6)
        if bool(positive_mask.any())
        else None,
    }


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
        active_regimes = sorted(
            {
                str(value)
                for value in group.get("capital_plan_regime", pd.Series(dtype=object)).dropna().unique()
                if str(value) and str(value) != "nan"
            }
        )
        active_symbols = sorted(
            {
                str(value)
                for value in group.get("symbol", pd.Series(dtype=object)).dropna().unique()
                if str(value) and str(value) != "nan"
            }
        )
        active_families = sorted(
            {
                str(value)
                for value in group.get("capital_plan_family", pd.Series(dtype=object)).dropna().unique()
                if str(value) and str(value) != "nan"
            }
        )
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
                    **_market_quality_output_fields(row),
                    **_market_quality_cost_output_fields(row),
                    "projected_fill_probability": row.get("projected_fill_probability"),
                    "fill_probability_components": row.get("fill_probability_components"),
                    **_fill_probability_haircut_output_fields(row),
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
                "active_trade_day": True,
                "active_regimes": ",".join(active_regimes),
                "active_symbols": ",".join(active_symbols),
                "active_families": ",".join(active_families),
            }
        )
    return pd.DataFrame(daily_rows), pd.DataFrame(trade_rows)


def _trade_risk_per_combo(row: pd.Series) -> float:
    risk = _float(row.get("risk_per_unit"))
    if risk > 0:
        return risk
    debit = abs(_float(row.get("entry_debit_per_unit")))
    if debit > 0:
        return debit
    price = abs(_float(row.get("entry_price_after_slippage"))) * 100.0
    return price if price > 0 else 0.0


def _trade_debit_cash_per_combo(row: pd.Series) -> float:
    debit = _float(row.get("entry_debit_per_unit"))
    if debit > 0:
        return debit
    price = _float(row.get("entry_price_after_slippage")) * 100.0
    return max(price, 0.0)


def _buckets_for_symbol(symbol: str, risk_config: dict[str, Any]) -> list[dict[str, Any]]:
    symbol = symbol.upper()
    return [
        bucket
        for bucket in risk_config.get("bucket_caps", [])
        if symbol in {str(item).upper() for item in bucket.get("symbols", [])}
    ]


def _reserved_risk(open_positions: dict[int, dict[str, Any]]) -> float:
    return sum(_float(position.get("risk_dollars")) for position in open_positions.values())


def _reserved_debit(open_positions: dict[int, dict[str, Any]]) -> float:
    return sum(_float(position.get("debit_cash")) for position in open_positions.values())


def _symbol_reserved_risk(open_positions: dict[int, dict[str, Any]], symbol: str) -> float:
    return sum(
        _float(position.get("risk_dollars"))
        for position in open_positions.values()
        if str(position.get("symbol") or "").upper() == symbol.upper()
    )


def _bucket_reserved_risk(
    open_positions: dict[int, dict[str, Any]], bucket: dict[str, Any]
) -> float:
    symbols = {str(symbol).upper() for symbol in bucket.get("symbols", [])}
    return sum(
        _float(position.get("risk_dollars"))
        for position in open_positions.values()
        if str(position.get("symbol") or "").upper() in symbols
    )


def _position_count(
    open_positions: dict[int, dict[str, Any]],
    *,
    symbol: str | None = None,
    regime: str | None = None,
) -> int:
    count = 0
    for position in open_positions.values():
        if symbol is not None and str(position.get("symbol") or "").upper() != symbol.upper():
            continue
        if regime is not None and str(position.get("regime") or "") != regime:
            continue
        count += 1
    return count


def _recent_position_count(
    open_positions: dict[int, dict[str, Any]],
    *,
    current_time: pd.Timestamp,
    window_minutes: int | None,
    regime: str | None = None,
    bucket: dict[str, Any] | None = None,
) -> int:
    if window_minutes is None or window_minutes <= 0:
        return 0
    symbols = (
        {str(symbol).upper() for symbol in bucket.get("symbols", [])}
        if bucket is not None
        else None
    )
    count = 0
    for position in open_positions.values():
        if regime is not None and str(position.get("regime") or "") != regime:
            continue
        if symbols is not None and str(position.get("symbol") or "").upper() not in symbols:
            continue
        entry_time = position.get("entry_time")
        if not isinstance(entry_time, pd.Timestamp):
            continue
        minutes = (current_time - entry_time).total_seconds() / 60.0
        if 0 <= minutes <= window_minutes:
            count += 1
    return count


def _production_risk_scale(*, equity: float, peak: float, risk_config: dict[str, Any]) -> float:
    threshold_pct = _float(risk_config.get("delever_drawdown_pct"))
    if threshold_pct <= 0 or peak <= 0:
        return 1.0
    drawdown_pct = (equity - peak) / peak * 100.0
    if drawdown_pct <= -threshold_pct:
        scale = _float(risk_config.get("delever_risk_scale"), 1.0)
        return scale if scale > 0 else 1.0
    return 1.0


def _production_regime_risk_scale(*, regime: str, risk_config: dict[str, Any]) -> float:
    scales = risk_config.get("regime_risk_scales") or {}
    if not isinstance(scales, dict):
        return 1.0
    scale = _float(scales.get(str(regime).lower()), 1.0)
    return scale if scale > 0.0 else 1.0


def _build_daily_equity_production_runtime(
    *,
    selected_trades: pd.DataFrame,
    initial_cash: float,
    risk_config: dict[str, Any],
    enforce_broker_equity_floor: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if selected_trades.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {
            "mode": "production_runtime",
            "accepted_trade_count": 0,
            "rejected_trade_count": 0,
        }
    trades = selected_trades.copy().reset_index(drop=True)
    trades["entry_ts"] = pd.to_datetime(
        trades.get("option_entry_time"), errors="coerce", utc=True
    )
    trades["exit_ts"] = pd.to_datetime(
        trades.get("option_exit_time"), errors="coerce", utc=True
    )
    events: list[tuple[pd.Timestamp, int, int]] = []
    risk_events: list[dict[str, Any]] = []
    for row_id, row in trades.iterrows():
        entry_ts = row.get("entry_ts")
        exit_ts = row.get("exit_ts")
        if not isinstance(entry_ts, pd.Timestamp) or pd.isna(entry_ts):
            risk_events.append(
                {
                    "row_id": row_id,
                    "decision": "rejected",
                    "decision_reason": "missing_option_entry_time",
                    "symbol": row.get("symbol"),
                    "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
                }
            )
            continue
        if not isinstance(exit_ts, pd.Timestamp) or pd.isna(exit_ts) or exit_ts <= entry_ts:
            risk_events.append(
                {
                    "row_id": row_id,
                    "decision": "rejected",
                    "decision_reason": "missing_or_invalid_option_exit_time",
                    "entry_time": str(entry_ts),
                    "symbol": row.get("symbol"),
                    "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
                }
            )
            continue
        if _trade_risk_per_combo(row) <= 0:
            risk_events.append(
                {
                    "row_id": row_id,
                    "decision": "rejected",
                    "decision_reason": "missing_risk_per_combo",
                    "entry_time": str(entry_ts),
                    "symbol": row.get("symbol"),
                    "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
                }
            )
            continue
        events.append((entry_ts, 1, row_id))
        events.append((exit_ts, 0, row_id))
    events.sort(key=lambda item: (item[0], item[1], item[2]))

    equity = initial_cash
    peak = initial_cash
    open_positions: dict[int, dict[str, Any]] = {}
    daily_pnl: dict[Any, float] = {}
    daily_trade_count: Counter[Any] = Counter()
    daily_rejected_count: Counter[Any] = Counter()
    daily_regimes: dict[Any, set[str]] = {}
    daily_symbols: dict[Any, set[str]] = {}
    daily_families: dict[Any, set[str]] = {}
    trade_rows: list[dict[str, Any]] = []

    def reject(row: pd.Series, reason: str, *, event_time: pd.Timestamp, details: dict[str, Any] | None = None) -> None:
        trade_date = pd.to_datetime(row.get("trade_date"), errors="coerce").date()
        daily_rejected_count[trade_date] += 1
        event = {
            "row_id": int(row.name),
            "event_time": str(event_time),
            "trade_date": str(trade_date),
            "decision": "rejected",
            "decision_reason": reason,
            "symbol": row.get("symbol"),
            "regime": row.get("capital_plan_regime"),
            "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
            "risk_fraction": _float(row.get("production_risk_fraction")),
            "max_contracts": _int(row.get("production_max_contracts")),
            "equity": round(equity, 6),
            "open_position_count": len(open_positions),
            "reserved_risk": round(_reserved_risk(open_positions), 6),
        }
        if details:
            event.update(details)
        risk_events.append(event)

    for event_time, event_type, row_id in events:
        row = trades.loc[row_id]
        if event_type == 0:
            position = open_positions.pop(row_id, None)
            if position is None:
                continue
            source_quantity = max(_int(row.get("quantity"), 1), 1)
            pnl_per_combo = _float(row.get("option_pnl")) / source_quantity
            scaled_pnl = pnl_per_combo * _int(position.get("quantity"), 0)
            trade_date = pd.to_datetime(row.get("trade_date"), errors="coerce").date()
            daily_pnl[trade_date] = daily_pnl.get(trade_date, 0.0) + scaled_pnl
            equity = max(equity + scaled_pnl, 0.0)
            peak = max(peak, equity)
            trade_rows.append(
                {
                    "trade_date": str(trade_date),
                    "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
                    "base_candidate_variant_id": row.get("base_candidate_variant_id"),
                    "aggregate_profile": row.get("aggregate_profile"),
                    "symbol": row.get("symbol"),
                    "family": row.get("capital_plan_family") or row.get("family"),
                    "intended_regime": row.get("capital_plan_regime"),
                    "portfolio_weight": _float(row.get("portfolio_weight")),
                    "risk_simulation_mode": "production_runtime",
                    "production_risk_fraction": _float(row.get("production_risk_fraction")),
                    "production_max_contracts": _int(row.get("production_max_contracts")),
                    "source_quantity": source_quantity,
                    "production_quantity": _int(position.get("quantity"), 0),
                    "source_risk_per_combo": round(_float(position.get("risk_per_combo")), 6),
                    "production_risk_dollars": round(_float(position.get("risk_dollars")), 6),
                    "production_debit_cash": round(_float(position.get("debit_cash")), 6),
                    "risk_scale": round(_float(position.get("risk_scale"), 1.0), 6),
                    "drawdown_risk_scale": round(
                        _float(position.get("drawdown_risk_scale"), 1.0), 6
                    ),
                    "regime_risk_scale": round(_float(position.get("regime_risk_scale"), 1.0), 6),
                    "source_option_pnl": round(_float(row.get("option_pnl")), 6),
                    "source_pnl_per_combo": round(pnl_per_combo, 6),
                    "dynamic_scale_factor": round(_int(position.get("quantity"), 0) / source_quantity, 8),
                    "scaled_option_pnl": round(scaled_pnl, 6),
                    "option_entry_time": str(row.get("entry_ts")),
                    "option_exit_time": str(row.get("exit_ts")),
                    **_market_quality_output_fields(row),
                    **_market_quality_cost_output_fields(row),
                    "projected_fill_probability": row.get("projected_fill_probability"),
                    "fill_probability_components": row.get("fill_probability_components"),
                    **_fill_probability_haircut_output_fields(row),
                }
            )
            risk_events.append(
                {
                    "row_id": int(row_id),
                    "event_time": str(event_time),
                    "trade_date": str(trade_date),
                    "decision": "closed",
                    "decision_reason": "exit_time",
                    "symbol": row.get("symbol"),
                    "regime": row.get("capital_plan_regime"),
                    "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
                    "quantity": _int(position.get("quantity"), 0),
                    "scaled_option_pnl": round(scaled_pnl, 6),
                    "equity": round(equity, 6),
                    "open_position_count": len(open_positions),
                }
            )
            continue

        trade_date = pd.to_datetime(row.get("trade_date"), errors="coerce").date()
        symbol = str(row.get("symbol") or "").upper()
        regime = str(row.get("capital_plan_regime") or row.get("intended_regime") or "")
        strategy_key = str(row.get("portfolio_candidate_variant_id") or row.get("base_candidate_variant_id") or "")
        risk_per_combo = _trade_risk_per_combo(row)
        debit_cash_per_combo = _trade_debit_cash_per_combo(row)
        drawdown_risk_scale = _production_risk_scale(equity=equity, peak=peak, risk_config=risk_config)
        regime_risk_scale = _production_regime_risk_scale(regime=regime, risk_config=risk_config)
        risk_scale = drawdown_risk_scale * regime_risk_scale
        if enforce_broker_equity_floor:
            broker_floor = risk_config.get("broker_min_equity_to_trade")
            if broker_floor is not None and equity < _float(broker_floor):
                reject(
                    row,
                    "broker_equity_below_trade_floor",
                    event_time=event_time,
                    details={"broker_min_equity_to_trade": _float(broker_floor)},
                )
                continue
        if len(open_positions) >= _int(risk_config.get("max_open_positions"), 10):
            reject(row, "max_open_positions", event_time=event_time)
            continue
        if _position_count(open_positions, regime=regime) >= _int(risk_config.get("max_positions_per_regime"), 10):
            reject(row, "max_positions_per_regime", event_time=event_time)
            continue
        if _position_count(open_positions, symbol=symbol) >= _int(risk_config.get("max_positions_per_symbol"), 3):
            reject(row, "max_positions_per_symbol", event_time=event_time)
            continue
        if any(position.get("strategy_key") == strategy_key for position in open_positions.values()):
            reject(row, "strategy_already_open", event_time=event_time)
            continue
        cluster_window = risk_config.get("entry_cluster_window_minutes")
        max_regime_window = risk_config.get("max_positions_per_regime_window")
        if max_regime_window is not None and _int(max_regime_window) > 0:
            recent_regime = _recent_position_count(
                open_positions,
                current_time=event_time,
                window_minutes=_int(cluster_window, 0),
                regime=regime,
            )
            if recent_regime >= _int(max_regime_window):
                reject(
                    row,
                    f"regime_entry_cluster:{regime}",
                    event_time=event_time,
                    details={"recent_regime_position_count": recent_regime},
                )
                continue
        max_bucket_regime_window = risk_config.get("max_positions_per_bucket_regime_window")
        if max_bucket_regime_window is not None and _int(max_bucket_regime_window) > 0:
            blocked_bucket = None
            blocked_count = 0
            for bucket in _buckets_for_symbol(symbol, risk_config):
                recent_bucket = _recent_position_count(
                    open_positions,
                    current_time=event_time,
                    window_minutes=_int(cluster_window, 0),
                    regime=regime,
                    bucket=bucket,
                )
                if recent_bucket >= _int(max_bucket_regime_window):
                    blocked_bucket = bucket
                    blocked_count = recent_bucket
                    break
            if blocked_bucket is not None:
                reject(
                    row,
                    f"bucket_regime_entry_cluster:{blocked_bucket['name']}:{regime}",
                    event_time=event_time,
                    details={"recent_bucket_regime_position_count": blocked_count},
                )
                continue
        reserved_risk = _reserved_risk(open_positions)
        remaining_risk = max(
            0.0,
            equity * _float(risk_config.get("max_open_risk_fraction"), 0.15) * risk_scale
            - reserved_risk,
        )
        per_trade_budget = equity * _float(row.get("production_risk_fraction"), 0.05) * risk_scale
        allocatable_risk = min(remaining_risk, per_trade_budget)
        limiting_reason: str | None = None
        per_symbol_cap = risk_config.get("max_open_risk_fraction_per_symbol")
        if per_symbol_cap is not None and _float(per_symbol_cap) > 0:
            symbol_remaining = max(
                0.0,
                equity * _float(per_symbol_cap) * risk_scale
                - _symbol_reserved_risk(open_positions, symbol),
            )
            if symbol_remaining < allocatable_risk:
                limiting_reason = "per_symbol_risk_cap"
            allocatable_risk = min(allocatable_risk, symbol_remaining)
        for bucket in _buckets_for_symbol(symbol, risk_config):
            bucket_remaining = max(
                0.0,
                equity * _float(bucket.get("max_open_risk_fraction")) * risk_scale
                - _bucket_reserved_risk(open_positions, bucket),
            )
            if bucket_remaining < allocatable_risk:
                limiting_reason = f"bucket_risk_cap:{bucket['name']}"
            allocatable_risk = min(allocatable_risk, bucket_remaining)
        quantity_by_risk = math.floor(allocatable_risk / risk_per_combo) if risk_per_combo > 0 else 0
        available_cash = max(0.0, equity - _reserved_debit(open_positions))
        quantity_by_cash = (
            math.floor(available_cash / debit_cash_per_combo)
            if debit_cash_per_combo > 0
            else _int(row.get("production_max_contracts"), 6)
        )
        quantity = min(
            _int(row.get("production_max_contracts"), 6),
            quantity_by_risk,
            quantity_by_cash,
        )
        if quantity < 1:
            if quantity_by_cash < 1:
                reason = "insufficient_cash"
            elif limiting_reason:
                reason = limiting_reason
            else:
                reason = "risk_budget_too_small"
            reject(
                row,
                reason,
                event_time=event_time,
                details={
                    "risk_scale": round(risk_scale, 6),
                    "drawdown_risk_scale": round(drawdown_risk_scale, 6),
                    "regime_risk_scale": round(regime_risk_scale, 6),
                    "remaining_risk": round(remaining_risk, 6),
                    "per_trade_budget": round(per_trade_budget, 6),
                    "allocatable_risk": round(allocatable_risk, 6),
                    "risk_per_combo": round(risk_per_combo, 6),
                    "quantity_by_risk": quantity_by_risk,
                    "quantity_by_cash": quantity_by_cash,
                },
            )
            continue
        risk_dollars = risk_per_combo * quantity
        debit_cash = debit_cash_per_combo * quantity
        open_positions[int(row_id)] = {
            "symbol": symbol,
            "regime": regime,
            "family": row.get("capital_plan_family") or row.get("family"),
            "entry_time": event_time,
            "strategy_key": strategy_key,
            "quantity": quantity,
            "risk_per_combo": risk_per_combo,
            "risk_dollars": risk_dollars,
            "debit_cash": debit_cash,
            "risk_scale": risk_scale,
            "drawdown_risk_scale": drawdown_risk_scale,
            "regime_risk_scale": regime_risk_scale,
        }
        daily_trade_count[trade_date] += 1
        daily_regimes.setdefault(trade_date, set()).add(regime)
        daily_symbols.setdefault(trade_date, set()).add(symbol)
        daily_families.setdefault(trade_date, set()).add(str(row.get("capital_plan_family") or row.get("family") or ""))
        risk_events.append(
            {
                "row_id": int(row_id),
                "event_time": str(event_time),
                "trade_date": str(trade_date),
                "decision": "accepted",
                "decision_reason": "risk_gates_clear",
                "symbol": symbol,
                "regime": regime,
                "candidate_variant_id": row.get("portfolio_candidate_variant_id"),
                "risk_fraction": _float(row.get("production_risk_fraction")),
                "max_contracts": _int(row.get("production_max_contracts")),
                "quantity": quantity,
                "risk_per_combo": round(risk_per_combo, 6),
                "risk_dollars": round(risk_dollars, 6),
                "debit_cash": round(debit_cash, 6),
                "risk_scale": round(risk_scale, 6),
                "drawdown_risk_scale": round(drawdown_risk_scale, 6),
                "regime_risk_scale": round(regime_risk_scale, 6),
                "equity": round(equity, 6),
                "open_position_count": len(open_positions),
                "reserved_risk_after_entry": round(_reserved_risk(open_positions), 6),
                "projected_fill_probability": row.get("projected_fill_probability"),
            }
        )

    active_dates = sorted(set(daily_pnl) | set(daily_trade_count) | set(daily_rejected_count))
    daily_rows: list[dict[str, Any]] = []
    curve_equity = initial_cash
    curve_peak = initial_cash
    for trade_date in active_dates:
        start_equity = curve_equity
        pnl = daily_pnl.get(trade_date, 0.0)
        curve_equity = max(start_equity + pnl, 0.0)
        curve_peak = max(curve_peak, curve_equity)
        drawdown = curve_equity - curve_peak
        daily_return = pnl / start_equity if start_equity > 0 else -1.0
        daily_rows.append(
            {
                "trade_date": str(trade_date),
                "starting_equity": round(start_equity, 6),
                "daily_pnl": round(pnl, 6),
                "daily_return": round(daily_return, 10),
                "ending_equity": round(curve_equity, 6),
                "peak_equity": round(curve_peak, 6),
                "drawdown": round(drawdown, 6),
                "drawdown_pct": round(drawdown / curve_peak, 10) if curve_peak > 0 else -1.0,
                "trade_count": int(daily_trade_count.get(trade_date, 0)),
                "rejected_trade_count": int(daily_rejected_count.get(trade_date, 0)),
                "active_trade_day": int(daily_trade_count.get(trade_date, 0)) > 0,
                "active_regimes": ",".join(sorted(daily_regimes.get(trade_date, set()))),
                "active_symbols": ",".join(sorted(daily_symbols.get(trade_date, set()))),
                "active_families": ",".join(sorted(item for item in daily_families.get(trade_date, set()) if item)),
            }
        )
    risk_events_frame = pd.DataFrame(risk_events)
    accepted_count = int((risk_events_frame.get("decision") == "accepted").sum()) if not risk_events_frame.empty else 0
    rejected_count = int((risk_events_frame.get("decision") == "rejected").sum()) if not risk_events_frame.empty else 0
    reason_counts = (
        risk_events_frame.loc[risk_events_frame["decision"].eq("rejected"), "decision_reason"]
        .value_counts()
        .to_dict()
        if not risk_events_frame.empty and "decision_reason" in risk_events_frame.columns
        else {}
    )
    sizing_counts = (
        selected_trades.get("production_strategy_sizing_mode", pd.Series(dtype=object))
        .fillna("unknown")
        .value_counts()
        .to_dict()
    )
    summary = {
        "mode": "production_runtime",
        "risk_config_source": risk_config.get("_source"),
        "enforce_broker_equity_floor": enforce_broker_equity_floor,
        "input_trade_count": int(len(selected_trades)),
        "accepted_trade_count": accepted_count,
        "closed_trade_count": int((risk_events_frame.get("decision") == "closed").sum()) if not risk_events_frame.empty else 0,
        "rejected_trade_count": rejected_count,
        "rejection_reason_counts": {str(key): int(value) for key, value in reason_counts.items()},
        "strategy_sizing_mode_counts": {str(key): int(value) for key, value in sizing_counts.items()},
        "risk_config": {
            key: value for key, value in risk_config.items() if not str(key).startswith("_")
        },
    }
    return pd.DataFrame(daily_rows), pd.DataFrame(trade_rows), risk_events_frame, summary


def _build_full_period_equity_curve(
    *,
    active_daily_curve: pd.DataFrame,
    calendar: pd.DataFrame,
    initial_cash: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if calendar.empty:
        return active_daily_curve.copy(), {
            "mode": "active_trade_days_only",
            "calendar_provided": False,
            "raw_dataset_trading_days": int(len(active_daily_curve)),
            "strategy_active_days": int(len(active_daily_curve)),
            "inactive_cash_days": 0,
            "active_day_coverage_pct": 100.0 if not active_daily_curve.empty else 0.0,
            "calendar_regime_label_source": "not_provided",
        }
    active_by_date = {
        pd.to_datetime(row["trade_date"]).date(): row
        for _, row in active_daily_curve.iterrows()
    }
    calendar_dates = set(calendar["trade_date"])
    extra_active_dates = sorted(set(active_by_date) - calendar_dates)
    rows: list[dict[str, Any]] = []
    equity = initial_cash
    peak = initial_cash
    active_days = 0
    for _, calendar_row in calendar.iterrows():
        trade_date = calendar_row["trade_date"]
        active = active_by_date.get(trade_date)
        start_equity = equity
        if active is None:
            daily_pnl = 0.0
            trade_count = 0
            active_trade_day = False
            active_regimes = ""
            active_symbols = ""
            active_families = ""
        else:
            daily_pnl = _float(active.get("daily_pnl"))
            trade_count = int(_float(active.get("trade_count")))
            active_trade_day = True
            active_days += 1
            active_regimes = str(active.get("active_regimes") or "")
            active_symbols = str(active.get("active_symbols") or "")
            active_families = str(active.get("active_families") or "")
        equity = max(start_equity + daily_pnl, 0.0)
        peak = max(peak, equity)
        drawdown = equity - peak
        daily_return = daily_pnl / start_equity if start_equity > 0 else -1.0
        rows.append(
            {
                "trade_date": str(trade_date),
                "calendar_regime": str(calendar_row.get("calendar_regime") or "unknown"),
                "starting_equity": round(start_equity, 6),
                "daily_pnl": round(daily_pnl, 6),
                "daily_return": round(daily_return, 10),
                "ending_equity": round(equity, 6),
                "peak_equity": round(peak, 6),
                "drawdown": round(drawdown, 6),
                "drawdown_pct": round(drawdown / peak, 10) if peak > 0 else -1.0,
                "trade_count": trade_count,
                "active_trade_day": active_trade_day,
                "active_regimes": active_regimes,
                "active_symbols": active_symbols,
                "active_families": active_families,
            }
        )
    raw_days = int(len(calendar))
    inactive_days = raw_days - active_days
    return pd.DataFrame(rows), {
        "mode": "full_calendar_cash_carry",
        "calendar_provided": True,
        "raw_dataset_trading_days": raw_days,
        "strategy_active_days": active_days,
        "inactive_cash_days": inactive_days,
        "active_day_coverage_pct": (
            round(active_days / raw_days * 100.0, 4) if raw_days else 0.0
        ),
        "extra_active_dates_not_in_calendar": [str(value) for value in extra_active_dates],
        "calendar_regime_label_source": (
            "calendar_csv" if set(calendar["calendar_regime"]) != {"unknown"} else "not_provided"
        ),
    }


def _split_labels(value: object) -> list[str]:
    if value is None:
        return []
    text = str(value)
    if not text or text == "nan":
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def _regime_coverage(
    *,
    daily_curve: pd.DataFrame,
    scaled_trades: pd.DataFrame,
    capital_plan: list[dict[str, Any]],
    projection_calendar: dict[str, Any],
) -> dict[str, Any]:
    required_regimes = ["bull", "bear", "choppy"]
    active_days_by_regime = {regime: 0 for regime in required_regimes}
    if not daily_curve.empty and "active_regimes" in daily_curve.columns:
        for _, row in daily_curve.iterrows():
            labels = set(_split_labels(row.get("active_regimes")))
            for regime in required_regimes:
                if regime in labels:
                    active_days_by_regime[regime] += 1
    trade_count_by_regime = {regime: 0 for regime in required_regimes}
    pnl_by_regime = {regime: 0.0 for regime in required_regimes}
    if not scaled_trades.empty and "intended_regime" in scaled_trades.columns:
        for regime, group in scaled_trades.groupby("intended_regime"):
            regime_key = str(regime)
            trade_count_by_regime[regime_key] = int(len(group))
            pnl_by_regime[regime_key] = round(float(group["scaled_option_pnl"].sum()), 6)
    capital_plan_count_by_regime = {regime: 0 for regime in required_regimes}
    for row in capital_plan:
        regime = str(row.get("intended_regime") or "")
        if regime:
            capital_plan_count_by_regime[regime] = capital_plan_count_by_regime.get(regime, 0) + 1
    required_status = {
        regime: {
            "covered": active_days_by_regime.get(regime, 0) > 0
            and capital_plan_count_by_regime.get(regime, 0) > 0,
            "capital_plan_count": capital_plan_count_by_regime.get(regime, 0),
            "active_days": active_days_by_regime.get(regime, 0),
            "trade_count": trade_count_by_regime.get(regime, 0),
            "scaled_pnl": round(pnl_by_regime.get(regime, 0.0), 6),
        }
        for regime in required_regimes
    }
    calendar_market_regime: dict[str, Any] = {}
    if (
        not daily_curve.empty
        and "calendar_regime" in daily_curve.columns
        and projection_calendar.get("calendar_regime_label_source") == "calendar_csv"
    ):
        for regime, group in daily_curve.groupby("calendar_regime"):
            calendar_market_regime[str(regime)] = {
                "calendar_days": int(len(group)),
                "active_days": int(group["active_trade_day"].astype(bool).sum()),
                "inactive_days": int((~group["active_trade_day"].astype(bool)).sum()),
                "scaled_pnl": round(float(group["daily_pnl"].sum()), 6),
            }
    return {
        "required_regimes": required_regimes,
        "required_regime_status": required_status,
        "all_required_regimes_covered": all(
            item["covered"] for item in required_status.values()
        ),
        "active_days_by_strategy_regime": active_days_by_regime,
        "trade_count_by_strategy_regime": trade_count_by_regime,
        "scaled_pnl_by_strategy_regime": pnl_by_regime,
        "capital_plan_count_by_strategy_regime": capital_plan_count_by_regime,
        "calendar_market_regime_coverage": calendar_market_regime,
        "projection_calendar": projection_calendar,
    }


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


def _candidate_group_columns(frame: pd.DataFrame) -> list[str]:
    return [
        column
        for column in [
            "base_candidate_variant_id",
            "candidate_variant_id",
            "aggregate_profile",
            "symbol",
            "family",
            "intended_regime",
        ]
        if column in frame.columns
    ]


def _train_test_optimization_report(
    *,
    scaled_trades: pd.DataFrame,
    train_end_date: str | None,
    min_train_trades: int,
    min_test_trades: int,
    max_per_symbol_regime: int | None,
) -> dict[str, Any]:
    if not train_end_date:
        return {
            "status": "not_configured",
            "candidate_count": 0,
            "eligible_candidate_count": 0,
            "selected_candidate_count": 0,
        }
    if scaled_trades.empty:
        return {
            "status": "enabled_no_trades",
            "train_end_date": train_end_date,
            "candidate_count": 0,
            "eligible_candidate_count": 0,
            "selected_candidate_count": 0,
        }
    trades = scaled_trades.copy()
    trades["trade_date_dt"] = pd.to_datetime(trades["trade_date"], errors="coerce").dt.date
    split_date = pd.to_datetime(train_end_date, errors="raise").date()
    group_columns = _candidate_group_columns(trades)
    if not group_columns:
        return {
            "status": "enabled_no_candidate_columns",
            "train_end_date": train_end_date,
            "candidate_count": 0,
            "eligible_candidate_count": 0,
            "selected_candidate_count": 0,
        }
    rows: list[dict[str, Any]] = []
    for key, group in trades.groupby(group_columns, dropna=False):
        key_values = key if isinstance(key, tuple) else (key,)
        identity = {column: value for column, value in zip(group_columns, key_values)}
        train = group[group["trade_date_dt"] <= split_date]
        test = group[group["trade_date_dt"] > split_date]
        train_trades = int(len(train))
        test_trades = int(len(test))
        train_pnl = float(train["scaled_option_pnl"].sum()) if train_trades else 0.0
        test_pnl = float(test["scaled_option_pnl"].sum()) if test_trades else 0.0
        reason_codes = []
        if train_trades < min_train_trades:
            reason_codes.append("min_train_trades")
        if test_trades < min_test_trades:
            reason_codes.append("min_test_trades")
        if train_pnl <= 0:
            reason_codes.append("train_pnl_not_positive")
        if test_pnl <= 0:
            reason_codes.append("test_pnl_not_positive")
        rows.append(
            {
                **identity,
                "train_trades": train_trades,
                "test_trades": test_trades,
                "total_trades": train_trades + test_trades,
                "train_scaled_pnl": round(train_pnl, 6),
                "test_scaled_pnl": round(test_pnl, 6),
                "total_scaled_pnl": round(train_pnl + test_pnl, 6),
                "train_expectancy": round(train_pnl / train_trades, 6)
                if train_trades
                else None,
                "test_expectancy": round(test_pnl / test_trades, 6) if test_trades else None,
                "eligible": not reason_codes,
                "blockers": ",".join(reason_codes),
            }
        )
    candidate_frame = pd.DataFrame(rows)
    eligible = candidate_frame[candidate_frame["eligible"].astype(bool)].copy()
    selected = eligible.copy()
    if max_per_symbol_regime is not None and max_per_symbol_regime > 0 and not eligible.empty:
        selected = (
            eligible.sort_values(
                ["symbol", "intended_regime", "train_scaled_pnl", "test_scaled_pnl"],
                ascending=[True, True, False, False],
            )
            .groupby(["symbol", "intended_regime"], dropna=False)
            .head(max_per_symbol_regime)
            .reset_index(drop=True)
        )
    reason_counts = Counter()
    for blockers in candidate_frame.get("blockers", pd.Series(dtype=object)).fillna(""):
        for blocker in str(blockers).split(","):
            if blocker:
                reason_counts[blocker] += 1
    return {
        "status": "enabled",
        "train_end_date": train_end_date,
        "min_train_trades": int(min_train_trades),
        "min_test_trades": int(min_test_trades),
        "max_per_symbol_regime": max_per_symbol_regime,
        "candidate_count": int(len(candidate_frame)),
        "eligible_candidate_count": int(len(eligible)),
        "selected_candidate_count": int(len(selected)),
        "blocker_counts": dict(sorted(reason_counts.items())),
        "candidate_rows": candidate_frame.to_dict(orient="records"),
        "selected_candidate_rows": selected.to_dict(orient="records"),
    }


def _share_threshold(value: float | None) -> float | None:
    if value is None:
        return None
    parsed = _normalize_share(value)
    if math.isnan(parsed):
        return None
    return min(max(parsed, 0.0), 1.0)


def _top_share(rows: list[dict[str, Any]], key: str) -> float:
    if not rows:
        return 0.0
    return max(_float(row.get(key)) for row in rows)


def _distribution_by_column(
    *, frame: pd.DataFrame, column: str, pnl_column: str = "scaled_option_pnl"
) -> list[dict[str, Any]]:
    if frame.empty or column not in frame.columns:
        return []
    total_trades = max(len(frame), 1)
    total_abs_pnl = float(frame[pnl_column].abs().sum()) if pnl_column in frame.columns else 0.0
    rows = []
    for value, group in frame.groupby(column, dropna=False):
        pnl = float(group[pnl_column].sum()) if pnl_column in group.columns else 0.0
        abs_pnl = float(group[pnl_column].abs().sum()) if pnl_column in group.columns else 0.0
        rows.append(
            {
                str(column): str(value),
                "trade_count": int(len(group)),
                "trade_share": round(len(group) / total_trades, 6),
                "scaled_pnl": round(pnl, 6),
                "abs_pnl_share": round(abs_pnl / total_abs_pnl, 6) if total_abs_pnl > 0 else 0.0,
            }
        )
    return sorted(rows, key=lambda row: row["trade_count"], reverse=True)


def _diversification_report(
    *,
    scaled_trades: pd.DataFrame,
    min_symbols: int | None,
    min_regimes: int | None,
    min_families: int | None,
    max_symbol_trade_share: float | None,
    max_family_trade_share: float | None,
    max_regime_trade_share: float | None,
    max_symbol_pnl_share: float | None,
) -> dict[str, Any]:
    thresholds = {
        "min_symbols": min_symbols,
        "min_regimes": min_regimes,
        "min_families": min_families,
        "max_symbol_trade_share": _share_threshold(max_symbol_trade_share),
        "max_family_trade_share": _share_threshold(max_family_trade_share),
        "max_regime_trade_share": _share_threshold(max_regime_trade_share),
        "max_symbol_pnl_share": _share_threshold(max_symbol_pnl_share),
    }
    configured = any(value is not None for value in thresholds.values())
    symbol_rows = _distribution_by_column(frame=scaled_trades, column="symbol")
    regime_rows = _distribution_by_column(frame=scaled_trades, column="intended_regime")
    family_rows = _distribution_by_column(frame=scaled_trades, column="family")
    symbol_count = len(symbol_rows)
    regime_count = len(regime_rows)
    family_count = len(family_rows)
    failures: list[str] = []
    if min_symbols is not None and symbol_count < min_symbols:
        failures.append("min_symbols")
    if min_regimes is not None and regime_count < min_regimes:
        failures.append("min_regimes")
    if min_families is not None and family_count < min_families:
        failures.append("min_families")
    max_symbol_trade = _top_share(symbol_rows, "trade_share")
    max_family_trade = _top_share(family_rows, "trade_share")
    max_regime_trade = _top_share(regime_rows, "trade_share")
    max_symbol_abs_pnl = _top_share(symbol_rows, "abs_pnl_share")
    if thresholds["max_symbol_trade_share"] is not None and max_symbol_trade > thresholds["max_symbol_trade_share"]:
        failures.append("max_symbol_trade_share")
    if thresholds["max_family_trade_share"] is not None and max_family_trade > thresholds["max_family_trade_share"]:
        failures.append("max_family_trade_share")
    if thresholds["max_regime_trade_share"] is not None and max_regime_trade > thresholds["max_regime_trade_share"]:
        failures.append("max_regime_trade_share")
    if thresholds["max_symbol_pnl_share"] is not None and max_symbol_abs_pnl > thresholds["max_symbol_pnl_share"]:
        failures.append("max_symbol_pnl_share")
    status = "not_configured"
    if configured:
        status = "failed" if failures else "passed"
    return {
        "status": status,
        "thresholds": thresholds,
        "failure_reasons": failures,
        "trade_count": int(len(scaled_trades)),
        "symbol_count": symbol_count,
        "regime_count": regime_count,
        "family_count": family_count,
        "max_symbol_trade_share": round(max_symbol_trade, 6),
        "max_family_trade_share": round(max_family_trade, 6),
        "max_regime_trade_share": round(max_regime_trade, 6),
        "max_symbol_abs_pnl_share": round(max_symbol_abs_pnl, 6),
        "symbol_distribution": symbol_rows[:50],
        "regime_distribution": regime_rows[:50],
        "family_distribution": family_rows[:50],
    }


def _evidence_grade(
    *,
    daily_curve: pd.DataFrame,
    historical: dict[str, Any],
    projection: dict[str, Any],
    capital_plan: list[dict[str, Any]],
    regime_coverage: dict[str, Any] | None = None,
    hardening_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not capital_plan:
        blockers.append("no_capital_plan")
    if daily_curve.empty:
        blockers.append("no_matched_trade_economics")
    elif len(daily_curve) < 250:
        warnings.append("less_than_250_trading_days")
    if regime_coverage:
        projection_calendar = regime_coverage.get("projection_calendar") or {}
        if projection_calendar.get("calendar_provided"):
            inactive_days = int(_float(projection_calendar.get("inactive_cash_days")))
            if inactive_days > 0:
                warnings.append("portfolio_has_inactive_cash_days")
        for regime, item in (regime_coverage.get("required_regime_status") or {}).items():
            if not item.get("covered"):
                blockers.append(f"missing_{regime}_strategy_coverage")
        if projection_calendar.get("calendar_regime_label_source") == "not_provided":
            warnings.append("calendar_market_regime_labels_not_provided")
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
    if hardening_summary:
        match_coverage = hardening_summary.get("match_coverage") or {}
        if int(_float(match_coverage.get("unmatched_capital_plan_count"))) > 0:
            warnings.append("unmatched_capital_plan_strategies")
        market_stress = hardening_summary.get("market_quality_stress") or {}
        if int(_float(market_stress.get("rejected_trade_count"))) > 0:
            warnings.append("market_quality_stress_rejected_trades")
        fill_model = hardening_summary.get("fill_probability_model") or {}
        if int(_float(fill_model.get("rejected_trade_count"))) > 0:
            warnings.append("fill_probability_model_rejected_trades")
        train_test = hardening_summary.get("train_test_optimization") or {}
        if train_test.get("status") == "enabled" and int(_float(train_test.get("selected_candidate_count"))) == 0:
            warnings.append("train_test_no_candidate_survived")
        diversification = hardening_summary.get("diversification_constraints") or {}
        if diversification.get("status") == "failed":
            blockers.append("diversification_constraints_failed")
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
    active_historical = packet.get("active_day_historical_metrics", {})
    projection = packet["bootstrap_projection"]
    grade = packet["evidence_grade"]
    projection_calendar = packet.get("projection_calendar", {})
    regime_coverage = packet.get("regime_coverage", {})
    capital_plan_merge = packet.get("capital_plan_merge", {})
    production_risk = packet.get("production_risk_simulation") or {}
    projection_hardening = packet.get("projection_hardening") or {}
    lines = [
        "# Portfolio Growth Projection",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Scope: `{packet['scope']}`",
        f"- Broker facing: `{str(packet['broker_facing']).lower()}`",
        f"- Live manifest effect: `{packet['live_manifest_effect']}`",
        f"- Risk policy effect: `{packet['risk_policy_effect']}`",
        f"- Risk simulation mode: `{packet.get('risk_simulation_mode')}`",
        f"- Initial cash: `${packet['initial_cash']}`",
        f"- Target equity: `${packet['target_equity']}`",
        f"- Evidence grade: `{grade['grade']}`",
        "",
        "## Capital Plan Merge",
        "",
        f"- Merge mode: `{capital_plan_merge.get('mode')}`",
        f"- Maximum symbol weight: `{capital_plan_merge.get('max_symbol_weight')}`",
        f"- Original allocated weight: `{capital_plan_merge.get('original_allocated_weight')}`",
        f"- Reweighted allocated weight: `{capital_plan_merge.get('reweighted_allocated_weight')}`",
        f"- Original symbol weights: `{capital_plan_merge.get('original_symbol_weights')}`",
        f"- Reweighted symbol weights: `{capital_plan_merge.get('reweighted_symbol_weights')}`",
        "",
        "## Production Risk Simulation",
        "",
    ]
    if not production_risk:
        lines.append("- Mode: `capital_plan`; runtime risk gates were not simulated.")
    else:
        lines.extend(
            [
                f"- Mode: `{production_risk.get('mode')}`",
                f"- Risk config source: `{production_risk.get('risk_config_source')}`",
                f"- Enforce broker equity floor: `{production_risk.get('enforce_broker_equity_floor')}`",
                f"- Input trades: `{production_risk.get('input_trade_count')}`",
                f"- Accepted entries: `{production_risk.get('accepted_trade_count')}`",
                f"- Rejected entries: `{production_risk.get('rejected_trade_count')}`",
                f"- Rejection reasons: `{production_risk.get('rejection_reason_counts')}`",
                f"- Strategy sizing modes: `{production_risk.get('strategy_sizing_mode_counts')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Projection Hardening",
            "",
        ]
    )
    match_coverage = projection_hardening.get("match_coverage") or {}
    market_stress = projection_hardening.get("market_quality_stress") or {}
    market_diagnostics = projection_hardening.get("market_quality_diagnostics") or {}
    market_cost = projection_hardening.get("market_quality_pnl_cost_model") or {}
    fill_model = projection_hardening.get("fill_probability_model") or {}
    fill_haircut = projection_hardening.get("fill_probability_pnl_haircut") or {}
    train_test = projection_hardening.get("train_test_optimization") or {}
    diversification = projection_hardening.get("diversification_constraints") or {}
    lines.extend(
        [
            f"- Capital-plan match rate: `{match_coverage.get('match_rate_pct')}%`",
            f"- Unmatched capital-plan strategies: `{match_coverage.get('unmatched_capital_plan_count')}`",
            f"- Market-quality stress status: `{market_stress.get('status')}`",
            f"- Market-quality stress rejected trades: `{market_stress.get('rejected_trade_count')}`",
            f"- Market-quality diagnostics status: `{market_diagnostics.get('status')}`",
            f"- Rows with any no-bid-ask source: `{market_diagnostics.get('rows_with_any_no_bid_ask_quote_source')}`",
            f"- Trade-print coverage: `{(market_diagnostics.get('trade_print_coverage') or {}).get('coverage_pct')}%`",
            f"- Market-quality PnL cost status: `{market_cost.get('status')}`",
            f"- Market-quality PnL cost: `${market_cost.get('total_market_quality_pnl_cost', 0.0)}`",
            f"- Fill-probability model status: `{fill_model.get('status')}`",
            f"- Fill-probability rejected trades: `{fill_model.get('rejected_trade_count')}`",
            f"- Fill-probability PnL haircut status: `{fill_haircut.get('status')}`",
            f"- Fill-probability PnL haircut amount: `${fill_haircut.get('total_positive_pnl_haircut', 0.0)}`",
            f"- Train/test optimization status: `{train_test.get('status')}`",
            f"- Train/test eligible candidates: `{train_test.get('eligible_candidate_count')}`",
            f"- Train/test selected candidates: `{train_test.get('selected_candidate_count')}`",
            f"- Diversification status: `{diversification.get('status')}`",
            f"- Diversification failures: `{diversification.get('failure_reasons')}`",
            "",
            "## Full-Year Calendar Coverage",
            "",
            f"- Calendar mode: `{projection_calendar.get('mode')}`",
            f"- Raw dataset trading days: `{projection_calendar.get('raw_dataset_trading_days')}`",
            f"- Strategy active days: `{projection_calendar.get('strategy_active_days')}`",
            f"- Inactive cash days: `{projection_calendar.get('inactive_cash_days')}`",
            f"- Active-day coverage: `{projection_calendar.get('active_day_coverage_pct')}%`",
            f"- Calendar market-regime labels: `{projection_calendar.get('calendar_regime_label_source')}`",
            "",
            "## Historical Compounded Curve",
            "",
        ]
    )
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
    if active_historical:
        lines.extend(["", "## Active-Day-Only Comparison", ""])
        for key in [
            "starting_date",
            "ending_date",
            "trading_days",
            "ending_equity",
            "total_return_pct",
            "cagr_pct",
            "max_drawdown",
            "max_drawdown_pct",
        ]:
            lines.append(f"- `{key}`: `{active_historical.get(key)}`")
    lines.extend(["", "## Strategy Regime Coverage", ""])
    required_status = regime_coverage.get("required_regime_status") or {}
    if not required_status:
        lines.append("- No regime coverage summary available.")
    else:
        lines.append("| Strategy Regime | Covered | Capital Plan Count | Active Days | Trades | Scaled PnL |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: |")
        for regime, row in required_status.items():
            lines.append(
                f"| `{regime}` | `{str(row.get('covered')).lower()}` | "
                f"{row.get('capital_plan_count')} | {row.get('active_days')} | "
                f"{row.get('trade_count')} | `${row.get('scaled_pnl')}` |"
            )
    market_regimes = regime_coverage.get("calendar_market_regime_coverage") or {}
    if market_regimes:
        lines.extend(["", "## Calendar Market-Regime Coverage", ""])
        lines.append("| Market Regime | Calendar Days | Active Days | Inactive Days | Scaled PnL |")
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        for regime, row in market_regimes.items():
            lines.append(
                f"| `{regime}` | {row.get('calendar_days')} | {row.get('active_days')} | "
                f"{row.get('inactive_days')} | `${row.get('scaled_pnl')}` |"
            )
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
    calendar_csv: Path | None = None,
    calendar_date_column: str = "trade_date",
    additional_portfolio_report_jsons: list[Path] | None = None,
    additional_replay_roots: list[Path] | None = None,
    max_symbol_weight: float | None = None,
    risk_simulation_mode: str = "capital_plan",
    production_risk_config_yaml: Path | None = None,
    production_strategy_manifest_yamls: list[Path] | None = None,
    production_default_risk_fraction: float = 0.05,
    production_default_max_contracts: int = 6,
    production_enforce_broker_equity_floor: bool = False,
    optimization_train_end_date: str | None = None,
    optimization_min_train_trades: int = 5,
    optimization_min_test_trades: int = 5,
    optimization_max_per_symbol_regime: int | None = None,
    stress_max_entry_spread_pct: float | None = None,
    stress_max_exit_spread_pct: float | None = None,
    stress_max_entry_quote_age_seconds: float | None = None,
    stress_max_exit_quote_age_seconds: float | None = None,
    market_quality_cost_model_enabled: bool = False,
    market_quality_no_bid_ask_spread_pct: float = 0.12,
    market_quality_zero_print_spread_pct: float = 0.08,
    market_quality_unknown_spread_pct: float = 0.10,
    market_quality_max_spread_pct: float = 0.50,
    market_quality_quote_age_grace_seconds: float = 5.0,
    market_quality_quote_age_spread_pct_per_minute: float = 0.02,
    fill_model_enabled: bool = False,
    min_fill_probability: float | None = None,
    unknown_fill_probability: float = 1.0,
    fill_model_haircut_positive_pnl: bool = False,
    diversification_min_symbols: int | None = None,
    diversification_min_regimes: int | None = None,
    diversification_min_families: int | None = None,
    diversification_max_symbol_trade_share: float | None = None,
    diversification_max_family_trade_share: float | None = None,
    diversification_max_regime_trade_share: float | None = None,
    diversification_max_symbol_pnl_share: float | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if risk_simulation_mode not in {"capital_plan", "production_runtime"}:
        raise ValueError(f"Unsupported risk_simulation_mode: {risk_simulation_mode}")
    portfolio_report_jsons = [portfolio_report_json] + list(additional_portfolio_report_jsons or [])
    replay_roots = [replay_root] + list(additional_replay_roots or [])
    strategy_sizing_by_key, strategy_sizing_summary = _load_strategy_sizing(
        list(production_strategy_manifest_yamls or [])
    )
    capital_plan, capital_plan_merge = _merge_capital_plans(
        portfolio_report_jsons=portfolio_report_jsons,
        initial_cash=initial_cash,
        max_symbol_weight=max_symbol_weight,
    )
    trades = _load_trade_economics_roots(
        replay_roots,
        candidate_filters=_capital_plan_trade_filters(capital_plan),
    )
    calendar = _load_projection_calendar(
        calendar_csv=calendar_csv,
        date_column=calendar_date_column,
    )
    selected_trades = _select_capital_plan_trades(
        trades=trades,
        capital_plan=capital_plan,
        initial_cash=initial_cash,
        backtest_allocation_fraction=backtest_allocation_fraction,
        strategy_sizing_by_key=strategy_sizing_by_key,
        default_risk_fraction=production_default_risk_fraction,
        default_max_contracts=production_default_max_contracts,
    )
    match_coverage = _capital_plan_match_coverage(
        capital_plan=capital_plan,
        selected_trades=selected_trades,
    )
    market_rejections = pd.DataFrame()
    fill_rejections = pd.DataFrame()
    selected_trades, market_rejections, market_quality_stress = _apply_market_quality_stress(
        selected_trades=selected_trades,
        max_entry_spread_pct=stress_max_entry_spread_pct,
        max_exit_spread_pct=stress_max_exit_spread_pct,
        max_entry_quote_age_seconds=stress_max_entry_quote_age_seconds,
        max_exit_quote_age_seconds=stress_max_exit_quote_age_seconds,
    )
    selected_trades, market_quality_pnl_cost_model = _apply_market_quality_pnl_cost_model(
        selected_trades=selected_trades,
        enabled=market_quality_cost_model_enabled,
        no_bid_ask_spread_pct=market_quality_no_bid_ask_spread_pct,
        zero_print_spread_pct=market_quality_zero_print_spread_pct,
        unknown_spread_pct=market_quality_unknown_spread_pct,
        max_spread_pct=market_quality_max_spread_pct,
        quote_age_grace_seconds=market_quality_quote_age_grace_seconds,
        quote_age_spread_pct_per_minute=market_quality_quote_age_spread_pct_per_minute,
    )
    selected_trades, fill_rejections, fill_probability_model = _apply_fill_probability_model(
        selected_trades=selected_trades,
        enabled=fill_model_enabled,
        min_fill_probability=min_fill_probability,
        unknown_fill_probability=unknown_fill_probability,
    )
    selected_trades, fill_probability_pnl_haircut = _apply_fill_probability_pnl_haircut(
        selected_trades=selected_trades,
        enabled=fill_model_haircut_positive_pnl,
    )
    market_quality_diagnostics = _market_quality_diagnostics(selected_trades)
    hardening_rejections = pd.concat(
        [frame for frame in [market_rejections, fill_rejections] if not frame.empty],
        ignore_index=True,
    ) if not market_rejections.empty or not fill_rejections.empty else pd.DataFrame()
    risk_events = pd.DataFrame()
    production_risk_summary: dict[str, Any] | None = None
    if risk_simulation_mode == "production_runtime":
        production_risk_config = _load_production_risk_config(
            production_risk_config_yaml or DEFAULT_PRODUCTION_RISK_CONFIG
        )
        active_daily_curve, scaled_trades, risk_events, production_risk_summary = (
            _build_daily_equity_production_runtime(
                selected_trades=selected_trades,
                initial_cash=initial_cash,
                risk_config=production_risk_config,
                enforce_broker_equity_floor=production_enforce_broker_equity_floor,
            )
        )
        production_risk_summary["strategy_sizing_sources"] = strategy_sizing_summary
        production_risk_summary["default_risk_fraction"] = production_default_risk_fraction
        production_risk_summary["default_max_contracts"] = production_default_max_contracts
    else:
        active_daily_curve, scaled_trades = _build_daily_equity(
            selected_trades=selected_trades,
            initial_cash=initial_cash,
            backtest_allocation_fraction=backtest_allocation_fraction,
        )
    daily_curve, projection_calendar = _build_full_period_equity_curve(
        active_daily_curve=active_daily_curve,
        calendar=calendar,
        initial_cash=initial_cash,
    )
    historical = _historical_metrics(
        daily_curve=daily_curve,
        initial_cash=initial_cash,
        target_equity=target_equity,
        annual_trading_days=annual_trading_days,
    )
    active_day_historical = _historical_metrics(
        daily_curve=active_daily_curve,
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
    regime_coverage = _regime_coverage(
        daily_curve=daily_curve,
        scaled_trades=scaled_trades,
        capital_plan=capital_plan,
        projection_calendar=projection_calendar,
    )
    train_test_optimization = _train_test_optimization_report(
        scaled_trades=scaled_trades,
        train_end_date=optimization_train_end_date,
        min_train_trades=optimization_min_train_trades,
        min_test_trades=optimization_min_test_trades,
        max_per_symbol_regime=optimization_max_per_symbol_regime,
    )
    diversification_constraints = _diversification_report(
        scaled_trades=scaled_trades,
        min_symbols=diversification_min_symbols,
        min_regimes=diversification_min_regimes,
        min_families=diversification_min_families,
        max_symbol_trade_share=diversification_max_symbol_trade_share,
        max_family_trade_share=diversification_max_family_trade_share,
        max_regime_trade_share=diversification_max_regime_trade_share,
        max_symbol_pnl_share=diversification_max_symbol_pnl_share,
    )
    projection_hardening = {
        "match_coverage": match_coverage,
        "market_quality_stress": market_quality_stress,
        "market_quality_diagnostics": market_quality_diagnostics,
        "market_quality_pnl_cost_model": market_quality_pnl_cost_model,
        "fill_probability_model": fill_probability_model,
        "fill_probability_pnl_haircut": fill_probability_pnl_haircut,
        "train_test_optimization": {
            key: value
            for key, value in train_test_optimization.items()
            if key not in {"candidate_rows", "selected_candidate_rows"}
        },
        "diversification_constraints": diversification_constraints,
    }
    evidence_grade = _evidence_grade(
        daily_curve=daily_curve,
        historical=historical,
        projection=projection,
        capital_plan=capital_plan,
        regime_coverage=regime_coverage,
        hardening_summary=projection_hardening,
    )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "portfolio_growth_projection_complete",
        "scope": "research_only_compounded_portfolio_projection",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "portfolio_report_json": str(portfolio_report_json),
        "portfolio_report_jsons": [str(path) for path in portfolio_report_jsons],
        "replay_root": str(replay_root),
        "replay_roots": [str(path) for path in replay_roots],
        "initial_cash": initial_cash,
        "target_equity": target_equity,
        "backtest_allocation_fraction": backtest_allocation_fraction,
        "max_symbol_weight": max_symbol_weight,
        "risk_simulation_mode": risk_simulation_mode,
        "production_risk_simulation": production_risk_summary,
        "projection_hardening": projection_hardening,
        "capital_plan_merge": capital_plan_merge,
        "capital_plan_count": len(capital_plan),
        "matched_trade_count": int(len(selected_trades)),
        "accepted_trade_count": int(len(scaled_trades)),
        "matched_daily_count": int(len(active_daily_curve)),
        "full_year_daily_count": int(len(daily_curve)),
        "projection_calendar": projection_calendar,
        "regime_coverage": regime_coverage,
        "historical_metrics": historical,
        "active_day_historical_metrics": active_day_historical,
        "bootstrap_projection": projection,
        "evidence_grade": evidence_grade,
        "capital_plan": capital_plan,
    }
    (output_dir / "portfolio_growth_projection.json").write_text(
        json.dumps(packet, indent=2, sort_keys=True), encoding="utf-8"
    )
    if not daily_curve.empty:
        daily_curve.to_csv(output_dir / "portfolio_growth_equity_curve.csv", index=False)
    if not active_daily_curve.empty:
        active_daily_curve.to_csv(
            output_dir / "portfolio_growth_active_day_equity_curve.csv",
            index=False,
        )
    if not scaled_trades.empty:
        scaled_trades.to_csv(output_dir / "portfolio_growth_scaled_trades.csv", index=False)
    if not risk_events.empty:
        risk_events.to_csv(output_dir / "portfolio_growth_risk_events.csv", index=False)
    if not hardening_rejections.empty:
        hardening_rejections.to_csv(
            output_dir / "portfolio_growth_hardening_rejections.csv",
            index=False,
        )
    unmatched_rows = match_coverage.get("unmatched_rows") or []
    if unmatched_rows:
        pd.DataFrame(unmatched_rows).to_csv(
            output_dir / "portfolio_growth_unmatched_capital_plan.csv",
            index=False,
        )
    train_test_rows = train_test_optimization.get("candidate_rows") or []
    if train_test_rows:
        pd.DataFrame(train_test_rows).to_csv(
            output_dir / "portfolio_growth_train_test_candidates.csv",
            index=False,
        )
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
        calendar_csv=Path(args.calendar_csv) if args.calendar_csv else None,
        calendar_date_column=args.calendar_date_column,
        additional_portfolio_report_jsons=[
            Path(path) for path in args.additional_portfolio_report_json
        ],
        additional_replay_roots=[Path(path) for path in args.additional_replay_root],
        max_symbol_weight=args.max_symbol_weight,
        risk_simulation_mode=args.risk_simulation_mode,
        production_risk_config_yaml=Path(args.production_risk_config_yaml)
        if args.production_risk_config_yaml
        else None,
        production_strategy_manifest_yamls=[
            Path(path) for path in args.production_strategy_manifest_yaml
        ],
        production_default_risk_fraction=args.production_default_risk_fraction,
        production_default_max_contracts=args.production_default_max_contracts,
        production_enforce_broker_equity_floor=args.production_enforce_broker_equity_floor,
        optimization_train_end_date=args.optimization_train_end_date,
        optimization_min_train_trades=args.optimization_min_train_trades,
        optimization_min_test_trades=args.optimization_min_test_trades,
        optimization_max_per_symbol_regime=args.optimization_max_per_symbol_regime,
        stress_max_entry_spread_pct=args.stress_max_entry_spread_pct,
        stress_max_exit_spread_pct=args.stress_max_exit_spread_pct,
        stress_max_entry_quote_age_seconds=args.stress_max_entry_quote_age_seconds,
        stress_max_exit_quote_age_seconds=args.stress_max_exit_quote_age_seconds,
        market_quality_cost_model_enabled=args.market_quality_cost_model_enabled,
        market_quality_no_bid_ask_spread_pct=args.market_quality_no_bid_ask_spread_pct,
        market_quality_zero_print_spread_pct=args.market_quality_zero_print_spread_pct,
        market_quality_unknown_spread_pct=args.market_quality_unknown_spread_pct,
        market_quality_max_spread_pct=args.market_quality_max_spread_pct,
        market_quality_quote_age_grace_seconds=args.market_quality_quote_age_grace_seconds,
        market_quality_quote_age_spread_pct_per_minute=args.market_quality_quote_age_spread_pct_per_minute,
        fill_model_enabled=args.fill_model_enabled,
        min_fill_probability=args.min_fill_probability,
        unknown_fill_probability=args.unknown_fill_probability,
        fill_model_haircut_positive_pnl=args.fill_model_haircut_positive_pnl,
        diversification_min_symbols=args.diversification_min_symbols,
        diversification_min_regimes=args.diversification_min_regimes,
        diversification_min_families=args.diversification_min_families,
        diversification_max_symbol_trade_share=args.diversification_max_symbol_trade_share,
        diversification_max_family_trade_share=args.diversification_max_family_trade_share,
        diversification_max_regime_trade_share=args.diversification_max_regime_trade_share,
        diversification_max_symbol_pnl_share=args.diversification_max_symbol_pnl_share,
    )
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

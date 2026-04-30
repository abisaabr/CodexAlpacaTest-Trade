from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.options.order_templates import (
    OptionLegTemplate,
    OptionStrategyTemplate,
    qqq_option_native_templates,
)
from scripts.run_option_aware_research_backtest import (
    CONTRACT_SELECTION_LIQUIDITY_FIRST,
    CONTRACT_SELECTION_NEAREST,
    _build_option_research_index,
    _first_option_bar,
    _load_option_inputs,
    _load_stock_bars,
    _metadata_json,
)

DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "qqq_option_native_tournament"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a research-only QQQ option-native regime tournament."
    )
    parser.add_argument("--stock-bars-path", required=True)
    parser.add_argument("--selected-contracts-root", required=True)
    parser.add_argument("--option-bars-root", required=True)
    parser.add_argument("--option-trades-root", default=None)
    parser.add_argument("--regime-labels-csv", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id", default="qqq_option_native_tournament")
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument(
        "--regime-filter",
        default=None,
        help="Optional comma-separated regime allowlist, e.g. bull,bear,choppy.",
    )
    parser.add_argument("--entry-offset-minutes", type=int, default=30)
    parser.add_argument("--exit-offset-minutes", type=int, default=210)
    parser.add_argument(
        "--entry-timing-mode",
        choices=["fixed_offset", "first_common_within_cutoff"],
        default="fixed_offset",
        help=(
            "fixed_offset enters at the exact offset. first_common_within_cutoff waits "
            "until every leg has an entry bar by the offset and reports skipped opportunity "
            "coverage separately."
        ),
    )
    parser.add_argument("--max-entry-lag-minutes", type=int, default=5)
    parser.add_argument("--max-exit-lag-minutes", type=int, default=10)
    parser.add_argument("--test-date-count", type=int, default=20)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--allocation-fraction", type=float, default=0.05)
    parser.add_argument("--slippage-bps", type=float, default=10.0)
    parser.add_argument("--fee-per-contract", type=float, default=0.65)
    parser.add_argument(
        "--contract-selection-method",
        choices=[CONTRACT_SELECTION_NEAREST, CONTRACT_SELECTION_LIQUIDITY_FIRST],
        default=CONTRACT_SELECTION_NEAREST,
        help=(
            "Research-only contract selector. nearest_contract preserves the template "
            "target. entry_liquidity_first_research_only chooses the nearest template "
            "target with an entry-window bar and is not broker-facing."
        ),
    )
    return parser.parse_args()


def _regime_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    regimes = {item.strip().lower() for item in value.split(",") if item.strip()}
    return regimes or None


def _load_regime_labels(path: Path, symbol: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if frame.empty:
        return frame
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    if "symbol" in frame.columns:
        frame = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    frame["regime"] = frame["regime"].astype(str).str.lower()
    return frame


def _session_times(stock_bars: pd.DataFrame, symbol: str) -> dict[Any, tuple[pd.Timestamp, pd.Timestamp]]:
    if stock_bars.empty:
        return {}
    frame = stock_bars.copy()
    if "symbol" in frame.columns:
        frame = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if frame.empty:
        return {}
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["trade_date"] = frame["timestamp"].dt.date
    sessions: dict[Any, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for trade_date, group in frame.groupby("trade_date", sort=True):
        sessions[trade_date] = (group["timestamp"].min(), group["timestamp"].max())
    return sessions


def _candidate_contracts_for_leg(
    contracts: pd.DataFrame,
    *,
    symbol: str,
    trade_date: Any,
    leg: OptionLegTemplate,
) -> pd.DataFrame:
    if contracts.empty:
        return pd.DataFrame()
    frame = contracts[
        (contracts["underlying_symbol"].astype(str).str.upper() == symbol.upper())
        & (contracts["trade_date"] == trade_date)
        & (contracts["option_type"].astype(str).str.lower() == leg.option_type)
    ].copy()
    if frame.empty:
        return frame
    frame["dte"] = pd.to_numeric(frame["dte"], errors="coerce")
    frame["relative_strike_step"] = pd.to_numeric(
        frame["relative_strike_step"], errors="coerce"
    )
    frame = frame[
        frame["dte"].between(leg.min_dte, leg.max_dte, inclusive="both")
        & frame["relative_strike_step"].notna()
    ].copy()
    if frame.empty:
        return frame
    frame["relative_step_distance"] = (
        frame["relative_strike_step"].astype(float) - float(leg.relative_strike_step)
    ).abs()
    frame["abs_relative_strike_step"] = frame["relative_strike_step"].abs()
    frame = frame.sort_values(
        ["relative_step_distance", "dte", "abs_relative_strike_step", "symbol"]
    )
    return frame


def _candidate_contract_for_leg(
    contracts: pd.DataFrame,
    *,
    symbol: str,
    trade_date: Any,
    leg: OptionLegTemplate,
) -> dict[str, Any] | None:
    frame = _candidate_contracts_for_leg(
        contracts,
        symbol=symbol,
        trade_date=trade_date,
        leg=leg,
    )
    if frame.empty:
        return None
    return frame.iloc[0].to_dict()


def _select_contract_for_leg(
    contracts: pd.DataFrame,
    *,
    symbol: str,
    trade_date: Any,
    leg: OptionLegTemplate,
    candidate_frame: pd.DataFrame | None = None,
    option_bars: pd.DataFrame,
    option_index: Any,
    entry_time: pd.Timestamp,
    max_entry_lag: timedelta,
    contract_selection_method: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    frame = (
        candidate_frame.copy()
        if candidate_frame is not None
        else _candidate_contracts_for_leg(
            contracts,
            symbol=symbol,
            trade_date=trade_date,
            leg=leg,
        )
    )
    if frame.empty:
        return None, None, "no_selected_contract"
    if contract_selection_method == CONTRACT_SELECTION_NEAREST:
        return frame.iloc[0].to_dict(), None, "selected"

    choices: list[tuple[tuple[float, ...], str, dict[str, Any], dict[str, Any]]] = []
    for contract in frame.to_dict("records"):
        contract_symbol = str(contract["symbol"])
        entry_bar = _first_option_bar(
            option_bars=option_bars,
            option_index=option_index,
            contract_symbol=contract_symbol,
            timestamp=entry_time,
            max_lag=max_entry_lag,
        )
        if not entry_bar:
            continue
        volume = float(entry_bar.get("volume") or 0.0)
        relative_distance = abs(
            float(contract.get("relative_strike_step") or 0.0)
            - float(leg.relative_strike_step)
        )
        dte = float(contract.get("dte") or 999.0)
        abs_step = abs(float(contract.get("relative_strike_step") or 0.0))
        # Entry-window evidence only: no future exit bars participate in selection.
        rank_key = (relative_distance, -volume, dte, abs_step)
        choices.append((rank_key, contract_symbol, contract, entry_bar))
    if not choices:
        return None, None, "no_entry_bar"
    choices.sort(key=lambda item: (item[0], item[1]))
    _, _, contract, entry_bar = choices[0]
    return contract, entry_bar, "selected"


def _leg_pnl(
    *,
    leg: OptionLegTemplate,
    entry_close: float,
    exit_close: float,
    strategy_quantity: int,
    slippage_bps: float,
    fee_per_contract: float,
) -> tuple[float, float]:
    slip = slippage_bps / 10000.0
    if leg.side == "buy":
        entry_price = entry_close * (1.0 + slip)
        exit_price = exit_close * (1.0 - slip)
        pnl = (exit_price - entry_price) * leg.quantity * strategy_quantity * 100.0
    else:
        entry_price = entry_close * (1.0 - slip)
        exit_price = exit_close * (1.0 + slip)
        pnl = (entry_price - exit_price) * leg.quantity * strategy_quantity * 100.0
    fees = fee_per_contract * leg.quantity * strategy_quantity * 2.0
    return pnl - fees, entry_price


def _risk_per_strategy_order(
    *,
    template: OptionStrategyTemplate,
    selected_legs: list[dict[str, Any]],
    entry_prices: dict[str, float],
) -> float:
    debit = 0.0
    gross_short = 0.0
    strikes = []
    for item in selected_legs:
        leg = item["leg"]
        contract = item["contract"]
        price = entry_prices[leg.role]
        notional = price * leg.quantity * 100.0
        if leg.side == "buy":
            debit += notional
        else:
            debit -= notional
            gross_short += notional
        strike = contract.get("strike_price")
        if strike is not None:
            strikes.append(float(strike))
    net_debit = max(debit, 0.0)
    width_risk = (max(strikes) - min(strikes)) * 100.0 if len(strikes) > 1 else 0.0
    if template.gross_short_quantity:
        return max(net_debit, width_risk - max(-debit, 0.0), gross_short * 0.25, 1.0)
    return max(net_debit, 1.0)


def _split_summary(rows: list[dict[str, Any]], *, test_date_count: int) -> dict[str, Any]:
    if not rows:
        return {
            "train_trade_count": 0,
            "train_net_pnl": 0.0,
            "test_trade_count": 0,
            "test_net_pnl": 0.0,
            "test_dates": [],
        }
    dates = sorted({row["trade_date"] for row in rows})
    count = max(1, min(int(test_date_count), len(dates)))
    test_dates = set(dates[-count:])
    train_rows = [row for row in rows if row["trade_date"] not in test_dates]
    test_rows = [row for row in rows if row["trade_date"] in test_dates]
    return {
        "train_trade_count": len(train_rows),
        "train_net_pnl": round(sum(float(row["option_pnl"]) for row in train_rows), 4),
        "test_trade_count": len(test_rows),
        "test_net_pnl": round(sum(float(row["option_pnl"]) for row in test_rows), 4),
        "test_dates": sorted(test_dates),
    }


def _economic_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "option_trade_count": 0,
            "net_pnl": 0.0,
            "expectancy": 0.0,
            "win_rate": 0.0,
            "profit_factor": None,
            "max_drawdown": 0.0,
            "ending_cumulative_pnl": 0.0,
        }
    pnl = [float(row["option_pnl"]) for row in rows]
    wins = [value for value in pnl if value > 0]
    losses = [value for value in pnl if value < 0]
    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnl:
        running += value
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
    return {
        "option_trade_count": len(rows),
        "net_pnl": round(sum(pnl), 4),
        "expectancy": round(sum(pnl) / len(pnl), 4),
        "win_rate": round(len(wins) / len(rows), 4),
        "profit_factor": round(gross_wins / gross_losses, 4) if gross_losses else None,
        "max_drawdown": round(max_drawdown, 4),
        "ending_cumulative_pnl": round(running, 4),
    }


def _candidate_variant_id(
    *,
    template: OptionStrategyTemplate,
    entry_offset: timedelta,
    exit_offset: timedelta,
    entry_timing_mode: str,
    max_entry_lag: timedelta,
    max_exit_lag: timedelta,
    contract_selection_method: str,
) -> str:
    entry_minutes = int(entry_offset.total_seconds() / 60)
    exit_minutes = int(exit_offset.total_seconds() / 60)
    entry_lag_minutes = int(max_entry_lag.total_seconds() / 60)
    exit_lag_minutes = int(max_exit_lag.total_seconds() / 60)
    selector_slug = contract_selection_method.replace("_research_only", "")
    return (
        f"{template.template_id}__{entry_timing_mode}"
        f"_e{entry_minutes}_x{exit_minutes}"
        f"_el{entry_lag_minutes}_xl{exit_lag_minutes}"
        f"_{selector_slug}"
    )


def _fill_failure_reason(summary: dict[str, Any]) -> str:
    if float(summary.get("strategy_fill_coverage") or 0.0) >= 0.90:
        return "fill_gate_clear"
    missing = {
        "selected_contract_universe_gap": int(summary.get("missing_no_selected_contract") or 0),
        "entry_bar_gap_or_entry_timing_mismatch": int(summary.get("missing_no_entry_bar") or 0),
        "exit_bar_gap_or_exit_policy_mismatch": int(summary.get("missing_no_exit_bar") or 0),
        "position_sizing_too_expensive": int(summary.get("missing_too_expensive") or 0),
    }
    reason, count = max(missing.items(), key=lambda item: item[1])
    if count <= 0:
        return "mixed_low_fill_gap"
    tied = [key for key, value in missing.items() if value == count]
    return reason if len(tied) == 1 else "mixed_low_fill_gap"


def _run_template(
    *,
    template: OptionStrategyTemplate,
    symbol: str,
    trade_dates: list[Any],
    sessions: dict[Any, tuple[pd.Timestamp, pd.Timestamp]],
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_index: Any,
    entry_offset: timedelta,
    exit_offset: timedelta,
    entry_timing_mode: str,
    max_entry_lag: timedelta,
    max_exit_lag: timedelta,
    initial_cash: float,
    allocation_fraction: float,
    slippage_bps: float,
    fee_per_contract: float,
    contract_selection_method: str,
    run_id: str,
    test_date_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    missing_counts: Counter[str] = Counter()
    candidate_id = _candidate_variant_id(
        template=template,
        entry_offset=entry_offset,
        exit_offset=exit_offset,
        entry_timing_mode=entry_timing_mode,
        max_entry_lag=max_entry_lag,
        max_exit_lag=max_exit_lag,
        contract_selection_method=contract_selection_method,
    )
    selected_all_count = 0
    entry_all_count = 0
    source_signal_count = len(trade_dates)
    eligible_signal_count = 0
    budget = initial_cash * allocation_fraction
    for trade_date in trade_dates:
        session = sessions.get(trade_date)
        if not session:
            missing_counts["no_session"] += 1
            continue
        session_start, session_end = session
        entry_time = session_start + entry_offset
        exit_time = min(session_start + exit_offset, session_end)
        candidate_frames: list[tuple[OptionLegTemplate, pd.DataFrame]] = []
        for leg in template.legs:
            candidate_frame = _candidate_contracts_for_leg(
                contracts,
                symbol=symbol,
                trade_date=trade_date,
                leg=leg,
            )
            if candidate_frame.empty:
                missing_counts["no_selected_contract"] += 1
                candidate_frames = []
                break
            candidate_frames.append((leg, candidate_frame))
        if not candidate_frames:
            continue
        selected_all_count += 1

        selected_legs: list[dict[str, Any]] = []
        for leg, candidate_frame in candidate_frames:
            selection_time = (
                session_start
                if entry_timing_mode == "first_common_within_cutoff"
                else entry_time
            )
            selection_lag = (
                entry_offset
                if entry_timing_mode == "first_common_within_cutoff"
                else max_entry_lag
            )
            contract, preliminary_entry_bar, selection_status = _select_contract_for_leg(
                contracts,
                symbol=symbol,
                trade_date=trade_date,
                leg=leg,
                candidate_frame=candidate_frame,
                option_bars=option_bars,
                option_index=option_index,
                entry_time=selection_time,
                max_entry_lag=selection_lag,
                contract_selection_method=contract_selection_method,
            )
            if not contract:
                if selection_status == "no_entry_bar":
                    missing_counts["no_entry_bar"] += 1
                else:
                    missing_counts["no_selected_contract"] += 1
                selected_legs = []
                break
            selected_legs.append(
                {
                    "leg": leg,
                    "contract": contract,
                    "preliminary_entry_bar": preliminary_entry_bar,
                }
            )
        if not selected_legs:
            continue

        leg_entries: dict[str, dict[str, Any]] = {}
        if entry_timing_mode == "first_common_within_cutoff":
            preliminary_entries = {}
            for item in selected_legs:
                leg = item["leg"]
                contract_symbol = str(item["contract"]["symbol"])
                entry_bar = item.get("preliminary_entry_bar") or _first_option_bar(
                    option_bars=option_bars,
                    option_index=option_index,
                    contract_symbol=contract_symbol,
                    timestamp=session_start,
                    max_lag=entry_offset,
                )
                if not entry_bar:
                    missing_counts["no_entry_bar"] += 1
                    preliminary_entries = {}
                    break
                preliminary_entries[leg.role] = entry_bar
            if not preliminary_entries:
                continue
            entry_time = max(
                pd.Timestamp(entry_bar["timestamp"]) for entry_bar in preliminary_entries.values()
            )
        for item in selected_legs:
            leg = item["leg"]
            contract_symbol = str(item["contract"]["symbol"])
            entry_bar = _first_option_bar(
                option_bars=option_bars,
                option_index=option_index,
                contract_symbol=contract_symbol,
                timestamp=entry_time,
                max_lag=max_entry_lag,
            )
            if not entry_bar:
                missing_counts["no_entry_bar"] += 1
                leg_entries = {}
                break
            leg_entries[leg.role] = entry_bar
        if not leg_entries:
            continue
        entry_all_count += 1
        eligible_signal_count += 1

        leg_exits: dict[str, dict[str, Any]] = {}
        for item in selected_legs:
            leg = item["leg"]
            contract_symbol = str(item["contract"]["symbol"])
            exit_bar = _first_option_bar(
                option_bars=option_bars,
                option_index=option_index,
                contract_symbol=contract_symbol,
                timestamp=exit_time,
                max_lag=max_exit_lag,
            )
            if not exit_bar:
                missing_counts["no_exit_bar"] += 1
                leg_exits = {}
                break
            leg_exits[leg.role] = exit_bar
        if not leg_exits:
            continue

        entry_prices = {
            item["leg"].role: float(leg_entries[item["leg"].role]["close"])
            for item in selected_legs
        }
        risk_per_order = _risk_per_strategy_order(
            template=template,
            selected_legs=selected_legs,
            entry_prices=entry_prices,
        )
        strategy_quantity = math.floor(budget / risk_per_order)
        if strategy_quantity < 1:
            missing_counts["too_expensive"] += 1
            continue

        leg_details = []
        order_pnl = 0.0
        contract_symbols = []
        for item in selected_legs:
            leg = item["leg"]
            contract = item["contract"]
            contract_symbol = str(contract["symbol"])
            contract_symbols.append(contract_symbol)
            entry_close = float(leg_entries[leg.role]["close"])
            exit_close = float(leg_exits[leg.role]["close"])
            leg_pnl, entry_price = _leg_pnl(
                leg=leg,
                entry_close=entry_close,
                exit_close=exit_close,
                strategy_quantity=strategy_quantity,
                slippage_bps=slippage_bps,
                fee_per_contract=fee_per_contract,
            )
            order_pnl += leg_pnl
            leg_details.append(
                {
                    "role": leg.role,
                    "side": leg.side,
                    "option_type": leg.option_type,
                    "quantity": leg.quantity,
                    "contract_symbol": contract_symbol,
                    "strike_price": contract.get("strike_price"),
                    "relative_strike_step": contract.get("relative_strike_step"),
                    "entry_close": round(entry_close, 4),
                    "exit_close": round(exit_close, 4),
                    "entry_time": str(leg_entries[leg.role]["timestamp"]),
                    "exit_time": str(leg_exits[leg.role]["timestamp"]),
                    "leg_pnl": round(leg_pnl, 4),
                    "entry_price_after_slippage": round(entry_price, 4),
                }
            )
        rows.append(
            {
                "candidate_variant_id": candidate_id,
                "strategy_id": candidate_id,
                "source_strategy_id": template.template_id,
                "symbol": symbol.upper(),
                "family": template.family,
                "intended_regime": template.intended_regime,
                "trade_date": str(trade_date),
                "entry_time": str(entry_time),
                "exit_time": str(exit_time),
                "strategy_quantity": strategy_quantity,
                "risk_per_order": round(risk_per_order, 4),
                "option_pnl": round(order_pnl, 4),
                "leg_count": template.leg_count,
                "contract_symbols": ",".join(contract_symbols),
                "leg_details_json": _metadata_json(leg_details),
                "run_id": run_id,
            }
        )

    filled_order_count = len(rows)
    intended_order_count = (
        eligible_signal_count
        if entry_timing_mode == "first_common_within_cutoff"
        else source_signal_count
    )
    skipped_order_count = max(intended_order_count - filled_order_count, 0)
    source_skipped_opportunity_count = max(source_signal_count - eligible_signal_count, 0)
    strategy_fill = round(filled_order_count / intended_order_count, 4) if intended_order_count else 0.0
    data_foundation = (
        round(selected_all_count / source_signal_count, 4) if source_signal_count else 0.0
    )
    entry_coverage = (
        round(entry_all_count / selected_all_count, 4) if selected_all_count else 0.0
    )
    exit_coverage = (
        round(filled_order_count / entry_all_count, 4) if entry_all_count else 0.0
    )
    parameters = {
        "candidate_variant_id": candidate_id,
        "template_id": template.template_id,
        "intended_regime": template.intended_regime,
        "contract_selection_method": contract_selection_method,
        "entry_timing_mode": entry_timing_mode,
        "entry_offset_minutes": int(entry_offset.total_seconds() / 60),
        "exit_offset_minutes": int(exit_offset.total_seconds() / 60),
        "max_entry_lag_minutes": int(max_entry_lag.total_seconds() / 60),
        "max_exit_lag_minutes": int(max_exit_lag.total_seconds() / 60),
        "slippage_bps": slippage_bps,
        "fee_per_contract": fee_per_contract,
        "leg_count": template.leg_count,
        "legs": [leg.to_dict() for leg in template.legs],
    }
    summary = {
        "candidate_variant_id": candidate_id,
        "symbol": symbol.upper(),
        "strategy_id": candidate_id,
        "source_strategy_id": template.template_id,
        "family": template.family,
        "parameter_set": _metadata_json(parameters),
        "directional_option_type": ",".join(template.required_option_types),
        "intended_regime": template.intended_regime,
        "profile": run_id,
        "source_stock_trade_count": intended_order_count,
        "source_signal_count": source_signal_count,
        "eligible_signal_count": eligible_signal_count,
        "source_skipped_opportunity_count": source_skipped_opportunity_count,
        "opportunity_coverage": (
            round(eligible_signal_count / source_signal_count, 4) if source_signal_count else 0.0
        ),
        "intended_order_count": intended_order_count,
        "filled_order_count": filled_order_count,
        "skipped_order_count": skipped_order_count,
        "missing_option_price_count": skipped_order_count,
        "missing_no_selected_contract": int(missing_counts.get("no_selected_contract", 0)),
        "missing_no_entry_bar": int(missing_counts.get("no_entry_bar", 0)),
        "missing_no_exit_bar": int(missing_counts.get("no_exit_bar", 0)),
        "missing_too_expensive": int(missing_counts.get("too_expensive", 0)),
        "missing_no_session": int(missing_counts.get("no_session", 0)),
        "fill_coverage": strategy_fill,
        "strategy_fill_coverage": strategy_fill,
        "data_foundation_coverage": data_foundation,
        "entry_bar_coverage": entry_coverage,
        "exit_bar_coverage": exit_coverage,
        "fill_coverage_numerator": filled_order_count,
        "fill_coverage_denominator": intended_order_count,
        "fill_coverage_unit": "filled_multi_leg_strategy_orders_per_intended_regime_day",
        "fill_coverage_semantics": (
            "Strategy-order fill coverage. Multi-leg orders count as filled only when every "
            "required leg has selected contract, entry bar, and exit bar. "
            "Opportunity coverage reports source regime days that passed the entry tradability gate."
        ),
        "broker_facing": False,
        "promotion_allowed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "contract_selection_method": contract_selection_method,
        "contract_selection_lookahead": (
            "entry_window_only"
            if contract_selection_method == CONTRACT_SELECTION_LIQUIDITY_FIRST
            else "none"
        ),
        **_economic_summary(rows),
        **_split_summary(rows, test_date_count=test_date_count),
    }
    summary["fill_failure_reason"] = _fill_failure_reason(summary)
    return summary, rows


def run_tournament(
    *,
    stock_bars_path: Path,
    selected_contracts_root: Path,
    option_bars_root: Path,
    option_trades_root: Path | None,
    regime_labels_csv: Path,
    output_dir: Path,
    run_id: str,
    symbol: str = "QQQ",
    regime_filter: set[str] | None = None,
    entry_offset_minutes: int = 30,
    exit_offset_minutes: int = 210,
    entry_timing_mode: str = "fixed_offset",
    max_entry_lag_minutes: int = 5,
    max_exit_lag_minutes: int = 10,
    test_date_count: int = 20,
    initial_cash: float = 25_000.0,
    allocation_fraction: float = 0.05,
    slippage_bps: float = 10.0,
    fee_per_contract: float = 0.65,
    contract_selection_method: str = CONTRACT_SELECTION_NEAREST,
) -> dict[str, Any]:
    stock_bars = _load_stock_bars(stock_bars_path, symbol_filter={symbol.upper()})
    queue = {
        "selected_contracts_root": str(selected_contracts_root),
        "option_bars_root": str(option_bars_root),
        "option_trades_root": str(option_trades_root or output_dir / "_empty_option_trades"),
    }
    if option_trades_root is None:
        empty_root = output_dir / "_empty_option_trades"
        empty_root.mkdir(parents=True, exist_ok=True)
        queue["option_trades_root"] = str(empty_root)
    contracts, option_bars, option_trades = _load_option_inputs(
        queue=queue,
        selected_contracts_root=selected_contracts_root,
        option_bars_root=option_bars_root,
        option_trades_root=Path(queue["option_trades_root"]),
    )
    option_index = _build_option_research_index(
        contracts=contracts,
        option_bars=option_bars,
        option_trades=option_trades,
    )
    regimes = _load_regime_labels(regime_labels_csv, symbol)
    sessions = _session_times(stock_bars, symbol)
    all_rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for template in qqq_option_native_templates():
        if regime_filter and template.intended_regime not in regime_filter:
            continue
        trade_dates = sorted(
            regimes.loc[regimes["regime"] == template.intended_regime, "trade_date"].tolist()
        )
        summary, rows = _run_template(
            template=template,
            symbol=symbol,
            trade_dates=trade_dates,
            sessions=sessions,
            contracts=contracts,
            option_bars=option_bars,
            option_index=option_index,
            entry_offset=timedelta(minutes=entry_offset_minutes),
            exit_offset=timedelta(minutes=exit_offset_minutes),
            entry_timing_mode=entry_timing_mode,
            max_entry_lag=timedelta(minutes=max_entry_lag_minutes),
            max_exit_lag=timedelta(minutes=max_exit_lag_minutes),
            initial_cash=initial_cash,
            allocation_fraction=allocation_fraction,
            slippage_bps=slippage_bps,
            fee_per_contract=fee_per_contract,
            contract_selection_method=contract_selection_method,
            run_id=run_id,
            test_date_count=test_date_count,
        )
        summaries.append(summary)
        all_rows.extend(rows)
    packet = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "status": "qqq_option_native_tournament_complete",
        "mode": "research_only",
        "broker_facing": False,
        "promotion_allowed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "run_id": run_id,
        "symbol": symbol.upper(),
        "regime_filter": sorted(regime_filter) if regime_filter else [],
        "entry_offset_minutes": entry_offset_minutes,
        "exit_offset_minutes": exit_offset_minutes,
        "entry_timing_mode": entry_timing_mode,
        "contract_selection_method": contract_selection_method,
        "contract_selection_lookahead": (
            "entry_window_only"
            if contract_selection_method == CONTRACT_SELECTION_LIQUIDITY_FIRST
            else "none"
        ),
        "max_entry_lag_minutes": max_entry_lag_minutes,
        "max_exit_lag_minutes": max_exit_lag_minutes,
        "template_count": len(summaries),
        "strategy_order_count": len(all_rows),
        "regime_counts": dict(sorted(Counter(regimes["regime"].tolist()).items())),
        "candidate_summaries": summaries,
        "trade_rows": all_rows,
        "next_step_contract": [
            "Feed this replay root into build_research_portfolio_report.py.",
            "Promote nothing unless the generated promotion packet says eligible_for_promotion_review.",
            "If fill is high but economics fail, redesign templates rather than relaxing gates.",
        ],
    }
    write_artifacts(output_dir, run_id, packet)
    return packet


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# QQQ Option-Native Tournament",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Run ID: `{packet['run_id']}`",
        f"- Mode: `{packet['mode']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Template count: `{packet['template_count']}`",
        f"- Strategy order count: `{packet['strategy_order_count']}`",
        "",
        "## Candidate Summaries",
        "",
    ]
    for row in packet["candidate_summaries"]:
        lines.append(
            "- "
            f"`{row['candidate_variant_id']}` regime `{row['intended_regime']}` "
            f"family `{row['family']}` net `{row['net_pnl']}` test `{row['test_net_pnl']}` "
            f"fill `{row['strategy_fill_coverage']}` data `{row['data_foundation_coverage']}` "
            f"opportunity `{row.get('opportunity_coverage')}` "
            f"reason `{row['fill_failure_reason']}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_artifacts(output_dir: Path, run_id: str, packet: dict[str, Any]) -> None:
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "qqq_option_native_tournament_packet.json", packet)
    _write_markdown(run_dir / "qqq_option_native_tournament_packet.md", packet)
    _write_json(run_dir / "option_aware_candidate_summary.json", packet["candidate_summaries"])
    _write_csv(run_dir / "option_aware_candidate_summary.csv", packet["candidate_summaries"])
    _write_json(run_dir / "option_aware_trade_economics.json", packet["trade_rows"])
    _write_csv(run_dir / "option_aware_trade_economics.csv", packet["trade_rows"])
    recommendation = {
        "generated_at": packet["generated_at"],
        "promotion_allowed": False,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "top_research_followups": packet["candidate_summaries"],
        "next_step_contract": packet["next_step_contract"],
    }
    _write_json(run_dir / "option_aware_recommendation_packet.json", recommendation)
    _write_markdown(run_dir / "option_aware_recommendation_packet.md", packet)


def main() -> None:
    args = parse_args()
    packet = run_tournament(
        stock_bars_path=Path(args.stock_bars_path),
        selected_contracts_root=Path(args.selected_contracts_root),
        option_bars_root=Path(args.option_bars_root),
        option_trades_root=Path(args.option_trades_root) if args.option_trades_root else None,
        regime_labels_csv=Path(args.regime_labels_csv),
        output_dir=Path(args.output_dir),
        run_id=args.run_id,
        symbol=args.symbol,
        regime_filter=_regime_filter(args.regime_filter),
        entry_offset_minutes=args.entry_offset_minutes,
        exit_offset_minutes=args.exit_offset_minutes,
        entry_timing_mode=args.entry_timing_mode,
        max_entry_lag_minutes=args.max_entry_lag_minutes,
        max_exit_lag_minutes=args.max_exit_lag_minutes,
        test_date_count=args.test_date_count,
        initial_cash=args.initial_cash,
        allocation_fraction=args.allocation_fraction,
        slippage_bps=args.slippage_bps,
        fee_per_contract=args.fee_per_contract,
        contract_selection_method=args.contract_selection_method,
    )
    print(json.dumps({key: value for key, value in packet.items() if key != "trade_rows"}, indent=2))


if __name__ == "__main__":
    main()

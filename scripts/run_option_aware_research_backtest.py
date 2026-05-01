from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.backtest.engine import FixedFractionSizer, LinearCostModel, run_backtest
from scripts.run_gcp_research_wave import _variant_stock_strategy, load_variants

DEFAULT_QUEUE_JSON = (
    REPO_ROOT
    / "reports"
    / "research_wave"
    / "option_aware_queue"
    / "option_aware_research_queue.json"
)
DEFAULT_VARIANTS_JSONL = (
    REPO_ROOT.parent
    / "CodexAlpacaTest-TradeMigratyion_gcp_lease_lane"
    / "docs"
    / "gcp_foundation"
    / "gcp_research_wave_variants.jsonl"
)
DEFAULT_OPTION_DATA_ROOT = (
    REPO_ROOT
    / "data"
    / "silver"
    / "historical"
    / "research_gld_put_options_20260421_20260423_option_bars_trades"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "option_aware_backtests"
CONTRACT_SELECTION_NEAREST = "nearest_contract"
CONTRACT_SELECTION_LIQUIDITY_FIRST = "entry_liquidity_first_research_only"


@dataclass(frozen=True)
class OptionResearchIndex:
    contracts_by_key: dict[tuple[str, str, Any], pd.DataFrame]
    bars_by_symbol: dict[str, pd.DataFrame]
    trade_timestamps_by_symbol: dict[str, pd.Series]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run research-only option-aware economics for queued candidates."
    )
    parser.add_argument("--queue-json", default=str(DEFAULT_QUEUE_JSON))
    parser.add_argument("--variants-jsonl", default=str(DEFAULT_VARIANTS_JSONL))
    parser.add_argument("--stock-bars-path", default=str(DEFAULT_OPTION_DATA_ROOT / "stock_bars"))
    parser.add_argument("--selected-contracts-root", default=None)
    parser.add_argument("--option-bars-root", default=None)
    parser.add_argument("--option-trades-root", default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument(
        "--symbol-filter", default=None, help="Optional comma-separated symbol allowlist."
    )
    parser.add_argument(
        "--skip-blocked-queue-items",
        action="store_true",
        help="Skip queue items carrying blockers such as missing selected contracts.",
    )
    parser.add_argument("--max-entry-lag-minutes", type=float, default=10.0)
    parser.add_argument("--max-exit-lag-minutes", type=float, default=10.0)
    parser.add_argument(
        "--test-date-count",
        type=int,
        default=1,
        help="Number of most-recent filled trade dates reserved for OOS/test summary.",
    )
    parser.add_argument("--initial-cash", type=float, default=100_000.0)
    parser.add_argument("--allocation-fraction", type=float, default=0.10)
    parser.add_argument("--slippage-bps", type=float, default=10.0)
    parser.add_argument("--fee-per-contract", type=float, default=0.65)
    parser.add_argument(
        "--contract-selection-method",
        choices=[CONTRACT_SELECTION_NEAREST, CONTRACT_SELECTION_LIQUIDITY_FIRST],
        default=CONTRACT_SELECTION_NEAREST,
        help=(
            "Research-only contract selector. The default preserves the nearest-contract "
            "path; liquidity-first only uses entry-window information and is not broker-facing."
        ),
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _path_matches_symbol_filter(path: Path, symbol_filter: set[str] | None) -> bool:
    if not symbol_filter:
        return True
    symbols = {symbol.upper() for symbol in symbol_filter}
    for part in path.parts:
        normalized = part.strip().upper()
        if normalized in symbols:
            return True
        if "=" in normalized:
            key, value = normalized.split("=", 1)
            if key in {"UNDERLYING", "UNDERLYING_SYMBOL", "SYMBOL"} and value in symbols:
                return True
            if key in {"UNDERLYING", "UNDERLYING_SYMBOL", "SYMBOL"} and value not in symbols:
                return False
        if normalized.isalpha() and 1 <= len(normalized) <= 6:
            return False
    return not any(
        part.strip().upper().startswith(("UNDERLYING=", "UNDERLYING_SYMBOL=", "SYMBOL="))
        or part.strip().upper() in symbols
        for part in path.parts
    )


def _load_parquet_tree(path: Path, symbol_filter: set[str] | None = None) -> pd.DataFrame:
    if path.is_file():
        return pd.read_parquet(path)
    frames = []
    for item in sorted(path.rglob("*.parquet")):
        if not _path_matches_symbol_filter(item.relative_to(path), symbol_filter):
            continue
        frame = pd.read_parquet(item)
        for part in item.relative_to(path).parts[:-1]:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key and key not in frame.columns:
                frame[key] = value
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _coerce_timestamp(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True)


def _load_option_inputs(
    *,
    queue: dict[str, Any],
    selected_contracts_root: Path | None,
    option_bars_root: Path | None,
    option_trades_root: Path | None,
    symbol_filter: set[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    selected_root = selected_contracts_root or Path(str(queue.get("selected_contracts_root")))
    bars_root = option_bars_root or Path(str(queue.get("option_bars_root")))
    trades_root = option_trades_root or Path(str(queue.get("option_trades_root")))
    contracts = _load_parquet_tree(selected_root, symbol_filter=symbol_filter)
    option_bars = _load_parquet_tree(bars_root, symbol_filter=symbol_filter)
    option_trades = _load_parquet_tree(trades_root, symbol_filter=symbol_filter)
    for frame in [contracts, option_bars, option_trades]:
        if not frame.empty and "timestamp" in frame.columns:
            frame["timestamp"] = _coerce_timestamp(frame["timestamp"])
    if not contracts.empty and "trade_date" in contracts.columns:
        contracts["trade_date"] = pd.to_datetime(contracts["trade_date"]).dt.date
    if not option_bars.empty and "trade_date" in option_bars.columns:
        option_bars["trade_date"] = pd.to_datetime(option_bars["trade_date"]).dt.date
    if not option_trades.empty and "trade_date" in option_trades.columns:
        option_trades["trade_date"] = pd.to_datetime(option_trades["trade_date"]).dt.date
    return contracts, option_bars, option_trades


def _load_stock_bars(path: Path, symbol_filter: set[str] | None = None) -> pd.DataFrame:
    bars = _load_parquet_tree(path, symbol_filter=symbol_filter)
    if "symbol" not in bars.columns and symbol_filter and len(symbol_filter) == 1:
        bars["symbol"] = next(iter(symbol_filter))
    if bars.empty:
        return bars
    bars["timestamp"] = _coerce_timestamp(bars["timestamp"])
    return bars


def _variant_map(variants_jsonl: Path) -> dict[str, dict[str, Any]]:
    return {
        str(variant.get("variant_id")): variant
        for variant in load_variants(variants_jsonl)
        if variant.get("variant_id")
    }


def _metadata_json(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _candidate_identity(queue_item: dict[str, Any], variant: dict[str, Any]) -> dict[str, str]:
    parameters = (
        queue_item.get("parameter_set")
        or variant.get("parameter_set")
        or variant.get("parameters")
        or {}
    )
    strategy_id = (
        queue_item.get("strategy_id")
        or variant.get("strategy_id")
        or queue_item.get("source_strategy_id")
        or variant.get("source_strategy_id")
        or ""
    )
    return {
        "strategy_id": str(strategy_id),
        "family": str(queue_item.get("family") or variant.get("family") or ""),
        "parameter_set": _metadata_json(parameters),
    }


def _build_option_research_index(
    *,
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_trades: pd.DataFrame,
) -> OptionResearchIndex:
    contracts_by_key: dict[tuple[str, str, Any], pd.DataFrame] = {}
    if not contracts.empty:
        contract_frame = contracts.copy()
        contract_frame["underlying_symbol_norm"] = (
            contract_frame["underlying_symbol"].astype(str).str.upper()
        )
        contract_frame["option_type_norm"] = contract_frame["option_type"].astype(str).str.lower()
        contract_frame["abs_relative_strike_step"] = pd.to_numeric(
            contract_frame["relative_strike_step"], errors="coerce"
        ).abs()
        for (symbol, option_type, trade_date), group in contract_frame.groupby(
            ["underlying_symbol_norm", "option_type_norm", "trade_date"], sort=False
        ):
            contracts_by_key[(str(symbol), str(option_type), trade_date)] = group.sort_values(
                ["dte", "abs_relative_strike_step", "symbol"]
            ).reset_index(drop=True)

    bars_by_symbol: dict[str, pd.DataFrame] = {}
    if not option_bars.empty:
        bar_frame = option_bars.copy()
        bar_frame["symbol_norm"] = bar_frame["symbol"].astype(str)
        for symbol, group in bar_frame.groupby("symbol_norm", sort=False):
            bars_by_symbol[str(symbol)] = group.sort_values("timestamp").reset_index(drop=True)

    trade_timestamps_by_symbol: dict[str, pd.Series] = {}
    if not option_trades.empty:
        trade_frame = option_trades.copy()
        trade_frame["symbol_norm"] = trade_frame["symbol"].astype(str)
        for symbol, group in trade_frame.groupby("symbol_norm", sort=False):
            trade_timestamps_by_symbol[str(symbol)] = (
                group["timestamp"].sort_values().reset_index(drop=True)
            )

    return OptionResearchIndex(
        contracts_by_key=contracts_by_key,
        bars_by_symbol=bars_by_symbol,
        trade_timestamps_by_symbol=trade_timestamps_by_symbol,
    )


def _candidate_contracts(
    *,
    contracts: pd.DataFrame,
    symbol: str,
    option_type: str,
    trade_date: Any,
) -> pd.DataFrame:
    if contracts.empty:
        return pd.DataFrame()
    frame = contracts[
        (contracts["underlying_symbol"].astype(str).str.upper() == symbol.upper())
        & (contracts["option_type"].astype(str).str.lower() == option_type.lower())
        & (contracts["trade_date"] == trade_date)
    ].copy()
    if frame.empty:
        return frame
    frame["abs_relative_strike_step"] = frame["relative_strike_step"].abs()
    return frame.sort_values(["dte", "abs_relative_strike_step", "symbol"])


def _candidate_contracts_from_index(
    *,
    option_index: OptionResearchIndex,
    symbol: str,
    option_type: str,
    trade_date: Any,
) -> pd.DataFrame:
    return option_index.contracts_by_key.get(
        (symbol.upper(), option_type.lower(), trade_date), pd.DataFrame()
    )


def _choose_contract(
    *,
    contracts: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    symbol: str,
    option_type: str,
    trade_date: Any,
) -> dict[str, Any] | None:
    frame = (
        _candidate_contracts_from_index(
            option_index=option_index,
            symbol=symbol,
            option_type=option_type,
            trade_date=trade_date,
        )
        if option_index
        else _candidate_contracts(
            contracts=contracts,
            symbol=symbol,
            option_type=option_type,
            trade_date=trade_date,
        )
    )
    if frame.empty:
        return None
    return frame.iloc[0].to_dict()


def _choose_entry_liquidity_first_contract(
    *,
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_trades: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    symbol: str,
    option_type: str,
    trade_date: Any,
    entry_time: pd.Timestamp,
    max_lag: timedelta,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    frame = (
        _candidate_contracts_from_index(
            option_index=option_index,
            symbol=symbol,
            option_type=option_type,
            trade_date=trade_date,
        )
        if option_index
        else _candidate_contracts(
            contracts=contracts,
            symbol=symbol,
            option_type=option_type,
            trade_date=trade_date,
        )
    )
    if frame.empty:
        return None, None, "no_selected_contract"

    choices: list[tuple[tuple[float, ...], str, dict[str, Any], dict[str, Any]]] = []
    for contract in frame.to_dict("records"):
        contract_symbol = str(contract["symbol"])
        entry_bar = _first_option_bar(
            option_bars=option_bars,
            option_index=option_index,
            contract_symbol=contract_symbol,
            timestamp=entry_time,
            max_lag=max_lag,
        )
        if not entry_bar:
            continue
        prints = _trade_print_count(
            option_trades=option_trades,
            option_index=option_index,
            contract_symbol=contract_symbol,
            start=entry_time,
            end=entry_time + max_lag,
        )
        volume = float(entry_bar.get("volume") or 0.0)
        abs_step = abs(float(contract.get("relative_strike_step") or 0.0))
        dte = float(contract.get("dte") or 999.0)
        # No future bars are used here: the selector ranks only entry-window evidence.
        rank_key = (-float(prints), -volume, abs_step, dte)
        choices.append((rank_key, contract_symbol, contract, entry_bar))

    if not choices:
        return None, None, "no_entry_bar"
    choices.sort(key=lambda item: (item[0], item[1]))
    _, _, contract, entry_bar = choices[0]
    return contract, entry_bar, "selected"


def _first_option_bar(
    *,
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    contract_symbol: str,
    timestamp: pd.Timestamp,
    max_lag: timedelta,
) -> dict[str, Any] | None:
    if option_index:
        frame = option_index.bars_by_symbol.get(contract_symbol)
        if frame is None or frame.empty:
            return None
        timestamps = frame["timestamp"]
        position = int(timestamps.searchsorted(timestamp, side="left"))
        if position >= len(frame):
            return None
        row = frame.iloc[position].to_dict()
        if pd.Timestamp(row["timestamp"]) <= timestamp + max_lag:
            return row
        return None

    frame = option_bars[
        (option_bars["symbol"].astype(str) == contract_symbol)
        & (option_bars["timestamp"] >= timestamp)
        & (option_bars["timestamp"] <= timestamp + max_lag)
    ].sort_values("timestamp")
    if frame.empty:
        return None
    return frame.iloc[0].to_dict()


def _trade_print_count(
    *,
    option_trades: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    contract_symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> int:
    if option_index:
        timestamps = option_index.trade_timestamps_by_symbol.get(contract_symbol)
        if timestamps is None or timestamps.empty:
            return 0
        left = int(timestamps.searchsorted(start, side="left"))
        right = int(timestamps.searchsorted(end, side="right"))
        return max(0, right - left)

    if option_trades.empty:
        return 0
    frame = option_trades[
        (option_trades["symbol"].astype(str) == contract_symbol)
        & (option_trades["timestamp"] >= start)
        & (option_trades["timestamp"] <= end)
    ]
    return int(len(frame))


def _stock_trades_for_variant(
    *,
    variant: dict[str, Any],
    stock_bars: pd.DataFrame,
    initial_cash: float,
    allocation_fraction: float,
) -> pd.DataFrame:
    symbol = str(variant.get("symbol") or "").upper()
    if stock_bars.empty:
        return pd.DataFrame()
    if "symbol" in stock_bars.columns:
        symbol_bars = stock_bars[stock_bars["symbol"].astype(str).str.upper() == symbol].copy()
    else:
        symbol_bars = stock_bars.copy()
        symbol_bars["symbol"] = symbol
    if symbol_bars.empty:
        return pd.DataFrame()
    result = run_backtest(
        symbol_bars,
        _variant_stock_strategy(variant),
        initial_cash=initial_cash,
        cost_model=LinearCostModel(slippage_bps=5.0, fee_per_unit=0.01),
        position_sizer=FixedFractionSizer(base_allocation_fraction=allocation_fraction),
    )
    return result.trades.copy()


def _split_trade_date(value: Any) -> str:
    return str(pd.Timestamp(value).date())


def _symbol_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {item.strip().upper() for item in value.split(",") if item.strip()}
    return symbols or None


def _summarize_trade_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "option_trade_count": 0,
            "net_pnl": 0.0,
            "expectancy": 0.0,
            "win_rate": 0.0,
            "profit_factor": None,
            "max_drawdown": 0.0,
        }
    pnl = [float(row["option_pnl"]) for row in rows]
    wins = [value for value in pnl if value > 0]
    losses = [value for value in pnl if value < 0]
    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    cumulative = []
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnl:
        running += value
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
        cumulative.append(running)
    return {
        "option_trade_count": len(rows),
        "net_pnl": round(sum(pnl), 4),
        "expectancy": round(sum(pnl) / len(pnl), 4),
        "win_rate": round(len(wins) / len(rows), 4),
        "profit_factor": round(gross_wins / gross_losses, 4) if gross_losses else None,
        "max_drawdown": round(max_drawdown, 4),
        "ending_cumulative_pnl": round(cumulative[-1], 4),
    }


def _recommendation(summary: dict[str, Any]) -> str:
    source_trades = int(summary["source_stock_trade_count"])
    filled = int(summary["option_trade_count"])
    fill_coverage = float(summary.get("strategy_fill_coverage") or summary["fill_coverage"])
    train_pnl = float(summary["train_net_pnl"])
    test_pnl = float(summary["test_net_pnl"])
    net_pnl = float(summary["net_pnl"])
    expectancy = float(summary["expectancy"])
    if source_trades < 3 or filled < 3:
        return "hold_insufficient_option_fills"
    if fill_coverage < 0.80:
        return "hold_option_fill_coverage"
    if net_pnl > 0 and expectancy > 0 and train_pnl >= 0 and test_pnl >= 0:
        if summary.get("contract_selection_method") == CONTRACT_SELECTION_LIQUIDITY_FIRST:
            return "research_candidate_liquidity_first_review"
        return "candidate_for_walk_forward_review"
    if net_pnl < -100 or expectancy < -10:
        return "quarantine_option_economics"
    return "hold_option_economics"


def _fill_failure_reason(summary: dict[str, Any]) -> str:
    source_trades = int(summary.get("source_stock_trade_count") or 0)
    filled = int(summary.get("option_trade_count") or 0)
    if source_trades == 0:
        return "no_source_stock_trades"
    fill_coverage = float(
        summary.get("strategy_fill_coverage") or summary.get("fill_coverage") or 0.0
    )
    if fill_coverage >= 0.90:
        return "fill_gate_clear"
    missing = {
        "selected_contract_universe_gap": int(summary.get("missing_no_selected_contract") or 0),
        "entry_bar_gap_or_entry_timing_mismatch": int(summary.get("missing_no_entry_bar") or 0),
        "exit_bar_gap_or_exit_policy_mismatch": int(summary.get("missing_no_exit_bar") or 0),
        "position_sizing_too_expensive": int(summary.get("missing_too_expensive") or 0),
    }
    if filled == 0 and not any(missing.values()):
        return "no_option_fills_unknown_gap"
    dominant_reason, dominant_count = max(missing.items(), key=lambda item: item[1])
    if dominant_count <= 0:
        return "mixed_low_fill_gap"
    tied = [reason for reason, count in missing.items() if count == dominant_count]
    return dominant_reason if len(tied) == 1 else "mixed_low_fill_gap"


def _option_rows_for_candidate(
    *,
    queue_item: dict[str, Any],
    variant: dict[str, Any],
    stock_bars: pd.DataFrame,
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_trades: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    initial_cash: float,
    allocation_fraction: float,
    slippage_bps: float,
    fee_per_contract: float,
    max_entry_lag: timedelta,
    max_exit_lag: timedelta,
    contract_selection_method: str,
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    source_trades = _stock_trades_for_variant(
        variant=variant,
        stock_bars=stock_bars,
        initial_cash=initial_cash,
        allocation_fraction=allocation_fraction,
    )
    option_rows: list[dict[str, Any]] = []
    missing_counts = {
        "no_selected_contract": 0,
        "no_entry_bar": 0,
        "no_exit_bar": 0,
        "too_expensive": 0,
    }
    option_type = str(queue_item.get("directional_option_type") or "").lower()
    symbol = str(queue_item.get("symbol") or "").upper()
    identity = _candidate_identity(queue_item, variant)
    for trade in source_trades.to_dict("records"):
        entry_time = pd.Timestamp(trade["entry_time"])
        exit_time = pd.Timestamp(trade["exit_time"])
        trade_date = entry_time.date()
        if contract_selection_method == CONTRACT_SELECTION_LIQUIDITY_FIRST:
            contract, entry_bar, status = _choose_entry_liquidity_first_contract(
                contracts=contracts,
                option_bars=option_bars,
                option_trades=option_trades,
                option_index=option_index,
                symbol=symbol,
                option_type=option_type,
                trade_date=trade_date,
                entry_time=entry_time,
                max_lag=max_entry_lag,
            )
            if status != "selected" or not contract or not entry_bar:
                missing_counts[status] = missing_counts.get(status, 0) + 1
                continue
        else:
            contract = _choose_contract(
                contracts=contracts,
                option_index=option_index,
                symbol=symbol,
                option_type=option_type,
                trade_date=trade_date,
            )
            if not contract:
                missing_counts["no_selected_contract"] += 1
                continue
            contract_symbol = str(contract["symbol"])
            entry_bar = _first_option_bar(
                option_bars=option_bars,
                option_index=option_index,
                contract_symbol=contract_symbol,
                timestamp=entry_time,
                max_lag=max_entry_lag,
            )
            if not entry_bar:
                missing_counts["no_entry_bar"] += 1
                continue

        contract_symbol = str(contract["symbol"])
        exit_bar = _first_option_bar(
            option_bars=option_bars,
            option_index=option_index,
            contract_symbol=contract_symbol,
            timestamp=exit_time,
            max_lag=max_exit_lag,
        )
        if not exit_bar:
            missing_counts["no_exit_bar"] += 1
            continue
        raw_entry = float(entry_bar["close"])
        raw_exit = float(exit_bar["close"])
        entry_price = raw_entry * (1.0 + slippage_bps / 10000.0)
        exit_price = raw_exit * (1.0 - slippage_bps / 10000.0)
        budget = initial_cash * allocation_fraction
        quantity = math.floor(budget / (entry_price * 100.0))
        if quantity < 1:
            missing_counts["too_expensive"] += 1
            continue
        fees = fee_per_contract * quantity * 2.0
        pnl = (exit_price - entry_price) * quantity * 100.0 - fees
        option_rows.append(
            {
                "candidate_variant_id": queue_item.get("candidate_variant_id"),
                **identity,
                "source_strategy_id": queue_item.get("source_strategy_id"),
                "symbol": symbol,
                "option_type": option_type,
                "contract_symbol": contract_symbol,
                "trade_date": str(trade_date),
                "stock_entry_time": str(entry_time),
                "stock_exit_time": str(exit_time),
                "option_entry_time": str(entry_bar["timestamp"]),
                "option_exit_time": str(exit_bar["timestamp"]),
                "stock_pnl_proxy": round(float(trade.get("pnl") or 0.0), 4),
                "entry_option_close": round(raw_entry, 4),
                "exit_option_close": round(raw_exit, 4),
                "entry_price_after_slippage": round(entry_price, 4),
                "exit_price_after_slippage": round(exit_price, 4),
                "quantity": quantity,
                "fees": round(fees, 4),
                "option_pnl": round(pnl, 4),
                "option_return_pct": round(pnl / (entry_price * quantity * 100.0), 6),
                "stock_exit_reason": trade.get("exit_reason"),
                "contract_selection_method": contract_selection_method,
                "contract_dte": contract.get("dte"),
                "contract_relative_strike_step": contract.get("relative_strike_step"),
                "entry_selection_trade_print_count": _trade_print_count(
                    option_trades=option_trades,
                    option_index=option_index,
                    contract_symbol=contract_symbol,
                    start=entry_time,
                    end=entry_time + max_entry_lag,
                ),
                "option_trade_print_count": _trade_print_count(
                    option_trades=option_trades,
                    option_index=option_index,
                    contract_symbol=contract_symbol,
                    start=entry_time,
                    end=exit_time,
                ),
            }
        )
    return option_rows, int(len(source_trades)), missing_counts


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


def build_option_aware_backtest(
    *,
    queue_json: Path,
    variants_jsonl: Path,
    stock_bars_path: Path,
    selected_contracts_root: Path | None,
    option_bars_root: Path | None,
    option_trades_root: Path | None,
    top_n: int,
    initial_cash: float,
    allocation_fraction: float,
    slippage_bps: float,
    fee_per_contract: float,
    max_entry_lag: timedelta,
    max_exit_lag: timedelta,
    symbol_filter: set[str] | None = None,
    skip_blocked_queue_items: bool = False,
    test_date_count: int = 1,
    contract_selection_method: str = CONTRACT_SELECTION_NEAREST,
) -> dict[str, Any]:
    queue = _load_json(queue_json)
    variants = _variant_map(variants_jsonl)
    stock_bars = _load_stock_bars(stock_bars_path, symbol_filter=symbol_filter)
    contracts, option_bars, option_trades = _load_option_inputs(
        queue=queue,
        selected_contracts_root=selected_contracts_root,
        option_bars_root=option_bars_root,
        option_trades_root=option_trades_root,
        symbol_filter=symbol_filter,
    )
    option_index = _build_option_research_index(
        contracts=contracts,
        option_bars=option_bars,
        option_trades=option_trades,
    )
    all_trade_rows: list[dict[str, Any]] = []
    candidate_summaries: list[dict[str, Any]] = []
    source_queue_items = [item for item in queue.get("queue_items", []) if isinstance(item, dict)]
    filtered_queue_items = []
    for item in source_queue_items:
        if symbol_filter and str(item.get("symbol") or "").upper() not in symbol_filter:
            continue
        if skip_blocked_queue_items and item.get("blockers"):
            continue
        filtered_queue_items.append(item)
    for queue_item in filtered_queue_items[:top_n]:
        if not isinstance(queue_item, dict):
            continue
        variant_id = str(queue_item.get("candidate_variant_id") or "")
        variant = variants.get(variant_id)
        if not variant:
            candidate_summaries.append(
                {
                    "candidate_variant_id": variant_id,
                    "status": "blocked_missing_variant_definition",
                    "promotion_allowed": False,
                    "broker_facing": False,
                }
            )
            continue
        rows, source_trade_count, missing_counts = _option_rows_for_candidate(
            queue_item=queue_item,
            variant=variant,
            stock_bars=stock_bars,
            contracts=contracts,
            option_bars=option_bars,
            option_trades=option_trades,
            option_index=option_index,
            initial_cash=initial_cash,
            allocation_fraction=allocation_fraction,
            slippage_bps=slippage_bps,
            fee_per_contract=fee_per_contract,
            max_entry_lag=max_entry_lag,
            max_exit_lag=max_exit_lag,
            contract_selection_method=contract_selection_method,
        )
        missing_price_count = sum(int(value) for value in missing_counts.values())
        selected_count = max(
            source_trade_count - int(missing_counts.get("no_selected_contract", 0)),
            0,
        )
        entry_fill_count = max(selected_count - int(missing_counts.get("no_entry_bar", 0)), 0)
        filled_order_count = len(rows)
        strategy_fill_coverage = (
            round(filled_order_count / source_trade_count, 4) if source_trade_count else 0.0
        )
        data_foundation_coverage = (
            round(selected_count / source_trade_count, 4) if source_trade_count else 0.0
        )
        entry_bar_coverage = (
            round(entry_fill_count / selected_count, 4) if selected_count else 0.0
        )
        exit_bar_coverage = (
            round(filled_order_count / entry_fill_count, 4) if entry_fill_count else 0.0
        )
        all_trade_rows.extend(rows)
        economics = _summarize_trade_rows(rows)
        split = _split_summary(rows, test_date_count=test_date_count)
        summary = {
            "candidate_variant_id": variant_id,
            "symbol": queue_item.get("symbol"),
            **_candidate_identity(queue_item, variant),
            "source_strategy_id": queue_item.get("source_strategy_id"),
            "directional_option_type": queue_item.get("directional_option_type"),
            "source_stock_trade_count": source_trade_count,
            "missing_option_price_count": missing_price_count,
            "missing_no_selected_contract": int(missing_counts.get("no_selected_contract", 0)),
            "missing_no_entry_bar": int(missing_counts.get("no_entry_bar", 0)),
            "missing_no_exit_bar": int(missing_counts.get("no_exit_bar", 0)),
            "missing_too_expensive": int(missing_counts.get("too_expensive", 0)),
            "intended_order_count": source_trade_count,
            "filled_order_count": filled_order_count,
            "skipped_order_count": missing_price_count,
            "fill_coverage": strategy_fill_coverage,
            "strategy_fill_coverage": strategy_fill_coverage,
            "data_foundation_coverage": data_foundation_coverage,
            "entry_bar_coverage": entry_bar_coverage,
            "exit_bar_coverage": exit_bar_coverage,
            "fill_coverage_numerator": filled_order_count,
            "fill_coverage_denominator": source_trade_count,
            "fill_coverage_unit": "filled_single_contract_option_orders_per_source_stock_trade",
            "fill_coverage_semantics": (
                "Strategy-level fill coverage, not raw option data coverage. "
                "Current engine models one directional option contract per source stock trade."
            ),
            **economics,
            **split,
            "promotion_allowed": False,
            "broker_facing": False,
            "live_manifest_effect": "none",
            "risk_policy_effect": "none",
            "contract_selection_method": contract_selection_method,
            "test_date_count": int(test_date_count),
            "contract_selection_lookahead": (
                "entry_window_only"
                if contract_selection_method == CONTRACT_SELECTION_LIQUIDITY_FIRST
                else "none"
            ),
        }
        summary["fill_failure_reason"] = _fill_failure_reason(summary)
        summary["recommendation"] = _recommendation(summary)
        candidate_summaries.append(summary)
    recommendation_counts: dict[str, int] = {}
    fill_failure_counts: dict[str, int] = {}
    for row in candidate_summaries:
        recommendation = str(row.get("recommendation") or row.get("status") or "unknown")
        recommendation_counts[recommendation] = recommendation_counts.get(recommendation, 0) + 1
        fill_reason = str(row.get("fill_failure_reason") or "unknown")
        fill_failure_counts[fill_reason] = fill_failure_counts.get(fill_reason, 0) + 1
    ranked = sorted(
        candidate_summaries,
        key=lambda row: float(row.get("expectancy") or 0.0),
        reverse=True,
    )
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "status": "completed",
        "queue_json": str(queue_json),
        "variants_jsonl": str(variants_jsonl),
        "stock_bars_path": str(stock_bars_path),
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "source_queue_item_count": len(source_queue_items),
        "queue_item_count_after_filters": len(filtered_queue_items),
        "symbol_filter": sorted(symbol_filter) if symbol_filter else [],
        "skip_blocked_queue_items": bool(skip_blocked_queue_items),
        "test_date_count": int(test_date_count),
        "contract_selection_method": contract_selection_method,
        "option_lookup_mode": "indexed_by_contract_and_symbol",
        "fill_coverage_unit": "filled_single_contract_option_orders_per_source_stock_trade",
        "fill_coverage_semantics": (
            "fill_coverage is an alias for strategy_fill_coverage. "
            "data_foundation_coverage measures selected-contract availability for source trades; "
            "entry_bar_coverage and exit_bar_coverage isolate timing/execution gaps."
        ),
        "option_index_counts": {
            "contract_keys": len(option_index.contracts_by_key),
            "bar_symbols": len(option_index.bars_by_symbol),
            "trade_symbols": len(option_index.trade_timestamps_by_symbol),
        },
        "contract_selection_lookahead": (
            "entry_window_only"
            if contract_selection_method == CONTRACT_SELECTION_LIQUIDITY_FIRST
            else "none"
        ),
        "candidate_count": len(candidate_summaries),
        "option_trade_count": len(all_trade_rows),
        "recommendation_counts": dict(sorted(recommendation_counts.items())),
        "fill_failure_counts": dict(sorted(fill_failure_counts.items())),
        "candidate_summaries": ranked,
        "trade_rows": all_trade_rows,
        "top_research_followups": ranked[:10],
        "next_step_contract": [
            "Keep these results out of deployment and promotion.",
            "Do not treat sparse positive PnL as actionable; minimum fill coverage and out-of-sample option evidence are mandatory.",
            "Use positive option-aware candidates only for data-coverage planning and loser-cluster review.",
            "Expand option quote/bar coverage or rerun with a declared diagnostic lag/selection profile before walk-forward review.",
            "Treat liquidity-first selection as research-only unless separately approved by governance.",
            "Require out-of-sample option-aware evidence before strategy governance review.",
        ],
    }


def _json_default(value: Any) -> str:
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Option-Aware Research Backtest",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Status: `{payload['status']}`",
        f"- Candidate count: `{payload['candidate_count']}`",
        f"- Option trade count: `{payload['option_trade_count']}`",
        f"- Promotion allowed: `{payload['promotion_allowed']}`",
        f"- Broker facing: `{payload['broker_facing']}`",
        f"- Contract selection method: `{payload.get('contract_selection_method')}`",
        f"- Contract selection lookahead: `{payload.get('contract_selection_lookahead')}`",
        f"- Fill coverage unit: `{payload.get('fill_coverage_unit')}`",
        f"- Fill coverage semantics: {payload.get('fill_coverage_semantics')}",
        "",
        "## Recommendation Counts",
        "",
    ]
    for key, value in payload["recommendation_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Fill Failure Counts", ""])
    for key, value in payload.get("fill_failure_counts", {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Top Research Follow-Ups", ""])
    for row in payload["top_research_followups"]:
        lines.append(
            "- "
            f"`{row.get('candidate_variant_id')}` "
            f"expectancy `{row.get('expectancy')}` "
            f"net_pnl `{row.get('net_pnl')}` "
            f"strategy_fill `{row.get('strategy_fill_coverage', row.get('fill_coverage'))}` "
            f"data_foundation `{row.get('data_foundation_coverage')}` "
            f"recommendation `{row.get('recommendation')}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in payload["next_step_contract"]:
        lines.append(f"- {item}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_artifacts(output_dir: Path, run_id: str, payload: dict[str, Any]) -> dict[str, str]:
    run_dir = output_dir / run_id
    artifacts = {
        "manifest": run_dir / "option_aware_research_run_manifest.json",
        "manifest_md": run_dir / "option_aware_research_run_manifest.md",
        "trade_economics": run_dir / "option_aware_trade_economics.csv",
        "trade_economics_json": run_dir / "option_aware_trade_economics.json",
        "candidate_summary": run_dir / "option_aware_candidate_summary.csv",
        "candidate_summary_json": run_dir / "option_aware_candidate_summary.json",
        "recommendation_packet": run_dir / "option_aware_recommendation_packet.json",
        "recommendation_packet_md": run_dir / "option_aware_recommendation_packet.md",
    }
    write_json(
        artifacts["manifest"], {key: value for key, value in payload.items() if key != "trade_rows"}
    )
    write_markdown(artifacts["manifest_md"], payload)
    write_csv(artifacts["trade_economics"], payload["trade_rows"])
    write_json(artifacts["trade_economics_json"], payload["trade_rows"])
    write_csv(artifacts["candidate_summary"], payload["candidate_summaries"])
    write_json(artifacts["candidate_summary_json"], payload["candidate_summaries"])
    recommendation = {
        "generated_at": payload["generated_at"],
        "promotion_allowed": False,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "recommendation_counts": payload["recommendation_counts"],
        "fill_failure_counts": payload.get("fill_failure_counts", {}),
        "top_research_followups": payload["top_research_followups"],
        "next_step_contract": payload["next_step_contract"],
    }
    write_json(artifacts["recommendation_packet"], recommendation)
    write_markdown(artifacts["recommendation_packet_md"], payload)
    return {key: str(value) for key, value in artifacts.items()}


def main() -> None:
    args = parse_args()
    run_id = (
        args.run_id
        or f"option_aware_research_{datetime.now().astimezone().strftime('%Y%m%d_%H%M%S')}"
    )
    payload = build_option_aware_backtest(
        queue_json=Path(args.queue_json),
        variants_jsonl=Path(args.variants_jsonl),
        stock_bars_path=Path(args.stock_bars_path),
        selected_contracts_root=(
            Path(args.selected_contracts_root) if args.selected_contracts_root else None
        ),
        option_bars_root=Path(args.option_bars_root) if args.option_bars_root else None,
        option_trades_root=Path(args.option_trades_root) if args.option_trades_root else None,
        top_n=args.top_n,
        symbol_filter=_symbol_filter(args.symbol_filter),
        skip_blocked_queue_items=args.skip_blocked_queue_items,
        initial_cash=args.initial_cash,
        allocation_fraction=args.allocation_fraction,
        slippage_bps=args.slippage_bps,
        fee_per_contract=args.fee_per_contract,
        max_entry_lag=timedelta(minutes=args.max_entry_lag_minutes),
        max_exit_lag=timedelta(minutes=args.max_exit_lag_minutes),
        test_date_count=args.test_date_count,
        contract_selection_method=args.contract_selection_method,
    )
    payload["run_id"] = run_id
    payload["artifacts"] = write_artifacts(Path(args.output_dir), run_id, payload)
    print(
        json.dumps({key: value for key, value in payload.items() if key != "trade_rows"}, indent=2)
    )


if __name__ == "__main__":
    main()

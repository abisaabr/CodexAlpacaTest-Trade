from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.backtest.engine import FixedFractionSizer, LinearCostModel, run_backtest
from alpaca_lab.qqq_portfolio.greeks import bs_greeks, implied_volatility
from scripts.run_gcp_research_wave import (
    _variant_stock_strategy,
    load_variants,
    variant_timing_parameters,
)

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
CONTRACT_SELECTION_DELTA_TARGET = "entry_delta_target_research_only"
ENTRY_LOOKUP_AT_OR_AFTER = "first_bar_at_or_after_entry_within_lag"
ENTRY_LOOKUP_AT_OR_AFTER_OR_ASOF = "first_bar_at_or_after_or_asof_entry_within_lag"
EXIT_LOOKUP_AT_OR_AFTER = "first_bar_at_or_after_exit_within_lag"
EXIT_LOOKUP_AT_OR_AFTER_OR_PRIOR = "first_bar_at_or_after_or_prior_exit_within_lag"
OPTION_EXIT_STOCK_PROXY = "stock_proxy_exit"
OPTION_EXIT_PREMIUM_TARGET_STOP = "premium_target_stop"
STOCK_SESSION_FILTER_NONE = "none"
STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY = "option_rth_same_day"
RUNTIME_PARITY_NONE = "none"
RUNTIME_PARITY_PAPER_SNAPSHOT_GREEKS = "paper_snapshot_greeks"
OPTION_SESSION_TIMEZONE = "America/New_York"
OPTION_SESSION_START = "09:35"
OPTION_SESSION_END = "15:55"
STRATEGY_FILL_COVERAGE_GATE = 0.90
REGIME_TOKENS = {"bull", "bear", "choppy"}
CANDIDATE_SELECTION_PRIORITY_ORDER = "priority_order"
CANDIDATE_SELECTION_REGIME_BALANCED = "regime_balanced"
DEFAULT_REGIME_BALANCE_ORDER = ("bull", "bear", "choppy", "unclassified")


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
    parser.add_argument(
        "--progress-dir",
        default=None,
        help=(
            "Optional directory for append-only per-candidate progress JSONL. "
            "Useful for long GCP shards where final artifacts are written at completion."
        ),
    )
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument(
        "--candidate-selection-mode",
        choices=[CANDIDATE_SELECTION_PRIORITY_ORDER, CANDIDATE_SELECTION_REGIME_BALANCED],
        default=CANDIDATE_SELECTION_PRIORITY_ORDER,
        help=(
            "How to build the candidate window before sharding. The default preserves "
            "existing queue priority order. regime_balanced round-robins bull/bear/choppy "
            "items so global top-N sweeps do not starve non-bull regimes."
        ),
    )
    parser.add_argument(
        "--regime-balance-order",
        default=",".join(DEFAULT_REGIME_BALANCE_ORDER),
        help=(
            "Comma-separated regime order used when --candidate-selection-mode "
            "is regime_balanced."
        ),
    )
    parser.add_argument(
        "--candidate-start-index",
        type=int,
        default=1,
        help=(
            "One-based start index inside the filtered top-N queue. Used only for "
            "research sharding; default preserves the original full top-N run."
        ),
    )
    parser.add_argument(
        "--candidate-count",
        type=int,
        default=None,
        help=(
            "Optional number of candidates to replay from --candidate-start-index. "
            "When omitted, replay runs through the end of the filtered top-N queue."
        ),
    )
    parser.add_argument(
        "--symbol-filter", default=None, help="Optional comma-separated symbol allowlist."
    )
    parser.add_argument(
        "--skip-blocked-queue-items",
        action="store_true",
        help="Skip queue items carrying blockers such as missing selected contracts.",
    )
    parser.add_argument("--max-entry-lag-minutes", type=float, default=10.0)
    parser.add_argument(
        "--entry-bar-lookup-mode",
        choices=[ENTRY_LOOKUP_AT_OR_AFTER, ENTRY_LOOKUP_AT_OR_AFTER_OR_ASOF],
        default=ENTRY_LOOKUP_AT_OR_AFTER,
        help=(
            "Entry lookup semantics. The default uses the first option bar at or after "
            "the stock signal. The as-of mode is research-only and falls back to the "
            "latest known prior option bar within --max-entry-staleness-minutes."
        ),
    )
    parser.add_argument("--max-entry-staleness-minutes", type=float, default=5.0)
    parser.add_argument("--max-exit-lag-minutes", type=float, default=10.0)
    parser.add_argument(
        "--exit-bar-lookup-mode",
        choices=[EXIT_LOOKUP_AT_OR_AFTER, EXIT_LOOKUP_AT_OR_AFTER_OR_PRIOR],
        default=EXIT_LOOKUP_AT_OR_AFTER,
        help=(
            "Exit lookup semantics. The default uses the first option bar at or after "
            "the stock exit signal. The prior-bar fallback is research-diagnostic only."
        ),
    )
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
        "--stock-session-filter",
        choices=[STOCK_SESSION_FILTER_NONE, STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY],
        default=STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY,
        help=(
            "Filter source stock-proxy trades before option replay. The default keeps "
            "same-day trades whose entry and exit are inside the option RTH window."
        ),
    )
    parser.add_argument(
        "--contract-selection-method",
        choices=[
            CONTRACT_SELECTION_NEAREST,
            CONTRACT_SELECTION_LIQUIDITY_FIRST,
            CONTRACT_SELECTION_DELTA_TARGET,
        ],
        default=CONTRACT_SELECTION_NEAREST,
        help=(
            "Research-only contract selector. The default preserves the nearest-contract "
            "path; liquidity-first and delta-target only use entry-window information "
            "and are not broker-facing."
        ),
    )
    parser.add_argument(
        "--runtime-parity-mode",
        choices=[RUNTIME_PARITY_NONE, RUNTIME_PARITY_PAPER_SNAPSHOT_GREEKS],
        default=RUNTIME_PARITY_NONE,
        help=(
            "Apply a named backtest contract that mirrors production paper execution "
            "semantics. paper_snapshot_greeks uses at-or-after option bars, disables "
            "prior/as-of entry staleness, keeps option RTH same-day source trades, and "
            "defaults nearest-contract requests to the delta-target selector."
        ),
    )
    return parser.parse_args()


def apply_runtime_parity_mode(args: argparse.Namespace) -> argparse.Namespace:
    if args.runtime_parity_mode == RUNTIME_PARITY_NONE:
        return args
    if args.runtime_parity_mode != RUNTIME_PARITY_PAPER_SNAPSHOT_GREEKS:
        raise ValueError(f"Unsupported runtime_parity_mode={args.runtime_parity_mode}")

    args.entry_bar_lookup_mode = ENTRY_LOOKUP_AT_OR_AFTER
    args.exit_bar_lookup_mode = EXIT_LOOKUP_AT_OR_AFTER
    args.max_entry_staleness_minutes = 0.0
    args.stock_session_filter = STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY
    if args.contract_selection_method == CONTRACT_SELECTION_NEAREST:
        args.contract_selection_method = CONTRACT_SELECTION_DELTA_TARGET
    return args


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


def _infer_intended_regime(*values: object) -> str:
    for value in values:
        tokens = re.split(r"[^a-z0-9]+", str(value or "").lower())
        for token in tokens:
            if token in REGIME_TOKENS:
                return token
    return ""


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
    candidate_id = queue_item.get("candidate_variant_id") or variant.get("variant_id") or ""
    return {
        "strategy_id": str(strategy_id),
        "family": str(queue_item.get("family") or variant.get("family") or ""),
        "intended_regime": str(
            queue_item.get("intended_regime")
            or variant.get("intended_regime")
            or _infer_intended_regime(strategy_id, candidate_id)
        ),
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


def _filter_contracts_for_dte_mode(frame: pd.DataFrame, dte_mode: str | None) -> pd.DataFrame:
    if frame.empty or "dte" not in frame.columns:
        return frame
    mode = str(dte_mode or "").lower()
    if not mode:
        return frame
    dte = pd.to_numeric(frame["dte"], errors="coerce")
    if mode == "same_day":
        return frame[dte <= 0].copy()
    if mode in {"next", "next_expiry", "next_trading_day"}:
        eligible = frame[dte > 0].copy()
        eligible_dte = pd.to_numeric(eligible["dte"], errors="coerce")
        if eligible.empty:
            eligible = frame.copy()
            eligible_dte = dte
        min_dte = eligible_dte.min()
        return eligible[eligible_dte == min_dte].copy()
    return frame


def _choose_contract(
    *,
    contracts: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    symbol: str,
    option_type: str,
    trade_date: Any,
    dte_mode: str | None = None,
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
    frame = _filter_contracts_for_dte_mode(frame, dte_mode)
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
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
    dte_mode: str | None = None,
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
    frame = _filter_contracts_for_dte_mode(frame, dte_mode)
    if frame.empty:
        return None, None, "no_selected_contract"

    choices: list[tuple[tuple[float, ...], str, dict[str, Any], dict[str, Any]]] = []
    for contract in frame.to_dict("records"):
        contract_symbol = str(contract["symbol"])
        entry_bar = _entry_option_bar(
            option_bars=option_bars,
            option_index=option_index,
            contract_symbol=contract_symbol,
            timestamp=entry_time,
            max_lag=max_lag,
            lookup_mode=entry_lookup_mode,
            max_staleness=max_entry_staleness,
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


def _choose_entry_delta_target_contract(
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
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
    entry_spot: float | None,
    parameters: dict[str, Any],
    dte_mode: str | None = None,
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
    frame = _filter_contracts_for_dte_mode(frame, dte_mode)
    if frame.empty:
        return None, None, "no_selected_contract"

    default_target = -0.55 if option_type.lower() == "put" else 0.55
    target_delta = _float_parameter(parameters, "target_delta", default_target)
    min_abs_delta = _float_parameter(parameters, "min_abs_delta", 0.05)
    max_abs_delta = _float_parameter(parameters, "max_abs_delta", 0.95)
    choices: list[tuple[tuple[float, ...], str, dict[str, Any], dict[str, Any]]] = []
    saw_entry_bar = False
    saw_greek_snapshot = False
    for contract in frame.to_dict("records"):
        contract_symbol = str(contract["symbol"])
        entry_bar = _entry_option_bar(
            option_bars=option_bars,
            option_index=option_index,
            contract_symbol=contract_symbol,
            timestamp=entry_time,
            max_lag=max_lag,
            lookup_mode=entry_lookup_mode,
            max_staleness=max_entry_staleness,
        )
        if not entry_bar:
            continue
        saw_entry_bar = True
        greek_snapshot = _entry_bar_greek_snapshot(
            contract=contract,
            entry_bar=entry_bar,
            entry_spot=entry_spot,
        )
        if greek_snapshot is None:
            continue
        saw_greek_snapshot = True
        delta = float(greek_snapshot["entry_delta"])
        if abs(delta) < min_abs_delta or abs(delta) > max_abs_delta:
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
        greek_contract = {**contract, **greek_snapshot}
        # No future bars are used here: the selector ranks only entry-window Greeks and liquidity.
        rank_key = (abs(delta - target_delta), -float(prints), -volume, abs_step, dte)
        choices.append((rank_key, contract_symbol, greek_contract, entry_bar))

    if not choices:
        if saw_greek_snapshot:
            return None, None, "no_selected_contract"
        if saw_entry_bar:
            return None, None, "no_greek_snapshot"
        return None, None, "no_entry_bar"
    choices.sort(key=lambda item: (item[0], item[1]))
    _, _, contract, entry_bar = choices[0]
    return contract, entry_bar, "selected"


def _variant_parameters(queue_item: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    value = queue_item.get("parameter_set") or variant.get("parameters") or {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return value if isinstance(value, dict) else {}


def _option_structure_family(queue_item: dict[str, Any], variant: dict[str, Any]) -> str:
    parameters = _variant_parameters(queue_item, variant)
    raw = (
        parameters.get("family_template")
        or queue_item.get("family")
        or variant.get("family")
        or queue_item.get("source_strategy_id")
        or variant.get("source_strategy_id")
        or ""
    )
    return re.sub(r"[^a-z0-9]+", "_", str(raw).lower()).strip("_")


def _contract_frame_for_type(
    *,
    contracts: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    symbol: str,
    option_type: str,
    trade_date: Any,
    dte_mode: str | None = None,
) -> pd.DataFrame:
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
    ).copy()
    return _filter_contracts_for_dte_mode(frame, dte_mode)


def _contract_strike(contract: dict[str, Any]) -> float:
    value = contract.get("strike_price", contract.get("strike"))
    return float(value)


def _bar_mark_price(bar: dict[str, Any]) -> float | None:
    for key in ("close", "vwap", "open"):
        value = bar.get(key)
        if value not in (None, ""):
            try:
                price = float(value)
            except (TypeError, ValueError):
                continue
            if price > 0.0:
                return price
    return None


def _contract_expiration_date(contract: dict[str, Any]) -> Any | None:
    for key in ("expiration_date", "expiration", "expiry"):
        value = contract.get(key)
        if value not in (None, ""):
            return value
    return None


def _years_to_option_expiry(contract: dict[str, Any], timestamp: pd.Timestamp) -> float:
    expiration_value = _contract_expiration_date(contract)
    if expiration_value is None:
        dte_value = contract.get("dte")
        try:
            return max(float(dte_value), 1.0 / (24.0 * 60.0)) / 365.0
        except (TypeError, ValueError):
            return 1.0 / (365.0 * 24.0)
    expiration_date = pd.Timestamp(expiration_value).date()
    expiry_et = pd.Timestamp(datetime.combine(expiration_date, time(16, 0))).tz_localize(
        OPTION_SESSION_TIMEZONE
    )
    ts = pd.Timestamp(timestamp)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    seconds = max(60.0, (expiry_et.tz_convert("UTC") - ts).total_seconds())
    return seconds / (365.0 * 24.0 * 3600.0)


def _entry_bar_greek_snapshot(
    *,
    contract: dict[str, Any],
    entry_bar: dict[str, Any],
    entry_spot: float | None,
) -> dict[str, float] | None:
    if entry_spot is None or entry_spot <= 0.0:
        return None
    market_price = _bar_mark_price(entry_bar)
    if market_price is None:
        return None
    option_type = str(contract.get("option_type") or "").lower()
    if option_type not in {"call", "put"}:
        return None
    strike = _contract_strike(contract)
    years = _years_to_option_expiry(contract, pd.Timestamp(entry_bar["timestamp"]))
    iv = implied_volatility(
        spot=float(entry_spot),
        strike=strike,
        years=years,
        market_price=market_price,
        option_type=option_type,
    )
    if iv is None:
        return None
    greeks = bs_greeks(
        spot=float(entry_spot),
        strike=strike,
        years=years,
        sigma=iv,
        option_type=option_type,
    )
    return {
        "entry_implied_vol": float(iv),
        "entry_delta": float(greeks["delta"]),
        "entry_gamma": float(greeks["gamma"]),
        "entry_theta": float(greeks["theta"]),
        "entry_vega": float(greeks["vega"]),
        "entry_greek_spot": float(entry_spot),
        "entry_greek_years": float(years),
    }


def _contract_entry_bar(
    *,
    contract: dict[str, Any],
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    entry_time: pd.Timestamp,
    max_lag: timedelta,
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
) -> dict[str, Any] | None:
    return _entry_option_bar(
        option_bars=option_bars,
        option_index=option_index,
        contract_symbol=str(contract["symbol"]),
        timestamp=entry_time,
        max_lag=max_lag,
        lookup_mode=entry_lookup_mode,
        max_staleness=max_entry_staleness,
    )


def _select_contract_with_entry(
    *,
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_trades: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    symbol: str,
    option_type: str,
    trade_date: Any,
    entry_time: pd.Timestamp,
    max_lag: timedelta,
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
    contract_selection_method: str,
    entry_spot: float | None = None,
    parameters: dict[str, Any] | None = None,
    dte_mode: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    if contract_selection_method == CONTRACT_SELECTION_LIQUIDITY_FIRST:
        return _choose_entry_liquidity_first_contract(
            contracts=contracts,
            option_bars=option_bars,
            option_trades=option_trades,
            option_index=option_index,
            symbol=symbol,
            option_type=option_type,
            trade_date=trade_date,
            entry_time=entry_time,
            max_lag=max_lag,
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
            dte_mode=dte_mode,
        )
    if contract_selection_method == CONTRACT_SELECTION_DELTA_TARGET:
        return _choose_entry_delta_target_contract(
            contracts=contracts,
            option_bars=option_bars,
            option_trades=option_trades,
            option_index=option_index,
            symbol=symbol,
            option_type=option_type,
            trade_date=trade_date,
            entry_time=entry_time,
            max_lag=max_lag,
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
            entry_spot=entry_spot,
            parameters=parameters or {},
            dte_mode=dte_mode,
        )

    contract = _choose_contract(
        contracts=contracts,
        option_index=option_index,
        symbol=symbol,
        option_type=option_type,
        trade_date=trade_date,
        dte_mode=dte_mode,
    )
    if not contract:
        return None, None, "no_selected_contract"
    entry_bar = _contract_entry_bar(
        contract=contract,
        option_bars=option_bars,
        option_index=option_index,
        entry_time=entry_time,
        max_lag=max_lag,
        entry_lookup_mode=entry_lookup_mode,
        max_entry_staleness=max_entry_staleness,
    )
    if not entry_bar:
        return contract, None, "no_entry_bar"
    return contract, entry_bar, "selected"


def _select_wing_contract_with_entry(
    *,
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    symbol: str,
    option_type: str,
    trade_date: Any,
    base_contract: dict[str, Any],
    higher: bool,
    width_steps: int,
    entry_time: pd.Timestamp,
    max_lag: timedelta,
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    frame = _contract_frame_for_type(
        contracts=contracts,
        option_index=option_index,
        symbol=symbol,
        option_type=option_type,
        trade_date=trade_date,
    )
    if frame.empty:
        return None, None, "no_selected_contract"

    base_strike = _contract_strike(base_contract)
    base_dte = base_contract.get("dte")
    if base_dte is not None and "dte" in frame.columns:
        same_dte = frame[pd.to_numeric(frame["dte"], errors="coerce") == float(base_dte)]
        if not same_dte.empty:
            frame = same_dte.copy()
    strikes = pd.to_numeric(frame.get("strike_price", frame.get("strike")), errors="coerce")
    if higher:
        candidates = frame[strikes > base_strike].copy()
        candidates["_strike_sort"] = pd.to_numeric(
            candidates.get("strike_price", candidates.get("strike")), errors="coerce"
        )
        candidates = candidates.sort_values(["_strike_sort", "symbol"])
    else:
        candidates = frame[strikes < base_strike].copy()
        candidates["_strike_sort"] = pd.to_numeric(
            candidates.get("strike_price", candidates.get("strike")), errors="coerce"
        )
        candidates = candidates.sort_values(["_strike_sort", "symbol"], ascending=[False, True])
    if candidates.empty:
        return None, None, "no_selected_contract"

    width_steps = max(1, int(width_steps))
    candidate_rows = candidates.to_dict("records")
    ordered = candidate_rows[width_steps - 1 :] + candidate_rows[: width_steps - 1]
    saw_candidate = False
    for contract in ordered:
        saw_candidate = True
        entry_bar = _contract_entry_bar(
            contract=contract,
            option_bars=option_bars,
            option_index=option_index,
            entry_time=entry_time,
            max_lag=max_lag,
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
        )
        if entry_bar:
            return contract, entry_bar, "selected"
    return (candidate_rows[0], None, "no_entry_bar") if saw_candidate else (None, None, "no_selected_contract")


def _select_matching_body_contract_with_entry(
    *,
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    symbol: str,
    option_type: str,
    trade_date: Any,
    base_contract: dict[str, Any],
    entry_time: pd.Timestamp,
    max_lag: timedelta,
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    frame = _contract_frame_for_type(
        contracts=contracts,
        option_index=option_index,
        symbol=symbol,
        option_type=option_type,
        trade_date=trade_date,
    )
    if frame.empty:
        return None, None, "no_selected_contract"

    base_strike = _contract_strike(base_contract)
    strikes = pd.to_numeric(frame.get("strike_price", frame.get("strike")), errors="coerce")
    candidates = frame[strikes == base_strike].copy()
    base_dte = base_contract.get("dte")
    if base_dte is not None and "dte" in candidates.columns:
        candidates = candidates[pd.to_numeric(candidates["dte"], errors="coerce") == float(base_dte)]
    if candidates.empty:
        return None, None, "no_selected_contract"

    for contract in candidates.sort_values("symbol").to_dict("records"):
        entry_bar = _contract_entry_bar(
            contract=contract,
            option_bars=option_bars,
            option_index=option_index,
            entry_time=entry_time,
            max_lag=max_lag,
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
        )
        if entry_bar:
            return contract, entry_bar, "selected"
    return candidates.iloc[0].to_dict(), None, "no_entry_bar"


def _leg(
    *,
    role: str,
    side: int,
    ratio: int,
    contract: dict[str, Any],
    entry_bar: dict[str, Any],
) -> dict[str, Any]:
    return {
        "role": role,
        "side": int(side),
        "ratio": int(ratio),
        "contract": contract,
        "entry_bar": entry_bar,
    }


def _option_structure_legs(
    *,
    queue_item: dict[str, Any],
    variant: dict[str, Any],
    contracts: pd.DataFrame,
    option_bars: pd.DataFrame,
    option_trades: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    symbol: str,
    trade_date: Any,
    entry_time: pd.Timestamp,
    max_entry_lag: timedelta,
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
    contract_selection_method: str,
    entry_spot: float | None = None,
) -> tuple[list[dict[str, Any]], str, str]:
    family = _option_structure_family(queue_item, variant)
    parameters = _variant_parameters(queue_item, variant)
    dte_mode = str(parameters.get("dte_mode") or "").lower()
    option_type = str(queue_item.get("directional_option_type") or "").lower()
    vertical_width = int(parameters.get("vertical_width_steps") or parameters.get("wing_width_steps") or 1)
    far_width = int(parameters.get("far_wing_width_steps") or max(vertical_width + 1, 2))
    short_width = int(parameters.get("short_width_steps") or parameters.get("body_width_steps") or 1)

    def base(option_type_value: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
        return _select_contract_with_entry(
            contracts=contracts,
            option_bars=option_bars,
            option_trades=option_trades,
            option_index=option_index,
            symbol=symbol,
            option_type=option_type_value,
            trade_date=trade_date,
            entry_time=entry_time,
            max_lag=max_entry_lag,
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
            contract_selection_method=contract_selection_method,
            entry_spot=entry_spot,
            parameters=parameters,
            dte_mode=dte_mode,
        )

    def wing(
        option_type_value: str,
        base_contract: dict[str, Any],
        *,
        higher: bool,
        width_steps: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
        return _select_wing_contract_with_entry(
            contracts=contracts,
            option_bars=option_bars,
            option_index=option_index,
            symbol=symbol,
            option_type=option_type_value,
            trade_date=trade_date,
            base_contract=base_contract,
            higher=higher,
            width_steps=width_steps,
            entry_time=entry_time,
            max_lag=max_entry_lag,
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
        )

    if "iron_butterfly" in family:
        call_frame = _contract_frame_for_type(
            contracts=contracts,
            option_index=option_index,
            symbol=symbol,
            option_type="call",
            trade_date=trade_date,
            dte_mode=dte_mode,
        )
        if call_frame.empty:
            return [], "iron_butterfly", "no_selected_contract"
        choices: list[tuple[tuple[float, ...], str, list[dict[str, Any]]]] = []
        statuses: list[str] = []
        for call_contract in call_frame.to_dict("records"):
            call_entry = _contract_entry_bar(
                contract=call_contract,
                option_bars=option_bars,
                option_index=option_index,
                entry_time=entry_time,
                max_lag=max_entry_lag,
                entry_lookup_mode=entry_lookup_mode,
                max_entry_staleness=max_entry_staleness,
            )
            if not call_entry:
                statuses.append("no_entry_bar")
                continue
            put_contract, put_entry, status = _select_matching_body_contract_with_entry(
                contracts=contracts,
                option_bars=option_bars,
                option_index=option_index,
                symbol=symbol,
                option_type="put",
                trade_date=trade_date,
                base_contract=call_contract,
                entry_time=entry_time,
                max_lag=max_entry_lag,
                entry_lookup_mode=entry_lookup_mode,
                max_entry_staleness=max_entry_staleness,
            )
            if status != "selected" or not put_contract or not put_entry:
                statuses.append(status)
                continue
            call_wing, call_wing_entry, status = wing(
                "call", call_contract, higher=True, width_steps=vertical_width
            )
            if status != "selected" or not call_wing or not call_wing_entry:
                statuses.append(status)
                continue
            put_wing, put_wing_entry, status = wing(
                "put", put_contract, higher=False, width_steps=vertical_width
            )
            if status != "selected" or not put_wing or not put_wing_entry:
                statuses.append(status)
                continue
            legs = [
                _leg(role="short_call_body", side=-1, ratio=1, contract=call_contract, entry_bar=call_entry),
                _leg(role="short_put_body", side=-1, ratio=1, contract=put_contract, entry_bar=put_entry),
                _leg(role="long_call_wing", side=1, ratio=1, contract=call_wing, entry_bar=call_wing_entry),
                _leg(role="long_put_wing", side=1, ratio=1, contract=put_wing, entry_bar=put_wing_entry),
            ]
            volume = float(call_entry.get("volume") or 0.0) + float(put_entry.get("volume") or 0.0)
            abs_step = abs(float(call_contract.get("relative_strike_step") or 0.0))
            dte = float(call_contract.get("dte") or 999.0)
            strike = _contract_strike(call_contract)
            choices.append(((-volume, abs_step, dte, strike), str(call_contract["symbol"]), legs))
        if not choices:
            status = "no_entry_bar" if statuses and all(item == "no_entry_bar" for item in statuses) else "no_selected_contract"
            return [], "iron_butterfly", status
        choices.sort(key=lambda item: (item[0], item[1]))
        return choices[0][2], "iron_butterfly", "selected"

    if "iron_condor" in family or "premium_defense_spread" in family:
        call_frame = _contract_frame_for_type(
            contracts=contracts,
            option_index=option_index,
            symbol=symbol,
            option_type="call",
            trade_date=trade_date,
            dte_mode=dte_mode,
        )
        if call_frame.empty:
            return [], "iron_condor", "no_selected_contract"
        choices: list[tuple[tuple[float, ...], str, list[dict[str, Any]]]] = []
        statuses: list[str] = []
        for call_body in call_frame.to_dict("records"):
            put_body, put_body_entry, status = _select_matching_body_contract_with_entry(
                contracts=contracts,
                option_bars=option_bars,
                option_index=option_index,
                symbol=symbol,
                option_type="put",
                trade_date=trade_date,
                base_contract=call_body,
                entry_time=entry_time,
                max_lag=max_entry_lag,
                entry_lookup_mode=entry_lookup_mode,
                max_entry_staleness=max_entry_staleness,
            )
            if status != "selected" or not put_body or not put_body_entry:
                statuses.append(status)
                continue
            short_call, short_call_entry, status = wing(
                "call", call_body, higher=True, width_steps=short_width
            )
            if status != "selected" or not short_call or not short_call_entry:
                statuses.append(status)
                continue
            long_call, long_call_entry, status = wing(
                "call", short_call, higher=True, width_steps=vertical_width
            )
            if status != "selected" or not long_call or not long_call_entry:
                statuses.append(status)
                continue
            short_put, short_put_entry, status = wing(
                "put", put_body, higher=False, width_steps=short_width
            )
            if status != "selected" or not short_put or not short_put_entry:
                statuses.append(status)
                continue
            long_put, long_put_entry, status = wing(
                "put", short_put, higher=False, width_steps=vertical_width
            )
            if status != "selected" or not long_put or not long_put_entry:
                statuses.append(status)
                continue
            legs = [
                _leg(role="short_call", side=-1, ratio=1, contract=short_call, entry_bar=short_call_entry),
                _leg(role="long_call_wing", side=1, ratio=1, contract=long_call, entry_bar=long_call_entry),
                _leg(role="short_put", side=-1, ratio=1, contract=short_put, entry_bar=short_put_entry),
                _leg(role="long_put_wing", side=1, ratio=1, contract=long_put, entry_bar=long_put_entry),
            ]
            volume = float(short_call_entry.get("volume") or 0.0) + float(
                short_put_entry.get("volume") or 0.0
            )
            abs_step = abs(float(call_body.get("relative_strike_step") or 0.0))
            dte = float(call_body.get("dte") or 999.0)
            strike = _contract_strike(call_body)
            choices.append(((-volume, abs_step, dte, strike), str(call_body["symbol"]), legs))
        if not choices:
            status = "no_entry_bar" if statuses and all(item == "no_entry_bar" for item in statuses) else "no_selected_contract"
            return [], "iron_condor", status
        choices.sort(key=lambda item: (item[0], item[1]))
        return choices[0][2], "iron_condor", "selected"

    if "bull_put_credit_spread" in family or "credit_put_vertical" in family:
        put_body, put_body_entry, status = base("put")
        if status != "selected" or not put_body or not put_body_entry:
            return [], "credit_put_vertical", status
        short_put, short_put_entry, status = wing(
            "put", put_body, higher=False, width_steps=short_width
        )
        if status != "selected" or not short_put or not short_put_entry:
            return [], "credit_put_vertical", status
        long_put, long_put_entry, status = wing(
            "put", short_put, higher=False, width_steps=vertical_width
        )
        if status != "selected" or not long_put or not long_put_entry:
            return [], "credit_put_vertical", status
        return (
            [
                _leg(role="short_put", side=-1, ratio=1, contract=short_put, entry_bar=short_put_entry),
                _leg(role="long_put_wing", side=1, ratio=1, contract=long_put, entry_bar=long_put_entry),
            ],
            "credit_put_vertical",
            "selected",
        )

    if "bear_call_credit_spread" in family or "credit_call_vertical" in family:
        call_body, call_body_entry, status = base("call")
        if status != "selected" or not call_body or not call_body_entry:
            return [], "credit_call_vertical", status
        short_call, short_call_entry, status = wing(
            "call", call_body, higher=True, width_steps=short_width
        )
        if status != "selected" or not short_call or not short_call_entry:
            return [], "credit_call_vertical", status
        long_call, long_call_entry, status = wing(
            "call", short_call, higher=True, width_steps=vertical_width
        )
        if status != "selected" or not long_call or not long_call_entry:
            return [], "credit_call_vertical", status
        return (
            [
                _leg(role="short_call", side=-1, ratio=1, contract=short_call, entry_bar=short_call_entry),
                _leg(role="long_call_wing", side=1, ratio=1, contract=long_call, entry_bar=long_call_entry),
            ],
            "credit_call_vertical",
            "selected",
        )

    if "debit_call_vertical" in family or "debit_put_vertical" in family:
        option_type = "put" if "put" in family else "call"
        long_contract, long_entry, status = base(option_type)
        if status != "selected" or not long_contract or not long_entry:
            return [], f"debit_{option_type}_vertical", status
        short_contract, short_entry, status = wing(
            option_type,
            long_contract,
            higher=option_type == "call",
            width_steps=vertical_width,
        )
        if status != "selected" or not short_contract or not short_entry:
            return [], f"debit_{option_type}_vertical", status
        return (
            [
                _leg(role=f"long_{option_type}", side=1, ratio=1, contract=long_contract, entry_bar=long_entry),
                _leg(role=f"short_{option_type}_wing", side=-1, ratio=1, contract=short_contract, entry_bar=short_entry),
            ],
            f"debit_{option_type}_vertical",
            "selected",
        )

    if "broken_wing_call_butterfly" in family or "broken_wing_put_butterfly" in family:
        option_type = "put" if "put" in family else "call"
        long_body, long_body_entry, status = base(option_type)
        if status != "selected" or not long_body or not long_body_entry:
            return [], f"broken_wing_{option_type}_butterfly", status
        short_mid, short_mid_entry, status = wing(
            option_type,
            long_body,
            higher=option_type == "call",
            width_steps=vertical_width,
        )
        if status != "selected" or not short_mid or not short_mid_entry:
            return [], f"broken_wing_{option_type}_butterfly", status
        long_far, long_far_entry, status = wing(
            option_type,
            long_body,
            higher=option_type == "call",
            width_steps=far_width,
        )
        if status != "selected" or not long_far or not long_far_entry:
            return [], f"broken_wing_{option_type}_butterfly", status
        return (
            [
                _leg(role=f"long_{option_type}_body", side=1, ratio=1, contract=long_body, entry_bar=long_body_entry),
                _leg(role=f"short_{option_type}_middle", side=-1, ratio=2, contract=short_mid, entry_bar=short_mid_entry),
                _leg(role=f"long_{option_type}_far_wing", side=1, ratio=1, contract=long_far, entry_bar=long_far_entry),
            ],
            f"broken_wing_{option_type}_butterfly",
            "selected",
        )

    if not option_type:
        return [], "unsupported", "unsupported_option_structure"
    contract, entry_bar, status = base(option_type)
    if status != "selected" or not contract or not entry_bar:
        return [], "single_leg", status
    return (
        [_leg(role=f"long_{option_type}", side=1, ratio=1, contract=contract, entry_bar=entry_bar)],
        "single_leg",
        "selected",
    )


def _structure_risk_per_unit(legs: list[dict[str, Any]], entry_debit_per_unit: float) -> float:
    if entry_debit_per_unit > 0:
        return entry_debit_per_unit
    spread_widths = _credit_structure_spread_widths(legs)
    strikes = [_contract_strike(leg["contract"]) for leg in legs]
    max_width = max(spread_widths) if spread_widths else max(strikes) - min(strikes) if strikes else 0.0
    credit = abs(entry_debit_per_unit)
    defined_risk = max_width * 100.0 - credit
    return max(defined_risk, max_width * 100.0 * 0.25, 0.01)


def _credit_structure_spread_widths(legs: list[dict[str, Any]]) -> list[float]:
    call_widths: list[float] = []
    put_widths: list[float] = []
    for short_leg in legs:
        if int(short_leg.get("side", 0)) >= 0:
            continue
        short_type = str(short_leg["contract"].get("option_type") or "").lower()
        short_strike = _contract_strike(short_leg["contract"])
        for long_leg in legs:
            if int(long_leg.get("side", 0)) <= 0:
                continue
            long_type = str(long_leg["contract"].get("option_type") or "").lower()
            if long_type != short_type:
                continue
            long_strike = _contract_strike(long_leg["contract"])
            if short_type == "call" and long_strike > short_strike:
                call_widths.append(long_strike - short_strike)
            elif short_type == "put" and long_strike < short_strike:
                put_widths.append(short_strike - long_strike)
    return call_widths + put_widths


def _leg_entry_price_after_slippage(leg: dict[str, Any], slippage_bps: float) -> float:
    side = int(leg["side"])
    close = float(leg["entry_bar"]["close"])
    multiplier = 1.0 + slippage_bps / 10000.0 if side > 0 else 1.0 - slippage_bps / 10000.0
    return close * multiplier


def _leg_exit_price_after_slippage(leg: dict[str, Any], exit_bar: dict[str, Any], slippage_bps: float) -> float:
    side = int(leg["side"])
    close = float(exit_bar["close"])
    multiplier = 1.0 - slippage_bps / 10000.0 if side > 0 else 1.0 + slippage_bps / 10000.0
    return close * multiplier


def _structure_entry_debit_per_unit(legs: list[dict[str, Any]], slippage_bps: float) -> float:
    total = 0.0
    for leg in legs:
        side = int(leg["side"])
        ratio = int(leg["ratio"])
        total += side * _leg_entry_price_after_slippage(leg, slippage_bps) * ratio * 100.0
    return total


def _structure_exit_value_per_unit(
    legs: list[dict[str, Any]],
    exit_bars: list[dict[str, Any]],
    slippage_bps: float,
) -> float:
    total = 0.0
    for leg, exit_bar in zip(legs, exit_bars, strict=True):
        side = int(leg["side"])
        ratio = int(leg["ratio"])
        total += side * _leg_exit_price_after_slippage(leg, exit_bar, slippage_bps) * ratio * 100.0
    return total


def _invalid_credit_structure(legs: list[dict[str, Any]], entry_debit_per_unit: float) -> bool:
    if entry_debit_per_unit >= 0:
        return False
    spread_widths = _credit_structure_spread_widths(legs)
    if not spread_widths:
        return False
    max_loss_before_credit = max(spread_widths) * 100.0
    credit = abs(entry_debit_per_unit)
    return credit >= max_loss_before_credit


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


def _previous_option_bar(
    *,
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    contract_symbol: str,
    timestamp: pd.Timestamp,
    max_staleness: timedelta,
) -> dict[str, Any] | None:
    earliest = timestamp - max_staleness
    if option_index:
        frame = option_index.bars_by_symbol.get(contract_symbol)
        if frame is None or frame.empty:
            return None
        timestamps = frame["timestamp"]
        position = int(timestamps.searchsorted(timestamp, side="right")) - 1
        if position < 0:
            return None
        row = frame.iloc[position].to_dict()
        if pd.Timestamp(row["timestamp"]) >= earliest:
            return row
        return None

    frame = option_bars[
        (option_bars["symbol"].astype(str) == contract_symbol)
        & (option_bars["timestamp"] >= earliest)
        & (option_bars["timestamp"] <= timestamp)
    ].sort_values("timestamp")
    if frame.empty:
        return None
    return frame.iloc[-1].to_dict()


def _entry_option_bar(
    *,
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    contract_symbol: str,
    timestamp: pd.Timestamp,
    max_lag: timedelta,
    lookup_mode: str,
    max_staleness: timedelta,
) -> dict[str, Any] | None:
    forward_bar = _first_option_bar(
        option_bars=option_bars,
        option_index=option_index,
        contract_symbol=contract_symbol,
        timestamp=timestamp,
        max_lag=max_lag,
    )
    if forward_bar:
        return forward_bar
    if lookup_mode != ENTRY_LOOKUP_AT_OR_AFTER_OR_ASOF:
        return None
    return _previous_option_bar(
        option_bars=option_bars,
        option_index=option_index,
        contract_symbol=contract_symbol,
        timestamp=timestamp,
        max_staleness=max_staleness,
    )


def _nearest_bar_context(
    *,
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    contract_symbol: str | None,
    timestamp: pd.Timestamp,
) -> dict[str, Any]:
    if not contract_symbol:
        return {}
    if option_index:
        frame = option_index.bars_by_symbol.get(contract_symbol)
    else:
        frame = option_bars[
            option_bars["symbol"].astype(str) == str(contract_symbol)
        ].sort_values("timestamp")
    if frame is None or frame.empty:
        return {"nearest_bar_status": "no_bars_for_contract"}
    timestamps = frame["timestamp"]
    next_pos = int(timestamps.searchsorted(timestamp, side="left"))
    previous_pos = next_pos - 1
    context: dict[str, Any] = {"nearest_bar_status": "bars_found"}
    if previous_pos >= 0:
        previous_time = pd.Timestamp(frame.iloc[previous_pos]["timestamp"])
        context["previous_bar_time"] = str(previous_time)
        context["previous_bar_age_minutes"] = round(
            (timestamp - previous_time).total_seconds() / 60.0, 4
        )
    if next_pos < len(frame):
        next_time = pd.Timestamp(frame.iloc[next_pos]["timestamp"])
        context["next_bar_time"] = str(next_time)
        context["next_bar_lag_minutes"] = round((next_time - timestamp).total_seconds() / 60.0, 4)
    return context


def _exit_option_bar(
    *,
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None = None,
    contract_symbol: str,
    timestamp: pd.Timestamp,
    max_lag: timedelta,
    lookup_mode: str = EXIT_LOOKUP_AT_OR_AFTER,
) -> dict[str, Any] | None:
    forward_bar = _first_option_bar(
        option_bars=option_bars,
        option_index=option_index,
        contract_symbol=contract_symbol,
        timestamp=timestamp,
        max_lag=max_lag,
    )
    if forward_bar:
        return forward_bar
    if lookup_mode != EXIT_LOOKUP_AT_OR_AFTER_OR_PRIOR:
        return None

    earliest = timestamp - max_lag
    if option_index:
        frame = option_index.bars_by_symbol.get(contract_symbol)
        if frame is None or frame.empty:
            return None
        timestamps = frame["timestamp"]
        position = int(timestamps.searchsorted(timestamp, side="right")) - 1
        if position < 0:
            return None
        row = frame.iloc[position].to_dict()
        if pd.Timestamp(row["timestamp"]) >= earliest:
            return row
        return None

    frame = option_bars[
        (option_bars["symbol"].astype(str) == contract_symbol)
        & (option_bars["timestamp"] >= earliest)
        & (option_bars["timestamp"] <= timestamp)
    ].sort_values("timestamp")
    if frame.empty:
        return None
    return frame.iloc[-1].to_dict()


def _planned_exit_bars(
    *,
    legs: list[dict[str, Any]],
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    exit_time: pd.Timestamp,
    max_exit_lag: timedelta,
    exit_lookup_mode: str,
) -> tuple[list[dict[str, Any]], str | None]:
    exit_bars: list[dict[str, Any]] = []
    for leg in legs:
        contract_symbol = str(leg["contract"]["symbol"])
        exit_bar = _exit_option_bar(
            option_bars=option_bars,
            option_index=option_index,
            contract_symbol=contract_symbol,
            timestamp=exit_time,
            max_lag=max_exit_lag,
            lookup_mode=exit_lookup_mode,
        )
        if not exit_bar:
            return [], contract_symbol
        exit_bars.append(exit_bar)
    return exit_bars, None


def _candidate_option_exit_times(
    *,
    legs: list[dict[str, Any]],
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[pd.Timestamp]:
    if not legs:
        return []
    primary_symbol = str(legs[0]["contract"]["symbol"])
    if option_index:
        frame = option_index.bars_by_symbol.get(primary_symbol)
    else:
        frame = option_bars[option_bars["symbol"].astype(str) == primary_symbol].sort_values("timestamp")
    if frame is None or frame.empty:
        return []
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    mask = timestamps.ge(start) & timestamps.le(end)
    return [pd.Timestamp(value) for value in timestamps.loc[mask].tolist()]


def _float_parameter(parameters: dict[str, Any], key: str, default: float) -> float:
    value = parameters.get(key)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_option_exit_bars(
    *,
    legs: list[dict[str, Any]],
    option_bars: pd.DataFrame,
    option_index: OptionResearchIndex | None,
    parameters: dict[str, Any],
    entry_debit_per_unit: float,
    risk_per_unit: float,
    entry_time: pd.Timestamp,
    planned_exit_time: pd.Timestamp,
    max_exit_lag: timedelta,
    exit_lookup_mode: str,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], str, str | None]:
    option_exit_mode = str(parameters.get("option_exit_mode") or OPTION_EXIT_STOCK_PROXY).lower()
    if option_exit_mode == OPTION_EXIT_PREMIUM_TARGET_STOP:
        entry_credit = max(-entry_debit_per_unit, 0.0)
        if entry_credit > 0:
            profit_threshold = entry_credit * _float_parameter(
                parameters, "option_profit_target_pct", 0.35
            )
            credit_stop = entry_credit * _float_parameter(
                parameters, "option_stop_loss_credit_multiple", 1.25
            )
            risk_stop = risk_per_unit * _float_parameter(parameters, "option_stop_loss_risk_pct", 0.35)
            loss_threshold = -min(credit_stop, risk_stop)
        else:
            profit_threshold = risk_per_unit * _float_parameter(
                parameters, "option_profit_target_pct", 0.35
            )
            loss_threshold = -risk_per_unit * _float_parameter(
                parameters, "option_stop_loss_pct", 0.25
            )
        min_hold_minutes = _float_parameter(parameters, "min_option_hold_minutes", 1.0)
        scan_start = entry_time + timedelta(minutes=min_hold_minutes)
        for timestamp in _candidate_option_exit_times(
            legs=legs,
            option_bars=option_bars,
            option_index=option_index,
            start=scan_start,
            end=planned_exit_time,
        ):
            exit_bars, missing_symbol = _planned_exit_bars(
                legs=legs,
                option_bars=option_bars,
                option_index=option_index,
                exit_time=timestamp,
                max_exit_lag=timedelta(0),
                exit_lookup_mode=EXIT_LOOKUP_AT_OR_AFTER,
            )
            if missing_symbol:
                continue
            exit_value = _structure_exit_value_per_unit(legs, exit_bars, slippage_bps)
            pnl_per_unit = exit_value - entry_debit_per_unit
            if pnl_per_unit >= profit_threshold:
                return exit_bars, "option_profit_target", None
            if pnl_per_unit <= loss_threshold:
                return exit_bars, "option_stop_loss", None

    exit_bars, missing_symbol = _planned_exit_bars(
        legs=legs,
        option_bars=option_bars,
        option_index=option_index,
        exit_time=planned_exit_time,
        max_exit_lag=max_exit_lag,
        exit_lookup_mode=exit_lookup_mode,
    )
    return exit_bars, OPTION_EXIT_STOCK_PROXY, missing_symbol


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


def _parse_hhmm(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def _local_time_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(OPTION_SESSION_TIMEZONE)


def _filter_stock_trades_for_option_session(
    trades: pd.DataFrame,
    *,
    stock_session_filter: str,
) -> pd.DataFrame:
    raw_count = int(len(trades))
    if trades.empty or stock_session_filter == STOCK_SESSION_FILTER_NONE:
        filtered = trades.copy()
        filtered.attrs["raw_source_stock_trade_count"] = raw_count
        filtered.attrs["source_session_filter"] = stock_session_filter
        filtered.attrs["source_session_dropped_count"] = 0
        return filtered

    if stock_session_filter != STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY:
        raise ValueError(f"Unsupported stock_session_filter={stock_session_filter}")

    entry_local = _local_time_series(trades["entry_time"])
    exit_local = _local_time_series(trades["exit_time"])
    session_start = _parse_hhmm(OPTION_SESSION_START)
    session_end = _parse_hhmm(OPTION_SESSION_END)
    mask = (
        entry_local.dt.date.eq(exit_local.dt.date)
        & entry_local.dt.time.ge(session_start)
        & entry_local.dt.time.le(session_end)
        & exit_local.dt.time.ge(session_start)
        & exit_local.dt.time.le(session_end)
    )
    filtered = trades.loc[mask].copy()
    filtered.attrs["raw_source_stock_trade_count"] = raw_count
    filtered.attrs["source_session_filter"] = stock_session_filter
    filtered.attrs["source_session_dropped_count"] = raw_count - int(len(filtered))
    return filtered


def _stock_trades_for_variant(
    *,
    variant: dict[str, Any],
    stock_bars: pd.DataFrame,
    initial_cash: float,
    allocation_fraction: float,
    stock_session_filter: str,
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
    return _filter_stock_trades_for_option_session(
        result.trades.copy(),
        stock_session_filter=stock_session_filter,
    )


def _stock_trade_cache_key(variant: dict[str, Any], *, stock_session_filter: str) -> str:
    parameters = variant.get("parameters") if isinstance(variant.get("parameters"), dict) else {}
    timing = variant_timing_parameters(parameters)
    source = str(variant.get("source_strategy_id") or variant.get("variant_id") or "").lower()
    direction = "bear" if any(token in source for token in ["put", "short", "bear"]) else "bull"
    payload = {
        "symbol": str(variant.get("symbol") or "").upper(),
        "direction": direction,
        "timing_profile": timing["timing_profile"],
        "hard_exit_minute": int(timing["hard_exit_minute"]),
        "stop_loss_multiple": float(timing["stop_loss_multiple"]),
        "profit_target_multiple": float(timing["profit_target_multiple"]),
        "liquidity_gate": str(timing["liquidity_gate"]),
        "stock_session_filter": stock_session_filter,
    }
    for key in (
        "stock_proxy_mode",
        "min_minutes_since_open",
        "max_minutes_since_open",
        "min_trend_gap_pct",
        "max_trend_gap_pct",
        "min_range_pct",
        "max_range_pct",
        "max_midpoint_distance_pct",
        "range_entry_side",
        "range_edge_pct",
        "entry_signal_mode",
        "signal_delay_bars",
        "cooldown_bars",
        "max_signals_per_day",
        "timeout_only_stock_proxy",
        "fast_window",
        "slow_window",
        "breakout_window",
        "min_volume_ratio",
    ):
        if key in parameters:
            payload[key] = parameters[key]
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _split_trade_date(value: Any) -> str:
    return str(pd.Timestamp(value).date())


def _symbol_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {item.strip().upper() for item in value.split(",") if item.strip()}
    return symbols or None


def _regime_balance_order(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_REGIME_BALANCE_ORDER
    order: list[str] = []
    for item in value.split(","):
        regime = item.strip().lower()
        if regime and regime not in order:
            order.append(regime)
    return tuple(order) or DEFAULT_REGIME_BALANCE_ORDER


def _queue_item_regime(item: dict[str, Any]) -> str:
    for key in ("intended_regime", "regime"):
        explicit = str(item.get(key) or "").strip().lower()
        if explicit in REGIME_TOKENS:
            return explicit
    haystack = " ".join(
        str(item.get(key) or "")
        for key in (
            "candidate_variant_id",
            "source_strategy_id",
            "strategy_id",
            "family",
            "strategy_family",
        )
    ).lower()
    for token in re.split(r"[^a-z]+", haystack):
        if token in REGIME_TOKENS:
            return token
    return "unclassified"


def _regime_balanced_queue_items(
    queue_items: list[dict[str, Any]],
    *,
    regime_balance_order: tuple[str, ...] = DEFAULT_REGIME_BALANCE_ORDER,
) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    seen_order: list[str] = []
    for item in queue_items:
        regime = _queue_item_regime(item)
        if regime not in buckets:
            buckets[regime] = []
            seen_order.append(regime)
        buckets[regime].append(item)
    ordered_regimes: list[str] = []
    for regime in regime_balance_order:
        if regime in buckets and regime not in ordered_regimes:
            ordered_regimes.append(regime)
    for regime in seen_order:
        if regime not in ordered_regimes:
            ordered_regimes.append(regime)

    balanced: list[dict[str, Any]] = []
    while any(buckets[regime] for regime in ordered_regimes):
        for regime in ordered_regimes:
            if buckets[regime]:
                balanced.append(buckets[regime].pop(0))
    return balanced


def _candidate_window(
    filtered_queue_items: list[dict[str, Any]],
    *,
    top_n: int,
    candidate_start_index: int = 1,
    candidate_count: int | None = None,
    candidate_selection_mode: str = CANDIDATE_SELECTION_PRIORITY_ORDER,
    regime_balance_order: tuple[str, ...] = DEFAULT_REGIME_BALANCE_ORDER,
) -> tuple[list[dict[str, Any]], int, int, int]:
    if candidate_selection_mode == CANDIDATE_SELECTION_REGIME_BALANCED:
        scoped_queue_items = _regime_balanced_queue_items(
            filtered_queue_items,
            regime_balance_order=regime_balance_order,
        )
    elif candidate_selection_mode == CANDIDATE_SELECTION_PRIORITY_ORDER:
        scoped_queue_items = filtered_queue_items
    else:
        raise ValueError(f"unsupported candidate_selection_mode={candidate_selection_mode!r}")
    top_queue_items = scoped_queue_items[:top_n] if top_n > 0 else scoped_queue_items
    start_offset = max(int(candidate_start_index or 1), 1) - 1
    if candidate_count is None or int(candidate_count) <= 0:
        candidate_end_index = len(top_queue_items)
        selected_queue_items = top_queue_items[start_offset:]
    else:
        candidate_end_index = min(start_offset + int(candidate_count), len(top_queue_items))
        selected_queue_items = top_queue_items[start_offset:candidate_end_index]
    return selected_queue_items, start_offset, candidate_end_index, len(top_queue_items)


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
    if fill_coverage < STRATEGY_FILL_COVERAGE_GATE:
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
    if fill_coverage >= STRATEGY_FILL_COVERAGE_GATE:
        return "fill_gate_clear"
    missing = {
        "selected_contract_universe_gap": int(summary.get("missing_no_selected_contract") or 0),
        "entry_bar_gap_or_entry_timing_mismatch": int(summary.get("missing_no_entry_bar") or 0),
        "exit_bar_gap_or_exit_policy_mismatch": int(summary.get("missing_no_exit_bar") or 0),
        "greek_snapshot_unavailable": int(summary.get("missing_no_greek_snapshot") or 0),
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
    entry_lookup_mode: str,
    max_entry_staleness: timedelta,
    max_exit_lag: timedelta,
    exit_lookup_mode: str,
    contract_selection_method: str,
    source_trades: pd.DataFrame | None = None,
) -> tuple[list[dict[str, Any]], int, dict[str, int], list[dict[str, Any]]]:
    if source_trades is None:
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
        "unsupported_option_structure": 0,
        "no_greek_snapshot": 0,
    }
    option_type = str(queue_item.get("directional_option_type") or "").lower()
    symbol = str(queue_item.get("symbol") or "").upper()
    identity = _candidate_identity(queue_item, variant)
    parameters = _variant_parameters(queue_item, variant)
    failure_rows: list[dict[str, Any]] = []

    def failure_row(
        *,
        reason: str,
        trade: dict[str, Any],
        contract_symbol: str | None = None,
        lookup_time: pd.Timestamp | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        entry_time = pd.Timestamp(trade["entry_time"])
        exit_time = pd.Timestamp(trade["exit_time"])
        row = {
            "candidate_variant_id": queue_item.get("candidate_variant_id"),
            **identity,
            "source_strategy_id": queue_item.get("source_strategy_id"),
            "symbol": symbol,
            "option_type": option_type,
            "contract_symbol": contract_symbol,
            "failure_reason": reason,
            "trade_date": str(entry_time.date()),
            "stock_entry_time": str(entry_time),
            "stock_exit_time": str(exit_time),
            "lookup_time": str(lookup_time) if lookup_time is not None else "",
            "entry_lookup_mode": entry_lookup_mode,
            "max_entry_lag_minutes": round(max_entry_lag.total_seconds() / 60.0, 4),
            "max_entry_staleness_minutes": round(
                max_entry_staleness.total_seconds() / 60.0, 4
            ),
            "max_exit_lag_minutes": round(max_exit_lag.total_seconds() / 60.0, 4),
            "exit_lookup_mode": exit_lookup_mode,
            "contract_selection_method": contract_selection_method,
        }
        if lookup_time is not None:
            row.update(
                _nearest_bar_context(
                    option_bars=option_bars,
                    option_index=option_index,
                    contract_symbol=contract_symbol,
                    timestamp=lookup_time,
                )
            )
        if extra:
            row.update(extra)
        return row

    for trade in source_trades.to_dict("records"):
        entry_time = pd.Timestamp(trade["entry_time"])
        exit_time = pd.Timestamp(trade["exit_time"])
        trade_date = entry_time.date()
        entry_spot = None
        try:
            entry_spot = float(trade.get("entry_price"))
        except (TypeError, ValueError):
            entry_spot = None
        legs, option_structure, status = _option_structure_legs(
            queue_item=queue_item,
            variant=variant,
            contracts=contracts,
            option_bars=option_bars,
            option_trades=option_trades,
            option_index=option_index,
            symbol=symbol,
            trade_date=trade_date,
            entry_time=entry_time,
            max_entry_lag=max_entry_lag,
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
            contract_selection_method=contract_selection_method,
            entry_spot=entry_spot,
        )
        if status != "selected" or not legs:
            missing_counts[status] = missing_counts.get(status, 0) + 1
            failure_rows.append(
                failure_row(
                    reason=status,
                    trade=trade,
                    lookup_time=entry_time,
                    extra={"option_structure": option_structure},
                )
            )
            continue

        leg_contract_symbols = ";".join(str(leg["contract"]["symbol"]) for leg in legs)
        entry_debit_per_unit = _structure_entry_debit_per_unit(legs, slippage_bps)
        total_contract_units = sum(abs(int(leg["ratio"])) for leg in legs)
        if _invalid_credit_structure(legs, entry_debit_per_unit):
            missing_counts["invalid_credit_structure"] = (
                missing_counts.get("invalid_credit_structure", 0) + 1
            )
            failure_rows.append(
                failure_row(
                    reason="invalid_credit_structure",
                    trade=trade,
                    contract_symbol=leg_contract_symbols,
                    lookup_time=entry_time,
                    extra={
                        "entry_debit_per_unit": round(entry_debit_per_unit, 4),
                        "max_credit_widths": ";".join(
                            str(width) for width in _credit_structure_spread_widths(legs)
                        ),
                        "option_structure": option_structure,
                    },
                )
            )
            continue
        risk_per_unit = _structure_risk_per_unit(legs, entry_debit_per_unit)
        budget = initial_cash * allocation_fraction
        quantity = math.floor(budget / risk_per_unit)
        if quantity < 1:
            missing_counts["too_expensive"] += 1
            failure_rows.append(
                failure_row(
                    reason="too_expensive",
                    trade=trade,
                    contract_symbol=leg_contract_symbols,
                    lookup_time=entry_time,
                    extra={
                        "entry_debit_per_unit": round(entry_debit_per_unit, 4),
                        "risk_per_unit": round(risk_per_unit, 4),
                        "budget": budget,
                        "option_structure": option_structure,
                    },
                )
            )
            continue
        option_entry_time = max(pd.Timestamp(leg["entry_bar"]["timestamp"]) for leg in legs)
        exit_bars, option_exit_reason, missing_exit_symbol = _resolve_option_exit_bars(
            legs=legs,
            option_bars=option_bars,
            option_index=option_index,
            parameters=parameters,
            entry_debit_per_unit=entry_debit_per_unit,
            risk_per_unit=risk_per_unit,
            entry_time=option_entry_time,
            planned_exit_time=exit_time,
            max_exit_lag=max_exit_lag,
            exit_lookup_mode=exit_lookup_mode,
            slippage_bps=slippage_bps,
        )
        if len(exit_bars) != len(legs):
            missing_counts["no_exit_bar"] += 1
            first_entry = legs[0]["entry_bar"] if legs else {}
            failure_rows.append(
                failure_row(
                    reason="no_exit_bar",
                    trade=trade,
                    contract_symbol=missing_exit_symbol,
                    lookup_time=exit_time,
                    extra={
                        "option_entry_time": str(first_entry.get("timestamp", "")),
                        "option_exit_reason": option_exit_reason,
                        "option_structure": option_structure,
                    },
                )
            )
            continue

        leg_details: list[dict[str, Any]] = []
        exit_value_per_unit = _structure_exit_value_per_unit(legs, exit_bars, slippage_bps)
        for leg_item, exit_bar in zip(legs, exit_bars, strict=True):
            side = int(leg_item["side"])
            ratio = int(leg_item["ratio"])
            entry_price = _leg_entry_price_after_slippage(leg_item, slippage_bps)
            exit_price = _leg_exit_price_after_slippage(leg_item, exit_bar, slippage_bps)
            leg_details.append(
                {
                    "role": leg_item["role"],
                    "side": side,
                    "ratio": ratio,
                    "contract_symbol": str(leg_item["contract"]["symbol"]),
                    "strike": _contract_strike(leg_item["contract"]),
                    "entry_price_after_slippage": round(entry_price, 4),
                    "exit_price_after_slippage": round(exit_price, 4),
                    "entry_time": str(leg_item["entry_bar"]["timestamp"]),
                    "exit_time": str(exit_bar["timestamp"]),
                    "relative_strike_step": leg_item["contract"].get("relative_strike_step"),
                    "dte": leg_item["contract"].get("dte"),
                    "entry_delta": leg_item["contract"].get("entry_delta"),
                    "entry_gamma": leg_item["contract"].get("entry_gamma"),
                    "entry_theta": leg_item["contract"].get("entry_theta"),
                    "entry_vega": leg_item["contract"].get("entry_vega"),
                    "entry_implied_vol": leg_item["contract"].get("entry_implied_vol"),
                    "entry_greek_spot": leg_item["contract"].get("entry_greek_spot"),
                }
            )
        fees = fee_per_contract * quantity * 2.0 * total_contract_units
        pnl = (exit_value_per_unit - entry_debit_per_unit) * quantity - fees
        primary_leg = leg_details[0]
        option_rows.append(
            {
                "candidate_variant_id": queue_item.get("candidate_variant_id"),
                **identity,
                "source_strategy_id": queue_item.get("source_strategy_id"),
                "symbol": symbol,
                "option_type": option_type,
                "option_structure": option_structure,
                "option_leg_count": len(legs),
                "contract_symbol": ";".join(item["contract_symbol"] for item in leg_details),
                "leg_details_json": json.dumps(leg_details, sort_keys=True, separators=(",", ":")),
                "trade_date": str(trade_date),
                "stock_entry_time": str(entry_time),
                "stock_exit_time": str(exit_time),
                "option_entry_time": primary_leg["entry_time"],
                "option_exit_time": primary_leg["exit_time"],
                "stock_pnl_proxy": round(float(trade.get("pnl") or 0.0), 4),
                "entry_option_close": round(entry_debit_per_unit / 100.0, 4),
                "exit_option_close": round(exit_value_per_unit / 100.0, 4),
                "entry_price_after_slippage": round(entry_debit_per_unit / 100.0, 4),
                "exit_price_after_slippage": round(exit_value_per_unit / 100.0, 4),
                "risk_per_unit": round(risk_per_unit, 4),
                "entry_debit_per_unit": round(entry_debit_per_unit, 4),
                "exit_value_per_unit": round(exit_value_per_unit, 4),
                "quantity": quantity,
                "fees": round(fees, 4),
                "option_pnl": round(pnl, 4),
                "option_return_pct": round(pnl / (risk_per_unit * quantity), 6),
                "stock_exit_reason": trade.get("exit_reason"),
                "option_exit_reason": option_exit_reason,
                "option_exit_mode": str(parameters.get("option_exit_mode") or OPTION_EXIT_STOCK_PROXY),
                "contract_selection_method": contract_selection_method,
                "contract_dte": ";".join(str(item["dte"]) for item in leg_details),
                "contract_relative_strike_step": ";".join(
                    str(item["relative_strike_step"]) for item in leg_details
                ),
                "entry_delta": primary_leg.get("entry_delta"),
                "entry_gamma": primary_leg.get("entry_gamma"),
                "entry_theta": primary_leg.get("entry_theta"),
                "entry_vega": primary_leg.get("entry_vega"),
                "entry_implied_vol": primary_leg.get("entry_implied_vol"),
                "entry_greek_spot": primary_leg.get("entry_greek_spot"),
                "target_delta": parameters.get("target_delta"),
                "entry_selection_trade_print_count": sum(
                    _trade_print_count(
                        option_trades=option_trades,
                        option_index=option_index,
                        contract_symbol=item["contract_symbol"],
                        start=entry_time,
                        end=entry_time + max_entry_lag,
                    )
                    for item in leg_details
                ),
                "option_trade_print_count": sum(
                    _trade_print_count(
                        option_trades=option_trades,
                        option_index=option_index,
                        contract_symbol=item["contract_symbol"],
                        start=entry_time,
                        end=exit_time,
                    )
                    for item in leg_details
                ),
            }
        )
    return option_rows, int(len(source_trades)), missing_counts, failure_rows


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
    entry_lookup_mode: str = ENTRY_LOOKUP_AT_OR_AFTER,
    exit_lookup_mode: str = EXIT_LOOKUP_AT_OR_AFTER,
    stock_session_filter: str = STOCK_SESSION_FILTER_OPTION_RTH_SAME_DAY,
    max_entry_staleness: timedelta | None = None,
    symbol_filter: set[str] | None = None,
    skip_blocked_queue_items: bool = False,
    test_date_count: int = 1,
    contract_selection_method: str = CONTRACT_SELECTION_NEAREST,
    candidate_start_index: int = 1,
    candidate_count: int | None = None,
    candidate_selection_mode: str = CANDIDATE_SELECTION_PRIORITY_ORDER,
    regime_balance_order: tuple[str, ...] = DEFAULT_REGIME_BALANCE_ORDER,
    runtime_parity_mode: str = RUNTIME_PARITY_NONE,
    progress_dir: Path | None = None,
) -> dict[str, Any]:
    if max_entry_staleness is None:
        max_entry_staleness = timedelta(minutes=5)

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
    print(
        "option_aware_inputs_loaded "
        f"stock_rows={len(stock_bars)} contract_rows={len(contracts)} "
        f"option_bar_rows={len(option_bars)} option_trade_rows={len(option_trades)}",
        flush=True,
    )
    option_index = _build_option_research_index(
        contracts=contracts,
        option_bars=option_bars,
        option_trades=option_trades,
    )
    print(
        "option_aware_index_built "
        f"contract_keys={len(option_index.contracts_by_key)} "
        f"bar_symbols={len(option_index.bars_by_symbol)} "
        f"trade_symbols={len(option_index.trade_timestamps_by_symbol)}",
        flush=True,
    )
    all_trade_rows: list[dict[str, Any]] = []
    all_failure_rows: list[dict[str, Any]] = []
    candidate_summaries: list[dict[str, Any]] = []
    stock_trade_cache: dict[str, pd.DataFrame] = {}
    source_queue_items = [item for item in queue.get("queue_items", []) if isinstance(item, dict)]
    filtered_queue_items = []
    for item in source_queue_items:
        if symbol_filter and str(item.get("symbol") or "").upper() not in symbol_filter:
            continue
        if skip_blocked_queue_items and item.get("blockers"):
            continue
        filtered_queue_items.append(item)
    (
        selected_queue_items,
        start_offset,
        candidate_end_index,
        candidate_scope_count,
    ) = _candidate_window(
        filtered_queue_items,
        top_n=top_n,
        candidate_start_index=candidate_start_index,
        candidate_count=candidate_count,
        candidate_selection_mode=candidate_selection_mode,
        regime_balance_order=regime_balance_order,
    )
    print(
        "option_aware_candidate_loop_start "
        f"selected={len(selected_queue_items)} filtered={len(filtered_queue_items)} "
        f"top_n={top_n} candidate_start_index={start_offset + 1} "
        f"candidate_count={candidate_count or ''} "
        f"candidate_scope_count={candidate_scope_count} "
        f"candidate_selection_mode={candidate_selection_mode} "
        f"regime_balance_order={','.join(regime_balance_order)} "
        f"contract_selection_method={contract_selection_method}",
        flush=True,
    )
    for index, queue_item in enumerate(selected_queue_items, start=1):
        if not isinstance(queue_item, dict):
            continue
        variant_id = str(queue_item.get("candidate_variant_id") or "")
        if index == 1 or index % 5 == 0 or index == len(selected_queue_items):
            absolute_index = start_offset + index
            print(
                "option_aware_candidate_started "
                f"index={absolute_index}/{candidate_scope_count} "
                f"window_index={index}/{len(selected_queue_items)} variant_id={variant_id}",
                flush=True,
            )
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
        cache_key = _stock_trade_cache_key(
            variant, stock_session_filter=stock_session_filter
        )
        source_trades = stock_trade_cache.get(cache_key)
        if source_trades is None:
            source_trades = _stock_trades_for_variant(
                variant=variant,
                stock_bars=stock_bars,
                initial_cash=initial_cash,
                allocation_fraction=allocation_fraction,
                stock_session_filter=stock_session_filter,
            )
            stock_trade_cache[cache_key] = source_trades
        raw_source_trade_count = int(
            source_trades.attrs.get("raw_source_stock_trade_count", len(source_trades))
        )
        source_session_dropped_count = int(
            source_trades.attrs.get("source_session_dropped_count", 0)
        )
        rows, source_trade_count, missing_counts, failure_rows = _option_rows_for_candidate(
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
            entry_lookup_mode=entry_lookup_mode,
            max_entry_staleness=max_entry_staleness,
            max_exit_lag=max_exit_lag,
            exit_lookup_mode=exit_lookup_mode,
            contract_selection_method=contract_selection_method,
            source_trades=source_trades,
        )
        missing_price_count = sum(int(value) for value in missing_counts.values())
        selected_count = max(
            source_trade_count - int(missing_counts.get("no_selected_contract", 0)),
            0,
        )
        entry_fill_count = max(
            selected_count
            - int(missing_counts.get("no_entry_bar", 0))
            - int(missing_counts.get("no_greek_snapshot", 0)),
            0,
        )
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
        all_failure_rows.extend(failure_rows)
        economics = _summarize_trade_rows(rows)
        split = _split_summary(rows, test_date_count=test_date_count)
        parameters = _variant_parameters(queue_item, variant)
        summary = {
            "candidate_variant_id": variant_id,
            "symbol": queue_item.get("symbol"),
            **_candidate_identity(queue_item, variant),
            "source_strategy_id": queue_item.get("source_strategy_id"),
            "directional_option_type": queue_item.get("directional_option_type"),
            "raw_source_stock_trade_count": raw_source_trade_count,
            "source_session_filter": stock_session_filter,
            "source_session_dropped_count": source_session_dropped_count,
            "source_stock_trade_count": source_trade_count,
            "missing_option_price_count": missing_price_count,
            "missing_no_selected_contract": int(missing_counts.get("no_selected_contract", 0)),
            "missing_no_entry_bar": int(missing_counts.get("no_entry_bar", 0)),
            "missing_no_exit_bar": int(missing_counts.get("no_exit_bar", 0)),
            "missing_too_expensive": int(missing_counts.get("too_expensive", 0)),
            "missing_unsupported_option_structure": int(
                missing_counts.get("unsupported_option_structure", 0)
            ),
            "missing_no_greek_snapshot": int(missing_counts.get("no_greek_snapshot", 0)),
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
            "fill_coverage_unit": "filled_option_structures_per_source_stock_trade",
            "strategy_fill_coverage_gate": STRATEGY_FILL_COVERAGE_GATE,
            "fill_coverage_semantics": (
                "Strategy-level fill coverage, not raw option data coverage. "
                "Family-aware structures require every leg to have entry and exit bars."
            ),
            "entry_lookup_mode": entry_lookup_mode,
            "max_entry_lag_minutes": round(max_entry_lag.total_seconds() / 60.0, 4),
            "max_entry_staleness_minutes": round(max_entry_staleness.total_seconds() / 60.0, 4),
            "exit_lookup_mode": exit_lookup_mode,
            "max_exit_lag_minutes": round(max_exit_lag.total_seconds() / 60.0, 4),
            "runtime_parity_mode": runtime_parity_mode,
            **economics,
            **split,
            "promotion_allowed": False,
            "broker_facing": False,
            "live_manifest_effect": "none",
            "risk_policy_effect": "none",
            "contract_selection_method": contract_selection_method,
            "option_exit_mode": str(parameters.get("option_exit_mode") or OPTION_EXIT_STOCK_PROXY),
            "test_date_count": int(test_date_count),
            "contract_selection_lookahead": (
                "entry_window_only"
                if contract_selection_method
                in {CONTRACT_SELECTION_LIQUIDITY_FIRST, CONTRACT_SELECTION_DELTA_TARGET}
                else "none"
            ),
        }
        summary["fill_failure_reason"] = _fill_failure_reason(summary)
        summary["recommendation"] = _recommendation(summary)
        candidate_summaries.append(summary)
        if progress_dir is not None:
            append_jsonl(progress_dir / "candidate_summary_progress.jsonl", summary)
            for failure_row in failure_rows:
                append_jsonl(progress_dir / "fill_failure_progress.jsonl", failure_row)
        if index == 1 or index % 5 == 0 or index == len(selected_queue_items):
            absolute_index = start_offset + index
            print(
                "option_aware_candidate_completed "
                f"index={absolute_index}/{candidate_scope_count} "
                f"window_index={index}/{len(selected_queue_items)} variant_id={variant_id} "
                f"source_trades={source_trade_count} filled={filled_order_count} "
                f"fill_coverage={strategy_fill_coverage}",
                flush=True,
            )
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
        "candidate_scope_count": candidate_scope_count,
        "candidate_start_index": start_offset + 1,
        "candidate_end_index": candidate_end_index,
        "candidate_count_requested": candidate_count,
        "candidate_selection_mode": candidate_selection_mode,
        "regime_balance_order": list(regime_balance_order),
        "stock_trade_cache_entry_count": len(stock_trade_cache),
        "symbol_filter": sorted(symbol_filter) if symbol_filter else [],
        "skip_blocked_queue_items": bool(skip_blocked_queue_items),
        "test_date_count": int(test_date_count),
        "runtime_parity_mode": runtime_parity_mode,
        "stock_session_filter": stock_session_filter,
        "stock_session_filter_window": {
            "timezone": OPTION_SESSION_TIMEZONE,
            "start": OPTION_SESSION_START,
            "end": OPTION_SESSION_END,
        },
        "contract_selection_method": contract_selection_method,
        "option_lookup_mode": "indexed_by_contract_and_symbol",
        "fill_coverage_unit": "filled_option_structures_per_source_stock_trade",
        "strategy_fill_coverage_gate": STRATEGY_FILL_COVERAGE_GATE,
        "fill_coverage_semantics": (
            "fill_coverage is an alias for strategy_fill_coverage. "
            "data_foundation_coverage measures selected-contract availability for source trades; "
            "entry_bar_coverage and exit_bar_coverage isolate timing/execution gaps."
        ),
        "exit_lookup_mode": exit_lookup_mode,
        "option_index_counts": {
            "contract_keys": len(option_index.contracts_by_key),
            "bar_symbols": len(option_index.bars_by_symbol),
            "trade_symbols": len(option_index.trade_timestamps_by_symbol),
        },
        "contract_selection_lookahead": (
            "entry_window_only"
            if contract_selection_method
            in {CONTRACT_SELECTION_LIQUIDITY_FIRST, CONTRACT_SELECTION_DELTA_TARGET}
            else "none"
        ),
        "candidate_count": len(candidate_summaries),
        "option_trade_count": len(all_trade_rows),
        "fill_failure_row_count": len(all_failure_rows),
        "recommendation_counts": dict(sorted(recommendation_counts.items())),
        "fill_failure_counts": dict(sorted(fill_failure_counts.items())),
        "candidate_summaries": ranked,
        "trade_rows": all_trade_rows,
        "fill_failure_rows": all_failure_rows,
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


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=_json_default, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
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
        f"- Candidate selection mode: `{payload.get('candidate_selection_mode')}`",
        f"- Regime balance order: `{','.join(payload.get('regime_balance_order') or [])}`",
        f"- Option trade count: `{payload['option_trade_count']}`",
        f"- Promotion allowed: `{payload['promotion_allowed']}`",
        f"- Broker facing: `{payload['broker_facing']}`",
        f"- Runtime parity mode: `{payload.get('runtime_parity_mode')}`",
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
        "fill_failures": run_dir / "option_aware_fill_failures.csv",
        "fill_failures_json": run_dir / "option_aware_fill_failures.json",
        "candidate_summary": run_dir / "option_aware_candidate_summary.csv",
        "candidate_summary_json": run_dir / "option_aware_candidate_summary.json",
        "recommendation_packet": run_dir / "option_aware_recommendation_packet.json",
        "recommendation_packet_md": run_dir / "option_aware_recommendation_packet.md",
    }
    write_json(
        artifacts["manifest"],
        {
            key: value
            for key, value in payload.items()
            if key not in {"trade_rows", "fill_failure_rows"}
        },
    )
    write_markdown(artifacts["manifest_md"], payload)
    write_csv(artifacts["trade_economics"], payload["trade_rows"])
    write_json(artifacts["trade_economics_json"], payload["trade_rows"])
    write_csv(artifacts["fill_failures"], payload.get("fill_failure_rows", []))
    write_json(artifacts["fill_failures_json"], payload.get("fill_failure_rows", []))
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
    args = apply_runtime_parity_mode(parse_args())
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
        candidate_start_index=args.candidate_start_index,
        candidate_count=args.candidate_count,
        candidate_selection_mode=args.candidate_selection_mode,
        regime_balance_order=_regime_balance_order(args.regime_balance_order),
        symbol_filter=_symbol_filter(args.symbol_filter),
        skip_blocked_queue_items=args.skip_blocked_queue_items,
        initial_cash=args.initial_cash,
        allocation_fraction=args.allocation_fraction,
        slippage_bps=args.slippage_bps,
        fee_per_contract=args.fee_per_contract,
        max_entry_lag=timedelta(minutes=args.max_entry_lag_minutes),
        entry_lookup_mode=args.entry_bar_lookup_mode,
        max_entry_staleness=timedelta(minutes=args.max_entry_staleness_minutes),
        max_exit_lag=timedelta(minutes=args.max_exit_lag_minutes),
        exit_lookup_mode=args.exit_bar_lookup_mode,
        stock_session_filter=args.stock_session_filter,
        test_date_count=args.test_date_count,
        contract_selection_method=args.contract_selection_method,
        runtime_parity_mode=args.runtime_parity_mode,
        progress_dir=Path(args.progress_dir) if args.progress_dir else None,
    )
    payload["run_id"] = run_id
    payload["artifacts"] = write_artifacts(Path(args.output_dir), run_id, payload)
    print(
        json.dumps(
            {
                key: value
                for key, value in payload.items()
                if key not in {"trade_rows", "fill_failure_rows"}
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

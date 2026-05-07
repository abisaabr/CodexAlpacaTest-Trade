from __future__ import annotations

import argparse
import csv
import json
import math
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from multiprocessing import Pool
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.analyze_micro_scalp_shadow import CONTRACT_MULTIPLIER, QuotePoint, _parse_ts, _safe_float


_POOL_OPTION_QUOTES: dict[str, list[QuotePoint]] = {}
_POOL_STOCK_QUOTES: dict[str, list["StockQuotePoint"]] = {}
_POOL_FEE_PER_CONTRACT = 0.65
_POOL_TOP_TRADES = 500


@dataclass(slots=True)
class StockQuotePoint:
    ts_epoch: float
    observed_epoch: float
    bid: float
    ask: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Research-only websocket microstructure event replay shard. "
            "Entries use quote/stock events causally; fills assume buy at ask and sell at bid."
        )
    )
    parser.add_argument("--events-jsonl", required=True)
    parser.add_argument("--grid-jsonl", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", default="")
    parser.add_argument("--worker-id", default="")
    parser.add_argument("--grid-start-index", type=int, default=1)
    parser.add_argument("--grid-count", type=int, default=0)
    parser.add_argument("--underlyings", default="")
    parser.add_argument("--max-contracts", type=int, default=0)
    parser.add_argument(
        "--max-contracts-per-underlying",
        type=int,
        default=0,
        help="Keep the most active N option contracts per underlying before replaying the grid.",
    )
    parser.add_argument("--fee-per-contract", type=float, default=0.65)
    parser.add_argument("--top-trades", type=int, default=500)
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Parallel worker processes for grid specs. Use >1 on GCP VMs.",
    )
    return parser.parse_args()


def _resolve_input_path(path_or_uri: str) -> Path:
    if not path_or_uri.startswith("gs://"):
        return Path(path_or_uri)
    temp_dir = Path(tempfile.mkdtemp(prefix="microstructure_replay_"))
    destination = temp_dir / Path(path_or_uri.rstrip("/")).name
    subprocess.run(["gcloud", "storage", "cp", path_or_uri, str(destination)], check=True)
    return destination


def _underlying_from_option_symbol(symbol: str) -> str:
    match = re.match(r"^([A-Z]+)\d{6}[CP]\d{8}$", symbol.upper())
    return match.group(1) if match else "UNKNOWN"


def _option_right(symbol: str) -> str:
    match = re.match(r"^[A-Z]+\d{6}([CP])\d{8}$", symbol.upper())
    if not match:
        return "unknown"
    return "call" if match.group(1) == "C" else "put"


def _csv_set(value: str) -> set[str]:
    return {item.strip().upper() for item in value.split(",") if item.strip()}


def _mid(bid: float, ask: float) -> float:
    return (bid + ask) / 2.0


def _quote_age(point: QuotePoint | StockQuotePoint) -> float:
    return max(0.0, point.observed_epoch - point.ts_epoch)


def _relative_spread(bid: float, ask: float) -> float:
    mid = _mid(bid, ask)
    return (ask - bid) / mid if mid > 0.0 else math.inf


def _spec_float(spec: dict[str, Any], key: str, default: float) -> float:
    try:
        value = spec.get(key, default)
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _option_entry_allowed(point: QuotePoint, spec: dict[str, Any]) -> bool:
    mid = _mid(point.bid, point.ask)
    if mid < float(spec["min_premium"]) or mid > float(spec["max_premium"]):
        return False
    if point.bid_size < float(spec["min_quote_size"]) or point.ask_size < float(spec["min_quote_size"]):
        return False
    spread = point.ask - point.bid
    if spread > float(spec["max_absolute_spread"]):
        return False
    if _relative_spread(point.bid, point.ask) > float(spec["max_relative_spread"]):
        return False
    if _quote_age(point) > float(spec["max_entry_quote_age_seconds"]):
        return False
    return True


def load_market_events(
    path: Path,
    *,
    underlyings: set[str],
) -> tuple[dict[str, list[QuotePoint]], dict[str, list[StockQuotePoint]], dict[str, Any]]:
    option_quotes: dict[str, list[QuotePoint]] = defaultdict(list)
    stock_quotes: dict[str, list[StockQuotePoint]] = defaultdict(list)
    stats: dict[str, Any] = {
        "event_count": 0,
        "option_quote_count": 0,
        "stock_quote_count": 0,
        "accepted_option_quote_count": 0,
        "accepted_stock_quote_count": 0,
        "rejected_count": 0,
        "filtered_count": 0,
        "underlying_filter": sorted(underlyings),
    }
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            stats["event_count"] += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                stats["rejected_count"] += 1
                continue
            event_type = event.get("event_type")
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            if event_type == "option_quote":
                stats["option_quote_count"] += 1
                symbol = str(payload.get("symbol") or "").upper()
                underlying = _underlying_from_option_symbol(symbol)
                if underlyings and underlying not in underlyings:
                    stats["filtered_count"] += 1
                    continue
                observed_epoch = _parse_ts(event.get("observed_at_utc"))
                quote_epoch = _parse_ts(payload.get("timestamp"))
                bid = _safe_float(payload.get("bid_price"))
                ask = _safe_float(payload.get("ask_price"))
                bid_size = _safe_float(payload.get("bid_size"))
                ask_size = _safe_float(payload.get("ask_size"))
                if not symbol or observed_epoch is None or quote_epoch is None:
                    stats["rejected_count"] += 1
                    continue
                if not (bid > 0.0 and ask > 0.0 and ask >= bid):
                    stats["rejected_count"] += 1
                    continue
                option_quotes[symbol].append(
                    QuotePoint(
                        ts_epoch=quote_epoch,
                        observed_epoch=observed_epoch,
                        bid=bid,
                        ask=ask,
                        bid_size=0.0 if math.isnan(bid_size) else bid_size,
                        ask_size=0.0 if math.isnan(ask_size) else ask_size,
                    )
                )
                stats["accepted_option_quote_count"] += 1
            elif event_type == "stock_quote":
                stats["stock_quote_count"] += 1
                symbol = str(payload.get("symbol") or "").upper()
                if underlyings and symbol not in underlyings:
                    stats["filtered_count"] += 1
                    continue
                observed_epoch = _parse_ts(event.get("observed_at_utc"))
                quote_epoch = _parse_ts(payload.get("timestamp"))
                bid = _safe_float(payload.get("bid_price"))
                ask = _safe_float(payload.get("ask_price"))
                if not symbol or observed_epoch is None or quote_epoch is None:
                    stats["rejected_count"] += 1
                    continue
                if not (bid > 0.0 and ask > 0.0 and ask >= bid):
                    stats["rejected_count"] += 1
                    continue
                stock_quotes[symbol].append(
                    StockQuotePoint(
                        ts_epoch=quote_epoch,
                        observed_epoch=observed_epoch,
                        bid=bid,
                        ask=ask,
                    )
                )
                stats["accepted_stock_quote_count"] += 1
    for points in option_quotes.values():
        points.sort(key=lambda point: (point.ts_epoch, point.observed_epoch))
    for points in stock_quotes.values():
        points.sort(key=lambda point: (point.ts_epoch, point.observed_epoch))
    return dict(option_quotes), dict(stock_quotes), stats


def load_grid(path: Path, *, start_index: int, count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    first = max(1, start_index)
    last = None if count <= 0 else first + count - 1
    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle, start=1):
            if index < first:
                continue
            if last is not None and index > last:
                break
            if line.strip():
                row = json.loads(line)
                if isinstance(row, dict):
                    row.setdefault("grid_row_index", index)
                    rows.append(row)
    return rows


def _point_at_or_before(points: list[StockQuotePoint], ts_epoch: float, start_index: int = 0) -> tuple[int, StockQuotePoint | None]:
    if not points:
        return start_index, None
    index = min(max(start_index, 0), len(points) - 1)
    while index + 1 < len(points) and points[index + 1].ts_epoch <= ts_epoch:
        index += 1
    while index > 0 and points[index].ts_epoch > ts_epoch:
        index -= 1
    if points[index].ts_epoch <= ts_epoch:
        return index, points[index]
    return index, None


def _option_prior_index(points: list[QuotePoint], index: int, lookback_seconds: float, prior_index: int) -> tuple[int, QuotePoint | None]:
    threshold = points[index].ts_epoch - lookback_seconds
    while prior_index + 1 < index and points[prior_index + 1].ts_epoch <= threshold:
        prior_index += 1
    prior = points[prior_index]
    if prior.ts_epoch <= threshold:
        return prior_index, prior
    return prior_index, None


def _option_at_or_after(
    points: list[QuotePoint],
    *,
    start_index: int,
    target_epoch: float,
    max_wait_seconds: float,
) -> tuple[int, QuotePoint | None]:
    index = max(start_index, 0)
    deadline = target_epoch + max(0.0, max_wait_seconds)
    while index < len(points) and points[index].ts_epoch < target_epoch:
        index += 1
    if index >= len(points):
        return index, None
    point = points[index]
    if point.ts_epoch > deadline:
        return index, None
    return index, point


def _stock_impulse(
    stock_points: list[StockQuotePoint],
    *,
    ts_epoch: float,
    lookback_seconds: float,
    option_right: str,
    hint_index: int,
) -> tuple[int, float | None]:
    current_index, current = _point_at_or_before(stock_points, ts_epoch, hint_index)
    _, prior = _point_at_or_before(stock_points, ts_epoch - lookback_seconds, max(0, current_index - 10))
    if current is None or prior is None:
        return current_index, None
    prior_mid = _mid(prior.bid, prior.ask)
    current_mid = _mid(current.bid, current.ask)
    if prior_mid <= 0.0:
        return current_index, None
    pct = (current_mid - prior_mid) / prior_mid
    signed = pct if option_right == "call" else -pct
    return current_index, signed


def _entry_signal(
    *,
    spec: dict[str, Any],
    points: list[QuotePoint],
    index: int,
    prior_option: QuotePoint,
    stock_impulse: float | None,
) -> tuple[bool, dict[str, Any]]:
    current = points[index]
    prior_mid = _mid(prior_option.bid, prior_option.ask)
    current_mid = _mid(current.bid, current.ask)
    option_momentum = (current_mid - prior_mid) / prior_mid if prior_mid > 0.0 else 0.0
    prior_rel_spread = _relative_spread(prior_option.bid, prior_option.ask)
    current_rel_spread = _relative_spread(current.bid, current.ask)
    signal_mode = str(spec["signal_mode"])
    option_ok = option_momentum >= float(spec["option_momentum_threshold_pct"])
    stock_ok = stock_impulse is not None and stock_impulse >= float(spec["stock_impulse_threshold_pct"])
    spread_ok = current_rel_spread <= prior_rel_spread * float(spec["spread_compression_factor"])
    if signal_mode == "option_momentum":
        ok = option_ok
    elif signal_mode == "stock_impulse_option_confirm":
        ok = stock_ok and option_ok
    elif signal_mode == "stock_impulse_only":
        ok = stock_ok
    elif signal_mode == "spread_compression_momentum":
        ok = option_ok and spread_ok
    else:
        ok = False
    return ok, {
        "option_momentum": option_momentum,
        "stock_impulse": stock_impulse,
        "prior_relative_spread": prior_rel_spread,
        "entry_relative_spread": current_rel_spread,
    }


def simulate_contract(
    *,
    symbol: str,
    points: list[QuotePoint],
    stock_points: list[StockQuotePoint],
    spec: dict[str, Any],
    fee_per_contract: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    option_right = _option_right(symbol)
    requested_right = str(spec.get("option_right") or "both").lower()
    if requested_right not in {"both", option_right}:
        return [], {"signal_count": 0, "fill_failure_count": 0}
    prior_option_index = 0
    stock_hint_index = 0
    index = 1
    signal_count = 0
    fill_failure_count = 0
    fill_failure_reasons: dict[str, int] = {}
    stale_exit_quote_count = 0
    latency_entry_skip_count = 0

    def record_fill_failure(reason: str) -> None:
        nonlocal fill_failure_count
        fill_failure_count += 1
        fill_failure_reasons[reason] = fill_failure_reasons.get(reason, 0) + 1

    while index < len(points):
        point = points[index]
        if not _option_entry_allowed(point, spec):
            index += 1
            continue
        prior_option_index, prior_option = _option_prior_index(
            points,
            index,
            float(spec["lookback_seconds"]),
            prior_option_index,
        )
        if prior_option is None:
            index += 1
            continue
        stock_hint_index, stock_move = _stock_impulse(
            stock_points,
            ts_epoch=point.ts_epoch,
            lookback_seconds=float(spec["lookback_seconds"]),
            option_right=option_right,
            hint_index=stock_hint_index,
        )
        signal_ok, signal_features = _entry_signal(
            spec=spec,
            points=points,
            index=index,
            prior_option=prior_option,
            stock_impulse=stock_move,
        )
        if not signal_ok:
            index += 1
            continue

        signal_count += 1
        signal_point = point
        entry_latency = _spec_float(spec, "entry_latency_seconds", 0.0)
        entry_fill_wait = _spec_float(spec, "entry_fill_wait_seconds", max(0.0, entry_latency))
        entry_index, entry = _option_at_or_after(
            points,
            start_index=index,
            target_epoch=signal_point.ts_epoch + entry_latency,
            max_wait_seconds=entry_fill_wait,
        )
        if entry is None:
            record_fill_failure("entry_latency_no_quote")
            latency_entry_skip_count += 1
            index += 1
            continue
        if not _option_entry_allowed(entry, spec):
            record_fill_failure("entry_latency_quote_gate_failed")
            latency_entry_skip_count += 1
            index = max(entry_index + 1, index + 1)
            continue
        max_entry_chase_pct = _spec_float(spec, "max_entry_chase_pct", math.inf)
        entry_chase_pct = (entry.ask - signal_point.ask) / signal_point.ask if signal_point.ask > 0.0 else math.inf
        if entry_chase_pct > max_entry_chase_pct:
            record_fill_failure("entry_chase_above_gate")
            latency_entry_skip_count += 1
            index = max(entry_index + 1, index + 1)
            continue
        estimated_round_trip_spread = (entry.ask - entry.bid) * 2.0
        target_profit_per_share = entry.ask * float(spec["target_pct"])
        spread_cost_to_target = (
            estimated_round_trip_spread / target_profit_per_share
            if target_profit_per_share > 0.0
            else math.inf
        )
        max_spread_cost_to_target = _spec_float(spec, "max_spread_cost_to_target", math.inf)
        if spread_cost_to_target > max_spread_cost_to_target:
            record_fill_failure("spread_cost_above_target_gate")
            index = max(entry_index + 1, index + 1)
            continue
        deadline = entry.ts_epoch + float(spec["max_hold_seconds"])
        target_bid = entry.ask * (1.0 + float(spec["target_pct"]))
        stop_bid = entry.ask * (1.0 - float(spec["stop_pct"]))
        trail_activation_bid = entry.ask * (1.0 + float(spec["trail_activation_pct"]))
        exit_point: QuotePoint | None = None
        exit_reason = "no_exit_quote"
        exit_index = index
        max_bid = entry.bid
        min_bid = entry.bid
        valid_exit_quote_count = 0
        quote_count_during_hold = 0
        exit_latency = _spec_float(spec, "exit_latency_seconds", 0.0)
        exit_fill_wait = _spec_float(spec, "exit_fill_wait_seconds", max(0.0, exit_latency))
        exit_fill_failure_recorded = False
        for exit_index in range(entry_index + 1, len(points)):
            candidate = points[exit_index]
            if candidate.ts_epoch > deadline:
                break
            quote_count_during_hold += 1
            if _quote_age(candidate) > float(spec["max_exit_quote_age_seconds"]):
                stale_exit_quote_count += 1
                continue
            valid_exit_quote_count += 1
            max_bid = max(max_bid, candidate.bid)
            min_bid = min(min_bid, candidate.bid)
            exit_point = candidate
            triggered_exit_reason: str | None = None
            if candidate.bid >= target_bid:
                triggered_exit_reason = "target"
            if candidate.bid <= stop_bid:
                triggered_exit_reason = triggered_exit_reason or "stop"
            if (
                float(spec["trail_activation_pct"]) > 0.0
                and max_bid >= trail_activation_bid
                and candidate.bid <= max_bid * (1.0 - float(spec["trail_retrace_pct"]))
            ):
                triggered_exit_reason = triggered_exit_reason or "trailing_exit"
            if _relative_spread(candidate.bid, candidate.ask) > float(spec["max_exit_relative_spread"]):
                triggered_exit_reason = triggered_exit_reason or "spread_widen_exit"
            if triggered_exit_reason is not None:
                if exit_latency > 0.0:
                    delayed_exit_index, delayed_exit = _option_at_or_after(
                        points,
                        start_index=exit_index,
                        target_epoch=candidate.ts_epoch + exit_latency,
                        max_wait_seconds=exit_fill_wait,
                    )
                    if delayed_exit is None:
                        record_fill_failure("exit_latency_no_quote")
                        exit_fill_failure_recorded = True
                        exit_point = None
                        exit_index = delayed_exit_index
                        break
                    exit_point = delayed_exit
                    exit_index = delayed_exit_index
                    if _quote_age(exit_point) > float(spec["max_exit_quote_age_seconds"]):
                        record_fill_failure("exit_latency_quote_stale")
                        exit_fill_failure_recorded = True
                        exit_point = None
                        break
                exit_reason = triggered_exit_reason
                break
        if exit_point is None:
            if not exit_fill_failure_recorded:
                record_fill_failure("no_exit_quote")
            index += 1
            continue
        if exit_reason == "no_exit_quote":
            exit_reason = "time_exit"
        gross_pnl = (exit_point.bid - entry.ask) * CONTRACT_MULTIPLIER
        net_pnl = gross_pnl - fee_per_contract * 2.0
        trades.append(
            {
                "grid_id": spec["grid_id"],
                "grid_row_index": spec.get("grid_row_index"),
                "signal_mode": spec["signal_mode"],
                "symbol": symbol,
                "underlying_symbol": _underlying_from_option_symbol(symbol),
                "option_right": option_right,
                "signal_time_utc": datetime.fromtimestamp(signal_point.ts_epoch, tz=UTC).isoformat(),
                "entry_time_utc": datetime.fromtimestamp(entry.ts_epoch, tz=UTC).isoformat(),
                "exit_time_utc": datetime.fromtimestamp(exit_point.ts_epoch, tz=UTC).isoformat(),
                "entry_latency_seconds": round(max(0.0, entry.ts_epoch - signal_point.ts_epoch), 6),
                "hold_seconds": round(max(0.0, exit_point.ts_epoch - entry.ts_epoch), 6),
                "signal_bid": signal_point.bid,
                "signal_ask": signal_point.ask,
                "entry_bid": entry.bid,
                "entry_ask": entry.ask,
                "entry_chase_pct": round(entry_chase_pct, 6),
                "entry_quote_age_seconds": round(_quote_age(entry), 6),
                "entry_absolute_spread": round(entry.ask - entry.bid, 6),
                "entry_relative_spread": round(_relative_spread(entry.bid, entry.ask), 6),
                "estimated_round_trip_spread": round(estimated_round_trip_spread, 6),
                "spread_cost_to_target": round(spread_cost_to_target, 6),
                "option_momentum": round(float(signal_features["option_momentum"]), 6),
                "stock_impulse": None
                if signal_features["stock_impulse"] is None
                else round(float(signal_features["stock_impulse"]), 6),
                "exit_bid": exit_point.bid,
                "exit_ask": exit_point.ask,
                "exit_quote_age_seconds": round(_quote_age(exit_point), 6),
                "exit_relative_spread": round(_relative_spread(exit_point.bid, exit_point.ask), 6),
                "exit_reason": exit_reason,
                "max_bid_within_hold": max_bid,
                "min_bid_within_hold": min_bid,
                "gross_pnl_per_contract": round(gross_pnl, 4),
                "net_pnl_per_contract": round(net_pnl, 4),
                "quote_count_during_hold": quote_count_during_hold,
                "valid_exit_quote_count": valid_exit_quote_count,
            }
        )
        index = max(exit_index + 1, index + 1)
        if float(spec.get("cooldown_seconds") or 0.0) > 0.0:
            cooldown_until = exit_point.ts_epoch + float(spec["cooldown_seconds"])
            while index < len(points) and points[index].ts_epoch < cooldown_until:
                index += 1
    return trades, {
        "signal_count": signal_count,
        "fill_failure_count": fill_failure_count,
        "latency_entry_skip_count": latency_entry_skip_count,
        "stale_exit_quote_count": stale_exit_quote_count,
        "fill_failure_reasons": fill_failure_reasons,
    }


def summarize_spec(spec: dict[str, Any], trades: list[dict[str, Any]], counters: dict[str, Any]) -> dict[str, Any]:
    signal_count = int(counters.get("signal_count") or 0)
    fill_failures = int(counters.get("fill_failure_count") or 0)
    filled = len(trades)
    winners = [trade for trade in trades if float(trade["net_pnl_per_contract"]) > 0.0]
    total_net = sum(float(trade["net_pnl_per_contract"]) for trade in trades)
    exit_counts: dict[str, int] = {}
    for trade in trades:
        exit_counts[str(trade["exit_reason"])] = exit_counts.get(str(trade["exit_reason"]), 0) + 1
    avg_entry_age = (
        sum(float(trade["entry_quote_age_seconds"]) for trade in trades) / filled if filled else 0.0
    )
    avg_entry_spread = (
        sum(float(trade["entry_relative_spread"]) for trade in trades) / filled if filled else 0.0
    )
    avg_entry_chase = (
        sum(float(trade.get("entry_chase_pct") or 0.0) for trade in trades) / filled if filled else 0.0
    )
    avg_spread_cost_to_target = (
        sum(float(trade.get("spread_cost_to_target") or 0.0) for trade in trades) / filled if filled else 0.0
    )
    fill_coverage = filled / signal_count if signal_count else 0.0
    max_review_avg_spread_cost_to_target = _spec_float(
        spec, "max_review_avg_spread_cost_to_target", math.inf
    )
    review_like = (
        fill_coverage >= 0.90
        and filled >= 20
        and total_net > 0.0
        and (total_net / filled if filled else 0.0) > 0.0
        and avg_spread_cost_to_target <= max_review_avg_spread_cost_to_target
    )
    return {
        "grid_id": spec["grid_id"],
        "grid_row_index": spec.get("grid_row_index"),
        "signal_mode": spec["signal_mode"],
        "lookback_seconds": spec["lookback_seconds"],
        "option_momentum_threshold_pct": spec["option_momentum_threshold_pct"],
        "stock_impulse_threshold_pct": spec["stock_impulse_threshold_pct"],
        "target_pct": spec["target_pct"],
        "stop_pct": spec["stop_pct"],
        "max_hold_seconds": spec["max_hold_seconds"],
        "execution_profile": spec.get("execution_profile", ""),
        "entry_latency_seconds": spec.get("entry_latency_seconds", 0.0),
        "exit_latency_seconds": spec.get("exit_latency_seconds", 0.0),
        "max_entry_chase_pct": spec.get("max_entry_chase_pct", ""),
        "max_spread_cost_to_target": spec.get("max_spread_cost_to_target", ""),
        "max_review_avg_spread_cost_to_target": spec.get("max_review_avg_spread_cost_to_target", ""),
        "max_entry_quote_age_seconds": spec["max_entry_quote_age_seconds"],
        "max_relative_spread": spec["max_relative_spread"],
        "trail_activation_pct": spec["trail_activation_pct"],
        "trail_retrace_pct": spec["trail_retrace_pct"],
        "signal_count": signal_count,
        "filled_trade_count": filled,
        "fill_failure_count": fill_failures,
        "fill_coverage": round(fill_coverage, 6),
        "winning_trade_count": len(winners),
        "win_rate": round(len(winners) / filled, 6) if filled else 0.0,
        "net_pnl_total": round(total_net, 4),
        "avg_net_pnl": round(total_net / filled, 4) if filled else 0.0,
        "best_net_pnl": round(max((float(trade["net_pnl_per_contract"]) for trade in trades), default=0.0), 4),
        "worst_net_pnl": round(min((float(trade["net_pnl_per_contract"]) for trade in trades), default=0.0), 4),
        "avg_entry_quote_age_seconds": round(avg_entry_age, 6),
        "avg_entry_relative_spread": round(avg_entry_spread, 6),
        "avg_entry_chase_pct": round(avg_entry_chase, 6),
        "avg_spread_cost_to_target": round(avg_spread_cost_to_target, 6),
        "latency_entry_skip_count": int(counters.get("latency_entry_skip_count") or 0),
        "stale_exit_quote_count": int(counters.get("stale_exit_quote_count") or 0),
        "fill_failure_reason_counts": counters.get("fill_failure_reasons") or {},
        "exit_reason_counts": exit_counts,
        "review_like": review_like,
    }


def simulate_spec(
    spec: dict[str, Any],
    *,
    option_quotes: dict[str, list[QuotePoint]],
    stock_quotes: dict[str, list[StockQuotePoint]],
    fee_per_contract: float,
    top_trades_limit: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    spec_trades: list[dict[str, Any]] = []
    counters = {
        "signal_count": 0,
        "fill_failure_count": 0,
        "stale_exit_quote_count": 0,
        "latency_entry_skip_count": 0,
        "fill_failure_reasons": {},
    }
    for symbol, points in option_quotes.items():
        underlying = _underlying_from_option_symbol(symbol)
        trades, contract_counters = simulate_contract(
            symbol=symbol,
            points=points,
            stock_points=stock_quotes.get(underlying, []),
            spec=spec,
            fee_per_contract=fee_per_contract,
        )
        spec_trades.extend(trades)
        for key, value in contract_counters.items():
            if key == "fill_failure_reasons" and isinstance(value, dict):
                target = counters.setdefault("fill_failure_reasons", {})
                if isinstance(target, dict):
                    for reason, count in value.items():
                        target[str(reason)] = int(target.get(str(reason), 0)) + int(count or 0)
            else:
                counters[key] = int(counters.get(key) or 0) + int(value or 0)
    spec_trades.sort(key=lambda row: float(row["net_pnl_per_contract"]), reverse=True)
    return summarize_spec(spec, spec_trades, counters), spec_trades[:top_trades_limit]


def _init_pool(
    option_quotes: dict[str, list[QuotePoint]],
    stock_quotes: dict[str, list[StockQuotePoint]],
    fee_per_contract: float,
    top_trades_limit: int,
) -> None:
    global _POOL_OPTION_QUOTES, _POOL_STOCK_QUOTES, _POOL_FEE_PER_CONTRACT, _POOL_TOP_TRADES
    _POOL_OPTION_QUOTES = option_quotes
    _POOL_STOCK_QUOTES = stock_quotes
    _POOL_FEE_PER_CONTRACT = fee_per_contract
    _POOL_TOP_TRADES = top_trades_limit


def _simulate_spec_from_pool(spec: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    return simulate_spec(
        spec,
        option_quotes=_POOL_OPTION_QUOTES,
        stock_quotes=_POOL_STOCK_QUOTES,
        fee_per_contract=_POOL_FEE_PER_CONTRACT,
        top_trades_limit=_POOL_TOP_TRADES,
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _limit_contracts_per_underlying(
    option_quotes: dict[str, list[QuotePoint]],
    *,
    limit: int,
) -> dict[str, list[QuotePoint]]:
    if limit <= 0:
        return option_quotes
    grouped: dict[str, list[tuple[str, list[QuotePoint]]]] = defaultdict(list)
    for symbol, points in option_quotes.items():
        grouped[_underlying_from_option_symbol(symbol)].append((symbol, points))
    limited: dict[str, list[QuotePoint]] = {}
    for contracts in grouped.values():
        for symbol, points in sorted(contracts, key=lambda item: len(item[1]), reverse=True)[:limit]:
            limited[symbol] = points
    return limited


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    events_path = _resolve_input_path(args.events_jsonl)
    grid_path = _resolve_input_path(args.grid_jsonl)
    grid = load_grid(grid_path, start_index=args.grid_start_index, count=args.grid_count)
    underlyings = _csv_set(args.underlyings)
    if not underlyings:
        for row in grid:
            values = row.get("underlyings") if isinstance(row.get("underlyings"), list) else []
            underlyings.update(str(item).upper() for item in values)
    option_quotes, stock_quotes, ingest_stats = load_market_events(events_path, underlyings=underlyings)
    if args.max_contracts_per_underlying > 0:
        option_quotes = _limit_contracts_per_underlying(
            option_quotes,
            limit=args.max_contracts_per_underlying,
        )
    if args.max_contracts > 0:
        option_quotes = dict(
            sorted(option_quotes.items(), key=lambda item: len(item[1]), reverse=True)[: args.max_contracts]
        )

    summary_rows: list[dict[str, Any]] = []
    top_trades: list[dict[str, Any]] = []
    processes = max(int(args.processes), 1)
    if processes > 1 and len(grid) > 1:
        with Pool(
            processes=processes,
            initializer=_init_pool,
            initargs=(option_quotes, stock_quotes, args.fee_per_contract, args.top_trades),
        ) as pool:
            for summary, spec_top_trades in pool.imap_unordered(_simulate_spec_from_pool, grid):
                summary_rows.append(summary)
                top_trades.extend(spec_top_trades)
                top_trades.sort(key=lambda row: float(row["net_pnl_per_contract"]), reverse=True)
                del top_trades[args.top_trades :]
    else:
        for spec in grid:
            summary, spec_top_trades = simulate_spec(
                spec,
                option_quotes=option_quotes,
                stock_quotes=stock_quotes,
                fee_per_contract=args.fee_per_contract,
                top_trades_limit=args.top_trades,
            )
            summary_rows.append(summary)
            top_trades.extend(spec_top_trades)
            top_trades.sort(key=lambda row: float(row["net_pnl_per_contract"]), reverse=True)
            del top_trades[args.top_trades :]

    summary_rows.sort(
        key=lambda row: (
            not bool(row["review_like"]),
            -float(row["net_pnl_total"]),
            -float(row["avg_net_pnl"]),
        )
    )
    packet = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "wave_id": args.wave_id,
        "worker_id": args.worker_id,
        "research_only": True,
        "broker_facing": False,
        "paper_orders": False,
        "input_events_jsonl": args.events_jsonl,
        "input_grid_jsonl": args.grid_jsonl,
        "grid_start_index": args.grid_start_index,
        "grid_count": len(grid),
        "underlyings": sorted(underlyings),
        "contract_count": len(option_quotes),
        "max_contracts_per_underlying": args.max_contracts_per_underlying,
        "stock_quote_symbols": sorted(stock_quotes),
        "execution_assumption": "causal_signal_buy_ask_exit_bid_with_quote_age_spread_gates",
        "processes": processes,
        "ingest_stats": ingest_stats,
        "review_like_count": sum(1 for row in summary_rows if row["review_like"]),
        "top_grids": summary_rows[:50],
        "top_trades": top_trades[: args.top_trades],
        "decision": "research_only_microstructure_shard",
        "promotion_status": "not_promotion_eligible_without_longer_tick_quote_replay_and_governed_packet",
    }
    (output_dir / "microstructure_event_replay_packet.json").write_text(
        json.dumps(packet, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_csv(output_dir / "microstructure_event_replay_summary.csv", summary_rows)
    _write_csv(output_dir / "microstructure_event_replay_top_trades.csv", top_trades)
    with (output_dir / "microstructure_event_replay_summary.jsonl").open("w", encoding="utf-8") as handle:
        for row in summary_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    md_lines = [
        "# Microstructure Event Replay Shard",
        "",
        f"Generated: `{packet['generated_at_utc']}`",
        "",
        "Research-only websocket quote replay. Entries are causal; fills use buy-at-ask and sell-at-bid.",
        "",
        f"- Wave ID: `{args.wave_id}`",
        f"- Worker ID: `{args.worker_id}`",
        f"- Grid count: `{len(grid)}`",
        f"- Contracts analyzed: `{len(option_quotes)}`",
        f"- Review-like grids: `{packet['review_like_count']}`",
        "",
        "## Top Grids",
        "",
    ]
    for row in summary_rows[:10]:
        md_lines.append(
            f"- `{row['grid_id']}` mode={row['signal_mode']} trades={row['filled_trade_count']} "
            f"fill={row['fill_coverage']} net={row['net_pnl_total']} avg={row['avg_net_pnl']} "
            f"win={row['win_rate']}"
        )
    md_lines.extend(
        [
            "",
            "## Promotion Posture",
            "",
            "This is a microstructure scouting shard only. Promotion requires longer tick/quote history, "
            "portfolio-context aggregation, and a governed promotion-review packet.",
            "",
        ]
    )
    (output_dir / "microstructure_event_replay_packet.md").write_text("\n".join(md_lines), encoding="utf-8")
    print(f"packet_json={output_dir / 'microstructure_event_replay_packet.json'}")
    print(f"summary_csv={output_dir / 'microstructure_event_replay_summary.csv'}")
    print(f"top_trades_csv={output_dir / 'microstructure_event_replay_top_trades.csv'}")


if __name__ == "__main__":
    main()

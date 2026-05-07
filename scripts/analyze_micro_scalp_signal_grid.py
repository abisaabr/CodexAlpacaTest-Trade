from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.analyze_micro_scalp_shadow import (
    CONTRACT_MULTIPLIER,
    QuotePoint,
    _underlying_from_option_symbol,
    load_option_quotes,
)


@dataclass(slots=True)
class GridSpec:
    lookback_seconds: float
    momentum_threshold_pct: float
    target_pct: float
    stop_pct: float
    max_hold_seconds: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Research-only causal micro-scalp signal grid. Entries use only prior option quote momentum; "
            "fills assume buy at ask and sell at bid."
        )
    )
    parser.add_argument("--events-jsonl", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--lookbacks", default="1,3,5,10")
    parser.add_argument("--momentum-thresholds", default="0.0025,0.005,0.01,0.015")
    parser.add_argument("--targets", default="0.01,0.015,0.02")
    parser.add_argument("--stops", default="0.006,0.01")
    parser.add_argument("--max-holds", default="15,30,60")
    parser.add_argument("--min-premium", type=float, default=0.15)
    parser.add_argument("--max-premium", type=float, default=12.0)
    parser.add_argument("--max-relative-spread", type=float, default=0.04)
    parser.add_argument("--max-absolute-spread", type=float, default=0.20)
    parser.add_argument("--min-quote-size", type=float, default=1.0)
    parser.add_argument("--fee-per-contract", type=float, default=0.65)
    parser.add_argument("--max-contracts", type=int, default=0)
    return parser.parse_args()


def _float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def _specs(args: argparse.Namespace) -> list[GridSpec]:
    specs: list[GridSpec] = []
    for lookback in _float_list(args.lookbacks):
        for threshold in _float_list(args.momentum_thresholds):
            for target in _float_list(args.targets):
                for stop in _float_list(args.stops):
                    for hold in _float_list(args.max_holds):
                        specs.append(
                            GridSpec(
                                lookback_seconds=lookback,
                                momentum_threshold_pct=threshold,
                                target_pct=target,
                                stop_pct=stop,
                                max_hold_seconds=hold,
                            )
                        )
    return specs


def _entry_allowed(
    point: QuotePoint,
    *,
    min_premium: float,
    max_premium: float,
    max_relative_spread: float,
    max_absolute_spread: float,
    min_quote_size: float,
) -> bool:
    spread = point.ask - point.bid
    mid = (point.ask + point.bid) / 2.0
    relative_spread = spread / mid if mid > 0.0 else math.inf
    return bool(
        min_premium <= point.ask <= max_premium
        and spread <= max_absolute_spread
        and relative_spread <= max_relative_spread
        and point.bid_size >= min_quote_size
        and point.ask_size >= min_quote_size
    )


def simulate_contract(
    *,
    symbol: str,
    points: list[QuotePoint],
    spec: GridSpec,
    min_premium: float,
    max_premium: float,
    max_relative_spread: float,
    max_absolute_spread: float,
    min_quote_size: float,
    fee_per_contract: float,
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    if len(points) < 3:
        return trades
    prior_index = 0
    index = 1
    while index < len(points):
        point = points[index]
        while prior_index + 1 < index and points[prior_index + 1].ts_epoch <= point.ts_epoch - spec.lookback_seconds:
            prior_index += 1
        prior = points[prior_index]
        if prior.ts_epoch > point.ts_epoch - spec.lookback_seconds:
            index += 1
            continue
        if not _entry_allowed(
            point,
            min_premium=min_premium,
            max_premium=max_premium,
            max_relative_spread=max_relative_spread,
            max_absolute_spread=max_absolute_spread,
            min_quote_size=min_quote_size,
        ):
            index += 1
            continue
        prior_mid = (prior.bid + prior.ask) / 2.0
        current_mid = (point.bid + point.ask) / 2.0
        if prior_mid <= 0.0:
            index += 1
            continue
        momentum = (current_mid - prior_mid) / prior_mid
        if momentum < spec.momentum_threshold_pct:
            index += 1
            continue

        entry = point
        target_bid = entry.ask * (1.0 + spec.target_pct)
        stop_bid = entry.ask * (1.0 - spec.stop_pct)
        deadline = entry.ts_epoch + spec.max_hold_seconds
        exit_point = entry
        exit_reason = "time_exit"
        exit_index = index
        max_bid = entry.bid
        min_bid = entry.bid
        quote_count = 0
        for j in range(index + 1, len(points)):
            candidate = points[j]
            if candidate.ts_epoch > deadline:
                break
            quote_count += 1
            exit_index = j
            exit_point = candidate
            max_bid = max(max_bid, candidate.bid)
            min_bid = min(min_bid, candidate.bid)
            if candidate.bid >= target_bid:
                exit_reason = "target"
                break
            if candidate.bid <= stop_bid:
                exit_reason = "stop"
                break
        if quote_count == 0:
            index += 1
            continue
        gross_pnl = (exit_point.bid - entry.ask) * CONTRACT_MULTIPLIER
        net_pnl = gross_pnl - (fee_per_contract * 2.0)
        spread = entry.ask - entry.bid
        mid = (entry.ask + entry.bid) / 2.0
        trades.append(
            {
                "symbol": symbol,
                "underlying_symbol": _underlying_from_option_symbol(symbol),
                "lookback_seconds": spec.lookback_seconds,
                "momentum_threshold_pct": spec.momentum_threshold_pct,
                "target_pct": spec.target_pct,
                "stop_pct": spec.stop_pct,
                "max_hold_seconds": spec.max_hold_seconds,
                "entry_time_utc": datetime.fromtimestamp(entry.ts_epoch, tz=timezone.utc).isoformat(),
                "exit_time_utc": datetime.fromtimestamp(exit_point.ts_epoch, tz=timezone.utc).isoformat(),
                "hold_seconds": round(max(0.0, exit_point.ts_epoch - entry.ts_epoch), 6),
                "entry_ask": entry.ask,
                "entry_bid": entry.bid,
                "entry_relative_spread": round(spread / mid, 6) if mid > 0.0 else None,
                "momentum": round(momentum, 6),
                "exit_bid": exit_point.bid,
                "exit_reason": exit_reason,
                "max_bid_within_hold": max_bid,
                "min_bid_within_hold": min_bid,
                "gross_pnl_per_contract": round(gross_pnl, 4),
                "net_pnl_per_contract": round(net_pnl, 4),
                "quote_count_during_hold": quote_count,
            }
        )
        index = max(exit_index + 1, index + 1)
    return trades


def summarize_trades(spec: GridSpec, trades: list[dict[str, Any]]) -> dict[str, Any]:
    winners = [trade for trade in trades if float(trade["net_pnl_per_contract"]) > 0.0]
    target_winners = [
        trade
        for trade in trades
        if trade["exit_reason"] == "target" and float(trade["net_pnl_per_contract"]) > 0.0
    ]
    total_net = sum(float(trade["net_pnl_per_contract"]) for trade in trades)
    return {
        "lookback_seconds": spec.lookback_seconds,
        "momentum_threshold_pct": spec.momentum_threshold_pct,
        "target_pct": spec.target_pct,
        "stop_pct": spec.stop_pct,
        "max_hold_seconds": spec.max_hold_seconds,
        "trade_count": len(trades),
        "winning_trade_count": len(winners),
        "target_win_count": len(target_winners),
        "win_rate": round(len(winners) / len(trades), 6) if trades else 0.0,
        "target_win_rate": round(len(target_winners) / len(trades), 6) if trades else 0.0,
        "net_pnl_total": round(total_net, 4),
        "avg_net_pnl": round(total_net / len(trades), 4) if trades else 0.0,
        "best_net_pnl": round(max((float(trade["net_pnl_per_contract"]) for trade in trades), default=0.0), 4),
        "worst_net_pnl": round(min((float(trade["net_pnl_per_contract"]) for trade in trades), default=0.0), 4),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    quotes, ingest_stats = load_option_quotes(Path(args.events_jsonl))
    if args.max_contracts > 0:
        ranked = sorted(quotes.items(), key=lambda item: len(item[1]), reverse=True)[: args.max_contracts]
        quotes = dict(ranked)
    specs = _specs(args)
    summary_rows: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    for spec in specs:
        spec_trades: list[dict[str, Any]] = []
        for symbol, points in quotes.items():
            spec_trades.extend(
                simulate_contract(
                    symbol=symbol,
                    points=points,
                    spec=spec,
                    min_premium=args.min_premium,
                    max_premium=args.max_premium,
                    max_relative_spread=args.max_relative_spread,
                    max_absolute_spread=args.max_absolute_spread,
                    min_quote_size=args.min_quote_size,
                    fee_per_contract=args.fee_per_contract,
                )
            )
        all_trades.extend(spec_trades)
        summary_rows.append(summarize_trades(spec, spec_trades))
    summary_rows.sort(key=lambda row: (float(row["avg_net_pnl"]), int(row["trade_count"])), reverse=True)
    all_trades.sort(key=lambda row: float(row["net_pnl_per_contract"]), reverse=True)
    packet = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "input_events_jsonl": args.events_jsonl,
        "research_only": True,
        "execution_assumption": "causal_momentum_buy_ask_exit_bid",
        "ingest_stats": ingest_stats,
        "contract_count": len(quotes),
        "grid_count": len(specs),
        "best_grid": summary_rows[0] if summary_rows else None,
        "top_grids": summary_rows[:20],
        "top_trades": all_trades[:100],
        "decision": "research_only_shadow_packet",
        "promotion_status": "not_promotion_eligible_without_longer_tick_quote_replay",
    }
    (output_dir / "micro_scalp_signal_grid_packet.json").write_text(
        json.dumps(packet, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_csv(output_dir / "micro_scalp_signal_grid_summary.csv", summary_rows)
    write_csv(output_dir / "micro_scalp_signal_grid_top_trades.csv", all_trades[:500])
    md_lines = [
        "# Micro Scalp Causal Signal Grid",
        "",
        f"Generated: `{packet['generated_at_utc']}`",
        "",
        "Research-only causal option quote momentum test. Entries use prior quote momentum only, "
        "with aggressive buy-at-ask and sell-at-bid economics.",
        "",
        f"- Contracts analyzed: `{packet['contract_count']}`",
        f"- Grid count: `{packet['grid_count']}`",
        "",
        "## Top Grids",
        "",
    ]
    for row in summary_rows[:10]:
        md_lines.append(
            f"- lookback={row['lookback_seconds']}s threshold={row['momentum_threshold_pct']} "
            f"target={row['target_pct']} stop={row['stop_pct']} hold={row['max_hold_seconds']}s "
            f"trades={row['trade_count']} avg_net={row['avg_net_pnl']} total_net={row['net_pnl_total']} "
            f"win_rate={row['win_rate']}"
        )
    md_lines.extend(
        [
            "",
            "## Promotion Posture",
            "",
            "This is not promotion-eligible. It is a scouting packet only. A viable next step requires "
            "longer OPRA quote/trade capture and a tick/quote replay integrated into the governed promoter.",
            "",
        ]
    )
    (output_dir / "micro_scalp_signal_grid_packet.md").write_text("\n".join(md_lines), encoding="utf-8")
    print(f"packet_json={output_dir / 'micro_scalp_signal_grid_packet.json'}")
    print(f"summary_csv={output_dir / 'micro_scalp_signal_grid_summary.csv'}")
    print(f"top_trades_csv={output_dir / 'micro_scalp_signal_grid_top_trades.csv'}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_MULTIPLIER = 100.0


@dataclass(slots=True)
class QuotePoint:
    ts_epoch: float
    observed_epoch: float
    bid: float
    ask: float
    bid_size: float
    ask_size: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Research-only option micro-scalp scout for websocket shadow logs. "
            "This uses aggressive taker assumptions: buy at ask, exit at bid."
        )
    )
    parser.add_argument("--events-jsonl", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-pct", type=float, default=0.01)
    parser.add_argument("--stop-pct", type=float, default=0.006)
    parser.add_argument("--max-hold-seconds", type=float, default=60.0)
    parser.add_argument("--entry-stride", type=int, default=10)
    parser.add_argument("--min-premium", type=float, default=0.15)
    parser.add_argument("--max-premium", type=float, default=12.0)
    parser.add_argument("--max-relative-spread", type=float, default=0.04)
    parser.add_argument("--max-absolute-spread", type=float, default=0.20)
    parser.add_argument("--min-quote-size", type=float, default=1.0)
    parser.add_argument("--fee-per-contract", type=float, default=0.65)
    parser.add_argument("--top-n", type=int, default=50)
    return parser.parse_args()


def _parse_ts(value: Any) -> float | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _underlying_from_option_symbol(symbol: str) -> str:
    match = re.match(r"^([A-Z]+)\d{6}[CP]\d{8}$", symbol.upper())
    return match.group(1) if match else "UNKNOWN"


def _safe_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result if math.isfinite(result) else math.nan


def load_option_quotes(path: Path) -> tuple[dict[str, list[QuotePoint]], dict[str, Any]]:
    quotes: dict[str, list[QuotePoint]] = defaultdict(list)
    stats: dict[str, Any] = {
        "event_count": 0,
        "option_quote_event_count": 0,
        "accepted_quote_count": 0,
        "rejected_quote_count": 0,
        "first_observed_at_utc": None,
        "last_observed_at_utc": None,
    }
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            stats["event_count"] += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                stats["rejected_quote_count"] += 1
                continue
            if event.get("event_type") != "option_quote":
                continue
            stats["option_quote_event_count"] += 1
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            symbol = str(payload.get("symbol") or "").upper()
            observed_epoch = _parse_ts(event.get("observed_at_utc"))
            quote_epoch = _parse_ts(payload.get("timestamp"))
            if not symbol or observed_epoch is None or quote_epoch is None:
                stats["rejected_quote_count"] += 1
                continue
            bid = _safe_float(payload.get("bid_price"))
            ask = _safe_float(payload.get("ask_price"))
            bid_size = _safe_float(payload.get("bid_size"))
            ask_size = _safe_float(payload.get("ask_size"))
            if not (bid > 0.0 and ask > 0.0 and ask >= bid):
                stats["rejected_quote_count"] += 1
                continue
            quotes[symbol].append(
                QuotePoint(
                    ts_epoch=quote_epoch,
                    observed_epoch=observed_epoch,
                    bid=bid,
                    ask=ask,
                    bid_size=0.0 if math.isnan(bid_size) else bid_size,
                    ask_size=0.0 if math.isnan(ask_size) else ask_size,
                )
            )
            stats["accepted_quote_count"] += 1
            observed_iso = event.get("observed_at_utc")
            stats["first_observed_at_utc"] = stats["first_observed_at_utc"] or observed_iso
            stats["last_observed_at_utc"] = observed_iso
    for points in quotes.values():
        points.sort(key=lambda point: (point.ts_epoch, point.observed_epoch))
    return dict(quotes), stats


def simulate_symbol(
    *,
    symbol: str,
    points: list[QuotePoint],
    target_pct: float,
    stop_pct: float,
    max_hold_seconds: float,
    entry_stride: int,
    min_premium: float,
    max_premium: float,
    max_relative_spread: float,
    max_absolute_spread: float,
    min_quote_size: float,
    fee_per_contract: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    opportunities: list[dict[str, Any]] = []
    attempted = 0
    eligible = 0
    for entry_index in range(0, max(0, len(points) - 1), max(1, entry_stride)):
        entry = points[entry_index]
        attempted += 1
        spread = entry.ask - entry.bid
        mid = (entry.ask + entry.bid) / 2.0
        relative_spread = spread / mid if mid > 0 else math.inf
        if (
            entry.ask < min_premium
            or entry.ask > max_premium
            or spread > max_absolute_spread
            or relative_spread > max_relative_spread
            or entry.ask_size < min_quote_size
            or entry.bid_size < min_quote_size
        ):
            continue
        eligible += 1
        target_bid = entry.ask * (1.0 + target_pct)
        stop_bid = entry.ask * (1.0 - stop_pct)
        deadline = entry.ts_epoch + max_hold_seconds
        exit_point = points[entry_index]
        exit_reason = "time_exit"
        max_bid = entry.bid
        min_bid = entry.bid
        quote_count = 0
        for point in points[entry_index + 1 :]:
            if point.ts_epoch > deadline:
                break
            quote_count += 1
            if point.bid > max_bid:
                max_bid = point.bid
            if point.bid < min_bid:
                min_bid = point.bid
            exit_point = point
            if point.bid >= target_bid:
                exit_reason = "target"
                break
            if point.bid <= stop_bid:
                exit_reason = "stop"
                break
        if quote_count == 0:
            continue
        gross_pnl = (exit_point.bid - entry.ask) * CONTRACT_MULTIPLIER
        net_pnl = gross_pnl - (fee_per_contract * 2.0)
        opportunities.append(
            {
                "symbol": symbol,
                "underlying_symbol": _underlying_from_option_symbol(symbol),
                "entry_time_utc": datetime.fromtimestamp(entry.ts_epoch, tz=timezone.utc).isoformat(),
                "exit_time_utc": datetime.fromtimestamp(exit_point.ts_epoch, tz=timezone.utc).isoformat(),
                "hold_seconds": round(max(0.0, exit_point.ts_epoch - entry.ts_epoch), 6),
                "entry_bid": entry.bid,
                "entry_ask": entry.ask,
                "entry_mid": round(mid, 4),
                "entry_spread": round(spread, 4),
                "entry_relative_spread": round(relative_spread, 6),
                "exit_bid": exit_point.bid,
                "exit_reason": exit_reason,
                "max_bid_within_hold": max_bid,
                "min_bid_within_hold": min_bid,
                "gross_pnl_per_contract": round(gross_pnl, 4),
                "net_pnl_per_contract": round(net_pnl, 4),
                "net_return_on_premium": round(net_pnl / (entry.ask * CONTRACT_MULTIPLIER), 6),
                "quote_count_during_hold": quote_count,
            }
        )
    wins = [row for row in opportunities if row["net_pnl_per_contract"] > 0]
    target_wins = [row for row in opportunities if row["exit_reason"] == "target" and row["net_pnl_per_contract"] > 0]
    summary = {
        "symbol": symbol,
        "underlying_symbol": _underlying_from_option_symbol(symbol),
        "quote_count": len(points),
        "attempted_entry_count": attempted,
        "eligible_entry_count": eligible,
        "simulated_trade_count": len(opportunities),
        "winning_trade_count": len(wins),
        "target_win_count": len(target_wins),
        "win_rate": round(len(wins) / len(opportunities), 6) if opportunities else 0.0,
        "target_win_rate": round(len(target_wins) / len(opportunities), 6) if opportunities else 0.0,
        "net_pnl_total": round(sum(float(row["net_pnl_per_contract"]) for row in opportunities), 4),
        "avg_net_pnl": round(
            sum(float(row["net_pnl_per_contract"]) for row in opportunities) / len(opportunities), 4
        )
        if opportunities
        else 0.0,
        "best_net_pnl": round(max((float(row["net_pnl_per_contract"]) for row in opportunities), default=0.0), 4),
        "worst_net_pnl": round(min((float(row["net_pnl_per_contract"]) for row in opportunities), default=0.0), 4),
    }
    return opportunities, summary


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
    events_path = Path(args.events_jsonl)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    quotes, ingest_stats = load_option_quotes(events_path)
    all_opportunities: list[dict[str, Any]] = []
    symbol_summaries: list[dict[str, Any]] = []
    for symbol, points in quotes.items():
        opportunities, summary = simulate_symbol(
            symbol=symbol,
            points=points,
            target_pct=args.target_pct,
            stop_pct=args.stop_pct,
            max_hold_seconds=args.max_hold_seconds,
            entry_stride=args.entry_stride,
            min_premium=args.min_premium,
            max_premium=args.max_premium,
            max_relative_spread=args.max_relative_spread,
            max_absolute_spread=args.max_absolute_spread,
            min_quote_size=args.min_quote_size,
            fee_per_contract=args.fee_per_contract,
        )
        all_opportunities.extend(opportunities)
        symbol_summaries.append(summary)

    all_opportunities.sort(key=lambda row: float(row["net_pnl_per_contract"]), reverse=True)
    symbol_summaries.sort(key=lambda row: (float(row["avg_net_pnl"]), int(row["simulated_trade_count"])), reverse=True)
    underlying: dict[str, dict[str, Any]] = {}
    for row in symbol_summaries:
        key = row["underlying_symbol"]
        bucket = underlying.setdefault(
            key,
            {
                "underlying_symbol": key,
                "contract_count": 0,
                "quote_count": 0,
                "simulated_trade_count": 0,
                "winning_trade_count": 0,
                "target_win_count": 0,
                "net_pnl_total": 0.0,
            },
        )
        bucket["contract_count"] += 1
        for field in ("quote_count", "simulated_trade_count", "winning_trade_count", "target_win_count"):
            bucket[field] += int(row[field])
        bucket["net_pnl_total"] += float(row["net_pnl_total"])
    for bucket in underlying.values():
        trades = int(bucket["simulated_trade_count"])
        bucket["win_rate"] = round(int(bucket["winning_trade_count"]) / trades, 6) if trades else 0.0
        bucket["target_win_rate"] = round(int(bucket["target_win_count"]) / trades, 6) if trades else 0.0
        bucket["avg_net_pnl"] = round(float(bucket["net_pnl_total"]) / trades, 4) if trades else 0.0
        bucket["net_pnl_total"] = round(float(bucket["net_pnl_total"]), 4)
    underlying_rows = sorted(
        underlying.values(),
        key=lambda row: (float(row["avg_net_pnl"]), int(row["simulated_trade_count"])),
        reverse=True,
    )

    top_opportunities = all_opportunities[: max(0, args.top_n)]
    packet = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "input_events_jsonl": str(events_path),
        "research_only": True,
        "execution_assumption": "aggressive_taker_buy_ask_exit_bid",
        "parameters": {
            "target_pct": args.target_pct,
            "stop_pct": args.stop_pct,
            "max_hold_seconds": args.max_hold_seconds,
            "entry_stride": args.entry_stride,
            "min_premium": args.min_premium,
            "max_premium": args.max_premium,
            "max_relative_spread": args.max_relative_spread,
            "max_absolute_spread": args.max_absolute_spread,
            "min_quote_size": args.min_quote_size,
            "fee_per_contract": args.fee_per_contract,
        },
        "ingest_stats": ingest_stats,
        "contract_count": len(quotes),
        "simulated_trade_count": len(all_opportunities),
        "winning_trade_count": sum(1 for row in all_opportunities if row["net_pnl_per_contract"] > 0),
        "target_win_count": sum(
            1
            for row in all_opportunities
            if row["exit_reason"] == "target" and row["net_pnl_per_contract"] > 0
        ),
        "net_pnl_total": round(sum(float(row["net_pnl_per_contract"]) for row in all_opportunities), 4),
        "avg_net_pnl": round(
            sum(float(row["net_pnl_per_contract"]) for row in all_opportunities) / len(all_opportunities),
            4,
        )
        if all_opportunities
        else 0.0,
        "top_underlyings": underlying_rows[:20],
        "top_contracts": symbol_summaries[:50],
        "top_opportunities": top_opportunities,
        "decision": "research_only_shadow_packet",
        "promotion_status": "not_promotion_eligible_without_longer_tick_quote_replay",
    }
    (output_dir / "micro_scalp_shadow_packet.json").write_text(
        json.dumps(packet, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_csv(output_dir / "micro_scalp_underlying_summary.csv", underlying_rows)
    write_csv(output_dir / "micro_scalp_contract_summary.csv", symbol_summaries)
    write_csv(output_dir / "micro_scalp_top_opportunities.csv", top_opportunities)
    md_lines = [
        "# Micro Scalp Shadow Packet",
        "",
        f"Generated: `{packet['generated_at_utc']}`",
        "",
        "This is research-only. It assumes aggressive entry at ask and exit at bid.",
        "",
        f"- Contracts analyzed: `{packet['contract_count']}`",
        f"- Simulated trades: `{packet['simulated_trade_count']}`",
        f"- Winning trades: `{packet['winning_trade_count']}`",
        f"- Target wins: `{packet['target_win_count']}`",
        f"- Average net PnL per contract: `{packet['avg_net_pnl']}`",
        f"- Total net PnL across sampled entries: `{packet['net_pnl_total']}`",
        "",
        "## Top Underlyings",
        "",
    ]
    for row in underlying_rows[:10]:
        md_lines.append(
            f"- `{row['underlying_symbol']}` trades={row['simulated_trade_count']} "
            f"win_rate={row['win_rate']} avg_net={row['avg_net_pnl']} total_net={row['net_pnl_total']}"
        )
    md_lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "A positive result here is only a scouting lead. Promotion requires a longer tick/quote replay set, "
            "quote-age/spread/fill modeling, order-lifecycle simulation, and a generated promotion-review packet.",
            "",
        ]
    )
    (output_dir / "micro_scalp_shadow_packet.md").write_text("\n".join(md_lines), encoding="utf-8")
    print(f"packet_json={output_dir / 'micro_scalp_shadow_packet.json'}")
    print(f"underlying_csv={output_dir / 'micro_scalp_underlying_summary.csv'}")
    print(f"contract_csv={output_dir / 'micro_scalp_contract_summary.csv'}")
    print(f"top_opportunities_csv={output_dir / 'micro_scalp_top_opportunities.csv'}")


if __name__ == "__main__":
    main()

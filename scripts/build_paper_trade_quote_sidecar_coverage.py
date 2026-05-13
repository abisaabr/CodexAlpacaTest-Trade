from __future__ import annotations

import argparse
import bisect
import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit whether completed PAPER trade entry/exit leg quotes are backed by "
            "raw OPRA websocket quote sidecar events."
        )
    )
    parser.add_argument("--session-json", required=True)
    parser.add_argument(
        "--events-jsonl",
        action="append",
        required=True,
        help="Raw Alpaca realtime shadow JSONL file. Repeat to combine capture restarts.",
    )
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--window-seconds", type=float, default=5.0)
    parser.add_argument(
        "--tail-bytes",
        type=int,
        default=0,
        help="Optional tail read for very large JSONL files. Default reads full file.",
    )
    return parser.parse_args()


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_bid_ask(leg: dict[str, Any], prefix: str) -> bool:
    bid_key = f"{prefix}_bid" if prefix else "bid"
    ask_key = f"{prefix}_ask" if prefix else "ask"
    bid = _safe_float(leg.get(bid_key))
    ask = _safe_float(leg.get(ask_key))
    return bool(bid is not None and ask is not None and ask >= bid > 0.0)


def _has_quote_time(leg: dict[str, Any], prefix: str) -> bool:
    key = f"{prefix}_quote_time" if prefix else "quote_time"
    return _parse_datetime(leg.get(key)) is not None


def _iter_jsonl_tail(path: Path, tail_bytes: int) -> Any:
    with path.open("rb") as handle:
        if tail_bytes > 0:
            handle.seek(max(0, path.stat().st_size - tail_bytes))
            if handle.tell() > 0:
                handle.readline()
        for raw_line in handle:
            yield raw_line


def _symbol_from_payload(payload: dict[str, Any]) -> str | None:
    symbol = payload.get("symbol") or payload.get("S")
    if symbol is None:
        return None
    value = str(symbol).strip().upper()
    return value or None


def _event_timestamp(row: dict[str, Any]) -> datetime | None:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    return _parse_datetime(payload.get("timestamp") or payload.get("t"))


def _load_option_quote_events(
    paths: list[Path],
    *,
    symbols: set[str],
    tail_bytes: int = 0,
) -> dict[str, list[dict[str, Any]]]:
    events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in paths:
        if not path.exists():
            continue
        for raw_line in _iter_jsonl_tail(path, max(0, int(tail_bytes))):
            try:
                row = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if str(row.get("event_type") or "") != "option_quote":
                continue
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            symbol = _symbol_from_payload(payload)
            if symbol not in symbols:
                continue
            timestamp = _event_timestamp(row)
            if timestamp is None:
                continue
            bid = _safe_float(payload.get("bid_price", payload.get("bp")))
            ask = _safe_float(payload.get("ask_price", payload.get("ap")))
            events[symbol].append(
                {
                    "timestamp": timestamp,
                    "timestamp_iso": timestamp.isoformat(),
                    "bid": bid,
                    "ask": ask,
                    "source_jsonl": str(path),
                }
            )
    for symbol in list(events):
        events[symbol].sort(key=lambda item: item["timestamp"])
    return dict(events)


def _nearest_event(
    events: list[dict[str, Any]],
    target: datetime | None,
) -> tuple[dict[str, Any] | None, float | None]:
    if target is None or not events:
        return None, None
    timestamps = [event["timestamp"] for event in events]
    index = bisect.bisect_left(timestamps, target)
    candidates: list[dict[str, Any]] = []
    if index < len(events):
        candidates.append(events[index])
    if index > 0:
        candidates.append(events[index - 1])
    if not candidates:
        return None, None
    nearest = min(
        candidates,
        key=lambda event: abs((event["timestamp"] - target).total_seconds()),
    )
    age = abs((nearest["timestamp"] - target).total_seconds())
    return nearest, age


def _signed_lag_seconds(event: dict[str, Any] | None, target: datetime | None) -> float | None:
    if event is None or target is None:
        return None
    return (event["timestamp"] - target).total_seconds()


def _has_valid_event_bid_ask(event: dict[str, Any] | None) -> bool:
    if event is None:
        return False
    bid = _safe_float(event.get("bid"))
    ask = _safe_float(event.get("ask"))
    return bool(bid is not None and ask is not None and ask >= bid > 0.0)


def _gap_reason(
    *,
    symbol: str,
    has_session_bid_ask: bool,
    has_session_quote_time: bool,
    symbol_event_count: int,
    nearest_event: dict[str, Any] | None,
    nearest_age: float | None,
    nearest_lag: float | None,
    sidecar_covered: bool,
    window_seconds: float,
) -> str:
    if sidecar_covered and has_session_bid_ask and has_session_quote_time:
        return "matched_session_and_sidecar_quote"
    if not symbol:
        return "missing_contract_symbol"
    if not has_session_quote_time:
        return "missing_session_quote_time"
    if not has_session_bid_ask:
        return "missing_session_bid_ask"
    if symbol_event_count <= 0:
        return "contract_not_in_sidecar_or_subscription_gap"
    if nearest_event is None or nearest_age is None:
        return "no_sidecar_quote_for_symbol"
    if not _has_valid_event_bid_ask(nearest_event):
        return "invalid_sidecar_bid_ask"
    if nearest_lag is not None and nearest_lag > window_seconds:
        return "nearest_sidecar_quote_after_decision_or_late_capture"
    if nearest_lag is not None and nearest_lag < -window_seconds:
        return "stale_sidecar_quote_before_decision"
    if not sidecar_covered:
        return "sidecar_quote_outside_window"
    return "unknown_quote_gap"


def _completed_leg_rows(session_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade_index, trade in enumerate(session_payload.get("completed_trades") or []):
        if not isinstance(trade, dict):
            continue
        for leg_index, leg in enumerate(trade.get("legs") or []):
            if not isinstance(leg, dict):
                continue
            rows.append(
                {
                    "trade_index": trade_index,
                    "leg_index": leg_index,
                    "strategy_name": trade.get("strategy_name"),
                    "underlying_symbol": trade.get("underlying_symbol"),
                    "regime": trade.get("regime"),
                    "exit_reason": trade.get("exit_reason"),
                    "net_pnl": trade.get("net_pnl"),
                    "entry_time_et": trade.get("entry_time_et"),
                    "exit_time_et": trade.get("exit_time_et"),
                    "option_symbol": str(leg.get("symbol") or "").strip().upper(),
                    "entry_quote_time": leg.get("quote_time"),
                    "exit_quote_time": leg.get("exit_quote_time"),
                    "entry_has_session_bid_ask": _has_bid_ask(leg, ""),
                    "exit_has_session_bid_ask": _has_bid_ask(leg, "exit"),
                    "entry_has_session_quote_time": _has_quote_time(leg, ""),
                    "exit_has_session_quote_time": _has_quote_time(leg, "exit"),
                }
            )
    return rows


def _pct(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total * 100.0, 4)


def build_sidecar_coverage(
    *,
    session_json: Path,
    events_jsonl: Path | list[Path],
    window_seconds: float,
    tail_bytes: int = 0,
) -> dict[str, Any]:
    session_payload = json.loads(session_json.read_text(encoding="utf-8"))
    if not isinstance(session_payload, dict):
        session_payload = {}
    rows = _completed_leg_rows(session_payload)
    symbols = {row["option_symbol"] for row in rows if row["option_symbol"]}
    event_paths = [events_jsonl] if isinstance(events_jsonl, Path) else list(events_jsonl)
    events_by_symbol = _load_option_quote_events(
        event_paths,
        symbols=symbols,
        tail_bytes=tail_bytes,
    )
    detailed_rows: list[dict[str, Any]] = []
    entry_covered = 0
    exit_covered = 0
    for row in rows:
        symbol = str(row["option_symbol"])
        symbol_events = events_by_symbol.get(symbol, [])
        entry_target = _parse_datetime(row.get("entry_quote_time")) or _parse_datetime(
            row.get("entry_time_et")
        )
        exit_target = _parse_datetime(row.get("exit_quote_time")) or _parse_datetime(
            row.get("exit_time_et")
        )
        entry_event, entry_age = _nearest_event(symbol_events, entry_target)
        exit_event, exit_age = _nearest_event(symbol_events, exit_target)
        entry_lag = _signed_lag_seconds(entry_event, entry_target)
        exit_lag = _signed_lag_seconds(exit_event, exit_target)
        entry_is_covered = (
            entry_age is not None
            and entry_age <= window_seconds
            and _has_valid_event_bid_ask(entry_event)
        )
        exit_is_covered = (
            exit_age is not None
            and exit_age <= window_seconds
            and _has_valid_event_bid_ask(exit_event)
        )
        entry_session_and_sidecar_covered = (
            bool(row.get("entry_has_session_bid_ask"))
            and bool(row.get("entry_has_session_quote_time"))
            and entry_is_covered
        )
        exit_session_and_sidecar_covered = (
            bool(row.get("exit_has_session_bid_ask"))
            and bool(row.get("exit_has_session_quote_time"))
            and exit_is_covered
        )
        if entry_is_covered:
            entry_covered += 1
        if exit_is_covered:
            exit_covered += 1
        detailed_rows.append(
            {
                **row,
                "sidecar_event_count_for_symbol": len(symbol_events),
                "entry_nearest_sidecar_quote_time": (
                    entry_event.get("timestamp_iso") if entry_event else None
                ),
                "entry_nearest_sidecar_quote_age_seconds": entry_age,
                "entry_nearest_sidecar_quote_lag_seconds": entry_lag,
                "entry_sidecar_bid": entry_event.get("bid") if entry_event else None,
                "entry_sidecar_ask": entry_event.get("ask") if entry_event else None,
                "entry_sidecar_has_valid_bid_ask": _has_valid_event_bid_ask(entry_event),
                "entry_sidecar_source_jsonl": entry_event.get("source_jsonl") if entry_event else None,
                "entry_sidecar_covered": entry_is_covered,
                "entry_session_and_sidecar_covered": entry_session_and_sidecar_covered,
                "entry_quote_gap_reason": _gap_reason(
                    symbol=symbol,
                    has_session_bid_ask=bool(row.get("entry_has_session_bid_ask")),
                    has_session_quote_time=bool(row.get("entry_has_session_quote_time")),
                    symbol_event_count=len(symbol_events),
                    nearest_event=entry_event,
                    nearest_age=entry_age,
                    nearest_lag=entry_lag,
                    sidecar_covered=entry_is_covered,
                    window_seconds=window_seconds,
                ),
                "exit_nearest_sidecar_quote_time": (
                    exit_event.get("timestamp_iso") if exit_event else None
                ),
                "exit_nearest_sidecar_quote_age_seconds": exit_age,
                "exit_nearest_sidecar_quote_lag_seconds": exit_lag,
                "exit_sidecar_bid": exit_event.get("bid") if exit_event else None,
                "exit_sidecar_ask": exit_event.get("ask") if exit_event else None,
                "exit_sidecar_has_valid_bid_ask": _has_valid_event_bid_ask(exit_event),
                "exit_sidecar_source_jsonl": exit_event.get("source_jsonl") if exit_event else None,
                "exit_sidecar_covered": exit_is_covered,
                "exit_session_and_sidecar_covered": exit_session_and_sidecar_covered,
                "exit_quote_gap_reason": _gap_reason(
                    symbol=symbol,
                    has_session_bid_ask=bool(row.get("exit_has_session_bid_ask")),
                    has_session_quote_time=bool(row.get("exit_has_session_quote_time")),
                    symbol_event_count=len(symbol_events),
                    nearest_event=exit_event,
                    nearest_age=exit_age,
                    nearest_lag=exit_lag,
                    sidecar_covered=exit_is_covered,
                    window_seconds=window_seconds,
                ),
            }
        )
    leg_count = len(rows)
    complete_legs = sum(
        1
        for row in detailed_rows
        if row["entry_session_and_sidecar_covered"]
        and row["exit_session_and_sidecar_covered"]
    )
    evidence_complete = leg_count > 0 and complete_legs == leg_count
    summary = {
        "status": "paper_trade_quote_sidecar_coverage_complete",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "session_json": str(session_json),
        "events_jsonl": [str(path) for path in event_paths],
        "events_jsonl_count": len(event_paths),
        "window_seconds": float(window_seconds),
        "tail_bytes": int(tail_bytes),
        "completed_trade_count": len(session_payload.get("completed_trades") or []),
        "completed_leg_count": leg_count,
        "unique_completed_leg_symbol_count": len(symbols),
        "sidecar_symbol_match_count": len(events_by_symbol),
        "entry_sidecar_covered_count": entry_covered,
        "entry_sidecar_coverage_pct": _pct(entry_covered, leg_count),
        "exit_sidecar_covered_count": exit_covered,
        "exit_sidecar_coverage_pct": _pct(exit_covered, leg_count),
        "complete_quote_backed_leg_count": complete_legs,
        "complete_quote_backed_leg_pct": _pct(complete_legs, leg_count),
        "evidence_status": (
            "complete_session_and_sidecar_quote_evidence"
            if evidence_complete
            else "quote_sidecar_gaps_present"
        ),
        "quote_backed_evidence_gate": "pass" if evidence_complete else "fail",
        "quote_backed_projection_input_allowed": evidence_complete,
        "quote_backed_optimizer_input_allowed": evidence_complete,
        "quote_backed_promotion_input_allowed": evidence_complete,
        "gate_policy": (
            "Completed PAPER trades may feed projection/optimizer/promotion only when every "
            "completed leg has entry and exit session quote fields plus raw OPRA sidecar quotes "
            "inside the configured time window."
        ),
        "rows": detailed_rows,
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }
    return summary


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    summary = build_sidecar_coverage(
        session_json=Path(args.session_json),
        events_jsonl=[Path(value) for value in args.events_jsonl],
        window_seconds=max(0.0, float(args.window_seconds)),
        tail_bytes=max(0, int(args.tail_bytes)),
    )
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    if args.output_csv:
        _write_csv(Path(args.output_csv), list(summary.get("rows") or []))
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

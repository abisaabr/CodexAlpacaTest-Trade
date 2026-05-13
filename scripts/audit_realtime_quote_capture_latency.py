from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
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
            "Summarize realtime quote-capture latency, symbol coverage, and "
            "option spread realism from realtime_shadow_events.jsonl."
        )
    )
    parser.add_argument("--events-jsonl", required=True, help="Realtime shadow JSONL event file.")
    parser.add_argument("--output-json", default=None, help="Optional output JSON path.")
    parser.add_argument(
        "--tail-bytes",
        type=int,
        default=0,
        help="Only audit the tail of a large JSONL file. Default reads the full file.",
    )
    return parser.parse_args()


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile(samples: list[float], pct: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    return round(float(ordered[int((len(ordered) - 1) * pct)]), 6)


def _sample_summary(samples: list[float]) -> dict[str, float | int | None]:
    return {
        "count": len(samples),
        "min": round(min(samples), 6) if samples else None,
        "p50": _percentile(samples, 0.50),
        "p90": _percentile(samples, 0.90),
        "p99": _percentile(samples, 0.99),
        "max": round(max(samples), 6) if samples else None,
    }


def _symbol_from_event(row: dict[str, Any]) -> str | None:
    payload = row.get("payload")
    if not isinstance(payload, dict):
        return None
    symbol = payload.get("symbol") or payload.get("S")
    if not symbol:
        return None
    return str(symbol).strip().upper()


def _iter_jsonl_tail(path: Path, tail_bytes: int) -> Any:
    with path.open("rb") as handle:
        if tail_bytes > 0:
            handle.seek(max(0, path.stat().st_size - tail_bytes))
            if handle.tell() > 0:
                handle.readline()
        for raw_line in handle:
            yield raw_line


def audit_events(path: Path, *, tail_bytes: int = 0) -> dict[str, Any]:
    event_counts: Counter[str] = Counter()
    symbol_counts: Counter[str] = Counter()
    latency_by_type: dict[str, list[float]] = defaultdict(list)
    latency_by_symbol: dict[str, list[float]] = defaultdict(list)
    option_spread_samples: list[float] = []
    option_relative_spread_samples: list[float] = []
    parse_errors = 0
    first_observed_at: str | None = None
    last_observed_at: str | None = None
    last_event: dict[str, Any] | None = None

    for raw_line in _iter_jsonl_tail(path, tail_bytes):
        try:
            row = json.loads(raw_line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue
        event_type = str(row.get("event_type") or "unknown")
        event_counts[event_type] += 1
        observed_at = row.get("observed_at_utc")
        if observed_at and first_observed_at is None:
            first_observed_at = str(observed_at)
        if observed_at:
            last_observed_at = str(observed_at)
        latency = _coerce_float(row.get("latency_seconds"))
        symbol = _symbol_from_event(row)
        if symbol:
            symbol_counts[symbol] += 1
        if latency is not None:
            latency_by_type[event_type].append(latency)
            if symbol:
                latency_by_symbol[symbol].append(latency)
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if event_type == "option_quote":
            bid = _coerce_float(payload.get("bid_price", payload.get("bp")))
            ask = _coerce_float(payload.get("ask_price", payload.get("ap")))
            if bid is not None and ask is not None and ask >= bid > 0.0:
                spread = ask - bid
                midpoint = (ask + bid) / 2.0
                option_spread_samples.append(spread)
                if midpoint > 0.0:
                    option_relative_spread_samples.append(spread / midpoint)
        last_event = row

    latency_all = [value for samples in latency_by_type.values() for value in samples]
    now_utc = datetime.now(UTC).isoformat()
    stale_seconds: float | None = None
    if last_observed_at:
        try:
            last_dt = datetime.fromisoformat(last_observed_at.replace("Z", "+00:00")).astimezone(UTC)
            stale_seconds = max(0.0, (datetime.now(UTC) - last_dt).total_seconds())
        except ValueError:
            stale_seconds = None
    return {
        "generated_at_utc": now_utc,
        "events_jsonl": str(path),
        "tail_bytes": int(tail_bytes),
        "file_size_bytes": path.stat().st_size,
        "parse_errors": parse_errors,
        "event_counts": {key: int(value) for key, value in sorted(event_counts.items())},
        "unique_symbol_count": len(symbol_counts),
        "top_symbols": symbol_counts.most_common(25),
        "first_observed_at_utc": first_observed_at,
        "last_observed_at_utc": last_observed_at,
        "last_observed_stale_seconds": round(stale_seconds, 6) if stale_seconds is not None else None,
        "latency_seconds": _sample_summary(latency_all),
        "latency_seconds_by_event_type": {
            key: _sample_summary(samples) for key, samples in sorted(latency_by_type.items())
        },
        "latency_seconds_by_symbol_top25": {
            symbol: _sample_summary(latency_by_symbol[symbol])
            for symbol, _count in symbol_counts.most_common(25)
            if symbol in latency_by_symbol
        },
        "option_quote_spread": _sample_summary(option_spread_samples),
        "option_quote_relative_spread": _sample_summary(option_relative_spread_samples),
        "last_event": last_event,
    }


def main() -> None:
    args = parse_args()
    path = Path(args.events_jsonl)
    if not path.exists():
        raise FileNotFoundError(f"events JSONL not found: {path}")
    summary = audit_events(path, tail_bytes=max(0, int(args.tail_bytes)))
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

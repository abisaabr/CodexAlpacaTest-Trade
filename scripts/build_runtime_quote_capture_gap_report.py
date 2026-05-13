from __future__ import annotations

import argparse
import csv
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

from alpaca_lab.config import load_settings
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from scripts.build_runtime_leg_quote_capture_symbols import (
    build_runtime_leg_quote_capture_rows,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare current runtime-selected PAPER strategy legs, session trade legs, "
            "and the active no-submit OPRA quote-capture subscription plan."
        )
    )
    parser.add_argument("--config", default=None, help="Optional base repo YAML config.")
    parser.add_argument(
        "--portfolio-config",
        default="config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml",
    )
    parser.add_argument("--subscription-plan-json", required=True)
    parser.add_argument("--session-json", default=None)
    parser.add_argument("--events-jsonl", default=None)
    parser.add_argument("--events-tail-bytes", type=int, default=30_000_000)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--output-csv", default=None)
    return parser.parse_args()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _normalise_symbols(values: Any) -> set[str]:
    if not values:
        return set()
    return {str(value).strip().upper() for value in values if str(value).strip()}


def _runtime_symbol_metadata(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "runtime_strategy_count": 0,
            "runtime_underlyings": set(),
            "runtime_regimes": set(),
            "runtime_families": set(),
            "runtime_strategies": set(),
        }
    )
    for row in rows:
        symbol = str(row.get("option_symbol") or "").strip().upper()
        if not symbol:
            continue
        payload = metadata[symbol]
        payload["runtime_strategy_count"] += 1
        for source_key, target_key in (
            ("underlying_symbol", "runtime_underlyings"),
            ("regime", "runtime_regimes"),
            ("family", "runtime_families"),
            ("strategy_name", "runtime_strategies"),
        ):
            value = str(row.get(source_key) or "").strip()
            if value:
                payload[target_key].add(value)
    return {
        symbol: {
            **payload,
            "runtime_underlyings": sorted(payload["runtime_underlyings"]),
            "runtime_regimes": sorted(payload["runtime_regimes"]),
            "runtime_families": sorted(payload["runtime_families"]),
            "runtime_strategies": sorted(payload["runtime_strategies"]),
        }
        for symbol, payload in metadata.items()
    }


def _session_symbol_metadata(session_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "session_leg_count": 0,
            "session_trade_buckets": set(),
            "session_underlyings": set(),
            "session_regimes": set(),
            "session_strategies": set(),
        }
    )
    for bucket in ("open_trades", "completed_trades"):
        for trade in session_payload.get(bucket) or []:
            if not isinstance(trade, dict):
                continue
            for leg in trade.get("legs") or []:
                if not isinstance(leg, dict):
                    continue
                symbol = str(leg.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                payload = metadata[symbol]
                payload["session_leg_count"] += 1
                payload["session_trade_buckets"].add(bucket)
                for source_key, target_key in (
                    ("underlying_symbol", "session_underlyings"),
                    ("regime", "session_regimes"),
                    ("strategy_name", "session_strategies"),
                ):
                    value = str(trade.get(source_key) or "").strip()
                    if value:
                        payload[target_key].add(value)
    return {
        symbol: {
            **payload,
            "session_trade_buckets": sorted(payload["session_trade_buckets"]),
            "session_underlyings": sorted(payload["session_underlyings"]),
            "session_regimes": sorted(payload["session_regimes"]),
            "session_strategies": sorted(payload["session_strategies"]),
        }
        for symbol, payload in metadata.items()
    }


def _iter_jsonl_tail(path: Path, tail_bytes: int) -> Any:
    with path.open("rb") as handle:
        if tail_bytes > 0:
            handle.seek(max(0, path.stat().st_size - tail_bytes))
            if handle.tell() > 0:
                handle.readline()
        for raw_line in handle:
            yield raw_line


def _observed_quote_symbols(path: Path, *, tail_bytes: int) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not path.exists():
        return counts
    for raw_line in _iter_jsonl_tail(path, max(0, int(tail_bytes))):
        try:
            row = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if str(row.get("event_type") or "") != "option_quote":
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        symbol = str(payload.get("symbol") or payload.get("S") or "").strip().upper()
        if symbol:
            counts[symbol] += 1
    return counts


def _pct(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total * 100.0, 4)


def build_gap_report(
    *,
    runtime_rows: list[dict[str, Any]],
    runtime_errors: list[dict[str, str]],
    plan_payload: dict[str, Any],
    session_payload: dict[str, Any] | None = None,
    observed_counts: Counter[str] | None = None,
) -> dict[str, Any]:
    session_payload = session_payload or {}
    observed_counts = observed_counts or Counter()
    runtime_meta = _runtime_symbol_metadata(runtime_rows)
    session_meta = _session_symbol_metadata(session_payload)
    runtime_symbols = set(runtime_meta)
    session_symbols = set(session_meta)
    plan_symbols = _normalise_symbols(plan_payload.get("option_symbols"))
    observed_symbols = set(observed_counts)
    all_symbols = sorted(runtime_symbols | session_symbols | plan_symbols | observed_symbols)
    rows: list[dict[str, Any]] = []
    for symbol in all_symbols:
        runtime_payload = runtime_meta.get(symbol, {})
        session_payload_for_symbol = session_meta.get(symbol, {})
        rows.append(
            {
                "option_symbol": symbol,
                "in_current_runtime_selection": symbol in runtime_symbols,
                "in_session_trades": symbol in session_symbols,
                "in_subscription_plan": symbol in plan_symbols,
                "observed_in_quote_tail": symbol in observed_symbols,
                "tail_option_quote_event_count": int(observed_counts.get(symbol, 0)),
                "runtime_strategy_count": runtime_payload.get("runtime_strategy_count", 0),
                "runtime_underlyings": ",".join(runtime_payload.get("runtime_underlyings", [])),
                "runtime_regimes": ",".join(runtime_payload.get("runtime_regimes", [])),
                "runtime_families": ",".join(runtime_payload.get("runtime_families", [])),
                "session_leg_count": session_payload_for_symbol.get("session_leg_count", 0),
                "session_trade_buckets": ",".join(
                    session_payload_for_symbol.get("session_trade_buckets", [])
                ),
                "session_underlyings": ",".join(
                    session_payload_for_symbol.get("session_underlyings", [])
                ),
                "session_regimes": ",".join(
                    session_payload_for_symbol.get("session_regimes", [])
                ),
            }
        )
    runtime_missing = sorted(runtime_symbols - plan_symbols)
    session_missing = sorted(session_symbols - plan_symbols)
    open_session_symbols = {
        symbol
        for symbol, payload in session_meta.items()
        if "open_trades" in set(payload.get("session_trade_buckets", []))
    }
    completed_session_symbols = {
        symbol
        for symbol, payload in session_meta.items()
        if "completed_trades" in set(payload.get("session_trade_buckets", []))
    }
    open_session_missing = sorted(open_session_symbols - plan_symbols)
    completed_session_missing = sorted(completed_session_symbols - plan_symbols)
    planned_unobserved = sorted(plan_symbols - observed_symbols) if observed_symbols else []
    restart_missing = sorted(set(runtime_missing).union(open_session_missing))
    dynamic_refresh_active = any(
        "runtime refresh" in str(note).lower()
        for note in (plan_payload.get("notes") or [])
    )
    if open_session_missing:
        decision = "quote_capture_plan_current_gap"
        restart_recommended = True
    elif runtime_missing and dynamic_refresh_active:
        decision = "quote_capture_runtime_gap_dynamic_refresh_active"
        restart_recommended = False
    elif runtime_missing:
        decision = "quote_capture_plan_current_gap"
        restart_recommended = True
    elif completed_session_missing:
        decision = "quote_capture_plan_currently_covered_with_historical_session_gaps"
        restart_recommended = False
    else:
        decision = "quote_capture_plan_covers_runtime_and_session_symbols"
        restart_recommended = False
    if restart_recommended:
        restart_guidance = (
            "During RTH, restart only the no-submit quote shadow after confirming a single "
            "existing shadow can be stopped cleanly; do not start a duplicate websocket stream."
        )
    elif runtime_missing and dynamic_refresh_active:
        restart_guidance = (
            "No quote shadow restart recommended: current gaps are runtime-selection drift, "
            "there are no open-trade gaps, and dynamic runtime refresh is active. Verify the "
            "next refresh adds these symbols before escalating."
        )
    elif completed_session_missing:
        restart_guidance = (
            "No quote shadow restart is needed for current runtime or open-trade coverage; "
            "completed-trade gaps are historical OPRA evidence gaps that require sidecar/backfill "
            "repair, not a live websocket restart."
        )
    else:
        restart_guidance = "No quote shadow restart needed for current runtime/session symbol coverage."
    return {
        "status": "runtime_quote_capture_gap_report_complete",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "decision": decision,
        "runtime_selected_leg_row_count": len(runtime_rows),
        "runtime_selected_unique_symbols": len(runtime_symbols),
        "runtime_error_count": len(runtime_errors),
        "runtime_errors": runtime_errors,
        "subscription_plan_symbol_count": len(plan_symbols),
        "session_trade_symbol_count": len(session_symbols),
        "observed_tail_symbol_count": len(observed_symbols),
        "runtime_symbols_covered_pct": _pct(
            len(runtime_symbols.intersection(plan_symbols)),
            len(runtime_symbols),
        ),
        "session_symbols_covered_pct": _pct(
            len(session_symbols.intersection(plan_symbols)),
            len(session_symbols),
        ),
        "open_session_symbols_covered_pct": _pct(
            len(open_session_symbols.intersection(plan_symbols)),
            len(open_session_symbols),
        ),
        "completed_session_symbols_covered_pct": _pct(
            len(completed_session_symbols.intersection(plan_symbols)),
            len(completed_session_symbols),
        ),
        "runtime_symbols_missing_from_plan": runtime_missing,
        "session_symbols_missing_from_plan": session_missing,
        "open_session_symbols_missing_from_plan": open_session_missing,
        "completed_session_symbols_missing_from_plan": completed_session_missing,
        "restart_relevant_symbols_missing_from_plan": restart_missing,
        "dynamic_runtime_refresh_active": dynamic_refresh_active,
        "planned_symbols_unobserved_in_tail": planned_unobserved,
        "restart_quote_shadow_recommended": restart_recommended,
        "restart_guidance": restart_guidance,
        "rows": rows,
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    portfolio_config = load_portfolio_config(args.portfolio_config)
    _trade_date, runtime_rows, runtime_errors = build_runtime_leg_quote_capture_rows(
        settings,
        portfolio_config,
    )
    plan_payload = _read_json(Path(args.subscription_plan_json), {})
    session_payload = _read_json(Path(args.session_json), {}) if args.session_json else {}
    observed_counts = (
        _observed_quote_symbols(Path(args.events_jsonl), tail_bytes=args.events_tail_bytes)
        if args.events_jsonl
        else Counter()
    )
    report = build_gap_report(
        runtime_rows=runtime_rows,
        runtime_errors=runtime_errors,
        plan_payload=plan_payload if isinstance(plan_payload, dict) else {},
        session_payload=session_payload if isinstance(session_payload, dict) else {},
        observed_counts=observed_counts,
    )
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    if args.output_csv:
        _write_csv(Path(args.output_csv), list(report.get("rows") or []))
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

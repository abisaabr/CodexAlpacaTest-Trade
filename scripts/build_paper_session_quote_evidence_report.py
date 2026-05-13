from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.multi_ticker_portfolio import load_portfolio_config


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _has_positive_bid_ask(prefix: str, leg: dict[str, Any]) -> bool:
    bid_key = f"{prefix}_bid" if prefix else "bid"
    ask_key = f"{prefix}_ask" if prefix else "ask"
    bid = _safe_float(leg.get(bid_key))
    ask = _safe_float(leg.get(ask_key))
    return bool(bid is not None and ask is not None and bid > 0.0 and ask >= bid)


def _has_quote_time(prefix: str, leg: dict[str, Any]) -> bool:
    key = f"{prefix}_quote_time" if prefix else "quote_time"
    return bool(str(leg.get(key) or "").strip())


def _has_freshness(prefix: str, leg: dict[str, Any]) -> bool:
    key = f"{prefix}_freshness_seconds" if prefix else "freshness_seconds"
    return _safe_float(leg.get(key)) is not None


def _has_spread(prefix: str, leg: dict[str, Any]) -> bool:
    key = f"{prefix}_spread_pct" if prefix else "spread_pct"
    spread = _safe_float(leg.get(key))
    return bool(spread is not None and spread >= 0.0)


def _trade_rows(session_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in ("completed_trades", "open_trades"):
        for trade_index, trade in enumerate(session_payload.get(bucket) or []):
            legs = trade.get("legs") or []
            for leg_index, leg in enumerate(legs):
                if not isinstance(leg, dict):
                    continue
                rows.append(
                    {
                        "trade_bucket": bucket,
                        "trade_index": trade_index,
                        "leg_index": leg_index,
                        "strategy_name": trade.get("strategy_name"),
                        "underlying_symbol": trade.get("underlying_symbol"),
                        "regime": trade.get("regime"),
                        "exit_reason": trade.get("exit_reason"),
                        "net_pnl": trade.get("net_pnl"),
                        "option_symbol": leg.get("symbol"),
                        "leg_side": leg.get("side"),
                        "entry_has_bid_ask": _has_positive_bid_ask("", leg),
                        "entry_has_quote_time": _has_quote_time("", leg),
                        "entry_has_freshness": _has_freshness("", leg),
                        "entry_has_spread": _has_spread("", leg),
                        "entry_bid": leg.get("bid"),
                        "entry_ask": leg.get("ask"),
                        "entry_mark": leg.get("mark"),
                        "entry_quote_time": leg.get("quote_time"),
                        "entry_spread_pct": leg.get("spread_pct"),
                        "entry_freshness_seconds": leg.get("freshness_seconds"),
                        "exit_has_bid_ask": _has_positive_bid_ask("exit", leg),
                        "exit_has_quote_time": _has_quote_time("exit", leg),
                        "exit_has_freshness": _has_freshness("exit", leg),
                        "exit_has_spread": _has_spread("exit", leg),
                        "exit_bid": leg.get("exit_bid"),
                        "exit_ask": leg.get("exit_ask"),
                        "exit_mark": leg.get("exit_mark"),
                        "exit_quote_time": leg.get("exit_quote_time"),
                        "exit_spread_pct": leg.get("exit_spread_pct"),
                        "exit_freshness_seconds": leg.get("exit_freshness_seconds"),
                    }
                )
    return rows


def _pct(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total * 100.0, 4)


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        f"# Paper Session Quote Evidence - {summary['trade_date']}",
        "",
        f"- Session found: `{summary['session_found']}`",
        f"- Completed trades: `{summary['completed_trade_count']}`",
        f"- Open trades: `{summary['open_trade_count']}`",
        f"- Leg rows: `{summary['leg_row_count']}`",
        f"- Entry bid/ask coverage: `{summary['entry_bid_ask_coverage_pct']}%`",
        f"- Entry quote-time coverage: `{summary['entry_quote_time_coverage_pct']}%`",
        f"- Exit bid/ask coverage on completed legs: `{summary['completed_exit_bid_ask_coverage_pct']}%`",
        f"- Exit quote-time coverage on completed legs: `{summary['completed_exit_quote_time_coverage_pct']}%`",
        f"- Evidence status: `{summary['evidence_status']}`",
        f"- Detail CSV: `{summary['quote_evidence_csv']}`",
        "",
    ]
    if summary.get("evidence_status") != "complete_entry_exit_quote_fields":
        lines.extend(
            [
                "## Blockers",
                "",
                "Completed paper trades are not fully quote-field-backed yet. Use this report with websocket sidecars and broker fills before promotion analysis.",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def build_paper_session_quote_evidence_report(
    *,
    state_root: Path,
    run_root: Path,
    trade_date: str,
) -> dict[str, Any]:
    session_path = state_root / f"session_{trade_date}.json"
    session_payload = _read_json(session_path, {})
    rows = _trade_rows(session_payload) if isinstance(session_payload, dict) else []
    frame = pd.DataFrame(rows)
    output_dir = run_root / trade_date
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"paper_session_quote_evidence_{trade_date}.csv"
    frame.to_csv(csv_path, index=False)

    completed = frame.loc[frame["trade_bucket"] == "completed_trades"].copy() if not frame.empty else frame
    leg_count = int(len(frame))
    completed_leg_count = int(len(completed))
    entry_bid_ask = int(frame["entry_has_bid_ask"].sum()) if not frame.empty else 0
    entry_quote_time = int(frame["entry_has_quote_time"].sum()) if not frame.empty else 0
    entry_freshness = int(frame["entry_has_freshness"].sum()) if not frame.empty else 0
    entry_spread = int(frame["entry_has_spread"].sum()) if not frame.empty else 0
    exit_bid_ask = int(completed["exit_has_bid_ask"].sum()) if completed_leg_count else 0
    exit_quote_time = int(completed["exit_has_quote_time"].sum()) if completed_leg_count else 0
    exit_freshness = int(completed["exit_has_freshness"].sum()) if completed_leg_count else 0
    exit_spread = int(completed["exit_has_spread"].sum()) if completed_leg_count else 0
    evidence_complete = (
        leg_count > 0
        and completed_leg_count > 0
        and entry_bid_ask == leg_count
        and entry_quote_time == leg_count
        and entry_freshness == leg_count
        and entry_spread == leg_count
        and exit_bid_ask == completed_leg_count
        and exit_quote_time == completed_leg_count
        and exit_freshness == completed_leg_count
        and exit_spread == completed_leg_count
    )
    summary = {
        "status": "paper_session_quote_evidence_complete",
        "trade_date": trade_date,
        "session_path": str(session_path),
        "session_found": bool(session_payload),
        "quote_evidence_csv": str(csv_path),
        "completed_trade_count": int(len(session_payload.get("completed_trades") or [])) if isinstance(session_payload, dict) else 0,
        "open_trade_count": int(len(session_payload.get("open_trades") or [])) if isinstance(session_payload, dict) else 0,
        "leg_row_count": leg_count,
        "completed_leg_row_count": completed_leg_count,
        "entry_bid_ask_count": entry_bid_ask,
        "entry_bid_ask_coverage_pct": _pct(entry_bid_ask, leg_count),
        "entry_quote_time_count": entry_quote_time,
        "entry_quote_time_coverage_pct": _pct(entry_quote_time, leg_count),
        "entry_freshness_count": entry_freshness,
        "entry_freshness_coverage_pct": _pct(entry_freshness, leg_count),
        "entry_spread_count": entry_spread,
        "entry_spread_coverage_pct": _pct(entry_spread, leg_count),
        "completed_exit_bid_ask_count": exit_bid_ask,
        "completed_exit_bid_ask_coverage_pct": _pct(exit_bid_ask, completed_leg_count),
        "completed_exit_quote_time_count": exit_quote_time,
        "completed_exit_quote_time_coverage_pct": _pct(exit_quote_time, completed_leg_count),
        "completed_exit_freshness_count": exit_freshness,
        "completed_exit_freshness_coverage_pct": _pct(exit_freshness, completed_leg_count),
        "completed_exit_spread_count": exit_spread,
        "completed_exit_spread_coverage_pct": _pct(exit_spread, completed_leg_count),
        "evidence_status": "complete_entry_exit_quote_fields" if evidence_complete else "quote_field_gaps_present",
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }
    json_path = output_dir / f"paper_session_quote_evidence_{trade_date}.json"
    md_path = output_dir / f"paper_session_quote_evidence_{trade_date}.md"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["quote_evidence_json"] = str(json_path)
    summary["quote_evidence_markdown"] = str(md_path)
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _write_markdown(md_path, summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a broker-free quote-field evidence report from a PAPER session JSON."
    )
    parser.add_argument(
        "--portfolio-config",
        default="config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml",
    )
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--state-root", type=Path, default=None)
    parser.add_argument("--run-root", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_portfolio_config(args.portfolio_config)
    state_root = Path(args.state_root or config.execution.state_root)
    run_root = Path(args.run_root or config.execution.run_root)
    summary = build_paper_session_quote_evidence_report(
        state_root=state_root,
        run_root=run_root,
        trade_date=str(args.trade_date),
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

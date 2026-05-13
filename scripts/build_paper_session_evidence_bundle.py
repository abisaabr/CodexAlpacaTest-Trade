from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover - importable module fallback
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
from scripts.build_paper_session_quote_evidence_report import (
    build_paper_session_quote_evidence_report,
)
from scripts.build_paper_trade_quote_sidecar_coverage import build_sidecar_coverage
from scripts.build_realtime_quote_quality_sidecar import build_realtime_quote_quality_sidecar


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a broker-free PAPER session evidence bundle. The bundle combines "
            "session quote fields, raw OPRA websocket sidecar coverage, and converted "
            "quote-quality sidecars, then fails closed for projection/optimizer use "
            "unless every completed trade leg is quote-backed."
        )
    )
    parser.add_argument("--portfolio-config", required=True)
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument(
        "--quote-events-jsonl",
        action="append",
        default=None,
        help="Raw Alpaca realtime shadow JSONL file. Repeat to combine capture restarts.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--state-root", default=None)
    parser.add_argument("--sidecar-window-seconds", type=float, default=5.0)
    parser.add_argument("--sidecar-tail-bytes", type=int, default=0)
    parser.add_argument(
        "--underlyings",
        default="",
        help="Optional comma-separated underlying filter for quote-quality sidecars.",
    )
    return parser.parse_args()


def _csv_set(value: str) -> set[str]:
    return {item.strip().upper() for item in str(value or "").split(",") if item.strip()}


def _read_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    candidate = Path(path)
    if not candidate.exists():
        return {}
    payload = json.loads(candidate.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _session_option_symbols(session_path: Path) -> set[str]:
    payload = _read_json(str(session_path))
    symbols: set[str] = set()
    for bucket in ("completed_trades", "open_trades"):
        for trade in payload.get(bucket) or []:
            if not isinstance(trade, dict):
                continue
            for leg in trade.get("legs") or []:
                if not isinstance(leg, dict):
                    continue
                symbol = str(leg.get("symbol") or "").strip().upper()
                if symbol:
                    symbols.add(symbol)
    return symbols


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        f"# Paper Session Evidence Bundle - {summary['trade_date']}",
        "",
        f"- Evidence status: `{summary['evidence_status']}`",
        f"- Session quote field gate: `{summary['session_quote_field_gate']}`",
        f"- Raw OPRA sidecar gate: `{summary['raw_sidecar_quote_gate']}`",
        f"- Quote-quality sidecar status: `{summary['quote_quality_sidecar_status']}`",
        f"- Projection input allowed: `{summary['quote_backed_projection_input_allowed']}`",
        f"- Optimizer input allowed: `{summary['quote_backed_optimizer_input_allowed']}`",
        f"- Promotion input allowed: `{summary['quote_backed_promotion_input_allowed']}`",
        "",
        "## Outputs",
        "",
    ]
    for name, output_path in sorted((summary.get("outputs") or {}).items()):
        lines.append(f"- {name}: `{output_path}`")
    if summary.get("blockers"):
        lines.extend(["", "## Blockers", ""])
        for blocker in summary["blockers"]:
            lines.append(f"- `{blocker}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_paper_session_evidence_bundle(
    *,
    portfolio_config: Path,
    trade_date: str,
    quote_events_jsonl: Path | list[Path] | None,
    output_dir: Path,
    state_root: Path | None = None,
    sidecar_window_seconds: float = 5.0,
    sidecar_tail_bytes: int = 0,
    underlyings: set[str] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    config = load_portfolio_config(portfolio_config)
    resolved_state_root = Path(state_root or config.execution.state_root)

    session_report = build_paper_session_quote_evidence_report(
        state_root=resolved_state_root,
        run_root=output_dir / "session_quote_evidence",
        trade_date=trade_date,
    )
    session_gate = str(session_report.get("session_quote_field_gate") or "fail")

    sidecar_summary: dict[str, Any] = {
        "quote_backed_evidence_gate": "fail",
        "evidence_status": "quote_sidecar_not_available",
    }
    quote_quality_summary: dict[str, Any] = {
        "quality_status": "quote_events_not_available",
    }
    outputs: dict[str, str] = {
        "session_quote_evidence_json": str(session_report.get("quote_evidence_json")),
        "session_quote_evidence_csv": str(session_report.get("quote_evidence_csv")),
    }
    quote_event_paths: list[Path] = []
    if isinstance(quote_events_jsonl, Path):
        quote_event_paths = [quote_events_jsonl]
    elif quote_events_jsonl:
        quote_event_paths = list(quote_events_jsonl)
    existing_quote_event_paths = [path for path in quote_event_paths if path.exists()]

    if existing_quote_event_paths:
        sidecar_summary = build_sidecar_coverage(
            session_json=resolved_state_root / f"session_{trade_date}.json",
            events_jsonl=existing_quote_event_paths,
            window_seconds=max(0.0, float(sidecar_window_seconds)),
            tail_bytes=max(0, int(sidecar_tail_bytes)),
        )
        sidecar_json = output_dir / "paper_trade_quote_sidecar_coverage.json"
        sidecar_csv = output_dir / "paper_trade_quote_sidecar_coverage.csv"
        sidecar_json.write_text(json.dumps(sidecar_summary, indent=2, sort_keys=True), encoding="utf-8")
        rows = sidecar_summary.get("rows") or []
        if rows:
            import pandas as pd

            pd.DataFrame(rows).to_csv(sidecar_csv, index=False)
        else:
            sidecar_csv.write_text("", encoding="utf-8")
        outputs["paper_trade_sidecar_coverage_json"] = str(sidecar_json)
        outputs["paper_trade_sidecar_coverage_csv"] = str(sidecar_csv)

        quote_quality_summary = build_realtime_quote_quality_sidecar(
            events_jsonl=existing_quote_event_paths,
            output_dir=output_dir / "quote_quality_sidecar",
            underlyings=underlyings,
            option_symbols=_session_option_symbols(resolved_state_root / f"session_{trade_date}.json"),
        )
        outputs.update(
            {
                key: str(value)
                for key, value in (quote_quality_summary.get("outputs") or {}).items()
            }
        )

    raw_sidecar_gate = str(sidecar_summary.get("quote_backed_evidence_gate") or "fail")
    quote_quality_status = str(quote_quality_summary.get("quality_status") or "unknown")
    allowed = (
        session_gate == "pass"
        and raw_sidecar_gate == "pass"
        and quote_quality_status == "quote_events_ready_for_asof_replay_join"
    )
    blockers: list[str] = []
    if session_gate != "pass":
        blockers.append("session_quote_field_gate_failed")
    if raw_sidecar_gate != "pass":
        blockers.append("raw_opra_sidecar_gate_failed")
    if quote_quality_status != "quote_events_ready_for_asof_replay_join":
        blockers.append("quote_quality_sidecar_not_ready")

    summary = {
        "status": "paper_session_evidence_bundle_complete",
        "trade_date": trade_date,
        "portfolio_config": str(portfolio_config),
        "state_root": str(resolved_state_root),
        "quote_events_jsonl": [str(path) for path in quote_event_paths],
        "quote_events_jsonl_count": len(quote_event_paths),
        "existing_quote_events_jsonl_count": len(existing_quote_event_paths),
        "session_quote_field_gate": session_gate,
        "raw_sidecar_quote_gate": raw_sidecar_gate,
        "quote_quality_sidecar_status": quote_quality_status,
        "evidence_status": "quote_backed_session_evidence_complete" if allowed else "quote_backed_session_evidence_blocked",
        "quote_backed_projection_input_allowed": allowed,
        "quote_backed_optimizer_input_allowed": allowed,
        "quote_backed_promotion_input_allowed": allowed,
        "blockers": blockers,
        "session_quote_summary": session_report,
        "raw_sidecar_quote_summary": {
            key: value for key, value in sidecar_summary.items() if key != "rows"
        },
        "quote_quality_sidecar_summary": quote_quality_summary,
        "outputs": outputs,
        "broker_facing": False,
        "paper_runner_state_changed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }
    summary_json = output_dir / "paper_session_evidence_bundle_summary.json"
    summary_md = output_dir / "paper_session_evidence_bundle_summary.md"
    outputs["bundle_summary_json"] = str(summary_json)
    outputs["bundle_summary_markdown"] = str(summary_md)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(summary_md, summary)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    summary = build_paper_session_evidence_bundle(
        portfolio_config=Path(args.portfolio_config),
        trade_date=str(args.trade_date),
        quote_events_jsonl=[Path(value) for value in args.quote_events_jsonl] if args.quote_events_jsonl else None,
        output_dir=Path(args.output_dir),
        state_root=Path(args.state_root) if args.state_root else None,
        sidecar_window_seconds=args.sidecar_window_seconds,
        sidecar_tail_bytes=args.sidecar_tail_bytes,
        underlyings=_csv_set(args.underlyings) or None,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

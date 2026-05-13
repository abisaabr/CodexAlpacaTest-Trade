from __future__ import annotations

import argparse
import json
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _write_markdown(path: Path, summary: dict[str, Any], strategy_table: pd.DataFrame) -> None:
    lines = [
        f"# Multi-Ticker Paper Postmortem - {summary['trade_date']}",
        "",
        f"- Session found: `{summary['session_found']}`",
        f"- Submit paper orders: `{summary.get('submit_paper_orders')}`",
        f"- Starting equity: `{summary.get('starting_equity')}`",
        f"- Ending equity: `{summary.get('ending_equity')}`",
        f"- Net PnL: `{summary.get('net_pnl')}`",
        f"- Strategy-attributed PnL: `{summary.get('completed_trade_net_pnl')}`",
        f"- Unattributed session PnL: `{summary.get('unattributed_session_pnl')}`",
        f"- Accounting status: `{summary.get('postmortem_accounting_status')}`",
        f"- Quote-backed evidence status: `{summary.get('quote_backed_evidence_status')}`",
        f"- Quote-backed optimizer input allowed: `{summary.get('quote_backed_optimizer_input_allowed')}`",
        f"- Completed trades: `{summary.get('completed_trade_count')}`",
        f"- Open trades at report time: `{summary.get('open_trade_count')}`",
        f"- Broker-flat stale session exits: `{summary.get('broker_flat_without_session_exit_count')}`",
        f"- Final session summary found: `{summary.get('session_summary_found')}`",
        f"- Shutdown reconciled: `{summary.get('shutdown_reconciled')}`",
        f"- Guardrail fire count: `{summary.get('guardrail_fire_count')}`",
        f"- Alert count: `{summary.get('alert_count')}`",
        f"- Strategy daily ledger: `{summary.get('strategy_daily_performance_ledger_path')}`",
        f"- Strategy cumulative ledger: `{summary.get('strategy_cumulative_performance_path')}`",
        "",
        "## Strategy Results",
        "",
    ]
    if summary.get("postmortem_accounting_status") != "fully_attributed":
        lines.extend(
            [
                "> Strategy ledgers include only completed trades with locally attributed exits. "
                "Unattributed session PnL requires broker-fill reconciliation before it is assigned to a strategy.",
                "",
            ]
        )
    if not summary.get("quote_backed_optimizer_input_allowed"):
        lines.extend(
            [
                "> Completed trade PnL is not optimizer-ready unless the quote-backed evidence bundle "
                "passes session quote-field coverage and raw OPRA sidecar coverage.",
                "",
            ]
        )
    if strategy_table.empty:
        lines.append("No completed strategy trades were available for this date.")
    else:
        lines.append(strategy_table.to_markdown(index=False))
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _completed_trade_frame(session_payload: dict[str, Any]) -> pd.DataFrame:
    frame = pd.DataFrame(session_payload.get("completed_trades") or [])
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "strategy_name",
                "underlying_symbol",
                "regime",
                "net_pnl",
                "entry_order_id",
                "exit_order_id",
            ]
        )
    for column in ("strategy_name", "underlying_symbol", "regime"):
        if column not in frame.columns:
            frame[column] = "unknown"
    frame["net_pnl"] = pd.to_numeric(frame.get("net_pnl", 0.0), errors="coerce").fillna(0.0)
    return frame


def _daily_strategy_table(completed_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    columns = [
        "trade_date",
        "strategy_name",
        "underlying_symbol",
        "regime",
        "trade_count",
        "win_count",
        "loss_count",
        "flat_count",
        "net_pnl",
        "avg_pnl",
        "win_rate_pct",
    ]
    if completed_df.empty:
        return pd.DataFrame(columns=columns)
    frame = completed_df.copy()
    frame["trade_date"] = trade_date
    grouped = frame.groupby(["trade_date", "strategy_name", "underlying_symbol", "regime"], dropna=False)
    table = grouped["net_pnl"].agg(["count", "sum", "mean"]).reset_index()
    table = table.merge(
        grouped["net_pnl"].apply(lambda series: int((series > 0.0).sum())).reset_index(name="win_count"),
        on=["trade_date", "strategy_name", "underlying_symbol", "regime"],
    )
    table = table.merge(
        grouped["net_pnl"].apply(lambda series: int((series < 0.0).sum())).reset_index(name="loss_count"),
        on=["trade_date", "strategy_name", "underlying_symbol", "regime"],
    )
    table = table.merge(
        grouped["net_pnl"].apply(lambda series: int((series == 0.0).sum())).reset_index(name="flat_count"),
        on=["trade_date", "strategy_name", "underlying_symbol", "regime"],
    )
    table = table.rename(columns={"count": "trade_count", "sum": "net_pnl", "mean": "avg_pnl"})
    table["win_rate_pct"] = (table["win_count"] / table["trade_count"].clip(lower=1) * 100.0).round(2)
    table["net_pnl"] = table["net_pnl"].round(4)
    table["avg_pnl"] = table["avg_pnl"].round(4)
    return table[columns].sort_values(["net_pnl", "trade_count", "strategy_name"], ascending=[False, False, True])


def _broker_flat_without_session_exit_count(run_root: Path, trade_date: str) -> int:
    events_path = run_root / trade_date / "trade_reconciliation_events.json"
    events = _read_json(events_path, [])
    if isinstance(events, dict):
        events = [events]
    if not isinstance(events, list):
        return 0
    return sum(
        1
        for event in events
        if isinstance(event, dict) and str(event.get("status") or "") == "broker_flat_without_session_exit"
    )


def _update_ledgers(run_root: Path, daily_table: pd.DataFrame, trade_date: str) -> dict[str, Any]:
    daily_path = run_root / "strategy_daily_performance_ledger.csv"
    cumulative_path = run_root / "strategy_cumulative_performance.csv"
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    if daily_path.exists():
        existing = pd.read_csv(daily_path)
        existing = existing.loc[existing["trade_date"].astype(str) != trade_date].copy()
        all_daily = pd.concat([existing, daily_table], ignore_index=True)
    else:
        all_daily = daily_table
    all_daily.to_csv(daily_path, index=False)
    if all_daily.empty:
        cumulative = pd.DataFrame(
            columns=[
                "strategy_name",
                "underlying_symbol",
                "regime",
                "first_trade_date",
                "last_trade_date",
                "trade_count",
                "win_count",
                "loss_count",
                "flat_count",
                "net_pnl",
                "avg_pnl",
                "win_rate_pct",
            ]
        )
    else:
        grouped = all_daily.groupby(["strategy_name", "underlying_symbol", "regime"], dropna=False)
        cumulative = grouped.agg(
            first_trade_date=("trade_date", "min"),
            last_trade_date=("trade_date", "max"),
            trade_count=("trade_count", "sum"),
            win_count=("win_count", "sum"),
            loss_count=("loss_count", "sum"),
            flat_count=("flat_count", "sum"),
            net_pnl=("net_pnl", "sum"),
        ).reset_index()
        cumulative["avg_pnl"] = (cumulative["net_pnl"] / cumulative["trade_count"].clip(lower=1)).round(4)
        cumulative["win_rate_pct"] = (
            cumulative["win_count"] / cumulative["trade_count"].clip(lower=1) * 100.0
        ).round(2)
        cumulative["net_pnl"] = cumulative["net_pnl"].round(4)
        cumulative = cumulative.sort_values(["net_pnl", "trade_count", "strategy_name"], ascending=[False, False, True])
    cumulative.to_csv(cumulative_path, index=False)
    return {
        "strategy_daily_performance_ledger_path": str(daily_path),
        "strategy_cumulative_performance_path": str(cumulative_path),
        "strategy_daily_rows_written": int(len(daily_table)),
        "strategy_cumulative_rows": int(len(cumulative)),
    }


def build_postmortem(
    *,
    state_root: Path,
    run_root: Path,
    trade_date: str,
    submit_paper_orders: bool | None = None,
    quote_evidence_json: Path | None = None,
    session_summary_json: Path | None = None,
) -> dict[str, Any]:
    session_path = state_root / f"session_{trade_date}.json"
    session_payload = _read_json(session_path, {})
    completed_df = _completed_trade_frame(session_payload)
    daily_table = _daily_strategy_table(completed_df, trade_date)
    ledger_summary = _update_ledgers(run_root, daily_table, trade_date)
    output_dir = run_root / trade_date
    output_dir.mkdir(parents=True, exist_ok=True)
    strategy_csv = output_dir / f"paper_strategy_postmortem_{trade_date}.csv"
    daily_table.to_csv(strategy_csv, index=False)
    starting_equity = float(session_payload.get("starting_equity") or 0.0)
    ending_equity = float(session_payload.get("virtual_cash") or starting_equity)
    session_net_pnl = ending_equity - starting_equity
    completed_trade_net_pnl = float(completed_df["net_pnl"].sum()) if "net_pnl" in completed_df else 0.0
    unattributed_session_pnl = session_net_pnl - completed_trade_net_pnl
    broker_flat_stale_exit_count = _broker_flat_without_session_exit_count(run_root, trade_date)
    accounting_status = (
        "fully_attributed"
        if abs(unattributed_session_pnl) <= 0.01 and broker_flat_stale_exit_count == 0
        else "needs_broker_fill_reconciliation"
    )
    quote_evidence = _read_json(quote_evidence_json, {}) if quote_evidence_json else {}
    if not isinstance(quote_evidence, dict):
        quote_evidence = {}
    if session_summary_json is None:
        auto_summary = run_root / trade_date / "multi_ticker_portfolio_session_summary.json"
        session_summary_json = auto_summary if auto_summary.exists() else None
    session_summary = _read_json(session_summary_json, {}) if session_summary_json else {}
    if not isinstance(session_summary, dict):
        session_summary = {}
    end_of_day_cleanup = session_summary.get("end_of_day_cleanup")
    if not isinstance(end_of_day_cleanup, dict):
        end_of_day_cleanup = {}
    summary = {
        "trade_date": trade_date,
        "session_found": bool(session_payload),
        "session_path": str(session_path),
        "run_dir": str(output_dir),
        "submit_paper_orders": submit_paper_orders,
        "starting_equity": round(starting_equity, 4),
        "ending_equity": round(ending_equity, 4),
        "net_pnl": round(session_net_pnl, 4),
        "completed_trade_net_pnl": round(completed_trade_net_pnl, 4),
        "unattributed_session_pnl": round(unattributed_session_pnl, 4),
        "broker_flat_without_session_exit_count": int(broker_flat_stale_exit_count),
        "postmortem_accounting_status": accounting_status,
        "quote_evidence_json": str(quote_evidence_json) if quote_evidence_json else None,
        "quote_backed_evidence_status": quote_evidence.get("evidence_status", "not_provided"),
        "quote_backed_projection_input_allowed": bool(
            quote_evidence.get("quote_backed_projection_input_allowed", False)
        ),
        "quote_backed_optimizer_input_allowed": bool(
            quote_evidence.get("quote_backed_optimizer_input_allowed", False)
        ),
        "quote_backed_promotion_input_allowed": bool(
            quote_evidence.get("quote_backed_promotion_input_allowed", False)
        ),
        "quote_backed_evidence_blockers": quote_evidence.get("blockers") or [],
        "session_summary_json": str(session_summary_json) if session_summary_json else None,
        "session_summary_found": bool(session_summary),
        "shutdown_reconciled": session_summary.get("shutdown_reconciled"),
        "end_of_day_cleanup_shutdown_reconciled": end_of_day_cleanup.get("shutdown_reconciled"),
        "end_of_day_cleanup_residual_broker_position_count": end_of_day_cleanup.get(
            "residual_broker_position_count"
        ),
        "end_of_day_cleanup_open_trade_count_after_cleanup": end_of_day_cleanup.get(
            "open_trade_count_after_cleanup"
        ),
        "guardrail_fire_count": session_summary.get("guardrail_fire_count"),
        "guardrail_reason_count": session_summary.get("guardrail_reason_count"),
        "guardrail_manual_review_count": session_summary.get("guardrail_manual_review_count"),
        "guardrail_needs_manual_review": session_summary.get("guardrail_needs_manual_review"),
        "completed_trade_count": int(len(session_payload.get("completed_trades") or [])),
        "open_trade_count": int(len(session_payload.get("open_trades") or [])),
        "alert_count": int(len(session_payload.get("alerts") or [])),
        "strategy_postmortem_csv": str(strategy_csv),
        **ledger_summary,
    }
    _write_json(output_dir / f"paper_trader_postmortem_{trade_date}.json", summary)
    _write_markdown(output_dir / f"paper_trader_postmortem_{trade_date}.md", summary, daily_table)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a daily multi-ticker PAPER trader postmortem and cumulative strategy scoreboards."
    )
    parser.add_argument(
        "--portfolio-config",
        default="config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml",
    )
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--state-root", type=Path, default=None)
    parser.add_argument("--run-root", type=Path, default=None)
    parser.add_argument("--quote-evidence-json", type=Path, default=None)
    parser.add_argument("--session-summary-json", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_portfolio_config(args.portfolio_config)
    state_root = args.state_root or config.execution.state_root
    run_root = args.run_root or config.execution.run_root
    summary = build_postmortem(
        state_root=Path(state_root),
        run_root=Path(run_root),
        trade_date=str(args.trade_date),
        submit_paper_orders=bool(config.execution.submit_paper_orders),
        quote_evidence_json=args.quote_evidence_json,
        session_summary_json=args.session_summary_json,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

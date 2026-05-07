from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_multi_ticker_paper_postmortem import build_postmortem


def test_build_postmortem_writes_daily_and_cumulative_strategy_outputs(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    run_root = tmp_path / "runs"
    state_root.mkdir()
    trade_date = "2026-05-07"
    (state_root / f"session_{trade_date}.json").write_text(
        json.dumps(
            {
                "trade_date": trade_date,
                "starting_equity": 25000.0,
                "virtual_cash": 25125.0,
                "completed_trades": [
                    {
                        "strategy_name": "qqq_greek_delta_call",
                        "underlying_symbol": "QQQ",
                        "regime": "bull",
                        "net_pnl": 200.0,
                    },
                    {
                        "strategy_name": "qqq_greek_delta_call",
                        "underlying_symbol": "QQQ",
                        "regime": "bull",
                        "net_pnl": -75.0,
                    },
                ],
                "open_trades": [],
                "alerts": [],
            }
        ),
        encoding="utf-8",
    )

    summary = build_postmortem(
        state_root=state_root,
        run_root=run_root,
        trade_date=trade_date,
        submit_paper_orders=True,
    )

    daily = pd.read_csv(summary["strategy_daily_performance_ledger_path"])
    cumulative = pd.read_csv(summary["strategy_cumulative_performance_path"])

    assert summary["completed_trade_count"] == 2
    assert summary["net_pnl"] == 125.0
    assert (run_root / trade_date / f"paper_trader_postmortem_{trade_date}.md").exists()
    assert daily.iloc[0]["trade_count"] == 2
    assert daily.iloc[0]["win_rate_pct"] == 50.0
    assert cumulative.iloc[0]["net_pnl"] == 125.0

# Multi-Symbol PAPER Session Postmortem - 2026-05-06

## Session Summary

The local PAPER trader ran with order submission enabled during the 2026-05-06 RTH
session and produced a broker-audited session bundle.

- Portfolio config: `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`
- Branch: `codex/phase2-fill-semantics-20260430`
- Runtime commit at session summary: `eb5b11855d86`
- Submit paper orders: true
- Starting equity: 25000.00
- Ending equity: 25604.48
- Net PnL: 604.48
- Completed trades: 12
- Entry submissions: 21
- Entry fills: 12
- Entry not filled: 3
- Exit fills: 12
- Open reconciled trades at shutdown: 0
- Ending broker position count: 0
- Shutdown reconciled: true

## Strategy PnL Notes

Top positive single-trade contributors:

- SPY bull single-leg repair: +287.79.
- SPY bull single-leg repair: +227.79.
- QQQ bull single-leg repair: +121.89.
- QQQ bull single-leg repair: +121.89.
- QQQ choppy single-leg repair: +58.89.
- QQQ choppy single-leg repair: +52.89.

Largest negative single-trade contributor:

- QQQ bear single-leg repair: -261.11.

## Guardrails

Guardrail scorecard:

- Guardrail fire count: 22.
- Signal filter count: 20.
- Alert guardrail count: 2.
- Session block count: 0.
- Final blocked new entries: false.
- Needs manual review: true.

Guardrail reasons:

- `max_positions_per_regime`: 18 fires across AMZN, AVGO, GOOGL, and TSM.
- `regime_entry_cluster:choppy`: 2 fires on SPY.
- Morning notification delivery failed: 1.
- Midday notification delivery failed: 1.

The trade engine reconciled cleanly. The manual-review items are post-session hardening
targets, not evidence of residual open broker risk.

## Durable Artifacts

Local session bundle:

```text
reports/multi_ticker_portfolio/runs/multi_symbol_governed_realtime_20260506/2026-05-06/
```

GCS mirror:

```text
gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260506/session_2026-05-06/2026-05-06/
```

Key files:

- `multi_ticker_portfolio_session_summary.json`
- `multi_ticker_portfolio_session_summary.md`
- `multi_ticker_portfolio_session_summary_completed_trades.csv`
- `multi_ticker_portfolio_session_summary_broker_order_audit.csv`
- `multi_ticker_portfolio_session_summary_ending_broker_positions.csv`
- `multi_ticker_portfolio_guardrail_scorecard.json`
- `multi_ticker_portfolio_guardrail_scorecard.md`
- `order_journal.json`
- `trade_reconciliation_events.json`

## Next Actions

- Review why the QQQ bear put took the largest loss and compare it against backtest
  loser clusters.
- Review capacity throttling on `max_positions_per_regime`; it filtered many bull
  signals and may need strategy-specific risk budgets rather than a global cap.
- Fix morning/midday notification delivery so operational alerts are reliable.
- Continue the QQQ/SPY/IWM expansion wave separately; it remains research-only and
  does not authorize live-manifest changes.

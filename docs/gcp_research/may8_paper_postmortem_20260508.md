# May 8 PAPER Session Postmortem - 2026-05-08

## Scope

This postmortem reviews the May 8, 2026 PAPER-only multi-symbol trader from local runtime artifacts. No broker-facing process was running at the May 12 follow-up check, all visible GCP VMs were terminated, and no paper config, live manifest, or risk policy was changed during this review.

## Artifacts

- Session state: `D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260508\session_2026-05-08.json`
- Runtime stdout: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260508\paper_trader_stdout.txt`
- Runtime stderr: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260508\paper_trader_stderr.txt`
- Strategy daily ledger: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260508\strategy_daily_performance_ledger.csv`
- Reconciliation CSV: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260508\2026-05-08\multi_ticker_portfolio_session_summary_trade_reconciliation.csv`
- Broker order audit CSV: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260508\2026-05-08\multi_ticker_portfolio_session_summary_broker_order_audit.csv`
- Guardrail scorecard: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260508\2026-05-08\multi_ticker_portfolio_guardrail_scorecard.json`

## Result

- Starting equity: `25000.00`
- Ending equity / virtual cash: `25262.56`
- Realized reconciled net PnL: `262.56`
- Completed reconciled trades: `4`
- Open trades at shutdown: `0`
- Startup preflight: `passed`
- EOD flatten checkpoints completed: `10,2`
- Shutdown reconciliation: `true`
- Ending broker position count: `0`
- stderr length: `0`

## Strategy Ledger

| Symbol | Strategy group | Trades | Win rate | Net PnL |
|---|---:|---:|---:|---:|
| QQQ | bull call single-leg repair `e59a...` | 1 | 100% | `154.89` |
| QQQ | bull call single-leg repair `7f37...` | 1 | 100% | `186.89` |
| AMZN | bull call single-leg repair `4f7c...` | 1 | 0% | `-4.11` |
| GOOGL | bull call single-leg repair `50e...` | 1 | 0% | `-75.11` |

## Signal And Execution Summary

- Signal attempts: `105`
- Eligible signals: `8`
- Skipped signals: `97`
- Completed trades: `4`
- Entry-not-filled attempts: `4`
- Entry submissions: `12`
- Exit submissions: `369`
- Entry fills: `4`
- Exit fills: `4`
- Most common skip reason: `risk_budget_too_small` (`50`)
- Other major skips: `max_positions_per_regime` (`22`), `regime_entry_cluster:bull` (`19`), `duplicate_signal` (`6`)

## Entry Circuit Breaker

The session ended with new entries blocked:

`entry_execution_circuit_breaker: 3 consecutive entry failures (last status not_filled)`

Observed entry-not-filled attempts:

- AMZN bull call single-leg repair at `2026-05-08T09:55:27-04:00`, quantity `2`, expected entry `2.50`, two submissions.
- QQQ choppy Greek call single-leg repair `4a322...` at `2026-05-08T10:39:06-04:00`, quantity `1`, expected entry `3.975`, two submissions.
- QQQ choppy Greek call single-leg repair `421a...` at `2026-05-08T10:40:20-04:00`, quantity `1`, expected entry `3.975`, two submissions.
- XOM bear broken-wing put butterfly `8f709...` at `2026-05-08T11:08:36-04:00`, quantity `3`, expected entry `0.495`, two submissions.

Interpretation: the circuit breaker did its job. The failures cluster in operator/regime-optional strategies, including Greek and multi-leg candidates. Before the next launch, entry retry policy should distinguish expected non-fill due to strict limit pricing from operational failure, and strategy admission should penalize candidates whose live entry attempts repeatedly do not fill.

## GOOGL Exit Issue

GOOGL entered at `2026-05-08T10:10:46-04:00`:

- Symbol: `GOOGL260511C00400000`
- Expected entry: `4.025`
- Actual entry fill: `4.00`
- Exit trigger: `auto_flatten_known_end_of_day_position`
- Actual exit fill: `3.25`
- Exit submissions: `312`
- Realized PnL: `-75.11`

The session emitted `156` GOOGL `exit did not fill` alerts. Broker order audit shows repeated canceled GOOGL sell orders followed by a final filled EOD cleanup sell at `2026-05-08T19:56:09Z`. This is the highest-priority hardening item: repeated exit resubmission should escalate earlier to a stronger exit path or mark the strategy/instrument as non-executable for that session.

## Broker And Guardrail Audit

- Broker order count: `382`
- Broker matched order count: `382`
- Broker unmatched order count: `0`
- Broker multi-leg order count: `2`
- Broker partially filled order count: `0`
- Broker status mismatch count: `374`
- Broker activity count: `8`
- Broker fill activity count: `8`
- Broker activity matched count: `7`
- Broker activity unmatched count: `1`

The unmatched activity is the final GOOGL EOD sell fill. This appears operationally explained by safeguard cleanup rather than an unaccounted open risk, but it should be reconciled to the local trade record in post-session ledgers.

Guardrail scorecard:

- Guardrail firings: `46`
- Guardrail reason count: `6`
- Manual review recommendations: `3`
- Auto-fixed recommendations: `1`
- Needs manual review: `true`

Manual-review categories:

- Entry execution circuit breaker fired `2` times.
- Morning notification delivery failed.
- Midday notification delivery failed.

Auto-fixed category:

- GOOGL required safeguard cleanup at EOD.

## What Worked

- PAPER endpoint/session completed without stderr.
- EOD flatten checkpoints at 10 and 2 minutes before close both fired.
- Shutdown reconciliation ended with zero open trades and zero broker positions.
- QQQ bull call single-leg strategies produced two profitable trades.
- Entry circuit breaker prevented continued order attempts after repeated not-filled entries.

## What Needs Fixing Before Next Launch

- Add a stronger GOOGL-style exit escalation path: after repeated canceled exits, switch to a more aggressive marketable limit or controlled market-exit fallback before EOD.
- Classify broker status mismatches by expected canceled-limit behavior versus real reconciliation risk; the count alone is too noisy.
- Treat unmatched broker activity from safeguard cleanup as matchable to the local trade record so daily strategy attribution remains complete.
- Add live-entry fill-rate penalties to optimizer/admission, especially for regime-optional Greek and multi-leg candidates.
- Review per-regime concentration controls. Bull clustering blocked `19` entries and max-positions-per-regime blocked `22`; that may be desirable risk control, but the optimizer should model it explicitly.
- Fix notification delivery plumbing, but do not treat notification failure as a trading blocker when runtime safety remains intact.

## Promotion And Paper-Runner State

- Eligible for governed promotion review from this postmortem alone: no.
- Paper-runner state changed: no.
- Paper config changed: no.
- Live manifest changed: no.
- Risk policy changed: no.

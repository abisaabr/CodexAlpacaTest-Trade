# May 13 Session Project Hardening Plan

## Current Session Posture

- The stale pre-launch heartbeat was deleted and replaced with `may13-paper-active-session-monitor`.
- Active PAPER trader remains local and broker-facing with `--submit-paper-orders`.
- Active quote capture is no-submit and narrowed to exact runtime-selected OPRA legs.
- Do not pause or restart the PAPER trader during RTH unless there is a safety defect, stale lease, duplicate process, or broker reconciliation failure.

## Data: 5 High-Impact Changes

1. Start each session with exact runtime-leg OPRA capture, not broad chain capture.
2. Build a quote latency and spread audit every 15-30 minutes and at EOD.
3. Persist forced-symbol lineage: strategy -> selected leg -> option symbol -> quote sidecar.
4. Add GCP no-submit latency canary after RTH to compare local versus cloud OPRA stream latency.
5. Store all quote sidecars and latency audits on `D:` and mirror post-session summaries to GCS.

## Backtester: 5 High-Impact Changes

1. Replay completed PAPER trades against same-day OPRA sidecars before accepting strategy economics.
2. Penalize stale quotes by quote-age bucket rather than one static haircut.
3. Model multi-leg fill delay separately from single-leg fill delay.
4. Add side-specific execution prices: buys pay ask-side/slippage, sells receive bid-side/slippage.
5. Split train/test metrics by regime, symbol, family, and quote-quality bucket.

## Promoter: 5 High-Impact Changes

1. Require quote-backed lineage before any new strategy enters the paper runner.
2. Add per-regime review packets, but label governance scope explicitly.
3. Reject candidates with positive backtest PnL but negative quote-backed replay PnL.
4. Track family concentration so `single_leg_repair` and one ticker cannot dominate projected PnL.
5. Promote only candidates that improve a constrained portfolio tier, not just standalone PnL.

## Trader: 5 High-Impact Changes

1. Add built-in runtime-leg quote-capture startup mode so no manual symbol generation is needed.
2. Add an order-entry cooldown after repeated same-family stop-outs in the same symbol/regime.
3. Record entry/exit quote-side evidence next to every broker fill and completed trade.
4. Add live session health JSON that includes process, broker, session, quote-capture latency, and EOD flatten readiness.
5. Make launch controller output files unique per attempt to avoid the May 13 redirection race.

## Session-Time Execution Order

1. Keep the active PAPER trader running unless a safety issue appears.
2. Keep exact runtime-leg quote capture active and audit it periodically.
3. Log material trades, stop-outs, order failures, stale quotes, and broker mismatches.
4. After RTH, build sidecars, postmortem, quote evidence report, and replay diagnostics.
5. Only after postmortem, decide whether to restart tomorrow from local or GCP.

## Restart Criteria

Pause/restart the PAPER trader only if one of these occurs:

- Duplicate broker-facing process.
- Lease/session heartbeat stale.
- Broker positions diverge from session state and cannot auto-reconcile.
- Order submission fails repeatedly and circuit breaker is not protecting entries.
- EOD flatten guard is not armed.
- A code fix is required to prevent unsafe broker behavior.

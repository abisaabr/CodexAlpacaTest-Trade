# May 13 Session Project Hardening Plan

## Current Session Posture

- The stale pre-launch heartbeat was deleted and replaced with `may13-paper-active-session-monitor`.
- Active PAPER trader remains local and broker-facing with `--submit-paper-orders`.
- Active quote capture is no-submit and narrowed to exact runtime-selected OPRA legs.
- Do not pause or restart the PAPER trader during RTH unless there is a safety defect, stale lease, duplicate process, or broker reconciliation failure.

## Completed During May 13 RTH Hardening

- Added built-in runtime-selected leg quote-capture mode to `scripts/run_multi_ticker_realtime_shadow_monitor.py` via `--runtime-selected-leg-symbols` and `--runtime-selected-leg-symbols-only`.
- Added `scripts/build_multi_ticker_session_health_snapshot.py` for broker-safe health snapshots covering process uniqueness, PAPER-only lock, ownership lease, session state, broker orders/positions, quote-capture latency, and EOD flatten readiness.
- Added `scripts/build_paper_trade_quote_sidecar_coverage.py` to fail-closed completed PAPER trade evidence unless every completed leg has session quote fields and raw OPRA websocket sidecar coverage at entry and exit.
- Added `scripts/build_runtime_quote_capture_gap_report.py` to compare current runtime-selected legs, session trade legs, OPRA subscription plans, and observed quote sidecars so capture drift is visible during RTH.
- Validated the new runtime-leg shadow mode in plan-only mode. It selected `55` unique option symbols from `415` runtime-selected legs without opening a websocket or touching orders.
- Logged the early QQQ quote-sidecar gap: the first two completed stop-outs cannot be used for quote-backed optimizer evidence because exact OPRA capture started after the entry/exit timestamps.
- Added neighbor-prioritized OPRA subscription buffering, runtime-selected leg lineage files, dynamic no-submit runtime-leg refresh, and per-line JSONL flushing for quote evidence.
- Patched multi-leg cleanup fallback exits to persist per-leg exit quote fields when a normal broker exit is not filled and the cleanup path completes the trade.
- Added explicit `quote_backed_*_input_allowed` gates to session quote-field and raw OPRA sidecar coverage reports so incomplete evidence cannot be accidentally consumed by projection, optimizer, or promotion steps.

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

## Multi-Agent Multi-Phase Execution Plan

### Phase 1: RTH Safety And Evidence Capture

- Agent A: Monitor PAPER-only process count, ownership lease, session freshness, broker orders/positions, and EOD flatten readiness with the health snapshot tool.
- Agent B: Monitor exact runtime-leg quote-capture latency and spread quality; do not launch additional Alpaca websocket streams during RTH because the account has already hit websocket connection limits.
- Agent C: Track material trade events and run sidecar coverage audits on completed trades. Completed trades without raw OPRA sidecar coverage are excluded from projection/promotion input.

### Phase 2: Data And Backtester Hardening

- Build post-session quote sidecars from the exact runtime-leg capture and apply them to completed PAPER trade economics.
- Repair unmatched strategy replay lineage before any optimizer run; fail closed if a paper strategy cannot map to replay evidence.
- Add quote-age, spread, and fill-probability stress buckets to candidate economics before train/test comparison.
- Use GCP only for offline replay/backtest shards during RTH; defer any GCP websocket latency canary until after the local stream is stopped or websocket capacity is confirmed.

### Phase 3: Promoter And Optimizer Hardening

- Require quote-backed replay status before adding any new strategy to a paper manifest.
- Produce portfolio tiers: unconstrained, current-risk, strict-institutional, drawdown-minimized, and `$200/day target relaxed-but-controlled`.
- Enforce diversification constraints across symbols, regimes, and families so one ticker or one family cannot dominate projected PnL.
- Compare all new candidates against `tt_top2_bull_choppy_up` and the current paper-runner risk profile before considering paper activation.

### Phase 4: Trader Hardening

- Use `--runtime-selected-leg-symbols-only` at launch so exact OPRA capture starts before order submission.
- Add launch-controller unique output paths to avoid the May 13 preflight redirection race.
- Add cooldown logic for repeated same-symbol/same-family stop-outs after postmortem confirms this is not expected behavior.
- Persist every order decision with selected strategy, selected legs, quote time, spread, freshness, fill attempt, and broker order id.
- Tune runtime-leg refresh after RTH. The current RTH-safe setting is a delayed no-submit refresh, not aggressive per-minute websocket mutation.

### Phase 5: GCP Parallel Work

- During RTH: run only broker-free offline replay/backtest workers on GCP.
- After RTH: run one local-vs-GCP no-submit latency canary at a time with exact runtime legs only; no stock quote flood, no option trades, and no trade-update stream.
- Use `-PrepareOnly` and `scripts/audit_gcp_paper_runtime_safety.py` before launching any GCP worker that touches market data or strategy configs.
- Mirror durable artifacts to `gs://codexalpaca-control-us/` and keep raw large sidecars on `D:` unless needed for GCS replay.

## Restart Criteria

Pause/restart the PAPER trader only if one of these occurs:

- Duplicate broker-facing process.
- Lease/session heartbeat stale.
- Broker positions diverge from session state and cannot auto-reconcile.
- Order submission fails repeatedly and circuit breaker is not protecting entries.
- EOD flatten guard is not armed.
- A code fix is required to prevent unsafe broker behavior.

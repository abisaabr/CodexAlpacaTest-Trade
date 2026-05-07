# Paper Runtime Issue Log - 2026-05-07

## Scope

Runtime issue log for the May 7, 2026 RTH multi-symbol governed PAPER session.
This file is for post-session repair work. It does not authorize live trading,
live-manifest changes, or risk-policy changes.

## Active Paper Runtime Snapshot

Checked at approximately `2026-05-07T15:59-04:00`.

- Runtime location: local Windows process.
- Active process count for order-submitting paper trader: `1`.
- Active PID at check: `60160`.
- Command: `scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml --submit-paper-orders`.
- Config: `config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`.
- Manifest: `config\promotion_manifests\multi_symbol_governed_validation_20260507.yaml`.
- Mode observed in logs: Alpaca PAPER.
- Dry run observed in logs: `false`.
- Session state: `D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260507\session_2026-05-07.json`.
- Latest session-state update at check: `2026-05-07T15:59:14-04:00`.
- Broker position count in latest startup/preflight section before the check: `0`.
- Open order count in latest startup/preflight section before the check: `0`.

## Issues

### 1. Transient Alpaca PAPER DNS / connection failure during order-status polling

- First observed in stderr tail: approximately `2026-05-07T15:58-04:00`.
- Failure class: `requests.exceptions.ConnectionError` caused by
  `NameResolutionError`.
- Host: `paper-api.alpaca.markets`.
- Runtime path involved: `alpaca_lab\brokers\alpaca.py` request retry path.
- Symptom: tenacity exhausted retries while polling an Alpaca PAPER order-status URL.
- Safety status after observation:
  - The paper trader process remained alive.
  - Session state continued updating after the traceback.
  - Latest stdout after the traceback showed successful Alpaca PAPER responses.
  - No duplicate broker-facing process was observed.

#### Post-session diagnosis target

Treat this as a resilience/logging hardening item unless it repeats or kills the
process. The broker gateway already retries `requests.RequestException`, but the
runner should make retry exhaustion observable as a structured runtime warning
with order id, strategy id, request path, retry count, and recovery status. Avoid
logging large raw tracebacks during normal recoverable network turbulence.

Recommended patch queue:

- Add structured broker request failure audit entries for retry-exhausted network
  errors.
- Keep full exception details in debug logs, but write a concise session alert for
  PAPER runtime monitoring.
- Add a runtime health counter for Alpaca DNS/connectivity failures per session.
- Escalate only if failures cluster, block exits, or stop session-state updates.

### 2. EOD local-session exit attribution gap after broker became flat

- First confirmed after the RTH session, approximately `2026-05-07T16:02-04:00`.
- Active trader process status after the check: no
  `run_multi_ticker_portfolio_paper_trader.py` process remained.
- Broker PAPER status after the check: `positions_count=0`, `open_orders_count=0`.
- Initial EOD guard reproduction failed on a stale local SPY open trade because the
  local session attempted `sell_to_close` after Alpaca was already flat. Alpaca
  rejected that as a `422` position-intent mismatch because the inferred intent
  would have been `sell_to_open`.
- Broker fill activities later confirmed late PAPER exits near the close that were
  not attributed into `session.completed_trades` before the process stopped.

#### Repair applied

- The trader now checks authoritative broker positions before forced flattening
  stale open trades.
- If all legs for a local open trade are already flat at the broker, the session
  removes the stale open trade and emits a structured
  `broker_flat_without_session_exit` reconciliation event instead of submitting a
  bad `sell_to_close`.
- The EOD close guard rerun reconciled cleanly:
  - `open_trade_count=0`
  - `residual_broker_positions=[]`
  - `forced_exit_skipped_broker_flat_count=5`

#### Remaining postmortem/audit item

The daily strategy ledger must not assign broker-flat stale exits to strategies
without explicit broker-fill reconciliation. The postmortem now separates
session-level accounting from strategy-attributed PnL:

- Session net PnL: `-1610.43`
- Completed strategy-attributed PnL: `467.92`
- Unattributed session PnL: `-2078.35`
- Broker-flat stale session exits: `5`
- Accounting status: `needs_broker_fill_reconciliation`

Output paths:

- `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260507\2026-05-07\eod_close_guard_report.json`
- `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260507\2026-05-07\paper_trader_postmortem_2026-05-07.json`
- `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260507\2026-05-07\paper_trader_postmortem_2026-05-07.md`

Next hardening target: reconstruct broker-fill-attributed realized PnL from
Alpaca account activities and local `attempt_id`/order evidence before updating
the strategy scoreboards for trades that exited outside the local terminal-order
path.

## Research Lane Status At Check

The research-only microstructure event replay wave
`microstructure_event_replay_v2_executable_20260507T1935Z` was active in GCP.
Sampled workers were compute-bound with four child Python processes near 100% CPU,
so the tranche was not stalled. No research wave was broker-facing and no PAPER
orders were submitted by the research workers.

Final status for this tranche: all 8 workers completed, aggregate artifacts were
mirrored to GCS, and terminated worker VMs were deleted. Strict aggregate result:
`2048` grid profiles, `0` eligible microstructure-review candidates, decision
`research_only_blocked`. Dominant blockers were non-positive total/average PnL
across every profile, plus fill-coverage failures on most profiles.

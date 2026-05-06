# Paper Runtime Issue Log - 2026-05-06

## Scope

Runtime issue log for the May 6, 2026 RTH multi-symbol governed paper session.
This file is for post-session repair work. It does not authorize live trading,
live-manifest changes, or risk-policy changes.

## Active Paper Runtime

- Runtime location at first issue check: local Windows process.
- PID: `37892`.
- Command: `scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --submit-paper-orders`.
- Config: `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`.
- Mode: Alpaca PAPER, order submission enabled.
- Startup preflight: passed.

## Ticker And Strategy Load Check

Checked at approximately `2026-05-06T10:26-04:00`.

- Active process count for paper order submission: `1`.
- Configured symbols: `QQQ`, `SPY`, `AMD`, `AMZN`, `MSFT`, `TSLA`, `AVGO`, `GOOGL`, `TSM`.
- Symbols missing strategies: none.
- Loaded strategy count: `120`.
- Strategy count by symbol:
  - `AMD`: `15`
  - `AMZN`: `5`
  - `AVGO`: `20`
  - `GOOGL`: `20`
  - `MSFT`: `20`
  - `QQQ`: `10`
  - `SPY`: `8`
  - `TSLA`: `2`
  - `TSM`: `20`
- Strategy count by regime:
  - `bull`: `14`
  - `bear`: `49`
  - `choppy`: `57`
- Strategy count by family:
  - `Single-leg long call`: `70`
  - `Single-leg long put`: `49`
  - `debit_call_vertical`: `1`
- Current runtime symbol regime coverage: all 9 configured symbols have a latest regime in session state.

Conclusion: ticker and strategy loading is correct. The issue observed so far is not
a missing-symbol or missing-strategy problem; it is an execution close-order handling
problem for a QQQ hard-exit.

## Issues

### 1. QQQ hard-exit limit order did not fill repeatedly

- First observed: approximately `2026-05-06T10:10:48-04:00`.
- Latest checked: approximately `2026-05-06T10:22:30-04:00`.
- Strategy:
  `qqq__governed__bear__put__single_leg_repair__bear_confirmed_break_35__055209d25ec8fb__qqq_regime_rescue__20260506`.
- Position: long `1` `QQQ260507P00689000`.
- Entry fill: `3.54`.
- Hard-exit mode: `minutes_after_entry`.
- Hard-exit setting: `40`.
- Open broker close order:
  - order id: `9d58ac0e-0b5a-4f66-b280-4d1e5f554b33`
  - client order id: `qqq__governe-0f27a09f3f31c59e`
  - side: `sell`
  - qty: `1`
  - order type: `limit`
  - limit price: `3.55`
  - filled qty: `0`
  - status at check: `new`
- Broker position mark at check: approximately `3.29`.
- Runtime symptom: repeated alerts: `exit did not fill`.
- Follow-up check at approximately `2026-05-06T10:26-04:00`:
  - new open close order id: `669ca16f-ae49-4f2d-8f3e-76859b93321a`
  - limit price remained `3.55`
  - broker position mark had moved to approximately `2.83`
  - unrealized PnL on the QQQ put was approximately `-$71`
- Follow-up check at approximately `2026-05-06T10:57-04:00`:
  - new open close order id: `ca965ffa-ca0a-4f00-a950-1f9003b56d47`
  - limit price remained `3.55`
  - broker position mark had moved to approximately `2.10`
  - unrealized PnL on the QQQ put was approximately `-$144`
  - runtime continued to cancel and resubmit sell-to-close limits without crossing the market.

#### Post-session diagnosis target

The runner appears to submit a fixed limit close that can remain above the current
market after the exit condition is reached. For hard-exit and stop-loss exits,
the close path should probably support cancel/replace with a fresh OPRA quote,
or a bounded marketable-limit fallback before the end-of-day market-exit fallback.

Do not patch this mid-session without a deliberate paper-only intervention plan.

## Post-Session Hardening Queue

- Add close-order refresh logic for stale unfilled exits.
- Distinguish profit-target exits from hard-exit/stop-loss exits in limit-pricing aggressiveness.
- Add runtime telemetry for quote-at-close-order, order age, mark-vs-limit gap, and cancel/replace count.
- Compare polling close timestamps against realtime-shadow OPRA quote timestamps.
- Consider moving order-submitting runtime to GCP only after proving single-owner lease and log mirroring.

### 2. Broker/session position reconciliation mismatch

- First observed: approximately `2026-05-06T10:57-04:00`.
- Session state reported `4` open trades and `1` completed trade.
- Broker reported only `2` option positions:
  - long `2` `QQQ260507C00690000`
  - long `1` `QQQ260507P00689000`
- Session still listed an open SPY call trade:
  `spy__governed__bull__call__single_leg_repair__qqq_spy_ff_spy_20260505__cbf97bbd739774__qqq_spy_ff_spy_202__20260506`.
- Broker had no SPY option position at the same check.

#### Post-session diagnosis target

Determine whether the second SPY call position was closed by broker state but not
recorded as completed in the session, or whether the broker position query missed
an expected open position. This needs reconciliation hardening before relying on
session state alone for open-risk accounting.

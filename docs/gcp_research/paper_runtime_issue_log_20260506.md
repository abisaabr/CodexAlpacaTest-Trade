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

### 3. GOOGL hard-exit limit order did not fill repeatedly

- First observed in alerts: approximately `2026-05-06T11:16:25-04:00`.
- Latest checked: approximately `2026-05-06T11:26:15-04:00`.
- Strategy:
  `googl__governed__bull__call__single_leg_repair__strong_trend_patient__50e98a5d4eff13__googl_regime_rescu__20260506`.
- Position: long `1` `GOOGL260508C00397500`.
- Entry fill: `4.50`.
- Broker close order at check:
  - order id: `8b2a898e-fd54-4fa3-8894-b750d3439e6d`
  - client order id: `googl__gover-d350d48bef5d784c`
  - side: `sell`
  - qty: `1`
  - order type: `limit`
  - limit price: `4.30`
  - filled qty: `0`
  - status at check: `new`
- Broker position mark at check: approximately `3.20`.
- Runtime symptom: repeated alerts: `exit did not fill`.

#### Post-session diagnosis target

This matches the QQQ hard-exit pattern: a non-marketable sell-to-close limit can
remain above the option market after the exit condition is reached. The close path
needs quote-aware refresh and bounded marketable-limit handling for hard exits and
stop losses.

### 4. Runtime open-trade accounting exceeds broker position footprint

- Latest checked: approximately `2026-05-06T11:26:15-04:00`.
- Session state reported `4` open trades:
  - `1` QQQ bear put on `QQQ260507P00689000`
  - `2` QQQ bull call strategies on `QQQ260507C00690000`
  - `1` GOOGL bull call on `GOOGL260508C00397500`
- Broker reported `3` option positions:
  - long `1` `QQQ260507P00689000`
  - long `2` `QQQ260507C00690000`
  - long `1` `GOOGL260508C00397500`
- The numeric broker position footprint is consistent with three contracts but
  not with four independently risk-accounted open strategy trades.
- The two QQQ bull strategy entries share the same contract and quantity footprint.

#### Post-session diagnosis target

The paper runner needs explicit aggregation-aware risk accounting for multiple
strategies sharing the same option contract. Session state should distinguish
strategy-level intents from broker-level net positions before computing available
risk, exits, and close-order quantities.

### 5. QQQ hard-exit stale close continued through midday

- Latest checked: approximately `2026-05-06T12:16:25-04:00`.
- Session state reported `2` open trades and `4` completed trades.
- Broker reported `2` option positions:
  - long `1` `GOOGL260508C00397500`
  - long `1` `QQQ260507P00689000`
- Broker had `1` open sell-to-close order:
  - symbol: `QQQ260507P00689000`
  - side: `sell`
  - qty: `1`
  - limit price: `3.55`
  - filled qty: `0`
  - status: `new`
- Broker mark on the QQQ put was approximately `1.65`.
- Runtime continued to emit `exit did not fill` warnings for the same QQQ hard-exit
  strategy.

#### Post-session diagnosis target

Prioritize a paper-only close-order refresh patch before relying on this runner
for unattended intraday exits. The current behavior can hold a losing option
position far past a hard-exit condition when the stale sell limit is no longer
marketable.

### 6. QQQ close retry loop reuses stale limit after cancel/replace

- Latest checked: approximately `2026-05-06T12:47:00-04:00`.
- Broker PAPER query at check:
  - open orders: `0`
  - positions: `3`
  - remaining QQQ position: long `1` `QQQ260507P00689000`
  - QQQ current price: approximately `1.98`
  - QQQ unrealized PnL: approximately `-156`
- Runtime order journal showed repeated QQQ sell-to-close attempts at the same
  stale limit price `3.55`, followed by terminal status `new`, cancel, and a
  replacement submission at the same stale limit.
- Runtime reconciliation expected exit fill moved with the market, for example
  approximately `1.71` and `1.835`, but the submitted broker limit remained
  `3.55`.

#### Post-session diagnosis target

The close path is refreshing the retry lifecycle but not refreshing the actual
exit limit from current option quotes. The fix should separate retry scheduling
from quote-derived close pricing and should make hard exits use a bounded
marketable limit when the configured order has become stale.

Latest follow-up at approximately `2026-05-06T12:54:30-04:00` still showed the
same pattern: broker PAPER had one open QQQ sell-to-close limit for
`QQQ260507P00689000`, quantity `1`, limit `3.55`, filled quantity `0`, while the
position current price was approximately `2.03`.

Latest follow-up at approximately `2026-05-06T13:34:00-04:00` still showed the
same pattern: broker PAPER had one open QQQ sell-to-close limit for
`QQQ260507P00689000`, quantity `1`, limit `3.55`, filled quantity `0`, while the
position current price was approximately `1.52` and unrealized PnL was
approximately `-$202`.

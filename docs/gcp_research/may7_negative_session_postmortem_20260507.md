# May 7 Negative PAPER Session Postmortem

Date: 2026-05-07

Scope: PAPER-only multi-symbol session using `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`. This postmortem does not authorize live trading, live-manifest changes, or risk-policy changes.

## Executive Result

The broker PAPER account finished flat, but the session had a negative account-level day and incomplete strategy attribution near EOD.

- Starting equity: `25000.00`
- Ending equity: `23389.57`
- Session net PnL: `-1610.43`
- Completed strategy-attributed PnL: `467.92`
- Unattributed session PnL: `-2078.35`
- Completed local trades: `9`
- Local open trades after EOD repair: `0`
- Broker PAPER positions after repair: `0`
- Broker PAPER open orders after repair: `0`
- Broker-flat stale local exits reconciled: `5`
- Accounting status: `needs_broker_fill_reconciliation`

## What Happened

The session-level equity dropped by `-1610.43`, but the local strategy ledger only captured `+467.92` of completed trade PnL. Late PAPER exits near the close left stale local open-trade records after the broker was already flat. A follow-up EOD guard run previously attempted `sell_to_close` on a flat broker position, which Alpaca rejected because the inferred position intent would have been inconsistent with a close.

The runtime was patched to query broker positions before forced EOD exits. If all legs for a local open trade are already flat at the broker, the trader now records `broker_flat_without_session_exit` and removes the stale local trade instead of sending a bad close order.

## Broker Fill Reconciliation Snapshot

Broker activity reconciliation artifacts:

- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_fill_reconciliation_summary.json`
- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_fills_20260507.csv`
- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_symbol_cash_summary_20260507.csv`

Broker fill count: `30`

Direct broker-fill strategy join:

- Matched fills by `order_id`: `22/30`
- Matched fill rate: `0.733333`
- Direct matched signed cash ex-fees: `-1615.00`
- Unmatched signed cash ex-fees: `984.00`

Inferred stale-exit attribution:

- Inferred allocations by symbol balance: `5`
- Inferred allocated signed cash ex-fees: `978.00`
- Still-unmatched fills: `4`
- Still-unmatched signed cash ex-fees: `6.00`
- Inference type: `inferred_symbol_balance`, not authoritative broker identity

Additional reconciliation artifacts:

- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_fill_strategy_reconciliation_summary_2026-05-07.json`
- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_fill_strategy_matches_2026-05-07.csv`
- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_strategy_cash_summary_2026-05-07.csv`
- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_fill_inferred_allocations_2026-05-07.csv`
- `reports/gcp_research/may7_negative_session_postmortem_20260507/broker_strategy_cash_summary_inferred_2026-05-07.csv`

Largest broker symbol cash contributors, excluding fees:

- `SPY260508C00735000`: `-723.00`
- `QQQ260508C00697000`: `-206.00`
- `IWM260508P00286000`: `-153.00`
- `AMZN260508C00272500`: `-136.00`
- `QQQ260508C00696000`: `+350.00`
- `IWM260508P00288000`: `+159.00`

## Strategy Attribution Caveat

Do not use the local strategy ledger alone to promote, demote, or kill strategies from this session. The strategy-attributed ledger is incomplete until broker-fill-level exits are joined back to strategy IDs and attempt IDs.

Known stale-exit strategy IDs from the broker-flat reconciliation:

- `spy__governed__bull__call__single_leg_repair__qqq_spy_ff_spy_20260505__4f33c5d56206d2__profile_qqq-spy-ff-spy-c037-040-spy-e10-x60-entry-liquidity-first-research-only`
- `spy__governed__bull__call__single_leg_repair__qqq_spy_ff_spy_20260505__cbf97bbd739774__profile_qqq-spy-ff-spy-c037-040-spy-e10-x60-entry-liquidity-first-research-only`
- `amzn__governed__bull__call__single_leg_repair__midday_continuation__a09e811fc20989__profile_amzn-bull-call-midday-continuation-amzn-e10-x60-entry-liquidity-first-research-only`
- `qqq__governed__choppy__call__single_leg_repair__iwm_choppy_midday_bands__7d707d30b09023__profile_iwm-choppy-midday-bands-qqq-e30-x60-entry-liquidity-first-research-only`
- `msft__governed__bear__put__single_leg_repair__bear_late_continuation_2__322b803bb266b9__profile_msft-bear-put-late-continuation-2-msft-e10-x60-entry-liquidity-first-research-only`

## Repairs Already Applied

- EOD flatten now checks authoritative broker positions before sending close orders for stale local trades.
- Broker-flat stale exits are reconciled without submitting invalid `sell_to_close` orders.
- Postmortem accounting now separates session net PnL from strategy-attributed PnL and flags `needs_broker_fill_reconciliation`.
- Microstructure shadow telemetry was added so websocket strategy candidates can be replayed against observed spread, quote age, and short-horizon move behavior.
- `scripts/build_broker_fill_strategy_reconciliation.py` now joins broker fills to local strategy identity by `order_id` and emits a separate inferred stale-exit view by symbol balance.

## Required Next Fix Before Trusting Strategy Scoreboards

The broker-fill joiner exists, but the daily strategy scoreboard should not automatically use inferred stale-exit rows until they are reviewed. The next production hardening step is to integrate direct `order_id` matches into the official daily scoreboard and keep inferred rows in an audit-only section unless an order-level or client-order-id-level match is recovered.

Until direct broker-fill scoreboard integration is complete, the May 7 session is valid for paper-runtime hardening and broker/account-level risk analysis, but not valid as clean per-strategy PnL evidence.

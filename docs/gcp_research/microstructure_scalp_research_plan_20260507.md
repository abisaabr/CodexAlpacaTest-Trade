# Microstructure Scalping Research Plan

Updated: 2026-05-07 13:15 ET

## Decision

Sub-minute option scalping is worth researching, but it should be treated as a new research-only microstructure lane. It should not be added to the active PAPER trader until we can replay it with tick/quote-level assumptions and then shadow it live without orders.

The target hypothesis is not "make 1% 100 times a day" as a raw PnL slogan. The realistic target is:

- very short holding periods, typically seconds to a few minutes;
- strict liquid-contract filters;
- entry only when spread, quote size, quote age, and underlying momentum agree;
- exit on fast profit capture, fast adverse quote movement, or time decay;
- pyramiding only after realized open PnL pays for the added risk.

## Alpaca Capability Constraints

Official Alpaca docs support a lower-latency path, but with constraints that matter for strategy design:

- Real-time options data is available through websocket option streams with trade and quote channels, and the option stream is intended to be more accurate and performant than polling latest endpoints for current pricing: https://docs.alpaca.markets/docs/real-time-option-data
- Option websocket messages include nanosecond timestamp fields for trades and quotes, which is sufficient for a microstructure event log if we persist the stream correctly.
- Option websocket payloads require MsgPack, and wildcard subscription is not allowed for option quotes. We must subscribe only to the selected contract universe.
- Alpaca market data plans distinguish indicative/free data from OPRA/SIP coverage. For a production-like paper trader, the correct path is SIP stocks plus OPRA options where the account plan supports it: https://docs.alpaca.markets/v1.3/docs/about-market-data-api
- The latest option quotes REST endpoint supports batches of up to 100 symbols and returns bid/ask prices, but REST polling is not the right primary mechanism for sub-minute scalping: https://docs.alpaca.markets/reference/optionlatestquotes
- Multi-leg orders can be submitted with `order_class=mleg`, which reduces partial-fill risk for verticals/butterflies/condors, but micro scalping should start with single-leg or simple verticals before complex structures: https://docs.alpaca.markets/docs/options-level-3-trading
- Options trading supports day time-in-force and market/limit order types, but not extended hours, fractional, or notional options orders: https://docs.alpaca.markets/docs/options-trading-overview

## Current Repo Gap

The current May 7 PAPER trader is not a sub-minute scalper:

- `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml` uses `poll_interval_seconds: 20`.
- It uses `order_status_poll_seconds: 10` and `order_fill_timeout_seconds: 45`.
- Runtime pricing uses snapshots/latest data through `alpaca_lab/multi_ticker_portfolio/trader.py`, not an event-driven websocket quote loop.
- `scripts/run_option_aware_research_backtest.py` can load option trades and now has `paper_snapshot_greeks` runtime parity, but promotion replay remains minute-bar centric for fills and exits.
- There is no governed pyramiding policy in the strategy/runtime config today.

This means a 15- to 45-second scalp cannot be fairly promoted by the current minute-bar backtester. It would be overfit or mismeasured unless we add a microstructure replay contract.

## May 7 Shadow Bootstrap Check

I ran the existing no-submit realtime shadow monitor in plan-only mode against the active May 7 armed portfolio. This did not open websocket streams and did not submit orders.

Command:

```powershell
python scripts\run_multi_ticker_realtime_shadow_monitor.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml `
  --output-dir reports\gcp_research\microstructure_shadow_plan_20260507 `
  --max-option-symbols 900 `
  --include-option-trades
```

Result:

- Status: `plan_only`
- Underlyings: `10`
- Stock feed: `sip`
- Option feed: `opra`
- Option subscription universe: `364` contracts
- Option symbol cap used for plan: `900`
- Local output: `reports\gcp_research\microstructure_shadow_plan_20260507\`
- GCS mirror: `gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_plan_20260507/`

Interpretation: the selected-contract universe for the currently armed 10-ticker portfolio is small enough for a focused OPRA quote/trade shadow run. The limiting issue is not subscription count; it is runtime architecture, replay accuracy, and whether the active account/SDK permits an additional stream connection without starving the order-submitting paper trader.

## Research Lane

### Phase 0: Live Shadow Recorder

Build a no-order recorder for QQQ/SPY/IWM first, then extend to active tickers:

- subscribe to SIP stock trades/quotes/bars for the underlying symbols;
- subscribe to OPRA option quotes/trades only for the selected next-expiry and same-day liquid contracts;
- persist raw messages to GCS by `run_id/date/symbol/contract/channel`;
- emit per-contract quote age, spread, size, trade frequency, and quote-update frequency;
- record candidate strategy decisions as "would_enter", "would_add", and "would_exit" without orders.

This should run alongside the paper trader only if websocket connection limits permit it. If not, it should run after RTH or on a separate allowed stream connection, not by starving the active trader.

### Phase 1: Tick/Quote Replay

Add a separate replay path that does not pretend minute bars are tick data:

- input: historical option trades and, if available, historical quotes;
- clock: event-time replay by option quote/trade timestamp;
- entry model: buy at ask or midpoint-plus-slippage only when ask size and spread pass;
- exit model: sell at bid or midpoint-minus-slippage only when bid size and spread pass;
- latency model: configurable decision latency, submit latency, and fill wait;
- fees: full Alpaca option fee model already used by the runner;
- fill coverage: successful entry and exit under quote/liquidity constraints per intended micro signal.

Required metrics:

- gross edge per trade;
- net edge after spread, slippage, and fees;
- median/95th percentile hold seconds;
- max adverse excursion before exit;
- quote age at entry and exit;
- spread paid as percent of target profit;
- cancel/replace count;
- stale-quote reject count;
- fill coverage under sub-minute semantics.

### Phase 2: Candidate Families

Start narrow. Do not run the full repo blindly.

- Single-leg momentum snap: ATM/one-step ITM call or put after underlying impulse plus tightening spread.
- Single-leg mean-reversion fade: ATM option fade after underlying stretch with immediate quote reversal.
- Gamma scalp with hard time stop: near-ATM 0-1 DTE option when underlying move exceeds short-window realized-vol threshold.
- Vertical micro-spread: debit vertical only when both legs quote tightly and combined spread is small enough.
- Opening-drive continuation: first 15-45 minutes only; reject first few minutes if spreads are unstable.
- VWAP reclaim/reject: enter only when underlying crosses/rejects VWAP and option quote confirms.

Avoid initially:

- four-leg structures;
- illiquid single-name weeklies;
- naked short option legs;
- strategies requiring wildcard option quote subscriptions;
- strategies whose expected profit is smaller than one round-trip spread plus fees.

### Phase 3: Pyramiding Policy

Pyramiding should be a governed risk module, not a simple "add when green" toggle.

Initial rule proposal:

- no pyramid until initial leg has unrealized PnL greater than 1.5x estimated round-trip spread plus fees;
- maximum 2 adds per strategy instance;
- each add must be smaller than or equal to the prior tranche;
- no add if spread widens above the entry threshold or quote age exceeds the freshness gate;
- no add within 30 seconds of the previous add;
- no add after strategy-specific cutoff minute;
- realized daily loss gate disables adds before it disables base entries;
- EOD flatten logic must flatten all tranches as one strategy group.

Promotion review should report base-entry PnL and pyramided PnL separately so pyramiding cannot hide a weak base signal.

## Promotion Gates For Micro Strategies

Keep the existing institutional gates and add micro-specific gates:

- `fill_coverage >= 0.90`;
- option trade count and test trade count above policy;
- positive full-period and out-of-sample net PnL after spreads, slippage, and fees;
- median quote age below threshold;
- p95 quote age below threshold;
- median spread below threshold as percent of option mark;
- estimated spread cost less than 35% of target profit;
- no severe loser cluster;
- no runtime compatibility blocker;
- no broker/paper order mismatch in shadow or paper validation.

## Implementation Order

1. Keep the active May 7 PAPER trader unchanged.
2. Finish current GCP rt2 research workers and aggregate them under the existing promotion packet process.
3. Add a no-order websocket shadow recorder for QQQ/SPY/IWM selected contracts.
4. Add a tick/quote replay script that consumes shadow logs plus historical option trades/quotes where available.
5. Run QQQ/SPY/IWM microstructure smoke tests with no paper activation.
6. Only if replay and shadow both agree, generate a separate `microstructure_promotion_review_packet`.
7. Add PAPER submission only after the packet clears and the runtime can enforce strategy-specific risk, strict spread/freshness gates, and grouped EOD flatten for tranches.

## Current Recommendation

Research it, but do not route it through the current 20-second polling runner as-is. The first production-grade improvement is a websocket-backed shadow recorder and a tick/quote replay lane. Once those exist, micro scalps and pyramiding can be tested without confusing minute-bar edge with real executable edge.

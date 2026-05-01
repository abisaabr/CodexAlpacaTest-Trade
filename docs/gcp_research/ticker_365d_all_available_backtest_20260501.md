# All-Available Ticker 365d Backtest Wave - 2026-05-01

## Purpose

Run a clean, aggressively sharded 365-day option-aware research backtest for every currently available ticker dataset. This wave is separate from the slower serial overnight workers and is intended to produce an apples-to-apples portfolio report, promotion-review packet, and `$25,000` to `$300,000` growth projection.

## Scope

- Symbols: `AAPL AMD AMZN INTC IWM META MSFT NVDA SPY TSLA AVGO GOOGL MU NFLX ORCL PLTR QQQ TSM XLE XOM`
- Dataset stage: `365d_5x5`
- Selectors per ticker: `nearest_contract`, `entry_liquidity_first_research_only`
- Sharding: one VM per ticker, two selector processes per VM
- Expected candidate-summary files: `40`

## GCS Prefix

`gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/`

## Outputs

- Worker outputs: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/workers/`
- Aggregate portfolio report: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/portfolio_report/`
- Aggregate promotion packet: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/promotion_packet/`
- Aggregate growth projection: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/growth_projection/`
- Aggregate status: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/status/ticker_365d_aggregate_status.json`

## Hard Rules

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Promotion means governed validation review only.

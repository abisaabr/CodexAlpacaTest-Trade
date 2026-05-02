# All-Available Ticker 365d Backtest Wave - 2026-05-01

## Purpose

Run a clean, aggressively sharded 365-day option-aware research backtest for every currently available ticker dataset. This wave is separate from the slower serial overnight workers and is intended to produce an apples-to-apples portfolio report, promotion-review packet, and `$25,000` to `$300,000` growth projection.

## Scope

- Symbols: `AAPL AMD AMZN INTC IWM META MSFT NVDA SPY TSLA AVGO GOOGL MU NFLX ORCL PLTR QQQ TSM XLE XOM`
- Dataset stage: `365d_5x5`
- Selectors per ticker: `nearest_contract`, `entry_liquidity_first_research_only`
- Sharding: one VM per ticker, two selector processes per VM
- Expected candidate-summary files: `40`

## Launch Status

The full 20-shard launch was prepared, but the project has a hard global CPU quota of `32` CPUs. At launch time, active research/paper infrastructure already used `30` CPUs, so the first 20-VM fanout was blocked by `CPUS_ALL_REGIONS`.

Completed aggregate/watch VMs were stopped to free `6` CPUs without stopping active ticker workers. With active ticker workers and the paper validation VM preserved, the maximum safe additional tranche was four `e2-standard-2` ticker shards.

First launched shard tranche:

- `ticker365-intc-20260501-2300z` in `us-east4-a`
- `ticker365-iwm-20260501-2300z` in `us-east4-a`
- `ticker365-spy-20260501-2300z` in `us-west1-a`
- `ticker365-tsla-20260501-2300z` in `us-west1-a`

Stopped nonessential completed VMs:

- `portfolio-12h-postagg-watch-20260501-1619z`
- `portfolio-overnight-12h-20260501-finalagg2-1255z`
- `portfolio-overnight-12h-20260501-fastlane-top40-agg-1040z`

Active ticker workers were left running.

## GCS Prefix

`gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/`

## Outputs

- Worker outputs: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/workers/`
- Aggregate portfolio report: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/portfolio_report/`
- Aggregate promotion packet: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/promotion_packet/`
- Aggregate growth projection: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/growth_projection/`
- Aggregate status: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/aggregate/status/ticker_365d_aggregate_status.json`
- Watchdog status: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/watchdog/ticker_365d_watch_status_20260501.json`

## Watchdog Automation

The local control-plane watchdog is `scripts/watch_ticker_365d_wave.py`, wrapped by `scripts/run_ticker_365d_watchdog.ps1`.

Operating contract:

- Runs every 30 minutes through Windows Task Scheduler task `CodexAlpacaTicker365Watchdog`.
- Refreshes the tracked source archive and startup scripts into this GCS wave prefix.
- Stops only completed `ticker365-*` research shards from this wave.
- Launches the next pending ticker shards only when `CPUS_ALL_REGIONS` quota allows it.
- Retries interrupted ticker shards up to four total VM attempts before requiring manual intervention.
- Triggers the aggregate VM only after all expected ticker summaries land.
- Writes JSON and Markdown status locally under ignored `reports/gcp_research/watchdog/` and mirrors them to the GCS `watchdog/` prefix.
- Does not start trading, submit paper orders, modify live manifests, change risk policy, or lower the `fill_coverage >= 0.90` gate.
- A paper-trader handoff remains blocked until the aggregate promotion packet says candidates are eligible for governed validation review.

## Hard Rules

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Promotion means governed validation review only.

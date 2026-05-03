# Ticker365 Fill Repair Wave - 2026-05-03

## Purpose

Run a research-only timing repair wave after the all-ticker 365-day aggregate showed strong raw data coverage but zero eligible governed-promotion candidates.

## Scope

- Wave: `ticker365_fill_repair_20260503T1330Z`
- GCS prefix: `gs://codexalpaca-control-us/research_results/ticker365_fill_repair_20260503T1330Z/`
- Watchdog script: `scripts/watch_ticker365_fill_repair_wave.py`
- Windows wrapper: `scripts/run_ticker365_fill_repair_watchdog.ps1`
- Scheduled task: `CodexAlpacaTicker365FillRepairWatchdog`
- Target universe: the same 20 tickers with 365-day stock and option-bar datasets from the all-ticker wave.

## Repair Experiment

Each ticker worker reruns the option-aware backtester with:

- Selectors: `nearest_contract,entry_liquidity_first_research_only`
- Lag profiles: `10:10,15:15,30:60,60:120`
- Top N per symbol: `80`
- Fill gate remains: `0.90`
- Promotion scope remains: governed validation review only

This is intended to separate three cases:

- Good economics plus short-lag fill repair: candidate can continue to governed review if the promotion packet says eligible.
- Good economics only under very wide lags: strategy needs execution-policy redesign, not promotion.
- Still-low fill coverage: strategy timing is structurally incompatible with the available option market data.

## Safety Contract

- Does not start trading.
- Does not submit paper orders.
- Does not modify live manifests.
- Does not change risk policy.
- Does not lower the `fill_coverage >= 0.90` promotion gate.

## Handoff Contract

The watchdog writes local and GCS status every run. The other machine can follow:

- Local: `reports/gcp_research/ticker365_fill_repair_20260503/ticker365_fill_repair_watch_status.md`
- GCS: `gs://codexalpaca-control-us/research_results/ticker365_fill_repair_20260503T1330Z/watchdog/`

If the aggregate packet produces eligible governed-review candidates, the next step is a no-order paper-trader staging packet. No paper orders should be sent from the repair wave itself.

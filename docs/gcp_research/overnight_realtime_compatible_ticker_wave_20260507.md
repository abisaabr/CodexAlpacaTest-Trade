# Overnight Realtime-Compatible Ticker Wave - 2026-05-07

Updated: 2026-05-06 21:07 ET / 2026-05-07T01:07Z

## Scope

This is a research-only wave for dense 365d option-data tickers that are not yet represented in the current governed-validation PAPER manifest.

- Current governed-validation manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260506.yaml`
- Current governed-validation symbols: AMD, AMZN, AVGO, GOOGL, IWM, MSFT, QQQ, SPY, TSLA, TSM
- Gap symbols selected for overnight testing: AAPL, NVDA, INTC, META, MU, NFLX, ORCL, PLTR, XLE, XOM
- Wave id: `other_tickers_realtime_compatible_20260507T0030Z`
- GCS root: `gs://codexalpaca-control-us/research_results/other_tickers_realtime_compatible_20260507T0030Z/`
- Launcher: `scripts/launch_gcp_realtime_compatible_remaining_tickers.ps1`

## Realtime-Compatible Backtest Definition

This is not a true historical websocket replay. The durable historical data is 1-minute stock bars plus 1-minute option bars from the dense selected-contract universe.

For this wave, "realtime-compatible" means the replay uses paper-runner-oriented execution semantics:

- Contract selection: `entry_liquidity_first_research_only`
- Stock session filter: `option_rth_same_day`
- Entry lookup: first option bar at or after the strategy entry within the configured lag
- Exit lookup: first option bar at or after the strategy exit within the configured lag
- Lag profiles: `0:60`, `10:60`, `30:120`
- Strategy identity mode for aggregation: `variant_profile`

True realtime strategies should be validated separately using no-submit websocket shadow logs, then promoted only after the shadow path proves timestamp, quote, order-idempotency, and risk-ledger compatibility.

## First Tranche Launched

The first tranche was launched at 2026-05-07T01:07Z with 12 research VMs:

- AAPL: c001-036, c037-072, c073-108
- NVDA: c001-036, c037-072, c073-108
- INTC: c001-036, c037-072, c073-108
- META: c001-036, c037-072, c073-108

All workers are `e2-standard-2` and non-broker-facing. They write under:

`gs://codexalpaca-control-us/research_results/other_tickers_realtime_compatible_20260507T0030Z/workers/`

Each symbol has 828 candidate templates. With `CandidateCountPerWorker=36` and one selector across three lag profiles, a full symbol run is expected to produce 23 workers and 69 replay summaries.

## Dense Dataset Roots

Top-ladder symbols use:

- Stock: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/{SYMBOL}/365d_5x5/stock_ref_silver/stock_bars/`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/{SYMBOL}/365d_5x5/research_wave/dense_universe/selected_option_contracts/`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/{SYMBOL}/365d_5x5/option_bars_silver/option_bars/`

Next-ladder symbols use:

- Stock: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/{SYMBOL}/365d_5x5/stock_ref_silver/stock_bars/`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/{SYMBOL}/365d_5x5/research_wave/dense_universe/selected_option_contracts/`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/{SYMBOL}/365d_5x5/option_bars_silver/option_bars/`

## Continue Commands

Launch the next candidate windows for the first four symbols after capacity frees:

```powershell
$wave = "other_tickers_realtime_compatible_20260507T0030Z"
powershell -ExecutionPolicy Bypass -File .\scripts\launch_gcp_realtime_compatible_remaining_tickers.ps1 `
  -WaveId $wave `
  -Symbols "AAPL,NVDA,INTC,META" `
  -StartCandidateIndex 109 `
  -CandidateCountPerWorker 36 `
  -MaxLaunchesPerSymbol 3 `
  -InstanceSuffix 20260507rt2
```

Then repeat with `-StartCandidateIndex 217`, `325`, `433`, `541`, `649`, and `757` until all first-four workers are covered.

Launch the next symbol cohort when capacity is available:

```powershell
$wave = "other_tickers_realtime_compatible_20260507T0030Z"
powershell -ExecutionPolicy Bypass -File .\scripts\launch_gcp_realtime_compatible_remaining_tickers.ps1 `
  -WaveId $wave `
  -Symbols "MU,NFLX,ORCL,PLTR" `
  -StartCandidateIndex 1 `
  -CandidateCountPerWorker 36 `
  -MaxLaunchesPerSymbol 3 `
  -InstanceSuffix 20260507rt1
```

Then launch XLE/XOM:

```powershell
$wave = "other_tickers_realtime_compatible_20260507T0030Z"
powershell -ExecutionPolicy Bypass -File .\scripts\launch_gcp_realtime_compatible_remaining_tickers.ps1 `
  -WaveId $wave `
  -Symbols "XLE,XOM" `
  -StartCandidateIndex 1 `
  -CandidateCountPerWorker 36 `
  -MaxLaunchesPerSymbol 3 `
  -InstanceSuffix 20260507rt1
```

## Aggregation And Promotion Review

After workers complete, pull worker artifacts and build strict reports:

```powershell
$wave = "other_tickers_realtime_compatible_20260507T0030Z"
$gcs = "gs://codexalpaca-control-us/research_results/$wave"
$local = "reports/gcp_research/$wave/gcs_worker_pull_final/workers"
$out = "reports/gcp_research/$wave/aggregate_final"

gcloud storage cp --recursive "$gcs/workers/" $local --project codexalpaca

python scripts\build_research_portfolio_report.py `
  --replay-root $local `
  --output-dir $out `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --max-positions 12 `
  --max-strategies-per-symbol 2 `
  --max-symbol-weight 0.20 `
  --initial-cash 25000 `
  --candidate-identity-mode variant_profile `
  --required-regimes bull,bear,choppy

python scripts\build_research_promotion_review_packet.py `
  --portfolio-report-json "$out/research_portfolio_report.json" `
  --output-dir "$out/promotion_packet"

gcloud storage cp --recursive $out "$gcs/aggregate_final/" --project codexalpaca
```

Only candidates with generated packet status `eligible_for_promotion_review` are allowed to proceed into a governed-validation manifest. Runtime manifest generation must use `scripts/build_governed_validation_manifest_from_packets.py`; unsupported multi-leg candidates must remain skipped until native paper-runtime mapping exists.

## Paper Runner Integration Rule

Do not run separate broker-facing realtime and one-minute strategy processes against the same Alpaca PAPER account.

Allowed:

- One order-submitting PAPER trader process with one shared ownership lease, risk ledger, order-idempotency layer, and EOD flatten guard.
- A no-submit realtime websocket shadow monitor in parallel for latency and signal-quality measurement.
- A future single broker-facing process that can host both 1-minute bar-driven and realtime-event-driven strategies behind the same risk/order gate.

Not allowed:

- Two independent order-submitting strategy runners, even in PAPER mode.
- Adding a strategy to the PAPER config from PnL alone.
- Lowering `fill_coverage >= 0.90`.

## Hard Rules

- Broker mode remains PAPER only.
- This wave does not start trading.
- This wave does not change live manifests or global risk policy.
- Promotion means governed-validation review only.
- Strategies are added to the paper runner only after a generated promotion-review packet clears gates and the manifest builder translates the strategy into runtime-supported semantics.

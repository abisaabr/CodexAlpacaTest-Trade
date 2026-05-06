# MU/NFLX Parallel Regime Refinement

Date context: 2026-05-06 RTH, while the local multi-symbol PAPER trader is
running.

## Purpose

Run the next safest regime-gap research waves in parallel using existing 365-day
dense option datasets. This is non-broker-facing research only.

The source packets showed:

- `MU`: full-regime packet was `research_only_blocked_regime_incomplete`; eligible
  regime was `choppy`, missing regimes were `bull,bear`.
- `NFLX`: full-regime packet was `research_only_blocked_regime_incomplete`;
  eligible regime was `bull`, missing regimes were `bear,choppy`.

`PLTR` bull and `XOM` choppy were not relaunched in this pass because the latest
targeted refinement waves already failed those missing regimes with `0` eligible
candidates.

## Safety Posture

- Broker-facing effect: `none`.
- Live manifest effect: `none`.
- Risk policy effect: `none`.
- Promotion effect: governed-validation review only if generated packets clear
  the existing gates.
- The active local PAPER trader was not stopped, restarted, duplicated, or
  modified.
- The `fill_coverage >= 0.90` promotion gate was not lowered.

## Active Paper Runtime Check

At launch verification, exactly one Python broker-facing process was present:

- PID: `37892`.
- Command:
  `scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --submit-paper-orders`.

## MU Wave

- Wave ID: `mu_bull_bear_momentum_refine_20260506T1750Z`.
- GCS root:
  `gs://codexalpaca-control-us/research_results/mu_bull_bear_momentum_refine_20260506T1750Z`
- Source commit: `dbba0e5`.
- Target regimes: `bull,bear`.
- Bull profile set: `momentum_refine`.
- Bear profile set: `signal_window_refine`.
- Selector: `entry_liquidity_first_research_only`.
- Lag profiles: `0:60,10:60,30:120`.
- Candidate count: `216`.
- Launched candidates: `1-200`.
- Tail remaining: `201-216`, to launch after capacity frees.

Input data:

- Stock bars:
  `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/MU/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts:
  `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/MU/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars:
  `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/MU/365d_5x5/option_bars_silver/option_bars`

Launch command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol MU `
  -WaveId mu_bull_bear_momentum_refine_20260506T1750Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/MU/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/MU/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/MU/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506g `
  -CandidateCountPerWorker 25 `
  -MaxLaunches 8 `
  -TargetRegimes bull,bear `
  -BullProfileSet momentum_refine `
  -BearProfileSet signal_window_refine `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,bear,choppy,unclassified `
  -LagProfiles "0:60,10:60,30:120"
```

Workers launched:

- `mu-rescue-c001-025-20260506g`
- `mu-rescue-c026-050-20260506g`
- `mu-rescue-c051-075-20260506g`
- `mu-rescue-c076-100-20260506g`
- `mu-rescue-c101-125-20260506g`
- `mu-rescue-c126-150-20260506g`
- `mu-rescue-c151-175-20260506g`
- `mu-rescue-c176-200-20260506g`

## NFLX Wave

- Wave ID: `nflx_bear_choppy_refine_20260506T1750Z`.
- GCS root:
  `gs://codexalpaca-control-us/research_results/nflx_bear_choppy_refine_20260506T1750Z`
- Source commit: `dbba0e5`.
- Target regimes: `bear,choppy`.
- Bear profile set: `signal_window_refine`.
- Choppy profile set: `timewindow_quality_filter`.
- Selector: `entry_liquidity_first_research_only`.
- Lag profiles: `0:60,10:60,30:120`.
- Candidate count: `150`.
- Launched candidates: `1-150`.

Input data:

- Stock bars:
  `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/NFLX/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts:
  `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/NFLX/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars:
  `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/NFLX/365d_5x5/option_bars_silver/option_bars`

Launch command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol NFLX `
  -WaveId nflx_bear_choppy_refine_20260506T1750Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/NFLX/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/NFLX/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/NFLX/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506g `
  -CandidateCountPerWorker 21 `
  -MaxLaunches 8 `
  -TargetRegimes bear,choppy `
  -BearProfileSet signal_window_refine `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bear,choppy,bull,unclassified `
  -LagProfiles "0:60,10:60,30:120"
```

Workers launched:

- `nflx-rescue-c001-021-20260506g`
- `nflx-rescue-c022-042-20260506g`
- `nflx-rescue-c043-063-20260506g`
- `nflx-rescue-c064-084-20260506g`
- `nflx-rescue-c085-105-20260506g`
- `nflx-rescue-c106-126-20260506g`
- `nflx-rescue-c127-147-20260506g`
- `nflx-rescue-c148-150-20260506g`

## Capacity Notes

Several `e2-standard-2` creates hit zone resource exhaustion in `us-east4-a` and
`us-east1-b`. Some `us-central1-a` retries hit the regional in-use-address quota.
The launcher successfully placed active workers in `us-central1-a` and
`us-west1-a`.

## Next Actions

1. Monitor all MU/NFLX workers until they terminate or report failure.
2. Launch MU tail candidates `201-216` after capacity frees.
3. Pull worker artifacts from GCS.
4. Build strict portfolio reports with `fill_coverage_gate=0.90`,
   `min_option_trades=20`, `min_test_net_pnl > 0`, and the relevant required
   regimes.
5. Build generated promotion-review packets without manual overrides.
6. If MU or NFLX becomes regime-complete, combine only through generated
   governed-validation artifacts; do not add either ticker to paper execution
   directly from this launch.

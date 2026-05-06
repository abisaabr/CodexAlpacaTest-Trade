# XLE Bull Momentum Refine Research Wave

Date context: 2026-05-06 RTH, while the local multi-symbol PAPER trader is
running.

## Purpose

The prior XLE full-regime rescue packet was
`research_only_blocked_regime_incomplete`: bear and choppy had governed-review
candidates, but bull had `0` eligible candidates. This research-only wave tests
the `momentum_refine` bull grid against the existing XLE 365-day dense option
dataset to see whether XLE can become regime-complete without changing promotion
gates.

## Safety Posture

- Broker-facing effect: `none`.
- Live manifest effect: `none`.
- Risk policy effect: `none`.
- Promotion effect: governed-validation review only if a generated packet clears
  the existing gates.
- The active local PAPER trader was not stopped, restarted, duplicated, or
  modified.
- The `fill_coverage >= 0.90` promotion gate was not lowered.

## Source And Inputs

- Source commit: `105db79`.
- Wave ID: `xle_bull_momentum_refine_20260506T1715Z`.
- GCS root:
  `gs://codexalpaca-control-us/research_results/xle_bull_momentum_refine_20260506T1715Z`
- Stock bars:
  `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/XLE/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts:
  `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/XLE/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars:
  `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/XLE/365d_5x5/option_bars_silver/option_bars`

All three data prefixes were checked with `gsutil ls` before launch.

## Launch

The input builder created `162` XLE bull candidates using
`bull_profile_set=momentum_refine`.

The launcher created `8` research-only workers. Some zones were capacity-limited
for `e2-standard-2`, but retry placement succeeded in `us-central1-a` and
`us-west1-a`.

Running workers at launch:

- `xle-rescue-c001-021-20260506f`
- `xle-rescue-c022-042-20260506f`
- `xle-rescue-c043-063-20260506f`
- `xle-rescue-c064-084-20260506f`
- `xle-rescue-c085-105-20260506f`
- `xle-rescue-c106-126-20260506f`
- `xle-rescue-c127-147-20260506f`
- `xle-rescue-c148-162-20260506f`

Launch command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol XLE `
  -WaveId xle_bull_momentum_refine_20260506T1715Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/XLE/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/XLE/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/XLE/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506f `
  -CandidateCountPerWorker 21 `
  -MaxLaunches 8 `
  -TargetRegimes bull `
  -BullProfileSet momentum_refine `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,bear,choppy,unclassified `
  -LagProfiles "0:60,10:60,30:120"
```

## Next Actions

1. Monitor workers until they terminate or report failure.
2. Pull worker artifacts from GCS.
3. Build a strict portfolio report with `fill_coverage_gate=0.90`,
   `min_option_trades=20`, `min_test_net_pnl > 0`, and
   `required_regimes=bull`.
4. Build a promotion-review packet without manual overrides.
5. If XLE gains eligible bull candidates, combine only through generated
   governed-review artifacts; do not add XLE to paper execution from this wave
   alone.

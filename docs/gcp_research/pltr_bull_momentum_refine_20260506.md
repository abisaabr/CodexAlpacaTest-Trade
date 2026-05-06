# PLTR Bull Momentum Refine Research Wave

Date context: 2026-05-06 RTH, while the local multi-symbol PAPER trader is running.

## Purpose

PLTR's prior full-regime rescue packet was `research_only_blocked_regime_incomplete`
with eligible bear and choppy candidates but no eligible bull sleeve. This
research-only wave applies the bull momentum refinement grid that produced ORCL
bull governed-review candidates.

## Safety Posture

- Broker-facing effect: `none`.
- Live manifest effect: `none`.
- Risk policy effect: `none`.
- Promotion effect: governed-validation review only if a generated packet clears
  the existing gates.
- The active local paper trader was not stopped, restarted, or duplicated.
- The `fill_coverage >= 0.90` promotion gate was not lowered.

## Source And Inputs

- Source commit: `f33689a`.
- Wave ID: `pltr_bull_momentum_refine_20260506T1625Z`.
- GCS root:
  `gs://codexalpaca-control-us/research_results/pltr_bull_momentum_refine_20260506T1625Z`
- Stock bars:
  `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/PLTR/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts:
  `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/PLTR/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars:
  `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/PLTR/365d_5x5/option_bars_silver/option_bars`

All three data prefixes were checked with `gsutil ls` before launch.

## Launch

The launcher created `8` research-only workers covering `162` candidates. Some
zones were capacity-limited for `e2-standard-2`, but retry placement succeeded.

Running workers at launch check:

- `pltr-rescue-c001-021-20260506d`
- `pltr-rescue-c022-042-20260506d`
- `pltr-rescue-c043-063-20260506d`
- `pltr-rescue-c064-084-20260506d`
- `pltr-rescue-c085-105-20260506d`
- `pltr-rescue-c106-126-20260506d`
- `pltr-rescue-c127-147-20260506d`
- `pltr-rescue-c148-162-20260506d`

Launch command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol PLTR `
  -WaveId pltr_bull_momentum_refine_20260506T1625Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/PLTR/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/PLTR/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/PLTR/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506d `
  -CandidateCountPerWorker 21 `
  -MaxLaunches 8 `
  -TargetRegimes bull `
  -BullProfileSet momentum_refine `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,bear,choppy,unclassified `
  -LagProfiles "0:60,10:60,30:120"
```

## Next Actions

1. Monitor workers until they terminate or report completion.
2. Pull worker artifacts from GCS.
3. Build a strict portfolio report with `fill_coverage_gate=0.90`,
   `min_option_trades=20`, `min_test_net_pnl > 0`, and `required_regimes=bull`.
4. Build a promotion-review packet without manual overrides.
5. If PLTR gains eligible bull candidates, combine only through generated
   governed-review artifacts; do not add PLTR to paper execution from this wave
   alone.

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

## Completion Result

All `8` workers reached `TERMINATED`. Worker artifacts were pulled from GCS into
the local research report tree; the pull transferred `374` objects and about
`362.7 MiB`.

Aggregate artifacts:

- Portfolio report:
  `reports/gcp_research/pltr_bull_momentum_refine_20260506T1625Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion-review packet:
  `reports/gcp_research/pltr_bull_momentum_refine_20260506T1625Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GCS aggregate mirror:
  `gs://codexalpaca-control-us/research_results/pltr_bull_momentum_refine_20260506T1625Z/aggregate/`

Strict promotion-review packet result:

- Decision: `research_only_blocked`.
- Eligible bull candidates: `0`.
- Candidate count: `486`.
- Blockers:
  - `fill_coverage_below_0.90`: `150`.
  - `min_net_pnl_not_positive`: `485`.
  - `test_net_pnl_not_above_0.01`: `452`.
- Best bull candidate:
  `portfolio12h__pltr__bull__call__single_leg_repair__ab777fccba96a6__profile_pltr-regime-rescue-c148-162-pltr-e30-x120-entry-liquidity-first-research-only`
- Best candidate metrics:
  - `best_min_net_pnl`: `-626.948`.
  - `best_min_test_net_pnl`: `868.675`.
  - `best_min_fill_coverage`: `0.9843`.

## Interpretation

PLTR bull remains research-only blocked. The best candidate cleared fill
coverage and had positive test-period PnL, but failed full-period net PnL. This
is primarily an economics/strategy-design failure, not a broad data-fill
failure. The correct next action is strategy redesign or quarantine of this
specific PLTR bull momentum-refine lane, not promotion and not a fill-gate
override.

The active local paper trader was not stopped, restarted, duplicated, or modified
while this research-only wave was aggregated.

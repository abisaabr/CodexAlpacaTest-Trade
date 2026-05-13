# XOM Choppy Quality Refine Research Wave

Date context: 2026-05-06 RTH, while the local multi-symbol PAPER trader is
running.

## Purpose

The prior XOM full-regime rescue packet was
`research_only_blocked_regime_incomplete`: bull and bear had governed-review
candidates, but choppy had `0` eligible candidates. This research-only wave
tests the `timewindow_quality_filter` choppy grid against the existing XOM
365-day dense option dataset to see whether XOM can become regime-complete
without changing promotion gates.

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

- Source commit: `9b0b09b`.
- Wave ID: `xom_choppy_quality_refine_20260506T1650Z`.
- GCS root:
  `gs://codexalpaca-control-us/research_results/xom_choppy_quality_refine_20260506T1650Z`
- Stock bars:
  `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/XOM/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts:
  `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/XOM/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars:
  `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/XOM/365d_5x5/option_bars_silver/option_bars`

All three data prefixes were checked with `gsutil ls` before launch.

## Launch

The input builder created `96` XOM choppy candidates using
`choppy_profile_set=timewindow_quality_filter` and
`choppy_signal_delay_bars=0,1,2`.

The launcher created `8` research-only workers with `12` candidates per worker.
`us-east4-a` was capacity-limited for two launches; retry placement succeeded in
`us-central1-a`.

Running workers at launch check:

- `xom-rescue-c001-012-20260506e` in `us-central1-a`
- `xom-rescue-c013-024-20260506e` in `us-west1-a`
- `xom-rescue-c025-036-20260506e` in `us-central1-a`
- `xom-rescue-c037-048-20260506e` in `us-east1-b`
- `xom-rescue-c049-060-20260506e` in `us-central1-a`
- `xom-rescue-c061-072-20260506e` in `us-west1-a`
- `xom-rescue-c073-084-20260506e` in `us-central1-a`
- `xom-rescue-c085-096-20260506e` in `us-east1-b`

Launch command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol XOM `
  -WaveId xom_choppy_quality_refine_20260506T1650Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/XOM/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/XOM/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/XOM/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506e `
  -CandidateCountPerWorker 12 `
  -MaxLaunches 8 `
  -TargetRegimes choppy `
  -ChoppyProfileSet timewindow_quality_filter `
  -ChoppySignalDelayBars "0,1,2" `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder choppy,bear,bull,unclassified `
  -LagProfiles "0:60,10:60,30:120"
```

## Completion Result

All `8` workers reached `TERMINATED`. Worker artifacts were pulled from GCS into
the local research report tree; the pull transferred `370` objects and about
`147.3 MiB`.

Aggregate artifacts:

- Portfolio report:
  `reports/gcp_research/xom_choppy_quality_refine_20260506T1650Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion-review packet:
  `reports/gcp_research/xom_choppy_quality_refine_20260506T1650Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GCS aggregate mirror:
  `gs://codexalpaca-control-us/research_results/xom_choppy_quality_refine_20260506T1650Z/aggregate/`

Strict promotion-review packet result:

- Decision: `research_only_blocked`.
- Eligible choppy candidates: `0`.
- Candidate count: `288`.
- Blockers:
  - `min_net_pnl_not_positive`: `288`.
  - `test_net_pnl_not_above_0.01`: `288`.
- Fill result:
  - `fill_gate_clear`: `288`.
- Best choppy candidate:
  `portfolio12h__xom__choppy__call__single_leg_repair__947049e9c5b726__profile_xom-regime-rescue-c049-060-xom-e30-x120-entry-liquidity-first-research-only`
- Best candidate metrics:
  - `best_min_net_pnl`: `-1448.346`.
  - `best_min_test_net_pnl`: `-384.194`.
  - `best_min_fill_coverage`: `0.9615`.

## Interpretation

XOM remains research-only blocked for choppy. The quality-filtered choppy grid
cleared the strategy fill gate across the full candidate population, so the
remaining failure is not data fill. The choppy design is structurally
unprofitable on this XOM dataset under the tested exits and should be redesigned
or quarantined rather than repaired with more data.

The active local PAPER trader was not stopped, restarted, duplicated, or modified
while this research-only wave was aggregated.

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

## Completion Result

All `8` research workers reached `TERMINATED`. Worker artifacts were pulled
locally from GCS and aggregated into strict report and promotion-review outputs.

Local aggregate outputs:

- `reports/gcp_research/xle_bull_momentum_refine_20260506T1715Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- `reports/gcp_research/xle_bull_momentum_refine_20260506T1715Z/aggregate/combined_portfolio_report/research_portfolio_report.md`
- `reports/gcp_research/xle_bull_momentum_refine_20260506T1715Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- `reports/gcp_research/xle_bull_momentum_refine_20260506T1715Z/aggregate/combined_promotion_packet/research_promotion_review_packet.md`

GCS aggregate root:

- `gs://codexalpaca-control-us/research_results/xle_bull_momentum_refine_20260506T1715Z/aggregate/`

Strict portfolio command:

```powershell
python scripts\build_research_portfolio_report.py `
  --replay-root reports\gcp_research\xle_bull_momentum_refine_20260506T1715Z\workers `
  --output-dir reports\gcp_research\xle_bull_momentum_refine_20260506T1715Z\aggregate\combined_portfolio_report `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0.01 `
  --initial-cash 25000 `
  --required-regimes bull `
  --candidate-identity-mode variant_profile
```

Promotion-review command:

```powershell
python scripts\build_research_promotion_review_packet.py `
  --portfolio-report-json reports\gcp_research\xle_bull_momentum_refine_20260506T1715Z\aggregate\combined_portfolio_report\research_portfolio_report.json `
  --output-dir reports\gcp_research\xle_bull_momentum_refine_20260506T1715Z\aggregate\combined_promotion_packet `
  --max-review-candidates 20
```

Generated packet decision:

- Decision: `ready_for_governed_validation_review`.
- Candidate count: `486`.
- Eligible bull candidates: `3`.
- Required regimes for this wave: `bull`.
- Eligible regimes for this wave: `bull`.
- Fill coverage gate: `0.90`.
- Minimum option trades: `20`.
- Minimum test net PnL: `0.01`.

Full-population blocker counts:

- `fill_coverage_below_0.90`: `319`.
- `min_net_pnl_not_positive`: `474`.
- `option_trades_below_20`: `49`.
- `test_net_pnl_not_above_0.01`: `400`.

## Review Candidates

1. `portfolio12h__xle__bull__call__single_leg_repair__fefb494890c122__profile_xle-regime-rescue-c022-042-xle-e30-x120-entry-liquidity-first-research-only`
   - Family: `single_leg_repair`.
   - Regime: `bull`.
   - `min_net_pnl`: `749.262`.
   - `min_test_net_pnl`: `2361.055`.
   - `min_fill_coverage`: `0.9508`.
   - `min_option_trade_count`: `58`.
   - `worst_drawdown`: `-2299.894`.

2. `portfolio12h__xle__bull__call__single_leg_repair__9a553f32f6c038__profile_xle-regime-rescue-c043-063-xle-e0-x60-entry-liquidity-first-research-only`
   - Family: `single_leg_repair`.
   - Regime: `bull`.
   - `min_net_pnl`: `408.769`.
   - `min_test_net_pnl`: `1209.71`.
   - `min_fill_coverage`: `0.9123`.
   - `min_option_trade_count`: `52`.
   - `worst_drawdown`: `-1990.166`.

3. `portfolio12h__xle__bull__call__single_leg_repair__c34ae968f47be0__profile_xle-regime-rescue-c043-063-xle-e0-x60-entry-liquidity-first-research-only`
   - Family: `single_leg_repair`.
   - Regime: `bull`.
   - `min_net_pnl`: `261.916`.
   - `min_test_net_pnl`: `791.129`.
   - `min_fill_coverage`: `0.9123`.
   - `min_option_trade_count`: `52`.
   - `worst_drawdown`: `-2054.102`.

## Interpretation

XLE now has generated bull governed-review candidates from the momentum-refine
grid. This closes the prior bull-side research gap for XLE at the wave level,
but it does not authorize live-manifest changes or paper activation. Any broader
XLE promotion must be assembled through generated governed-validation artifacts
that combine the prior XLE bear/choppy evidence with this bull packet, then
reviewed separately.

The active local PAPER trader was not stopped, restarted, duplicated, or
modified while this research wave completed.

## Next Actions

1. Combine XLE bull, bear, and choppy evidence only through generated
   governed-validation artifacts.
2. Do not activate XLE in paper execution from this wave alone.
3. Keep the `fill_coverage >= 0.90`, option-trade-count, full/test PnL,
   loser-cluster, and portfolio-context gates intact.
4. Review runtime translation before activating any vertical-style generated
   candidate; this wave's eligible candidates are single-leg repair, but the
   broader manifest builder should still preserve multi-leg semantics before
   any future paper activation of vertical families.

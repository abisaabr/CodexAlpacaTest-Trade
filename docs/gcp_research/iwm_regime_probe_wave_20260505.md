# IWM Regime Probe Wave - 2026-05-05

## Scope

IWM is the next symbol after QQQ and SPY in the paper-readiness sequence.

This wave is research-only:

- Broker-facing trading: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Promotion rule: do not promote anything unless a generated promotion-review packet says `eligible_for_promotion_review`

## Dataset

IWM has a 365 calendar day 5x5 option-fill ladder dataset available in GCS.

- Dataset ID: `option_fill_ladder_20260429_iwm_365d_5x5`
- Date window: `2025-04-29` to `2026-04-28`
- Selected trade dates: `251`
- Selected contract-days: `5522`
- Contract-days with bars: `5510`
- Option-bar rows: `952910`
- Raw contract-day coverage: `0.997827`
- Missing contract-days: `12`

GCS roots:

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/IWM/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/IWM/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/IWM/365d_5x5/option_bars_silver/option_bars`
- Dataset status: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/IWM/365d_5x5/fill_ladder_status.json`

## Strict Replay Profile

The first IWM tranche mirrors the QQQ/SPY fill-safe repair profile:

- Contract selector: `entry_liquidity_first_research_only`
- Entry lag: `0`
- Exit lag: `60`
- Entry lookup: `first_bar_at_or_after_entry_within_lag`
- Exit lookup: `first_bar_at_or_after_exit_within_lag`
- Stock session filter: `option_rth_same_day`
- Initial cash: `25000`
- Allocation fraction: `0.05`
- Slippage: `10 bps`
- Fee: `$0.65` per contract
- Test date count: `20`

## Waves

Wave stamp: `20260505T1035Z`

- Bull: `ticker365_iwm_bull_premium_exit_20260505T1035Z`
- Bear: `ticker365_iwm_bear_premium_exit_20260505T1035Z`
- Choppy: `ticker365_iwm_choppy_refine_20260505T1035Z`

Inputs:

- Bull inputs: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bull_premium_exit_20260505T1035Z/inputs/`
- Bear inputs: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_premium_exit_20260505T1035Z/inputs/`
- Choppy inputs: `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_refine_20260505T1035Z/inputs/`
- Source archive copied to each wave under `inputs/source/codexalpaca_repo_source.tar.gz`
- Startup script copied to each wave under `ops/option_worker_startup.sh`
- Launch rows: `gs://codexalpaca-control-us/research_results/ticker365_iwm_launch_20260505T1035Z/ops/iwm_probe_launch_rows.json`

Note: an initial recursive GCS copy produced duplicate nested `inputs/inputs/` objects. The canonical worker URIs use the non-nested `inputs/` files listed above.

## Workers

First tranche target uses the full 32-vCPU project quota with 16 `e2-standard-2` workers:

- Bull candidates `1-8`, one candidate per worker
- Bear candidates `1-8`, two candidates per worker
- Choppy candidates `1-8`, two candidates per worker

Worker names:

- Bull: `iwmbull-c001-20260505a` through `iwmbull-c008-20260505a`
- Bear: `iwmbear-c001-002-20260505a`, `iwmbear-c003-004-20260505a`, `iwmbear-c005-006-20260505a`, `iwmbear-c007-008-20260505a`
- Choppy: `iwmchop-c001-002-20260505a`, `iwmchop-c003-004-20260505a`, `iwmchop-c005-006-20260505a`, `iwmchop-c007-008-20260505a`

Operational notes:

- Boot disk: `50GB pd-standard`
- Service account: `ramzi-service-account@codexalpaca.iam.gserviceaccount.com`
- Scopes: `cloud-platform`
- First creation attempt failed before instance creation because label value `20260505T1035Z` had uppercase characters.
- Retry used lowercase-safe label value `20260505t1035z`.
- Retry launch log: `gs://codexalpaca-control-us/research_results/ticker365_iwm_launch_20260505T1035Z/ops/iwm_worker_launch_log_retry1.txt`

## Current Status

As of the first post-launch check, all 16 IWM workers were `RUNNING` and status files had begun uploading. Observed phases included:

- `startup`
- `staging_source`
- `staging_data`

No worker had reported a failed phase at the first check.

The first tranche, covering bull `1-8`, bear `1-8`, and choppy `1-8`, completed and was cleaned up after GCS status said `completed`.

First-tranche result:

- Bull produced one governed-validation-review eligible candidate:
  - Candidate: `portfolio12h__iwm__bull__call__single_leg_repair__3dc6b95db3d9a6`
  - Net PnL: `617.845`
  - Test Net PnL: `829.028`
  - Fill coverage: `0.994`
  - Option trades: `166`
- Bear `1-8` was blocked by economics, not raw data.
- Choppy `1-8` was blocked by economics, not raw data.
- Fill behavior was materially repaired versus the original low-fill problem; single-leg IWM candidates commonly cleared or nearly cleared the `0.90` fill gate.

Extension `9-16` result:

- Bear `9-16` completed with no eligible candidate.
- Choppy `9-16` completed with no eligible candidate.
- Best choppy candidates had positive test-period PnL but negative full-period PnL, so they remained blocked by `min_net_pnl_not_positive`.
- Best bear candidates either had positive full-period PnL with negative test PnL or positive test PnL with negative full-period PnL.

Additional extension launched:

- Bear candidates `17-32`
- Choppy candidates `17-32`
- Launch rows: `gs://codexalpaca-control-us/research_results/ticker365_iwm_launch_20260505T1035Z/ops/iwm_extension_17_32_launch_rows.json`
- Launch log: `gs://codexalpaca-control-us/research_results/ticker365_iwm_launch_20260505T1035Z/ops/iwm_extension_17_32_launch_log.txt`

Extension `17-32` result:

- Bear `17-32` completed with no eligible candidate.
- Choppy single-leg refine `17-32` completed with no eligible candidate.
- Some choppy candidates had positive test-period PnL, but all remained negative over the full 365-day window.
- The four original east-zone workers failed because `us-east1-b` had insufficient resources; they were relaunched successfully into central/west zones.

Additional choppy premium lane launched:

- Wave: `ticker365_iwm_choppy_premium_exit_20260505T1135Z`
- Source queue: copied from the IWM premium-exit queue.
- Candidate ranks: `33-64` from `qqq_premium_exit_option_queue.json`
- Coverage target: choppy defined-risk families that were not tested by the single-leg choppy refine queue:
  - `iron_condor`
  - `iron_butterfly`
  - `premium_defense_spread`
- Launch rows: `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_premium_exit_20260505T1135Z/ops/iwm_choppy_premium_33_64_launch_rows.json`
- Launch log: `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_premium_exit_20260505T1135Z/ops/iwm_choppy_premium_33_64_launch_log.txt`

Choppy premium `33-64` result:

- No eligible candidate.
- Best full-period net PnL remained negative.
- Many defined-risk structures also failed the `0.90` fill gate because multi-leg same-structure execution is harder than the repaired single-leg path.
- Best observed choppy premium candidate by full-period PnL:
  - Candidate: `portfolio12h__iwm__choppy__call__iron_condor__ee3ea3ecae7051`
  - Net PnL: `-12174.878`
  - Test Net PnL: `370.809`
  - Fill coverage: `0.6939`
  - Blockers: `fill_coverage_below_0.90`, `min_net_pnl_not_positive`
- Cleanup log: `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_premium_exit_20260505T1135Z/ops/ops_cleanup_completed_iwm_choppy_premium_workers_1.txt`

Institutional conclusion:

- IWM is not regime-complete.
- IWM has one governed-validation-review eligible bull candidate only.
- IWM should not be added to the paper portfolio yet.
- Further IWM work should be an IWM-specific bear/choppy redesign wave, not more blind breadth from the current QQQ/SPY templates.

## Next Steps

1. Continue monitoring worker status JSON under each wave's `workers/` prefix.
2. Delete only completed terminated research workers after their GCS `status.json` says `phase=completed`.
3. Do not launch choppy premium `65-68`; `33-64` was sufficiently negative to stop blind breadth.
4. Build a targeted IWM-specific bear/choppy redesign wave if IWM remains a priority.
5. Aggregate IWM only as a bull-only research finding unless a future redesign produces bear/choppy eligibility.
6. Keep the paper-ready portfolio focused on QQQ first, then QQQ+SPY.
7. Do not start broker-facing paper or submit paper orders without explicit operator approval.

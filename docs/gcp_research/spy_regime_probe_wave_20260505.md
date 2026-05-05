# SPY Regime Probe Wave - 2026-05-05

## Scope

After QQQ reached a regime-complete governed-validation packet, SPY is the next symbol in the paper-readiness sequence.

This wave is research-only:

- Broker-facing trading: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Promotion rule: do not promote anything unless a generated promotion-review packet says `eligible_for_promotion_review`

## Dataset

SPY has a 365 calendar day 5x5 option-fill ladder dataset available in GCS.

- Dataset ID: `option_fill_ladder_20260429_spy_365d_5x5`
- Date window: `2025-04-29` to `2026-04-28`
- Selected trade dates: `251`
- Selected contract-days: `5522`
- Contract-days with bars: `5521`
- Option-bar rows: `1778313`
- Raw contract-day coverage: `0.999819`

GCS roots:

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/SPY/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/SPY/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/SPY/365d_5x5/option_bars_silver/option_bars`
- Dataset status: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/SPY/365d_5x5/fill_ladder_status.json`

Prior SPY replay packet:

- Packet: `gs://codexalpaca-control-us/research_results/overnight_365d_bruteforce_20260429/top10_replay_gcsfix_spy_rerun_03bfc25_20260429T1810Z/SPY/promotion_packet/research_promotion_review_packet.json`
- Prior decision: `research_only_blocked`
- Prior blocker: all 36 candidates had `fill_coverage_below_0.90`
- Prior interpretation: raw data coverage is strong; replay/timing/selector semantics are the blocker, same class as QQQ before the option-native repair.

## Waves

The first SPY tranche mirrors the QQQ-winning repair profile:

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

Wave IDs:

- Bull: `ticker365_spy_bull_premium_exit_20260505T0945Z`
- Bear: `ticker365_spy_bear_premium_exit_20260505T0945Z`
- Choppy: `ticker365_spy_choppy_refine_20260505T0945Z`

Inputs:

- Bull inputs: `gs://codexalpaca-control-us/research_results/ticker365_spy_bull_premium_exit_20260505T0945Z/inputs/`
- Bear inputs: `gs://codexalpaca-control-us/research_results/ticker365_spy_bear_premium_exit_20260505T0945Z/inputs/`
- Choppy inputs: `gs://codexalpaca-control-us/research_results/ticker365_spy_choppy_refine_20260505T0945Z/inputs/`
- Source archive copied to each wave under `inputs/source/codexalpaca_repo_source.tar.gz`
- Startup script copied to each wave under `ops/option_worker_startup.sh`
- Launch rows: `gs://codexalpaca-control-us/research_results/ticker365_spy_launch_20260505T0945Z/ops/spy_probe_launch_rows.json`

## Workers

First tranche target:

- Bull candidates: `1-8`
- Bear candidates: `1-8`
- Choppy candidates: `1-8`

Worker naming:

- Bull: `spybull-c001-20260505a` through `spybull-c008-20260505a`
- Bear: `spybear-c001-20260505a` through `spybear-c008-20260505a`
- Choppy: `spychop-c001-20260505a` through `spychop-c008-20260505a`

Quota notes:

- Initial 100GB `pd-balanced` workers hit regional `SSD_TOTAL_GB` quota.
- Remaining workers were launched with 50GB `pd-standard`.
- Global CPU quota is `32`, so at most 16 `e2-standard-2` workers can run concurrently.
- Completed terminated workers may be deleted after their GCS `status.json` says `phase=completed`.

Cleanup log:

- `gs://codexalpaca-control-us/research_results/ticker365_spy_launch_20260505T0945Z/ops/ops_cleanup_completed_spy_workers_1.txt`

## Next Steps

1. Poll worker `status.json` files under each wave's `workers/` prefix.
2. When all 24 target workers are complete, pull their `option_aware_candidate_summary.csv` files into a local aggregate.
3. Build one SPY portfolio report and promotion-review packet across bull, bear, and choppy candidates.
4. If SPY has at least one eligible strategy in each regime, create a SPY governed-validation manifest and then repeat the same lane for IWM.
5. If any SPY regime fails, use the best near-miss to launch the next small tranche before moving to IWM.

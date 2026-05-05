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

Bear extension:

- Bear candidates: `9-16`
- Reason: bear candidates `1-8` cleared fill on single-leg structures but failed economics; QQQ's winning bear came from the later bear tranche, so SPY required the same follow-up.

Worker naming:

- Bull: `spybull-c001-20260505a` through `spybull-c008-20260505a`
- Bear: `spybear-c001-20260505a` through `spybear-c008-20260505a`
- Choppy: `spychop-c001-20260505a` through `spychop-c008-20260505a`

Quota notes:

- Initial 100GB `pd-balanced` workers hit regional `SSD_TOTAL_GB` quota.
- Remaining workers were launched with 50GB `pd-standard`.
- Global CPU quota is `32`, so at most 16 `e2-standard-2` workers can run concurrently.
- Completed terminated workers may be deleted after their GCS `status.json` says `phase=completed`.

Cleanup logs:

- `gs://codexalpaca-control-us/research_results/ticker365_spy_launch_20260505T0945Z/ops/ops_cleanup_completed_spy_workers_1.txt`
- `gs://codexalpaca-control-us/research_results/ticker365_spy_launch_20260505T0945Z/ops/ops_cleanup_completed_spy_workers_2.txt`

## Result

SPY reached a regime-complete governed-validation packet.

- Combined local packet: `reports/gcp_research/spy_regime_complete_20260505/promotion_packet/research_promotion_review_packet.json`
- Combined GCS packet: `gs://codexalpaca-control-us/research_results/spy_regime_complete_20260505/promotion_packet/research_promotion_review_packet.json`
- Decision: `ready_for_governed_validation_review`
- Eligible count: `3`
- Capital allocated weight: `1.0`
- Broker-facing: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

Selected SPY candidates:

- Choppy: `portfolio12h__spy__choppy__call__single_leg_repair__f9a907650802b7`
- Choppy metrics: `net_pnl=5508.298`, `test_net_pnl=194.349`, `fill_coverage=0.994`, `option_trade_count=166`
- Bear: `portfolio12h__spy__bear__put__single_leg_repair__1f8d8ba1e9de3b`
- Bear metrics: `net_pnl=2071.481`, `test_net_pnl=512.94`, `fill_coverage=1.0`, `option_trade_count=69`
- Bull: `portfolio12h__spy__bull__call__single_leg_repair__9460508770cf7e`
- Bull metrics: `net_pnl=179.621`, `test_net_pnl=2270.178`, `fill_coverage=0.9941`, `option_trade_count=168`

Capital plan from the combined packet:

- Choppy weight: `0.531657`
- Bear weight: `0.385667`
- Bull weight: `0.082676`

## Runner Packet

SPY now has a broker-free governed-validation runner packet matching the QQQ runner shape.

- Promotion manifest: `config/promotion_manifests/spy_regime_complete_governed_validation_20260505.yaml`
- Portfolio config: `config/spy_regime_complete_paper_portfolio.yaml`
- Default order submission: `false`
- Broker-facing trading: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Runner strategy count: `3`

No-order startup preflight:

- Local: `reports/gcp_research/spy_regime_complete_20260505/paper_launch_pack/startup_preflight_no_orders_0820ET.json`
- GCS: `gs://codexalpaca-control-us/research_results/spy_regime_complete_20260505/paper_launch_pack/startup_preflight_no_orders_0820ET.json`
- Result: `startup_preflight_pending`
- Pending reason: `SPY stock frame not ready yet`
- Broker/account checks reached successfully:
  - Buying power: `399225.72`
  - Broker equity: `99806.43`
  - Broker positions: `0`
  - Open orders: `0`
- Orders submitted: `false`

Validation:

- `config/spy_regime_complete_paper_portfolio.yaml` loads with `3` SPY governed strategies.
- Focused runner gate: `67 passed`

## Next Steps

1. Keep QQQ as the first paper-readiness candidate.
2. Re-run QQQ and SPY no-order startup preflights after live RTH stock frames are available.
3. Do not add SPY to broker-facing paper until QQQ broker-free shadow/preflight is clean and the operator explicitly approves.
4. IWM is bull-only after the first full pass and should not be added to the paper portfolio until targeted bear/choppy redesign produces eligible candidates.
5. If QQQ and SPY both pass broker-free shadow validation, build a controlled QQQ+SPY validation portfolio.

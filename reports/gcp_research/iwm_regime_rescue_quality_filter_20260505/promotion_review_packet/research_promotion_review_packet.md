# Research Promotion Review Packet

- Generated at: `2026-05-05T19:56:32.961268+00:00`
- Decision: `ready_for_governed_validation_review`
- Candidate-level decision: `ready_for_governed_validation_review`
- Promotion scope: `research_governed_validation_review_only`
- Broker facing: `False`
- Candidate count: `1272`
- Eligible count: `8`
- Top-candidate count: `100`
- Blocker count scope: `full_candidate_population`
- Required regimes: `bull, bear, choppy`
- Missing eligible regimes: `none`
- Regime complete for promotion review: `True`
- Fill coverage unit: `None`
- Fill coverage semantics: None
- Capital allocated weight: `0.999999`
- Capital unallocated dollars: `$0.03`

## Review Candidates

- `IWM` `portfolio12h__iwm__bear__put__single_leg_repair__89134067786703__profile_iwm-rescue-c001-012-iwm-e0-x60-nearest-contract` family `single_leg_repair` regime `bear` min_net `1419.675` min_test `1689.62` strategy_fill `0.9056` data_foundation `1.0` blockers `none`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__25a40ccaf9318c__profile_iwm-rescue-c049-060-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` min_net `2675.75` min_test `284.97` strategy_fill `1.0` data_foundation `1.0` blockers `none`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__e0872c62579f6c__profile_iwm-rescue-c049-060-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` min_net `2510.915` min_test `246.009` strategy_fill `1.0` data_foundation `1.0` blockers `none`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__659e26dfde89ff__profile_iwm-rescue-c073-084-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` min_net `2294.355` min_test `264.45` strategy_fill `1.0` data_foundation `1.0` blockers `none`
- `IWM` `portfolio12h__iwm__bear__put__single_leg_repair__c7895e06f5c278__profile_iwm-rescue-c001-012-iwm-e0-x60-nearest-contract` family `single_leg_repair` regime `bear` min_net `511.584` min_test `1833.476` strategy_fill `0.9056` data_foundation `1.0` blockers `none`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__2dcc788cb02d52__profile_iwm-rescue-c073-084-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` min_net `2230.419` min_test `263.451` strategy_fill `1.0` data_foundation `1.0` blockers `none`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__9875e5091f29bd__profile_iwm-rescue-c061-072-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` min_net `2386.04` min_test `14.241` strategy_fill `1.0` data_foundation `1.0` blockers `none`
- `IWM` `portfolio12h__iwm__bull__call__single_leg_repair__3dc6b95db3d9a6__profile_iwm-bull-c007-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `bull` min_net `617.845` min_test `829.028` strategy_fill `0.994` data_foundation `1.0` blockers `none`

## Symbol Exposure

- `IWM` strategies `3` weight `100.00%` dollars `$24999.98`

## Blocker Counts

- `fill_coverage_below_0.90`: `544`
- `min_net_pnl_not_positive`: `1243`
- `test_net_pnl_not_above_0`: `1033`

Top-candidate blocker counts:

- `fill_coverage_below_0.90`: `22`
- `min_net_pnl_not_positive`: `71`
- `test_net_pnl_not_above_0`: `59`

## Regime Summary

- `bull` candidates `8` eligible `1` best `portfolio12h__iwm__bull__call__single_leg_repair__d927c9c9c680bc__profile_iwm-bull-c006-iwm-e0-x60-entry-liquidity-first-research-only` best_fill `0.9896` best_status `research_only_blocked`
- `bear` candidates `152` eligible `2` best `portfolio12h__iwm__bear__put__single_leg_repair__89134067786703__profile_iwm-rescue-c001-012-iwm-e0-x60-nearest-contract` best_fill `0.9056` best_status `eligible_for_promotion_review`
- `choppy` candidates `1112` eligible `5` best `portfolio12h__iwm__choppy__put__debit_put_vertical__4049584f1b04d5__profile_iwm-rescue-c145-156-iwm-e0-x60-entry-liquidity-first-research-only` best_fill `0.7489` best_status `research_only_blocked`

## Data Repair Targets

- `IWM` `portfolio12h__iwm__choppy__put__debit_put_vertical__981bf7626eaa9e__profile_iwm-bc-red-c067-072-iwm-e0-x60-entry-liquidity-first-research-only` family `debit_put_vertical` regime `choppy` score `94203.6995` blockers `fill_coverage_below_0.90`

## Strategy Redesign Targets

- `IWM` `portfolio12h__iwm__choppy__put__debit_put_vertical__4049584f1b04d5__profile_iwm-rescue-c145-156-iwm-e0-x60-entry-liquidity-first-research-only` family `debit_put_vertical` regime `choppy` strategy_fill `0.7489` data_foundation `0.9106` blockers `fill_coverage_below_0.90`
- `IWM` `portfolio12h__iwm__choppy__put__debit_put_vertical__e5179504877234__profile_iwm-rescue-c109-120-iwm-e0-x60-entry-liquidity-first-research-only` family `debit_put_vertical` regime `choppy` strategy_fill `0.7796` data_foundation `0.9102` blockers `fill_coverage_below_0.90`
- `IWM` `portfolio12h__iwm__choppy__put__debit_put_vertical__29bc370cf153c5__profile_iwm-rescue-c133-144-iwm-e0-x60-entry-liquidity-first-research-only` family `debit_put_vertical` regime `choppy` strategy_fill `0.7532` data_foundation `0.9106` blockers `fill_coverage_below_0.90, min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__choppy__put__debit_put_vertical__7abd9b752d8a77__profile_iwm-rescue-c097-108-iwm-e0-x60-entry-liquidity-first-research-only` family `debit_put_vertical` regime `choppy` strategy_fill `0.7837` data_foundation `0.9102` blockers `fill_coverage_below_0.90, min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__465963de9a2fc7__profile_iwm-rescue-c085-096-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `1.0` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__bear__put__single_leg_repair__45686d942d35c9__profile_iwm-rescue-c001-012-iwm-e0-x60-nearest-contract` family `single_leg_repair` regime `bear` strategy_fill `0.9056` data_foundation `1.0` blockers `min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__bull__call__single_leg_repair__d927c9c9c680bc__profile_iwm-bull-c006-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `bull` strategy_fill `0.9896` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__bear__put__single_leg_repair__45686d942d35c9__profile_iwm-rescue-c001-012-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `bear` strategy_fill `1.0` data_foundation `1.0` blockers `min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__bear__put__single_leg_repair__5884e22d380daf__profile_iwm-bc-red-c001-006-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `bear` strategy_fill `1.0` data_foundation `1.0` blockers `min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__0cbc3d76fd53c6__profile_iwm-rescue-c085-096-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `0.991` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__54a1b5aac281a3__profile_iwm-rescue-c073-084-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `0.991` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__57240dc9a918cd__profile_iwm-rescue-c061-072-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `1.0` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__d1878aa0483682__profile_iwm-rescue-c085-096-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `1.0` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__bull__call__single_leg_repair__0b7b67e4df9a20__profile_iwm-bull-c001-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `bull` strategy_fill `1.0` data_foundation `1.0` blockers `min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__21eae5a5f5cfce__profile_iwm-rescue-c001-012-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `1.0` data_foundation `1.0` blockers `min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__bear__put__single_leg_repair__c7895e06f5c278__profile_iwm-rescue-c001-012-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `bear` strategy_fill `1.0` data_foundation `1.0` blockers `min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__bear__put__credit_call_vertical__4b9d19d03f617e__profile_iwm-rescue-c001-012-iwm-e0-x60-nearest-contract` family `credit_call_vertical` regime `bear` strategy_fill `0.8722` data_foundation `1.0` blockers `fill_coverage_below_0.90, min_net_pnl_not_positive`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__240dc5c79eeb68__profile_iwm-rescue-c073-084-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `0.991` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__f95e1e2d478627__profile_iwm-rescue-c049-060-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `0.9889` data_foundation `1.0` blockers `test_net_pnl_not_above_0`
- `IWM` `portfolio12h__iwm__choppy__call__single_leg_repair__6040b5e6bbcf2d__profile_iwm-rescue-c085-096-iwm-e0-x60-entry-liquidity-first-research-only` family `single_leg_repair` regime `choppy` strategy_fill `0.9926` data_foundation `1.0` blockers `test_net_pnl_not_above_0`

## Next Actions

- Review promotion-review candidates against the strategy-governance policy before any activation discussion.
- Require a clean broker-audited paper session before control-plane promotion beyond research review.
- Do not modify live manifests, strategy selection, or risk policy from this packet alone.

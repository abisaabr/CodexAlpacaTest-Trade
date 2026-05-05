# QQQ Regime Paper-Readiness Status - 2026-05-05

## Scope

Goal: produce a QQQ regime-complete governed-validation packet with at least one bull, bear, and choppy strategy before expanding the same process to SPY, then IWM, then the remaining tickers.

Hard rules still in force:

- Broker-facing trading: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Do not lower `fill_coverage >= 0.90`
- Do not treat a strategy as promoted unless the generated promotion-review packet says `eligible_for_promotion_review`

## Current QQQ State

Bull is formally ready for governed validation review.

- Wave: `ticker365_qqq_markettime_refine_20260505T0630Z`
- Candidate: `portfolio12h__qqq__bull__call__single_leg_repair__74e2243aff4d9b`
- Regime: `bull`
- Instrument: QQQ call single-leg repair
- Net PnL: `4796.579`
- Test Net PnL: `4020.821`
- Fill coverage: `0.9948`
- Option trades: `192`
- Promotion packet decision: `ready_for_governed_validation_review`
- Local packet: `reports/gcp_research/ticker365_qqq_markettime_refine_20260505T0630Z/promotion_packet_c001/research_promotion_review_packet.json`
- GCS packet: `gs://codexalpaca-control-us/research_results/ticker365_qqq_markettime_refine_20260505T0630Z/promotion_review_c001/research_promotion_review_packet.json`

Bear is formally ready for governed validation review.

- Wave: `ticker365_qqq_premium_exit_20260505T0745Z`
- Candidate: `portfolio12h__qqq__bear__put__single_leg_repair__7f3cb2963a2dab`
- Regime: `bear`
- Instrument: QQQ put single-leg repair
- Net PnL: `1597.188`
- Test Net PnL: `143.787`
- Fill coverage: `1.0`
- Option trades: `134`
- Promotion packet decision: `ready_for_governed_validation_review`
- Local packet: `reports/gcp_research/ticker365_qqq_premium_exit_20260505T0745Z/promotion_packet_c013/research_promotion_review_packet.json`
- GCS packet: `gs://codexalpaca-control-us/research_results/ticker365_qqq_premium_exit_20260505T0745Z/promotion_review_c013/research_promotion_review_packet.json`

Choppy is not ready yet.

- Active wave: `ticker365_qqq_choppy_refine_20260505T0855Z`
- Candidate count: `48`
- Active tranche: candidates `1-16`
- Running workers: `qqqfam-micro-c001-20260505r` through `qqqfam-micro-c016-20260505r`
- Latest partial aggregate: `gs://codexalpaca-control-us/research_results/ticker365_qqq_choppy_refine_20260505T0855Z/aggregate/qqq_choppy_refine_partial_c001_c016_latest.json`
- First completed candidate had healthy fill coverage but failed economics: `fill_coverage=0.9794`, `net_pnl=-953.175`, `test_net_pnl=1391.889`, `recommendation=quarantine_option_economics`

## Operational Trail

Old terminated QQQ micro-shard instances from suffixes `20260505p` and `20260505q` were deleted after their artifacts were mirrored to GCS. Cleanup log:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_choppy_refine_20260505T0855Z/ops/ops_cleanup_old_qqq_micro_20260505T085656Z.txt`

Current recognized running QQQ research VMs are suffix `20260505r`. They are research-only workers.

## Next Steps

1. Let QQQ choppy refine candidates `1-16` finish and rebuild the partial aggregate.
2. If any choppy candidate is eligible-like, build a formal promotion-review packet for that candidate.
3. If no choppy candidate clears economics, diagnose the best near-miss from trade economics and launch the next small choppy tranche instead of expanding tickers.
4. Once bull, bear, and choppy all have formal eligible packets, build one combined QQQ regime packet.
5. Only after QQQ is regime-complete, repeat the same sequence for SPY and then IWM.


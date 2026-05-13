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

QQQ now has one bull, one bear, and one choppy option-native strategy formally ready for governed validation review. This is still not broker-facing trading approval; it is the controlled packet required before broker-free shadow validation and an operator-gated paper session.

- Combined packet: `reports/gcp_research/qqq_regime_complete_option_native_20260505/promotion_packet/research_promotion_review_packet.json`
- GCS packet: `gs://codexalpaca-control-us/research_results/qqq_regime_complete_option_native_20260505/promotion_packet/research_promotion_review_packet.json`
- Decision: `ready_for_governed_validation_review`
- Eligible count: `3`

Bull:

- Wave: `ticker365_qqq_bull_premium_exit_20260505T0910Z`
- Candidate: `portfolio12h__qqq__bull__call__single_leg_repair__5cc1e1ac7bdf2f`
- Regime: `bull`
- Instrument: QQQ call single-leg repair
- Net PnL: `2901.476`
- Test Net PnL: `2495.348`
- Fill coverage: `0.9948`
- Option trades: `192`
- Promotion packet decision: `ready_for_governed_validation_review`

Choppy:

- Wave: `ticker365_qqq_choppy_refine_20260505T0855Z`
- Candidate: `portfolio12h__qqq__choppy__call__single_leg_repair__38b24047758471`
- Regime: `choppy`
- Instrument: QQQ call single-leg repair
- Net PnL: `2198.67`
- Test Net PnL: `1830.45`
- Fill coverage: `0.9794`
- Option trades: `190`
- Promotion packet decision: `ready_for_governed_validation_review`

Bear:

- Wave: `ticker365_qqq_premium_exit_20260505T0745Z`
- Candidate: `portfolio12h__qqq__bear__put__single_leg_repair__7f3cb2963a2dab`
- Regime: `bear`
- Instrument: QQQ put single-leg repair
- Net PnL: `1597.188`
- Test Net PnL: `143.787`
- Fill coverage: `1.0`
- Option trades: `134`
- Promotion packet decision: `ready_for_governed_validation_review`

## Runner Wiring

- Promotion manifest: `config/promotion_manifests/qqq_regime_complete_governed_validation_20260505.yaml`
- Broker-free portfolio config: `config/qqq_regime_complete_paper_portfolio.yaml`
- Launch-pack command: `python scripts/build_qqq_paper_launch_pack.py`
- Startup-preflight command: `python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/qqq_regime_complete_paper_portfolio.yaml --startup-preflight --no-submit-paper-orders`
- Broker-facing paper order submission remains blocked unless an operator explicitly runs with `--submit-paper-orders`.

Broker-free launch-pack result:

- Local directory: `reports/gcp_research/qqq_regime_complete_paper_launch_pack_20260505/`
- GCS directory: `gs://codexalpaca-control-us/research_results/qqq_regime_complete_option_native_20260505/paper_launch_pack/`
- Decision: `ready_for_broker_free_shadow_validation_only`
- Broker-free shadow ready: `true`
- Broker-facing paper ready: `false`
- Checks: `promotion_manifest_scope`, `eligible_strategy_count`, `runner_audit_identity_fields`, `strategy_schema_validation`, `runner_semantics_alignment`, and `broker_adapter_dry_run_order_shapes` all passed.

Startup preflight result at `2026-05-05T05:29:06-04:00`:

- Command: `python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/qqq_regime_complete_paper_portfolio.yaml --startup-preflight --no-submit-paper-orders`
- Status: `startup_preflight_pending`
- Submit paper orders: `false`
- Broker cleanup allowed: `false`
- Would allow trading: `false`
- Account/broker checks reached: buying power, equity, broker positions, and open orders were read successfully.
- Pending reason: `QQQ stock frame not ready yet`, expected before RTH because no fresh stock frame exists at 5:29 AM ET.
- GCS result: `gs://codexalpaca-control-us/research_results/qqq_regime_complete_option_native_20260505/paper_launch_pack/startup_preflight_no_orders_20260505.json`

## Operational Trail

Old terminated QQQ micro-shard instances from suffixes `20260505p` and `20260505q` were deleted after their artifacts were mirrored to GCS. Cleanup log:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_choppy_refine_20260505T0855Z/ops/ops_cleanup_old_qqq_micro_20260505T085656Z.txt`

Terminated QQQ micro-shard instances from suffixes `20260505r` and `20260505s` were also deleted after their outputs were aggregated and mirrored. Cleanup log:

- `gs://codexalpaca-control-us/research_results/qqq_regime_complete_option_native_20260505/ops/ops_cleanup_qqq_micro_20260505.txt`

Remaining GCP instances after cleanup are the pre-existing terminated runner/controller machines:

- `multi-ticker-trader-v1`
- `paper-ready-controller-20260504qa`
- `vm-execution-paper-01`

No broker-facing paper session has been started by this packet.

## Next Steps

1. Re-run startup preflight near RTH with `--no-submit-paper-orders` so the QQQ stock frame can be populated.
2. If startup preflight passes, run broker-free shadow validation for QQQ with no order submission.
3. Operator must explicitly approve before any broker-facing paper order submission.
4. After QQQ is broker-free preflight clean, repeat the same fill-first process for SPY and then IWM.

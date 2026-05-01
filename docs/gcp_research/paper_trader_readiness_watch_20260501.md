# Paper Trader Readiness Watch - 2026-05-01

Status updated UTC: `2026-05-01T03:41:24Z`

## Current Decision

Decision: `blocked_waiting_for_evidence`

The paper VM is running and the startup-preflight command is available, but the final overnight portfolio aggregate promotion packet has not landed yet. The correct action is to keep monitoring and not arm paper orders or change manifests until the generated final promotion packet exists, has eligible candidates, and remains research-only with no manifest or risk-policy effect.

At `2026-05-01T03:41:24Z`, the readiness monitor reports `decision=blocked_waiting_for_evidence`, `running_wave_vm_count=10`, and `aggregate_artifact_count=0`.

The monitor also sees the corrected profile-isolated partial aggregate. That packet has 8 eligible profile-level candidates and 4 unique eligible base candidates, but it is evidence-only and explicitly does not unblock the final aggregate requirement.

An additive `top_n=40` fastlane is now running because the original deep option-aware workers are compute-active but single-core/sequential. The fastlane is also research-only and cannot unblock launch by itself; it feeds the same governed-validation promotion gates and writes a separate aggregate packet before RTH.

## Active Watch

Readiness monitor script: `scripts/monitor_paper_trader_readiness.py`

GCS readiness prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/paper_readiness/`

Latest readiness pointer: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/paper_readiness/paper_trader_readiness_latest.json`

The monitor is read-only. It checks GCP VM state, aggregate artifacts, promotion packet safety, local paper config presence, and startup-preflight availability. It does not start trading, submit paper orders, modify live manifests, or change risk policy.

## Current Gates

- `paper_vm_running`: passed
- `paper_vm_validation_only_label`: passed
- `overnight_workers_active_or_complete`: passed, four option-aware workers plus aggregator are running
- `overnight_promotion_packet_present`: failed, waiting for aggregate output
- `overnight_packet_has_eligible_candidates`: failed, waiting for aggregate output
- `overnight_packet_safety_scope`: failed, waiting for aggregate output
- `profile_isolated_partial_aggregate_present`: passed as warning/evidence only
- `profile_isolated_partial_aggregate_has_candidates`: passed as warning/evidence only, 4 unique base candidates
- `profile_isolated_partial_aggregate_safety_scope`: passed as warning/evidence only
- `local_paper_config_present`: passed
- `startup_preflight_available`: passed
- `qqq_fallback_governed_candidates`: passed as a warning/fallback only

## Fastlane Evidence Path

Fastlane packet:

`gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/inputs/fastlane_top40_packet_v2/portfolio_overnight_12h_tournament_packet.json`

Workers:

- `fastlane-top40-a`: `AAPL AMD AMZN INTC IWM`
- `fastlane-top40-b`: `META MSFT NVDA SPY TSLA`
- `fastlane-top40-c`: `AVGO GOOGL MU NFLX ORCL`
- `fastlane-top40-d`: `PLTR QQQ TSM XLE XOM`
- `fastlane-top40-agg-1040z`: wakes around `2026-05-01T10:40:00Z` and writes `aggregate_fastlane_top40_20260501/`

This path uses `top_n=40`, `test_date_count=20`, both `nearest_contract` and `entry_liquidity_first_research_only`, and the same `fill_coverage >= 0.90`, `min_option_trades >= 20`, `min_test_net_pnl >= 0`, and `min_net_pnl >= 0` gates.

## Profile-Isolated Partial Aggregate

Promotion packet:

`gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_partial_profile_20260501T031218Z/promotion_packet_deduped_45dd637/promotion_packet/research_promotion_review_packet.json`

Portfolio report:

`gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_partial_profile_20260501T031218Z/portfolio_report/portfolio_overnight_12h_aggregate/research_portfolio_report.json`

This evidence is research-only, non-broker-facing, and has no live-manifest or risk-policy effect. It is useful for human review, but launch remains blocked until `finalagg2-1255z` writes the final aggregate packet to `aggregate/`.

## Paper VM

Paper VM: `vm-execution-paper-01`

Zone: `us-east1-b`

Status at snapshot: `RUNNING`

Mode labels indicate validation stage. This is good for readiness, but it is not permission to submit orders.

## Commands

Preflight only, no orders:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_ticker_paper_portfolio.yaml --startup-preflight --no-submit-paper-orders
```

Paper order launch requires separate explicit operator approval:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_ticker_paper_portfolio.yaml --submit-paper-orders
```

## Next Action

Keep the 15-minute readiness monitor running. When either the fastlane or final promotion packet appears, inspect `eligible_for_promotion_review_count`, `unique_eligible_base_candidate_count`, `review_candidates`, `broker_facing`, `live_manifest_effect`, and `risk_policy_effect`. If a packet is eligible and safe, prepare an operator-reviewed launch packet; do not automatically alter the live manifest or start the trader. If an aggregator runs before all intended workers have uploaded artifacts, rerun that aggregator after the missing lane completes.

# Paper Trader Readiness Watch - 2026-05-01

Status updated UTC: `2026-05-01T02:29:45Z`

## Current Decision

Decision: `blocked_waiting_for_evidence`

The paper VM is running and the startup-preflight command is available, but the overnight portfolio aggregate promotion packet has not landed yet. The correct action is to keep monitoring and not arm paper orders or change manifests until the generated promotion packet exists, has eligible candidates, and remains research-only with no manifest or risk-policy effect.

## Active Watch

Readiness monitor script: `scripts/monitor_paper_trader_readiness.py`

GCS readiness prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/paper_readiness/`

Latest readiness pointer: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/paper_readiness/paper_trader_readiness_latest.json`

The monitor is read-only. It checks GCP VM state, aggregate artifacts, promotion packet safety, local paper config presence, and startup-preflight availability. It does not start trading, submit paper orders, modify live manifests, or change risk policy.

## Current Gates

- `paper_vm_running`: passed
- `paper_vm_validation_only_label`: passed
- `overnight_workers_active_or_complete`: passed
- `overnight_promotion_packet_present`: failed, waiting for aggregate output
- `overnight_packet_has_eligible_candidates`: failed, waiting for aggregate output
- `overnight_packet_safety_scope`: failed, waiting for aggregate output
- `local_paper_config_present`: passed
- `startup_preflight_available`: passed
- `qqq_fallback_governed_candidates`: passed as a warning/fallback only

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

Keep the 15-minute readiness monitor running. When the promotion packet appears, inspect `eligible_for_promotion_review_count`, `review_candidates`, `broker_facing`, `live_manifest_effect`, and `risk_policy_effect`. If the packet is eligible and safe, prepare an operator-reviewed launch packet; do not automatically alter the live manifest or start the trader.

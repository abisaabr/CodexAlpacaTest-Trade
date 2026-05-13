# QQQ Governed Shadow Validation Handoff

Generated: 2026-05-01

## Goal

Move the three governed QQQ option-native candidates from research review into broker-free runner shadow validation while preserving GitHub and GCS as the canonical handoff surfaces.

## Scope

- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Universe: `QQQ` only
- Strategies: bull long call, bear call credit spread, choppy iron condor

## Canonical Files

- Shadow config: `config/multi_ticker_qqq_governed_shadow_validation.yaml`
- Shadow manifest: `config/strategy_manifests/qqq_option_native_governed_shadow_validation_20260501.yaml`
- Packet builder: `scripts/build_qqq_shadow_validation_packet.py`
- Promotion manifest: `config/promotion_manifests/qqq_option_native_governed_validation_20260430.yaml`

## Coordination

The shadow config uses a GCS generation-match lease:

`gs://codexalpaca-control-us/execution_locks/qqq_governed_shadow_validation_20260501/ownership_lease.json`

Both machines should use the same config and GCS lease so only one no-order shadow runner controls the validation lane at a time.

## Commands

Build/refresh the canonical packet:

```powershell
python scripts\build_qqq_shadow_validation_packet.py
```

Broker-free startup preflight:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_ticker_qqq_governed_shadow_validation.yaml --startup-preflight --no-submit-paper-orders
```

Broker-free one-cycle shadow run:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_ticker_qqq_governed_shadow_validation.yaml --run-once --no-submit-paper-orders
```

## Required Evidence Before Paper Canary

- Shadow packet decision is `ready_for_no_order_gcp_shadow_validation`.
- Startup preflight passes on the target GCP VM with `--no-submit-paper-orders`.
- Runner event logs preserve `candidate_variant_id`, `source_strategy_id`, `research_profile`, and `runner_semantics_status`.
- Any shadow signal/order event is attributable to the QQQ-only governed shadow manifest.
- Human operator explicitly approves a separate paper-canary PR.

## Hard Stop

Do not start a broker-facing paper session from this handoff. This phase is shadow validation only.

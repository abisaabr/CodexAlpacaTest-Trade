# QQQ Timing Smoke Wave - 2026-05-04

## Purpose

Run a small QQQ-only diagnostic in parallel with the full QQQ timing redesign wave. The full wave remains canonical for promotion review, but the smoke wave gives a faster read on whether strict entry/exit timing profiles are improving strategy fill coverage.

## Scope

- Wave ID: `ticker365_qqq_timing_smoke_20260504T1345Z`
- GCS prefix: `gs://codexalpaca-control-us/research_results/ticker365_qqq_timing_smoke_20260504T1345Z`
- Symbol: `QQQ`
- Dataset: QQQ dense 365-day next-trading-day ATM +/- 5 selected-contract dataset
- Top N: `12`
- Selectors: `nearest_contract`, `entry_liquidity_first_research_only`
- Lag profiles: `0:60`, `60:240`, `120:390`
- Entry lookup: `first_bar_at_or_after_entry_within_lag`
- Max entry staleness: `0`

## Workers

- `ticker365-smoke-qqq-e0x60-20260504`
- `ticker365-smoke-qqq-e60x240-20260504`
- `ticker365-smoke-qqq-e120x390-20260504`

## Guardrails

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Do not promote from the smoke wave alone; use it only to guide the full QQQ redesign wave and next engineering patch.

# QQQ 365d Canonical Backtest - 2026-05-01

## Purpose

Run a dedicated research-only 365-day QQQ option-native portfolio backtest with its own portfolio report, promotion-review packet, and `$25,000` to `$300,000` growth projection.

This is separate from the all-ticker overnight wave so QQQ evidence can be reviewed cleanly without waiting for the remaining ticker lanes.

## GCS Prefix

`gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/`

## Launch Status

- Instance: `qqq-365d-canonical-20260501-2250z`
- Zone: `us-central1-a`
- Machine type: `e2-standard-4`
- Status at first check: `RUNNING`
- First observed phase: `staging_source`
- Startup status file: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/status/qqq_365d_status.json`

## Dataset

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/`
- Selected contracts: `gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/`
- Option bars: `gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/underlying=QQQ/`

## Profiles

- `fixed_e330_x390_lag15_15` with `nearest_contract`
- `first_common_e330_x390_lag15_15` with `nearest_contract`
- `fixed_e300_x390_lag30_60` with `nearest_contract`
- `first_common_e300_x390_lag30_60` with `nearest_contract`
- `fixed_e330_x390_lag15_15` with `entry_liquidity_first_research_only`
- `first_common_e330_x390_lag15_15` with `entry_liquidity_first_research_only`

## Gates

- `fill_coverage_gate`: `0.90`
- `min_option_trades`: `20`
- `min_test_net_pnl`: `0`
- `candidate_identity_mode`: `variant_profile`
- `initial_cash`: `25000`
- `target_equity`: `300000`

## Outputs

- Portfolio report: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/portfolio_report/`
- Promotion packet: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/promotion_packet/`
- Growth projection: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/growth_projection/`
- Status: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/status/qqq_365d_status.json`

## Hard Rules

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Promotion means governed validation review only.

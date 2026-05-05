# QQQ Choppy Reversion Wave - 2026-05-05

## Purpose

The QQQ premium-exit short-premium choppy tranche showed high fill coverage but deeply negative economics. This wave pivots choppy research away from selling premium and toward directional mean-reversion scalps inside range-bound conditions.

## Engineering Change

`VariantStockProxyStrategy` now supports range-side filters:

- `range_entry_side=lower_band` emits bullish range-reversion entries near the lower side of a range.
- `range_entry_side=upper_band` emits bearish range-reversion entries near the upper side of a range.
- `range_edge_pct` controls how far from the midpoint a signal must be.

The option-aware backtester cache key includes these range-side parameters, so different choppy reversion windows are not accidentally collapsed into the same stock-trade source.

## New Wave

- Wave ID: `ticker365_qqq_choppy_reversion_20260505T0830Z`
- Target symbol: `QQQ`
- Target regime: `choppy`
- Candidate count: `48`
- Broker-facing: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

Local inputs:

- `reports/gcp_research/ticker365_qqq_choppy_reversion_20260505T0830Z/inputs/qqq_choppy_reversion_variants.jsonl`
- `reports/gcp_research/ticker365_qqq_choppy_reversion_20260505T0830Z/inputs/qqq_choppy_reversion_option_queue.json`
- `reports/gcp_research/ticker365_qqq_choppy_reversion_20260505T0830Z/inputs/qqq_choppy_reversion_manifest.json`

Expected GCS root:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_choppy_reversion_20260505T0830Z/`

## Promotion Rule

Do not promote any strategy unless the generated promotion-review packet says `eligible_for_promotion_review`.

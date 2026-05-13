# QQQ Choppy Refine Wave - 2026-05-05

## Source Finding

The first QQQ choppy-reversion tranche found one near-miss:

- `portfolio12h__qqq__choppy__call__single_leg_repair__2d46549a60d4a2`
- `fill_coverage=0.9894`
- `net_pnl=-59.883`
- `train_net_pnl=-771.46`
- `test_net_pnl=711.577`
- `profit_factor=0.9981`

Trade-row diagnostics showed the first 90 minutes after open were the main drag, while entries roughly 90-150 minutes after open were positive in aggregate.

## New Wave

- Wave ID: `ticker365_qqq_choppy_refine_20260505T0855Z`
- Target symbol: `QQQ`
- Target regime: `choppy`
- Candidate count: `48`
- Design: lower-band long-call reversion only
- Broker-facing: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

This wave tightens the near-miss by varying:

- Entry window: `75-150`, `90-150`, `90-180`, `105-165` minutes after open
- Range edge: `0.0005`, `0.0010`
- Max signals per day: `1`, `2`
- Option exits: `target30_stop12`, `target30_stop14`, `target35_stop16`

Expected GCS root:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_choppy_refine_20260505T0855Z/`

## Promotion Rule

Do not promote any strategy unless the generated promotion-review packet says `eligible_for_promotion_review`.

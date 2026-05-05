# QQQ Premium-Exit Redesign Wave - 2026-05-05

## Purpose

The corrected QQQ market-time wave completed all 50 candidates and produced one governed-validation-review bull candidate:

- `portfolio12h__qqq__bull__call__single_leg_repair__74e2243aff4d9b`
- `fill_coverage=0.9948`
- `net_pnl=4796.579`
- `train_net_pnl=775.758`
- `test_net_pnl=4020.821`
- `recommendation=research_candidate_liquidity_first_review`

It did not produce eligible bear or choppy candidates. The blocker is no longer raw option data or entry/exit bar coverage. Bear/choppy candidates mostly have adequate fill but fail economics, especially OOS/test PnL.

## Engineering Change

The backtester now supports an opt-in research-only option-native exit mode:

- `option_exit_mode=premium_target_stop`
- Credit structures can exit on premium capture or credit/risk stop.
- Debit structures can exit on option PnL target or stop.
- Existing variants keep the old stock-proxy exit unless the parameter is explicitly set.

The stock-trade cache key was also tightened so variants with different stock-signal windows, range filters, trend filters, cooldowns, or daily signal caps do not accidentally share source trades.

## New Wave

- Wave ID: `ticker365_qqq_premium_exit_20260505T0745Z`
- Target symbol: `QQQ`
- Target regimes: `bear`, `choppy`
- Candidate count: `68`
- Broker-facing: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

Local inputs:

- `reports/gcp_research/ticker365_qqq_premium_exit_20260505T0745Z/inputs/qqq_premium_exit_variants.jsonl`
- `reports/gcp_research/ticker365_qqq_premium_exit_20260505T0745Z/inputs/qqq_premium_exit_option_queue.json`
- `reports/gcp_research/ticker365_qqq_premium_exit_20260505T0745Z/inputs/qqq_premium_exit_manifest.json`

Expected GCS root:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_premium_exit_20260505T0745Z/`

## Promotion Rule

Do not promote any strategy unless the generated promotion-review packet says `eligible_for_promotion_review`.

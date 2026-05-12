# IWM Structure-Aware Diagnostic - 2026-05-08

## Scope

This diagnostic reviews the IWM worker from the structure-aware bear/choppy rerun. It is research-only. No paper config, live manifest, broker state, or global risk policy was changed.

## Source Artifacts

- Source commit reviewed: `02c7fd9`
- Rerun GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_structure_aware_rerun_20260508T1745ET/`
- Worker: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/workers/iwm_regime_rescue_c121_144`
- Worker portfolio report: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/workers/iwm_regime_rescue_c121_144/portfolio_report/iwm_regime_rescue_c121_144_portfolio_report/research_portfolio_report.json`
- Worker promotion packet: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/workers/iwm_regime_rescue_c121_144/promotion_packet/iwm_regime_rescue_c121_144_promotion_packet/research_promotion_review_packet.json`
- Generated diagnostic summary: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/iwm_structure_aware_diagnostic/iwm_structure_aware_diagnostic_summary.json`
- Candidate blocker CSV: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/iwm_structure_aware_diagnostic/iwm_structure_candidate_blockers.csv`
- Fill failure CSV: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/iwm_structure_aware_diagnostic/iwm_structure_fill_failures.csv`

## Promotion Result

- Packet decision: `research_only_blocked`
- Candidate count: `72`
- Eligible for promotion review: `0`
- Required regimes: `bull`, `bear`, `choppy`
- Eligible regimes: none
- Fill coverage gate: `0.90`

Blocker counts:

- `min_net_pnl_not_positive`: `72`
- `test_net_pnl_not_above_0`: `59`
- `fill_coverage_below_0.90`: `16`

## Diagnostic Summary

The structure-aware IWM rerun is no longer primarily a raw data coverage problem for the best rows. It is an edge problem.

- Total rows reviewed: `72`
- Rows with fill coverage below `0.90`: `16`
- Rows with positive full-period PnL: `0`
- Rows with positive test PnL: `13`
- Rows with both positive full-period and positive test PnL: `0`
- Rows clearing fill plus full/test PnL: `0`

The best test-period row was an IWM choppy debit-put-vertical candidate:

- Candidate: `portfolio12h__iwm__choppy__put__debit_put_vertical__29bc370cf153c5`
- Profile: `iwm_regime_rescue_c121_144_iwm_e0_x60_entry_liquidity_first_research_only`
- Fill coverage: `0.9404`
- Data foundation coverage: `0.9915`
- Option trades: `221`
- Full-period PnL: `-13827.828`
- Test PnL: `14195.993`
- Blocker: `min_net_pnl_not_positive`

This is not promotable. The strong recent test PnL is outweighed by negative full-period evidence.

## Fill Failure Details

Fill failures still matter, but they are secondary for this shard because even fill-clear candidates are unprofitable full-period.

Failure counts across the worker:

- `no_entry_bar`: `616`
- `invalid_credit_structure`: `268`
- `no_exit_bar`: `143`
- `no_selected_contract`: `122`

By profile:

- `e0_x60`: `705` failures, dominated by `no_entry_bar` (`540`) and `no_selected_contract` (`118`).
- `e10_x60`: `199` failures, dominated by `invalid_credit_structure` (`96`) and `no_exit_bar` (`59`).
- `e30_x120`: `245` failures, dominated by `invalid_credit_structure` (`164`) and `no_exit_bar` (`45`).

Interpretation:

- The `10` and `30` minute entry-lag profiles repaired much of the strict zero-lag `no_entry_bar` problem.
- The repaired profiles still did not produce any full-period profitable candidate.
- Iron condor and iron butterfly rows need separate structure-pricing diagnostics because `invalid_credit_structure` dominates those families.
- The earlier selected-contract gap diagnostic for IWM bear credit spreads showed `no_valid_wing_contract`, meaning the multi-leg universe can still be too narrow for some IWM credit structures.

## Recommendation

- Do not promote any IWM candidate from this shard.
- Do not rerun the same IWM choppy grid unchanged; it already answers the fill question and fails on full-period economics.
- If continuing IWM, split work into two targeted waves:
  - IWM choppy debit-put-vertical exit repair around `29bc370cf153c5`, with stricter train/test validation and capped loser clusters.
  - IWM credit-spread/condor structure repair, focused on selected-contract universe width and `invalid_credit_structure`, not broader signal sweeps.
- Keep IWM out of the paper runner unless a generated packet clears the relevant per-regime governed-validation review gates.

## State Changes

- Eligible for governed promotion review: no.
- Paper-runner state changed: no.
- Paper config changed: no.
- Live manifest changed: no.
- Risk policy changed: no.

# IWM Choppy Fill Diagnostic - 2026-05-05

## Decision

`research_only_diagnostic_prepared`

The active QQQ/SPY PAPER session was left untouched. This diagnostic uses existing GCS artifacts from the completed IWM bear/choppy redesign wave and does not change broker state, paper symbols, live manifests, risk policy, or promotion gates.

IWM remains excluded from the active paper runner because the generated promotion packets show `0` eligible candidates.

## Source Artifacts

- Wave ID: `ticker365_iwm_bear_choppy_redesign_20260505T1505Z`
- Worker: `iwm_bc_red_c067_072`
- Candidate summary: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/workers/iwm_bc_red_c067_072/reports/research_wave/iwm_bc_red_c067_072_iwm_e0_x60_entry_liquidity_first_research_only/iwm_bc_red_c067_072_iwm_e0_x60_entry_liquidity_first_research_only/option_aware_candidate_summary.json`
- Fill failures: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/workers/iwm_bc_red_c067_072/reports/research_wave/iwm_bc_red_c067_072_iwm_e0_x60_entry_liquidity_first_research_only/iwm_bc_red_c067_072_iwm_e0_x60_entry_liquidity_first_research_only/option_aware_fill_failures.json`
- Promotion packet: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/workers/iwm_bc_red_c067_072/promotion_packet/iwm_bc_red_c067_072_promotion_packet/research_promotion_review_packet.json`

New reusable local tool:

- `scripts/analyze_option_fill_failures.py`

Generated diagnostic report:

- GCS JSON: `gs://codexalpaca-control-us/research_results/iwm_choppy_fill_diagnostic_20260505/iwm_choppy_debit_put_vertical_fill_diagnostic_20260505.json`
- GCS Markdown: `gs://codexalpaca-control-us/research_results/iwm_choppy_fill_diagnostic_20260505/iwm_choppy_debit_put_vertical_fill_diagnostic_20260505.md`

## Candidate Under Diagnostic

- Candidate: `portfolio12h__iwm__choppy__put__debit_put_vertical__981bf7626eaa9e`
- Strategy: `iwm__choppy__put__debit_put_vertical`
- Symbol: `IWM`
- Family: `debit_put_vertical`
- Intended regime: `choppy`
- Full-period net PnL: `15732.003`
- Test net PnL: `47262.66`
- Strategy fill coverage: `0.7587`
- Data foundation coverage: `0.8986`
- Entry bar coverage: `0.8444`
- Exit bar coverage: `1.0`
- Option trade count: `217`
- Promotion status: `research_only_blocked`
- Promotion blocker: `fill_coverage_below_0.90`

## Failure Diagnosis

The near-miss has `69` unfilled source trades. Failure counts:

- `no_entry_bar`: `40`
- `no_selected_contract`: `29`

Dominant classifications:

- `entry_bar_gap_or_entry_timing_mismatch`
- `selected_contract_universe_gap`

All `69` failure rows are `debit_put_vertical` failures and all have missing `contract_symbol` in the fill-failure row. This points away from exit policy as the primary blocker because exit bar coverage is already `1.0`. The first repair target should be vertical-leg selection/availability and strict zero-minute entry lookup, not broad raw data downloading.

## Interpretation

The blocker is not a generic lack of IWM raw option bars. The completed wave already had strong raw ladder coverage, and the same worker produced single-leg IWM choppy puts with `0.993` strategy fill coverage but negative economics. The profitable vertical structure is specifically failing because the replay cannot build and price the required debit-put-vertical structure for enough source stock trades under strict `0` minute entry semantics and the current selected-contract universe.

The current `0.7587` strategy fill coverage is too far below the `0.90` gate for governed promotion review. The gate should not be lowered.

## Next Research Micro-Wave

Run a bounded research-only IWM choppy debit-put-vertical diagnostic before any broader IWM sweep:

1. Reproduce the exact candidate with strict current semantics: entry lag `0`, exit lag `60`, entry lookup `first_bar_at_or_after_entry_within_lag`, selector `entry_liquidity_first_research_only`.
2. Split failures by `no_selected_contract` vs `no_entry_bar` and preserve source stock trade IDs through the report.
3. For `no_selected_contract`, test whether the selected-contract universe lacks the alternate vertical leg or whether the candidate asks for a spread outside the available 5x5 next-expiry ladder.
4. For `no_entry_bar`, test realistic entry-lag profiles such as `1`, `2`, and `5` minutes as diagnostics only; do not present wider lag as promotion-grade unless it matches paper execution semantics.
5. Rerun the generated promotion packet unchanged after each diagnostic variant. Promotion remains blocked unless the packet reports `eligible_for_promotion_review`.

## Safety State

- Broker-facing trading: `false`
- Paper-runner changes: `none`
- IWM paper activation: `false`
- Live manifest changes: `none`
- Risk policy changes: `none`
- Fill gate change: `none`; `fill_coverage >= 0.90` remains required

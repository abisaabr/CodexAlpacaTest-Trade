# Greek Selector Diagnostic - 2026-05-08

## Scope

This diagnostic pauses broad Greek-grid expansion and uses existing Greek replay outputs to classify why candidates fail before launching more compute.

Inputs:

- `reports/gcp_research/greek_scalping_20ticker_overnight_20260507T2215ET`
- `reports/gcp_research/qqq_spy_iwm_greek_expanded_20260507T0315Z`
- `reports/gcp_research/qqq_spy_iwm_greek_choppy_gamma_20260507T1355Z`
- `reports/gcp_research/qqq_spy_iwm_greek_research_20260507T0215Z`

Output:

- `reports/gcp_research/greek_selector_diagnostic_20260508/greek_selector_diagnostic_summary.json`
- `reports/gcp_research/greek_selector_diagnostic_20260508/greek_selector_diagnostic_rows.csv`
- `reports/gcp_research/greek_selector_diagnostic_20260508/greek_selector_diagnostic_groups.csv`
- `reports/gcp_research/greek_selector_diagnostic_20260508/greek_selector_diagnostic.md`

GCS mirror:

- `gs://codexalpaca-control-us/gcp_research/greek_selector_diagnostic_20260508/`

## Result

Candidate summaries scanned:

- 720 candidate-summary files
- 44,892 candidate rows
- fill coverage gate: 0.90
- minimum trades: 20

Dominant cause counts:

- `bad_dte_or_strike_availability`: 35,923
- `missing_option_price_count`: 5,249
- `too_strict_delta_targeting`: 2,875
- `selected_contract_universe_gap`: 835
- `exit_bar_gap_or_exit_policy_mismatch`: 4
- `bad_exits`: 4
- `entry_bar_gap_or_entry_timing_mismatch`: 2

Cause flags are intentionally overlapping. A row can be both a same-day DTE/strike miss and a strict delta miss. Flag counts:

- `missing_option_price_count`: 44,888
- `selected_contract_universe_gap`: 44,736
- `bad_dte_or_strike_availability`: 39,985
- `too_strict_delta_targeting`: 35,452
- `entry_bar_gap_or_entry_timing_mismatch`: 28,660
- `exit_bar_gap_or_exit_policy_mismatch`: 27,881
- `too_few_trades`: 26,584
- `bad_exits`: 5,388
- `missing_greek_snapshot`: 378

## Interpretation

The broad Greek grids are failing mainly before exits matter. The dominant failure is selector availability: the requested DTE/strike/delta structures are often not present in the selected-contract universe or do not have executable option prices under paper-parity lookup.

The next Greek tests should not be broader grids. They should be selector diagnostics:

- Compare `same_day` versus `next_expiry` with the same symbol, regime, family, timing, and exit profile.
- Relax delta bands around targets before adding new families.
- Separate single-leg selector availability from multi-leg strike construction.
- Keep choppy put-side variants out of PAPER until runtime supports upper-band put reversion semantics.
- Only tune exits after fill coverage and trade count are no longer the primary blockers.

## Promotion Governance

Per-regime governed-validation review is now explicit:

- `governance_review_scope: per_regime_governed_validation_review`
- `regime_completeness_policy: informational_only_not_a_hard_promotion_gate`

This means a bull-only, bear-only, or choppy-only strategy can be reviewed if it clears its own gates. Missing regimes remain research targets, not hidden blockers.

## Microstructure Note

The exhaustive microstructure run remains active, but it is discovery-only. The current one-day websocket stream is not sufficient for governed promotion. Any microstructure PAPER experiment must be explicitly labeled as operator-approved experimental paper, not governed promotion.

# Portfolio Optimizer Priority Pass - 2026-05-08

## Scope

This pass addressed the May 8 portfolio optimizer priority list:

- Repair replay lineage for the full May 8 PAPER strategy book.
- Harden the optimizer so it evaluates production-scaled economics, not raw per-combo economics.
- Add explicit daily-PnL target and concentration constraints for symbol, regime, and family exposure.
- Run portfolio tiers for unconstrained, paper-risk-like, strict target, relaxed target, and drawdown-minimized portfolios.

Broker-facing state changed: **no**.
Live manifests changed: **no**.
Paper strategy manifests changed: **no**.

## Lineage Repair

The original projection had 344 May 8 PAPER strategies, but only 228 matched replay artifacts. GCS profile recovery repaired the remaining 116 unmatched strategies.

- Final repaired replay root: `reports/gcp_research/lineage_gcs_recovery_may8_20260508T1905Z/repaired_replay_final`
- Final repair summary: `reports/gcp_research/lineage_gcs_recovery_may8_20260508T1905Z/lineage_repair_after_gcs_final/lineage_repair_summary.json`
- Final match result: `344 / 344` strategies matched, `0` unmatched.

Canonical GCS mirror:

- `gs://codexalpaca-control-us/research_results/lineage_gcs_recovery_may8_20260508T1905Z/`

## Optimizer Changes

Updated `scripts/optimize_portfolio_projection_candidates.py`:

- Uses `scaled_option_pnl` before raw per-combo PnL so optimizer economics match production-risk projection output.
- Adds `--min-average-daily-pnl`.
- Adds concentration gates for symbol, regime, and family trade share and absolute-PnL share.
- Adds exact-search concentration pruning and final concentration reporting.
- Adds a vectorized exact simulator so 20+ candidate constrained searches are tractable.

Validation:

- `python -m pytest tests\test_build_portfolio_growth_projection.py tests\test_optimize_portfolio_projection_candidates.py tests\test_repair_projection_replay_lineage.py -q`
- Result: `12 passed`
- `python -m pytest -q`
- Result: `304 passed, 1 warning`

## Full-Lineage Projection

Projection command output:

- `reports/gcp_research/portfolio_optimizer_priority_pass_20260508T1900Z/full_lineage_current_hardened_projection/portfolio_growth_projection.json`
- `reports/gcp_research/portfolio_optimizer_priority_pass_20260508T1900Z/full_lineage_current_hardened_projection/portfolio_growth_scaled_trades.csv`

Key results:

- Strategy match coverage: `344 / 344`, `100.0%`.
- Input replay trades before production-risk simulation: `36,326`.
- Accepted production-risk simulated trades: `4,749`.
- Rejected production-risk simulated trades: `31,577`.
- Historical ending equity from `$25,000`: `$29,735.06`.
- Historical net PnL: `$4,735.06`.
- Approx active-day average PnL: `$18.86/day`.
- Max drawdown: `-27.6939%`.
- Evidence grade: `directional_expectation_only`.

Major production-risk rejection counts:

- `max_positions_per_regime`: `8,001`
- `regime_entry_cluster:choppy`: `6,859`
- `regime_entry_cluster:bull`: `5,251`
- `risk_budget_too_small`: `5,151`
- `regime_entry_cluster:bear`: `4,816`
- `max_positions_per_symbol`: `1,023`

## Exact Tier Results

Trusted tier output root:

- `reports/gcp_research/portfolio_optimizer_priority_pass_20260508T1900Z/full_lineage_optimizer_tiers_exact_v3_scaled/`
- Summary CSV: `reports/gcp_research/portfolio_optimizer_priority_pass_20260508T1900Z/full_lineage_optimizer_tiers_exact_v3_scaled/tier_summary.csv`
- Summary JSON: `reports/gcp_research/portfolio_optimizer_priority_pass_20260508T1900Z/full_lineage_optimizer_tiers_exact_v3_scaled/tier_summary.json`

| Tier | Status | Selected | Symbols | Regimes | Families | Ending Equity | Net PnL | Avg Daily PnL | Max DD |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `unconstrained_max_profit` | passed | 20 | 10 | 3 | 3 | `$36,211.30` | `$11,211.30` | `$45.95` | `-3.2273%` |
| `paper_risk_like` | passed | 12 | 9 | 3 | 3 | `$33,739.91` | `$8,739.91` | `$49.10` | `-1.5657%` |
| `drawdown_minimized` | passed | 11 | 9 | 3 | 3 | `$33,505.04` | `$8,505.04` | `$49.74` | `-1.6750%` |
| `target_50_day_controlled` | passed | 9 | 7 | 3 | 2 | `$34,264.47` | `$9,264.47` | `$60.95` | `-1.6658%` |
| `target_100_day_controlled` | passed | 6 | 5 | 3 | 3 | `$37,803.91` | `$12,803.91` | `$104.10` | `-4.0624%` |
| `strict_200_day` | failed | 0 | 0 | 0 | 0 | `$25,000.00` | `$0.00` | n/a | `0.0%` |

The strict `$200/day` tier used exact search over the train/test-positive pool and evaluated `72,580` feasible diversified subsets. Every feasible subset failed `min_average_daily_pnl=200`, so the miss is structural for this candidate pool.

## Interpretation

The optimizer can build a diversified, low-drawdown portfolio around `$50/day` to `$104/day` from current replay evidence, depending on target strictness. It cannot support a governed `$200/day` objective yet without adding higher-quality train/test-positive candidates.

The best controlled portfolios are still dominated by single-leg calls and puts. One `debit_call_vertical` appears in the controlled tiers, but debit/credit verticals, broken-wing butterflies, condors, and iron condors are not yet contributing enough train/test-positive candidates.

## Next Research Targets

Priority sweeps should target candidate diversity, not another broad single-leg sweep:

- Bear/choppy `SPY`, `QQQ`, `IWM`, `AVGO`, `GOOGL`, `MSFT`, `AMZN`, and `TSM`.
- Non-single-leg families: debit verticals, credit verticals, broken-wing butterflies, condors, iron condors.
- Train/test-positive gates should remain active.
- Fill coverage gate remains `>= 0.90`.
- Objective should look for candidates that survive production-risk projection and reduce current concentration, especially away from single-leg calls.

No strategy is newly eligible for live activation from this pass. Promotion remains governed-validation review only.

## Follow-On GCP Sweep Launched

Research-only follow-on wave:

- Wave ID: `bear_choppy_non_single_refine_20260508T2005Z`
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260508T2005Z/`
- Source archive: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260508T2005Z/inputs/source/codexalpaca_repo_source.tar.gz`
- Source commit: `5c94b1c`
- Broker-facing: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Target symbols: `QQQ`, `SPY`, `IWM`, `AVGO`
- Target regimes: `bear`, `choppy`
- Candidate ranges: `c097-120`, `c121-144`
- Choppy family filter: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- Bear profile set: `signal_window_refine`
- Choppy profile set: `timewindow_quality_filter`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60`, `10:60`, `30:120`

Expected active research VMs:

- `qqq-rescue-c097-120-20260508ns1`
- `qqq-rescue-c121-144-20260508ns1`
- `spy-rescue-c097-120-20260508ns1`
- `spy-rescue-c121-144-20260508ns1`
- `iwm-rescue-c097-120-20260508ns1`
- `iwm-rescue-c121-144-20260508ns1`
- `avgo-rescue-c097-120-20260508ns1`
- `avgo-rescue-c121-144-20260508ns1`

When these workers terminate, sync artifacts, build strict portfolio reports and promotion-review packets, mirror aggregate outputs to GCS, delete only synced TERMINATED VMs, and compare any new eligible candidates against the `full_lineage_optimizer_tiers_exact_v3_scaled` benchmark.

## Follow-On GCP Sweep Completion

Aggregate output:

- Local aggregate root: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/`
- Portfolio rollup: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/portfolio_report/research_wave_portfolio_rollup.json`
- Promotion packet: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/portfolio_report/promotion_review_packet/research_promotion_review_packet.json`
- Source commit used for worker launch: `5c94b1c`
- Current handoff source commit: `eddb88e`

Aggregate command:

```powershell
python scripts\build_research_wave_portfolio_rollup.py `
  --report-root reports\gcp_research\bear_choppy_non_single_refine_20260508T2005Z\workers `
  --output-dir reports\gcp_research\bear_choppy_non_single_refine_20260508T2005Z\aggregate\portfolio_report `
  --pattern research_portfolio_report.json `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --max-positions 12 `
  --max-strategies-per-symbol 3 `
  --max-symbol-weight 0.35 `
  --initial-cash 25000 `
  --max-review-candidates 50 `
  --required-regimes bear,choppy
```

Results:

- Decision: `ready_for_governed_validation_review`
- Governance review scope: `per_regime_governed_validation_review`
- Broker-facing: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Source reports: `8`
- Candidates: `400`
- Eligible variant-profile candidates: `14`
- Unique review candidates in packet: `6`
- Eligible regimes: `bear`
- Missing eligible regimes: `choppy`
- Eligible candidate concentration: `QQQ` and `AVGO`, all `single_leg_repair`.

Blockers:

- `fill_coverage_below_0.90`: `258`
- `min_net_pnl_not_positive`: `357`
- `test_net_pnl_not_above_0`: `321`
- Dominant fill issue: `selected_contract_universe_gap` with `197` occurrences.

Important near misses:

- `AVGO` bear `debit_put_vertical` had positive full/test PnL but remained blocked by fill coverage around `0.8242` to `0.8438`.
- `IWM` bear `bear_call_credit_spread` had positive full/test PnL but remained blocked by fill coverage around `0.8696`.
- `AVGO` choppy `debit_call_vertical` had positive full/test PnL but remained blocked by fill coverage around `0.8855`.

This means the non-single-leg search did not fail mainly because of economics. It failed because multi-leg selected-contract availability and leg-pair construction still miss the `0.90` strategy fill gate.

## Follow-On Projection Comparison

Standalone projection for the new bear-only sleeve:

- Output root: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/growth_projection/capital_plan_projection/`
- Strategy match coverage: `5 / 5`, `100.0%`
- Accepted production-risk simulated trades: `275`
- Historical ending equity from `$25,000`: `$29,741.40`
- Total return: `18.9656%`
- Max drawdown: `-6.4166%`
- Diversification status: `failed`
- Diversification failure: only `2` symbols, `1` regime, and `1` family.
- Train/test-positive candidates: `1 / 5`

Benchmark-plus-new-bear projection:

- Output root: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/growth_projection/benchmark_plus_bear_sweep/`
- Benchmark: `tt_top2_bull_choppy_up`
- Risk config: `reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/risk_configs/bull_choppy_up.yaml`
- Historical ending equity from `$25,000`: `$35,945.80`
- Benchmark-only ending equity: `$35,949.02`
- Combined max drawdown: `-19.7277%`
- Benchmark-only max drawdown: `-18.2398%`
- Diversification status: `passed`
- Train/test-positive candidates: `6 / 19`

Adding this sweep's bear sleeve to `tt_top2_bull_choppy_up` did not improve the benchmark. It slightly reduced ending equity and worsened drawdown, so these candidates should remain research/governed-review material rather than portfolio-optimizer additions.

## Next Best Technical Step

Do not launch another broad non-single-leg grid yet. The evidence points to selector/data repair:

- Diagnose selected-contract universe gaps for AVGO debit put verticals, IWM bear call credit spreads, and AVGO choppy debit call verticals.
- Compare lower-leg and upper-leg availability separately for each blocked multi-leg entry.
- Test whether failures come from DTE selection, strike-width constraints, missing opposite leg bars, or entry timestamps landing outside option quote availability.
- Only after a targeted fill diagnostic clears should another GCP tranche run.

No new strategy should be added to the paper runner from this sweep. No paper-runner state changed.

## Selector And Fill Diagnostics

Selector diagnostic output:

- Local root: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/selector_diagnostic/`
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260508T2005Z/aggregate/selector_diagnostic/`
- Command:

```powershell
python scripts\build_greek_selector_diagnostic.py `
  --input-root reports\gcp_research\bear_choppy_non_single_refine_20260508T2005Z\workers `
  --output-dir reports\gcp_research\bear_choppy_non_single_refine_20260508T2005Z\aggregate\selector_diagnostic `
  --fill-coverage-gate 0.90 `
  --min-trades 20
```

Diagnostic summary:

- Candidate summary files: `24`
- Candidate rows classified: `576`
- Dominant causes:
- `missing_option_price_count`: `328`
- `bad_dte_or_strike_availability`: `204`
- `bad_exits`: `38`
- `passed_candidate_level_gates`: `6`
- Overlapping cause flags:
- `missing_option_price_count`: `532`
- `selected_contract_universe_gap`: `504`
- `bad_dte_or_strike_availability`: `504`
- `too_strict_delta_targeting`: `504`
- `entry_bar_gap_or_entry_timing_mismatch`: `270`
- `bad_exits`: `181`
- `exit_bar_gap_or_exit_policy_mismatch`: `71`

Targeted fill diagnostics:

- `AVGO` choppy `debit_call_vertical` `aa587aef25db42`
- Local: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/fill_failure_diagnostic/avgo_choppy_debit_call_vertical_aa587a_e10x60/`
- GCS: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260508T2005Z/aggregate/fill_failure_diagnostic/avgo_choppy_debit_call_vertical_aa587a_e10x60/`
- Metrics: fill `0.8855`, data foundation `0.8931`, entry `0.9915`, exit `1.0`, option trades `116`, net PnL `910.568`, test PnL `1539.36`.
- Failures: `14` `no_selected_contract`, `1` `no_entry_bar`.

- `IWM` bear `bear_call_credit_spread` `6f6e242f049e80`
- Local: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/fill_failure_diagnostic/iwm_bear_call_credit_spread_6f6e_e10x60/`
- GCS: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260508T2005Z/aggregate/fill_failure_diagnostic/iwm_bear_call_credit_spread_6f6e_e10x60/`
- Metrics: fill `0.8696`, data foundation `0.8696`, entry `1.0`, exit `1.0`, option trades `40`, net PnL `495.159`, test PnL `951.966`.
- Failures: `6` `no_selected_contract`.

- `AVGO` bear `debit_put_vertical` `bdd4792e98d95a`
- Local: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/fill_failure_diagnostic/avgo_bear_debit_put_vertical_bdd479_e30x120/`
- GCS: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260508T2005Z/aggregate/fill_failure_diagnostic/avgo_bear_debit_put_vertical_bdd479_e30x120/`
- Metrics: fill `0.8333`, data foundation `0.8542`, entry `0.9878`, exit `0.9877`, option trades `80`, net PnL `6228.555`, test PnL `9867.691`.
- Failures: `14` `no_selected_contract`, `1` `no_entry_bar`, `1` `no_exit_bar`.

Interpretation:

- The best non-single-leg near misses are mostly contract-universe repair problems, not broad strategy problems.
- For these candidates, entry and exit bar coverage is already high; selected-contract availability is the binding gate.
- The next repair should produce a contract-date request list for the missing vertical legs, then rerun only these candidates and adjacent width/DTE variants.

Selected-contract gap request packet:

- Local root: `reports/gcp_research/bear_choppy_non_single_refine_20260508T2005Z/aggregate/selected_contract_gap_requests/`
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260508T2005Z/aggregate/selected_contract_gap_requests/`
- Request rows: `34`
- Case count: `3`
- Cases:
- `AVGO` choppy `debit_call_vertical`: `14` missing selected-contract dates.
- `IWM` bear `bear_call_credit_spread`: `6` missing selected-contract dates.
- `AVGO` bear `debit_put_vertical`: `14` missing selected-contract dates.

The packet is intentionally a contract/date selector-repair input, not a promotion packet. The next GCP work should use it to determine whether the current selected-contract root is too narrow for realistic vertical leg pairing, then rerun the same candidates after repair without changing promotion gates.

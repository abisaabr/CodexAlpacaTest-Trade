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

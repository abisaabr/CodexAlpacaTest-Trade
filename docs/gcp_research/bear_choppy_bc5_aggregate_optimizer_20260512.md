# Bear/Choppy BC5 Aggregate And Optimizer Pass - 2026-05-12

## Scope

Research-only continuation for `bear_choppy_non_single_refine_20260512T1620ET`.

No live trading was started. No paper trader was started. No live manifests, active paper configs, or global risk policy were changed. The `fill_coverage >= 0.90` gate remained intact.

## Source Lineage

- Worker wave ID: `bear_choppy_non_single_refine_20260512T1620ET`
- Worker GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1620ET/`
- Worker source archive: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1620ET/inputs/source/codexalpaca_repo_source.tar.gz`
- Worker source commit recorded for the wave: `763f27a`
- Local aggregation/operator source commit: `a252b77dab22319ff92531cec63e5564d97114ad`

## Preservation And Cleanup

Synced worker artifacts from:

- `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1620ET/workers/`

Local mirror:

- `reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/workers/`

Deleted only synced TERMINATED research VMs:

- `avgo-rescue-c193-216-20260512bc5`
- `googl-rescue-c193-216-20260512bc5`
- `tsm-rescue-c193-216-20260512bc5`
- `msft-rescue-c193-216-20260512bc5`
- `amzn-rescue-c193-216-20260512bc5`
- `iwm-rescue-c193-216-20260512bc5`

No `bc5` research VM remains active. The only expected active workers after this pass are discovery-only microstructure VMs.

## Commands

Worker sync:

```powershell
gsutil -m rsync -r gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1620ET/workers reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/workers
```

Strict portfolio report:

```powershell
python scripts/build_research_portfolio_report.py --replay-root reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/workers --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/portfolio_report --fill-coverage-gate 0.90 --min-option-trades 20 --min-test-net-pnl 0 --required-regimes bear,choppy --candidate-identity-mode variant_profile
```

Promotion-review packet:

```powershell
python scripts/build_research_promotion_review_packet.py --portfolio-report-json reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/portfolio_report/research_portfolio_report.json --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/promotion_packet --max-review-candidates 20
```

Optimizer candidate pool:

```powershell
python scripts/build_research_portfolio_report.py --replay-root reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/workers --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/optimizer_candidate_pool_report --fill-coverage-gate 0.90 --min-option-trades 20 --min-test-net-pnl 0 --required-regimes bear,choppy --candidate-identity-mode variant_profile --max-positions 30 --max-strategies-per-symbol 10 --max-symbol-weight 1.0
```

Hardened projection:

```powershell
python scripts/build_portfolio_growth_projection.py --portfolio-report-json reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/optimizer_candidate_pool_report/research_portfolio_report.json --replay-root reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/workers --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/growth_projection/optimizer_candidate_pool_projection --initial-cash 25000 --target-equity 300000 --backtest-allocation-fraction 0.05 --projection-years 5 --bootstrap-runs 2000 --seed 20260512 --optimization-train-end-date 2026-01-30 --optimization-min-train-trades 5 --optimization-min-test-trades 5 --optimization-max-per-symbol-regime 2 --fill-model-enabled --unknown-fill-probability 1.0 --diversification-min-symbols 3 --diversification-min-regimes 2 --diversification-min-families 2 --diversification-max-symbol-trade-share 0.60 --diversification-max-family-trade-share 0.85 --diversification-max-regime-trade-share 0.80 --diversification-max-symbol-pnl-share 0.80
```

Strict optimizer:

```powershell
python scripts/optimize_portfolio_projection_candidates.py --portfolio-report-json reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/optimizer_candidate_pool_report/research_portfolio_report.json --scaled-trades-csv reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/growth_projection/optimizer_candidate_pool_projection/portfolio_growth_scaled_trades.csv --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/constrained_optimizer_strict --initial-cash 25000 --backtest-allocation-fraction 0.05 --train-end-date 2026-01-30 --min-train-trades 5 --min-test-trades 5 --max-candidates 12 --max-per-symbol 2 --max-per-regime 8 --max-per-family 4 --min-symbols 3 --min-regimes 2 --min-families 2 --max-drawdown-pct 22 --min-average-daily-pnl 50 --max-symbol-trade-share 0.60 --max-regime-trade-share 0.80 --max-family-trade-share 0.85 --max-candidate-trade-share 0.45 --max-symbol-pnl-share 0.80 --max-regime-pnl-share 0.80 --max-family-pnl-share 0.85 --max-candidate-pnl-share 0.50 --objective risk_adjusted --max-exact-candidates 20
```

Relaxed diagnostic optimizer:

```powershell
python scripts/optimize_portfolio_projection_candidates.py --portfolio-report-json reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/optimizer_candidate_pool_report/research_portfolio_report.json --scaled-trades-csv reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/growth_projection/optimizer_candidate_pool_projection/portfolio_growth_scaled_trades.csv --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/constrained_optimizer_relaxed_diagnostic --initial-cash 25000 --backtest-allocation-fraction 0.05 --train-end-date 2026-01-30 --min-train-trades 3 --min-test-trades 3 --max-candidates 12 --max-per-symbol 3 --max-per-regime 8 --max-per-family 6 --min-symbols 2 --min-regimes 2 --min-families 2 --max-drawdown-pct 35 --min-average-daily-pnl 25 --max-symbol-trade-share 0.70 --max-regime-trade-share 0.85 --max-family-trade-share 0.85 --max-candidate-trade-share 0.55 --max-symbol-pnl-share 0.85 --max-regime-pnl-share 0.90 --max-family-pnl-share 0.85 --max-candidate-pnl-share 0.60 --objective risk_adjusted --max-exact-candidates 20
```

GCS aggregate mirror:

```powershell
gsutil -m rsync -r reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1620ET/aggregate
```

## Results

Strict portfolio report:

- Candidate count: `576`
- Eligible for promotion review: `29`
- Strict capital-plan candidates: `5`
- Strict capital-plan symbols: `IWM`, `AVGO`, `TSM`
- Strict capital-plan regimes: `bear`, `choppy`

Promotion-review packet:

- Decision: `ready_for_governed_validation_review`
- Candidate-level decision: `ready_for_governed_validation_review`
- Governance review scope: `multi_regime_governed_validation_review`
- Review candidates emitted: `19`
- Required regimes: `bear,choppy`
- Missing eligible regimes: none

Optimizer candidate-pool projection:

- Candidate-pool capital-plan count: `19`
- Replay match rate: `100%`
- Accepted trade rows: `1801`
- Train/test optimization: `0` eligible candidates
- Train/test blockers: `19` test-PnL failures, `14` train-PnL failures
- Historical ending equity: `$0.00`
- Historical max drawdown: `-100.0%`
- Evidence grade: `not_institutional_expectation`

Strict optimizer:

- Status: `failed`
- Eligible candidates: `0`
- Selected candidates: `0`
- Failure reasons: `min_symbols`, `min_regimes`, `min_families`, `min_average_daily_pnl`

Relaxed diagnostic optimizer:

- Status: `failed`
- Eligible candidates: `0`
- Selected candidates: `0`
- Failure reasons: `min_symbols`, `min_regimes`, `min_families`, `min_average_daily_pnl`

Benchmark comparison:

- Benchmark: `tt_top2_bull_choppy_up`
- Benchmark projection: `reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/tt_top2_bull_choppy_up_final_2000/portfolio_growth_projection.json`
- Benchmark accepted trades: `1389`
- Benchmark ending equity: `$35,949.02`
- Benchmark max drawdown: `-18.2398%`
- Benchmark train/test eligible candidates: `6`
- Benchmark evidence grade: `directional_expectation_only`

## Decision

No new paper-runner manifest was created from `bc5`.

Rationale: the generated promotion packet is eligible for governed-validation review, but the hardened optimizer rejects the candidate pool. The pool does not improve on `tt_top2_bull_choppy_up`; it has zero train/test-positive candidates and fails both strict and relaxed constrained optimizer passes.

Promotion status: `ready_for_governed_validation_review` only.

Paper-runner state changed: `false`.

## Active Follow-Up

Microstructure discovery workers are still running and remain discovery-only:

- `micro-event-c92161-92672-20260512m4`: `425 / 512`, progress `0.830078`, review-like count `0`
- `micro-event-c92673-93184-20260512m4`: `225 / 512`, progress `0.439453`, review-like count `0`

Do not add microstructure candidates to PAPER without an eligible generated packet or a clearly labeled operator-approved experiment packet.

## Next Actions

1. Keep `tt_top2_bull_choppy_up` as the benchmark portfolio.
2. Do not activate `bc5` candidates in the paper runner unless a later optimizer pass finds a train/test-positive subset.
3. Diagnose why `bc5` candidates are promotion-review eligible at the packet level but fail train/test replay projection.
4. Prioritize new sweeps that improve train-period stability, not just test-period PnL.
5. Continue microstructure discovery to completion, then aggregate as discovery-only.

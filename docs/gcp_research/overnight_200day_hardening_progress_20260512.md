# Overnight $200/Day Hardening Progress - 2026-05-12

## Scope

This is a PAPER-only research handoff for the overnight `$200/day` optimization loop. No live manifests, paper configs, or global risk policy were changed.

Active research wave:

- Wave: `choppy_non_single_train_test_refine_20260512T1925ET`
- GCS root: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/`
- Source commit: `817f7ef`
- Source archive: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/inputs/source/codexalpaca_repo_source.tar.gz`

## Completed Tranche c001-c024

Completed, synced workers:

- `amzn_regime_rescue_c001_024`
- `avgo_regime_rescue_c001_024`
- `googl_regime_rescue_c001_024`
- `iwm_regime_rescue_c001_024`
- `msft_regime_rescue_c001_024`
- `qqq_regime_rescue_c001_024`
- `spy_regime_rescue_c001_024`
- `tsm_regime_rescue_c001_024`

Aggregate outputs:

- Portfolio report: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/promotion_packet/research_promotion_review_packet.json`
- Quote-quality lineage audit: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/quote_quality_lineage_audit/lineage_repair_summary.json`
- Quote-cost benchmark projection: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/growth_projection/benchmark_plus_choppy_quote_cost/portfolio_growth_projection.json`
- Strict optimizer: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/constrained_optimizer_strict_200_day_quote_cost/optimizer_summary.json`
- Relaxed diagnostic optimizer: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/constrained_optimizer_relaxed_25_day_quote_cost/optimizer_summary.json`
- GCS aggregate mirror: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/`

Strict portfolio packet:

- Candidate count: `576`
- Eligible for governed-validation review: `4`
- Packet decision: `ready_for_governed_validation_review`
- Eligible candidates: TSM choppy `debit_call_vertical` variants only
- Fill coverage: candidates clear the `0.90` gate
- Promotion scope: governed-validation review only

Quote-quality and optimizer result:

- Quote-quality lineage matched the current capital-plan rows, but both rows are `quote_quality_gap`.
- Required data action: `rerun_replay_with_bid_ask_spread_quote_age_and_trade_prints`
- Benchmark-plus-new-choppy quote-cost ending equity: `$4,941.71`
- Average daily PnL: `-$79.91`
- Max drawdown: `-80.2332%`
- Market-quality PnL cost: `$226,868.997008`
- Fill-probability positive-PnL haircut: `$155,921.462839`
- Train/test eligible after quote-cost and fill-haircut: `0`
- Strict `$200/day` constrained optimizer: `failed`
- Relaxed `$25/day` diagnostic optimizer: `failed`

Conclusion: the new c001-c024 TSM choppy verticals are useful research candidates, but they are not eligible for paper-runner addition. The blocker is quote-backed economics and train/test stability, not packet formatting.

## Cleanup

Deleted after artifacts were synced and aggregate outputs were mirrored:

- `amzn-rescue-c001-024-20260512bc6a`
- `googl-rescue-c001-024-20260512bc6a`
- `msft-rescue-c001-024-20260512bc6a`
- `tsm-rescue-c001-024-20260512bc6a`

The other c001-c024 workers were not present in the active Compute Engine list at cleanup time; their artifacts were already present in GCS and synced locally.

## Active Tranche c025-c048

Launched after c001-c024 was fully synced and classified:

- `qqq-rescue-c025-048-20260512bc6b`
- `spy-rescue-c025-048-20260512bc6b`
- `iwm-rescue-c025-048-20260512bc6b`

Launch settings:

- Symbols: `QQQ`, `SPY`, `IWM`
- Candidate range: `c025-c048`
- Candidate count per worker: `24`
- Target regime: `choppy`
- Families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Zone: `us-west1-a`
- Machine type: `e2-standard-2`
- Broker-facing effect: none

Next monitor action:

1. Monitor `20260512bc6b` workers until terminated.
2. Sync artifacts to `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/workers`.
3. Rebuild strict portfolio report and promotion packet with existing gates.
4. Rerun quote-quality lineage, quote-cost/fill-haircut projection, and constrained optimizer.
5. Do not add candidates to the paper runner unless quote-backed train/test and portfolio-risk evidence supports it.

## Aggregate c001-c048

Completed and classified after `20260512bc6b` termination:

- `qqq_regime_rescue_c025_048`
- `spy_regime_rescue_c025_048`
- `iwm_regime_rescue_c025_048`

Aggregate outputs:

- Portfolio report: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_048/portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_048/promotion_packet/research_promotion_review_packet.json`
- Quote-quality lineage audit: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_048/quote_quality_lineage_audit/lineage_repair_summary.json`
- Quote-cost benchmark projection: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_048/growth_projection/benchmark_plus_choppy_quote_cost/portfolio_growth_projection.json`
- Strict optimizer: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_048/constrained_optimizer_strict_200_day_quote_cost/optimizer_summary.json`
- Relaxed diagnostic optimizer: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_048/constrained_optimizer_relaxed_25_day_quote_cost/optimizer_summary.json`
- GCS aggregate mirror: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_048/`

Strict portfolio packet:

- Candidate count: `792`
- Eligible for governed-validation review: `4`
- Packet decision: `ready_for_governed_validation_review`
- Eligible candidates: unchanged from c001-c024, TSM choppy `debit_call_vertical` variants only
- c025-c048 added no new eligible candidates
- Main blockers: `min_net_pnl_not_positive=784`, `test_net_pnl_not_above_0=722`, `fill_coverage_below_0.90=40`
- Promotion scope: governed-validation review only

Quote-quality and optimizer result:

- Quote-quality lineage matched the current capital-plan rows, but both rows are `quote_quality_gap`.
- Required data action remains `rerun_replay_with_bid_ask_spread_quote_age_and_trade_prints`.
- Benchmark-plus-new-choppy quote-cost ending equity: `$4,941.71`
- Average daily PnL: `-$79.91`
- Max drawdown: `-80.2332%`
- Market-quality PnL cost: `$226,868.997008`
- Fill-probability positive-PnL haircut: `$155,921.462839`
- Train/test eligible after quote-cost and fill-haircut: `0`
- Strict `$200/day` constrained optimizer: `failed`
- Relaxed `$25/day` diagnostic optimizer: `failed`

Conclusion: c025-c048 did not improve the portfolio. The blocker is still quote-backed economics and train/test stability, so no c001-c048 candidates should be added to the May 13 paper runner.

## Cleanup c025-c048

Deleted after artifacts were synced and aggregate outputs were mirrored:

- `qqq-rescue-c025-048-20260512bc6b`
- `spy-rescue-c025-048-20260512bc6b`
- `iwm-rescue-c025-048-20260512bc6b`

## Active Tranche c049-c072

Launched after c025-c048 was fully synced, classified, mirrored, and cleaned:

- `qqq-rescue-c049-072-20260512bc6c`
- `spy-rescue-c049-072-20260512bc6c`
- `iwm-rescue-c049-072-20260512bc6c`

Launch settings:

- Symbols: `QQQ`, `SPY`, `IWM`
- Candidate range: `c049-c072`
- Candidate count per worker: `24`
- Target regime: `choppy`
- Families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Zone: `us-west1-a`
- Machine type: `e2-standard-2`
- Broker-facing effect: none

Next monitor action:

1. Monitor `20260512bc6c` workers until terminated.
2. Sync artifacts to `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/workers`.
3. Rebuild strict portfolio report and promotion packet with existing gates.
4. Rerun quote-quality lineage, quote-cost/fill-haircut projection, and constrained optimizer.
5. Do not add candidates to the paper runner unless quote-backed train/test and portfolio-risk evidence supports it.

## Quote-Backed Evidence Repair Sweep

Run ID: `full_sweep_20260513T0030Z`

Local outputs:

- Summary: `reports/gcp_research/evidence_repair_sweep_20260513/full_sweep_20260513T0030Z/evidence_repair_sweep_summary.json`
- Combined lineage: `reports/gcp_research/evidence_repair_sweep_20260513/full_sweep_20260513T0030Z/combined_quote_quality_lineage_after_sidecar.csv`
- Rejections: `reports/gcp_research/evidence_repair_sweep_20260513/full_sweep_20260513T0030Z/quote_backed_rejections.csv`
- Survivor report: `reports/gcp_research/evidence_repair_sweep_20260513/full_sweep_20260513T0030Z/quote_backed_survivor_report.json`
- Review candidate universe: `reports/gcp_research/evidence_repair_sweep_20260513/full_sweep_20260513T0030Z/review_candidates_combined_report.json`

GCS outputs:

- Sweep summary artifacts: `gs://codexalpaca-control-us/gcp_research/evidence_repair_sweep_20260513/full_sweep_20260513T0030Z/`
- OPRA/SIP sidecars: `gs://codexalpaca-control-us/gcp_research/evidence_repair_sweep_20260513/quote_sidecars/microstructure_shadow_fastwriter_20260507/`

Sidecar source:

- Events: `D:/codexalpaca_runtime/runs/microstructure_shadow_stream_fastwriter_20260507T1340ET/realtime_shadow_events.jsonl`
- Accepted option quotes: `879466`
- Accepted option trades: `2954`
- Accepted stock quotes: `41035`
- Quote symbols: `359`

Evidence-repair result:

- Current paper strategies audited: `344`
- Paper replay lineage repaired from search history: `116`
- Paper quote-backed survivors after sidecar apply: `0`
- Review candidate universe before dedupe: `2346`
- Review candidates after dedupe: `523`
- Review replay lineage missing after search: `8`
- Review quote-backed survivors after sidecar apply: `0`
- Paper sidecar-applied rows: `150300`
- Review sidecar-applied rows: `550653`
- Paper rows with entry/exit quote sidecar missing: `150300/150300`
- Review rows with entry/exit quote sidecar missing: `550653/550653`
- Optimizer status: `skipped_no_quote_backed_survivors`

Conclusion: the available May 7 OPRA/SIP quote stream is valid as a sidecar, but it does not cover the current paper/review replay contracts and timestamps closely enough to produce any `quote_backed_replay` survivor. No strategy can be optimized or added to the paper runner from this sweep.

Implementation added:

- `scripts/run_quote_backed_evidence_repair_sweep.py`

This script is research-only. It repairs replay lineage, applies quote sidecars using causal as-of joins, rejects incomplete quote lineage, and emits a survivor report only when candidates have full quote-backed replay evidence.

## Aggregate c001-c072

Completed and classified after `20260512bc6c` termination:

- `qqq_regime_rescue_c049_072`
- `spy_regime_rescue_c049_072`
- `iwm_regime_rescue_c049_072`

Aggregate outputs:

- Portfolio report: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_072/portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_072/promotion_packet/research_promotion_review_packet.json`
- Quote-quality lineage audit: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_072/quote_quality_lineage_audit/lineage_repair_summary.json`
- Quote-cost benchmark projection: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_072/growth_projection/benchmark_plus_choppy_quote_cost/portfolio_growth_projection.json`
- Strict optimizer: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_072/constrained_optimizer_strict_200_day_quote_cost/optimizer_summary.json`
- Relaxed diagnostic optimizer: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_072/constrained_optimizer_relaxed_25_day_quote_cost/optimizer_summary.json`
- GCS aggregate mirror: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_072/`

Strict portfolio packet:

- Candidate count: `1008`
- Eligible for governed-validation review: `4`
- Packet decision: `ready_for_governed_validation_review`
- Eligible candidates: unchanged TSM choppy `debit_call_vertical` variants only
- c049-c072 added no new eligible candidates
- Main blockers: `min_net_pnl_not_positive=1000`, `test_net_pnl_not_above_0=938`, `fill_coverage_below_0.90=40`
- Promotion scope: governed-validation review only

Quote-quality and optimizer result:

- Quote-quality lineage matched the current capital-plan rows, but both rows are `quote_quality_gap`.
- Required data action remains `rerun_replay_with_bid_ask_spread_quote_age_and_trade_prints`.
- Benchmark-plus-new-choppy quote-cost ending equity: `$4,941.71`
- Average daily PnL: `-$79.91`
- Max drawdown: `-80.2332%`
- Market-quality PnL cost: `$226,868.997008`
- Fill-probability positive-PnL haircut: `$155,921.462839`
- Train/test eligible after quote-cost and fill-haircut: `0`
- Strict `$200/day` constrained optimizer: `failed`
- Relaxed `$25/day` diagnostic optimizer: `failed`

Conclusion: c049-c072 did not improve the portfolio. The blocker remains quote-backed economics and train/test stability, so no c001-c072 candidates should be added to the May 13 paper runner.

## Cleanup c049-c072

Deleted after artifacts were synced and aggregate outputs were mirrored:

- `qqq-rescue-c049-072-20260512bc6c`
- `spy-rescue-c049-072-20260512bc6c`
- `iwm-rescue-c049-072-20260512bc6c`

## Active Tranche c073-c096

Launched after c049-c072 was fully synced, classified, mirrored, and cleaned:

- `qqq-rescue-c073-096-20260512bc6d`
- `spy-rescue-c073-096-20260512bc6d`
- `iwm-rescue-c073-096-20260512bc6d`

Launch settings:

- Symbols: `QQQ`, `SPY`, `IWM`
- Candidate range: `c073-c096`
- Candidate count per worker: `24`
- Target regime: `choppy`
- Families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Zone: `us-west1-a`
- Machine type: `e2-standard-2`
- Broker-facing effect: none

Next monitor action:

1. Monitor `20260512bc6d` workers until terminated.
2. Sync artifacts to `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/workers`.
3. Rebuild strict portfolio report and promotion packet with existing gates.
4. Rerun quote-quality lineage, quote-cost/fill-haircut projection, and constrained optimizer.
5. Do not add candidates to the paper runner unless quote-backed train/test and portfolio-risk evidence supports it.

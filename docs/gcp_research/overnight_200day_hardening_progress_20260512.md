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

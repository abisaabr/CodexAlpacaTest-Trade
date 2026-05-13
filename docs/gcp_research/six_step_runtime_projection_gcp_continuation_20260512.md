# Six-Step Runtime, Projection, And GCP Continuation - 2026-05-12

## Summary

Executed the requested six-step continuation without changing live manifests, paper configs, or global risk policy.

- Source commit for code hardening and new GCP source archive: `6a617a8`.
- Broker mode / paper-runner state: no paper-runner state changed; no live-trading path started.
- Fill gate: `fill_coverage >= 0.90` preserved.
- Promotion posture: governed-validation review only. No automatic paper activation.

## Runtime And Projection Hardening

Code commit:

- `6a617a8 feat: harden paper submissions and projection quality`
- Pushed branch: `codex/phase2-fill-semantics-20260430`

Changes:

- `scripts/build_portfolio_growth_projection.py` now recognizes backtester spread aliases: `entry_average_relative_spread`, `entry_max_relative_spread`, `exit_average_relative_spread`, `exit_max_relative_spread`.
- Projection scaled-trade rows now preserve quote-age/spread source columns plus `entry_quote_source` and `exit_quote_source`.
- Fill-probability modeling now penalizes `option_bar_close_no_bid_ask`, `unknown`, `missing`, and `none` quote sources.
- `alpaca_lab/multi_ticker_portfolio/trader.py` now journals broker submit exceptions as `order_submission_error` / `submit_error` and continues safely instead of crashing the PAPER runner.
- Cleanup order submission now retries after transient submit exceptions and writes `broker_position_cleanup.json` evidence.

Verification:

```powershell
python -m pytest tests/test_multi_ticker_portfolio.py::test_execute_attempts_records_submit_failure_without_crashing tests/test_multi_ticker_portfolio.py::test_submit_cleanup_order_retries_after_submit_failure tests/test_multi_ticker_portfolio.py::test_execute_attempts_records_cancel_failure_without_crashing tests/test_multi_ticker_portfolio.py::test_submit_cleanup_order_retries_after_cancelled_attempt tests/test_build_portfolio_growth_projection.py tests/test_optimize_portfolio_projection_candidates.py tests/test_build_governed_validation_manifest_from_packets.py tests/test_broker_multileg_orders.py -q
```

Result: `21 passed in 3.23s`.

## Runtime Review Packets

Choppy non-single review manifest:

- Source packet: `reports/gcp_research/choppy_non_single_refine_20260512T1750Z/aggregate_full/promotion_packet/research_promotion_review_packet.json`
- Runtime review manifest: `reports/gcp_research/choppy_non_single_refine_20260512T1750Z/runtime_review/choppy_non_single_review_manifest.yaml`
- GCS: `gs://codexalpaca-control-us/research_results/choppy_non_single_refine_20260512T1750Z/runtime_review/choppy_non_single_review_manifest.yaml`
- Strategy count: `20`
- Symbols: `AVGO`, `GOOGL`, `TSM`
- Regime: `choppy`
- Runner semantics: all `packet_translated_to_runtime_native_multileg`

New bear/choppy c169-c192 review manifest:

- Wave: `bear_choppy_non_single_refine_20260512T1546ET`
- Source packet: `reports/gcp_research/bear_choppy_non_single_refine_20260512T1546ET/aggregate/promotion_packet/research_promotion_review_packet.json`
- Runtime review manifest: `reports/gcp_research/bear_choppy_non_single_refine_20260512T1546ET/runtime_review/bear_choppy_non_single_review_manifest.yaml`
- GCS: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1546ET/runtime_review/bear_choppy_non_single_review_manifest.yaml`
- Strategy count: `16`
- Symbols: `AMZN`, `AVGO`, `GOOGL`, `IWM`, `MSFT`, `QQQ`, `TSM`
- Regimes: `bear`, `choppy`
- Runner semantics: `11` native multi-leg, `5` single-leg

## Projection Evidence

Quality-stressed projection output:

- Local root: `reports/gcp_research/next_eval_quality_stress_20260512/`
- GCS root: `gs://codexalpaca-control-us/gcp_research/next_eval_quality_stress_20260512/`

Result:

- Spread source columns are now correctly recognized.
- Strict spread stress rejected all `2,251` trades because replay rows lack bid/ask spread values.
- Quote-source/fill-model run kept `218 / 2,251` after quote-age filtering, then rejected all `218` because average projected fill probability was `0.144375`, below the `0.45` threshold.
- Interpretation: current bar-close/no-bid-ask replay is not institutional-grade evidence for spread-sensitive activation.

Benchmark-plus-new-bear/choppy projection:

- Local root: `reports/gcp_research/next_eval_bear_choppy_c169_192_20260512/`
- GCS root: `gs://codexalpaca-control-us/gcp_research/next_eval_bear_choppy_c169_192_20260512/`
- Benchmark: `tt_top2_bull_choppy_up`
- Combined projection output: `reports/gcp_research/next_eval_bear_choppy_c169_192_20260512/benchmark_plus_bc4/portfolio_growth_projection.json`

Result versus benchmark:

- Benchmark ending equity: `$35,949.02`
- Combined ending equity: `$36,168.54`
- Benchmark max drawdown: `-18.2398%`
- Combined max drawdown: `-16.6148%`
- Accepted production-runtime trades: `1,578`
- Train/test eligible candidates: `8 / 19`

Optimizer tiers:

- Strict `$200/day`, `$100/day`, and `$50/day` all failed because the train/test-positive pool could not satisfy `min_symbols`, `min_regimes`, and `min_families`.
- Relaxed controlled `$100/day` passed:
- Output: `reports/gcp_research/next_eval_bear_choppy_c169_192_20260512/optimizer_benchmark_plus_bc4_relaxed_100_day/optimizer_summary.json`
- Selected candidates: `5`
- Average daily PnL: `$114.1447`
- Ending equity: `$48,171.38`
- Max drawdown: `-8.1289%`
- Symbols: `AVGO`, `INTC`, `IWM`, `NVDA`, `SPY`
- Regimes: `bear`, `bull`, `choppy`
- Families: single-leg long call, single-leg long put

## GCP Research

Targeted bear/choppy non-single tranche:

- Wave: `bear_choppy_non_single_refine_20260512T1546ET`
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1546ET/`
- Source archive: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1546ET/inputs/source/codexalpaca_repo_source.tar.gz`
- Source commit: `6a617a8`
- Candidate range: `c169-192`
- Symbols: `QQQ`, `SPY`, `IWM`, `AVGO`, `GOOGL`, `MSFT`, `AMZN`, `TSM`
- Instance suffix: `20260512bc4`
- Local worker mirror: `D:\gcp_bc4_workers`
- Aggregate GCS: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1546ET/aggregate/`

Aggregate result:

- Candidate profiles: `576`
- Eligible profiles: `29`
- Review candidates: `16`
- Packet decision: `ready_for_governed_validation_review`
- Eligible regimes: `bear`, `choppy`
- Main blockers: `min_net_pnl_not_positive`, `test_net_pnl_not_above_0`, `fill_coverage_below_0.90`, `option_trades_below_20`
- Deleted all completed `20260512bc4` research VMs after `gsutil` sync succeeded.

Microstructure discovery:

- Wave: `microstructure_rare_event_overnight_20260507T2030ET`
- GCS root: `gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/`
- New chunks launched: `c92161-92672`, `c92673-93184`
- Instances: `micro-event-c92161-92672-20260512m4`, `micro-event-c92673-93184-20260512m4`
- Zones: `us-west1-a`, `us-east4-a`
- Status at handoff: still running
- Promotion posture: discovery-only; do not add to paper without an eligible generated packet or explicitly labeled operator-approved experiment packet.

## Commands

Targeted GCP tranche:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 -Symbol <SYMBOL> -WaveId bear_choppy_non_single_refine_20260512T1546ET -StartCandidateIndex 169 -CandidateCountPerWorker 24 -MaxLaunches 1 -InstanceSuffix 20260512bc4 -TargetRegimes bear,choppy -BearProfileSet signal_window_refine -ChoppyProfileSet timewindow_quality_filter -CandidateSelectionMode regime_balanced -RegimeBalanceOrder bear,choppy,bull,unclassified -ChoppyFamilies debit_call_vertical,debit_put_vertical,bull_put_credit_spread,bear_call_credit_spread,broken_wing_call_butterfly,broken_wing_put_butterfly
```

Aggregate:

```powershell
python scripts/build_research_portfolio_report.py --replay-root D:\gcp_bc4_workers --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1546ET/aggregate/portfolio_report --fill-coverage-gate 0.90 --min-option-trades 20 --min-test-net-pnl 0 --required-regimes bear,choppy --candidate-identity-mode variant_profile
python scripts/build_research_promotion_review_packet.py --portfolio-report-json reports/gcp_research/bear_choppy_non_single_refine_20260512T1546ET/aggregate/portfolio_report/research_portfolio_report.json --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1546ET/aggregate/promotion_packet --max-review-candidates 20
```

Projection and optimizer:

```powershell
python scripts/build_portfolio_growth_projection.py --portfolio-report-json reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/portfolio_reports/tt_top2.json --additional-portfolio-report-json reports/gcp_research/bear_choppy_non_single_refine_20260512T1546ET/aggregate/portfolio_report/research_portfolio_report.json --replay-root reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/filtered_replay_projection_compatible --additional-replay-root D:\gcp_bc4_workers --output-dir reports/gcp_research/next_eval_bear_choppy_c169_192_20260512/benchmark_plus_bc4 --initial-cash 25000 --target-equity 300000 --risk-simulation-mode production_runtime --production-risk-config-yaml reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/risk_configs/bull_choppy_up.yaml --optimization-train-end-date 2026-01-30 --optimization-min-train-trades 5 --optimization-min-test-trades 5
python scripts/optimize_portfolio_projection_candidates.py --portfolio-report-json reports/gcp_research/next_eval_bear_choppy_c169_192_20260512/benchmark_plus_bc4/portfolio_growth_projection.json --scaled-trades-csv reports/gcp_research/next_eval_bear_choppy_c169_192_20260512/benchmark_plus_bc4/portfolio_growth_scaled_trades.csv --output-dir reports/gcp_research/next_eval_bear_choppy_c169_192_20260512/optimizer_benchmark_plus_bc4_relaxed_100_day --min-average-daily-pnl 100 --min-symbols 3 --min-regimes 2 --min-families 2 --max-drawdown-pct 22 --objective risk_adjusted
```

## Next Steps

- Monitor the two microstructure VMs, sync artifacts, aggregate discovery output, then delete only after artifacts are preserved.
- Do not activate the relaxed optimizer subset directly; it passes a relaxed projection tier but still fails the stricter institutional diversity tier.
- Add quote/bid-ask replay or quote snapshots to the backtest economics so strict spread and quote-source stress can be meaningful instead of rejecting all bar-close evidence.
- Run the next targeted tranche only after comparing the new c169-c192 candidates against quote-quality constraints or improving replay economics.

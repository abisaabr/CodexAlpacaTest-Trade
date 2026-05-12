# Quote Realism Projection Hardening - 2026-05-12

- Source commit: `fa6ba33ec3172ffc747ef5e47ab713444d97a278`
- Broker mode impact: `none`
- Paper-runner state changed: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Promotion decision: `no new strategy is eligible for paper-runner addition from this pass`

## Code Change

`scripts/build_portfolio_growth_projection.py` now supports `--fill-model-haircut-positive-pnl`.

This applies projected fill probability to positive option PnL before equity, train/test, and optimizer calculations, while leaving losses unreduced. The intent is conservative: missing bid/ask, stale quote, or zero-print evidence can reduce upside assumptions but cannot make losing trades look safer.

The projection packet also now records market-quality diagnostics:

- quote-source counts
- spread coverage
- quote-age coverage
- trade-print coverage
- rows with no-bid-ask quote sources when quote-source fields exist

Validation command:

```powershell
python -m pytest tests/test_build_portfolio_growth_projection.py tests/test_optimize_portfolio_projection_candidates.py -q
```

Result: `14 passed`.

## Projection Runs

### Current May 8 Paper-Runner Strategy Set

- Local output: `reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/projection_runtime_risk_fill_haircut_20260512/`
- GCS output: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/current_paper_runtime_risk_fill_haircut_20260512/`
- Command/run ID: `current_paper_runtime_risk_fill_haircut_20260512`

```powershell
python scripts/build_portfolio_growth_projection.py --portfolio-report-json reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/current_paper_synthetic_portfolio_report.json --replay-root reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/filtered_replay_projection_compatible --output-dir reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/projection_runtime_risk_fill_haircut_20260512 --initial-cash 25000 --target-equity 300000 --risk-simulation-mode production_runtime --production-risk-config-yaml config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml --production-strategy-manifest-yaml config/promotion_manifests/multi_symbol_governed_validation_20260508_runtime_unique.yaml --production-strategy-manifest-yaml config/promotion_manifests/tsm_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config/promotion_manifests/avgo_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config/promotion_manifests/avgo_c109_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config/promotion_manifests/qqq_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config/promotion_manifests/regime_optional_governed_validation_20260508.yaml --projection-years 5 --bootstrap-runs 2000 --seed 7 --optimization-train-end-date 2026-01-30 --optimization-min-train-trades 5 --optimization-min-test-trades 5 --optimization-max-per-symbol-regime 2 --fill-model-enabled --fill-model-haircut-positive-pnl --unknown-fill-probability 1.0 --diversification-min-symbols 3 --diversification-min-regimes 2 --diversification-min-families 2 --diversification-max-symbol-trade-share 0.6 --diversification-max-family-trade-share 0.85 --diversification-max-regime-trade-share 0.8 --diversification-max-symbol-pnl-share 0.8
```

Result:

- Capital-plan strategies: `344`
- Accepted simulated trades: `2427`
- Ending equity: `$6072.73`
- Total return: `-75.7091%`
- Max drawdown: `-76.7295%`
- Fill-probability PnL haircut: `$2605560.9554`
- Train/test eligible candidates: `0`
- Optimizer status: `failed`
- Optimizer selected candidates: `0`

Market-quality diagnostics:

- Quote-source fields: `not present` in this replay-compatible lineage
- Trade-print source column: `entry_selection_trade_print_count`
- Trade-print coverage: `100.0%`
- Zero-or-missing trade-print rows: `24918`

### Benchmark `tt_top2_bull_choppy_up`

- Local output: `reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/tt_top2_bull_choppy_up_fill_haircut_20260512/`
- GCS output: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/tt_top2_bull_choppy_up_fill_haircut_20260512/`
- Command/run ID: `tt_top2_bull_choppy_up_fill_haircut_20260512`

Result:

- Capital-plan strategies: `14`
- Accepted simulated trades: `933`
- Ending equity: `$12197.82`
- Total return: `-51.2087%`
- Max drawdown: `-54.2939%`
- Fill-probability PnL haircut: `$192937.69625`
- Train/test eligible candidates: `0`
- Optimizer status: `failed`
- Optimizer selected candidates: `0`

Market-quality diagnostics:

- Quote-source fields: `not present` in this replay-compatible lineage
- Trade-print source column: `entry_selection_trade_print_count`
- Trade-print coverage: `100.0%`
- Zero-or-missing trade-print rows: `1805`

### Latest Bear/Choppy Non-Single `bc5` Candidate Pool

- Local output: `reports/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/aggregate/growth_projection/optimizer_candidate_pool_projection_fill_haircut_20260512/`
- GCS output: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/bc5_optimizer_candidate_pool_projection_fill_haircut_20260512/`
- Command/run ID: `bc5_optimizer_candidate_pool_projection_fill_haircut_20260512`

Result:

- Capital-plan strategies: `19`
- Accepted simulated trades: `1801`
- Ending equity: `$0.00`
- Total return: `-100.0%`
- Max drawdown: `-100.0%`
- Fill-probability PnL haircut: `$2229455.347086`
- Train/test eligible candidates: `0`
- Optimizer status: `failed`
- Optimizer selected candidates: `0`

Market-quality diagnostics:

- Entry quote source: `option_bar_close_no_bid_ask` for `1801/1801` rows
- Exit quote source: `option_bar_close_no_bid_ask` for `1801/1801` rows
- Rows with any no-bid-ask source: `1801` (`100.0%`)
- Trade-print source column: `entry_selection_trade_print_count`
- Trade-print coverage: `100.0%`
- Zero-or-missing trade-print rows: `1801`

## Conclusion

The old positive projection and the latest bear/choppy review-like candidates do not survive conservative quote-realism economics. The main blocker is not promotion packet formatting; it is insufficient execution-quality evidence:

- Current replay-compatible paper lineage lacks bid/ask quote-source, spread, and quote-age fields.
- The latest `bc5` lineage has quote-source fields, but every accepted row is no-bid-ask option-bar close.
- After positive-PnL fill haircuts, no current paper, benchmark, or `bc5` candidate is train/test-positive.

No strategy should be added to the paper runner from this pass.

## Next Research Direction

1. Repair replay lineage so every paper-runner strategy can be replayed with bid/ask, quote-source, spread, and quote-age fields.
2. Prefer quote-backed replays over broad parameter expansion; otherwise the new hardening correctly converts apparent edge into negative expectancy.
3. Continue GCP sweeps only when outputs are evaluated with `--fill-model-haircut-positive-pnl`.
4. Treat microstructure websocket data as discovery-only until it has multi-day quote coverage and train/test split evidence.

## GCP Worker Cleanup

- Synced terminated worker: `micro-event-c92161-92672-20260512m4`
- Local synced path: `reports/gcp_research/microstructure_rare_event_overnight_20260507T2030ET/workers/micro_event_c92161_92672/`
- GCS source path: `gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/workers/micro_event_c92161_92672/`
- Deleted VM after artifact sync: `micro-event-c92161-92672-20260512m4` in `us-west1-a`
- Still running at last check: `micro-event-c92673-93184-20260512m4` in `us-east4-a`, progress `350/512`, review-like count `0`

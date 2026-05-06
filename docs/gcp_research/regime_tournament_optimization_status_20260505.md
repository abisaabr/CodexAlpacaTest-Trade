# Regime Tournament Optimization Status - 2026-05-05

This is a research-only status packet. It does not start trading, does not arm paper trading, does not change live manifests, and does not change risk policy. Promotion below means `eligible_for_promotion_review` / governed validation review only.

## Current Code State

- Branch target: `codex/phase2-fill-semantics-20260430`
- Latest pushed source commit before INTC/META launch: `6fcea08`
- Key optimization: `scripts/build_regime_rescue_research_inputs.py` now supports a full-regime symbol-generic grid.
- Full-regime grid shape: 166 variants per symbol for `bull,bear,choppy` when using `--bear-profile-set signal_window_refine --choppy-profile-set timewindow_quality_filter`.
- Validation: `21 passed` for `tests/test_build_regime_rescue_research_inputs.py` and `tests/test_run_option_aware_research_backtest.py`.
- Projection validation: `2 passed` for `tests/test_build_portfolio_growth_projection.py`.

## Completed Regime-Complete Packets

### QQQ/SPY

- Combined packet: `reports/gcp_research/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GCS packet: `gs://codexalpaca-control-us/research_results/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_promotion_packet/combined_promotion_packet/research_promotion_review_packet.json`
- Decision: `ready_for_governed_validation_review`
- Candidate count: `2772`
- Eligible count: `199`
- Unique eligible base candidates: `18`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bull,bear,choppy`
- Missing regimes: none
- Fill gate: `0.90` strategy fill coverage

Top examples:

| Symbol | Regime | Family | Min Net PnL | Test Net PnL | Fill Coverage | Trades |
|---|---:|---|---:|---:|---:|---:|
| QQQ | bull | single_leg_repair | 4495.259 | 4234.781 | 0.9891 | 181 |
| QQQ | bear | single_leg_repair | 3733.520 | 2252.189 | 1.0000 | 34 |
| QQQ | choppy | single_leg_repair | 3738.727 | 1948.970 | 0.9913 | 229 |
| SPY | bull | single_leg_repair | 6076.333 | 734.633 | 0.9842 | 187 |
| SPY | bear | single_leg_repair | 2535.879 | 1564.911 | 1.0000 | 26 |
| SPY | choppy | single_leg_repair | 4096.054 | 1297.555 | 0.9858 | 208 |

### IWM

- Packet: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/promotion_review_packet/research_promotion_review_packet.json`
- Decision: `ready_for_governed_validation_review`
- Candidate count: `1272`
- Eligible count: `8`
- Unique eligible base candidates: `8`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bull,bear,choppy`
- Missing regimes: none
- Note: this older packet predates the latest explicit fill-semantics fields in the packet summary, but candidate rows still carry fill coverage and the strict 0.90 gate was enforced.

Top examples:

| Symbol | Regime | Family | Min Net PnL | Test Net PnL | Fill Coverage | Trades |
|---|---:|---|---:|---:|---:|---:|
| IWM | bull | single_leg_repair | 617.845 | 829.028 | 0.9940 | 166 |
| IWM | bear | single_leg_repair | 1419.675 | 1689.620 | 0.9056 | 163 |
| IWM | choppy | single_leg_repair | 2675.750 | 284.970 | 1.0000 | 57 |

## AAPL/NVDA Wave Result

- Wave ID: `aapl_nvda_full_regime_rescue_20260505T2245Z`
- GCS root: `gs://codexalpaca-control-us/research_results/aapl_nvda_full_regime_rescue_20260505T2245Z/`
- Source commit: `72c1174`
- Symbols: `AAPL`, `NVDA`
- Grid: `bull,bear,choppy`
- Per-symbol candidates: `166`
- Candidate shard size: `21`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Bear profile set: `signal_window_refine`
- Choppy profile set: `timewindow_quality_filter`

Launch state at this handoff:

Result:

- AAPL packet: `reports/gcp_research/aapl_nvda_full_regime_rescue_20260505T2245Z/aggregate/aapl_promotion_packet/research_promotion_review_packet.json`
- NVDA packet: `reports/gcp_research/aapl_nvda_full_regime_rescue_20260505T2245Z/aggregate/nvda_promotion_packet/research_promotion_review_packet.json`
- Combined packet: `reports/gcp_research/aapl_nvda_full_regime_rescue_20260505T2245Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GCS combined packet: `gs://codexalpaca-control-us/research_results/aapl_nvda_full_regime_rescue_20260505T2245Z/aggregate/combined_promotion_packet/combined_promotion_packet/research_promotion_review_packet.json`
- Decision: `research_only_blocked_regime_incomplete`
- AAPL: `498` candidates, `6` eligible, eligible regimes `bull`, missing regimes `bear,choppy`.
- NVDA: `498` candidates, `6` eligible, eligible regimes `bull`, missing regimes `bear,choppy`.
- Combined: `996` candidates, `12` eligible, eligible regimes `bull`, missing regimes `bear,choppy`.
- Dominant blockers: full-period net PnL and test net PnL, not fill coverage.
- AAPL blocker counts: `fill_coverage_below_0.90=19`, `min_net_pnl_not_positive=491`, `test_net_pnl_not_above_0=446`.
- NVDA blocker counts: `fill_coverage_below_0.90=24`, `min_net_pnl_not_positive=489`, `test_net_pnl_not_above_0=455`.
- Completed AAPL/NVDA workers self-terminated. The 16 terminated `regime-rescue` instances were deleted after outputs were uploaded and mirrored.

## AMD/AMZN Wave Result

- Wave ID: `amd_amzn_full_regime_rescue_20260505T2315Z`
- GCS root: `gs://codexalpaca-control-us/research_results/amd_amzn_full_regime_rescue_20260505T2315Z/`
- Source commit lineage: `97c830a/ae7b37a/6fcea08`
- Symbols: `AMD`, `AMZN`
- Grid: `bull,bear,choppy`
- Per-symbol candidates: `166`
- Candidate shard size: `21`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Bear profile set: `signal_window_refine`
- Choppy profile set: `timewindow_quality_filter`
- Launch/result state: all 8 AMD shards and all 8 AMZN shards completed and uploaded.
- Combined packet: `reports/gcp_research/amd_amzn_full_regime_rescue_20260505T2315Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GCS combined packet: `gs://codexalpaca-control-us/research_results/amd_amzn_full_regime_rescue_20260505T2315Z/aggregate/combined_promotion_packet/combined_promotion_packet/research_promotion_review_packet.json`
- Combined decision: `ready_for_governed_validation_review`
- Combined candidates: `996`
- Combined eligible count: `169`
- Combined unique eligible base candidates: `20`
- Combined eligible regimes: `bull,bear,choppy`
- Combined missing regimes: none
- AMD standalone: `498` candidates, `130` eligible, eligible regimes `bear,choppy`, missing regime `bull`.
- AMZN standalone: `498` candidates, `39` eligible, eligible regimes `bull,bear`, missing regime `choppy`.
- Dominant blockers across combined packet: `fill_coverage_below_0.90=52`, `min_net_pnl_not_positive=662`, `test_net_pnl_not_above_0=687`.
- Interpretation: use AMD/AMZN as a combined governed-review sleeve only. Do not treat either symbol as standalone regime-complete yet.

Top combined examples:

| Symbol | Regime | Family | Min Net PnL | Test Net PnL | Fill Coverage | Trades |
|---|---:|---|---:|---:|---:|---:|
| AMZN | bull | single_leg_repair | 7488.222 | 12040.116 | 0.9810 | 206 |
| AMZN | bull | single_leg_repair | 6586.023 | 7815.760 | 0.9729 | 215 |
| AMD | choppy | single_leg_repair | 3160.280 | 2043.181 | 0.9722 | 35 |
| AMD | choppy | single_leg_repair | 3030.303 | 1839.397 | 0.9677 | 30 |
| AMZN | bear | single_leg_repair | 1465.993 | 1886.157 | 0.9623 | 51 |

## Portfolio Growth Tracker

These projections are research-only, trade-level compounded curves. They do not authorize paper orders or live manifest changes.

### QQQ/SPY/IWM

- Projection packet: `reports/gcp_research/qqq_spy_iwm_institutional_projection_20260505T2330Z/projection/portfolio_growth_projection.json`
- GCS packet: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_institutional_projection_20260505T2330Z/qqq_spy_iwm_institutional_projection_20260505T2330Z/projection/portfolio_growth_projection.json`
- Symbols: `QQQ,SPY,IWM`
- Starting cash: `$25,000`
- Ending equity: `$158,599.87`
- Total return: `534.3995%`
- Max drawdown: `-26.6845%`
- Matched trades: `859`
- Active days: `245 / 251`
- Evidence grade: `directional_expectation_only`

### QQQ/SPY/IWM/AMD/AMZN

- Projection packet: `reports/gcp_research/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/portfolio_growth_projection.json`
- GCS packet: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/portfolio_growth_projection.json`
- Symbols: `QQQ,SPY,IWM,AMD,AMZN`
- Starting cash: `$25,000`
- Ending equity: `$198,790.49`
- Total return: `695.1620%`
- Max drawdown: `-25.5938%`
- Matched trades: `1575`
- Active days: `249 / 251`
- Symbol weights after portfolio-level cap: `AMD=0.1875`, `AMZN=0.1875`, `IWM=0.25`, `QQQ=0.1875`, `SPY=0.1875`
- Strategy-regime coverage: `bull=3`, `bear=9`, `choppy=7` capital-plan candidates.
- Evidence grade: `directional_expectation_only`

## INTC/META Wave Result

- Wave ID: `intc_meta_full_regime_rescue_20260506T0005Z`
- GCS root: `gs://codexalpaca-control-us/research_results/intc_meta_full_regime_rescue_20260506T0005Z/`
- Source commit: `6fcea08`
- Symbols: `INTC`, `META`
- Grid: `bull,bear,choppy`
- Per-symbol candidates: `166`
- Candidate shard size: `21`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Bear profile set: `signal_window_refine`
- Choppy profile set: `timewindow_quality_filter`
- Launch/result state: all 8 INTC shards and all 8 META shards completed and uploaded.
- Combined packet: `reports/gcp_research/intc_meta_full_regime_rescue_20260506T0005Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GCS combined packet: `gs://codexalpaca-control-us/research_results/intc_meta_full_regime_rescue_20260506T0005Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Combined decision: `research_only_blocked_regime_incomplete`
- Combined candidates: `996`
- Combined eligible count: `1`
- Combined unique eligible base candidates: `1`
- Combined eligible regimes: `bear`
- Combined missing regimes: `bull,choppy`
- INTC standalone: `498` candidates, `1` eligible, eligible regimes `bear`, missing regimes `bull,choppy`.
- META standalone: `498` candidates, `0` eligible, missing regimes `bull,bear,choppy`.
- Dominant combined blockers: `fill_coverage_below_0.90=529`, `min_net_pnl_not_positive=861`, `test_net_pnl_not_above_0=613`.
- Dominant combined fill-failure classes: `fill_gate_clear=467`, `position_sizing_too_expensive=462`, `selected_contract_universe_gap=54`, `entry_bar_gap_or_entry_timing_mismatch=13`.
- Interpretation: do not add INTC/META to the cumulative growth tracker yet. INTC has one bear research lead; META is mainly blocked by option cost/position sizing and fill coverage under the current `$25,000`/`0.05` allocation semantics.
- Completed INTC/META workers self-terminated. The 16 terminated `regime-rescue` instances were deleted after outputs were uploaded and mirrored.

## Active GCP Wave

- Wave ID: `msft_tsla_full_regime_rescue_20260506T0025Z`
- GCS root: `gs://codexalpaca-control-us/research_results/msft_tsla_full_regime_rescue_20260506T0025Z/`
- Source commit: `2697eaa`
- Symbols: `MSFT`, `TSLA`
- Grid: `bull,bear,choppy`
- Per-symbol candidates: `166`
- Candidate shard size: `21`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Bear profile set: `signal_window_refine`
- Choppy profile set: `timewindow_quality_filter`
- Launch state: all 8 MSFT shards and all 8 TSLA shards launched.
- Active VM count: `16` research-only `regime-rescue` VMs.
- Capacity note: some zones were resource constrained, but the launcher rerouted and completed all shard launches.

## Data Coverage Inventory

365d 5x5 stock, selected-contract, and option-bar prefixes exist under `option_fill_ladder_20260429` for:

- `AAPL`
- `AMD`
- `AMZN`
- `INTC`
- `IWM`
- `META`
- `MSFT`
- `NVDA`
- `SPY`
- `TSLA`

QQQ uses the separate 365d next-trading-day 5x5 prefix:

- Stock: `gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/`
- Selected contracts: `gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/`
- Option bars: `gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/`

## Next Loop

1. Monitor `msft_tsla_full_regime_rescue_20260506T0025Z` until all 16 shards complete.
2. Pull worker artifacts locally and build strict portfolio reports plus promotion-review packets per symbol and combined.
3. If MSFT/TSLA are regime-complete, record them as governed-review candidates only and update the cumulative growth tracker.
4. If a regime is missing, classify the dominant blocker: fill coverage, full-period economics, test PnL, or trade count.
5. Do not launch additional ticker pairs until MSFT/TSLA aggregate is complete or until capacity is clearly idle.
6. Do not lower the 0.90 fill gate, do not change live manifests, and do not start broker-facing sessions.

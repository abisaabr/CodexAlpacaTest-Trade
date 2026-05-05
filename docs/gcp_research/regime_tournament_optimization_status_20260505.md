# Regime Tournament Optimization Status - 2026-05-05

This is a research-only status packet. It does not start trading, does not arm paper trading, does not change live manifests, and does not change risk policy. Promotion below means `eligible_for_promotion_review` / governed validation review only.

## Current Code State

- Branch target: `codex/phase2-fill-semantics-20260430`
- Latest pushed source commit for new launches: `72c1174`
- Key optimization: `scripts/build_regime_rescue_research_inputs.py` now supports a full-regime symbol-generic grid.
- Full-regime grid shape: 166 variants per symbol for `bull,bear,choppy` when using `--bear-profile-set signal_window_refine --choppy-profile-set timewindow_quality_filter`.
- Validation: `21 passed` for `tests/test_build_regime_rescue_research_inputs.py` and `tests/test_run_option_aware_research_backtest.py`.

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

## Active GCP Wave

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

- AAPL shards `1-21`, `22-42`, and `43-63` completed.
- AAPL shards `64-84`, `85-105`, `106-126`, `127-147`, and `148-166` are running.
- NVDA shards `1-21`, `22-42`, `43-63`, `64-84`, `85-105`, `106-126`, `127-147`, and `148-166` are running or staging.
- A quota cleanup deleted 38 terminated `regime-rescue` instances only. Running workers were not touched.

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

1. Monitor `aapl_nvda_full_regime_rescue_20260505T2245Z` until all 16 shards complete.
2. Pull worker artifacts locally and build strict portfolio reports plus promotion-review packets per symbol and combined.
3. If AAPL/NVDA are regime-complete, record them as governed-review candidates only.
4. If a regime is missing, classify the dominant blocker: fill coverage, full-period economics, test PnL, or trade count.
5. Launch the next available pair from `AMD,AMZN,INTC,META,MSFT,TSLA` using the same full-regime grid, staying within project CPU/instance quotas.
6. Do not lower the 0.90 fill gate, do not change live manifests, and do not start broker-facing sessions.

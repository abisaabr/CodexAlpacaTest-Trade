# Projection Lineage, Optimizer, And Bear/Choppy Sweep

Generated: 2026-05-08

Scope: research-only hardening for projection accuracy and new bear/choppy edge discovery. This did not change the active paper trader, live manifests, paper config, or global risk policy.

## Source Commit

- Commit: `50c5d7207b96cf3c1cd1f1f778903ab675b26f44`
- Branch: `codex/phase2-fill-semantics-20260430`
- Test status: `python -m pytest -q` -> `301 passed, 1 warning`

## Backtester Economics Patch

`scripts/run_option_aware_research_backtest.py` now emits quote-quality fields in option trade economics when the replay data has the inputs:

- Entry/exit quote time and option bar time
- Entry/exit bid, ask, mid, absolute spread, relative spread, and spread pct
- Entry/exit quote age seconds and option-bar lag seconds
- Entry/exit quote source, distinguishing `option_quote_bid_ask` from `option_bar_close_no_bid_ask`
- Aggregated multi-leg quote-quality fields, including average/max relative spread and legs with bid/ask

This keeps old bar-only datasets usable while explicitly labeling them as no-bid/ask economics.

## Replay Lineage Repair

New tool:

- `scripts/repair_projection_replay_lineage.py`

May 8 paper strategy lineage audit:

- Input portfolio report: `reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/current_paper_synthetic_portfolio_report.json`
- Replay root: `reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/filtered_replay_projection_compatible`
- Search root: `reports/gcp_research`
- Output root: `reports/gcp_research/lineage_repair_may8_paper_20260508T1710Z`
- Capital-plan rows audited: `344`
- Matched current rows: `228`
- Repaired from local search: `0`
- Still unmatched: `116`
- Unmatched by symbol: `AMD 15`, `AMZN 5`, `AVGO 20`, `GOOGL 20`, `MSFT 20`, `QQQ 8`, `SPY 6`, `TSLA 2`, `TSM 20`

Interpretation: no hidden local replay artifacts were found for the remaining unmatched rows. Those need GCS artifact recovery or fresh backtest reconstruction.

## Benchmark Portfolio

Use `tt_top2_bull_choppy_up` as the benchmark portfolio for future projection comparisons.

Benchmark output:

- Projection: `reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/tt_top2_bull_choppy_up_final_2000/portfolio_growth_projection.json`
- Trade curve: `reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/tt_top2_bull_choppy_up_final_2000/portfolio_growth_scaled_trades.csv`
- Scenario summary: `reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/scenario_summary.json`

Benchmark metrics:

- Starting equity: `$25,000`
- Ending equity: `$35,949.02`
- Net PnL: `$10,949.02`
- Average daily PnL: `$43.62`
- Max drawdown: `-18.2398%`
- Accepted trades: `1,389`
- One-year bootstrap median ending equity: `$36,250.67`
- Five-year bootstrap median ending equity: `$160,453.62`

This is a research benchmark, not an automatic paper-config change.

## Constrained Optimizer

New tool:

- `scripts/optimize_portfolio_projection_candidates.py`

The first constrained run used train/test-positive candidates from the hardened current-book replay with symbol, regime, family, and drawdown caps.

Output root:

- `reports/gcp_research/constrained_optimizer_may8_current_20260508T1710Z`

Result:

- Status: `passed`
- Candidate count: `160`
- Eligible candidate count: `11`
- Selected candidate count: `11`
- Selected symbols: `AVGO 2`, `INTC 1`, `IWM 2`, `META 1`, `QQQ 3`, `SPY 2`
- Selected regimes: `bull 6`, `bear 2`, `choppy 3`
- Selected families: `Single-leg long call 8`, `Single-leg long put 1`, `broken_wing_put_butterfly 1`, `debit_call_vertical 1`
- Ending equity: `$32,722.25`
- Net PnL: `$7,722.25`
- Average daily PnL: `$31.65`
- Max drawdown: `-4.7454%`

Interpretation: the constrained optimizer reduced drawdown substantially versus the benchmark, but it also reduced expected PnL. The current edge set is still below the `$200/day` target.

## Active GCP Sweep

Research-only wave:

- Wave ID: `bear_choppy_train_test_refine_20260508T1730Z`
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_train_test_refine_20260508T1730Z/`
- Source archive: `gs://codexalpaca-control-us/research_results/bear_choppy_train_test_refine_20260508T1730Z/inputs/source/codexalpaca_repo_source.tar.gz`
- Source commit archived: `50c5d7207b96cf3c1cd1f1f778903ab675b26f44`
- Target regimes: `bear,choppy`
- Profile sets: bear `signal_window_refine`, choppy `timewindow_quality_filter`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Candidate count per worker: `24`
- First tranche: `c001-048` for each symbol
- Symbols launched: `QQQ`, `SPY`, `IWM`, `AVGO`

Running VMs at launch:

- `qqq-rescue-c001-024-20260508bc1`, `qqq-rescue-c025-048-20260508bc1`
- `spy-rescue-c001-024-20260508bc1`, `spy-rescue-c025-048-20260508bc1`
- `iwm-rescue-c001-024-20260508bc1`, `iwm-rescue-c025-048-20260508bc1`
- `avgo-rescue-c001-024-20260508bc1`, `avgo-rescue-c025-048-20260508bc1`

Local launch rows:

- `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/qqq_regime_rescue_launch_rows.json`
- `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/spy_regime_rescue_launch_rows.json`
- `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/iwm_regime_rescue_launch_rows.json`
- `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/avgo_regime_rescue_launch_rows.json`

First-tranche aggregate:

- Aggregate report: `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/aggregate/portfolio_report/research_wave_portfolio_rollup.json`
- Promotion packet: `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/aggregate/portfolio_report/promotion_review_packet/research_promotion_review_packet.json`
- Decision: `ready_for_governed_validation_review`
- Source reports: `8`
- Candidate count: `400`
- Eligible review candidates: `82`
- Unique eligible base candidates: `34`
- Eligible regimes: `bear,choppy`
- Capital-plan count: `8`
- Symbol eligible counts: `SPY 34`, `QQQ 22`, `AVGO 21`, `IWM 5`

First-tranche projection:

- Projection: `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/aggregate/growth_projection/capital_plan_projection/portfolio_growth_projection.json`
- Matched capital-plan coverage: `8/8`
- Starting equity: `$25,000`
- Ending equity: `$111,144.44`
- Net PnL: `$86,144.44`
- Average active-day PnL: about `$402.54/day` across `214` active days
- Max drawdown: `-49.4021%`
- Evidence grade: `not_institutional_expectation`

First-tranche constrained optimizer:

- Strict output: `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/aggregate/constrained_optimizer/optimizer_summary.json`
- Strict status: `failed`
- Strict blocker: no train/test-positive subset met drawdown `<=22%` while also requiring at least `3` symbols, `2` regimes, and `2` families.
- Relaxed diagnostic output: `reports/gcp_research/bear_choppy_train_test_refine_20260508T1730Z/aggregate/constrained_optimizer_dd50_minfam1/optimizer_summary.json`
- Relaxed diagnostic status: `passed`
- Relaxed diagnostic selected candidates: `4`
- Relaxed diagnostic symbols: `AVGO 1`, `QQQ 2`, `SPY 1`
- Relaxed diagnostic regimes: `bear 3`, `choppy 1`
- Relaxed diagnostic families: `single_leg_repair 4`
- Relaxed diagnostic ending equity: `$103,239.92`
- Relaxed diagnostic average daily PnL: `$535.89`
- Relaxed diagnostic max drawdown: `-31.4371%`

Interpretation: the first tranche found strong bear/choppy candidates, but the strict institutional optimizer correctly rejects the basket because the train/test-positive subset is family-concentrated and still too volatile. This is a research breakthrough, not a paper-runner change by itself.

Second active tranche:

- Wave ID: `bear_choppy_train_test_refine_20260508T1745Z`
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_train_test_refine_20260508T1745Z/`
- Source commit archived: `fe783b6f4ba7f7a7a31d70e7042f506d45047eef`
- Target regimes: `bear,choppy`
- First candidate index: `49`
- Candidate count per worker: `24`
- Active ranges: `c049-072` and `c073-096` for `QQQ`, `SPY`, `IWM`, and `AVGO`
- Instance suffix: `20260508bc2`

Next monitor action: when second-tranche workers terminate, sync artifacts, build strict portfolio reports and promotion-review packets, compare against `tt_top2_bull_choppy_up`, delete only synced terminated VMs, and continue non-overlapping candidate ranges if capacity is available.

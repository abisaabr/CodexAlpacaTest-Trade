# 365d Dataset And Paper Readiness - 2026-05-02

## Dataset Inventory

- Configured 365d backtest tickers: `20`
- Verified 365d backtest-ready tickers in GCS: `20`
- Ready symbols: `AAPL AMD AMZN INTC IWM META MSFT NVDA SPY TSLA AVGO GOOGL MU NFLX ORCL PLTR QQQ TSM XLE XOM`
- Required roots per ticker: stock bars, selected option contracts, and option bars.
- Option-bar partition span: `2025-04-29` through `2026-04-28` for the configured 365d datasets.
- Corrected inventory packet: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/inventory/365d_dataset_inventory_20260502.json`
- Human-readable inventory packet: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/inventory/365d_dataset_inventory_20260502.md`

Backtest-ready means the required 365d GCS roots exist and contain objects. It does not mean a ticker has an eligible promoted strategy.

## Growth Projection Contract

The account-growth projection must be built only from the completed 365d replay portfolio report, not from configuration, candidate manifests, or partial worker summaries.

Canonical projection command shape:

```powershell
python scripts\build_portfolio_growth_projection.py `
  --portfolio-report-json <aggregate_portfolio_report_json> `
  --replay-root <completed_365d_replay_root> `
  --output-dir <growth_projection_output> `
  --initial-cash 25000 `
  --target-equity 300000 `
  --backtest-allocation-fraction 0.05 `
  --bootstrap-runs 2000
```

Current completed QQQ-only 365d projection:

- Prefix: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/`
- Promotion packet: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/promotion_packet/qqq_365d_promotion_packet/research_promotion_review_packet.json`
- Growth projection: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/growth_projection/qqq_365d_growth_projection/portfolio_growth_projection.json`
- Result: `$25,000` to `$35,845.88` over `78` matched trading days.
- Evidence grade: `directional_expectation_only`.
- Warnings: less than 252 trading days, annualized volatility above 80%, high CAGR extrapolation requires walk-forward confirmation, and bootstrap probability of 50% drawdown above 5%.

## Promotion And Paper-Trader Readiness

Paper-trader readiness requires all of the following:

- Completed all-ticker 365d aggregate portfolio report.
- Completed all-ticker promotion-review packet.
- Candidate says `eligible_for_promotion_review`.
- `fill_coverage >= 0.90`.
- `min_option_trades >= 20`.
- `min_test_net_pnl > 0`.
- `min_net_pnl > 0`.
- Strategy identity preserved through replay, portfolio report, promotion packet, and paper-trader staging packet.
- No live manifest or risk-policy change without explicit operator approval.

Current status:

- QQQ-only 365d run has eligible governed-review candidates.
- Eligible QQQ regimes currently include bull and choppy; no eligible QQQ bear candidate has been confirmed.
- All-ticker aggregate is still pending because the sharded ticker workers have not produced all `40` expected selector summaries.
- The paper trader is not yet considered ready for final strategy activation.

## Option Data Repair Contract

If a ticker fails promotion because raw data coverage is low, use targeted repair rather than broad downloading.

Required flow:

```powershell
python scripts\build_option_data_repair_plan.py <args>
python scripts\download_option_market_data_for_selected_contracts.py <args>
```

Repair should be triggered only when replay diagnostics show missing selected contract-days or missing entry/exit option bars. If raw data roots are present and data-foundation coverage is high, fix strategy timing, contract selection, or replay semantics first.

## Reproducibility Logs

- Watchdog status: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/watchdog/ticker_365d_watch_status_20260501.json`
- Watchdog local log: `logs/ticker_365d_watchdog.log`
- All-ticker wave doc: `docs/gcp_research/ticker_365d_all_available_backtest_20260501.md`
- Dataset inventory: `gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z/inventory/365d_dataset_inventory_20260502.json`

## Hard Rules

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Promotion means governed validation review until an eligible packet is reviewed and explicitly approved for paper staging.

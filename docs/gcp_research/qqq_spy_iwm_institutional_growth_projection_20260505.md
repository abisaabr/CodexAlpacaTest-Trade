# QQQ/SPY/IWM Institutional Growth Projection

Generated: 2026-05-05

Scope: research-only compounded portfolio projection for the current governed-review QQQ/SPY/IWM capital plans. This does not authorize trading, paper orders, live-manifest edits, risk-policy edits, or any relaxation of the `fill_coverage >= 0.90` gate.

## Inputs

- QQQ/SPY governed packet source: `reports/gcp_research/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- IWM governed packet source: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/promotion_review_packet/research_promotion_review_packet.json`
- IWM trade economics pulled from GCS:
  - `gs://codexalpaca-control-us/research_results/ticker365_iwm_regime_rescue_fast_20260505T1345Z/workers/iwm_rescue_c001_012/reports/research_wave/iwm_rescue_c001_012_iwm_e0_x60_nearest_contract/iwm_rescue_c001_012_iwm_e0_x60_nearest_contract/option_aware_trade_economics.csv`
  - `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_quality_filter_20260505T2003Z/workers/iwm_rescue_c049_060/reports/research_wave/iwm_rescue_c049_060_iwm_e0_x60_entry_liquidity_first_research_only/iwm_rescue_c049_060_iwm_e0_x60_entry_liquidity_first_research_only/option_aware_trade_economics.csv`
- Projection calendar: 251 trade dates from QQQ/SPY/IWM 365d option-bar partitions, 2025-04-29 through 2026-04-28.
- Calendar market-regime labels: not provided; strategy-regime labels are from the selected capital-plan candidates.

## Method

The projection uses trade-level `option_aware_trade_economics.csv`, not just packet-level PnL totals. The combined plan reweights the source QQQ/SPY and IWM capital plans into one $25,000 portfolio with a `0.35` maximum symbol weight:

- QQQ: `0.325`
- SPY: `0.325`
- IWM: `0.35`

The curve compounds by trade date, carries cash across inactive calendar days, and scales backtest trade PnL from the research allocation fraction of `0.05` to the active candidate weight at the start of each trade date.

## Result

- Evidence grade: `directional_expectation_only`
- Matched trades: `859`
- Full calendar days: `251`
- Strategy active days: `245`
- Inactive cash days: `6`
- Ending equity from `$25,000`: `$158,599.87`
- Full-period return: `534.3995%`
- Max drawdown: `-$27,416.53`
- Max drawdown pct: `-26.6845%`
- Historical curve reached `$300,000`: `false`

Strategy-regime contribution:

| Strategy Regime | Capital Plan Count | Active Days | Trades | Scaled PnL |
| --- | ---: | ---: | ---: | ---: |
| Bull | 1 | 179 | 181 | `$4,183.01` |
| Bear | 7 | 180 | 356 | `$94,398.72` |
| Choppy | 3 | 211 | 322 | `$35,018.14` |

Bootstrap summary from 5,000 resamples:

| Horizon | P10 Ending | Median Ending | P90 Ending | Target Hit Probability |
| --- | ---: | ---: | ---: | ---: |
| 0.25 years | `$26,115.88` | `$39,101.85` | `$59,674.28` | `0.0%` |
| 0.5 years | `$34,276.39` | `$62,072.18` | `$116,574.43` | `0.04%` |
| 1.0 years | `$68,673.85` | `$158,212.42` | `$372,331.93` | `17.12%` |
| 2.0 years | `$294,494.67` | `$1,016,411.01` | `$3,433,790.75` | `89.7%` |

## Institutional Interpretation

This is much better than the earlier packet-sum estimate because it uses chronological trade-level compounding and full-calendar cash carry. It is still not an institutional expectation. The evidence grade remains `directional_expectation_only` because the projection has inactive cash days, lacks independent calendar market-regime labels, and the historical CAGR is high enough that extrapolation requires walk-forward and broker-audited paper validation.

The most important portfolio finding is that the current selected sleeve is bear-heavy. Bull exposure exists, but only one selected capital-plan strategy represents bull. Before treating this as a production-like portfolio target, the next research step should be a regime-balanced capital-plan pass that forces at least one promoted candidate per symbol/regime where eligible candidates exist, then reruns this same projection.

## Artifacts

- Projection JSON: `reports/gcp_research/qqq_spy_iwm_institutional_projection_20260505T2330Z/projection/portfolio_growth_projection.json`
- Projection markdown: `reports/gcp_research/qqq_spy_iwm_institutional_projection_20260505T2330Z/projection/portfolio_growth_projection.md`
- Full equity curve CSV: `reports/gcp_research/qqq_spy_iwm_institutional_projection_20260505T2330Z/projection/portfolio_growth_equity_curve.csv`
- Active-day equity curve CSV: `reports/gcp_research/qqq_spy_iwm_institutional_projection_20260505T2330Z/projection/portfolio_growth_active_day_equity_curve.csv`
- Scaled trades CSV: `reports/gcp_research/qqq_spy_iwm_institutional_projection_20260505T2330Z/projection/portfolio_growth_scaled_trades.csv`
- Calendar packet: `reports/gcp_research/qqq_spy_iwm_institutional_projection_20260505T2330Z/calendar/projection_calendar_packet.json`

## Commands

```powershell
python scripts\build_gcs_projection_calendar.py `
  --launch-rows-json reports\gcp_research\qqq_spy_iwm_institutional_projection_20260505T2330Z\inputs\projection_calendar_launch_rows.json `
  --output-dir reports\gcp_research\qqq_spy_iwm_institutional_projection_20260505T2330Z\calendar `
  --gcloud gcloud `
  --listing-cache-json reports\gcp_research\qqq_spy_iwm_institutional_projection_20260505T2330Z\calendar\projection_calendar_listing_cache.json

python scripts\build_portfolio_growth_projection.py `
  --portfolio-report-json reports\gcp_research\qqq_spy_combined_regime_portfolio_20260505T2240Z\aggregate\combined_portfolio_report\research_portfolio_report.json `
  --additional-portfolio-report-json reports\gcp_research\iwm_regime_rescue_quality_filter_20260505\promotion_review_packet\research_promotion_review_packet.json `
  --replay-root reports\research_wave\qqq_spy_fillfriendly_tournament_20260505T2035Z_final\workers `
  --additional-replay-root reports\gcp_research\qqq_spy_bear_signal_refine_20260505T221907Z\workers `
  --additional-replay-root reports\gcp_research\qqq_spy_bear_choppy_rescue_20260505T213830Z\workers `
  --additional-replay-root reports\gcp_research\qqq_spy_iwm_institutional_projection_20260505T2330Z\iwm_replay_profiles `
  --output-dir reports\gcp_research\qqq_spy_iwm_institutional_projection_20260505T2330Z\projection `
  --initial-cash 25000 `
  --target-equity 300000 `
  --backtest-allocation-fraction 0.05 `
  --max-symbol-weight 0.35 `
  --annual-trading-days 252 `
  --projection-years 5 `
  --bootstrap-runs 5000 `
  --seed 20260505 `
  --calendar-csv reports\gcp_research\qqq_spy_iwm_institutional_projection_20260505T2330Z\calendar\projection_calendar.csv
```

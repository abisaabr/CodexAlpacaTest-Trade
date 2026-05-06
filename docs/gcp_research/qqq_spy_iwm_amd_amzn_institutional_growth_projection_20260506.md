# QQQ/SPY/IWM/AMD/AMZN Institutional Growth Projection

Generated: 2026-05-06

Scope: research-only compounded portfolio projection after adding the AMD/AMZN governed-review combined sleeve to the existing QQQ/SPY/IWM governed-review projection. This does not authorize trading, paper orders, live-manifest edits, risk-policy edits, or any relaxation of the `fill_coverage >= 0.90` gate.

## Inputs

- QQQ/SPY governed packet source: `reports/gcp_research/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- IWM governed packet source: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/promotion_review_packet/research_promotion_review_packet.json`
- AMD/AMZN governed packet source: `reports/gcp_research/amd_amzn_full_regime_rescue_20260505T2315Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Projection calendar: 251 trade dates from 365d option-bar partitions, 2025-04-29 through 2026-04-28.
- Calendar market-regime labels: not provided; strategy-regime labels are from selected capital-plan candidates.

## Method

The projection uses trade-level `option_aware_trade_economics.csv`, not just packet-level PnL totals. The combined plan reweights selected governed-review capital-plan candidates into one `$25,000` account with a portfolio-level `0.25` maximum symbol weight:

| Symbol | Portfolio Weight |
| --- | ---: |
| AMD | `0.1875` |
| AMZN | `0.1875` |
| IWM | `0.25` |
| QQQ | `0.1875` |
| SPY | `0.1875` |

The curve compounds by trade date, carries cash across inactive calendar days, and scales backtest trade PnL from the research allocation fraction of `0.05` to the active candidate weight at the start of each trade date.

## Result

- Evidence grade: `directional_expectation_only`
- Capital-plan candidates: `19`
- Matched trades: `1575`
- Full calendar days: `251`
- Strategy active days: `249`
- Inactive cash days: `2`
- Ending equity from `$25,000`: `$198,790.49`
- Full-period return: `695.1620%`
- Max drawdown: `-$32,361.25`
- Max drawdown pct: `-25.5938%`
- Historical curve reached `$300,000`: `false`

Strategy-regime contribution:

| Strategy Regime | Capital Plan Count | Active Days | Trades | Scaled PnL |
| --- | ---: | ---: | ---: | ---: |
| Bull | 3 | 242 | 602 | `$51,529.90` |
| Bear | 9 | 201 | 457 | `$77,188.17` |
| Choppy | 7 | 233 | 516 | `$45,072.41` |

Bootstrap summary from 5,000 resamples:

| Horizon | P10 Ending | Median Ending | P90 Ending | Target Hit Probability |
| --- | ---: | ---: | ---: | ---: |
| 0.25 years | `$27,661.19` | `$41,701.60` | `$64,584.60` | `0.0%` |
| 0.5 years | `$38,598.08` | `$69,492.80` | `$130,692.96` | `0.22%` |
| 1.0 years | `$85,877.23` | `$200,999.74` | `$483,178.38` | `27.36%` |
| 2.0 years | `$479,634.77` | `$1,601,209.44` | `$5,491,715.03` | `96.16%` |

## Institutional Interpretation

This is a stronger portfolio-level research curve than the QQQ/SPY/IWM-only projection because AMD/AMZN added additional eligible bull, bear, and choppy candidates and reduced inactive days from 6 to 2. It is still not a production expectation. The evidence grade remains `directional_expectation_only` because the projection lacks independent calendar market-regime labels, contains high CAGR extrapolation risk, and still requires walk-forward plus broker-audited paper validation.

The most important portfolio finding is that the current sleeve is more diversified than the three-symbol plan, but it is still not a final paper-trader allocation. AMD and AMZN only clear as a combined governed-review sleeve; neither should be treated as a standalone regime-complete symbol yet.

## Artifacts

- Projection JSON: `reports/gcp_research/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/portfolio_growth_projection.json`
- Projection markdown: `reports/gcp_research/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/portfolio_growth_projection.md`
- Full equity curve CSV: `reports/gcp_research/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/portfolio_growth_equity_curve.csv`
- Active-day equity curve CSV: `reports/gcp_research/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/portfolio_growth_active_day_equity_curve.csv`
- Scaled trades CSV: `reports/gcp_research/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/portfolio_growth_scaled_trades.csv`
- GCS projection root: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/qqq_spy_iwm_amd_amzn_institutional_projection_20260506T0005Z/projection/`

## Current Next Step

Keep moving through the ticker ladder. The active wave is `intc_meta_full_regime_rescue_20260506T0005Z`; when it completes, aggregate INTC/META promotion-review packets, classify blockers by regime, and add any regime-complete governed-review candidates to this same portfolio-growth tracker before launching the next pair.

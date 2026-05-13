# QQQ/SPY/IWM/AMD/AMZN/MSFT/TSLA Institutional Growth Projection

Generated: 2026-05-06

Scope: research-only compounded portfolio projection after adding the MSFT/TSLA governed-review combined sleeve to the existing QQQ/SPY/IWM/AMD/AMZN projection. This does not authorize trading, paper orders, live-manifest edits, risk-policy edits, or any relaxation of the `fill_coverage >= 0.90` gate.

## Inputs

- QQQ/SPY governed packet source: `reports/gcp_research/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- IWM governed packet source: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/promotion_review_packet/research_promotion_review_packet.json`
- AMD/AMZN governed packet source: `reports/gcp_research/amd_amzn_full_regime_rescue_20260505T2315Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- MSFT/TSLA governed packet source: `reports/gcp_research/msft_tsla_full_regime_rescue_20260506T0025Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Projection calendar: 251 trade dates from 365d option-bar partitions, 2025-04-29 through 2026-04-28.
- Calendar market-regime labels: not provided; strategy-regime labels are from selected capital-plan candidates.

## Method

The projection uses trade-level `option_aware_trade_economics.csv`, not just packet-level PnL totals. The combined plan reweights selected governed-review capital-plan candidates into one `$25,000` account with a portfolio-level `0.25` maximum symbol weight:

| Symbol | Portfolio Weight |
| --- | ---: |
| AMD | `0.125` |
| AMZN | `0.125` |
| IWM | `0.25` |
| MSFT | `0.125` |
| QQQ | `0.125` |
| SPY | `0.125` |
| TSLA | `0.125` |

The curve compounds by trade date, carries cash across inactive calendar days, and scales backtest trade PnL from the research allocation fraction of `0.05` to the active candidate weight at the start of each trade date.

## Result

- Evidence grade: `directional_expectation_only`
- Capital-plan candidates: `25`
- Matched trades: `2192`
- Full calendar days: `251`
- Strategy active days: `250`
- Inactive cash days: `1`
- Ending equity from `$25,000`: `$144,462.65`
- Full-period return: `477.8506%`
- Max drawdown: `-$18,843.42`
- Max drawdown pct: `-22.9055%`
- Historical curve reached `$300,000`: `false`

Strategy-regime contribution:

| Strategy Regime | Capital Plan Count | Active Days | Trades | Scaled PnL |
| --- | ---: | ---: | ---: | ---: |
| Bull | 3 | 242 | 602 | `$25,465.50` |
| Bear | 15 | 239 | 1074 | `$62,713.31` |
| Choppy | 7 | 233 | 516 | `$31,283.84` |

Bootstrap summary from 5,000 resamples:

| Horizon | P10 Ending | Median Ending | P90 Ending | Target Hit Probability |
| --- | ---: | ---: | ---: | ---: |
| 0.25 years | `$27,060.74` | `$38,437.97` | `$56,862.09` | `0.0%` |
| 0.5 years | `$35,874.13` | `$59,856.44` | `$104,081.34` | `0.0%` |
| 1.0 years | `$69,024.37` | `$145,162.42` | `$310,724.42` | `11.0%` |
| 2.0 years | `$294,639.91` | `$841,353.52` | `$2,459,949.55` | `89.42%` |

## Institutional Interpretation

Adding MSFT/TSLA improved breadth, increased matched trades, and reduced historical max drawdown versus the five-symbol projection. It also reduced the compounded ending equity under the portfolio-level `0.25` symbol cap because capital was reweighted away from the highest-return earlier sleeves. That is the correct behavior for an institutional portfolio tracker: diversification and drawdown control can reduce raw backtest return while improving portfolio robustness.

The seven-symbol sleeve remains research-only. The evidence grade is still `directional_expectation_only` because the projection lacks independent calendar market-regime labels, contains high CAGR extrapolation risk, and requires walk-forward plus broker-audited paper validation.

## Artifacts

- Projection JSON: `reports/gcp_research/qqq_spy_iwm_amd_amzn_msft_tsla_institutional_projection_20260506T0100Z/projection/portfolio_growth_projection.json`
- Projection markdown: `reports/gcp_research/qqq_spy_iwm_amd_amzn_msft_tsla_institutional_projection_20260506T0100Z/projection/portfolio_growth_projection.md`
- Full equity curve CSV: `reports/gcp_research/qqq_spy_iwm_amd_amzn_msft_tsla_institutional_projection_20260506T0100Z/projection/portfolio_growth_equity_curve.csv`
- Active-day equity curve CSV: `reports/gcp_research/qqq_spy_iwm_amd_amzn_msft_tsla_institutional_projection_20260506T0100Z/projection/portfolio_growth_active_day_equity_curve.csv`
- Scaled trades CSV: `reports/gcp_research/qqq_spy_iwm_amd_amzn_msft_tsla_institutional_projection_20260506T0100Z/projection/portfolio_growth_scaled_trades.csv`
- GCS projection root: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_amd_amzn_msft_tsla_institutional_projection_20260506T0100Z/`

## Current Next Step

The ticker ladder has finished the known 365d data set through `QQQ,SPY,IWM,AMD,AMZN,INTC,META,MSFT,TSLA`, with `AAPL,NVDA` and `INTC,META` classified as blocked and `AMD,AMZN,MSFT,TSLA` useful only through their combined governed-review sleeves. The next efficient step is blocker-specific redesign for AAPL/NVDA/INTC/META/TSLA rather than launching more broad sweeps without data.

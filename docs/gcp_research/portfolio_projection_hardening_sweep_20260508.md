# Portfolio Projection Hardening Sweep

Generated: 2026-05-08

Scope: offline research-only projection tuning using the hardened projector. No paper-trader config, promotion manifest, live manifest, or global risk policy was changed.

## Inputs

- Current paper config: `config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`
- Synthetic portfolio report: `reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/current_paper_synthetic_portfolio_report.json`
- Replay root: `reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/filtered_replay_projection_compatible`
- Sweep root: `reports/gcp_research/portfolio_projection_hardening_sweep_20260508T1640Z/`
- Starting account: `$25,000`
- Target: `$200/day`, which requires about `$50,200` net PnL over the 251-trading-day backtest window.

## Result

The upgraded projector did not find a realistic `$200/day` configuration from the currently available strategy/replay set.

Best scenario:

- Scenario: `tt_top2_bull_choppy_up`
- Strategy set: train/test-positive candidates, max 2 per symbol/regime, original weights normalized
- Risk overlay: relaxed bull/choppy, bear downweighted
- Ending equity: `$35,949.02`
- Net PnL: `$10,949.02`
- Average daily PnL: `$43.62`
- Max drawdown: `-18.2398%`
- Accepted trades: `1,389`
- Evidence grade: `directional_expectation_only`
- `$200/day` target met: `false`

The same best scenario rerun with 2,000 bootstrap paths had:

- One-year median ending equity: `$36,250.67`
- Five-year median ending equity: `$160,453.62`
- Five-year `$300,000` target-hit probability: `16.6%`
- Probability of dropping below 50% of starting equity: `0.05%`
- Risk of ruin: `0.0%`

## Scenario Ranking

| Scenario | Ending Equity | Avg Daily PnL | Max DD | Accepted Trades |
| --- | ---: | ---: | ---: | ---: |
| `tt_top2_bull_choppy_up` | `$35,949.02` | `$43.62` | `-18.2398%` | `1,389` |
| `tt_top2_current` | `$33,601.13` | `$34.27` | `-17.2970%` | `1,489` |
| `tt_top2_relaxed` | `$33,547.39` | `$34.05` | `-16.7339%` | `1,654` |
| `tt_top2_aggressive` | `$33,525.91` | `$33.97` | `-17.2312%` | `1,668` |
| `tt_top1_relaxed` | `$32,875.65` | `$31.38` | `-9.9619%` | `1,331` |
| `tt_top1_aggressive` | `$32,865.59` | `$31.34` | `-9.9647%` | `1,333` |
| `tt_top1_current` | `$32,447.08` | `$29.67` | `-10.5549%` | `1,313` |
| `current_hardened` | `$28,081.23` | `$12.28` | `-25.4975%` | `3,643` |

## Hardening Findings

- The full current book is positive but weak: `$3,081.23` net PnL, about `$12.28/day`.
- Relaxing risk on the full book made results worse because it admitted more negative bear-sleeve exposure.
- Train/test filtering materially improved the book by reducing noisy strategy count and improving regime balance.
- The best filtered book is still about `4.6x` short of the `$200/day` target.
- A linear 4.6x risk scale would imply an unacceptable drawdown regime, roughly `80%+` using the best scenario's historical drawdown as a simple scale reference.
- The hardened match-coverage diagnostic still shows incomplete replay lineage for the current full book, with 116 unmatched capital-plan rows in the reconstructed projection inputs.
- Fill-probability thresholds of `0.65` and `0.85` rejected all current replay trades because the available projection-compatible replay lacks enough strong print/freshness evidence. That is a data-quality blocker, not a deployable portfolio setting.
- Spread and quote-age stress cannot yet be applied to this replay set because historical spread/quote-age columns are not present in the projection-compatible trade economics.

## Next Best Steps

1. Repair unmatched strategy replay lineage before trusting full-book projections. Every paper strategy should map to preserved replay trade economics by `base_candidate_variant_id` and `aggregate_profile`.
2. Add quote spread, quote age, and quote source fields to backtest trade economics so the hardened projector can run realistic spread/quote-age stress instead of rejecting missing data.
3. Use the `tt_top2_bull_choppy_up` subset as the research benchmark, not as an automatic production change. It improved expectancy, but it does not meet the daily PnL goal.
4. Focus new research on finding additional independent high-expectancy candidates, especially choppy and bear candidates that are train/test positive. The current target cannot be reached by knob turning alone.
5. Add a portfolio optimizer that selects candidates under explicit constraints: minimum symbols, minimum regimes, max ticker/family exposure, train/test-positive requirement, and drawdown cap.
6. Run GCP sweeps for new strategy families with quote-quality outputs enabled: debit/credit verticals, broken-wing butterflies, condors, volatility/gamma scalps, and event-driven realtime entries.
7. Treat `$200/day` as a portfolio edge-discovery goal, not a risk-scaling goal. The current evidence supports roughly `$30-$45/day` directional expectancy under the best filtered configurations.


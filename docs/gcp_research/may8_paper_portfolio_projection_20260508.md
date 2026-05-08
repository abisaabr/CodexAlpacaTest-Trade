# May 8 Paper Portfolio Growth Projection

Generated: 2026-05-08

Scope: research-only projection for the May 8 PAPER trader configuration. This does not authorize live trading, live manifest edits, or risk-policy changes.

## Inputs

- Paper config: `config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`
- Starting equity: `$25,000`
- Projection risk mode: `production_runtime`
- Strategy count in paper config: `344`
- Symbol count in paper config: `15`
- Strategy regimes: `76 bull`, `125 bear`, `143 choppy`
- Strategy manifests:
  - `config/promotion_manifests/multi_symbol_governed_validation_20260508_runtime_unique.yaml`
  - `config/promotion_manifests/tsm_overnight_governed_validation_20260508.yaml`
  - `config/promotion_manifests/avgo_overnight_governed_validation_20260508.yaml`
  - `config/promotion_manifests/avgo_c109_overnight_governed_validation_20260508.yaml`
  - `config/promotion_manifests/qqq_overnight_governed_validation_20260508.yaml`
  - `config/promotion_manifests/regime_optional_governed_validation_20260508.yaml`

## Output Paths

Local root:

- `reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/`

GCS mirror:

- `gs://codexalpaca-control-us/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/`

Main outputs:

- `projection_runtime_risk_compatible/portfolio_growth_projection.json`
- `projection_runtime_risk_compatible/portfolio_growth_projection.md`
- `projection_runtime_risk_compatible/portfolio_growth_equity_curve.csv`
- `projection_runtime_risk_compatible/portfolio_growth_active_day_equity_curve.csv`
- `projection_runtime_risk_compatible/portfolio_growth_scaled_trades.csv`
- `projection_runtime_risk_compatible/portfolio_growth_risk_events.csv`

## Replay Coverage

This is a comparable reconstruction, not a perfect exact full-book reproduction. The current PAPER strategy set references older promoted waves whose trade-level artifacts are only partially present locally.

- Requested unique base candidate IDs: `320`
- Matched base candidate IDs: `222`
- Unmatched base candidate IDs: `98`
- Requested aggregate profiles: `153`
- Matched aggregate profiles: `93`
- Unmatched aggregate profiles: `60`
- Matched replay trade rows written before risk simulation: `75,225`
- Projection matched trades after capital-plan filtering: `24,918`
- Production-risk accepted trades: `3,643`
- Production-risk rejected trades: `21,275`

## Production-Risk Gates Applied

The projection used the paper-trader runtime risk profile instead of a symbol-cap proxy:

- `max_open_positions: 999`
- `max_open_risk_fraction: 0.14`
- `max_open_risk_fraction_per_symbol: 0.08`
- `max_positions_per_symbol: 3`
- `max_positions_per_regime: 3`
- `max_positions_per_regime_window: 2`
- `max_positions_per_bucket_regime_window: 4`
- Bucket caps: `index_beta 0.08`, `growth_tech 0.09`, `energy 0.03`

Broker-equity floor was not enforced because this is a sleeve-level `$25,000` research projection, not a broker-account projection.

## Results

Historical compounded curve:

- Period: `2025-04-29` through `2026-04-28`
- Trading days: `251`
- Starting equity: `$25,000`
- Ending equity: `$28,081.23`
- Total return: `12.3249%`
- CAGR: `12.3769%`
- Annualized volatility: `24.8809%`
- Sharpe-like ratio: `0.5927`
- Max drawdown: `-$8,414.34`
- Max drawdown percent: `-25.4975%`

Accepted-trade PnL by strategy regime:

| Regime | Accepted Trades | Scaled PnL |
| --- | ---: | ---: |
| Bull | 1,021 | `$6,828.12` |
| Bear | 1,693 | `-$5,536.19` |
| Choppy | 929 | `$1,789.30` |

Bootstrap projection, 2,000 runs:

| Horizon | P10 Ending | Median Ending | P90 Ending |
| --- | ---: | ---: | ---: |
| 0.25 years | `$22,111.00` | `$25,823.39` | `$30,221.33` |
| 0.5 years | `$21,171.56` | `$26,500.71` | `$33,157.49` |
| 1 year | `$20,803.58` | `$28,417.28` | `$39,157.43` |
| 2 years | `$20,420.75` | `$31,585.08` | `$49,812.13` |
| 3 years | `$20,492.16` | `$35,371.89` | `$60,866.82` |
| 5 years | `$22,503.55` | `$45,469.93` | `$93,262.00` |

Other bootstrap risk stats:

- Five-year target hit probability for `$300,000`: `0.05%`
- Median worst drawdown: `-$15,955.00`
- Median worst drawdown percent: `-35.7033%`
- Probability of equity drawdown below 50% of starting capital: `3.55%`
- Risk of ruin: `0.0%`

## Command

```powershell
python scripts\build_portfolio_growth_projection.py --portfolio-report-json reports\gcp_research\may8_paper_portfolio_projection_20260508T1255Z\current_paper_synthetic_portfolio_report.json --replay-root reports\gcp_research\may8_paper_portfolio_projection_20260508T1255Z\filtered_replay_projection_compatible --output-dir reports\gcp_research\may8_paper_portfolio_projection_20260508T1255Z\projection_runtime_risk_compatible --initial-cash 25000 --target-equity 300000 --risk-simulation-mode production_runtime --production-risk-config-yaml config\multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml --production-strategy-manifest-yaml config\promotion_manifests\multi_symbol_governed_validation_20260508_runtime_unique.yaml --production-strategy-manifest-yaml config\promotion_manifests\tsm_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config\promotion_manifests\avgo_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config\promotion_manifests\avgo_c109_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config\promotion_manifests\qqq_overnight_governed_validation_20260508.yaml --production-strategy-manifest-yaml config\promotion_manifests\regime_optional_governed_validation_20260508.yaml --projection-years 5 --bootstrap-runs 2000 --seed 7
```

## Interpretation

The current paper book has positive reconstructed expectancy under production-risk gates, but the edge is uneven: bull and choppy sleeves are positive while the bear sleeve is negative in this reconstruction. The result should be treated as directional expectation only until every promoted strategy has preserved trade-level replay artifacts and the paper trader produces broker-audited order/fill evidence.

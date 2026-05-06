# Production Risk Projection Mode - 2026-05-06

## Change

`scripts/build_portfolio_growth_projection.py` now supports an opt-in production-style runtime risk simulator:

```powershell
python scripts\build_portfolio_growth_projection.py `
  --risk-simulation-mode production_runtime `
  --production-risk-config-yaml config\risk_controls\multi_ticker_portfolio.yaml
```

The default remains `capital_plan`, preserving prior behavior for existing reports.

## What The New Mode Simulates

The production mode replays option trades by entry/exit timestamp and admits entries through runtime-style gates:

- `max_open_positions`
- `max_positions_per_regime`
- `max_positions_per_symbol`
- `entry_cluster_window_minutes`
- `max_positions_per_regime_window`
- `max_positions_per_bucket_regime_window`
- `max_open_risk_fraction`
- `max_open_risk_fraction_per_symbol`
- `bucket_caps`
- debit cash availability
- drawdown deleveraging via `delever_drawdown_pct` and `delever_risk_scale`

It sizes accepted trades from `risk_per_unit`, `entry_debit_per_unit`, strategy `risk_fraction`, and strategy `max_contracts`. Strategy sizing can be loaded from one or more promotion/strategy manifests via `--production-strategy-manifest-yaml`; unmatched strategies use explicit projection defaults.

Broker-equity floor enforcement is available but disabled by default because growth projections usually model a sleeve, while broker account equity is external:

```powershell
--production-enforce-broker-equity-floor
```

## New Outputs

Production mode writes the existing projection outputs plus:

- `portfolio_growth_risk_events.csv`: accepted, rejected, and closed risk-gate events
- `production_risk_simulation` in `portfolio_growth_projection.json`
- a Markdown section summarizing accepted/rejected entries and dominant rejection reasons

## Seven-Symbol Integration Run

Output root:

`reports/gcp_research/qqq_spy_iwm_amd_amzn_msft_tsla_production_risk_projection_20260506T0135Z/projection/`

GCS mirror:

`gs://codexalpaca-control-us/research_results/qqq_spy_iwm_amd_amzn_msft_tsla_production_risk_projection_20260506T0135Z/`

Inputs:

- QQQ/SPY combined governed-review packet
- IWM governed-review packet
- AMD/AMZN combined governed-review packet
- MSFT/TSLA combined governed-review packet
- QQQ/SPY governed validation manifests for exact runtime sizing where available
- `config/risk_controls/multi_ticker_portfolio.yaml`
- broker-facing disabled
- live-manifest effect: none
- risk-policy effect: none

Result:

| Metric | Value |
|---|---:|
| Input trades | `2,192` |
| Accepted entries | `1,430` |
| Rejected entries | `762` |
| Starting equity | `$25,000.00` |
| Ending equity | `$37,010.45` |
| Total return | `48.0418%` |
| Max drawdown | `-18.4747%` |
| 5-year bootstrap target-hit probability to `$300,000` | `23.8%` |

Dominant rejection reasons:

| Reason | Count |
|---|---:|
| `per_symbol_risk_cap` | `335` |
| `risk_budget_too_small` | `179` |
| `bucket_risk_cap:growth_tech` | `90` |
| `bucket_regime_entry_cluster:growth_tech:bear` | `83` |
| `bucket_regime_entry_cluster:index_beta:choppy` | `26` |
| `bucket_regime_entry_cluster:index_beta:bear` | `15` |
| `bucket_risk_cap:index_beta` | `14` |
| `regime_entry_cluster:choppy` | `16` |
| `max_positions_per_symbol` | `3` |
| `missing_or_invalid_option_exit_time` | `1` |

Sizing lineage:

| Source | Trade Rows |
|---|---:|
| Strategy manifest sizing | `582` |
| Projection default sizing | `1,610` |

## Interpretation

This result is the current best production-style growth estimate for the seven-symbol research packet because it applies runtime-like admission, risk, bucket, symbol, cluster, and cash gates. It is materially more conservative than the uncapped source-weight projection, which allocated roughly `310%` of the account and produced an unrealistic upper-bound curve.

The remaining gap is strategy sizing lineage: QQQ/SPY had manifest sizing, but AMD/AMZN/IWM/MSFT/TSLA used projection defaults because corresponding runtime manifests were not present. Before treating this as paper-runner-ready sizing, generate governed validation manifests for those symbols or pass their exact manifests into the projection command.

## Verification

Direct function checks passed for:

- production sizing by `risk_fraction`
- per-symbol open-position rejection
- CLI help exposes the new production-mode arguments
- real seven-symbol projection completed successfully

`pytest` could not be run in the local environment because neither system Python nor the repo virtualenv has `pytest` installed. AST parsing passed for the modified script and tests.

## Hard Rule

This is research-only projection tooling. It does not authorize trading, paper orders, live-manifest edits, risk-policy edits, or lowering promotion gates.


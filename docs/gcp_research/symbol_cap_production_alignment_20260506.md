# Symbol Cap Production Alignment - 2026-05-06

## Decision

The portfolio growth projection was rerun with the projection-only `--max-symbol-weight` argument omitted so the projection no longer applies an artificial symbol-cap reweighting layer.

This does **not** change live manifests, broker-facing execution, or paper-runner risk policy. Production-style controls remain runtime risk controls, not research projection reweighting:

- `max_open_risk_fraction: 0.15`
- `max_open_risk_fraction_per_symbol: 0.05`
- `max_open_positions: 10`
- `max_positions_per_symbol: 3`
- bucket-level open-risk caps
- broker equity and buying-power guards

## Why This Matters

The prior seven-symbol projection used a `0.25` portfolio-level max symbol weight. That cap was useful for institutional concentration analysis, but it does not directly match the current paper runtime model. The paper runner is governed by open-risk, per-symbol risk, bucket, position-count, and broker-state gates.

Removing the projection cap makes the projection use source capital-plan weights exactly as emitted by each governed-review packet. That is closer to the research artifacts, but it also exposes a separate issue: merging multiple independent packet capital plans without normalization can over-allocate the same account.

## Production-Aligned No-Cap Rerun

Output root:

`reports/gcp_research/qqq_spy_iwm_amd_amzn_msft_tsla_production_aligned_projection_20260506T0125Z/projection/`

Command behavior:

- `--max-symbol-weight` omitted
- `capital_plan_merge.mode = source_capital_plan_weights`
- `max_symbol_weight = null`
- no live-manifest effect
- no risk-policy effect
- broker-facing: `false`

Historical result:

| Metric | Value |
|---|---:|
| Starting equity | `$25,000.00` |
| Ending equity | `$1,502,532.13` |
| Total return | `5910.1285%` |
| Max drawdown | `-68.8694%` |
| Matched trades | `2,192` |
| Active-day coverage | `99.6016%` |
| First `$300,000` target hit | `2026-02-13` |
| 1-year bootstrap target-hit probability | `80.18%` |

Source symbol weights after removing the cap:

| Symbol | Source Weight |
|---|---:|
| AMD | `0.350000` |
| AMZN | `0.350000` |
| IWM | `0.999999` |
| MSFT | `0.349999` |
| QQQ | `0.350000` |
| SPY | `0.350001` |
| TSLA | `0.350000` |
| Total | `3.099999` |

## Interpretation

The no-cap projection is **not** a deployable expectation for a single `$25,000` paper account because the merged source capital plans allocate roughly `310%` of account capital before runtime gates. It is best treated as an upper-bound research stress view that shows what the source packets imply without an added concentration cap.

The more operationally honest production alignment is:

1. Do not apply a backtest-only symbol cap when the goal is to mirror paper-runner selection.
2. Do model production risk gates explicitly before treating the growth curve as paper-account realistic.
3. Keep the paper runtime controls unchanged unless a separate governed paper-launch packet approves the change.

## Comparison To Capped Research Sweep

The capped sensitivity sweep remains useful as a concentration-risk study:

| Max Symbol Weight | Ending Equity | Total Return | Max Drawdown | Return / Abs DD |
|---:|---:|---:|---:|---:|
| `0.15` | `$145,119.54` | `480.4782%` | `-21.6560%` | `22.1868` |
| `0.20` | `$145,200.79` | `480.8031%` | `-21.9025%` | `21.9520` |
| `0.25` | `$144,462.65` | `477.8506%` | `-22.9055%` | `20.8618` |
| no projection cap | `$1,502,532.13` | `5910.1285%` | `-68.8694%` | `85.8175` |

The no-cap return is much higher because capital-plan exposure is much higher. The drawdown is also materially worse.

## Next Engineering Step

Patch the growth-projection tooling to add an explicit production-risk simulation mode instead of relying on symbol caps as a proxy. That mode should enforce the current runtime contract:

- total open-risk fraction
- per-symbol open-risk fraction
- bucket risk caps
- max open positions
- max positions per symbol
- entry clustering gates
- broker buying-power and equity guards

Until that exists, use:

- no projection cap for source-packet lineage review
- capped sweep for concentration-risk sensitivity
- runtime paper logs for broker-audited evidence


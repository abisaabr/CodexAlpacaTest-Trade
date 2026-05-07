# Greek Strategy Backtest Lane - 2026-05-07

Updated: 2026-05-06 22:20 ET / 2026-05-07T02:20Z

## Status

Greek-aware historical replay is now supported as a research-only lane.

- Active wave id: `qqq_spy_iwm_greek_research_20260507T0215Z`
- Active GCS root: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_research_20260507T0215Z/`
- Invalidated stale wave: `qqq_spy_iwm_greek_research_20260507T0145Z`
- Symbols: QQQ, SPY, IWM
- Builder: `scripts/build_greek_strategy_research_inputs.py`
- Launcher: `scripts/launch_gcp_greek_strategy_shards.ps1`
- Backtester selector: `entry_delta_target_research_only`
- Active first tranche launched: QQQ/SPY/IWM c001-028 with suffix `20260507g2`

The first `20260507g1` workers under `qqq_spy_iwm_greek_research_20260507T0145Z` were stopped and must not be used for promotion review because they were launched before the Greek selector patch was present in the worker source archive. Aggregate only the active `T0215Z` wave unless a later handoff supersedes it.

This lane is not broker-facing and does not change live manifests or risk policy.

## Data Semantics

Alpaca option snapshot/chain APIs expose latest option Greeks and implied volatility for live/snapshot use. Historical option bars are OHLCV-style; the backtest lane therefore computes historical entry Greeks from:

- historical stock entry price,
- selected option contract strike/expiry/type,
- historical option entry bar mark,
- Black-Scholes implied volatility inversion,
- Black-Scholes delta/gamma/theta/vega.

The live paper runner already computes Greeks for runtime option selection and portfolio delta/vega caps. This patch aligns the historical backtester with that runtime behavior by adding an entry-time delta-target selector.

## Strategy Families In Test

The Greek input builder creates 168 QQQ/SPY/IWM variants:

- Bull: delta-target long calls and debit call verticals.
- Bear: delta-target long puts and debit put verticals.
- Choppy: theta/vega-aware iron butterfly and iron condor templates.

Directional variants test target deltas 0.35, 0.50, and 0.65 with bounded min/max absolute-delta filters. Choppy variants carry theta/vega intent in the parameter metadata but still require native multi-leg runtime support before broker-facing activation.

## Commands

Prepare-only smoke:

```powershell
$wave = "qqq_spy_iwm_greek_research_20260507T0215Z"
powershell -ExecutionPolicy Bypass -File .\scripts\launch_gcp_greek_strategy_shards.ps1 `
  -WaveId $wave `
  -Symbols QQQ,SPY,IWM `
  -CandidateCountPerWorker 28 `
  -MaxLaunchesPerSymbol 1 `
  -InstanceSuffix 20260507g2 `
  -PrepareOnly
```

First tranche launch:

```powershell
$wave = "qqq_spy_iwm_greek_research_20260507T0215Z"
powershell -ExecutionPolicy Bypass -File .\scripts\launch_gcp_greek_strategy_shards.ps1 `
  -WaveId $wave `
  -Symbols QQQ,SPY,IWM `
  -CandidateCountPerWorker 28 `
  -MaxLaunchesPerSymbol 1 `
  -InstanceSuffix 20260507g2
```

Continue second tranche after capacity frees:

```powershell
$wave = "qqq_spy_iwm_greek_research_20260507T0215Z"
powershell -ExecutionPolicy Bypass -File .\scripts\launch_gcp_greek_strategy_shards.ps1 `
  -WaveId $wave `
  -Symbols QQQ,SPY,IWM `
  -StartCandidateIndex 29 `
  -CandidateCountPerWorker 28 `
  -MaxLaunchesPerSymbol 1 `
  -InstanceSuffix 20260507g3
```

## Aggregation

After workers complete:

```powershell
$wave = "qqq_spy_iwm_greek_research_20260507T0215Z"
$gcs = "gs://codexalpaca-control-us/research_results/$wave"
$local = "reports/gcp_research/$wave/gcs_worker_pull_final/workers"
$out = "reports/gcp_research/$wave/aggregate_final"

gcloud storage cp --recursive "$gcs/workers/" $local --project codexalpaca

python scripts\build_research_portfolio_report.py `
  --replay-root $local `
  --output-dir $out `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --max-positions 12 `
  --max-strategies-per-symbol 2 `
  --max-symbol-weight 0.20 `
  --initial-cash 25000 `
  --candidate-identity-mode variant_profile `
  --required-regimes bull,bear,choppy

python scripts\build_research_promotion_review_packet.py `
  --portfolio-report-json "$out/research_portfolio_report.json" `
  --output-dir "$out/promotion_packet"

gcloud storage cp --recursive $out "$gcs/aggregate_final/" --project codexalpaca
```

## Promotion Rule

No Greek strategy may be added to the PAPER runner from this wave unless:

- the generated promotion-review packet marks it `eligible_for_promotion_review`,
- `fill_coverage >= 0.90`,
- option trade count and OOS/test PnL gates clear,
- severe loser-cluster and portfolio-context gates clear,
- runtime manifest generation preserves the strategy semantics,
- multi-leg candidates remain held until native paper-runtime support is available.

Promotion means governed-validation review only. It is not live activation.

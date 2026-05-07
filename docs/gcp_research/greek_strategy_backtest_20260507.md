# Greek Strategy Backtest Lane - 2026-05-07

Updated: 2026-05-06 22:55 ET / 2026-05-07T02:55Z

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
- Active second tranche launched: QQQ/SPY/IWM c029-056 with suffix `20260507g3`

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

Expanded builder status: after the first compact wave, `scripts/build_greek_strategy_research_inputs.py` was expanded to generate 2,304 QQQ/SPY/IWM variants. The expanded grid adds target deltas 0.25 and 0.80, same-day DTE, credit call/put verticals, broken-wing call/put butterflies, premium-defense spreads, and wider neutral theta structures. Launch expanded waves under a new wave id so they do not mix with the compact `T0215Z` artifacts.

## Runtime-Parity Contract

`scripts/run_option_aware_research_backtest.py` now accepts `--runtime-parity-mode paper_snapshot_greeks`. This mode makes the historical replay contract explicit for realtime/PAPER compatibility:

- entry option bars must be at or after the stock signal;
- exit option bars must be at or after the stock exit signal;
- prior/as-of entry staleness is disabled with `max_entry_staleness_minutes=0`;
- source stock trades are filtered to option RTH same-day windows;
- nearest-contract requests are promoted to the entry-time delta-target selector.

`scripts/launch_gcp_greek_strategy_shards.ps1` passes this mode for new Greek shard launches, and worker status JSON plus candidate summaries record `runtime_parity_mode`. Already-running workers launched before this source commit still used equivalent explicit metadata for entry/exit lookup, but future waves should prefer the named mode so promotion packets prove the replay was paper-parity rather than research-diagnostic.

## First Expanded-Tranche Status

Expanded wave `qqq_spy_iwm_greek_expanded_20260507T0315Z` has first-tranche packets for QQQ, SPY, and IWM under:

- `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/workers/qqq_greek_c001_064/`
- `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/workers/spy_greek_c001_064/`
- `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/workers/iwm_greek_c001_064/`

All three first-tranche packets are `research_only_blocked` with zero review candidates. Dominant blockers are `fill_coverage_below_0.90`, `option_trades_below_20`, `min_net_pnl_not_positive`, and `test_net_pnl_not_above_0`. These results should not be added to the PAPER runner.

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

Daily paper postmortem after the May 7 session:

```powershell
python scripts\build_multi_ticker_paper_postmortem.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml `
  --trade-date 2026-05-07
```

The paper runner also writes cumulative strategy scoreboards during normal session finalization:

- `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260507\strategy_daily_performance_ledger.csv`
- `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260507\strategy_cumulative_performance.csv`

Order journals and trade reconciliation events include strategy name, source strategy id, candidate variant id, phase, request, response, and terminal order status for every PAPER buy/sell attempt. The standalone postmortem builder can be rerun from session artifacts if the runner exits before normal finalization.

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

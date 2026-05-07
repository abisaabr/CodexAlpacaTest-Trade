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

Second-tranche packets `c065-128` are also present for QQQ, SPY, and IWM. All three are `research_only_blocked` with zero review candidates and the same dominant blocker stack: fill coverage, trade count, full-period PnL, and test PnL. These results should not be added to the PAPER runner.

Third-tranche workers `c129-192` were launched after commit `5a24bc0` so their worker status and candidate summaries should record `runtime_parity_mode=paper_snapshot_greeks` directly. Use these workers as the first explicit paper-runtime-parity Greek tranche for downstream diagnostics.

Partial aggregate `aggregate_partial_20260506T224725` covers expanded workers through `c129-192`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260506T224725/aggregate_partial_20260506T224725/`
- Candidate profiles aggregated: 1,728
- Portfolio-gate eligible profiles: 6
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 2
- New lead cluster: IWM bull only, specifically `iwm__bull__call__bull_put_credit_spread` and `iwm__bull__call__single_leg_repair`
- Do not add these to PAPER yet because the generated packet is not `eligible_for_promotion_review`; it is blocked until regime context is complete or a separate governed packet explicitly scopes an IWM bull-only validation review.

Fourth-tranche workers `c193-256` were launched after this aggregate with source commit `f8a9c20`.

Fourth-tranche `c193-256` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`. Per-worker packets remain `research_only_blocked_regime_incomplete`, but the tranche materially increased the number of bull leads:

- QQQ `c193-256`: 4 review candidates inside a blocked/regime-incomplete packet.
- SPY `c193-256`: 2 review candidates inside a blocked/regime-incomplete packet.
- IWM `c193-256`: 13 review candidates inside a blocked/regime-incomplete packet.

Partial aggregate `aggregate_partial_20260506T232205` covers expanded workers through `c193-256`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260506T232205/`
- Candidate profiles aggregated: 2,304
- Portfolio-gate eligible profiles: 54
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 19
- Lead cluster: bull-only QQQ/IWM single-leg repair plus IWM bull-put credit-spread variants.

The generated packet still does not authorize PAPER activation. A no-required-regime aggregate attempt at `aggregate_bull_only_review_20260506T232349` also remained `research_only_blocked_regime_incomplete` because the packet builder's current governed completeness rule still requires bull, bear, and choppy eligible regimes. Do not bypass this by manually editing manifests.

Fifth-tranche workers `c257-320` are now running under suffix `20260507gx5`.

Fifth-tranche `c257-320` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`:

- QQQ `c257-320`: `research_only_blocked`, 0 review candidates.
- SPY `c257-320`: `research_only_blocked_regime_incomplete`, 1 review candidate.
- IWM `c257-320`: `research_only_blocked_regime_incomplete`, 6 review candidates.

Partial aggregate `aggregate_partial_20260506T235727` covers completed expanded workers through `c257-320`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260506T235727/`
- Candidate profiles aggregated: 2,816
- Portfolio-gate eligible profiles: 69
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 20
- Lead cluster remains bull-only.
- New notable lead: IWM `broken_wing_call_butterfly` with `min_net_pnl=26370.25`, `min_test_net_pnl=362.49`, and fill gate clear inside a blocked/regime-incomplete packet.

The generated packet still does not authorize PAPER activation. The current blocker is not lack of bull leads; it is missing eligible bear/choppy regime coverage under the governed packet rule.

Sixth-tranche workers `c321-384` are now running under suffix `20260507gx6`.

Sixth-tranche `c321-384` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`:

- QQQ `c321-384`: `research_only_blocked`, 0 review candidates.
- SPY `c321-384`: `research_only_blocked_regime_incomplete`, 2 review candidates.
- IWM `c321-384`: `research_only_blocked_regime_incomplete`, 1 review candidate.

Partial aggregate `aggregate_partial_20260507T003221` covers completed expanded workers through `c321-384`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260507T003221/`
- Candidate profiles aggregated: 3,456
- Portfolio-gate eligible profiles: 80
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 20
- Lead cluster remains bull-only despite added SPY/IWM candidates.

No new PAPER activation is authorized. The remaining gap is still eligible bear/choppy regime coverage, not bull-candidate quality.

Seventh-tranche workers `c385-448` are now running under suffix `20260507gx7`.

Seventh-tranche `c385-448` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`. All three per-worker packets were `research_only_blocked` with 0 review candidates and the full blocker stack across fill coverage, trade count, full-period PnL, and test PnL.

Partial aggregate `aggregate_partial_20260507T010720` covers completed expanded workers through `c385-448`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260507T010720/`
- Candidate profiles aggregated: 3,968
- Portfolio-gate eligible profiles: 80
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 20
- Lead cluster remains bull-only; `c385-448` added no new review candidates.

No new PAPER activation is authorized. Continue scanning remaining expanded tranches for eligible bear/choppy coverage.

Eighth-tranche workers `c449-512` are now running under suffix `20260507gx8`.

Eighth-tranche `c449-512` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`. All three per-worker packets were `research_only_blocked` with 0 review candidates and the full blocker stack across fill coverage, trade count, full-period PnL, and test PnL.

Partial aggregate `aggregate_partial_20260507T014241` covers completed expanded workers through `c449-512`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260507T014241/`
- Candidate profiles aggregated: 4,608
- Portfolio-gate eligible profiles: 80
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 20
- Lead cluster remains bull-only; `c449-512` added no new review candidates.

No new PAPER activation is authorized.

Ninth-tranche workers `c513-576` are now running under suffix `20260507gx9`.

Ninth-tranche `c513-576` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`:

- QQQ `c513-576`: `research_only_blocked_regime_incomplete`, 2 review candidates.
- SPY `c513-576`: `research_only_blocked`, 0 review candidates.
- IWM `c513-576`: `research_only_blocked_regime_incomplete`, 1 review candidate.

Partial aggregate `aggregate_partial_20260507T021819` covers completed expanded workers through `c513-576`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260507T021819/`
- Candidate profiles aggregated: 5,184
- Portfolio-gate eligible profiles: 87
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 20
- Eligible regimes now include bull and bear, but choppy remains missing.
- Notable bear leads: IWM `debit_put_vertical` with `min_net_pnl=26855.24`, `min_test_net_pnl=6699.60`; QQQ `broken_wing_put_butterfly` with positive full/test PnL and fill gate clear.

This is material progress, but no new PAPER activation is authorized until the generated packet has eligible choppy coverage or another governed review packet explicitly narrows scope.

Tenth-tranche workers `c577-640` are now running under suffix `20260507gx10`.

Tenth-tranche `c577-640` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`:

- QQQ `c577-640`: `research_only_blocked_regime_incomplete`, 1 review candidate.
- SPY `c577-640`: `research_only_blocked_regime_incomplete`, 2 review candidates.
- IWM `c577-640`: `research_only_blocked_regime_incomplete`, 5 review candidates.

Partial aggregate `aggregate_partial_20260507T025506` covers completed expanded workers through `c577-640`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260507T025506/`
- Candidate profiles aggregated: 5,760
- Portfolio-gate eligible profiles: 101
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 20
- Eligible regimes remain bull and bear; choppy remains missing.
- Bear stack improved: IWM `debit_put_vertical` has two strong profiles, and SPY `broken_wing_put_butterfly` joined the bear lead set.

No new PAPER activation is authorized until eligible choppy coverage is found or a separate governed packet explicitly narrows scope.

Eleventh-tranche workers `c641-704` are now running under suffix `20260507gx11`.

Eleventh-tranche `c641-704` completed for QQQ, SPY, and IWM with `runtime_parity_mode=paper_snapshot_greeks`:

- QQQ `c641-704`: `research_only_blocked`, 0 review candidates.
- SPY `c641-704`: `research_only_blocked_regime_incomplete`, 2 review candidates.
- IWM `c641-704`: `research_only_blocked_regime_incomplete`, 5 review candidates.

Partial aggregate `aggregate_partial_20260507T033134` covers completed expanded workers through `c641-704`:

- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_partial_20260507T033134/`
- Candidate profiles aggregated: 6,336
- Portfolio-gate eligible profiles: 115
- Promotion packet decision: `research_only_blocked_regime_incomplete`
- Review candidates in packet: 20
- Eligible regimes remain bull and bear; choppy remains missing.
- Bear stack deepened further: IWM `debit_put_vertical`, SPY `broken_wing_put_butterfly`, SPY `single_leg_repair`, and QQQ `broken_wing_put_butterfly` all appear in the review candidate set.

No new PAPER activation is authorized until eligible choppy coverage is found or a separate governed packet explicitly narrows scope.

Final expanded-tranche workers `c705-768` are now running under suffix `20260507gx12`.

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

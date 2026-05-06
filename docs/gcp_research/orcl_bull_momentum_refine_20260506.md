# ORCL Bull Momentum Refine Research Wave

Date context: 2026-05-06 RTH, while the local multi-symbol PAPER trader is running.

## Purpose

ORCL previously produced strict eligible bear and limited choppy candidates, but
the generated promotion packet remained `research_only_blocked_regime_incomplete`
because no bull candidate cleared the governed review gates. This wave adds and
runs a research-only bull momentum refinement grid so ORCL can be tested for a
complete bull/bear/choppy governed-validation sleeve without changing live
manifests, risk policy, or the active paper process.

## Safety Posture

- Broker-facing effect: `none`.
- Live manifest effect: `none`.
- Risk policy effect: `none`.
- Promotion effect: governed-validation review only if a generated packet clears
  the existing gates.
- Active local paper trader was not stopped, restarted, or duplicated.
- The `fill_coverage >= 0.90` promotion gate was not lowered.

## Code Changes

- Source commit: `24e8902436af75d162c8a2b7aa574af95eba02df`.
- Added `--bull-profile-set momentum_refine` to
  `scripts/build_regime_rescue_research_inputs.py`.
- Added launcher support for `-BullProfileSet`.
- Hardened launcher optional argument handling so empty `-ChoppyFamilies` does
  not break bull-only runs.

## Validation

- Builder smoke command succeeded and produced `162` ORCL bull candidates:

```powershell
python scripts\build_regime_rescue_research_inputs.py --symbol ORCL --wave-id smoke_orcl_bull_momentum_refine --output-dir reports\gcp_research\smoke_orcl_bull_momentum_refine\inputs --target-regimes bull --bull-profile-set momentum_refine
```

- Launcher `-PrepareOnly` smoke succeeded and uploaded a dry-run input packet to
  `gs://codexalpaca-control-us/research_results/smoke_orcl_launcher_bull_momentum_prepare`.
- `python -m compileall scripts\build_regime_rescue_research_inputs.py` succeeded.
- `python -m pytest tests\test_build_regime_rescue_research_inputs.py` could not
  run locally because the active Python environment does not have `pytest`
  installed.

## GCS Inputs

- Stock bars:
  `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/ORCL/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts:
  `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/ORCL/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars:
  `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/ORCL/365d_5x5/option_bars_silver/option_bars`

All three prefixes were checked with `gsutil ls` before launch.

## Active Wave

- Wave ID: `orcl_bull_momentum_refine_20260506T1535Z`
- GCS root:
  `gs://codexalpaca-control-us/research_results/orcl_bull_momentum_refine_20260506T1535Z`
- Input variants:
  `gs://codexalpaca-control-us/research_results/orcl_bull_momentum_refine_20260506T1535Z/inputs/orcl_regime_rescue_variants.jsonl`
- Input queue:
  `gs://codexalpaca-control-us/research_results/orcl_bull_momentum_refine_20260506T1535Z/inputs/orcl_regime_rescue_option_queue.json`
- Launch rows:
  `gs://codexalpaca-control-us/research_results/orcl_bull_momentum_refine_20260506T1535Z/ops/orcl_regime_rescue_launch_rows.json`
- Worker root:
  `gs://codexalpaca-control-us/research_results/orcl_bull_momentum_refine_20260506T1535Z/workers/`

Launch command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol ORCL `
  -WaveId orcl_bull_momentum_refine_20260506T1535Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/ORCL/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/ORCL/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/ORCL/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506c `
  -CandidateCountPerWorker 21 `
  -MaxLaunches 8 `
  -TargetRegimes bull `
  -BullProfileSet momentum_refine `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,bear,choppy,unclassified `
  -LagProfiles "0:60,10:60,30:120"
```

## Running Workers

At the launch check, all `8` worker VMs were running:

- `orcl-rescue-c001-021-20260506c`
- `orcl-rescue-c022-042-20260506c`
- `orcl-rescue-c043-063-20260506c`
- `orcl-rescue-c064-084-20260506c`
- `orcl-rescue-c085-105-20260506c`
- `orcl-rescue-c106-126-20260506c`
- `orcl-rescue-c127-147-20260506c`
- `orcl-rescue-c148-162-20260506c`

Some requested zones were temporarily capacity-limited for `e2-standard-2`, but
the launcher retried and placed all eight workers in available zones.

## Local Paper Trader State At Launch

- One local order-submitting paper process remained active.
- Session state at approximately `2026-05-06T11:31:32-04:00`:
  - `blocked_new_entries`: `false`
  - signals fired: `6`
  - open trades: `4`
  - completed trades: `2`
- Known runtime issues are documented in
  `docs/gcp_research/paper_runtime_issue_log_20260506.md`, including repeated
  non-filled QQQ and GOOGL sell-to-close limit orders and strategy-level versus
  broker-level open-position accounting differences.

## Next Actions

1. Monitor the worker VMs and GCS worker artifacts until all workers terminate or
   report completion.
2. Pull worker reports and aggregate into a strict portfolio report.
3. Build a promotion-review packet without manually overriding decision fields.
4. If ORCL gains at least one eligible bull candidate, combine it with prior ORCL
   eligible bear/choppy evidence only through a generated governed-review packet.
5. Do not add ORCL to paper execution unless a generated packet clears the
   governed-validation path and a separate paper-runner activation step is taken.

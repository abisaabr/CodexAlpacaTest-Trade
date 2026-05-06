# INTC/META Parallel Refinement Wave - 2026-05-06

Generated: 2026-05-06 during the active local PAPER trader session.

## Safety Status

- Broker-facing effect: none.
- Live manifest effect: none.
- Risk policy effect: none.
- Promotion meaning: governed-validation review only.
- Fill gate remains `fill_coverage >= 0.90`.
- Active local paper trader was checked before launch; only the local PAPER process using `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml` was observed.

## Prior Wave Closure

- `nflx_bear_choppy_refine_20260506T1750Z`: complete and `research_only_blocked`.
- NFLX eligible candidates: `0`.
- Dominant NFLX blockers: `fill_coverage_below_0.90`, `min_net_pnl_not_positive`, `test_net_pnl_not_above_0.01`.
- `mu_bull_bear_momentum_refine_20260506T1750Z`: complete and `research_only_blocked`.
- MU eligible candidates: `0`.
- Dominant MU blockers: `fill_coverage_below_0.90`, `min_net_pnl_not_positive`, `test_net_pnl_not_above_0.01`.
- Best MU lead remains research-only: strong PnL but `min_fill_coverage=0.8571`, `min_data_foundation_coverage=0.875`, dominant reason `selected_contract_universe_gap`.

## New Parallel Waves

### INTC Bull/Choppy Refinement

- Wave ID: `intc_bull_choppy_refine_20260506T1840Z`
- GCS root: `gs://codexalpaca-control-us/research_results/intc_bull_choppy_refine_20260506T1840Z/`
- Target regimes: `bull,choppy`
- Profile sets: `bull=momentum_refine`, `choppy=timewindow_quality_filter`, `bear=rescue`
- Template count: `258`
- Initial launched candidates: `1-168`
- Tail remaining after capacity frees: `169-258`
- Running instances:
  - `intc-rescue-c001-042-20260506h`
  - `intc-rescue-c043-084-20260506h`
  - `intc-rescue-c085-126-20260506h`
  - `intc-rescue-c127-168-20260506h`

Command used:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol INTC `
  -WaveId intc_bull_choppy_refine_20260506T1840Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/INTC/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/INTC/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/INTC/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506h `
  -TargetRegimes bull,choppy `
  -BullProfileSet momentum_refine `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,choppy,bear,unclassified `
  -CandidateCountPerWorker 42 `
  -MaxLaunches 4
```

Tail command, after current workers terminate:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol INTC `
  -WaveId intc_bull_choppy_refine_20260506T1840Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/INTC/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/INTC/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/INTC/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506i `
  -StartCandidateIndex 169 `
  -TargetRegimes bull,choppy `
  -BullProfileSet momentum_refine `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,choppy,bear,unclassified `
  -CandidateCountPerWorker 45 `
  -MaxLaunches 2
```

### META Full Momentum/Quality Refinement

- Wave ID: `meta_full_momentum_quality_refine_20260506T1840Z`
- GCS root: `gs://codexalpaca-control-us/research_results/meta_full_momentum_quality_refine_20260506T1840Z/`
- Target regimes: `bull,bear,choppy`
- Profile sets: `bull=momentum_refine`, `bear=signal_window_refine`, `choppy=timewindow_quality_filter`
- Template count: `312`
- Initial launched candidates: `1-260`
- Tail remaining after capacity frees: `261-312`
- Running instances:
  - `meta-rescue-c001-065-20260506h`
  - `meta-rescue-c066-130-20260506h`
  - `meta-rescue-c131-195-20260506h`
  - `meta-rescue-c196-260-20260506h`

Command used:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol META `
  -WaveId meta_full_momentum_quality_refine_20260506T1840Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/META/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/META/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/META/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506h `
  -TargetRegimes bull,bear,choppy `
  -BullProfileSet momentum_refine `
  -BearProfileSet signal_window_refine `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,bear,choppy,unclassified `
  -CandidateCountPerWorker 65 `
  -MaxLaunches 4
```

Tail command, after current workers terminate:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol META `
  -WaveId meta_full_momentum_quality_refine_20260506T1840Z `
  -StockUri gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/META/365d_5x5/stock_ref_silver/stock_bars `
  -ContractsUri gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/META/365d_5x5/research_wave/dense_universe/selected_option_contracts `
  -BarsUri gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/META/365d_5x5/option_bars_silver/option_bars `
  -InstanceSuffix 20260506i `
  -StartCandidateIndex 261 `
  -TargetRegimes bull,bear,choppy `
  -BullProfileSet momentum_refine `
  -BearProfileSet signal_window_refine `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bull,bear,choppy,unclassified `
  -CandidateCountPerWorker 52 `
  -MaxLaunches 1
```

## Monitor Procedure

1. Verify no duplicate local paper trader and no live-mode process.
2. Check active research instances:

```powershell
gcloud compute instances list --project codexalpaca --filter="name~'intc-rescue|meta-rescue'" --format="table(name,zone,status,labels.symbol,labels.role,labels.wave)"
```

3. Pull completed worker artifacts from each wave.
4. Build strict portfolio reports and promotion-review packets with existing gates.
5. If the initial chunks complete, launch the documented tail commands.
6. Delete only terminated research VMs after artifacts are safely mirrored.
7. Do not add INTC or META to paper unless a generated governed-validation packet later clears the required regime coverage and promotion gates.

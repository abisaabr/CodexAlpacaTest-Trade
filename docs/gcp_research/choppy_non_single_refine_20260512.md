# Choppy Non-Single Refinement Wave - 2026-05-12

## Summary

Wave `choppy_non_single_refine_20260512T1750Z` completed successfully and produced a strict choppy-only governed-validation review packet.

- Source commit: `89520fc`
- Broker-facing effect: none
- Paper-runner state changed: no
- Live manifest effect: none
- Risk policy effect: none
- Fill gate: `fill_coverage >= 0.90` preserved
- Final decision: `ready_for_governed_validation_review`
- Governance review scope: choppy-only non-single structures

This does not authorize live trading or automatic paper activation. The leading candidates are multi-leg structures and need runtime order/exit compatibility review before being added to any order-submitting paper manifest.

## GCS Artifacts

- Source archive: `gs://codexalpaca-control-us/research_results/choppy_non_single_refine_20260512T1750Z/inputs/source/codexalpaca_repo_source.tar.gz`
- Worker artifacts: `gs://codexalpaca-control-us/research_results/choppy_non_single_refine_20260512T1750Z/workers/`
- Interim aggregate: `gs://codexalpaca-control-us/research_results/choppy_non_single_refine_20260512T1750Z/aggregate_c001_096/`
- Final aggregate: `gs://codexalpaca-control-us/research_results/choppy_non_single_refine_20260512T1750Z/aggregate_full/`
- Durable handoff mirror: `gs://codexalpaca-control-us/gcp_research/choppy_non_single_refine_20260512.md`

## Run Configuration

- Symbols: `QQQ`, `SPY`, `IWM`, `AVGO`, `GOOGL`, `MSFT`, `AMZN`, `TSM`
- Target regime: `choppy`
- Families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60`, `10:60`, `30:120`
- Choppy profile set: `timewindow_quality_filter`
- Candidate selection mode: `regime_balanced`
- Candidate range: `c001-c192` per symbol
- Shards: 64 completed, synced, and deleted

Data roots:

- `QQQ`, `AVGO`, `GOOGL`, `TSM`: `option_fill_ladder_next10_20260429`
- `SPY`, `IWM`, `MSFT`, `AMZN`: `option_fill_ladder_20260429`

## Commands

Source archive:

```powershell
$wave = 'choppy_non_single_refine_20260512T1750Z'
$archive = Join-Path $env:TEMP "$wave-codexalpaca_repo_source.tar.gz"
git archive --format=tar.gz --output $archive HEAD
gcloud storage cp $archive "gs://codexalpaca-control-us/research_results/$wave/inputs/source/codexalpaca_repo_source.tar.gz" --project codexalpaca
```

Worker launch pattern:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol $sym `
  -WaveId choppy_non_single_refine_20260512T1750Z `
  -StockUri $stock `
  -ContractsUri $contracts `
  -BarsUri $bars `
  -InstanceSuffix $suffix `
  -StartCandidateIndex $start `
  -CandidateCountPerWorker 24 `
  -MaxLaunches 1 `
  -MachineType e2-standard-2 `
  -Selectors entry_liquidity_first_research_only `
  -LagProfiles '0:60,10:60,30:120' `
  -TargetRegimes choppy `
  -ChoppyFamilies 'debit_call_vertical,debit_put_vertical,bull_put_credit_spread,bear_call_credit_spread,broken_wing_call_butterfly,broken_wing_put_butterfly' `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder 'choppy,bear,bull,unclassified' `
  -Zones $zones
```

Shard suffixes:

- `20260512cp1`: `c001-c024`
- `20260512cp2`: `c025-c048`
- `20260512cp3`: `c049-c072`
- `20260512cp4`: `c073-c096`
- `20260512cp5`: `c097-c120`
- `20260512cp6`: `c121-c144`
- `20260512cp7`: `c145-c168`
- `20260512cp8`: `c169-c192`

Final aggregation:

```powershell
$wave = 'choppy_non_single_refine_20260512T1750Z'
$root = "reports/gcp_research/$wave/workers"
$out = "reports/gcp_research/$wave/aggregate_full"
python scripts/build_research_portfolio_report.py --replay-root $root --output-dir "$out/portfolio_report" --fill-coverage-gate 0.90 --min-option-trades 20 --min-test-net-pnl 0 --required-regimes choppy --candidate-identity-mode variant_profile
python scripts/build_research_promotion_review_packet.py --portfolio-report-json "$out/portfolio_report/research_portfolio_report.json" --output-dir "$out/promotion_packet" --max-review-candidates 20
gsutil -m rsync -r reports\gcp_research\choppy_non_single_refine_20260512T1750Z\aggregate_full gs://codexalpaca-control-us/research_results/choppy_non_single_refine_20260512T1750Z/aggregate_full/
```

## Final Packet

- Portfolio profiles evaluated: 4,608
- Eligible profiles: 71
- Promotion packet review candidates: 20
- Packet decision: `ready_for_governed_validation_review`
- Required regime used for this packet: `choppy`

Capital-plan leaders:

| Symbol | Strategy | Family | Full PnL | Test PnL | Fill | Trades | Weight |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| AVGO | `avgo__choppy__call__broken_wing_call_butterfly` | `broken_wing_call_butterfly` | 2,188.258 | 18,200.097 | 0.9902 | 101 | 0.065614 |
| AVGO | `avgo__choppy__call__debit_call_vertical` | `debit_call_vertical` | 6,520.921 | 4,004.905 | 0.9902 | 101 | 0.323502 |
| GOOGL | `googl__choppy__call__debit_call_vertical` | `debit_call_vertical` | 7,991.531 | 731.292 | 1.0000 | 90 | 0.290194 |
| TSM | `tsm__choppy__call__debit_call_vertical` | `debit_call_vertical` | 2,716.720 | 2,888.534 | 0.9747 | 77 | 0.160682 |
| TSM | `tsm__choppy__call__debit_call_vertical` | `debit_call_vertical` | 2,680.034 | 2,888.534 | 0.9747 | 77 | 0.160007 |

## Operational Notes

- All 64 worker directories, portfolio reports, and promotion packets were synced locally.
- All `20260512cp*` GCP research VMs were deleted after artifacts were present.
- A first-half interim packet (`aggregate_c001_096`) also cleared choppy-only review with 11 review candidates, mostly TSM/AVGO debit-call vertical variants.
- The app automation update tool could not update/create a monitor because this thread already had an active hidden heartbeat automation ID. No workaround cron was created.
- Existing unrelated dirty files remained untouched: `scripts/hourly_gcp_research_sweep.py` and `tests/test_hourly_gcp_research_sweep.py`.

## Next Actions

1. Review native multi-leg runtime compatibility for debit verticals and broken-wing butterflies before paper activation.
2. Feed the eligible choppy candidates into the constrained optimizer as a choppy add-on to the existing `tt_top2_bull_choppy_up` benchmark.
3. If optimizer results improve drawdown-adjusted return, create a separate governed-validation manifest update for review; do not edit live manifests from this packet alone.

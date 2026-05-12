# Overnight $200/Day Hardening Launch - 2026-05-12

- Source commit: `817f7ef36c475f07abf8f60a7126e03e6b6ec054`
- Broker mode impact: `none`
- Paper-runner state changed: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Promotion decision: `no new strategy is eligible for paper-runner addition from this launch`

## Automations

Updated existing thread heartbeat:

- Automation ID: `may13-paper-readiness-quote-capture-monitor`
- New name: `overnight-200day-optimizer-hardening-loop`
- Schedule: every `30` minutes until `2026-05-13T10:00:00Z` (`6:00 AM America/New_York`)
- Scope: monitor PAPER-only safety, active GCP wave, quote-realism hardening, optimizer tiers, and May 13 readiness

Created May 13 readiness cron:

- Automation ID: `may13-9am-paper-trader-readiness`
- Schedule: `9:00 AM America/New_York`, May 13, 2026
- Scope: run PAPER preflight and start PAPER order submission only if all safety checks pass

## Baseline $200/Day Evidence

Current hardened projections do not support a credible `$200/day` claim yet.

Quote-cost/fill-haircut projection status:

- Current May 8 paper book: ending equity `$3,714.60`, average daily PnL `-$84.80`, train/test eligible candidates `0`
- `tt_top2_bull_choppy_up` quote-cost benchmark: ending equity `$9,111.91`, average daily PnL `-$63.30`, train/test eligible candidates `0`
- All-local candidate scan with quote costs: ending equity `$0.00`, average daily PnL `-$99.60`, train/test eligible candidates `0`
- Latest `bc5` choppy/bear non-single pool with quote costs: ending equity `$0.00`, average daily PnL `-$107.76`, train/test eligible candidates `0`

Raw benchmark context:

- `tt_top2_bull_choppy_up` without quote costs: ending equity `$35,949.02`, average daily PnL about `$43.62`, max drawdown `-18.2398%`, train/test eligible candidates `6`

Interpretation: the project needs quote-backed replay evidence and train/test-stable choppy/non-single candidates before sizing toward `$200/day`.

## Active Research Wave

Run ID: `choppy_non_single_train_test_refine_20260512T1925ET`

GCS root:

- `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/`

Local root:

- `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/`

Source archive:

- `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/inputs/source/codexalpaca_repo_source.tar.gz`

Target:

- Choppy-only
- Non-single-leg families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60`, `10:60`, `30:120`
- Candidate range: `c001-c024` out of `192`

Expected active workers:

- `qqq-rescue-c001-024-20260512bc6a`
- `spy-rescue-c001-024-20260512bc6a`
- `iwm-rescue-c001-024-20260512bc6a`
- `avgo-rescue-c001-024-20260512bc6a`
- `googl-rescue-c001-024-20260512bc6a`
- `msft-rescue-c001-024-20260512bc6a`
- `amzn-rescue-c001-024-20260512bc6a`
- `tsm-rescue-c001-024-20260512bc6a`

Status at launch verification:

- All `8` workers were `RUNNING`
- Zone: `us-central1-a`
- Machine type: `e2-standard-2`
- Broker-facing process: none detected
- Live-mode markers in May 13 config/runtime path: none detected

## Commands Used

Source archive upload:

```powershell
$wave='choppy_non_single_train_test_refine_20260512T1925ET'
$archive=Join-Path $env:TEMP "$wave-codexalpaca_repo_source.tar.gz"
git archive --format=tar.gz --output $archive HEAD
gcloud storage cp $archive "gs://codexalpaca-control-us/research_results/$wave/inputs/source/codexalpaca_repo_source.tar.gz" --project codexalpaca
```

Launch template used for each symbol:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol <SYMBOL> `
  -WaveId choppy_non_single_train_test_refine_20260512T1925ET `
  -StockUri <SYMBOL_STOCK_URI> `
  -ContractsUri <SYMBOL_CONTRACTS_URI> `
  -BarsUri <SYMBOL_BARS_URI> `
  -StartCandidateIndex 1 `
  -CandidateCountPerWorker 24 `
  -MaxLaunches 1 `
  -InstanceSuffix 20260512bc6a `
  -TargetRegimes choppy `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder choppy,bear,bull,unclassified `
  -ChoppyFamilies debit_call_vertical,debit_put_vertical,bull_put_credit_spread,bear_call_credit_spread,broken_wing_call_butterfly,broken_wing_put_butterfly `
  -Selectors entry_liquidity_first_research_only `
  -LagProfiles "0:60,10:60,30:120" `
  -MachineType e2-standard-2
```

Worker status verification:

```powershell
gcloud compute instances list --filter="name~'20260512bc6a'" --format="table(name,zone.basename(),status,machineType.basename(),creationTimestamp)"
```

## Overnight Completion Contract

When workers terminate:

1. Sync artifacts:

```powershell
gsutil -m rsync -r gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/workers reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/workers
```

2. Build strict portfolio report:

```powershell
python scripts/build_research_portfolio_report.py --replay-root reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/workers --output-dir reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/portfolio_report --fill-coverage-gate 0.90 --min-option-trades 20 --min-test-net-pnl 0 --required-regimes choppy --candidate-identity-mode variant_profile
```

3. Build promotion-review packet:

```powershell
python scripts/build_research_promotion_review_packet.py --portfolio-report-json reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/portfolio_report/research_portfolio_report.json --output-dir reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate/promotion_packet --max-review-candidates 20
```

4. Run hardened projection and constrained optimizer against `tt_top2_bull_choppy_up`, keeping quote-cost/fill-haircut constraints enabled.

5. Mirror aggregate outputs:

```powershell
gsutil -m rsync -r reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate
```

6. Delete only synced `TERMINATED` workers.

## May 13 Paper Priority

The paper runner should use:

- `config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml`

Do not add the active choppy wave to PAPER before:

- Fill coverage remains `>= 0.90`
- Train and test PnL are positive
- Quote-cost/fill-haircut projection remains positive
- Constrained optimizer shows value versus `tt_top2_bull_choppy_up`
- Quote-backed evidence is available from RTH OPRA/SIP capture

The May 13 data-quality priority is the no-submit realtime quote shadow capture and sidecar conversion, not adding unverified strategies.

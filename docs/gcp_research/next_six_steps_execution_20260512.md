# Next Six Steps Execution - 2026-05-12

## Summary

Executed the requested next six steps as PAPER-safe research and governed-validation work only.

- Latest source commit: `763f27a`
- Code hardening commit: `763f27a feat: cap single-candidate optimizer concentration`
- Prior hardening commit used by completed projection/runtime review work: `6a617a8`
- Broker-facing state: no local paper trader process was running during this pass.
- Paper-runner state changed: no.
- Live manifest / risk policy changed: no.
- Fill gate: `fill_coverage >= 0.90` preserved.
- Promotion posture: governed-validation review only. No automatic paper activation.

## Step Results

1. Microstructure discovery

- Active discovery-only wave: `microstructure_rare_event_overnight_20260507T2030ET`
- GCS root: `gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/`
- Active workers at last check:
  - `micro-event-c92161-92672-20260512m4`: `275 / 512` grids complete, `review_like_count=0`
  - `micro-event-c92673-93184-20260512m4`: `150 / 512` grids complete, `review_like_count=0`
- Status: still running, discovery-only, not eligible for governed promotion.

2. Governed review evidence

- Completed packet: `bear_choppy_non_single_refine_20260512T1546ET`
- Aggregate GCS: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1546ET/aggregate/`
- Runtime review manifest GCS: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1546ET/runtime_review/bear_choppy_non_single_review_manifest.yaml`
- Packet decision: `ready_for_governed_validation_review`
- Review candidates: `16`
- Regimes represented: `bear`, `choppy`
- Activation status: not activated; review-only.

3. Projection realism and optimizer hardening

- Added optimizer constraints:
  - `--max-candidate-trade-share`
  - `--max-candidate-pnl-share`
- Purpose: prevent a portfolio from passing while one strategy candidate dominates trade count or absolute PnL.
- Projection/optimizer GCS root: `gs://codexalpaca-control-us/gcp_research/next_eval_bear_choppy_c169_192_20260512/`

Key optimizer results:

- Strict `$50/day` with candidate caps: failed.
- Relaxed `$100/day` with candidate caps: failed.
- Diagnostic `$50/day` with looser candidate caps: passed.
- Diagnostic result: `4` candidates, average daily PnL `$87.56`, ending equity `$40,498.11`, max drawdown `-8.0849%`.
- Remaining weakness: PnL still concentrated; SPY choppy candidate contributes `0.65181651` of absolute PnL and single-leg long call contributes `0.82548278` by family.

4. Paper trader hardening evidence

Validated existing hardening paths with tests:

```powershell
python -m pytest tests/test_multi_ticker_portfolio.py tests/test_build_multi_ticker_paper_postmortem.py tests/test_broker_multileg_orders.py tests/test_build_governed_validation_manifest_from_packets.py -q
```

Result: `81 passed in 13.82s`.

Coverage includes:

- Strategy attribution in journals and ledgers.
- Multi-leg exit cleanup fallback.
- Startup unexpected-position cleanup suppression/cleanup behavior.
- Scheduled EOD flatten at `10` and `2` minutes before close.
- Known and unexpected broker-position flatten behavior.
- Cleanup retry behavior after submit failures and canceled attempts.

5. New targeted GCP sweep

- New wave: `bear_choppy_non_single_refine_20260512T1620ET`
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1620ET/`
- Local launch records mirror: `gs://codexalpaca-control-us/gcp_research/bear_choppy_non_single_refine_20260512T1620ET/local_launch_records/`
- Source archive: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1620ET/inputs/source/codexalpaca_repo_source.tar.gz`
- Source commit: `763f27a`
- Candidate range: `c193-c216`
- Symbols: `QQQ`, `SPY`, `IWM`, `AVGO`, `GOOGL`, `MSFT`, `AMZN`, `TSM`
- Regimes: `bear`, `choppy`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Choppy families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`

Worker status at last check:

- Synced and deleted after termination:
  - `qqq-rescue-c193-216-20260512bc5`
  - `spy-rescue-c193-216-20260512bc5`
- Still running:
  - `avgo-rescue-c193-216-20260512bc5`
  - `googl-rescue-c193-216-20260512bc5`
  - `iwm-rescue-c193-216-20260512bc5`
  - `msft-rescue-c193-216-20260512bc5`
  - `amzn-rescue-c193-216-20260512bc5`
  - `tsm-rescue-c193-216-20260512bc5`

Early worker-level packet status:

- QQQ c193-c216: `ready_for_governed_validation_review`, `6` eligible review candidates, `2` unique eligible base candidates, eligible regime `bear`; missing `bull,choppy`.
- SPY c193-c216: `ready_for_governed_validation_review`, `5` eligible review candidates, `2` unique eligible base candidates, eligible regime `bear`; missing `bull,choppy`.

6. Monitoring

- Updated active heartbeat automation: `monitor-microstructure-bc4-20260512`
- New name: `monitor-bear-choppy-bc5-micro-20260512`
- Interval: every `30` minutes
- Scope:
  - monitor active c193-c216 bear/choppy workers
  - monitor microstructure discovery-only workers
  - sync artifacts when terminated
  - aggregate strict reports and promotion packets when complete
  - mirror outputs to GCS
  - delete only synced TERMINATED research VMs
  - do not change paper/live config or lower gates

## Commands Of Record

Optimizer hardening tests:

```powershell
python -m pytest tests/test_optimize_portfolio_projection_candidates.py tests/test_build_portfolio_growth_projection.py tests/test_multi_ticker_portfolio.py::test_execute_attempts_records_submit_failure_without_crashing tests/test_multi_ticker_portfolio.py::test_submit_cleanup_order_retries_after_submit_failure -q
```

Paper hardening tests:

```powershell
python -m pytest tests/test_multi_ticker_portfolio.py tests/test_build_multi_ticker_paper_postmortem.py tests/test_broker_multileg_orders.py tests/test_build_governed_validation_manifest_from_packets.py -q
```

New GCP wave launcher pattern:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch_gcp_regime_rescue_shards.ps1 `
  -Symbol <SYMBOL> `
  -WaveId bear_choppy_non_single_refine_20260512T1620ET `
  -StockUri <dense-stock-uri> `
  -ContractsUri <dense-contracts-uri> `
  -BarsUri <dense-option-bars-uri> `
  -InstanceSuffix 20260512bc5 `
  -StartCandidateIndex 193 `
  -CandidateCountPerWorker 24 `
  -MaxLaunches 1 `
  -MachineType e2-standard-2 `
  -Selectors entry_liquidity_first_research_only `
  -LagProfiles 0:60,10:60,30:120 `
  -MaxEntryStalenessMinutes 0 `
  -TargetRegimes bear,choppy `
  -BearProfileSet signal_window_refine `
  -ChoppyProfileSet timewindow_quality_filter `
  -CandidateSelectionMode regime_balanced `
  -RegimeBalanceOrder bear,choppy,bull,unclassified `
  -ChoppyFamilies debit_call_vertical,debit_put_vertical,bull_put_credit_spread,bear_call_credit_spread,broken_wing_call_butterfly,broken_wing_put_butterfly
```

## Next Required Actions

- When remaining `bc5` workers terminate, sync all artifacts and build an aggregate strict portfolio report and promotion-review packet for the full `c193-c216` wave.
- Compare the full `bc5` aggregate against `tt_top2_bull_choppy_up` and the candidate-capped optimizer tiers.
- If the new wave improves bear/choppy diversity, build a runtime review manifest; keep it governed-validation review only until explicitly activated.
- When microstructure workers finish, aggregate discovery output only. Do not promote microstructure candidates from the one-day websocket stream without separate governed evidence or a clearly labeled operator-approved experiment packet.
- Keep focusing data repair on bid/ask spread and quote-age availability; current realistic projection stress still rejects bar-close/no-bid-ask evidence.

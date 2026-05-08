# Structure-Aware Bear/Choppy Rerun Status - 2026-05-08

## Summary

The structure-aware multi-leg selector rerun completed for the launched AVGO/IWM shards and produced a governed-validation review result. This is research/promotion-review output only. No paper-trader manifest, live manifest, or global risk policy was changed.

## Paper Session Safety

- Paper session checked: May 8, 2026 multi-symbol PAPER trader.
- Broker-facing process state at post-EOD check: no local `run_multi_ticker_portfolio_paper_trader.py` process running.
- Post-EOD interpretation: acceptable because the session summary shows `status=session_complete`.
- Starting equity: `25000.0`.
- Ending equity: `25262.56`.
- Realized reconciled net PnL: `262.56`.
- Completed reconciled trades: `4`.
- EOD reconciliation: `shutdown_reconciled=true`.
- Ending broker position count: `0`.
- Broker order audit: `382` orders, `382` matched, `0` unmatched.
- Broker activity audit: `8` activities, `7` matched, `1` unmatched.
- Runtime issues for postmortem: entry circuit breaker active after 3 consecutive entry failures, `guardrail_needs_manual_review=true`, `guardrail_manual_review_count=3`, and `broker_status_mismatch_count=374`.

## Research Wave

- Wave ID: `bear_choppy_structure_aware_rerun_20260508T1745ET`.
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_structure_aware_rerun_20260508T1745ET/`.
- Source commit used for worker launch: `1261e69`.
- Handoff/source commit before this status doc: `bd6a891`.
- Broker-facing: `false`.
- Paper orders: `false`.
- Live manifest effect: `none`.
- Risk policy effect: `none`.
- Fill gate: `fill_coverage >= 0.90`.
- Required aggregate regimes: `bear,choppy`.

## Workers

- `avgo-rescue-c097-120-20260508sa1`: synced, aggregated, deleted after artifact verification.
- `avgo-rescue-c121-144-20260508sa1`: synced, aggregated, deleted after artifact verification.
- `iwm-rescue-c121-144-20260508sa1`: synced, aggregated, deleted after artifact verification.
- Remaining expected `20260508sa1` research VMs: none after cleanup.

## Aggregate Result

- Local aggregate: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/portfolio_report/`.
- GCS aggregate: `gs://codexalpaca-control-us/research_results/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/portfolio_report/`.
- Rollup JSON: `research_wave_portfolio_rollup.json`.
- Promotion packet JSON: `promotion_review_packet/research_promotion_review_packet.json`.
- Decision: `ready_for_governed_validation_review`.
- Governance review scope: `per_regime_governed_validation_review`.
- Source reports: `3`.
- Candidate count: `150`.
- Eligible variant-profile candidates: `6`.
- Review candidates in generated promotion packet: `5`.
- Capital plan candidates: `3`.
- Capital plan symbols: `AVGO`.
- Capital plan families: `debit_call_vertical`, `debit_put_vertical`.

## Commands

```powershell
python scripts\build_research_wave_portfolio_rollup.py `
  --report-root reports\gcp_research\bear_choppy_structure_aware_rerun_20260508T1745ET\workers `
  --output-dir reports\gcp_research\bear_choppy_structure_aware_rerun_20260508T1745ET\aggregate\portfolio_report `
  --pattern research_portfolio_report.json `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --max-positions 12 `
  --max-strategies-per-symbol 3 `
  --max-symbol-weight 0.35 `
  --initial-cash 25000 `
  --max-review-candidates 50 `
  --required-regimes bear,choppy
```

```powershell
gcloud storage cp --recursive `
  reports\gcp_research\bear_choppy_structure_aware_rerun_20260508T1745ET\aggregate `
  gs://codexalpaca-control-us/research_results/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate
```

## Promotion And Paper-Runner State

- Eligible for governed validation review: yes, at the research aggregate level.
- Added to paper runner: no.
- Paper-runner state changed: no.
- Live trading: not started.
- Live manifest changed: no.
- Risk policy changed: no.

## Next Steps

- Review the AVGO choppy vertical candidates against runtime multi-leg compatibility and current paper-trader risk gates before any manifest addition.
- Include the May 8 paper postmortem guardrail/manual-review items before the next paper launch.
- Continue targeted selector repair for IWM, because the IWM c121-144 shard remained blocked despite the structure-aware selector patch.

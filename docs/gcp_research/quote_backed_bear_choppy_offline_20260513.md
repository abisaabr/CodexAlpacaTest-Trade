# Quote-Backed Bear/Choppy Offline Sweep - 2026-05-13

## Scope

This is a research-only GCP sweep for tomorrow's optimizer candidate pool. It does not authorize paper-runner additions by itself.

- Wave ID: `bear_choppy_quote_backed_offline_20260513T1255ET`
- Source commit: `af5aa82`
- Broker-facing: `false`
- Paper runner changed: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Candidate range: `c217-c240`
- Symbols: `QQQ`, `SPY`, `IWM`, `AVGO`, `GOOGL`, `MSFT`, `AMZN`, `TSM`
- Target regimes: `bear,choppy`
- Families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60`, `10:60`, `30:120`
- Evidence gate: optimizer input is allowed only for candidates with strict train/test-positive backtest results plus quote-backed replay lineage.

## Active Workers

Complete, synced, aggregated, mirrored locally, and cleaned from GCP.

## Strict Aggregate

- Candidate profiles: `576`
- Eligible for governed-validation review before quote evidence: `38`
- Unique eligible base candidates: `22`
- Required regimes: `bear,choppy`
- Eligible regimes found: `bear,choppy`
- Promotion packet status: `ready_for_governed_validation_review`
- Paper-runner authorization: `false`
- Optimizer authorization: `false` until quote-backed evidence passes

## Quote Evidence Result

The May 13 runtime OPRA capture plan does not overlap the offline replay contracts.

- Replay rows diagnosed: `38,746`
- Replay contracts diagnosed: `5,389`
- May 13 capture-plan contracts: `169`
- Exact replay contracts present in capture plan: `0`
- Capture-plan overlap status: `no_exact_replay_contract_overlap`
- Root-cause hint: `historical_or_offline_replay_contracts_not_in_runtime_opra_capture_plan`
- Strict quote-backed replay rows: `0`
- Quote-backed survivor candidates after widened review audit: `0`
- Optimizer status: `skipped_no_quote_backed_survivors`

This wave produced useful bear/choppy research candidates, but none may feed tomorrow's optimizer or paper runner until historical OPRA/SIP quote sidecars are available for the replay decision dates/contracts.

## Follow-Up Commands

```powershell
$wave = 'bear_choppy_quote_backed_offline_20260513T1255ET'
gsutil -m rsync -r "gs://codexalpaca-control-us/research_results/$wave/workers" "reports/gcp_research/$wave/workers"
```

```powershell
python scripts/build_research_portfolio_report.py `
  --replay-root reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/workers `
  --output-dir reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/portfolio_report `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --required-regimes bear,choppy `
  --candidate-identity-mode variant_profile
```

```powershell
python scripts/build_research_promotion_review_packet.py `
  --portfolio-report-json reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/portfolio_report/research_portfolio_report.json `
  --output-dir reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/promotion_packet `
  --max-review-candidates 20
```

After strict aggregation, run the quote-gap diagnostic and evidence repair sweep with a sidecar that covers the replay contract/date universe. Do not feed the optimizer from this wave unless `quote_backed_survivor_count > 0`.

```powershell
python scripts/diagnose_quote_sidecar_gaps.py `
  --trade-economics-root reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/workers `
  --quote-sidecar-csv <may13_option_quote_sidecar.csv> `
  --output-dir reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/quote_gap_diagnostic `
  --max-quote-age-seconds 60
```

```powershell
python scripts/run_quote_backed_evidence_repair_sweep.py `
  --paper-report-json reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/portfolio_report/research_portfolio_report.json `
  --paper-replay-root reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/workers `
  --quote-sidecar-csv <may13_option_quote_sidecar.csv> `
  --output-dir reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/evidence_repair `
  --initial-cash 25000
```

If candidates survive the evidence repair gate, use `quote_backed_survivor_report.json` as the only projection/optimizer input for tomorrow's candidate comparison.

Current evidence outputs:

- `reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/quote_gap_diagnostic_capture_plan/quote_gap_diagnostic_summary.json`
- `reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/evidence_repair_capture_plan_no_overlap_max50/evidence_repair_sweep_summary.json`
- `reports/gcp_research/bear_choppy_quote_backed_offline_20260513T1255ET/aggregate/evidence_repair_capture_plan_no_overlap_max50/quote_backed_survivor_report.json`

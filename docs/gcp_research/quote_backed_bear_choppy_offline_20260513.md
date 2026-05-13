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

- `qqq-rescue-c217-240-20260513qb1`
- `spy-rescue-c217-240-20260513qb1`
- `iwm-rescue-c217-240-20260513qb1`
- `avgo-rescue-c217-240-20260513qb1`
- `googl-rescue-c217-240-20260513qb1`
- `msft-rescue-c217-240-20260513qb1`
- `amzn-rescue-c217-240-20260513qb1`
- `tsm-rescue-c217-240-20260513qb1`

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

After strict aggregation, run the quote-gap diagnostic and evidence repair sweep with the May 13 exact runtime-leg sidecar. Do not feed the optimizer from this wave unless `quote_backed_survivor_count > 0`.

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

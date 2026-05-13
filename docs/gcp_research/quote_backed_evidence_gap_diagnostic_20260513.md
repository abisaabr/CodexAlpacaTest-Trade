# Quote-Backed Evidence Gap Diagnostic - 2026-05-13

## Summary

The missing OPRA/SIP matching problem is a data-coverage problem, not a symbol-normalization or as-of join bug.

For QQQ, the full evidence-repair replay has 104,826 diagnosed trade rows and 4,456 replay option contracts. The available OPRA sidecar contains QQQ quotes only for 2026-05-07, while the replay trades span 2025-04-29 through 2026-04-28. Zero replay contracts are present in the sidecar, and every entry/exit gap is classified as `trade_date_not_in_sidecar`.

For the QQQ/SPY/IWM choppy c001-c096 replay set, the same failure mode appears: 79,856 diagnosed trade rows, 1,488 replay option contracts, zero replay contracts present in the sidecar, and every entry/exit gap is `trade_date_not_in_sidecar`.

## Evidence

- QQQ full paper/review sweep diagnostic:
  - Local: `reports/gcp_research/evidence_repair_sweep_20260513/qqq_quote_gap_diagnostic_20260513T0100Z/`
  - Replays: `reports/gcp_research/evidence_repair_sweep_20260513/full_sweep_20260513T0030Z/{paper,review}/quote_sidecar_replay`
  - Sidecar: `reports/gcp_research/evidence_repair_sweep_20260513/quote_sidecars/microstructure_shadow_fastwriter_20260507/option_quote_sidecar.csv`
  - Diagnosed trade rows: `104826`
  - Replay contracts: `4456`
  - Replay contracts present in sidecar: `0`
  - Entry gap reason: `trade_date_not_in_sidecar=104826`
  - Exit gap reason: `trade_date_not_in_sidecar=104826`

- QQQ/SPY/IWM choppy c001-c096 diagnostic:
  - Local: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_096/qqq_spy_iwm_quote_gap_diagnostic/`
  - Diagnosed trade rows: `79856`
  - Replay contracts: `1488`
  - Replay contracts present in sidecar: `0`
  - Entry gap reason: `trade_date_not_in_sidecar=79856`
  - Exit gap reason: `trade_date_not_in_sidecar=79856`

## Current Data Constraint

The installed Alpaca SDK exposes historical option bars/trades and latest option quotes/snapshots, but this environment does not expose a historical OPRA quote request class. That means the current Alpaca path can support forward realtime OPRA capture and proxy historical cost realism, but it cannot backfill true historical quote sidecars for 2025-04-29 through 2026-04-28.

## Implemented Repair Tools

- `scripts/diagnose_quote_sidecar_gaps.py` classifies quote-sidecar misses by date coverage, contract coverage, and quote-age/as-of availability.
- `scripts/run_multi_ticker_realtime_shadow_monitor.py` now supports `--underlying` and `--extra-option-symbols-file` so no-submit OPRA/SIP shadow capture can target the exact runtime/review universe.
- `alpaca_lab/multi_ticker_portfolio/realtime_shadow.py` now preserves forced option symbols ahead of the normal symbol cap, which prevents targeted repair contracts from being truncated out of the websocket subscription.

## Operational Conclusion

Do not add candidates to the paper runner based on rematched historical replay unless the replay dates and option contracts have matching OPRA quote sidecars. For tomorrow's evidence path, run no-submit OPRA/SIP capture during RTH for the exact candidate universe, build sidecars after the session, apply the sidecars to same-day replays, then optimize only quote-backed survivors.

GCP can still run research-only candidate discovery in parallel, but it does not solve missing historical OPRA quotes unless those workers are given same-date historical quote sidecars or an external historical OPRA quote source.

## May 13 Capture Scope

A no-submit full-portfolio capture plan was generated at:

- Local: `reports/gcp_research/evidence_repair_sweep_20260513/full_portfolio_realtime_capture_plan_20260513T0148Z/`
- GCS: `gs://codexalpaca-control-us/gcp_research/evidence_repair_sweep_20260513/full_portfolio_realtime_capture_plan_20260513T0148Z/`

The plan covers the May 13 paper portfolio config with 15 underlyings and 504 OPRA option symbols using SIP stock data and OPRA option data. It was plan-only and submitted no orders.

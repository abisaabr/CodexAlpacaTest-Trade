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
- `scripts/build_quote_acquisition_manifest.py` converts replay trade economics into exact OPRA acquisition requirements by contract, trade date, entry/exit decision time, requested quote window, strategy family, and sidecar coverage status.
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

Use `scripts/run_no_submit_quote_capture_session.ps1` during RTH to capture the same universe and build sidecars after the stream closes. The script runs the realtime shadow monitor with `--no-trade-updates`, `--include-stock-quotes`, and `--include-option-trades`; it is broker-free and does not submit orders.

## Choppy c001-c120 Result

The QQQ/SPY/IWM c097-c120 GCP tranche for `choppy_non_single_train_test_refine_20260512T1925ET` completed, was synced, aggregated into c001-c120, mirrored, and cleaned up. It did not add new eligible candidates.

- GCS aggregate: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_120/`
- Candidate count: `1440`
- Eligible governed-review candidates: `4`
- Eligible candidates remained the prior TSM choppy debit-call-vertical set from c001-c024.
- QQQ/SPY/IWM quote-gap diagnostic: `94224` diagnosed trade rows, `1554` replay contracts, `0` replay contracts present in the 2026-05-07 sidecar, and every entry/exit miss was `trade_date_not_in_sidecar`.
- Quote-lineage audit: `2` current capital-plan rows matched replay lineage, but both are still `quote_quality_gap`.
- Hardened quote-cost/fill-haircut projection: ending equity stayed near `$4941.71` from `$25000`, with train/test optimizer selecting `0` candidates.
- Strict `$200/day` and relaxed `$25/day` constrained optimizers both selected `0` candidates.

Decision: do not add the c001-c120 choppy candidates to the paper runner without matching-date OPRA/SIP sidecars and a positive quote-backed train/test projection.

## QQQ Acquisition Manifest

The QQQ replay gap was converted from a diagnosis into an actionable acquisition manifest:

- Local: `reports/gcp_research/evidence_repair_sweep_20260513/qqq_quote_acquisition_manifest_20260513Tlocal/`
- GCS: `gs://codexalpaca-control-us/gcp_research/evidence_repair_sweep_20260513/qqq_quote_acquisition_manifest_20260513Tlocal/`
- Trade-economics CSVs scanned: `60`
- Input trade rows scanned: `134846`
- Missing OPRA leg events requested: `148788`
- Unique QQQ contract/date pairs: `455`
- QQQ trade dates needing OPRA coverage: `120`
- Replay quote source count: `option_bar_close_no_bid_ask=148788`
- Sidecar coverage status: `trade_date_not_in_sidecar=148788`
- Families represented: `debit_call_vertical=59592`, `broken_wing_call_butterfly=89196`

This manifest is the practical QQQ repair artifact. It lists the exact contract/date/window records needed for historical OPRA quote backfill or for validating same-day forward capture. Until those windows are filled with matching OPRA/SIP quotes, QQQ replay results remain proxy-priced and should not be treated as quote-backed.

## Choppy c001-c144 Result

The QQQ/SPY/IWM c121-c144 GCP tranche completed, was synced, aggregated into c001-c144, mirrored, and cleaned up. It did not add new eligible QQQ/SPY/IWM candidates.

- GCS aggregate: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_144/`
- Candidate count: `1656`
- Eligible governed-review candidates: `4`
- Eligible candidates remained the prior TSM choppy debit-call-vertical set from c001-c024.
- QQQ/SPY/IWM quote-gap diagnostic: `108592` diagnosed trade rows, `1554` replay contracts, `0` replay contracts present in the 2026-05-07 sidecar, and every entry/exit miss was `trade_date_not_in_sidecar`.
- QQQ/SPY/IWM acquisition manifest: `542928` missing OPRA leg events across `1554` contract/date pairs; all currently replay from `option_bar_close_no_bid_ask`.
- Quote-lineage audit: `2` current capital-plan rows matched replay lineage, but both are still `quote_quality_gap`.
- Hardened quote-cost/fill-haircut projection: ending equity stayed near `$4941.71` from `$25000`, with train/test optimizer selecting `0` candidates.
- Strict `$200/day` and relaxed `$25/day` constrained optimizers both selected `0` candidates.

Decision: do not add the c001-c144 choppy candidates to the paper runner without matching-date OPRA/SIP sidecars and a positive quote-backed train/test projection.

## Active Follow-On Discovery

After c001-c144 cleanup, a non-overlapping research-only c145-c168 tranche was launched for QQQ, SPY, and IWM under the same wave with suffix `20260513bc6g`.

- Expected workers: `qqq-rescue-c145-168-20260513bc6g`, `spy-rescue-c145-168-20260513bc6g`, `iwm-rescue-c145-168-20260513bc6g`
- Scope: QQQ/SPY/IWM choppy non-single-leg discovery only.
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

These workers can find additional research candidates, but they do not repair quote-backed evidence by themselves. Any candidate from this tranche still requires matching-date OPRA/SIP sidecars before paper-runner activation.

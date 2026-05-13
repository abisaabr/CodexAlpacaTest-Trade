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
- `scripts/download_option_trade_prints_from_acquisition_manifest.py` downloads historical Alpaca option trade prints for the exact contract/date/window records in an acquisition manifest. This repairs liquidity/fill evidence only; it does not create historical bid/ask quote or quote-age evidence.
- `scripts/diagnose_option_trade_print_coverage.py` measures requested-window, forward-window, and prior-window option trade-print coverage for every missing OPRA leg event in an acquisition manifest.
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

## QQQ Trade-Print Liquidity Repair

The QQQ acquisition manifest was also repaired with historical option trade prints from Alpaca's historical option trades endpoint.

- Local: `reports/gcp_research/evidence_repair_sweep_20260513/qqq_trade_print_backfill_20260513Tlocal/`
- GCS: `gs://codexalpaca-control-us/gcp_research/evidence_repair_sweep_20260513/qqq_trade_print_backfill_20260513Tlocal/`
- Contract/date rows requested: `455`
- Alpaca request count: `120`
- Option trade-print rows downloaded: `106484`
- Unique contracts with trade prints: `455`
- Failed requests: `0`

Coverage diagnostic:

- Local: `reports/gcp_research/evidence_repair_sweep_20260513/qqq_trade_print_coverage_20260513Tlocal/`
- GCS: `gs://codexalpaca-control-us/gcp_research/evidence_repair_sweep_20260513/qqq_trade_print_coverage_20260513Tlocal/`
- Manifest leg events: `148788`
- Events with requested-window trade print: `148788`
- Events with forward-window trade print: `148788`
- Events with recent-prior trade print: `136621`
- Coverage statuses: `forward_and_prior_prints=136621`, `forward_print_only=12167`

Conclusion: QQQ had actual option trading around every replay decision. The remaining blocker is not option-trade liquidity; it is missing historical OPRA bid/ask quote, spread, and quote-age evidence for the replay dates.

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

## Choppy c001-c144 Trade-Print Liquidity Repair

The QQQ/SPY/IWM choppy c001-c144 acquisition manifest was repaired with historical option trade prints.

- Local: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_144/qqq_spy_iwm_trade_print_backfill_20260513Tlocal/`
- GCS: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_144/qqq_spy_iwm_trade_print_backfill_20260513Tlocal/`
- Contract/date rows requested: `1554`
- Alpaca request count: `382`
- Option trade-print rows downloaded: `361327`
- Unique contracts with trade prints: `1554`
- Failed requests: `0`

Coverage diagnostic:

- Local: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_144/qqq_spy_iwm_trade_print_coverage_20260513Tlocal/`
- GCS: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_144/qqq_spy_iwm_trade_print_coverage_20260513Tlocal/`
- Manifest leg events: `542928`
- Events with requested-window trade print: `542928`
- Events with forward-window trade print: `542928`
- Events with recent-prior trade print: `432493`
- Coverage statuses: `forward_and_prior_prints=432493`, `forward_print_only=110435`

Conclusion: liquidity evidence is broad enough for the QQQ/SPY/IWM c001-c144 replay set, but quote-backed economics remain blocked until historical bid/ask quote windows are filled.

## Choppy c001-c168 Result

The non-overlapping QQQ/SPY/IWM c145-c168 research-only tranche completed, was synced, aggregated into c001-c168, mirrored, and cleaned up. No active `20260513bc6g` research VMs remain.

- GCS aggregate: `gs://codexalpaca-control-us/research_results/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_168/`
- Candidate count: `1872`
- Eligible governed-review candidates: `4`
- Eligible candidates remained the prior TSM choppy debit-call-vertical set from c001-c024.
- New QQQ c145-c168 candidates showed strong full-period PnL and fill coverage, but failed the positive test-PnL gate and remain research-only blocked.
- Portfolio report blocker counts: `min_net_pnl_not_positive=1856`, `test_net_pnl_not_above_0=1802`, `fill_coverage_below_0.90=40`.
- Promotion packet decision: `ready_for_governed_validation_review` for the four existing TSM candidates only.
- QQQ/SPY/IWM quote-gap diagnostic: `126324` diagnosed trade rows, `1570` replay contracts, `0` replay contracts present in the 2026-05-07 sidecar, and every entry/exit miss was `trade_date_not_in_sidecar`.
- QQQ/SPY/IWM acquisition manifest: `631588` missing OPRA leg events across `1570` contract/date pairs; all currently replay from `option_bar_close_no_bid_ask`.
- Quote-lineage audit: `2` current capital-plan rows matched replay lineage, but both are still `quote_quality_gap`.

Trade-print liquidity repair for c001-c168:

- Local: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_168/qqq_spy_iwm_trade_print_backfill_20260513Tlocal/`
- Contract/date rows requested: `1570`
- Alpaca request count: `382`
- Option trade-print rows downloaded: `373871`
- Unique contracts with trade prints: `1570`
- Failed requests: `0`
- Coverage diagnostic local: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_168/qqq_spy_iwm_trade_print_coverage_20260513Tlocal/`
- Manifest leg events: `631588`
- Events with requested-window trade print: `631588`
- Events with forward-window trade print: `631588`
- Events with recent-prior trade print: `503239`
- Coverage statuses: `forward_and_prior_prints=503239`, `forward_print_only=128349`

Hardened projection and optimizer result:

- Projection output: `reports/gcp_research/choppy_non_single_train_test_refine_20260512T1925ET/aggregate_c001_168/growth_projection/benchmark_plus_choppy_quote_cost/`
- Starting equity: `$25000`
- Ending equity: `$4941.71`
- Total return: `-80.2332%`
- Max drawdown: `-80.2332%`
- Target hit probability: `0.0%`
- Train/test selected candidates: `0`
- Strict `$200/day` constrained optimizer selected candidates: `0`
- Relaxed `$25/day` constrained optimizer selected candidates: `0`

Decision: do not add the c001-c168 choppy candidates to the paper runner. The trade-print repair shows option liquidity exists, but quote-backed economics still require matching historical OPRA bid/ask quote windows or same-day forward OPRA capture before promotion.

## Updated Repair Path

The immediate QQQ example is now split cleanly:

- Solved: exact contract/date/window acquisition manifest exists for missing QQQ OPRA evidence.
- Solved: historical option trade prints are available for every QQQ replay leg event, confirming liquidity around each decision.
- Unsolved: historical OPRA bid/ask quotes are not available through the current Alpaca SDK path for the 2025-04-29 through 2026-04-28 replay dates.

Next operational repair path:

- Acquire external historical OPRA BBO quote data for the contract/date/window rows in the acquisition manifests, or capture OPRA/SIP forward during RTH and only replay same-day strategy decisions against those sidecars.
- Enrich trade economics with bid, ask, midpoint, spread, quote age, quote source, and trade-print liquidity fields before projection.
- Reject candidates with incomplete quote lineage.
- Optimize only quote-backed survivors under train/test-positive, drawdown, diversification, and real paper-trader risk constraints.

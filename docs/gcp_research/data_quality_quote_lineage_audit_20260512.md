# Data Quality Quote Lineage Audit - 2026-05-12

- Source commit before this pass: `e62bd38f93c27d69453a69d25e778b065bfb2a32`
- Broker mode impact: `none`
- Paper-runner state changed: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Promotion decision: `no new strategy is eligible for paper-runner addition from this pass`

## What Changed

`scripts/repair_projection_replay_lineage.py` now emits quote-quality lineage diagnostics alongside replay lineage repair outputs.

New output:

- `quote_quality_lineage.csv`
- `quote_quality_status_counts`
- `recommended_data_action_counts`

Each candidate is classified for:

- replay lineage present or missing
- entry and exit bid/ask-backed quote-source rate
- entry and exit spread coverage
- entry and exit quote-age coverage
- entry and option trade-print zero/missing rates
- recommended data action before projection or promotion

Validation command:

```powershell
python -m pytest -q
```

Result: `315 passed, 1 warning`.

## Current Paper Strategy Set Audit

- Command/run ID: `current_paper_lineage_quote_quality_audit_20260512`
- Local output: `reports/gcp_research/quote_realism_projection_hardening_20260512/current_paper_lineage_quote_quality_audit_20260512/`
- GCS output: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/current_paper_lineage_quote_quality_audit_20260512/`

Command:

```powershell
python scripts/repair_projection_replay_lineage.py --portfolio-report-json reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/current_paper_synthetic_portfolio_report.json --replay-root reports/gcp_research/may8_paper_portfolio_projection_20260508T1255Z/filtered_replay_projection_compatible --search-root reports/gcp_research --search-root D:\codexalpaca_runtime\gcp_sync --search-root D:\gcp_bc4_workers --output-dir reports/gcp_research/quote_realism_projection_hardening_20260512/current_paper_lineage_quote_quality_audit_20260512
```

Result:

- Capital-plan strategies: `344`
- Matched in current replay root: `228`
- Repaired from search history: `116`
- Unmatched replay lineage: `0`
- Quote-backed replays: `0`
- Quote-quality gaps: `344`
- Required action: `rerun_replay_with_bid_ask_spread_quote_age_and_trade_prints` for all `344`

Interpretation: lineage is no longer the immediate blocker for the current paper set; quote-quality evidence is. The available replays still do not contain sufficient bid/ask, spread, quote-age, and trade-print evidence for hardened projection.

## All-Local Candidate Pool Audit

- Command/run ID: `all_local_candidate_lineage_quote_quality_audit_20260512`
- Local output: `reports/gcp_research/quote_realism_projection_hardening_20260512/all_local_candidate_scan/lineage_quote_quality_audit_20260512/`
- GCS output: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/all_local_candidate_scan/lineage_quote_quality_audit_20260512/`

Command:

```powershell
python scripts/repair_projection_replay_lineage.py --portfolio-report-json reports/gcp_research/quote_realism_projection_hardening_20260512/all_local_candidate_scan/combined_research_portfolio_report.json --replay-root reports/gcp_research/quote_realism_projection_hardening_20260512/all_local_candidate_scan/targeted_replay_inputs --search-root reports/gcp_research --search-root D:\codexalpaca_runtime\gcp_sync --search-root D:\gcp_bc4_workers --output-dir reports/gcp_research/quote_realism_projection_hardening_20260512/all_local_candidate_scan/lineage_quote_quality_audit_20260512
```

Result:

- Capital-plan candidates: `203`
- Matched in current replay root: `201`
- Repaired from search history: `2`
- Unmatched replay lineage: `0`
- Quote-backed replays: `0`
- Quote-quality gaps: `203`
- Required action: `rerun_replay_with_bid_ask_spread_quote_age_and_trade_prints` for all `203`

Interpretation: the broader local candidate pool also has replay lineage, but no candidate has quote-backed replay evidence.

## Realtime Quote Capture Plan

- Command/run ID: `realtime_quote_capture_plan_20260512`
- Local output: `reports/gcp_research/quote_realism_projection_hardening_20260512/realtime_quote_capture_plan_20260512/`
- GCS output: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/realtime_quote_capture_plan_20260512/`

Command:

```powershell
python scripts/run_multi_ticker_realtime_shadow_monitor.py --portfolio-config config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml --output-dir reports/gcp_research/quote_realism_projection_hardening_20260512/realtime_quote_capture_plan_20260512 --max-option-symbols 900 --include-stock-quotes --include-option-trades --no-trade-updates
```

Result:

- Mode: `no_submit_shadow`
- Stream requested: `false`
- Underlyings: `15`
- Option symbols planned: `504`
- Stock feed: `sip`
- Option feed: `opra`
- Paper orders: `false`

This is a safe plan-only artifact. It did not connect streams and did not submit orders.

## Next Best Data-Quality Steps

1. Run the realtime shadow monitor during RTH in `--stream` mode for the active paper universe to collect multi-day OPRA bid/ask quotes, option trades, SIP stock quotes, stock bars, and latency.
2. Convert captured websocket quote events into replay-compatible quote-backed option bars or a quote-sidecar keyed by `option_symbol,timestamp`.
3. Rerun the current paper strategy set and all-local candidate pool against quote-backed replay inputs.
4. Require `quote_backed_replay` status before using a candidate in the hardened projector.
5. Keep `--market-quality-cost-model-enabled` and `--fill-model-haircut-positive-pnl` on for all portfolio projections.
6. Do not add candidates to the paper runner until train/test stability survives quote-backed economics.

## Final Status

No strategy is eligible for paper-runner addition from this pass. The correct next project focus is collecting and replaying real quote-backed evidence, not expanding parameter grids on bar-close/no-bid-ask data.

# QQQ Market-Time Refinement Wave - 2026-05-05

## Why This Wave Exists

The completed QQQ edge-throttle wave produced zero eligible strategies, but it
also exposed a timing bug in the new entry-window layer.

The edge-throttle entry windows were evaluated using raw timestamp hours. The
QQQ stock bars are UTC-oriented, while the option-session filter evaluates
entries and exits in New York market time. That mismatch shifted intended
morning/midday/late windows and caused several candidates to generate source
stock trades that were later dropped as outside option RTH.

## Completed Prior Wave

Wave:

- `ticker365_qqq_edge_throttle_20260505T0500Z`

Final aggregate:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_edge_throttle_20260505T0500Z/aggregate/qqq_edge_throttle_full_summary.json`

Result:

- Completed candidates: `50`
- Eligible-like candidates after hardened summarizer: `0`
- Missing candidates: none

Most useful non-eligible clue:

- `portfolio12h__qqq__bull__call__single_leg_repair__2825e3bf6cf9b8`
  - Fill coverage: `0.9916`
  - Net PnL: `3521.619`
  - Test PnL: `3781.646`
  - Train PnL: `-260.027`
  - Recommendation: `hold_option_economics`

Decision:

- No QQQ strategy from this wave should be promoted.
- No paper-trader manifest change should be made from this wave.

## Patch

`scripts/run_gcp_research_wave.py` now converts timestamps to
`America/New_York` before applying:

- `min_minutes_since_open`
- `max_minutes_since_open`
- daily signal grouping/throttling

This aligns stock-proxy entry windows with the option replay
`option_rth_same_day` filter.

Validation:

```powershell
python -m pytest -q tests\test_run_gcp_research_wave.py tests\test_run_option_aware_research_backtest.py
python -m ruff check scripts\run_gcp_research_wave.py scripts\build_qqq_regime_research_inputs.py tests\test_run_gcp_research_wave.py
```

Observed:

- `23 passed`
- `ruff` clean

## New Wave

Wave:

- `ticker365_qqq_markettime_refine_20260505T0630Z`

Inputs:

- `reports\gcp_research\ticker365_qqq_markettime_refine_20260505T0630Z\inputs\qqq_regime_redesign_variants.jsonl`
- `reports\gcp_research\ticker365_qqq_markettime_refine_20260505T0630Z\inputs\qqq_regime_redesign_option_queue.json`

Candidate count:

- `50`

Plan:

- Re-run the same 50 QQQ candidates after the market-time fix.
- Prioritize candidates `1-16` first because these include the bull/bear
  single-leg and debit structures most likely to benefit from corrected entry
  windows.
- Continue through credit and choppy only after confirming the first tranche
  no longer has systematic RTH drops.

Promotion rule:

- Do not promote anything unless the generated promotion-review packet says
  `eligible_for_promotion_review`.


# QQQ Paper Readiness Edge-Throttle Handoff - 2026-05-05

## Operator Summary

QQQ is not paper-trader ready yet. The latest guarded credit wave fixed the
false-positive short-premium path and completed all targeted candidates, but
zero candidates met governed promotion-review criteria.

The important conclusion is that QQQ no longer looks blocked primarily by raw
option data coverage. The blocker has moved to strategy edge and trade
selection quality:

- Dense QQQ option data remains the intended foundation.
- Strategy fill coverage is strong for many single-leg and choppy defined-risk
  structures.
- Choppy short-premium results that appeared very profitable before the guard
  were invalid because impossible credit structures could be sized with near
  zero risk.
- After rejecting impossible credit structures, choppy fill stayed high but
  economics were strongly negative.
- Bull and bear credit spreads found a few positive full-period/test candidates,
  but fill coverage stayed below the `0.90` gate.

No paper trading, broker-facing execution, live manifest change, or risk policy
change was made.

## Latest Guarded QQQ Wave

Wave:

- `ticker365_qqq_credit_guarded_20260505T0340Z`

GCS summary:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_credit_guarded_20260505T0340Z/aggregate/qqq_credit_guarded_summary.json`

Observed summary:

- Completed candidates: `38`
- Eligible-like candidates: `0`
- Missing candidate indices: none
- Bull candidates: `10`
- Bear candidates: `10`
- Choppy candidates: `18`

Best non-eligible observations:

- Best bull by net PnL:
  `portfolio12h__qqq__bull__call__bull_put_credit_spread__7c1e5796b55153`
  had fill `0.688`, net PnL `807.625`, test PnL `-41.706`.
- Best bear by net PnL:
  `portfolio12h__qqq__bear__put__bear_call_credit_spread__607f9c4352904d`
  had fill `0.7692`, net PnL `1571.185`, test PnL `405.129`.
- Best choppy economics after the guard were still negative:
  `portfolio12h__qqq__choppy__call__iron_butterfly__40c7b75c395833`
  had fill `0.9406`, net PnL `-58203.859`, test PnL `-5445.853`.

Decision:

- No QQQ candidate is eligible for governed promotion review.
- No QQQ candidate should be pushed into the paper trader from this wave.

## Patch Added For Next QQQ Wave

The next wave should test cleaner stock-proxy entries rather than simply adding
more option structures.

Implemented:

- Explicit entry windows using `min_minutes_since_open` and
  `max_minutes_since_open`.
- `entry_signal_mode` support for `continuous`, `rising_edge`, and
  `daily_first`.
- `cooldown_bars` and `max_signals_per_day` throttles.
- Directional trend-strength filter using `min_trend_gap_pct`.
- Updated QQQ regime input builder to generate a smaller, higher-quality
  50-candidate packet:
  - Bull single-leg and debit vertical: `8`
  - Bear single-leg and debit vertical: `8`
  - Bull put credit spread: `8`
  - Bear call credit spread: `8`
  - Choppy iron condor, iron butterfly, premium defense: `18`

Validation:

```powershell
python -m pytest -q tests\test_run_gcp_research_wave.py tests\test_run_option_aware_research_backtest.py
python -m ruff check scripts\run_gcp_research_wave.py scripts\build_qqq_regime_research_inputs.py tests\test_run_gcp_research_wave.py
```

Observed:

- `23 passed`
- `ruff` clean

## Next Wave

Wave:

- `ticker365_qqq_edge_throttle_20260505T0500Z`

Local inputs:

- `reports\gcp_research\ticker365_qqq_edge_throttle_20260505T0500Z\inputs\qqq_regime_redesign_variants.jsonl`
- `reports\gcp_research\ticker365_qqq_edge_throttle_20260505T0500Z\inputs\qqq_regime_redesign_option_queue.json`

Launch command:

```powershell
.\scripts\launch_gcp_qqq_family_econ_micro_shards.ps1 `
  -WaveId ticker365_qqq_edge_throttle_20260505T0500Z `
  -InstanceSuffix 20260505t `
  -CandidateIndices @(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) `
  -TopN 50 `
  -MaxLaunches 20 `
  -VariantPath reports\gcp_research\ticker365_qqq_edge_throttle_20260505T0500Z\inputs\qqq_regime_redesign_variants.jsonl `
  -QueuePath reports\gcp_research\ticker365_qqq_edge_throttle_20260505T0500Z\inputs\qqq_regime_redesign_option_queue.json
```

Follow-on tranches:

- `21-34`: remaining bull/bear credit and first choppy structures.
- `35-50`: remaining choppy structures.

Summarize after each tranche:

```powershell
python scripts\summarize_gcp_micro_wave.py `
  --wave-id ticker365_qqq_edge_throttle_20260505T0500Z `
  --candidate-start 1 `
  --candidate-end 50 `
  --gcloud-bin C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd `
  --output-json reports\gcp_research\ticker365_qqq_edge_throttle_20260505T0500Z\qqq_edge_throttle_summary.json
```

## Promotion Rule

Hard rule remains unchanged:

- Do not promote anything unless the generated promotion-review packet says
  `eligible_for_promotion_review`.
- Do not lower `fill_coverage >= 0.90`.
- Do not start paper trading from a research summary alone.


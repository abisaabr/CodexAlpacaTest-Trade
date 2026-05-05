# QQQ Regime Redesign Wave - 2026-05-05

## Objective

Move QQQ from fill-repair into governed paper-readiness research by testing a
small, explicit bull/bear/choppy queue against the 365-day dense QQQ option
dataset.

## Guardrails

- Broker-facing execution: none.
- Paper orders: none.
- Live manifest changes: none.
- Risk policy changes: none.
- Promotion gate remains `fill_coverage >= 0.90`.
- Promotion means governed validation review only until a generated promotion
  packet says the candidate is eligible.

## Code Changes

- `scripts/build_qqq_regime_research_inputs.py` builds a compact QQQ regime
  queue with:
  - 5 bull call single-leg repair variants.
  - 5 bear put single-leg repair variants.
  - 18 choppy premium/defined-risk variants across iron condor, iron butterfly,
    and premium defense structures.
- `scripts/run_gcp_research_wave.py` now supports range-bound, timeout-only
  stock proxy entries for choppy short-premium strategies.
- `scripts/run_option_aware_research_backtest.py` now maps
  `premium_defense_spread` to a complete iron-condor-style four-leg structure
  and sizes credit structures using side spread width instead of full condor
  span.
- `scripts/launch_gcp_qqq_family_econ_micro_shards.ps1` accepts custom
  `VariantPath` and `QueuePath` so small targeted queues can be launched without
  mutating the canonical portfolio input packet.

## Validation

```powershell
python -m pytest -q tests\test_run_option_aware_research_backtest.py tests\test_run_gcp_research_wave.py
python -m ruff check scripts\run_gcp_research_wave.py scripts\run_option_aware_research_backtest.py scripts\build_qqq_regime_research_inputs.py tests\test_run_option_aware_research_backtest.py tests\test_run_gcp_research_wave.py
```

Observed:

- `18 passed`
- `ruff` clean

## Input Generation

```powershell
python scripts\build_qqq_regime_research_inputs.py `
  --output-dir reports\gcp_research\ticker365_qqq_regime_redesign_20260505T0400Z\inputs `
  --wave-id ticker365_qqq_regime_redesign_20260505T0400Z
```

Generated input shape:

- `bull/single_leg_repair`: 5
- `bear/single_leg_repair`: 5
- `choppy/iron_condor`: 6
- `choppy/iron_butterfly`: 6
- `choppy/premium_defense_spread`: 6
- Total: 28

## Planned Launch

Wave ID:

- `ticker365_qqq_regime_redesign_20260505T0400Z`

Expected GCS root:

- `gs://codexalpaca-control-us/research_results/ticker365_qqq_regime_redesign_20260505T0400Z/`

Launch command shape:

```powershell
.\scripts\launch_gcp_qqq_family_econ_micro_shards.ps1 `
  -WaveId ticker365_qqq_regime_redesign_20260505T0400Z `
  -InstanceSuffix 20260505r `
  -CandidateIndices @(1,2,3,4,5,6,7,8,9,10,11,12) `
  -TopN 28 `
  -MaxLaunches 12 `
  -VariantPath reports\gcp_research\ticker365_qqq_regime_redesign_20260505T0400Z\inputs\qqq_regime_redesign_variants.jsonl `
  -QueuePath reports\gcp_research\ticker365_qqq_regime_redesign_20260505T0400Z\inputs\qqq_regime_redesign_option_queue.json
```

Run candidates in small tranches so the controller can react quickly:

- `1-5`: bull call single-leg repair.
- `6-10`: bear put single-leg repair.
- `11-16`: choppy iron condor.
- `17-22`: choppy iron butterfly.
- `23-28`: choppy premium defense spread.

## Decision Rule

No candidate moves forward unless its generated promotion packet says
`eligible_for_promotion_review`.

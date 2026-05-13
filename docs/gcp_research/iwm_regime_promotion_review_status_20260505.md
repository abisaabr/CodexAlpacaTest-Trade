# IWM Regime Promotion-Review Status - 2026-05-05

This is a research-only governed-validation packet. It does not authorize live trading, broker-facing paper activation, IWM manifest changes, or risk-policy changes.

## Final Decision

- Final rollup decision: `ready_for_governed_validation_review`
- Candidate-level decision: `ready_for_governed_validation_review`
- Required regimes: `bull`, `bear`, `choppy`
- Eligible regimes: `bull`, `bear`, `choppy`
- Missing eligible regimes: none
- Eligible candidate count: `8`
- Source report count: `115`
- Candidate count: `1272`
- Fill gate: `0.90`

## Eligible Regime Sleeve

| Regime | Candidate | Family | Net PnL | Test PnL | Fill | Trades |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| bull | `portfolio12h__iwm__bull__call__single_leg_repair__3dc6b95db3d9a6__profile_iwm-bull-c007-iwm-e0-x60-entry-liquidity-first-research-only` | `single_leg_repair` | `617.845` | `829.028` | `0.9940` | `166` |
| bear | `portfolio12h__iwm__bear__put__single_leg_repair__89134067786703__profile_iwm-rescue-c001-012-iwm-e0-x60-nearest-contract` | `single_leg_repair` | `1419.675` | `1689.620` | `0.9056` | `163` |
| bear | `portfolio12h__iwm__bear__put__single_leg_repair__c7895e06f5c278__profile_iwm-rescue-c001-012-iwm-e0-x60-nearest-contract` | `single_leg_repair` | `511.584` | `1833.476` | `0.9056` | `163` |
| choppy | `portfolio12h__iwm__choppy__call__single_leg_repair__25a40ccaf9318c__profile_iwm-rescue-c049-060-iwm-e0-x60-entry-liquidity-first-research-only` | `single_leg_repair` | `2675.750` | `284.970` | `1.0000` | `57` |
| choppy | `portfolio12h__iwm__choppy__call__single_leg_repair__e0872c62579f6c__profile_iwm-rescue-c049-060-iwm-e0-x60-entry-liquidity-first-research-only` | `single_leg_repair` | `2510.915` | `246.009` | `1.0000` | `57` |
| choppy | `portfolio12h__iwm__choppy__call__single_leg_repair__659e26dfde89ff__profile_iwm-rescue-c073-084-iwm-e0-x60-entry-liquidity-first-research-only` | `single_leg_repair` | `2294.355` | `264.450` | `1.0000` | `78` |
| choppy | `portfolio12h__iwm__choppy__call__single_leg_repair__2dcc788cb02d52__profile_iwm-rescue-c073-084-iwm-e0-x60-entry-liquidity-first-research-only` | `single_leg_repair` | `2230.419` | `263.451` | `1.0000` | `78` |
| choppy | `portfolio12h__iwm__choppy__call__single_leg_repair__9875e5091f29bd__profile_iwm-rescue-c061-072-iwm-e0-x60-entry-liquidity-first-research-only` | `single_leg_repair` | `2386.040` | `14.241` | `1.0000` | `57` |

## What Changed

- Added a reproducible IWM choppy time-window refine grid in commit `c33d84b9cf3ccbdeeb93cc9f403f925eac61c09e`.
- Added a tighter IWM choppy micro-exit refine grid in commit `28e86109812a18f75a88d1e548a88fbf0b464fe5`.
- Added a stricter IWM choppy quality-filter grid in commit `5b890495c02bea7d92ff360b205e365d7190fa1a`.
- The quality-filter wave is the first combined rollup that produced bull, bear, and choppy governed-review eligibility without lowering the `fill_coverage >= 0.90` gate.

## Why Choppy Finally Cleared

Earlier IWM choppy candidates split into two failure modes:

- High-PnL debit-put-vertical candidates were not promotion-ready because fill coverage stayed around `0.75-0.78`, and the strongest PnL was dominated by a single April 2026 outlier.
- High-fill lower-band single-leg call candidates had `~0.99-1.00` fill but negative full-period economics.

The successful choppy candidates use the high-fill lower-band call structure, then add an `ultra_narrow_low_trend` choppy-state filter:

- `max_range_pct=0.004`
- `max_trend_gap_pct=0.0008`
- `max_midpoint_distance_pct=0.003`
- `min_minutes_since_open=120`
- `range_entry_side=lower_band`
- `range_edge_pct=0.0012`
- strict next-expiry option selection with entry-liquidity-first lookup

This preserved fill coverage at `1.0000` while turning full-period net PnL positive.

## Artifact Paths

- Final rollup JSON: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/research_wave_portfolio_rollup.json`
- Final rollup Markdown: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/research_wave_portfolio_rollup.md`
- Promotion packet JSON: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/promotion_review_packet/research_promotion_review_packet.json`
- Promotion packet Markdown: `reports/gcp_research/iwm_regime_rescue_quality_filter_20260505/promotion_review_packet/research_promotion_review_packet.md`

## GCS Inputs And Outputs

- Final quality-filter wave: `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_quality_filter_20260505T2003Z/`
- Time-window wave: `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_timewindow_refine_20260505T1912Z/`
- Micro-exit wave: `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_micro_exit_20260505T1938Z/`
- Final rollup mirror: `gs://codexalpaca-control-us/research_results/iwm_regime_rescue_quality_filter_20260505/`

## Safety Notes

- No broker-facing session was started.
- No live manifest was changed.
- No risk policy was changed.
- No IWM paper activation was performed.
- This packet only supports governed validation review.

## Next Step

Run an independent holdout/walk-forward confirmation for the 8 eligible IWM candidates before any separate paper-runner launch packet is considered. The choppy sleeve is promising but should be treated as newly discovered and not live-ready until broker-audited paper validation is separately approved.

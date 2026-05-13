# IWM Regime-Complete Promotion Readiness - 2026-05-05

## Decision

`research_only_blocked_regime_incomplete`

IWM is not ready for a regime-complete paper portfolio. The current evidence supports one bull candidate for governed-validation review context, but bear and choppy have zero eligible candidates. No live manifest, risk policy, broker state, or paper-runner symbol set was changed.

## Scope

- Symbol: `IWM`
- Required regimes: `bull`, `bear`, `choppy`
- Source reports aggregated: `52`
- Candidate rows aggregated: `144`
- Candidate-level governed-review eligible rows: `1`
- Eligible regimes: `bull`
- Missing eligible regimes: `bear`, `choppy`
- Regime-complete for promotion review: `false`
- Broker-facing trading: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

## GCS Inputs

The rollup inspected existing research portfolio reports from these research-only waves:

- `gs://codexalpaca-control-us/research_results/ticker365_iwm_bull_premium_exit_20260505T1035Z/`
- `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_premium_exit_20260505T1035Z/`
- `gs://codexalpaca-control-us/research_results/ticker365_iwm_choppy_refine_20260505T1035Z/`
- `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/`

## Regime Summary

| Regime | Candidates | Eligible | Best candidate | Best fill | Best status | Main blockers |
| --- | ---: | ---: | --- | ---: | --- | --- |
| Bull | 8 | 1 | `portfolio12h__iwm__bull__call__single_leg_repair__d927c9c9c680bc__profile_iwm-bull-c006-iwm-e0-x60-entry-liquidity-first-research-only` | 0.9896 | `research_only_blocked` | `min_net_pnl_not_positive`, `test_net_pnl_not_above_0` |
| Bear | 56 | 0 | `portfolio12h__iwm__bear__put__single_leg_repair__5884e22d380daf__profile_iwm-bc-red-c001-006-iwm-e0-x60-entry-liquidity-first-research-only` | 1.0 | `research_only_blocked` | `min_net_pnl_not_positive`, `test_net_pnl_not_above_0`, `fill_coverage_below_0.90` |
| Choppy | 80 | 0 | `portfolio12h__iwm__choppy__put__debit_put_vertical__981bf7626eaa9e__profile_iwm-bc-red-c067-072-iwm-e0-x60-entry-liquidity-first-research-only` | 0.7587 | `research_only_blocked` | `min_net_pnl_not_positive`, `test_net_pnl_not_above_0`, `fill_coverage_below_0.90` |

The best eligible candidate is the bull single-leg repair candidate:

- Candidate: `portfolio12h__iwm__bull__call__single_leg_repair__3dc6b95db3d9a6__profile_iwm-bull-c007-iwm-e0-x60-entry-liquidity-first-research-only`
- Net PnL: `617.845`
- Test net PnL: `829.028`
- Strategy fill coverage: `0.994`
- Data foundation coverage: `1.0`
- Option trades: `166`
- Promotion blockers: none

## Aggregate Blockers

- `min_net_pnl_not_positive`: `140`
- `test_net_pnl_not_above_0`: `109`
- `fill_coverage_below_0.90`: `55`

## Interpretation

The reporting chain now separates candidate-level eligibility from regime-complete portfolio readiness:

- Candidate-level decision: `ready_for_governed_validation_review`
- Portfolio/regime-complete decision: `research_only_blocked_regime_incomplete`

This is the correct outcome for IWM. The bull sleeve has one eligible research candidate, but IWM should remain excluded from a bull/bear/choppy paper portfolio until bear and choppy each produce at least one generated `eligible_for_promotion_review` candidate.

## Commands Used

The GCS reports were copied locally as small JSON control artifacts, then aggregated with:

```powershell
python scripts\build_research_wave_portfolio_rollup.py `
  --report-root "$env:TEMP\codex_iwm_regime_reports_20260505" `
  --output-dir reports\gcp_research\iwm_regime_readiness_20260505 `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --max-positions 5 `
  --max-strategies-per-symbol 3 `
  --max-symbol-weight 1.0 `
  --initial-cash 25000 `
  --max-review-candidates 20 `
  --required-regimes bull,bear,choppy
```

## Output Artifacts

Local generated artifacts:

- `reports/gcp_research/iwm_regime_readiness_20260505/research_wave_portfolio_rollup.json`
- `reports/gcp_research/iwm_regime_readiness_20260505/research_wave_portfolio_rollup.md`
- `reports/gcp_research/iwm_regime_readiness_20260505/promotion_review_packet/research_promotion_review_packet.json`
- `reports/gcp_research/iwm_regime_readiness_20260505/promotion_review_packet/research_promotion_review_packet.md`

GCS mirror:

- `gs://codexalpaca-control-us/gcp_research/iwm_regime_complete_promotion_readiness_20260505.md`
- `gs://codexalpaca-control-us/research_results/iwm_regime_readiness_20260505/`

## Next Research Step

Do not run more blind IWM breadth. The highest-value next work is two targeted lanes:

- Bear: redesign economics first. Several bear single-leg rows have perfect fill but fail full-period or test-period PnL, so this is mainly strategy design, not data repair.
- Choppy: repair or redesign the debit-put-vertical near-miss. It has strong PnL but only `0.7587` strategy fill coverage, with entry timing and selected-contract availability as the dominant gaps.

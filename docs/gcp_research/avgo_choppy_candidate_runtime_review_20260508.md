# AVGO Choppy Runtime Compatibility Review - 2026-05-08

## Scope

This review covers the AVGO choppy candidates from the structure-aware bear/choppy rerun. It is a runtime-translation review only. No paper config, live manifest, broker state, or global risk policy was changed.

## Source Artifacts

- Source commit reviewed: `02c7fd9`
- Rerun GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_structure_aware_rerun_20260508T1745ET/`
- Promotion packet: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/portfolio_report/promotion_review_packet/research_promotion_review_packet.json`
- Temporary runtime review manifest: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/runtime_review/avgo_choppy_review_manifest.yaml`

## Promotion Packet Decision

- Packet decision: `ready_for_governed_validation_review`
- Governance review scope: `per_regime_governed_validation_review`
- Eligible regimes: `choppy`
- Missing eligible regimes: `bear`
- Regime completeness policy: `informational_only_not_a_hard_promotion_gate`
- Fill coverage gate: `0.90`
- Eligible review candidates: `6`
- Unique eligible base candidates: `5`

The packet is not a full bear/choppy regime-complete portfolio. It is eligible only as a per-regime governed-validation review packet.

## Runtime Translation Result

The manifest builder translated `3` AVGO choppy strategies into a non-broker-facing review manifest:

| Candidate | Family | Runtime status | Fill | Trades | Full PnL | Test PnL |
|---|---:|---|---:|---:|---:|---:|
| `a02aa65e48140b` | debit_call_vertical | `packet_translated_to_runtime_native_multileg` | `0.972` | `243` | `1941.243` | `5470.859` |
| `ae9cdb85a49d1e` | debit_call_vertical | `packet_translated_to_runtime_native_multileg` | `0.972` | `243` | `1557.568` | `5016.848` |
| `1f824f17149565` | single_leg_repair | `packet_translated_to_runtime_single_leg` | `0.908` | `227` | `2653.645` | `206.838` |

Two AVGO debit-put-vertical candidates were skipped by the runtime translator because the current governed choppy runtime path supports lower-band call reversion, not upper-band put reversion:

- `729fa1f3d12013`
- `0143988b9a7352`

## Compatibility Assessment

The two AVGO debit-call-vertical candidates are technically translatable to native multi-leg runtime strategies. Their generated legs are a long call around `0.55` delta and a short call around `0.37` delta, with tight liquidity gates, midday choppy range-bound entries, and premium target/stop exits.

The single-leg AVGO candidate is also translatable, but its test PnL is materially weaker than the vertical candidates.

Broker multi-leg order tests and manifest translation tests passed locally after the stale multi-leg semantics test was fixed. The generated manifest correctly labels native multi-leg strategies with `packet_translated_to_runtime_native_multileg`.

## Operational Risk From May 8 Paper Session

The May 8 paper postmortem found that runtime safety worked, but execution quality still needs hardening before expanding multi-leg order submission:

- Four entry attempts did not fill and eventually activated the entry execution circuit breaker.
- One XOM broken-wing put butterfly entry attempt did not fill.
- One GOOGL single-leg trade required repeated exit attempts before EOD flatten cleanup.
- Exit submission noise was high, especially for GOOGL.

These findings do not invalidate the AVGO backtest packet, but they argue against adding these candidates directly to the next order-submitting paper session without either shadow validation or stronger execution admission penalties.

## Recommendation

- Keep the generated AVGO runtime review manifest as research/governed-validation evidence only.
- Do not add the skipped AVGO debit-put verticals to paper until upper-band put reversion is implemented and tested in the runtime path.
- If AVGO choppy is tested in paper, start with the two debit-call verticals in a clearly labeled PAPER validation scope after exit escalation and live-entry fill-rate penalties are patched.
- Prefer a no-submit shadow run first if realtime option quote coverage is uncertain.

## State Changes

- Eligible for governed promotion review: yes, per-regime choppy only.
- Eligible for full bear/choppy regime-complete promotion review: no.
- Paper-runner state changed: no.
- Paper config changed: no.
- Live manifest changed: no.
- Risk policy changed: no.

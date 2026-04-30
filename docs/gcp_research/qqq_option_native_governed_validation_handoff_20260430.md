# QQQ Option-Native Governed Validation Handoff

Generated: 2026-04-30

## Status

The QQQ option-native research chain now has one bull, one bear, and one choppy candidate ready for governed validation review. This is not broker-facing and does not modify the paper-runner live manifest or risk policy.

## Packet

- Local replay root: `C:\Users\rabisaab\Downloads\gcp_backtester_review_20260430\qqq_option_native_all_regime_promoted_local`
- Local portfolio report: `C:\Users\rabisaab\Downloads\gcp_backtester_review_20260430\qqq_option_native_all_regime_portfolio_3regime\research_portfolio_report.json`
- Local promotion packet: `C:\Users\rabisaab\Downloads\gcp_backtester_review_20260430\qqq_option_native_all_regime_promotion_packet_3regime\research_promotion_review_packet.json`
- GCS mirror: `gs://codexalpaca-control-us/research_results/qqq_option_native_governed_validation_20260430/`
- Repo manifest: `config/promotion_manifests/qqq_option_native_governed_validation_20260430.yaml`

## Run Profile

- Dataset: QQQ 365d next-trading-day 5x5 dense selected-contract bars.
- Regime labels: `qqq_regime_labels_bearm0p01`.
- Entry timing: `first_common_within_cutoff`.
- Contract selector: `nearest_contract`.
- Entry offset: `330`.
- Exit offset: `390`.
- Entry lag: `15`.
- Exit lag: `15`.
- Fill gate: `0.90`.
- Minimum option trades: `20`.
- Minimum test net PnL: `0`.
- Max strategies per symbol for this research packet: `3`.

## Eligible Candidates

| Regime | Candidate | Family | Fill | Trades | Net PnL | Test PnL |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| bull | `qqq_bull_long_call_atm__first_common_within_cutoff_e330_x390_el15_xl15_nearest_contract` | `long_call` | `0.9506` | `77` | `3886.861` | `142.792` |
| bear | `qqq_bear_call_credit_spread__first_common_within_cutoff_e330_x390_el15_xl15_nearest_contract` | `call_credit_spread` | `0.9130` | `21` | `1416.421` | `494.192` |
| choppy | `qqq_choppy_iron_condor__first_common_within_cutoff_e330_x390_el15_xl15_nearest_contract` | `iron_condor` | `0.9268` | `38` | `162.677` | `294.945` |

## Engineering Changes

- Added parameterized QQQ option-native strategy identities so timing/selector variants do not contaminate each other in promotion reports.
- Added `first_common_within_cutoff` entry timing semantics with separate opportunity coverage, data foundation coverage, entry-bar coverage, and exit-bar coverage.
- Added research-only `entry_liquidity_first_research_only` contract selector support; the promoted three-regime packet uses `nearest_contract`.
- Added bear call-credit-spread templates to cover bearish premium-collection structures.
- Preserved `intended_regime` through replay, portfolio report, and promotion packet.

## Hard Rules

- Do not start trading from this packet.
- Do not change live manifests from this packet.
- Do not change risk policy from this packet.
- A broker-facing paper validation session requires a separate controlled runner PR, explicit operator approval, and a clean closeout path.

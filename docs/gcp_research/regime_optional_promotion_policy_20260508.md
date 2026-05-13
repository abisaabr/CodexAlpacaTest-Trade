# Regime-Optional Promotion Policy Update - 2026-05-08

## Policy Change

Regime completeness is no longer a hard promotion-review blocker. A strategy can move to governed-validation review when its own bull, bear, or choppy sleeve survives the candidate-level gates.

The following gates remain intact:

- `fill_coverage >= 0.90`
- required option trade count and test trade count gates from the portfolio report
- positive full-period and out-of-sample/test PnL gates
- no unresolved promotion blocker on the candidate
- runtime translation support for the paper runner
- existing PAPER-only broker arming and runtime risk guards

Missing bull, bear, or choppy coverage is now reported as an informational research gap, not a hard blocker for otherwise eligible sleeves.

## Code Paths Updated

- `scripts/build_research_portfolio_report.py`
- `scripts/build_research_promotion_review_packet.py`
- `scripts/build_research_wave_portfolio_rollup.py`

The generated packets now include:

- `regime_completeness_policy: informational_only_not_a_hard_promotion_gate`
- `promotion_allowed_regime_complete: true` when at least one candidate is eligible
- `decision: ready_for_governed_validation_review` when candidate-level gates pass, even if only bull, bear, or choppy is present

## Reclassified Candidate Pool

Reclassification source:

- `reports/gcp_research/regime_optional_reclassification_20260508/reclassified_packet_paths.txt`

Combined new-candidate packet:

- `reports/gcp_research/regime_optional_reclassification_20260508/combined_regime_optional_new_candidates_packet/research_promotion_review_packet.json`
- `reports/gcp_research/regime_optional_reclassification_20260508/combined_regime_optional_new_candidates_packet/research_promotion_review_packet.md`

Candidate counts:

- 198 unique eligible candidates found in previously regime-incomplete packets
- 44 were already present in the May 8 PAPER config
- 154 were new relative to the May 8 PAPER config
- 152 translated into runtime-supported governed-validation strategies
- 2 were skipped because current choppy runtime semantics support lower-band call reversion only, not choppy put verticals

New translated strategy split:

- Symbols: AMD 20, AVGO 11, INTC 1, IWM 32, META 10, MSFT 3, NVDA 2, PLTR 1, QQQ 25, SPY 41, TSM 4, XOM 4
- Regimes: bear 44, bull 37, choppy 73
- Families: single-leg repair 124, debit call vertical 13, debit put vertical 6, broken-wing put butterfly 6, bull-put credit spread 3, bear-call credit spread 2

Skipped candidates:

- `portfolio12h__iwm__choppy__put__debit_put_vertical__7db7ff3f2b946c__profile_iwm-greek-c865-912-iwm-e30-x120-entry-delta-target-research-only`
- `portfolio12h__iwm__choppy__put__debit_put_vertical__4e5f58b5faa1c3__profile_iwm-greek-c865-912-iwm-e10-x60-entry-delta-target-research-only`

## Runtime Manifest

New governed-validation manifest:

- `config/promotion_manifests/regime_optional_governed_validation_20260508.yaml`

May 8 PAPER config updated:

- `config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`

Loaded runtime config after update:

- 344 total runtime strategies
- 15 symbols: QQQ, SPY, IWM, AMD, AMZN, MSFT, TSLA, AVGO, GOOGL, TSM, INTC, META, NVDA, PLTR, XOM
- 125 bear strategies
- 76 bull strategies
- 143 choppy strategies
- 297 single-leg translated strategies
- 47 native multi-leg translated strategies
- no duplicate strategy names
- no duplicate candidate IDs

## Verification

Focused tests:

```text
python -m pytest -q tests\test_build_research_promotion_review_packet.py tests\test_build_research_portfolio_report.py tests\test_build_research_wave_portfolio_rollup.py tests\test_build_governed_validation_manifest_from_packets.py
```

Result:

```text
16 passed
```

Config load check:

```text
from alpaca_lab.multi_ticker_portfolio.config import load_portfolio_config
cfg = load_portfolio_config('config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml')
```

Result:

```text
strategy_count=344, manifest_count=6
```

## Operational Notes

This change does not modify the live manifest and does not start trading by itself.

PAPER order submission remains gated by the explicit May 8 paper config and the normal startup checks:

- broker endpoint must be PAPER
- no duplicate broker-facing process
- no unexpected open PAPER orders or positions
- ownership lease valid
- fresh stock and option data preflight passes
- EOD flatten guards remain active

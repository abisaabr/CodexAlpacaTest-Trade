# Overnight scalping status - 2026-05-07 22:30 ET

## Scope

PAPER-only overnight research and May 8 readiness status. No live trading was started. No live manifest was changed. No promotion gate was lowered.

## Current Compute

GCP is fully loaded with research workers:

| Wave | Worker group | Count | Machine type | Purpose |
| --- | --- | ---: | --- | --- |
| `microstructure_rare_event_overnight_20260507T2030ET` | `micro_event_c89089_89600` through `micro_event_c90625_91136` | 4 | `e2-standard-4` | websocket/quote-based option-momentum microstructure replay |
| `traditional_20ticker_realtime_compatible_overnight_20260507T2045ET` | `AMD, AVGO, IWM, MSFT` candidates `37-72` | 4 | `e2-standard-2` | second tranche for first-slice lead-bearing symbols |
| `traditional_20ticker_realtime_compatible_overnight_20260507T2045ET` | `QQQ, SPY, XOM, TSM` candidates `37-72` | 4 | `e2-standard-2` | second tranche for first-slice lead-bearing or governed-review symbols |

Total active expected footprint: 32 vCPUs.

## Greek Scalping First Tranche

Wave:

`gs://codexalpaca-control-us/research_results/greek_scalping_20ticker_overnight_20260507T2215ET/`

Completed workers:

| Worker | Packet decision | Review candidates |
| --- | --- | ---: |
| `amd_greek_c001_028` | `research_only_blocked` | 0 |
| `avgo_greek_c001_028` | `research_only_blocked` | 0 |
| `tsm_greek_c001_028` | `research_only_blocked` | 0 |
| `xom_greek_c001_028` | `research_only_blocked` | 0 |

Dominant blockers were fill coverage below `0.90`, non-positive full-period PnL, and non-positive test PnL.

Decision: do not continue this exact Greek tranche pattern until the traditional lead-bearing tranche results are reviewed. The repo supports Greek delta targeting, condors, butterflies, verticals, and credit spreads, but this first tranche did not produce paper-ready evidence.

## Traditional Realtime-Compatible Status

First tranche status remains:

| State | Count |
| --- | ---: |
| `ready_for_governed_validation_review` | 1 |
| `research_only_blocked_regime_incomplete` | 8 |
| `research_only_blocked` | 11 |

TSM remains the only new regime-complete governed-validation packet so far.

The May 8 PAPER config currently stages:

| Manifest | Strategies |
| --- | ---: |
| `promotion_manifests/multi_symbol_governed_validation_20260507.yaml` | 160 |
| `promotion_manifests/tsm_overnight_governed_validation_20260508.yaml` | 7 |

Total staged PAPER manifest strategies: 167.

## Microstructure Status

Latest synced progress for active microstructure workers:

| Worker | Completed / Total | Review-like candidates |
| --- | ---: | ---: |
| `micro_event_c89089_89600` | 175 / 512 | 0 |
| `micro_event_c89601_90112` | 325 / 512 | 0 |
| `micro_event_c90113_90624` | 150 / 512 | 0 |
| `micro_event_c90625_91136` | 175 / 512 | 0 |

No microstructure candidate is currently eligible for governed promotion or paper activation.

## May 8 Risk Notes

The May 7 session ended account-negative, and the daily strategy scoreboard is not yet clean enough for promotion/demotion decisions. The May 8 preflight must verify:

1. PAPER endpoint only.
2. No duplicate paper trader.
3. No unexpected PAPER open orders or positions.
4. May 8 ownership lease and state/run paths.
5. EOD flatten at 10 and 2 minutes before close.
6. Broker equity meets `broker_min_equity_to_trade`, or any exception is explicitly reviewed before order submission.

## Next Actions

1. Sync and aggregate the active microstructure tranche when workers terminate.
2. Sync and inspect traditional candidate `37-72` packets when workers terminate.
3. Build additional governed manifests only from generated packets that are `eligible_for_promotion_review`.
4. If capacity frees and no new traditional packet is eligible, continue traditional candidate `73-108` for the highest-value lead-bearing symbols before launching more Greek variants.

# Overnight research status - 2026-05-07 21:48 ET

## Scope

This is a PAPER-only research and readiness status packet for the May 8, 2026 RTH preparation loop.

No live trading was started. No live manifest was changed. No fill, PnL, trade-count, loser-cluster, or portfolio-context gate was lowered.

## Traditional realtime-compatible sweep

Wave:

`gs://codexalpaca-control-us/research_results/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/`

The first 36-candidate realtime-compatible regime-rescue slice has now completed for all 20 target tickers:

`AAPL, AMD, AMZN, INTC, IWM, META, MSFT, NVDA, SPY, TSLA, AVGO, GOOGL, MU, NFLX, ORCL, PLTR, QQQ, TSM, XLE, XOM`

Completed worker artifacts were synced locally under:

`reports/gcp_research/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/workers/`

Result summary:

| Symbol | Worker | Packet decision | Review candidates |
| --- | --- | --- | ---: |
| AAPL | aapl_regime_rescue_c001_036 | research_only_blocked | 0 |
| AMD | amd_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 7 |
| AMZN | amzn_regime_rescue_c001_036 | research_only_blocked | 0 |
| AVGO | avgo_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 5 |
| GOOGL | googl_regime_rescue_c001_036 | research_only_blocked | 0 |
| INTC | intc_regime_rescue_c001_036 | research_only_blocked | 0 |
| IWM | iwm_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 3 |
| META | meta_regime_rescue_c001_036 | research_only_blocked | 0 |
| MSFT | msft_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 1 |
| MU | mu_regime_rescue_c001_036 | research_only_blocked | 0 |
| NFLX | nflx_regime_rescue_c001_036 | research_only_blocked | 0 |
| NVDA | nvda_regime_rescue_c001_036 | research_only_blocked | 0 |
| ORCL | orcl_regime_rescue_c001_036 | research_only_blocked | 0 |
| PLTR | pltr_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 1 |
| QQQ | qqq_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 4 |
| SPY | spy_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 6 |
| TSLA | tsla_regime_rescue_c001_036 | research_only_blocked | 0 |
| TSM | tsm_regime_rescue_c001_036 | ready_for_governed_validation_review | 7 |
| XLE | xle_regime_rescue_c001_036 | research_only_blocked | 0 |
| XOM | xom_regime_rescue_c001_036 | research_only_blocked_regime_incomplete | 4 |

### New governed-validation candidate set

TSM is the only completed ticker slice that currently has a packet decision of `ready_for_governed_validation_review`.

Packet:

`reports/gcp_research/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/workers/tsm_regime_rescue_c001_036/promotion_packet/tsm_regime_rescue_c001_036_promotion_packet/research_promotion_review_packet.json`

Gate summary from the packet:

| Field | Value |
| --- | --- |
| candidate_count | 108 |
| eligible_for_promotion_review_count | 11 |
| unique_eligible_base_candidate_count | 7 |
| required_regimes | bull, bear, choppy |
| eligible_regimes | bull, bear, choppy |
| missing_eligible_regimes | none |
| fill_coverage_gate | 0.90 |
| min_option_trades | 20 |
| min_test_net_pnl | 0 |

Review candidate families:

| Regime | Family | Count |
| --- | --- | ---: |
| bull | single_leg_repair | 3 |
| bear | single_leg_repair | 3 |
| choppy | debit_call_vertical | 1 |

The choppy candidate cleared the fill gate exactly at `0.9000`, so it should be treated as eligible for governed validation review but still fragile for paper activation until runtime compatibility and portfolio-context review are explicitly accepted.

## Microstructure rare-event sweep

Wave:

`gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/`

Latest aggregate:

`gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/aggregate_partial/aggregate_partial_20260507T214746/`

Aggregate status:

| Field | Value |
| --- | ---: |
| grid_result_count | 10752 |
| source_summary_file_count | 21 |
| eligible_for_microstructure_review_count | 0 |
| review_candidates | 0 |

Dominant blockers:

| Blocker | Count |
| --- | ---: |
| avg_net_pnl_not_positive | 10752 |
| net_pnl_not_positive | 10752 |
| fill_coverage_below_gate | 10416 |
| trade_count_below_gate | 10716 |

Decision remains `research_only_blocked`. No microstructure strategy is eligible for governed promotion or paper activation from this aggregate.

The next option-momentum tranche was launched after deleting completed worker VMs:

| Worker | Grid range | Zone | Status at launch |
| --- | --- | --- | --- |
| micro_event_c89089_89600 | 89089-89600 | us-central1-a | RUNNING |
| micro_event_c89601_90112 | 89601-90112 | us-west1-a | RUNNING |
| micro_event_c90113_90624 | 90113-90624 | us-east4-a | RUNNING |
| micro_event_c90625_91136 | 90625-91136 | us-east1-b | RUNNING |

## Cleanup and safety

Completed research VMs were deleted after artifacts were present locally and in GCS.

No local Python `run_multi_ticker_portfolio_paper_trader.py` process was detected during this check. No duplicate broker-facing paper process was detected.

## Next actions

1. Continue the microstructure grid from `91137` after workers `89089-91136` complete.
2. Keep TSM as a governed-validation review candidate, not automatic paper activation.
3. Before May 8 RTH, run the fresh PAPER preflight, verify no unexpected PAPER orders or positions, verify ownership lease and EOD flatten guards, then start order submission only if the preflight is clean.

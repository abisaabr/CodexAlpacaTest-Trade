# Overnight scalping expansion plan - 2026-05-07

## Scope

This is a PAPER-only overnight research, promotion-review, and May 8 RTH readiness plan.

Do not start live trading. Do not mutate the checked-in live manifest. Do not lower the governed gates. PAPER activation can use generated promotion manifests and an explicit operator-approved PAPER config only after a fresh May 8 preflight passes.

## Objectives

1. Expand beyond the existing microstructure rare-event sweep into traditional scalping families:
   - stock-price based momentum/range scalps
   - option-premium target/stop scalps
   - Greek delta-targeted directional and choppy premium structures
   - condors, butterflies, verticals, and credit spreads
2. Keep the current microstructure grid moving for true websocket/quote-derived candidates.
3. Promote all generated `eligible_for_promotion_review` candidates into PAPER-governed manifests without editing the live manifest.
4. Preserve May 7 negative-session postmortem lessons before May 8 order submission.

## Promotion and Activation Rules

Governed promotion-review candidates must clear the generated packet gates:

| Gate | Requirement |
| --- | --- |
| Strategy fill coverage | `>= 0.90` |
| Option trade count | `>= 20` |
| Full-period net PnL | `> 0` |
| Test/OOS net PnL | `> 0` |
| Regime coverage | bull, bear, and choppy where packet requires regime completeness |
| Runtime lineage | source commit, dataset paths, selector profile, lag profile, and packet path retained |

Promotion here means governed-validation or operator-approved PAPER experiment. It does not mean live activation.

## Current Active Compute

### Microstructure rare-event wave

Wave:

`gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/`

Active tranche:

| Worker | Grid range | Role |
| --- | --- | --- |
| `micro_event_c89089_89600` | 89089-89600 | option-momentum microstructure replay |
| `micro_event_c89601_90112` | 89601-90112 | option-momentum microstructure replay |
| `micro_event_c90113_90624` | 90113-90624 | option-momentum microstructure replay |
| `micro_event_c90625_91136` | 90625-91136 | option-momentum microstructure replay |

Latest aggregate before this tranche:

`gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/aggregate_partial/aggregate_partial_20260507T214746/`

Status: `research_only_blocked`, `10752` grid rows, `0` review candidates.

### Traditional realtime-compatible wave

Wave:

`gs://codexalpaca-control-us/research_results/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/`

First slice complete across 20 tickers. TSM produced the first new regime-complete packet:

`reports/gcp_research/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/workers/tsm_regime_rescue_c001_036/promotion_packet/tsm_regime_rescue_c001_036_promotion_packet/research_promotion_review_packet.json`

New manifest from that packet:

`config/promotion_manifests/tsm_overnight_governed_validation_20260508.yaml`

Second tranche launched on lead-bearing tickers:

| Symbol | Candidate range | Worker suffix |
| --- | --- | --- |
| AMD | 37-72 | `20260507trad7` |
| AVGO | 37-72 | `20260507trad7` |
| IWM | 37-72 | `20260507trad7` |
| MSFT | 37-72 | `20260507trad7` |

### Greek scalping wave

Wave:

`gs://codexalpaca-control-us/research_results/greek_scalping_20ticker_overnight_20260507T2215ET/`

Purpose: test Greek delta-targeted scalping and choppy premium/condor/butterfly structures against the 365d dense datasets.

First tranche launched:

| Symbol | Candidate range | Selector |
| --- | --- | --- |
| TSM | 1-28 | `entry_delta_target_research_only` |
| AMD | 1-28 | `entry_delta_target_research_only` |
| AVGO | 1-28 | `entry_delta_target_research_only` |
| XOM | 1-28 | `entry_delta_target_research_only` |

Launcher repair:

`scripts/launch_gcp_greek_strategy_shards.ps1` now maps next10 symbols (`AVGO, GOOGL, MU, NFLX, ORCL, PLTR, QQQ, TSM, XLE, XOM`) to `option_fill_ladder_next10_20260429`, matching the dense data root used by the other launchers.

## Strategy Coverage

Already supported by the current repo:

| Strategy class | Current implementation |
| --- | --- |
| Stock momentum scalps | `build_regime_rescue_research_inputs.py` via breakout/volume/trend windows |
| Stock range/choppy scalps | `build_regime_rescue_research_inputs.py` choppy/range windows |
| Option premium scalps | `run_option_aware_research_backtest.py` premium target/stop exits |
| Greek delta-targeted scalps | `build_greek_strategy_research_inputs.py` plus `entry_delta_target_research_only` |
| Vertical spreads | debit call/put verticals and credit call/put spreads |
| Condors/butterflies | iron condor, iron butterfly, broken-wing call/put butterflies |
| Spread/quote microstructure | `build_microstructure_research_grid.py` and `run_microstructure_event_replay_shard.py` |

Not yet true implementations:

| Strategy class | Current status |
| --- | --- |
| Ratio spreads | Broken-wing butterflies include ratio-like 1:-2:1 geometry, but standalone call/put ratio spreads are not implemented as replay families. |
| Dynamic gamma scalping | The repo has gamma-edge entries, but no stock hedge overlay, re-hedge loop, or gamma PnL attribution. |
| Full volatility scalping | Greeks/IV exist, but there is no robust IV-rank vs realized-volatility or surface/skew model yet. |
| Multi-leg order-book scalping | Backtests use minute bars per leg; websocket microstructure currently replays single-option quote events. |

## May 7 Negative Session Controls

Postmortem:

`docs/gcp_research/may7_negative_session_postmortem_20260507.md`

Hard lessons to preserve:

1. Do not trust May 7 per-strategy PnL alone. Broker fills matched local order IDs for only `22/30` fills; inferred stale-exit allocations remain audit-only.
2. The account finished flat, but session net PnL was `-1610.43`.
3. EOD stale-local-trade repair is mandatory. Broker-flat trades must be reconciled instead of sending invalid `sell_to_close` orders.
4. Daily strategy scoreboards need direct broker-fill integration before they are used for promotion/demotion.

## May 8 PAPER Config

Prepared config:

`config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`

Manifest inputs:

| Manifest | Strategy count |
| --- | ---: |
| `promotion_manifests/multi_symbol_governed_validation_20260507.yaml` | 160 |
| `promotion_manifests/tsm_overnight_governed_validation_20260508.yaml` | 7 |

The config keeps the same risk gates as May 7, including `max_open_positions: 999`, per-symbol/per-regime caps, open-risk gates, EOD flatten at 10 and 2 minutes before close, and PAPER-only explicit arming.

Important: `broker_min_equity_to_trade` remains `26000`. If the PAPER account has not been reset or funded above that threshold after the May 7 drawdown, startup preflight should block trading. That should be treated as a risk gate, not a bug.

## May 8 Startup Checklist

Before order submission:

1. Confirm no duplicate `run_multi_ticker_portfolio_paper_trader.py` process.
2. Confirm Alpaca endpoint is PAPER.
3. Confirm no unexpected PAPER open orders or positions.
4. Run startup preflight after fresh SIP bars are available:

```powershell
$env:MULTI_TICKER_MACHINE_LABEL='local-primary-paper-20260508'
$env:MULTI_TICKER_OWNERSHIP_LEASE_PATH='D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260508_ownership_lease.json'
python scripts\run_multi_ticker_portfolio_paper_trader.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml `
  --startup-preflight `
  --no-submit-paper-orders
```

Only if preflight passes:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml `
  --submit-paper-orders
```

## Overnight Continuation Rules

1. When a worker completes, sync artifacts from GCS before deleting the VM.
2. Aggregate every completed tranche with strict gates.
3. If a generated promotion-review packet says `ready_for_governed_validation_review`, build a new promotion manifest and add it to the May 8 PAPER config only if runtime compatibility is documented.
4. If microstructure remains `research_only_blocked`, do not put micro scalping into PAPER as governed promotion. Use only a clearly labeled operator-approved experiment manifest if the operator explicitly accepts short-shadow evidence.
5. Continue traditional lead-bearing tickers first, then broaden to remaining symbols after capacity frees.

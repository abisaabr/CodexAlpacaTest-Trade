# Ticker365 Timing Rescue Wave - 2026-05-03

## Purpose

Run the next research-only repair pass after the `ticker365_fill_repair_20260503T1330Z`
aggregate completed with `0` eligible promotion candidates.

The previous aggregate showed strong raw selected-contract data foundation, but
strategy-level fill coverage remained below the `0.90` promotion gate. This wave
therefore repairs replay timing semantics and tests wider, bounded entry/exit
windows instead of downloading more raw option bars first.

## Scope

- Wave: `ticker365_timing_rescue_20260503T1906Z`
- GCS prefix: `gs://codexalpaca-control-us/research_results/ticker365_timing_rescue_20260503T1906Z/`
- Watchdog: `scripts/run_ticker365_timing_rescue_watchdog.ps1`
- Readiness monitor: `scripts/run_ticker365_timing_rescue_readiness_watchdog.ps1`
- Instance suffix: `20260503c`
- Target universe: all 20 tickers from the 365-day all-available launch rows
- Top N per ticker: `40`
- Selectors: `nearest_contract,entry_liquidity_first_research_only`
- Lag profiles: `30:120,60:180,120:240,180:390`

## Code Repairs Included

- Preserve `intended_regime` from queue/variant metadata, with fallback parsing
  from strategy and candidate IDs.
- Preserve regime labels through the portfolio report so growth projection does
  not misclassify obvious `bull`, `bear`, or `choppy` strategies as `unknown`.
- Keep entry fills causal by requiring a bar at or after the entry signal.
- Allow exits to use the first bar after the exit timestamp, or the latest bar
  before the exit timestamp within the same lag tolerance when no forward bar is
  present. This repairs end-of-day and exit-policy timestamp mismatch without
  lowering the fill gate.

## Safety Contract

- Does not start trading.
- Does not submit paper orders.
- Does not modify live manifests.
- Does not change risk policy.
- Does not lower `fill_coverage >= 0.90`.
- Promotion means governed validation review only, and only if the generated
  promotion packet says `eligible_for_promotion_review`.

## Expected Decision

If this wave finds eligible candidates, build a no-order paper launch handoff for
operator review. If it still has zero eligible candidates, classify blockers by
symbol/family and run a narrower strategy-design wave rather than widening the
execution tolerance again.

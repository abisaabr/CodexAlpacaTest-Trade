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
- Launch preference: try `us-central1-a` and `us-east1-b` before the
  launch-row zone because east4/west1 quota was failing during kickoff.

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

## Automation State - 2026-05-03 19:45 ET

- `160/160` ticker timing-rescue candidate summaries are present in GCS.
- All 20 ticker workers completed successfully, including `QQQ`.
- The completed current-wave worker VMs with suffix `20260503c` were deleted
  after GCS artifact confirmation to free regional instance quota for the
  aggregate builder.
- Aggregate VM `ticker365-repair-agg-20260503c` is running in `us-central1-a`.
- Active local automations:
  - `CodexAlpacaTicker365TimingRescueWatchdog`, every 15 minutes.
  - `CodexAlpacaTicker365TimingRescueReadinessWatchdog`, every 15 minutes.
- Both active wrappers use local lock files under `logs/` so scheduled runs do
  not overlap.
- Stale older-wave local automations were disabled to avoid duplicate launch
  loops and confusing readiness reports:
  - `CodexAlpacaTicker365Watchdog`
  - `CodexAlpacaTicker365FillRepairWatchdog`
  - `CodexAlpacaTicker365FillRepairReadinessWatchdog`
  - `CodexAlpacaTicker365PaperReadinessWatchdog`

## Recovery Behavior

- `scripts/watch_ticker365_fill_repair_wave.py` can now try aggregate fallback
  zones if the primary aggregate zone is quota-blocked.
- The timing-rescue watchdog wrapper passes
  `--delete-completed-worker-instances`, which only deletes completed, stopped
  worker VMs for the exact active wave suffix after per-worker GCS artifacts are
  confirmed.
- The cleanup intentionally does not touch paper-runner infrastructure, live
  manifests, risk policy, or any non-matching VM.

## Next Gate

Wait for the aggregate VM to publish:

- `aggregate/portfolio_report/ticker_365d_all_available_portfolio_report/research_portfolio_report.json`
- `aggregate/promotion_packet/ticker_365d_all_available_promotion_packet/research_promotion_review_packet.json`
- `aggregate/growth_projection/ticker_365d_all_available_growth_projection/portfolio_growth_projection.json`

If the generated promotion packet has eligible candidates and the growth
projection is institutionally acceptable, stage a no-order paper-runner handoff
for operator review. If not, run a targeted strategy-design wave by regime and
symbol; do not lower the `fill_coverage >= 0.90` gate.

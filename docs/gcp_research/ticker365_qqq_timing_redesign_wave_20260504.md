# QQQ Timing Redesign Wave - 2026-05-04

## Purpose

Fix QQQ first before expanding to other tickers. The completed entry-as-of rescue wave showed QQQ raw data foundation near `1.0`, but best strategy fill coverage only around `0.538`. The QQQ heatmap points to entry timing as the dominant blocker, not missing raw option bars.

## Important Fix

The prior portfolio input queue included `timing_profile` values (`fast`, `base`, `patient`, `slow`), but the stock-proxy strategy path used default timing unless explicit `hard_exit_minute`, `stop_loss_multiple`, or `profit_target_multiple` values were present. This made several timing variants behave identically.

This wave patches timing-profile semantics so:

- `fast` maps to shorter exits and tighter liquidity assumptions.
- `base` maps to a mid-session timing profile.
- `patient` maps to the prior default profile.
- `slow` maps to a later/longer profile.

## 2026-05-04 Session-Filter Repair

The first strict rerun still showed low QQQ fill coverage even though raw selected-contract data coverage was strong. The diagnosis was that the replay denominator still included stock-proxy trades that occurred outside regular option tradability, while QQQ option bars are regular-session bars. This made strategy fill coverage look like a raw data failure when it was really a source-timing/denominator mismatch.

The active QQQ rerun now uses:

- `--stock-session-filter option_rth_same_day`
- `--entry-bar-lookup-mode first_bar_at_or_after_entry_within_lag`
- `--exit-bar-lookup-mode first_bar_at_or_after_exit_within_lag`
- `fill_coverage >= 0.90` unchanged

The five stale pre-filter QQQ profile-shard VMs were deleted and relaunched from the current source on 2026-05-04. Fresh VM serial logs confirmed `stock_session_filter=option_rth_same_day` and loaded the expected dense QQQ foundation: `200,994` stock rows, `5,522` selected contracts, and `1,593,974` option-bar rows.

Early first-candidate evidence after relaunch:

- Most strict lag/selector profiles improved to `0.9762` through `0.9841` fill coverage.
- One zero-entry-lag profile remained below gate at `0.8492`, so the full profile-shard run must finish before any promotion decision.
- This is evidence that QQQ's primary blocker was non-option-session source-trade timing in the denominator, not missing raw QQQ option bars.

## 2026-05-04 Candidate-Shard Acceleration

The first post-filter QQQ profile shards proved the semantic fix, but each VM still had to replay all `126` QQQ candidates per selector. At observed speed, that makes QQQ-first iteration unnecessarily slow.

The backtester now supports non-overlapping candidate windows:

- `--candidate-start-index`
- `--candidate-count`

The GCP shard script passes those values through metadata, and the watchdog can use `--expected-summary-count-override` for manually chunked waves. This lets QQQ run as profile plus candidate chunks without changing any research gate.

Canonical chunked QQQ wave:

- Wave ID: `ticker365_qqq_timing_redesign_chunked_20260504T1600Z`
- GCS prefix: `gs://codexalpaca-control-us/research_results/ticker365_qqq_timing_redesign_chunked_20260504T1600Z`
- Candidate chunks: `1-42`, `43-84`, `85-126`
- Lag profiles: `0:60`, `15:120`, `30:180`, `60:240`, `120:390`
- Selectors: `nearest_contract`, `entry_liquidity_first_research_only`
- Expected summary files: `30`
- Launcher: `scripts/run_ticker365_qqq_timing_redesign_candidate_shards.ps1`

This is still research-only. It does not trade, does not alter live manifests, and does not lower the `0.90` fill gate.

## Wave Configuration

- Wave ID: `ticker365_qqq_timing_redesign_20260504T1245Z`
- GCS prefix: `gs://codexalpaca-control-us/research_results/ticker365_qqq_timing_redesign_20260504T1245Z`
- Scope: QQQ only
- Dataset: QQQ dense 365-day next-trading-day ATM +/- 5 selected-contract dataset
- Queue size: `126` QQQ variants
- Lag profiles: `0:60,15:120,30:180,60:240,120:390`
- Selectors: `nearest_contract,entry_liquidity_first_research_only`
- Entry lookup mode: `first_bar_at_or_after_entry_within_lag`
- Exit lookup mode: `first_bar_at_or_after_exit_within_lag`
- Stock session filter: `option_rth_same_day`
- Max entry staleness: `0`
- Fill gate: unchanged at `0.90`

## Hard Rules

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Promotion means governed validation review only until a generated packet says eligible.

## Inputs

- Canonical Git input folder: `docs/gcp_research/qqq_timing_redesign_20260504/inputs`
- Variants: `qqq_timing_redesign_variants.jsonl`
- Queue: `qqq_timing_redesign_option_queue.json`
- Launch rows: `qqq_timing_redesign_launch_rows.json`

## Automation

- Worker/aggregate watchdog: `CodexAlpacaTicker365QQQTimingRedesignWatchdog`
- Readiness watchdog: `CodexAlpacaTicker365QQQTimingRedesignReadinessWatchdog`
- Worker script: `scripts/run_ticker365_qqq_timing_redesign_watchdog.ps1`
- Profile-shard launcher: `scripts/run_ticker365_qqq_timing_redesign_profile_shards.ps1`
- Readiness script: `scripts/run_ticker365_qqq_timing_redesign_readiness_watchdog.ps1`

## QQQ-First Acceleration

The first launch used a single QQQ worker that runs all five lag profiles sequentially. To keep the QQQ fix moving faster without changing promotion gates, the profile-shard launcher can run each strict lag profile on its own worker prefix:

- `ticker365fillrepair_qqq_e0x60`
- `ticker365fillrepair_qqq_e15x120`
- `ticker365fillrepair_qqq_e30x180`
- `ticker365fillrepair_qqq_e60x240`
- `ticker365fillrepair_qqq_e120x390`

Each profile-shard worker still uses both contract selectors and the same dense QQQ 365-day dataset. The aggregate remains blocked until all `10` strict candidate summaries exist, and no strategy may move forward unless the generated promotion packet says `eligible_for_promotion_review`.

The profile-shard launcher now uses `200GB` `pd-standard` boot disks for reruns. The earlier `40GB` `pd-standard` workers staged data successfully but produced no candidate summaries after an extended compute window, consistent with local parquet read/index I/O becoming the bottleneck. A `pd-ssd` rerun was attempted but blocked by project `SSD_TOTAL_GB` quota, so the safe improvement is larger standard disks without changing CPU quota or promotion gates.

## Expected Decision Rule

Only move a QQQ strategy toward governed promotion review if the generated promotion packet says `eligible_for_promotion_review`. If QQQ remains below the fill gate, the next step is deeper source-signal timing redesign, not broader ticker expansion and not gate relaxation.

# Ticker365 Entry As-Of Rescue Wave - 2026-05-03

## Purpose

The completed all-ticker timing rescue aggregate found 6,400 candidates and 0 eligible for governed promotion review. The dominant blocker remained strategy-level fill coverage below the unchanged `0.90` gate, with raw data repair candidates at `0`.

This wave is a research-only diagnostic/rescue pass. It tests whether low strategy fill is caused by option bars appearing shortly before stock signal timestamps rather than at-or-after them. It does not start trading, submit paper orders, change live manifests, change risk policy, or lower promotion gates.

## Hard Rules

- Do not start live or paper trading from this wave.
- Do not edit live manifests from this wave.
- Do not change risk policy from this wave.
- Do not lower `fill_coverage >= 0.90`.
- Treat any eligible result as governed validation review only until the generated promotion packet says `eligible_for_promotion_review`.
- Treat as-of entry pricing as research-only because it may use stale option bars.

## Wave Configuration

- Wave ID: `ticker365_entry_asof_rescue_20260503T2352Z`
- GCS prefix: `gs://codexalpaca-control-us/research_results/ticker365_entry_asof_rescue_20260503T2352Z`
- Instance suffix: `20260503d`
- Universe: all available 365-day ticker datasets in the ticker365 launch rows
- Top N per ticker/profile: `40`
- Lag profiles: `180:390,240:390`
- Selectors: `nearest_contract,entry_liquidity_first_research_only`
- Entry lookup mode: `first_bar_at_or_after_or_asof_entry_within_lag`
- Max entry staleness: `15` minutes
- Fill gate remains: `0.90`

## Automation

- Worker/aggregate watchdog task: `CodexAlpacaTicker365EntryAsofRescueWatchdog`
- Readiness watchdog task: `CodexAlpacaTicker365EntryAsofRescueReadinessWatchdog`
- Worker script: `scripts/run_ticker365_entry_asof_rescue_watchdog.ps1`
- Readiness script: `scripts/run_ticker365_entry_asof_rescue_readiness_watchdog.ps1`

The watchdog launches bounded worker tranches, deletes only completed stopped workers for this exact wave after GCS artifact confirmation, and launches the aggregate when expected shard artifacts exist.

## Expected Artifacts

- Watch status: `reports/gcp_research/ticker365_fill_repair_20260503/ticker365_fill_repair_watch_status.json`
- Worker artifacts: `gs://codexalpaca-control-us/research_results/ticker365_entry_asof_rescue_20260503T2352Z/workers/`
- Aggregate artifacts: `gs://codexalpaca-control-us/research_results/ticker365_entry_asof_rescue_20260503T2352Z/aggregate/`
- Paper readiness report: `reports/gcp_research/ticker365_entry_asof_rescue_paper_readiness_20260503/`

## Takeover Commands

```powershell
$env:CLOUDSDK_PYTHON="C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\rabisaab\Downloads\codexalpaca-7bcb9ac9a02d.json"
$env:GOOGLE_CLOUD_PROJECT="codexalpaca"
.\scripts\run_ticker365_entry_asof_rescue_watchdog.ps1
.\scripts\run_ticker365_entry_asof_rescue_readiness_watchdog.ps1
```

## Current Interpretation

If this wave materially improves fill coverage but remains economically poor, the next engineering step is strategy timing redesign and loser taxonomy, not data repair. If it improves both fill and economics, the next step is a stricter no-stale-entry replay before any governed paper-readiness handoff.

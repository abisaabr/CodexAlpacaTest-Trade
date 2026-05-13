# GCP Running Instance Cleanup - 2026-05-03

## Purpose

Audit live Google Cloud VMs and keep only infrastructure required for the current plan:

- Finish the all-ticker aggregate.
- Preserve logs/artifacts in GCS.
- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.

## Audit Snapshot

- Audit time: `2026-05-03T08:51:01-04:00`
- Project: `codexalpaca`
- Active plan phase: all-ticker aggregate after `40/40` ticker summaries landed.
- Required active VM: `ticker365-agg-20260501-2300z`

## Keep Running

| Instance | Zone | Reason |
| --- | --- | --- |
| `ticker365-agg-20260501-2300z` | `us-central1-a` | Active all-ticker aggregate VM. Required to publish `aggregate/projection_calendar`, `aggregate/portfolio_report`, `aggregate/promotion_packet`, and `aggregate/growth_projection`. |

## Stop

| Instance | Zone | Reason |
| --- | --- | --- |
| `portfolio-overnight-12h-20260501-fastlane-top40-a` | `us-central1-a` | Stale older overnight tournament worker, superseded by the completed all-ticker shard wave. |
| `portfolio-overnight-12h-20260501-fastlane-top40-b` | `us-central1-a` | Stale older overnight tournament worker, superseded by the completed all-ticker shard wave. |
| `portfolio-overnight-12h-20260501-fastlane-top40-c` | `us-central1-a` | Stale older overnight tournament worker, superseded by the completed all-ticker shard wave. |
| `portfolio-overnight-12h-20260501-fastlane-top40-d` | `us-east1-b` | Stale older overnight tournament worker, superseded by the completed all-ticker shard wave. |
| `portfolio-overnight-12h-20260501-option-aware-core-c` | `us-central1-a` | Stale older overnight tournament worker, superseded by the completed all-ticker shard wave. |
| `portfolio-overnight-12h-20260501-option-aware-core-d` | `us-central1-a` | Stale older overnight tournament worker, superseded by the completed all-ticker shard wave. |
| `qqq-365d-canonical-20260501-2250z` | `us-central1-a` | Recognized QQQ canonical research VM, but its Phase 1 full-year projection artifacts are already published and it is no longer required. |
| `vm-execution-paper-01` | `us-east1-b` | Recognized validation-only paper execution VM. Paper handoff remains blocked until aggregate promotion review says eligible, so it does not need to run right now. |

## Safety Notes

- These stops are infrastructure hygiene only.
- No trading process was started.
- No live manifest was changed.
- No risk policy was changed.
- Stopped VMs can be restarted later if a governed handoff says they are needed.

## Post-Cleanup Confirmation

- Confirmed running after cleanup: `ticker365-agg-20260501-2300z` only.
- GCS mirror: `gs://codexalpaca-control-us/ops/gcp_runtime_cleanup/gcp_running_instance_cleanup_20260503.md`
- Aggregate artifacts still pending at cleanup time; only aggregate status files had landed.

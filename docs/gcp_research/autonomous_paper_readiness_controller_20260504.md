# Autonomous Paper Readiness Controller

Status: GCP-resident research automation prepared.

This controller is designed to keep the QQQ research-to-paper-readiness plan moving from Google Cloud without requiring the local Windows scheduled tasks to stay alive.

## Hard Rules

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Diagnostic timing profiles cannot be promoted.
- Promotion means governed validation review until a packet explicitly says eligible.
- Runtime arming and paper launch require explicit operator approval.

## What The Controller Can Do

- Sync the current repo source into GCS for worker VMs.
- Monitor QQQ micro fill-squash summaries.
- Relaunch missing or stalled QQQ micro shards.
- Build and mirror the QQQ fill-squash heatmap.
- Select only a legitimate strict, option-session-filtered profile for full expansion.
- Launch the optimized full `126` QQQ expansion using the winning selector by default.
- Launch the aggregate promotion-review VM once full expansion completes.
- Publish JSON and Markdown status to GCS every pass.
- Delete only completed stopped research worker VMs when their GCS artifacts are confirmed.

## What The Controller Cannot Do

- It cannot start the paper trader.
- It cannot place orders.
- It cannot arm runtime execution.
- It cannot modify live manifests.
- It cannot change risk policy.
- It cannot promote diagnostic profiles.
- It cannot lower gates to force a promotion.

## GCS Status

- Controller root: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/`
- Status JSON: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/autonomous_paper_readiness_status.json`
- Status Markdown: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/autonomous_paper_readiness_status.md`

## Launch Command

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File scripts\launch_gcp_autonomous_paper_readiness_controller.ps1
```

The launch script uploads a source archive and creates a small GCP control VM. The VM runs:

```bash
python -u scripts/gcp_autonomous_paper_readiness_controller.py \
  --gcloud gcloud \
  --loop \
  --sleep-seconds 900 \
  --max-launches-per-pass 16 \
  --allow-delete-terminated
```

## State Machine

1. `micro_fill_squash`: complete QQQ micro wave to `72/72`.
2. `full_qqq_expansion`: expand the selected strict profile over all `126` QQQ candidates.
3. `aggregate_promotion_review`: build portfolio report, promotion packet, and growth projection.
4. `research_promotion_candidates_available`: stop at independent reproduction and runner-preflight handoff.
5. `research_blocked_or_redesign_needed`: document blockers and require a redesign wave.

## North Star Alignment

This automation optimizes for a paper trader that is ready because it survived evidence, lineage, fill coverage, economics, promotion review, and runner preflight. It explicitly avoids optimizing for the fastest route to placing orders.

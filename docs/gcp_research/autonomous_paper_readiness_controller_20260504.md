# Autonomous Paper Readiness Controller

Status: GCP-resident research automation launched.

This controller is designed to keep the QQQ research-to-paper-readiness plan moving from Google Cloud without requiring the local Windows scheduled tasks to stay alive.

Launched controller VM:

- Instance: `paper-ready-controller-20260504qa`
- Zone: `us-central1-a`
- Machine type: `e2-standard-2`
- Branch: `codex/phase2-fill-semantics-20260430`
- Launch commit: `25b4a2b`

Local Windows QQQ fill-squash scheduled tasks were disabled after the GCP controller published its first status artifact.

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
- Restart incomplete research worker VMs that exceed the stale-worker age threshold.
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
  --max-launches-per-pass 16 \
  --allow-delete-terminated \
  --stale-worker-max-age-minutes 240
```

The VM bootstrap runs this as a one-pass controller, then sleeps for `900` seconds, pulls the latest branch, and runs another pass. That keeps the controller patchable from GitHub/GCS instead of trapping it inside a stale long-lived Python process.

## State Machine

1. `micro_fill_squash`: complete QQQ micro wave to `72/72`.
2. `full_qqq_expansion`: expand the selected strict profile over all `126` QQQ candidates.
3. `aggregate_promotion_review`: build portfolio report, promotion packet, and growth projection.
4. `research_promotion_candidates_available`: stop at independent reproduction and runner-preflight handoff.
5. `research_blocked_or_redesign_needed`: document blockers and require a redesign wave.

## North Star Alignment

This automation optimizes for a paper trader that is ready because it survived evidence, lineage, fill coverage, economics, promotion review, and runner preflight. It explicitly avoids optimizing for the fastest route to placing orders.

## 2026-05-04 23:20 UTC Update

Controller repair and full-expansion launch are now complete enough for unattended continuation:

- Repo commit `fb0f618` adds the missing `tabulate` dependency required by `pandas.to_markdown()` during heatmap generation and repairs the stale loader unit test.
- Local validation after the repair: `210 passed, 1 warning`.
- The broken controller VM was deleted and recreated as `paper-ready-controller-20260504qa`; the refreshed VM installed `tabulate` and is running branch `codex/phase2-fill-semantics-20260430` at commit `fb0f618`.
- The QQQ micro fill-squash wave reached `72 / 72` summaries.
- The selected legitimate strict profile for the full expansion is `strict-e0x60` with `entry_liquidity_first_research_only`.
- Selection metadata is at `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_full126_20260504T1900Z/selection/qqq_fill_squash_selected_strict_profile.json`.
- The full QQQ expansion is in phase `full_qqq_expansion`; latest observed direct GCS count was `7 / 18` summary files with `13` progress files.
- All `18` full chunks were either completed, running, or launched by the latest controller/local pass.
- `79` old terminated research VMs were deleted to free regional instance-count quota; the deletion list was mirrored to `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/terminated_research_instances_deleted_20260504.csv`.

Operational note: a local controller pass briefly overlapped with the GCP controller, causing duplicate active workers for two chunks. The later duplicate copies were deleted, leaving one active worker per chunk. The remaining controller should be allowed to continue alone.

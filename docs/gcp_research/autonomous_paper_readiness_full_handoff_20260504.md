# Autonomous Paper Readiness Full Handoff

Status: canonical handoff for the other machine.

Generated context date: `2026-05-04`

This handoff captures the current project state, Git history, GCS locations, GCP controller ownership, local task state, logs, and recovery commands for the QQQ research-to-paper-readiness automation.

## North Star

The goal is to reach a paper trader that is ready because it survived evidence, lineage, fill coverage, economics, promotion review, independent reproduction, and runner preflight.

Paper-ready means:

- `research_promoted`
- `runtime_armed`
- `operator_approved`

The autonomous controller may optimize and heal research orchestration. It must not autonomously start trading.

## Hard Rules

- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Diagnostic timing profiles cannot be promoted.
- Promotion means governed validation review until a packet explicitly says eligible.
- Runtime arming and paper launch require explicit operator approval.

## Current Git State

- Repo: `https://github.com/abisaabr/CodexAlpacaTest-Trade.git`
- Branch: `codex/phase2-fill-semantics-20260430`
- Latest commit at handoff: `fa2ea76 Add stale worker healing to autonomous controller`

Recent commits:

- `fa2ea76` Add stale worker healing to autonomous controller
- `309797f` Make autonomous controller self-updating between passes
- `28988ed` Record autonomous controller launch
- `25b4a2b` Add GCP autonomous paper readiness controller
- `aefa65f` Optimize QQQ fill-squash expansion handoff
- `07db9dc` Add QQQ fill-squash expansion controller
- `8a0983a` Stream QQQ fill-squash shard progress
- `367f712` Add QQQ fill-squash micro wave
- `e313a2f` Prevent duplicate QQQ shard launches
- `75b2055` Skip completed QQQ shard reruns
- `79d9305` Avoid nested GCS replay output paths
- `75500d1` Document QQQ chunked watchdog handoff

Other machine bootstrap:

```powershell
git fetch origin
git checkout codex/phase2-fill-semantics-20260430
git pull --ff-only origin codex/phase2-fill-semantics-20260430
```

## Current GCP Ownership

The loop is now GCP-owned. Local Windows scheduled tasks were disabled after the GCP controller published a valid status artifact.

Controller VM:

- Instance: `paper-ready-controller-20260504qa`
- Zone: `us-central1-a`
- Machine type: `e2-standard-2`
- Branch pulled by VM: `codex/phase2-fill-semantics-20260430`
- Confirmed VM bootstrap commit: `309797f`
- Latest branch commit available for next pass: `fa2ea76`

The bootstrap pulls latest GitHub between passes, runs one controller pass, sleeps `900` seconds, then repeats. This lets the controller patch itself through GitHub.

## Local Scheduled Tasks

These local tasks were disabled so they do not race the GCP controller:

- `CodexAlpacaTicker365QQQFillSquashMicroShardWatchdog`
- `CodexAlpacaTicker365QQQFillSquashController`
- `CodexAlpacaTicker365QQQTimingRedesignCandidateShardWatchdog`

The other machine should not re-enable them unless intentionally taking control back from GCP.

## Live Research State

Latest direct GCS count at final handoff verification:

- QQQ micro summaries: `72 / 72`
- QQQ full expansion summaries: `0`
- Active/recent GCP research instances:
- `paper-ready-controller-20260504qa`
- `qqqfs-strict-e0x60-c029-035-20260504qs`
- `qqqfs-strict-e30x180-c036-042-20260504qs`

Last controller-published status artifact may lag direct GCS counts by one controller pass. At last read, the controller artifact reported:

- Phase: `micro_fill_squash`
- Next action: `continue_micro_wave`
- Controller-reported micro summaries: `68 / 72`
- Controller-reported full summaries: `0`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

If the direct GCS count and controller status disagree, trust the direct GCS count for latest progress and wait for the next controller pass to publish the reconciled status. At final handoff verification, direct GCS count had reached `72 / 72`, while the last controller artifact still showed `68 / 72`.

## Canonical GCS Paths

Autonomous controller:

- Root: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/`
- Live status JSON: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/autonomous_paper_readiness_status.json`
- Live status Markdown: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/autonomous_paper_readiness_status.md`
- Controller startup log: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/controller_vm_startup.log`
- Controller launch status: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/controller_launch_status.json`
- Controller handoff doc: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/autonomous_paper_readiness_controller_20260504.md`

Full handoff/log archive:

- Root: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/full_handoff_20260504/`
- Local logs: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/full_handoff_20260504/local_logs/`
- Controller snapshots: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/full_handoff_20260504/controller_snapshots/`
- Handoff docs: `gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/full_handoff_20260504/docs/`

QQQ micro wave:

- Root: `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_micro_20260504T1730Z/`
- Heatmap partial: `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_micro_20260504T1730Z/heatmap_partial/qqq_fill_squash_micro_20260504/`
- Handoff: `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_micro_20260504T1730Z/handoff/qqq_fill_squash_micro_20260504.md`

QQQ full expansion:

- Root: `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_full126_20260504T1900Z/`
- Selection: `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_full126_20260504T1900Z/selection/`
- Aggregate: `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_full126_20260504T1900Z/aggregate/`

QQQ dense dataset:

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/`
- Selected contracts: `gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/`
- Option bars: `gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/underlying=QQQ/`

## Controller Behavior

The GCP autonomous controller:

- Syncs source/input packets to GCS.
- Monitors QQQ micro wave until `72 / 72`.
- Relaunches missing micro shards.
- Restarts incomplete active research workers older than `240` minutes.
- Builds QQQ fill-squash heatmap.
- Selects only strict, option-session-filtered profiles.
- Launches optimized full `126` QQQ expansion.
- Uses winning micro selector by default, not both selectors.
- Launches aggregate promotion-review VM when full expansion completes.
- Publishes JSON and Markdown status to GCS.
- Stops at promotion/reproduction/preflight gates.

It cannot:

- Place paper or live orders.
- Start the paper trader.
- Modify live manifests.
- Change risk policy.
- Lower promotion gates.
- Promote diagnostic profiles.

## How To Check Status

PowerShell:

```powershell
$env:CLOUDSDK_PYTHON='C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe'
$env:GOOGLE_APPLICATION_CREDENTIALS='C:\Users\rabisaab\Downloads\codexalpaca-7bcb9ac9a02d.json'
$env:GOOGLE_CLOUD_PROJECT='codexalpaca'
$g='C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd'

& $g storage cat gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/autonomous_paper_readiness_status.json

(& $g storage ls --recursive gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_micro_20260504T1730Z/**/option_aware_candidate_summary.json | Measure-Object).Count

(& $g storage ls --recursive gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_full126_20260504T1900Z/**/option_aware_candidate_summary.json | Measure-Object).Count

& $g compute instances list --project codexalpaca --filter="name~'paper-ready-controller' OR name~'qqqfs-' OR name~'qqqfull-'" --format="table(name,zone,status,machineType.basename(),creationTimestamp)"
```

Linux/GCP shell:

```bash
gcloud storage cat gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller/autonomous_paper_readiness_status.json
gcloud storage ls --recursive 'gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_micro_20260504T1730Z/**/option_aware_candidate_summary.json' | wc -l
gcloud storage ls --recursive 'gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_full126_20260504T1900Z/**/option_aware_candidate_summary.json' | wc -l
gcloud compute instances list --project codexalpaca --filter="name~'paper-ready-controller' OR name~'qqqfs-' OR name~'qqqfull-'" --format="table(name,zone,status,machineType.basename(),creationTimestamp)"
```

## How To Take Over Safely

Preferred path:

1. Pull the latest branch.
2. Read this handoff.
3. Read the live controller status JSON.
4. Read the QQQ micro heatmap after `72 / 72`.
5. Let the controller launch full expansion unless it reports an error.
6. Do not start paper trading.
7. Do not modify live manifests or risk policy.

If the controller VM must be restarted:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File scripts\launch_gcp_autonomous_paper_readiness_controller.ps1
```

If duplicate controller VMs exist, keep only one `paper-ready-controller-*` instance running.

## Known Open Work

- QQQ micro wave reached `72 / 72` by final direct GCS verification.
- Full QQQ `126` expansion had not started yet at final direct verification.
- No strategy was eligible for paper promotion yet.
- No paper runner was armed.
- Independent reproduction and runner preflight are still downstream gates.

## What To Do When Micro Reaches 72/72

At final handoff verification, the micro count had reached `72 / 72`. On the next controller pass, it should automatically:

1. Build the heatmap.
2. Select the best legitimate strict profile.
3. Write selection JSON to the full expansion GCS prefix.
4. Launch full QQQ shards.

The other machine should verify:

- Selected profile starts with `strict_`.
- `entry_lookup_mode=first_bar_at_or_after_entry_within_lag`.
- `exit_lookup_mode=first_bar_at_or_after_exit_within_lag`.
- `source_session_filter=option_rth_same_day`.
- `selected_expansion_selectors` contains the winning selector only unless an explicit diagnostic rerun was requested.

## What To Do When Full Expansion Completes

The controller should launch aggregate promotion review. The other machine should inspect:

- `aggregate/portfolio_report/`
- `aggregate/promotion_packet/research_promotion_review_packet.json`
- `aggregate/growth_projection/`

No candidate should move forward unless the generated promotion-review packet says `eligible_for_promotion_review`.

## Final Gate

Even if research candidates appear:

- Run independent reproduction.
- Run paper runner preflight.
- Run shadow mode.
- Get explicit operator approval.

Only then should paper execution be armed.

# GCP Paper Runtime Safety Status - 2026-05-05

## Audit Result

Runtime audit completed at `2026-05-05T12:31:25Z`.

- Project: `codexalpaca`
- GCP VM count: `3`
- Running VM count: `0`
- VM count with unsafe broker-facing metadata: `1`

## Instance Decisions

### multi-ticker-trader-v1

- Status: `TERMINATED`
- Zone: `us-central1-a`
- Unsafe metadata hits: `--submit-paper-orders`, `DRY_RUN=false`
- Recommendation: `do_not_start_without_rewriting_startup_metadata`

This is the legacy runner VM. Do not start it blindly for no-order validation,
because its current startup metadata is capable of broker-facing paper order
submission.

### vm-execution-paper-01

- Status: `TERMINATED`
- Zone: `us-east1-b`
- Unsafe metadata hits: `none`
- Known validation-only: `true`
- Recommendation: `ok_monitor_only`

This is the safer target if we need a GCP-hosted validation-only/no-order
preflight lane.

### paper-ready-controller-20260504qa

- Status: `TERMINATED`
- Zone: `us-central1-a`
- Unsafe metadata hits: `none`
- Recommendation: `ok_monitor_only`

This is the earlier autonomous controller VM. Keep it stopped unless explicitly
needed for research/controller work.

## Artifacts

- Local JSON: `reports/gcp_research/gcp_paper_runtime_safety_20260505/gcp_paper_runtime_safety_audit.json`
- Local Markdown: `reports/gcp_research/gcp_paper_runtime_safety_20260505/gcp_paper_runtime_safety_audit.md`
- GCS: `gs://codexalpaca-control-us/research_results/gcp_paper_runtime_safety_20260505/`

## Hard Rule

Do not start `multi-ticker-trader-v1` without first replacing its startup
metadata or container command with a no-order/validation-only command path.

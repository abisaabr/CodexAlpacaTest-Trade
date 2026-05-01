# Portfolio Post-Completion Aggregate Watch - 2026-05-01

## Purpose

Run one additional research-only aggregate/promotion-review packet after more all-ticker GCP worker outputs land.

This watcher does not start trading, submit paper orders, edit live manifests, edit risk policy, or lower the `fill_coverage >= 0.90` gate.

## Watcher VM

- Instance: `portfolio-12h-postagg-watch-20260501-1619z`
- Zone: `us-central1-a`
- Machine type: `e2-standard-2`
- Provisioning model: `STANDARD`
- Service account: `ramzi-service-account@codexalpaca.iam.gserviceaccount.com`
- Startup script: `scripts/gcp_post_completion_aggregate_watch.sh`

## GCS Output Prefix

- Monitor prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_post_completion_20260501T161922Z/monitor/`
- Portfolio report prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_post_completion_20260501T161922Z/portfolio_report/`
- Promotion packet prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_post_completion_20260501T161922Z/promotion_packet/`

## Baseline At Watch Start

- Standard all-ticker summary files: `11`
- Standard unique symbols represented: `6`
- Running all-ticker worker count: `6`
- Running workers were left running.

## Aggregate Trigger

The watcher checks every `900` seconds and triggers the aggregate when one of these happens:

- All all-ticker workers finish.
- At least `4` additional standard summary files land.
- At least `2` additional standard unique symbols land.
- `21600` seconds elapse, producing a bounded snapshot instead of waiting indefinitely.

## Aggregate Gates

- `fill_coverage_gate`: `0.90`
- `min_option_trades`: `20`
- `min_test_net_pnl`: `0`
- `max_positions`: `8`
- `max_strategies_per_symbol`: `2`
- `max_symbol_weight`: `0.20`
- `initial_cash`: `25000`
- `candidate_identity_mode`: `variant_profile`

## Handoff Notes

The existing all-ticker workers should not be stopped for this watcher. The current final aggregate remains a partial cutoff snapshot until this post-completion aggregate lands.

The other machine can monitor progress with:

```powershell
$gcloud='C:\Users\<you>\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud storage cat 'gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_post_completion_20260501T161922Z/monitor/post_completion_aggregate_watch_status.json'
& $gcloud compute instances get-serial-port-output portfolio-12h-postagg-watch-20260501-1619z --zone us-central1-a --port 1
```

Hard rule: no strategy should move beyond governed validation review unless the generated promotion-review packet marks it `eligible_for_promotion_review`.

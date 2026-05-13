# QQQ+SPY Real-Time GCP Canary Status - 2026-05-05

## Result

The GCP-hosted broker-free canary passed using the realtime market-data config:

- Portfolio config: `config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml`
- Stock feed: `sip`
- Option feed: `opra`
- Broker-facing: `false`
- Submit paper orders: `false`
- Startup preflight: `startup_preflight_passed`
- Run-once: `ran_once`
- Broker positions: `0`
- Open orders: `0`
- Buying power observed: `399225.72`
- Broker equity observed: `99806.43`

## GCS Evidence

- Corrected summary: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-canary-noorder-20260505T1410Z/gcp_canary/outputs/canary_summary_corrected.json`
- Preflight JSON: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-canary-noorder-20260505T1410Z/gcp_canary/outputs/qqq_spy_realtime_startup_preflight_no_orders.json`
- Preflight raw log: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-canary-noorder-20260505T1410Z/gcp_canary/outputs/qqq_spy_realtime_startup_preflight_no_orders.raw`
- Run-once JSON: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-canary-noorder-20260505T1410Z/gcp_canary/outputs/qqq_spy_realtime_run_once_no_orders.json`
- Run-once raw log: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-canary-noorder-20260505T1410Z/gcp_canary/outputs/qqq_spy_realtime_run_once_no_orders.raw`

The raw run-once log includes Alpaca REST requests with `feed=sip` for stock
bars and `feed=opra` for option snapshots.

## Cleanup

Temporary one-off canary VMs were deleted after their artifacts were mirrored to
GCS:

- `qqq-spy-noorder-canary-20260505`
- `qqq-spy-realtime-noorder-20260505`

Remaining GCP VMs after cleanup:

- `multi-ticker-trader-v1`: terminated, legacy startup metadata is unsafe to
  start without rewrite
- `paper-ready-controller-20260504qa`: terminated
- `vm-execution-paper-01`: terminated, validation-only

## Launch Meaning

This proves QQQ+SPY governed validation can run in GCP against the paid Alpaca
SIP/OPRA REST data lane without submitting paper orders. It does not by itself
authorize broker-facing paper submission.

Broker-facing paper still requires an explicit operator command with
`--submit-paper-orders`.

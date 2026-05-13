# QQQ SPY Realtime Broker Canary Status - 2026-05-05

## Decision

`broker_facing_canary_complete_no_open_orders_or_positions`

The first operator-approved broker-facing paper canary completed on a fresh one-off GCP VM. It used the committed realtime QQQ/SPY launch packet source at commit `9bea0f2135cf0ff15b873d1bdda13f12a9e18bb3`, required the explicit CLI arming flag `--submit-paper-orders`, and left no open broker state.

This was a bounded canary only. It did not start an unattended full-session trader.

## Run Identity

- Run ID: `qqq-spy-realtime-broker-canary-20260505T142421Z`
- VM: `qqq-spy-paper-canary-20260505t1424`
- Zone: `us-central1-a`
- Source commit: `9bea0f2135cf0ff15b873d1bdda13f12a9e18bb3`
- Portfolio config: `config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml`
- Stock feed: `sip`
- Option feed: `opra`
- Scope: QQQ + SPY, governed bull/bear/choppy strategy set
- VM outcome: self-stopped after evidence upload; deleted after post-run verification

## GCS Evidence

- Source archive: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/input/codexalpaca_repo_qqq-spy-realtime-broker-canary-20260505T142421Z.tar.gz`
- Startup script: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/input/startup_broker_canary.sh`
- Summary: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/gcp_broker_canary/outputs/broker_canary_summary.json`
- Markdown summary: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/gcp_broker_canary/outputs/broker_canary_summary.md`
- Broker state: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/gcp_broker_canary/outputs/broker_state.json`
- Raw broker run stdout: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/gcp_broker_canary/outputs/broker_run_stdout.txt`
- Raw preflight stdout: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/gcp_broker_canary/outputs/preflight_stdout.txt`
- Startup console log: `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-broker-canary-20260505T142421Z/gcp_broker_canary/outputs/startup_console.log`

## Results

```json
{
  "broker_facing": true,
  "submit_paper_orders": true,
  "preflight_exit": 0,
  "preflight_startup_check_status": "passed",
  "preflight_would_allow_trading": true,
  "broker_run_exit": 0,
  "broker_run_status": "ran_once",
  "broker_run_trade_date": "2026-05-05",
  "broker_run_open_trades": 0,
  "broker_run_completed_trades": 0,
  "broker_run_blocked_new_entries": false,
  "post_open_order_count": 0,
  "post_position_count": 0,
  "unsafe_residual_state": false,
  "broker_equity": "99806.43",
  "buying_power": "399225.72",
  "options_buying_power": "99806.43"
}
```

The canary reached Alpaca paper trading with realtime SIP/OPRA data enabled, but no entry signal fired during the single diagnostic cycle. No paper order remained open and no position remained after the run.

## Cleanup

After the VM self-stopped and the uploaded broker-state summary showed zero open orders and zero positions, the one-off VM was deleted. The only remaining Compute Engine instances at cleanup time were the known terminated baseline instances:

- `multi-ticker-trader-v1`
- `paper-ready-controller-20260504qa`
- `vm-execution-paper-01`

## Next Safe Step

The next promotion step is an explicitly armed bounded RTH session for the same QQQ/SPY governed strategy set, with the same SIP/OPRA config and a hard post-session broker-state capture. That session should remain paper-only, preserve the `--submit-paper-orders` operator boundary, and publish its run state to GCS before any strategy expansion.

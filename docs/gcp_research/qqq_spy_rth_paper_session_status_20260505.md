# QQQ SPY RTH Paper Session Status - 2026-05-05

## Current Decision

`rth_paper_session_in_progress`

The operator-approved QQQ/SPY broker-facing paper session is running on a fresh one-off GCP VM. This is the next step after the successful one-cycle broker canary. It is paper-only, bounded, and limited to the governed QQQ/SPY bull, bear, and choppy strategy set.

This is not a broad multi-ticker expansion and it is not live trading.

## Run Identity

- Run ID: `qqq-spy-rth-paper-session-20260505T143315Z`
- VM: `qqq-spy-rth-paper-20260505t1433`
- Zone: `us-central1-a`
- Source commit: `a9020805f49c4eb4797493073dfcc4739e8b6931`
- Portfolio config: `config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml`
- Stock feed: `sip`
- Option feed: `opra`
- Scope: QQQ + SPY governed bull/bear/choppy strategy set
- Broker mode: Alpaca paper
- Order arming: explicit `--submit-paper-orders`
- Session bound: 8 hours max from startup, expected to finish at or after RTH close
- Heartbeat cadence: 5 minutes to GCS

## GCS Evidence

- Source archive: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/input/codexalpaca_repo_qqq-spy-rth-paper-session-20260505T143315Z.tar.gz`
- Startup script: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/input/startup_rth_paper_session.sh`
- Session outputs: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/`
- Startup preflight: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/preflight_result.json`
- Heartbeat: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/session_heartbeat.json`
- Session stdout: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/session_stdout.txt`
- Session stderr: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/session_stderr.txt`

## Initial Validation

Startup preflight passed with:

```json
{
  "status": "startup_preflight_passed",
  "startup_check_status": "passed",
  "would_allow_trading": true,
  "submit_paper_orders": false,
  "broker_position_count": 0,
  "open_order_count": 0,
  "buying_power": 399225.72,
  "broker_equity": 99806.43,
  "trade_date": "2026-05-05"
}
```

The first heartbeat showed the trader process alive:

```json
{
  "captured_at_utc": "2026-05-05T14:37:24.407702+00:00",
  "trader_process_alive": true
}
```

A direct broker-state check from the control workstation after launch showed:

```json
{
  "clock_open": true,
  "positions": 0,
  "open_orders": 0,
  "equity": "99806.43",
  "buying_power": "399225.72"
}
```

## Cleanup Rule

The VM startup script will upload final session artifacts, capture post-session broker state, and self-stop only if there are zero open broker orders and zero broker positions. If residual broker state exists, the VM remains running for manual review.

## Next Check

Watch the GCS heartbeat and final `rth_session_summary.json`. Do not expand to additional tickers until this bounded QQQ/SPY RTH paper session has completed and its post-session broker state is clean.

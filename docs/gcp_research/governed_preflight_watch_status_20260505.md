# Governed Preflight Watch Status - 2026-05-05

## Purpose

Broker-free readiness monitor for the governed QQQ, SPY, and combined QQQ+SPY
paper-runner configurations. The monitor polls startup preflight only and never
submits broker-facing paper orders.

## Active command

```powershell
python scripts\watch_governed_preflight_readiness.py `
  --poll-seconds 300 `
  --max-checks 24 `
  --run-once-after-pass `
  --stop-when-all-pass `
  --gcloud-bin C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd
```

The optional run-once step is also guarded by `--no-submit-paper-orders` inside
the watchdog.

## Portfolios watched

- `qqq`: `config/qqq_regime_complete_paper_portfolio.yaml`
- `spy`: `config/spy_regime_complete_paper_portfolio.yaml`
- `qqq_spy`: `config/qqq_spy_regime_complete_paper_portfolio.yaml`

## Current state

Started locally from the control runner on 2026-05-05 before RTH. The first
cycle completed successfully and all three portfolios remained pending only
because live stock frames were not yet available pre-open:

- QQQ pending reason: `QQQ stock frame not ready yet`
- SPY pending reason: `SPY stock frame not ready yet`
- QQQ+SPY pending reasons: `QQQ stock frame not ready yet`, `SPY stock frame not ready yet`

Broker/account preflight fields were clean:

- `submit_paper_orders`: `false`
- broker positions: `0`
- open orders: `0`
- buying power observed: `399225.72`
- broker equity observed: `99806.43`

## Artifacts

Local logs:

- `reports/gcp_research/governed_preflight_watch_20260505/watch_summary.json`
- `reports/gcp_research/governed_preflight_watch_20260505/watchdog_stdout.txt`
- `reports/gcp_research/governed_preflight_watch_20260505/watchdog_stderr.txt`

GCS mirror:

- `gs://codexalpaca-control-us/research_results/governed_preflight_watch_20260505/`

## Hard rules

- Do not start broker-facing paper trading from this monitor.
- Do not remove `--no-submit-paper-orders`.
- Do not promote IWM yet; it has only bull-regime eligibility.
- QQQ and SPY are the only regime-complete governed-validation portfolios in
  this watch.

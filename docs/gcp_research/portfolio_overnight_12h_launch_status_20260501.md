# Portfolio Overnight 12h Launch Status - 2026-05-01

Status updated UTC: `2026-05-01T02:21:03Z`

## Current State

The `portfolio_overnight_12h_20260501` research fleet is running in Google Cloud. This is a research-only fleet: it is not broker-facing, does not submit paper orders, does not change live manifests, and does not change risk policy. Promotion means governed validation review only.

Canonical runner branch: `codex/phase2-fill-semantics-20260430`

Latest pushed commit: `c09d48a478feec8414349d58c9adba4badda6012`

GitHub PR: `https://github.com/abisaabr/CodexAlpacaTest-Trade/pull/2`

GCS wave prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/`

## Validation

- Focused tests: `5 passed`
- Focused ruff: clean
- Test command: `python -m pytest -q tests\test_build_portfolio_overnight_tournament_packet.py tests\test_build_portfolio_overnight_research_inputs.py tests\test_run_option_aware_research_backtest.py`
- Ruff command: `python -m ruff check scripts\build_portfolio_overnight_tournament_packet.py tests\test_build_portfolio_overnight_tournament_packet.py`

## Fleet

| VM | Role | Status | Notes |
| --- | --- | --- | --- |
| `portfolio-overnight-12h-20260501-data-coverage-top10` | data coverage | DELETED AFTER COMPLETION | Completed at `2026-05-01T02:01:39Z`; startup, command, source commit, and artifacts manifest uploaded. |
| `portfolio-overnight-12h-20260501-data-coverage-next10` | data coverage | DELETED AFTER COMPLETION | Completed at `2026-05-01T02:02:25Z`; startup, command, source commit, and artifacts manifest uploaded. |
| `portfolio-overnight-12h-20260501-qqq-deep-regime-grid` | QQQ deep grid | DELETED AFTER COMPLETION | Completed at `2026-05-01T01:57:32Z`; three `option_aware_candidate_summary.csv` files uploaded. |
| `portfolio-overnight-12h-20260501-option-aware-core-a` | option-aware tournament | RUNNING | Symbols: `AAPL AMD AMZN INTC IWM`; long replay stage, no terminal serial error observed. |
| `portfolio-overnight-12h-20260501-option-aware-core-b` | option-aware tournament | RUNNING | Symbols: `META MSFT NVDA SPY TSLA`; long replay stage, no terminal serial error observed. |
| `portfolio-overnight-12h-20260501-option-aware-core-c` | option-aware tournament | RUNNING | Symbols: `AVGO GOOGL MU NFLX ORCL`; recreated on clean disk after reused-disk pip corruption. |
| `portfolio-overnight-12h-20260501-option-aware-core-d` | option-aware tournament | RUNNING | Symbols: `PLTR QQQ TSM XLE XOM`; long replay stage, no terminal serial error observed. |
| `portfolio-overnight-12h-20260501-aggregator-promotion-rev` | aggregate and promote | RUNNING | Sleeps until the worker window completes, then builds portfolio report and promotion packet. |

## Fixes Applied During Launch

- `4230a2f1a90ee5a9ae52dc004a3131d06a2ab3ea`: ladder workers now use selected contract universe roots instead of raw contract inventory roots.
- `c09d48a478feec8414349d58c9adba4badda6012`: worker startup scripts now clear `${REPO_DIR}` before unpack/install, preventing partial `.venv` reuse after a Spot interruption.

## Launch Events

- The first fleet shape exceeded GCP disk quota, so the launch packet was resized from 250GB workers to 20GB/40GB workers.
- The next fleet shape exceeded the 32-vCPU quota, so the launch packet was resized to 26 research vCPUs plus the existing 4-vCPU paper VM.
- The first option-aware launch used raw contract inventory for ladder datasets and failed on `relative_strike_step`; this was corrected by staging `selected_option_contracts`.
- `option-aware-core-c` hit a corrupted `pip._vendor.resolvelib` import after a same-disk restart; it was recreated on a clean disk and the packet now has idempotent startup scripts.
- Startup metadata on all fleet VMs was refreshed with the idempotent scripts for future Spot restarts.
- Completed data coverage workers and the completed QQQ deep grid worker were deleted after artifact upload to reduce idle compute cost.

## Monitor Commands

```powershell
$env:CLOUDSDK_PYTHON='C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe'
$gcloud='C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd'
& $gcloud compute instances list --filter='name~portfolio-overnight-12h-20260501' --format='table(name,status,machineType,lastStartTimestamp,lastStopTimestamp)'
& $gcloud storage ls gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/workers/ --recursive
& $gcloud storage ls gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate/ --recursive
```

## Next Decision

Let the option-aware workers continue unless a VM terminates or a terminal traceback appears. Once worker outputs land, rerun or restart the aggregator if needed so it sees all completed worker reports before building the promotion packet.

# Portfolio Overnight 12h Launch Status - 2026-05-01

Status updated UTC: `2026-05-01T03:35:00Z`

## Current State

The `portfolio_overnight_12h_20260501` research fleet is running in Google Cloud. This is a research-only fleet: it is not broker-facing, does not submit paper orders, does not change live manifests, and does not change risk policy. Promotion means governed validation review only.

Canonical runner branch: `codex/phase2-fill-semantics-20260430`

Branch head after monitor/readiness recovery: `8d6e4336312cf5236a70b4a2486b269274a545bd`

GitHub PR: `https://github.com/abisaabr/CodexAlpacaTest-Trade/pull/2`

GCS wave prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/`

GCS monitor prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/monitor/`

## Validation

- Focused tests: `5 passed`
- Focused ruff: clean
- Monitor script smoke: `python scripts\monitor_portfolio_overnight_wave.py --once ...` wrote `portfolio_overnight_monitor_latest.json` to GCS.
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
| `portfolio-overnight-12h-20260501-option-aware-core-c` | option-aware tournament | RUNNING | Symbols: `AVGO GOOGL MU NFLX ORCL`; recreated as STANDARD after Spot termination, no terminal serial error observed. |
| `portfolio-overnight-12h-20260501-option-aware-core-d` | option-aware tournament | RUNNING | Symbols: `PLTR QQQ TSM XLE XOM`; recreated as STANDARD after Spot termination and observed staging PLTR option bars at `2026-05-01T03:21:15Z`. |
| `portfolio-overnight-12h-20260501-finalagg2-1255z` | aggregate and promote | RUNNING | STANDARD delayed final aggregator; sleeps until about `2026-05-01T12:55:00Z`, then writes to `aggregate/` using `candidate_identity_mode=variant_profile`. |

## Fixes Applied During Launch

- `4230a2f1a90ee5a9ae52dc004a3131d06a2ab3ea`: ladder workers now use selected contract universe roots instead of raw contract inventory roots.
- `c09d48a478feec8414349d58c9adba4badda6012`: worker startup scripts now clear `${REPO_DIR}` before unpack/install, preventing partial `.venv` reuse after a Spot interruption.
- `0f43267`: aggregate recovery workers can be launched with a bounded delay and output subdir.
- `8ad565a`: aggregate reports can isolate candidates by replay profile to avoid cross-profile evidence contamination.
- `45dd637`: promotion review packets dedupe eligible candidates by base strategy.
- `8d6e433`: paper readiness monitor now surfaces the profile-isolated partial aggregate as evidence-only without unblocking launch.

## Launch Events

- The first fleet shape exceeded GCP disk quota, so the launch packet was resized from 250GB workers to 20GB/40GB workers.
- The next fleet shape exceeded the 32-vCPU quota, so the launch packet was resized to 26 research vCPUs plus the existing 4-vCPU paper VM.
- The first option-aware launch used raw contract inventory for ladder datasets and failed on `relative_strike_step`; this was corrected by staging `selected_option_contracts`.
- `option-aware-core-c` hit a corrupted `pip._vendor.resolvelib` import after a same-disk restart; it was recreated on a clean disk and the packet now has idempotent startup scripts.
- Startup metadata on all fleet VMs was refreshed with the idempotent scripts for future Spot restarts.
- Completed data coverage workers and the completed QQQ deep grid worker were deleted after artifact upload to reduce idle compute cost.
- `option-aware-core-c` was later recreated as a standard on-demand `e2-standard-4` worker after another Spot termination, preserving the `AVGO GOOGL MU NFLX ORCL` lane for the overnight portfolio tournament.
- The old sleeping `aggregator-promotion-rev` was replaced by `finalagg2-1255z` so the final aggregate uses profile-isolated candidate identity and base-strategy dedupe.
- `option-aware-core-d` was recreated as a standard on-demand `e2-standard-4` worker after Spot termination, preserving the `PLTR QQQ TSM XLE XOM` lane.
- A profile-isolated partial aggregate exists at `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_partial_profile_20260501T031218Z/`; its deduped packet has 4 unique eligible base candidates, but it is evidence-only and does not unblock the final aggregate requirement.
- Direct and IAP SSH from this machine failed with network connection abort/closed errors, so monitoring currently relies on GCS artifacts, serial logs, and GitHub/PR logging.

## Monitor Commands

```powershell
$env:CLOUDSDK_PYTHON='C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe'
$gcloud='C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd'
python scripts\monitor_portfolio_overnight_wave.py --once --gcloud-bin $gcloud
& $gcloud compute instances list --filter='name~portfolio-overnight-12h-20260501' --format='table(name,status,machineType,lastStartTimestamp,lastStopTimestamp)'
& $gcloud storage ls gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/workers/ --recursive
& $gcloud storage ls gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate/ --recursive
```

## Next Decision

Let the option-aware workers continue unless a VM terminates or a terminal traceback appears. Once worker outputs land, rerun or restart the aggregator if needed so it sees all completed worker reports before building the promotion packet.

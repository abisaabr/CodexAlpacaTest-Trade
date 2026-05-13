# Portfolio Overnight 12h Launch Status - 2026-05-01

Status updated UTC: `2026-05-01T13:38:02Z`

## Current State

The `portfolio_overnight_12h_20260501` research fleet is running in Google Cloud. This is a research-only fleet: it is not broker-facing, does not submit paper orders, does not change live manifests, and does not change risk policy. Promotion means governed validation review only.

Canonical runner branch: `codex/phase2-fill-semantics-20260430`

Branch head after fastlane-aware monitors: `a169a77bb441e2d811eaea71a65bfbd218c21cb5`

GitHub PR: `https://github.com/abisaabr/CodexAlpacaTest-Trade/pull/2`

GCS wave prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/`

GCS monitor prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/monitor/`

## Validation

- Focused tests: `5 passed`
- Focused ruff: clean
- Monitor script smoke: `python scripts\monitor_portfolio_overnight_wave.py --once ...` wrote `portfolio_overnight_monitor_latest.json` to GCS.
- Paper readiness smoke after final aggregate: `decision=operator_review_ready_no_orders`, `aggregate_artifact_count=9`, `eligible_for_promotion_review_count=8`, `fastlane_top40_unique_eligible_base_count=4`, `running_wave_vm_count=8`.
- QQQ governed shadow no-order preflight passed with `submit_paper_orders=false`, broker position count `0`, open order count `0`, and QQQ stock/options frame ready.
- Test command: `python -m pytest -q tests\test_build_portfolio_overnight_tournament_packet.py tests\test_build_portfolio_overnight_research_inputs.py tests\test_run_option_aware_research_backtest.py`
- Ruff command: `python -m ruff check scripts\build_portfolio_overnight_tournament_packet.py tests\test_build_portfolio_overnight_tournament_packet.py`

## Fleet

| VM | Role | Status | Notes |
| --- | --- | --- | --- |
| `portfolio-overnight-12h-20260501-data-coverage-top10` | data coverage | DELETED AFTER COMPLETION | Completed at `2026-05-01T02:01:39Z`; startup, command, source commit, and artifacts manifest uploaded. |
| `portfolio-overnight-12h-20260501-data-coverage-next10` | data coverage | DELETED AFTER COMPLETION | Completed at `2026-05-01T02:02:25Z`; startup, command, source commit, and artifacts manifest uploaded. |
| `portfolio-overnight-12h-20260501-qqq-deep-regime-grid` | QQQ deep grid | DELETED AFTER COMPLETION | Completed at `2026-05-01T01:57:32Z`; three `option_aware_candidate_summary.csv` files uploaded. |
| `portfolio-overnight-12h-20260501-option-aware-core-a` | option-aware tournament | TERMINATED | Spot worker terminated before a worker output prefix was found. |
| `portfolio-overnight-12h-20260501-option-aware-core-b` | option-aware tournament | TERMINATED | Spot worker terminated before a worker output prefix was found. |
| `portfolio-overnight-12h-20260501-option-aware-core-c` | option-aware tournament | RUNNING | Symbols: `AVGO GOOGL MU NFLX ORCL`; recreated as STANDARD after Spot termination, no terminal serial error observed. |
| `portfolio-overnight-12h-20260501-option-aware-core-d` | option-aware tournament | RUNNING | Symbols: `PLTR QQQ TSM XLE XOM`; recreated as STANDARD after Spot termination and observed staging PLTR option bars at `2026-05-01T03:21:15Z`. |
| `portfolio-overnight-12h-20260501-fastlane-top40-a` | option-aware fastlane | RUNNING | STANDARD top-40 shard for `AAPL AMD AMZN INTC IWM`; first run `AAPL nearest_contract` started at `2026-05-01T03:39:17Z`; CPU near one full core. |
| `portfolio-overnight-12h-20260501-fastlane-top40-b` | option-aware fastlane | RUNNING | STANDARD top-40 shard for `META MSFT NVDA SPY TSLA`; first run `META nearest_contract` started at `2026-05-01T03:39:30Z`; CPU near one full core. |
| `portfolio-overnight-12h-20260501-fastlane-top40-c` | option-aware fastlane | RUNNING | STANDARD top-40 shard for `AVGO GOOGL MU NFLX ORCL`; first run `AVGO nearest_contract` started at `2026-05-01T03:39:31Z`; CPU near one full core. |
| `portfolio-overnight-12h-20260501-fastlane-top40-d` | option-aware fastlane | RUNNING | STANDARD top-40 shard for `PLTR QQQ TSM XLE XOM`; first run `PLTR nearest_contract` started at `2026-05-01T03:41:01Z`; launched in `us-east1-b` after `us-central1` external-address quota was exhausted. |
| `portfolio-overnight-12h-20260501-fastlane-top40-agg-1040z` | aggregate and promote | RUNNING | STANDARD delayed fastlane aggregator in `us-east1-b`; woke before RTH and wrote the fastlane aggregate packet to `aggregate_fastlane_top40_20260501/`. |
| `portfolio-overnight-12h-20260501-finalagg2-1255z` | aggregate and promote | RUNNING | STANDARD delayed final aggregator; sleeps until about `2026-05-01T12:55:00Z`, then writes to `aggregate/` using `candidate_identity_mode=variant_profile`. |

## Fixes Applied During Launch

- `4230a2f1a90ee5a9ae52dc004a3131d06a2ab3ea`: ladder workers now use selected contract universe roots instead of raw contract inventory roots.
- `c09d48a478feec8414349d58c9adba4badda6012`: worker startup scripts now clear `${REPO_DIR}` before unpack/install, preventing partial `.venv` reuse after a Spot interruption.
- `0f43267`: aggregate recovery workers can be launched with a bounded delay and output subdir.
- `8ad565a`: aggregate reports can isolate candidates by replay profile to avoid cross-profile evidence contamination.
- `45dd637`: promotion review packets dedupe eligible candidates by base strategy.
- `8d6e433`: paper readiness monitor now surfaces the profile-isolated partial aggregate as evidence-only without unblocking launch.
- `56409a9`: packet builder and config now support a reproducible top-40 fastlane recovery lane with unbuffered run markers.
- `78e248b`: fastlane config now includes a delayed pre-RTH aggregate worker.
- `b894ff3`: overnight monitors now surface the fastlane aggregate path separately from the final aggregate path.

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
- CPU telemetry showed the original option-aware workers running at roughly one fully used core each on `e2-standard-4`; a STANDARD `top_n=40` fastlane was launched as an additive recovery path instead of interrupting the exhaustive lane.
- Fastlane packet and source are mirrored under `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/inputs/fastlane_top40_packet_v2/` and `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/inputs/source/codexalpaca_repo_source_fastlane_top40_20260501.tar.gz`.
- The active 15-minute monitors are running locally as PID `38352` for portfolio wave status and PID `18824` for paper readiness after the fastlane aggregate path correction.
- The fastlane aggregate has now landed at `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate_fastlane_top40_20260501/`. The packet is research-only and reports 8 eligible profile-level candidates, 4 unique eligible base candidates, and no broker-facing/live-manifest/risk-policy effect.
- Eligible fastlane candidates are currently QQQ bull/choppy only: three QQQ bull structures and one QQQ choppy iron-condor structure. No QQQ bear strategy is eligible yet; bear candidates remain blocked mainly by fill coverage and option-trade count.
- The paper-readiness monitor path was corrected for nested aggregate output folders. This is observability only; it does not alter promotion gates or execution behavior.
- Duplicate stale monitor processes were stopped. Exactly one portfolio monitor and one paper-readiness monitor are now running on a 15-minute cadence as local PIDs `4448` and `41044`.
- Final aggregator serial output shows `aggregate_wait_seconds=34627`, `aggregate_output_subdir=aggregate`, and `candidate_identity_mode=variant_profile`; no terminal error is visible.
- The canonical final aggregate has landed at `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate/`. The packet is research-only, non-broker-facing, and reports 364 candidates, 8 profile-level eligible candidates, and 4 unique eligible base candidates.
- Final eligible candidates are QQQ bull/choppy only. No QQQ bear strategy passed the final overnight packet, so any bear exposure should remain limited to the separate QQQ governed shadow-validation evidence path until a final-packet bear candidate qualifies.
- The 20-ticker tournament is not complete. The final aggregate is a cutoff snapshot from available worker outputs. Summary evidence observed so far covers AAPL, AVGO, GOOGL, META, MSFT, PLTR, and QQQ-specialized lanes; active workers are still progressing on additional symbols.
- QQQ governed shadow-validation no-order run passed with `status=ran_once`, `startup_check_status=passed`, open trades `0`, completed trades `0`, and `submit_paper_orders=false`.
- Detailed scope/coverage checkpoint: `docs/gcp_research/portfolio_overnight_12h_scope_coverage_20260501.md`.

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

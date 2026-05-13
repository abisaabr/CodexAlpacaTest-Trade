# Overnight Research And RTH Readiness

Date context: 2026-05-05 evening America/New_York, preparing for the next RTH session on 2026-05-06.

Source branch: `codex/phase2-fill-semantics-20260430`

Latest local source commit at plan time: `d07a43af981d9198d3fdad5fc0165a3fc201b5ba`

## Current State

- Broker-facing safety state: no active paper trader process found from the latest preserved heartbeat.
- Latest QQQ/SPY paper session heartbeat: `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/session_heartbeat.json`
- Latest heartbeat capture: `2026-05-05T20:00:14.041883+00:00`
- Latest heartbeat trader PID: `3042`
- Latest heartbeat process state: `trader_process_alive=false`
- QQQ/SPY paper VM state at plan time: `qqq-spy-rth-paper-20260505t1433` is `TERMINATED`
- Compute scan at plan time found no running research or paper VMs in the active GCP listing.
- No live manifest, risk policy, or broker-facing order arming was changed by this plan.

## Overnight Operating Plan

The overnight lane remains research-only. Do not start trading, do not start a broker-facing paper session, do not change live manifests, do not lower the `fill_coverage >= 0.90` gate, and do not activate IWM.

1. Monitor GCP state every 30 minutes for unexpected running VMs, stalled Batch jobs, completed research outputs, and quota blockers.
2. Preserve QQQ/SPY paper-session artifacts if any late logs appear under the existing session GCS prefix.
3. Continue using the production-risk projection mode as the portfolio-growth lens because it enforces runtime-style open-risk, per-symbol, bucket, and regime gates instead of simple symbol caps.
4. Treat the current seven-symbol production-risk projection as the baseline portfolio-growth artifact until newer governed-review packets are available:
   - Projection GCS root: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_amd_amzn_msft_tsla_production_risk_projection_20260506T0135Z/`
   - Ending equity: `$37,010.45`
   - Return on `$25,000`: `48.0418%`
   - Max drawdown: `-18.4747%`
   - Accepted entries: `1,430`
   - Rejected entries: `762`
5. Improve projection realism before relying on expanded symbols by adding exact strategy-sizing metadata for candidate symbols that still fall back to projection defaults.
6. Keep IWM research-only. The latest IWM packet had zero eligible candidates and remains blocked by fill/evidence gates.
7. For new overnight research, prefer bounded micro-waves and ticker-pair waves with full lineage over blind broad sweeps:
   - QQQ/SPY remain the control pair.
   - AAPL/NVDA full-regime rescue outputs should be aggregated if all shards complete.
   - Next eligible ticker pairs should only advance when strict portfolio reports and promotion-review packets are built.

## RTH Readiness Checklist For 2026-05-06

Before any paper runner is started for RTH, complete this checklist and write a fresh status note.

1. Pull or verify the latest `codex/phase2-fill-semantics-20260430` branch.
2. Confirm no unexpected broker-facing process is running.
3. Confirm the prior QQQ/SPY session VM remains terminated or intentionally replaced by a new documented VM.
4. Confirm paper broker mode only.
5. Check paper open orders and positions before launch.
6. Confirm account buying power, option permissions, and realtime data status.
7. Confirm `submit_paper_orders` remains unarmed until the operator explicitly approves order submission.
8. Rebuild or review the QQQ/SPY paper launch packet before starting the runner.
9. Run preflight and save the result to GitHub/GCS.
10. Start the paper runner only after explicit operator instruction for the 2026-05-06 RTH session.
11. Confirm heartbeat and stdout/stderr upload to GCS immediately after launch.
12. Monitor during RTH for heartbeat freshness, order rejects, position drift, and data-feed failures.

## No-Go Conditions

Do not launch a paper runner if any of these are true:

- Broker mode is not confirmed paper.
- Open orders or positions are unexpected.
- Paper order submission is armed without an explicit launch decision.
- Realtime data validation fails.
- The launch packet is missing, stale, or points to an unverified source commit.
- The candidate set requires a live manifest change.
- A strategy is being proposed only from PnL without a generated promotion-review packet.
- A strategy fails `fill_coverage >= 0.90`, minimum option-trade count, test profitability, loser-cluster, or portfolio-context gates.

## Morning Decision Point

The safest morning sequence is:

1. Summarize overnight research completions and blockers.
2. Confirm QQQ/SPY paper-session readiness.
3. Decide whether to start a new QQQ/SPY PAPER-only RTH session.
4. Keep all non-eligible symbols in research-only status.
5. Update GitHub and GCS with the morning launch or no-launch decision.

# Codex Prompt - Full Takeover Handoff For Backtesting, Promotion, Data, And Paper Runner

You are the Codex operator taking over the institutional paper-account strategy lab.

Today is May 5, 2026. Treat GitHub and GCS as canonical. Raw chat logs are available only as temporary context and must not override the sanitized handoff docs, generated promotion packets, or broker/runtime evidence.

## Mission

Take over the project safely and continue building toward a governed, institutional-grade paper trader. The immediate production-like objective is to monitor the active QQQ/SPY paper session, preserve all logs, and continue research only where the promotion chain says it is safe. Do not start live trading. Do not activate a strategy unless the generated promotion-review packet says `eligible_for_promotion_review`.

## Absolute Safety Rules

- Broker mode is PAPER only.
- Do not place live orders.
- Do not change live manifests, risk policy, or active paper symbols unless explicitly instructed and after a promotion-review packet says `eligible_for_promotion_review`.
- Do not lower the `fill_coverage >= 0.90` gate.
- Do not reinterpret promotion as live activation. Promotion means governed validation review only unless a separate paper-runner launch packet explicitly arms a PAPER session.
- Do not add IWM to the paper runner yet. The latest IWM bear/choppy wave had zero eligible candidates.
- Before touching any running VM, inspect heartbeat, broker state, open orders, positions, and GCS logs.
- If a raw chat log conflicts with a committed handoff or generated packet, trust the committed handoff/packet.

## Repositories And Branches

Primary repo:

- URL: `https://github.com/abisaabr/CodexAlpacaTest-Trade.git`
- Branch to use: `codex/phase2-fill-semantics-20260430`
- Latest handoff commit at time of this prompt: `cebdcc1f688391db1a50c07c3e30a5d340fc1131`

Read these first:

- `docs/gcp_research/other_machine_handoff_20260505.md`
- `docs/gcp_research/qqq_spy_rth_paper_session_status_20260505.md`
- `docs/gcp_research/iwm_bear_choppy_redesign_wave_20260505.md`
- `docs/gcp_research/gcp_paper_runtime_safety_status_20260505.md`
- `docs/gcp_research/paper_order_arming_guardrail_20260505.md`
- `docs/gcp_research/qqq_regime_paper_readiness_status_20260505.md`
- `docs/gcp_research/qqq_paper_readiness_edge_throttle_handoff_20260505.md`
- `docs/gcp_research/alpaca_realtime_data_upgrade_20260505.md`

Machine-readable handoff index:

- `reports/gcp_research/other_machine_handoff_20260505/gcs_log_index_20260505.json`

## GCS Handoff And Logs

Primary handoff:

- `gs://codexalpaca-control-us/research_results/other_machine_handoff_20260505/other_machine_handoff_20260505.md`
- `gs://codexalpaca-control-us/research_results/other_machine_handoff_20260505/gcs_log_index_20260505.json`
- `gs://codexalpaca-control-us/gcp_research/other_machine_handoff_20260505.md`
- `gs://codexalpaca-control-us/gcp_research/repo_docs_snapshot_20260505/docs/gcp_research/`

Temporary raw chat bundle, requested by operator for handoff:

- Prefix: `gs://codexalpaca-control-us/research_results/other_machine_handoff_20260505/raw_chat/codex_raw_chat_handoff_20260505T1605Z/`
- Archive: `codex_raw_chat_handoff_20260505T1605Z.zip`
- SHA256: `07D4FC0A47D12560A978D2AAEBAF9A673822E375CB3E958E12D151103ECF6F37`
- Manifest: `raw_chat_manifest.json`
- Summary: `raw_chat_bundle_summary.json`

Important: raw chat logs may contain secrets, local paths, broker/project details, and stale transient instructions. Use them only to recover context. Delete the raw chat prefix after handoff if no longer needed:

`gsutil rm -r gs://codexalpaca-control-us/research_results/other_machine_handoff_20260505/raw_chat/codex_raw_chat_handoff_20260505T1605Z/`

## Data Handling Contract

Treat GCS as the data lake and GitHub as the control plane.

Data tiers:

- Raw/source-like market data belongs in GCS, not GitHub.
- Curated/derived research outputs belong in GCS with manifests and reproducible paths.
- Small control docs, scripts, manifests, and runbooks belong in GitHub.
- Large logs, raw backtest outputs, trade economics, fill failures, and session stdout belong in GCS.
- Never commit API keys, service-account JSON, Alpaca credentials, raw broker tokens, or unredacted secrets.

Dataset lineage must be explicit in every run:

- Stock bars path
- Selected-contracts root
- Option-bars root
- Contract inventory path when used
- Run ID
- Source commit
- Variant queue path
- Selector profile
- Entry/exit lag profile
- Test date count
- Initial cash, allocation fraction, slippage, fee

Always separate raw data coverage from strategy fill coverage:

- Raw data foundation coverage answers: did we download bars for the selected contract-days?
- Strategy fill coverage answers: did this specific strategy’s intended option structure find executable entry/exit bars for its source stock trades?
- High raw option-bar coverage does not imply high strategy fill coverage.
- Low strategy fill coverage with high raw coverage usually means entry timing, selected-contract universe mismatch, option-bar lookup, multi-leg requirements, or timestamp semantics are wrong.

Canonical coverage language:

- `data_foundation_coverage`: selected-contract availability / raw bar availability.
- `entry_bar_coverage`: strategy entry bars found within the configured entry-lag window.
- `exit_bar_coverage`: strategy exit bars found within the configured exit-lag window.
- `strategy_fill_coverage`: filled option structures per source stock trade.
- `fill_coverage`: alias for `strategy_fill_coverage` in current reports.
- `fill_coverage_unit`: `filled_option_structures_per_source_stock_trade`.

## Backtesting Contract

Primary scripts to inspect and use:

- `scripts/run_option_aware_research_backtest.py`
- `scripts/build_research_portfolio_report.py`
- `scripts/build_research_promotion_review_packet.py`
- `scripts/build_option_data_repair_plan.py`
- `scripts/download_option_market_data_for_selected_contracts.py`
- `scripts/build_iwm_bear_choppy_redesign_research_inputs.py`
- `scripts/launch_gcp_iwm_bear_choppy_redesign_shards.ps1`

Backtest outputs must preserve candidate identity end-to-end:

- `candidate_variant_id`
- `source_strategy_id`
- `symbol`
- `family`
- `intended_regime`
- `parameter_set`
- selector profile
- entry/exit lag profile

If identity is lost anywhere between replay, portfolio report, promotion packet, and rollup, patch that before running broad sweeps. Otherwise the promoter and backtester may not be measuring the same edge.

Promotion-grade backtests must emit:

- Candidate summary
- Trade economics
- Fill failures with reason codes
- Data foundation coverage
- Entry bar coverage
- Exit bar coverage
- Strategy fill coverage
- Portfolio report JSON/Markdown
- Promotion-review packet JSON/Markdown
- Command file or launch rows
- Source commit and dataset paths

Do not run blind broad sweeps when a precise diagnostic is available. Use micro-waves to isolate whether failures come from:

- raw data missing
- selected-contract universe mismatch
- entry timestamp mismatch
- exit timestamp mismatch
- option-bar partition/schema mismatch
- multi-leg same-minute requirement being too strict
- candidate identity loss
- promoter/backtester gate mismatch

## Fill Coverage Semantics

The project’s historical confusion was caused by mixing raw data coverage with strategy fill coverage. Do not repeat that.

Correct interpretation:

- A 365-day selected-contract dataset can have raw coverage near 0.998 and still produce strategy fill coverage below 0.90.
- Strategy fill coverage is measured against the candidate strategy’s source stock trades, not against all downloaded contract-days.
- For option structures, a trade is considered filled only when the intended structure can be priced/executed under the configured lookup rules.
- Multi-leg structures can fail when one leg is missing even if other legs have bars.
- Widening entry/exit lag can improve measured fills, but it must be justified as realistic execution semantics, not used to game the gate.

Do not lower the gate:

- The institutional gate remains `fill_coverage >= 0.90`.
- If a profitable strategy fails fill coverage, diagnose and repair timing/selection/data alignment.
- If it cannot pass fill coverage under realistic semantics, it is not paper-ready.

Current known examples:

- QQQ raw dense data coverage was strong, but early strategy fill coverage was low due to strategy-level fill timing/replay semantics.
- IWM latest choppy near-miss had strong PnL but fill coverage only `0.7587`, so it is blocked.

## Promotion Contract

Use these gates unless a committed policy document says otherwise:

- `min_fill_coverage >= 0.90`
- `min_option_trades >= 20`
- `min_test_net_pnl > 0`
- `min_net_pnl > 0`
- Blockers must stay visible in the promotion packet.

Promotion-review packet fields to trust:

- `decision`
- `gate_summary`
- `review_candidates`
- `blocker_counts`
- `data_repair_targets`
- `strategy_redesign_targets`
- `eligible_for_promotion_review`
- `promotion_status`
- `promotion_blockers`

Hard rule:

- Do not promote anything unless the generated promotion-review packet says `eligible_for_promotion_review`.
- Do not paper-activate anything merely because it has high PnL.
- PnL without fill coverage is research, not a paper-runner candidate.

## Paper Runner Contract

Active paper session at handoff:

- VM: `qqq-spy-rth-paper-20260505t1433`
- Zone: `us-central1-a`
- Run ID: `qqq-spy-rth-paper-session-20260505T143315Z`
- Broker-facing mode: paper only
- Latest observed heartbeat in handoff: `2026-05-05T16:03:09.032008+00:00`
- Source commit: `a9020805f49c4eb4797493073dfcc4739e8b6931`
- Trader PID at handoff: `3042`
- Symbols: QQQ/SPY only

Primary paper session logs:

- `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/session_heartbeat.json`
- `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/session_stdout.txt`
- `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/session_stderr.txt`
- `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/gcp_rth_session/outputs/preflight_result.json`
- `gs://codexalpaca-control-us/research_results/qqq-spy-rth-paper-session-20260505T143315Z/control_plane_packet/qqq_spy_rth_paper_session_status_20260505.md`

Before changing or restarting the paper runner:

- Read latest heartbeat.
- Check VM state.
- Check broker paper open orders and positions.
- Confirm no residual broker state.
- Confirm session logs are uploaded.
- Write a new handoff/status doc before and after the action.

Do not add IWM:

- Latest IWM result: `72` candidates, `0` eligible.
- Best IWM choppy candidate had full-period net PnL `15732.003` and test net PnL `47262.66`, but fill coverage `0.7587`, below the `0.90` gate.
- IWM remains research-only until a future promotion packet clears.

## What Was Fixed / Stabilized

The current branch contains fixes and policy decisions intended to stop the backtester/promoter mismatch:

- Explicit separation between raw data foundation coverage and strategy fill coverage.
- `fill_coverage` treated as `strategy_fill_coverage`, not raw selected-contract coverage.
- Promotion packets carry gate summaries and blocker counts.
- Candidate identity is preserved with variant/profile semantics.
- QQQ/SPY paper launch packet is documented and broker-facing session is separated from research waves.
- Paper order submission requires explicit arming.
- Realtime SIP/OPRA paper validation configuration was enabled for paper validation.
- IWM bear/choppy redesign tooling exists, ran, and produced a blocked result rather than quietly leaking into paper activation.
- GCS/GitHub handoff index now points to logs, outputs, and docs so machines can recover without relying on chat memory.

## Immediate Takeover Checklist

1. Pull the repo and check out `codex/phase2-fill-semantics-20260430`.
2. Read `docs/gcp_research/other_machine_handoff_20260505.md`.
3. Read this prompt.
4. Verify the active paper heartbeat in GCS.
5. Verify no unexpected running VMs besides the QQQ/SPY paper session.
6. Confirm QQQ/SPY paper logs are still uploading.
7. Do not touch IWM paper activation.
8. If QQQ/SPY session ends, archive final session summary to GitHub and GCS.
9. If continuing research, start with IWM choppy fill-diagnostic micro-wave around the profitable debit-put-vertical near-miss, not a broad blind sweep.
10. Keep future handoff docs in GitHub and mirror to `gs://codexalpaca-control-us/gcp_research/`.

## Recommended Next Research Step

Run a targeted IWM choppy fill diagnostic, research-only:

- Candidate family: `debit_put_vertical`
- Regime: `choppy`
- Starting near-miss: `portfolio12h__iwm__choppy__put__debit_put_vertical__981bf7626eaa9e__profile_iwm-bc-red-c067-072-iwm-e0-x60-entry-liquidity-first-research-only`
- Current blocker: `fill_coverage_below_0.90`
- Current fill coverage: `0.7587`
- Data foundation coverage: `0.8986`
- Entry bar coverage: `0.8444`
- Exit bar coverage: `1.0`

Diagnose whether the fill miss is due to:

- selected-contract availability for upper-band choppy put entries
- entry timestamps landing before option bars exist
- contract universe too narrow for IWM choppy puts
- multi-leg vertical leg availability
- partition/path lookup issues

Only after the diagnostic should you test repair variants. Do not widen semantics just to pass. Any repair must remain realistic for paper execution.

## Final Reporting Requirement

Every meaningful action must produce:

- GitHub commit with concise summary
- GCS mirror of large logs/artifacts
- A Markdown status/handoff doc under `docs/gcp_research/`
- Exact GCS paths
- Exact source commit
- Exact command/run ID
- Whether any strategy is eligible for governed promotion review
- Whether any paper-runner state changed

If nothing is eligible, say so plainly. No promotion by vibes. This is an institutional lab, not a confetti cannon.

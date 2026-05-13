# May 13 Intraday Hardening Handoff

## Scope

Research-only and PAPER-only hardening performed during the May 13 RTH session. No live manifests, paper configs, or global risk policy were changed. The active PAPER trader was not restarted.

## Commits

- `9431b68` - Added side-aware quote-backed replay PnL to `apply_quote_sidecar_to_trade_economics.py`, projection controls for `--prefer-quote-backed-pnl` and `--require-quote-backed-replay`, and optimizer support for `scaled_projection_pnl`.
- `1710be7` - Added an opt-in governed manifest gate, `--require-quote-backed-replay`, so eligible review candidates can be skipped unless their packet row carries quote-backed evidence.
- `8089c1a` - Added a configurable future-session stop-loss cooldown guardrail for repeated same symbol/regime/family stop-outs.
- `36da6ee` - Added `--session-trade-leg-symbols` to the no-submit quote shadow so current session trade legs can be forced into the OPRA capture plan and refresh loop.
- Pending local patch - Added a post-session evidence bundle, exact-symbol quote sidecar filtering, postmortem quote-evidence surfacing, and a reusable optimizer tier sweep. These are broker-free and do not edit paper manifests.

## Runtime Status At 11:30 ET

- Active PAPER trader PID: `12976`.
- Active no-submit quote shadow PID: `12616`.
- Broker safety: `0` open orders, `0` positions.
- Paper lock: `LIVE_TRADING=false`, paper endpoint, no risky processes.
- Session: `4` completed trades, `0` open trades.
- Runtime state: new entries are blocked by the entry-execution circuit breaker after adverse slippage.
- EOD flatten checkpoints remain configured for `10` and `2` minutes before close.

## Quote Evidence

Latest runtime gap report:

- Runtime-selected symbols covered by quote plan: `100%`.
- Runtime selected unique symbols: `54`.
- Subscription plan symbols: `136`.
- Session-trade symbol coverage: `75%`.
- Missing completed-trade symbols: `AMD260515P00422500`, `QQQ260514P00703000`.
- Runtime errors: `0`.

Latest quote health snapshot:

- Quote event tail count: `88,780`.
- Unique quote symbols in tail: `136`.
- Quote latency p50: `0.282756s`.
- Quote latency p90: `0.547411s`.
- Quote latency p99: `0.768531s`.
- Relative spread p50: `0.009132`.
- Relative spread p90: `0.052724`.
- Relative spread p99: `0.142857`.

## Decisions

- Do not add new strategies to the paper runner from current research until they have complete quote-backed replay lineage and survive train/test projection under cost and fill haircuts.
- Do not restart the active trader during RTH unless a safety defect appears. The current circuit breaker is functioning as intended.
- Treat today's early completed trades as partially quote-backed only; two completed-trade symbols were outside the corrected capture plan.
- Do not launch more GCP compute until the OPRA/SIP lineage bundle tells us there are quote-backed survivors. The current blocker is evidence lineage, not backtest throughput.

## Post-Session Evidence Flow

1. Build the evidence bundle after RTH from the session JSON and exact runtime-leg websocket file:

```powershell
python scripts\build_paper_session_evidence_bundle.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml --trade-date 2026-05-13 --quote-events-jsonl D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow_runtime_legs_neighbor_120_delayed_refresh_20260513T145514Z\realtime_shadow_events.jsonl --output-dir D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\post_session_evidence_bundle
```

2. Build the daily postmortem and include the evidence gate:

```powershell
python scripts\build_multi_ticker_paper_postmortem.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml --trade-date 2026-05-13 --quote-evidence-json D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\post_session_evidence_bundle\paper_session_evidence_bundle_summary.json
```

3. Only if `quote_backed_optimizer_input_allowed=true`, run quote-backed projection and optimizer tiers:

```powershell
python scripts\run_portfolio_optimizer_tier_sweep.py --portfolio-report-json <quote_backed_survivor_report.json> --scaled-trades-csv <portfolio_growth_scaled_trades.csv> --output-dir <optimizer_tier_sweep_dir> --initial-cash 25000 --backtest-allocation-fraction 0.05 --train-end-date 2026-01-30
```

## Next Steps

- After RTH, build the full May 13 postmortem and quote evidence report from the completed session and sidecar.
- Apply the quote sidecar to any completed trade economics using the new quote-backed replay PnL path.
- Run projection with `--require-quote-backed-replay`, market-quality cost model, fill model, and constrained optimizer.
- Run `scripts\run_portfolio_optimizer_tier_sweep.py` to compare `unconstrained_max_profit`, `current_paper_risk`, `strict_institutional`, `$200/day relaxed-but-controlled`, and `drawdown_minimized_benchmark` tiers from the same quote-backed survivor pool.
- Use `--require-quote-backed-replay` when building any future governed validation manifest intended for PAPER consideration.
- Evaluate whether the stop-loss cooldown guardrail should be enabled in the next paper config after postmortem review.
- Launch the next no-submit quote shadow with `--session-trade-leg-symbols` so actual selected trade legs remain captured even when they differ from the precomputed runtime leg set.

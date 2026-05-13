# May 13 PAPER Runtime Issue Log

## 2026-05-13 09:31 ET - Launch Controller Redirection Failure

- The guarded RTH launch controller failed before starting the trader because PowerShell could not open `startup_preflight_20260513T093105_stdout.json`.
- Manual takeover ran a fresh no-submit preflight successfully and started exactly one PAPER trader.
- Follow-up: make startup preflight output paths unique per attempt and avoid redirecting directly to a path that another process may hold.

## 2026-05-13 09:33 ET - PAPER Trader Manual Launch

- Active broker-facing trader PID: `12976`.
- Command: `python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml --submit-paper-orders`.
- Broker state after launch: PAPER endpoint, live mode false, zero open orders, zero positions.

## 2026-05-13 09:33-09:53 ET - Quote Capture Load Tuning

- Initial 900-symbol no-submit quote shadow captured OPRA/SIP events but produced repeated `slow client (407)` warnings and option quote latency over 20 seconds.
- A three-shard quote capture attempt hit Alpaca websocket connection limits (`HTTP 429` / `connection limit exceeded`), so it was stopped.
- A reduced 240-symbol stream removed connection errors but still had high option latency.
- Current quote capture uses runtime-selected leg symbols only: 55 forced OPRA option contracts, no stock quote flood, no option trade subscription, no trade-update stream.
- Current exact runtime-leg capture path: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow_runtime_legs_exact`.
- Latest observed latency after exact runtime-leg capture: option quote p50 about 0.5s, p90 about 5.8s, no slow-client or connection-limit errors observed.
- Follow-up: add an automated runtime-leg quote-capture mode so the session starts with the exact strategy-leg universe instead of broad contract inventory.

## 2026-05-13 09:49-09:52 ET - Early QQQ Greek Multi-Leg Stop-Outs

- Two QQQ bear broken-wing put butterfly strategies fired and both exited on stop loss.
- Completed realized paper PnL so far: `-56.40`.
- Broker state after exits: zero open orders and zero positions.
- Follow-up: postmortem should compare entry/exit OPRA sidecar quotes against order fills and verify whether the stop-loss trigger was expected under quote-cost and fill-haircut assumptions.

## 2026-05-13 09:40 ET - Health Check Hardening

- `scripts/run_multi_ticker_health_check.py` was hardened to avoid crashing when the Windows scheduled task is missing and to detect manually launched system-Python PAPER trader processes.
- Commit: `f4b466d Harden multi-ticker health check process detection`.

## 2026-05-13 10:10 ET - Completed Trade Sidecar Evidence Gap

- New completed-trade sidecar audit found the two early QQQ stop-outs have session-level entry/exit bid/ask and quote timestamps, but `0/8` completed legs have matching raw OPRA websocket sidecar quotes within five seconds.
- Primary cause: the exact runtime-leg OPRA shadow stream began after the QQQ entry/exit quote timestamps. One selected short leg, `QQQ260514P00709000`, was also not present in the initial forced-symbol file, so the sidecar is not complete enough for quote-backed optimizer use.
- Output: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\paper_trade_quote_sidecar_coverage_20260513_current.json`.
- Follow-up: start exact runtime-leg quote capture before order submission on future sessions and require `complete_session_and_sidecar_quote_evidence` before using completed PAPER trades in projections or promotion decisions.

## 2026-05-13 10:15 ET - AMD Bear Butterfly Stop-Outs And Broker Flat

- Health snapshot found exactly one broker-facing PAPER trader, exactly one no-submit quote shadow, PAPER-only runtime, fresh lease/session, zero broker orders, and zero broker positions.
- Session advanced to `4` completed trades and `0` open trades. The two new completed trades were AMD bear broken-wing put butterfly stop-outs with net PnL `-195.40` and `-215.40`; cumulative completed-trade PnL is `-467.20`.
- Exact runtime-leg quote capture is healthy: 55 OPRA symbols, option quote p50/p90/p99 about `0.34s/0.77s/1.04s`, no stale capture.
- Sidecar audit still fails closed: `0/16` completed legs have complete raw OPRA sidecar coverage. The AMD symbols were not in the forced OPRA sidecar universe, and one AMD stop-out is missing exit quote fields in session state.
- Output: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\paper_trade_quote_sidecar_coverage_20260513_heartbeat.json`.
- Follow-up: include any runtime-selected legs for newly triggered symbols in quote capture before orders can fire, and investigate why one completed AMD stop-out missed exit quote field persistence.

## 2026-05-13 10:24-11:02 ET - Runtime-Leg Quote Capture Drift Hardening

- Added a broker-free runtime quote-capture gap report to compare current runtime-selected legs, completed/open session trade legs, the active OPRA subscription plan, and observed OPRA tail symbols.
- The exact-only 55-symbol stream missed new current runtime legs as selected strikes drifted intraday. A 100-symbol neighbor-buffer plan improved coverage from about `81.8%` to about `96.4%`, but still missed some current runtime legs.
- Added neighbor-prioritized option-universe merging so forced runtime legs stay first and remaining capacity fills nearby same-root/same-expiry strikes rather than alphabetically overloading early tickers.
- Added dynamic no-submit runtime-leg refresh support with a hard symbol cap. The active quote shadow was restarted to `--max-option-symbols 120 --runtime-refresh-seconds 300 --runtime-refresh-max-total-symbols 180`; it added three drifted OPRA symbols at `11:01 ET` without touching the broker-facing trader.
- Current active quote shadow path: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow_runtime_legs_neighbor_120_delayed_refresh_20260513T145514Z`.
- Latest health snapshot remained PAPER-safe: one trader PID `12976`, one no-submit quote shadow PID `12616`, zero broker orders, zero broker positions, option quote p50/p90/p99 about `0.16s/0.64s/0.83s`, and fresh lease/session writes.
- Follow-up: after RTH, tune runtime refresh interval and dynamic subscription behavior under a no-submit canary before making shorter refresh intervals the default. During RTH, avoid further quote-shadow restarts unless capture becomes stale or broker safety requires it.

## 2026-05-13 10:24 ET - Cleanup Fallback Exit Quote Persistence

- Patched the multi-leg cleanup fallback so forced cleanup exits preserve exit bid/ask/mark/quote-time/spread/freshness fields from the current option chain.
- This addresses the AMD cleanup path where one completed stop-out had missing exit quote fields even though the normal `_run_exit` path enriches quote evidence.
- Follow-up: rerun EOD completed-trade quote evidence report and verify any future cleanup exits include per-leg exit quote fields before using them in quote-backed projections.

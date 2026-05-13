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

# Multi-Symbol Paper Order Submission Status

Date context: 2026-05-06 RTH, America/New_York.

This is a PAPER-only runtime handoff. It does not authorize live trading and does not modify the live strategy manifest or global risk policy.

## Runtime State

- Run ID: `multi-symbol-governed-paper-20260506T0955ET`
- Runtime command: `python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --submit-paper-orders`
- Local PID: `37892`
- Process state at launch check: `running`
- Broker mode: `paper`
- Runtime order submission: `enabled by explicit --submit-paper-orders`
- Config default: `submit_paper_orders=false`
- Live manifest effect: `none`
- Risk policy effect: `none`

## Promotion Scope

The active paper runner uses the generated governed-validation manifest:

- Manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260506.yaml`
- Paper config: `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`
- Strategy count: `120`
- Ticker count: `9`
- Tickers: `AMD, AMZN, AVGO, GOOGL, MSFT, QQQ, SPY, TSLA, TSM`
- Regime counts: `bull=14`, `bear=49`, `choppy=57`
- Family counts: `Single-leg long call=70`, `Single-leg long put=49`, `debit_call_vertical=1`
- Non-eligible strategies in manifest: `0`

Per-symbol strategy counts:

- `AMD`: `15`
- `AMZN`: `5`
- `AVGO`: `20`
- `GOOGL`: `20`
- `MSFT`: `20`
- `QQQ`: `10`
- `SPY`: `8`
- `TSLA`: `2`
- `TSM`: `20`

## Pre-Launch Checks

Fresh no-order startup preflight passed before order-submission launch:

- `startup_preflight_passed`
- `would_allow_trading=true`
- Broker positions before launch: `0`
- Open broker orders before launch: `0`
- Required stock and option inventory: available for all 9 governed symbols

No-submit diagnostic run also completed cleanly:

- Status: `ran_once`
- Startup check status: `passed`
- Open trades: `0`
- Completed trades: `0`
- Blocked new entries: `false`

## Initial Runtime Observation

Initial session state after order-submission launch:

- Startup check status: `passed`
- Blocked new entries: `false`
- Signals fired: `0`
- Open trades: `0`
- Completed trades: `0`
- Last observed regimes: `QQQ=bull`, `SPY=neutral`, `AMD=neutral`, `AMZN=neutral`, `MSFT=bull`, `TSLA=bull`, `AVGO=bear`, `GOOGL=neutral`, `TSM=neutral`
- Alert: morning notification delivery failed; this does not block trading logic but should be repaired separately.

## Logs And State

Local runtime artifacts:

- Stdout: `reports/multi_ticker_portfolio/runs/multi-symbol-governed-paper-20260506T0955ET/session_stdout.txt`
- Stderr: `reports/multi_ticker_portfolio/runs/multi-symbol-governed-paper-20260506T0955ET/session_stderr.txt`
- PID file: `reports/multi_ticker_portfolio/runs/multi-symbol-governed-paper-20260506T0955ET/session_pid.txt`
- Session state: `reports/multi_ticker_portfolio/state/multi_symbol_governed_realtime_20260506/session_2026-05-06.json`

GCS mirror:

- `gs://codexalpaca-control-us/research_results/multi-symbol-governed-paper-20260506T0955ET/`

## Hard Rules

- PAPER mode only.
- Do not start live trading.
- Do not change live manifests or global risk policy from this runtime.
- Do not lower the `fill_coverage >= 0.90` research gate.
- Do not add blocked or regime-incomplete symbols to this paper config.
- Runtime paper order submission is active only for the governed 9-symbol manifest above.

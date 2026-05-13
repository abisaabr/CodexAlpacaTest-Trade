# Paper Order Arming Guardrail - 2026-05-05

## Change

The paper-runner entrypoints now require the operator CLI flag
`--submit-paper-orders` before broker-facing paper order submission can be
enabled. A YAML config value of `execution.submit_paper_orders: true` is no
longer enough to arm submission by itself.

## Patched Entrypoints

- `scripts/run_multi_ticker_portfolio_paper_trader.py`
- `scripts/run_multi_ticker_portable_daemon.py`
- `scripts/run_multi_ticker_eod_close_guard.py`
- `scripts/run_qqq_portfolio_paper_trader.py`

## Why

The GCP audit found the terminated legacy VM `multi-ticker-trader-v1` has
startup metadata containing `--submit-paper-orders` and `DRY_RUN=false`. The
explicit CLI flag remains the broker-facing operator action, but configs should
not silently arm order submission if a command is run without that flag.

## Validation

Focused gate:

```powershell
python -m pytest -q `
  tests\test_runner_submit_order_arming.py `
  tests\test_multi_ticker_portfolio.py `
  tests\test_build_qqq_paper_launch_pack.py `
  tests\test_build_qqq_shadow_validation_packet.py
```

Result: `72 passed`

## Current Launch Posture

- QQQ/SPY/QQQ+SPY watchdog: broker-free, `--no-submit-paper-orders`
- Broker-facing paper orders: not started
- Required broker-facing action: explicit operator command using
  `--submit-paper-orders`

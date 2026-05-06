# Paper Runner EOD Flatten Hardening - 2026-05-06

## Objective

Make the PAPER runner flatten all broker positions before the regular session close, including broker positions that are not recognized in local strategy/session state.

This is broker-facing PAPER safety hardening only. It does not change the live manifest, global risk policy, promotion gates, or strategy selection.

## Runtime Behavior

- `execution.eod_flatten_minutes_before_close` now defaults to `[10, 2]`.
- At each configured checkpoint, the runner blocks new entries and runs the same end-of-day cleanup safeguard used during final shutdown.
- The safeguard attempts known local trade exits, known trade cleanup orders, and broker-position cleanup with `flatten_all_remaining=true`.
- Because the checkpoints run before 16:00 ET, option cleanup orders use market orders under the existing RTH cleanup-order rule.
- Checkpoints are persisted in session state as `eod_flatten_checkpoints_completed`, so each scheduled flatten fires once per session checkpoint.
- If the stock data frame is empty near close, the run loop still checks the clock-derived RTH minute and can trigger the scheduled broker-position cleanup path.

## Configured Paper Portfolio

Updated config:

- `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`

Execution setting:

```yaml
eod_flatten_minutes_before_close: [10, 2]
```

## Code Paths

- `alpaca_lab/multi_ticker_portfolio/config.py`
- `alpaca_lab/multi_ticker_portfolio/trader.py`
- `tests/test_multi_ticker_portfolio.py`

New event journal type:

- `scheduled_eod_flatten`

Scheduled flatten reasons:

- `scheduled_end_of_day_flatten_10m_before_close`
- `scheduled_end_of_day_flatten_2m_before_close`

## Verification

Commands run:

```powershell
uv run --no-project --python C:\Users\abisa\AppData\Local\Programs\Python\Python311\python.exe --with ".[dev]" pytest tests\test_multi_ticker_portfolio.py -q -k "scheduled_eod_flatten or reconcile_and_trade_triggers_scheduled_eod_flatten or finalize_session_retries_reconciliation_until_broker_is_flat or startup_check_auto_flattens_unexpected_positions"
uv run --no-project --python C:\Users\abisa\AppData\Local\Programs\Python\Python311\python.exe --with ".[dev]" pytest tests\test_runner_submit_order_arming.py -q
uv run --no-project --python C:\Users\abisa\AppData\Local\Programs\Python\Python311\python.exe --with ".[dev]" pytest tests\test_multi_ticker_portfolio.py -q
```

Results:

- Targeted multi-ticker/EOD tests: `4 passed, 62 deselected`
- Paper order arming tests: `4 passed`
- Full multi-ticker portfolio test file: `66 passed`

## Operational Notes

- This does not start or restart a paper trader.
- This does not widen order submission arming.
- This does not lower any promotion gate.
- For the next RTH paper run, verify the session event journal contains `scheduled_eod_flatten` events at the 10-minute and 2-minute checkpoints and verify the ending broker-position audit is flat.

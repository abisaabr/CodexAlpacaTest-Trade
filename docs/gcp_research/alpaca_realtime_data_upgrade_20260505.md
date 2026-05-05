# Alpaca Real-Time Data Upgrade - 2026-05-05

## Decision

Use the paid Alpaca market-data entitlement in the governed QQQ+SPY paper
validation lane:

- Stock bars: `sip`
- Option snapshots: `opra`

This keeps order submission unchanged. The realtime config remains
broker-free by default with `execution.submit_paper_orders: false`, and the
runner still requires the explicit CLI flag `--submit-paper-orders` before any
paper order submission can be armed.

## Code Change

The option feed setting already existed in portfolio configs, but the
multi-symbol option snapshot call did not pass it through. The runner now sends
the configured feed to Alpaca for option snapshots.

Patched paths:

- `alpaca_lab/brokers/alpaca.py`
- `alpaca_lab/multi_ticker_portfolio/trader.py`
- `alpaca_lab/qqq_portfolio/trader.py`
- `config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml`

## Validation

Local no-order preflight:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py `
  --portfolio-config config\qqq_spy_regime_complete_realtime_paper_portfolio.yaml `
  --startup-preflight `
  --no-submit-paper-orders
```

Result: `startup_preflight_passed`

Local no-order run-once:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py `
  --portfolio-config config\qqq_spy_regime_complete_realtime_paper_portfolio.yaml `
  --run-once `
  --no-submit-paper-orders
```

Result: `ran_once`

The run-once logs showed:

- stock bars request with `feed=sip`
- option snapshots requests with `feed=opra`
- Alpaca responses: HTTP `200`
- broker-facing orders submitted: `false`

Focused test gate:

```powershell
python -m pytest -q `
  tests\test_multi_ticker_portfolio.py `
  tests\test_runner_submit_order_arming.py `
  tests\test_build_qqq_paper_launch_pack.py `
  tests\test_build_qqq_shadow_validation_packet.py
```

Result: `72 passed`

## WebSocket Path

The current governed runner still uses REST polling. Alpaca's WebSocket streams
can be added as a separate runtime upgrade after the SIP/OPRA REST lane is
stable. The first institutional step should be quote/tick ingestion and
heartbeat monitoring in shadow mode, not direct strategy execution off the
stream.

## Hard Rule

Do not combine the data-feed upgrade with broker-facing order submission until
the GCP no-order realtime canary has passed and its artifacts are mirrored to
GCS.

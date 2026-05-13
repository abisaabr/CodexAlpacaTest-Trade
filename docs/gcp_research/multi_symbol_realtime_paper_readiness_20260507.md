# Multi-Symbol Realtime PAPER Readiness - 2026-05-07

Updated: 2026-05-06 20:20 ET

## Status

- Broker-facing mode: PAPER only.
- Active local paper trader at handoff: none.
- Active GCP research VMs/jobs at handoff: none observed.
- Operator approval recorded: May 7, 2026 RTH PAPER order submission is approved for appropriate runtime-compatible governed-validation strategies, conditional on the preflight and broker-state gates below.
- Governed-validation manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260506.yaml`.
- No-submit/preflight paper portfolio config: `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`.
- Armed May 7 PAPER portfolio config: `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`.
- Runtime order submission default remains off in the no-submit/preflight config. The armed May 7 config sets `execution.submit_paper_orders: true` and `execution.paper_order_arming_mode: config_explicit`, which is the only config-driven arming mode accepted by the runner.

## Activated Runtime-Compatible Strategy Set

- Runtime-compatible governed-validation strategies: 136.
- Symbols: AMD, AMZN, AVGO, GOOGL, IWM, MSFT, QQQ, SPY, TSLA, TSM.
- Regimes represented in manifest: bull, bear, choppy.
- Execution symbols for May 7 PAPER readiness: QQQ, SPY, IWM, AMD, AMZN, MSFT, TSLA, AVGO, GOOGL, TSM.
- Realtime feeds in config: stock `sip`, option `opra`.
- Realtime shadow subscription plan: `reports/multi_ticker_portfolio/realtime_shadow/multi_symbol_governed_20260507_plan/realtime_shadow_subscription_plan.json`.
- Realtime shadow plan size: 10 underlyings and 504 option symbols.

## Runtime Compatibility Holds

The manifest builder now refuses to silently translate unsupported multi-leg research structures into one-leg PAPER runtime strategies.

Held candidates:

- TSM bull `debit_call_vertical`: skipped with `unsupported_runtime_family_for_paper_manifest:debit_call_vertical`.
- IWM bull `bull_put_credit_spread`: three candidates skipped with `unsupported_runtime_family_for_paper_manifest:bull_put_credit_spread`.

Reason: the research backtester supports native multi-leg structures, but the governed-validation paper manifest builder only preserves exact runtime semantics for `single_leg` / `single_leg_repair` candidates today. Multi-leg candidates require a native paper-runtime mapping and a no-submit preflight/replay before broker-facing PAPER activation.

## Required Preflight Before PAPER Orders

Set the local primary ownership guard before preflight/launch:

```powershell
$env:MULTI_TICKER_MACHINE_LABEL = "local-primary-paper-20260507"
$env:MULTI_TICKER_OWNERSHIP_LEASE_PATH = "D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260507_ownership_lease.json"
```

The D-drive lease file was initialized and `run_multi_ticker_standby_failover_check.py` returned `status=ready` with the lease visible and currently unowned.

Run near the May 7 RTH open after fresh SIP stock bars are available:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --startup-preflight --no-submit-paper-orders
```

Expected after-hours behavior: preflight can fail only because stock bars are stale. The 2026-05-06 after-hours preflight reached Alpaca PAPER and validated option inventory, but failed with stale stock bars for all ten symbols.

Do not launch broker-facing PAPER order submission unless:

- The startup preflight returns `startup_check_status: passed`.
- Broker endpoint is PAPER.
- No duplicate paper trader process is running.
- PAPER open orders and positions are reviewed.
- Any unexpected open orders or positions are either reconciled or explicitly accepted before launch.
- The launch uses the dedicated armed config, not the no-submit/preflight config.
- The armed config is local-primary self-contained for May 7: `ownership.lease_path` points to the D-drive lease and `ownership.machine_label` is `local-primary-paper-20260507`. A standby machine must override the machine label before use.
- The launched strategy set remains restricted to runtime-compatible governed-validation strategies generated from eligible promotion-review packets.

## PAPER Order-Submitting Command

Authorized May 7 PAPER launch command after the fresh preflight passes:

```powershell
$env:MULTI_TICKER_MACHINE_LABEL = "local-primary-paper-20260507"
$env:MULTI_TICKER_OWNERSHIP_LEASE_PATH = "D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260507_ownership_lease.json"
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml
```

This is PAPER-only. It does not authorize live trading, live manifest edits, or risk-policy changes.

## EOD Flatten Guardrail

The paper runner is configured to block new entries and flatten all broker positions, including unexpected/unrecognized positions, at both 10 and 2 minutes before close:

- `eod_flatten_minutes_before_close: [10, 2]`
- `auto_flatten_unexpected_positions: true`
- `allow_market_exit_fallback: true`

## Overnight Research Loop

Next research-only priority:

1. Patch native multi-leg governed-validation manifest/runtime mapping for credit/debit verticals, starting with IWM bull `bull_put_credit_spread`.
2. Run no-submit paper-runtime compatibility checks for those multi-leg candidates.
3. Continue GCP regime-completion refinement for blocked symbols only after current PAPER readiness remains clean.
4. Keep `fill_coverage >= 0.90`, positive full/test PnL, trade-count, loser-cluster, and portfolio-context gates intact.

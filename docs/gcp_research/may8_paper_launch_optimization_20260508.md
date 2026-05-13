# May 8 Paper Launch Optimization

Generated: 2026-05-08

Scope: launch-safe improvements for `config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`.

## Changes Kept

- Reduced PAPER trader poll interval from `20s` to `15s`.
- Reduced order-status poll interval from `10s` to `5s`.
- Reduced order-fill timeout from `45s` to `35s` so stale unfilled limits are cancelled sooner.
- Reduced option contract refresh interval from `15m` to `10m`.
- Tightened option quote stale gate from `120s` to `75s`.
- Tightened stock freshness gate from `180s` to `120s`.
- Tightened max relative option spread from `0.35` to `0.30`.
- Added selected-leg `quote_time`, `spread_pct`, and `freshness_seconds` to runtime trade payloads and order-decision audit logs.
- Added optional `risk.regime_risk_scales` support to the paper trader and projection tool so future controlled risk overlays can be simulated and enforced consistently.

## Risk Change Rejected

A test run downweighted the bear sleeve to `0.5x` because the prior reconstruction showed negative bear-sleeve PnL. The rerun was worse:

- Optimized-test ending equity: `$27,696.51`
- Optimized-test max drawdown: `-29.132%`
- Prior neutral-risk ending equity: `$28,081.23`
- Prior neutral-risk max drawdown: `-25.4975%`

The May 8 launch config therefore keeps neutral regime scaling:

```yaml
risk:
  regime_risk_scales:
    bull: 1.0
    bear: 1.0
    choppy: 1.0
```

## Validation

- `python -m py_compile alpaca_lab\multi_ticker_portfolio\config.py alpaca_lab\multi_ticker_portfolio\trader.py scripts\build_portfolio_growth_projection.py`
- `python -m pytest tests\test_multi_ticker_portfolio.py tests\test_runner_submit_order_arming.py tests\test_build_multi_ticker_paper_postmortem.py -q`
- `python -m pytest tests\test_build_portfolio_growth_projection.py tests\test_build_projection_calendar.py tests\test_build_gcs_projection_calendar.py -q`
- `python -m pytest -q`

Full suite result: `296 passed, 1 warning`.

## Launch Interpretation

The kept changes improve freshness, cancellation speed, spread discipline, and post-session auditability without changing live manifests, lowering promotion gates, or adding untested strategy families. The rejected bear downweight is documented because it looked plausible from regime PnL but failed the production-risk projection check.

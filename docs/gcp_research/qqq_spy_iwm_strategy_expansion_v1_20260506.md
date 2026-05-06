# QQQ/SPY/IWM Strategy Expansion V1 - 2026-05-06

## Scope

This is a research-only expansion pass for the option-aware strategy repo. It does not
start trading, does not change live manifests, does not change global risk policy, and
does not lower the `fill_coverage >= 0.90` promotion gate.

The active local PAPER trader remains the broker-facing runtime. GCP is reserved for
non-broker-facing research shards.

## Offline Strategy Expansion

The generic regime rescue generator was expanded beyond single-leg repair and debit
call verticals so the tournament can test more realistic defined-risk structures:

- Bull momentum: `single_leg_repair`, `debit_call_vertical`,
  `bull_put_credit_spread`, `broken_wing_call_butterfly`.
- Bear signal-window refine: `single_leg_repair`, `debit_put_vertical`,
  `bear_call_credit_spread`, `broken_wing_put_butterfly`.
- IWM choppy quality filter: `single_leg_repair`, `debit_call_vertical`,
  `broken_wing_call_butterfly`.

The intent is to keep fill-friendly contract selection while adding payoff shapes that
can survive wider spreads and different regimes. Promotion remains based on generated
portfolio reports and promotion-review packets only.

## Realtime Strategy Lane

Realtime data should be used in two separate ways:

1. Execution improvement for already promoted/backtested strategy semantics.
2. New realtime-only alpha hypotheses that must be shadow-recorded before promotion.

Alpaca's official docs state that realtime stock and option data are available through
WebSocket market-data streams, and the option stream supports trades and quotes through
`v1beta1/{feed}` with `indicative` or `opra` depending on subscription. Alpaca also
documents that many subscriptions allow only one connection per endpoint, so the paper
runtime and shadow monitor must avoid duplicate stream connections.

The existing safe tool is:

```powershell
python scripts\run_multi_ticker_realtime_shadow_monitor.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml `
  --output-dir reports\multi_ticker_portfolio\realtime_shadow\multi_symbol_governed_20260506_full_rth `
  --max-option-symbols 900 `
  --duration-seconds 23400 `
  --stream
```

That command is no-submit shadow capture only. It records stream timing, stock bars,
OPRA option quotes, and paper trade updates without creating a second order-submitting
process.

## Realtime Hypotheses To Test

These are not promotion-ready until they are captured and replayed from durable stream
logs:

- Quote-persistence entry: enter only when the target option's bid/ask spread stays
  below the configured threshold for N consecutive quotes after the stock signal.
- Spread-tightening confirmation: prefer entries where the option spread tightens after
  a stock breakout instead of widening into the signal.
- Option-mid momentum confirmation: require the target option mid to move in the same
  direction as the stock signal before entry.
- Stock/option divergence reject: block entries where the stock signal fires but the
  option mid, bid size, or quote count does not confirm liquidity.
- First-valid-quote execution: measure fill quality using the first valid OPRA quote
  after a bar-close signal instead of waiting for the next REST polling cycle.
- Realtime exit acceleration: exit when OPRA quotes hit target/stop conditions before
  the next polling interval, while preserving the same tested target/stop semantics.
- Intraminute failed-breakout fade: research-only. This requires tick/quote replay and
  should not be promoted from one-minute historical bars alone.

## Dataset Roots

QQQ:

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars`
- Selected contracts: `gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts`
- Option bars: `gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars`

SPY:

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/SPY/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/SPY/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/SPY/365d_5x5/option_bars_silver/option_bars`

IWM:

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/IWM/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/IWM/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/IWM/365d_5x5/option_bars_silver/option_bars`

## Smoke Plan

Use a bounded first pass before launching full 828-template queues:

- Symbols: QQQ, SPY, IWM.
- Regimes: bull, bear, choppy.
- Bull profile: `momentum_refine`.
- Bear profile: `signal_window_refine`.
- Choppy profile: `timewindow_quality_filter`.
- Selector: `entry_liquidity_first_research_only`.
- Lag profiles: `0:60`, `10:60`, `30:120`.
- First wave: two workers per symbol, about 36 to 40 variants per worker.

Only after smoke shards populate valid fill metrics should the remaining tails be
launched. Full portfolio reports and promotion-review packets must remain the promotion
source of truth.

## Verification

Local tests passed:

```text
29 passed in 2.56s
```

Generator probe counts:

- QQQ full expansion: 828 templates.
- SPY full expansion: 828 templates.
- IWM full expansion: 828 templates.
- IWM choppy quality-only expansion: 288 templates.

## References

- Alpaca realtime stock data: `https://docs.alpaca.markets/docs/real-time-stock-pricing-data`
- Alpaca realtime option data: `https://docs.alpaca.markets/docs/real-time-option-data`
- Alpaca market-data WebSocket stream: `https://docs.alpaca.markets/docs/streaming-market-data`
- Existing shadow lane: `docs/gcp_research/realtime_shadow_execution_upgrade_20260506.md`

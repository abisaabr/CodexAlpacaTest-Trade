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

## Smoke Launch

Wave ID:

```text
qqq_spy_iwm_strategy_expansion_v1_20260506T1945Z
```

GCS root:

```text
gs://codexalpaca-control-us/research_results/qqq_spy_iwm_strategy_expansion_v1_20260506T1945Z/
```

Initial smoke workers:

- `qqq-rescue-c001-036-20260506v1`
- `qqq-rescue-c037-072-20260506v1`
- `spy-rescue-c001-036-20260506v1`
- `spy-rescue-c037-072-20260506v1`
- `iwm-rescue-c001-036-20260506v1`
- `iwm-rescue-c037-072-20260506v1`

Early sample outputs showed valid strategy-fill metrics, including 1.0 fill coverage
for sampled QQQ/SPY candidates and 0.992 fill coverage for a sampled IWM candidate.
The next larger chunks were launched to reduce duplicated data-staging overhead:

- `qqq-rescue-c073-144-20260506v1`
- `qqq-rescue-c145-216-20260506v1`
- `spy-rescue-c073-144-20260506v1`
- `spy-rescue-c145-216-20260506v1`
- `iwm-rescue-c073-144-20260506v1`
- `iwm-rescue-c145-216-20260506v1`

Current launched coverage is candidates 1-216 of 828 per symbol.

After the first partial aggregation, the wave was extended again to search for missing
bull coverage while keeping bear/choppy candidates:

- `qqq-rescue-c217-288-20260506v1`
- `qqq-rescue-c289-360-20260506v1`
- `spy-rescue-c217-288-20260506v1`
- `spy-rescue-c289-360-20260506v1`
- `iwm-rescue-c217-288-20260506v1`
- `iwm-rescue-c289-360-20260506v1`

Current launched coverage is candidates 1-360 of 828 per symbol.

Each worker is research-only, broker-facing false, live-manifest effect none, and
risk-policy effect none.

## Partial Aggregate

Partial aggregate path:

```text
gs://codexalpaca-control-us/research_results/qqq_spy_iwm_strategy_expansion_v1_20260506T1945Z/aggregate_partial/aggregate_partial_20260506T155635/
```

Strict partial report:

- Candidate/profile rows: 648.
- Eligible under strict candidate gates: 61.
- Packet decision: `research_only_blocked_regime_incomplete`.
- Missing regime: bull.
- Eligible families found so far: `single_leg_repair`.
- Eligible symbols/regimes found so far: QQQ bear/choppy, SPY bear/choppy, IWM bear.

This partial packet is not a final promotion packet. It is a checkpoint proving the
fill-friendly path is working and identifying bull as the next search target.

## Final Aggregate

Final aggregate path:

```text
gs://codexalpaca-control-us/research_results/qqq_spy_iwm_strategy_expansion_v1_20260506T1945Z/aggregate_final/aggregate_final_20260506T164345/
```

Local packet path:

```text
reports/gcp_research/qqq_spy_iwm_strategy_expansion_v1_20260506T1945Z/aggregate_final_20260506T164345/
```

Strict final report:

- Candidate/profile rows: 1,944.
- Eligible under strict candidate gates: 147.
- Packet decision: `ready_for_governed_validation_review`.
- Regime-complete for promotion review: true.
- Missing eligible regimes: none.
- Required regimes represented: bull, bear, choppy.
- Review candidates in packet: 20.
- Promotion scope: research/governed-validation review only.
- Broker-facing effect: none.
- Live-manifest effect: none.
- Risk-policy effect: none.

Representative eligible regimes:

- Bull: IWM `bull_put_credit_spread`, min net PnL `3218.285`, min test net PnL `5719.042`, strategy fill `0.9114`.
- Bear: QQQ `single_leg_repair`, min net PnL `2589.3`, min test net PnL `2045.219`, strategy fill `1.0`.
- Choppy: SPY `single_leg_repair`, min net PnL `3325.554`, min test net PnL `584.913`, strategy fill `0.9899`.

Capital-plan exposure in the strict aggregate:

- QQQ: 2 strategies, 50.00% research-only weight.
- SPY: 1 strategy, 32.68% research-only weight.
- IWM: 2 strategies, 17.32% research-only weight.

Full-population blocker counts remained useful for follow-up repair/redesign:

- `fill_coverage_below_0.90`: 1,096.
- `min_net_pnl_not_positive`: 1,700.
- `option_trades_below_20`: 42.
- `test_net_pnl_not_above_0`: 1,540.

Operational cleanup:

- The six remaining terminated expansion VMs were deleted after artifacts were present in GCS and the final aggregate was built:
  `qqq/spy/iwm-rescue-c073-144-20260506v1` and `qqq/spy/iwm-rescue-c217-288-20260506v1`.
- No duplicate broker-facing paper process was observed during the heartbeat check.
- No live-trading mode was started or changed.

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

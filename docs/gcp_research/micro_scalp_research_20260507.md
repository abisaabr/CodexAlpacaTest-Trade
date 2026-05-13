# Micro Scalp Research Notes - 2026-05-07

Updated: 2026-05-07 14:25 ET

## Decision

Do not add micro scalping to PAPER yet.

The first websocket shadow tests show that blind sub-minute option scalping is not robust under realistic aggressive execution economics. The spread is too large relative to a 1% target when we model buy-at-ask and sell-at-bid.

## Inputs

Shadow data:

```text
D:\codexalpaca_runtime\runs\microstructure_shadow_stream_fastwriter_20260507T1340ET\realtime_shadow_events.jsonl
```

Mirrored GCS root:

```text
gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/
```

The source shadow stream was no-submit and did not change broker state.

Research outputs:

```text
reports\gcp_research\micro_scalp_shadow_20260507T1410ET\
reports\gcp_research\micro_scalp_signal_grid_compact_20260507T1422ET\
reports\gcp_research\micro_scalp_signal_grid_liquid_targets_20260507T1430ET\
```

GCS mirrors:

```text
gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/micro_scalp_shadow_20260507T1410ET/
gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/micro_scalp_signal_grid_compact_20260507T1422ET/
gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/micro_scalp_signal_grid_liquid_targets_20260507T1430ET/
gs://codexalpaca-control-us/gcp_research/micro_scalp_research_20260507.md
```

## Scripts Added

- `scripts/analyze_micro_scalp_shadow.py`
- `scripts/analyze_micro_scalp_signal_grid.py`
- `tests/test_micro_scalp_shadow_analysis.py`

Both analyzers are research-only. They assume aggressive option execution:

- Entry: buy at ask
- Exit: sell at bid
- Fees: configurable per contract
- No midpoint-fill credit
- No hidden liquidity assumption

This is deliberately conservative. If a micro strategy cannot survive ask-to-bid economics in replay, it should not be promoted to the paper trader.

Validation:

```powershell
python -m pytest tests\test_micro_scalp_shadow_analysis.py tests\test_option_backtest_greek_selector.py -q
python -m py_compile scripts\analyze_micro_scalp_shadow.py scripts\analyze_micro_scalp_signal_grid.py
```

Result: `6 passed`; both analyzers compile.

## Blind 1% Scout

Command:

```powershell
python scripts\analyze_micro_scalp_shadow.py `
  --events-jsonl D:\codexalpaca_runtime\runs\microstructure_shadow_stream_fastwriter_20260507T1340ET\realtime_shadow_events.jsonl `
  --output-dir reports\gcp_research\micro_scalp_shadow_20260507T1410ET `
  --target-pct 0.01 `
  --stop-pct 0.006 `
  --max-hold-seconds 60 `
  --entry-stride 10 `
  --min-premium 0.15 `
  --max-premium 12 `
  --max-relative-spread 0.04 `
  --max-absolute-spread 0.20 `
  --min-quote-size 1 `
  --fee-per-contract 0.65 `
  --top-n 100
```

Result:

- Contracts analyzed: 359
- Simulated sampled entries: 74,726
- Winning trades: 539
- Target wins: 539
- Average net PnL per contract: -4.5013
- Total net PnL across sampled entries: -336,367.8

Interpretation: blind high-frequency 1% option scalps are not viable in this sample. Rare wins exist, mostly QQQ/SPY/TSLA bursts, but spread and fees dominate.

## Causal Momentum Grid

Command:

```powershell
python scripts\analyze_micro_scalp_signal_grid.py `
  --events-jsonl D:\codexalpaca_runtime\runs\microstructure_shadow_stream_fastwriter_20260507T1340ET\realtime_shadow_events.jsonl `
  --output-dir reports\gcp_research\micro_scalp_signal_grid_compact_20260507T1422ET `
  --lookbacks 1,3,5 `
  --momentum-thresholds 0.005,0.01,0.015 `
  --targets 0.01,0.02 `
  --stops 0.006,0.01 `
  --max-holds 15,30 `
  --min-premium 0.15 `
  --max-premium 12 `
  --max-relative-spread 0.04 `
  --max-absolute-spread 0.20 `
  --min-quote-size 1 `
  --fee-per-contract 0.65 `
  --max-contracts 80
```

Best compact grid:

- Lookback: 1 second
- Momentum threshold: 1.5%
- Target: 1%
- Stop: 0.6%
- Max hold: 15 seconds
- Trades: 27,106
- Winning trades: 3
- Average net PnL per contract: -3.0523
- Total net PnL: -82,736.8

Interpretation: even with causal quote momentum, short-horizon option scalping loses under taker economics in this sample.

## Liquid 2-5% Target Grid

Command:

```powershell
python scripts\analyze_micro_scalp_signal_grid.py `
  --events-jsonl D:\codexalpaca_runtime\runs\microstructure_shadow_stream_fastwriter_20260507T1340ET\realtime_shadow_events.jsonl `
  --output-dir reports\gcp_research\micro_scalp_signal_grid_liquid_targets_20260507T1430ET `
  --underlyings QQQ,SPY,IWM `
  --lookbacks 1,3,5,10 `
  --momentum-thresholds 0.01,0.015,0.02,0.03 `
  --targets 0.02,0.03,0.05 `
  --stops 0.01,0.015,0.02 `
  --max-holds 15,30,60 `
  --min-premium 0.15 `
  --max-premium 12 `
  --max-relative-spread 0.04 `
  --max-absolute-spread 0.20 `
  --min-quote-size 1 `
  --fee-per-contract 0.65 `
  --max-contracts 120
```

Best liquid-target grid:

- Underlyings: QQQ, SPY, IWM
- Contracts analyzed: 120
- Grid count: 432
- Lookback: 1 second
- Momentum threshold: 3%
- Target: 2%
- Stop: 2%
- Max hold: 15 seconds
- Trades: 3,531
- Average net PnL per contract: -2.4396
- Total net PnL: -8,614.3
- Win rate: 0.1982%

Interpretation: widening the target to 2-5% and restricting to liquid underlyings still did not overcome taker spread and fees in this sample.

## What This Means

The practical blocker is not websocket speed. The fast-writer websocket shadow already showed sub-second option quote/trade observation. The blocker is execution economics:

- A 1% option premium target is often smaller than spread plus fees.
- Bid/ask spread widens sharply outside the most liquid strikes.
- Quote momentum entries cluster during fast reprices, where the exit bid can still lag the entry ask.
- Many repeated micro entries would amplify friction faster than edge.

## Best Next Direction

Do not pursue “100 trades for 1% each” as the default design.

Better research direction:

- Use websocket data for fast exits and quote-age/spread gates on already-promoted strategies.
- Test rarer event-driven scalps with larger expected move, for example 2-5% option targets where spread is less dominant.
- Restrict to QQQ/SPY/possibly TSLA only until the edge survives ask-to-bid replay.
- Require stock quote impulse plus option quote confirmation, not option quote momentum alone.
- Require longer OPRA quote/trade capture before any promotion packet.

Promotion remains blocked until a generated packet proves:

- strategy fill coverage >= 0.90;
- positive full and test PnL;
- sufficient trade count;
- robust quote-age/spread behavior;
- no severe loser clusters;
- portfolio-context compatibility.

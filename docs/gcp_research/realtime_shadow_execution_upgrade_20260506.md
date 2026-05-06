# Realtime Shadow Execution Upgrade - 2026-05-06

## Purpose

Add a no-submit realtime shadow lane for the governed multi-symbol paper portfolio.
The objective is to reduce execution-timing lag without changing promoted strategy
semantics or creating a second order-submitting process.

## Design

- Strategy signals remain bar-close compatible with the promotion/backtest stack.
- Stock market data is observed from the Alpaca SIP websocket `bars` and `updatedBars`
  channels.
- Option execution data is observed from the Alpaca OPRA websocket quote stream for
  the bootstrapped same-day and next-expiry candidate option contracts.
- Paper order/fill updates can be observed from the Alpaca paper trading stream.
- The shadow monitor never submits orders. It writes only a subscription plan,
  event JSONL, and a summary JSON.

This preserves the tested edge definition: the stock bar confirms the strategy
event, while the first valid realtime OPRA quote after that event is the candidate
execution price.

## Commands

Plan-only validation:

```powershell
python scripts\run_multi_ticker_realtime_shadow_monitor.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml `
  --output-dir reports\multi_ticker_portfolio\realtime_shadow\multi_symbol_governed_20260506_plan `
  --max-option-symbols 900
```

Realtime no-submit sample:

```powershell
python scripts\run_multi_ticker_realtime_shadow_monitor.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml `
  --output-dir reports\multi_ticker_portfolio\realtime_shadow\multi_symbol_governed_20260506_stream_sample_stats `
  --max-option-symbols 900 `
  --duration-seconds 20 `
  --stream
```

## Results

- Underlyings in scope: 9 (`QQQ`, `SPY`, `AMD`, `AMZN`, `MSFT`, `TSLA`, `AVGO`, `GOOGL`, `TSM`).
- Bootstrapped OPRA option subscriptions: 448.
- No-submit stream sample status: `completed`.
- Option quote events captured in the 20 second sample: 54,800.
- Latency sample count: 54,800.
- Latency p50: 9.329093 seconds.
- Latency p90: 15.594922 seconds.
- Latency p99: 17.288102 seconds.
- Max observed latency: 17.580932 seconds.

No paper orders were submitted by the realtime shadow monitor. The active order-submitting
paper trader process remained the existing local runner.

## Files

- `alpaca_lab/multi_ticker_portfolio/realtime_shadow.py`
- `scripts/run_multi_ticker_realtime_shadow_monitor.py`
- `tests/test_realtime_shadow_monitor.py`
- `reports/multi_ticker_portfolio/realtime_shadow/multi_symbol_governed_20260506_plan/realtime_shadow_subscription_plan.json`
- `reports/multi_ticker_portfolio/realtime_shadow/multi_symbol_governed_20260506_plan/realtime_shadow_summary.json`
- `reports/multi_ticker_portfolio/realtime_shadow/multi_symbol_governed_20260506_stream_sample_stats/realtime_shadow_subscription_plan.json`
- `reports/multi_ticker_portfolio/realtime_shadow/multi_symbol_governed_20260506_stream_sample_stats/realtime_shadow_events.jsonl`
- `reports/multi_ticker_portfolio/realtime_shadow/multi_symbol_governed_20260506_stream_sample_stats/realtime_shadow_summary.json`

## Next Step

Run the shadow lane for a full RTH window and compare:

- Time from SIP bar close to signal recognition.
- Time from signal recognition to first valid OPRA quote.
- Polling runner entry/exit timestamps versus realtime shadow timestamps.
- Any quote staleness or spread-gate rejection clusters.

Only after that comparison should the order-submitting runner move from REST polling
to event-driven execution.

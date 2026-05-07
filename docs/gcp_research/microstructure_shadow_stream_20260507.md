# Microstructure Websocket Shadow Stream - 2026-05-07

Updated: 2026-05-07 13:53 ET

## Scope

This was a no-submit realtime market-data shadow run while the May 7 PAPER trader remained active. It did not submit orders, restart the trader, change the live manifest, or change risk policy.

Active PAPER trader context:

- Config: `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`
- Manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260507.yaml`
- Strategy count: 160 across 10 tickers
- Runtime mode: PAPER order submission

## Runs

GCS artifact roots:

- Initial stream pilot: `gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_20260507T1322ET/microstructure_shadow_stream_20260507T1322ET/`
- Initial post-hoc summary: `gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_20260507T1322ET_posthoc/microstructure_shadow_stream_20260507T1322ET/realtime_shadow_posthoc_summary.json`
- Fast-writer stream pilot: `gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/`
- Control-plane mirror: `gs://codexalpaca-control-us/gcp_research/microstructure_shadow_stream_20260507.md`

### Initial Stream Pilot

Command:

```powershell
python scripts\run_multi_ticker_realtime_shadow_monitor.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml `
  --output-dir D:\codexalpaca_runtime\runs\microstructure_shadow_stream_20260507T1322ET `
  --max-option-symbols 900 `
  --duration-seconds 90 `
  --stream `
  --include-stock-quotes `
  --include-option-trades `
  --no-trade-updates
```

Result:

- Status: `stream_complete`
- Underlyings: 10
- Option contracts subscribed: 364
- Event counts: 25,936 option quotes, 110 option trades, 26,040 stock quotes, 10 stock bars
- Problem found: apparent option quote latency was tens of seconds because the recorder opened and closed the JSONL file on every event, creating callback backlog.

### Fast-Writer Stream Pilot

Patch applied before this run:

- `JsonlEventWriter` now keeps a persistent locked line-buffered file handle during the stream.
- `RealtimeShadowStats` now emits `latency_by_event_type` so stock bar completion latency is not mixed with quote/trade event latency.

Command:

```powershell
python scripts\run_multi_ticker_realtime_shadow_monitor.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml `
  --output-dir D:\codexalpaca_runtime\runs\microstructure_shadow_stream_fastwriter_20260507T1340ET `
  --max-option-symbols 900 `
  --duration-seconds 60 `
  --stream `
  --include-stock-quotes `
  --include-option-trades `
  --no-trade-updates
```

Result:

- Status: `stream_complete`
- Underlyings: 10
- Option contracts subscribed: 364
- Raw JSONL size: about 330 MB
- Event counts: 879,750 option quotes, 2,954 option trades, 41,037 stock quotes, 10 stock bars

Latency by event type:

- Option quote: p50 0.184s, p90 0.641s, p99 0.895s
- Option trade: p50 0.144s, p90 0.599s, p99 0.824s
- Stock quote: p50 0.000s, p90 0.027s, p99 0.115s
- Stock bar: p50 about 60s, expected because bar timestamps are the bar start, not a quote/trade arrival timestamp

Spread statistics:

- Option quote absolute spread p50 0.02, p90 0.11, p99 0.70
- Option quote relative spread p50 1.44%, p90 5.13%, p99 22.22%

## Interpretation

The websocket lane is viable for realtime shadowing and potentially for a future event-driven paper runner. The fast-writer pilot shows sub-second quote/trade observation for the selected option universe when the recorder is not the bottleneck.

This does not yet authorize sub-minute live or PAPER microstructure strategies. Promotion still requires a tick/quote replay path and a generated promotion-review packet that proves the edge survives spread, slippage, quote age, and order-lifecycle constraints.

## Runtime Issues Found

### Stale Heartbeat During Exit Waits

The active PAPER trader entered a degraded observability state during sequential exit-order waits:

- The process stayed alive and continued polling Alpaca order status.
- The session JSON and ownership lease could become stale while the runner waited through multiple exit attempts.
- The lease refreshed at the next loop boundary, but this can create false stale-runner alarms and increases duplicate-runner risk if a watchdog only reads the lease timestamp.

Patch applied:

- `MultiTickerPortfolioPaperTrader._wait_for_terminal_order` now renews runtime ownership during order waits.
- The wait loop saves session state when a session object is available.
- The May 7 runner was later restarted after the DNS failure described below.

### Transient Alpaca Order Poll Failure

At about 13:47 ET, the active PAPER trader process exited after a transient DNS/name-resolution failure while polling Alpaca order status for a QQQ exit order:

- Failed operation: `GET /v2/orders/d77407b1-cc40-4f38-982d-31da7a1e3d6d`
- Alpaca endpoint: PAPER
- Failure class: `NameResolutionError` / `requests.exceptions.ConnectionError`
- Immediate broker state: one stale open `QQQ260508C00697000` sell-to-close limit order remained.

Operator action taken:

- Cancelled stale open QQQ sell-to-close order `d77407b1-cc40-4f38-982d-31da7a1e3d6d`.
- Restarted exactly one PAPER trader process with the same armed May 7 config.
- Restarted process PID: `60160`.
- Daily loss gate triggered after restart at broker equity `24412.17`, so new entries are blocked and the runner is managing exits/EOD flatten behavior only.

Patch applied after restart:

- Order status polling now catches transient broker/API exceptions and continues waiting instead of terminating the process.
- Unknown order state is treated as cancelable at timeout to reduce stale-open-order risk.
- Cancel failures are logged and recorded as `cancel_requested: false` instead of crashing the runner.
- This patch is validated but will only apply to a subsequently started runner process.

Validation:

```powershell
python -m pytest tests\test_realtime_shadow_monitor.py tests\test_multi_ticker_portfolio.py -q
python -m py_compile alpaca_lab\multi_ticker_portfolio\trader.py alpaca_lab\multi_ticker_portfolio\realtime_shadow.py
```

Result: `78 passed`.

## Next Implementation Step

Do not wire websockets directly into order submission mid-session. The next safe build step is an explicit realtime market cache behind a config flag:

- stream SIP stock quotes/trades and OPRA option quotes/trades;
- maintain latest bid/ask/quote age/size per contract;
- keep polling/snapshot fallback for resilience;
- emit the strategy ID, contract, quote age, spread, and decision timestamp for every buy and sell;
- reject entries/exits when spread or quote age fails the strategy-specific gate;
- use the cache in no-submit shadow first, then in PAPER after replay and shadow packet agreement.

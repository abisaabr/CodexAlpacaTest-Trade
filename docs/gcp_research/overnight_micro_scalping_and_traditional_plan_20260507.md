# Overnight Micro Scalping And Traditional Backtest Plan

Updated: 2026-05-07 16:45 ET

Scope: PAPER-only research and preparation for the next RTH paper session. This plan does not authorize live trading. Microstructure candidates from short websocket shadow data must be labeled as operator-approved PAPER experiments unless a generated governed promotion-review packet clears the normal gates.

## Objectives

- Run rare-event microstructure backtests on QQQ, SPY, and IWM using websocket shadow data.
- Identify at least three viable microstructure candidates for a clearly labeled PAPER experiment if the replay evidence supports them.
- Continue traditional realtime-compatible backtests across the 20-symbol research universe.
- Preserve `fill_coverage >= 0.90`, trade-count, positive full/test PnL, loser-cluster, and portfolio-context checks for governed promotion.
- Prepare the next PAPER trader config only after fresh preflight, PAPER endpoint verification, no duplicate process, and clean broker PAPER state.

## Active Microstructure Wave

- Wave ID: `microstructure_rare_event_overnight_20260507T2030ET`
- GCS root: `gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/`
- Source events: `gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/realtime_shadow_events.jsonl`
- Profile: `rare_event_larger_move_v2`
- Grid count: `106920`
- Chunk count at 512 rows per worker: `209`
- First tranche launched: grid rows `1` through `4096`
- Active workers: `8`
- Broker-facing effect: `none`
- Live-manifest effect: `none`

The rare-event profile is intentionally stricter than the first microstructure smoke wave. It favors fewer entries, larger expected option moves, tight quote-age/spread gates, and fast exits over high-frequency churn.

## Current First-Tranche Status

Latest local sync from GCS showed all first-tranche workers still in `running_replay` with zero review-like candidates so far.

- `micro_event_c00001_00512`: `250/512`
- `micro_event_c00513_01024`: `225/512`
- `micro_event_c01025_01536`: `150/512`
- `micro_event_c01537_02048`: `125/512`
- `micro_event_c02049_02560`: `125/512`
- `micro_event_c02561_03072`: `225/512`
- `micro_event_c03073_03584`: `225/512`
- `micro_event_c03585_04096`: `125/512`

Capacity note: the first tranche uses eight `e2-standard-4` workers and consumes the current 32-vCPU quota.

## Continuation Patch

`scripts/launch_gcp_microstructure_event_replay_shards.ps1` now supports `-StartGridIndex`. This prevents the overnight loop from relaunching already-tested chunks after terminated workers are deleted.

Next microstructure continuation command after the first tranche completes and artifacts are synced:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\launch_gcp_microstructure_event_replay_shards.ps1 `
  -WaveId microstructure_rare_event_overnight_20260507T2030ET `
  -EventsJsonlUri 'gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/realtime_shadow_events.jsonl' `
  -Underlyings 'QQQ,SPY,IWM' `
  -Profile rare_event_larger_move_v2 `
  -InstanceSuffix '20260507r2' `
  -ChunkSize 512 `
  -StartGridIndex 4097 `
  -MaxLaunches 8 `
  -MaxContracts 0 `
  -MaxContractsPerUnderlying 120 `
  -MaxCreateAttempts 48 `
  -Processes 4 `
  -MachineType e2-standard-4
```

## Aggregation Recipe

After each tranche completes:

```powershell
$wave='microstructure_rare_event_overnight_20260507T2030ET'
gcloud storage rsync -r "gs://codexalpaca-control-us/research_results/$wave/workers" "reports\gcp_research\$wave\workers" --project codexalpaca
python scripts\aggregate_microstructure_event_replay.py `
  --workers-root "reports\gcp_research\$wave\workers" `
  --output-dir "reports\gcp_research\$wave\aggregate" `
  --wave-id $wave `
  --min-fill-coverage 0.90 `
  --min-trades 20 `
  --min-net-pnl 0 `
  --max-avg-spread-cost-to-target 0.65 `
  --max-review-candidates 50
gcloud storage rsync -r "reports\gcp_research\$wave\aggregate" "gs://codexalpaca-control-us/research_results/$wave/aggregate/" --project codexalpaca
```

Only terminated worker VMs should be deleted, and only after worker artifacts exist in GCS.

## Traditional 20-Ticker Backtest Wave

- Wave ID: `traditional_20ticker_realtime_compatible_overnight_20260507T2045ET`
- Symbols: `AAPL, AMD, AMZN, INTC, IWM, META, MSFT, NVDA, SPY, TSLA, AVGO, GOOGL, MU, NFLX, ORCL, PLTR, QQQ, TSM, XLE, XOM`
- Local root: `reports/gcp_research/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/`
- GCS root: `gs://codexalpaca-control-us/research_results/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/`
- Prepared launch rows observed for 19 symbols; the local prepare-only process was still running while XOM preparation was pending.

Traditional launches should wait until the microstructure tranche frees quota, then run in small batches to avoid starving microstructure continuation.

## Decision Rules For Tomorrow

- Governed promotion requires generated packet eligibility. Do not call short-shadow microstructure leads governed-promoted unless they clear a generated packet with data lineage.
- If the operator wants at least three microstructure strategies in PAPER tomorrow before full historical evidence exists, create a separate operator-approved PAPER experiment manifest with explicit labels, reduced risk, and realtime telemetry requirements.
- Do not use the May 7 strategy-attributed ledger for per-strategy promotion until broker-fill reconciliation is complete.
- PAPER trader startup still requires fresh RTH preflight, PAPER endpoint, no duplicate trader, no unexpected PAPER orders/positions, and active heartbeat/log preservation.


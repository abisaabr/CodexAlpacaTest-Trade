# Microstructure And Scalping Parallel Research Plan - 2026-05-08

Scope: PAPER-only research and backtesting. This plan does not start live trading, does not change the checked-in live manifest, and does not lower fill, risk, or promotion gates.

## Current Cloud State

- GCP project: `codexalpaca`
- Control bucket root: `gs://codexalpaca-control-us/research_results/`
- Active microstructure wave: `microstructure_rare_event_overnight_20260507T2030ET`
- Wave GCS root: `gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/`
- Event source: `gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/realtime_shadow_events.jsonl`
- Profile: `rare_event_larger_move_v2`
- Grid size: `106920`
- Chunk size: `512`
- Broker-facing effect: `none`
- Live manifest effect: `none`

Existing worker artifacts are non-contiguous. Known completed ranges cover `1-6144`, `64513-66048`, and `86017-96256`. The exhaustive gap-fill plan must backfill:

- `6145-64512`
- `66049-86016`
- `96257-106920`

## First May 8 Backfill Tranche

Launched at 2026-05-08 morning ET:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\launch_gcp_microstructure_event_replay_shards.ps1 `
  -WaveId microstructure_rare_event_overnight_20260507T2030ET `
  -EventsJsonlUri 'gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/realtime_shadow_events.jsonl' `
  -Underlyings 'QQQ,SPY,IWM' `
  -Profile rare_event_larger_move_v2 `
  -InstanceSuffix '20260508bf1' `
  -ChunkSize 512 `
  -StartGridIndex 6145 `
  -MaxLaunches 8 `
  -MaxContracts 0 `
  -MaxContractsPerUnderlying 120 `
  -MaxCreateAttempts 48 `
  -Processes 4 `
  -MachineType e2-standard-4
```

Launched workers:

- `micro-event-c06145-06656-20260508bf1`
- `micro-event-c06657-07168-20260508bf1`
- `micro-event-c07169-07680-20260508bf1`
- `micro-event-c07681-08192-20260508bf1`
- `micro-event-c08193-08704-20260508bf1`
- `micro-event-c08705-09216-20260508bf1`
- `micro-event-c09217-09728-20260508bf1`
- `micro-event-c09729-10240-20260508bf1`

## Multi-Agent / Multi-Phase Operating Model

Agent 1: Microstructure gap-fill executor

- Owns the `rare_event_larger_move_v2` exhaustive backfill over QQQ/SPY/IWM.
- Runs 8 `e2-standard-4` workers per tranche to fit the current 32-vCPU quota.
- Uses `-StartGridIndex` to avoid relaunching completed chunks.
- Deletes only `TERMINATED` workers after GCS artifacts are present and locally synced.

Agent 2: Aggregation and candidate classifier

- Runs strict aggregation after each tranche:

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

- Any microstructure candidate remains `research_only` unless the output packet and runtime compatibility are explicitly documented.
- Short single-day websocket evidence can support an operator-approved PAPER experiment only if clearly labeled and risk-reduced; it is not governed promotion by itself.

Agent 3: Traditional scalping wave executor

- Runs realtime-compatible 365d strategy waves for stock-price and option-price scalping families.
- Prioritizes lead-bearing and liquid symbols first: `QQQ, SPY, IWM, TSM, AVGO, AMD, MSFT, GOOGL, AAPL, NVDA`.
- Uses generated promotion packets only; blocked or regime-incomplete packets are not added to PAPER configs.

Agent 4: Greek / volatility scalping executor

- Runs delta-target, vertical, butterfly, condor, and Greek-filtered variants where dense option datasets exist.
- Keeps multi-leg runtime semantics explicit: native multi-leg strategies must carry `packet_translated_to_runtime_native_multileg`.
- Treats dynamic gamma scalping and volatility-surface scalping as research gaps until hedge-loop and IV/skew replay support exists.

Agent 5: Paper-readiness guard

- Verifies that research VMs do not start broker-facing sessions.
- Keeps May 8 PAPER preflight separate from research.
- Checks no duplicate `run_multi_ticker_portfolio_paper_trader.py` process, PAPER endpoint, clean broker orders/positions, D-drive runtime paths, and account equity before any PAPER launch.

## Next Tranche Sequence

After the active `6145-10240` tranche terminates and artifacts are synced:

1. Aggregate and classify.
2. Launch the next backfill tranche at `StartGridIndex=10241`.
3. Continue in 8-worker increments through `64512`.
4. Skip already-present chunks `64513-66048`.
5. Resume at `66049` through `86016`.
6. Skip already-present chunks `86017-96256`.
7. Resume at `96257` through `106920`.

Use a new suffix per tranche, for example `20260508bf2`, `20260508bf3`, etc.

## Promotion Rules

Do not promote microstructure candidates into governed validation unless the generated packet says they are eligible. Preserve these gates:

- `fill_coverage >= 0.90`
- `min_trades >= 20`
- `net_pnl > 0`
- spread-cost-to-target within strict limits
- explicit event-stream lineage
- runtime compatibility documented

Traditional 365d strategy candidates may be added to governed-validation manifests only when their generated promotion-review packet clears the configured gates. No live manifest changes are authorized by this plan.

## Known Gaps For Future Patches

- The microstructure launcher lacks a true `-Help`/usage mode; invoking it without valid inputs can build a grid and then fail on GCS lookup.
- The exhaustive wave currently uses a single May 7 websocket shadow stream. Broader ticker coverage requires collecting May 8+ shadow events for all active paper symbols.
- Dynamic gamma scalping needs a hedge/re-hedge simulator and separate stock hedge PnL attribution before it can be treated as institutional-grade.
- Volatility scalping needs IV-rank, realized-volatility, and skew/surface features rather than using Greeks alone.

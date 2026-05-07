# Microstructure Event Replay Wave - 2026-05-07

Updated: 2026-05-07 14:55 ET

## Scope

This is a research-only microstructure replay wave for fast exits, quote-age/spread gates, and rare event-driven entries with larger option targets. It does not start trading, does not change the live manifest, and does not authorize new PAPER strategies.

Active PAPER trader remains separate:

- PID: `60160`
- Config: `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`
- Mode: PAPER order submission
- Safety posture: monitor only; do not start a duplicate process.

## Code Added

Commits:

- `6b36b07` - `Add microstructure event replay wave`
- `a8e3458` - `Fix microstructure GCP metadata escaping`
- `ac9124b` - `Fix microstructure worker metadata underlyings`

Scripts:

- `scripts/build_microstructure_research_grid.py`
- `scripts/run_microstructure_event_replay_shard.py`
- `scripts/aggregate_microstructure_event_replay.py`
- `scripts/gcp_microstructure_event_replay_shard.sh`
- `scripts/launch_gcp_microstructure_event_replay_shards.ps1`

Validation:

```powershell
python -m pytest tests\test_micro_scalp_shadow_analysis.py tests\test_option_backtest_greek_selector.py -q
python -m py_compile scripts\build_microstructure_research_grid.py scripts\run_microstructure_event_replay_shard.py scripts\aggregate_microstructure_event_replay.py
```

Result: `8 passed`; compile checks passed.

## Input Data

Websocket shadow input:

```text
gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/realtime_shadow_events.jsonl
```

Important limitation: this is a short May 7 websocket shadow capture, not a long historical quote dataset. This wave can identify microstructure leads, but it is not by itself promotion-grade historical evidence.

## Local Smoke

Command:

```powershell
python scripts\build_microstructure_research_grid.py `
  --wave-id microstructure_exhaustive_20260507T1435Z `
  --output-dir reports\gcp_research\microstructure_exhaustive_20260507T1435Z\inputs `
  --underlyings QQQ,SPY,IWM `
  --profile smoke `
  --chunk-size 32

python scripts\run_microstructure_event_replay_shard.py `
  --events-jsonl D:\codexalpaca_runtime\runs\microstructure_shadow_stream_fastwriter_20260507T1340ET\realtime_shadow_events.jsonl `
  --grid-jsonl reports\gcp_research\microstructure_exhaustive_20260507T1435Z\inputs\microstructure_research_grid.jsonl `
  --output-dir reports\gcp_research\microstructure_exhaustive_20260507T1435Z\local_smoke `
  --wave-id microstructure_exhaustive_20260507T1435Z `
  --worker-id local_smoke `
  --grid-start-index 1 `
  --grid-count 16 `
  --underlyings QQQ,SPY,IWM `
  --max-contracts 20
```

Result:

- Grid rows tested: 16
- Contracts analyzed: 20
- Review-like grids: 0
- Best local smoke grid: `micro_00009`, 116 trades, fill coverage 1.0, net PnL -348.8, average net PnL -3.0069

Interpretation: the first local smoke stayed negative after ask-to-bid execution economics. This matches the earlier micro-scalp finding and justifies using GCP for broader search rather than activating anything.

## GCP Exhaustive Wave

Wave ID:

```text
microstructure_event_replay_exhaustive_20260507T1445Z
```

GCS root:

```text
gs://codexalpaca-control-us/research_results/microstructure_event_replay_exhaustive_20260507T1445Z/
```

Launch command:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\launch_gcp_microstructure_event_replay_shards.ps1 `
  -WaveId microstructure_event_replay_exhaustive_20260507T1445Z `
  -Profile liquid_exhaustive_v1 `
  -Underlyings QQQ,SPY,IWM,AMZN,MSFT,AAPL,AMD,GOOGL,TSM,TSLA `
  -ChunkSize 256 `
  -MaxLaunches 8 `
  -MaxCreateAttempts 32 `
  -MaxContracts 240 `
  -MaxContractsPerUnderlying 0 `
  -MachineType e2-standard-4 `
  -InstanceSuffix 20260507m2
```

Grid:

- Profile: `liquid_exhaustive_v1`
- Grid rows: 165,600
- Chunk count: 647
- First launch tranche target: up to 8 workers
- Per worker: 256 grid rows, top 240 option contracts by quote activity

Signal families in this wave:

- `option_momentum`
- `stock_impulse_option_confirm`
- `stock_impulse_only`
- `spread_compression_momentum`

Execution semantics:

- Entry at ask
- Exit at bid
- Quote-age gate at entry and exit
- Spread gate at entry
- Spread-widen exit
- Target, stop, trailing, and time exits
- Fee: 0.65 per contract side

Active/observed workers at handoff:

- `micro_event_c00257_00512`
- `micro_event_c01281_01536`
- `micro_event_c02305_02560`
- `micro_event_c03329_03584`
- `micro_event_c04353_04608`
- `micro_event_c05377_05632`
- `micro_event_c06401_06656`
- `micro_event_c06913_07168` was provisioning during the last check

Some earlier rows were skipped due zone/capacity behavior during launch; use `microstructure_event_replay_launch_rows.json` to identify remaining chunks.

## Aggregation Recipe

After workers complete:

```powershell
gsutil -m rsync -r `
  gs://codexalpaca-control-us/research_results/microstructure_event_replay_exhaustive_20260507T1445Z/workers `
  reports\gcp_research\microstructure_event_replay_exhaustive_20260507T1445Z\workers

python scripts\aggregate_microstructure_event_replay.py `
  --workers-root reports\gcp_research\microstructure_event_replay_exhaustive_20260507T1445Z\workers `
  --output-dir reports\gcp_research\microstructure_event_replay_exhaustive_20260507T1445Z\aggregate `
  --wave-id microstructure_event_replay_exhaustive_20260507T1445Z `
  --min-fill-coverage 0.90 `
  --min-trades 20 `
  --min-net-pnl 0

gsutil -m rsync -r `
  reports\gcp_research\microstructure_event_replay_exhaustive_20260507T1445Z\aggregate `
  gs://codexalpaca-control-us/research_results/microstructure_event_replay_exhaustive_20260507T1445Z/aggregate/
```

## Promotion Posture

No microstructure strategy is PAPER-authorized from this wave yet.

A candidate can become a review lead only if it clears:

- fill coverage >= 0.90;
- at least 20 filled trades;
- positive total net PnL;
- positive average net PnL;
- realistic quote-age and spread behavior;
- no severe loser cluster after longer evidence is available.

Even if this short-capture wave finds positive leads, the next step is longer OPRA/SIP shadow capture or historical quote download support, then a governed promotion-review packet. Do not add these strategies to PAPER directly from this scouting wave.

## V2 Executability Upgrade - 2026-05-07

The replay lane now models additional execution realism before treating a signal as filled:

- `entry_latency_seconds` and `exit_latency_seconds` delay fills after a signal/exit trigger.
- `entry_fill_wait_seconds` and `exit_fill_wait_seconds` bound how long the simulator can wait for a usable quote after latency.
- `max_entry_chase_pct` rejects entries where the option ask runs too far away after the signal.
- `max_spread_cost_to_target` rejects entries where estimated round-trip spread cost is too large relative to the target profit.
- `avg_spread_cost_to_target` is emitted into shard summaries and enforced by the aggregate packet via `--max-avg-spread-cost-to-target`.
- `fill_failure_reason_counts` now classifies blocked microstructure signals, including latency/chase and spread-cost failures.
- `--max-contracts-per-underlying` keeps smoke and GCP shards balanced across QQQ, SPY, and IWM instead of spending all compute on the top global contracts.

Local v2 smoke command:

```powershell
python scripts\build_microstructure_research_grid.py `
  --wave-id microstructure_v2_smoke_20260507T1545ET `
  --output-dir reports\gcp_research\microstructure_v2_smoke_20260507T1545ET\inputs `
  --profile smoke `
  --underlyings QQQ,SPY,IWM `
  --chunk-size 16

python scripts\run_microstructure_event_replay_shard.py `
  --events-jsonl D:\codexalpaca_runtime\runs\microstructure_shadow_stream_fastwriter_20260507T1340ET\realtime_shadow_events.jsonl `
  --grid-jsonl reports\gcp_research\microstructure_v2_smoke_20260507T1545ET\inputs\microstructure_research_grid.jsonl `
  --output-dir reports\gcp_research\microstructure_v2_smoke_20260507T1545ET\local_smoke `
  --wave-id microstructure_v2_smoke_20260507T1545ET `
  --worker-id local_smoke `
  --grid-start-index 1 `
  --grid-count 16 `
  --underlyings QQQ,SPY,IWM `
  --max-contracts-per-underlying 5 `
  --processes 2

python scripts\aggregate_microstructure_event_replay.py `
  --workers-root reports\gcp_research\microstructure_v2_smoke_20260507T1545ET `
  --output-dir reports\gcp_research\microstructure_v2_smoke_20260507T1545ET\aggregate `
  --wave-id microstructure_v2_smoke_20260507T1545ET `
  --min-fill-coverage 0.90 `
  --min-trades 20 `
  --min-net-pnl 0 `
  --max-avg-spread-cost-to-target 0.65
```

Smoke result:

- Grid rows replayed: `16`
- Contracts replayed: `15`
- Eligible for microstructure review: `0`
- Decision: `research_only_blocked`
- Blockers: `fill_coverage_below_gate=16`, `net_pnl_not_positive=16`, `avg_net_pnl_not_positive=16`, `trade_count_below_gate=8`

Interpretation: the short websocket capture still does not produce an executable micro-scalping candidate under realistic spread/latency economics. The useful next search direction is rarer event-driven entries with larger target moves and strict quote/spread gates, not higher-frequency 1% scalps.

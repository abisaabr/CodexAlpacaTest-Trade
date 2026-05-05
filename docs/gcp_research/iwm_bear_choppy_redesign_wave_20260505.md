# IWM Bear/Choppy Redesign Wave - 2026-05-05

## Decision

`research_wave_running`

The IWM bear/choppy redesign wave is running in Google Cloud while the QQQ/SPY paper trader continues separately. This is research-only work and does not alter the active paper trader, live manifests, broker state, or risk policy.

## Scope

- Wave ID: `ticker365_iwm_bear_choppy_redesign_20260505T1505Z`
- Source commit: `cb69745986c33d61c57ff83f889c537976911fbc`
- Builder: `scripts/build_iwm_bear_choppy_redesign_research_inputs.py`
- Launcher: `scripts/launch_gcp_iwm_bear_choppy_redesign_shards.ps1`
- Broker-facing trading: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Promotion rule: do not promote anything unless a generated promotion-review packet says `eligible_for_promotion_review`

## Research Design

This is not a blind re-run of the previous IWM wave. The prior IWM pass showed strong raw coverage and repaired single-leg fill behavior, but only bull economics cleared governed review. Bear and choppy remained blocked by economics.

The new queue targets two gaps:

- Bear redesign: lower-target, tighter-stop IWM put structures using `single_leg_repair` and `debit_put_vertical`.
- Choppy redesign: two-sided range reversion using lower-band calls and upper-band puts across `single_leg_repair`, `debit_call_vertical`, and `debit_put_vertical`.

Strict replay profile:

- Contract selector: `entry_liquidity_first_research_only`
- Entry lag: `0`
- Exit lag: `60`
- Entry lookup: `first_bar_at_or_after_entry_within_lag`
- Exit lookup: `first_bar_at_or_after_exit_within_lag`
- Stock session filter: `option_rth_same_day`
- Initial cash: `25000`
- Allocation fraction: `0.05`
- Slippage: `10 bps`
- Fee: `$0.65` per contract
- Test date count: `20`
- Promotion fill gate: `fill_coverage >= 0.90`

## Inputs

- Variants: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/inputs/iwm_bear_choppy_redesign_variants.jsonl`
- Queue: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/inputs/iwm_bear_choppy_redesign_option_queue.json`
- Manifest: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/inputs/iwm_bear_choppy_redesign_manifest.json`
- Source archive: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/inputs/source/codexalpaca_repo_source.tar.gz`
- Launch rows: `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/ops/iwm_bear_choppy_redesign_launch_rows.json`

Input counts:

- Total candidates: `72`
- Bear candidates: `24`
- Choppy candidates: `48`
- Worker count: `12`
- Candidates per worker: `6`

## Dataset

The wave uses the existing IWM 365-day 5x5 option-fill ladder dataset.

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_20260429/IWM/365d_5x5/stock_ref_silver/stock_bars/`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_20260429/IWM/365d_5x5/research_wave/dense_universe/selected_option_contracts/`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_20260429/IWM/365d_5x5/option_bars_silver/option_bars/`

Prior data foundation:

- Selected trade dates: `251`
- Selected contract-days: `5522`
- Contract-days with bars: `5510`
- Option-bar rows: `952910`
- Raw contract-day coverage: `0.997827`

## Workers

First launch attempt started two workers, then `us-east4-a` returned `ZONE_RESOURCE_POOL_EXHAUSTED`. The wave was relaunched using `us-central1-a` and `us-west1-a`.

Running workers:

- `iwm-bc-red-c001-006-20260505a`
- `iwm-bc-red-c007-012-20260505a`
- `iwm-bc-red-c013-018-20260505a`
- `iwm-bc-red-c019-024-20260505a`
- `iwm-bc-red-c025-030-20260505a`
- `iwm-bc-red-c031-036-20260505a`
- `iwm-bc-red-c037-042-20260505a`
- `iwm-bc-red-c043-048-20260505a`
- `iwm-bc-red-c049-054-20260505a`
- `iwm-bc-red-c055-060-20260505a`
- `iwm-bc-red-c061-066-20260505a`
- `iwm-bc-red-c067-072-20260505a`

As of the first post-launch check, all 12 workers had uploaded `ticker_365d_status.json` with:

- `phase`: `running_selectors`
- `selectors`: `entry_liquidity_first_research_only`
- `lag_profiles`: `0:60`

## Concurrent Paper Session

The active QQQ/SPY RTH paper session remains separate and healthy.

- Paper run ID: `qqq-spy-rth-paper-session-20260505T143315Z`
- Last checked heartbeat: `2026-05-05T15:17:45Z`
- Trader process alive: `true`
- Paper VM: `qqq-spy-rth-paper-20260505t1433`

## Next Steps

1. Monitor each worker status under `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/workers/*/ticker_365d_status.json`.
2. When workers complete, aggregate promotion packets and identify whether any bear or choppy candidate is `eligible_for_promotion_review`.
3. If no bear/choppy candidate clears, do not add IWM to the paper trader.
4. If one bear and one choppy candidate clear, build an IWM governed-validation manifest and run broker-free preflight before any paper-trader expansion.

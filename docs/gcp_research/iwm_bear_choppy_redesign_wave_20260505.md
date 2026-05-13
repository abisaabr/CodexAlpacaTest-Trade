# IWM Bear/Choppy Redesign Wave - 2026-05-05

## Decision

`research_only_blocked`

The IWM bear/choppy redesign wave completed in Google Cloud. It produced zero candidates eligible for governed promotion review, so IWM remains excluded from the active paper trader. This was research-only work and did not alter the active paper trader, live manifests, broker state, or risk policy.

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

Completed workers:

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

All 12 workers uploaded promotion packets under:

- `gs://codexalpaca-control-us/research_results/ticker365_iwm_bear_choppy_redesign_20260505T1505Z/workers/*/promotion_packet/*/research_promotion_review_packet.json`

## Final Results

Promotion packet summary:

- Promotion packets: `12`
- Candidate rows tested: `72`
- Bear candidates: `24`
- Choppy candidates: `48`
- Review-eligible candidates: `0`
- Worker decisions: `research_only_blocked`
- Fill coverage gate: `0.90`
- Fill coverage unit: `filled_option_structures_per_source_stock_trade`

Blocker counts:

- `min_net_pnl_not_positive`: `71`
- `test_net_pnl_not_above_0`: `59`
- `fill_coverage_below_0.90`: `33`

Best near-miss by full-period and test net PnL:

- Candidate: `portfolio12h__iwm__choppy__put__debit_put_vertical__981bf7626eaa9e__profile_iwm-bc-red-c067-072-iwm-e0-x60-entry-liquidity-first-research-only`
- Strategy: `iwm__choppy__put__debit_put_vertical`
- Intended regime: `choppy`
- Full-period net PnL: `15732.003`
- Test net PnL: `47262.66`
- Fill coverage: `0.7587`
- Data foundation coverage: `0.8986`
- Entry bar coverage: `0.8444`
- Exit bar coverage: `1.0`
- Option trade count: `217`
- Promotion blockers: `fill_coverage_below_0.90`

Interpretation:

- IWM bear/choppy economics are still not promotion-grade as a portfolio set.
- The best IWM choppy candidate has promising PnL but fails the institutional fill gate, mostly at entry/data-foundation coverage.
- IWM should not be added to the paper trader until a generated promotion-review packet says `eligible_for_promotion_review`.
- The next IWM pass should focus on entry-timing/contract-availability alignment for the choppy put vertical near-miss, plus separate bear economics redesign. A broader blind sweep is lower value than diagnosing why this profitable choppy candidate only fills `75.87%`.

## Concurrent Paper Session

The active QQQ/SPY RTH paper session remains separate and healthy.

- Paper run ID: `qqq-spy-rth-paper-session-20260505T143315Z`
- Last checked heartbeat: `2026-05-05T15:17:45Z`
- Trader process alive: `true`
- Paper VM: `qqq-spy-rth-paper-20260505t1433`

## Next Steps

1. Keep IWM out of the active QQQ/SPY paper trader.
2. Build an IWM choppy fill-diagnostic micro-wave around the profitable debit-put-vertical near-miss.
3. Test whether entry timestamp shifts, narrower option universe rules, or same-day selected-contract availability can raise fill coverage above `0.90` without weakening the gate.
4. Build a separate IWM bear economics redesign; current bear candidates did not produce a promotion-grade result.

# QQQ Family-Economics Micro Wave - 2026-05-05

## Purpose

Move QQQ from repaired fill mechanics toward governed paper-trader readiness for at least one bull, one bear, and one choppy strategy. This wave does not start trading, does not submit paper orders, does not change live manifests, and does not change risk policy.

## Wave

- Wave ID: `ticker365_qqq_family_econ_micro_20260505T0135Z`
- GCS root: `gs://codexalpaca-control-us/research_results/ticker365_qqq_family_econ_micro_20260505T0135Z/`
- Source archive: `gs://codexalpaca-control-us/research_results/ticker365_qqq_family_econ_micro_20260505T0135Z/inputs/source/codexalpaca_repo_source.tar.gz`
- Launch manifest: `gs://codexalpaca-control-us/research_results/ticker365_qqq_family_econ_micro_20260505T0135Z/inputs/qqq_family_econ_micro_launch_rows.json`
- Launcher: `scripts/launch_gcp_qqq_family_econ_micro_shards.ps1`
- Selector: `entry_liquidity_first_research_only`
- Timing: strict entry `0` minutes, exit `60` minutes
- Fill gate: `fill_coverage >= 0.90`
- Minimum option trades: `20`
- Minimum test net PnL: `> 0`

## Data

- Stock bars: `gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/`
- Selected contracts: `gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/`
- Option bars: `gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/underlying=QQQ/`

## Candidate Coverage

Candidates `6-20` were launched as one-candidate shards to accelerate QQQ decisioning under the 32-vCPU quota:

- `6-8`: choppy put broken-wing put butterfly
- `9-12`: bull call debit vertical
- `13-16`: bear put debit vertical
- `17-20`: choppy call iron butterfly

## Quota And Instance Decisions

The project hit global CPU quota at `32` vCPUs. To launch candidates `17-20`, redundant older broad workers and the older controller were stopped after logging notes to the wave `ops/` prefix:

- Stopped: `qqqfam-econ-smoke-20260505a`
- Stopped: `qqqfam-econ-c009-020-20260505a`
- Stopped: `paper-ready-controller-20260504qa`

These stopped instances were not broker-facing and were not managing live trading state.

## Early Result Snapshot

Early completed candidates show the fill repair is mostly successful, but economics are still rejecting candidates:

- Candidate `7` cleared fill at `0.9315`, but was quarantined for option economics.
- Candidate `8` cleared fill at `0.9375`, but had only `15` filled option trades and negative economics.
- Candidate `11` cleared fill at `0.9286`, but was quarantined for option economics.
- Candidate `12` cleared fill at `1.0000`, but had only `18` filled option trades and negative economics.

No QQQ strategy from this micro-wave should be moved to governed promotion review unless the generated promotion-review packet says `eligible_for_promotion_review`.

## Next Steps

1. Wait for candidates `13-20` to finish.
2. Build a QQQ-only portfolio report and promotion-review packet from the completed micro-wave.
3. If no bull, bear, and choppy strategies are eligible, redesign the QQQ strategy family economics rather than weakening gates.
4. Only after QQQ has governed eligible candidates should the paper runner manifest be prepared for operator approval.

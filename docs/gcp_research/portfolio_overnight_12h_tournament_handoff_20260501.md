# Portfolio Overnight 12h Tournament Handoff

Generated: 2026-05-01

## Goal

Run a research-only, multi-VM overnight tournament across every ticker with usable stock and option data in the current Google Cloud research buckets. The target is a governed-validation portfolio candidate set for a `$25,000` paper account, split across bull, bear, and choppy market conditions.

## Current Decision

Status: `ready_for_operator_approved_research_fleet_launch`

The packet is ready to launch a billable research fleet after explicit operator approval. It is not broker-facing and does not submit paper orders.

The VM commands target Debian workers using `ramzi-service-account@codexalpaca.iam.gserviceaccount.com`. They still require generated startup scripts that bind each worker's local input paths before execution; do not run the create commands until those startup scripts are materialized and reviewed.

## Canonical Inputs

- Config: `config/research_tournaments/portfolio_overnight_12h_20260501.yaml`
- Input builder: `scripts/build_portfolio_overnight_research_inputs.py`
- Packet builder: `scripts/build_portfolio_overnight_tournament_packet.py`
- Packet output: `reports/gcp_research/portfolio_overnight_12h_20260501/`
- GCS prefix: `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/`

## Covered Universe

Primary 20-symbol option ladder universe:

`AAPL AMD AMZN INTC IWM META MSFT NVDA SPY TSLA AVGO GOOGL MU NFLX ORCL PLTR QQQ TSM XLE XOM`

Deep QQQ universe:

`QQQ 365d next-trading-day ATM +/- 5`

Curated smoke universe:

`GLD MSFT QQQ SLV TSLA`

## Fleet Shape

- `data_coverage_top10`: top10 option-fill ladder coverage
- `data_coverage_next10`: next10 option-fill ladder coverage
- `qqq_deep_regime_grid`: QQQ bull/bear/choppy deep regime grid
- `option_aware_core_a`: `AAPL AMD AMZN INTC IWM`
- `option_aware_core_b`: `META MSFT NVDA SPY TSLA`
- `option_aware_core_c`: `AVGO GOOGL MU NFLX ORCL`
- `option_aware_core_d`: `PLTR QQQ TSM XLE XOM`
- `aggregator_promotion_review`: portfolio report and promotion-review packet

## Promotion Gates

- `fill_coverage >= 0.90`
- `min_option_trades >= 20`
- `min_test_net_pnl > 0`
- `min_net_pnl > 0`
- `max_positions = 8`
- `max_strategies_per_symbol = 2`
- `max_symbol_weight = 0.20`
- `initial_cash = 25000`

## Commands

Build/refresh the packet:

```powershell
python scripts\build_portfolio_overnight_research_inputs.py --variants-jsonl <local_or_downloaded_gcp_research_wave_variants.jsonl> --output-dir <packet-output>\inputs
python scripts\build_portfolio_overnight_tournament_packet.py
```

Review packet before launching any billable VM:

```powershell
Get-Content reports\gcp_research\portfolio_overnight_12h_20260501\portfolio_overnight_12h_tournament_packet.md
```

Before VM launch, review one startup script per worker in the packet `startup_scripts` folder. The startup scripts unpack the exact GCS source archive, stage the worker-specific GCS datasets locally, write `command.txt`, `source_commit.txt`, `startup.log`, and `artifacts_manifest.json`, then upload all worker outputs under `gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/workers/{worker_id}/`.

## Hard Stop

Do not start trading, do not submit paper orders, do not modify live manifests, and do not change risk policy from this packet. Promotion means governed validation review only.

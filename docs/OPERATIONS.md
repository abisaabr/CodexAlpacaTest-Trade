# Operations

## Safe Daily Flow

1. Run `python scripts/doctor.py`
2. Build or refresh historical data with `python scripts/build_historical_dataset.py ...`
3. Run `python scripts/run_sample_backtest.py --synthetic` or point to local parquet data.
4. Generate or review a promotion board.
5. Run `python scripts/run_paper_equities.py --board-path <path>` or `python scripts/run_paper_options.py --board-path <path>`

## Paper Order Safeguards

- The orchestrators default to dry-run.
- No order is sent unless `--submit-paper-orders` is provided.
- Live routing is refused by config and broker code.
- All submitted or previewed payloads are journaled under `reports/`.

## Recovery

- Historical builds are restart-safe through chunk manifests in `data/raw/manifests/`.
- If a run is interrupted, rerun the same build command to continue completed chunks.
- Review `failed_chunks.csv` and `build_summary.md` before retrying large builds.

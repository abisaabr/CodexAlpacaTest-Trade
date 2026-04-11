# Operations

## Safe Daily Flow

1. Run `python scripts/doctor.py`
2. Build or refresh historical data with `python scripts/build_historical_dataset.py ...`
3. Run `python scripts/run_sample_backtest.py --synthetic` or point to local parquet data.
4. Generate or review a promotion board.
5. Run `python scripts/run_paper_equities.py --board-path <path>` or `python scripts/run_paper_options.py --board-path <path>`

## QQQ Portfolio Flow

1. Bootstrap the repo so `.venv` exists.
2. Confirm paper-only credentials are available through `.env` or user environment variables.
3. Run `python scripts/run_qqq_portfolio_paper_trader.py --portfolio-config config\qqq_paper_portfolio.yaml --run-once`.
4. Review `reports/qqq_portfolio/` for state, alerts, and the latest session summary.
5. Install or refresh the Windows task with `powershell -ExecutionPolicy Bypass -File .\scripts\install_qqq_paper_task.ps1 -TaskName "QQQ Portfolio Paper Trader" -StartTime "09:20"`.
6. When you want the live paper session to submit orders, keep `config\qqq_paper_portfolio.yaml` on `submit_paper_orders: true` or pass `--submit-paper-orders`.

## Paper Order Safeguards

- The orchestrators default to dry-run.
- No order is sent unless `--submit-paper-orders` is provided.
- Live routing is refused by config and broker code.
- All submitted or previewed payloads are journaled under `reports/`.
- The QQQ portfolio runner is paper-only as well. It uses a local `$25,000` virtual sleeve and still exits all trades before the session ends.

## Recovery

- Historical builds are restart-safe through chunk manifests in `data/raw/manifests/`.
- If a run is interrupted, rerun the same build command to continue completed chunks.
- Review `failed_chunks.csv` and `build_summary.md` before retrying large builds.

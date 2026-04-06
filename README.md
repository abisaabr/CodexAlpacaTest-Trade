# Codex Alpaca Test Trade

Portable, public Alpaca research and execution lab that is locked to paper-only mode by default for:

- historical stock and options ingestion
- resumable dataset builds with manifests and quality checks
- pluggable backtests
- paper-trading orchestration through a single broker gateway
- local reports, journals, and alert queues

## Safety Model

- This repo is public. Never commit secrets, populated `.env` files, or tokens.
- This repo is locked to paper-only mode until you intentionally redesign the safeguards.
- `LIVE_TRADING=true`, `ALPACA_PAPER_TRADE=false`, `ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE=true`, and any non-paper Alpaca trading base URL all raise clear errors.
- Live trading is refused by configuration, by the broker gateway, and by runtime trading-API checks.
- All broker calls route through `alpaca_lab/brokers/alpaca.py`.
- Paper execution defaults to dry-run previews and requires an explicit submit flag to send paper orders to the Alpaca paper endpoint only.
- No GitHub workflow assumes secrets are available.

## Layout

```text
.github/workflows/   CI, sample backtest, repo health
docs/                machine setup, operations, data model, safety
config/              portable example configs and sample candidate boards
scripts/             bootstrap, doctor, historical build, backtests, paper runners
alpaca_lab/          broker, data, backtest, execution, options, reporting
tests/               config, safeguards, ingestion, backtest, selector, reporting tests
```

## New Machine Setup

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap_windows.ps1
.\.venv\Scripts\Activate.ps1
where python
python scripts\doctor.py --skip-connectivity
```

Important: running `bootstrap_windows.ps1` does not keep `.venv` activated in the PowerShell window you are using. The script runs in its own process, so after it exits you should activate `.venv` in your current shell and confirm `where python` points at `.venv\Scripts\python.exe`.
Also keep the repo in paper-only mode: `ALPACA_PAPER_TRADE=true`, `LIVE_TRADING=false`, and `APCA_API_BASE_URL` unset or set to `https://paper-api.alpaca.markets`.

### macOS / Linux

```bash
bash ./scripts/bootstrap_mac.sh
# or
bash ./scripts/bootstrap_linux.sh
cp .env.example .env
python scripts/doctor.py
```

## Environment Variables

The config loader supports both Alpaca env families:

- `ALPACA_API_KEY` / `ALPACA_SECRET_KEY`
- `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY`

Optional paper endpoint support:

- `ALPACA_PAPER_TRADE=true`
- `APCA_API_BASE_URL=https://paper-api.alpaca.markets`

This repo does not allow live mode or custom trading endpoints. Copy `.env.example` to `.env`, then fill in paper-only credentials locally.

## Local Commands

Install:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
where python
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

Tests:

```powershell
python -m pytest
```

Doctor:

```powershell
python scripts/doctor.py
```

Sample backtest:

```powershell
python scripts/run_sample_backtest.py --synthetic
```

Historical build:

```powershell
python scripts/build_historical_dataset.py --days 365 --symbols QQQ,SPY,NVDA,TSLA,META,AAPL,AMD,GOOG,MSFT,AMZN,PLTR,ASML --stock-chunk-days 5 --contract-chunk-days 30 --option-batch-size 25 --min-dte 0 --max-dte 14 --strike-steps 3
```

Paper equities dry-run:

```powershell
python scripts/run_paper_equities.py --board-path config\sample_equities_board.json
```

Paper options dry-run:

```powershell
python scripts/run_paper_options.py --board-path config\sample_options_board.json
```

## What Is Not Committed

- `.env` and any secrets
- local `data/` outputs
- local `reports/` outputs
- caches, virtualenvs, notebooks, and other machine-specific artifacts

## Notes

- The historical builder supports stock bars, option contract inventory, option bars, option trades, and latest quote/snapshot enrichment for non-expired selected contracts.
- The sample promotion boards are placeholders. Replace their symbols before any intentional paper submission.
- Paper submission, when explicitly enabled, still routes only to Alpaca paper trading. Live trading is intentionally impossible in this repo.
- Historical options quotes are not assumed to exist in Alpaca's current public data surface for this repo.
- The repo is intentionally structured so code from older local repos can be migrated into `alpaca_lab/` with minimal reshuffling later.

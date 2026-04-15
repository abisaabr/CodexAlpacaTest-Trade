# Portable Deployment

## Easiest Path

The easiest way to run this repo on any machine is:

1. Clone the repo from GitHub.
2. Copy `.env.example` to `.env`.
3. Fill `.env` with Alpaca paper credentials and your notification settings.
4. Run the Docker-based setup helper.
5. Start the long-running services:
   - `portfolio-trader`
   - `portfolio-watchdog`
   - `portfolio-close-guard`

That path removes Windows Task Scheduler from the critical path and works the same way on Windows, macOS, and Linux as long as Docker is installed.

## One-Command Setup

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_new_machine.ps1 -Mode docker
```

### macOS / Linux

```bash
bash ./scripts/setup_new_machine.sh --mode docker
```

Both scripts:

- create `data/` and `reports/`
- create `.env` from `.env.example` when needed
- build the Docker image
- print the exact next commands to start services

## Start The Portable Trader

```bash
docker compose up -d portfolio-trader portfolio-watchdog portfolio-close-guard
```

Useful follow-up commands:

```bash
docker compose ps
docker compose logs -f portfolio-trader
docker compose logs -f portfolio-watchdog
docker compose logs -f portfolio-close-guard
```

## Runtime Model

### `portfolio-trader`

This service runs `scripts/run_multi_ticker_portable_daemon.py`, which:

- starts the existing multi-ticker trader
- lets it run through the full session
- waits for the next session after the close instead of exiting and bouncing

That keeps Docker restart policies safe. A plain one-shot paper-trader command would exit after the close and restart immediately, which would be noisy and confusing.

### `portfolio-watchdog`

This service runs `scripts/run_multi_ticker_watchdog.py`, which:

- checks the current session file
- watches for stale updates during market hours
- watches for missing morning, midday, and end-of-day notifications
- sends alerts through ntfy, email, and Discord if configured

### `portfolio-close-guard`

This service runs `scripts/run_multi_ticker_eod_close_guard.py`, which:

- wakes up near `3:58 PM ET` each market day
- runs an independent end-of-day flatten and broker-reconciliation sweep
- keeps retrying until the book is flat or the guard times out with a report

## Data And Reports

The compose stack mounts these local folders into the container:

- `./data -> /app/data`
- `./reports -> /app/reports`

That means:

- your state persists across container restarts
- you can move `data/`, `reports/`, and `.env` to another machine with the repo
- Git still ignores all machine-specific outputs

If you only want the live paper trader on the new machine, you do not need to copy the large historical cleanroom datasets. Those are only needed for local research and backtesting. For the live runner, the essentials are:

- the repo itself
- a local `.env`
- optional prior `data/` and `reports/` if you want continuity of state and logs

## What Stays Local

Do not commit these:

- `.env`
- `data/`
- `reports/`

Those are the only machine-specific pieces you need to carry forward.

## Native Windows Path

The native Windows scheduler remains supported and is still a good choice on one Windows box:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_new_machine.ps1 -Mode native -InstallTasks
```

But for true “run this on any machine” portability, Docker is now the recommended default.

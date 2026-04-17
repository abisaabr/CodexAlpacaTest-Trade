# Portable Deployment

## Easiest Path

The easiest way to run this repo on any machine is:

1. Clone the repo from GitHub.
2. Run the setup helper.
3. Fill `.env` with Alpaca paper credentials and notification settings.
4. Point both machines at the same shared ownership lease path.
5. Run the standby failover check on the standby machine.
6. Start the long-running services or scheduled tasks.

That keeps GitHub as the source of truth for code while local state, secrets, and shared ownership stay outside the repo.

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
- build the Docker image in Docker mode
- print the exact next commands to start services

## Shared Ownership Lease

To keep two machines from trading the same Alpaca paper account at the same time, both machines should share the same ownership lease path.

The easiest approach is a cloud-synced folder such as Google Drive:

```env
MULTI_TICKER_OWNERSHIP_ENABLED=true
MULTI_TICKER_OWNERSHIP_LEASE_PATH=C:\Users\you\Google Drive\CodexAlpaca\leases\multi_ticker_portfolio.json
MULTI_TICKER_OWNERSHIP_TTL_SECONDS=180
MULTI_TICKER_MACHINE_LABEL=trading-laptop
```

What this does:

- the active machine renews the lease continuously during the session
- the standby machine sees the active lease and stands down instead of sending orders
- if the active machine stops renewing, the standby machine can take over after the lease TTL expires

Before starting the standby machine, run:

```powershell
python scripts\run_multi_ticker_standby_failover_check.py --expected-lease-path "C:\Users\you\Google Drive\CodexAlpaca\leases\multi_ticker_portfolio.json"
```

If you are using Docker:

```powershell
docker compose run --rm portfolio-trader python scripts/run_multi_ticker_standby_failover_check.py --expected-lease-path "C:\Users\you\Google Drive\CodexAlpaca\leases\multi_ticker_portfolio.json"
```

## Start The Portable Trader

```powershell
docker compose up -d portfolio-trader portfolio-watchdog portfolio-close-guard
```

Useful follow-up commands:

```powershell
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

## What Stays In GitHub

GitHub should hold:

- code
- configs
- docs
- tests
- setup scripts
- migration scripts
- risk controls

GitHub should not hold:

- `.env`
- `data/`
- `reports/`
- cleanroom datasets
- `repo_archives`
- large runtime handoff zips

## Immediate Machine Migration

If you need to move the live paper trader right away, use the runtime migration bundle workflow instead of reconstructing local state by hand.

### On the source machine

```powershell
python scripts\create_multi_ticker_migration_bundle.py
```

That creates an ignored bundle under:

```text
reports/multi_ticker_portfolio/migration_bundles/
```

The bundle includes:

- the local `.env`
- the current session state files
- the current trade-date run folder
- the latest health snapshot
- a manifest with the source branch, commit, lease path, and restore notes

Move either the bundle folder or the generated `.zip` file to the destination machine.

### On the destination machine

1. Clone the repo and check out the same branch and commit listed in the bundle manifest.
2. Restore the bundle:

```powershell
python scripts\restore_multi_ticker_migration_bundle.py "<bundle-path>" --target-repo "<cloned-repo-path>" --machine-label "<new-machine-label>"
```

3. Run the standby failover preflight:

```powershell
python scripts\run_multi_ticker_standby_failover_check.py
```

4. Start the trader only after the failover check passes.

## Cleanroom Research Migration

The runtime migration bundle does not include the separate `qqq_options_30d_cleanroom` research workspace we use to test new tickers.

To move that research workspace too, create a dedicated cleanroom bundle:

```powershell
python scripts\create_cleanroom_research_bundle.py
```

That writes a fresh handoff bundle under the archive folder, including:

- a zipped snapshot of `qqq_options_30d_cleanroom`
- a `RESTORE_RESEARCH_WORKSPACE.ps1` script inside the bundle
- a manifest listing the source workspace and any related archive zips detected nearby

On the destination Windows machine:

1. Keep `codexalpaca_repo` and `qqq_options_30d_cleanroom` as sibling folders.
2. Unzip the handoff bundle.
3. Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\RESTORE_RESEARCH_WORKSPACE.ps1 -TargetParent "C:\Users\<you>\Downloads"
```

After that, use the `codexalpaca_repo` virtualenv to run the cleanroom scripts, for example:

```powershell
cd C:\Users\<you>\Downloads\codexalpaca_repo
.\.venv\Scripts\python.exe ..\qqq_options_30d_cleanroom\research_candidate_ticker_batch.py --tickers aapl,amzn
```

## Native Windows Path

The native Windows scheduler remains supported and is still a good choice on one Windows box:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_new_machine.ps1 -Mode native -InstallTasks
```

But for true "run this on any machine" portability, Docker is still the recommended default.

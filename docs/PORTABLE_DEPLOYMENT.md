# Portable Deployment

## Easiest Path

The easiest way to run this repo on any machine is:

1. Clone the repo from GitHub.
2. Run the Docker-based setup helper.
3. Copy `.env.example` to `.env` if needed and fill `.env` with Alpaca paper credentials and your notification settings.
4. For standby-safe multi-machine deployment, point both machines at the same ownership lease path.
5. Run the standby failover check on the standby machine.
6. Start the long-running services:
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

## Shared Ownership Lease

To keep two machines from trading the same Alpaca paper account at the same time, both machines should share the same ownership lease path.

The easiest approach is a cloud-synced folder such as OneDrive:

```env
MULTI_TICKER_OWNERSHIP_ENABLED=true
MULTI_TICKER_OWNERSHIP_LEASE_PATH=C:\Users\you\OneDrive\CodexAlpaca\leases\multi_ticker_portfolio.json
MULTI_TICKER_OWNERSHIP_TTL_SECONDS=180
MULTI_TICKER_MACHINE_LABEL=trading-laptop
```

What this does:

- the active machine renews the lease continuously during the session
- the standby machine sees the active lease and stands down instead of sending orders
- if the active machine stops renewing, the standby machine can take over after the lease TTL expires

Before starting the standby machine, run:

```bash
docker compose run --rm portfolio-trader python scripts/run_multi_ticker_standby_failover_check.py
```

That check verifies:

- ownership is enabled
- the lease path is absolute and not repo-local
- the standby machine has a machine label
- the shared lease file is visible
- the standby machine can see the current owner when the active machine already holds the lease

If you want to compare against an exact expected lease path, add:

```bash
docker compose run --rm portfolio-trader python scripts/run_multi_ticker_standby_failover_check.py --expected-lease-path "C:\Users\you\OneDrive\CodexAlpaca\leases\multi_ticker_portfolio.json"
```

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

1. Clone the repo and check out the same branch/commit listed in the bundle manifest.
2. Restore the bundle:

```powershell
python scripts\restore_multi_ticker_migration_bundle.py "<bundle-path>" --target-repo "<cloned-repo-path>" --machine-label "<new-machine-label>"
```

3. Run the standby failover preflight:

```powershell
python scripts\run_multi_ticker_standby_failover_check.py
```

4. Start the services only after the failover check passes.

This is the fastest safe handoff path when you need to move the runner midstream.

## Native Windows Path

The native Windows scheduler remains supported and is still a good choice on one Windows box:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_new_machine.ps1 -Mode native -InstallTasks
```

But for true “run this on any machine” portability, Docker is now the recommended default.

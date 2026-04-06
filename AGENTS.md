# Repository Instructions

- Default every workflow to paper trading, read-only broker checks, and dry-run execution previews.
- Never place live orders. Live-routing paths are not allowed in this repo.
- Never commit secrets, API keys, tokens, or populated `.env` files.
- Route every broker call through `alpaca_lab/brokers/alpaca.py`.
- Keep generated data, manifests, reports, and caches out of git.
- Every material code change must include or update tests.
- Prefer typed, modular Python with clear layer boundaries between data, backtesting, execution, options, and reporting.
- Bootstrap scripts and `scripts/doctor.py` must stay working across machines.
- Preserve deterministic manifests, journal files, and reports so interrupted runs can be resumed or audited.

# Repository Instructions

- Default every workflow to paper trading, read-only broker checks, and dry-run execution previews.
- Treat this repo as locked to paper-only mode until the user explicitly requests a redesign of the safeguards.
- Never place live orders. Live-routing paths are not allowed in this repo.
- Treat `LIVE_TRADING=true`, `ALPACA_PAPER_TRADE=false`, `ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE=true`, or any non-paper Alpaca trading URL as hard errors.
- Never commit secrets, API keys, tokens, or populated `.env` files.
- Route every broker call through `alpaca_lab/brokers/alpaca.py`.
- Keep generated data, manifests, reports, and caches out of git.
- Every material code change must include or update tests.
- Prefer typed, modular Python with clear layer boundaries between data, backtesting, execution, options, and reporting.
- Bootstrap scripts and `scripts/doctor.py` must stay working across machines.
- Preserve deterministic manifests, journal files, and reports so interrupted runs can be resumed or audited.

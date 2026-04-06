# Safety

## Non-Negotiables

- This repository is locked to Alpaca paper-only mode until the safeguards are intentionally redesigned.
- `LIVE_TRADING=true`, `ALPACA_PAPER_TRADE=false`, `ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE=true`, and any non-paper Alpaca trading base URL all raise clear configuration errors.
- Live trading is refused in config, in the broker gateway, and in runtime trading-API checks.
- No secrets belong in git, GitHub Actions, docs, or issue comments.
- All broker access goes through `alpaca_lab/brokers/alpaca.py`.

## Order Safety

- Order scripts default to dry-run previews.
- Paper submission requires an explicit CLI flag and still routes only to the Alpaca paper endpoint.
- Cancel and submit actions both require explicit approval flags in code.
- Idempotent `client_order_id` values are generated through the broker gateway.

## Connectivity Safety

- The doctor script performs read-only checks only.
- The doctor and bootstrap scripts both remind operators that this repo is paper-only.
- Historical build and backtest scripts do not place any orders.
- GitHub workflows do not require secrets and do not assume account access.

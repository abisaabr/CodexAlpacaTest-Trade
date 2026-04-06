# Safety

## Non-Negotiables

- This repository is paper-trading-first.
- Live trading is refused in config and in the broker gateway.
- No secrets belong in git, GitHub Actions, docs, or issue comments.
- All broker access goes through `alpaca_lab/brokers/alpaca.py`.

## Order Safety

- Order scripts default to dry-run previews.
- Paper submission requires an explicit CLI flag.
- Cancel and submit actions both require explicit approval flags in code.
- Idempotent `client_order_id` values are generated through the broker gateway.

## Connectivity Safety

- The doctor script performs read-only checks only.
- Historical build and backtest scripts do not place any orders.
- GitHub workflows do not require secrets and do not assume account access.

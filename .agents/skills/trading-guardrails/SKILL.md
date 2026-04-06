---
name: trading-guardrails
description: Use for Alpaca-backed research, ingestion, backtesting, paper orchestration, options selection, cost modeling, risk gating, and reporting in this repo.
---

# Trading Guardrails

Use this skill whenever work touches Alpaca data, backtests, option selection, or paper-order orchestration.

## Rules

- Stay paper-only and dry-run by default.
- Never commit secrets or `.env` values.
- Route all broker traffic through `alpaca_lab/brokers/alpaca.py`.
- Keep execution code separate from research and backtesting code.
- Log assumptions for contract selection, slippage, fees, and risk limits.

## Practical Workflow

1. Load settings through `alpaca_lab.config`.
2. Use `alpaca_lab.data` for ingestion, normalization, manifests, and quality checks.
3. Use `alpaca_lab.backtest` for simulations and `alpaca_lab.options` for option-selection logic.
4. Use `alpaca_lab.execution` only for paper orchestration and only with explicit approval flags.
5. Write local summaries, journals, and alert queues under `reports/`.
6. Add or update tests before considering the work complete.

# Data Model

## Primary Local Layout

```text
data/raw/
  manifests/
  historical/<build_name>/
data/silver/
  historical/<build_name>/
reports/
  sample_backtest/
  paper_equities/
  paper_options/
```

## Core Datasets

- `stock_bars`: normalized 1-minute stock bars
- `option_contract_inventory`: option metadata and discovery snapshots
- `selected_option_contracts`: daily ATM-window selections
- `option_bars`: historical option minute bars
- `option_trades`: historical option trades
- `option_latest_quotes`: current quote enrichment for non-expired selected contracts
- `option_snapshots`: current snapshot and greeks enrichment for non-expired selected contracts

## Quality Tracking

- every chunk writes status to a manifest
- every dataset emits schema and empty-response checks
- bar datasets track missing minute intervals and duplicate rows
- reports summarize coverage by symbol and date

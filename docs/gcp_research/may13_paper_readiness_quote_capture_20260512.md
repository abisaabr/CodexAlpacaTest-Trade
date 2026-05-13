# May 13 Paper Readiness And Quote Capture - 2026-05-12

- Source commit: `1b2a467acfe6a938fa052c5f676a2e134366718d`
- Broker mode impact: `none`
- Paper-runner state changed: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Promotion decision: `no new strategy is eligible for paper-runner addition from this pass`

## What Changed

Added a PAPER-only May 13 runtime config:

- `config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml`
- Same strategy manifest list as the May 8 armed config
- Same risk gates as the May 8 armed config
- New May 13 ownership lease, state root, run root, task name, and machine label

Added realtime quote-quality sidecar tooling:

- `scripts/build_realtime_quote_quality_sidecar.py`
- `scripts/apply_quote_sidecar_to_trade_economics.py`
- `tests/test_build_realtime_quote_quality_sidecar.py`
- `tests/test_apply_quote_sidecar_to_trade_economics.py`

The sidecar builder converts no-submit websocket shadow logs into:

- `option_quote_sidecar.csv`
- `option_quote_quality_by_minute.csv`
- `option_trade_prints.csv`
- `stock_quote_sidecar.csv`
- `quote_quality_sidecar_summary.json`

The causal contract is explicit: use `option_quote_sidecar.csv` for as-of joins at or before the strategy decision timestamp. The minute aggregate is diagnostic only unless replay code treats its timestamp causally.

The as-of join tool applies that contract to existing `option_aware_trade_economics.csv` files:

```powershell
python scripts/apply_quote_sidecar_to_trade_economics.py --trade-economics-csv <option_aware_trade_economics.csv> --quote-sidecar-csv <option_quote_sidecar.csv> --option-trades-csv <option_trade_prints.csv> --output-csv <quote_backed_option_aware_trade_economics.csv>
```

It writes bid/ask-backed `entry_*` and `exit_*` quote-source, spread, quote-age, leg coverage, and trade-print fields without using future quotes. Multi-leg rows are marked `option_quote_bid_ask` only when every leg has an as-of quote; partial multi-leg evidence is labeled `option_quote_partial_bid_ask`.

## Validation

Run ID: `full_pytest_after_quote_sidecar_20260512`

```powershell
python -m pytest -q
```

Result before as-of join patch: `317 passed, 1 warning`.

Run ID: `quote_sidecar_asof_join_tests_20260512`

```powershell
python -m pytest tests/test_apply_quote_sidecar_to_trade_economics.py tests/test_build_realtime_quote_quality_sidecar.py tests/test_repair_projection_replay_lineage.py tests/test_build_portfolio_growth_projection.py -q
```

Result: `17 passed`.

Run ID: `may13_config_load_20260512`

```powershell
python - <<'PY'
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config
cfg = load_portfolio_config('config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml')
print(cfg.name)
print(len(cfg.execution.underlying_symbols), cfg.execution.underlying_symbols)
print(cfg.execution.option_feed, cfg.execution.stock_feed, cfg.execution.submit_paper_orders)
print(cfg.ownership.lease_path)
PY
```

Result:

- Config name: `multi_symbol_governed_realtime_paper_trader_20260513_armed`
- Symbols: `15`
- Feeds: `opra` options, `sip` stock
- Submit flag in config: `true`, still requires explicit runtime command and PAPER endpoint checks
- Lease path: `D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260513_ownership_lease.json`

Run ID: `may13_strategy_inventory_20260512`

Result:

- Runtime strategies: `344`
- Runtime symbols: `AMD,AMZN,AVGO,GOOGL,INTC,IWM,META,MSFT,NVDA,PLTR,QQQ,SPY,TSLA,TSM,XOM`
- Regime counts: `bull=76`, `bear=125`, `choppy=143`
- Multi-leg strategies: `47`
- Main families: `Single-leg long call=194`, `Single-leg long put=103`, `debit_call_vertical=17`, `broken_wing_put_butterfly=11`, `debit_put_vertical=9`, `bull_put_credit_spread=7`, `bear_call_credit_spread=2`, `broken_wing_call_butterfly=1`

Run ID: `realtime_quote_sidecar_plan_only_dryrun_20260512`

```powershell
python scripts/build_realtime_quote_quality_sidecar.py --events-jsonl reports/gcp_research/quote_realism_projection_hardening_20260512/realtime_quote_capture_plan_20260512/realtime_shadow_events.jsonl --output-dir reports/gcp_research/quote_realism_projection_hardening_20260512/realtime_quote_capture_plan_20260512/quote_quality_sidecar_dryrun
```

Result: `quality_status=no_option_quote_events`, expected because the prior artifact was a plan-only shadow run, not an RTH stream capture.

## May 13 RTH Launch Contract

Do not start live trading. Do not add strategies. Do not lower fill, risk, or quote gates.

Before RTH launch, set:

```powershell
$env:MULTI_TICKER_MACHINE_LABEL = "local-primary-paper-20260513"
$env:MULTI_TICKER_OWNERSHIP_LEASE_PATH = "D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260513_ownership_lease.json"
```

Run startup preflight only after fresh SIP stock bars are available:

```powershell
python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml --startup-preflight --no-submit-paper-orders
```

Start PAPER order submission only if:

- Startup preflight passes
- Broker endpoint is PAPER
- No duplicate broker-facing process exists
- No unexpected PAPER open orders or positions exist
- Ownership lease is valid
- State and run roots are writable
- EOD flatten guards remain active at 10 and 2 minutes before close

Runtime command:

```powershell
python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml --submit-paper-orders
```

## May 13 Quote Capture Contract

Run this no-submit shadow capture during RTH, preferably in a separate process from the PAPER trader:

```powershell
python scripts/run_multi_ticker_realtime_shadow_monitor.py --portfolio-config config/multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml --output-dir D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow --max-option-symbols 900 --duration-seconds 23400 --stream --include-stock-quotes --include-option-trades --no-trade-updates
```

After capture completes, convert the stream into quote-quality sidecars:

```powershell
python scripts/build_realtime_quote_quality_sidecar.py --events-jsonl D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow\realtime_shadow_events.jsonl --output-dir D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow\quote_quality_sidecar
```

Apply sidecars to any same-day replay economics before projection:

```powershell
python scripts/apply_quote_sidecar_to_trade_economics.py --trade-economics-csv <option_aware_trade_economics.csv> --quote-sidecar-csv D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow\quote_quality_sidecar\option_quote_sidecar.csv --option-trades-csv D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow\quote_quality_sidecar\option_trade_prints.csv --output-csv <quote_backed_option_aware_trade_economics.csv>
```

Mirror durable outputs:

```powershell
gcloud storage cp --recursive D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260513\realtime_quote_shadow gs://codexalpaca-control-us/paper_runs/multi_symbol_governed_realtime_20260513/realtime_quote_shadow
```

## GCS Mirror Paths

- Dry-run sidecar output: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/realtime_quote_capture_plan_20260512/quote_quality_sidecar_dryrun/`
- Handoff doc: `gs://codexalpaca-control-us/gcp_research/quote_realism_projection_hardening_20260512/docs/may13_paper_readiness_quote_capture_20260512.md`
- Planned May 13 quote capture: `gs://codexalpaca-control-us/paper_runs/multi_symbol_governed_realtime_20260513/realtime_quote_shadow/`

## Next Best Steps

1. Run May 13 PAPER preflight after fresh RTH SIP bars.
2. Start PAPER order submission only after the launch contract passes.
3. Run the no-submit realtime quote shadow stream during the session.
4. Convert the stream to quote-quality sidecars after RTH.
5. Apply `option_quote_sidecar.csv` to same-day replay economics with the as-of join script.
6. Rerun projection and optimizer only after replay rows have quote-backed bid/ask, spread, quote-age, and trade-print evidence.

No new strategy is eligible for paper-runner addition from this pass. The priority is quote-backed evidence collection and causal replay integration.

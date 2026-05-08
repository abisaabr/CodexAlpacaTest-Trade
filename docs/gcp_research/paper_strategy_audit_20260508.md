## Paper Strategy Audit — May 8, 2026 (PAPER readiness)

**Audit target**
- Portfolio config: `config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`
- Strategy manifests referenced (resolved relative to the config’s directory):
  - `config/promotion_manifests/multi_symbol_governed_validation_20260508_runtime_unique.yaml`
  - `config/promotion_manifests/tsm_overnight_governed_validation_20260508.yaml`
  - `config/promotion_manifests/avgo_overnight_governed_validation_20260508.yaml`
  - `config/promotion_manifests/avgo_c109_overnight_governed_validation_20260508.yaml`
  - `config/promotion_manifests/qqq_overnight_governed_validation_20260508.yaml`

### Summary (PASS with 2 operational caveats)
- Config + manifests load cleanly via `alpaca_lab.multi_ticker_portfolio.config.load_portfolio_config(...)` (Pydantic validation passes).
- **Unique strategy IDs:** 192 total strategies loaded; **0 duplicates** across all referenced manifests.
- **Symbol coverage:** Config `execution.underlying_symbols` exactly matches symbols present in the manifests (QQQ, SPY, IWM, AMD, AMZN, MSFT, TSLA, AVGO, GOOGL, TSM).
- **Leg semantics:** Supported by runner (single-leg, 2-leg verticals, and 4-leg broken-wing butterflies observed).
- **Feeds:** `execution.option_feed=opra` (used for option snapshots) and `execution.stock_feed=sip` (used for stock bars) are consistent with broker adapter call sites.
- **EOD flatten:** `execution.eod_flatten_minutes_before_close=(10, 2)` and `execution.auto_flatten_unexpected_positions=true` are enabled.
- **Paper-only guardrails:** `execution.submit_paper_orders=true` and `execution.paper_order_arming_mode=config_explicit` (explicitly armed, still paper-only).

### Operational caveats to verify before running (May 8)
1) **Broker equity floor may block entries**
   - Config sets `risk.broker_min_equity_to_trade=26000` with `risk.sleeve_starting_equity=25000`.
   - If the Alpaca paper account equity is **< $26,000**, the runner will skip new entries with reason `broker_equity_below_trade_floor`.
   - Action: confirm paper account equity is >= $26k, or expect a “no-entries” session (by design).

2) **Machine portability: absolute `D:/...` state paths**
   - `execution.state_root`, `execution.run_root`, and `ownership.lease_path` are hard-coded to `D:/codexalpaca_runtime/...`.
   - Action: verify the target machine has a `D:` drive with permissions, or the run will fail early when writing state/lease files.

### Notes on strategy composition (for quick sanity)
- Signal distribution: `governed_breakout_put` (84), `governed_lower_band_reversion_call` (72), `governed_breakout_call` (36).
- Leg counts: 173x single-leg, 13x two-leg, 6x four-leg (broken-wing butterflies).

### Focused tests executed
- `pytest -q tests/test_config.py tests/test_broker_safeguards.py tests/test_broker_multileg_orders.py`
- Result: **21 passed**


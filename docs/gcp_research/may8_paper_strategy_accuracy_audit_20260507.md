# May 8 PAPER Strategy Accuracy Audit

Generated: 2026-05-07 ET

## Runtime Config Reviewed

- Portfolio config: `config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`
- Runtime manifest base: `config/promotion_manifests/multi_symbol_governed_validation_20260508_runtime_unique.yaml`
- Newly added governed packet: `config/promotion_manifests/avgo_c109_overnight_governed_validation_20260508.yaml`
- Broker mode: PAPER only
- Live manifest effect: none

## Strategy Audit Result

- Total runtime strategies: 192
- Runtime symbols: AMD, AMZN, AVGO, GOOGL, IWM, MSFT, QQQ, SPY, TSLA, TSM
- Single-leg strategies: 173
- Native multi-leg strategies: 19
- Duplicate runtime strategy IDs: 0
- Strategy symbols missing from execution symbols: 0
- Multi-leg runner semantics mismatches: 0 after repair
- Fill gate check: all loaded strategies with `min_fill_coverage` clear `>= 0.90`

Regime count:

- bull: 39
- bear: 81
- choppy: 72

Runtime guardrails still active:

- `execution.option_feed: opra`
- `execution.stock_feed: sip`
- `execution.submit_paper_orders: true`
- `execution.paper_order_arming_mode: config_explicit`
- `risk.max_open_positions: 999`
- `risk.max_positions_per_symbol: 3`
- `risk.max_open_risk_fraction: 0.14`
- `risk.max_open_risk_fraction_per_symbol: 0.08`
- `risk.broker_min_equity_to_trade: 26000`
- `risk.broker_equity_emergency_stop: 25500`
- `execution.eod_flatten_minutes_before_close: [10, 2]`
- `execution.auto_flatten_unexpected_positions: true`

## Changes Made

- Added eligible AVGO `c109-144` governed-review packet to the May 8 PAPER config.
- Repaired stale multi-leg audit labels in `multi_symbol_governed_validation_20260508_runtime_unique.yaml` so every multi-leg strategy now reports `packet_translated_to_runtime_native_multileg`.
- Did not lower promotion gates, risk policy, or fill gate.
- Did not change the live strategy manifest.

## Newly Synced Research Results

Eligible for governed validation and added:

- `avgo_regime_rescue_c109_144`: `ready_for_governed_validation_review`, 6 review candidates, bull/bear/choppy complete.

Research-only blocked, not added:

- `amd_regime_rescue_c109_144`: regime-incomplete, bear/choppy only.
- `tsm_regime_rescue_c073_108`: regime-incomplete, bear/choppy only.
- `qqq_regime_rescue_c109_144`: regime-incomplete, bear/choppy only.
- `spy_regime_rescue_c109_144`: regime-incomplete, bear/choppy only.
- `micro_event_c89089_89600`: research-only microstructure shard, zero review-like candidates.
- `micro_event_c90113_90624`: research-only microstructure shard, zero review-like candidates.

## Active Research Workers After Cleanup

Traditional realtime-compatible wave:

- `aapl-rescue-c037-072-20260507trad15`
- `nvda-rescue-c037-072-20260507trad15`
- `intc-rescue-c037-072-20260507trad15`
- `meta-rescue-c037-072-20260507trad15`

Microstructure rare-event wave:

- `micro-event-c91137-91648-20260507om4`
- `micro-event-c91649-92160-20260507om5`

Terminated workers with synced artifacts were deleted to free quota.

## Verification

Focused tests passed:

```text
python -m pytest -q tests\test_build_governed_validation_manifest_from_packets.py tests\test_runner_submit_order_arming.py tests\test_build_broker_fill_strategy_reconciliation.py
8 passed
```

Offline config audit passed with no issues after the multi-leg semantics repair.

## May 8 Launch Notes

Before starting the PAPER trader, run a fresh startup preflight after SIP stock bars are current and confirm:

- No duplicate broker-facing process exists.
- Broker endpoint is PAPER.
- No unexpected PAPER open orders or positions exist.
- Ownership lease path is valid for May 8.
- Account equity clears `broker_min_equity_to_trade` or the launch remains correctly blocked by risk controls.

Do not add research-only blocked packets to the May 8 runtime config without a generated eligible packet or a separately documented operator-approved PAPER experiment.

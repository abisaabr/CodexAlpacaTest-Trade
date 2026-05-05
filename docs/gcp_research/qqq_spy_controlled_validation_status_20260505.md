# QQQ + SPY Controlled Validation Status - 2026-05-05

## Scope

This packet creates a broker-free controlled validation lane for the two symbols that are currently regime-complete:

- QQQ
- SPY

IWM is intentionally excluded because the first institutional pass produced only a bull candidate. IWM bear and choppy remain redesign work.

## Runner Config

Combined portfolio config:

- `config/qqq_spy_regime_complete_paper_portfolio.yaml`

The config uses `strategy_manifest_paths` so the runner can load two audited strategy manifests without duplicating strategy definitions:

- `config/promotion_manifests/qqq_regime_complete_governed_validation_20260505.yaml`
- `config/promotion_manifests/spy_regime_complete_governed_validation_20260505.yaml`

Runtime posture:

- Broker-facing trading: `false`
- Default order submission: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Strategy count: `6`
- Symbols: `QQQ`, `SPY`

## Validation

Local config load:

- `config/qqq_spy_regime_complete_paper_portfolio.yaml` loads successfully.
- Loaded strategies: `6`
- Loaded symbols: `QQQ`, `SPY`

Focused test gate:

- Command: `python -m pytest -q tests\test_multi_ticker_portfolio.py tests\test_build_qqq_paper_launch_pack.py tests\test_build_qqq_shadow_validation_packet.py`
- Result: `68 passed`

No-order startup preflight:

- Local: `reports/gcp_research/qqq_spy_regime_complete_paper_launch_pack_20260505/startup_preflight_no_orders_0825ET.json`
- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_regime_complete_20260505/paper_launch_pack/startup_preflight_no_orders_0825ET.json`
- Result: `startup_preflight_pending`
- Pending reasons:
  - `QQQ stock frame not ready yet`
  - `SPY stock frame not ready yet`
- Buying power: `399225.72`
- Broker equity: `99806.43`
- Broker positions: `0`
- Open orders: `0`
- Orders submitted: `false`

## Code Change

The portfolio config loader now supports `strategy_manifest_paths` in addition to the existing single `strategy_manifest_path`.

Reason:

- QQQ and SPY should remain separately audited governed-validation packets.
- The paper validation runner still needs a single portfolio config.
- Multi-manifest loading avoids copying strategy blocks and keeps handoff cleaner.

## Next Steps

1. Re-run QQQ no-order preflight after live RTH stock frames are available.
2. If QQQ passes, run QQQ broker-free `--run-once` shadow validation.
3. Re-run SPY no-order preflight after live RTH stock frames are available.
4. If QQQ and SPY both pass broker-free checks, use the combined QQQ+SPY config for controlled broker-free validation.
5. Do not submit broker-facing paper orders without explicit operator approval.

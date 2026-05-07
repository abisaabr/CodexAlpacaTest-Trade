# May 7 Paper Activation Update

Date: 2026-05-07

## Operator Request

The operator requested three changes for the May 7 PAPER session:

- Remove the global `max_open_positions` cap for today.
- Promote bull/bear Greek-based candidates where governed packet evidence supports them.
- Promote realtime strategies where packet evidence supports them.

This update keeps the existing safety distinction:

- The full QQQ/SPY/IWM Greek/realtime-parity packet remains blocked because it is not bull/bear/choppy complete.
- A separate bull/bear-scoped Greek packet was generated and is allowed only for bull/bear governed-validation review.
- Multi-leg Greek candidates remain excluded from the PAPER manifest until native runtime support exists.

## Greek/Realtime-Parity Packet State

Full strict packet:

- Path: `reports/gcp_research/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_final_20260507T054014/promotion_packet/research_promotion_review_packet.json`
- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_final_20260507T054014/`
- Decision: `research_only_blocked_regime_incomplete`
- Candidate profiles: 6,848
- Portfolio-gate eligible profiles: 116
- Review candidates: 20
- Eligible regimes: bull, bear
- Missing regime: choppy

Bull/bear-scoped packet:

- Path: `reports/gcp_research/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_bull_bear_scoped_20260507T0915/promotion_packet/research_promotion_review_packet.json`
- GCS: `gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_expanded_20260507T0315Z/aggregate_bull_bear_scoped_20260507T0915/`
- Decision: `ready_for_governed_validation_review`
- Candidate profiles: 6,848
- Portfolio-gate eligible profiles: 116
- Review candidates: 20
- Required regimes: bull, bear
- Missing regimes: none

## Manifest Change

Generated May 7 governed-validation manifest:

- Path: `config/promotion_manifests/multi_symbol_governed_validation_20260507.yaml`
- Strategy count: 144
- Symbols: AMD, AMZN, AVGO, GOOGL, IWM, MSFT, QQQ, SPY, TSLA, TSM
- Regimes: bull, bear, choppy
- Runtime-supported families included: single-leg long call, single-leg long put
- Skipped candidates: 16

Skipped candidates are intentionally visible. They are not hidden or treated as promoted. The skipped set is dominated by unsupported runtime multi-leg Greek families such as debit verticals, credit spreads, and broken-wing butterflies.

## PAPER Config Change

Updated May 7 armed PAPER config:

- Path: `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`
- Strategy manifest path changed from `multi_symbol_governed_validation_20260506.yaml` to `multi_symbol_governed_validation_20260507.yaml`.
- Global `risk.max_open_positions` changed from `8` to `999`.

This neutralizes only the global position-count cap. These guards remain active:

- `max_positions_per_symbol: 3`
- `max_positions_per_regime: 3`
- `max_positions_per_regime_window: 2`
- `max_positions_per_bucket_regime_window: 4`
- `max_open_risk_fraction: 0.14`
- `max_open_risk_fraction_per_symbol: 0.08`
- bucket risk caps
- broker minimum equity and emergency stop
- spread, freshness, and EOD flatten controls

## Runtime Count After Update

The May 7 PAPER config loads successfully with:

- Tickers: 10
- Strategies: 144
- Bull strategies: 21
- Bear strategies: 62
- Choppy strategies: 61
- Single-leg long call strategies: 82
- Single-leg long put strategies: 62

Per-symbol strategy count:

- AMD: 15
- AMZN: 5
- AVGO: 20
- GOOGL: 20
- IWM: 4
- MSFT: 20
- QQQ: 19
- SPY: 20
- TSLA: 2
- TSM: 19

## Launch Rule

Do not start live trading.

The PAPER trader may submit PAPER orders only after:

- Startup preflight passes.
- Broker endpoint is PAPER.
- No duplicate paper-trader process exists.
- No unexpected PAPER open orders or positions exist.
- Fresh SIP/OPRA data checks are acceptable.

Startup preflight command:

```powershell
$env:MULTI_TICKER_MACHINE_LABEL = "local-primary-paper-20260507"
$env:MULTI_TICKER_OWNERSHIP_LEASE_PATH = "D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260507_ownership_lease.json"
python scripts\run_multi_ticker_portfolio_paper_trader.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml `
  --startup-preflight `
  --no-submit-paper-orders
```

Order-submitting PAPER command after a clean preflight:

```powershell
$env:MULTI_TICKER_MACHINE_LABEL = "local-primary-paper-20260507"
$env:MULTI_TICKER_OWNERSHIP_LEASE_PATH = "D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260507_ownership_lease.json"
python scripts\run_multi_ticker_portfolio_paper_trader.py `
  --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml `
  --submit-paper-orders
```

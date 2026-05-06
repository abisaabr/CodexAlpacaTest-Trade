# Multi-Symbol Paper Readiness And AVGO Research Status

Date context: 2026-05-05 evening America/New_York, preparing for 2026-05-06 RTH.

This packet is PAPER-only and research-governed. It does not start live trading and does not modify the live strategy manifest.

## Paper Portfolio Prepared For 2026-05-06 RTH

Created a generated governed-validation strategy manifest from regime-complete promotion-review packets:

- Manifest builder: `scripts/build_governed_validation_manifest_from_packets.py`
- Generated manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260506.yaml`
- Paper config: `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`
- Strategy count: `60`
- Symbols: `AMD, AMZN, MSFT, QQQ, SPY, TSLA`
- Portfolio-level regimes represented: `bull, bear, choppy`
- Config default: `submit_paper_orders=false`

Source packets used:

- QQQ/SPY: `reports/gcp_research/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- AMD/AMZN: `reports/gcp_research/amd_amzn_full_regime_rescue_20260505T2315Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- MSFT/TSLA: `reports/gcp_research/msft_tsla_full_regime_rescue_20260506T0025Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`

Excluded from this May 6 paper config:

- AAPL/NVDA: blocked because the combined packet is regime-incomplete.
- INTC/META: blocked because the combined packet is regime-incomplete.
- IWM: kept out of this new paper config because the latest explicit handoff blocked IWM paper activation pending newer fill-semantics validation, even though an older strict packet exists.

## No-Order Preflight Result

Command:

```powershell
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --startup-preflight --no-submit-paper-orders
```

Result:

- Status: `startup_preflight_failed`
- Submit paper orders: `false`
- Broker positions: `0`
- Open broker orders: `0`
- Buying power: `$399,225.72`
- Broker equity: `$99,806.43`
- Failure reason: stock data was stale after the 2026-05-05 close for all six symbols.

Interpretation: this is an expected after-hours failure, not a manifest/schema failure. Re-run the same preflight near the 2026-05-06 RTH launch window.

## RTH Launch Sequence

Use this sequence on 2026-05-06 before any paper order submission:

```powershell
git fetch origin codex/phase2-fill-semantics-20260430
git checkout codex/phase2-fill-semantics-20260430
git reset --hard origin/codex/phase2-fill-semantics-20260430
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --startup-preflight --no-submit-paper-orders
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --run-once --no-submit-paper-orders
python scripts\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_symbol_governed_realtime_paper_portfolio_20260506.yaml --submit-paper-orders
```

Do not run the final command unless the startup preflight passes with fresh SIP/OPRA data and broker state remains clean.

## AVGO Research Wave Launched

Launched the next one-ticker research wave using the next10 365d 5x5 data:

- Wave ID: `avgo_full_regime_rescue_20260506T0135Z`
- GCS root: `gs://codexalpaca-control-us/research_results/avgo_full_regime_rescue_20260506T0135Z`
- Symbol: `AVGO`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60,10:60,30:120`
- Bear profile set: `signal_window_refine`
- Choppy profile set: `timewindow_quality_filter`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

Launched workers:

- `avgo-rescue-c001-021-20260506a`
- `avgo-rescue-c022-042-20260506a`
- `avgo-rescue-c043-063-20260506a`
- `avgo-rescue-c064-084-20260506a`
- `avgo-rescue-c085-105-20260506a`
- `avgo-rescue-c106-126-20260506a`
- `avgo-rescue-c127-147-20260506a`
- `avgo-rescue-c148-166-20260506a`

## Next Research Loop

1. Monitor AVGO worker statuses under the wave GCS root.
2. When all AVGO shards self-stop and upload reports, aggregate worker outputs into a strict portfolio report and promotion-review packet.
3. If AVGO is regime-complete, add it to the governed-validation manifest and rerun production-risk projection.
4. If AVGO is blocked, classify blockers by fill coverage, full-period PnL, test PnL, trade count, and sizing.
5. Then advance to the next available next10 ticker, one ticker at a time, starting with `GOOGL`, then `MU`, `NFLX`, `ORCL`, `PLTR`, `TSM`, `XLE`, `XOM`.

## Hard Rules

- Paper mode only.
- No live orders.
- Do not lower the `fill_coverage >= 0.90` gate.
- Do not present blocked or regime-incomplete candidates as promoted.
- Do not add a ticker to the paper config unless a generated packet supports governed-validation review or a fresh packet is built from strict gates.
- Promotion here means governed validation review; runtime paper order submission remains a separate explicit command.

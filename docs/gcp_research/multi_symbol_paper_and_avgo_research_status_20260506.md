# Multi-Symbol Paper Readiness And AVGO Research Status

Date context: 2026-05-05 evening America/New_York, preparing for 2026-05-06 RTH.

This packet is PAPER-only and research-governed. It does not start live trading and does not modify the live strategy manifest.

## Paper Portfolio Prepared For 2026-05-06 RTH

Created a generated governed-validation strategy manifest from regime-complete promotion-review packets:

- Manifest builder: `scripts/build_governed_validation_manifest_from_packets.py`
- Generated manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260506.yaml`
- Paper config: `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`
- Strategy count: `100`
- Symbols: `AMD, AMZN, AVGO, GOOGL, MSFT, QQQ, SPY, TSLA`
- Portfolio-level regimes represented: `bull, bear, choppy`
- Config default: `submit_paper_orders=false`

Source packets used:

- QQQ/SPY: `reports/gcp_research/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- AMD/AMZN: `reports/gcp_research/amd_amzn_full_regime_rescue_20260505T2315Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- MSFT/TSLA: `reports/gcp_research/msft_tsla_full_regime_rescue_20260506T0025Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- AVGO: `reports/gcp_research/avgo_full_regime_rescue_20260506T0135Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GOOGL: `reports/gcp_research/googl_full_regime_rescue_20260506T0225Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`

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
- Failure reason: stock data was stale after the 2026-05-05 close for all configured symbols.

Interpretation: this is an expected after-hours failure, not a manifest/schema failure. Re-run the same preflight near the 2026-05-06 RTH launch window.

After AVGO and GOOGL were added, no-order startup preflights were run. They failed only because after-hours stock data was stale; the startup checks loaded all configured symbols and showed broker state remained clean with `0` broker positions and `0` open orders.

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

## AVGO Research Wave Result

The AVGO full-regime research wave completed, was aggregated, and was mirrored to GCS:

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
- Portfolio report: `reports/gcp_research/avgo_full_regime_rescue_20260506T0135Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/avgo_full_regime_rescue_20260506T0135Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `ready_for_governed_validation_review`
- Eligible candidates: `191`
- Unique eligible base candidates in packet view: `29`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bull,bear,choppy`
- Regime complete: `true`
- Full-population blockers: `fill_coverage_below_0.90=137`, `min_net_pnl_not_positive=232`, `test_net_pnl_not_above_0=184`

Promotion-chain reliability note: the first AVGO aggregate exposed a promoter-boundary issue where the gate summary was regime-complete, but the top review/capital candidate slices omitted the lower-scoring eligible bull representative. The report, promotion-packet, and generated-manifest builders were patched so `eligible_regime_representatives` are preserved explicitly and consumed by the manifest builder before top-candidate pruning.

Completed and deleted workers:

- `avgo-rescue-c001-021-20260506a`
- `avgo-rescue-c022-042-20260506a`
- `avgo-rescue-c043-063-20260506a`
- `avgo-rescue-c064-084-20260506a`
- `avgo-rescue-c085-105-20260506a`
- `avgo-rescue-c106-126-20260506a`
- `avgo-rescue-c127-147-20260506a`
- `avgo-rescue-c148-166-20260506a`

## Active GOOGL Research Wave

GOOGL completed, was aggregated, and was mirrored to GCS:

- Wave ID: `googl_full_regime_rescue_20260506T0225Z`
- GCS root: `gs://codexalpaca-control-us/research_results/googl_full_regime_rescue_20260506T0225Z`
- Symbol: `GOOGL`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Portfolio report: `reports/gcp_research/googl_full_regime_rescue_20260506T0225Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/googl_full_regime_rescue_20260506T0225Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `ready_for_governed_validation_review`
- Eligible candidates: `66`
- Unique eligible base candidates in packet view: `25`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bull,bear,choppy`
- Regime complete: `true`
- Full-population blockers: `fill_coverage_below_0.90=26`, `min_net_pnl_not_positive=392`, `test_net_pnl_not_above_0=206`
- Completed workers were deleted after aggregation.

## MU Research Wave Result

MU completed, was aggregated, and was mirrored to GCS. It is not eligible for the governed paper manifest because it is regime-incomplete:

- Wave ID: `mu_full_regime_rescue_20260506T0310Z`
- GCS root: `gs://codexalpaca-control-us/research_results/mu_full_regime_rescue_20260506T0310Z`
- Symbol: `MU`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Portfolio report: `reports/gcp_research/mu_full_regime_rescue_20260506T0310Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/mu_full_regime_rescue_20260506T0310Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `research_only_blocked_regime_incomplete`
- Eligible candidates: `14`
- Unique eligible base candidates in packet view: `14`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `choppy`
- Missing eligible regimes: `bull,bear`
- Regime complete: `false`
- Full-population blockers: `fill_coverage_below_0.90=402`, `min_net_pnl_not_positive=385`, `test_net_pnl_not_above_0=277`
- Completed workers were deleted after aggregation.

## NFLX Research Wave Result

NFLX completed, was aggregated, and was mirrored to GCS. It is not eligible for the governed paper manifest because it is regime-incomplete:

- Wave ID: `nflx_full_regime_rescue_20260506T0350Z`
- GCS root: `gs://codexalpaca-control-us/research_results/nflx_full_regime_rescue_20260506T0350Z`
- Symbol: `NFLX`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Portfolio report: `reports/gcp_research/nflx_full_regime_rescue_20260506T0350Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/nflx_full_regime_rescue_20260506T0350Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `research_only_blocked_regime_incomplete`
- Eligible candidates: `2`
- Unique eligible base candidates in packet view: `1`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bull`
- Missing eligible regimes: `bear,choppy`
- Regime complete: `false`
- Full-population blockers: `fill_coverage_below_0.90=493`, `min_net_pnl_not_positive=474`, `test_net_pnl_not_above_0=429`
- Completed workers were deleted after aggregation.

## ORCL Research Wave Result

ORCL completed, was aggregated, and was mirrored to GCS. It is not eligible for the governed paper manifest because it is regime-incomplete:

- Wave ID: `orcl_full_regime_rescue_20260506T0430Z`
- GCS root: `gs://codexalpaca-control-us/research_results/orcl_full_regime_rescue_20260506T0430Z`
- Symbol: `ORCL`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Portfolio report: `reports/gcp_research/orcl_full_regime_rescue_20260506T0430Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/orcl_full_regime_rescue_20260506T0430Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `research_only_blocked_regime_incomplete`
- Eligible candidates: `70`
- Unique eligible base candidates in packet view: `27`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bear,choppy`
- Missing eligible regimes: `bull`
- Regime complete: `false`
- Full-population blockers: `fill_coverage_below_0.90=68`, `min_net_pnl_not_positive=401`, `test_net_pnl_not_above_0=290`
- Completed workers were deleted after aggregation.

## PLTR Research Wave Result

PLTR completed, was aggregated, and was mirrored to GCS. It is not eligible for the governed paper manifest because it is regime-incomplete:

- Wave ID: `pltr_full_regime_rescue_20260506T0500Z`
- GCS root: `gs://codexalpaca-control-us/research_results/pltr_full_regime_rescue_20260506T0500Z`
- Symbol: `PLTR`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Portfolio report: `reports/gcp_research/pltr_full_regime_rescue_20260506T0500Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/pltr_full_regime_rescue_20260506T0500Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `research_only_blocked_regime_incomplete`
- Eligible candidates: `27`
- Unique eligible base candidates in packet view: `11`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bear,choppy`
- Missing eligible regimes: `bull`
- Regime complete: `false`
- Full-population blockers: `fill_coverage_below_0.90=31`, `min_net_pnl_not_positive=469`, `test_net_pnl_not_above_0=380`
- Completed workers were deleted after aggregation.

## Active TSM Research Wave

TSM is now running as the next one-ticker full-regime rescue wave:

- Wave ID: `tsm_full_regime_rescue_20260506T0540Z`
- GCS root: `gs://codexalpaca-control-us/research_results/tsm_full_regime_rescue_20260506T0540Z`
- Symbol: `TSM`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Workers: `8`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`

## Next Research Loop

1. Monitor TSM worker statuses under the wave GCS root.
2. When all TSM shards self-stop and upload reports, aggregate worker outputs into a strict portfolio report and promotion-review packet.
3. If TSM is regime-complete, add it to the governed-validation manifest and rerun production-risk projection.
4. If TSM is blocked, classify blockers by fill coverage, full-period PnL, test PnL, trade count, and sizing.
5. Then advance to the next available next10 ticker, one ticker at a time: `XLE`, `XOM`.

## Hard Rules

- Paper mode only.
- No live orders.
- Do not lower the `fill_coverage >= 0.90` gate.
- Do not present blocked or regime-incomplete candidates as promoted.
- Do not add a ticker to the paper config unless a generated packet supports governed-validation review or a fresh packet is built from strict gates.
- Promotion here means governed validation review; runtime paper order submission remains a separate explicit command.

# Multi-Symbol Paper Readiness And AVGO Research Status

Date context: 2026-05-05 evening America/New_York, preparing for 2026-05-06 RTH.

This packet is PAPER-only and research-governed. It does not start live trading and does not modify the live strategy manifest.

## Paper Portfolio Prepared For 2026-05-06 RTH

Created a generated governed-validation strategy manifest from regime-complete promotion-review packets:

- Manifest builder: `scripts/build_governed_validation_manifest_from_packets.py`
- Generated manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260506.yaml`
- Paper config: `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`
- Strategy count: `120`
- Symbols: `AMD, AMZN, AVGO, GOOGL, MSFT, QQQ, SPY, TSLA, TSM`
- Portfolio-level regimes represented: `bull, bear, choppy`
- Config default: `submit_paper_orders=false`

Source packets used:

- QQQ/SPY: `reports/gcp_research/qqq_spy_combined_regime_portfolio_20260505T2240Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- AMD/AMZN: `reports/gcp_research/amd_amzn_full_regime_rescue_20260505T2315Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- MSFT/TSLA: `reports/gcp_research/msft_tsla_full_regime_rescue_20260506T0025Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- AVGO: `reports/gcp_research/avgo_full_regime_rescue_20260506T0135Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- GOOGL: `reports/gcp_research/googl_full_regime_rescue_20260506T0225Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- TSM: `reports/gcp_research/tsm_full_regime_rescue_20260506T0540Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`

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

After TSM was added, another no-order startup preflight was run against `config/multi_symbol_governed_realtime_paper_portfolio_20260506.yaml`. It returned `startup_preflight_pending` because 2026-05-06 RTH stock frames were not ready yet for the configured symbols. Broker state remained clean with `0` broker positions, `0` open orders, `$399,225.72` buying power, and `$99,806.43` broker equity.

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

## TSM Research Wave Result

TSM completed, was aggregated, and was mirrored to GCS. It is eligible for governed-validation review and was added to the generated governed-validation manifest and paper config:

- Wave ID: `tsm_full_regime_rescue_20260506T0540Z`
- GCS root: `gs://codexalpaca-control-us/research_results/tsm_full_regime_rescue_20260506T0540Z`
- Symbol: `TSM`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Portfolio report: `reports/gcp_research/tsm_full_regime_rescue_20260506T0540Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/tsm_full_regime_rescue_20260506T0540Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `ready_for_governed_validation_review`
- Eligible candidates: `49`
- Unique eligible base candidates in packet view: `35`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bull,bear,choppy`
- Regime complete: `true`
- Full-population blockers: `fill_coverage_below_0.90=228`, `min_net_pnl_not_positive=311`, `test_net_pnl_not_above_0=373`
- Completed workers were deleted after aggregation.

The generated governed-validation manifest now includes:

- Manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260506.yaml`
- Strategy count: `120`
- Symbols: `AMD, AMZN, AVGO, GOOGL, MSFT, QQQ, SPY, TSLA, TSM`
- Source packet count: `6`

The PAPER config now includes TSM in `execution.underlying_symbols` and the existing `growth_tech` risk bucket without changing risk limits. `submit_paper_orders` remains `false`.

## XLE Research Wave Result

XLE completed, was aggregated, and was mirrored to GCS. It is not eligible for the governed paper manifest because it is regime-incomplete:

- Wave ID: `xle_full_regime_rescue_20260506T0620Z`
- GCS root: `gs://codexalpaca-control-us/research_results/xle_full_regime_rescue_20260506T0620Z`
- Symbol: `XLE`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Portfolio report: `reports/gcp_research/xle_full_regime_rescue_20260506T0620Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/xle_full_regime_rescue_20260506T0620Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `research_only_blocked_regime_incomplete`
- Eligible candidates: `10`
- Unique eligible base candidates in packet view: `10`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bear,choppy`
- Missing eligible regimes: `bull`
- Regime complete: `false`
- Full-population blockers: `fill_coverage_below_0.90=232`, `min_net_pnl_not_positive=464`, `option_trades_below_20=26`, `test_net_pnl_not_above_0=443`
- Completed workers were deleted after aggregation.

## XOM Research Wave Result

XOM completed, was aggregated, and was mirrored to GCS. It is not eligible for the governed paper manifest because it is regime-incomplete:

- Wave ID: `xom_full_regime_rescue_20260506T0645Z`
- GCS root: `gs://codexalpaca-control-us/research_results/xom_full_regime_rescue_20260506T0645Z`
- Symbol: `XOM`
- Candidate count: `166`
- Target regimes: `bull,bear,choppy`
- Broker-facing: `false`
- Paper orders: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Stock data: `gs://codexalpaca-data-us/research_stock_data/option_fill_ladder_next10_20260429/XOM/365d_5x5/stock_ref_silver/stock_bars`
- Selected contracts: `gs://codexalpaca-control-us/research_results/option_fill_ladder_next10_20260429/XOM/365d_5x5/research_wave/dense_universe/selected_option_contracts`
- Option bars: `gs://codexalpaca-data-us/research_option_data/option_fill_ladder_next10_20260429/XOM/365d_5x5/option_bars_silver/option_bars`
- Portfolio report: `reports/gcp_research/xom_full_regime_rescue_20260506T0645Z/aggregate/combined_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/xom_full_regime_rescue_20260506T0645Z/aggregate/combined_promotion_packet/research_promotion_review_packet.json`
- Packet decision: `research_only_blocked_regime_incomplete`
- Eligible candidates: `28`
- Unique eligible base candidates in packet view: `15`
- Required regimes: `bull,bear,choppy`
- Eligible regimes: `bull,bear`
- Missing eligible regimes: `choppy`
- Regime complete: `false`
- Full-population blockers: `fill_coverage_below_0.90=52`, `min_net_pnl_not_positive=454`, `option_trades_below_20=2`, `test_net_pnl_not_above_0=391`
- Completed workers were deleted after aggregation.

## Next Research Loop

The available next10 dense-data ticker set for this overnight pass has been exhausted:

- Processed as governed-validation additions: `AVGO`, `GOOGL`, `TSM`
- Processed but blocked as regime-incomplete: `MU`, `NFLX`, `ORCL`, `PLTR`, `XLE`, `XOM`
- Already covered separately before this loop: `QQQ`

Next safe actions:

1. Keep the 2026-05-06 RTH paper config unchanged until a fresh RTH preflight passes.
2. Do not add blocked tickers to the governed paper manifest.
3. If more overnight research is needed, start a new data-download lane for additional liquid symbols before launching more full-regime rescue waves.
4. During RTH preparation, prioritize startup preflight, no-order run-once validation, broker state checks, and log archival before any explicit PAPER order-submission command.

## Hard Rules

- Paper mode only.
- No live orders.
- Do not lower the `fill_coverage >= 0.90` gate.
- Do not present blocked or regime-incomplete candidates as promoted.
- Do not add a ticker to the paper config unless a generated packet supports governed-validation review or a fresh packet is built from strict gates.
- Promotion here means governed validation review; runtime paper order submission remains a separate explicit command.

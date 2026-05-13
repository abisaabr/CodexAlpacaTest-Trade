# Optimizer And Bear/Choppy Wave Status - 2026-05-12

## Summary

The six-step continuation completed in research-only mode. The May 8 paper postmortem, AVGO choppy runtime review, IWM structure-aware diagnostic, current-paper projection/optimizer reruns, and a new bear/choppy non-single refinement wave were produced without starting live trading or changing paper/live manifests.

## Safety State

- Broker mode used for this continuation: research-only, non-broker-facing.
- Live trading started: no.
- Paper trader started or duplicated: no.
- Paper-runner state changed: no.
- Live manifest changed: no.
- Global risk policy changed: no.
- Fill gate changed: no, `fill_coverage >= 0.90` remained intact.
- Local broker-facing paper process check during continuation: no `run_multi_ticker_portfolio_paper_trader.py` process was running.

## Source And Artifacts

- Source commit for this handoff: `02c7fd9`.
- Current branch: `codex/phase2-fill-semantics-20260430`.
- Optimizer local root: `reports/gcp_research/optimizer_continuation_20260512T1315Z/`.
- Optimizer GCS root: `gs://codexalpaca-control-us/research_results/optimizer_continuation_20260512T1315Z/`.
- New wave ID: `bear_choppy_non_single_refine_20260512T1325Z`.
- New wave GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1325Z/`.
- New wave local root: `reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/`.
- Handoff docs GCS root: `gs://codexalpaca-control-us/gcp_research/`.

## May 8 Paper Postmortem

- Local doc: `docs/gcp_research/may8_paper_postmortem_20260508.md`.
- Starting equity: `$25,000.00`.
- Ending/virtual cash: `$25,262.56`.
- Realized reconciled net PnL: `$262.56`.
- Completed trades: `4`.
- EOD flatten checkpoints: `10` and `2` minutes before close, both completed.
- Ending broker position count: `0`.
- Main runtime issue: entry execution circuit breaker after 3 consecutive entry failures.
- Main execution issue: one GOOGL open trade generated repeated exit-not-filled warnings before EOD flatten.

## Runtime And Selector Diagnostics

- AVGO choppy review doc: `docs/gcp_research/avgo_choppy_candidate_runtime_review_20260508.md`.
- IWM diagnostic doc: `docs/gcp_research/iwm_structure_aware_diagnostic_20260508.md`.
- IWM diagnostic output: `reports/gcp_research/bear_choppy_structure_aware_rerun_20260508T1745ET/aggregate/iwm_structure_aware_diagnostic/`.
- AVGO choppy interpretation: research evidence only; candidates need runtime/shadow review before any activation.
- IWM interpretation: blocked primarily by economics, not selected-contract availability. No IWM candidate from that shard should be promoted.

## Projection And Optimizer Results

- Full current paper lineage projection: `reports/gcp_research/optimizer_continuation_20260512T1315Z/current_paper_full_lineage_manifest_sized/portfolio_growth_projection.json`.
- Full current paper lineage result: ending equity `$29,735.06`, total return `18.9402%`, max drawdown `-27.6939%`, accepted trades `4,749`, matched trades `36,326`.
- `$200/day` optimizer: `failed`; no feasible diversified portfolio met the target with current eligible candidates.
- `$100/day` optimizer: `passed`; selected `6`, average daily PnL `$104.10`, ending equity `$37,803.91`, max drawdown `-4.0624%`, but concentration remains too high for institutional use.
- Paper-like optimizer: `passed`; selected `10`, average daily PnL `$53.20`, ending equity `$34,470.35`, max drawdown `-2.3584%`.
- Benchmark `tt_top2_bull_choppy_up`: ending equity `$35,949.02`, return `43.7961%`, max drawdown `-18.2398%`.
- Benchmark plus structure-aware candidates: ending equity `$34,927.80`, return `39.7112%`, max drawdown `-20.9213%`.
- Conclusion: use `tt_top2_bull_choppy_up` as the benchmark; adding structure-aware AVGO candidates degraded the projection.

## New GCP Wave

- Launch root: `reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/`.
- GCS root: `gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1325Z/`.
- Broker-facing: `false`.
- Paper orders: `false`.
- Live manifest effect: `none`.
- Risk policy effect: `none`.
- Symbols launched: `QQQ`, `SPY`, `IWM`, `AVGO`, `GOOGL`, `MSFT`, `AMZN`, `TSM`.
- Candidate index range: `c145-168`.
- Target regimes: `bear,choppy`.
- Main families: `debit_call_vertical`, `debit_put_vertical`, `bull_put_credit_spread`, `bear_call_credit_spread`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`.
- Selectors: `entry_liquidity_first_research_only`.
- Lag profiles: `0:60,10:60,30:120`.
- Worker VMs launched and cleaned after sync: `qqq-rescue-c145-168-20260512bc3`, `spy-rescue-c145-168-20260512bc3`, `iwm-rescue-c145-168-20260512bc3`, `avgo-rescue-c145-168-20260512bc3`, `googl-rescue-c145-168-20260512bc3`, `msft-rescue-c145-168-20260512bc3`, `amzn-rescue-c145-168-20260512bc3`, `tsm-rescue-c145-168-20260512bc3`.
- Remaining expected `20260512bc3` research VMs after cleanup: none.

## Aggregate Result

- Aggregate portfolio report: `reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/aggregate/portfolio_report/research_portfolio_report.json`.
- Aggregate promotion packet: `reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/aggregate/promotion_packet/research_promotion_review_packet.json`.
- Packet decision: `ready_for_governed_validation_review`.
- Governance scope: `per_regime_governed_validation_review`.
- Required regimes in strict wave packet: `bear,choppy`.
- Eligible regimes: `bear`.
- Missing eligible regimes: `choppy`.
- Regime complete for promotion review: `false`.
- Candidate profiles tested: `576`.
- Candidate-level eligible profiles: `7`.
- Unique eligible base candidates: `6`.
- Blockers across the full population: `min_net_pnl_not_positive=540`, `test_net_pnl_not_above_0=472`, `fill_coverage_below_0.90=47`.

## Review-Ready Candidates

- `AVGO` `avgo__bear__put__debit_put_vertical`: bear, `debit_put_vertical`, full PnL `$157,736.76`, test PnL `$163,836.80`, fill `0.9730`, trades `144`, worst drawdown `-$6,748.91`.
- `GOOGL` `googl__bear__put__broken_wing_put_butterfly`: bear, `broken_wing_put_butterfly`, full PnL `$383,195.68`, test PnL `$1,080.09`, fill `1.0000`, trades `155`, worst drawdown `-$26,208.66`.
- `AVGO` `avgo__bear__call__bear_call_credit_spread`: bear, `bear_call_credit_spread`, full PnL `$501.78`, test PnL `$3,793.74`, fill `0.9588`, trades `163`, worst drawdown `-$5,380.67`.
- `MSFT` `msft__bear__call__bear_call_credit_spread`: bear, `bear_call_credit_spread`, full PnL `$1,868.10`, test PnL `$961.56`, fill `0.9868`, trades `75`, worst drawdown `-$831.99`.
- `QQQ` `qqq__bear__put__single_leg_repair`: bear, `single_leg_repair`, full PnL `$85.66`, test PnL `$574.12`, fill `1.0000`, trades `98`, worst drawdown `-$1,359.99`.
- `TSM` `tsm__bear__put__single_leg_repair`: bear, `single_leg_repair`, full PnL `$199.61`, test PnL `$51.25`, fill `0.9083`, trades `109`, worst drawdown `-$1,340.70`.

## Promotion And Paper Runner

- Eligible for governed validation review: yes, per-regime bear-only governed-validation review.
- Eligible for balanced bear/choppy review: no, choppy coverage is still missing.
- Added to paper runner: no.
- Paper-runner state changed: no.
- Recommendation: do not add the outsized AVGO/GOOGL multi-leg candidates to PAPER until a runtime compatibility and economics sanity audit confirms the spread construction, fill pricing, max loss, and paper-order semantics.

## Commands

```powershell
python scripts/build_research_portfolio_report.py `
  --replay-root reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/workers `
  --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/aggregate/portfolio_report `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --required-regimes bear,choppy `
  --candidate-identity-mode variant_profile
```

```powershell
python scripts/build_research_promotion_review_packet.py `
  --portfolio-report-json reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/aggregate/portfolio_report/research_portfolio_report.json `
  --output-dir reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/aggregate/promotion_packet `
  --max-review-candidates 20
```

```powershell
gcloud storage cp --recursive `
  reports/gcp_research/bear_choppy_non_single_refine_20260512T1325Z/aggregate `
  gs://codexalpaca-control-us/research_results/bear_choppy_non_single_refine_20260512T1325Z/aggregate
```

## Next Steps

- Run a multi-leg sanity audit on `AVGO` debit-put vertical and `GOOGL` broken-wing put butterfly before considering activation.
- Project the 6 unique bear candidates against the current paper risk model and compare against `tt_top2_bull_choppy_up`.
- Launch the next choppy-only refinement wave; this wave did not solve choppy coverage.
- Continue optimizer work with explicit concentration caps before pursuing the `$200/day` target.

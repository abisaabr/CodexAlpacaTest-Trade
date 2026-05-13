# QQQ/SPY Fill-Friendly Tournament Final Status - 2026-05-05

## Scope

Research-only QQQ/SPY 365-day option-aware tournament:

- GCS root: `gs://codexalpaca-control-us/research_results/qqq_spy_fillfriendly_tournament_20260505T2035Z/`
- Symbols: `QQQ`, `SPY`
- Selectors: `nearest_contract`, `entry_liquidity_first_research_only`
- Lag profiles: `0:60`, `10:60`, `30:120`
- Gate: `strategy_fill_coverage >= 0.90`
- Broker-facing effect: `none`
- Paper-order effect: `none`
- Live-manifest effect: `none`
- Risk-policy effect: `none`

Both worker VMs completed and uploaded the expected six profile summaries per symbol.

## Strict Final Aggregate

Generated locally from the final GCS mirror, then intended to mirror back to GCS:

- Portfolio report: `reports/gcp_research/qqq_spy_fillfriendly_tournament_20260505T2035Z/strict_final_portfolio_report/research_portfolio_report.json`
- Promotion packet: `reports/gcp_research/qqq_spy_fillfriendly_tournament_20260505T2035Z/strict_final_promotion_packet/research_promotion_review_packet.json`

Strict aggregate result:

- Candidate-profile rows: `1512`
- Eligible for governed promotion review: `51`
- Eligible regimes: `bull`
- Missing eligible regimes: `bear`, `choppy`
- Promotion packet decision: `research_only_blocked_regime_incomplete`

The result is correctly blocked for regime-complete portfolio promotion. It identifies bullish single-leg repair candidates for QQQ/SPY, but it does not yet satisfy the bull/bear/choppy portfolio objective.

## Symbol-Regime Status

| Symbol | Regime | Candidates | Eligible | Main blockers |
| --- | ---: | ---: | ---: | --- |
| QQQ | bull | 486 | 15 | Some weak variants fail fill/economics, but strong candidates pass. |
| QQQ | bear | 48 | 0 | `test_net_pnl_not_above_0`, `min_net_pnl_not_positive`, many fill gaps. |
| QQQ | choppy | 192 | 0 | `test_net_pnl_not_above_0`, `min_net_pnl_not_positive`, many fill gaps. |
| SPY | bull | 486 | 36 | Strong candidates pass. |
| SPY | bear | 48 | 0 | `test_net_pnl_not_above_0`, `min_net_pnl_not_positive`, insufficient viable family coverage. |
| SPY | choppy | 192 | 0 | `test_net_pnl_not_above_0`, `min_net_pnl_not_positive`, many fill gaps. |

Aggregate blocker counts:

- `fill_coverage_below_0.90`: `386`
- `min_net_pnl_not_positive`: `1427`
- `option_trades_below_20`: `288`
- `test_net_pnl_not_above_0`: `880`

Fill failure counts:

- `fill_gate_clear`: `1126`
- `selected_contract_universe_gap`: `346`
- `exit_bar_gap_or_exit_policy_mismatch`: `30`
- `entry_bar_gap_or_entry_timing_mismatch`: `8`
- `mixed_low_fill_gap`: `2`

## Code Improvements Made

The following research-control upgrades were added after the wave launched, so they apply to future reruns:

- `scripts/run_option_aware_research_backtest.py` now supports opt-in `--candidate-selection-mode regime_balanced`, preserving default priority-order behavior while allowing bull/bear/choppy round-robin candidate windows.
- `scripts/gcp_single_ticker_365d_shard.sh` now passes candidate-selection metadata through to the backtester and worker status JSON, defaulting to priority-order behavior for compatibility.
- `scripts/build_portfolio_overnight_research_inputs.py` now writes normalized `directional_option_type`, `intended_regime`, and `family` metadata into generated variants and their parameter payloads, not only queue rows.
- `scripts/build_research_portfolio_report.py` now emits `symbol_regime_summary`, making per-ticker bull/bear/choppy coverage gaps first-class in JSON and Markdown reports.

Validation:

- `python -m py_compile scripts/run_option_aware_research_backtest.py`
- `python -m py_compile scripts/build_portfolio_overnight_research_inputs.py scripts/build_research_portfolio_report.py`
- `bash -n scripts/gcp_single_ticker_365d_shard.sh`
- `uv run --with pytest python -m pytest tests/test_build_portfolio_overnight_research_inputs.py tests/test_build_research_portfolio_report.py tests/test_run_option_aware_research_backtest.py -q`
- Result: `25 passed`

## Next Research Step

Do not promote or activate this wave as a regime-complete portfolio. The next bounded wave should be a bear/choppy rescue wave over QQQ/SPY using the same 365-day data foundation:

- Bear: add/test single-leg long-put repair, same-day/next-expiry ATM puts, tighter stop/target profiles, and put debit spreads only after long-put baselines are measured.
- Choppy: add/test long straddle, long strangle, narrow iron butterfly, and premium-target-stop variants; do not rely only on broken-wing butterfly or premium-defense templates.
- Run QQQ/SPY split by symbol and regime on separate GCP workers.
- Use `--candidate-selection-mode regime_balanced` for any mixed-regime queue.
- Aggregate with `--candidate-identity-mode variant_profile`.
- Keep `strategy_fill_coverage >= 0.90`, `min_option_trades >= 20`, `min_test_net_pnl > 0`, and `min_net_pnl > 0`.

Promotion remains governed-validation-review only. This status does not authorize paper/live activation.

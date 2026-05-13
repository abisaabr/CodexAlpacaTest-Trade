# QQQ 30-Session Backtester Optimization Pass

- Generated: 2026-05-05
- Scope: offline research only
- Broker-facing effect: none
- Paper-runner effect: none
- Live manifest effect: none
- Risk policy effect: none

## Local QQQ Example

The cleanroom QQQ runner was updated locally to support bounded date windows via `--max-days`, `--start-date`, and `--end-date`.

Input panel:

- `C:\Users\abisa\Downloads\qqq_options_30d_cleanroom\output_live_retest_20260430\qqq_365d_option_1min_wide_backtest.parquet`
- `C:\Users\abisa\Downloads\qqq_options_30d_cleanroom\output_live_retest_20260430\qqq_365d_option_daily_universe.parquet`

Replay output:

- `C:\Users\abisa\Downloads\qqq_options_30d_cleanroom\output\codex_qqq30_opt_20260505_v1\qqq30_v1_strategy_summary.csv`
- `C:\Users\abisa\Downloads\qqq_options_30d_cleanroom\output\codex_qqq30_opt_20260505_v1\qqq30_v1_strategy_trades.csv`
- `C:\Users\abisa\Downloads\qqq_options_30d_cleanroom\output\codex_qqq30_opt_20260505_v1\qqq30_v1_report.md`
- `C:\Users\abisa\Downloads\qqq_options_30d_cleanroom\output\codex_qqq30_opt_20260505_v1\qqq30_v1_assumptions.json`

Window tested:

- Complete sessions: 30
- First session: 2025-05-02
- Last session: 2025-06-13
- Available complete sessions in panel: 39

Top local results:

| Rank | Strategy | Family | Trades | Final Equity | Return |
|---:|---|---|---:|---:|---:|
| 1 | `orb_long_call_same_day` | Single-leg long call | 21 | 26031.58 | 4.13% |
| 2 | `trend_long_call_next_expiry` | Single-leg long call | 14 | 25916.35 | 3.67% |
| 3 | `iron_condor_same_day` | Iron condor | 18 | 24834.76 | -0.66% |
| 4 | `bull_put_credit_spread_same_day` | Credit put spread | 17 | 24824.60 | -0.70% |
| 5 | `long_straddle_same_day` | Long straddle | 6 | 24106.35 | -3.57% |

Interpretation:

- The 30-session local example supports bull single-leg call structures.
- Bear and choppy structures were not promotion-ready in this local cleanroom pass.
- The result argues for expanding liquidity-first ATM and one-step variants before broad cloud reruns.

## Versioned Optimizations

The QQQ option-native template registry now includes liquidity-first variants designed to improve fill quality:

- Same-day ATM long call.
- One-step call debit spread.
- Same-day ATM long put.
- One-step put debit spread.
- One-step iron butterfly.
- Same-day ATM long straddle.

The portfolio overnight input builder now prefers explicit metadata before token inference for:

- `directional_option_type`
- `intended_regime`
- `family`

This reduces backtester/promoter mismatch risk when strategy names contain misleading tokens.

Updated template counts:

- Bull: 5
- Bear: 7
- Choppy: 6
- Total: 18

## Active Cloud State

The QQQ/SPY fill-friendly tournament remains research-only and running:

- Wave: `qqq_spy_fillfriendly_tournament_20260505T2035Z`
- QQQ worker: `qqq-spy-ff-qqq-20260505a`, `us-central1-a`, `RUNNING`
- SPY worker: `qqq-spy-ff-spy-20260505a`, `us-east1-b`, `RUNNING`
- GCS root: `gs://codexalpaca-control-us/research_results/qqq_spy_fillfriendly_tournament_20260505T2035Z/`

## Verification

Targeted tests passed:

```powershell
uv run --with pytest python -m pytest tests/test_qqq_option_template_registry.py tests/test_build_portfolio_overnight_research_inputs.py tests/test_build_research_portfolio_report.py tests/test_run_qqq_option_native_tournament.py -q
```

Result: 12 passed.

## Next Step

Do not launch a new broad wave until the active QQQ/SPY fill-friendly tournament finishes. After completion, aggregate the GCS outputs into a strict portfolio report and promotion-review packet. If QQQ bull strategies clear but bear/choppy remain weak, run a bounded QQQ-only liquidity-first rerun using the expanded 18-template registry before expanding to other symbols.

# Portfolio Overnight 12h Scope Coverage - 2026-05-01

Status updated UTC: `2026-05-01T16:09:29Z`

## Answer

The research wave was configured for a 20-ticker option-aware strategy tournament, but the final pre-RTH promotion packet is not a complete 20-ticker result. It is a cutoff snapshot from the worker outputs that existed when the final aggregator ran.

Configured 20-ticker universe:

- Top10 ladder: `AAPL AMD AMZN INTC IWM META MSFT NVDA SPY TSLA`
- Next10 ladder: `AVGO GOOGL MU NFLX ORCL PLTR QQQ TSM XLE XOM`
- QQQ also had a dedicated dense 365-day bull/bear/choppy deep-grid lane.

## Completed Evidence At Final Aggregate

Canonical final aggregate:

`gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/aggregate/`

Final packet summary:

- Candidate count: `364`
- Profile-level eligible candidates: `8`
- Unique eligible base candidates: `4`
- Promotion decision: `ready_for_governed_validation_review`
- Broker facing: `false`
- Live manifest effect: `none`
- Risk policy effect: `none`
- Fill coverage gate: `0.90`

Final eligible base candidates are all QQQ:

- QQQ bull long call, fixed offset
- QQQ bull long call, first common within cutoff
- QQQ bull call debit spread, fixed offset
- QQQ choppy iron condor, first common within cutoff

No QQQ bear candidate passed the final overnight aggregate packet.

## Worker Coverage

Strategy summary outputs observed so far include:

- `AAPL`
- `AVGO`
- `GOOGL`
- `META`
- `MSFT`
- `PLTR`
- `QQQ` specialized lanes

Missing or not yet summary-complete in the final aggregate:

- `AMD`
- `AMZN`
- `INTC`
- `IWM`
- `MU`
- `NFLX`
- `NVDA`
- `ORCL`
- `SPY`
- `TSLA`
- `TSM`
- `XLE`
- `XOM`

The still-running fastlane workers show serial-log activity after the final aggregate cutoff, so the all-ticker backtest is still in progress rather than finished. The correct next research step is to let the active workers continue and run a post-completion aggregate refresh after more summary files land.

## Paper Readiness

The QQQ governed shadow-validation no-order runner path passed startup preflight and a single no-order run:

- Startup preflight: `startup_preflight_passed`
- Run-once status: `ran_once`
- Submit paper orders: `false`
- Open trades: `0`
- Completed trades: `0`
- Startup check status: `passed`

This validates the no-order runner mechanics only. It does not authorize paper order submission.

## Current Recommendation

Use QQQ only for the next controlled no-order shadow validation lane because QQQ is the only final-packet eligible symbol. Do not launch a broad 20-ticker paper portfolio from the current final aggregate.

For the full 20-ticker strategy lab, continue the active GCP workers and then build a post-completion aggregate packet. Only tickers that pass the same gates should be considered for governed validation review:

- `fill_coverage >= 0.90`
- `min_option_trades >= 20`
- `min_test_net_pnl > 0`
- `min_net_pnl > 0`
- broker-facing remains `false` until separate operator approval

## Hard Rules

- No trading was started.
- No paper orders were submitted.
- No live manifest was changed.
- No risk policy was changed.
- The fill coverage gate was not lowered.

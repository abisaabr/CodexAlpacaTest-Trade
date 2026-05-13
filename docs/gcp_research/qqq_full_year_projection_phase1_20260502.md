# QQQ Full-Year Projection Phase 1 - 2026-05-02

## Purpose

Phase 1 fixes the portfolio-growth projection denominator. The prior QQQ 365d projection compounded only on `78` strategy-active days. This phase rebuilds the projection over the tradable option-bar calendar and carries cash through inactive days.

## Implementation

- Added `scripts/build_projection_calendar.py`.
- Extended `scripts/build_portfolio_growth_projection.py` with `--calendar-csv`.
- Updated `scripts/gcp_qqq_365d_canonical_backtest.sh` so future QQQ canonical runs build a projection calendar from option-bar `trade_date=` partitions and join QQQ regime labels onto that calendar.
- Added focused tests for calendar construction and cash-carry projection behavior.

## Corrected QQQ Full-Year Result

- Dataset calendar: `251` tradable QQQ option-bar days.
- Calendar span: `2025-04-29` through `2026-04-28`.
- Strategy-active days: `78`.
- Inactive cash days: `173`.
- Active-day coverage: `31.0757%`.
- Full-year ending equity: `$35,845.88`.
- Full-year total return: `43.3835%`.
- Full-year CAGR estimate: `43.5895%`.
- Active-day-only CAGR estimate retained for comparison: `220.3412%`.
- Evidence grade: `not_institutional_expectation`.

## Regime Coverage

Calendar market-regime counts:

- `bull`: `84` days
- `bear`: `17` days
- `choppy`: `59` days
- `mixed`: `71` days
- `warmup_unclassified`: `20` days

Current promoted-capital-plan coverage:

- `bull`: covered, `78` active days, `156` scaled trades
- `bear`: not covered
- `choppy`: not covered

Blockers:

- `missing_bear_strategy_coverage`
- `missing_choppy_strategy_coverage`

Warnings:

- `portfolio_has_inactive_cash_days`
- `target_hit_probability_below_50pct`
- `bootstrap_probability_of_50pct_drawdown_above_5pct`

## GCS Artifacts

- Full-year projection JSON: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/full_year_projection/qqq_365d_full_year_growth_projection/portfolio_growth_projection.json`
- Full-year projection Markdown: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/full_year_projection/qqq_365d_full_year_growth_projection/portfolio_growth_projection.md`
- Full-year equity curve: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/full_year_projection/qqq_365d_full_year_growth_projection/portfolio_growth_equity_curve.csv`
- Active-day comparison curve: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/full_year_projection/qqq_365d_full_year_growth_projection/portfolio_growth_active_day_equity_curve.csv`
- Projection calendar: `gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z/full_year_projection/projection_calendar/projection_calendar/projection_calendar.csv`

## Interpretation

The QQQ result remains promising, but it is not paper-ready as a full-year institutional portfolio. The current promoted capital plan is bull-only, so the next phase should expand or repair QQQ bear and choppy candidates, then rerun portfolio-level promotion and the full-year projection.

Hard rules remain unchanged: no trading, no paper orders, no live-manifest edits, no risk-policy edits, and no lowering `fill_coverage >= 0.90`.

# Multi-Ticker Paper Portfolio

## Deployment Book

This runner now trades the validated shared-account book across:

- `QQQ`
- `SPY`
- `IWM`
- `NVDA`
- `TSLA`
- `MSFT`
- `BAC`
- `PLTR`
- `GLD`
- `ARKK`
- `XLE`

The live book uses a shared virtual `$25,000` sleeve and `38` strategy entries, including one XLE choppy alias that intentionally reuses the same opening-range call setup under a separate regime label.

### Promoted Strategies

#### QQQ
- Bull:
  `qqq__fast__trend_long_call_next_expiry`
  `qqq__slow__trend_long_call_next_expiry`
- Bear:
  `qqq__fast__trend_long_put_next_expiry`
  `qqq__slow__orb_long_put_same_day`

#### SPY
- Bull:
  `spy__fast__trend_long_call_next_expiry`
- Bear:
  `spy__base__trend_long_put_next_expiry`
  `spy__fast__trend_long_put_next_expiry`

#### IWM
- Bull:
  `iwm__fast__trend_long_call_next_expiry`
  `iwm__slow__trend_long_call_next_expiry`
- Bear:
  `iwm__fast__trend_long_put_next_expiry`

#### NVDA
- Bull:
  `nvda__fast__trend_long_call_next_expiry`
- Bear:
  `nvda__base__trend_long_put_next_expiry`

#### TSLA
- Bull:
  `tsla__base__trend_long_call_next_expiry`
- Bear:
  `tsla__base__trend_long_put_next_expiry`
  `tsla__fast__trend_long_put_next_expiry`

#### MSFT
- Bull:
  `msft__fast__trend_long_call_next_expiry`
  `msft__base__trend_long_call_next_expiry`
  `msft__slow__trend_long_call_next_expiry`
- Bear:
  `msft__base__trend_long_put_next_expiry`
  `msft__slow__trend_long_put_next_expiry`

#### BAC
- Bull:
  `bac__fast__trend_long_call_next_expiry`
- Bear:
  `bac__fast__trend_long_put_next_expiry`

#### PLTR
- Bull:
  `pltr__fast__trend_long_call_next_expiry`
  `pltr__base__trend_long_call_next_expiry`
- Bear:
  `pltr__fast__trend_long_put_next_expiry`
  `pltr__base__trend_long_put_next_expiry`

#### GLD
- Bull:
  `gld__base__trend_long_call_next_expiry`
  `gld__slow__trend_long_call_next_expiry`
- Bear:
  `gld__base__trend_long_put_next_expiry`

#### ARKK
- Bull:
  `arkk__fast__trend_long_call_next_expiry`
  `arkk__slow__trend_long_call_next_expiry`
  `arkk__fast__orb_long_call_same_day`
- Bear:
  `arkk__fast__trend_long_put_next_expiry`

#### XLE
- Bull:
  `xle__slow__orb_long_call_same_day`
  `xle__base__orb_long_call_same_day`
  `xle__base__trend_long_call_next_expiry`
- Bear:
  `xle__fast__trend_long_put_next_expiry`
- Choppy:
  `xle__base__orb_long_call_same_day__choppy`

## Research Result

The current deployment book comes from a first expansion pass where the existing six-ticker live book was held fixed, six new candidates were fully researched (`AMD`, `PLTR`, `BAC`, `GLD`, `XLE`, `ARKK`), and the shared-account selector then greedily added only the names that improved the live portfolio score. A final strategy-prune pass removed sleeves that hurt the shared book.

All numbers below are on the common `120`-session out-of-sample window shared by the core book and the expansion basket:

- Current live deployment book:
  `$305,039.74`
  `+1120.16%`
  `913` trades
  `62.87%` win rate
  `-11.26%` max drawdown
- Current six-ticker core on the same OOS window:
  `$231,822.58`
  `+827.29%`
  `618` trades
  `64.40%` win rate
  `-11.26%` max drawdown
- Full screened union without greedy selection and pruning:
  `$296,700.37`
  `+1086.80%`
  `1011` trades
  `61.82%` win rate
  `-13.93%` max drawdown

The promoted additions were `BAC`, `PLTR`, `GLD`, `ARKK`, and `XLE`. Against the real live baseline, that lifted return by `292.87` percentage points with essentially unchanged max drawdown on the shared OOS window.

## Live Safety

The runner starts with a morning self-check and refuses to trade if:

- Alpaca buying power is below the configured minimum
- unexpected broker positions are already open at session start
- stock bars are stale after the startup grace period
- same-day or next-expiry option inventory is missing for any symbol

It also sends Discord webhook check-ins for:

- successful morning start
- midday status
- end-of-day status

The Discord webhook is loaded from `DISCORD_WEBHOOK_URL` in your local `.env`. It is intentionally not committed to GitHub.

The live overlay now uses the validated shared-account settings for the expanded book:

- `max_open_risk_fraction: 15%`
- `daily_loss_gate_pct: disabled`
- `delever_drawdown_pct: 8%`
- `delever_risk_scale: 50%`
- `max_open_positions: 10`
- `max_positions_per_regime: 10`
- `max_positions_per_symbol: 3`
- soft alerts:
  `delta ~= 3100 shares`
  `vega ~= 720 dollars per 1 vol point`

Two findings drove those settings:

- the old `2%` daily loss gate clipped rebound days and reduced return while worsening drawdown
- the expanded research regularly used up to `10` same-regime positions intraday, so the old regime cap of `6` would have undertraded the validated book

## Config

- Portfolio config:
  `config/multi_ticker_paper_portfolio.yaml`
- Runner:
  `scripts/run_multi_ticker_portfolio_paper_trader.py`
- Windows wrapper:
  `scripts/run_multi_ticker_portfolio_session.ps1`
- Windows installer:
  `scripts/install_multi_ticker_paper_task.ps1`
- State and reports:
  `reports/multi_ticker_portfolio/`

## Local Commands

Diagnostic one-shot:

```powershell
python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_ticker_paper_portfolio.yaml --run-once
```

Full paper session:

```powershell
python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\multi_ticker_paper_portfolio.yaml --submit-paper-orders
```

Install the weekday task:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_multi_ticker_paper_task.ps1 -TaskName "Multi-Ticker Portfolio Paper Trader" -StartTime "09:20"
```

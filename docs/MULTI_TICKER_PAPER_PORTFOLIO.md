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
- `GDX`
- `SLV`

The live book uses a shared virtual `$25,000` sleeve and `48` strategy entries, including one XLE choppy alias that intentionally reuses the same opening-range call setup under a separate regime label.

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

#### GDX
- Bull:
  `gdx__fast__trend_long_call_next_expiry`
  `gdx__base__trend_long_call_next_expiry`
- Bear:
  `gdx__slow__trend_long_put_next_expiry`
  `gdx__base__trend_long_put_next_expiry`
  `gdx__fast__trend_long_put_next_expiry`

#### SLV
- Bull:
  `slv__base__trend_long_call_next_expiry`
  `slv__fast__trend_long_call_next_expiry`
  `slv__slow__trend_long_call_next_expiry`
- Bear:
  `slv__fast__trend_long_put_next_expiry`
  `slv__base__trend_long_put_next_expiry`

## Research Result

The current deployment book now comes from two promotion rounds:

- Phase one held the original six-ticker live book fixed, fully researched `AMD`, `PLTR`, `BAC`, `GLD`, `XLE`, and `ARKK`, then greedily added only the names that improved the shared-account score.
- Phase two used the cached cleanroom datasets that were already on disk, re-tested `C`, `GDX`, `TLT`, `SLV`, and `PFE` against the real live 11-ticker book, and then promoted only the clean additions that both improved the shared account and still looked credible standalone.

Phase-one numbers on the common `120`-session out-of-sample window shared by the original expansion basket:

- 11-ticker deployment book after phase one:
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

The promoted additions from phase one were `BAC`, `PLTR`, `GLD`, `ARKK`, and `XLE`.

Phase-two cached-candidate validation used a stricter common `105`-session out-of-sample window shared by the live book plus `C`, `GDX`, `TLT`, `SLV`, and `PFE`:

- 11-ticker live baseline on that same common window:
  `$250,905.31`
  `+903.62%`
  `897` trades
  `60.98%` win rate
  `-13.50%` max drawdown
- Best clean promotion set:
  `GDX + SLV`
  `$259,147.53`
  `+936.59%`
  `1040` trades
  `61.35%` win rate
  `-12.00%` max drawdown

`TLT + SLV + PFE` narrowly won the full candidate sweep on a pure risk-adjusted score, but `PFE` was negative standalone and `TLT` was only marginally positive standalone, so the live promotion stayed conservative and promoted only `GDX` and `SLV`.

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
  `delta ~= 3150 shares`
  `vega ~= 655 dollars per 1 vol point`

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

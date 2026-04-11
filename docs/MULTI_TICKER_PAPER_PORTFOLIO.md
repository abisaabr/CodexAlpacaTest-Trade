# Multi-Ticker Paper Portfolio

## Deployment Book

This runner trades the refined shared-account winners from the 365-day cleanroom tournament across:

- `QQQ`
- `SPY`
- `IWM`
- `NVDA`
- `TSLA`
- `MSFT`

The promoted live book uses `21` validated single-leg sleeves and a shared virtual `$25,000` account.

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
  `iwm__base__trend_long_put_next_expiry`

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

## Research Result

The final promoted `21`-strategy book was chosen because it improved risk-adjusted performance versus the raw `29`-strategy promoted set.

- Refined shared-account book:
  `$232,091.95`
  `+828.37%`
  `625` trades
  `64.00%` win rate
  `-12.45%` max drawdown
- Raw `29`-strategy promoted set:
  `$244,082.22`
  `+876.33%`
  `751` trades
  `61.65%` win rate
  `-17.99%` max drawdown
- QQQ-only promoted baseline:
  `$62,218.86`
  `+148.88%`
  `181` trades
  `58.01%` win rate
  `-15.21%` max drawdown

The multi-ticker refined book beat the QQQ-only baseline by `679.49` percentage points while also reducing drawdown.

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

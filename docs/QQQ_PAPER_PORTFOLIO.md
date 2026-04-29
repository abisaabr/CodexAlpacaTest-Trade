# QQQ Paper Portfolio

## Strategy Book

This runner trades a causal intraday QQQ options book built from the validated research sleeves:

- Bull:
  `trend_long_call_next_expiry`
  `bull_call_spread_next_expiry`
  `orb_long_call_same_day`
- Bear:
  `trend_long_put_next_expiry`
  `bear_put_spread_next_expiry`
  `orb_long_put_same_day`
- Choppy:
  `iron_condor_same_day`

Every trade is opened and closed inside the same RTH session. The runner never intentionally carries positions overnight.

## Risk Model

- Virtual sleeve start: `$25,000`
- Max open risk: `15%` of current sleeve equity
- Daily new-entry stop: block new entries once sleeve equity is down `2%` from the day start
- Delever rule: when sleeve drawdown from its high-water mark reaches `12%`, scale strategy risk to `75%`
- Soft Greek alerts:
  delta `838.78` shares equivalent
  vega `171.42` dollars per 1 vol point
- Position caps:
  max `3` open positions overall
  max `2` open positions per regime

## How It Trades

1. Fetch 1-minute QQQ bars and compute intraday VWAP, fast EMA, and slow EMA.
2. Infer a causal intraday regime from the current session state.
3. Pull same-day and next-expiry QQQ options around the current spot.
4. Compute IV and Greeks from live quotes when Alpaca snapshots do not provide them directly.
5. Select legs by target delta.
6. Submit either:
   simple option orders for the single-leg sleeves
   Alpaca `mleg` orders for spreads and the condor
7. Monitor open trades every loop for:
   profit target
   stop loss
   hard time exit
   forced end-of-day flatten

## Files

- Runner: `scripts/run_qqq_portfolio_paper_trader.py`
- Windows wrapper: `scripts/run_qqq_portfolio_session.ps1`
- Windows scheduler installer: `scripts/install_qqq_paper_task.ps1`
- Portfolio config: `config/qqq_paper_portfolio.yaml`
- State and reports: `reports/qqq_portfolio/`

## Local Commands

Diagnostic one-shot:

```powershell
python scripts/run_qqq_portfolio_paper_trader.py --portfolio-config config\qqq_paper_portfolio.yaml --run-once
```

Full paper session:

```powershell
python scripts/run_qqq_portfolio_paper_trader.py --portfolio-config config\qqq_paper_portfolio.yaml --submit-paper-orders
```

Install the weekday task:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_qqq_paper_task.ps1 -TaskName "QQQ Portfolio Paper Trader" -StartTime "09:20"
```

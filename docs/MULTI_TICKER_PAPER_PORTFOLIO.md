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
- `AMZN`
- `JPM`
- `XOM`
- `ORCL`
- `SHOP`
- `CRM`

The live book uses a shared virtual `$25,000` sleeve and `78` strategy entries, including two explicit choppy aliases that intentionally reuse validated same-day single-leg setups under separate regime labels.

## Notifications

The paper trader can publish its morning startup check, midday status, and end-of-day summary through any combination of:

- `NTFY_TOPIC` via `https://ntfy.sh`
- `DISCORD_WEBHOOK_URL`
- SMTP email settings

`ntfy` is the simplest path to get mobile push notifications quickly. Set `NTFY_TOPIC` in the local `.env`, then subscribe to that topic in the ntfy mobile app or by opening `https://ntfy.sh/<your-topic>` in a browser.

The notification payloads now include higher-signal operating context, including open positions grouped by ticker plus realized day PnL summaries for the strongest and weakest strategies so far.

## Portable Runtime

For the easiest cross-machine deployment, the repo now includes:

- `Dockerfile`
- `docker-compose.yml`
- `scripts/run_multi_ticker_portable_daemon.py`
- `scripts/run_multi_ticker_watchdog.py`
- `scripts/setup_new_machine.ps1`
- `scripts/setup_new_machine.sh`

The recommended portable runtime is:

```bash
docker compose up -d portfolio-trader portfolio-watchdog
```

That keeps the strategy book, state, and alerts identical across machines while avoiding OS-specific schedulers.

## Health Check

An hourly local health-check runner is available at `scripts/run_multi_ticker_health_check.py`. It verifies the main scheduled task, checks whether the paper trader is running and updating its session during market hours, and sends ntfy alerts when something is wrong. Safe operational fixes such as reinstalling the main scheduled task or restarting a missing trader process can be enabled with `--restart-if-needed`.

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

#### AMZN
- Bull:
  `amzn__slow__trend_long_call_next_expiry`
  `amzn__fast__trend_long_call_next_expiry`
  `amzn__base__trend_long_call_next_expiry`
- Bear:
  `amzn__slow__trend_long_put_next_expiry`
  `amzn__base__trend_long_put_next_expiry`
  `amzn__fast__trend_long_put_next_expiry`
- Choppy:
  `amzn__slow__orb_long_put_same_day`

#### JPM
- Bull:
  `jpm__base__trend_long_call_next_expiry`
  `jpm__slow__trend_long_call_next_expiry`
  `jpm__fast__trend_long_call_next_expiry`
- Bear:
  `jpm__fast__trend_long_put_next_expiry`
  `jpm__base__trend_long_put_next_expiry`
  `jpm__slow__trend_long_put_next_expiry`

#### XOM
- Bull:
  `xom__slow__trend_long_call_next_expiry`
  `xom__base__trend_long_call_next_expiry`
  `xom__fast__trend_long_call_next_expiry`

#### ORCL
- Bull:
  `orcl__slow__trend_long_call_next_expiry`
  `orcl__fast__trend_long_call_next_expiry`
- Bear:
  `orcl__slow__trend_long_put_next_expiry`
  `orcl__fast__trend_long_put_next_expiry`

#### SHOP
- Bull:
  `shop__slow__trend_long_call_next_expiry`
  `shop__fast__trend_long_call_next_expiry`
  `shop__base__trend_long_call_next_expiry`
- Bear:
  `shop__fast__trend_long_put_next_expiry`
  `shop__slow__trend_long_put_next_expiry`

#### CRM
- Bull:
  `crm__fast__trend_long_call_next_expiry`
- Bear:
  `crm__fast__trend_long_put_next_expiry`
  `crm__slow__trend_long_put_next_expiry`
  `crm__base__trend_long_put_next_expiry`
- Choppy:
  `crm__base__orb_long_put_same_day`

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

Phase-three fresh-download validation then tested the newly downloaded `AAPL`, `AMZN`, `META`, `AVGO`, `GOOGL`, `JPM`, `XLV`, and `XLI` batch. The first finished names were enough to make an immediate promotion decision:

- Current 13-ticker live baseline on the common `110`-session overlap used for the first batch comparison:
  `$254,011.16`
  `+916.04%`
  `1085` trades
  `60.18%` win rate
  `-16.35%` max drawdown
- Best early add from that batch:
  `AMZN`
  `$287,890.83`
  `+1051.56%`
  `1131` trades
  `61.89%` win rate
  `-12.78%` max drawdown

`AAPL` improved return too, but it was clearly weaker than `AMZN` on the same overlap slice and carried worse standalone drawdown, so it stayed out of the live book for now. `XLV` and `XLI` were both too weak standalone to justify promotion.

Once `AVGO` and `JPM` finished, the shared-account gate produced a second clean result on the same `110`-session overlap:

- Current 14-ticker live baseline after adding `AMZN`:
  `$287,890.83`
  `+1051.56%`
  `1131` trades
  `61.89%` win rate
  `-12.78%` max drawdown
- Best next add:
  `JPM`
  `$300,142.68`
  `+1100.57%`
  `1157` trades
  `62.06%` win rate
  `-12.63%` max drawdown

`AVGO` was positive standalone, but it weakened the shared account when added alone. `AVGO + JPM` produced slightly higher ending equity than `JPM` alone, but it also worsened drawdown and risk-adjusted score, so the live promotion stayed disciplined and added only `JPM`.

The next completed cleanroom wave then compared `UNH`, `LLY`, `WMT`, `BA`, `CVX`, `DIA`, `SMH`, `NFLX`, `XOM`, and `UBER` against the 15-ticker live book on candidate-specific overlap windows and then re-checked the strongest names on shared windows:

- Current 15-ticker live baseline on the common `110`-session overlap shared with the strongest additions:
  `$300,142.68`
  `+1100.57%`
  `1157` trades
  `62.06%` win rate
  `-12.63%` max drawdown
- Best next add:
  `XOM`
  `$307,369.68`
  `+1129.48%`
  `1184` trades
  `63.18%` win rate
  `-12.37%` max drawdown

`UBER` was mildly additive, but `XOM` was stronger on both ending equity and risk-adjusted score on the same overlap. `BA` improved return but worsened drawdown, while `DIA` only looked strong on a much shorter `65`-session overlap and stayed in research instead of being promoted.

The next partial wave from the newest cleanroom batch completed `ORCL`, `ADBE`, `CRM`, `PANW`, and `SHOP` before lower-coverage names halted the batch. Those finished names were then re-tested against the current 16-ticker live book that already included `XOM`:

- Current 16-ticker live baseline on the common `101`-session overlap shared by the best new combo:
  `$286,760.69`
  `+1047.04%`
  `-13.13%` max drawdown
- Best promoted combo:
  `ORCL + SHOP + CRM`
  `$325,774.50`
  `+1203.10%`
  `-11.80%` max drawdown

Each of those three also improved the shared account individually on its own overlap window:

- `ORCL` was the strongest single add:
  on `110` shared sessions it improved the live book from `$307,369.68` to `$324,856.82` while slightly reducing drawdown from `-12.37%` to `-12.11%`
- `SHOP` improved the live book on `104` shared sessions from `$296,728.38` to `$299,324.39` and reduced drawdown from `-12.69%` to `-11.63%`
- `CRM` improved the live book on `104` shared sessions from `$287,887.92` to `$293,208.70` and reduced drawdown from `-12.88%` to `-12.59%`

`ADBE` and `PANW` both finished their standalone tournaments, but neither improved the shared account, so they stayed out of the live deployment book.

## Live Safety

The runner starts with a morning self-check and refuses to trade if:

- Alpaca buying power is below the configured minimum
- unexpected broker positions are already open at session start
- stock bars are stale after the startup grace period
- same-day or next-expiry option inventory is missing for any symbol

By default, the runner now tries to auto-clean unexpected paper positions before failing startup, and it performs an end-of-day broker reconciliation sweep. Known leftover trades are force-closed with `auto_flatten_known_end_of_day_position`, and truly orphaned broker positions are closed and journaled with an `auto_flatten_unexpected_*` reason in `reports/multi_ticker_portfolio/runs/<trade-date>/broker_position_cleanup.json`.

At the end of the day, the runner now also writes a dedicated guardrail scorecard bundle:

- `multi_ticker_portfolio_guardrail_scorecard.json`
- `multi_ticker_portfolio_guardrail_scorecard.md`
- `multi_ticker_portfolio_guardrail_scorecard_guardrail_firings.csv`
- `multi_ticker_portfolio_guardrail_scorecard_guardrail_reason_counts.csv`
- `multi_ticker_portfolio_guardrail_scorecard_guardrail_recommendations.csv`

That scorecard explains which guardrails fired, why they fired, whether the issue was already auto-fixed by the runner, and which items still need manual review. This is the “keep learning” layer for the live paper trader: it creates a compact daily feedback loop without silently changing trading logic on its own.

It also sends outbound notifications for:

- successful morning start
- midday status
- end-of-day status

Supported channels:

- Discord webhook via `DISCORD_WEBHOOK_URL`
- SMTP email via local `.env` settings such as `EMAIL_SMTP_HOST`, `EMAIL_USERNAME`, `EMAIL_PASSWORD`, `EMAIL_FROM`, and `EMAIL_TO`

For Gmail, use:

- `EMAIL_SMTP_HOST=smtp.gmail.com`
- `EMAIL_SMTP_PORT=587`
- `EMAIL_USE_STARTTLS=true`
- `EMAIL_USERNAME=<your gmail address>`
- `EMAIL_PASSWORD=<your 16-character Gmail app password>`
- `EMAIL_FROM=<your gmail address>`
- `EMAIL_TO=<recipient list>`

Secrets stay in your local `.env` and are intentionally not committed to GitHub.

The live overlay now uses the validated shared-account settings for the expanded book:

- dedicated risk controls file:
  `config/risk_controls/multi_ticker_portfolio.yaml`
- `max_open_risk_fraction: 15%`
- `max_open_risk_fraction_per_symbol: 5%`
- bucket caps:
  `index_beta: 8%`
  `growth_tech: 9%`
  `metals_energy: 8%`
  `financials: 6%`
- `daily_loss_gate_pct: disabled`
- `delever_drawdown_pct: 8%`
- `delever_risk_scale: 50%`
- `severe_loss_halt_new_entries_pct: 3.5%`
- `severe_loss_flatten_all_pct: 5%`
- `max_open_positions: 10`
- `max_positions_per_regime: 10`
- `max_positions_per_symbol: 3`
- `broker_min_equity_to_trade: $26,000`
- `broker_equity_emergency_stop: $25,500`
- soft alerts:
  `delta ~= 3200 shares`
  `vega ~= 620 dollars per 1 vol point`
- hard projected entry caps:
  `delta ~= 4000 shares`
  `vega ~= 750 dollars per 1 vol point`
- execution circuit breaker:
  `3` consecutive entry failures
  or `20%` average adverse entry slippage over the last `4` filled entries
- late-day entry cutoffs:
  `same_day ~= minute 300`
  `all other entries ~= minute 345`
- manual event blackout calendar:
  `risk.event_blackouts` in `config/risk_controls/multi_ticker_portfolio.yaml`

The virtual research sleeve is still `$25,000`, but the live broker-equity guardrails are intentionally higher. That keeps the runner from opening new day trades when the actual brokerage account is too close to the FINRA PDT minimum of `$25,000`.

The new concentration controls work before order submission, not after. Entries are now reduced or skipped if they would push one symbol or one correlated bucket beyond its configured open-risk cap. The severe-loss kill switch is separate from the normal daily-loss gate: it halts new entries once the sleeve is down `3.5%` on the day, and it force-flattens the book at `5%`.

The projected Greek caps add a second layer on top of that position sizing. Before a new order goes out, the runner estimates what total portfolio delta and vega would become if the trade fills at the planned quantity. If the projected book would move past the hard cap, the entry is blocked. Separately, the execution circuit breaker watches the live plumbing. If entries stop filling normally or recent fills slip badly against us, the runner stops opening fresh positions for the rest of the day while still managing exits and end-of-day cleanup.

The late-day entry cutoff is a simpler quality filter: same-day contracts are blocked after minute `300`, and all other new entries are blocked after minute `345`. That keeps us from opening fresh risk too close to the close unless we explicitly relax the thresholds. The event blackout list is operator-controlled on purpose. You can add one-off windows for things like CPI, FOMC, or single-name earnings by date, minute range, symbol, regime, timing profile, or DTE mode without changing code.

For auto-patching, the system currently draws a safety line on purpose:

- runtime and scheduler issues can self-heal automatically through the health-check tooling
- orphaned paper positions can auto-flatten and journal the reason
- trading-logic changes are not auto-written intraday

That keeps the runner “smart” without letting it quietly rewrite live strategy behavior during market hours.

Two findings drove those settings:

- the old `2%` daily loss gate clipped rebound days and reduced return while worsening drawdown
- the expanded research regularly used up to `10` same-regime positions intraday, so the old regime cap of `6` would have undertraded the validated book

## Config

- Portfolio config:
  `config/multi_ticker_paper_portfolio.yaml`
- Risk controls:
  `config/risk_controls/multi_ticker_portfolio.yaml`
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

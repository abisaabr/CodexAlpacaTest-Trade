# QQQ/SPY/IWM Greek Choppy-Gamma Research Wave - 2026-05-07

Updated: 2026-05-07 14:05 ET

## Purpose

The previous QQQ/SPY/IWM Greek/realtime-parity expansion produced eligible bull and bear candidates but remained blocked because eligible choppy coverage was missing. This wave adds research-only choppy range-edge gamma/reversion candidates instead of repeating already-covered bull/bear variants.

This is not broker-facing. It does not change the live manifest, paper config, global risk policy, or fill/promotion gates.

## Source Commits

- `4259c4e` - realtime shadow writer/order-wait hardening
- `e3416cf` - choppy gamma Greek research variants

## Variant Expansion

Builder changed: `scripts/build_greek_strategy_research_inputs.py`

New candidates per symbol:

- 144 choppy gamma/reversion variants
- Candidate index range: `769-912`
- Directions: lower-band reversion calls and upper-band reversion puts
- Families: `single_leg_repair`, `debit_call_vertical`, `debit_put_vertical`, `broken_wing_call_butterfly`, `broken_wing_put_butterfly`
- DTE modes: `same_day`, `next_expiry`
- Greek selector: `entry_delta_target_research_only`
- Runtime parity: `paper_snapshot_greeks`

Validation:

```powershell
python -m pytest tests\test_option_backtest_greek_selector.py tests\test_multi_ticker_portfolio.py tests\test_realtime_shadow_monitor.py -q
python -m py_compile scripts\build_greek_strategy_research_inputs.py tests\test_option_backtest_greek_selector.py
```

Result: `81 passed`.

## Launch

Wave ID:

```text
qqq_spy_iwm_greek_choppy_gamma_20260507T1355Z
```

GCS root:

```text
gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_choppy_gamma_20260507T1355Z/
```

Launch command:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\launch_gcp_greek_strategy_shards.ps1 `
  -WaveId qqq_spy_iwm_greek_choppy_gamma_20260507T1355Z `
  -Symbols QQQ,SPY,IWM `
  -InstanceSuffix 20260507cg1 `
  -StartCandidateIndex 769 `
  -CandidateCountPerWorker 48 `
  -MaxLaunchesPerSymbol 3 `
  -MachineType e2-standard-2 `
  -LagProfiles "0:60,10:60,30:120" `
  -Selectors "entry_delta_target_research_only"
```

Launched workers:

- `qqq-greek-c769-816-20260507cg1`
- `qqq-greek-c817-864-20260507cg1`
- `qqq-greek-c865-912-20260507cg1`
- `spy-greek-c769-816-20260507cg1`
- `spy-greek-c817-864-20260507cg1`
- `spy-greek-c865-912-20260507cg1`
- `iwm-greek-c769-816-20260507cg1`
- `iwm-greek-c817-864-20260507cg1`
- `iwm-greek-c865-912-20260507cg1`

All nine are research-only GCP VMs. Some zones were capacity-limited, but the launcher retried other zones and all planned workers were created.

## Current Status At Launch Handoff

GCS worker status files are present under:

```text
gs://codexalpaca-control-us/research_results/qqq_spy_iwm_greek_choppy_gamma_20260507T1355Z/workers/
```

Observed phases shortly after launch:

- QQQ workers: running selectors and already writing candidate summaries for at least `qqq_greek_c769_816`.
- SPY workers: staging/running selectors.
- IWM workers: staging/running selectors.

## Aggregation Requirement

When all workers finish:

1. Pull worker artifacts from the GCS wave root.
2. Build strict portfolio reports and promotion-review packets.
3. Keep gates intact:
   - `fill_coverage >= 0.90`
   - `min_option_trades >= 20`
   - `min_test_net_pnl > 0`
   - positive full-period net PnL
   - no severe unresolved loser cluster
4. Require bull, bear, and choppy coverage for a full-regime packet unless a separate governed packet explicitly narrows scope.
5. Treat any generated candidates as governed-validation review only, not live activation.

## Active PAPER Trader Context

At launch handoff, the active local PAPER trader was still running:

- Process: one `run_multi_ticker_portfolio_paper_trader.py` process
- Config: `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`
- Mode: PAPER order submission
- New entries: blocked by daily loss gate
- Open broker state: remaining PAPER option positions plus active sell-to-close attempts
- Safety posture: manage exits and EOD flatten; do not start duplicate paper trader

The patched order-poll hardening in `4259c4e` is committed and pushed, but the currently running process only receives it after a restart. Do not restart solely for the patch while open exits are active unless the process dies or broker state requires recovery.

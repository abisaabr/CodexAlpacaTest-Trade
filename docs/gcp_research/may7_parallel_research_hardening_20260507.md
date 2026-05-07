# May 7 Parallel Research Hardening

Updated: 2026-05-07 13:05 ET

## Purpose

Keep non-broker-facing research moving while the May 7 PAPER trader runs locally. This lane is for next-session strategy hardening only; it does not start live trading, start a second paper trader, alter live manifests, or lower promotion gates.

## Active PAPER Runtime

- Runner: `scripts/run_multi_ticker_portfolio_paper_trader.py`
- Config: `config/multi_symbol_governed_realtime_paper_portfolio_20260507_armed.yaml`
- Manifest: `config/promotion_manifests/multi_symbol_governed_validation_20260507.yaml`
- Current recovered PID at launch handoff: `42628`
- Current durable recovery commit: `ec6d914`
- Runtime root: `D:\codexalpaca_runtime\runs\multi_symbol_governed_realtime_20260507`
- State root: `D:\codexalpaca_runtime\state\multi_symbol_governed_realtime_20260507`

The paper process must remain the only broker-facing process. Research VMs are non-broker-facing and should not modify live runtime state.

## Active Research Wave

- Wave id: `other_tickers_realtime_compatible_20260507T0030Z`
- GCS root: `gs://codexalpaca-control-us/research_results/other_tickers_realtime_compatible_20260507T0030Z/`
- Launcher: `scripts/launch_gcp_realtime_compatible_remaining_tickers.ps1`
- Selector: `entry_liquidity_first_research_only`
- Lag profiles: `0:60`, `10:60`, `30:120`
- Candidate windows launched in this pass: `c109-216`
- Symbols: `AAPL`, `NVDA`, `INTC`, `META`

## Launched Workers

The following research-only VMs were launched for the rt2 continuation tranche:

- `aapl-rescue-c109-144-20260507rt2`
- `aapl-rescue-c145-180-20260507rt2`
- `aapl-rescue-c181-216-20260507rt2`
- `nvda-rescue-c109-144-20260507rt2`
- `nvda-rescue-c145-180-20260507rt2`
- `nvda-rescue-c181-216-20260507rt2`
- `intc-rescue-c109-144-20260507rt2`
- `intc-rescue-c145-180-20260507rt2`
- `intc-rescue-c181-216-20260507rt2`
- `meta-rescue-c109-144-20260507rt2`
- `meta-rescue-c145-180-20260507rt2`
- `meta-rescue-c181-216-20260507rt2`

One `us-east4-a` create attempt for META c181-216 hit `ZONE_RESOURCE_POOL_EXHAUSTED`; the launcher retried successfully in `us-central1-a`.

## Next Actions

1. Monitor worker status and GCS progress under `$GCS_ROOT/workers/`.
2. After the c109-216 workers terminate and artifacts are present, pull workers and build strict portfolio reports.
3. Build promotion-review packets with the existing gates, including `fill_coverage >= 0.90`, option trade count, positive full/test PnL, loser-cluster, and regime-context checks.
4. Delete only terminated research VMs after artifacts are confirmed in GCS.
5. Launch the next candidate windows, starting at c217, only after capacity frees.

## Aggregation Recipe

```powershell
$wave = "other_tickers_realtime_compatible_20260507T0030Z"
$gcs = "gs://codexalpaca-control-us/research_results/$wave"
$local = "reports/gcp_research/$wave/gcs_worker_pull_rt2/workers"
$out = "reports/gcp_research/$wave/aggregate_rt2"

gcloud storage cp --recursive "$gcs/workers/" $local --project codexalpaca

python scripts\build_research_portfolio_report.py `
  --replay-root $local `
  --output-dir $out `
  --fill-coverage-gate 0.90 `
  --min-option-trades 20 `
  --min-test-net-pnl 0 `
  --max-positions 12 `
  --max-strategies-per-symbol 2 `
  --max-symbol-weight 0.20 `
  --initial-cash 25000 `
  --candidate-identity-mode variant_profile `
  --required-regimes bull,bear,choppy

python scripts\build_research_promotion_review_packet.py `
  --portfolio-report-json "$out/research_portfolio_report.json" `
  --output-dir "$out/promotion_packet"

gcloud storage cp --recursive $out "$gcs/aggregate_rt2/" --project codexalpaca
```

## Guardrails

- PAPER mode only.
- No duplicate broker-facing process.
- No live-manifest changes from this research lane.
- No promotion by manual override.
- No lowering of `fill_coverage >= 0.90`.
- No strategy is eligible for future paper activation unless a generated promotion-review packet clears gates and runtime compatibility is documented.

## Cleanup Update

Updated: 2026-05-07 13:20 ET

Terminated rt2 research VMs with visible GCS worker directories were deleted to free capacity. The remaining rt2 workers at this checkpoint are:

- `meta-rescue-c109-144-20260507rt2`
- `meta-rescue-c181-216-20260507rt2`

The `meta-rescue-c145-180-20260507rt2` worker reached `TERMINATED` after artifacts were visible and was deleted. No broker-facing process or paper runtime state was changed.

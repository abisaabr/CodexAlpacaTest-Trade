# QQQ+SPY Real-Time Paper Launch Packet Status - 2026-05-05

## Status

The QQQ+SPY realtime launch packet is ready for an operator-armed
broker-facing paper canary.

- Packet decision: `ready_for_operator_armed_broker_facing_canary`
- Broker-facing started by packet: `false`
- Requires explicit operator approval: `true`
- Runner commit: `cf20b30db5a47783df748a23a637e219aadc0f38`
- Data lane: SIP stock bars, OPRA option snapshots
- Symbols: `QQQ`, `SPY`
- Strategies: 6 total, one bull/bear/choppy candidate per symbol
- IWM: excluded until regime-complete

## Evidence

- GCP no-order realtime canary corrected summary:
  `gs://codexalpaca-control-us/research_results/qqq-spy-realtime-canary-noorder-20260505T1410Z/gcp_canary/outputs/canary_summary_corrected.json`
- Launch packet JSON:
  `gs://codexalpaca-control-us/research_results/qqq_spy_regime_complete_realtime_20260505/paper_launch_packet/qqq_spy_realtime_paper_launch_packet.json`
- Launch packet Markdown:
  `gs://codexalpaca-control-us/research_results/qqq_spy_regime_complete_realtime_20260505/paper_launch_packet/qqq_spy_realtime_paper_launch_packet.md`

## Required Operator Boundary

Broker-facing paper submission requires an explicit operator command containing
`--submit-paper-orders`. The packet itself does not submit orders.

Recommended first broker-facing canary command after a fresh no-order preflight:

```bash
python scripts/run_multi_ticker_portfolio_paper_trader.py \
  --portfolio-config config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml \
  --run-once \
  --submit-paper-orders
```

Do not start `multi-ticker-trader-v1` without replacing its legacy startup
metadata first.

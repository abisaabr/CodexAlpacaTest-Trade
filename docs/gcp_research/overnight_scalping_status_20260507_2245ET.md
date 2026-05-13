# Overnight Scalping And Realtime-Compatible Research Status - 2026-05-07 22:45 ET

Scope: PAPER-only May 8 readiness plus research-only overnight expansion. No live trading was started, no live manifest was changed, and the `fill_coverage >= 0.90` gate remains intact.

## May 8 PAPER Config

Prepared config:

- `config/multi_symbol_governed_realtime_paper_portfolio_20260508_armed.yaml`

The config now references four governed-validation manifests with 186 total runtime strategies and zero duplicate runtime strategy IDs:

- `promotion_manifests/multi_symbol_governed_validation_20260508_runtime_unique.yaml`: 160 strategies copied from the May 7 governed set with four duplicate runtime IDs disambiguated for ledger/order attribution.
- `promotion_manifests/tsm_overnight_governed_validation_20260508.yaml`: 15 TSM strategies from two regime-complete overnight packets.
- `promotion_manifests/avgo_overnight_governed_validation_20260508.yaml`: 5 AVGO strategies from a regime-complete overnight packet.
- `promotion_manifests/qqq_overnight_governed_validation_20260508.yaml`: 6 QQQ strategies from a regime-complete overnight packet.

Validation:

- `python -m pytest -q tests\test_build_governed_validation_manifest_from_packets.py tests\test_runner_submit_order_arming.py tests\test_build_broker_fill_strategy_reconciliation.py`
- Result: `8 passed`.

Important runtime note: `broker_min_equity_to_trade: 26000` and `broker_equity_emergency_stop: 25500` remain active. The May 8 PAPER trader must still pass fresh preflight, PAPER endpoint checks, duplicate-process checks, broker open-order/position checks, and equity/risk gates before order submission.

## New Eligible Packets

New governed-validation additions from `traditional_20ticker_realtime_compatible_overnight_20260507T2045ET`:

- TSM c037-072: `ready_for_governed_validation_review`, 8 candidates, bull/bear/choppy covered.
- AVGO c073-108: `ready_for_governed_validation_review`, 5 candidates, bull/bear/choppy covered.
- QQQ c073-108: `ready_for_governed_validation_review`, 6 candidates, bull/bear/choppy covered.

Still blocked / regime-incomplete:

- SPY c037-072 and c073-108 found bear/choppy leads but no regime-complete packet yet.
- AMD c037-072 and c073-108 remain regime-incomplete, though AMD c073-108 produced a strong bear broken-wing put butterfly and choppy leads.
- XOM, PLTR, MSFT c073, and IWM c037 did not produce eligible governed packets in the synced slices.

## Active Research Workers

Active traditional workers at this handoff:

- `amd-rescue-c109-144-20260507trad13`
- `avgo-rescue-c109-144-20260507trad13`
- `qqq-rescue-c109-144-20260507trad14`
- `spy-rescue-c109-144-20260507trad14`
- `tsm-rescue-c073-108-20260507trad13`

Active microstructure workers:

- `micro-event-c89089-89600-20260507om3`
- `micro-event-c90113-90624-20260507om3`
- `micro-event-c90625-91136-20260507om3`
- `micro-event-c91137-91648-20260507om4`

Microstructure status so far: completed synced chunks through c89601-90112 have produced zero review-like candidates. Continue the grid only as research-only unless strict packet output later supports governed review or a separate operator-approved PAPER experiment is explicitly documented.

## GCS Roots

Traditional wave:

- `gs://codexalpaca-control-us/research_results/traditional_20ticker_realtime_compatible_overnight_20260507T2045ET/`

Microstructure wave:

- `gs://codexalpaca-control-us/research_results/microstructure_rare_event_overnight_20260507T2030ET/`

## Next Operating Steps

1. Sync and parse active worker artifacts as each VM terminates.
2. Delete only terminated research VMs after packets/artifacts are confirmed present.
3. Build additional governed-validation manifests only from generated packets with `ready_for_governed_validation_review`.
4. Keep SPY/AMD searching for missing regime coverage; continue TSM/AVGO/QQQ while they keep producing regime-complete packets.
5. Before May 8 RTH, run a fresh PAPER startup preflight only after SIP stock data is fresh and broker state is clean.

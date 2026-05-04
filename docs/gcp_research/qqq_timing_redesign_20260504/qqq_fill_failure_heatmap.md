# Ticker365 Fill-Failure Heatmap

- Generated UTC: `2026-05-04T12:40:50Z`
- Source wave: `gs://codexalpaca-control-us/research_results/ticker365_entry_asof_rescue_20260503T2352Z`
- Candidate summary files: `4`
- Fill failure files: `4`
- Candidate rows: `160`
- Fill failure rows: `48448`
- Decision: `strategy_entry_exit_redesign_wave_required`

## Failure Reasons

- `no_entry_bar`: `29824`
- `no_exit_bar`: `18080`
- `too_expensive`: `416`
- `no_selected_contract`: `128`

## Top Symbol/Reason Hotspots

- `QQQ` `no_entry_bar`: `29824`
- `QQQ` `no_exit_bar`: `18080`
- `QQQ` `too_expensive`: `416`
- `QQQ` `no_selected_contract`: `128`

## Top Redesign Targets

- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `base` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `fast` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `patient` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `slow` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__bull__call__debit_call_vertical` `base` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__bull__call__debit_call_vertical` `fast` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__bull__call__debit_call_vertical` `patient` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__bull__call__debit_call_vertical` `slow` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__iron_butterfly` `base` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__iron_butterfly` `fast` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__iron_butterfly` `patient` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__iron_butterfly` `slow` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__premium_defense_spread` `base` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__premium_defense_spread` `fast` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__premium_defense_spread` `patient` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__premium_defense_spread` `slow` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `base` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `fast` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `patient` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`
- `QQQ` `qqq__choppy__call__broken_wing_call_butterfly` `slow` action `entry_timing_redesign` fill `0.538` entry `0.7302` exit `0.7367` test_pnl `6299.41`

## Next Step Contract

- Do not promote any strategy from this heatmap.
- Patch timing-profile semantics before rerunning redesign candidates.
- Run a strict at-or-after entry replay first; use as-of only as a research diagnostic.
- Keep fill_coverage >= 0.90 and all paper/live/risk manifests unchanged.
- If the strict redesign wave remains below the fill gate, redesign source signal timing rather than widening the gate.

# QQQ Fill-Squash Micro Wave

Status: launching research-only GCP micro-shards

Wave ID: `ticker365_qqq_fill_squash_micro_20260504T1730Z`

GCS root: `gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_micro_20260504T1730Z/`

Hard rules:
- Do not start trading.
- Do not submit paper orders.
- Do not modify live manifests.
- Do not change risk policy.
- Do not lower `fill_coverage >= 0.90`.
- Promotion means governed validation review only unless a generated packet explicitly says eligible.

## Purpose

The prior QQQ dense data check showed raw option data coverage is strong, while strategy-level fill coverage can fail because source stock-trade timestamps do not line up cleanly with selected option bars. This wave breaks QQQ into smaller 7-candidate chunks and tests timing/lookup hypotheses in parallel so the fill blocker can be isolated quickly.

## Phase 1 Micro Triage

Scope:
- Symbol: `QQQ`
- Dataset: dense 365-day next-trading-day ATM +/- 5 selected-contract set
- Candidate scope: top `42`
- Chunk size: `7`
- Chunks: `c001-007`, `c008-014`, `c015-021`, `c022-028`, `c029-035`, `c036-042`
- Selectors per worker: `nearest_contract`, `entry_liquidity_first_research_only`
- Expected summary files: `72`

Profiles:
- `strict-e0x60`: strict entry, strict exit, entry lag `0`, exit lag `60`, option-session filtered.
- `strict-e15x120`: strict entry, strict exit, entry lag `15`, exit lag `120`, option-session filtered.
- `strict-e30x180`: strict entry, strict exit, entry lag `30`, exit lag `180`, option-session filtered.
- `asof-e5s1-x120`: research-only as-of entry fallback up to `1` minute stale, strict exit, option-session filtered.
- `diag-priorexit-e15x120`: diagnostic prior-exit fallback, option-session filtered.
- `diag-nosession-e15x120`: no source session filter diagnostic.

Interpretation:
- `strict-*` profiles are candidates for legitimate replay semantics if they also pass economics.
- `asof-*` profiles diagnose entry timestamp mismatch. They are not automatically promotion-ready without a rationale that avoids lookahead/stale-pricing bias.
- `diag-priorexit-*` profiles diagnose exit timestamp mismatch. They are not automatically promotion-ready if stale exits inflate fills.
- `diag-nosession-*` profiles confirm how much of the blocker comes from pre/post-option-session source trades.

## Phase 2 Expand Winner

After Phase 1 reaches enough completed summaries, build the fill-squash heatmap:

```powershell
python scripts\build_qqq_fill_squash_heatmap.py `
  --gcs-prefix gs://codexalpaca-control-us/research_results/ticker365_qqq_fill_squash_micro_20260504T1730Z `
  --output-dir reports\gcp_research\qqq_fill_squash_micro_20260504
```

The winning legitimate profile must be expanded to all `126` QQQ candidates before any promotion-review decision. The full expansion should use the same hard gates and write an auditable promotion-review packet.

## Launch Command

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File scripts\run_ticker365_qqq_fill_squash_micro_shards.ps1 `
  -MaxLaunches 8
```

The launcher is quota-aware, skips completed shards, skips active duplicate instance names across zones, and uses a local mutex to avoid overlapping scheduled/manual launch races.

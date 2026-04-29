from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUMMARY_JSON = REPO_ROOT / "reports" / "research_wave" / "summaries" / "gcp_research_run_summary.json"
DEFAULT_SELECTED_CONTRACTS_ROOT = (
    REPO_ROOT
    / "data"
    / "silver"
    / "historical"
    / "research_preferred_1min_20260421_20260423_stock_contracts"
    / "selected_option_contracts"
)
DEFAULT_OPTION_BARS_ROOT = (
    REPO_ROOT
    / "data"
    / "silver"
    / "historical"
    / "research_preferred_1min_20260421_20260423_stock_contracts"
    / "option_bars"
)
DEFAULT_OPTION_TRADES_ROOT = (
    REPO_ROOT
    / "data"
    / "silver"
    / "historical"
    / "research_preferred_1min_20260421_20260423_stock_contracts"
    / "option_trades"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "option_aware_queue"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a governed option-aware follow-up queue from real-bar smoke candidates."
    )
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--selected-contracts-root", default=str(DEFAULT_SELECTED_CONTRACTS_ROOT))
    parser.add_argument("--option-bars-root", default=str(DEFAULT_OPTION_BARS_ROOT))
    parser.add_argument("--option-trades-root", default=str(DEFAULT_OPTION_TRADES_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--contracts-per-candidate", type=int, default=5)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _candidate_direction(candidate: dict[str, Any]) -> str:
    source = str(candidate.get("source_strategy_id") or candidate.get("variant_id") or "").lower()
    if "put" in source or "short" in source or "bear" in source:
        return "put"
    return "call"


def _load_selected_contracts(root: Path, symbol: str, option_type: str) -> pd.DataFrame:
    frames = []
    symbol_root = root / f"underlying={symbol.upper()}"
    for path in sorted(symbol_root.rglob("part.parquet")):
        frame = pd.read_parquet(path)
        if "option_type" not in frame.columns:
            continue
        frames.append(frame[frame["option_type"].astype(str).str.lower() == option_type])
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(
        ["trade_date", "dte", "relative_strike_step", "symbol"]
    )


def _contract_records(frame: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    ranked = frame.assign(abs_relative_strike_step=frame["relative_strike_step"].abs())
    ranked = ranked.sort_values(["trade_date", "dte", "abs_relative_strike_step", "symbol"]).head(limit)
    keys = [
        "trade_date",
        "reference_timestamp",
        "reference_price",
        "underlying_symbol",
        "symbol",
        "expiration_date",
        "option_type",
        "strike_price",
        "dte",
        "relative_strike_step",
    ]
    records = []
    for row in ranked.to_dict("records"):
        records.append({key: str(row.get(key)) for key in keys if key in row})
    return records


def _has_files(root: Path) -> bool:
    return root.exists() and any(path.is_file() for path in root.rglob("*"))


def build_queue(
    *,
    summary_json: Path,
    selected_contracts_root: Path,
    option_bars_root: Path,
    option_trades_root: Path,
    top_n: int,
    contracts_per_candidate: int,
) -> dict[str, Any]:
    summary = _load_json(summary_json)
    candidates = summary.get("top_candidates") if isinstance(summary.get("top_candidates"), list) else []
    option_bars_present = _has_files(option_bars_root)
    option_trades_present = _has_files(option_trades_root)
    queue_items: list[dict[str, Any]] = []
    for rank, candidate in enumerate(candidates[:top_n], start=1):
        if not isinstance(candidate, dict):
            continue
        symbol = str(candidate.get("symbol") or "").upper()
        option_type = _candidate_direction(candidate)
        contracts = _load_selected_contracts(selected_contracts_root, symbol, option_type)
        blockers = []
        if contracts.empty:
            blockers.append("missing_selected_option_contracts")
        if not option_bars_present:
            blockers.append("missing_historical_option_bars")
        if not option_trades_present:
            blockers.append("missing_historical_option_trades")
        queue_items.append(
            {
                "rank": rank,
                "state": "research_follow_up_only",
                "candidate_variant_id": candidate.get("variant_id"),
                "symbol": symbol,
                "directional_option_type": option_type,
                "source_strategy_id": candidate.get("source_strategy_id"),
                "stock_smoke_expectancy_after_cost": candidate.get("expectancy_after_cost"),
                "stock_smoke_net_pnl": candidate.get("net_pnl"),
                "stock_smoke_trade_count": candidate.get("actual_trade_count"),
                "representative_contract_count": int(len(contracts)),
                "representative_contracts": _contract_records(
                    contracts, limit=contracts_per_candidate
                ),
                "promotion_allowed": False,
                "broker_facing": False,
                "live_manifest_effect": "none",
                "risk_policy_effect": "none",
                "required_next_evidence": [
                    "historical option bars for selected contracts",
                    "historical option trades or quote/spread data for fill-cost calibration",
                    "option-aware entry/exit economics table",
                    "train/test or walk-forward split before any strategy governance review",
                ],
                "blockers": blockers,
            }
        )

    blocker_counts: dict[str, int] = {}
    for item in queue_items:
        for blocker in item["blockers"]:
            blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
    status = "blocked_missing_option_market_data" if blocker_counts else "ready_for_option_aware_backtest"
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "status": status,
        "summary_json": str(summary_json),
        "selected_contracts_root": str(selected_contracts_root),
        "option_bars_root": str(option_bars_root),
        "option_trades_root": str(option_trades_root),
        "source_candidate_count": len(candidates),
        "queue_item_count": len(queue_items),
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "promotion_allowed": False,
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "queue_items": queue_items,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Option-Aware Research Queue",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Status: `{payload['status']}`",
        f"- Queue items: `{payload['queue_item_count']}`",
        f"- Promotion allowed: `{payload['promotion_allowed']}`",
        f"- Broker facing: `{payload['broker_facing']}`",
        "",
        "## Blocker Counts",
        "",
    ]
    for blocker, count in payload["blocker_counts"].items():
        lines.append(f"- `{blocker}`: `{count}`")
    lines.extend(["", "## Top Follow-Ups", ""])
    for item in payload["queue_items"][:10]:
        lines.append(
            "- "
            f"`{item['candidate_variant_id']}` "
            f"`{item['symbol']}` `{item['directional_option_type']}` "
            f"contracts `{item['representative_contract_count']}` "
            f"blockers `{','.join(item['blockers'])}`"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    payload = build_queue(
        summary_json=Path(args.summary_json),
        selected_contracts_root=Path(args.selected_contracts_root),
        option_bars_root=Path(args.option_bars_root),
        option_trades_root=Path(args.option_trades_root),
        top_n=args.top_n,
        contracts_per_candidate=args.contracts_per_candidate,
    )
    output_dir = Path(args.output_dir)
    write_json(output_dir / "option_aware_research_queue.json", payload)
    write_markdown(output_dir / "option_aware_research_queue.md", payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

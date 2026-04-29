from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.data.schemas import SELECTED_OPTION_SCHEMA
from scripts.run_option_aware_research_backtest import (
    _load_parquet_tree,
    _load_stock_bars,
    _stock_trades_for_variant,
    _variant_map,
)

DEFAULT_QUEUE_JSON = (
    REPO_ROOT
    / "reports"
    / "research_wave"
    / "option_aware_queue"
    / "option_aware_research_queue.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "event_driven_option_contracts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build research-only option contract selections around actual strategy entry times."
    )
    parser.add_argument("--queue-json", default=str(DEFAULT_QUEUE_JSON))
    parser.add_argument("--variants-jsonl", required=True)
    parser.add_argument("--stock-bars-path", required=True)
    parser.add_argument("--option-contracts-root", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--symbol-filter", default=None)
    parser.add_argument("--skip-blocked-queue-items", action="store_true")
    parser.add_argument("--min-dte", type=int, default=1)
    parser.add_argument("--max-dte", type=int, default=75)
    parser.add_argument("--strike-steps", type=int, default=10)
    parser.add_argument("--initial-cash", type=float, default=25_000.0)
    parser.add_argument("--allocation-fraction", type=float, default=0.10)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _symbol_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {item.strip().upper() for item in value.split(",") if item.strip()}
    return symbols or None


def _normalize_inventory(contracts: pd.DataFrame) -> pd.DataFrame:
    if contracts.empty:
        return contracts
    frame = contracts.copy()
    if "inventory_status" not in frame.columns:
        frame["inventory_status"] = pd.NA
    frame["underlying_symbol"] = frame["underlying_symbol"].astype(str).str.upper()
    frame["option_type"] = frame["option_type"].astype(str).str.lower()
    frame["expiration_date"] = pd.to_datetime(
        frame["expiration_date"], errors="coerce"
    ).dt.normalize()
    frame["strike_price"] = pd.to_numeric(frame["strike_price"], errors="coerce")
    return frame.dropna(subset=["symbol", "underlying_symbol", "expiration_date", "strike_price"])


def select_event_driven_contracts(
    *,
    contracts: pd.DataFrame,
    trades: pd.DataFrame,
    symbol: str,
    option_type: str,
    candidate_variant_id: str,
    min_dte: int,
    max_dte: int,
    strike_steps: int,
) -> pd.DataFrame:
    if contracts.empty or trades.empty:
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())
    if strike_steps < 0:
        raise ValueError("strike_steps must be non-negative.")

    inventory = _normalize_inventory(contracts)
    symbol = symbol.upper()
    option_type = option_type.lower()
    inventory = inventory[
        (inventory["underlying_symbol"] == symbol) & (inventory["option_type"] == option_type)
    ].copy()
    if inventory.empty:
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())

    rows: list[pd.DataFrame] = []
    trade_frame = trades.copy()
    trade_frame["entry_time"] = pd.to_datetime(trade_frame["entry_time"], utc=True, errors="coerce")
    trade_frame["entry_price"] = pd.to_numeric(trade_frame["entry_price"], errors="coerce")
    trade_frame = trade_frame.dropna(subset=["entry_time", "entry_price"])

    for trade in trade_frame.to_dict("records"):
        entry_time = pd.Timestamp(trade["entry_time"])
        trade_day = entry_time.tz_convert("America/New_York").normalize().tz_localize(None)
        reference_price = float(trade["entry_price"])
        subset = inventory.copy()
        subset["dte"] = (subset["expiration_date"] - trade_day).dt.days
        subset = subset[(subset["dte"] >= min_dte) & (subset["dte"] <= max_dte)]
        if subset.empty:
            continue

        for expiration_date, contract_group in subset.groupby("expiration_date", sort=True):
            strikes = sorted(
                float(value) for value in contract_group["strike_price"].dropna().unique()
            )
            if not strikes:
                continue
            atm_index = min(
                range(len(strikes)),
                key=lambda index: (abs(strikes[index] - reference_price), strikes[index]),
            )
            atm_strike = strikes[atm_index]
            lower_index = max(0, atm_index - strike_steps)
            upper_index = min(len(strikes) - 1, atm_index + strike_steps)
            selected_strikes = set(strikes[lower_index : upper_index + 1])
            step_lookup = {
                strike: selected_index - atm_index
                for selected_index, strike in enumerate(strikes)
                if strike in selected_strikes
            }
            selected = contract_group[contract_group["strike_price"].isin(selected_strikes)].copy()
            if selected.empty:
                continue
            selected["trade_date"] = trade_day
            selected["reference_timestamp"] = entry_time
            selected["reference_price"] = reference_price
            selected["atm_strike"] = atm_strike
            selected["relative_strike_step"] = (
                selected["strike_price"].map(step_lookup).astype("Int64")
            )
            entry_time_label = entry_time.isoformat()
            expiration_date_label = pd.Timestamp(expiration_date).date().isoformat()
            selected["selection_reason"] = selected.apply(
                lambda row, entry_time_label=entry_time_label, expiration_date_label=expiration_date_label: (
                    "reference=strategy_entry_time;"
                    f" candidate_variant_id={candidate_variant_id};"
                    f" entry_time={entry_time_label};"
                    f" expiration_date={expiration_date_label};"
                    f" dte={int(row['dte'])};"
                    f" strike_step={int(row['relative_strike_step'])};"
                    f" within_plus_minus_{strike_steps}_steps"
                ),
                axis=1,
            )
            rows.append(
                selected[
                    [
                        "trade_date",
                        "reference_timestamp",
                        "reference_price",
                        "underlying_symbol",
                        "symbol",
                        "expiration_date",
                        "option_type",
                        "strike_price",
                        "dte",
                        "atm_strike",
                        "relative_strike_step",
                        "selection_reason",
                        "inventory_status",
                    ]
                ]
            )

    if not rows:
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())
    result = pd.concat(rows, ignore_index=True)
    result = result.drop_duplicates(subset=["trade_date", "symbol"]).sort_values(
        ["underlying_symbol", "trade_date", "dte", "relative_strike_step", "symbol"]
    )
    return SELECTED_OPTION_SCHEMA.apply(result.reset_index(drop=True))


def _write_partitioned_selected(root: Path, frame: pd.DataFrame) -> list[str]:
    artifacts: list[str] = []
    if frame.empty:
        return artifacts
    for (underlying, trade_date), subset in frame.groupby(
        ["underlying_symbol", "trade_date"], sort=True
    ):
        day = pd.Timestamp(trade_date).date().isoformat()
        path = root / f"underlying={underlying}" / f"trade_date={day}" / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        subset.to_parquet(path, index=False)
        artifacts.append(str(path))
    return artifacts


def build_event_driven_contract_packet(
    *,
    queue_json: Path,
    variants_jsonl: Path,
    stock_bars_path: Path,
    option_contracts_root: Path,
    output_dir: Path,
    top_n: int,
    symbol_filter: set[str] | None,
    skip_blocked_queue_items: bool,
    min_dte: int,
    max_dte: int,
    strike_steps: int,
    initial_cash: float,
    allocation_fraction: float,
) -> dict[str, Any]:
    queue = _load_json(queue_json)
    variants = _variant_map(variants_jsonl)
    stock_bars = _load_stock_bars(stock_bars_path)
    contracts = _load_parquet_tree(option_contracts_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_root = output_dir / "selected_option_contracts"

    selected_frames: list[pd.DataFrame] = []
    candidate_summaries: list[dict[str, Any]] = []
    source_items = [item for item in queue.get("queue_items", []) if isinstance(item, dict)]
    filtered_items: list[dict[str, Any]] = []
    for item in source_items:
        symbol = str(item.get("symbol") or "").upper()
        if symbol_filter and symbol not in symbol_filter:
            continue
        if skip_blocked_queue_items and item.get("blockers"):
            continue
        filtered_items.append(item)

    for item in filtered_items[:top_n]:
        variant_id = str(item.get("candidate_variant_id") or "")
        variant = variants.get(variant_id)
        if not variant:
            candidate_summaries.append(
                {
                    "candidate_variant_id": variant_id,
                    "symbol": item.get("symbol"),
                    "status": "blocked_missing_variant_definition",
                    "selected_contract_count": 0,
                }
            )
            continue
        trades = _stock_trades_for_variant(
            variant=variant,
            stock_bars=stock_bars,
            initial_cash=initial_cash,
            allocation_fraction=allocation_fraction,
        )
        selected = select_event_driven_contracts(
            contracts=contracts,
            trades=trades,
            symbol=str(item.get("symbol") or ""),
            option_type=str(item.get("directional_option_type") or ""),
            candidate_variant_id=variant_id,
            min_dte=min_dte,
            max_dte=max_dte,
            strike_steps=strike_steps,
        )
        selected_frames.append(selected)
        candidate_summaries.append(
            {
                "candidate_variant_id": variant_id,
                "symbol": item.get("symbol"),
                "source_stock_trade_count": int(len(trades)),
                "selected_contract_count": int(len(selected)),
                "selected_trade_date_count": (
                    int(selected["trade_date"].nunique()) if not selected.empty else 0
                ),
                "status": (
                    "ready_event_driven_contracts"
                    if not selected.empty
                    else "blocked_no_event_contracts"
                ),
            }
        )

    combined = (
        SELECTED_OPTION_SCHEMA.apply(pd.concat(selected_frames, ignore_index=True))
        if selected_frames
        else SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())
    )
    if not combined.empty:
        combined = combined.drop_duplicates(subset=["trade_date", "symbol"]).sort_values(
            ["underlying_symbol", "trade_date", "dte", "relative_strike_step", "symbol"]
        )
    artifacts = _write_partitioned_selected(selected_root, combined)
    packet = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "status": "completed" if artifacts else "blocked_no_selected_contracts",
        "selection_method": "event_driven_strategy_entry_contracts_research_only",
        "promotion_allowed": False,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "queue_json": str(queue_json),
        "variants_jsonl": str(variants_jsonl),
        "stock_bars_path": str(stock_bars_path),
        "option_contracts_root": str(option_contracts_root),
        "selected_contracts_root": str(selected_root),
        "source_queue_item_count": len(source_items),
        "queue_item_count_after_filters": len(filtered_items),
        "candidate_count": len(candidate_summaries),
        "selected_contract_count": int(len(combined)),
        "selected_symbol_count": int(combined["symbol"].nunique()) if not combined.empty else 0,
        "min_dte": min_dte,
        "max_dte": max_dte,
        "strike_steps": strike_steps,
        "symbol_filter": sorted(symbol_filter) if symbol_filter else [],
        "skip_blocked_queue_items": skip_blocked_queue_items,
        "candidate_summaries": candidate_summaries,
        "artifacts": artifacts,
        "next_step_contract": [
            "Use this selected-contract root only for research-only option bar/trade downloads.",
            "Rerun option-aware replay after downloading bars/trades and require >=0.90 data fill before promotion review.",
            "Do not change live manifests or risk policy from this packet.",
        ],
    }
    (output_dir / "event_driven_option_contract_packet.json").write_text(
        json.dumps(packet, indent=2, default=str),
        encoding="utf-8",
    )
    return packet


def main() -> None:
    args = parse_args()
    packet = build_event_driven_contract_packet(
        queue_json=Path(args.queue_json),
        variants_jsonl=Path(args.variants_jsonl),
        stock_bars_path=Path(args.stock_bars_path),
        option_contracts_root=Path(args.option_contracts_root),
        output_dir=Path(args.output_dir),
        top_n=args.top_n,
        symbol_filter=_symbol_filter(args.symbol_filter),
        skip_blocked_queue_items=args.skip_blocked_queue_items,
        min_dte=args.min_dte,
        max_dte=args.max_dte,
        strike_steps=args.strike_steps,
        initial_cash=args.initial_cash,
        allocation_fraction=args.allocation_fraction,
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

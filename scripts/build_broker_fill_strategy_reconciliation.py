from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


IDENTITY_COLUMNS = [
    "strategy_name",
    "source_strategy_id",
    "candidate_variant_id",
    "underlying_symbol",
    "regime",
    "phase",
]


def _side_position_delta(side: Any, qty: Any) -> float:
    value = float(qty or 0.0)
    side_text = str(side or "").lower()
    if side_text in {"sell", "sell_short"}:
        return -value
    return value


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _normalise_order_id(value: Any) -> str:
    return str(value or "").strip()


def _load_order_identity(order_journal_path: Path) -> pd.DataFrame:
    payload = _read_json(order_journal_path, [])
    if isinstance(payload, dict):
        payload = [payload]
    rows: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict):
            continue
        order_id = _normalise_order_id(item.get("order_id"))
        if not order_id:
            continue
        row = {"order_id": order_id}
        for column in IDENTITY_COLUMNS:
            row[column] = item.get(column)
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=["order_id", *IDENTITY_COLUMNS])
    frame = pd.DataFrame(rows)
    frame["_identity_score"] = frame[IDENTITY_COLUMNS].notna().sum(axis=1)
    frame = (
        frame.sort_values(["order_id", "_identity_score"], ascending=[True, False])
        .drop_duplicates(subset=["order_id"], keep="first")
        .drop(columns=["_identity_score"])
        .reset_index(drop=True)
    )
    return frame


def _signed_cash(row: pd.Series) -> float:
    if "signed_cash_ex_fees" in row and pd.notna(row["signed_cash_ex_fees"]):
        return float(row["signed_cash_ex_fees"])
    qty = float(row.get("qty") or 0.0)
    price = float(row.get("price") or 0.0)
    side = str(row.get("side") or "").lower()
    multiplier = -1.0 if side == "buy" else 1.0
    return multiplier * qty * price * 100.0


def _strategy_summary(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    columns = [
        "trade_date",
        "strategy_name",
        "source_strategy_id",
        "underlying_symbol",
        "regime",
        "fill_count",
        "buy_qty",
        "sell_qty",
        "net_cash_ex_fees",
        "symbols",
    ]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    strategy_frame = frame.copy()
    strategy_frame["trade_date"] = trade_date
    strategy_frame["buy_qty_component"] = strategy_frame.apply(
        lambda row: float(row.get("qty") or 0.0) if str(row.get("side") or "").lower() == "buy" else 0.0,
        axis=1,
    )
    strategy_frame["sell_qty_component"] = strategy_frame.apply(
        lambda row: float(row.get("qty") or 0.0)
        if str(row.get("side") or "").lower() in {"sell", "sell_short"}
        else 0.0,
        axis=1,
    )
    grouped = strategy_frame.groupby(
        ["trade_date", "strategy_name", "source_strategy_id", "underlying_symbol", "regime"],
        dropna=False,
    )
    summary = grouped.agg(
        fill_count=("order_id", "count"),
        buy_qty=("buy_qty_component", "sum"),
        sell_qty=("sell_qty_component", "sum"),
        net_cash_ex_fees=("signed_cash_ex_fees", "sum"),
        symbols=("symbol", lambda values: ",".join(sorted({str(value) for value in values if pd.notna(value)}))),
    ).reset_index()
    summary["net_cash_ex_fees"] = summary["net_cash_ex_fees"].round(4)
    return summary[columns].sort_values(
        ["net_cash_ex_fees", "fill_count", "strategy_name"],
        ascending=[False, False, True],
    )


def _infer_unmatched_exit_allocations(matched: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    direct = matched.loc[matched["matched_to_strategy"]].copy()
    if direct.empty:
        return pd.DataFrame(), matched.loc[~matched["matched_to_strategy"]].copy()

    direct["position_delta"] = direct.apply(lambda row: _side_position_delta(row.get("side"), row.get("qty")), axis=1)
    position_keys = ["strategy_name", "source_strategy_id", "candidate_variant_id", "underlying_symbol", "regime", "symbol"]
    positions = (
        direct.groupby(position_keys, dropna=False)["position_delta"]
        .sum()
        .reset_index()
        .rename(columns={"position_delta": "open_qty"})
    )
    inferred_rows: list[dict[str, Any]] = []
    still_unmatched_rows: list[dict[str, Any]] = []
    unmatched = matched.loc[~matched["matched_to_strategy"]].copy()
    if "transaction_time" in unmatched.columns:
        unmatched = unmatched.sort_values("transaction_time")

    for _, fill in unmatched.iterrows():
        symbol = str(fill.get("symbol") or "")
        qty = float(fill.get("qty") or 0.0)
        delta = _side_position_delta(fill.get("side"), qty)
        if not symbol or qty <= 0.0 or delta == 0.0:
            still_unmatched_rows.append(fill.to_dict())
            continue

        if delta < 0:
            candidates = positions.loc[(positions["symbol"].astype(str) == symbol) & (positions["open_qty"] > 0.0)].copy()
        else:
            candidates = positions.loc[(positions["symbol"].astype(str) == symbol) & (positions["open_qty"] < 0.0)].copy()

        candidate_abs_qty = float(candidates["open_qty"].abs().sum()) if not candidates.empty else 0.0
        allocatable_qty = min(abs(delta), candidate_abs_qty)
        if candidates.empty or allocatable_qty <= 0.0:
            still_unmatched_rows.append(fill.to_dict())
            continue

        allocated_qty_total = 0.0
        allocated_cash_total = 0.0
        for _, candidate in candidates.iterrows():
            proportion = abs(float(candidate["open_qty"])) / candidate_abs_qty
            allocated_qty = min(abs(delta) * proportion, abs(float(candidate["open_qty"])))
            allocated_cash = float(fill["signed_cash_ex_fees"]) * (allocated_qty / abs(delta))
            allocated_qty_total += allocated_qty
            allocated_cash_total += allocated_cash
            row = fill.to_dict()
            for column in position_keys:
                row[column] = candidate[column]
            row["qty"] = round(allocated_qty, 6)
            row["signed_cash_ex_fees"] = round(allocated_cash, 4)
            row["matched_to_strategy"] = True
            row["match_kind"] = "inferred_symbol_balance"
            inferred_rows.append(row)

            mask = pd.Series(True, index=positions.index)
            for column in position_keys:
                mask &= positions[column].eq(candidate[column])
            positions.loc[mask, "open_qty"] = positions.loc[mask, "open_qty"] + (
                delta * (allocated_qty / abs(delta))
            )

        remaining_qty = abs(delta) - allocated_qty_total
        if remaining_qty > 0.000001:
            row = fill.to_dict()
            row["qty"] = round(remaining_qty, 6)
            row["signed_cash_ex_fees"] = round(
                float(fill["signed_cash_ex_fees"]) - allocated_cash_total,
                4,
            )
            still_unmatched_rows.append(row)

    return pd.DataFrame(inferred_rows), pd.DataFrame(still_unmatched_rows)


def reconcile_broker_fills(
    *,
    broker_fills_csv: Path,
    order_journal_path: Path,
    output_dir: Path,
    trade_date: str,
) -> dict[str, Any]:
    fills = pd.read_csv(broker_fills_csv)
    if "order_id" not in fills.columns:
        raise ValueError("broker fills CSV must include order_id")
    fills = fills.copy()
    fills["order_id"] = fills["order_id"].map(_normalise_order_id)
    fills["signed_cash_ex_fees"] = fills.apply(_signed_cash, axis=1).round(4)

    identity = _load_order_identity(order_journal_path)
    matched = fills.merge(identity, on="order_id", how="left", indicator=True)
    matched["matched_to_strategy"] = matched["_merge"].eq("both")
    matched = matched.drop(columns=["_merge"])
    for column in IDENTITY_COLUMNS:
        if column not in matched.columns:
            matched[column] = None

    output_dir.mkdir(parents=True, exist_ok=True)
    match_csv = output_dir / f"broker_fill_strategy_matches_{trade_date}.csv"
    strategy_csv = output_dir / f"broker_strategy_cash_summary_{trade_date}.csv"
    inferred_allocations_csv = output_dir / f"broker_fill_inferred_allocations_{trade_date}.csv"
    inferred_strategy_csv = output_dir / f"broker_strategy_cash_summary_inferred_{trade_date}.csv"
    summary_json = output_dir / f"broker_fill_strategy_reconciliation_summary_{trade_date}.json"
    matched.to_csv(match_csv, index=False)

    strategy_frame = matched.loc[matched["matched_to_strategy"]].copy()
    strategy_summary = _strategy_summary(strategy_frame, trade_date)
    strategy_summary.to_csv(strategy_csv, index=False)

    inferred_allocations, still_unmatched = _infer_unmatched_exit_allocations(matched)
    inferred_allocations.to_csv(inferred_allocations_csv, index=False)
    inferred_input = pd.concat([strategy_frame, inferred_allocations], ignore_index=True)
    inferred_strategy_summary = _strategy_summary(inferred_input, trade_date)
    inferred_strategy_summary.to_csv(inferred_strategy_csv, index=False)

    summary = {
        "trade_date": trade_date,
        "broker_fills_csv": str(broker_fills_csv),
        "order_journal_path": str(order_journal_path),
        "broker_fill_count": int(len(matched)),
        "matched_fill_count": int(matched["matched_to_strategy"].sum()),
        "unmatched_fill_count": int((~matched["matched_to_strategy"]).sum()),
        "matched_fill_rate": round(float(matched["matched_to_strategy"].mean()) if len(matched) else 0.0, 6),
        "broker_signed_cash_ex_fees": round(float(matched["signed_cash_ex_fees"].sum()), 4),
        "matched_signed_cash_ex_fees": round(
            float(matched.loc[matched["matched_to_strategy"], "signed_cash_ex_fees"].sum()),
            4,
        ),
        "unmatched_signed_cash_ex_fees": round(
            float(matched.loc[~matched["matched_to_strategy"], "signed_cash_ex_fees"].sum()),
            4,
        ),
        "inferred_allocation_count": int(len(inferred_allocations)),
        "inferred_allocated_signed_cash_ex_fees": round(
            float(inferred_allocations["signed_cash_ex_fees"].sum()) if not inferred_allocations.empty else 0.0,
            4,
        ),
        "still_unmatched_fill_count": int(len(still_unmatched)),
        "still_unmatched_signed_cash_ex_fees": round(
            float(still_unmatched["signed_cash_ex_fees"].sum()) if not still_unmatched.empty else 0.0,
            4,
        ),
        "strategy_summary_count": int(len(strategy_summary)),
        "inferred_strategy_summary_count": int(len(inferred_strategy_summary)),
        "broker_fill_strategy_matches_csv": str(match_csv),
        "broker_strategy_cash_summary_csv": str(strategy_csv),
        "broker_fill_inferred_allocations_csv": str(inferred_allocations_csv),
        "broker_strategy_cash_summary_inferred_csv": str(inferred_strategy_csv),
    }
    _write_json(summary_json, summary)
    summary["broker_fill_strategy_reconciliation_summary_json"] = str(summary_json)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Join broker fill rows to local paper strategy identity.")
    parser.add_argument("--broker-fills-csv", type=Path, required=True)
    parser.add_argument("--order-journal", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--trade-date", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = reconcile_broker_fills(
        broker_fills_csv=args.broker_fills_csv,
        order_journal_path=args.order_journal,
        output_dir=args.output_dir,
        trade_date=str(args.trade_date),
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

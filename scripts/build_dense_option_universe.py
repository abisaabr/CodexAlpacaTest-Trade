from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.data.schemas import SELECTED_OPTION_SCHEMA
from scripts.run_option_aware_research_backtest import _load_parquet_tree, _load_stock_bars

DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "dense_option_universe"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a research-only dense option universe around each symbol/day ATM "
            "reference price. This is the generic form of the high-fill QQQ cleanroom "
            "pattern and does not contact a broker."
        )
    )
    parser.add_argument("--stock-bars-path", required=True)
    parser.add_argument("--option-contracts-root", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--symbol-filter", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--min-dte", type=int, default=0)
    parser.add_argument("--max-dte", type=int, default=7)
    parser.add_argument("--strike-steps", type=int, default=5)
    parser.add_argument(
        "--reference-bar",
        choices=["first", "middle", "last"],
        default="first",
        help="Intraday stock bar used as the daily ATM reference price.",
    )
    return parser.parse_args()


def _symbol_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {item.strip().upper() for item in value.split(",") if item.strip()}
    return symbols or None


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _normalize_contracts(contracts: pd.DataFrame) -> pd.DataFrame:
    if contracts.empty:
        return contracts
    frame = contracts.copy()
    if "option_type" not in frame.columns and "type" in frame.columns:
        frame["option_type"] = frame["type"]
    if "inventory_status" not in frame.columns:
        frame["inventory_status"] = pd.NA
    frame["underlying_symbol"] = frame["underlying_symbol"].astype(str).str.upper()
    frame["option_type"] = frame["option_type"].astype(str).str.lower()
    frame["expiration_date"] = pd.to_datetime(
        frame["expiration_date"], errors="coerce"
    ).dt.normalize()
    frame["strike_price"] = pd.to_numeric(frame["strike_price"], errors="coerce")
    return frame.dropna(
        subset=["symbol", "underlying_symbol", "option_type", "expiration_date", "strike_price"]
    )


def _stock_reference_rows(
    stock_bars: pd.DataFrame,
    *,
    symbol_filter: set[str] | None,
    start_date: date | None,
    end_date: date | None,
    reference_bar: str,
) -> pd.DataFrame:
    if stock_bars.empty:
        return pd.DataFrame()
    frame = stock_bars.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["symbol", "timestamp", "close"])
    frame["trade_date"] = frame["timestamp"].dt.tz_convert("America/New_York").dt.date
    if symbol_filter:
        frame = frame[frame["symbol"].isin(symbol_filter)]
    if start_date:
        frame = frame[frame["trade_date"] >= start_date]
    if end_date:
        frame = frame[frame["trade_date"] <= end_date]
    if frame.empty:
        return pd.DataFrame()
    selected = []
    for (_, _), group in frame.sort_values("timestamp").groupby(["symbol", "trade_date"]):
        if reference_bar == "first":
            row = group.iloc[0]
        elif reference_bar == "last":
            row = group.iloc[-1]
        else:
            row = group.iloc[len(group) // 2]
        selected.append(row)
    return pd.DataFrame(selected).reset_index(drop=True)


def _select_strike_window(
    contracts: pd.DataFrame,
    *,
    reference_price: float,
    strike_steps: int,
) -> pd.DataFrame:
    strikes = sorted(float(value) for value in contracts["strike_price"].dropna().unique())
    if not strikes:
        return contracts.iloc[0:0].copy()
    atm_index = min(
        range(len(strikes)),
        key=lambda index: (abs(strikes[index] - reference_price), strikes[index]),
    )
    lower_index = max(0, atm_index - strike_steps)
    upper_index = min(len(strikes) - 1, atm_index + strike_steps)
    selected_strikes = set(strikes[lower_index : upper_index + 1])
    step_lookup = {
        strike: selected_index - atm_index
        for selected_index, strike in enumerate(strikes)
        if strike in selected_strikes
    }
    selected = contracts[contracts["strike_price"].isin(selected_strikes)].copy()
    selected["atm_strike"] = strikes[atm_index]
    selected["relative_strike_step"] = selected["strike_price"].map(step_lookup).astype("Int64")
    return selected


def select_dense_option_universe(
    *,
    contracts: pd.DataFrame,
    stock_bars: pd.DataFrame,
    symbol_filter: set[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    min_dte: int = 0,
    max_dte: int = 7,
    strike_steps: int = 5,
    reference_bar: str = "first",
) -> pd.DataFrame:
    if min_dte < 0:
        raise ValueError("min_dte must be non-negative.")
    if max_dte < min_dte:
        raise ValueError("max_dte must be greater than or equal to min_dte.")
    if strike_steps < 0:
        raise ValueError("strike_steps must be non-negative.")

    inventory = _normalize_contracts(contracts)
    references = _stock_reference_rows(
        stock_bars,
        symbol_filter=symbol_filter,
        start_date=start_date,
        end_date=end_date,
        reference_bar=reference_bar,
    )
    if inventory.empty or references.empty:
        return SELECTED_OPTION_SCHEMA.apply(pd.DataFrame())

    rows: list[pd.DataFrame] = []
    for reference in references.to_dict("records"):
        symbol = str(reference["symbol"]).upper()
        trade_date = pd.Timestamp(reference["trade_date"])
        reference_time = pd.Timestamp(reference["timestamp"])
        reference_price = float(reference["close"])
        symbol_contracts = inventory[inventory["underlying_symbol"] == symbol].copy()
        if symbol_contracts.empty:
            continue
        symbol_contracts["dte"] = (symbol_contracts["expiration_date"] - trade_date).dt.days
        symbol_contracts = symbol_contracts[
            (symbol_contracts["dte"] >= min_dte) & (symbol_contracts["dte"] <= max_dte)
        ]
        if symbol_contracts.empty:
            continue
        for (expiration_date, option_type), group in symbol_contracts.groupby(
            ["expiration_date", "option_type"], sort=True
        ):
            selected = _select_strike_window(
                group,
                reference_price=reference_price,
                strike_steps=strike_steps,
            )
            if selected.empty:
                continue
            expiration_label = pd.Timestamp(expiration_date).date().isoformat()
            selected["trade_date"] = trade_date
            selected["reference_timestamp"] = reference_time
            selected["reference_price"] = reference_price
            selected["selection_reason"] = (
                f"reference=dense_daily_{reference_bar}_bar;"
                f" reference_time={reference_time.isoformat()};"
                f" expiration_date={expiration_label};"
                f" min_dte={min_dte};"
                f" max_dte={max_dte};"
                f" strike_steps={strike_steps};"
                " research_only=true"
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
        [
            "underlying_symbol",
            "trade_date",
            "expiration_date",
            "option_type",
            "relative_strike_step",
            "symbol",
        ]
    )
    return SELECTED_OPTION_SCHEMA.apply(result.reset_index(drop=True))


def _write_partitioned(root: Path, selected: pd.DataFrame) -> int:
    if selected.empty:
        return 0
    count = 0
    for (underlying, trade_date_value), subset in selected.groupby(
        ["underlying_symbol", "trade_date"], sort=True
    ):
        day = pd.Timestamp(trade_date_value).date().isoformat()
        path = root / f"underlying={underlying}" / f"trade_date={day}" / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        SELECTED_OPTION_SCHEMA.apply(subset).to_parquet(path, index=False)
        count += 1
    return count


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Dense Option Universe Packet",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Status: `{packet['status']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Trading effect: `{packet['trading_effect']}`",
        f"- Selected contract count: `{packet['selected_contract_count']}`",
        f"- Selected partitions: `{packet['selected_contract_partitions']}`",
        f"- Min DTE: `{packet['min_dte']}`",
        f"- Max DTE: `{packet['max_dte']}`",
        f"- Strike steps: `{packet['strike_steps']}`",
        f"- Reference bar: `{packet['reference_bar']}`",
        "",
        "## Symbol Counts",
        "",
    ]
    for row in packet["symbol_counts"]:
        lines.append(
            f"- `{row['symbol']}` contracts `{row['selected_contract_count']}` days `{row['trade_date_count']}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_dense_option_universe_packet(
    *,
    stock_bars_path: Path,
    option_contracts_root: Path,
    output_dir: Path,
    symbol_filter: set[str] | None,
    start_date: date | None,
    end_date: date | None,
    min_dte: int,
    max_dte: int,
    strike_steps: int,
    reference_bar: str,
) -> dict[str, Any]:
    stock_bars = _load_stock_bars(stock_bars_path)
    contracts = _load_parquet_tree(option_contracts_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_root = output_dir / "selected_option_contracts"
    selected = select_dense_option_universe(
        contracts=contracts,
        stock_bars=stock_bars,
        symbol_filter=symbol_filter,
        start_date=start_date,
        end_date=end_date,
        min_dte=min_dte,
        max_dte=max_dte,
        strike_steps=strike_steps,
        reference_bar=reference_bar,
    )
    partition_count = _write_partitioned(selected_root, selected)
    symbol_counts = []
    if not selected.empty:
        for symbol, group in selected.groupby("underlying_symbol", sort=True):
            symbol_counts.append(
                {
                    "symbol": str(symbol),
                    "selected_contract_count": int(len(group)),
                    "trade_date_count": int(group["trade_date"].nunique()),
                    "unique_contract_count": int(group["symbol"].nunique()),
                }
            )
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "dense_option_universe_complete",
        "mode": "research_only_dense_option_universe",
        "broker_facing": False,
        "trading_effect": "none",
        "promotion_allowed": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "stock_bars_path": str(stock_bars_path),
        "option_contracts_root": str(option_contracts_root),
        "output_dir": str(output_dir),
        "selected_contracts_root": str(selected_root),
        "symbol_filter": sorted(symbol_filter) if symbol_filter else [],
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "min_dte": min_dte,
        "max_dte": max_dte,
        "strike_steps": strike_steps,
        "reference_bar": reference_bar,
        "selected_contract_count": int(len(selected)),
        "selected_contract_partitions": partition_count,
        "symbol_counts": symbol_counts,
        "next_step_contract": [
            "Use selected_contracts_root with download_option_market_data_for_selected_contracts.py.",
            "Replay strategy variants against the dense downloaded option data before promotion review.",
            "Require the 0.90 fill gate, positive holdout economics, and governed stress packets before any broker-facing validation.",
        ],
    }
    (output_dir / "dense_option_universe_packet.json").write_text(
        json.dumps(packet, indent=2, default=str), encoding="utf-8"
    )
    _write_markdown(output_dir / "dense_option_universe_packet.md", packet)
    return packet


def main() -> None:
    args = parse_args()
    packet = build_dense_option_universe_packet(
        stock_bars_path=Path(args.stock_bars_path),
        option_contracts_root=Path(args.option_contracts_root),
        output_dir=Path(args.output_dir),
        symbol_filter=_symbol_filter(args.symbol_filter),
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        min_dte=args.min_dte,
        max_dte=args.max_dte,
        strike_steps=args.strike_steps,
        reference_bar=args.reference_bar,
    )
    print(json.dumps(packet, indent=2, default=str))


if __name__ == "__main__":
    main()

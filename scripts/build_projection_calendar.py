from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TRADE_DATE_RE = re.compile(r"trade_date=(\d{4}-\d{2}-\d{2})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a full-period projection calendar from tradable option-bar "
            "trade_date partitions, optionally joined to market-regime labels."
        )
    )
    parser.add_argument("--option-bars-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--symbol", default="")
    parser.add_argument("--regime-labels-csv", default=None)
    parser.add_argument("--regime-date-column", default="trade_date")
    parser.add_argument("--regime-column", default="regime")
    return parser.parse_args()


def _option_trade_dates(option_bars_root: Path) -> list[str]:
    dates: set[str] = set()
    root_text = str(option_bars_root)
    for match in TRADE_DATE_RE.finditer(root_text):
        dates.add(match.group(1))
    if option_bars_root.exists():
        for path in option_bars_root.rglob("*"):
            for match in TRADE_DATE_RE.finditer(str(path)):
                dates.add(match.group(1))
    if not dates:
        raise ValueError(f"No trade_date=YYYY-MM-DD partitions found under {option_bars_root}")
    return sorted(dates)


def _load_regime_labels(
    *,
    regime_labels_csv: Path | None,
    date_column: str,
    regime_column: str,
) -> dict[str, str]:
    if regime_labels_csv is None:
        return {}
    if not regime_labels_csv.exists():
        raise FileNotFoundError(f"Regime labels CSV not found: {regime_labels_csv}")
    labels = pd.read_csv(regime_labels_csv)
    if date_column not in labels.columns:
        raise ValueError(f"Regime labels CSV missing date column {date_column!r}")
    if regime_column not in labels.columns:
        raise ValueError(f"Regime labels CSV missing regime column {regime_column!r}")
    frame = labels[[date_column, regime_column]].copy()
    frame["trade_date"] = pd.to_datetime(frame[date_column], errors="coerce").dt.date.astype(str)
    frame["calendar_regime"] = frame[regime_column].fillna("unknown").astype(str)
    frame = frame.dropna(subset=["trade_date"]).drop_duplicates(subset=["trade_date"])
    return dict(zip(frame["trade_date"], frame["calendar_regime"], strict=False))


def build_projection_calendar(
    *,
    option_bars_root: Path,
    output_dir: Path,
    symbol: str = "",
    regime_labels_csv: Path | None = None,
    regime_date_column: str = "trade_date",
    regime_column: str = "regime",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trade_dates = _option_trade_dates(option_bars_root)
    regimes = _load_regime_labels(
        regime_labels_csv=regime_labels_csv,
        date_column=regime_date_column,
        regime_column=regime_column,
    )
    rows = [
        {
            "trade_date": trade_date,
            "calendar_regime": regimes.get(trade_date, "unknown"),
        }
        for trade_date in trade_dates
    ]
    calendar = pd.DataFrame(rows)
    csv_path = output_dir / "projection_calendar.csv"
    json_path = output_dir / "projection_calendar_packet.json"
    md_path = output_dir / "projection_calendar_packet.md"
    calendar.to_csv(csv_path, index=False)
    regime_counts = dict(sorted(calendar["calendar_regime"].value_counts().to_dict().items()))
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "projection_calendar_complete",
        "mode": "research_only",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "symbol": symbol.upper() if symbol else None,
        "option_bars_root": str(option_bars_root),
        "regime_labels_csv": str(regime_labels_csv) if regime_labels_csv else None,
        "trade_date_count": int(len(calendar)),
        "first_trade_date": str(calendar["trade_date"].iloc[0]) if not calendar.empty else None,
        "last_trade_date": str(calendar["trade_date"].iloc[-1]) if not calendar.empty else None,
        "regime_counts": regime_counts,
        "artifacts": {
            "calendar_csv": str(csv_path),
            "packet_json": str(json_path),
            "packet_md": str(md_path),
        },
    }
    json_path.write_text(json.dumps(packet, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(md_path, packet)
    return packet


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# Projection Calendar",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Symbol: `{packet.get('symbol')}`",
        f"- Trade dates: `{packet['trade_date_count']}`",
        f"- First trade date: `{packet['first_trade_date']}`",
        f"- Last trade date: `{packet['last_trade_date']}`",
        f"- Broker facing: `{str(packet['broker_facing']).lower()}`",
        "",
        "## Regime Counts",
        "",
    ]
    for regime, count in packet["regime_counts"].items():
        lines.append(f"- `{regime}`: `{count}`")
    lines.extend(
        [
            "",
            "Hard rule: this calendar is research-only. It does not authorize trading, paper orders, live-manifest edits, or risk-policy edits.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    packet = build_projection_calendar(
        option_bars_root=Path(args.option_bars_root),
        output_dir=Path(args.output_dir),
        symbol=args.symbol,
        regime_labels_csv=Path(args.regime_labels_csv) if args.regime_labels_csv else None,
        regime_date_column=args.regime_date_column,
        regime_column=args.regime_column,
    )
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

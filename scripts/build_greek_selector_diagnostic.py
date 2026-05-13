from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


CAUSE_DELTA_TARGET_TOO_STRICT = "too_strict_delta_targeting"
CAUSE_DTE_OR_STRIKE_AVAILABILITY = "bad_dte_or_strike_availability"
CAUSE_TOO_FEW_TRADES = "too_few_trades"
CAUSE_BAD_EXITS = "bad_exits"
CAUSE_ENTRY_TIMING = "entry_bar_gap_or_entry_timing_mismatch"
CAUSE_EXIT_TIMING = "exit_bar_gap_or_exit_policy_mismatch"
CAUSE_SELECTED_CONTRACT_GAP = "selected_contract_universe_gap"
CAUSE_GREEK_SNAPSHOT_GAP = "missing_greek_snapshot"
CAUSE_OPTION_PRICE_GAP = "missing_option_price_count"
CAUSE_PASSED = "passed_candidate_level_gates"


def _float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _load_params(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _candidate_summary_paths(input_roots: list[Path]) -> list[Path]:
    paths: list[Path] = []
    for root in input_roots:
        if root.is_file() and root.name.endswith(".csv"):
            paths.append(root)
        elif root.exists():
            paths.extend(root.rglob("option_aware_candidate_summary.csv"))
    return sorted(set(paths))


def _read_rows(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                row["_source_summary_path"] = path.as_posix()
                rows.append(row)
    return rows


def _delta_width(params: dict[str, Any]) -> float | None:
    if "min_abs_delta" not in params or "max_abs_delta" not in params:
        return None
    return abs(_float(params.get("max_abs_delta")) - _float(params.get("min_abs_delta")))


def _is_extreme_delta(params: dict[str, Any]) -> bool:
    target = abs(_float(params.get("target_delta")))
    return target <= 0.30 or target >= 0.75


def _is_narrow_delta_band(params: dict[str, Any]) -> bool:
    width = _delta_width(params)
    return width is not None and width <= 0.22


def _dominant_missing_count(row: dict[str, Any]) -> tuple[str, int]:
    keys = (
        "missing_no_selected_contract",
        "missing_no_entry_bar",
        "missing_no_exit_bar",
        "missing_no_greek_snapshot",
        "missing_option_price_count",
        "missing_too_expensive",
        "missing_unsupported_option_structure",
    )
    counts = {key: _int(row.get(key)) for key in keys}
    key, value = max(counts.items(), key=lambda item: item[1])
    return key, value


def classify_row(
    row: dict[str, Any],
    *,
    fill_coverage_gate: float,
    min_trades: int,
) -> tuple[str, list[str]]:
    params = _load_params(row.get("parameter_set"))
    flags: list[str] = []

    fill_coverage = _float(row.get("fill_coverage"))
    option_trades = _int(row.get("option_trade_count"))
    source_trades = _int(row.get("source_stock_trade_count"))
    net_pnl = _float(row.get("net_pnl"))
    test_net_pnl = _float(row.get("test_net_pnl"))
    entry_coverage = _float(row.get("entry_bar_coverage"))
    exit_coverage = _float(row.get("exit_bar_coverage"))
    selected_missing = _int(row.get("missing_no_selected_contract"))
    greek_missing = _int(row.get("missing_no_greek_snapshot"))
    price_missing = _int(row.get("missing_option_price_count"))
    entry_missing = _int(row.get("missing_no_entry_bar"))
    exit_missing = _int(row.get("missing_no_exit_bar"))
    dte_mode = str(params.get("dte_mode") or "").lower()
    family = str(row.get("family") or "").lower()
    is_multileg = family not in {"single_leg", "single_leg_repair"}

    if selected_missing > 0:
        flags.append(CAUSE_SELECTED_CONTRACT_GAP)
        if dte_mode == "same_day" or is_multileg:
            flags.append(CAUSE_DTE_OR_STRIKE_AVAILABILITY)
        if _is_extreme_delta(params) or _is_narrow_delta_band(params):
            flags.append(CAUSE_DELTA_TARGET_TOO_STRICT)
    if greek_missing > 0:
        flags.append(CAUSE_GREEK_SNAPSHOT_GAP)
    if price_missing > 0:
        flags.append(CAUSE_OPTION_PRICE_GAP)
    if entry_missing > 0 or entry_coverage < fill_coverage_gate:
        flags.append(CAUSE_ENTRY_TIMING)
    if exit_missing > 0 or exit_coverage < fill_coverage_gate:
        flags.append(CAUSE_EXIT_TIMING)
    if source_trades < min_trades or option_trades < min_trades:
        flags.append(CAUSE_TOO_FEW_TRADES)
    if fill_coverage >= fill_coverage_gate and option_trades >= min_trades and (
        net_pnl <= 0 or test_net_pnl <= 0
    ):
        flags.append(CAUSE_BAD_EXITS)

    if not flags and fill_coverage >= fill_coverage_gate and option_trades >= min_trades:
        return CAUSE_PASSED, [CAUSE_PASSED]

    missing_key, missing_value = _dominant_missing_count(row)
    if selected_missing > 0 and missing_key == "missing_no_selected_contract":
        if CAUSE_DTE_OR_STRIKE_AVAILABILITY in flags and (
            dte_mode == "same_day" or is_multileg
        ):
            return CAUSE_DTE_OR_STRIKE_AVAILABILITY, flags
        if CAUSE_DELTA_TARGET_TOO_STRICT in flags:
            return CAUSE_DELTA_TARGET_TOO_STRICT, flags
        return CAUSE_SELECTED_CONTRACT_GAP, flags
    if greek_missing > 0 and missing_key == "missing_no_greek_snapshot":
        return CAUSE_GREEK_SNAPSHOT_GAP, flags
    if price_missing > 0 and missing_key == "missing_option_price_count":
        return CAUSE_OPTION_PRICE_GAP, flags
    if entry_missing > 0 or (entry_coverage < fill_coverage_gate and missing_value > 0):
        return CAUSE_ENTRY_TIMING, flags
    if exit_missing > 0 or (exit_coverage < fill_coverage_gate and missing_value > 0):
        return CAUSE_EXIT_TIMING, flags
    if CAUSE_TOO_FEW_TRADES in flags:
        return CAUSE_TOO_FEW_TRADES, flags
    if CAUSE_BAD_EXITS in flags:
        return CAUSE_BAD_EXITS, flags
    return flags[0] if flags else "unclassified", flags or ["unclassified"]


def _group_key(row: dict[str, Any], params: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(row.get("symbol") or ""),
        str(row.get("intended_regime") or ""),
        str(row.get("family") or ""),
        str(row.get("directional_option_type") or ""),
        str(params.get("dte_mode") or ""),
        str(params.get("timing_profile") or ""),
        str(params.get("option_exit_profile") or ""),
        str(params.get("target_delta") or ""),
    )


def build_diagnostic(
    *,
    input_roots: list[Path],
    output_dir: Path,
    fill_coverage_gate: float = 0.90,
    min_trades: int = 20,
) -> dict[str, Any]:
    paths = _candidate_summary_paths(input_roots)
    raw_rows = _read_rows(paths)
    output_dir.mkdir(parents=True, exist_ok=True)

    diagnostic_rows: list[dict[str, Any]] = []
    cause_counts: Counter[str] = Counter()
    flag_counts: Counter[str] = Counter()
    symbol_counts: Counter[str] = Counter()
    regime_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    dte_counts: Counter[str] = Counter()
    delta_counts: Counter[str] = Counter()
    grouped: dict[tuple[str, ...], dict[str, Any]] = defaultdict(
        lambda: {
            "candidate_count": 0,
            "filled_trade_count": 0,
            "source_trade_count": 0,
            "net_pnl": 0.0,
            "test_net_pnl": 0.0,
            "dominant_causes": Counter(),
            "cause_flags": Counter(),
        }
    )

    for row in raw_rows:
        params = _load_params(row.get("parameter_set"))
        dominant, flags = classify_row(
            row,
            fill_coverage_gate=fill_coverage_gate,
            min_trades=min_trades,
        )
        symbol = str(row.get("symbol") or "UNKNOWN")
        regime = str(row.get("intended_regime") or "unknown")
        family = str(row.get("family") or "unknown")
        dte_mode = str(params.get("dte_mode") or "unknown")
        target_delta = str(params.get("target_delta") or "unknown")
        cause_counts[dominant] += 1
        for flag in flags:
            flag_counts[flag] += 1
        symbol_counts[symbol] += 1
        regime_counts[regime] += 1
        family_counts[family] += 1
        dte_counts[dte_mode] += 1
        delta_counts[target_delta] += 1

        group = grouped[_group_key(row, params)]
        group["candidate_count"] += 1
        group["filled_trade_count"] += _int(row.get("option_trade_count"))
        group["source_trade_count"] += _int(row.get("source_stock_trade_count"))
        group["net_pnl"] += _float(row.get("net_pnl"))
        group["test_net_pnl"] += _float(row.get("test_net_pnl"))
        group["dominant_causes"][dominant] += 1
        for flag in flags:
            group["cause_flags"][flag] += 1

        diagnostic_rows.append(
            {
                "symbol": symbol,
                "intended_regime": regime,
                "family": family,
                "directional_option_type": row.get("directional_option_type"),
                "dte_mode": dte_mode,
                "target_delta": target_delta,
                "min_abs_delta": params.get("min_abs_delta"),
                "max_abs_delta": params.get("max_abs_delta"),
                "delta_band_width": _delta_width(params),
                "timing_profile": params.get("timing_profile"),
                "option_exit_profile": params.get("option_exit_profile"),
                "candidate_variant_id": row.get("candidate_variant_id"),
                "source_strategy_id": row.get("source_strategy_id"),
                "dominant_cause": dominant,
                "cause_flags": ";".join(flags),
                "fill_coverage": row.get("fill_coverage"),
                "option_trade_count": row.get("option_trade_count"),
                "source_stock_trade_count": row.get("source_stock_trade_count"),
                "net_pnl": row.get("net_pnl"),
                "test_net_pnl": row.get("test_net_pnl"),
                "entry_bar_coverage": row.get("entry_bar_coverage"),
                "exit_bar_coverage": row.get("exit_bar_coverage"),
                "data_foundation_coverage": row.get("data_foundation_coverage"),
                "missing_no_selected_contract": row.get("missing_no_selected_contract"),
                "missing_no_greek_snapshot": row.get("missing_no_greek_snapshot"),
                "missing_no_entry_bar": row.get("missing_no_entry_bar"),
                "missing_no_exit_bar": row.get("missing_no_exit_bar"),
                "missing_option_price_count": row.get("missing_option_price_count"),
                "recommendation": row.get("recommendation"),
                "source_summary_path": row.get("_source_summary_path"),
            }
        )

    grouped_rows: list[dict[str, Any]] = []
    for key, stats in grouped.items():
        dominant_causes = stats["dominant_causes"]
        flags = stats["cause_flags"]
        grouped_rows.append(
            {
                "symbol": key[0],
                "intended_regime": key[1],
                "family": key[2],
                "directional_option_type": key[3],
                "dte_mode": key[4],
                "timing_profile": key[5],
                "option_exit_profile": key[6],
                "target_delta": key[7],
                "candidate_count": stats["candidate_count"],
                "filled_trade_count": stats["filled_trade_count"],
                "source_trade_count": stats["source_trade_count"],
                "net_pnl": round(stats["net_pnl"], 6),
                "test_net_pnl": round(stats["test_net_pnl"], 6),
                "top_dominant_cause": dominant_causes.most_common(1)[0][0]
                if dominant_causes
                else "unclassified",
                "dominant_cause_counts": json.dumps(dict(dominant_causes), sort_keys=True),
                "cause_flag_counts": json.dumps(dict(flags), sort_keys=True),
            }
        )
    grouped_rows.sort(
        key=lambda row: (
            row["top_dominant_cause"],
            row["symbol"],
            row["intended_regime"],
            row["family"],
            row["dte_mode"],
            row["target_delta"],
        )
    )
    diagnostic_rows.sort(
        key=lambda row: (
            row["dominant_cause"],
            row["symbol"],
            row["intended_regime"],
            row["family"],
            str(row["target_delta"]),
            str(row["candidate_variant_id"]),
        )
    )

    summary = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "status": "greek_selector_diagnostic_complete",
        "research_only": True,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "input_roots": [path.as_posix() for path in input_roots],
        "candidate_summary_file_count": len(paths),
        "candidate_count": len(raw_rows),
        "fill_coverage_gate": fill_coverage_gate,
        "min_trades": min_trades,
        "dominant_cause_counts": dict(cause_counts.most_common()),
        "cause_flag_counts": dict(flag_counts.most_common()),
        "symbol_counts": dict(symbol_counts.most_common()),
        "regime_counts": dict(regime_counts.most_common()),
        "family_counts": dict(family_counts.most_common()),
        "dte_mode_counts": dict(dte_counts.most_common()),
        "target_delta_counts": dict(delta_counts.most_common()),
        "interpretation": [
            "Dominant cause is a triage label, not proof that other causes are absent.",
            "Cause flags preserve overlapping evidence such as same-day DTE gaps plus narrow delta bands.",
            "Microstructure results remain discovery-only unless a separate governed packet is generated.",
        ],
    }

    summary_path = output_dir / "greek_selector_diagnostic_summary.json"
    rows_path = output_dir / "greek_selector_diagnostic_rows.csv"
    groups_path = output_dir / "greek_selector_diagnostic_groups.csv"
    markdown_path = output_dir / "greek_selector_diagnostic.md"

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _write_csv(rows_path, diagnostic_rows)
    _write_csv(groups_path, grouped_rows)
    _write_markdown(markdown_path, summary)
    return summary


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Greek Selector Diagnostic",
        "",
        f"- Generated UTC: `{summary['generated_at_utc']}`",
        f"- Candidate summary files: `{summary['candidate_summary_file_count']}`",
        f"- Candidate rows: `{summary['candidate_count']}`",
        f"- Fill coverage gate: `{summary['fill_coverage_gate']}`",
        f"- Minimum trades: `{summary['min_trades']}`",
        "- Broker-facing effect: `none`",
        "- Live manifest effect: `none`",
        "",
        "## Dominant Causes",
        "",
    ]
    for cause, count in summary["dominant_cause_counts"].items():
        lines.append(f"- `{cause}`: {count}")
    lines.extend(["", "## Cause Flags", ""])
    for cause, count in summary["cause_flag_counts"].items():
        lines.append(f"- `{cause}`: {count}")
    lines.extend(["", "## Notes", ""])
    for item in summary["interpretation"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify Greek replay failures by selector, DTE/strike, trade-count, and exit causes."
    )
    parser.add_argument(
        "--input-root",
        action="append",
        required=True,
        help="Greek report root or option_aware_candidate_summary.csv path. Repeatable.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--fill-coverage-gate", type=float, default=0.90)
    parser.add_argument("--min-trades", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_diagnostic(
        input_roots=[Path(value) for value in args.input_root],
        output_dir=Path(args.output_dir),
        fill_coverage_gate=args.fill_coverage_gate,
        min_trades=args.min_trades,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

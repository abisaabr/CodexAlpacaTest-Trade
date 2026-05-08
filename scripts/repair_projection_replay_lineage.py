from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit and optionally repair projection replay lineage for a portfolio "
            "capital plan. This is research-only and only copies trade economics CSVs."
        )
    )
    parser.add_argument("--portfolio-report-json", required=True)
    parser.add_argument("--replay-root", action="append", default=[])
    parser.add_argument("--search-root", action="append", default=[])
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--repaired-replay-root",
        default=None,
        help="Optional output root to receive copied missing option_aware_trade_economics.csv files.",
    )
    parser.add_argument(
        "--copy-existing-replay-roots",
        action="store_true",
        help="Also copy existing replay-root CSVs into --repaired-replay-root.",
    )
    return parser.parse_args()


@dataclass(frozen=True)
class PlanRow:
    candidate_variant_id: str
    base_candidate_variant_id: str
    aggregate_profile: str
    symbol: str
    family: str
    intended_regime: str

    @property
    def key(self) -> tuple[str, str]:
        return self.base_candidate_variant_id, self.aggregate_profile


def _load_plan(path: Path) -> list[PlanRow]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for row in payload.get("capital_plan") or []:
        if not isinstance(row, dict):
            continue
        candidate_id = str(row.get("candidate_variant_id") or "")
        base_id = str(row.get("base_candidate_variant_id") or candidate_id).split("__profile_", 1)[0]
        profile = str(row.get("aggregate_profile") or "")
        if not base_id or not profile:
            continue
        rows.append(
            PlanRow(
                candidate_variant_id=candidate_id,
                base_candidate_variant_id=base_id,
                aggregate_profile=profile,
                symbol=str(row.get("symbol") or "").upper(),
                family=str(row.get("family") or ""),
                intended_regime=str(row.get("intended_regime") or ""),
            )
        )
    return rows


def _profile_name(path: Path) -> str:
    return path.parent.name


def _csv_candidate_ids(path: Path) -> set[str]:
    try:
        frame = pd.read_csv(path, usecols=["candidate_variant_id"], low_memory=False)
    except (ValueError, pd.errors.EmptyDataError):
        return set()
    return {str(value) for value in frame["candidate_variant_id"].dropna().unique()}


def _scan_roots(roots: list[Path]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("option_aware_trade_economics.csv"):
            profile = _profile_name(path)
            try:
                row_count = sum(1 for _ in path.open("r", encoding="utf-8")) - 1
            except UnicodeDecodeError:
                row_count = 0
            for candidate_id in _csv_candidate_ids(path):
                index.setdefault((candidate_id, profile), []).append(
                    {
                        "path": str(path),
                        "profile": profile,
                        "candidate_variant_id": candidate_id,
                        "row_count": max(row_count, 0),
                    }
                )
    return index


def _best_source(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not items:
        return None
    return sorted(items, key=lambda item: int(item.get("row_count") or 0), reverse=True)[0]


def _safe_profile_dir_name(profile: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in profile)
    return safe or "profile"


def _copy_source(source: dict[str, Any], repaired_root: Path) -> Path:
    target_dir = repaired_root / _safe_profile_dir_name(str(source["profile"]))
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "option_aware_trade_economics.csv"
    if target.exists():
        try:
            existing = pd.read_csv(target, low_memory=False)
            incoming = pd.read_csv(source["path"], low_memory=False)
            pd.concat([existing, incoming], ignore_index=True).drop_duplicates().to_csv(
                target,
                index=False,
            )
        except (pd.errors.EmptyDataError, UnicodeDecodeError):
            shutil.copy2(source["path"], target)
    else:
        shutil.copy2(source["path"], target)
    return target


def build_lineage_repair(
    *,
    portfolio_report_json: Path,
    replay_roots: list[Path],
    search_roots: list[Path],
    output_dir: Path,
    repaired_replay_root: Path | None = None,
    copy_existing_replay_roots: bool = False,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    plan = _load_plan(portfolio_report_json)
    current_index = _scan_roots(replay_roots)
    search_index = _scan_roots(search_roots)
    matched = []
    repaired = []
    unmatched = []
    for row in plan:
        current = _best_source(current_index.get(row.key, []))
        if current:
            matched.append({**row.__dict__, "source_path": current["path"], "source_status": "current"})
            if repaired_replay_root and copy_existing_replay_roots:
                copied = _copy_source(current, repaired_replay_root)
                repaired.append({**row.__dict__, "source_path": current["path"], "copied_to": str(copied), "source_status": "current_copied"})
            continue
        source = _best_source(search_index.get(row.key, []))
        if source:
            copied_to = ""
            if repaired_replay_root:
                copied_to = str(_copy_source(source, repaired_replay_root))
            repaired.append({**row.__dict__, "source_path": source["path"], "copied_to": copied_to, "source_status": "repaired_from_search"})
            continue
        unmatched.append({**row.__dict__, "source_status": "unmatched"})

    pd.DataFrame(matched).to_csv(output_dir / "matched_current_lineage.csv", index=False)
    pd.DataFrame(repaired).to_csv(output_dir / "repaired_lineage_sources.csv", index=False)
    pd.DataFrame(unmatched).to_csv(output_dir / "unmatched_lineage.csv", index=False)
    unmatched_by_symbol = (
        pd.Series([str(row.get("symbol") or "UNKNOWN") for row in unmatched])
        .value_counts()
        .to_dict()
        if unmatched
        else {}
    )
    summary = {
        "status": "lineage_repair_complete",
        "portfolio_report_json": str(portfolio_report_json),
        "replay_roots": [str(path) for path in replay_roots],
        "search_roots": [str(path) for path in search_roots],
        "repaired_replay_root": str(repaired_replay_root) if repaired_replay_root else None,
        "capital_plan_count": len(plan),
        "matched_current_count": len(matched),
        "repaired_from_search_count": len(
            [row for row in repaired if row.get("source_status") == "repaired_from_search"]
        ),
        "unmatched_count": len(unmatched),
        "unmatched_by_symbol": {str(key): int(value) for key, value in unmatched_by_symbol.items()},
        "outputs": {
            "matched_current_lineage_csv": str(output_dir / "matched_current_lineage.csv"),
            "repaired_lineage_sources_csv": str(output_dir / "repaired_lineage_sources.csv"),
            "unmatched_lineage_csv": str(output_dir / "unmatched_lineage.csv"),
        },
    }
    (output_dir / "lineage_repair_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def main() -> None:
    args = parse_args()
    summary = build_lineage_repair(
        portfolio_report_json=Path(args.portfolio_report_json),
        replay_roots=[Path(path) for path in args.replay_root],
        search_roots=[Path(path) for path in args.search_root],
        output_dir=Path(args.output_dir),
        repaired_replay_root=Path(args.repaired_replay_root)
        if args.repaired_replay_root
        else None,
        copy_existing_replay_roots=args.copy_existing_replay_roots,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_iwm_regime_rescue_research_inputs import (  # noqa: E402
    _csv_ints,
    _csv_set,
    build_iwm_regime_rescue_rows,
)
from scripts.build_qqq_regime_research_inputs import build_queue  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a symbol-generic bear/choppy regime-rescue queue using the "
            "current liquidity-first strategy templates. This is research-only "
            "input generation; it does not start trading or modify live manifests."
        )
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--wave-id", required=True)
    parser.add_argument("--target-regimes", default="bear,choppy")
    parser.add_argument("--output-prefix", default="")
    parser.add_argument(
        "--choppy-families",
        default="",
        help="Optional comma-separated choppy family filter for bounded rescue waves.",
    )
    parser.add_argument(
        "--choppy-signal-delay-bars",
        default="0",
        help="Comma-separated completed-stock-bar signal delays for choppy rows.",
    )
    parser.add_argument(
        "--choppy-profile-set",
        choices=(
            "rescue",
            "timewindow_refine",
            "timewindow_micro_exit",
            "timewindow_quality_filter",
        ),
        default="rescue",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    symbol = str(args.symbol).upper()
    symbol_slug = symbol.lower()
    output_prefix = args.output_prefix.strip() or f"{symbol_slug}_regime_rescue"
    target_regimes = _csv_set(args.target_regimes)
    choppy_families = _csv_set(args.choppy_families)

    rows = build_iwm_regime_rescue_rows(
        symbol=symbol,
        wave_id=args.wave_id,
        target_regimes=target_regimes,
        choppy_signal_delay_bars=_csv_ints(args.choppy_signal_delay_bars),
        choppy_families=choppy_families or None,
        choppy_profile_set=args.choppy_profile_set,
    )
    queue = build_queue(rows=rows, wave_id=args.wave_id)
    manifest = {
        "broker_facing": False,
        "builder": "build_regime_rescue_research_inputs.py",
        "choppy_families": sorted(choppy_families),
        "choppy_profile_set": args.choppy_profile_set,
        "execution_effect": "none",
        "live_manifest_effect": "none",
        "output_prefix": output_prefix,
        "promotion_allowed": False,
        "risk_policy_effect": "none",
        "status": "ready_for_symbol_regime_rescue_backtest",
        "target_regimes": sorted(target_regimes),
        "target_symbols": sorted({row["symbol"] for row in rows}),
        "template_count": len(rows),
        "wave_id": args.wave_id,
    }

    variants_path = output_dir / f"{output_prefix}_variants.jsonl"
    queue_path = output_dir / f"{output_prefix}_option_queue.json"
    manifest_path = output_dir / f"{output_prefix}_manifest.json"
    variants_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    queue_path.write_text(json.dumps(queue, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

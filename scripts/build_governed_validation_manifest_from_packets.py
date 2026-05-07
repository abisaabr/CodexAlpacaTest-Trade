from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

try:
    from _bootstrap import bootstrap_repo_root
except ModuleNotFoundError:  # pragma: no cover
    from scripts._bootstrap import bootstrap_repo_root

bootstrap_repo_root()


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _candidate_hash(candidate: dict[str, Any]) -> str:
    base_id = str(candidate.get("base_candidate_variant_id") or candidate.get("candidate_variant_id") or "")
    parts = [part for part in base_id.split("__") if part]
    return parts[-1] if parts else "unknown"


def _slug(value: str, *, max_len: int = 48) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return (slug or "unknown")[:max_len].strip("_")


def _load_parameter_set(candidate: dict[str, Any]) -> dict[str, Any]:
    raw = candidate.get("parameter_set")
    if isinstance(raw, dict):
        return raw
    if raw in (None, ""):
        return {}
    try:
        decoded = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_default(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _strategy_name(candidate: dict[str, Any], params: dict[str, Any], generated_for: str) -> str:
    symbol = _slug(str(candidate.get("symbol") or "unknown"), max_len=8)
    regime = _slug(str(candidate.get("intended_regime") or "unknown"), max_len=12)
    option_type = _slug(str(candidate.get("directional_option_type") or "option"), max_len=8)
    family = _slug(str(candidate.get("family") or "strategy"), max_len=24)
    timing_profile = _slug(str(params.get("timing_profile") or candidate.get("aggregate_profile") or "profile"), max_len=24)
    profile = _slug(str(candidate.get("aggregate_profile") or ""), max_len=18)
    return "__".join(
        [
            symbol,
            "governed",
            regime,
            option_type,
            family,
            timing_profile,
            _candidate_hash(candidate)[:14],
            profile,
            generated_for,
        ]
    )


def _signal_name(regime: str, option_type: str, params: dict[str, Any]) -> str:
    stock_proxy_mode = str(params.get("stock_proxy_mode") or "")
    if regime == "choppy" or stock_proxy_mode == "range_bound":
        if option_type != "call":
            raise ValueError(
                "runtime currently supports governed choppy lower-band reversion for calls only"
            )
        return "governed_lower_band_reversion_call"
    if option_type == "put":
        return "governed_breakout_put"
    return "governed_breakout_call"


def _family_label(option_type: str, raw_family: str) -> str:
    if raw_family == "single_leg_repair":
        return "Single-leg long put" if option_type == "put" else "Single-leg long call"
    return raw_family


def _is_runtime_supported_family(raw_family: str) -> bool:
    normalized = _slug(raw_family, max_len=64)
    return normalized in {
        "single_leg",
        "single_leg_repair",
        "debit_call_vertical",
        "debit_put_vertical",
        "bull_put_credit_spread",
        "bear_call_credit_spread",
        "broken_wing_call_butterfly",
        "broken_wing_put_butterfly",
    }


def _runner_semantics_status(raw_family: str) -> str:
    normalized = _slug(raw_family, max_len=64)
    if normalized in {"single_leg", "single_leg_repair"}:
        return "packet_translated_to_runtime_single_leg"
    return "packet_translated_to_runtime_native_multileg"


def _clamp_abs_delta(value: float) -> float:
    return max(0.05, min(0.95, abs(value)))


def _leg(option_type: str, side: str, target_delta: float) -> dict[str, Any]:
    abs_delta = _clamp_abs_delta(target_delta)
    return {
        "option_type": option_type,
        "side": side,
        "target_delta": target_delta,
        "min_abs_delta": max(0.05, abs_delta - 0.20),
        "max_abs_delta": min(0.95, abs_delta + 0.20),
    }


def _strategy_legs(option_type: str, raw_family: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = _slug(raw_family, max_len=64)
    target = _float_or_none(params.get("target_delta"))
    if target is None:
        target = -0.55 if option_type == "put" else 0.55
    direction = -1.0 if target < 0 else 1.0
    abs_target = _clamp_abs_delta(target)

    if normalized in {"single_leg", "single_leg_repair"}:
        return [_leg(option_type, "long", direction * abs_target)]
    if normalized == "debit_call_vertical":
        return [
            _leg("call", "long", abs_target),
            _leg("call", "short", max(0.05, abs_target - 0.18)),
        ]
    if normalized == "debit_put_vertical":
        return [
            _leg("put", "long", -abs_target),
            _leg("put", "short", -max(0.05, abs_target - 0.18)),
        ]
    if normalized == "bull_put_credit_spread":
        short_abs = min(0.95, max(0.20, abs_target))
        return [
            _leg("put", "short", -short_abs),
            _leg("put", "long", -max(0.05, short_abs - 0.18)),
        ]
    if normalized == "bear_call_credit_spread":
        short_abs = min(0.95, max(0.20, abs_target))
        return [
            _leg("call", "short", short_abs),
            _leg("call", "long", max(0.05, short_abs - 0.18)),
        ]
    if normalized == "broken_wing_call_butterfly":
        center = min(0.90, max(0.30, abs_target))
        return [
            _leg("call", "long", min(0.95, center + 0.12)),
            _leg("call", "short", center),
            _leg("call", "short", max(0.05, center - 0.08)),
            _leg("call", "long", max(0.05, center - 0.24)),
        ]
    if normalized == "broken_wing_put_butterfly":
        center = min(0.90, max(0.30, abs_target))
        return [
            _leg("put", "long", -min(0.95, center + 0.12)),
            _leg("put", "short", -center),
            _leg("put", "short", -max(0.05, center - 0.08)),
            _leg("put", "long", -max(0.05, center - 0.24)),
        ]
    raise ValueError(f"unsupported_runtime_family_for_paper_manifest:{raw_family or 'unknown'}")


def _risk_fraction(symbol: str, regime: str, option_type: str) -> float:
    del option_type
    index_symbols = {"QQQ", "SPY", "IWM"}
    if symbol in index_symbols:
        return {"bull": 0.025, "bear": 0.02, "choppy": 0.025}.get(regime, 0.02)
    return {"bull": 0.02, "bear": 0.018, "choppy": 0.018}.get(regime, 0.018)


def _max_contracts(symbol: str) -> int:
    if symbol in {"QQQ", "SPY"}:
        return 4
    if symbol in {"AMZN", "NVDA", "TSLA"}:
        return 2
    return 3


def _liquidity_gate(value: Any) -> str:
    normalized = str(value or "tight").strip().lower()
    if normalized in {"tight", "loose"}:
        return normalized
    return "loose"


def _build_strategy(
    candidate: dict[str, Any],
    *,
    packet_path: Path,
    packet_uri: str | None,
    output_manifest_path: Path,
    generated_for: str,
) -> dict[str, Any]:
    params = _load_parameter_set(candidate)
    symbol = str(candidate.get("symbol") or "").upper()
    regime = str(candidate.get("intended_regime") or "").lower()
    option_type = str(candidate.get("directional_option_type") or "").lower()
    raw_family = str(candidate.get("family") or "")
    if not symbol or regime not in {"bull", "bear", "choppy"} or option_type not in {"call", "put"}:
        raise ValueError(f"unsupported candidate identity: {candidate.get('candidate_variant_id')}")
    if not _is_runtime_supported_family(raw_family):
        raise ValueError(
            f"unsupported_runtime_family_for_paper_manifest:{raw_family or 'unknown'}"
        )
    signal_name = _signal_name(regime, option_type, params)
    profit_target = _float_or_none(params.get("profit_target_multiple")) or 0.35
    stop_loss = _float_or_none(params.get("stop_loss_multiple")) or 0.18
    strategy: dict[str, Any] = {
        "name": _strategy_name(candidate, params, generated_for),
        "underlying_symbol": symbol,
        "regime": regime,
        "family": _family_label(option_type, raw_family),
        "description": (
            f"{symbol} governed {regime} {option_type} candidate from generated promotion-review "
            f"packet; strategy_fill_coverage gate cleared and source identity preserved."
        ),
        "dte_mode": str(params.get("dte_mode") or "next_expiry"),
        "signal_name": signal_name,
        "timing_profile": "governed_late",
        "hard_exit_minute": _int_or_default(params.get("hard_exit_minute"), 90),
        "runner_hard_exit_mode": "minutes_after_entry",
        "risk_fraction": _risk_fraction(symbol, regime, option_type),
        "max_contracts": _max_contracts(symbol),
        "profit_target_multiple": profit_target,
        "stop_loss_multiple": stop_loss,
        "candidate_variant_id": candidate.get("candidate_variant_id"),
        "source_strategy_id": candidate.get("source_strategy_id") or candidate.get("strategy_id"),
        "promotion_manifest_path": str(output_manifest_path.as_posix()),
        "governed_validation_packet_uri": packet_uri or str(packet_path.as_posix()),
        "promotion_status": candidate.get("promotion_status"),
        "min_fill_coverage": _float_or_none(candidate.get("min_fill_coverage")),
        "min_data_foundation_coverage": _float_or_none(candidate.get("min_data_foundation_coverage")),
        "min_option_trade_count": _int_or_default(candidate.get("min_option_trade_count"), 0),
        "min_net_pnl": _float_or_none(candidate.get("min_net_pnl")),
        "min_test_net_pnl": _float_or_none(candidate.get("min_test_net_pnl")),
        "research_profile": candidate.get("aggregate_profile"),
        "research_entry_timing_mode": params.get("entry_signal_mode"),
        "runner_semantics_status": _runner_semantics_status(raw_family),
        "stock_proxy_mode": params.get("stock_proxy_mode") or ("range_bound" if regime == "choppy" else "breakout"),
        "entry_signal_mode": params.get("entry_signal_mode") or ("rising_edge" if regime != "bull" else "daily_first"),
        "min_minutes_since_open": _int_or_default(params.get("min_minutes_since_open"), 15),
        "max_minutes_since_open": _int_or_default(params.get("max_minutes_since_open"), 270),
        "liquidity_gate": _liquidity_gate(params.get("liquidity_gate")),
        "option_exit_mode": "premium_target_stop",
        "option_exit_profile": f"target{int(round(profit_target * 100))}_stop{int(round(stop_loss * 100))}",
        "option_profit_target_pct": profit_target,
        "option_stop_loss_pct": stop_loss,
        "min_option_hold_minutes": _int_or_default(params.get("min_option_hold_minutes"), 3),
        "legs": _strategy_legs(option_type, raw_family, params),
    }
    optional_float_fields = (
        "min_trend_gap_pct",
        "max_trend_gap_pct",
        "min_range_pct",
        "max_range_pct",
        "max_midpoint_distance_pct",
        "range_edge_pct",
    )
    for field in optional_float_fields:
        value = _float_or_none(params.get(field))
        if value is not None:
            strategy[field] = value
    if params.get("range_entry_side"):
        strategy["range_entry_side"] = params["range_entry_side"]
    return strategy


def build_manifest(
    *,
    packet_paths: list[Path],
    output_path: Path,
    generated_for: str,
    packet_uris: list[str],
) -> dict[str, Any]:
    strategies: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    packet_sources: list[dict[str, Any]] = []
    for index, packet_path in enumerate(packet_paths):
        packet = _load_json(packet_path)
        packet_uri = packet_uris[index] if index < len(packet_uris) else None
        gate_summary = packet.get("gate_summary") or {}
        packet_sources.append(
            {
                "path": str(packet_path.as_posix()),
                "gcs_uri": packet_uri,
                "decision": packet.get("decision"),
                "candidate_level_decision": packet.get("candidate_level_decision"),
                "eligible_for_promotion_review_count": gate_summary.get(
                    "eligible_for_promotion_review_count"
                ),
                "eligible_regimes": gate_summary.get("eligible_regimes"),
                "missing_eligible_regimes": gate_summary.get("missing_eligible_regimes"),
            }
        )
        candidate_rows: list[dict[str, Any]] = []
        seen_candidate_ids: set[str] = set()
        for section_name in ("eligible_regime_representatives", "review_candidates", "capital_plan"):
            for candidate in packet.get(section_name) or []:
                if not isinstance(candidate, dict):
                    continue
                candidate_id = str(candidate.get("candidate_variant_id") or "")
                if not candidate_id or candidate_id in seen_candidate_ids:
                    continue
                seen_candidate_ids.add(candidate_id)
                candidate_rows.append(candidate)
        for candidate in candidate_rows:
            if candidate.get("promotion_status") != "eligible_for_promotion_review":
                continue
            try:
                strategies.append(
                    _build_strategy(
                        candidate,
                        packet_path=packet_path,
                        packet_uri=packet_uri,
                        output_manifest_path=output_path,
                        generated_for=generated_for,
                    )
                )
            except ValueError as exc:
                skipped.append(
                    {
                        "candidate_variant_id": str(candidate.get("candidate_variant_id")),
                        "reason": str(exc),
                    }
                )
    manifest = {
        "version": 1,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "scope": "research_governed_validation_review_only",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "source_packets": packet_sources,
        "strategy_count": len(strategies),
        "symbols": sorted({str(strategy["underlying_symbol"]) for strategy in strategies}),
        "regimes": sorted({str(strategy["regime"]) for strategy in strategies}),
        "skipped_candidates": skipped,
        "strategies": strategies,
        "hard_rules": [
            "This manifest does not modify the live strategy manifest.",
            "This manifest does not lower promotion gates.",
            "This manifest does not start trading by itself.",
            (
                "Broker-facing paper order submission requires explicit runner arming: either "
                "--submit-paper-orders or a config with paper_order_arming_mode=config_explicit."
            ),
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(manifest, sort_keys=False, width=120), encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a StrategyConfig-compatible governed-validation manifest from promotion packets."
    )
    parser.add_argument("--packet", action="append", required=True, help="Promotion packet JSON path.")
    parser.add_argument(
        "--packet-uri",
        action="append",
        default=[],
        help="Optional GCS URI corresponding to each --packet, repeated in the same order.",
    )
    parser.add_argument("--output", required=True, help="Output YAML manifest path.")
    parser.add_argument("--generated-for", default="20260506", help="Suffix used in generated strategy names.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    packet_paths = [Path(path) for path in args.packet]
    output_path = Path(args.output)
    manifest = build_manifest(
        packet_paths=packet_paths,
        output_path=output_path,
        generated_for=args.generated_for,
        packet_uris=list(args.packet_uri),
    )
    print(
        json.dumps(
            {
                "output": str(output_path),
                "strategy_count": manifest["strategy_count"],
                "symbols": manifest["symbols"],
                "regimes": manifest["regimes"],
                "skipped_candidate_count": len(manifest["skipped_candidates"]),
            },
            indent=2,
        )
    )
    if manifest["skipped_candidates"]:
        print(json.dumps(manifest["skipped_candidates"], indent=2), file=sys.stderr)


if __name__ == "__main__":
    main()

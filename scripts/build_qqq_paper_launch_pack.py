from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter, OrderLeg
from alpaca_lab.config import LabSettings
from alpaca_lab.multi_ticker_portfolio.config import StrategyConfig

DEFAULT_PROMOTION_MANIFEST = (
    REPO_ROOT / "config" / "promotion_manifests" / "qqq_regime_complete_governed_validation_20260505.yaml"
)
DEFAULT_PORTFOLIO_CONFIG = REPO_ROOT / "config" / "qqq_regime_complete_paper_portfolio.yaml"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "gcp_research" / "qqq_regime_complete_paper_launch_pack_20260505"

AUDIT_IDENTITY_FIELDS = (
    "candidate_variant_id",
    "source_strategy_id",
    "promotion_manifest_path",
    "governed_validation_packet_uri",
    "research_profile",
)


def _run_git(*args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = (result.stdout or "").strip()
    return output or None


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML mapping at {path}")
    return payload


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _write_markdown(path: Path, packet: dict[str, Any]) -> Path:
    checks = packet["checks"]
    blockers = packet["broker_facing_blockers"]
    strategies = packet["strategies"]
    lines = [
        "# QQQ Option-Native Paper Launch Pack",
        "",
        f"- Generated UTC: `{packet['generated_at_utc']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Broker-facing paper ready: `{str(packet['ready_for_broker_facing_paper']).lower()}`",
        f"- Broker-free shadow ready: `{str(packet['ready_for_broker_free_shadow_validation']).lower()}`",
        f"- Promotion manifest: `{packet['promotion_manifest_path']}`",
        f"- Runner branch: `{packet['runner_git']['branch']}`",
        f"- Runner commit: `{packet['runner_git']['commit']}`",
        "",
        "## Strategies",
        "",
        "| Regime | Candidate | Source | Family | Fill Coverage | Test PnL | Status |",
        "| --- | --- | --- | --- | ---: | ---: | --- |",
    ]
    for strategy in strategies:
        lines.append(
            "| {regime} | `{candidate}` | `{source}` | `{family}` | `{fill}` | `{test_pnl}` | `{status}` |".format(
                regime=strategy.get("intended_regime") or strategy.get("regime"),
                candidate=strategy.get("candidate_variant_id"),
                source=strategy.get("source_strategy_id"),
                family=strategy.get("family"),
                fill=strategy.get("min_fill_coverage"),
                test_pnl=strategy.get("min_test_net_pnl"),
                status=strategy.get("promotion_status"),
            )
        )
    lines.extend(
        [
            "",
            "## Checks",
            "",
        ]
    )
    for check in checks:
        lines.append(f"- `{check['name']}`: `{check['status']}` - {check['detail']}")
    lines.extend(
        [
            "",
            "## Broker-Facing Blockers",
            "",
        ]
    )
    for blocker in blockers:
        lines.append(f"- {blocker}")
    lines.extend(
        [
            "",
            "## Operator-Gated Commands",
            "",
            "The launch-pack builder is broker-free. The startup-preflight command may read the paper broker account, but it submits no orders because `--no-submit-paper-orders` is set.",
            "",
            "```powershell",
            "python scripts\\build_qqq_paper_launch_pack.py",
            "python scripts\\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\\qqq_regime_complete_paper_portfolio.yaml --startup-preflight --no-submit-paper-orders",
            "```",
            "",
            "Hard rule: this packet does not start trading, does not modify the live paper-runner manifest, and does not change risk policy.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _sample_option_symbol(strategy: dict[str, Any]) -> str:
    option_type = str(strategy.get("directional_option_type") or "").lower()
    if option_type not in {"call", "put"}:
        for leg in strategy.get("legs") or []:
            if str(leg.get("side")) == "long":
                option_type = str(leg.get("option_type") or "").lower()
                break
    suffix = "C00425000" if option_type == "call" else "P00425000"
    return f"QQQ260501{suffix}"


def _adapter_safety_checks(strategies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    broker = AlpacaBrokerAdapter(
        LabSettings(
            default_underlyings=("QQQ",),
            alpaca_api_key="paper-key",
            alpaca_secret_key="paper-secret",
        )
    )
    results: list[dict[str, Any]] = []
    for strategy in strategies:
        legs = strategy.get("legs") or []
        if len(legs) <= 1:
            leg = legs[0] if legs else {"side": "long"}
            request = broker.build_order_request(
                symbol=_sample_option_symbol(strategy),
                side="buy" if str(leg.get("side")) == "long" else "sell",
                strategy_name=str(strategy.get("name") or strategy.get("source_strategy_id")),
                asset_class="option",
                qty=1,
                order_type="limit",
                limit_price=2.50,
                extra={"position_intent": "buy_to_open" if str(leg.get("side")) == "long" else "sell_to_open"},
            )
        else:
            request = broker.build_multileg_order_request(
                strategy_name=str(strategy.get("name") or strategy.get("source_strategy_id")),
                qty=1,
                limit_price=-0.80,
                legs=[
                    OrderLeg(
                        _sample_option_symbol({"directional_option_type": leg.get("option_type")}),
                        "buy" if str(leg.get("side")) == "long" else "sell",
                        position_intent="buy_to_open"
                        if str(leg.get("side")) == "long"
                        else "sell_to_open",
                    )
                    for leg in legs
                ],
            )
        preview = broker.submit_order(request, dry_run=True)
        results.append(
            {
                "strategy_name": request.strategy_name,
                "status": preview.get("status"),
                "order_class": preview.get("payload", {}).get("order_class", "simple"),
                "leg_count": len(preview.get("payload", {}).get("legs", [])),
            }
        )
    return results


def build_launch_pack(*, promotion_manifest_path: Path, output_dir: Path) -> dict[str, Any]:
    manifest = _load_yaml(promotion_manifest_path)
    strategies = manifest.get("strategies", [])
    if not isinstance(strategies, list):
        raise ValueError("Promotion manifest strategies must be a list.")

    strategy_fields = set(StrategyConfig.model_fields)
    missing_audit_fields = [field for field in AUDIT_IDENTITY_FIELDS if field not in strategy_fields]
    schema_required = any("name" in strategy for strategy in strategies)
    validated_strategy_count = 0
    validation_errors: list[str] = []
    if schema_required:
        for index, strategy in enumerate(strategies, start=1):
            try:
                StrategyConfig.model_validate(strategy)
                validated_strategy_count += 1
            except Exception as exc:  # noqa: BLE001 - launch pack should report all schema blockers.
                validation_errors.append(f"strategy[{index}]: {exc}")
    adapter_checks = _adapter_safety_checks(strategies)
    eligible_count = sum(
        1
        for strategy in strategies
        if str(strategy.get("promotion_status")) == "eligible_for_promotion_review"
    )
    runner_aligned_count = sum(
        1
        for strategy in strategies
        if str(strategy.get("runner_semantics_status")) == "runner_aligned"
    )
    explicit_runner_semantics = any("runner_semantics_status" in strategy for strategy in strategies)
    checks = [
        {
            "name": "promotion_manifest_scope",
            "status": "passed"
            if manifest.get("broker_facing") is False
            and manifest.get("live_manifest_effect") == "none"
            and manifest.get("risk_policy_effect") == "none"
            else "failed",
            "detail": "Manifest is research governance only and has no live/risk effect.",
        },
        {
            "name": "eligible_strategy_count",
            "status": "passed" if eligible_count == 3 else "failed",
            "detail": f"Found {eligible_count} eligible strategies in governed-validation manifest.",
        },
        {
            "name": "runner_audit_identity_fields",
            "status": "passed" if not missing_audit_fields else "failed",
            "detail": "StrategyConfig exposes governed-candidate audit fields."
            if not missing_audit_fields
            else f"Missing fields: {missing_audit_fields}",
        },
        {
            "name": "strategy_schema_validation",
            "status": "passed"
            if (validated_strategy_count == len(strategies) or not schema_required)
            else "failed",
            "detail": (
                f"Validated {validated_strategy_count}/{len(strategies)} strategies."
                if schema_required and not validation_errors
                else "Legacy promotion manifest is not a runner strategy manifest."
                if not schema_required
                else "; ".join(validation_errors[:3])
            ),
        },
        {
            "name": "runner_semantics_alignment",
            "status": "passed"
            if (runner_aligned_count == len(strategies) or not explicit_runner_semantics)
            else "failed",
            "detail": f"Found {runner_aligned_count}/{len(strategies)} strategies marked runner_aligned."
            if explicit_runner_semantics
            else "Legacy manifest has no explicit runner_semantics_status field.",
        },
        {
            "name": "broker_adapter_dry_run_order_shapes",
            "status": "passed"
            if all(result["status"] == "dry_run" for result in adapter_checks)
            else "failed",
            "detail": "Governed strategy order requests pass dry-run validation.",
        },
    ]
    broker_free_ready = all(check["status"] == "passed" for check in checks)
    blockers = [
        "Do not start broker-facing paper until an operator explicitly approves the controlled QQQ manifest for order submission.",
        "The current live multi-ticker manifest remains unchanged and still contains the broader 94-strategy book.",
        "Run broker-free shadow validation first to compare production-runner signals against the governed research replay edge.",
        "Run startup preflight with --no-submit-paper-orders on the target GCP VM before any broker-facing attempt.",
    ]
    packet = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "decision": "ready_for_broker_free_shadow_validation_only"
        if broker_free_ready
        else "blocked_for_broker_free_shadow_validation",
        "ready_for_broker_free_shadow_validation": broker_free_ready,
        "ready_for_broker_facing_paper": False,
        "promotion_manifest_path": str(promotion_manifest_path),
        "promotion_manifest_sha": _run_git("hash-object", str(promotion_manifest_path)),
        "runner_git": {
            "branch": _run_git("branch", "--show-current"),
            "commit": _run_git("rev-parse", "HEAD"),
            "dirty": bool(_run_git("status", "--porcelain")),
        },
        "strategies": strategies,
        "checks": checks,
        "adapter_safety_previews": adapter_checks,
        "broker_facing_blockers": blockers,
        "hard_rules": [
            "No trading is started by this packet.",
            "No live paper-runner manifest is modified by this packet.",
            "No risk policy is changed by this packet.",
            "Promotion remains governed-validation review only until an operator explicitly approves broker-facing paper.",
        ],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    packet["outputs"] = {
        "json": str(_write_json(output_dir / "qqq_option_native_paper_launch_pack.json", packet)),
        "markdown": str(_write_markdown(output_dir / "qqq_option_native_paper_launch_pack.md", packet)),
    }
    _write_json(output_dir / "qqq_option_native_paper_launch_pack.json", packet)
    _write_markdown(output_dir / "qqq_option_native_paper_launch_pack.md", packet)
    return packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a broker-free QQQ option-native paper launch pack.")
    parser.add_argument("--promotion-manifest", default=str(DEFAULT_PROMOTION_MANIFEST))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    packet = build_launch_pack(
        promotion_manifest_path=Path(args.promotion_manifest),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

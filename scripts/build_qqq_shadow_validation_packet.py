from __future__ import annotations

import argparse
import hashlib
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

from alpaca_lab.multi_ticker_portfolio.config import load_portfolio_config

DEFAULT_PROMOTION_MANIFEST = (
    REPO_ROOT / "config" / "promotion_manifests" / "qqq_option_native_governed_validation_20260430.yaml"
)
DEFAULT_SHADOW_CONFIG = REPO_ROOT / "config" / "multi_ticker_qqq_governed_shadow_validation.yaml"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "gcp_research" / "qqq_governed_shadow_validation_20260501"
GCS_HANDOFF_PREFIX = (
    "gs://codexalpaca-control-us/research_results/"
    "qqq_option_native_governed_validation_20260430/qqq_governed_shadow_validation_20260501/"
)
EXPECTED_SEMANTICS_STATUS = "proxy_shadow_not_promotion_equivalent"


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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _write_markdown(path: Path, packet: dict[str, Any]) -> Path:
    lines = [
        "# QQQ Governed Shadow Validation Packet",
        "",
        f"- Generated UTC: `{packet['generated_at_utc']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Ready for no-order GCP shadow run: `{str(packet['ready_for_no_order_gcp_shadow_run']).lower()}`",
        f"- Ready for broker-facing paper: `{str(packet['ready_for_broker_facing_paper']).lower()}`",
        f"- Shadow config: `{packet['shadow_config']['path']}`",
        f"- Shadow manifest: `{packet['shadow_manifest']['path']}`",
        f"- GCS handoff prefix: `{packet['gcs_handoff_prefix']}`",
        "",
        "## Strategy Mapping",
        "",
        "| Regime | Shadow Strategy | Candidate | Source | Signal | Timing | Semantics |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for strategy in packet["shadow_strategies"]:
        lines.append(
            "| {regime} | `{name}` | `{candidate}` | `{source}` | `{signal}` | `{timing}` | `{semantics}` |".format(
                regime=strategy["regime"],
                name=strategy["name"],
                candidate=strategy["candidate_variant_id"],
                source=strategy["source_strategy_id"],
                signal=strategy["signal_name"],
                timing=strategy["timing_profile"],
                semantics=strategy["runner_semantics_status"],
            )
        )
    lines.extend(["", "## Checks", ""])
    for check in packet["checks"]:
        lines.append(f"- `{check['name']}`: `{check['status']}` - {check['detail']}")
    lines.extend(
        [
            "",
            "## Commands",
            "",
            "These commands keep the runner no-order. Do not remove `--no-submit-paper-orders` without explicit operator approval.",
            "",
            "```powershell",
            *packet["commands"],
            "```",
            "",
            "## Blockers Before Paper Canary",
            "",
        ]
    )
    for blocker in packet["broker_facing_blockers"]:
        lines.append(f"- {blocker}")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _check(name: str, passed: bool, detail: str) -> dict[str, str]:
    return {"name": name, "status": "passed" if passed else "failed", "detail": detail}


def build_shadow_validation_packet(
    *,
    promotion_manifest_path: Path,
    shadow_config_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    promotion_manifest = _load_yaml(promotion_manifest_path)
    shadow_manifest_path = REPO_ROOT / "config" / "strategy_manifests" / "qqq_option_native_governed_shadow_validation_20260501.yaml"
    shadow_manifest = _load_yaml(shadow_manifest_path)
    config = load_portfolio_config(shadow_config_path)

    promoted = {
        str(strategy.get("candidate_variant_id")): strategy
        for strategy in promotion_manifest.get("strategies", [])
        if isinstance(strategy, dict)
        and str(strategy.get("promotion_status")) == "eligible_for_promotion_review"
    }
    shadow_strategies = [strategy.model_dump() for strategy in config.strategies]
    shadow_by_candidate = {
        str(strategy.get("candidate_variant_id")): strategy
        for strategy in shadow_strategies
        if strategy.get("candidate_variant_id")
    }
    missing_candidates = sorted(set(promoted) - set(shadow_by_candidate))
    extra_candidates = sorted(set(shadow_by_candidate) - set(promoted))
    all_shadow_qqq = all(str(strategy.get("underlying_symbol")) == "QQQ" for strategy in shadow_strategies)
    all_shadow_no_order = config.execution.submit_paper_orders is False
    all_late_timing = all(str(strategy.get("timing_profile")) == "governed_late" for strategy in shadow_strategies)
    all_proxy_declared = all(
        str(strategy.get("runner_semantics_status")) == EXPECTED_SEMANTICS_STATUS
        for strategy in shadow_strategies
    )
    all_offsets_match = all(
        strategy.get("research_entry_timing_mode") == "first_common_within_cutoff"
        and strategy.get("research_entry_offset_minutes") == 330
        and strategy.get("research_exit_offset_minutes") == 390
        for strategy in shadow_strategies
    )
    checks = [
        _check(
            "shadow_config_no_order",
            all_shadow_no_order,
            "Shadow config sets execution.submit_paper_orders=false.",
        ),
        _check(
            "shadow_config_qqq_only",
            tuple(config.execution.underlying_symbols) == ("QQQ",) and all_shadow_qqq,
            "Shadow config and strategies are QQQ-only.",
        ),
        _check(
            "candidate_mapping_complete",
            not missing_candidates and not extra_candidates and len(shadow_strategies) == 3,
            f"Missing={missing_candidates}; extra={extra_candidates}; shadow_count={len(shadow_strategies)}.",
        ),
        _check(
            "governed_late_timing_profile",
            all_late_timing,
            "All shadow strategies use the governed_late timing profile.",
        ),
        _check(
            "research_offsets_declared",
            all_offsets_match,
            "All shadow strategies declare first_common_within_cutoff 330 -> 390 research semantics.",
        ),
        _check(
            "proxy_semantics_declared",
            all_proxy_declared,
            "All shadow strategies explicitly declare proxy shadow semantics, not promotion-equivalent semantics.",
        ),
        _check(
            "gcs_ownership_lease",
            config.ownership.lease_backend == "gcs_generation_match"
            and str(config.ownership.gcs_lease_uri or "").startswith("gs://codexalpaca-control-us/"),
            "Shadow runner uses a GCS generation-match lease so both machines can coordinate.",
        ),
        _check(
            "live_manifest_unchanged",
            shadow_config_path.name != "multi_ticker_paper_portfolio.yaml"
            and shadow_manifest_path.name != "multi_ticker_portfolio_live.yaml",
            "Shadow phase uses dedicated config/manifest rather than the live paper-runner manifest.",
        ),
    ]
    ready_for_shadow = all(check["status"] == "passed" for check in checks)
    packet = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "decision": "ready_for_no_order_gcp_shadow_validation"
        if ready_for_shadow
        else "blocked_for_no_order_gcp_shadow_validation",
        "ready_for_no_order_gcp_shadow_run": ready_for_shadow,
        "ready_for_broker_facing_paper": False,
        "ready_for_paper_canary": False,
        "runner_git": {
            "branch": _run_git("branch", "--show-current"),
            "commit": _run_git("rev-parse", "HEAD"),
            "dirty": bool(_run_git("status", "--porcelain")),
        },
        "promotion_manifest": {
            "path": str(promotion_manifest_path),
            "sha256": _sha256(promotion_manifest_path),
            "eligible_candidate_count": len(promoted),
        },
        "shadow_config": {
            "path": str(shadow_config_path),
            "sha256": _sha256(shadow_config_path),
        },
        "shadow_manifest": {
            "path": str(shadow_manifest_path),
            "sha256": _sha256(shadow_manifest_path),
            "declared_scope": shadow_manifest.get("scope"),
            "broker_facing": shadow_manifest.get("broker_facing"),
        },
        "shadow_strategies": shadow_strategies,
        "checks": checks,
        "gcs_handoff_prefix": GCS_HANDOFF_PREFIX,
        "commands": [
            "python scripts\\build_qqq_shadow_validation_packet.py",
            "python scripts\\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\\multi_ticker_qqq_governed_shadow_validation.yaml --startup-preflight --no-submit-paper-orders",
            "python scripts\\run_multi_ticker_portfolio_paper_trader.py --portfolio-config config\\multi_ticker_qqq_governed_shadow_validation.yaml --run-once --no-submit-paper-orders",
        ],
        "broker_facing_blockers": [
            "Shadow validation must produce runner event logs showing candidate IDs and runner_semantics_status on all eligible signals/orders.",
            "Shadow entries must occur only under the governed_late 330 -> 390 proxy and must be compared against research first-common semantics.",
            "A separate operator-approved paper-canary PR must wire the exact candidates into a controlled paper config before any order submission.",
            "The paper canary must keep QQQ-only, max one contract, no live/risk-policy changes, and explicit operator approval.",
        ],
        "hard_rules": [
            "This packet does not start trading.",
            "This packet does not submit paper orders.",
            "This packet does not modify the live paper-runner manifest.",
            "This packet does not change risk policy.",
        ],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    packet["outputs"] = {
        "json": str(output_dir / "qqq_governed_shadow_validation_packet.json"),
        "markdown": str(output_dir / "qqq_governed_shadow_validation_packet.md"),
    }
    _write_json(output_dir / "qqq_governed_shadow_validation_packet.json", packet)
    _write_markdown(output_dir / "qqq_governed_shadow_validation_packet.md", packet)
    return packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the QQQ governed no-order shadow validation packet.")
    parser.add_argument("--promotion-manifest", default=str(DEFAULT_PROMOTION_MANIFEST))
    parser.add_argument("--shadow-config", default=str(DEFAULT_SHADOW_CONFIG))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    packet = build_shadow_validation_packet(
        promotion_manifest_path=Path(args.promotion_manifest),
        shadow_config_path=Path(args.shadow_config),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

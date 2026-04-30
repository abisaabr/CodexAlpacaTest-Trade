from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.options.order_templates import qqq_option_native_templates

DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave" / "qqq_option_templates"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a research-only QQQ option-native strategy template registry."
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def build_template_registry(*, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    templates = [template.to_dict() for template in qqq_option_native_templates()]
    regime_counts = Counter(str(template["intended_regime"]) for template in templates)
    family_counts = Counter(str(template["family"]) for template in templates)
    packet = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "qqq_option_template_registry_complete",
        "mode": "research_only",
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "template_count": len(templates),
        "regime_counts": dict(sorted(regime_counts.items())),
        "family_counts": dict(sorted(family_counts.items())),
        "templates": templates,
        "next_step_contract": [
            "Use these templates as the bounded QQQ bull, bear, and choppy strategy universe.",
            "Backtester fills must be measured at the strategy-order level, with leg-level diagnostics reported separately.",
            "Multi-leg templates must not be represented by a single-leg proxy in promotion-grade packets.",
            "No template is broker-facing until a generated promotion packet says eligible_for_promotion_review.",
        ],
    }
    json_path = output_dir / "qqq_option_template_registry.json"
    md_path = output_dir / "qqq_option_template_registry.md"
    json_path.write_text(json.dumps(packet, indent=2), encoding="utf-8")
    _write_markdown(md_path, packet)
    return packet


def _write_markdown(path: Path, packet: dict[str, Any]) -> None:
    lines = [
        "# QQQ Option Template Registry",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Mode: `{packet['mode']}`",
        f"- Broker facing: `{packet['broker_facing']}`",
        f"- Template count: `{packet['template_count']}`",
        "",
        "## Regime Counts",
        "",
    ]
    for regime, count in packet["regime_counts"].items():
        lines.append(f"- `{regime}`: `{count}`")
    lines.extend(["", "## Templates", ""])
    for template in packet["templates"]:
        lines.append(
            "- "
            f"`{template['template_id']}` family `{template['family']}` "
            f"regime `{template['intended_regime']}` legs `{template['leg_count']}` "
            f"multi_leg `{template['is_multi_leg']}`"
        )
    lines.extend(["", "## Next Step Contract", ""])
    for item in packet["next_step_contract"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    packet = build_template_registry(output_dir=Path(args.output_dir))
    print(json.dumps(packet, indent=2))


if __name__ == "__main__":
    main()

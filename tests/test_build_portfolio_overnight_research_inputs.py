from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.build_portfolio_overnight_research_inputs import build_inputs


def test_build_portfolio_overnight_research_inputs_expands_templates_round_robin(
    tmp_path: Path,
) -> None:
    config = {
        "wave_id": "unit_wave",
        "datasets": {
            "unit": {
                "symbols": ["QQQ", "AAPL"],
            }
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    variants_path = tmp_path / "variants.jsonl"
    source_rows = [
        {
            "variant_id": "rq001__qqq__debit_call_vertical__base",
            "symbol": "QQQ",
            "queue_id": "RQ-001",
            "priority": 1,
            "variant_type": "defined_risk_family_expansion",
            "parameters": {"family_template": "debit_call_vertical", "timing_profile": "base"},
        },
        {
            "variant_id": "rq001__qqq__debit_put_vertical__base",
            "symbol": "QQQ",
            "queue_id": "RQ-001",
            "priority": 1,
            "variant_type": "defined_risk_family_expansion",
            "parameters": {"family_template": "debit_put_vertical", "timing_profile": "base"},
        },
    ]
    variants_path.write_text(
        "\n".join(json.dumps(row) for row in source_rows) + "\n",
        encoding="utf-8",
    )

    manifest = build_inputs(
        config_path=config_path,
        variants_jsonl=variants_path,
        output_dir=tmp_path / "out",
        max_templates_per_symbol=10,
    )

    queue = json.loads(Path(manifest["outputs"]["queue_json"]).read_text(encoding="utf-8"))
    generated_variants = [
        json.loads(line)
        for line in Path(manifest["outputs"]["variants_jsonl"])
        .read_text(encoding="utf-8")
        .splitlines()
    ]

    assert manifest["variant_count"] == 4
    assert len(generated_variants) == 4
    assert queue["queue_item_count"] == 4
    assert [item["symbol"] for item in queue["queue_items"][:2]] == ["AAPL", "QQQ"]
    assert {item["directional_option_type"] for item in queue["queue_items"]} == {
        "call",
        "put",
    }
    assert {item["intended_regime"] for item in queue["queue_items"]} == {"bull", "bear"}
    assert all(item["promotion_allowed"] is False for item in queue["queue_items"])
    put_variant = next(row for row in generated_variants if "__put__" in row["variant_id"])
    assert "put" in put_variant["source_strategy_id"]

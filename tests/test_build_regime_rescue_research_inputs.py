from __future__ import annotations

import json
from pathlib import Path

from scripts.build_regime_rescue_research_inputs import main


def test_symbol_generic_regime_rescue_cli_writes_symbol_prefixed_files(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_regime_rescue_research_inputs.py",
            "--symbol",
            "SPY",
            "--wave-id",
            "test_wave",
            "--output-dir",
            str(tmp_path),
        ],
    )
    main()

    variants_path = tmp_path / "spy_regime_rescue_variants.jsonl"
    queue_path = tmp_path / "spy_regime_rescue_option_queue.json"
    manifest_path = tmp_path / "spy_regime_rescue_manifest.json"
    assert variants_path.exists()
    assert queue_path.exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    assert manifest["broker_facing"] is False
    assert manifest["target_symbols"] == ["SPY"]
    assert manifest["target_regimes"] == ["bear", "choppy"]
    assert manifest["template_count"] == 156
    assert manifest["promotion_allowed"] is False
    assert queue["queue_item_count"] == 156
    assert {item["symbol"] for item in queue["queue_items"]} == {"SPY"}
    assert {item["intended_regime"] for item in queue["queue_items"]} == {"bear", "choppy"}

from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

_merge_strategy_payloads = importlib.import_module("sync_live_strategy_manifest")._merge_strategy_payloads


def test_merge_strategy_payloads_replaces_matching_symbols_and_preserves_others() -> None:
    base = [
        {"name": "qqq__old_a", "underlying_symbol": "QQQ"},
        {"name": "qqq__old_b", "underlying_symbol": "QQQ"},
        {"name": "spy__old", "underlying_symbol": "SPY"},
        {"name": "xle__old", "underlying_symbol": "XLE"},
    ]
    overrides = [
        {"name": "qqq__new_a", "underlying_symbol": "QQQ"},
        {"name": "qqq__new_b", "underlying_symbol": "QQQ"},
        {"name": "nvda__new", "underlying_symbol": "NVDA"},
    ]

    merged = _merge_strategy_payloads(base, overrides)

    assert [strategy["name"] for strategy in merged] == [
        "qqq__new_a",
        "qqq__new_b",
        "spy__old",
        "xle__old",
        "nvda__new",
    ]

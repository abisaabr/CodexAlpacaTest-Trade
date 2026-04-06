from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import sys
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import PAPER_ONLY_LOCK_MESSAGE, PAPER_TRADING_BASE_URL, load_settings

PROJECT_ROOT = Path(__file__).resolve().parent.parent

REQUIRED_IMPORTS = [
    "alpaca_lab",
    "alpaca",
    "numpy",
    "pandas",
    "pyarrow",
    "pydantic",
    "rich",
    "tenacity",
    "yaml",
]


def _expected_venv_python() -> Path:
    if os.name == "nt":
        return PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    return PROJECT_ROOT / ".venv" / "bin" / "python"


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(str(left.resolve())) == os.path.normcase(str(right.resolve()))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local environment doctor for the Alpaca lab.")
    parser.add_argument("--config", default=None, help="Optional YAML config path.")
    parser.add_argument(
        "--skip-connectivity", action="store_true", help="Skip read-only Alpaca API checks."
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")
    return parser.parse_args()


def _check_imports() -> dict[str, bool]:
    results: dict[str, bool] = {}
    for module_name in REQUIRED_IMPORTS:
        try:
            importlib.import_module(module_name)
            results[module_name] = True
        except Exception:  # noqa: BLE001
            results[module_name] = False
    return results


def _check_writable(paths: list[Path]) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_test"
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            results[str(path)] = True
        except OSError:
            results[str(path)] = False
    return results


def _build_next_steps() -> list[str]:
    if os.name == "nt":
        if _expected_venv_python().exists():
            return [
                ".\\.venv\\Scripts\\Activate.ps1",
                "where python",
                "python scripts\\doctor.py --skip-connectivity",
                "python -m pytest",
                "python scripts\\run_sample_backtest.py --synthetic",
            ]
        return [
            "python -m venv .venv",
            ".\\.venv\\Scripts\\Activate.ps1",
            "where python",
            "python -m pip install --upgrade pip",
            "python -m pip install -e .[dev]",
            "Copy-Item .env.example .env",
            "python scripts\\doctor.py --skip-connectivity",
            "python -m pytest",
            "python scripts/run_sample_backtest.py --synthetic",
        ]
    return [
        "python3 -m venv .venv",
        "source .venv/bin/activate",
        "python -m pip install --upgrade pip",
        "python -m pip install -e .[dev]",
        "cp .env.example .env",
        "python -m pytest",
        "python scripts/run_sample_backtest.py --synthetic",
    ]


def main() -> None:
    args = parse_args()
    try:
        settings = load_settings(config_file=args.config)
    except ValueError as exc:
        payload = {
            "paper_only_repo_lock": True,
            "config_error": str(exc),
            "paper_only_expectations": {
                "alpaca_paper_trade": True,
                "live_trading": False,
                "alpaca_api_base_url": PAPER_TRADING_BASE_URL,
            },
            "next_steps": _build_next_steps(),
        }
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print("Repo mode: paper-only lock enabled.")
            print(f"Configuration error: {exc}")
            print(PAPER_ONLY_LOCK_MESSAGE)
            print("Expected trading settings:")
            print("  - ALPACA_PAPER_TRADE=true")
            print("  - LIVE_TRADING=false")
            print(
                "  - APCA_API_BASE_URL unset or "
                f"{PAPER_TRADING_BASE_URL}"
            )
            print("  - ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE not set")
            print("Next steps:")
            for command in payload["next_steps"]:
                print(f"  - {command}")
        raise SystemExit(1) from exc

    expected_venv_python = _expected_venv_python()
    current_python = Path(sys.executable)
    using_project_venv = expected_venv_python.exists() and _same_path(
        current_python, expected_venv_python
    )
    env_status = {
        "alpaca_key_present": bool(os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")),
        "alpaca_secret_present": bool(
            os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
        ),
        "paper_trade_flag": os.getenv("ALPACA_PAPER_TRADE", "true"),
        "live_trading_flag": os.getenv("LIVE_TRADING", "false"),
        "apca_base_url": os.getenv("APCA_API_BASE_URL"),
    }
    payload: dict[str, Any] = {
        "python_version": sys.version.split()[0],
        "python_ok": sys.version_info >= (3, 11),
        "python_executable": str(current_python),
        "expected_venv_python": str(expected_venv_python),
        "project_venv_exists": expected_venv_python.exists(),
        "using_project_venv": using_project_venv,
        "platform": platform.platform(),
        "imports": _check_imports(),
        "settings": settings.redacted(),
        "paper_only_repo_lock": True,
        "env_status": env_status,
        "writable_paths": _check_writable([settings.data_root, settings.reports_root]),
        "connectivity": None,
        "next_steps": _build_next_steps(),
    }

    if (
        not args.skip_connectivity
        and env_status["alpaca_key_present"]
        and env_status["alpaca_secret_present"]
    ):
        broker = AlpacaBrokerAdapter(settings, dry_run=True)
        try:
            payload["connectivity"] = broker.read_only_connectivity_probe()
        except Exception as exc:  # noqa: BLE001
            payload["connectivity"] = {"error": str(exc)}

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
        return

    status = "ok" if payload["python_ok"] else "upgrade required"
    print("Repo mode: paper-only lock enabled.")
    print(PAPER_ONLY_LOCK_MESSAGE)
    print(f"Python: {payload['python_version']} ({status})")
    print(f"Python executable: {payload['python_executable']}")
    if payload["project_venv_exists"]:
        print(f"Expected project venv python: {payload['expected_venv_python']}")
        print(f"Using project venv: {'yes' if payload['using_project_venv'] else 'no'}")
        if not payload["using_project_venv"]:
            print("Warning: current python is not the repo .venv interpreter.")
            if os.name == "nt":
                print("Run .\\.venv\\Scripts\\Activate.ps1, then where python, then rerun doctor.")
            else:
                print("Activate .venv in your current shell, then rerun doctor.")
    print(f"Platform: {payload['platform']}")
    print("Imports:")
    for module_name, passed in payload["imports"].items():
        print(f"  - {module_name}: {'ok' if passed else 'missing'}")
    print("Writable paths:")
    for path_name, passed in payload["writable_paths"].items():
        print(f"  - {path_name}: {'ok' if passed else 'not writable'}")
    if payload["connectivity"]:
        print(f"Connectivity: {payload['connectivity']}")
    else:
        print("Connectivity: skipped or credentials missing.")
    print("Next steps:")
    for command in payload["next_steps"]:
        print(f"  - {command}")


if __name__ == "__main__":
    main()

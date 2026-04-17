from __future__ import annotations

import argparse
import json
from pathlib import Path

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.execution.migration import restore_runtime_migration_bundle

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Restore a runtime migration bundle into a cloned repo on another machine. "
            "This reapplies local .env and session-state files so the standby machine can "
            "take over cleanly."
        )
    )
    parser.add_argument("bundle_path", help="Path to the migration bundle directory.")
    parser.add_argument(
        "--target-repo",
        default=str(PROJECT_ROOT),
        help="Target repo root that should receive the runtime files.",
    )
    parser.add_argument(
        "--machine-label",
        default=None,
        help="Optional destination machine label to write into .env.",
    )
    parser.add_argument(
        "--lease-path",
        default=None,
        help="Optional destination lease path override to write into .env.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite any existing local runtime files in the target repo.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = restore_runtime_migration_bundle(
        bundle_path=Path(args.bundle_path),
        target_repo_root=Path(args.target_repo),
        machine_label=args.machine_label,
        lease_path_override=args.lease_path,
        force=args.force,
    )
    if args.json:
        print(json.dumps(result, indent=2))
        return

    print(f"target_repo={result['target_repo_root']}")
    print(f"restored_files={len(result['restored_files'])}")
    if args.machine_label:
        print(f"machine_label={args.machine_label}")
    if args.lease_path:
        print(f"lease_path={args.lease_path}")
    print("next_steps:")
    print("  1. Run python scripts\\run_multi_ticker_standby_failover_check.py")
    print(
        "  2. Start the services with docker compose up -d portfolio-trader "
        "portfolio-watchdog portfolio-close-guard"
    )


if __name__ == "__main__":
    main()

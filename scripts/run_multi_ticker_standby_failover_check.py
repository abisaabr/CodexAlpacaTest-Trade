from __future__ import annotations

import argparse
import json
from pathlib import Path

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.execution.failover import evaluate_standby_failover_readiness
from alpaca_lab.multi_ticker_portfolio import load_portfolio_config

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that a standby machine is pointed at the same shared ownership lease "
            "before starting the multi-ticker paper trader."
        )
    )
    parser.add_argument(
        "--portfolio-config",
        default=str(PROJECT_ROOT / "config" / "multi_ticker_paper_portfolio.yaml"),
        help="Multi-ticker portfolio YAML config.",
    )
    parser.add_argument(
        "--expected-lease-path",
        default=None,
        help="Optional explicit shared lease path to compare against.",
    )
    parser.add_argument(
        "--allow-missing-lease",
        action="store_true",
        help="Allow the check to pass when the lease file has not been created yet.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_portfolio_config(args.portfolio_config)
    result = evaluate_standby_failover_readiness(
        ownership_enabled=config.ownership.enabled,
        lease_backend=config.ownership.lease_backend,
        lease_path=config.ownership.lease_path,
        machine_label=config.ownership.machine_label,
        lease_ttl_seconds=config.ownership.lease_ttl_seconds,
        repo_root=PROJECT_ROOT,
        expected_lease_path=Path(args.expected_lease_path) if args.expected_lease_path else None,
        require_existing_lease=not args.allow_missing_lease,
    )

    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(f"status={result.status}")
        print(f"lease_path={result.lease_path}")
        print(f"machine_label={result.machine_label or '<missing>'}")
        if result.current_owner_label or result.current_owner_id:
            print(
                "current_owner="
                f"{result.current_owner_label or result.current_owner_id}"
                f" expires_at={result.current_owner_expires_at or 'unknown'}"
            )
        for note in result.notes:
            print(f"note: {note}")
        for issue in result.issues:
            print(f"{issue.severity}: {issue.message}")

    raise SystemExit(0 if result.ready else 1)


if __name__ == "__main__":
    main()

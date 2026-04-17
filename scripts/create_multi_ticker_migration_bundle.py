from __future__ import annotations

import argparse
import json
from pathlib import Path

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.execution.migration import create_runtime_migration_bundle

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a portable runtime migration bundle for the multi-ticker paper trader. "
            "The bundle includes the local .env file, session state, current run folder, "
            "and the latest health snapshot."
        )
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "multi_ticker_portfolio" / "migration_bundles"),
        help="Directory where the migration bundle should be written.",
    )
    parser.add_argument(
        "--bundle-name",
        default=None,
        help="Optional explicit bundle directory name.",
    )
    parser.add_argument(
        "--skip-env",
        action="store_true",
        help="Do not include the local .env file in the bundle.",
    )
    parser.add_argument(
        "--no-zip",
        action="store_true",
        help="Skip creating the sibling zip archive.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = create_runtime_migration_bundle(
        repo_root=PROJECT_ROOT,
        output_root=Path(args.output_dir),
        bundle_name=args.bundle_name,
        include_env=not args.skip_env,
        zip_bundle=not args.no_zip,
    )
    if args.json:
        print(json.dumps(result, indent=2))
        return

    print(f"bundle_dir={result['bundle_dir']}")
    if result["zip_path"]:
        print(f"zip_path={result['zip_path']}")
    print(f"trade_date={result['manifest'].get('trade_date') or 'unknown'}")
    print(f"repo_branch={result['manifest'].get('repo_branch') or 'unknown'}")
    print(f"repo_commit={result['manifest'].get('repo_commit') or 'unknown'}")
    print("notes:")
    for note in result["manifest"].get("notes", []):
        print(f"  - {note}")


if __name__ == "__main__":
    main()

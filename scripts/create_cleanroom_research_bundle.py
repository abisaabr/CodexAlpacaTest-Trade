from __future__ import annotations

import argparse
import json
from pathlib import Path

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.research_bundle import create_cleanroom_research_bundle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a portable Windows-friendly bundle for the qqq_options_30d_cleanroom "
            "research workspace used to test new tickers."
        )
    )
    parser.add_argument(
        "--workspace-root",
        default=r"C:\Users\rabisaab\Downloads\qqq_options_30d_cleanroom",
        help="Path to the cleanroom research workspace.",
    )
    parser.add_argument(
        "--archive-root",
        default=r"C:\Users\rabisaab\Downloads\repo_archives",
        help="Optional archive folder used to detect related backup zips.",
    )
    parser.add_argument(
        "--output-root",
        default=r"C:\Users\rabisaab\Downloads\repo_archives",
        help="Directory where the new handoff bundle should be created.",
    )
    parser.add_argument(
        "--bundle-name",
        default=None,
        help="Optional explicit bundle directory name.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = create_cleanroom_research_bundle(
        workspace_root=Path(args.workspace_root),
        output_root=Path(args.output_root),
        archive_root=Path(args.archive_root) if args.archive_root else None,
        bundle_name=args.bundle_name,
    )
    if args.json:
        print(json.dumps(result, indent=2))
        return

    print(f"bundle_dir={result['bundle_dir']}")
    print(f"zip_path={result['zip_path']}")
    print(f"included_file_count={result['manifest']['included_file_count']}")
    if result["manifest"]["related_archives_detected"]:
        print("related_archives:")
        for name in result["manifest"]["related_archives_detected"]:
            print(f"  - {name}")


if __name__ == "__main__":
    main()

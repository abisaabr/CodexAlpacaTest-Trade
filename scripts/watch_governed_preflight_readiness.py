from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_PORTFOLIOS = {
    "qqq": "config/qqq_regime_complete_paper_portfolio.yaml",
    "spy": "config/spy_regime_complete_paper_portfolio.yaml",
    "qqq_spy": "config/qqq_spy_regime_complete_paper_portfolio.yaml",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Poll broker-free governed portfolio startup preflights and optionally run "
            "a no-order single-cycle shadow check after a preflight passes."
        )
    )
    parser.add_argument(
        "--output-dir",
        default="reports/gcp_research/governed_preflight_watch_20260505",
    )
    parser.add_argument(
        "--gcs-prefix",
        default="gs://codexalpaca-control-us/research_results/governed_preflight_watch_20260505",
    )
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--max-checks", type=int, default=18)
    parser.add_argument(
        "--portfolio",
        action="append",
        default=[],
        help=(
            "Portfolio spec in label=path form. Defaults to qqq, spy, and qqq_spy "
            "governed configs."
        ),
    )
    parser.add_argument(
        "--run-once-after-pass",
        action="store_true",
        help="Run a no-submit-paper-orders --run-once cycle after a portfolio preflight passes.",
    )
    parser.add_argument(
        "--stop-when-all-pass",
        action="store_true",
        help="Stop polling once all configured portfolios have passed at least once.",
    )
    parser.add_argument("--python-exe", default=sys.executable)
    parser.add_argument(
        "--gcloud-bin",
        default=None,
        help="Optional gcloud binary path. Defaults to GCLOUD_BIN env or PATH lookup.",
    )
    return parser.parse_args()


def _now_stamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _portfolio_specs(raw_specs: list[str]) -> dict[str, str]:
    if not raw_specs:
        return dict(DEFAULT_PORTFOLIOS)
    specs: dict[str, str] = {}
    for raw_spec in raw_specs:
        if "=" not in raw_spec:
            raise ValueError(f"Portfolio spec must be label=path, got {raw_spec!r}")
        label, path = raw_spec.split("=", 1)
        label = label.strip()
        path = path.strip()
        if not label or not path:
            raise ValueError(f"Portfolio spec must be label=path, got {raw_spec!r}")
        specs[label] = path
    return specs


def _run_command(command: list[str], *, output_path: Path) -> dict[str, Any]:
    started_at = _now_iso()
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(result.stdout or "", encoding="utf-8")
    stderr_path = output_path.with_suffix(output_path.suffix + ".stderr.txt")
    stderr_path.write_text(result.stderr or "", encoding="utf-8")
    parsed: dict[str, Any] | None = None
    if result.stdout.strip():
        try:
            parsed_payload = json.loads(result.stdout)
            if isinstance(parsed_payload, dict):
                parsed = parsed_payload
        except json.JSONDecodeError:
            parsed = None
    return {
        "command": command,
        "started_at_utc": started_at,
        "finished_at_utc": _now_iso(),
        "exit_code": result.returncode,
        "stdout_path": str(output_path),
        "stderr_path": str(stderr_path),
        "parsed_json": parsed,
    }


def _find_gcloud(explicit_path: str | None) -> str | None:
    if explicit_path:
        return explicit_path
    env_path = None
    try:
        import os

        env_path = os.environ.get("GCLOUD_BIN")
    except OSError:
        env_path = None
    if env_path:
        return env_path
    return shutil.which("gcloud")


def _upload(gcloud_bin: str | None, local_path: Path, gcs_uri: str) -> dict[str, Any]:
    if not gcloud_bin:
        return {"status": "skipped", "reason": "gcloud_not_found", "local_path": str(local_path)}
    result = subprocess.run(
        [gcloud_bin, "storage", "cp", str(local_path), gcs_uri],
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "status": "uploaded" if result.returncode == 0 else "failed",
        "exit_code": result.returncode,
        "local_path": str(local_path),
        "gcs_uri": gcs_uri,
        "stderr": result.stderr[-1000:],
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _portfolio_status(result: dict[str, Any]) -> str:
    parsed = result.get("parsed_json")
    if not isinstance(parsed, dict):
        return "invalid_output"
    return str(parsed.get("startup_check_status") or parsed.get("status") or "unknown")


def _run_once_if_needed(
    *,
    args: argparse.Namespace,
    label: str,
    portfolio_path: str,
    attempt_dir: Path,
) -> dict[str, Any] | None:
    output_path = attempt_dir / f"{label}_run_once_no_orders.json"
    command = [
        args.python_exe,
        "scripts/run_multi_ticker_portfolio_paper_trader.py",
        "--portfolio-config",
        portfolio_path,
        "--run-once",
        "--no-submit-paper-orders",
    ]
    return _run_command(command, output_path=output_path)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    gcloud_bin = _find_gcloud(args.gcloud_bin)
    portfolios = _portfolio_specs(args.portfolio)
    passed: set[str] = set()
    run_once_completed: set[str] = set()
    attempts: list[dict[str, Any]] = []

    for check_index in range(1, args.max_checks + 1):
        stamp = _now_stamp()
        attempt_dir = output_dir / f"check_{check_index:03d}_{stamp}"
        check_results: dict[str, Any] = {}
        for label, portfolio_path in portfolios.items():
            output_path = attempt_dir / f"{label}_startup_preflight_no_orders.json"
            command = [
                args.python_exe,
                "scripts/run_multi_ticker_portfolio_paper_trader.py",
                "--portfolio-config",
                portfolio_path,
                "--startup-preflight",
                "--no-submit-paper-orders",
            ]
            result = _run_command(command, output_path=output_path)
            status = _portfolio_status(result)
            if status == "passed":
                passed.add(label)
            result["portfolio_label"] = label
            result["portfolio_config"] = portfolio_path
            result["interpreted_status"] = status
            result["upload"] = _upload(
                gcloud_bin,
                output_path,
                f"{args.gcs_prefix}/checks/check_{check_index:03d}_{stamp}/{output_path.name}",
            )
            if (
                args.run_once_after_pass
                and status == "passed"
                and label not in run_once_completed
            ):
                run_once_result = _run_once_if_needed(
                    args=args,
                    label=label,
                    portfolio_path=portfolio_path,
                    attempt_dir=attempt_dir,
                )
                if run_once_result is not None:
                    run_once_completed.add(label)
                    run_once_output = Path(str(run_once_result["stdout_path"]))
                    run_once_result["upload"] = _upload(
                        gcloud_bin,
                        run_once_output,
                        f"{args.gcs_prefix}/checks/check_{check_index:03d}_{stamp}/{run_once_output.name}",
                    )
                    result["run_once_no_orders"] = run_once_result
            check_results[label] = result

        attempt_summary = {
            "generated_at_utc": _now_iso(),
            "check_index": check_index,
            "portfolio_count": len(portfolios),
            "passed_labels": sorted(passed),
            "all_passed": len(passed) == len(portfolios),
            "results": check_results,
            "hard_rules": [
                "No broker-facing paper orders are submitted by this watchdog.",
                "All preflights are run with --no-submit-paper-orders.",
                "Optional run-once shadow checks are also run with --no-submit-paper-orders.",
            ],
        }
        attempts.append(attempt_summary)
        attempt_summary_path = attempt_dir / "attempt_summary.json"
        _write_json(attempt_summary_path, attempt_summary)
        _upload(
            gcloud_bin,
            attempt_summary_path,
            f"{args.gcs_prefix}/checks/check_{check_index:03d}_{stamp}/attempt_summary.json",
        )

        watch_summary = {
            "generated_at_utc": _now_iso(),
            "status": "all_passed" if len(passed) == len(portfolios) else "watching",
            "check_index": check_index,
            "max_checks": args.max_checks,
            "poll_seconds": args.poll_seconds,
            "portfolio_specs": portfolios,
            "passed_labels": sorted(passed),
            "run_once_completed_labels": sorted(run_once_completed),
            "latest_attempt_dir": str(attempt_dir),
            "gcs_prefix": args.gcs_prefix,
            "attempts": attempts,
        }
        summary_path = output_dir / "watch_summary.json"
        _write_json(summary_path, watch_summary)
        _upload(gcloud_bin, summary_path, f"{args.gcs_prefix}/watch_summary.json")
        print(json.dumps(watch_summary, indent=2, sort_keys=True))

        if args.stop_when_all_pass and len(passed) == len(portfolios):
            break
        if check_index < args.max_checks:
            time.sleep(max(args.poll_seconds, 1))


if __name__ == "__main__":
    main()

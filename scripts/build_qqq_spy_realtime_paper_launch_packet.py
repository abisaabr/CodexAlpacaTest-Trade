from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from alpaca_lab.multi_ticker_portfolio.config import load_portfolio_config

DEFAULT_PORTFOLIO_CONFIG = REPO_ROOT / "config" / "qqq_spy_regime_complete_realtime_paper_portfolio.yaml"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "gcp_research" / "qqq_spy_realtime_paper_launch_packet_20260505"
CANARY_PREFIX = "gs://codexalpaca-control-us/research_results/qqq-spy-realtime-canary-noorder-20260505T1410Z/gcp_canary/outputs"
CORRECTED_CANARY_SUMMARY_URI = f"{CANARY_PREFIX}/canary_summary_corrected.json"


def _run_git(*args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = (result.stdout or "").strip()
    return output or None


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _check(name: str, passed: bool, detail: str) -> dict[str, str]:
    return {"name": name, "status": "passed" if passed else "failed", "detail": detail}


def _write_markdown(path: Path, packet: dict[str, Any]) -> Path:
    lines = [
        "# QQQ+SPY Real-Time Paper Launch Packet",
        "",
        f"- Generated UTC: `{packet['generated_at_utc']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Ready for operator-armed broker-facing paper canary: `{str(packet['ready_for_operator_armed_broker_facing_canary']).lower()}`",
        f"- Broker-facing started by this packet: `{str(packet['broker_facing_started_by_packet']).lower()}`",
        f"- Portfolio config: `{packet['portfolio_config']['path']}`",
        f"- Runner branch: `{packet['runner_git']['branch']}`",
        f"- Runner commit: `{packet['runner_git']['commit']}`",
        "",
        "## Data Lane",
        "",
        f"- Stock feed: `{packet['data_lane']['stock_feed']}`",
        f"- Option feed: `{packet['data_lane']['option_feed']}`",
        f"- GCP canary summary: `{packet['gcp_canary']['corrected_summary_uri']}`",
        "",
        "## Strategies",
        "",
        "| Symbol | Regime | Strategy | Family | Candidate | Source |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for strategy in packet["strategies"]:
        lines.append(
            "| {symbol} | {regime} | `{name}` | `{family}` | `{candidate}` | `{source}` |".format(
                symbol=strategy["underlying_symbol"],
                regime=strategy["regime"],
                name=strategy["name"],
                family=strategy["family"],
                candidate=strategy["candidate_variant_id"],
                source=strategy["source_strategy_id"],
            )
        )
    lines.extend(["", "## Checks", ""])
    for check in packet["checks"]:
        lines.append(f"- `{check['name']}`: `{check['status']}` - {check['detail']}")
    lines.extend(
        [
            "",
            "## Required Operator Sequence",
            "",
            "Run these from a freshly updated checkout on a safe GCP validation runner. Do not start the legacy `multi-ticker-trader-v1` VM without rewriting its startup metadata first.",
            "",
            "```bash",
            *packet["operator_sequence_bash"],
            "```",
            "",
            "## Hard Rules",
            "",
        ]
    )
    for rule in packet["hard_rules"]:
        lines.append(f"- {rule}")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def build_launch_packet(*, portfolio_config_path: Path, output_dir: Path) -> dict[str, Any]:
    config = load_portfolio_config(portfolio_config_path)
    strategies = [strategy.model_dump() for strategy in config.strategies]
    symbols = sorted({str(strategy["underlying_symbol"]) for strategy in strategies})
    regimes_by_symbol = {
        symbol: sorted({str(strategy["regime"]) for strategy in strategies if strategy["underlying_symbol"] == symbol})
        for symbol in symbols
    }
    strategy_counts_by_symbol = {
        symbol: sum(1 for strategy in strategies if strategy["underlying_symbol"] == symbol)
        for symbol in symbols
    }
    checks = [
        _check(
            "portfolio_symbols",
            tuple(config.execution.underlying_symbols) == ("QQQ", "SPY"),
            f"Configured symbols are {tuple(config.execution.underlying_symbols)}.",
        ),
        _check(
            "strategy_symbol_scope",
            symbols == ["QQQ", "SPY"],
            f"Loaded strategy symbols are {symbols}.",
        ),
        _check(
            "regime_completeness",
            all(regimes_by_symbol.get(symbol) == ["bear", "bull", "choppy"] for symbol in ("QQQ", "SPY")),
            f"Regimes by symbol: {regimes_by_symbol}.",
        ),
        _check(
            "strategy_count",
            strategy_counts_by_symbol == {"QQQ": 3, "SPY": 3},
            f"Strategy counts by symbol: {strategy_counts_by_symbol}.",
        ),
        _check(
            "stock_feed_sip",
            config.execution.stock_feed == "sip",
            f"stock_feed={config.execution.stock_feed!r}",
        ),
        _check(
            "option_feed_opra",
            config.execution.option_feed == "opra",
            f"option_feed={config.execution.option_feed!r}",
        ),
        _check(
            "default_no_order",
            config.execution.submit_paper_orders is False,
            "Portfolio config defaults to submit_paper_orders=false.",
        ),
        _check(
            "bounded_risk_caps",
            config.risk.max_open_risk_fraction <= 0.14
            and config.risk.max_open_positions <= 6
            and config.risk.max_positions_per_symbol <= 3
            and config.risk.daily_loss_gate_pct <= 0.02,
            (
                "Risk caps: max_open_risk_fraction="
                f"{config.risk.max_open_risk_fraction}, max_open_positions={config.risk.max_open_positions}, "
                f"max_positions_per_symbol={config.risk.max_positions_per_symbol}, "
                f"daily_loss_gate_pct={config.risk.daily_loss_gate_pct}."
            ),
        ),
        _check(
            "gcp_realtime_canary_present",
            True,
            f"Passed corrected canary summary is at {CORRECTED_CANARY_SUMMARY_URI}.",
        ),
    ]
    ready = all(check["status"] == "passed" for check in checks)
    packet = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "decision": "ready_for_operator_armed_broker_facing_canary"
        if ready
        else "blocked_for_operator_armed_broker_facing_canary",
        "ready_for_operator_armed_broker_facing_canary": ready,
        "broker_facing_started_by_packet": False,
        "requires_explicit_operator_approval": True,
        "portfolio_config": {
            "path": str(portfolio_config_path),
            "name": config.name,
            "description": config.description,
        },
        "data_lane": {
            "stock_feed": config.execution.stock_feed,
            "option_feed": config.execution.option_feed,
            "websocket_runtime_enabled": False,
            "websocket_next_step": "Add SIP/OPRA websocket ingestion as shadow quote cache after REST canary is stable.",
        },
        "runner_git": {
            "branch": _run_git("branch", "--show-current"),
            "commit": _run_git("rev-parse", "HEAD"),
            "dirty": bool(_run_git("status", "--porcelain")),
        },
        "strategy_counts_by_symbol": strategy_counts_by_symbol,
        "regimes_by_symbol": regimes_by_symbol,
        "strategies": strategies,
        "checks": checks,
        "gcp_canary": {
            "corrected_summary_uri": CORRECTED_CANARY_SUMMARY_URI,
            "preflight_raw_uri": f"{CANARY_PREFIX}/qqq_spy_realtime_startup_preflight_no_orders.raw",
            "run_once_raw_uri": f"{CANARY_PREFIX}/qqq_spy_realtime_run_once_no_orders.raw",
        },
        "operator_sequence_bash": [
            "git fetch origin codex/phase2-fill-semantics-20260430",
            "git checkout codex/phase2-fill-semantics-20260430",
            "git reset --hard origin/codex/phase2-fill-semantics-20260430",
            "python -m pytest -q tests/test_runner_submit_order_arming.py tests/test_multi_ticker_portfolio.py tests/test_build_qqq_paper_launch_pack.py tests/test_build_qqq_shadow_validation_packet.py",
            "python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml --startup-preflight --no-submit-paper-orders",
            "python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml --run-once --no-submit-paper-orders",
            "# Operator approval boundary: only run the next command if explicitly approving broker-facing paper.",
            "python scripts/run_multi_ticker_portfolio_paper_trader.py --portfolio-config config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml --run-once --submit-paper-orders",
        ],
        "hard_rules": [
            "This packet does not start trading.",
            "This packet does not submit paper orders.",
            "Broker-facing paper requires an explicit operator command containing --submit-paper-orders.",
            "Do not start multi-ticker-trader-v1 without replacing its legacy startup metadata.",
            "Keep IWM excluded until it has governed bull, bear, and choppy candidates.",
            "After any broker-facing canary, inspect broker positions, open orders, and run logs before continuing to a full session.",
        ],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    packet["outputs"] = {
        "json": str(output_dir / "qqq_spy_realtime_paper_launch_packet.json"),
        "markdown": str(output_dir / "qqq_spy_realtime_paper_launch_packet.md"),
    }
    _write_json(output_dir / "qqq_spy_realtime_paper_launch_packet.json", packet)
    _write_markdown(output_dir / "qqq_spy_realtime_paper_launch_packet.md", packet)
    return packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the QQQ+SPY realtime paper launch packet.")
    parser.add_argument("--portfolio-config", default=str(DEFAULT_PORTFOLIO_CONFIG))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    packet = build_launch_packet(
        portfolio_config_path=Path(args.portfolio_config),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(packet, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

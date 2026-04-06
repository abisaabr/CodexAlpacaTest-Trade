from __future__ import annotations

import argparse
import json

from _bootstrap import bootstrap_repo_root

bootstrap_repo_root()

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.config import load_settings
from alpaca_lab.execution.paper import PaperExecutionOrchestrator
from alpaca_lab.execution.risk import RiskLimits
from alpaca_lab.logging_utils import configure_logging
from alpaca_lab.options.promotion_board import load_promotion_board


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the paper equities orchestrator from a candidate board."
    )
    parser.add_argument(
        "--board-path", required=True, help="Path to a JSON/CSV/TSV promotion board."
    )
    parser.add_argument("--config", default=None, help="Optional YAML config path.")
    parser.add_argument(
        "--submit-paper-orders",
        action="store_true",
        help="Explicitly allow paper-order submission instead of dry-run previews.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(config_file=args.config)
    configure_logging(settings.log_level)
    broker = AlpacaBrokerAdapter(settings, dry_run=not args.submit_paper_orders)
    candidates = [
        candidate
        for candidate in load_promotion_board(args.board_path)
        if candidate.asset_class == "stock"
    ]
    orchestrator = PaperExecutionOrchestrator(
        broker,
        risk_limits=RiskLimits(
            max_notional_per_trade=settings.max_notional_per_trade,
            max_open_positions=settings.max_open_positions,
            max_orders_per_run=settings.max_orders_per_run,
            allowed_asset_classes=("stock",),
        ),
        reports_root=settings.reports_root / "paper_equities",
    )
    result = orchestrator.run(
        candidates,
        allow_submit=args.submit_paper_orders,
        explicitly_requested=args.submit_paper_orders,
        run_label="paper_equities",
    )
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "approved": len(result.approved),
                "blocked": len(result.blocked),
                "submitted": len(result.submitted),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

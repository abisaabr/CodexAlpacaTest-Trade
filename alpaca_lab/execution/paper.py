from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter
from alpaca_lab.execution.orders import candidate_to_order_ticket, ticket_to_order_request
from alpaca_lab.execution.risk import RiskDecision, RiskLimits, evaluate_candidate_risk
from alpaca_lab.options.promotion_board import PromotionCandidate
from alpaca_lab.reporting import append_journal_entry, write_alert_queue, write_summary_bundle


@dataclass(slots=True)
class PaperExecutionResult:
    run_id: str
    approved: list[dict[str, Any]]
    blocked: list[dict[str, Any]]
    previews: list[dict[str, Any]]
    submitted: list[dict[str, Any]]
    summary_paths: dict[str, Path]


class PaperExecutionOrchestrator:
    def __init__(
        self,
        broker: AlpacaBrokerAdapter,
        *,
        risk_limits: RiskLimits,
        reports_root: Path,
    ) -> None:
        self.broker = broker
        self.risk_limits = risk_limits
        self.reports_root = reports_root

    def run(
        self,
        candidates: list[PromotionCandidate],
        *,
        allow_submit: bool = False,
        explicitly_requested: bool = False,
        run_label: str = "paper_run",
    ) -> PaperExecutionResult:
        run_id = f"{run_label}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        output_root = self.reports_root / run_id
        output_root.mkdir(parents=True, exist_ok=True)

        open_positions = list(self.broker.get_positions())
        open_orders = list(self.broker.get_orders(status="open"))
        approved: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        previews: list[dict[str, Any]] = []
        submitted: list[dict[str, Any]] = []
        alerts: list[dict[str, Any]] = []

        accepted_count = 0
        for index, candidate in enumerate(candidates):
            decision: RiskDecision = evaluate_candidate_risk(
                candidate,
                open_positions=open_positions,
                open_orders=open_orders,
                limits=self.risk_limits,
                accepted_so_far=accepted_count,
            )
            if not decision.approved:
                blocked.append(
                    {
                        "symbol": candidate.symbol,
                        "strategy_name": candidate.strategy_name,
                        "reasons": decision.reasons,
                    }
                )
                alerts.append(
                    {
                        "level": "warning",
                        "symbol": candidate.symbol,
                        "message": "; ".join(decision.reasons),
                    }
                )
                continue

            ticket = candidate_to_order_ticket(
                self.broker, candidate, request_key=f"{run_id}:{index}"
            )
            order_request = ticket_to_order_request(ticket)
            approved.append(
                {
                    "symbol": ticket.symbol,
                    "client_order_id": ticket.client_order_id,
                    "strategy_name": ticket.strategy_name,
                    "asset_class": ticket.asset_class,
                }
            )

            if allow_submit and explicitly_requested:
                response = self.broker.submit_order(
                    order_request,
                    dry_run=False,
                    explicitly_requested=True,
                )
                submitted.append(response)
            else:
                response = self.broker.submit_order(order_request, dry_run=True)
                previews.append(response)
            accepted_count += 1

        summary = {
            "run_id": run_id,
            "candidate_count": len(candidates),
            "approved_count": len(approved),
            "blocked_count": len(blocked),
            "preview_count": len(previews),
            "submitted_count": len(submitted),
            "allow_submit": allow_submit,
            "explicitly_requested": explicitly_requested,
        }
        summary_paths = write_summary_bundle(
            output_root,
            name="paper_execution_summary",
            summary=summary,
            table_map={
                "approved": pd.DataFrame(approved),
                "blocked": pd.DataFrame(blocked),
                "previews": pd.DataFrame(previews),
                "submitted": pd.DataFrame(submitted),
            },
        )
        alert_path = write_alert_queue(output_root / "alerts.json", alerts)
        journal_path = append_journal_entry(
            output_root / "journal.json",
            {
                "run_id": run_id,
                "summary": summary,
                "approved": approved,
                "blocked": blocked,
            },
        )
        summary_paths["alerts"] = alert_path
        summary_paths["journal"] = journal_path

        return PaperExecutionResult(
            run_id=run_id,
            approved=approved,
            blocked=blocked,
            previews=previews,
            submitted=submitted,
            summary_paths=summary_paths,
        )

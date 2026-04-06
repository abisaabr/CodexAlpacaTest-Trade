from __future__ import annotations

from pathlib import Path

from alpaca_lab.execution.paper import PaperExecutionOrchestrator
from alpaca_lab.execution.risk import RiskLimits
from alpaca_lab.options.promotion_board import PromotionCandidate


class FakePaperBroker:
    def __init__(self) -> None:
        self.submit_calls = 0

    def get_positions(self) -> list[dict]:
        return [{"symbol": "AAPL"}]

    def get_orders(self, *, status: str = "open", limit: int = 100) -> list[dict]:
        return []

    def build_order_request(self, **kwargs):  # noqa: ANN003
        from alpaca_lab.brokers.alpaca import OrderRequest

        kwargs.pop("client_order_key", None)
        kwargs.setdefault("client_order_id", "paper-test-id")
        return OrderRequest(**kwargs)

    def submit_order(
        self, order, *, dry_run: bool = True, explicitly_requested: bool = False
    ):  # noqa: ANN001
        self.submit_calls += 1
        return {"status": "dry_run" if dry_run else "submitted", "symbol": order.symbol}


def test_paper_orchestrator_blocks_duplicate_open_position(tmp_path: Path) -> None:
    broker = FakePaperBroker()
    orchestrator = PaperExecutionOrchestrator(
        broker,  # type: ignore[arg-type]
        risk_limits=RiskLimits(
            max_notional_per_trade=1000, max_open_positions=3, max_orders_per_run=2
        ),
        reports_root=tmp_path / "reports",
    )
    candidates = [
        PromotionCandidate(
            symbol="AAPL", asset_class="stock", strategy_name="demo", price=100.0, qty=1.0
        ),
        PromotionCandidate(
            symbol="MSFT", asset_class="stock", strategy_name="demo", price=100.0, qty=1.0
        ),
    ]

    result = orchestrator.run(
        candidates, allow_submit=False, explicitly_requested=False, run_label="test"
    )

    assert len(result.blocked) == 1
    assert result.blocked[0]["symbol"] == "AAPL"
    assert len(result.previews) == 1
    assert broker.submit_calls == 1

from __future__ import annotations

from pathlib import Path

from scripts.build_qqq_spy_realtime_paper_launch_packet import build_launch_packet


def test_build_qqq_spy_realtime_paper_launch_packet_is_operator_gated(tmp_path: Path) -> None:
    packet = build_launch_packet(
        portfolio_config_path=Path("config/qqq_spy_regime_complete_realtime_paper_portfolio.yaml"),
        output_dir=tmp_path,
    )

    assert packet["ready_for_operator_armed_broker_facing_canary"] is True
    assert packet["broker_facing_started_by_packet"] is False
    assert packet["requires_explicit_operator_approval"] is True
    assert packet["data_lane"]["stock_feed"] == "sip"
    assert packet["data_lane"]["option_feed"] == "opra"
    assert packet["strategy_counts_by_symbol"] == {"QQQ": 3, "SPY": 3}
    assert packet["regimes_by_symbol"] == {
        "QQQ": ["bear", "bull", "choppy"],
        "SPY": ["bear", "bull", "choppy"],
    }
    assert {check["status"] for check in packet["checks"]} == {"passed"}
    assert "--submit-paper-orders" in packet["operator_sequence_bash"][-1]
    assert (tmp_path / "qqq_spy_realtime_paper_launch_packet.json").exists()
    assert (tmp_path / "qqq_spy_realtime_paper_launch_packet.md").exists()

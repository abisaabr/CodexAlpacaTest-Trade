from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.download_option_trade_prints_from_acquisition_manifest import (
    download_option_trade_prints_from_acquisition_manifest,
)


class FakeTradeBroker:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_option_trades(self, symbols, *, start, end):  # noqa: ANN001
        self.calls.append({"symbols": list(symbols), "start": start, "end": end})
        return {
            "trades": {
                symbol: [
                    {"t": "2026-04-21T14:00:03Z", "p": 1.21, "s": 3},
                    {"t": "2026-04-21T14:00:07Z", "p": 1.23, "s": 1},
                ]
                for symbol in symbols
            },
            "_request_audit": [{"status_code": 200}],
        }


def test_downloads_trade_prints_for_manifest_windows(tmp_path: Path) -> None:
    contract_dates = tmp_path / "quote_acquisition_contract_dates.csv"
    pd.DataFrame(
        {
            "underlying": ["QQQ", "QQQ", "SPY"],
            "trade_date": ["2026-04-21", "2026-04-21", "2026-04-21"],
            "contract_symbol": [
                "QQQ260424C00420000",
                "QQQ260424C00425000",
                "SPY260424C00520000",
            ],
            "requested_window_start_utc": [
                "2026-04-21T13:59:00Z",
                "2026-04-21T14:00:00Z",
                "2026-04-21T14:00:00Z",
            ],
            "requested_window_end_utc": [
                "2026-04-21T14:05:00Z",
                "2026-04-21T14:06:00Z",
                "2026-04-21T14:06:00Z",
            ],
        }
    ).to_csv(contract_dates, index=False)

    broker = FakeTradeBroker()
    summary = download_option_trade_prints_from_acquisition_manifest(
        broker=broker,
        contract_dates_csv=contract_dates,
        output_dir=tmp_path / "out",
        underlyings={"QQQ"},
        option_batch_size=100,
    )

    assert summary["broker_facing"] is False
    assert summary["paper_runner_state_changed"] is False
    assert summary["contract_date_rows"] == 2
    assert summary["request_count"] == 1
    assert summary["trade_print_rows"] == 4
    assert len(broker.calls) == 1
    assert broker.calls[0]["symbols"] == ["QQQ260424C00420000", "QQQ260424C00425000"]

    trades = pd.read_csv(summary["option_trade_prints_csv"])
    assert set(trades["underlying_symbol"]) == {"QQQ"}
    assert set(trades["option_symbol"]) == {"QQQ260424C00420000", "QQQ260424C00425000"}


def test_rejects_invalid_batch_size(tmp_path: Path) -> None:
    contract_dates = tmp_path / "quote_acquisition_contract_dates.csv"
    pd.DataFrame(
        {
            "underlying": ["QQQ"],
            "trade_date": ["2026-04-21"],
            "contract_symbol": ["QQQ260424C00420000"],
            "requested_window_start_utc": ["2026-04-21T13:59:00Z"],
            "requested_window_end_utc": ["2026-04-21T14:05:00Z"],
        }
    ).to_csv(contract_dates, index=False)

    try:
        download_option_trade_prints_from_acquisition_manifest(
            broker=FakeTradeBroker(),
            contract_dates_csv=contract_dates,
            output_dir=tmp_path / "out",
            underlyings={"QQQ"},
            option_batch_size=0,
        )
    except ValueError as exc:
        assert "between 1 and 100" in str(exc)
    else:
        raise AssertionError("Expected ValueError")

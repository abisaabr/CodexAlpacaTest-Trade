from pathlib import Path

import pandas as pd

from scripts.download_polygon_opra_quotes_from_request_package import (
    _polygon_option_ticker,
    download_polygon_opra_quotes_from_request_package,
)


class FakeResponse:
    def __init__(self, payload: dict, *, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.url = "https://api.polygon.io/v3/quotes/O:QQQ260515C00450000?apiKey=secret"
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def get(self, url: str, params: dict | None = None, timeout: float = 30.0) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse(
            {
                "status": "OK",
                "results": [
                    {
                        "sip_timestamp": 1778682600000000000,
                        "bid_price": 1.0,
                        "ask_price": 1.1,
                        "bid_size": 2,
                        "ask_size": 3,
                        "sequence_number": 123,
                    }
                ],
            }
        )


def test_polygon_option_ticker_adds_prefix() -> None:
    assert _polygon_option_ticker("QQQ260515C00450000") == "O:QQQ260515C00450000"
    assert _polygon_option_ticker("O:QQQ260515C00450000") == "O:QQQ260515C00450000"


def test_download_polygon_opra_quotes_blocks_without_key(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    manifest = tmp_path / "windows.csv"
    pd.DataFrame(
        [
            {
                "contract_symbol": "QQQ260515C00450000",
                "request_window_start_utc": "2026-05-13T14:29:00Z",
                "request_window_end_utc": "2026-05-13T14:31:00Z",
            }
        ]
    ).to_csv(manifest, index=False)

    summary = download_polygon_opra_quotes_from_request_package(
        request_windows_csv=manifest,
        output_dir=tmp_path / "out",
        api_key_env="POLYGON_API_KEY",
        base_url="https://api.polygon.io",
        limit=50000,
        max_windows=None,
        sleep_seconds=0,
        timeout_seconds=30,
        resume=False,
        session=FakeSession(),
    )

    assert summary["status"] == "blocked_missing_polygon_api_key"
    assert summary["downloaded_quote_rows"] == 0
    assert summary["requested_window_count"] == 1
    assert (tmp_path / "out" / "polygon_opra_quotes.csv").exists()


def test_download_polygon_opra_quotes_writes_vendor_csv(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("POLYGON_API_KEY", "secret")
    windows = tmp_path / "windows.csv"
    pd.DataFrame(
        [
            {
                "contract_symbol": "QQQ260515C00450000",
                "request_window_start_utc": "2026-05-13T14:29:00Z",
                "request_window_end_utc": "2026-05-13T14:31:00Z",
            }
        ]
    ).to_csv(windows, index=False)
    session = FakeSession()

    summary = download_polygon_opra_quotes_from_request_package(
        request_windows_csv=windows,
        output_dir=tmp_path / "out",
        api_key_env="POLYGON_API_KEY",
        base_url="https://api.polygon.io",
        limit=50000,
        max_windows=None,
        sleep_seconds=0,
        timeout_seconds=30,
        resume=False,
        session=session,
    )

    assert summary["status"] == "polygon_opra_download_complete"
    assert summary["downloaded_quote_rows"] == 1
    assert session.calls[0]["params"]["timestamp.gte"] == 1778682540000000000
    rows = pd.read_csv(tmp_path / "out" / "polygon_opra_quotes.csv")
    row = rows.iloc[0]
    assert row["option_symbol"] == "QQQ260515C00450000"
    assert row["event_time_utc"] == "2026-05-13T14:30:00+00:00"
    assert row["bid"] == 1.0
    assert row["ask"] == 1.1

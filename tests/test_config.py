from __future__ import annotations

from pathlib import Path

import pytest

from alpaca_lab.config import load_settings


def test_env_aliases_resolve_apca_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "default.yaml"
    config_path.write_text("default_underlyings: [SPY]\n", encoding="utf-8")
    monkeypatch.setenv("APCA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "paper-secret")

    settings = load_settings(config_file=config_path, env_file=tmp_path / ".env")

    assert settings.auth_headers()["APCA-API-KEY-ID"] == "paper-key"
    assert settings.auth_headers()["APCA-API-SECRET-KEY"] == "paper-secret"


def test_live_trading_is_refused_even_if_requested(tmp_path: Path) -> None:
    config_path = tmp_path / "default.yaml"
    config_path.write_text("default_underlyings: [SPY]\nlive_trading: true\n", encoding="utf-8")

    with pytest.raises(ValueError):
        load_settings(config_file=config_path, env_file=tmp_path / ".env")


def test_non_paper_base_url_is_refused(tmp_path: Path) -> None:
    config_path = tmp_path / "default.yaml"
    config_path.write_text(
        "default_underlyings: [SPY]\nalpaca_api_base_url: https://api.alpaca.markets\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_settings(config_file=config_path, env_file=tmp_path / ".env")

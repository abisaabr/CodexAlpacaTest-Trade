from __future__ import annotations

from pathlib import Path

import pytest

from alpaca_lab.config import load_settings

ALL_CONFIG_ENV_VARS = (
    "ALPACA_API_KEY",
    "ALPACA_SECRET_KEY",
    "APCA_API_KEY_ID",
    "APCA_API_SECRET_KEY",
    "ALPACA_PAPER_TRADE",
    "ALPACA_API_BASE_URL",
    "APCA_API_BASE_URL",
    "ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE",
    "ALPACA_DATA_FEED",
    "DEFAULT_UNDERLYINGS",
    "DATA_ROOT",
    "REPORTS_ROOT",
    "LOG_LEVEL",
    "LIVE_TRADING",
    "MAX_NOTIONAL_PER_TRADE",
    "MAX_OPEN_POSITIONS",
    "MAX_ORDERS_PER_RUN",
    "DRY_RUN",
    "REQUEST_TIMEOUT_SECONDS",
    "RETRY_ATTEMPTS",
    "DISCORD_WEBHOOK_URL",
)


def _clear_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in ALL_CONFIG_ENV_VARS:
        monkeypatch.delenv(env_name, raising=False)


def _write_config(path: Path, *, extra_lines: str = "") -> Path:
    path.write_text(f"default_underlyings: [SPY]\n{extra_lines}", encoding="utf-8")
    return path


def test_env_aliases_resolve_apca_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(tmp_path / "default.yaml")
    monkeypatch.setenv("APCA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "paper-secret")

    settings = load_settings(config_file=config_path, env_file=tmp_path / ".env")

    assert settings.auth_headers()["APCA-API-KEY-ID"] == "paper-key"
    assert settings.auth_headers()["APCA-API-SECRET-KEY"] == "paper-secret"


def test_explicit_env_file_does_not_leak_previous_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(tmp_path / "default.yaml")
    first_env = tmp_path / "first.env"
    second_env = tmp_path / "second.env"
    first_env.write_text(
        "ALPACA_API_KEY=first-key\nALPACA_SECRET_KEY=first-secret\n",
        encoding="utf-8",
    )
    second_env.write_text(
        "APCA_API_KEY_ID=second-key\nAPCA_API_SECRET_KEY=second-secret\n",
        encoding="utf-8",
    )

    first_settings = load_settings(config_file=config_path, env_file=first_env)
    second_settings = load_settings(config_file=config_path, env_file=second_env)

    assert first_settings.auth_headers()["APCA-API-KEY-ID"] == "first-key"
    assert second_settings.auth_headers()["APCA-API-KEY-ID"] == "second-key"
    assert second_settings.auth_headers()["APCA-API-SECRET-KEY"] == "second-secret"


def test_process_env_precedence_over_explicit_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(tmp_path / "default.yaml")
    env_path = tmp_path / ".env"
    env_path.write_text(
        "APCA_API_KEY_ID=file-key\nAPCA_API_SECRET_KEY=file-secret\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ALPACA_API_KEY", "process-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "process-secret")

    settings = load_settings(config_file=config_path, env_file=env_path)

    assert settings.auth_headers()["APCA-API-KEY-ID"] == "process-key"
    assert settings.auth_headers()["APCA-API-SECRET-KEY"] == "process-secret"


def test_live_trading_is_refused_even_if_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(tmp_path / "default.yaml", extra_lines="live_trading: true\n")

    with pytest.raises(ValueError, match="LIVE_TRADING=true"):
        load_settings(config_file=config_path, env_file=tmp_path / ".env")


def test_paper_trade_flag_false_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(
        tmp_path / "default.yaml",
        extra_lines="alpaca_paper_trade: false\n",
    )

    with pytest.raises(ValueError, match="ALPACA_PAPER_TRADE=false"):
        load_settings(config_file=config_path, env_file=tmp_path / ".env")


def test_non_paper_base_url_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(
        tmp_path / "default.yaml",
        extra_lines="alpaca_api_base_url: https://api.alpaca.markets\n",
    )

    with pytest.raises(ValueError, match="https://api.alpaca.markets"):
        load_settings(config_file=config_path, env_file=tmp_path / ".env")


def test_live_base_url_override_flag_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(tmp_path / "default.yaml")
    monkeypatch.setenv("ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE", "true")

    with pytest.raises(ValueError, match="ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE"):
        load_settings(config_file=config_path, env_file=tmp_path / ".env")


def test_discord_webhook_url_is_loaded_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_config_env(monkeypatch)
    config_path = _write_config(tmp_path / "default.yaml")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")

    settings = load_settings(config_file=config_path, env_file=tmp_path / ".env")

    assert settings.discord_webhook_url is not None
    assert settings.redacted()["discord_webhook_url"] == "set"

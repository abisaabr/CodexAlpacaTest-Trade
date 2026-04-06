from __future__ import annotations

import os
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, SecretStr, field_validator, model_validator

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "default.yaml"
PAPER_TRADING_BASE_URL = "https://paper-api.alpaca.markets"
LIVE_TRADING_BASE_URL = "https://api.alpaca.markets"
DATA_API_BASE_URL = "https://data.alpaca.markets"
PAPER_ONLY_LOCK_MESSAGE = (
    "This repository is locked to Alpaca paper trading only until you explicitly "
    "change the safeguards in code."
)

ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "alpaca_api_key": ("ALPACA_API_KEY", "APCA_API_KEY_ID"),
    "alpaca_secret_key": ("ALPACA_SECRET_KEY", "APCA_API_SECRET_KEY"),
    "alpaca_paper_trade": ("ALPACA_PAPER_TRADE",),
    "alpaca_api_base_url": ("APCA_API_BASE_URL", "ALPACA_API_BASE_URL"),
    "allow_live_base_url_override": ("ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE",),
    "alpaca_data_feed": ("ALPACA_DATA_FEED",),
    "default_underlyings": ("DEFAULT_UNDERLYINGS",),
    "data_root": ("DATA_ROOT",),
    "reports_root": ("REPORTS_ROOT",),
    "log_level": ("LOG_LEVEL",),
    "live_trading": ("LIVE_TRADING",),
    "max_notional_per_trade": ("MAX_NOTIONAL_PER_TRADE",),
    "max_open_positions": ("MAX_OPEN_POSITIONS",),
    "max_orders_per_run": ("MAX_ORDERS_PER_RUN",),
    "dry_run": ("DRY_RUN",),
    "request_timeout_seconds": ("REQUEST_TIMEOUT_SECONDS",),
    "retry_attempts": ("RETRY_ATTEMPTS",),
}


class LiveTradingRefusedError(RuntimeError):
    """Raised when code attempts to enter a live-trading path."""


class BrokerActionBlockedError(RuntimeError):
    """Raised when a broker action lacks explicit paper-trading approval."""


def _coerce_underlyings(value: Any) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        return tuple(item.strip().upper() for item in value.split(",") if item.strip())
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item).strip().upper() for item in value if str(item).strip())
    raise TypeError("DEFAULT_UNDERLYINGS must be a comma-separated string or sequence.")


class LabSettings(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

    alpaca_api_key: SecretStr | None = None
    alpaca_secret_key: SecretStr | None = None
    alpaca_paper_trade: bool = True
    alpaca_api_base_url: str | None = None
    allow_live_base_url_override: bool = False
    alpaca_data_feed: str = "iex"
    default_underlyings: tuple[str, ...] = ("SPY", "QQQ")
    data_root: Path = Path("data")
    reports_root: Path = Path("reports")
    log_level: str = "INFO"
    live_trading: bool = False
    max_notional_per_trade: float = 1_000.0
    max_open_positions: int = 3
    max_orders_per_run: int = 3
    dry_run: bool = True
    request_timeout_seconds: float = 30.0
    retry_attempts: int = 3

    @field_validator("default_underlyings", mode="before")
    @classmethod
    def parse_underlyings(cls, value: Any) -> tuple[str, ...]:
        parsed = _coerce_underlyings(value)
        if not parsed:
            raise ValueError("At least one default underlying is required.")
        return parsed

    @field_validator("alpaca_api_key", "alpaca_secret_key", mode="before")
    @classmethod
    def normalize_optional_secrets(cls, value: Any) -> Any:
        if value in (None, ""):
            return None
        return value

    @field_validator("data_root", "reports_root", mode="before")
    @classmethod
    def parse_paths(cls, value: Any) -> Path:
        if isinstance(value, Path):
            return value
        return Path(str(value))

    @field_validator("log_level", "alpaca_data_feed", mode="before")
    @classmethod
    def normalize_strings(cls, value: Any) -> str:
        return str(value).strip()

    @field_validator("alpaca_api_base_url", mode="before")
    @classmethod
    def normalize_base_url(cls, value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).rstrip("/")

    @field_validator("alpaca_data_feed", mode="after")
    @classmethod
    def normalize_feed(cls, value: str) -> str:
        return value.lower()

    @model_validator(mode="after")
    def validate_controls(self) -> LabSettings:
        if self.live_trading:
            raise ValueError(
                "LIVE_TRADING=true is refused. "
                f"{PAPER_ONLY_LOCK_MESSAGE}"
            )
        if not self.alpaca_paper_trade:
            raise ValueError(
                "ALPACA_PAPER_TRADE=false is refused. "
                f"{PAPER_ONLY_LOCK_MESSAGE}"
            )
        if self.allow_live_base_url_override:
            raise ValueError(
                "ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE is reserved for a future internal design "
                "and is disabled in this repo. "
                f"{PAPER_ONLY_LOCK_MESSAGE}"
            )
        if self.max_notional_per_trade <= 0:
            raise ValueError("MAX_NOTIONAL_PER_TRADE must be positive.")
        if self.max_open_positions <= 0:
            raise ValueError("MAX_OPEN_POSITIONS must be positive.")
        if self.max_orders_per_run <= 0:
            raise ValueError("MAX_ORDERS_PER_RUN must be positive.")
        if self.request_timeout_seconds <= 0:
            raise ValueError("REQUEST_TIMEOUT_SECONDS must be positive.")
        if self.retry_attempts < 1:
            raise ValueError("RETRY_ATTEMPTS must be at least 1.")
        if self.alpaca_api_base_url:
            normalized = self.alpaca_api_base_url.lower()
            if normalized == LIVE_TRADING_BASE_URL:
                raise ValueError(
                    "APCA_API_BASE_URL may not point to https://api.alpaca.markets. "
                    f"{PAPER_ONLY_LOCK_MESSAGE}"
                )
            if normalized != PAPER_TRADING_BASE_URL:
                raise ValueError(
                    "APCA_API_BASE_URL must point to https://paper-api.alpaca.markets or be "
                    "left unset. Live and custom trading endpoints are disabled here. "
                    f"{PAPER_ONLY_LOCK_MESSAGE}"
                )
        return self

    @property
    def raw_data_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def bronze_data_dir(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_data_dir(self) -> Path:
        return self.data_root / "silver"

    @property
    def feature_data_dir(self) -> Path:
        return self.data_root / "features"

    @property
    def trading_api_base_url(self) -> str:
        return self.alpaca_api_base_url or PAPER_TRADING_BASE_URL

    @property
    def data_api_base_url(self) -> str:
        return DATA_API_BASE_URL

    @property
    def trading_mode(self) -> str:
        return "paper"

    def assert_paper_only_runtime(self) -> None:
        if self.live_trading:
            raise LiveTradingRefusedError(
                "LIVE_TRADING=true is refused at runtime. "
                f"{PAPER_ONLY_LOCK_MESSAGE}"
            )
        if not self.alpaca_paper_trade:
            raise LiveTradingRefusedError(
                "ALPACA_PAPER_TRADE must remain true at runtime. "
                f"{PAPER_ONLY_LOCK_MESSAGE}"
            )
        if self.allow_live_base_url_override:
            raise LiveTradingRefusedError(
                "ALPACA_ALLOW_LIVE_BASE_URL_OVERRIDE is disabled in this repo. "
                f"{PAPER_ONLY_LOCK_MESSAGE}"
            )
        if self.trading_api_base_url != PAPER_TRADING_BASE_URL:
            raise LiveTradingRefusedError(
                "Trading API base URL must remain https://paper-api.alpaca.markets. "
                f"{PAPER_ONLY_LOCK_MESSAGE}"
            )

    def auth_headers(self) -> dict[str, str]:
        if self.alpaca_api_key is None or self.alpaca_secret_key is None:
            raise ValueError(
                "Alpaca credentials are missing. Populate either "
                "ALPACA_API_KEY/ALPACA_SECRET_KEY or APCA_API_KEY_ID/APCA_API_SECRET_KEY."
            )
        return {
            "APCA-API-KEY-ID": self.alpaca_api_key.get_secret_value(),
            "APCA-API-SECRET-KEY": self.alpaca_secret_key.get_secret_value(),
        }

    def require_destructive_broker_action(
        self,
        *,
        action: str,
        explicitly_requested: bool,
        requested_live: bool,
    ) -> None:
        if requested_live:
            raise LiveTradingRefusedError(
                f"{action} requested live routing, but "
                "live trading is permanently refused in this repo."
            )
        self.assert_paper_only_runtime()
        if not explicitly_requested:
            raise BrokerActionBlockedError(
                f"{action} is blocked until the call is explicitly "
                "marked as paper-trading approved."
            )

    def redacted(self) -> dict[str, Any]:
        return {
            "paper_only_repo_lock": True,
            "alpaca_paper_trade": self.alpaca_paper_trade,
            "alpaca_api_base_url": self.trading_api_base_url,
            "allow_live_base_url_override": self.allow_live_base_url_override,
            "alpaca_data_feed": self.alpaca_data_feed,
            "default_underlyings": list(self.default_underlyings),
            "data_root": str(self.data_root),
            "reports_root": str(self.reports_root),
            "log_level": self.log_level,
            "live_trading": self.live_trading,
            "max_notional_per_trade": self.max_notional_per_trade,
            "max_open_positions": self.max_open_positions,
            "max_orders_per_run": self.max_orders_per_run,
            "dry_run": self.dry_run,
            "request_timeout_seconds": self.request_timeout_seconds,
            "retry_attempts": self.retry_attempts,
            "alpaca_api_key": "set" if self.alpaca_api_key else "missing",
            "alpaca_secret_key": "set" if self.alpaca_secret_key else "missing",
        }


def _load_yaml_config(config_file: Path | None) -> dict[str, Any]:
    path = config_file or DEFAULT_CONFIG_PATH
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Config file {path} must contain a top-level mapping.")
    return payload


def _resolve_env_aliases(source: Mapping[str, Any]) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for field_name, env_names in ENV_ALIASES.items():
        for env_name in env_names:
            if env_name in source:
                resolved[field_name] = source[env_name]
                break
    return resolved


def _load_process_env_overrides() -> dict[str, Any]:
    return _resolve_env_aliases(os.environ)


def _load_env_file_overrides(env_file: Path | None) -> dict[str, Any]:
    if env_file is None or not env_file.exists():
        return {}
    raw_values = {key: value for key, value in dotenv_values(env_file).items() if value is not None}
    return _resolve_env_aliases(raw_values)


def load_settings(
    *,
    env_file: str | Path | None = None,
    config_file: str | Path | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> LabSettings:
    """Load settings with explicit precedence.

    Precedence is:
    1. explicit process environment variables
    2. the selected env file
    3. config-file defaults
    4. explicit `overrides`

    The env file is parsed directly and never mutates ``os.environ``. When
    ``env_file`` is provided, only that file is consulted; the repo-root `.env`
    is not read as a fallback.
    """
    env_path = ROOT_DIR / ".env" if env_file is None else Path(env_file)

    merged: dict[str, Any] = {}
    merged.update(_load_yaml_config(Path(config_file) if config_file else None))
    merged.update(_load_env_file_overrides(env_path))
    merged.update(_load_process_env_overrides())
    if overrides:
        merged.update(dict(overrides))
    return LabSettings.model_validate(merged)


@lru_cache(maxsize=1)
def get_settings() -> LabSettings:
    return load_settings()

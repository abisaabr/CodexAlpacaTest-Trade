from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrategyLegConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    option_type: Literal["call", "put"]
    side: Literal["long", "short"]
    target_delta: float
    min_abs_delta: float = 0.05
    max_abs_delta: float = 0.95


class StrategyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    underlying_symbol: str
    regime: Literal["bull", "bear", "choppy"]
    family: str
    description: str
    dte_mode: Literal["same_day", "next_expiry"]
    signal_name: Literal[
        "orb_call",
        "orb_put",
        "trend_call",
        "trend_put",
        "credit_bull",
        "credit_bear",
        "long_straddle",
        "iron_condor",
    ]
    timing_profile: Literal["fast", "base", "slow"] = "base"
    hard_exit_minute: int
    risk_fraction: float
    max_contracts: int
    profit_target_multiple: float
    stop_loss_multiple: float
    legs: tuple[StrategyLegConfig, ...]

    @field_validator("underlying_symbol", mode="before")
    @classmethod
    def normalize_underlying_symbol(cls, value: object) -> str:
        return str(value).strip().upper()


class RiskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sleeve_starting_equity: float = 25_000.0
    max_open_risk_fraction: float = 0.15
    daily_loss_gate_pct: float = 0.02
    delever_drawdown_pct: float = 12.0
    delever_risk_scale: float = 0.75
    max_open_positions: int = 10
    max_positions_per_regime: int = 6
    max_positions_per_symbol: int = 3
    min_required_buying_power: float = 7_500.0
    soft_alert_delta_shares: float = 1_500.0
    soft_alert_vega_dollars_1pct: float = 350.0


class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    underlying_symbols: tuple[str, ...] = ("QQQ", "SPY", "IWM", "NVDA", "TSLA", "MSFT")
    option_feed: str = "indicative"
    stock_feed: str | None = None
    submit_paper_orders: bool = True
    poll_interval_seconds: int = 20
    order_status_poll_seconds: int = 10
    order_fill_timeout_seconds: int = 45
    contract_refresh_minutes: int = 15
    quote_stale_seconds: int = 120
    stock_freshness_seconds: int = 180
    max_relative_spread: float = 0.35
    max_dte_days: int = 7
    state_root: Path = Path("reports/multi_ticker_portfolio/state")
    run_root: Path = Path("reports/multi_ticker_portfolio/runs")
    task_name: str = "Multi-Ticker Portfolio Paper Trader"
    allow_market_exit_fallback: bool = True
    market_exit_fallback_minute: int = 385
    startup_lead_minutes: int = 10
    midday_report_minute: int = 180

    @field_validator("underlying_symbols", mode="before")
    @classmethod
    def normalize_underlying_symbols(cls, value: object) -> tuple[str, ...]:
        if isinstance(value, str):
            items = [item.strip().upper() for item in value.split(",") if item.strip()]
            return tuple(items)
        if isinstance(value, (list, tuple, set)):
            return tuple(str(item).strip().upper() for item in value if str(item).strip())
        raise TypeError("underlying_symbols must be a comma-separated string or sequence")

    @field_validator("state_root", "run_root", mode="before")
    @classmethod
    def normalize_path(cls, value: object) -> Path:
        return Path(str(value))


class MultiTickerPortfolioConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = "multi_ticker_portfolio_paper_trader"
    description: str = (
        "Shared-account intraday options paper portfolio across QQQ, SPY, IWM, NVDA, TSLA, and MSFT "
        "using the validated 365-day bull and bear winners."
    )
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    strategies: tuple[StrategyConfig, ...]

    @property
    def strategies_by_name(self) -> dict[str, StrategyConfig]:
        return {strategy.name: strategy for strategy in self.strategies}

    @property
    def strategies_by_symbol(self) -> dict[str, list[StrategyConfig]]:
        grouped: dict[str, list[StrategyConfig]] = {}
        for strategy in self.strategies:
            grouped.setdefault(strategy.underlying_symbol, []).append(strategy)
        return grouped


def _base_strategy_map() -> dict[str, dict[str, object]]:
    return {
        "trend_long_call_next_expiry": {
            "regime": "bull",
            "family": "Single-leg long call",
            "description": "Buy the next-expiry call closest to +0.60 delta on upside trend continuation.",
            "dte_mode": "next_expiry",
            "signal_name": "trend_call",
            "hard_exit_minute": 360,
            "risk_fraction": 0.05,
            "max_contracts": 6,
            "profit_target_multiple": 0.45,
            "stop_loss_multiple": 0.30,
            "legs": (
                StrategyLegConfig(option_type="call", side="long", target_delta=0.60),
            ),
        },
        "trend_long_put_next_expiry": {
            "regime": "bear",
            "family": "Single-leg long put",
            "description": "Buy the next-expiry put closest to -0.60 delta on downside trend continuation.",
            "dte_mode": "next_expiry",
            "signal_name": "trend_put",
            "hard_exit_minute": 360,
            "risk_fraction": 0.05,
            "max_contracts": 6,
            "profit_target_multiple": 0.45,
            "stop_loss_multiple": 0.30,
            "legs": (
                StrategyLegConfig(option_type="put", side="long", target_delta=-0.60),
            ),
        },
        "orb_long_put_same_day": {
            "regime": "bear",
            "family": "Single-leg long put",
            "description": "Buy the same-day put closest to -0.50 delta on a confirmed opening-range breakdown.",
            "dte_mode": "same_day",
            "signal_name": "orb_put",
            "hard_exit_minute": 375,
            "risk_fraction": 0.05,
            "max_contracts": 8,
            "profit_target_multiple": 0.50,
            "stop_loss_multiple": 0.35,
            "legs": (
                StrategyLegConfig(option_type="put", side="long", target_delta=-0.50),
            ),
        },
    }


def _selected_strategy_specs() -> tuple[tuple[str, str, str], ...]:
    return (
        ("QQQ", "fast", "trend_long_call_next_expiry"),
        ("QQQ", "slow", "trend_long_call_next_expiry"),
        ("QQQ", "fast", "trend_long_put_next_expiry"),
        ("QQQ", "slow", "orb_long_put_same_day"),
        ("SPY", "fast", "trend_long_call_next_expiry"),
        ("SPY", "base", "trend_long_put_next_expiry"),
        ("SPY", "fast", "trend_long_put_next_expiry"),
        ("IWM", "fast", "trend_long_call_next_expiry"),
        ("IWM", "slow", "trend_long_call_next_expiry"),
        ("IWM", "fast", "trend_long_put_next_expiry"),
        ("IWM", "base", "trend_long_put_next_expiry"),
        ("NVDA", "fast", "trend_long_call_next_expiry"),
        ("NVDA", "base", "trend_long_put_next_expiry"),
        ("TSLA", "base", "trend_long_call_next_expiry"),
        ("TSLA", "base", "trend_long_put_next_expiry"),
        ("TSLA", "fast", "trend_long_put_next_expiry"),
        ("MSFT", "fast", "trend_long_call_next_expiry"),
        ("MSFT", "base", "trend_long_call_next_expiry"),
        ("MSFT", "slow", "trend_long_call_next_expiry"),
        ("MSFT", "base", "trend_long_put_next_expiry"),
        ("MSFT", "slow", "trend_long_put_next_expiry"),
    )


def _default_strategies() -> tuple[StrategyConfig, ...]:
    base_map = _base_strategy_map()
    strategies: list[StrategyConfig] = []
    for underlying_symbol, timing_profile, base_name in _selected_strategy_specs():
        template = base_map[base_name]
        strategies.append(
            StrategyConfig(
                name=f"{underlying_symbol.lower()}__{timing_profile}__{base_name}",
                underlying_symbol=underlying_symbol,
                regime=template["regime"],
                family=template["family"],
                description=f"{underlying_symbol} [{timing_profile}] {template['description']}",
                dte_mode=template["dte_mode"],
                signal_name=template["signal_name"],
                timing_profile=timing_profile,
                hard_exit_minute=template["hard_exit_minute"],
                risk_fraction=template["risk_fraction"],
                max_contracts=template["max_contracts"],
                profit_target_multiple=template["profit_target_multiple"],
                stop_loss_multiple=template["stop_loss_multiple"],
                legs=template["legs"],
            )
        )
    return tuple(strategies)


def default_portfolio_config() -> MultiTickerPortfolioConfig:
    return MultiTickerPortfolioConfig(strategies=_default_strategies())


def load_portfolio_config(path: str | Path | None = None) -> MultiTickerPortfolioConfig:
    if path is None:
        return default_portfolio_config()
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Portfolio config not found: {config_path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("Portfolio config must contain a top-level mapping.")
    if "strategies" not in payload:
        payload["strategies"] = [strategy.model_dump() for strategy in _default_strategies()]
    return MultiTickerPortfolioConfig.model_validate(payload)

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
    regime: Literal["bull", "bear", "choppy"]
    family: str
    description: str
    dte_mode: Literal["same_day", "next_expiry"]
    signal_name: Literal["orb_call", "orb_put", "trend_call", "trend_put", "iron_condor"]
    hard_exit_minute: int
    risk_fraction: float
    max_contracts: int
    profit_target_multiple: float
    stop_loss_multiple: float
    legs: tuple[StrategyLegConfig, ...]


class RiskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sleeve_starting_equity: float = 25_000.0
    max_open_risk_fraction: float = 0.15
    daily_loss_gate_pct: float = 0.02
    delever_drawdown_pct: float = 12.0
    delever_risk_scale: float = 0.75
    max_open_positions: int = 3
    max_positions_per_regime: int = 2
    soft_alert_delta_shares: float = 838.78
    soft_alert_vega_dollars_1pct: float = 171.42


class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    underlying_symbol: str = "QQQ"
    option_feed: str = "indicative"
    stock_feed: str | None = None
    submit_paper_orders: bool = True
    poll_interval_seconds: int = 20
    order_status_poll_seconds: int = 10
    order_fill_timeout_seconds: int = 45
    contract_refresh_minutes: int = 15
    quote_stale_seconds: int = 120
    max_relative_spread: float = 0.35
    max_dte_days: int = 7
    state_root: Path = Path("reports/qqq_portfolio/state")
    run_root: Path = Path("reports/qqq_portfolio/runs")
    task_name: str = "QQQ Portfolio Paper Trader"
    allow_market_exit_fallback: bool = True
    market_exit_fallback_minute: int = 385
    startup_lead_minutes: int = 10

    @field_validator("underlying_symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: object) -> str:
        return str(value).strip().upper()

    @field_validator("state_root", "run_root", mode="before")
    @classmethod
    def normalize_path(cls, value: object) -> Path:
        return Path(str(value))


class QQQPortfolioConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = "qqq_portfolio_paper_trader"
    description: str = "Causal intraday QQQ options paper portfolio built from the validated bull, bear, and choppy sleeves."
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    strategies: tuple[StrategyConfig, ...]

    @property
    def strategies_by_name(self) -> dict[str, StrategyConfig]:
        return {strategy.name: strategy for strategy in self.strategies}


def _default_strategies() -> tuple[StrategyConfig, ...]:
    return (
        StrategyConfig(
            name="trend_long_call_next_expiry",
            regime="bull",
            family="Single-leg long call",
            description="Buy the next-expiry call closest to +0.60 delta on upside trend continuation.",
            dte_mode="next_expiry",
            signal_name="trend_call",
            hard_exit_minute=360,
            risk_fraction=0.05,
            max_contracts=6,
            profit_target_multiple=0.45,
            stop_loss_multiple=0.30,
            legs=(StrategyLegConfig(option_type="call", side="long", target_delta=0.60),),
        ),
        StrategyConfig(
            name="bull_call_spread_next_expiry",
            regime="bull",
            family="Debit call spread",
            description="Buy a next-expiry bull call spread targeting +0.55 and +0.30 deltas.",
            dte_mode="next_expiry",
            signal_name="trend_call",
            hard_exit_minute=360,
            risk_fraction=0.06,
            max_contracts=8,
            profit_target_multiple=0.40,
            stop_loss_multiple=0.28,
            legs=(
                StrategyLegConfig(option_type="call", side="long", target_delta=0.55),
                StrategyLegConfig(option_type="call", side="short", target_delta=0.30),
            ),
        ),
        StrategyConfig(
            name="orb_long_call_same_day",
            regime="bull",
            family="Single-leg long call",
            description="Buy the same-day call closest to +0.50 delta on a confirmed opening-range breakout.",
            dte_mode="same_day",
            signal_name="orb_call",
            hard_exit_minute=375,
            risk_fraction=0.05,
            max_contracts=8,
            profit_target_multiple=0.50,
            stop_loss_multiple=0.35,
            legs=(StrategyLegConfig(option_type="call", side="long", target_delta=0.50),),
        ),
        StrategyConfig(
            name="trend_long_put_next_expiry",
            regime="bear",
            family="Single-leg long put",
            description="Buy the next-expiry put closest to -0.60 delta on downside trend continuation.",
            dte_mode="next_expiry",
            signal_name="trend_put",
            hard_exit_minute=360,
            risk_fraction=0.05,
            max_contracts=6,
            profit_target_multiple=0.45,
            stop_loss_multiple=0.30,
            legs=(StrategyLegConfig(option_type="put", side="long", target_delta=-0.60),),
        ),
        StrategyConfig(
            name="bear_put_spread_next_expiry",
            regime="bear",
            family="Debit put spread",
            description="Buy a next-expiry bear put spread targeting -0.55 and -0.30 deltas.",
            dte_mode="next_expiry",
            signal_name="trend_put",
            hard_exit_minute=360,
            risk_fraction=0.06,
            max_contracts=8,
            profit_target_multiple=0.40,
            stop_loss_multiple=0.28,
            legs=(
                StrategyLegConfig(option_type="put", side="long", target_delta=-0.55),
                StrategyLegConfig(option_type="put", side="short", target_delta=-0.30),
            ),
        ),
        StrategyConfig(
            name="orb_long_put_same_day",
            regime="bear",
            family="Single-leg long put",
            description="Buy the same-day put closest to -0.50 delta on a confirmed opening-range breakdown.",
            dte_mode="same_day",
            signal_name="orb_put",
            hard_exit_minute=375,
            risk_fraction=0.05,
            max_contracts=8,
            profit_target_multiple=0.50,
            stop_loss_multiple=0.35,
            legs=(StrategyLegConfig(option_type="put", side="long", target_delta=-0.50),),
        ),
        StrategyConfig(
            name="iron_condor_same_day",
            regime="choppy",
            family="Iron condor",
            description="Sell a same-day iron condor around +/-0.25 delta when early trade is narrow and mean-reverting.",
            dte_mode="same_day",
            signal_name="iron_condor",
            hard_exit_minute=375,
            risk_fraction=0.04,
            max_contracts=6,
            profit_target_multiple=0.40,
            stop_loss_multiple=1.25,
            legs=(
                StrategyLegConfig(option_type="call", side="short", target_delta=0.25),
                StrategyLegConfig(option_type="call", side="long", target_delta=0.10),
                StrategyLegConfig(option_type="put", side="short", target_delta=-0.25),
                StrategyLegConfig(option_type="put", side="long", target_delta=-0.10),
            ),
        ),
    )


def default_portfolio_config() -> QQQPortfolioConfig:
    return QQQPortfolioConfig(strategies=_default_strategies())


def load_portfolio_config(path: str | Path | None = None) -> QQQPortfolioConfig:
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
    return QQQPortfolioConfig.model_validate(payload)

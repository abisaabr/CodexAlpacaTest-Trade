from __future__ import annotations

import os
from datetime import date as dt_date
from pathlib import Path
from typing import Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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
        "governed_breakout_call",
        "governed_breakout_put",
        "governed_lower_band_reversion_call",
    ]
    timing_profile: Literal["reactive", "fast", "base", "slow", "patient", "governed_late"] = "base"
    hard_exit_minute: int
    risk_fraction: float
    max_contracts: int
    profit_target_multiple: float
    stop_loss_multiple: float
    legs: tuple[StrategyLegConfig, ...]
    candidate_variant_id: str | None = None
    source_strategy_id: str | None = None
    promotion_manifest_path: str | None = None
    governed_validation_packet_uri: str | None = None
    promotion_status: str | None = None
    min_fill_coverage: float | None = None
    min_data_foundation_coverage: float | None = None
    min_option_trade_count: int | None = None
    min_net_pnl: float | None = None
    min_test_net_pnl: float | None = None
    profit_factor: float | None = None
    research_profile: str | None = None
    research_entry_timing_mode: str | None = None
    research_entry_offset_minutes: int | None = None
    research_exit_offset_minutes: int | None = None
    runner_semantics_status: str | None = None
    stock_proxy_mode: Literal["breakout", "range_bound"] | None = None
    entry_signal_mode: Literal["continuous", "daily_first", "rising_edge"] | None = None
    min_minutes_since_open: int | None = None
    max_minutes_since_open: int | None = None
    min_trend_gap_pct: float | None = None
    max_trend_gap_pct: float | None = None
    min_range_pct: float | None = None
    max_range_pct: float | None = None
    max_midpoint_distance_pct: float | None = None
    range_entry_side: Literal["center", "lower_band", "upper_band"] | None = None
    range_edge_pct: float | None = None
    liquidity_gate: Literal["tight", "loose"] | None = None
    option_exit_mode: Literal["premium_target_stop"] | None = None
    option_exit_profile: str | None = None
    option_profit_target_pct: float | None = None
    option_stop_loss_pct: float | None = None
    min_option_hold_minutes: int | None = None
    runner_hard_exit_mode: Literal["absolute_minute", "minutes_after_entry"] | None = None

    @field_validator("underlying_symbol", mode="before")
    @classmethod
    def normalize_underlying_symbol(cls, value: object) -> str:
        return str(value).strip().upper()


class RiskBucketConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    symbols: tuple[str, ...]
    max_open_risk_fraction: float

    @field_validator("symbols", mode="before")
    @classmethod
    def normalize_symbols(cls, value: object) -> tuple[str, ...]:
        if isinstance(value, str):
            items = [item.strip().upper() for item in value.split(",") if item.strip()]
            return tuple(items)
        if isinstance(value, (list, tuple, set)):
            return tuple(str(item).strip().upper() for item in value if str(item).strip())
        raise TypeError("symbols must be a comma-separated string or sequence")


class EventBlackoutConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    reason: str
    start_date: dt_date
    end_date: dt_date | None = None
    start_minute: int = 0
    end_minute: int = 390
    symbols: tuple[str, ...] = ()
    regimes: tuple[Literal["bull", "bear", "choppy"], ...] = ()
    timing_profiles: tuple[Literal["reactive", "fast", "base", "slow", "patient"], ...] = ()
    dte_modes: tuple[Literal["same_day", "next_expiry"], ...] = ()
    enabled: bool = True

    @field_validator("symbols", mode="before")
    @classmethod
    def normalize_symbols(cls, value: object) -> tuple[str, ...]:
        if value in (None, "", []):
            return ()
        if isinstance(value, str):
            items = [item.strip().upper() for item in value.split(",") if item.strip()]
            return tuple(items)
        if isinstance(value, (list, tuple, set)):
            return tuple(str(item).strip().upper() for item in value if str(item).strip())
        raise TypeError("symbols must be a comma-separated string or sequence")

    @field_validator("regimes", "timing_profiles", "dte_modes", mode="before")
    @classmethod
    def normalize_tuple_strs(cls, value: object) -> tuple[str, ...]:
        if value in (None, "", []):
            return ()
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",") if item.strip()]
            return tuple(items)
        if isinstance(value, (list, tuple, set)):
            return tuple(str(item).strip() for item in value if str(item).strip())
        raise TypeError("value must be a comma-separated string or sequence")


class RiskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sleeve_starting_equity: float = 25_000.0
    max_open_risk_fraction: float = 0.15
    daily_loss_gate_pct: float | None = None
    delever_drawdown_pct: float = 8.0
    delever_risk_scale: float = 0.50
    max_open_positions: int = 10
    max_positions_per_regime: int = 10
    max_positions_per_symbol: int = 3
    entry_cluster_window_minutes: int | None = 15
    max_positions_per_regime_window: int | None = 3
    max_positions_per_bucket_regime_window: int | None = 2
    max_open_risk_fraction_per_symbol: float | None = 0.05
    regime_risk_scales: dict[str, float] = Field(default_factory=dict)
    bucket_caps: tuple[RiskBucketConfig, ...] = Field(
        default_factory=lambda: (
            RiskBucketConfig(
                name="index_beta",
                symbols=("QQQ", "SPY", "IWM"),
                max_open_risk_fraction=0.08,
            ),
            RiskBucketConfig(
                name="growth_tech",
                symbols=("NVDA", "TSLA", "MSFT", "AMZN", "ORCL", "SHOP", "CRM", "PLTR", "ARKK"),
                max_open_risk_fraction=0.09,
            ),
            RiskBucketConfig(
                name="metals_energy",
                symbols=("GLD", "GDX", "SLV", "XLE", "XOM"),
                max_open_risk_fraction=0.08,
            ),
            RiskBucketConfig(
                name="financials",
                symbols=("BAC", "JPM", "SCHW"),
                max_open_risk_fraction=0.06,
            ),
        )
    )
    min_required_buying_power: float = 7_500.0
    broker_min_equity_to_trade: float | None = 26_000.0
    broker_equity_emergency_stop: float | None = 25_500.0
    severe_loss_halt_new_entries_pct: float | None = 0.035
    severe_loss_flatten_all_pct: float | None = 0.05
    soft_alert_delta_shares: float = 3_200.0
    soft_alert_vega_dollars_1pct: float = 620.0
    hard_cap_delta_shares: float | None = 4_000.0
    hard_cap_vega_dollars_1pct: float | None = 750.0
    entry_failure_streak_limit: int | None = 3
    entry_adverse_slippage_fraction_limit: float | None = 0.20
    entry_adverse_slippage_lookback: int = 4
    entry_cutoff_minute: int | None = 345
    same_day_entry_cutoff_minute: int | None = 300
    event_blackouts: tuple[EventBlackoutConfig, ...] = Field(default_factory=tuple)

    @field_validator("regime_risk_scales", mode="before")
    @classmethod
    def normalize_regime_risk_scales(cls, value: object) -> dict[str, float]:
        if value in (None, "", []):
            return {}
        if not isinstance(value, dict):
            raise TypeError("regime_risk_scales must be a mapping of regime to positive scale")
        normalized: dict[str, float] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key).strip().lower()
            if not key:
                continue
            if key not in {"bull", "bear", "choppy"}:
                raise ValueError("regime_risk_scales keys must be bull, bear, or choppy")
            scale = float(raw_value)
            if scale <= 0.0:
                raise ValueError("regime_risk_scales values must be positive")
            normalized[key] = scale
        return normalized


class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    underlying_symbols: tuple[str, ...] = (
        "QQQ",
        "SPY",
        "IWM",
        "NVDA",
        "TSLA",
        "MSFT",
        "BAC",
        "PLTR",
        "GLD",
        "ARKK",
        "XLE",
        "GDX",
        "SLV",
        "AMZN",
        "JPM",
        "XOM",
        "ORCL",
        "SHOP",
        "CRM",
        "SCHW",
        "NKE",
    )
    option_feed: str = "indicative"
    stock_feed: str | None = None
    submit_paper_orders: bool = True
    paper_order_arming_mode: Literal["cli_flag_only", "config_explicit"] = "cli_flag_only"
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
    eod_flatten_minutes_before_close: tuple[int, ...] = (10, 2)
    auto_flatten_unexpected_positions: bool = True
    unexpected_position_cleanup_timeout_seconds: int = 45

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

    @field_validator("eod_flatten_minutes_before_close", mode="before")
    @classmethod
    def normalize_eod_flatten_minutes_before_close(cls, value: object) -> tuple[int, ...]:
        if value in (None, "", []):
            return ()
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",") if item.strip()]
            return tuple(int(item) for item in items)
        if isinstance(value, (list, tuple, set)):
            return tuple(int(item) for item in value)
        raise TypeError("eod_flatten_minutes_before_close must be a comma-separated string or sequence")

    @field_validator("eod_flatten_minutes_before_close")
    @classmethod
    def validate_eod_flatten_minutes_before_close(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        normalized = tuple(sorted({int(item) for item in value}, reverse=True))
        for minutes_before_close in normalized:
            if minutes_before_close <= 0 or minutes_before_close >= 390:
                raise ValueError("eod_flatten_minutes_before_close values must be between 1 and 389")
        return normalized


class OwnershipConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    lease_backend: Literal["file", "gcs_generation_match"] = "file"
    lease_path: Path = Path("reports/multi_ticker_portfolio/state/ownership_lease.json")
    gcs_lease_uri: str | None = None
    lease_ttl_seconds: int = 180
    machine_label: str | None = None

    @field_validator("lease_path", mode="before")
    @classmethod
    def normalize_path(cls, value: object) -> Path:
        return Path(str(value))

    @model_validator(mode="after")
    def validate_backend_requirements(self) -> OwnershipConfig:
        if self.lease_backend == "gcs_generation_match" and not self.gcs_lease_uri:
            raise ValueError(
                "ownership.gcs_lease_uri is required when ownership.lease_backend is "
                "'gcs_generation_match'."
            )
        return self


class MultiTickerPortfolioConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = "multi_ticker_portfolio_paper_trader"
    description: str = (
        "Shared-account intraday options paper portfolio across QQQ, SPY, IWM, NVDA, TSLA, MSFT, BAC, "
        "PLTR, GLD, ARKK, XLE, GDX, SLV, AMZN, JPM, XOM, ORCL, SHOP, CRM, SCHW, and NKE using the validated "
        "shared-account winners."
    )
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    ownership: OwnershipConfig = Field(default_factory=OwnershipConfig)
    strategy_manifest_path: Path | None = None
    strategy_manifest_paths: tuple[Path, ...] = ()
    strategies: tuple[StrategyConfig, ...]

    @field_validator("strategy_manifest_path", mode="before")
    @classmethod
    def normalize_strategy_manifest_path(cls, value: object) -> Path | None:
        if value in (None, ""):
            return None
        return Path(str(value))

    @field_validator("strategy_manifest_paths", mode="before")
    @classmethod
    def normalize_strategy_manifest_paths(cls, value: object) -> tuple[Path, ...]:
        if value in (None, "", []):
            return ()
        if isinstance(value, str):
            return tuple(Path(item.strip()) for item in value.split(",") if item.strip())
        if isinstance(value, (list, tuple, set)):
            return tuple(Path(str(item)) for item in value if str(item).strip())
        raise TypeError("strategy_manifest_paths must be a string or sequence")

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
        "orb_long_call_same_day": {
            "regime": "bull",
            "family": "Single-leg long call",
            "description": "Buy the same-day call closest to +0.50 delta on a confirmed opening-range breakout.",
            "dte_mode": "same_day",
            "signal_name": "orb_call",
            "hard_exit_minute": 375,
            "risk_fraction": 0.05,
            "max_contracts": 8,
            "profit_target_multiple": 0.50,
            "stop_loss_multiple": 0.35,
            "legs": (
                StrategyLegConfig(option_type="call", side="long", target_delta=0.50),
            ),
        },
    }


def _selected_strategy_specs() -> tuple[dict[str, object], ...]:
    return (
        {"underlying_symbol": "QQQ", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "QQQ", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "QQQ", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "QQQ", "timing_profile": "slow", "base_name": "orb_long_put_same_day"},
        {"underlying_symbol": "SPY", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "SPY", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "SPY", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "IWM", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "IWM", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "IWM", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "NVDA", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "NVDA", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "TSLA", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "TSLA", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "TSLA", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "MSFT", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "MSFT", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "MSFT", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "MSFT", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "MSFT", "timing_profile": "slow", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "BAC", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "BAC", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "PLTR", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "PLTR", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "PLTR", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "PLTR", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "GLD", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "GLD", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "GLD", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "ARKK", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "ARKK", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "ARKK", "timing_profile": "fast", "base_name": "orb_long_call_same_day"},
        {"underlying_symbol": "ARKK", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "XLE", "timing_profile": "slow", "base_name": "orb_long_call_same_day"},
        {"underlying_symbol": "XLE", "timing_profile": "base", "base_name": "orb_long_call_same_day"},
        {"underlying_symbol": "XLE", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "XLE", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {
            "underlying_symbol": "XLE",
            "timing_profile": "base",
            "base_name": "orb_long_call_same_day",
            "regime": "choppy",
            "name": "xle__base__orb_long_call_same_day__choppy",
        },
        {"underlying_symbol": "GDX", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "GDX", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "GDX", "timing_profile": "slow", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "GDX", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "GDX", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "SLV", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "SLV", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "SLV", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "SLV", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "SLV", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "AMZN", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "AMZN", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "AMZN", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "AMZN", "timing_profile": "slow", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "AMZN", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "AMZN", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {
            "underlying_symbol": "AMZN",
            "timing_profile": "slow",
            "base_name": "orb_long_put_same_day",
            "regime": "choppy",
            "name": "amzn__slow__orb_long_put_same_day",
        },
        {"underlying_symbol": "JPM", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "JPM", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "JPM", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "JPM", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "JPM", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "JPM", "timing_profile": "slow", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "XOM", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "XOM", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "XOM", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "ORCL", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "ORCL", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "ORCL", "timing_profile": "slow", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "ORCL", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "SHOP", "timing_profile": "slow", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "SHOP", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "SHOP", "timing_profile": "base", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "SHOP", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "SHOP", "timing_profile": "slow", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "CRM", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "CRM", "timing_profile": "fast", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "CRM", "timing_profile": "slow", "base_name": "trend_long_put_next_expiry"},
        {"underlying_symbol": "CRM", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
        {
            "underlying_symbol": "CRM",
            "timing_profile": "base",
            "base_name": "orb_long_put_same_day",
            "regime": "choppy",
            "name": "crm__base__orb_long_put_same_day",
        },
        {"underlying_symbol": "SCHW", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {
            "underlying_symbol": "SCHW",
            "timing_profile": "base",
            "base_name": "orb_long_put_same_day",
            "regime": "choppy",
            "name": "schw__base__orb_long_put_same_day",
        },
        {"underlying_symbol": "NKE", "timing_profile": "fast", "base_name": "trend_long_call_next_expiry"},
        {"underlying_symbol": "NKE", "timing_profile": "base", "base_name": "trend_long_put_next_expiry"},
    )


def _default_strategies() -> tuple[StrategyConfig, ...]:
    base_map = _base_strategy_map()
    strategies: list[StrategyConfig] = []
    for spec in _selected_strategy_specs():
        underlying_symbol = str(spec["underlying_symbol"])
        timing_profile = str(spec["timing_profile"])
        base_name = str(spec["base_name"])
        template = base_map[base_name]
        regime = str(spec.get("regime", template["regime"]))
        name = str(spec.get("name", f"{underlying_symbol.lower()}__{timing_profile}__{base_name}"))
        description = f"{underlying_symbol} [{timing_profile}] {template['description']}"
        if regime != template["regime"]:
            description = f"{description} [{regime}]"
        strategies.append(
            StrategyConfig(
                name=name,
                underlying_symbol=underlying_symbol,
                regime=regime,  # type: ignore[arg-type]
                family=template["family"],
                description=description,
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
    default_config_path = _default_portfolio_config_path()
    if default_config_path.exists():
        return load_portfolio_config(default_config_path)
    payload: dict[str, object] = {}
    default_manifest_path = _default_strategy_manifest_path()
    if default_manifest_path.exists():
        payload["strategy_manifest_path"] = default_manifest_path
    payload["strategies"] = _resolve_strategy_payloads(payload, config_path=None)
    return MultiTickerPortfolioConfig.model_validate(payload)


def _deep_merge(base: dict[str, object], overlay: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key, value in overlay.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _default_strategy_manifest_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "strategy_manifests" / "multi_ticker_portfolio_live.yaml"


def _default_portfolio_config_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "multi_ticker_paper_portfolio.yaml"


def _load_strategy_manifest_payload(manifest_path: Path) -> list[dict[str, object]]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"Strategy manifest not found: {manifest_path}")
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    if isinstance(payload, dict):
        strategies = payload.get("strategies")
    elif isinstance(payload, list):
        strategies = payload
    else:
        raise ValueError("Strategy manifest must contain a top-level mapping or list.")
    if not isinstance(strategies, list):
        raise ValueError("Strategy manifest must contain a top-level 'strategies' list.")
    return strategies


def _resolve_strategy_payloads(
    payload: dict[str, object],
    *,
    config_path: Path | None,
) -> list[dict[str, object]]:
    strategies_value = payload.get("strategies")
    if strategies_value is not None:
        if not isinstance(strategies_value, list):
            raise ValueError("Strategies config must contain a list.")
        return strategies_value

    manifest_paths_value = payload.get("strategy_manifest_paths")
    if manifest_paths_value not in (None, "", []):
        if payload.get("strategy_manifest_path") not in (None, ""):
            raise ValueError(
                "Use either strategy_manifest_path or strategy_manifest_paths, not both."
            )
        if isinstance(manifest_paths_value, str):
            manifest_path_values = [
                item.strip() for item in manifest_paths_value.split(",") if item.strip()
            ]
        elif isinstance(manifest_paths_value, (list, tuple, set)):
            manifest_path_values = [
                str(item).strip() for item in manifest_paths_value if str(item).strip()
            ]
        else:
            raise TypeError("strategy_manifest_paths must be a string or sequence")
        if not manifest_path_values:
            raise ValueError("strategy_manifest_paths must not be empty.")
        base_dir = config_path.parent if config_path is not None else Path(__file__).resolve().parents[2]
        resolved_manifest_paths: list[Path] = []
        strategy_payloads: list[dict[str, object]] = []
        for manifest_path_value in manifest_path_values:
            manifest_path = Path(manifest_path_value)
            if not manifest_path.is_absolute():
                manifest_path = (base_dir / manifest_path).resolve()
            resolved_manifest_paths.append(manifest_path)
            strategy_payloads.extend(_load_strategy_manifest_payload(manifest_path))
        payload["strategy_manifest_paths"] = resolved_manifest_paths
        return strategy_payloads

    manifest_path_value = payload.get("strategy_manifest_path")
    manifest_path: Path | None = None
    if manifest_path_value not in (None, ""):
        manifest_path = Path(str(manifest_path_value))
        if not manifest_path.is_absolute():
            base_dir = config_path.parent if config_path is not None else Path(__file__).resolve().parents[2]
            manifest_path = (base_dir / manifest_path).resolve()
    elif config_path is None:
        default_manifest_path = _default_strategy_manifest_path()
        if default_manifest_path.exists():
            manifest_path = default_manifest_path

    if manifest_path is not None:
        payload["strategy_manifest_path"] = manifest_path
        return _load_strategy_manifest_payload(manifest_path)

    return [strategy.model_dump() for strategy in _default_strategies()]


def load_portfolio_config(path: str | Path | None = None) -> MultiTickerPortfolioConfig:
    if path is None:
        return default_portfolio_config()
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Portfolio config not found: {config_path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("Portfolio config must contain a top-level mapping.")
    risk_controls_path_value = payload.pop("risk_controls_path", None)
    if risk_controls_path_value:
        risk_controls_path = Path(str(risk_controls_path_value))
        if not risk_controls_path.is_absolute():
            risk_controls_path = config_path.parent / risk_controls_path
        if not risk_controls_path.exists():
            raise FileNotFoundError(f"Risk controls config not found: {risk_controls_path}")
        risk_payload = yaml.safe_load(risk_controls_path.read_text(encoding="utf-8")) or {}
        if not isinstance(risk_payload, dict):
            raise ValueError("Risk controls config must contain a top-level mapping.")
        payload = _deep_merge(payload, risk_payload)
    payload["strategies"] = _resolve_strategy_payloads(payload, config_path=config_path)
    repo_root = Path(__file__).resolve().parents[2]
    env_file_payload = dotenv_values(repo_root / ".env") if (repo_root / ".env").exists() else {}

    def _env_override(name: str) -> str | None:
        value = os.environ.get(name)
        if value not in (None, ""):
            return value
        raw = env_file_payload.get(name)
        if raw in (None, ""):
            return None
        return str(raw)

    ownership_payload = payload.setdefault("ownership", {})
    if not isinstance(ownership_payload, dict):
        raise ValueError("Ownership config must contain a top-level mapping.")
    if _env_override("MULTI_TICKER_OWNERSHIP_LEASE_PATH"):
        ownership_payload["lease_path"] = _env_override("MULTI_TICKER_OWNERSHIP_LEASE_PATH")
    if _env_override("MULTI_TICKER_OWNERSHIP_LEASE_BACKEND"):
        ownership_payload["lease_backend"] = _env_override("MULTI_TICKER_OWNERSHIP_LEASE_BACKEND")
    if _env_override("MULTI_TICKER_OWNERSHIP_GCS_LEASE_URI"):
        ownership_payload["gcs_lease_uri"] = _env_override("MULTI_TICKER_OWNERSHIP_GCS_LEASE_URI")
    if _env_override("MULTI_TICKER_OWNERSHIP_ENABLED"):
        ownership_payload["enabled"] = _env_override("MULTI_TICKER_OWNERSHIP_ENABLED")
    if _env_override("MULTI_TICKER_OWNERSHIP_TTL_SECONDS"):
        ownership_payload["lease_ttl_seconds"] = _env_override("MULTI_TICKER_OWNERSHIP_TTL_SECONDS")
    if _env_override("MULTI_TICKER_MACHINE_LABEL"):
        ownership_payload["machine_label"] = _env_override("MULTI_TICKER_MACHINE_LABEL")
    return MultiTickerPortfolioConfig.model_validate(payload)

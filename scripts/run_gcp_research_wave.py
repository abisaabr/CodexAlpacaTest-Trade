from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from alpaca_lab.backtest.engine import FixedFractionSizer, LinearCostModel, run_backtest
from alpaca_lab.strategies.base import BaseStrategy

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "research_wave"
DEFAULT_EVIDENCE_MODE = "metadata_proxy_smoke"
REAL_STOCK_BAR_EVIDENCE_MODE = "real_stock_bar_smoke"
MARKET_TIMEZONE = "America/New_York"
REQUIRED_OUTPUTS = [
    "research_run_manifest",
    "normalized_backtest_results",
    "train_test_or_walk_forward_summary",
    "after_cost_expectancy_table",
    "drawdown_and_tail_loss_report",
    "loser_cluster_comparison",
    "candidate_hold_kill_quarantine_recommendation",
]
PREFERRED_SYMBOL_BONUS = {
    "QQQ": 9.0,
    "MSFT": 7.5,
    "GLD": 5.5,
    "SLV": 4.5,
    "TSLA": 2.5,
}
SHADOW_SYMBOL_PENALTY = {
    "NVDA": -12.0,
    "AMZN": -9.0,
    "PLTR": -8.0,
    "IWM": -6.0,
    "SPY": -4.0,
    "XLE": -3.5,
}
VARIANT_TYPE_BONUS = {
    "defined_risk_family_expansion": 10.0,
    "single_leg_repair": 4.0,
    "loser_cluster_shadow_diagnostic": -5.0,
    "regime_liquidity_feature_grid": 0.0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a research-only GCP wave chunk.")
    parser.add_argument("--variants-jsonl", required=True, help="Local path or gs:// variants JSONL.")
    parser.add_argument("--wave-manifest-json", default=None, help="Optional wave manifest for chunk slicing.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--chunk-id", default=None)
    parser.add_argument("--queue-id", action="append", default=[])
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--priority", action="append", type=int, default=[])
    parser.add_argument("--max-variants", type=int, default=None)
    parser.add_argument("--evidence-mode", default=DEFAULT_EVIDENCE_MODE)
    parser.add_argument("--allow-non-smoke-evidence", action="store_true")
    parser.add_argument("--bars-path", default=None, help="Parquet stock bars for real stock-bar smoke.")
    parser.add_argument("--initial-cash", type=float, default=100_000.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-per-unit", type=float, default=0.01)
    parser.add_argument("--allocation-fraction", type=float, default=0.10)
    return parser.parse_args()


def _download_gcs_uri(uri: str) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="research_wave_"))
    destination = temp_dir / Path(uri).name
    subprocess.run(["gcloud", "storage", "cp", uri, str(destination)], check=True)
    return destination


def resolve_input_path(path_or_uri: str) -> Path:
    if path_or_uri.startswith("gs://"):
        return _download_gcs_uri(path_or_uri)
    return Path(path_or_uri)


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def load_variants(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def chunk_slice(wave_manifest: dict[str, Any], chunk_id: str | None) -> tuple[int, int] | None:
    if not chunk_id:
        return None
    chunks = wave_manifest.get("chunks") if isinstance(wave_manifest.get("chunks"), list) else []
    for chunk in chunks:
        if isinstance(chunk, dict) and chunk.get("chunk_id") == chunk_id:
            return int(chunk["start_index"]), int(chunk["end_index"]) + 1
    raise ValueError(f"Chunk id not found in wave manifest: {chunk_id}")


def filter_variants(
    variants: list[dict[str, Any]],
    *,
    wave_manifest: dict[str, Any],
    chunk_id: str | None,
    queue_ids: set[str],
    symbols: set[str],
    priorities: set[int],
    max_variants: int | None,
) -> list[dict[str, Any]]:
    sliced = variants
    bounds = chunk_slice(wave_manifest, chunk_id)
    if bounds:
        sliced = sliced[bounds[0] : bounds[1]]
    filtered = []
    for row in sliced:
        if queue_ids and str(row.get("queue_id")) not in queue_ids:
            continue
        if symbols and str(row.get("symbol")) not in symbols:
            continue
        if priorities and int(row.get("priority", 0)) not in priorities:
            continue
        filtered.append(row)
    if max_variants is not None:
        filtered = filtered[: max(max_variants, 0)]
    return filtered


def stable_unit_interval(*parts: Any) -> float:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(0xFFFFFFFFFFFF)


def _parameter_score(parameters: dict[str, Any]) -> float:
    timing = variant_timing_parameters(parameters)
    score = 0.0
    if timing["liquidity_gate"] == "tight":
        score += 3.0
    if parameters.get("avoid_after_loser_similarity") is True:
        score += 4.0
    stop = timing["stop_loss_multiple"]
    score += max(0.0, 0.30 - float(stop)) * 30.0
    exit_minute = timing["hard_exit_minute"]
    if float(exit_minute) <= 300:
        score += 2.0
    template = str(parameters.get("family_template") or "")
    if "butterfly" in template:
        score += 2.5
    if "premium_defense" in template:
        score += 2.0
    return score


class VariantStockProxyStrategy(BaseStrategy):
    def __init__(
        self,
        *,
        name: str,
        direction: int,
        signal_mode: str,
        fast_window: int,
        slow_window: int,
        breakout_window: int,
        min_volume_ratio: float,
        stop_pct: float,
        target_pct: float,
        timeout_bars: int,
        max_trend_gap_pct: float = 0.004,
        min_trend_gap_pct: float = 0.0,
        min_range_pct: float = 0.0015,
        max_range_pct: float = 0.018,
        max_midpoint_distance_pct: float = 0.006,
        range_entry_side: str = "center",
        range_edge_pct: float = 0.001,
        min_minutes_since_open: int = 5,
        max_minutes_since_open: int = 385,
        entry_signal_mode: str = "continuous",
        cooldown_bars: int = 0,
        max_signals_per_day: int = 0,
    ) -> None:
        super().__init__(name=name, instrument_type="stock", contract_multiplier=1.0)
        self.direction = 1 if direction >= 0 else -1
        self.signal_mode = signal_mode
        self.fast_window = fast_window
        self.slow_window = slow_window
        self.breakout_window = breakout_window
        self.min_volume_ratio = min_volume_ratio
        self.stop_pct = stop_pct
        self.target_pct = target_pct
        self.timeout_bars = timeout_bars
        self.max_trend_gap_pct = max_trend_gap_pct
        self.min_trend_gap_pct = min_trend_gap_pct
        self.min_range_pct = min_range_pct
        self.max_range_pct = max_range_pct
        self.max_midpoint_distance_pct = max_midpoint_distance_pct
        self.range_entry_side = range_entry_side
        self.range_edge_pct = range_edge_pct
        self.min_minutes_since_open = min_minutes_since_open
        self.max_minutes_since_open = max_minutes_since_open
        self.entry_signal_mode = entry_signal_mode
        self.cooldown_bars = cooldown_bars
        self.max_signals_per_day = max_signals_per_day

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        self.validate_bars(bars, ("symbol", "timestamp", "open", "high", "low", "close", "volume"))
        frame = bars.sort_values(["symbol", "timestamp"]).copy()
        grouped = frame.groupby("symbol", sort=False)
        frame["fast_sma"] = grouped["close"].transform(
            lambda series: series.rolling(self.fast_window).mean()
        )
        frame["slow_sma"] = grouped["close"].transform(
            lambda series: series.rolling(self.slow_window).mean()
        )
        frame["rolling_high"] = grouped["high"].transform(
            lambda series: series.shift(1).rolling(self.breakout_window).max()
        )
        frame["rolling_low"] = grouped["low"].transform(
            lambda series: series.shift(1).rolling(self.breakout_window).min()
        )
        frame["volume_sma"] = grouped["volume"].transform(
            lambda series: series.rolling(self.slow_window).mean()
        )
        frame["volume_ratio"] = frame["volume"] / frame["volume_sma"].replace(0, pd.NA)
        volume_ok = frame["volume_ratio"].fillna(0).ge(self.min_volume_ratio)
        timestamps = pd.to_datetime(frame["timestamp"], utc=True)
        market_timestamps = timestamps.dt.tz_convert(MARKET_TIMEZONE)
        minutes_since_open = (market_timestamps.dt.hour * 60 + market_timestamps.dt.minute) - (
            9 * 60 + 30
        )
        time_ok = minutes_since_open.ge(self.min_minutes_since_open) & minutes_since_open.le(
            self.max_minutes_since_open
        )
        if self.signal_mode == "range_bound":
            range_width = (frame["rolling_high"] - frame["rolling_low"]).abs()
            range_pct = range_width / frame["close"].replace(0, pd.NA)
            trend_gap_pct = (frame["fast_sma"] - frame["slow_sma"]).abs() / frame[
                "close"
            ].replace(0, pd.NA)
            midpoint = (frame["rolling_high"] + frame["rolling_low"]) / 2.0
            midpoint_distance_pct = (frame["close"] - midpoint).abs() / frame[
                "close"
            ].replace(0, pd.NA)
            active = (
                volume_ok
                & time_ok
                & range_pct.ge(self.min_range_pct).fillna(False)
                & range_pct.le(self.max_range_pct).fillna(False)
                & trend_gap_pct.le(self.max_trend_gap_pct).fillna(False)
                & midpoint_distance_pct.le(self.max_midpoint_distance_pct).fillna(False)
            )
            if self.range_entry_side == "lower_band":
                edge = frame["close"].le(midpoint * (1.0 - self.range_edge_pct)).fillna(False)
                frame["signal"] = (active & edge).fillna(False).astype(int)
            elif self.range_entry_side == "upper_band":
                edge = frame["close"].ge(midpoint * (1.0 + self.range_edge_pct)).fillna(False)
                frame["signal"] = -(active & edge).fillna(False).astype(int)
            else:
                frame["signal"] = active.fillna(False).astype(int)
        elif self.direction > 0:
            trend_gap_pct = (frame["fast_sma"] - frame["slow_sma"]) / frame[
                "close"
            ].replace(0, pd.NA)
            active = (
                frame["close"].gt(frame["rolling_high"])
                & frame["fast_sma"].gt(frame["slow_sma"])
                & trend_gap_pct.ge(self.min_trend_gap_pct).fillna(False)
                & time_ok
                & volume_ok
            )
            frame["signal"] = active.fillna(False).astype(int)
        else:
            trend_gap_pct = (frame["slow_sma"] - frame["fast_sma"]) / frame[
                "close"
            ].replace(0, pd.NA)
            active = (
                frame["close"].lt(frame["rolling_low"])
                & frame["fast_sma"].lt(frame["slow_sma"])
                & trend_gap_pct.ge(self.min_trend_gap_pct).fillna(False)
                & time_ok
                & volume_ok
            )
            frame["signal"] = -active.fillna(False).astype(int)
        frame["signal"] = self._throttle_signals(frame, market_timestamps)
        frame["stop_pct"] = self.stop_pct
        frame["target_pct"] = self.target_pct
        frame["timeout_bars"] = self.timeout_bars
        frame["size_fraction"] = 1.0
        return self.finalize_signal_frame(frame)

    def _throttle_signals(
        self, frame: pd.DataFrame, market_timestamps: pd.Series
    ) -> pd.Series:
        raw = frame["signal"].fillna(0).astype(int)
        if (
            self.entry_signal_mode == "continuous"
            and self.cooldown_bars <= 0
            and self.max_signals_per_day <= 0
        ):
            return raw

        trade_dates = market_timestamps.dt.date
        throttled = pd.Series(0, index=frame.index, dtype=int)
        for _, group_index in frame.groupby(["symbol", trade_dates], sort=False).groups.items():
            indices = list(group_index)
            signals_today = 0
            cooldown_remaining = 0
            was_active = False
            for idx in indices:
                signal = int(raw.loc[idx])
                is_active = signal != 0
                should_emit = is_active
                if self.entry_signal_mode in {"rising_edge", "daily_first"}:
                    should_emit = should_emit and not was_active
                if cooldown_remaining > 0:
                    should_emit = False
                    cooldown_remaining -= 1
                if self.max_signals_per_day > 0 and signals_today >= self.max_signals_per_day:
                    should_emit = False
                if should_emit:
                    throttled.loc[idx] = signal
                    signals_today += 1
                    cooldown_remaining = max(self.cooldown_bars, 0)
                was_active = is_active
        return throttled


def _variant_direction(variant: dict[str, Any]) -> int:
    source = str(variant.get("source_strategy_id") or variant.get("variant_id") or "").lower()
    if "__bull__" in source or "bull__" in source:
        return 1
    if "__bear__" in source or "bear__" in source:
        return -1
    if "put" in source or "short" in source or "bear" in source:
        return -1
    return 1


TIMING_PROFILE_DEFAULTS: dict[str, dict[str, float | str]] = {
    "scalp": {
        "hard_exit_minute": 120,
        "stop_loss_multiple": 0.16,
        "profit_target_multiple": 0.28,
        "liquidity_gate": "tight",
    },
    "fast": {
        "hard_exit_minute": 180,
        "stop_loss_multiple": 0.18,
        "profit_target_multiple": 0.32,
        "liquidity_gate": "tight",
    },
    "base": {
        "hard_exit_minute": 240,
        "stop_loss_multiple": 0.22,
        "profit_target_multiple": 0.40,
        "liquidity_gate": "baseline",
    },
    "patient": {
        "hard_exit_minute": 300,
        "stop_loss_multiple": 0.24,
        "profit_target_multiple": 0.45,
        "liquidity_gate": "baseline",
    },
    "slow": {
        "hard_exit_minute": 360,
        "stop_loss_multiple": 0.28,
        "profit_target_multiple": 0.55,
        "liquidity_gate": "baseline",
    },
}


def variant_timing_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    profile = str(parameters.get("timing_profile") or "patient").lower()
    defaults = TIMING_PROFILE_DEFAULTS.get(profile, TIMING_PROFILE_DEFAULTS["patient"])
    return {
        "timing_profile": profile,
        "hard_exit_minute": int(parameters.get("hard_exit_minute") or defaults["hard_exit_minute"]),
        "stop_loss_multiple": float(
            parameters.get("stop_loss_multiple") or defaults["stop_loss_multiple"]
        ),
        "profit_target_multiple": float(
            parameters.get("profit_target_multiple") or defaults["profit_target_multiple"]
        ),
        "liquidity_gate": str(parameters.get("liquidity_gate") or defaults["liquidity_gate"]),
    }


def _variant_stock_strategy(variant: dict[str, Any]) -> VariantStockProxyStrategy:
    parameters = variant.get("parameters") if isinstance(variant.get("parameters"), dict) else {}
    timing = variant_timing_parameters(parameters)
    hard_exit = int(timing["hard_exit_minute"])
    timing_scale = 0 if hard_exit <= 210 else 1 if hard_exit <= 300 else 2
    stop_multiple = float(timing["stop_loss_multiple"])
    target_multiple = float(timing["profit_target_multiple"])
    liquidity_gate = str(timing["liquidity_gate"])
    source = str(variant.get("source_strategy_id") or variant.get("variant_id") or "").lower()
    family_template = str(parameters.get("family_template") or source).lower()
    inferred_choppy = "choppy" in source or any(
        token in family_template
        for token in ("iron_butterfly", "iron_condor", "premium_defense")
    )
    signal_mode = str(
        parameters.get("stock_proxy_mode") or ("range_bound" if inferred_choppy else "breakout")
    ).lower()
    timeout_only = bool(parameters.get("timeout_only_stock_proxy")) or signal_mode == "range_bound"
    return VariantStockProxyStrategy(
        name=f"variant_stock_proxy__{variant.get('variant_id')}",
        direction=_variant_direction(variant),
        signal_mode=signal_mode,
        fast_window=4 + timing_scale,
        slow_window=18 + timing_scale * 3,
        breakout_window=18 + timing_scale * 3,
        min_volume_ratio=1.05 if liquidity_gate == "tight" else 0.80,
        stop_pct=0.0 if timeout_only else max(0.003, min(0.04, stop_multiple * 0.05)),
        target_pct=0.0 if timeout_only else max(0.005, min(0.08, target_multiple * 0.05)),
        timeout_bars=max(5, min(390, hard_exit)),
        max_trend_gap_pct=float(parameters.get("max_trend_gap_pct") or 0.004),
        min_trend_gap_pct=float(parameters.get("min_trend_gap_pct") or 0.0),
        min_range_pct=float(parameters.get("min_range_pct") or 0.0015),
        max_range_pct=float(parameters.get("max_range_pct") or 0.018),
        max_midpoint_distance_pct=float(parameters.get("max_midpoint_distance_pct") or 0.006),
        range_entry_side=str(parameters.get("range_entry_side") or "center").lower(),
        range_edge_pct=float(parameters.get("range_edge_pct") or 0.001),
        min_minutes_since_open=int(parameters.get("min_minutes_since_open") or 5),
        max_minutes_since_open=int(parameters.get("max_minutes_since_open") or 385),
        entry_signal_mode=str(parameters.get("entry_signal_mode") or "continuous"),
        cooldown_bars=int(parameters.get("cooldown_bars") or 0),
        max_signals_per_day=int(parameters.get("max_signals_per_day") or 0),
    )


def score_variant(variant: dict[str, Any], *, evidence_mode: str) -> dict[str, Any]:
    symbol = str(variant.get("symbol") or "UNKNOWN")
    variant_type = str(variant.get("variant_type") or "unknown")
    parameters = variant.get("parameters") if isinstance(variant.get("parameters"), dict) else {}
    noise = (stable_unit_interval(variant.get("variant_id"), json.dumps(parameters, sort_keys=True)) - 0.5) * 18.0
    gross_expectancy = (
        VARIANT_TYPE_BONUS.get(variant_type, 0.0)
        + PREFERRED_SYMBOL_BONUS.get(symbol, 0.0)
        + SHADOW_SYMBOL_PENALTY.get(symbol, 0.0)
        + _parameter_score(parameters)
        + noise
    )
    estimated_cost = 2.5
    net_expectancy = gross_expectancy - estimated_cost
    drawdown = max(20.0, 180.0 - gross_expectancy * 3.0 + stable_unit_interval(symbol, variant_type) * 80.0)
    tail_loss = max(8.0, drawdown * (0.28 + stable_unit_interval(variant.get("variant_id"), "tail") * 0.25))
    win_rate = min(0.72, max(0.30, 0.48 + net_expectancy / 120.0))
    synthetic_trade_count = 20 + int(stable_unit_interval(variant.get("variant_id"), "trades") * 40)
    if evidence_mode == "metadata_proxy_smoke":
        recommendation = "hold_for_real_backtest"
    elif net_expectancy >= 12 and drawdown <= 190 and tail_loss <= 85:
        recommendation = "research_review_candidate"
    elif net_expectancy <= -8 or tail_loss >= 120:
        recommendation = "quarantine"
    else:
        recommendation = "hold"
    if variant_type == "loser_cluster_shadow_diagnostic" and recommendation == "research_review_candidate":
        recommendation = "hold_shadow_diagnostic"
    return {
        "variant_id": variant.get("variant_id"),
        "queue_id": variant.get("queue_id"),
        "priority": variant.get("priority"),
        "symbol": symbol,
        "variant_type": variant_type,
        "source_strategy_id": variant.get("source_strategy_id"),
        "evidence_mode": evidence_mode,
        "synthetic_trade_count": synthetic_trade_count,
        "gross_expectancy_proxy": round(gross_expectancy, 4),
        "estimated_cost_proxy": estimated_cost,
        "net_expectancy_after_cost_proxy": round(net_expectancy, 4),
        "win_rate_proxy": round(win_rate, 4),
        "max_drawdown_proxy": round(drawdown, 4),
        "tail_loss_proxy": round(tail_loss, 4),
        "recommendation": recommendation,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "broker_facing": False,
    }


def score_variant_with_real_stock_bars(
    variant: dict[str, Any],
    *,
    bars: pd.DataFrame,
    initial_cash: float,
    slippage_bps: float,
    fee_per_unit: float,
    allocation_fraction: float,
) -> dict[str, Any]:
    symbol = str(variant.get("symbol") or "UNKNOWN").upper()
    variant_type = str(variant.get("variant_type") or "unknown")
    base = {
        "variant_id": variant.get("variant_id"),
        "queue_id": variant.get("queue_id"),
        "priority": variant.get("priority"),
        "symbol": symbol,
        "variant_type": variant_type,
        "source_strategy_id": variant.get("source_strategy_id"),
        "evidence_mode": REAL_STOCK_BAR_EVIDENCE_MODE,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "broker_facing": False,
    }
    if variant_type != "single_leg_repair":
        return {
            **base,
            "synthetic_trade_count": 0,
            "actual_trade_count": 0,
            "net_pnl": 0.0,
            "expectancy_after_cost": 0.0,
            "win_rate": 0.0,
            "profit_factor": None,
            "max_drawdown": 0.0,
            "gross_expectancy_proxy": 0.0,
            "estimated_cost_proxy": 0.0,
            "net_expectancy_after_cost_proxy": 0.0,
            "win_rate_proxy": 0.0,
            "max_drawdown_proxy": 0.0,
            "tail_loss_proxy": 0.0,
            "recommendation": "hold_unsupported_real_bar_variant",
        }
    symbol_bars = bars[bars["symbol"].astype(str).str.upper() == symbol].copy()
    if symbol_bars.empty:
        return {
            **base,
            "synthetic_trade_count": 0,
            "actual_trade_count": 0,
            "net_pnl": 0.0,
            "expectancy_after_cost": 0.0,
            "win_rate": 0.0,
            "profit_factor": None,
            "max_drawdown": 0.0,
            "gross_expectancy_proxy": 0.0,
            "estimated_cost_proxy": 0.0,
            "net_expectancy_after_cost_proxy": 0.0,
            "win_rate_proxy": 0.0,
            "max_drawdown_proxy": 0.0,
            "tail_loss_proxy": 0.0,
            "recommendation": "hold_missing_symbol_bars",
        }
    result = run_backtest(
        symbol_bars,
        _variant_stock_strategy(variant),
        initial_cash=initial_cash,
        cost_model=LinearCostModel(slippage_bps=slippage_bps, fee_per_unit=fee_per_unit),
        position_sizer=FixedFractionSizer(base_allocation_fraction=allocation_fraction),
    )
    summary = result.summary
    trade_count = int(summary.get("trade_count") or 0)
    expectancy = float(summary.get("expectancy") or 0.0)
    net_pnl = float(summary.get("net_pnl") or 0.0)
    max_drawdown = float(summary.get("max_drawdown") or 0.0)
    worst_trade = float(result.trades["pnl"].min()) if not result.trades.empty else 0.0
    profit_factor = summary.get("profit_factor")
    if trade_count < 3:
        recommendation = "hold_insufficient_trades"
    elif expectancy > 0 and net_pnl > 0 and max_drawdown >= -0.015:
        recommendation = "candidate_for_deeper_option_backtest"
    elif expectancy < -5 or net_pnl < -250 or (profit_factor is not None and profit_factor < 0.75):
        recommendation = "quarantine"
    else:
        recommendation = "hold"
    return {
        **base,
        "synthetic_trade_count": trade_count,
        "actual_trade_count": trade_count,
        "net_pnl": round(net_pnl, 4),
        "expectancy_after_cost": round(expectancy, 4),
        "win_rate": round(float(summary.get("win_rate") or 0.0), 4),
        "profit_factor": profit_factor,
        "max_drawdown": round(max_drawdown, 6),
        "sharpe_like_daily": summary.get("sharpe_like_daily"),
        "gross_expectancy_proxy": round(expectancy, 4),
        "estimated_cost_proxy": round(float(slippage_bps) / 10000.0 + float(fee_per_unit), 6),
        "net_expectancy_after_cost_proxy": round(expectancy, 4),
        "win_rate_proxy": round(float(summary.get("win_rate") or 0.0), 4),
        "max_drawdown_proxy": round(abs(max_drawdown) * initial_cash, 4),
        "tail_loss_proxy": round(abs(min(worst_trade, 0.0)), 4),
        "recommendation": recommendation,
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, title: str, payload: dict[str, Any]) -> None:
    lines = [f"# {title}", ""]
    for key, value in payload.items():
        lines.append(f"- {key}: `{value}`")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {
            "variant_count": 0,
            "mean_net_expectancy_after_cost_proxy": 0.0,
            "best_variant_id": None,
            "recommendation_counts": {},
        }
    best = max(results, key=lambda row: float(row["net_expectancy_after_cost_proxy"]))
    recommendation_counts: dict[str, int] = {}
    for row in results:
        key = str(row["recommendation"])
        recommendation_counts[key] = recommendation_counts.get(key, 0) + 1
    return {
        "variant_count": len(results),
        "mean_net_expectancy_after_cost_proxy": round(
            sum(float(row["net_expectancy_after_cost_proxy"]) for row in results) / len(results), 4
        ),
        "best_variant_id": best["variant_id"],
        "best_symbol": best["symbol"],
        "best_net_expectancy_after_cost_proxy": best["net_expectancy_after_cost_proxy"],
        "recommendation_counts": dict(sorted(recommendation_counts.items())),
    }


def group_summary(results: list[dict[str, Any]], group_key: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in results:
        groups.setdefault(str(row.get(group_key)), []).append(row)
    output = []
    for key, rows in sorted(groups.items()):
        output.append(
            {
                group_key: key,
                "variant_count": len(rows),
                "mean_net_expectancy_after_cost_proxy": round(
                    sum(float(row["net_expectancy_after_cost_proxy"]) for row in rows) / len(rows), 4
                ),
                "worst_tail_loss_proxy": max(float(row["tail_loss_proxy"]) for row in rows),
                "quarantine_count": sum(1 for row in rows if row["recommendation"] == "quarantine"),
            }
        )
    return output


def build_recommendation_packet(results: list[dict[str, Any]], evidence_mode: str) -> dict[str, Any]:
    ranked = sorted(results, key=lambda row: float(row["net_expectancy_after_cost_proxy"]), reverse=True)
    promotion_allowed = False if evidence_mode in {DEFAULT_EVIDENCE_MODE, REAL_STOCK_BAR_EVIDENCE_MODE} else True
    if evidence_mode == DEFAULT_EVIDENCE_MODE:
        promotion_note = "Metadata proxy smoke output cannot promote strategies; it only prioritizes real backtests."
    elif evidence_mode == REAL_STOCK_BAR_EVIDENCE_MODE:
        promotion_note = (
            "Real stock-bar smoke output cannot promote strategies; "
            "it only gates deeper option-aware research."
        )
    else:
        promotion_note = "Promotion still requires governance review and broker-audited evidence."
    return {
        "evidence_mode": evidence_mode,
        "promotion_allowed": promotion_allowed,
        "promotion_note": promotion_note,
        "top_research_priorities": ranked[:10],
        "quarantine_candidates": [row for row in ranked if row["recommendation"] == "quarantine"][:25],
    }


def write_artifacts(
    *,
    output_dir: Path,
    run_id: str,
    variants: list[dict[str, Any]],
    results: list[dict[str, Any]],
    args: argparse.Namespace,
    wave_manifest: dict[str, Any],
) -> dict[str, str]:
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "run_id": run_id,
        "evidence_mode": args.evidence_mode,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
        "wave_id": wave_manifest.get("wave_id"),
        "chunk_id": args.chunk_id,
        "input_variant_count": len(variants),
        "result_summary": summarize_results(results),
        "required_outputs": REQUIRED_OUTPUTS,
    }
    train_rows = group_summary(results, "queue_id")
    expectancy_rows = group_summary(results, "symbol")
    loser_rows = group_summary(results, "variant_type")
    drawdown_packet = {
        "run_id": run_id,
        "worst_max_drawdown_proxy": max((float(row["max_drawdown_proxy"]) for row in results), default=0.0),
        "worst_tail_loss_proxy": max((float(row["tail_loss_proxy"]) for row in results), default=0.0),
        "evidence_mode": args.evidence_mode,
    }
    recommendation_packet = build_recommendation_packet(results, args.evidence_mode)

    artifacts = {
        "research_run_manifest": run_dir / "research_run_manifest.json",
        "research_run_manifest_md": run_dir / "research_run_manifest.md",
        "normalized_backtest_results": run_dir / "normalized_backtest_results.csv",
        "normalized_backtest_results_json": run_dir / "normalized_backtest_results.json",
        "train_test_or_walk_forward_summary": run_dir / "train_test_or_walk_forward_summary.json",
        "after_cost_expectancy_table": run_dir / "after_cost_expectancy_table.csv",
        "drawdown_and_tail_loss_report": run_dir / "drawdown_and_tail_loss_report.json",
        "drawdown_and_tail_loss_report_md": run_dir / "drawdown_and_tail_loss_report.md",
        "loser_cluster_comparison": run_dir / "loser_cluster_comparison.csv",
        "candidate_hold_kill_quarantine_recommendation": run_dir
        / "candidate_hold_kill_quarantine_recommendation.json",
        "candidate_hold_kill_quarantine_recommendation_md": run_dir
        / "candidate_hold_kill_quarantine_recommendation.md",
    }
    _write_json(artifacts["research_run_manifest"], manifest)
    _write_markdown(artifacts["research_run_manifest_md"], "Research Run Manifest", manifest)
    _write_csv(artifacts["normalized_backtest_results"], results)
    _write_json(artifacts["normalized_backtest_results_json"], results)
    _write_json(artifacts["train_test_or_walk_forward_summary"], train_rows)
    _write_csv(artifacts["after_cost_expectancy_table"], expectancy_rows)
    _write_json(artifacts["drawdown_and_tail_loss_report"], drawdown_packet)
    _write_markdown(
        artifacts["drawdown_and_tail_loss_report_md"],
        "Drawdown And Tail Loss Report",
        drawdown_packet,
    )
    _write_csv(artifacts["loser_cluster_comparison"], loser_rows)
    _write_json(artifacts["candidate_hold_kill_quarantine_recommendation"], recommendation_packet)
    _write_markdown(
        artifacts["candidate_hold_kill_quarantine_recommendation_md"],
        "Candidate Hold Kill Quarantine Recommendation",
        {
            "evidence_mode": recommendation_packet["evidence_mode"],
            "promotion_allowed": recommendation_packet["promotion_allowed"],
            "promotion_note": recommendation_packet["promotion_note"],
            "top_research_priority_count": len(recommendation_packet["top_research_priorities"]),
            "quarantine_candidate_count": len(recommendation_packet["quarantine_candidates"]),
        },
    )
    return {key: str(path) for key, path in artifacts.items()}


def run(args: argparse.Namespace) -> dict[str, Any]:
    smoke_modes = {DEFAULT_EVIDENCE_MODE, REAL_STOCK_BAR_EVIDENCE_MODE}
    if args.evidence_mode not in smoke_modes and not args.allow_non_smoke_evidence:
        raise ValueError("--allow-non-smoke-evidence is required for evidence modes beyond metadata proxy smoke.")
    variants_path = resolve_input_path(args.variants_jsonl)
    wave_manifest_path = resolve_input_path(args.wave_manifest_json) if args.wave_manifest_json else None
    wave_manifest = load_json(wave_manifest_path)
    variants = load_variants(variants_path)
    selected = filter_variants(
        variants,
        wave_manifest=wave_manifest,
        chunk_id=args.chunk_id,
        queue_ids={str(item) for item in args.queue_id},
        symbols={str(item).upper() for item in args.symbol},
        priorities=set(args.priority),
        max_variants=args.max_variants,
    )
    if args.evidence_mode == REAL_STOCK_BAR_EVIDENCE_MODE:
        if not args.bars_path:
            raise ValueError("--bars-path is required for real stock-bar smoke.")
        bars = pd.read_parquet(resolve_input_path(args.bars_path))
        results = [
            score_variant_with_real_stock_bars(
                variant,
                bars=bars,
                initial_cash=args.initial_cash,
                slippage_bps=args.slippage_bps,
                fee_per_unit=args.fee_per_unit,
                allocation_fraction=args.allocation_fraction,
            )
            for variant in selected
        ]
    else:
        results = [score_variant(variant, evidence_mode=args.evidence_mode) for variant in selected]
    run_id = args.run_id or f"research_wave_{datetime.now().astimezone().strftime('%Y%m%d_%H%M%S')}"
    artifacts = write_artifacts(
        output_dir=Path(args.output_dir),
        run_id=run_id,
        variants=selected,
        results=results,
        args=args,
        wave_manifest=wave_manifest,
    )
    return {
        "run_id": run_id,
        "evidence_mode": args.evidence_mode,
        "selected_variant_count": len(selected),
        "summary": summarize_results(results),
        "artifacts": artifacts,
        "broker_facing": False,
        "live_manifest_effect": "none",
        "risk_policy_effect": "none",
    }


def main() -> None:
    result = run(parse_args())
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

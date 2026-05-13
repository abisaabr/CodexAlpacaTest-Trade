from __future__ import annotations

import getpass
import json
import math
import platform
import re
import subprocess
import time
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca_lab.brokers.alpaca import AlpacaBrokerAdapter, OrderLeg, OrderRequest
from alpaca_lab.config import LabSettings
from alpaca_lab.execution.ownership import (
    FileOwnershipLease,
    GCSGenerationMatchLeaseStore,
    GenerationMatchOwnershipLease,
    NoopOwnershipLease,
    OwnershipLeaseStatus,
)
from alpaca_lab.logging_utils import get_logger
from alpaca_lab.multi_ticker_portfolio.config import (
    MultiTickerPortfolioConfig,
    RiskBucketConfig,
    StrategyConfig,
)
from alpaca_lab.multi_ticker_portfolio.signals import (
    MINUTES_PER_RTH_SESSION,
    build_stock_frame,
    governed_research_signal_is_true,
    infer_symbol_regime,
    signal_is_true,
)
from alpaca_lab.notifications import DiscordWebhookNotifier, EmailNotifier, NtfyNotifier
from alpaca_lab.qqq_portfolio.greeks import bs_greeks, implied_volatility
from alpaca_lab.reporting import append_journal_entry, write_alert_queue, write_summary_bundle

RUNNER_REPO_ROOT = Path(__file__).resolve().parents[2]
ET = ZoneInfo("America/New_York")
OPEN_STATUSES = {"accepted", "new", "partially_filled", "pending_new", "accepted_for_bidding"}
TERMINAL_STATUSES = {"filled", "canceled", "expired", "done_for_day", "rejected"}
CONTRACT_MULTIPLIER = 100.0
RUNNER_EXECUTION_CAPABILITY_EPOCH = 1
RUNNER_EXECUTION_CAPABILITY_LABEL = "broker_audited_session_bundle_v1"
RUNNER_SOURCE_STAMP_FILENAME = ".codexalpaca_source_stamp.json"
ALPACA_OPTION_BROKER_COMMISSION_PER_CONTRACT = 0.0
ALPACA_OPTION_ORF_PER_CONTRACT = 0.02295
ALPACA_OPTION_OCC_PER_CONTRACT = 0.025
ALPACA_OPTION_TAF_PER_CONTRACT = 0.00329
ALPACA_OPTION_CAT_PER_EQUIVALENT_SHARE = 0.0
AUTO_FLATTEN_UNEXPECTED_STARTUP_REASON = "auto_flatten_unexpected_startup_position"
AUTO_FLATTEN_KNOWN_EOD_REASON = "auto_flatten_known_end_of_day_position"
AUTO_FLATTEN_UNEXPECTED_EOD_REASON = "auto_flatten_unexpected_end_of_day_position"
AUTO_FLATTEN_UNEXPECTED_INTRADAY_REASON = "auto_flatten_unexpected_intraday_position"
SCHEDULED_EOD_FLATTEN_REASON = "scheduled_end_of_day_flatten"
BROKER_EQUITY_EMERGENCY_STOP_REASON = "broker_equity_emergency_stop"
SEVERE_LOSS_HALT_REASON = "severe_loss_halt_new_entries"
SEVERE_LOSS_FLATTEN_REASON = "severe_loss_flatten_all"
PROJECTED_DELTA_HARD_CAP_REASON = "projected_delta_hard_cap"
PROJECTED_VEGA_HARD_CAP_REASON = "projected_vega_hard_cap"
ENTRY_EXECUTION_CIRCUIT_BREAKER_REASON = "entry_execution_circuit_breaker"
STOP_LOSS_COOLDOWN_REASON = "stop_loss_cooldown"
LATE_DAY_ENTRY_CUTOFF_REASON = "late_day_entry_cutoff"
EVENT_BLACKOUT_REASON = "event_blackout"
REGIME_ENTRY_CLUSTER_REASON = "regime_entry_cluster"
BUCKET_REGIME_ENTRY_CLUSTER_REASON = "bucket_regime_entry_cluster"


def _source_stamp_metadata(repo_root: Path) -> dict[str, Any]:
    stamp_path = repo_root / RUNNER_SOURCE_STAMP_FILENAME
    if not stamp_path.exists():
        return {
            "runner_source_stamp_available": False,
            "runner_source_stamp_path": str(stamp_path),
        }
    try:
        payload = json.loads(stamp_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "runner_source_stamp_available": False,
            "runner_source_stamp_path": str(stamp_path),
            "runner_source_stamp_parse_error": True,
        }
    commit = str(payload.get("runner_commit") or "")
    return {
        "runner_source_stamp_available": True,
        "runner_source_stamp_path": str(stamp_path),
        "runner_source_stamp_commit": commit or None,
        "runner_source_stamp_branch": payload.get("runner_branch"),
        "runner_source_stamp_deployed_at": payload.get("deployed_at"),
        "runner_source_stamp_archive_sha256": payload.get("archive_sha256"),
        "runner_source_stamp_deploy_method": payload.get("deploy_method"),
        "runner_source_stamp_broker_facing": payload.get("broker_facing"),
        "runner_source_stamp_live_manifest_effect": payload.get("live_manifest_effect"),
        "runner_source_stamp_risk_policy_effect": payload.get("risk_policy_effect"),
    }


def _git_metadata(repo_root: Path) -> dict[str, Any]:
    def run_git(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_root), *args],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if result.returncode != 0:
            return None
        output = (result.stdout or "").strip()
        return output or None

    stamp_metadata = _source_stamp_metadata(repo_root)
    status_output = run_git("status", "--porcelain")
    git_toplevel = run_git("rev-parse", "--show-toplevel")
    git_metadata_available = git_toplevel is not None
    git_commit = run_git("rev-parse", "--short=12", "HEAD")
    git_branch = run_git("branch", "--show-current")
    if not git_metadata_available and stamp_metadata.get("runner_source_stamp_available"):
        stamped_commit = str(stamp_metadata.get("runner_source_stamp_commit") or "")
        git_commit = stamped_commit[:12] if stamped_commit else None
        git_branch = cast(str | None, stamp_metadata.get("runner_source_stamp_branch"))
    return {
        "runner_repo_commit": git_commit,
        "runner_repo_branch": git_branch,
        "runner_repo_dirty": bool(status_output) if git_metadata_available else False,
        "runner_repo_metadata_available": git_metadata_available,
        **stamp_metadata,
    }


def _runner_execution_metadata() -> dict[str, Any]:
    metadata = _git_metadata(RUNNER_REPO_ROOT)
    metadata.update(
        {
            "runner_capability_epoch": RUNNER_EXECUTION_CAPABILITY_EPOCH,
            "runner_capability_label": RUNNER_EXECUTION_CAPABILITY_LABEL,
        }
    )
    return metadata


@dataclass(slots=True)
class SelectedLeg:
    symbol: str
    expiration_date: str
    option_type: str
    side: str
    strike_price: float
    target_delta: float
    bid: float
    ask: float
    mark: float
    delta: float
    gamma: float
    theta: float
    vega: float
    quote_time: str | None
    spread_pct: float = 0.0
    freshness_seconds: float | None = None
    quote_source: str | None = None


@dataclass(slots=True)
class OpenTrade:
    strategy_name: str
    underlying_symbol: str
    regime: str
    quantity: int
    entry_time_et: str
    entry_minute: int
    hard_exit_minute: int
    underlying_entry: float
    entry_debit: float
    max_loss_per_combo: float
    max_profit_per_combo: float
    profit_target_dollars: float
    stop_loss_dollars: float
    entry_order_id: str | None
    entry_fill_price: float
    legs: list[dict[str, Any]]
    entry_attempt_id: str | None = None
    candidate_variant_id: str | None = None
    source_strategy_id: str | None = None
    promotion_manifest_path: str | None = None
    governed_validation_packet_uri: str | None = None
    research_profile: str | None = None
    research_entry_timing_mode: str | None = None
    research_entry_offset_minutes: int | None = None
    research_exit_offset_minutes: int | None = None
    runner_semantics_status: str | None = None
    min_option_hold_minutes: int | None = None
    runner_hard_exit_mode: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CompletedTrade:
    strategy_name: str
    underlying_symbol: str
    regime: str
    quantity: int
    entry_time_et: str
    exit_time_et: str
    entry_minute: int
    exit_minute: int
    entry_fill_price: float
    exit_fill_price: float
    underlying_entry: float
    underlying_exit: float
    exit_reason: str
    entry_order_id: str | None
    exit_order_id: str | None
    net_pnl: float
    max_loss_per_combo: float
    max_profit_per_combo: float
    delta_shares_at_entry: float
    vega_dollars_1pct_at_entry: float
    legs: list[dict[str, Any]]
    entry_attempt_id: str | None = None
    candidate_variant_id: str | None = None
    source_strategy_id: str | None = None
    promotion_manifest_path: str | None = None
    governed_validation_packet_uri: str | None = None
    research_profile: str | None = None
    research_entry_timing_mode: str | None = None
    research_entry_offset_minutes: int | None = None
    research_exit_offset_minutes: int | None = None
    runner_semantics_status: str | None = None
    min_option_hold_minutes: int | None = None
    runner_hard_exit_mode: str | None = None
    entry_total_fees: float = 0.0
    exit_total_fees: float = 0.0
    entry_regulatory_fees: float = 0.0
    exit_regulatory_fees: float = 0.0
    via_cleanup: bool = False


@dataclass(slots=True)
class OptionFeeBreakdown:
    broker_commission: float
    regulatory_fees: float
    orf: float
    occ: float
    cat: float
    taf: float
    total_fees: float


@dataclass(slots=True)
class PortfolioLedger:
    realized_equity: float
    high_watermark: float
    closed_days: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class SessionState:
    trade_date: str
    starting_equity: float
    virtual_cash: float
    blocked_new_entries: bool = False
    block_reason: str | None = None
    execution_guardrails: dict[str, Any] = field(default_factory=dict)
    signals_fired: list[str] = field(default_factory=list)
    open_trades: list[dict[str, Any]] = field(default_factory=list)
    completed_trades: list[dict[str, Any]] = field(default_factory=list)
    alerts: list[dict[str, Any]] = field(default_factory=list)
    last_symbol_regimes: dict[str, str] = field(default_factory=dict)
    startup_check_status: str = "pending"
    startup_check_details: dict[str, Any] = field(default_factory=dict)
    notified_morning: bool = False
    notified_midday: bool = False
    notified_end_of_day: bool = False
    eod_flatten_checkpoints_completed: list[int] = field(default_factory=list)
    last_updated_at: str | None = None


@dataclass(slots=True)
class SymbolSnapshot:
    underlying_symbol: str
    trade_date: date
    stock_frame: pd.DataFrame
    option_chain: pd.DataFrame
    mark_map: dict[str, float]
    latest_close: float
    current_minute: int
    latest_timestamp_et: datetime | None


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _now_et() -> datetime:
    return datetime.now(UTC).astimezone(ET)


def _rth_open_for(day: date) -> datetime:
    return datetime.combine(day, dt_time(9, 30), tzinfo=ET)


def _trade_date_from_clock(clock: dict[str, Any]) -> date:
    if clock.get("timestamp"):
        return (
            datetime.fromisoformat(str(clock["timestamp"]).replace("Z", "+00:00"))
            .astimezone(ET)
            .date()
        )
    return _now_et().date()


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _position_mark_cashflow(legs: list[dict[str, Any]], mark_map: dict[str, float]) -> float:
    cashflow = 0.0
    for leg in legs:
        mark = mark_map.get(str(leg["symbol"]))
        if mark is None:
            raise KeyError(str(leg["symbol"]))
        if leg["side"] == "long":
            cashflow += mark * CONTRACT_MULTIPLIER
        else:
            cashflow -= mark * CONTRACT_MULTIPLIER
    return cashflow


def _option_intrinsic(option_type: str, strike_price: float, spot: float) -> float:
    if option_type == "call":
        return max(spot - strike_price, 0.0)
    return max(strike_price - spot, 0.0)


def _combo_payoff_at_expiry(legs: list[dict[str, Any]], spot: float) -> float:
    payoff = 0.0
    for leg in legs:
        intrinsic = _option_intrinsic(str(leg["option_type"]), float(leg["strike_price"]), spot)
        entry_price = float(leg["entry_fill_price"])
        if leg["side"] == "long":
            payoff += (intrinsic - entry_price) * CONTRACT_MULTIPLIER
        else:
            payoff += (entry_price - intrinsic) * CONTRACT_MULTIPLIER
    return payoff


def _estimate_combo_bounds(legs: list[dict[str, Any]]) -> tuple[float, float]:
    strikes = [float(leg["strike_price"]) for leg in legs]
    if not strikes:
        return 0.0, 0.0
    lower = min(strikes)
    upper = max(strikes)
    span = max(1.0, upper - lower)
    candidates = {
        0.01,
        max(0.01, lower - 2.0 * span),
        max(0.01, lower - span),
        lower,
        (lower + upper) / 2.0,
        upper,
        upper + span,
        upper + 2.0 * span,
    }
    pnl_values = [_combo_payoff_at_expiry(legs, spot) for spot in sorted(candidates)]
    return max(0.01, -min(pnl_values)), max(pnl_values)


def _round_up_to_cents(amount: float) -> float:
    if amount <= 0.0:
        return 0.0
    return math.ceil(amount * 100.0) / 100.0


def _entry_sell_contract_count(legs: list[dict[str, Any]], quantity: int) -> int:
    return sum(1 for leg in legs if str(leg["side"]) == "short") * quantity


def _exit_sell_contract_count(legs: list[dict[str, Any]], quantity: int) -> int:
    return sum(1 for leg in legs if str(leg["side"]) == "long") * quantity


def _alpaca_option_fee_breakdown(*, total_contracts: int, sold_contracts: int) -> OptionFeeBreakdown:
    broker_commission = ALPACA_OPTION_BROKER_COMMISSION_PER_CONTRACT * total_contracts
    orf = ALPACA_OPTION_ORF_PER_CONTRACT * total_contracts
    occ = ALPACA_OPTION_OCC_PER_CONTRACT * total_contracts
    cat = ALPACA_OPTION_CAT_PER_EQUIVALENT_SHARE * CONTRACT_MULTIPLIER * total_contracts
    taf = ALPACA_OPTION_TAF_PER_CONTRACT * sold_contracts
    regulatory_fees = _round_up_to_cents(orf + occ + cat + taf)
    total_fees = _round_up_to_cents(broker_commission + regulatory_fees)
    return OptionFeeBreakdown(
        broker_commission=round(broker_commission, 6),
        regulatory_fees=regulatory_fees,
        orf=round(orf, 6),
        occ=round(occ, 6),
        cat=round(cat, 6),
        taf=round(taf, 6),
        total_fees=total_fees,
    )


def _entry_fee_breakdown(legs: list[dict[str, Any]], quantity: int) -> OptionFeeBreakdown:
    total_contracts = len(legs) * quantity
    return _alpaca_option_fee_breakdown(
        total_contracts=total_contracts,
        sold_contracts=_entry_sell_contract_count(legs, quantity),
    )


def _exit_fee_breakdown(legs: list[dict[str, Any]], quantity: int) -> OptionFeeBreakdown:
    total_contracts = len(legs) * quantity
    return _alpaca_option_fee_breakdown(
        total_contracts=total_contracts,
        sold_contracts=_exit_sell_contract_count(legs, quantity),
    )


def _entry_cashflow_from_debit(entry_debit: float, quantity: int, legs: list[dict[str, Any]]) -> float:
    gross = -entry_debit * CONTRACT_MULTIPLIER * quantity
    return gross - _entry_fee_breakdown(legs, quantity).total_fees


def _exit_cashflow_from_fill(*, fill_price: float, quantity: int, legs: list[dict[str, Any]]) -> float:
    gross = fill_price * CONTRACT_MULTIPLIER * quantity
    return gross - _exit_fee_breakdown(legs, quantity).total_fees


def _current_equity(state: SessionState, mark_map: dict[str, float] | None = None) -> float:
    equity = state.virtual_cash
    mark_map = mark_map or {}
    for trade_payload in state.open_trades:
        try:
            mark_cashflow = _position_mark_cashflow(trade_payload["legs"], mark_map)
        except KeyError:
            continue
        exit_fees = _exit_fee_breakdown(
            cast(list[dict[str, Any]], trade_payload["legs"]),
            int(trade_payload["quantity"]),
        ).total_fees
        equity += mark_cashflow * int(trade_payload["quantity"]) - exit_fees
    return equity


def _format_signed_dollars(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.2f}"


class MultiTickerPortfolioPaperTrader:
    def __init__(
        self,
        settings: LabSettings,
        portfolio_config: MultiTickerPortfolioConfig,
        *,
        broker: AlpacaBrokerAdapter | None = None,
        submit_paper_orders: bool | None = None,
    ) -> None:
        self.settings = settings
        self.portfolio_config = portfolio_config
        self.submit_paper_orders = (
            portfolio_config.execution.submit_paper_orders
            if submit_paper_orders is None
            else submit_paper_orders
        )
        self.broker = broker or AlpacaBrokerAdapter(settings, dry_run=not self.submit_paper_orders)
        self.ntfy_notifier = NtfyNotifier(settings)
        self.discord_notifier = DiscordWebhookNotifier(settings)
        self.email_notifier = EmailNotifier(settings)
        self.logger = get_logger("multi_ticker_portfolio")
        self._failed_notification_phases: set[str] = set()
        self.state_root = portfolio_config.execution.state_root
        self.run_root = portfolio_config.execution.run_root
        self.ledger_path = self.state_root / "ledger.json"
        self.contract_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.contract_cache_loaded_at: dict[str, datetime] = {}
        self.underlyings = list(portfolio_config.execution.underlying_symbols)
        self.ownership_lease = self._build_ownership_lease()

    def _ownership_audit_context(self) -> dict[str, Any]:
        machine_label = self.portfolio_config.ownership.machine_label or ""
        source = "vm" if machine_label.startswith("vm-") else "workstation"
        return {
            "plane": "execution",
            "environment": "paper",
            "source": source,
            "lease_backend": self.portfolio_config.ownership.lease_backend,
        }

    def _build_ownership_lease(self) -> Any:
        ownership = self.portfolio_config.ownership
        if not ownership.enabled:
            return NoopOwnershipLease()
        if ownership.lease_backend == "file":
            return FileOwnershipLease(
                path=ownership.lease_path,
                owner_label=ownership.machine_label,
                ttl_seconds=ownership.lease_ttl_seconds,
            )
        store = GCSGenerationMatchLeaseStore.from_gcs_uri(str(ownership.gcs_lease_uri))
        runner_metadata = _git_metadata(RUNNER_REPO_ROOT)
        return GenerationMatchOwnershipLease(
            store=store,
            lease_path=str(ownership.gcs_lease_uri),
            owner_label=ownership.machine_label,
            ttl_seconds=ownership.lease_ttl_seconds,
            machine_label=ownership.machine_label,
            runner_path=str(RUNNER_REPO_ROOT),
            git_commit=runner_metadata.get("runner_repo_commit"),
            audit_context=self._ownership_audit_context(),
        )

    def _ownership_metadata(self, *, role: str) -> dict[str, Any]:
        return {
            "role": role,
            "hostname": platform.node(),
            "username": getpass.getuser(),
            "repo_root": str(Path(__file__).resolve().parents[2]),
        }

    def acquire_runtime_ownership(self, *, role: str) -> OwnershipLeaseStatus:
        return self.ownership_lease.acquire(role=role, metadata=self._ownership_metadata(role=role))

    def load_ledger(self) -> PortfolioLedger:
        payload = _read_json(
            self.ledger_path,
            {
                "realized_equity": self.portfolio_config.risk.sleeve_starting_equity,
                "high_watermark": self.portfolio_config.risk.sleeve_starting_equity,
                "closed_days": [],
            },
        )
        return PortfolioLedger(
            realized_equity=float(payload["realized_equity"]),
            high_watermark=float(payload["high_watermark"]),
            closed_days=list(payload.get("closed_days", [])),
        )

    def save_ledger(self, ledger: PortfolioLedger) -> Path:
        return _write_json(self.ledger_path, asdict(ledger))

    def session_path(self, trade_date: date) -> Path:
        return self.state_root / f"session_{trade_date.isoformat()}.json"

    def load_or_create_session(self, trade_date: date, ledger: PortfolioLedger) -> SessionState:
        path = self.session_path(trade_date)
        if path.exists():
            payload = _read_json(path, {})
            return SessionState(
                trade_date=str(payload["trade_date"]),
                starting_equity=float(payload["starting_equity"]),
                virtual_cash=float(payload["virtual_cash"]),
                blocked_new_entries=bool(payload.get("blocked_new_entries", False)),
                block_reason=payload.get("block_reason"),
                execution_guardrails=dict(payload.get("execution_guardrails", {})),
                signals_fired=list(payload.get("signals_fired", [])),
                open_trades=list(payload.get("open_trades", [])),
                completed_trades=list(payload.get("completed_trades", [])),
                alerts=list(payload.get("alerts", [])),
                last_symbol_regimes=dict(payload.get("last_symbol_regimes", {})),
                startup_check_status=str(payload.get("startup_check_status", "pending")),
                startup_check_details=dict(payload.get("startup_check_details", {})),
                notified_morning=bool(payload.get("notified_morning", False)),
                notified_midday=bool(payload.get("notified_midday", False)),
                notified_end_of_day=bool(payload.get("notified_end_of_day", False)),
                eod_flatten_checkpoints_completed=[
                    int(value)
                    for value in payload.get("eod_flatten_checkpoints_completed", [])
                    if str(value).strip()
                ],
                last_updated_at=payload.get("last_updated_at"),
            )
        return SessionState(
            trade_date=trade_date.isoformat(),
            starting_equity=ledger.realized_equity,
            virtual_cash=ledger.realized_equity,
        )

    def save_session(self, session: SessionState) -> Path:
        session.last_updated_at = _now_et().isoformat()
        return _write_json(self.session_path(date.fromisoformat(session.trade_date)), asdict(session))

    def _session_run_dir(self, trade_date: date) -> Path:
        run_dir = self.run_root / trade_date.isoformat()
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def _trade_reconciliation_events_path(self, trade_date: date) -> Path:
        return self._session_run_dir(trade_date) / "trade_reconciliation_events.json"

    def _append_trade_event(self, trade_date: date, event: dict[str, Any]) -> Path:
        payload = {"timestamp_et": _now_et().isoformat(), **event}
        return append_journal_entry(self._trade_reconciliation_events_path(trade_date), payload)

    def _broker_position_cleanup_path(self, trade_date: date) -> Path:
        return self._session_run_dir(trade_date) / "broker_position_cleanup.json"

    def _append_broker_position_cleanup_entry(
        self,
        trade_date: date,
        entry: dict[str, Any],
    ) -> Path:
        payload = {"timestamp_et": _now_et().isoformat(), **entry}
        return append_journal_entry(self._broker_position_cleanup_path(trade_date), payload)

    def _expected_broker_position_map(self, session: SessionState) -> dict[str, float]:
        expected: dict[str, float] = {}
        for trade_payload in session.open_trades:
            quantity = float(trade_payload.get("quantity") or 0.0)
            for leg in trade_payload.get("legs", []):
                symbol = str(leg.get("symbol") or "").strip()
                if not symbol:
                    continue
                sign = 1.0 if str(leg.get("side")) == "long" else -1.0
                expected[symbol] = expected.get(symbol, 0.0) + sign * quantity
        return expected

    def _broker_position_qty_map(self) -> dict[str, float]:
        actual: dict[str, float] = {}
        broker = getattr(self, "broker", None)
        get_positions = getattr(broker, "get_positions", None)
        if get_positions is None:
            return actual
        for position in get_positions():
            symbol = str(position.get("symbol") or "").strip()
            if not symbol:
                continue
            actual[symbol] = actual.get(symbol, 0.0) + self._signed_broker_position_qty(position)
        return actual

    def _signed_broker_position_qty(self, position_payload: dict[str, Any]) -> float:
        qty = float(position_payload.get("qty") or 0.0)
        side = str(position_payload.get("side") or "long").lower()
        if side == "short":
            return -abs(qty)
        if side == "long":
            return abs(qty)
        return qty

    def _normalized_broker_asset_class(self, position_payload: dict[str, Any]) -> str:
        raw_asset_class = str(position_payload.get("asset_class") or "").lower()
        if "option" in raw_asset_class:
            return "option"
        symbol = str(position_payload.get("symbol") or "")
        return "option" if any(character.isdigit() for character in symbol) else "stock"

    def _remove_open_trade_from_session(self, session: SessionState, trade: OpenTrade) -> None:
        if trade.entry_attempt_id:
            session.open_trades = [
                item
                for item in session.open_trades
                if str(item.get("entry_attempt_id") or "") != str(trade.entry_attempt_id)
            ]
            return
        session.open_trades = [
            item
            for item in session.open_trades
            if not (
                item.get("strategy_name") == trade.strategy_name
                and item.get("entry_time_et") == trade.entry_time_et
                and item.get("underlying_symbol") == trade.underlying_symbol
            )
        ]

    @staticmethod
    def _trade_has_no_broker_position(
        trade_payload: dict[str, Any],
        broker_position_map: dict[str, float],
    ) -> bool:
        leg_symbols = {
            str(leg.get("symbol") or "").strip()
            for leg in trade_payload.get("legs", [])
            if str(leg.get("symbol") or "").strip()
        }
        return bool(leg_symbols) and all(
            math.isclose(float(broker_position_map.get(symbol, 0.0)), 0.0, abs_tol=1e-9)
            for symbol in leg_symbols
        )

    def _drop_open_trade_already_flat_at_broker(
        self,
        *,
        trade_payload: dict[str, Any],
        session: SessionState,
        trade_date: date,
        reason: str,
    ) -> None:
        trade = OpenTrade(**trade_payload)
        self._remove_open_trade_from_session(session, trade)
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="exit"),
                "event_type": "exit_result",
                "status": "broker_flat_without_session_exit",
                "exit_reason": reason,
                "order_id": None,
                "expected_exit_fill_price": None,
                "actual_exit_fill_price": None,
                "exit_slippage": None,
                "net_pnl": None,
                "virtual_cash_after": round(float(session.virtual_cash), 4),
                "via_cleanup": True,
            },
        )
        self._alert(
            session,
            "warning",
            f"{trade.strategy_name} removed from open session state because broker is already flat for its legs",
        )

    def _cleanup_leg_plans_for_trade(
        self,
        trade: OpenTrade,
        *,
        broker_position_map: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
        actual_map = broker_position_map or {}
        authoritative_actuals = any(str(leg.get("symbol") or "").strip() in actual_map for leg in trade.legs)
        plans: list[dict[str, Any]] = []
        for leg in trade.legs:
            symbol = str(leg.get("symbol") or "").strip()
            if not symbol:
                continue
            expected_signed_qty = float(trade.quantity) if str(leg.get("side")) == "long" else -float(trade.quantity)
            broker_signed_qty = float(actual_map.get(symbol, 0.0))
            cleanup_signed_qty = broker_signed_qty if authoritative_actuals else expected_signed_qty
            if math.isclose(cleanup_signed_qty, 0.0, abs_tol=1e-9):
                continue
            plans.append(
                {
                    "symbol": symbol,
                    "leg_side": str(leg.get("side") or ""),
                    "expected_signed_qty": expected_signed_qty,
                    "broker_signed_qty": broker_signed_qty,
                    "cleanup_signed_qty": cleanup_signed_qty,
                    "cleanup_qty": abs(cleanup_signed_qty),
                    "order_side": "sell" if cleanup_signed_qty > 0 else "buy",
                    "position_intent": "sell_to_close" if cleanup_signed_qty > 0 else "buy_to_close",
                    "mark": leg.get("mark"),
                    "entry_fill_price": leg.get("entry_fill_price"),
                    "used_broker_positions": authoritative_actuals,
                }
            )
        return sorted(
            plans,
            key=lambda plan: self._cleanup_order_priority(
                cleanup_signed_qty=float(plan["cleanup_signed_qty"]),
                symbol=str(plan["symbol"]),
            ),
        )

    @staticmethod
    def _cleanup_order_priority(*, cleanup_signed_qty: float, symbol: str) -> tuple[int, str]:
        # Close shorts first so protective long legs are not removed before margin-reducing covers.
        return (0 if cleanup_signed_qty < 0 else 1, symbol)

    def _broker_position_mismatch_messages(
        self,
        session: SessionState,
        positions: list[dict[str, Any]],
    ) -> list[str]:
        expected = self._expected_broker_position_map(session)
        actual: dict[str, float] = {}
        for position in positions:
            symbol = str(position.get("symbol") or "").strip()
            if not symbol:
                continue
            actual[symbol] = actual.get(symbol, 0.0) + self._signed_broker_position_qty(position)

        mismatches: list[str] = []
        all_symbols = sorted(set(expected) | set(actual))
        for symbol in all_symbols:
            expected_qty = expected.get(symbol, 0.0)
            actual_qty = actual.get(symbol, 0.0)
            if math.isclose(expected_qty, actual_qty, abs_tol=1e-9):
                continue
            if math.isclose(expected_qty, 0.0, abs_tol=1e-9):
                mismatches.append(
                    f"broker reported unexpected open position for {symbol} ({actual_qty:+.0f})"
                )
                continue
            if math.isclose(actual_qty, 0.0, abs_tol=1e-9):
                mismatches.append(
                    f"broker position missing for {symbol} (expected {expected_qty:+.0f})"
                )
                continue
            mismatches.append(
                f"broker position mismatch for {symbol} (expected {expected_qty:+.0f}, actual {actual_qty:+.0f})"
            )
        return mismatches

    def _serialize_order_request(self, request: OrderRequest) -> dict[str, Any]:
        payload = request.to_payload()
        return {
            "symbol": request.symbol,
            "side": request.side,
            "qty": request.qty,
            "notional": request.notional,
            "order_type": request.order_type,
            "time_in_force": request.time_in_force,
            "limit_price": request.limit_price,
            "stop_price": request.stop_price,
            "client_order_id": request.client_order_id,
            "asset_class": request.asset_class,
            "strategy_name": request.strategy_name,
            "legs": payload.get("legs"),
            "extra": request.extra,
        }

    def _event_base_for_trade(self, trade: OpenTrade, *, phase: str) -> dict[str, Any]:
        return {
            "attempt_id": trade.entry_attempt_id,
            "trade_date": date.fromisoformat(trade.entry_time_et[:10]).isoformat(),
            "strategy_name": trade.strategy_name,
            "underlying_symbol": trade.underlying_symbol,
            "regime": trade.regime,
            "phase": phase,
            "entry_minute": int(trade.entry_minute),
            "quantity": int(trade.quantity),
            "candidate_variant_id": trade.candidate_variant_id,
            "source_strategy_id": trade.source_strategy_id,
            "promotion_manifest_path": trade.promotion_manifest_path,
            "governed_validation_packet_uri": trade.governed_validation_packet_uri,
            "research_profile": trade.research_profile,
            "research_entry_timing_mode": trade.research_entry_timing_mode,
            "research_entry_offset_minutes": trade.research_entry_offset_minutes,
            "research_exit_offset_minutes": trade.research_exit_offset_minutes,
            "runner_semantics_status": trade.runner_semantics_status,
            "min_option_hold_minutes": trade.min_option_hold_minutes,
            "runner_hard_exit_mode": trade.runner_hard_exit_mode,
        }

    def _notify_lines(self, *lines: object) -> bool:
        if not self.submit_paper_orders:
            return False
        delivered = False
        for notifier in (
            getattr(self, "ntfy_notifier", None),
            getattr(self, "email_notifier", None),
            getattr(self, "discord_notifier", None),
        ):
            if notifier is None or not getattr(notifier, "enabled", False):
                continue
            delivered = notifier.send_lines(*lines) or delivered
        return delivered

    def _open_positions_by_ticker_line(self, session: SessionState) -> str:
        counts = Counter(str(trade["underlying_symbol"]) for trade in session.open_trades)
        if not counts:
            return "Open positions by ticker: none"
        summary = ", ".join(f"{symbol} x{counts[symbol]}" for symbol in sorted(counts))
        return f"Open positions by ticker: {summary}"

    def _strategy_pnl_summary_lines(
        self,
        session: SessionState,
        *,
        limit: int = 3,
    ) -> list[str]:
        if not session.completed_trades:
            return ["Strategy day PnL: no closed trades yet"]

        totals: dict[str, float] = {}
        for trade in session.completed_trades:
            strategy_name = str(trade.get("strategy_name", "unknown"))
            totals[strategy_name] = totals.get(strategy_name, 0.0) + float(trade.get("net_pnl", 0.0))

        winners = [
            (name, pnl)
            for name, pnl in sorted(totals.items(), key=lambda item: (-item[1], item[0]))
            if pnl > 0
        ][:limit]
        losers = [
            (name, pnl)
            for name, pnl in sorted(totals.items(), key=lambda item: (item[1], item[0]))
            if pnl < 0
        ][:limit]

        lines: list[str] = []
        if winners:
            winner_summary = "; ".join(
                f"{name} {_format_signed_dollars(pnl)}" for name, pnl in winners
            )
            lines.append(f"Top strategy PnL: {winner_summary}")
        if losers:
            loser_summary = "; ".join(
                f"{name} {_format_signed_dollars(pnl)}" for name, pnl in losers
            )
            lines.append(f"Lagging strategies: {loser_summary}")
        if not lines:
            lines.append("Strategy day PnL: flat so far")
        return lines

    def _build_morning_notification_lines(
        self,
        session: SessionState,
        details: dict[str, Any],
    ) -> list[object]:
        return [
            "**Multi-Ticker Portfolio Morning Check**",
            f"Trade date: {session.trade_date}",
            f"Buying power: ${float(details.get('buying_power', 0.0)):,.2f}",
            f"Required buying power: ${float(details.get('required_buying_power', 0.0)):,.2f}",
            f"Strategies loaded: {len(self.portfolio_config.strategies)} across {len(self.underlyings)} tickers",
            self._open_positions_by_ticker_line(session),
            "Startup check passed. Paper trader is live for RTH.",
        ]

    def _build_midday_notification_lines(
        self,
        session: SessionState,
        *,
        current_equity: float,
    ) -> list[object]:
        day_pnl = current_equity - session.starting_equity
        open_symbols = sorted({trade["underlying_symbol"] for trade in session.open_trades})
        return [
            "**Multi-Ticker Portfolio Midday Update**",
            f"Trade date: {session.trade_date}",
            f"Current equity: ${current_equity:,.2f}",
            f"Day PnL: {_format_signed_dollars(day_pnl)}",
            f"Completed trades: {len(session.completed_trades)}",
            f"Open trades: {len(session.open_trades)}",
            f"Active symbols: {', '.join(open_symbols) if open_symbols else 'none'}",
            self._open_positions_by_ticker_line(session),
            *self._strategy_pnl_summary_lines(session),
        ]

    def _build_end_of_day_notification_lines(
        self,
        session: SessionState,
        *,
        ending_equity: float,
    ) -> list[object]:
        return [
            "**Multi-Ticker Portfolio End Of Day**",
            f"Trade date: {session.trade_date}",
            f"Ending equity: ${ending_equity:,.2f}",
            f"Day PnL: {_format_signed_dollars(ending_equity - session.starting_equity)}",
            f"Completed trades: {len(session.completed_trades)}",
            f"Blocked new entries: {'yes' if session.blocked_new_entries else 'no'}",
            self._open_positions_by_ticker_line(session),
            *self._strategy_pnl_summary_lines(session),
        ]

    def _record_notification_failure(self, session: SessionState, phase: str) -> None:
        failed_phases = getattr(self, "_failed_notification_phases", None)
        if failed_phases is None:
            failed_phases = set()
            self._failed_notification_phases = failed_phases
        failed_phases.add(phase)
        message = f"{phase.title()} notification delivery failed"
        if any(alert.get("message") == message for alert in session.alerts):
            return
        self._alert(session, "warning", message)

    def _fetch_today_stock_frames(self, trade_date: date) -> dict[str, pd.DataFrame]:
        start = _rth_open_for(trade_date).astimezone(UTC)
        end = _now_et().astimezone(UTC)
        if end <= start:
            return {symbol: build_stock_frame([]) for symbol in self.underlyings}
        payload = self.broker.get_stock_bars(
            self.underlyings,
            start=start,
            end=end,
            timeframe="1Min",
            feed=self.portfolio_config.execution.stock_feed or self.settings.alpaca_data_feed,
            limit=10000,
        )
        frames: dict[str, pd.DataFrame] = {}
        for symbol in self.underlyings:
            rows = payload.get("bars", {}).get(symbol, [])
            frames[symbol] = build_stock_frame(rows)
        return frames

    def _refresh_contract_cache_if_needed(
        self,
        trade_date: date,
        underlying_symbol: str,
    ) -> list[dict[str, Any]]:
        loaded_at = self.contract_cache_loaded_at.get(underlying_symbol)
        stale = (
            loaded_at is None
            or (_now_et() - loaded_at).total_seconds()
            >= self.portfolio_config.execution.contract_refresh_minutes * 60
        )
        cache_key = (underlying_symbol, trade_date.isoformat())
        if stale or cache_key not in self.contract_cache:
            expiration_lte = trade_date + timedelta(days=self.portfolio_config.execution.max_dte_days)
            payload = self.broker.get_option_contracts(
                [underlying_symbol],
                expiration_date_gte=trade_date.isoformat(),
                expiration_date_lte=expiration_lte.isoformat(),
                status="active",
                limit=1000,
            )
            contracts: list[dict[str, Any]] = []
            for contract in payload.get("option_contracts", []):
                expiration_date = date.fromisoformat(str(contract["expiration_date"]))
                contracts.append(
                    {
                        "symbol": str(contract["symbol"]),
                        "underlying_symbol": underlying_symbol,
                        "expiration_date": expiration_date,
                        "strike_price": float(contract["strike_price"]),
                        "option_type": str(contract["type"]),
                    }
                )
            self.contract_cache[cache_key] = contracts
            self.contract_cache_loaded_at[underlying_symbol] = _now_et()
        return self.contract_cache[cache_key]

    def _candidate_symbols_for_snapshot(
        self,
        contracts: list[dict[str, Any]],
        spot_price: float,
        trade_date: date,
    ) -> tuple[list[str], dict[str, dict[str, Any]]]:
        if not contracts:
            return [], {}
        same_day_expiry = trade_date
        next_expiry_candidates = sorted(
            {contract["expiration_date"] for contract in contracts if contract["expiration_date"] > trade_date}
        )
        next_expiry = next_expiry_candidates[0] if next_expiry_candidates else None
        keep: list[dict[str, Any]] = []
        for expiry in [same_day_expiry, next_expiry]:
            if expiry is None:
                continue
            for option_type in ("call", "put"):
                subset = [
                    contract
                    for contract in contracts
                    if contract["expiration_date"] == expiry and contract["option_type"] == option_type
                ]
                subset = sorted(subset, key=lambda item: abs(item["strike_price"] - spot_price))
                keep.extend(subset[:14])
        metadata = {contract["symbol"]: contract for contract in keep}
        return [contract["symbol"] for contract in keep], metadata

    def _mark_from_snapshot(
        self,
        snapshot: dict[str, Any],
    ) -> tuple[float | None, float | None, float | None, str | None]:
        bid, ask, mark, quote_time, _source = self._mark_from_snapshot_with_source(snapshot)
        return bid, ask, mark, quote_time

    def _mark_from_snapshot_with_source(
        self,
        snapshot: dict[str, Any],
    ) -> tuple[float | None, float | None, float | None, str | None, str | None]:
        latest_quote = snapshot.get("latestQuote", {}) or {}
        latest_trade = snapshot.get("latestTrade", {}) or {}
        minute_bar = snapshot.get("minuteBar", {}) or {}
        bid = latest_quote.get("bp")
        ask = latest_quote.get("ap")
        quote_time = latest_quote.get("t")
        bid_value = float(bid) if bid not in (None, "") else None
        ask_value = float(ask) if ask not in (None, "") else None
        if bid_value is not None and ask_value is not None and ask_value >= bid_value > 0.0:
            return bid_value, ask_value, (bid_value + ask_value) / 2.0, quote_time, "option_quote_bid_ask"
        trade_price = latest_trade.get("p")
        if trade_price not in (None, ""):
            price = float(trade_price)
            return price, price, price, latest_trade.get("t"), "option_trade_print_no_bid_ask"
        bar_close = minute_bar.get("c")
        if bar_close not in (None, ""):
            price = float(bar_close)
            return price, price, price, minute_bar.get("t"), "option_bar_close_no_bid_ask"
        return None, None, None, None, None

    def _bbo_quote_quality_from_snapshot(
        self,
        *,
        symbol: str,
        snapshot: dict[str, Any],
        now_et: datetime,
    ) -> dict[str, Any] | None:
        latest_quote = snapshot.get("latestQuote", {}) or {}
        bid = latest_quote.get("bp")
        ask = latest_quote.get("ap")
        quote_time = latest_quote.get("t")
        bid_value = float(bid) if bid not in (None, "") else None
        ask_value = float(ask) if ask not in (None, "") else None
        if bid_value is None or ask_value is None or not (ask_value >= bid_value > 0.0):
            return None
        mark = (bid_value + ask_value) / 2.0
        spread_pct = max(0.0, (ask_value - bid_value) / max(mark, 0.01))
        freshness_seconds = None
        if quote_time:
            freshness_seconds = max(
                0.0,
                (
                    now_et
                    - datetime.fromisoformat(str(quote_time).replace("Z", "+00:00")).astimezone(ET)
                ).total_seconds(),
            )
        return {
            "symbol": symbol,
            "bid": bid_value,
            "ask": ask_value,
            "mark": mark,
            "quote_time": quote_time,
            "spread_pct": spread_pct,
            "freshness_seconds": freshness_seconds,
            "quote_source": "option_quote_bid_ask",
        }

    def _exit_leg_quality_map(
        self,
        trade: OpenTrade,
        option_chain: pd.DataFrame | None,
    ) -> dict[str, dict[str, Any]]:
        quality_by_symbol: dict[str, dict[str, Any]] = {}
        if option_chain is not None and not option_chain.empty and "symbol" in option_chain.columns:
            for row in option_chain.to_dict("records"):
                symbol = str(row.get("symbol") or "")
                if symbol:
                    quality_by_symbol[symbol] = row
        missing_symbols = [
            str(leg.get("symbol") or "").strip()
            for leg in trade.legs
            if str(leg.get("symbol") or "").strip()
            and str(leg.get("symbol") or "").strip() not in quality_by_symbol
        ]
        get_snapshots = getattr(getattr(self, "broker", None), "get_option_snapshots", None)
        if not missing_symbols or not callable(get_snapshots):
            return quality_by_symbol
        now_et = _now_et()
        for batch in _chunked(sorted(set(missing_symbols)), 50):
            try:
                payload = get_snapshots(
                    batch,
                    feed=self.portfolio_config.execution.option_feed,
                )
            except Exception as exc:  # noqa: BLE001 - exit should not fail because quote audit enrichment failed.
                self.logger.warning("failed to fetch exit quote quality for %s: %r", batch, exc)
                continue
            for symbol, snapshot in (payload.get("snapshots") or {}).items():
                if not isinstance(snapshot, dict):
                    continue
                quality = self._bbo_quote_quality_from_snapshot(
                    symbol=str(symbol),
                    snapshot=snapshot,
                    now_et=now_et,
                )
                if quality is not None:
                    quality_by_symbol[str(symbol)] = quality
        return quality_by_symbol

    def _fetch_option_chain(
        self,
        symbols: list[str],
        metadata: dict[str, dict[str, Any]],
        *,
        spot_price: float,
        trade_date: date,
        underlying_symbol: str,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        rows: list[dict[str, Any]] = []
        now_et = _now_et()
        for batch in _chunked(symbols, 50):
            payload = self.broker.get_option_snapshots(
                batch,
                feed=self.portfolio_config.execution.option_feed,
            )
            for symbol, snapshot in payload.get("snapshots", {}).items():
                info = metadata.get(symbol)
                if info is None:
                    continue
                bid, ask, mark, quote_time, quote_source = self._mark_from_snapshot_with_source(snapshot)
                if mark is None or mark <= 0.0:
                    continue
                expiration_date = info["expiration_date"]
                expiration_ts = datetime.combine(expiration_date, dt_time(16, 0), tzinfo=ET)
                years_to_expiry = max(60.0, (expiration_ts - now_et).total_seconds()) / (
                    365.0 * 24.0 * 3600.0
                )
                iv = implied_volatility(
                    spot=spot_price,
                    strike=float(info["strike_price"]),
                    years=years_to_expiry,
                    market_price=mark,
                    option_type=str(info["option_type"]),
                )
                if iv is None:
                    continue
                greeks = bs_greeks(
                    spot=spot_price,
                    strike=float(info["strike_price"]),
                    years=years_to_expiry,
                    sigma=iv,
                    option_type=str(info["option_type"]),
                )
                spread_pct = 0.0
                if bid is not None and ask is not None and ask > 0.0:
                    spread_pct = max(0.0, (ask - bid) / max(mark, 0.01))
                freshness_seconds = None
                if quote_time:
                    freshness_seconds = max(
                        0.0,
                        (
                            now_et
                            - datetime.fromisoformat(str(quote_time).replace("Z", "+00:00")).astimezone(ET)
                        ).total_seconds(),
                    )
                rows.append(
                    {
                        "symbol": symbol,
                        "underlying_symbol": underlying_symbol,
                        "expiration_date": expiration_date,
                        "dte": max(0, (expiration_date - trade_date).days),
                        "option_type": str(info["option_type"]),
                        "strike_price": float(info["strike_price"]),
                        "bid": float(bid if bid is not None else mark),
                        "ask": float(ask if ask is not None else mark),
                        "mark": float(mark),
                        "delta": float(greeks["delta"]),
                        "gamma": float(greeks["gamma"]),
                        "theta": float(greeks["theta"]),
                        "vega": float(greeks["vega"]),
                        "implied_vol": float(iv),
                        "spread_pct": float(spread_pct),
                        "freshness_seconds": freshness_seconds,
                        "quote_time": quote_time,
                        "quote_source": quote_source,
                    }
                )
        return pd.DataFrame(rows)

    def _select_legs(
        self,
        strategy: StrategyConfig,
        option_chain: pd.DataFrame,
        trade_date: date,
    ) -> list[SelectedLeg]:
        if option_chain.empty:
            return []
        if strategy.dte_mode == "same_day":
            expiry = trade_date
        else:
            future_dates = sorted(
                value for value in option_chain["expiration_date"].unique().tolist() if value > trade_date
            )
            if not future_dates:
                return []
            expiry = future_dates[0]
        used_symbols: set[str] = set()
        legs: list[SelectedLeg] = []
        for leg_template in strategy.legs:
            subset = option_chain[
                (option_chain["expiration_date"] == expiry)
                & (option_chain["option_type"] == leg_template.option_type)
                & (option_chain["mark"] > 0.0)
                & (option_chain["spread_pct"] <= self.portfolio_config.execution.max_relative_spread)
            ].copy()
            if subset.empty:
                return []
            subset = subset[~subset["symbol"].isin(used_symbols)].copy()
            subset = subset[
                (subset["delta"].abs() >= leg_template.min_abs_delta)
                & (subset["delta"].abs() <= leg_template.max_abs_delta)
            ].copy()
            if subset.empty:
                return []
            subset = subset[
                subset["freshness_seconds"].isna()
                | (subset["freshness_seconds"] <= self.portfolio_config.execution.quote_stale_seconds)
            ].copy()
            if subset.empty:
                return []
            subset["delta_distance"] = (subset["delta"] - leg_template.target_delta).abs()
            subset = subset.sort_values(["delta_distance", "spread_pct", "mark"], ascending=[True, True, False])
            chosen = subset.iloc[0]
            selected_leg = SelectedLeg(
                symbol=str(chosen["symbol"]),
                expiration_date=str(chosen["expiration_date"]),
                option_type=str(chosen["option_type"]),
                side=leg_template.side,
                strike_price=float(chosen["strike_price"]),
                target_delta=float(leg_template.target_delta),
                bid=float(chosen["bid"]),
                ask=float(chosen["ask"]),
                mark=float(chosen["mark"]),
                delta=float(chosen["delta"]),
                gamma=float(chosen["gamma"]),
                theta=float(chosen["theta"]),
                vega=float(chosen["vega"]),
                quote_time=chosen["quote_time"] if pd.notna(chosen["quote_time"]) else None,
                spread_pct=float(chosen["spread_pct"]),
                freshness_seconds=float(chosen["freshness_seconds"])
                if pd.notna(chosen["freshness_seconds"])
                else None,
                quote_source=chosen["quote_source"] if pd.notna(chosen.get("quote_source")) else None,
            )
            legs.append(selected_leg)
            used_symbols.add(selected_leg.symbol)
        return legs

    def _effective_risk_scale(self, ledger: PortfolioLedger, current_equity: float) -> float:
        high_watermark = max(ledger.high_watermark, current_equity)
        drawdown_pct = 0.0
        if high_watermark > 0.0:
            drawdown_pct = (1.0 - current_equity / high_watermark) * 100.0
        if drawdown_pct >= self.portfolio_config.risk.delever_drawdown_pct:
            return self.portfolio_config.risk.delever_risk_scale
        return 1.0

    def _regime_risk_scale(self, regime: str) -> float:
        scale = self.portfolio_config.risk.regime_risk_scales.get(str(regime).lower(), 1.0)
        return max(0.0, float(scale))

    def _regime_position_count(self, session: SessionState, regime: str) -> int:
        return sum(1 for trade in session.open_trades if trade["regime"] == regime)

    def _symbol_position_count(self, session: SessionState, underlying_symbol: str) -> int:
        return sum(1 for trade in session.open_trades if trade["underlying_symbol"] == underlying_symbol)

    @staticmethod
    def _trade_entry_minute(trade_payload: dict[str, Any]) -> int | None:
        raw_value = trade_payload.get("entry_minute")
        if raw_value in (None, ""):
            return None
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return None

    def _recent_open_trades(
        self,
        session: SessionState,
        *,
        current_minute: int,
        window_minutes: int | None,
    ) -> list[dict[str, Any]]:
        if window_minutes is None or window_minutes <= 0:
            return list(session.open_trades)
        window_start = max(0, int(current_minute) - int(window_minutes))
        recent_trades: list[dict[str, Any]] = []
        for trade in session.open_trades:
            entry_minute = self._trade_entry_minute(trade)
            if entry_minute is None or entry_minute >= window_start:
                recent_trades.append(trade)
        return recent_trades

    def _recent_regime_position_count(
        self,
        session: SessionState,
        *,
        regime: str,
        current_minute: int,
        window_minutes: int | None,
    ) -> int:
        return sum(
            1
            for trade in self._recent_open_trades(
                session,
                current_minute=current_minute,
                window_minutes=window_minutes,
            )
            if trade["regime"] == regime
        )

    def _recent_bucket_regime_position_count(
        self,
        session: SessionState,
        *,
        bucket: RiskBucketConfig,
        regime: str,
        current_minute: int,
        window_minutes: int | None,
    ) -> int:
        bucket_symbols = set(bucket.symbols)
        return sum(
            1
            for trade in self._recent_open_trades(
                session,
                current_minute=current_minute,
                window_minutes=window_minutes,
            )
            if trade["regime"] == regime and str(trade["underlying_symbol"]).upper() in bucket_symbols
        )

    def _trade_open_risk(self, trade_payload: dict[str, Any]) -> float:
        return float(trade_payload["max_loss_per_combo"]) * int(trade_payload["quantity"])

    def _symbol_open_risk(self, session: SessionState, underlying_symbol: str) -> float:
        return sum(
            self._trade_open_risk(trade)
            for trade in session.open_trades
            if trade["underlying_symbol"] == underlying_symbol
        )

    def _bucket_configs_for_symbol(self, underlying_symbol: str) -> list[RiskBucketConfig]:
        symbol = str(underlying_symbol).upper()
        return [
            bucket
            for bucket in self.portfolio_config.risk.bucket_caps
            if symbol in bucket.symbols
        ]

    def _bucket_open_risk(self, session: SessionState, bucket: RiskBucketConfig) -> float:
        bucket_symbols = set(bucket.symbols)
        return sum(
            self._trade_open_risk(trade)
            for trade in session.open_trades
            if str(trade["underlying_symbol"]).upper() in bucket_symbols
        )

    def _daily_loss_gate_check(self, session: SessionState, current_equity: float) -> tuple[bool, str | None]:
        gate_pct = self.portfolio_config.risk.daily_loss_gate_pct
        if gate_pct is None or gate_pct <= 0.0:
            return False, None
        threshold = session.starting_equity * (1.0 - gate_pct)
        if current_equity <= threshold:
            return True, f"daily_loss_gate triggered at equity {current_equity:.2f}"
        return False, None

    def _severe_loss_kill_switch_check(
        self,
        session: SessionState,
        current_equity: float,
    ) -> tuple[str | None, str | None]:
        flatten_pct = self.portfolio_config.risk.severe_loss_flatten_all_pct
        if flatten_pct is not None and flatten_pct > 0.0:
            flatten_threshold = session.starting_equity * (1.0 - flatten_pct)
            if current_equity <= flatten_threshold:
                return (
                    "flatten",
                    f"{SEVERE_LOSS_FLATTEN_REASON} triggered at equity {current_equity:.2f}",
                )
        halt_pct = self.portfolio_config.risk.severe_loss_halt_new_entries_pct
        if halt_pct is not None and halt_pct > 0.0:
            halt_threshold = session.starting_equity * (1.0 - halt_pct)
            if current_equity <= halt_threshold:
                return (
                    "halt",
                    f"{SEVERE_LOSS_HALT_REASON} triggered at equity {current_equity:.2f}",
                )
        return None, None

    def _extract_broker_equity(self, account: dict[str, Any]) -> float | None:
        for key in ("equity", "portfolio_value", "last_equity"):
            raw_value = account.get(key)
            if raw_value in (None, ""):
                continue
            try:
                return float(raw_value)
            except (TypeError, ValueError):
                continue
        return None

    def _broker_min_equity_to_trade(self) -> float | None:
        threshold = self.portfolio_config.risk.broker_min_equity_to_trade
        if threshold is None or threshold <= 0.0:
            return None
        return float(threshold)

    def _broker_equity_emergency_stop(self) -> float | None:
        threshold = self.portfolio_config.risk.broker_equity_emergency_stop
        if threshold is None or threshold <= 0.0:
            return None
        return float(threshold)

    def _record_guardrail_block(
        self,
        session: SessionState,
        *,
        level: str,
        reason: str,
    ) -> None:
        session.blocked_new_entries = True
        if session.block_reason != reason:
            session.block_reason = reason
            self._alert(session, level, reason)

    def _execution_guardrail_state(self, session: SessionState) -> dict[str, Any]:
        state = session.execution_guardrails
        if not isinstance(state, dict):
            state = {}
            session.execution_guardrails = state
        state["entry_failure_streak"] = int(state.get("entry_failure_streak", 0) or 0)
        samples = state.get("recent_entry_adverse_slippage_fractions", [])
        if not isinstance(samples, list):
            samples = []
        state["recent_entry_adverse_slippage_fractions"] = [
            float(sample) for sample in samples if isinstance(sample, (int, float))
        ]
        state["circuit_breaker_triggered"] = bool(state.get("circuit_breaker_triggered", False))
        circuit_reason = state.get("circuit_breaker_reason")
        state["circuit_breaker_reason"] = str(circuit_reason) if circuit_reason else None
        last_failure_status = state.get("last_failure_status")
        state["last_failure_status"] = str(last_failure_status) if last_failure_status else None
        return state

    def _apply_entry_execution_circuit_breaker(self, session: SessionState) -> None:
        state = self._execution_guardrail_state(session)
        if state["circuit_breaker_triggered"]:
            self._record_guardrail_block(
                session,
                level="error",
                reason=state["circuit_breaker_reason"] or ENTRY_EXECUTION_CIRCUIT_BREAKER_REASON,
            )
            return
        failure_limit = self.portfolio_config.risk.entry_failure_streak_limit
        if failure_limit is not None and failure_limit > 0 and state["entry_failure_streak"] >= failure_limit:
            last_status = state.get("last_failure_status") or "not_filled"
            reason = (
                f"{ENTRY_EXECUTION_CIRCUIT_BREAKER_REASON}: "
                f"{state['entry_failure_streak']} consecutive entry failures "
                f"(last status {last_status})"
            )
            state["circuit_breaker_triggered"] = True
            state["circuit_breaker_reason"] = reason
            self._record_guardrail_block(session, level="error", reason=reason)
            return
        slippage_limit = self.portfolio_config.risk.entry_adverse_slippage_fraction_limit
        lookback = max(1, int(self.portfolio_config.risk.entry_adverse_slippage_lookback))
        samples = state["recent_entry_adverse_slippage_fractions"]
        if (
            slippage_limit is not None
            and slippage_limit > 0.0
            and len(samples) >= lookback
        ):
            recent_samples = samples[-lookback:]
            average_slippage = sum(recent_samples) / len(recent_samples)
            if average_slippage >= slippage_limit:
                reason = (
                    f"{ENTRY_EXECUTION_CIRCUIT_BREAKER_REASON}: "
                    f"average adverse entry slippage {average_slippage:.2%} "
                    f"over last {len(recent_samples)} fills"
                )
                state["circuit_breaker_triggered"] = True
                state["circuit_breaker_reason"] = reason
                self._record_guardrail_block(session, level="error", reason=reason)

    def _record_entry_execution_outcome(
        self,
        session: SessionState,
        *,
        success: bool,
        adverse_slippage_fraction: float | None = None,
        failure_status: str | None = None,
    ) -> None:
        state = self._execution_guardrail_state(session)
        if success:
            state["entry_failure_streak"] = 0
            state["last_failure_status"] = None
            if adverse_slippage_fraction is not None:
                samples = state["recent_entry_adverse_slippage_fractions"]
                samples.append(max(0.0, float(adverse_slippage_fraction)))
                lookback = max(1, int(self.portfolio_config.risk.entry_adverse_slippage_lookback))
                if len(samples) > lookback:
                    del samples[:-lookback]
        else:
            state["entry_failure_streak"] = int(state.get("entry_failure_streak", 0)) + 1
            state["last_failure_status"] = str(failure_status or "not_filled")
        self._apply_entry_execution_circuit_breaker(session)

    def _family_key_from_source_strategy_id(self, value: object) -> str:
        raw = str(value or "")
        parts = [part for part in raw.lower().split("__") if part]
        if len(parts) >= 4:
            return re.sub(r"[^a-z0-9]+", "_", parts[3]).strip("_")
        return re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")

    def _family_key_for_strategy(self, strategy: StrategyConfig) -> str:
        if strategy.source_strategy_id:
            return self._family_key_from_source_strategy_id(strategy.source_strategy_id)
        return re.sub(r"[^a-z0-9]+", "_", str(strategy.family or "").lower()).strip("_")

    def _family_key_for_completed_trade(self, trade: dict[str, Any]) -> str:
        source_strategy_id = trade.get("source_strategy_id")
        if source_strategy_id:
            return self._family_key_from_source_strategy_id(source_strategy_id)
        return re.sub(r"[^a-z0-9]+", "_", str(trade.get("strategy_name") or "").lower()).strip("_")

    def _stop_loss_cooldown_decision(
        self,
        session: SessionState,
        strategy: StrategyConfig,
        *,
        current_minute: int,
    ) -> dict[str, Any] | None:
        cooldown_count = self.portfolio_config.risk.stop_loss_cooldown_count
        cooldown_minutes = self.portfolio_config.risk.stop_loss_cooldown_minutes
        if cooldown_count is None or cooldown_minutes is None:
            return None
        if cooldown_count <= 0 or cooldown_minutes <= 0:
            return None
        scope = self.portfolio_config.risk.stop_loss_cooldown_scope
        family_key = self._family_key_for_strategy(strategy)
        matches: list[dict[str, Any]] = []
        for trade in session.completed_trades:
            if str(trade.get("exit_reason") or "") != "stop_loss":
                continue
            exit_minute_raw = trade.get("exit_minute")
            try:
                exit_minute = int(exit_minute_raw)
            except (TypeError, ValueError):
                continue
            if exit_minute > current_minute:
                continue
            age_minutes = current_minute - exit_minute
            if age_minutes > cooldown_minutes:
                continue
            if scope == "strategy":
                if str(trade.get("strategy_name") or "") != strategy.name and str(
                    trade.get("source_strategy_id") or ""
                ) != str(strategy.source_strategy_id or ""):
                    continue
            elif scope == "symbol_regime":
                if str(trade.get("underlying_symbol") or "").upper() != strategy.underlying_symbol:
                    continue
                if str(trade.get("regime") or "").lower() != strategy.regime:
                    continue
            else:
                if str(trade.get("underlying_symbol") or "").upper() != strategy.underlying_symbol:
                    continue
                if str(trade.get("regime") or "").lower() != strategy.regime:
                    continue
                if self._family_key_for_completed_trade(trade) != family_key:
                    continue
            matches.append(
                {
                    "strategy_name": trade.get("strategy_name"),
                    "source_strategy_id": trade.get("source_strategy_id"),
                    "exit_minute": exit_minute,
                    "age_minutes": age_minutes,
                    "net_pnl": trade.get("net_pnl"),
                }
            )
        if len(matches) < cooldown_count:
            return None
        return {
            "decision_reason": (
                f"{STOP_LOSS_COOLDOWN_REASON}:{scope}:"
                f"{len(matches)}_stop_losses_in_{cooldown_minutes}m"
            ),
            "stop_loss_cooldown_scope": scope,
            "stop_loss_cooldown_count": int(cooldown_count),
            "stop_loss_cooldown_minutes": int(cooldown_minutes),
            "recent_stop_loss_count": len(matches),
            "recent_stop_losses": matches[-10:],
        }

    def _current_portfolio_expected_greeks(self, session: SessionState) -> tuple[float, float]:
        total_delta_shares = 0.0
        total_vega_dollars = 0.0
        for trade_payload in session.open_trades:
            trade = OpenTrade(**trade_payload)
            delta_shares, vega_dollars = self._expected_entry_greeks(trade)
            total_delta_shares += delta_shares
            total_vega_dollars += vega_dollars
        return total_delta_shares, total_vega_dollars

    def _entry_cutoff_minute_for_strategy(self, strategy: StrategyConfig) -> int | None:
        if strategy.dte_mode == "same_day":
            cutoff = self.portfolio_config.risk.same_day_entry_cutoff_minute
            if cutoff is not None and cutoff > 0:
                return int(cutoff)
        cutoff = self.portfolio_config.risk.entry_cutoff_minute
        if cutoff is None or cutoff <= 0:
            return None
        return int(cutoff)

    def _matching_event_blackouts(
        self,
        *,
        strategy: StrategyConfig,
        trade_date: date,
        current_minute: int,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        for blackout in self.portfolio_config.risk.event_blackouts:
            if not blackout.enabled:
                continue
            end_date = blackout.end_date or blackout.start_date
            if trade_date < blackout.start_date or trade_date > end_date:
                continue
            if current_minute < blackout.start_minute or current_minute > blackout.end_minute:
                continue
            if blackout.symbols and strategy.underlying_symbol not in blackout.symbols:
                continue
            if blackout.regimes and strategy.regime not in blackout.regimes:
                continue
            if blackout.timing_profiles and strategy.timing_profile not in blackout.timing_profiles:
                continue
            if blackout.dte_modes and strategy.dte_mode not in blackout.dte_modes:
                continue
            matches.append(
                {
                    "name": blackout.name,
                    "reason": blackout.reason,
                    "start_date": blackout.start_date.isoformat(),
                    "end_date": (blackout.end_date or blackout.start_date).isoformat(),
                    "start_minute": int(blackout.start_minute),
                    "end_minute": int(blackout.end_minute),
                }
            )
        return matches

    def _mark_to_close(self, open_trade: OpenTrade, option_chain: pd.DataFrame) -> dict[str, float]:
        mark_map: dict[str, float] = {}
        for leg in open_trade.legs:
            subset = option_chain[option_chain["symbol"] == leg["symbol"]]
            if subset.empty:
                continue
            mark_map[str(leg["symbol"])] = float(subset.iloc[0]["mark"])
        return mark_map

    def _evaluate_entry(
        self,
        *,
        strategy: StrategyConfig,
        session: SessionState,
        ledger: PortfolioLedger,
        option_chain: pd.DataFrame,
        spot_price: float,
        current_minute: int,
        current_equity: float,
        broker_equity: float | None,
        attempt_id: str,
    ) -> tuple[OpenTrade | None, dict[str, Any]]:
        event: dict[str, Any] = {
            "event_type": "signal_decision",
            "attempt_id": attempt_id,
            "trade_date": session.trade_date,
            "strategy_name": strategy.name,
            "underlying_symbol": strategy.underlying_symbol,
            "regime": strategy.regime,
            "candidate_variant_id": strategy.candidate_variant_id,
            "source_strategy_id": strategy.source_strategy_id,
            "promotion_manifest_path": strategy.promotion_manifest_path,
            "governed_validation_packet_uri": strategy.governed_validation_packet_uri,
            "research_profile": strategy.research_profile,
            "research_entry_timing_mode": strategy.research_entry_timing_mode,
            "research_entry_offset_minutes": strategy.research_entry_offset_minutes,
            "research_exit_offset_minutes": strategy.research_exit_offset_minutes,
            "runner_semantics_status": strategy.runner_semantics_status,
            "min_option_hold_minutes": strategy.min_option_hold_minutes,
            "runner_hard_exit_mode": strategy.runner_hard_exit_mode,
            "signal_name": strategy.signal_name,
            "timing_profile": strategy.timing_profile,
            "current_minute": int(current_minute),
            "current_equity": round(float(current_equity), 4),
            "broker_equity": round(float(broker_equity), 4) if broker_equity is not None else None,
            "decision": "skipped",
            "decision_reason": None,
        }
        if strategy.name in session.signals_fired:
            event["decision_reason"] = "duplicate_signal"
            return None, event
        stop_loss_cooldown = self._stop_loss_cooldown_decision(
            session,
            strategy,
            current_minute=current_minute,
        )
        if stop_loss_cooldown is not None:
            event.update(stop_loss_cooldown)
            return None, event
        if len(session.open_trades) >= self.portfolio_config.risk.max_open_positions:
            event["decision_reason"] = "max_open_positions"
            return None, event
        if self._regime_position_count(session, strategy.regime) >= self.portfolio_config.risk.max_positions_per_regime:
            event["decision_reason"] = "max_positions_per_regime"
            return None, event
        if self._symbol_position_count(session, strategy.underlying_symbol) >= self.portfolio_config.risk.max_positions_per_symbol:
            event["decision_reason"] = "max_positions_per_symbol"
            return None, event
        cluster_window_minutes = self.portfolio_config.risk.entry_cluster_window_minutes
        max_regime_positions_window = self.portfolio_config.risk.max_positions_per_regime_window
        if max_regime_positions_window is not None and max_regime_positions_window > 0:
            recent_regime_positions = self._recent_regime_position_count(
                session,
                regime=strategy.regime,
                current_minute=current_minute,
                window_minutes=cluster_window_minutes,
            )
            if recent_regime_positions >= max_regime_positions_window:
                event.update(
                    {
                        "decision_reason": f"{REGIME_ENTRY_CLUSTER_REASON}:{strategy.regime}",
                        "entry_cluster_window_minutes": int(cluster_window_minutes or 0),
                        "recent_regime_position_count": int(recent_regime_positions),
                        "max_positions_per_regime_window": int(max_regime_positions_window),
                    }
                )
                return None, event
        max_bucket_regime_positions_window = self.portfolio_config.risk.max_positions_per_bucket_regime_window
        if max_bucket_regime_positions_window is not None and max_bucket_regime_positions_window > 0:
            for bucket in self._bucket_configs_for_symbol(strategy.underlying_symbol):
                recent_bucket_regime_positions = self._recent_bucket_regime_position_count(
                    session,
                    bucket=bucket,
                    regime=strategy.regime,
                    current_minute=current_minute,
                    window_minutes=cluster_window_minutes,
                )
                if recent_bucket_regime_positions >= max_bucket_regime_positions_window:
                    event.update(
                        {
                            "decision_reason": f"{BUCKET_REGIME_ENTRY_CLUSTER_REASON}:{bucket.name}:{strategy.regime}",
                            "entry_cluster_window_minutes": int(cluster_window_minutes or 0),
                            "recent_bucket_regime_position_count": int(recent_bucket_regime_positions),
                            "max_positions_per_bucket_regime_window": int(max_bucket_regime_positions_window),
                            "bucket_name": bucket.name,
                        }
                    )
                    return None, event
        if any(trade["strategy_name"] == strategy.name for trade in session.open_trades):
            event["decision_reason"] = "strategy_already_open"
            return None, event
        trade_date = date.fromisoformat(session.trade_date)
        entry_cutoff_minute = self._entry_cutoff_minute_for_strategy(strategy)
        if entry_cutoff_minute is not None and current_minute >= entry_cutoff_minute:
            event["decision_reason"] = LATE_DAY_ENTRY_CUTOFF_REASON
            event["entry_cutoff_minute"] = int(entry_cutoff_minute)
            return None, event
        matching_blackouts = self._matching_event_blackouts(
            strategy=strategy,
            trade_date=trade_date,
            current_minute=current_minute,
        )
        if matching_blackouts:
            first_blackout = matching_blackouts[0]
            event["decision_reason"] = f"{EVENT_BLACKOUT_REASON}:{first_blackout['name']}"
            event["event_blackouts"] = matching_blackouts
            return None, event
        legs = self._select_legs(strategy, option_chain, trade_date)
        if not legs:
            event["decision_reason"] = "no_eligible_legs"
            return None, event
        entry_debit = sum(leg.mark if leg.side == "long" else -leg.mark for leg in legs)
        leg_payloads = [
            {
                "symbol": leg.symbol,
                "expiration_date": leg.expiration_date,
                "option_type": leg.option_type,
                "side": leg.side,
                "strike_price": leg.strike_price,
                "target_delta": leg.target_delta,
                "entry_fill_price": leg.mark,
                "bid": leg.bid,
                "ask": leg.ask,
                "mark": leg.mark,
                "delta": leg.delta,
                "gamma": leg.gamma,
                "theta": leg.theta,
                "vega": leg.vega,
                "quote_time": leg.quote_time,
                "quote_source": leg.quote_source,
                "spread_pct": leg.spread_pct,
                "freshness_seconds": leg.freshness_seconds,
            }
            for leg in legs
        ]
        max_loss_per_combo, max_profit_per_combo = _estimate_combo_bounds(leg_payloads)
        if max_loss_per_combo <= 0.0:
            event["decision_reason"] = "invalid_max_loss"
            return None, event
        drawdown_risk_scale = self._effective_risk_scale(ledger, current_equity)
        regime_risk_scale = self._regime_risk_scale(strategy.regime)
        risk_scale = drawdown_risk_scale * regime_risk_scale
        reserved_risk = sum(
            float(trade["max_loss_per_combo"]) * int(trade["quantity"]) for trade in session.open_trades
        )
        symbol_reserved_risk = self._symbol_open_risk(session, strategy.underlying_symbol)
        remaining_risk = max(
            0.0,
            current_equity * self.portfolio_config.risk.max_open_risk_fraction * risk_scale - reserved_risk,
        )
        per_trade_budget = current_equity * strategy.risk_fraction * risk_scale
        allocatable_risk = min(remaining_risk, per_trade_budget)
        limiting_reason: str | None = None
        symbol_remaining_risk: float | None = None
        per_symbol_cap = self.portfolio_config.risk.max_open_risk_fraction_per_symbol
        if per_symbol_cap is not None and per_symbol_cap > 0.0:
            symbol_remaining_risk = max(
                0.0,
                current_equity * per_symbol_cap * risk_scale - symbol_reserved_risk,
            )
            if symbol_remaining_risk < allocatable_risk:
                limiting_reason = "per_symbol_risk_cap"
            allocatable_risk = min(allocatable_risk, symbol_remaining_risk)
        bucket_remaining_risk: dict[str, float] = {}
        for bucket in self._bucket_configs_for_symbol(strategy.underlying_symbol):
            bucket_reserved_risk = self._bucket_open_risk(session, bucket)
            remaining_bucket_risk = max(
                0.0,
                current_equity * bucket.max_open_risk_fraction * risk_scale - bucket_reserved_risk,
            )
            bucket_remaining_risk[bucket.name] = round(remaining_bucket_risk, 4)
            if remaining_bucket_risk < allocatable_risk:
                limiting_reason = f"bucket_risk_cap:{bucket.name}"
            allocatable_risk = min(allocatable_risk, remaining_bucket_risk)
        min_broker_equity = self._broker_min_equity_to_trade()
        broker_remaining_risk: float | None = None
        if broker_equity is not None and min_broker_equity is not None:
            if broker_equity < min_broker_equity:
                event["decision_reason"] = "broker_equity_below_trade_floor"
                event["broker_min_equity_to_trade"] = round(min_broker_equity, 4)
                return None, event
            broker_remaining_risk = max(0.0, broker_equity - min_broker_equity - reserved_risk)
            allocatable_risk = min(allocatable_risk, broker_remaining_risk)
        quantity_by_risk = math.floor(allocatable_risk / max_loss_per_combo)
        if quantity_by_risk < 1:
            if broker_remaining_risk is not None and broker_remaining_risk < max_loss_per_combo:
                event["decision_reason"] = "broker_equity_risk_buffer"
                event["broker_min_equity_to_trade"] = round(min_broker_equity or 0.0, 4)
                event["broker_remaining_risk"] = round(broker_remaining_risk, 4)
            elif symbol_remaining_risk is not None and symbol_remaining_risk < max_loss_per_combo:
                event["decision_reason"] = "per_symbol_risk_cap"
                event["symbol_reserved_risk"] = round(symbol_reserved_risk, 4)
                event["symbol_remaining_risk"] = round(symbol_remaining_risk, 4)
            elif limiting_reason is not None and limiting_reason.startswith("bucket_risk_cap:"):
                event["decision_reason"] = limiting_reason
                event["bucket_remaining_risk"] = bucket_remaining_risk
            else:
                event["decision_reason"] = limiting_reason or "risk_budget_too_small"
            event["risk_scale"] = round(risk_scale, 6)
            event["drawdown_risk_scale"] = round(drawdown_risk_scale, 6)
            event["regime_risk_scale"] = round(regime_risk_scale, 6)
            event["reserved_risk"] = round(reserved_risk, 4)
            event["symbol_reserved_risk"] = round(symbol_reserved_risk, 4)
            event["remaining_risk"] = round(remaining_risk, 4)
            event["per_trade_budget"] = round(per_trade_budget, 4)
            if symbol_remaining_risk is not None:
                event["symbol_remaining_risk"] = round(symbol_remaining_risk, 4)
            if bucket_remaining_risk:
                event["bucket_remaining_risk"] = bucket_remaining_risk
            return None, event
        debit_cash = max(0.0, entry_debit * CONTRACT_MULTIPLIER)
        quantity_by_cash = (
            math.floor(max(0.0, session.virtual_cash) / debit_cash)
            if debit_cash > 0.0
            else strategy.max_contracts
        )
        quantity = min(strategy.max_contracts, quantity_by_risk, quantity_by_cash)
        if quantity < 1:
            event["decision_reason"] = "insufficient_cash"
            event["debit_cash_per_combo"] = round(debit_cash, 4)
            event["virtual_cash"] = round(float(session.virtual_cash), 4)
            return None, event
        open_trade = OpenTrade(
            strategy_name=strategy.name,
            underlying_symbol=strategy.underlying_symbol,
            regime=strategy.regime,
            quantity=quantity,
            entry_time_et=_now_et().isoformat(),
            entry_minute=current_minute,
            hard_exit_minute=strategy.hard_exit_minute,
            underlying_entry=spot_price,
            entry_debit=entry_debit,
            max_loss_per_combo=max_loss_per_combo,
            max_profit_per_combo=max_profit_per_combo,
            profit_target_dollars=abs(entry_debit) * CONTRACT_MULTIPLIER * strategy.profit_target_multiple,
            stop_loss_dollars=abs(entry_debit) * CONTRACT_MULTIPLIER * strategy.stop_loss_multiple,
            entry_order_id=None,
            entry_fill_price=entry_debit,
            legs=leg_payloads,
            entry_attempt_id=attempt_id,
            candidate_variant_id=strategy.candidate_variant_id,
            source_strategy_id=strategy.source_strategy_id,
            promotion_manifest_path=strategy.promotion_manifest_path,
            governed_validation_packet_uri=strategy.governed_validation_packet_uri,
            research_profile=strategy.research_profile,
            research_entry_timing_mode=strategy.research_entry_timing_mode,
            research_entry_offset_minutes=strategy.research_entry_offset_minutes,
            research_exit_offset_minutes=strategy.research_exit_offset_minutes,
            runner_semantics_status=strategy.runner_semantics_status,
            min_option_hold_minutes=strategy.min_option_hold_minutes,
            runner_hard_exit_mode=strategy.runner_hard_exit_mode,
        )
        delta_shares, vega_dollars = self._expected_entry_greeks(open_trade)
        portfolio_delta_shares, portfolio_vega_dollars = self._current_portfolio_expected_greeks(session)
        projected_delta_shares = portfolio_delta_shares + delta_shares
        projected_vega_dollars = portfolio_vega_dollars + vega_dollars
        hard_delta_cap = self.portfolio_config.risk.hard_cap_delta_shares
        if hard_delta_cap is not None and hard_delta_cap > 0.0 and abs(projected_delta_shares) > hard_delta_cap:
            event.update(
                {
                    "decision_reason": PROJECTED_DELTA_HARD_CAP_REASON,
                    "current_portfolio_delta_shares": round(float(portfolio_delta_shares), 4),
                    "projected_portfolio_delta_shares": round(float(projected_delta_shares), 4),
                    "hard_cap_delta_shares": round(float(hard_delta_cap), 4),
                    "expected_delta_shares": round(float(delta_shares), 4),
                }
            )
            return None, event
        hard_vega_cap = self.portfolio_config.risk.hard_cap_vega_dollars_1pct
        if hard_vega_cap is not None and hard_vega_cap > 0.0 and abs(projected_vega_dollars) > hard_vega_cap:
            event.update(
                {
                    "decision_reason": PROJECTED_VEGA_HARD_CAP_REASON,
                    "current_portfolio_vega_dollars_1pct": round(float(portfolio_vega_dollars), 4),
                    "projected_portfolio_vega_dollars_1pct": round(float(projected_vega_dollars), 4),
                    "hard_cap_vega_dollars_1pct": round(float(hard_vega_cap), 4),
                    "expected_vega_dollars_1pct": round(float(vega_dollars), 4),
                }
            )
            return None, event
        event.update(
            {
                "decision": "eligible",
                "decision_reason": "eligible",
                "quantity_planned": int(quantity),
                "risk_scale": round(risk_scale, 6),
                "drawdown_risk_scale": round(drawdown_risk_scale, 6),
                "regime_risk_scale": round(regime_risk_scale, 6),
                "reserved_risk": round(reserved_risk, 4),
                "symbol_reserved_risk": round(symbol_reserved_risk, 4),
                "remaining_risk": round(remaining_risk, 4),
                "per_trade_budget": round(per_trade_budget, 4),
                "allocatable_risk": round(allocatable_risk, 4),
                "quantity_by_risk": int(quantity_by_risk),
                "quantity_by_cash": int(quantity_by_cash),
                "symbol_remaining_risk": round(symbol_remaining_risk, 4)
                if symbol_remaining_risk is not None
                else None,
                "bucket_remaining_risk": bucket_remaining_risk or None,
                "broker_remaining_risk": round(broker_remaining_risk, 4)
                if broker_remaining_risk is not None
                else None,
                "expected_entry_debit": round(float(entry_debit), 4),
                "expected_entry_fill_price": round(float(open_trade.entry_fill_price), 4),
                "max_loss_per_combo": round(float(max_loss_per_combo), 4),
                "max_profit_per_combo": round(float(max_profit_per_combo), 4),
                "expected_delta_shares": round(float(delta_shares), 4),
                "expected_vega_dollars_1pct": round(float(vega_dollars), 4),
                "current_portfolio_delta_shares": round(float(portfolio_delta_shares), 4),
                "projected_portfolio_delta_shares": round(float(projected_delta_shares), 4),
                "hard_cap_delta_shares": round(float(hard_delta_cap), 4) if hard_delta_cap is not None else None,
                "current_portfolio_vega_dollars_1pct": round(float(portfolio_vega_dollars), 4),
                "projected_portfolio_vega_dollars_1pct": round(float(projected_vega_dollars), 4),
                "hard_cap_vega_dollars_1pct": round(float(hard_vega_cap), 4)
                if hard_vega_cap is not None
                else None,
            }
        )
        return open_trade, event

    def _simple_entry_order_requests(self, trade: OpenTrade) -> list[OrderRequest]:
        leg = trade.legs[0]
        mark = float(leg["mark"])
        ask = float(leg["ask"])
        limits = [min(ask, mark + 0.02), ask]
        request_seed = self._order_request_seed(trade=trade, phase="entry")
        requests: list[OrderRequest] = []
        for request_index, price in enumerate(limits, start=1):
            limit_price = round(max(0.01, price), 2)
            requests.append(
                self.broker.build_order_request(
                    symbol=str(leg["symbol"]),
                    side="buy",
                    strategy_name=trade.strategy_name,
                    asset_class="option",
                    qty=float(trade.quantity),
                    order_type="limit",
                    time_in_force="day",
                    limit_price=limit_price,
                    client_order_key=f"{request_seed}|limit|{request_index}|{limit_price:.2f}",
                    extra={"position_intent": "buy_to_open"},
                )
            )
        return requests

    @staticmethod
    def _normalize_combo_limit_price(raw_price: float) -> float:
        if raw_price > 0.0:
            return round(max(0.01, raw_price), 2)
        if raw_price < 0.0:
            return round(min(-0.01, raw_price), 2)
        return 0.01

    @staticmethod
    def _entry_order_leg(leg: dict[str, Any]) -> OrderLeg:
        leg_side = str(leg["side"])
        if leg_side == "long":
            return OrderLeg(
                symbol=str(leg["symbol"]),
                side="buy",
                ratio_qty=1,
                position_intent="buy_to_open",
            )
        return OrderLeg(
            symbol=str(leg["symbol"]),
            side="sell",
            ratio_qty=1,
            position_intent="sell_to_open",
        )

    @staticmethod
    def _exit_order_leg(leg: dict[str, Any]) -> OrderLeg:
        leg_side = str(leg["side"])
        if leg_side == "long":
            return OrderLeg(
                symbol=str(leg["symbol"]),
                side="sell",
                ratio_qty=1,
                position_intent="sell_to_close",
            )
        return OrderLeg(
            symbol=str(leg["symbol"]),
            side="buy",
            ratio_qty=1,
            position_intent="buy_to_close",
        )

    def _multileg_entry_order_requests(self, trade: OpenTrade) -> list[OrderRequest]:
        request_seed = self._order_request_seed(trade=trade, phase="entry")
        first_pass_prices = [
            min(float(leg["ask"]), float(leg["mark"]) + 0.02)
            if str(leg["side"]) == "long"
            else max(float(leg["bid"]), float(leg["mark"]) - 0.02)
            for leg in trade.legs
        ]
        second_pass_prices = [
            float(leg["ask"]) if str(leg["side"]) == "long" else float(leg["bid"]) for leg in trade.legs
        ]
        requests: list[OrderRequest] = []
        for request_index, leg_prices in enumerate((first_pass_prices, second_pass_prices), start=1):
            net_debit = sum(
                price if str(leg["side"]) == "long" else -price
                for leg, price in zip(trade.legs, leg_prices, strict=True)
            )
            limit_price = self._normalize_combo_limit_price(net_debit)
            requests.append(
                self.broker.build_multileg_order_request(
                    strategy_name=trade.strategy_name,
                    qty=int(trade.quantity),
                    legs=[self._entry_order_leg(leg) for leg in trade.legs],
                    order_type="limit",
                    time_in_force="day",
                    limit_price=limit_price,
                    client_order_key=f"{request_seed}|mleg|limit|{request_index}|{limit_price:.2f}",
                )
            )
        return requests

    def _simple_exit_order_requests(
        self,
        trade: OpenTrade,
        mark_map: dict[str, float],
        *,
        market_fallback: bool,
    ) -> list[OrderRequest]:
        leg = trade.legs[0]
        mark = float(mark_map[str(leg["symbol"])])
        bid = float(leg["bid"])
        request_seed = self._order_request_seed(trade=trade, phase="exit")
        first_limit = round(max(0.01, max(bid, mark - 0.02)), 2)
        second_limit = round(max(0.01, bid), 2)
        requests = [
            self.broker.build_order_request(
                symbol=str(leg["symbol"]),
                side="sell",
                strategy_name=f"{trade.strategy_name}_exit",
                asset_class="option",
                qty=float(trade.quantity),
                order_type="limit",
                time_in_force="day",
                limit_price=first_limit,
                client_order_key=f"{request_seed}|limit|1|{first_limit:.2f}",
                extra={"position_intent": "sell_to_close"},
            ),
            self.broker.build_order_request(
                symbol=str(leg["symbol"]),
                side="sell",
                strategy_name=f"{trade.strategy_name}_exit",
                asset_class="option",
                qty=float(trade.quantity),
                order_type="limit",
                time_in_force="day",
                limit_price=second_limit,
                client_order_key=f"{request_seed}|limit|2|{second_limit:.2f}",
                extra={"position_intent": "sell_to_close"},
            ),
        ]
        if market_fallback:
            requests.append(
                self.broker.build_order_request(
                    symbol=str(leg["symbol"]),
                    side="sell",
                    strategy_name=f"{trade.strategy_name}_exit",
                    asset_class="option",
                    qty=float(trade.quantity),
                    order_type="market",
                    time_in_force="day",
                    client_order_key=f"{request_seed}|market|3",
                    extra={"position_intent": "sell_to_close"},
                )
            )
        return requests

    def _multileg_exit_order_requests(
        self,
        trade: OpenTrade,
        mark_map: dict[str, float],
    ) -> list[OrderRequest]:
        request_seed = self._order_request_seed(trade=trade, phase="exit")
        first_pass_cashflow = 0.0
        second_pass_cashflow = 0.0
        for leg in trade.legs:
            leg_side = str(leg["side"])
            mark = float(mark_map[str(leg["symbol"])])
            bid = float(leg["bid"])
            ask = float(leg["ask"])
            if leg_side == "long":
                first_fill_price = max(bid, mark - 0.02)
                second_fill_price = bid
                first_pass_cashflow += first_fill_price * CONTRACT_MULTIPLIER
                second_pass_cashflow += second_fill_price * CONTRACT_MULTIPLIER
            else:
                first_fill_price = min(ask, mark + 0.02)
                second_fill_price = ask
                first_pass_cashflow -= first_fill_price * CONTRACT_MULTIPLIER
                second_pass_cashflow -= second_fill_price * CONTRACT_MULTIPLIER
        requests: list[OrderRequest] = []
        for request_index, cashflow in enumerate((first_pass_cashflow, second_pass_cashflow), start=1):
            limit_price = self._normalize_combo_limit_price(-(cashflow / CONTRACT_MULTIPLIER))
            requests.append(
                self.broker.build_multileg_order_request(
                    strategy_name=f"{trade.strategy_name}_exit",
                    qty=int(trade.quantity),
                    legs=[self._exit_order_leg(leg) for leg in trade.legs],
                    order_type="limit",
                    time_in_force="day",
                    limit_price=limit_price,
                    client_order_key=f"{request_seed}|mleg|limit|{request_index}|{limit_price:.2f}",
                )
            )
        return requests

    def _entry_order_requests(self, trade: OpenTrade) -> list[OrderRequest]:
        if len(trade.legs) > 1:
            return self._multileg_entry_order_requests(trade)
        return self._simple_entry_order_requests(trade)

    def _exit_order_requests(
        self,
        trade: OpenTrade,
        mark_map: dict[str, float],
        *,
        market_fallback: bool,
    ) -> list[OrderRequest]:
        if len(trade.legs) > 1:
            return self._multileg_exit_order_requests(trade, mark_map)
        return self._simple_exit_order_requests(trade, mark_map, market_fallback=market_fallback)

    def _order_request_seed(self, *, trade: OpenTrade, phase: str) -> str:
        base = trade.entry_attempt_id or trade.entry_time_et
        return f"{base}|{phase}|{_now_et().isoformat(timespec='microseconds')}"

    @staticmethod
    def _with_attempt_client_order_id(request: OrderRequest, *, attempt_index: int) -> OrderRequest:
        if attempt_index <= 1 or not request.client_order_id:
            return request
        suffix = f"-r{attempt_index}"
        max_length = 48
        trimmed = request.client_order_id[: max(1, max_length - len(suffix))]
        return replace(request, client_order_id=f"{trimmed}{suffix}")

    def _heartbeat_runtime_ownership(self, *, context: str) -> None:
        try:
            status = self.acquire_runtime_ownership(role="portfolio_trader")
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("runtime ownership heartbeat failed during %s: %s", context, exc)
            return
        if status.blocked:
            self.logger.error(
                "runtime ownership blocked during %s by owner=%s label=%s expires_at=%s",
                context,
                status.blocked_by_owner_id,
                status.blocked_by_owner_label,
                status.expires_at,
            )

    def _get_order_status_or_last(
        self,
        order_id: str,
        *,
        last: dict[str, Any] | None,
        poll_error_count: int,
        context: str,
    ) -> tuple[dict[str, Any], int]:
        try:
            payload = self.broker.get_order(order_id)
        except Exception as exc:  # noqa: BLE001
            poll_error_count += 1
            self.logger.warning("order status poll failed during %s for %s: %s", context, order_id, exc)
            fallback = dict(last or {})
            fallback.setdefault("id", order_id)
            if not fallback.get("status"):
                # Treat unknown order state as still cancelable at timeout. A stale open
                # order is more dangerous than a failed cancel request against a filled one.
                fallback["status"] = "new"
            fallback["order_status_poll_error_count"] = poll_error_count
            fallback["order_status_poll_error"] = str(exc)
            fallback["order_status_poll_unavailable"] = True
            return fallback, poll_error_count
        if poll_error_count:
            payload = dict(payload)
            payload["order_status_poll_error_count"] = poll_error_count
        return payload, poll_error_count

    def _request_order_cancel(self, order_id: str, *, context: str) -> bool:
        try:
            self.broker.cancel_order(order_id, dry_run=False, explicitly_requested=True)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("order cancel failed during %s for %s: %s", context, order_id, exc)
            return False
        return True

    def _wait_for_terminal_order(
        self,
        order_id: str,
        *,
        session: SessionState | None = None,
    ) -> dict[str, Any]:
        deadline = time.time() + self.portfolio_config.execution.order_fill_timeout_seconds
        poll_error_count = 0
        last, poll_error_count = self._get_order_status_or_last(
            order_id,
            last=None,
            poll_error_count=poll_error_count,
            context="order_wait_initial",
        )
        while time.time() < deadline:
            status = str(last.get("status", ""))
            if status in TERMINAL_STATUSES:
                return last
            self._heartbeat_runtime_ownership(context=f"order_wait:{order_id}")
            if session is not None:
                self.save_session(session)
            time.sleep(self.portfolio_config.execution.order_status_poll_seconds)
            last, poll_error_count = self._get_order_status_or_last(
                order_id,
                last=last,
                poll_error_count=poll_error_count,
                context="order_wait",
            )
        return last

    def _wait_for_terminal_order_with_timeout(
        self,
        order_id: str,
        *,
        timeout_seconds: int,
        session: SessionState | None = None,
    ) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        poll_error_count = 0
        last, poll_error_count = self._get_order_status_or_last(
            order_id,
            last=None,
            poll_error_count=poll_error_count,
            context="order_wait_initial",
        )
        while time.time() < deadline:
            status = str(last.get("status", ""))
            if status in TERMINAL_STATUSES:
                return last
            self._heartbeat_runtime_ownership(context=f"order_wait:{order_id}")
            if session is not None:
                self.save_session(session)
            time.sleep(self.portfolio_config.execution.order_status_poll_seconds)
            last, poll_error_count = self._get_order_status_or_last(
                order_id,
                last=last,
                poll_error_count=poll_error_count,
                context="order_wait",
            )
        return last

    def _is_filled(self, order_payload: dict[str, Any]) -> bool:
        status = str(order_payload.get("status", ""))
        if status == "filled":
            return True
        filled_qty = float(order_payload.get("filled_qty") or 0.0)
        total_qty = float(order_payload.get("qty") or 0.0)
        return total_qty > 0.0 and filled_qty >= total_qty

    def _execute_attempts(
        self,
        requests: list[OrderRequest],
        *,
        journal_name: str,
        trade: OpenTrade,
        phase: str,
        session: SessionState | None = None,
    ) -> tuple[dict[str, Any], float]:
        trade_date = date.fromisoformat(trade.entry_time_et[:10])
        run_dir = self._session_run_dir(trade_date)
        event_base = self._event_base_for_trade(trade, phase=phase)
        for request_index, request in enumerate(requests, start=1):
            serialized_request = self._serialize_order_request(request)
            try:
                response = self.broker.submit_order(
                    request,
                    dry_run=not self.submit_paper_orders,
                    explicitly_requested=self.submit_paper_orders,
                )
            except Exception as exc:
                response = {
                    "status": "submit_error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                self.logger.warning(
                    "order submission failed strategy=%s phase=%s request_index=%s error=%s",
                    trade.strategy_name,
                    phase,
                    request_index,
                    exc,
                )
                append_journal_entry(
                    run_dir / "order_journal.json",
                    {
                        **event_base,
                        "journal": journal_name,
                        "event_type": "order_submission_error",
                        "request_index": request_index,
                        "request": serialized_request,
                        "response": response,
                        "response_status": response["status"],
                        "order_id": None,
                        "client_order_id": request.client_order_id,
                    },
                )
                self._append_trade_event(
                    trade_date,
                    {
                        **event_base,
                        "event_type": "order_submission_error",
                        "request_index": request_index,
                        "request": serialized_request,
                        "response_status": response["status"],
                        "error": response["error"],
                        "error_type": response["error_type"],
                        "order_id": None,
                        "client_order_id": request.client_order_id,
                    },
                )
                continue
            append_journal_entry(
                run_dir / "order_journal.json",
                {
                    **event_base,
                    "journal": journal_name,
                    "event_type": "order_submission",
                    "request_index": request_index,
                    "request": serialized_request,
                    "response": response,
                    "response_status": response.get("status"),
                    "order_id": response.get("id"),
                    "client_order_id": request.client_order_id,
                },
            )
            self._append_trade_event(
                trade_date,
                {
                    **event_base,
                    "event_type": "order_submission",
                    "request_index": request_index,
                    "request": serialized_request,
                    "response_status": response.get("status"),
                    "order_id": response.get("id"),
                    "client_order_id": request.client_order_id,
                },
            )
            if response.get("status") == "dry_run":
                fallback_price = 0.0 if request.order_type == "market" else float(request.limit_price or 0.0)
                append_journal_entry(
                    run_dir / "order_journal.json",
                    {
                        **event_base,
                        "journal": journal_name,
                        "event_type": "order_terminal",
                        "request_index": request_index,
                        "request": serialized_request,
                        "status": "dry_run",
                        "order_id": response.get("id"),
                        "client_order_id": request.client_order_id,
                        "filled_avg_price": fallback_price,
                    },
                )
                self._append_trade_event(
                    trade_date,
                    {
                        **event_base,
                        "event_type": "order_terminal",
                        "request_index": request_index,
                        "status": "dry_run",
                        "order_id": response.get("id"),
                        "client_order_id": request.client_order_id,
                        "filled_avg_price": fallback_price,
                    },
                )
                return response, fallback_price
            order_id = str(response.get("id") or "")
            terminal = self._wait_for_terminal_order(order_id, session=session)
            append_journal_entry(
                run_dir / "order_journal.json",
                {
                    **event_base,
                    "journal": journal_name,
                    "event_type": "order_terminal",
                    "request_index": request_index,
                    "request": serialized_request,
                    "terminal": terminal,
                    "status": terminal.get("status"),
                    "order_id": order_id,
                    "client_order_id": request.client_order_id,
                    "filled_qty": terminal.get("filled_qty"),
                    "qty": terminal.get("qty"),
                    "filled_avg_price": terminal.get("filled_avg_price"),
                },
            )
            self._append_trade_event(
                trade_date,
                {
                    **event_base,
                    "event_type": "order_terminal",
                    "request_index": request_index,
                    "status": terminal.get("status"),
                    "order_id": order_id,
                    "client_order_id": request.client_order_id,
                    "filled_qty": terminal.get("filled_qty"),
                    "qty": terminal.get("qty"),
                    "filled_avg_price": terminal.get("filled_avg_price"),
                },
            )
            if self._is_filled(terminal):
                filled_avg_price = float(terminal.get("filled_avg_price") or request.limit_price or 0.0)
                return terminal, filled_avg_price
            if str(terminal.get("status", "")) in OPEN_STATUSES:
                cancel_requested = self._request_order_cancel(order_id, context=f"{phase}_attempt")
                self._append_trade_event(
                    trade_date,
                    {
                        **event_base,
                        "event_type": "order_cancel",
                        "request_index": request_index,
                        "order_id": order_id,
                        "client_order_id": request.client_order_id,
                        "cancel_requested": cancel_requested,
                    },
                )
        return {"status": "not_filled"}, 0.0

    def _alert(self, session: SessionState, level: str, message: str) -> None:
        alert = {"timestamp_et": _now_et().isoformat(), "level": level, "message": message}
        session.alerts.append(alert)
        self.logger.warning("multi ticker portfolio alert %s", alert)

    def _expected_entry_greeks(self, trade: OpenTrade) -> tuple[float, float]:
        delta_shares = 0.0
        vega_dollars = 0.0
        for leg in trade.legs:
            sign = 1.0 if leg["side"] == "long" else -1.0
            delta_shares += sign * float(leg["delta"]) * CONTRACT_MULTIPLIER
            vega_dollars += sign * float(leg["vega"]) * CONTRACT_MULTIPLIER
        return delta_shares * int(trade.quantity), vega_dollars * int(trade.quantity)

    def _run_entry(self, trade: OpenTrade, session: SessionState, current_equity: float) -> bool:
        delta_shares, vega_dollars = self._expected_entry_greeks(trade)
        trade_date = date.fromisoformat(session.trade_date)
        expected_entry_fill_price = float(trade.entry_fill_price)
        if abs(delta_shares) >= self.portfolio_config.risk.soft_alert_delta_shares:
            self._alert(
                session,
                "warning",
                f"{trade.strategy_name} entry delta alert: {delta_shares:.2f} shares equivalent",
            )
        if abs(vega_dollars) >= self.portfolio_config.risk.soft_alert_vega_dollars_1pct:
            self._alert(
                session,
                "warning",
                f"{trade.strategy_name} entry vega alert: {vega_dollars:.2f} dollars per 1 vol point",
            )
        response, fill_price = self._execute_attempts(
            self._entry_order_requests(trade),
            journal_name=f"{trade.strategy_name}_entry",
            trade=trade,
            phase="entry",
            session=session,
        )
        if response.get("status") == "not_filled":
            self._record_entry_execution_outcome(
                session,
                success=False,
                failure_status=str(response.get("status") or "not_filled"),
            )
            self._alert(session, "warning", f"{trade.strategy_name} entry did not fill")
            self._append_trade_event(
                trade_date,
                {
                    **self._event_base_for_trade(trade, phase="entry"),
                    "event_type": "entry_result",
                    "status": "not_filled",
                    "expected_entry_fill_price": round(expected_entry_fill_price, 4),
                    "actual_entry_fill_price": None,
                    "entry_slippage": None,
                    "virtual_cash_after": round(float(session.virtual_cash), 4),
                },
            )
            return False
        trade.entry_order_id = str(response.get("id") or "") if response.get("id") else None
        trade.entry_fill_price = fill_price if fill_price != 0.0 else trade.entry_fill_price
        trade.entry_debit = trade.entry_fill_price
        if len(trade.legs) == 1:
            trade.legs[0]["entry_fill_price"] = trade.entry_fill_price
        adverse_slippage_fraction = max(
            0.0,
            (float(trade.entry_fill_price) - expected_entry_fill_price) / max(abs(expected_entry_fill_price), 0.01),
        )
        self._record_entry_execution_outcome(
            session,
            success=True,
            adverse_slippage_fraction=adverse_slippage_fraction,
        )
        entry_fee_breakdown = _entry_fee_breakdown(trade.legs, int(trade.quantity))
        session.virtual_cash += _entry_cashflow_from_debit(
            float(trade.entry_debit), int(trade.quantity), trade.legs
        )
        session.open_trades.append(asdict(trade))
        session.signals_fired.append(trade.strategy_name)
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="entry"),
                "event_type": "entry_result",
                "status": str(response.get("status") or "filled"),
                "order_id": trade.entry_order_id,
                "expected_entry_fill_price": round(expected_entry_fill_price, 4),
                "actual_entry_fill_price": round(float(trade.entry_fill_price), 4),
                "entry_slippage": round(float(trade.entry_fill_price) - expected_entry_fill_price, 4),
                "entry_adverse_slippage_fraction": round(float(adverse_slippage_fraction), 6),
                "entry_total_fees": round(entry_fee_breakdown.total_fees, 4),
                "entry_regulatory_fees": round(entry_fee_breakdown.regulatory_fees, 4),
                "entry_broker_commission": round(entry_fee_breakdown.broker_commission, 4),
                "entry_orf_fees": round(entry_fee_breakdown.orf, 6),
                "entry_occ_fees": round(entry_fee_breakdown.occ, 6),
                "entry_cat_fees": round(entry_fee_breakdown.cat, 6),
                "entry_taf_fees": round(entry_fee_breakdown.taf, 6),
                "virtual_cash_after": round(float(session.virtual_cash), 4),
            },
        )
        self.logger.info(
            "entered %s symbol=%s qty=%s equity=%.2f submit=%s",
            trade.strategy_name,
            trade.underlying_symbol,
            trade.quantity,
            current_equity,
            self.submit_paper_orders,
        )
        return True

    def _should_exit_trade(
        self,
        trade: OpenTrade,
        current_minute: int,
        mark_map: dict[str, float],
    ) -> tuple[bool, str, float]:
        current_close_cashflow = _position_mark_cashflow(trade.legs, mark_map)
        exit_fee_breakdown = _exit_fee_breakdown(trade.legs, int(trade.quantity))
        current_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), int(trade.quantity), trade.legs)
            + current_close_cashflow * int(trade.quantity)
            - exit_fee_breakdown.total_fees
        )
        hold_minutes = max(0, current_minute - int(trade.entry_minute))
        min_hold_minutes = int(trade.min_option_hold_minutes or 0)
        if hold_minutes >= min_hold_minutes:
            if current_pnl >= trade.profit_target_dollars * int(trade.quantity):
                return True, "profit_target", current_pnl
            if current_pnl <= -trade.stop_loss_dollars * int(trade.quantity):
                return True, "stop_loss", current_pnl
        if str(trade.runner_hard_exit_mode or "absolute_minute") == "minutes_after_entry":
            hard_exit_due = hold_minutes >= int(trade.hard_exit_minute)
        else:
            hard_exit_due = current_minute >= trade.hard_exit_minute
        if hard_exit_due:
            return True, "time_exit", current_pnl
        return False, "", current_pnl

    def _run_exit(
        self,
        trade_payload: dict[str, Any],
        session: SessionState,
        snapshot: SymbolSnapshot,
        exit_reason: str,
    ) -> bool:
        trade = OpenTrade(**trade_payload)
        mark_map = self._mark_to_close(trade, snapshot.option_chain)
        if len(mark_map) != len(trade.legs):
            return False
        trade_date = date.fromisoformat(session.trade_date)
        expected_exit_fill_price = (
            float(next(iter(mark_map.values())))
            if len(mark_map) == 1
            else -(_position_mark_cashflow(trade.legs, mark_map) / CONTRACT_MULTIPLIER)
        )
        current_close_cashflow = _position_mark_cashflow(trade.legs, mark_map)
        exit_fee_breakdown = _exit_fee_breakdown(trade.legs, int(trade.quantity))
        expected_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), int(trade.quantity), trade.legs)
            + current_close_cashflow * int(trade.quantity)
            - exit_fee_breakdown.total_fees
        )
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="exit"),
                "event_type": "exit_trigger",
                "exit_reason": exit_reason,
                "expected_exit_fill_price": round(float(expected_exit_fill_price), 4),
                "expected_net_pnl": round(float(expected_pnl), 4),
            },
        )
        market_fallback = (
            self.portfolio_config.execution.allow_market_exit_fallback
            and snapshot.current_minute >= self.portfolio_config.execution.market_exit_fallback_minute
        )
        response, fill_price = self._execute_attempts(
            self._exit_order_requests(trade, mark_map, market_fallback=market_fallback),
            journal_name=f"{trade.strategy_name}_exit",
            trade=trade,
            phase="exit",
            session=session,
        )
        if response.get("status") == "not_filled":
            if len(trade.legs) > 1:
                self._alert(
                    session,
                    "warning",
                    f"{trade.strategy_name} combo exit did not fill; attempting cleanup fallback",
                )
                self._append_trade_event(
                    trade_date,
                    {
                        **self._event_base_for_trade(trade, phase="exit"),
                        "event_type": "exit_cleanup_fallback",
                        "status": "starting",
                        "exit_reason": exit_reason,
                        "expected_exit_fill_price": round(float(expected_exit_fill_price), 4),
                        "expected_net_pnl": round(float(expected_pnl), 4),
                        "cleanup_trigger_reason": "combo_exit_not_filled",
                        "via_cleanup": True,
                        "virtual_cash_after": round(float(session.virtual_cash), 4),
                    },
                )
                return self._force_cleanup_known_trade(
                    trade_payload=trade_payload,
                    session=session,
                    trade_date=trade_date,
                    stock_frames={snapshot.underlying_symbol: snapshot.stock_frame},
                    option_chain=snapshot.option_chain,
                    reason=exit_reason,
                    emit_exit_trigger=False,
                    expected_exit_fill_price=float(expected_exit_fill_price),
                    expected_net_pnl=float(expected_pnl),
                    cleanup_trigger_reason="combo_exit_not_filled",
                    alert_message=(
                        f"{trade.strategy_name} required combo exit cleanup fallback "
                        f"after not-filled combo exit ({exit_reason})"
                    ),
                )
            self._alert(session, "warning", f"{trade.strategy_name} exit did not fill")
            self._append_trade_event(
                trade_date,
                {
                    **self._event_base_for_trade(trade, phase="exit"),
                    "event_type": "exit_result",
                    "status": "not_filled",
                    "exit_reason": exit_reason,
                    "order_id": None,
                    "expected_exit_fill_price": round(float(expected_exit_fill_price), 4),
                    "actual_exit_fill_price": None,
                    "exit_slippage": None,
                    "net_pnl": None,
                    "via_cleanup": False,
                    "virtual_cash_after": round(float(session.virtual_cash), 4),
                },
            )
            return False
        normalized_fill_price = -fill_price if len(trade.legs) > 1 else fill_price
        exit_cashflow = _exit_cashflow_from_fill(
            fill_price=normalized_fill_price,
            quantity=int(trade.quantity),
            legs=trade.legs,
        )
        session.virtual_cash += exit_cashflow
        delta_shares, vega_dollars = self._expected_entry_greeks(trade)
        net_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), int(trade.quantity), trade.legs)
            + exit_cashflow
        )
        entry_fee_breakdown = _entry_fee_breakdown(trade.legs, int(trade.quantity))
        exit_leg_quality = self._exit_leg_quality_map(trade, snapshot.option_chain)
        completed_legs: list[dict[str, Any]] = []
        for leg in trade.legs:
            completed_leg = dict(leg)
            quality = exit_leg_quality.get(str(leg.get("symbol") or ""))
            if quality:
                completed_leg.update(
                    {
                        "exit_bid": quality.get("bid"),
                        "exit_ask": quality.get("ask"),
                        "exit_mark": quality.get("mark"),
                        "exit_quote_time": quality.get("quote_time"),
                        "exit_spread_pct": quality.get("spread_pct"),
                        "exit_freshness_seconds": quality.get("freshness_seconds"),
                        "exit_quote_source": quality.get("quote_source") or "option_quote_bid_ask",
                    }
                )
            completed_legs.append(completed_leg)
        completed = CompletedTrade(
            strategy_name=trade.strategy_name,
            underlying_symbol=trade.underlying_symbol,
            regime=trade.regime,
            quantity=int(trade.quantity),
            entry_time_et=trade.entry_time_et,
            exit_time_et=_now_et().isoformat(),
            entry_minute=int(trade.entry_minute),
            exit_minute=snapshot.current_minute,
            entry_fill_price=float(trade.entry_fill_price),
            exit_fill_price=float(fill_price),
            underlying_entry=float(trade.underlying_entry),
            underlying_exit=float(snapshot.latest_close),
            exit_reason=exit_reason,
            entry_order_id=trade.entry_order_id,
            exit_order_id=str(response.get("id") or "") if response.get("id") else None,
            net_pnl=round(net_pnl, 4),
            max_loss_per_combo=float(trade.max_loss_per_combo),
            max_profit_per_combo=float(trade.max_profit_per_combo),
            delta_shares_at_entry=round(delta_shares, 4),
            vega_dollars_1pct_at_entry=round(vega_dollars, 4),
            legs=completed_legs,
            entry_attempt_id=trade.entry_attempt_id,
            candidate_variant_id=trade.candidate_variant_id,
            source_strategy_id=trade.source_strategy_id,
            promotion_manifest_path=trade.promotion_manifest_path,
            governed_validation_packet_uri=trade.governed_validation_packet_uri,
            research_profile=trade.research_profile,
            research_entry_timing_mode=trade.research_entry_timing_mode,
            research_entry_offset_minutes=trade.research_entry_offset_minutes,
            research_exit_offset_minutes=trade.research_exit_offset_minutes,
            runner_semantics_status=trade.runner_semantics_status,
            min_option_hold_minutes=trade.min_option_hold_minutes,
            runner_hard_exit_mode=trade.runner_hard_exit_mode,
            entry_total_fees=round(entry_fee_breakdown.total_fees, 4),
            exit_total_fees=round(exit_fee_breakdown.total_fees, 4),
            entry_regulatory_fees=round(entry_fee_breakdown.regulatory_fees, 4),
            exit_regulatory_fees=round(exit_fee_breakdown.regulatory_fees, 4),
            via_cleanup=False,
        )
        session.completed_trades.append(asdict(completed))
        self._remove_open_trade_from_session(session, trade)
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="exit"),
                "event_type": "exit_result",
                "status": str(response.get("status") or "filled"),
                "exit_reason": exit_reason,
                "order_id": completed.exit_order_id,
                "expected_exit_fill_price": round(float(expected_exit_fill_price), 4),
                "actual_exit_fill_price": round(float(fill_price), 4),
                "exit_slippage": round(float(fill_price) - float(expected_exit_fill_price), 4),
                "exit_total_fees": round(exit_fee_breakdown.total_fees, 4),
                "exit_regulatory_fees": round(exit_fee_breakdown.regulatory_fees, 4),
                "exit_broker_commission": round(exit_fee_breakdown.broker_commission, 4),
                "exit_orf_fees": round(exit_fee_breakdown.orf, 6),
                "exit_occ_fees": round(exit_fee_breakdown.occ, 6),
                "exit_cat_fees": round(exit_fee_breakdown.cat, 6),
                "exit_taf_fees": round(exit_fee_breakdown.taf, 6),
                "net_pnl": round(float(net_pnl), 4),
                "via_cleanup": False,
                "virtual_cash_after": round(float(session.virtual_cash), 4),
            },
        )
        self.logger.info("exited %s reason=%s pnl=%.2f", trade.strategy_name, exit_reason, net_pnl)
        return True

    def _submit_cleanup_order(
        self,
        *,
        trade_date: date,
        request: OrderRequest,
        reason: str,
        metadata: dict[str, Any],
        max_attempts: int = 3,
    ) -> dict[str, Any]:
        last_result: dict[str, Any] = {
            "status": "not_filled",
            "order_id": None,
            "filled_avg_price": None,
        }
        for attempt_index in range(1, max_attempts + 1):
            request_for_attempt = self._with_attempt_client_order_id(
                request,
                attempt_index=attempt_index,
            )
            serialized_request = self._serialize_order_request(request_for_attempt)
            try:
                response = self.broker.submit_order(
                    request_for_attempt,
                    dry_run=not getattr(self, "submit_paper_orders", True),
                    explicitly_requested=getattr(self, "submit_paper_orders", True),
                )
            except Exception as exc:
                response = {
                    "status": "submit_error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                self.logger.warning(
                    "cleanup order submission failed reason=%s attempt_index=%s error=%s",
                    reason,
                    attempt_index,
                    exc,
                )
                self._append_broker_position_cleanup_entry(
                    trade_date,
                    {
                        "reason": reason,
                        "request": serialized_request,
                        "response": response,
                        "terminal": response,
                        "attempt_index": attempt_index,
                        "max_attempts": max_attempts,
                        **metadata,
                    },
                )
                last_result = {
                    "status": "submit_error",
                    "order_id": None,
                    "filled_avg_price": None,
                }
                continue
            journal_entry: dict[str, Any] = {
                "reason": reason,
                "request": serialized_request,
                "response": response,
                "attempt_index": attempt_index,
                "max_attempts": max_attempts,
                **metadata,
            }
            if response.get("status") == "dry_run":
                journal_entry["terminal"] = {"status": "dry_run", "filled_avg_price": None}
                self._append_broker_position_cleanup_entry(trade_date, journal_entry)
                return {
                    "status": "dry_run",
                    "order_id": response.get("id"),
                    "filled_avg_price": None,
                }

            order_id = str(response.get("id") or "")
            terminal = self._wait_for_terminal_order_with_timeout(
                order_id,
                timeout_seconds=self.portfolio_config.execution.unexpected_position_cleanup_timeout_seconds,
            )
            journal_entry["terminal"] = terminal
            if str(terminal.get("status", "")) in OPEN_STATUSES:
                journal_entry["cancel_requested"] = self._request_order_cancel(
                    order_id,
                    context="broker_position_cleanup",
                )
            self._append_broker_position_cleanup_entry(trade_date, journal_entry)
            if self._is_filled(terminal):
                return {
                    "status": str(terminal.get("status") or "filled"),
                    "order_id": order_id,
                    "filled_avg_price": terminal.get("filled_avg_price"),
                }
            last_result = {
                "status": str(terminal.get("status") or "not_filled"),
                "order_id": order_id,
                "filled_avg_price": terminal.get("filled_avg_price"),
            }
        return last_result

    def _effective_exit_snapshot(
        self,
        trade: OpenTrade,
        stock_frames: dict[str, pd.DataFrame] | None,
    ) -> tuple[int, float]:
        stock_frame = (stock_frames or {}).get(trade.underlying_symbol, pd.DataFrame())
        if stock_frame.empty:
            fallback_now = _now_et()
            minute_index = max(
                0,
                int((fallback_now - _rth_open_for(date.fromisoformat(trade.entry_time_et[:10]))).total_seconds() // 60),
            )
            return minute_index, float(trade.underlying_entry)
        latest = stock_frame.iloc[-1]
        return int(latest["minute_index"]), float(latest["close"])

    def _cleanup_uses_market_orders(self, *, asset_class: str) -> bool:
        if asset_class != "option":
            return True
        now_et = _now_et()
        market_close = datetime.combine(now_et.date(), dt_time(16, 0), tzinfo=ET)
        return _rth_open_for(now_et.date()) <= now_et < market_close

    def _build_cleanup_order_request(
        self,
        *,
        symbol: str,
        side: Literal["buy", "sell"],
        strategy_name: str,
        asset_class: Literal["stock", "option"],
        qty: float,
        position_intent: str,
        client_order_key: str | None = None,
    ) -> OrderRequest:
        order_type = "market"
        limit_price: float | None = None
        if not self._cleanup_uses_market_orders(asset_class=asset_class):
            order_type = "limit"
            limit_price = 0.01 if side == "sell" else 1000.0
        request_kwargs: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "strategy_name": strategy_name,
            "asset_class": asset_class,
            "qty": qty,
            "order_type": order_type,
            "time_in_force": "day",
            "limit_price": limit_price,
            "extra": {"position_intent": position_intent},
        }
        if client_order_key is not None:
            request_kwargs["client_order_key"] = client_order_key
        try:
            return self.broker.build_order_request(
                **request_kwargs,
            )
        except TypeError as exc:
            if "client_order_key" not in request_kwargs or "client_order_key" not in str(exc):
                raise
            request_kwargs.pop("client_order_key", None)
            return self.broker.build_order_request(**request_kwargs)

    def _force_cleanup_known_trade(
        self,
        *,
        trade_payload: dict[str, Any],
        session: SessionState,
        trade_date: date,
        stock_frames: dict[str, pd.DataFrame] | None,
        option_chain: pd.DataFrame | None = None,
        reason: str,
        emit_exit_trigger: bool = True,
        expected_exit_fill_price: float | None = None,
        expected_net_pnl: float | None = None,
        cleanup_trigger_reason: str | None = None,
        alert_message: str | None = None,
    ) -> bool:
        trade = OpenTrade(**trade_payload)
        exit_minute, underlying_exit = self._effective_exit_snapshot(trade, stock_frames)
        if emit_exit_trigger:
            self._append_trade_event(
                trade_date,
                {
                    **self._event_base_for_trade(trade, phase="exit"),
                    "event_type": "exit_trigger",
                    "exit_reason": reason,
                    "expected_exit_fill_price": expected_exit_fill_price,
                    "expected_net_pnl": expected_net_pnl,
                    "cleanup_trigger_reason": cleanup_trigger_reason,
                    "via_cleanup": True,
                },
            )

        gross_exit_cashflow = 0.0
        order_ids: list[str] = []
        fill_prices: list[float] = []
        quantity = int(trade.quantity)
        broker_position_map = self._broker_position_qty_map()
        cleanup_plans = self._cleanup_leg_plans_for_trade(trade, broker_position_map=broker_position_map)
        for cleanup_plan in cleanup_plans:
            symbol = str(cleanup_plan["symbol"])
            leg_side = str(cleanup_plan["leg_side"])
            cleanup_qty = float(cleanup_plan["cleanup_qty"])
            order_side = cast(Literal["buy", "sell"], str(cleanup_plan["order_side"]))
            position_intent = str(cleanup_plan["position_intent"])
            request = self._build_cleanup_order_request(
                symbol=symbol,
                side=order_side,
                strategy_name=f"{trade.strategy_name}_cleanup_exit",
                asset_class="option",
                qty=cleanup_qty,
                position_intent=position_intent,
                client_order_key=(
                    f"{trade.entry_attempt_id or trade.entry_time_et}|{reason}|{symbol}|cleanup|"
                    f"{_now_et().isoformat(timespec='microseconds')}"
                ),
            )
            result = self._submit_cleanup_order(
                trade_date=trade_date,
                request=request,
                reason=reason,
                metadata={
                    "scope": "known_trade",
                    "underlying_symbol": trade.underlying_symbol,
                    "strategy_name": trade.strategy_name,
                    "trade_entry_attempt_id": trade.entry_attempt_id,
                    "symbol": symbol,
                    "leg_side": leg_side,
                    "cleanup_qty": round(cleanup_qty, 4),
                    "expected_signed_qty": round(float(cleanup_plan["expected_signed_qty"]), 4),
                    "broker_signed_qty": round(float(cleanup_plan["broker_signed_qty"]), 4),
                    "used_broker_positions": bool(cleanup_plan["used_broker_positions"]),
                },
            )
            if result["status"] == "not_filled":
                self._alert(
                    session,
                    "error",
                    f"{trade.strategy_name} cleanup exit did not fill for {symbol}",
                )
                self._append_trade_event(
                    trade_date,
                    {
                        **self._event_base_for_trade(trade, phase="exit"),
                        "event_type": "exit_result",
                        "status": "not_filled",
                        "exit_reason": reason,
                        "order_id": result.get("order_id"),
                        "expected_exit_fill_price": expected_exit_fill_price,
                        "actual_exit_fill_price": None,
                        "exit_slippage": None,
                        "net_pnl": None,
                        "cleanup_trigger_reason": cleanup_trigger_reason,
                        "virtual_cash_after": round(float(session.virtual_cash), 4),
                        "via_cleanup": True,
                    },
                )
                return False
            raw_fill_price = result.get("filled_avg_price")
            fill_price = (
                float(raw_fill_price)
                if raw_fill_price is not None
                else float(cleanup_plan.get("mark") or cleanup_plan.get("entry_fill_price") or 0.0)
            )
            fill_prices.append(fill_price)
            if result.get("order_id"):
                order_ids.append(str(result["order_id"]))
            cashflow_sign = 1.0 if order_side == "sell" else -1.0
            gross_exit_cashflow += cashflow_sign * fill_price * CONTRACT_MULTIPLIER * cleanup_qty

        exit_fee_breakdown = _exit_fee_breakdown(trade.legs, quantity)
        exit_cashflow = gross_exit_cashflow - exit_fee_breakdown.total_fees
        effective_exit_fill_price = (
            gross_exit_cashflow / (CONTRACT_MULTIPLIER * quantity)
            if quantity > 0
            else 0.0
        )
        exit_slippage = (
            round(float(effective_exit_fill_price) - float(expected_exit_fill_price), 4)
            if expected_exit_fill_price is not None
            else None
        )
        session.virtual_cash += exit_cashflow
        delta_shares, vega_dollars = self._expected_entry_greeks(trade)
        entry_fee_breakdown = _entry_fee_breakdown(trade.legs, quantity)
        net_pnl = (
            _entry_cashflow_from_debit(float(trade.entry_debit), quantity, trade.legs)
            + exit_cashflow
        )
        exit_leg_quality = self._exit_leg_quality_map(trade, option_chain)
        completed_legs: list[dict[str, Any]] = []
        for leg in trade.legs:
            completed_leg = dict(leg)
            quality = exit_leg_quality.get(str(leg.get("symbol") or ""))
            if quality:
                completed_leg.update(
                    {
                        "exit_bid": quality.get("bid"),
                        "exit_ask": quality.get("ask"),
                        "exit_mark": quality.get("mark"),
                        "exit_quote_time": quality.get("quote_time"),
                        "exit_spread_pct": quality.get("spread_pct"),
                        "exit_freshness_seconds": quality.get("freshness_seconds"),
                        "exit_quote_source": quality.get("quote_source") or "option_quote_bid_ask",
                    }
                )
            completed_legs.append(completed_leg)
        completed = CompletedTrade(
            strategy_name=trade.strategy_name,
            underlying_symbol=trade.underlying_symbol,
            regime=trade.regime,
            quantity=quantity,
            entry_time_et=trade.entry_time_et,
            exit_time_et=_now_et().isoformat(),
            entry_minute=int(trade.entry_minute),
            exit_minute=exit_minute,
            entry_fill_price=float(trade.entry_fill_price),
            exit_fill_price=float(effective_exit_fill_price),
            underlying_entry=float(trade.underlying_entry),
            underlying_exit=underlying_exit,
            exit_reason=reason,
            entry_order_id=trade.entry_order_id,
            exit_order_id=",".join(order_ids) if order_ids else None,
            net_pnl=round(net_pnl, 4),
            max_loss_per_combo=float(trade.max_loss_per_combo),
            max_profit_per_combo=float(trade.max_profit_per_combo),
            delta_shares_at_entry=round(delta_shares, 4),
            vega_dollars_1pct_at_entry=round(vega_dollars, 4),
            legs=completed_legs,
            entry_attempt_id=trade.entry_attempt_id,
            candidate_variant_id=trade.candidate_variant_id,
            source_strategy_id=trade.source_strategy_id,
            promotion_manifest_path=trade.promotion_manifest_path,
            governed_validation_packet_uri=trade.governed_validation_packet_uri,
            research_profile=trade.research_profile,
            research_entry_timing_mode=trade.research_entry_timing_mode,
            research_entry_offset_minutes=trade.research_entry_offset_minutes,
            research_exit_offset_minutes=trade.research_exit_offset_minutes,
            runner_semantics_status=trade.runner_semantics_status,
            min_option_hold_minutes=trade.min_option_hold_minutes,
            runner_hard_exit_mode=trade.runner_hard_exit_mode,
            entry_total_fees=round(entry_fee_breakdown.total_fees, 4),
            exit_total_fees=round(exit_fee_breakdown.total_fees, 4),
            entry_regulatory_fees=round(entry_fee_breakdown.regulatory_fees, 4),
            exit_regulatory_fees=round(exit_fee_breakdown.regulatory_fees, 4),
            via_cleanup=True,
        )
        session.completed_trades.append(asdict(completed))
        self._remove_open_trade_from_session(session, trade)
        self._append_trade_event(
            trade_date,
            {
                **self._event_base_for_trade(trade, phase="exit"),
                "event_type": "exit_result",
                "status": "filled",
                "exit_reason": reason,
                "order_id": completed.exit_order_id,
                "expected_exit_fill_price": expected_exit_fill_price,
                "actual_exit_fill_price": round(float(effective_exit_fill_price), 4),
                "exit_slippage": exit_slippage,
                "exit_total_fees": round(exit_fee_breakdown.total_fees, 4),
                "exit_regulatory_fees": round(exit_fee_breakdown.regulatory_fees, 4),
                "cleanup_trigger_reason": cleanup_trigger_reason,
                "net_pnl": round(float(net_pnl), 4),
                "virtual_cash_after": round(float(session.virtual_cash), 4),
                "via_cleanup": True,
            },
        )
        self._alert(
            session,
            "warning",
            alert_message or f"{trade.strategy_name} required safeguard cleanup at end of day ({reason})",
        )
        return True

    def _cleanup_known_open_trades(
        self,
        *,
        session: SessionState,
        trade_date: date,
        stock_frames: dict[str, pd.DataFrame] | None,
        reason: str,
    ) -> int:
        cleaned = 0
        symbols_with_open_close_orders = self._symbols_with_open_close_orders()
        broker = getattr(self, "broker", None)
        broker_positions_authoritative = callable(getattr(broker, "get_positions", None))
        broker_position_map = self._broker_position_qty_map() if broker_positions_authoritative else {}
        for trade_payload in list(session.open_trades):
            leg_symbols = {
                str(leg.get("symbol") or "").strip()
                for leg in trade_payload.get("legs", [])
                if str(leg.get("symbol") or "").strip()
            }
            if leg_symbols and leg_symbols.issubset(symbols_with_open_close_orders):
                continue
            if broker_positions_authoritative and self._trade_has_no_broker_position(
                trade_payload,
                broker_position_map,
            ):
                self._drop_open_trade_already_flat_at_broker(
                    trade_payload=trade_payload,
                    session=session,
                    trade_date=trade_date,
                    reason=reason,
                )
                cleaned += 1
                continue
            if self._force_cleanup_known_trade(
                trade_payload=trade_payload,
                session=session,
                trade_date=trade_date,
                stock_frames=stock_frames,
                reason=reason,
            ):
                cleaned += 1
        return cleaned

    def _close_unexpected_broker_positions(
        self,
        *,
        session: SessionState,
        trade_date: date,
        reason: str,
        flatten_all_remaining: bool = False,
        expected_position_map: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
        expected = expected_position_map or self._expected_broker_position_map(session)
        cleanup_entries: list[dict[str, Any]] = []
        symbols_with_open_close_orders = self._symbols_with_open_close_orders()
        positions = sorted(
            self.broker.get_positions(),
            key=lambda position: self._cleanup_order_priority(
                cleanup_signed_qty=self._signed_broker_position_qty(position),
                symbol=str(position.get("symbol") or ""),
            ),
        )
        for position in positions:
            symbol = str(position.get("symbol") or "").strip()
            if not symbol:
                continue
            actual_qty = self._signed_broker_position_qty(position)
            if math.isclose(actual_qty, 0.0, abs_tol=1e-9):
                continue
            if symbol in symbols_with_open_close_orders:
                cleanup_entries.append(
                    {
                        "symbol": symbol,
                        "broker_position_qty": round(float(actual_qty), 4),
                        "expected_session_qty": round(float(expected.get(symbol, 0.0)), 4),
                        "cleanup_qty": 0.0,
                        "reason": reason,
                        "status": "pending_existing_close_order",
                        "order_id": None,
                        "filled_avg_price": None,
                    }
                )
                continue
            expected_qty = expected.get(symbol, 0.0)
            cleanup_qty = 0.0
            if flatten_all_remaining:
                cleanup_qty = actual_qty
            elif math.isclose(expected_qty, 0.0, abs_tol=1e-9):
                cleanup_qty = actual_qty
            elif actual_qty * expected_qty < 0:
                cleanup_qty = actual_qty
            elif abs(actual_qty) > abs(expected_qty):
                cleanup_qty = actual_qty - expected_qty

            if math.isclose(cleanup_qty, 0.0, abs_tol=1e-9):
                continue

            effective_reason = (
                AUTO_FLATTEN_KNOWN_EOD_REASON
                if flatten_all_remaining and not math.isclose(expected_qty, 0.0, abs_tol=1e-9)
                else reason
            )
            order_side = "sell" if cleanup_qty > 0 else "buy"
            position_intent = "sell_to_close" if cleanup_qty > 0 else "buy_to_close"
            asset_class = self._normalized_broker_asset_class(position)
            request = self._build_cleanup_order_request(
                symbol=symbol,
                side=order_side,
                strategy_name="system_position_cleanup",
                asset_class=cast(Literal["stock", "option"], asset_class),
                qty=abs(float(cleanup_qty)),
                position_intent=position_intent,
                client_order_key=(
                    f"{symbol}|{effective_reason}|{cleanup_qty}|"
                    f"{_now_et().isoformat(timespec='microseconds')}"
                ),
            )
            result = self._submit_cleanup_order(
                trade_date=trade_date,
                request=request,
                reason=effective_reason,
                metadata={
                    "scope": "unexpected_position",
                    "symbol": symbol,
                    "broker_position_qty": round(float(actual_qty), 4),
                    "expected_session_qty": round(float(expected_qty), 4),
                    "cleanup_qty": round(float(cleanup_qty), 4),
                    "asset_class": asset_class,
                },
            )
            cleanup_entry = {
                "symbol": symbol,
                "broker_position_qty": round(float(actual_qty), 4),
                "expected_session_qty": round(float(expected_qty), 4),
                "cleanup_qty": round(float(cleanup_qty), 4),
                "reason": effective_reason,
                "status": result.get("status"),
                "order_id": result.get("order_id"),
                "filled_avg_price": result.get("filled_avg_price"),
            }
            cleanup_entries.append(cleanup_entry)
            if result.get("status") == "not_filled":
                self._alert(
                    session,
                    "error",
                    f"Unexpected broker position cleanup failed for {symbol} ({effective_reason})",
                )
            else:
                self._alert(
                    session,
                    "warning",
                    f"Unexpected broker position auto-closed for {symbol} ({effective_reason})",
                )
        return cleanup_entries

    def _active_broker_positions(self) -> list[dict[str, Any]]:
        active_positions: list[dict[str, Any]] = []
        for position in self.broker.get_positions():
            symbol = str(position.get("symbol") or "").strip()
            if not symbol:
                continue
            actual_qty = self._signed_broker_position_qty(position)
            if math.isclose(actual_qty, 0.0, abs_tol=1e-9):
                continue
            active_positions.append(
                {
                    "symbol": symbol,
                    "qty": round(float(actual_qty), 4),
                    "asset_class": self._normalized_broker_asset_class(position),
                    "raw_position": position,
                }
            )
        return active_positions

    @staticmethod
    def _close_order_symbols(order_payload: dict[str, Any]) -> set[str]:
        symbols: set[str] = set()
        symbol = str(order_payload.get("symbol") or "").strip()
        position_intent = str(order_payload.get("position_intent") or "").strip()
        if symbol and position_intent in {"sell_to_close", "buy_to_close"}:
            symbols.add(symbol)
        for leg in order_payload.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            leg_symbol = str(leg.get("symbol") or "").strip()
            leg_position_intent = str(leg.get("position_intent") or "").strip()
            if leg_symbol and leg_position_intent in {"sell_to_close", "buy_to_close"}:
                symbols.add(leg_symbol)
        return symbols

    def _open_broker_orders(self, *, limit: int = 200) -> list[dict[str, Any]]:
        get_orders = getattr(self.broker, "get_orders", None)
        if get_orders is None:
            return []
        return list(get_orders(status="open", limit=limit))

    def _symbols_from_open_close_orders(self, orders: list[dict[str, Any]]) -> set[str]:
        symbols: set[str] = set()
        for order in orders:
            symbols.update(self._close_order_symbols(order))
        return symbols

    def _symbols_with_open_close_orders(self) -> set[str]:
        return self._symbols_from_open_close_orders(self._open_broker_orders())

    def _build_symbol_snapshot(
        self,
        *,
        trade_date: date,
        underlying_symbol: str,
        stock_frame: pd.DataFrame,
        session: SessionState,
    ) -> SymbolSnapshot | None:
        if stock_frame.empty:
            return None
        latest = stock_frame.iloc[-1]
        spot_price = float(latest["close"])
        current_minute = int(latest["minute_index"])
        contracts = self._refresh_contract_cache_if_needed(trade_date, underlying_symbol)
        symbols, metadata = self._candidate_symbols_for_snapshot(contracts, spot_price, trade_date)
        open_symbols = [
            str(leg["symbol"])
            for trade in session.open_trades
            if trade["underlying_symbol"] == underlying_symbol
            for leg in trade["legs"]
            if str(leg["symbol"]) not in symbols
        ]
        for symbol in open_symbols:
            match = next((contract for contract in contracts if contract["symbol"] == symbol), None)
            if match is not None:
                metadata[symbol] = match
                symbols.append(symbol)
        option_chain = self._fetch_option_chain(
            symbols,
            metadata,
            spot_price=spot_price,
            trade_date=trade_date,
            underlying_symbol=underlying_symbol,
        )
        mark_map = {
            str(row.symbol): float(row.mark)
            for row in option_chain.itertuples(index=False)
            if pd.notna(row.mark)
        }
        latest_timestamp = latest["timestamp_et"]
        if pd.isna(latest_timestamp):
            latest_timestamp = None
        return SymbolSnapshot(
            underlying_symbol=underlying_symbol,
            trade_date=trade_date,
            stock_frame=stock_frame,
            option_chain=option_chain,
            mark_map=mark_map,
            latest_close=spot_price,
            current_minute=current_minute,
            latest_timestamp_et=latest_timestamp.to_pydatetime() if latest_timestamp is not None else None,
        )

    def _perform_startup_check(
        self,
        *,
        session: SessionState,
        trade_date: date,
        snapshots: dict[str, SymbolSnapshot],
        allow_broker_cleanup: bool = True,
    ) -> tuple[str, dict[str, Any]]:
        details: dict[str, Any] = {"trade_date": trade_date.isoformat(), "underlyings": {}}
        now_et = _now_et()
        open_et = _rth_open_for(trade_date)
        grace_cutoff = open_et + timedelta(minutes=5)

        account = self.broker.get_account()
        positions = self.broker.get_positions()
        open_orders = self._open_broker_orders()
        symbols_with_open_close_orders = self._symbols_from_open_close_orders(open_orders)
        buying_power = float(account.get("buying_power") or 0.0)
        broker_equity = self._extract_broker_equity(account)
        details["buying_power"] = round(buying_power, 2)
        details["broker_equity"] = round(broker_equity, 2) if broker_equity is not None else None
        details["required_buying_power"] = round(
            self.portfolio_config.risk.min_required_buying_power,
            2,
        )
        details["broker_position_count"] = len(positions)
        details["open_order_count"] = len(open_orders)

        failures: list[str] = []
        pending_reasons: list[str] = []
        if buying_power < self.portfolio_config.risk.min_required_buying_power:
            failures.append(
                f"buying power {buying_power:.2f} below required {self.portfolio_config.risk.min_required_buying_power:.2f}"
            )
        min_broker_equity = self._broker_min_equity_to_trade()
        if (
            broker_equity is not None
            and min_broker_equity is not None
            and broker_equity < min_broker_equity
        ):
            failures.append(
                f"broker equity {broker_equity:.2f} below minimum trading threshold {min_broker_equity:.2f}"
            )
        if positions and self.portfolio_config.execution.auto_flatten_unexpected_positions:
            positions_pending_existing_close = [
                position
                for position in positions
                if str(position.get("symbol") or "").strip() in symbols_with_open_close_orders
            ]
            positions_requiring_cleanup = [
                position
                for position in positions
                if str(position.get("symbol") or "").strip() not in symbols_with_open_close_orders
            ]
            if positions_pending_existing_close:
                details["pending_broker_close_orders"] = sorted(
                    {
                        str(position.get("symbol") or "").strip()
                        for position in positions_pending_existing_close
                        if str(position.get("symbol") or "").strip()
                    }
                )
                pending_symbols_text = ", ".join(details["pending_broker_close_orders"])
                if now_et <= grace_cutoff:
                    pending_reasons.append(
                        f"broker cleanup orders still pending for {pending_symbols_text}"
                    )
                else:
                    failures.append(
                        f"broker cleanup orders still pending after startup grace period for {pending_symbols_text}"
                    )
            if positions_requiring_cleanup:
                if allow_broker_cleanup:
                    cleanup_entries = self._close_unexpected_broker_positions(
                        session=session,
                        trade_date=trade_date,
                        reason=AUTO_FLATTEN_UNEXPECTED_STARTUP_REASON,
                    )
                    if cleanup_entries:
                        details["broker_position_cleanup"] = cleanup_entries
                        positions = self.broker.get_positions()
                        details["broker_position_count"] = len(positions)
                    open_orders = self._open_broker_orders()
                    details["open_order_count"] = len(open_orders)
                    symbols_with_open_close_orders = self._symbols_from_open_close_orders(open_orders)
                else:
                    details["broker_position_cleanup_suppressed"] = sorted(
                        {
                            str(position.get("symbol") or "").strip()
                            for position in positions_requiring_cleanup
                            if str(position.get("symbol") or "").strip()
                        }
                    )

        positions_relevant_to_startup = [
            position
            for position in positions
            if str(position.get("symbol") or "").strip() not in symbols_with_open_close_orders
        ]
        position_mismatches = self._broker_position_mismatch_messages(
            session,
            positions_relevant_to_startup,
        )
        if position_mismatches:
            failures.extend(position_mismatches)

        for symbol in self.underlyings:
            snapshot = snapshots.get(symbol)
            symbol_details: dict[str, Any] = {}
            if snapshot is None:
                symbol_details["status"] = "missing_stock_frame"
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} stock frame not ready yet")
                else:
                    failures.append(f"{symbol} stock frame missing after startup grace period")
                details["underlyings"][symbol] = symbol_details
                continue
            freshness_seconds = None
            if snapshot.latest_timestamp_et is not None:
                freshness_seconds = max(0.0, (now_et - snapshot.latest_timestamp_et).total_seconds())
            symbol_details["latest_minute"] = snapshot.current_minute
            symbol_details["stock_freshness_seconds"] = freshness_seconds
            same_day_calls = int(
                (
                    (snapshot.option_chain["dte"] == 0)
                    & (snapshot.option_chain["option_type"] == "call")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            same_day_puts = int(
                (
                    (snapshot.option_chain["dte"] == 0)
                    & (snapshot.option_chain["option_type"] == "put")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            next_expiry_calls = int(
                (
                    (snapshot.option_chain["dte"] > 0)
                    & (snapshot.option_chain["option_type"] == "call")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            next_expiry_puts = int(
                (
                    (snapshot.option_chain["dte"] > 0)
                    & (snapshot.option_chain["option_type"] == "put")
                ).sum()
            ) if not snapshot.option_chain.empty else 0
            symbol_details["same_day_calls"] = same_day_calls
            symbol_details["same_day_puts"] = same_day_puts
            symbol_details["next_expiry_calls"] = next_expiry_calls
            symbol_details["next_expiry_puts"] = next_expiry_puts
            required_inventory = {
                "same_day_calls": False,
                "same_day_puts": False,
                "next_expiry_calls": False,
                "next_expiry_puts": False,
            }
            for strategy in self.portfolio_config.strategies_by_symbol.get(symbol, []):
                dte_prefix = "same_day" if strategy.dte_mode == "same_day" else "next_expiry"
                for leg in strategy.legs:
                    bucket = f"{dte_prefix}_{leg.option_type}s"
                    required_inventory[bucket] = True
            symbol_details["required_inventory"] = required_inventory
            if freshness_seconds is None:
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} latest stock bar timestamp not ready yet")
                else:
                    failures.append(f"{symbol} latest stock bar timestamp missing")
            elif freshness_seconds > self.portfolio_config.execution.stock_freshness_seconds:
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} stock data stale at {freshness_seconds:.0f}s")
                else:
                    failures.append(f"{symbol} stock data stale at {freshness_seconds:.0f}s")
            missing_inventory: list[str] = []
            inventory_counts = {
                "same_day_calls": same_day_calls,
                "same_day_puts": same_day_puts,
                "next_expiry_calls": next_expiry_calls,
                "next_expiry_puts": next_expiry_puts,
            }
            available_strategies: list[str] = []
            unavailable_strategies: list[dict[str, Any]] = []
            for strategy in self.portfolio_config.strategies_by_symbol.get(symbol, []):
                strategy_missing: list[str] = []
                dte_prefix = "same_day" if strategy.dte_mode == "same_day" else "next_expiry"
                for leg in strategy.legs:
                    bucket = f"{dte_prefix}_{leg.option_type}s"
                    if inventory_counts[bucket] == 0:
                        strategy_missing.append(bucket)
                if strategy_missing:
                    unavailable_strategies.append(
                        {
                            "name": strategy.name,
                            "missing_inventory": strategy_missing,
                        }
                    )
                    missing_inventory.extend(strategy_missing)
                else:
                    available_strategies.append(strategy.name)
            symbol_details["available_strategies"] = available_strategies
            if unavailable_strategies:
                symbol_details["unavailable_strategies"] = unavailable_strategies
            if not available_strategies:
                symbol_details["missing_inventory"] = sorted(set(missing_inventory))
                if now_et <= grace_cutoff:
                    pending_reasons.append(f"{symbol} option inventory incomplete")
                else:
                    failures.append(f"{symbol} option inventory incomplete")
            details["underlyings"][symbol] = symbol_details

        if failures:
            details["failures"] = failures
            return "failed", details
        if pending_reasons:
            details["pending_reasons"] = pending_reasons
            return "pending", details
        details["status"] = "passed"
        return "passed", details

    def run_startup_preflight(self) -> dict[str, Any]:
        """Run the launch-time readiness checks without entering the trading loop."""
        clock = self.broker.get_clock()
        trade_date = _trade_date_from_clock(clock)
        ledger = self.load_ledger()
        session = self.load_or_create_session(trade_date, ledger)
        stock_frames = self._fetch_today_stock_frames(trade_date)
        snapshots = {
            symbol: snapshot
            for symbol, snapshot in (
                (
                    symbol,
                    self._build_symbol_snapshot(
                        trade_date=trade_date,
                        underlying_symbol=symbol,
                        stock_frame=stock_frame,
                        session=session,
                    ),
                )
                for symbol, stock_frame in stock_frames.items()
            )
            if snapshot is not None
        }
        status, details = self._perform_startup_check(
            session=session,
            trade_date=trade_date,
            snapshots=snapshots,
            allow_broker_cleanup=False,
        )
        return {
            "status": f"startup_preflight_{status}",
            "trade_date": trade_date.isoformat(),
            "startup_check_status": status,
            "submit_paper_orders": self.submit_paper_orders,
            "broker_cleanup_allowed": False,
            "would_allow_trading": status == "passed",
            "details": details,
        }

    def _send_morning_notification(self, session: SessionState, details: dict[str, Any]) -> None:
        failed_phases = getattr(self, "_failed_notification_phases", set())
        if session.notified_morning or "morning" in failed_phases:
            return
        delivered = self._notify_lines(*self._build_morning_notification_lines(session, details))
        if delivered:
            session.notified_morning = True
        else:
            self._record_notification_failure(session, "morning")

    def _maybe_send_midday_notification(
        self,
        *,
        session: SessionState,
        current_equity: float,
        snapshots: dict[str, SymbolSnapshot],
    ) -> None:
        failed_phases = getattr(self, "_failed_notification_phases", set())
        if session.notified_midday or "midday" in failed_phases:
            return
        current_minute = max((snapshot.current_minute for snapshot in snapshots.values()), default=-1)
        if current_minute < self.portfolio_config.execution.midday_report_minute:
            return
        delivered = self._notify_lines(
            *self._build_midday_notification_lines(session, current_equity=current_equity)
        )
        if delivered:
            session.notified_midday = True
        else:
            self._record_notification_failure(session, "midday")

    def _current_rth_minute(self, trade_date: date) -> int:
        now_et = _now_et()
        rth_open = _rth_open_for(trade_date)
        elapsed_minutes = int((now_et - rth_open).total_seconds() // 60)
        return max(0, min(MINUTES_PER_RTH_SESSION, elapsed_minutes))

    def _due_eod_flatten_checkpoints(
        self,
        *,
        session: SessionState,
        current_minute: int,
    ) -> list[int]:
        completed = {int(value) for value in session.eod_flatten_checkpoints_completed}
        due: list[int] = []
        for minutes_before_close in self.portfolio_config.execution.eod_flatten_minutes_before_close:
            trigger_minute = max(0, MINUTES_PER_RTH_SESSION - int(minutes_before_close))
            if current_minute >= trigger_minute and int(minutes_before_close) not in completed:
                due.append(int(minutes_before_close))
        return due

    def _scheduled_eod_flatten_reason(self, due_checkpoints: list[int]) -> str:
        checkpoint_text = "_".join(f"{minutes}m" for minutes in due_checkpoints)
        return f"{SCHEDULED_EOD_FLATTEN_REASON}_{checkpoint_text}_before_close"

    def _mark_eod_flatten_checkpoints_completed(
        self,
        session: SessionState,
        due_checkpoints: list[int],
    ) -> None:
        completed = {int(value) for value in session.eod_flatten_checkpoints_completed}
        completed.update(int(value) for value in due_checkpoints)
        session.eod_flatten_checkpoints_completed = sorted(completed, reverse=True)

    def _maybe_run_scheduled_eod_flatten(
        self,
        *,
        session: SessionState,
        trade_date: date,
        stock_frames: dict[str, pd.DataFrame] | None,
        current_minute: int,
    ) -> dict[str, Any] | None:
        due_checkpoints = self._due_eod_flatten_checkpoints(
            session=session,
            current_minute=current_minute,
        )
        if not due_checkpoints:
            return None
        reason = self._scheduled_eod_flatten_reason(due_checkpoints)
        if not session.blocked_new_entries or str(session.block_reason or "").startswith(
            SCHEDULED_EOD_FLATTEN_REASON
        ):
            self._record_guardrail_block(session, level="warning", reason=reason)
        else:
            self._alert(session, "warning", reason)
        cleanup_summary = self._run_end_of_day_cleanup_safeguard(
            session=session,
            trade_date=trade_date,
            stock_frames=stock_frames,
        )
        self._mark_eod_flatten_checkpoints_completed(session, due_checkpoints)
        event = {
            "event_type": "scheduled_eod_flatten",
            "trade_date": session.trade_date,
            "reason": reason,
            "current_minute": int(current_minute),
            "minutes_before_close_due": due_checkpoints,
            "open_trade_count_after": len(session.open_trades),
            "shutdown_reconciled": bool(cleanup_summary.get("shutdown_reconciled", False)),
            "unexpected_position_cleanup_count": int(
                cleanup_summary.get("unexpected_position_cleanup_count", 0) or 0
            ),
            "known_trade_cleanup_count": int(cleanup_summary.get("known_trade_cleanup_count", 0) or 0),
        }
        self._append_trade_event(trade_date, event)
        return {
            **event,
            "cleanup_summary": cleanup_summary,
        }

    def _reconcile_and_trade(
        self,
        *,
        session: SessionState,
        ledger: PortfolioLedger,
        stock_frames: dict[str, pd.DataFrame],
        broker_equity: float | None = None,
    ) -> tuple[dict[str, SymbolSnapshot], float]:
        trade_date = date.fromisoformat(session.trade_date)
        snapshots: dict[str, SymbolSnapshot] = {}
        for symbol, stock_frame in stock_frames.items():
            snapshot = self._build_symbol_snapshot(
                trade_date=trade_date,
                underlying_symbol=symbol,
                stock_frame=stock_frame,
                session=session,
            )
            if snapshot is None:
                continue
            snapshots[symbol] = snapshot
            session.last_symbol_regimes[symbol] = infer_symbol_regime(stock_frame)

        combined_mark_map = {
            symbol: mark
            for snapshot in snapshots.values()
            for symbol, mark in snapshot.mark_map.items()
        }
        current_equity = _current_equity(session, combined_mark_map)
        loss_gate, reason = self._daily_loss_gate_check(session, current_equity)
        if loss_gate:
            self._record_guardrail_block(
                session,
                level="warning",
                reason=reason or "daily loss gate triggered",
            )
        severe_action, severe_reason = self._severe_loss_kill_switch_check(session, current_equity)
        if severe_action == "flatten":
            self._record_guardrail_block(
                session,
                level="error",
                reason=severe_reason or SEVERE_LOSS_FLATTEN_REASON,
            )
            self._cleanup_known_open_trades(
                session=session,
                trade_date=trade_date,
                stock_frames=stock_frames,
                reason=SEVERE_LOSS_FLATTEN_REASON,
            )
            self._close_unexpected_broker_positions(
                session=session,
                trade_date=trade_date,
                reason=SEVERE_LOSS_FLATTEN_REASON,
                flatten_all_remaining=True,
            )
            combined_mark_map = {
                symbol: mark
                for snapshot in snapshots.values()
                for symbol, mark in snapshot.mark_map.items()
            }
            current_equity = _current_equity(session, combined_mark_map)
        elif severe_action == "halt":
            self._record_guardrail_block(
                session,
                level="warning",
                reason=severe_reason or SEVERE_LOSS_HALT_REASON,
            )
        min_broker_equity = self._broker_min_equity_to_trade()
        emergency_broker_equity = self._broker_equity_emergency_stop()
        if (
            broker_equity is not None
            and emergency_broker_equity is not None
            and broker_equity <= emergency_broker_equity
        ):
            reason = (
                f"broker equity emergency stop triggered at {broker_equity:.2f} "
                f"(threshold {emergency_broker_equity:.2f})"
            )
            self._record_guardrail_block(session, level="error", reason=reason)
            self._cleanup_known_open_trades(
                session=session,
                trade_date=trade_date,
                stock_frames=stock_frames,
                reason=BROKER_EQUITY_EMERGENCY_STOP_REASON,
            )
            self._close_unexpected_broker_positions(
                session=session,
                trade_date=trade_date,
                reason=BROKER_EQUITY_EMERGENCY_STOP_REASON,
                flatten_all_remaining=True,
            )
        elif (
            broker_equity is not None
            and min_broker_equity is not None
            and broker_equity < min_broker_equity
        ):
            reason = (
                f"broker equity {broker_equity:.2f} below minimum trading threshold "
                f"{min_broker_equity:.2f}"
            )
            self._record_guardrail_block(session, level="warning", reason=reason)

        exiting: list[tuple[dict[str, Any], SymbolSnapshot, str]] = []
        for trade_payload in list(session.open_trades):
            trade = OpenTrade(**trade_payload)
            snapshot = snapshots.get(trade.underlying_symbol)
            if snapshot is None:
                continue
            mark_map = self._mark_to_close(trade, snapshot.option_chain)
            if len(mark_map) != len(trade.legs):
                continue
            should_exit, exit_reason, _current_pnl = self._should_exit_trade(
                trade, snapshot.current_minute, mark_map
            )
            if should_exit:
                exiting.append((trade_payload, snapshot, exit_reason))
        for trade_payload, snapshot, exit_reason in exiting:
            self._run_exit(trade_payload, session, snapshot, exit_reason)

        self._close_unexpected_broker_positions(
            session=session,
            trade_date=trade_date,
            reason=AUTO_FLATTEN_UNEXPECTED_INTRADAY_REASON,
        )
        current_minute = max(
            (snapshot.current_minute for snapshot in snapshots.values()),
            default=self._current_rth_minute(trade_date),
        )
        self._maybe_run_scheduled_eod_flatten(
            session=session,
            trade_date=trade_date,
            stock_frames=stock_frames,
            current_minute=current_minute,
        )

        combined_mark_map = {
            symbol: mark
            for snapshot in snapshots.values()
            for symbol, mark in snapshot.mark_map.items()
        }
        current_equity = _current_equity(session, combined_mark_map)
        self._maybe_send_midday_notification(
            session=session,
            current_equity=current_equity,
            snapshots=snapshots,
        )
        self._apply_entry_execution_circuit_breaker(session)
        if session.blocked_new_entries:
            return snapshots, current_equity

        for underlying_symbol, strategies in self.portfolio_config.strategies_by_symbol.items():
            snapshot = snapshots.get(underlying_symbol)
            if snapshot is None or snapshot.option_chain.empty:
                continue
            for strategy in strategies:
                if strategy.signal_name.startswith("governed_"):
                    signal_active = governed_research_signal_is_true(
                        strategy.signal_name,
                        snapshot.stock_frame,
                        hard_exit_minute=strategy.hard_exit_minute,
                        liquidity_gate=strategy.liquidity_gate,
                        min_minutes_since_open=strategy.min_minutes_since_open,
                        max_minutes_since_open=strategy.max_minutes_since_open,
                        min_trend_gap_pct=strategy.min_trend_gap_pct,
                        max_trend_gap_pct=strategy.max_trend_gap_pct,
                        min_range_pct=strategy.min_range_pct,
                        max_range_pct=strategy.max_range_pct,
                        max_midpoint_distance_pct=strategy.max_midpoint_distance_pct,
                        range_entry_side=strategy.range_entry_side,
                        range_edge_pct=strategy.range_edge_pct,
                    )
                else:
                    signal_active = signal_is_true(
                        strategy.signal_name,
                        snapshot.stock_frame,
                        timing_profile=strategy.timing_profile,
                    )
                if not signal_active:
                    continue
                attempt_id = f"{strategy.name}:{session.trade_date}:{snapshot.current_minute}:{time.time_ns()}"
                open_trade, signal_event = self._evaluate_entry(
                    strategy=strategy,
                    session=session,
                    ledger=ledger,
                    option_chain=snapshot.option_chain,
                    spot_price=snapshot.latest_close,
                    current_minute=snapshot.current_minute,
                    current_equity=current_equity,
                    broker_equity=broker_equity,
                    attempt_id=attempt_id,
                )
                self._append_trade_event(trade_date, signal_event)
                if open_trade is None:
                    continue
                if self._run_entry(open_trade, session, current_equity):
                    current_equity = _current_equity(session, combined_mark_map)
                if session.blocked_new_entries:
                    return snapshots, current_equity
        return snapshots, current_equity

    def _flatten_all(self, session: SessionState, stock_frames: dict[str, pd.DataFrame]) -> dict[str, int]:
        summary = {
            "forced_exit_attempt_count": 0,
            "forced_exit_failure_count": 0,
            "forced_exit_cleanup_count": 0,
            "forced_exit_skipped_existing_close_order_count": 0,
            "forced_exit_skipped_broker_flat_count": 0,
        }
        if not session.open_trades:
            return summary
        trade_date = date.fromisoformat(session.trade_date)
        symbols_with_open_close_orders = self._symbols_with_open_close_orders()
        broker = getattr(self, "broker", None)
        broker_positions_authoritative = callable(getattr(broker, "get_positions", None))
        broker_position_map = self._broker_position_qty_map() if broker_positions_authoritative else {}
        snapshots: dict[str, SymbolSnapshot] = {}
        for symbol in {trade["underlying_symbol"] for trade in session.open_trades}:
            stock_frame = stock_frames.get(symbol, pd.DataFrame())
            snapshot = self._build_symbol_snapshot(
                trade_date=trade_date,
                underlying_symbol=symbol,
                stock_frame=stock_frame,
                session=session,
            )
            if snapshot is not None:
                snapshots[symbol] = snapshot
        for trade_payload in list(session.open_trades):
            snapshot = snapshots.get(trade_payload["underlying_symbol"])
            if snapshot is None:
                continue
            leg_symbols = {
                str(leg.get("symbol") or "").strip()
                for leg in trade_payload.get("legs", [])
                if str(leg.get("symbol") or "").strip()
            }
            if leg_symbols and leg_symbols.issubset(symbols_with_open_close_orders):
                summary["forced_exit_skipped_existing_close_order_count"] += 1
                continue
            if broker_positions_authoritative and self._trade_has_no_broker_position(
                trade_payload,
                broker_position_map,
            ):
                summary["forced_exit_skipped_broker_flat_count"] += 1
                self._drop_open_trade_already_flat_at_broker(
                    trade_payload=trade_payload,
                    session=session,
                    trade_date=trade_date,
                    reason=AUTO_FLATTEN_KNOWN_EOD_REASON,
                )
                continue
            summary["forced_exit_attempt_count"] += 1
            if self._run_exit(trade_payload, session, snapshot, "forced_flatten"):
                continue
            summary["forced_exit_failure_count"] += 1
            if not self.portfolio_config.execution.auto_flatten_unexpected_positions:
                continue
            if self._force_cleanup_known_trade(
                trade_payload=trade_payload,
                session=session,
                trade_date=trade_date,
                stock_frames=stock_frames,
                reason=AUTO_FLATTEN_KNOWN_EOD_REASON,
            ):
                summary["forced_exit_cleanup_count"] += 1
        return summary

    def _run_end_of_day_cleanup_safeguard(
        self,
        *,
        session: SessionState,
        trade_date: date,
        stock_frames: dict[str, pd.DataFrame] | None,
    ) -> dict[str, Any]:
        expected_positions_before_flatten = self._expected_broker_position_map(session)
        flatten_summary = {
            "forced_exit_attempt_count": 0,
            "forced_exit_failure_count": 0,
            "forced_exit_cleanup_count": 0,
            "forced_exit_skipped_broker_flat_count": 0,
        }
        if stock_frames is not None:
            flatten_summary = self._flatten_all(session, stock_frames)
        cleanup_summary: dict[str, Any] = dict(flatten_summary)
        residual_broker_positions: list[dict[str, Any]] = []
        reconciliation_passes: list[dict[str, Any]] = []
        if self.portfolio_config.execution.auto_flatten_unexpected_positions:
            known_cleanup_total = int(flatten_summary["forced_exit_cleanup_count"])
            unexpected_cleanup_entries: list[dict[str, Any]] = []
            for pass_index in range(1, 4):
                known_cleanup_count = self._cleanup_known_open_trades(
                    session=session,
                    trade_date=trade_date,
                    stock_frames=stock_frames,
                    reason=AUTO_FLATTEN_KNOWN_EOD_REASON,
                )
                known_cleanup_total += known_cleanup_count
                unexpected_cleanup_batch = self._close_unexpected_broker_positions(
                    session=session,
                    trade_date=trade_date,
                    reason=AUTO_FLATTEN_UNEXPECTED_EOD_REASON,
                    flatten_all_remaining=True,
                    expected_position_map=expected_positions_before_flatten,
                )
                unexpected_cleanup_entries.extend(unexpected_cleanup_batch)
                residual_broker_positions = self._active_broker_positions()
                reconciliation_passes.append(
                    {
                        "pass_index": pass_index,
                        "known_trade_cleanup_count": known_cleanup_count,
                        "unexpected_position_cleanup_count": len(unexpected_cleanup_batch),
                        "open_trade_count_after_pass": len(session.open_trades),
                        "residual_broker_position_count": len(residual_broker_positions),
                        "residual_broker_symbols": [
                            position["symbol"] for position in residual_broker_positions
                        ],
                    }
                )
                if not session.open_trades and not residual_broker_positions:
                    break
            cleanup_summary.update(
                {
                    "known_trade_cleanup_count": known_cleanup_total,
                    "unexpected_position_cleanup_count": len(unexpected_cleanup_entries),
                    "unexpected_position_cleanup": unexpected_cleanup_entries,
                    "reconciliation_passes": reconciliation_passes,
                }
            )
        if residual_broker_positions:
            cleanup_summary["residual_broker_positions"] = residual_broker_positions
            self._alert(
                session,
                "error",
                "End-of-day reconciliation incomplete; broker still shows open positions after cleanup attempts",
            )
        cleanup_summary["shutdown_reconciled"] = not session.open_trades and not residual_broker_positions
        return cleanup_summary

    def _load_trade_reconciliation_events(self, trade_date: date) -> pd.DataFrame:
        payload = _read_json(self._trade_reconciliation_events_path(trade_date), [])
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list) or not payload:
            return pd.DataFrame()
        frame = pd.DataFrame(payload)
        if "timestamp_et" in frame.columns:
            frame["timestamp_et"] = frame["timestamp_et"].astype(str)
        return frame

    def _build_broker_order_audit_outputs(
        self,
        *,
        trade_date: date,
        events_df: pd.DataFrame,
        active_positions: list[dict[str, Any]] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        get_orders = getattr(getattr(self, "broker", None), "get_orders", None)
        if get_orders is None:
            return pd.DataFrame(), {
                "broker_order_audit_available": False,
                "broker_order_count": 0,
                "broker_order_matched_count": 0,
                "broker_order_unmatched_count": 0,
                "broker_multileg_order_count": 0,
                "broker_partially_filled_order_count": 0,
                "broker_status_mismatch_count": 0,
                "local_order_without_broker_match_count": 0,
                "ending_broker_position_count": int(
                    len(active_positions if active_positions is not None else self._active_broker_positions())
                ),
            }

        event_lookup: dict[tuple[str, str], pd.DataFrame] = {}
        if not events_df.empty:
            if "order_id" in events_df.columns:
                order_series = events_df["order_id"].fillna("").astype(str).str.strip()
                for order_id in sorted({value for value in order_series.tolist() if value}):
                    event_lookup[("order_id", order_id)] = events_df.loc[order_series == order_id].copy()
            if "client_order_id" in events_df.columns:
                client_series = events_df["client_order_id"].fillna("").astype(str).str.strip()
                for client_order_id in sorted({value for value in client_series.tolist() if value}):
                    event_lookup[("client_order_id", client_order_id)] = events_df.loc[client_series == client_order_id].copy()

        audit_rows: list[dict[str, Any]] = []
        matched_order_ids: set[str] = set()
        matched_client_order_ids: set[str] = set()
        for order in get_orders(status="all", limit=500):
            order_id = str(order.get("id") or "").strip()
            client_order_id = str(order.get("client_order_id") or "").strip()
            timestamp_value = (
                order.get("submitted_at")
                or order.get("created_at")
                or order.get("filled_at")
                or order.get("updated_at")
            )
            timestamp = pd.to_datetime(timestamp_value, errors="coerce")
            relevant_by_date = bool(pd.notna(timestamp) and timestamp.date() == trade_date)

            matched_events = pd.DataFrame()
            match_kind = "none"
            if order_id and ("order_id", order_id) in event_lookup:
                matched_events = event_lookup[("order_id", order_id)]
                match_kind = "order_id"
                matched_order_ids.add(order_id)
            elif client_order_id and ("client_order_id", client_order_id) in event_lookup:
                matched_events = event_lookup[("client_order_id", client_order_id)]
                match_kind = "client_order_id"
                matched_client_order_ids.add(client_order_id)

            if not relevant_by_date and matched_events.empty:
                continue

            close_symbols = sorted(self._close_order_symbols(order))
            legs = [leg for leg in (order.get("legs") or []) if isinstance(leg, dict)]
            local_terminal_rows = (
                matched_events.loc[matched_events["event_type"] == "order_terminal"].copy()
                if not matched_events.empty and "event_type" in matched_events.columns
                else pd.DataFrame()
            )
            local_phase_values = (
                sorted({str(value) for value in matched_events.get("phase", pd.Series(dtype="object")).dropna().astype(str).tolist()})
                if not matched_events.empty and "phase" in matched_events.columns
                else []
            )
            local_terminal_status = (
                str(local_terminal_rows.iloc[-1].get("status") or "")
                if not local_terminal_rows.empty
                else ""
            )
            broker_status = str(order.get("status") or "")
            qty = float(order.get("qty") or 0.0)
            filled_qty = float(order.get("filled_qty") or 0.0)
            partially_filled = filled_qty > 0.0 and (qty <= 0.0 or filled_qty < qty)

            audit_rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "order_id": order_id or None,
                    "client_order_id": client_order_id or None,
                    "broker_status": broker_status or None,
                    "local_terminal_status": local_terminal_status or None,
                    "status_match": None
                    if not local_terminal_status
                    else bool(local_terminal_status == broker_status),
                    "matched_to_local": not matched_events.empty,
                    "match_kind": match_kind,
                    "local_phase": ",".join(local_phase_values) if local_phase_values else None,
                    "local_event_count": int(len(matched_events)),
                    "local_submission_count": int(
                        len(matched_events.loc[matched_events["event_type"] == "order_submission"])
                    )
                    if not matched_events.empty and "event_type" in matched_events.columns
                    else 0,
                    "symbol": str(order.get("symbol") or "").strip() or None,
                    "order_class": str(order.get("order_class") or "").strip() or None,
                    "side": str(order.get("side") or "").strip() or None,
                    "position_intent": str(order.get("position_intent") or "").strip() or None,
                    "leg_count": int(len(legs)),
                    "close_leg_count": int(len(close_symbols)),
                    "close_symbols": ",".join(close_symbols) if close_symbols else None,
                    "qty": qty,
                    "filled_qty": filled_qty,
                    "filled_avg_price": float(order.get("filled_avg_price") or 0.0)
                    if order.get("filled_avg_price") not in (None, "")
                    else None,
                    "partially_filled": bool(partially_filled or broker_status == "partially_filled"),
                    "submitted_at": str(timestamp_value) if timestamp_value is not None else None,
                    "relevant_by_date": relevant_by_date,
                }
            )

        audit_df = pd.DataFrame(audit_rows)
        local_reference_frame = (
            events_df.loc[events_df["event_type"] == "order_submission", ["order_id", "client_order_id"]].copy()
            if not events_df.empty and "event_type" in events_df.columns
            else pd.DataFrame(columns=["order_id", "client_order_id"])
        )
        if not local_reference_frame.empty:
            local_reference_frame["order_id"] = local_reference_frame["order_id"].fillna("").astype(str).str.strip()
            local_reference_frame["client_order_id"] = local_reference_frame["client_order_id"].fillna("").astype(str).str.strip()
            local_reference_frame = local_reference_frame.drop_duplicates().reset_index(drop=True)
            local_reference_frame["matched"] = local_reference_frame.apply(
                lambda row: bool(
                    (row["order_id"] and row["order_id"] in matched_order_ids)
                    or (row["client_order_id"] and row["client_order_id"] in matched_client_order_ids)
                ),
                axis=1,
            )
            local_unmatched_order_ids = sorted(
                value for value in local_reference_frame.loc[~local_reference_frame["matched"], "order_id"].tolist() if value
            )
            local_unmatched_client_order_ids = sorted(
                value
                for value in local_reference_frame.loc[~local_reference_frame["matched"], "client_order_id"].tolist()
                if value
            )
            local_order_without_broker_match_count = int((~local_reference_frame["matched"]).sum())
        else:
            local_unmatched_order_ids = []
            local_unmatched_client_order_ids = []
            local_order_without_broker_match_count = 0
        resolved_active_positions = active_positions if active_positions is not None else self._active_broker_positions()
        summary = {
            "broker_order_audit_available": True,
            "broker_order_count": int(len(audit_df)),
            "broker_order_matched_count": int(audit_df["matched_to_local"].sum()) if not audit_df.empty else 0,
            "broker_order_unmatched_count": int((~audit_df["matched_to_local"]).sum()) if not audit_df.empty else 0,
            "broker_multileg_order_count": int((audit_df["leg_count"] > 1).sum()) if not audit_df.empty else 0,
            "broker_partially_filled_order_count": int(audit_df["partially_filled"].sum()) if not audit_df.empty else 0,
            "broker_status_mismatch_count": int(
                (~audit_df["status_match"].map(lambda value: True if pd.isna(value) else bool(value))).sum()
            )
            if not audit_df.empty and "status_match" in audit_df.columns
            else 0,
            "local_order_without_broker_match_count": local_order_without_broker_match_count,
            "local_unmatched_order_ids": local_unmatched_order_ids,
            "local_unmatched_client_order_ids": local_unmatched_client_order_ids,
            "ending_broker_position_count": int(len(resolved_active_positions)),
        }
        return audit_df, summary

    def _build_broker_activity_outputs(
        self,
        *,
        trade_date: date,
        events_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        get_account_activities = getattr(getattr(self, "broker", None), "get_account_activities", None)
        if get_account_activities is None:
            return pd.DataFrame(), {
                "broker_activity_audit_available": False,
                "broker_activity_count": 0,
                "broker_fill_activity_count": 0,
                "broker_partial_fill_activity_count": 0,
                "broker_activity_matched_count": 0,
                "broker_activity_unmatched_count": 0,
                "local_filled_order_without_activity_match_count": 0,
            }

        session_start_utc = datetime.combine(trade_date, dt_time.min, tzinfo=ET).astimezone(UTC)
        session_end_utc = (datetime.combine(trade_date + timedelta(days=1), dt_time.min, tzinfo=ET)).astimezone(UTC)
        activities = get_account_activities(
            category="trade_activity",
            after=session_start_utc,
            until=session_end_utc,
            direction="asc",
            page_size=100,
        )

        local_filled_order_ids = (
            {
                str(value).strip()
                for value in events_df.loc[
                    (events_df.get("event_type", pd.Series(dtype="object")) == "order_terminal")
                    & (events_df.get("status", pd.Series(dtype="object")) == "filled"),
                    "order_id",
                ]
                .dropna()
                .astype(str)
                .tolist()
                if str(value).strip()
            }
            if not events_df.empty and "order_id" in events_df.columns
            else set()
        )

        activity_rows: list[dict[str, Any]] = []
        matched_order_ids: set[str] = set()
        for activity in activities:
            activity_type = str(activity.get("activity_type") or "").strip()
            if activity_type and activity_type != "FILL":
                continue
            order_id = str(activity.get("order_id") or "").strip()
            transaction_time_value = activity.get("transaction_time") or activity.get("date")
            transaction_time = pd.to_datetime(transaction_time_value, errors="coerce", utc=True)
            relevant_by_date = bool(pd.notna(transaction_time) and transaction_time.date() == trade_date)
            if not relevant_by_date and order_id not in local_filled_order_ids:
                continue
            matched_to_local = bool(order_id and order_id in local_filled_order_ids)
            if matched_to_local:
                matched_order_ids.add(order_id)
            activity_rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "activity_id": str(activity.get("id") or "").strip() or None,
                    "activity_type": activity_type or None,
                    "fill_type": str(activity.get("type") or "").strip() or None,
                    "order_id": order_id or None,
                    "symbol": str(activity.get("symbol") or "").strip() or None,
                    "side": str(activity.get("side") or "").strip() or None,
                    "qty": float(activity.get("qty") or 0.0) if activity.get("qty") not in (None, "") else None,
                    "cum_qty": float(activity.get("cum_qty") or 0.0)
                    if activity.get("cum_qty") not in (None, "")
                    else None,
                    "leaves_qty": float(activity.get("leaves_qty") or 0.0)
                    if activity.get("leaves_qty") not in (None, "")
                    else None,
                    "price": float(activity.get("price") or 0.0) if activity.get("price") not in (None, "") else None,
                    "transaction_time": str(transaction_time_value) if transaction_time_value is not None else None,
                    "matched_to_local": matched_to_local,
                    "relevant_by_date": relevant_by_date,
                }
            )

        activity_df = pd.DataFrame(activity_rows)
        local_filled_order_without_activity_match_count = int(
            len([order_id for order_id in local_filled_order_ids if order_id not in matched_order_ids])
        )
        summary = {
            "broker_activity_audit_available": True,
            "broker_activity_count": int(len(activity_df)),
            "broker_fill_activity_count": int(len(activity_df)),
            "broker_partial_fill_activity_count": int(
                (activity_df["fill_type"] == "partial_fill").sum()
            )
            if not activity_df.empty and "fill_type" in activity_df.columns
            else 0,
            "broker_activity_matched_count": int(activity_df["matched_to_local"].sum())
            if not activity_df.empty
            else 0,
            "broker_activity_unmatched_count": int((~activity_df["matched_to_local"]).sum())
            if not activity_df.empty
            else 0,
            "local_filled_order_without_activity_match_count": local_filled_order_without_activity_match_count,
        }
        return activity_df, summary

    def _build_trade_reconciliation_outputs(
        self,
        *,
        session: SessionState,
        trade_date: date,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        events_df = self._load_trade_reconciliation_events(trade_date)
        completed_df = pd.DataFrame(session.completed_trades)
        if completed_df.empty:
            completed_df = pd.DataFrame(columns=[field for field in CompletedTrade.__dataclass_fields__.keys()])
        else:
            completed_df = completed_df.copy()

        if events_df.empty:
            empty = pd.DataFrame()
            summary = {
                "signal_attempt_count": 0,
                "eligible_signal_count": 0,
                "skipped_signal_count": 0,
                "entry_submission_count": 0,
                "exit_submission_count": 0,
                "entry_fill_count": 0,
                "entry_not_filled_count": 0,
                "exit_fill_count": 0,
                "open_reconciled_trade_count": len(session.open_trades),
                "completed_reconciled_trade_count": len(session.completed_trades),
                "realized_reconciled_net_pnl": round(
                    float(completed_df["net_pnl"].sum()) if "net_pnl" in completed_df else 0.0,
                    4,
                ),
            }
            return empty, empty, empty, empty, summary

        if "phase" not in events_df.columns:
            events_df["phase"] = None
        if "attempt_id" not in events_df.columns:
            events_df["attempt_id"] = None

        signal_df = events_df.loc[events_df["event_type"] == "signal_decision"].copy()
        if signal_df.empty:
            signal_df = pd.DataFrame(columns=["attempt_id"])
        entry_results = (
            events_df.loc[events_df["event_type"] == "entry_result"]
            .sort_values("timestamp_et")
            .drop_duplicates(subset=["attempt_id"], keep="last")
        )
        exit_triggers = (
            events_df.loc[events_df["event_type"] == "exit_trigger"]
            .sort_values("timestamp_et")
            .drop_duplicates(subset=["attempt_id"], keep="last")
        )
        exit_results = (
            events_df.loc[events_df["event_type"] == "exit_result"]
            .sort_values("timestamp_et")
            .drop_duplicates(subset=["attempt_id"], keep="last")
        )
        if not completed_df.empty and "entry_attempt_id" in completed_df.columns:
            completed_lookup = completed_df.set_index("entry_attempt_id", drop=False).to_dict("index")
        else:
            completed_lookup = {}
        open_attempt_ids = {
            str(trade.get("entry_attempt_id"))
            for trade in session.open_trades
            if trade.get("entry_attempt_id") is not None
        }

        reconciliation_rows: list[dict[str, Any]] = []
        for signal_row in signal_df.sort_values("timestamp_et").to_dict("records"):
            attempt_id = str(signal_row.get("attempt_id") or "")
            entry_row = (
                entry_results.loc[entry_results["attempt_id"] == attempt_id].iloc[0].to_dict()
                if attempt_id and not entry_results.loc[entry_results["attempt_id"] == attempt_id].empty
                else {}
            )
            exit_trigger_row = (
                exit_triggers.loc[exit_triggers["attempt_id"] == attempt_id].iloc[0].to_dict()
                if attempt_id and not exit_triggers.loc[exit_triggers["attempt_id"] == attempt_id].empty
                else {}
            )
            exit_row = (
                exit_results.loc[exit_results["attempt_id"] == attempt_id].iloc[0].to_dict()
                if attempt_id and not exit_results.loc[exit_results["attempt_id"] == attempt_id].empty
                else {}
            )
            completed_row = completed_lookup.get(attempt_id, {})
            if signal_row.get("decision") != "eligible":
                final_status = "skipped"
            elif exit_row:
                final_status = "completed" if exit_row.get("status") != "not_filled" else "exit_not_filled"
            elif attempt_id in open_attempt_ids:
                final_status = "open"
            elif entry_row:
                final_status = "entered" if entry_row.get("status") != "not_filled" else "entry_not_filled"
            else:
                final_status = "signal_only"
            reconciliation_rows.append(
                {
                    "attempt_id": attempt_id,
                    "trade_date": signal_row.get("trade_date"),
                    "strategy_name": signal_row.get("strategy_name"),
                    "underlying_symbol": signal_row.get("underlying_symbol"),
                    "regime": signal_row.get("regime"),
                    "candidate_variant_id": signal_row.get("candidate_variant_id"),
                    "source_strategy_id": signal_row.get("source_strategy_id"),
                    "promotion_manifest_path": signal_row.get("promotion_manifest_path"),
                    "governed_validation_packet_uri": signal_row.get("governed_validation_packet_uri"),
                    "research_profile": signal_row.get("research_profile"),
                    "research_entry_timing_mode": signal_row.get("research_entry_timing_mode"),
                    "research_entry_offset_minutes": signal_row.get("research_entry_offset_minutes"),
                    "research_exit_offset_minutes": signal_row.get("research_exit_offset_minutes"),
                    "runner_semantics_status": signal_row.get("runner_semantics_status"),
                    "signal_name": signal_row.get("signal_name"),
                    "timing_profile": signal_row.get("timing_profile"),
                    "signal_time_et": signal_row.get("timestamp_et"),
                    "signal_minute": signal_row.get("current_minute"),
                    "signal_decision": signal_row.get("decision"),
                    "signal_reason": signal_row.get("decision_reason"),
                    "quantity_planned": signal_row.get("quantity_planned"),
                    "expected_entry_fill_price": signal_row.get("expected_entry_fill_price"),
                    "entry_submission_count": int(
                        len(
                            events_df.loc[
                                (events_df["attempt_id"] == attempt_id)
                                & (events_df["event_type"] == "order_submission")
                                & (events_df["phase"] == "entry")
                            ]
                        )
                    ),
                    "entry_status": entry_row.get("status"),
                    "entry_order_id": entry_row.get("order_id"),
                    "entry_actual_fill_price": entry_row.get("actual_entry_fill_price"),
                    "entry_slippage": entry_row.get("entry_slippage"),
                    "exit_trigger_reason": exit_trigger_row.get("exit_reason"),
                    "expected_exit_fill_price": exit_row.get(
                        "expected_exit_fill_price",
                        exit_trigger_row.get("expected_exit_fill_price"),
                    ),
                    "exit_submission_count": int(
                        len(
                            events_df.loc[
                                (events_df["attempt_id"] == attempt_id)
                                & (events_df["event_type"] == "order_submission")
                                & (events_df["phase"] == "exit")
                            ]
                        )
                    ),
                    "exit_status": exit_row.get("status"),
                    "exit_order_id": exit_row.get("order_id"),
                    "exit_actual_fill_price": exit_row.get("actual_exit_fill_price"),
                    "exit_slippage": exit_row.get("exit_slippage"),
                    "exit_reason": completed_row.get("exit_reason", exit_row.get("exit_reason")),
                    "realized_net_pnl": completed_row.get("net_pnl", exit_row.get("net_pnl")),
                    "final_status": final_status,
                }
            )

        reconciliation_df = pd.DataFrame(reconciliation_rows)
        completed_for_perf = reconciliation_df.loc[
            reconciliation_df["final_status"] == "completed"
        ].copy() if not reconciliation_df.empty else pd.DataFrame()

        def _performance_table(group_col: str) -> pd.DataFrame:
            if completed_for_perf.empty:
                return pd.DataFrame(
                    columns=[
                        group_col,
                        "trade_count",
                        "net_pnl",
                        "avg_pnl",
                        "win_rate_pct",
                    ]
                )
            grouped = completed_for_perf.groupby(group_col, dropna=False)
            table = grouped["realized_net_pnl"].agg(["count", "sum", "mean"]).reset_index()
            wins = (
                completed_for_perf.assign(_win=completed_for_perf["realized_net_pnl"].astype(float) > 0.0)
                .groupby(group_col, dropna=False)["_win"]
                .mean()
                .reset_index(name="win_rate_pct")
            )
            merged = table.merge(wins, on=group_col, how="left")
            merged = merged.rename(
                columns={
                    "count": "trade_count",
                    "sum": "net_pnl",
                    "mean": "avg_pnl",
                }
            )
            merged["net_pnl"] = merged["net_pnl"].round(4)
            merged["avg_pnl"] = merged["avg_pnl"].round(4)
            merged["win_rate_pct"] = (merged["win_rate_pct"] * 100.0).round(2)
            return merged.sort_values("net_pnl", ascending=False).reset_index(drop=True)

        ticker_df = _performance_table("underlying_symbol")
        strategy_df = _performance_table("strategy_name")
        summary = {
            "signal_attempt_count": int(len(signal_df)),
            "eligible_signal_count": int((signal_df["decision"] == "eligible").sum()) if "decision" in signal_df else 0,
            "skipped_signal_count": int((signal_df["decision"] != "eligible").sum()) if "decision" in signal_df else 0,
            "entry_submission_count": int(
                len(events_df.loc[(events_df["event_type"] == "order_submission") & (events_df["phase"] == "entry")])
            ),
            "exit_submission_count": int(
                len(events_df.loc[(events_df["event_type"] == "order_submission") & (events_df["phase"] == "exit")])
            ),
            "entry_fill_count": int(
                len(entry_results.loc[entry_results["status"].isin(["filled", "dry_run"])])
            ) if not entry_results.empty else 0,
            "entry_not_filled_count": int(
                len(entry_results.loc[entry_results["status"] == "not_filled"])
            ) if not entry_results.empty else 0,
            "exit_fill_count": int(
                len(exit_results.loc[exit_results["status"].isin(["filled", "dry_run"])])
            ) if not exit_results.empty else 0,
            "open_reconciled_trade_count": int((reconciliation_df["final_status"] == "open").sum()) if not reconciliation_df.empty else 0,
            "completed_reconciled_trade_count": int((reconciliation_df["final_status"] == "completed").sum()) if not reconciliation_df.empty else 0,
            "realized_reconciled_net_pnl": round(
                float(pd.to_numeric(completed_for_perf["realized_net_pnl"], errors="coerce").fillna(0.0).sum()),
                4,
            ) if not completed_for_perf.empty else 0.0,
        }
        return events_df, reconciliation_df, ticker_df, strategy_df, summary

    def _update_strategy_performance_ledgers(
        self,
        *,
        trade_date: date,
        completed_df: pd.DataFrame,
    ) -> dict[str, Any]:
        daily_path = self.run_root / "strategy_daily_performance_ledger.csv"
        cumulative_path = self.run_root / "strategy_cumulative_performance.csv"
        daily_columns = [
            "trade_date",
            "strategy_name",
            "underlying_symbol",
            "regime",
            "trade_count",
            "win_count",
            "loss_count",
            "flat_count",
            "net_pnl",
            "avg_pnl",
            "win_rate_pct",
        ]
        cumulative_columns = [
            "strategy_name",
            "underlying_symbol",
            "regime",
            "first_trade_date",
            "last_trade_date",
            "trade_count",
            "win_count",
            "loss_count",
            "flat_count",
            "net_pnl",
            "avg_pnl",
            "win_rate_pct",
        ]
        if completed_df.empty:
            daily_path.parent.mkdir(parents=True, exist_ok=True)
            if not daily_path.exists():
                pd.DataFrame(columns=daily_columns).to_csv(daily_path, index=False)
            if not cumulative_path.exists():
                pd.DataFrame(columns=cumulative_columns).to_csv(cumulative_path, index=False)
            return {
                "strategy_daily_performance_ledger_path": str(daily_path),
                "strategy_cumulative_performance_path": str(cumulative_path),
                "strategy_daily_rows_written": 0,
                "strategy_cumulative_rows": 0,
            }

        frame = completed_df.copy()
        for column in ("strategy_name", "underlying_symbol", "regime"):
            if column not in frame.columns:
                frame[column] = "unknown"
        frame["net_pnl"] = pd.to_numeric(frame.get("net_pnl", 0.0), errors="coerce").fillna(0.0)
        frame["trade_date"] = trade_date.isoformat()
        grouping_columns = ["trade_date", "strategy_name", "underlying_symbol", "regime"]
        grouped = frame.groupby(grouping_columns, dropna=False)
        daily = grouped["net_pnl"].agg(["count", "sum", "mean"]).reset_index()
        daily = daily.merge(
            grouped["net_pnl"].apply(lambda series: int((series > 0.0).sum())).reset_index(name="win_count"),
            on=grouping_columns,
        )
        daily = daily.merge(
            grouped["net_pnl"].apply(lambda series: int((series < 0.0).sum())).reset_index(name="loss_count"),
            on=grouping_columns,
        )
        daily = daily.merge(
            grouped["net_pnl"].apply(lambda series: int((series == 0.0).sum())).reset_index(name="flat_count"),
            on=grouping_columns,
        )
        daily = daily.rename(columns={"count": "trade_count", "sum": "net_pnl", "mean": "avg_pnl"})
        daily["win_rate_pct"] = (daily["win_count"] / daily["trade_count"].clip(lower=1) * 100.0).round(2)
        daily["net_pnl"] = daily["net_pnl"].round(4)
        daily["avg_pnl"] = daily["avg_pnl"].round(4)
        daily = daily[daily_columns].sort_values(
            ["trade_date", "net_pnl", "strategy_name"],
            ascending=[True, False, True],
        )

        if daily_path.exists():
            existing_daily = pd.read_csv(daily_path)
            existing_daily = existing_daily.loc[
                existing_daily["trade_date"].astype(str) != trade_date.isoformat()
            ].copy()
            daily_all = pd.concat([existing_daily, daily], ignore_index=True)
        else:
            daily_all = daily
        daily_path.parent.mkdir(parents=True, exist_ok=True)
        daily_all.to_csv(daily_path, index=False)

        cumulative_grouped = daily_all.groupby(
            ["strategy_name", "underlying_symbol", "regime"],
            dropna=False,
        )
        cumulative = cumulative_grouped.agg(
            first_trade_date=("trade_date", "min"),
            last_trade_date=("trade_date", "max"),
            trade_count=("trade_count", "sum"),
            win_count=("win_count", "sum"),
            loss_count=("loss_count", "sum"),
            flat_count=("flat_count", "sum"),
            net_pnl=("net_pnl", "sum"),
        ).reset_index()
        cumulative["avg_pnl"] = (cumulative["net_pnl"] / cumulative["trade_count"].clip(lower=1)).round(4)
        cumulative["win_rate_pct"] = (
            cumulative["win_count"] / cumulative["trade_count"].clip(lower=1) * 100.0
        ).round(2)
        cumulative["net_pnl"] = cumulative["net_pnl"].round(4)
        cumulative = cumulative.sort_values(
            ["net_pnl", "trade_count", "strategy_name"],
            ascending=[False, False, True],
        )
        cumulative.to_csv(cumulative_path, index=False)
        return {
            "strategy_daily_performance_ledger_path": str(daily_path),
            "strategy_cumulative_performance_path": str(cumulative_path),
            "strategy_daily_rows_written": int(len(daily)),
            "strategy_cumulative_rows": int(len(cumulative)),
        }

    def _classify_guardrail_reason(self, reason: str | None) -> str | None:
        if reason is None:
            return None
        text = str(reason).strip()
        if not text:
            return None
        lower = text.lower()
        if lower in {"max_open_positions", "max_positions_per_regime", "max_positions_per_symbol"}:
            return "capacity"
        if (
            lower == "per_symbol_risk_cap"
            or lower.startswith("bucket_risk_cap:")
            or lower.startswith(f"{REGIME_ENTRY_CLUSTER_REASON}:")
            or lower.startswith(f"{BUCKET_REGIME_ENTRY_CLUSTER_REASON}:")
        ):
            return "concentration"
        if lower in {
            PROJECTED_DELTA_HARD_CAP_REASON,
            PROJECTED_VEGA_HARD_CAP_REASON,
        }:
            return "portfolio_greeks"
        if lower in {LATE_DAY_ENTRY_CUTOFF_REASON} or lower.startswith(f"{EVENT_BLACKOUT_REASON}:"):
            return "timing_filter"
        if lower in {"broker_equity_below_trade_floor", "broker_equity_risk_buffer"}:
            return "equity_buffer"
        if ENTRY_EXECUTION_CIRCUIT_BREAKER_REASON in lower:
            return "execution"
        if SEVERE_LOSS_HALT_REASON in lower or SEVERE_LOSS_FLATTEN_REASON in lower:
            return "loss_limit"
        if BROKER_EQUITY_EMERGENCY_STOP_REASON in lower:
            return "equity_stop"
        if "notification delivery failed" in lower:
            return "notification"
        if "unexpected open positions" in lower or "auto_flatten_" in lower:
            return "cleanup"
        return None

    def _build_guardrail_scorecard_outputs(
        self,
        *,
        session: SessionState,
        trade_date: date,
        events_df: pd.DataFrame,
        cleanup_summary: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
        firing_rows: list[dict[str, Any]] = []
        if not events_df.empty:
            signal_df = events_df.loc[events_df["event_type"] == "signal_decision"].copy()
            for _, row in signal_df.iterrows():
                reason = row.get("decision_reason")
                category = self._classify_guardrail_reason(reason)
                if category is None:
                    continue
                firing_rows.append(
                    {
                        "source": "signal_filter",
                        "timestamp_et": row.get("timestamp_et"),
                        "category": category,
                        "reason": reason,
                        "strategy_name": row.get("strategy_name"),
                        "underlying_symbol": row.get("underlying_symbol"),
                        "regime": row.get("regime"),
                        "current_minute": row.get("current_minute"),
                        "detail": row.get("decision"),
                    }
                )
        for alert in session.alerts:
            message = str(alert.get("message") or "").strip()
            category = self._classify_guardrail_reason(message)
            if category is None:
                continue
            firing_rows.append(
                {
                    "source": "alert",
                    "timestamp_et": alert.get("timestamp_et"),
                    "category": category,
                    "reason": message,
                    "strategy_name": None,
                    "underlying_symbol": None,
                    "regime": None,
                    "current_minute": None,
                    "detail": alert.get("level"),
                }
            )
        if session.block_reason:
            category = self._classify_guardrail_reason(session.block_reason)
            if category is not None:
                firing_rows.append(
                    {
                        "source": "session_block",
                        "timestamp_et": session.last_updated_at,
                        "category": category,
                        "reason": session.block_reason,
                        "strategy_name": None,
                        "underlying_symbol": None,
                        "regime": None,
                        "current_minute": None,
                        "detail": "blocked_new_entries",
                    }
                )
        cleanup_summary = cleanup_summary or {}
        if cleanup_summary.get("known_trade_cleanup_count"):
            firing_rows.append(
                {
                    "source": "cleanup",
                    "timestamp_et": session.last_updated_at,
                    "category": "cleanup",
                    "reason": AUTO_FLATTEN_KNOWN_EOD_REASON,
                    "strategy_name": None,
                    "underlying_symbol": None,
                    "regime": None,
                    "current_minute": None,
                    "detail": f"count={int(cleanup_summary['known_trade_cleanup_count'])}",
                }
            )
        for cleanup_entry in cleanup_summary.get("unexpected_position_cleanup", []) or []:
            reason = str(cleanup_entry.get("reason") or AUTO_FLATTEN_UNEXPECTED_EOD_REASON)
            firing_rows.append(
                {
                    "source": "cleanup",
                    "timestamp_et": cleanup_entry.get("timestamp_et", session.last_updated_at),
                    "category": self._classify_guardrail_reason(reason) or "cleanup",
                    "reason": reason,
                    "strategy_name": None,
                    "underlying_symbol": cleanup_entry.get("symbol"),
                    "regime": None,
                    "current_minute": None,
                    "detail": cleanup_entry.get("status"),
                }
            )

        firings_df = pd.DataFrame(firing_rows)
        if firings_df.empty:
            firings_df = pd.DataFrame(
                columns=[
                    "source",
                    "timestamp_et",
                    "category",
                    "reason",
                    "strategy_name",
                    "underlying_symbol",
                    "regime",
                    "current_minute",
                    "detail",
                ]
            )
        reason_counts_df = pd.DataFrame(
            columns=["category", "reason", "source_count", "fire_count", "symbols", "strategies"]
        )
        if not firings_df.empty:
            grouped_rows: list[dict[str, Any]] = []
            for (category, reason), group in firings_df.groupby(["category", "reason"], dropna=False):
                symbols = sorted({str(value) for value in group["underlying_symbol"].dropna().astype(str) if value})
                strategies = sorted({str(value) for value in group["strategy_name"].dropna().astype(str) if value})
                grouped_rows.append(
                    {
                        "category": category,
                        "reason": reason,
                        "source_count": int(group["source"].nunique()),
                        "fire_count": int(len(group)),
                        "symbols": ", ".join(symbols),
                        "strategies": ", ".join(strategies),
                    }
                )
            reason_counts_df = pd.DataFrame(grouped_rows).sort_values(
                ["fire_count", "category", "reason"], ascending=[False, True, True]
            ).reset_index(drop=True)

        recommendation_rows: list[dict[str, Any]] = []

        def _recommend(
            *,
            priority: str,
            category: str,
            recommendation: str,
            action: str,
            evidence_count: int,
        ) -> None:
            recommendation_rows.append(
                {
                    "priority": priority,
                    "category": category,
                    "recommendation": recommendation,
                    "action": action,
                    "evidence_count": int(evidence_count),
                }
            )

        if reason_counts_df.empty:
            _recommend(
                priority="info",
                category="steady_state",
                recommendation="No guardrails fired today. Keep monitoring the live book and compare tomorrow against the same baseline.",
                action="monitor_only",
                evidence_count=0,
            )
        else:
            for _, row in reason_counts_df.iterrows():
                reason = str(row["reason"])
                category = str(row["category"])
                fire_count = int(row["fire_count"])
                if category in {"loss_limit", "equity_stop"}:
                    _recommend(
                        priority="critical",
                        category=category,
                        recommendation=(
                            f"{reason} fired {fire_count} time(s). Review the full session before changing risk. "
                            "This is a real capital-protection event, not an auto-patch candidate."
                        ),
                        action="manual_review",
                        evidence_count=fire_count,
                    )
                elif category == "execution":
                    _recommend(
                        priority="high",
                        category=category,
                        recommendation=(
                            f"{reason} fired {fire_count} time(s). Check fills, spreads, and broker responses before loosening execution guards."
                        ),
                        action="manual_review",
                        evidence_count=fire_count,
                    )
                elif category == "cleanup":
                    _recommend(
                        priority="high",
                        category=category,
                        recommendation=(
                            f"{reason} fired {fire_count} time(s). The runner already auto-cleaned the operational issue, "
                            "but the wrapper and session logs should be reviewed."
                        ),
                        action="already_auto_fixed",
                        evidence_count=fire_count,
                    )
                elif category == "notification":
                    _recommend(
                        priority="medium",
                        category=category,
                        recommendation=(
                            f"{reason} fired {fire_count} time(s). Notifications degraded; runtime can keep trading, "
                            "but delivery plumbing needs follow-up."
                        ),
                        action="manual_review",
                        evidence_count=fire_count,
                    )
                elif category in {"concentration", "portfolio_greeks"} and fire_count >= 3:
                    _recommend(
                        priority="medium",
                        category=category,
                        recommendation=(
                            f"{reason} blocked {fire_count} entries. The portfolio is crowding one risk bucket, "
                            "so review diversification before relaxing the cap."
                        ),
                        action="monitor_only",
                        evidence_count=fire_count,
                    )
                elif category == "timing_filter" and reason.startswith(f"{EVENT_BLACKOUT_REASON}:"):
                    _recommend(
                        priority="info",
                        category=category,
                        recommendation=(
                            f"{reason} blocked {fire_count} entries as designed. Verify the blackout calendar is still correct for upcoming sessions."
                        ),
                        action="monitor_only",
                        evidence_count=fire_count,
                    )
                elif category == "timing_filter" and reason == LATE_DAY_ENTRY_CUTOFF_REASON and fire_count >= 3:
                    _recommend(
                        priority="info",
                        category=category,
                        recommendation=(
                            f"Late-day entry cutoff blocked {fire_count} entries. Keep it in place unless repeated review shows we are undertrading quality late-day setups."
                        ),
                        action="monitor_only",
                        evidence_count=fire_count,
                    )

        recommendations_df = pd.DataFrame(recommendation_rows)
        if recommendations_df.empty:
            recommendations_df = pd.DataFrame(
                columns=["priority", "category", "recommendation", "action", "evidence_count"]
            )

        summary = {
            "trade_date": trade_date.isoformat(),
            "guardrail_fire_count": int(len(firings_df)),
            "guardrail_reason_count": int(len(reason_counts_df)),
            "signal_filter_count": int((firings_df["source"] == "signal_filter").sum()) if not firings_df.empty else 0,
            "alert_guardrail_count": int((firings_df["source"] == "alert").sum()) if not firings_df.empty else 0,
            "cleanup_guardrail_count": int((firings_df["source"] == "cleanup").sum()) if not firings_df.empty else 0,
            "session_block_count": int((firings_df["source"] == "session_block").sum()) if not firings_df.empty else 0,
            "final_blocked_new_entries": bool(session.blocked_new_entries),
            "final_block_reason": session.block_reason,
            "recommendation_count": int(len(recommendations_df)),
            "manual_review_recommendation_count": int(
                (recommendations_df["action"] == "manual_review").sum()
            ) if not recommendations_df.empty else 0,
            "already_auto_fixed_count": int(
                (recommendations_df["action"] == "already_auto_fixed").sum()
            ) if not recommendations_df.empty else 0,
            "needs_manual_review": bool(
                not recommendations_df.empty and (recommendations_df["action"] == "manual_review").any()
            ),
            "execution_guardrails": dict(session.execution_guardrails),
        }
        return summary, {
            "guardrail_firings": firings_df,
            "guardrail_reason_counts": reason_counts_df,
            "guardrail_recommendations": recommendations_df,
        }

    def _backfill_open_trade_reconciliation(self, session: SessionState) -> bool:
        if not session.open_trades:
            return False
        trade_date = date.fromisoformat(session.trade_date)
        events_df = self._load_trade_reconciliation_events(trade_date)
        existing_attempt_ids = (
            set(events_df["attempt_id"].dropna().astype(str).tolist())
            if not events_df.empty and "attempt_id" in events_df.columns
            else set()
        )
        updated = False
        for index, trade_payload in enumerate(session.open_trades):
            if not trade_payload.get("entry_attempt_id"):
                trade_payload["entry_attempt_id"] = (
                    f"recovered:{trade_payload['strategy_name']}:{trade_payload['entry_minute']}:{index}"
                )
                updated = True
            attempt_id = str(trade_payload["entry_attempt_id"])
            if attempt_id in existing_attempt_ids:
                continue
            trade = OpenTrade(**trade_payload)
            delta_shares, vega_dollars = self._expected_entry_greeks(trade)
            self._append_trade_event(
                trade_date,
                {
                    "event_type": "signal_decision",
                    "attempt_id": attempt_id,
                    "trade_date": session.trade_date,
                    "strategy_name": trade.strategy_name,
                    "underlying_symbol": trade.underlying_symbol,
                    "regime": trade.regime,
                    "candidate_variant_id": trade.candidate_variant_id,
                    "source_strategy_id": trade.source_strategy_id,
                    "promotion_manifest_path": trade.promotion_manifest_path,
                    "governed_validation_packet_uri": trade.governed_validation_packet_uri,
                    "research_profile": trade.research_profile,
                    "research_entry_timing_mode": trade.research_entry_timing_mode,
                    "research_entry_offset_minutes": trade.research_entry_offset_minutes,
                    "research_exit_offset_minutes": trade.research_exit_offset_minutes,
                    "runner_semantics_status": trade.runner_semantics_status,
                    "signal_name": "recovered_open_trade",
                    "timing_profile": "recovered",
                    "current_minute": int(trade.entry_minute),
                    "current_equity": round(float(session.starting_equity), 4),
                    "decision": "eligible",
                    "decision_reason": "backfilled_open_trade",
                    "quantity_planned": int(trade.quantity),
                    "expected_entry_debit": round(float(trade.entry_debit), 4),
                    "expected_entry_fill_price": round(float(trade.entry_fill_price), 4),
                    "expected_delta_shares": round(float(delta_shares), 4),
                    "expected_vega_dollars_1pct": round(float(vega_dollars), 4),
                    "backfilled": True,
                },
            )
            self._append_trade_event(
                trade_date,
                {
                    **self._event_base_for_trade(trade, phase="entry"),
                    "event_type": "entry_result",
                    "status": "filled",
                    "order_id": trade.entry_order_id,
                    "expected_entry_fill_price": round(float(trade.entry_fill_price), 4),
                    "actual_entry_fill_price": round(float(trade.entry_fill_price), 4),
                    "entry_slippage": 0.0,
                    "virtual_cash_after": round(float(session.virtual_cash), 4),
                    "backfilled": True,
                },
            )
            existing_attempt_ids.add(attempt_id)
            updated = True
        return updated

    def finalize_session(
        self,
        session: SessionState,
        ledger: PortfolioLedger,
        *,
        stock_frames: dict[str, pd.DataFrame] | None = None,
    ) -> dict[str, Any]:
        trade_date = date.fromisoformat(session.trade_date)
        cleanup_summary = self._run_end_of_day_cleanup_safeguard(
            session=session,
            trade_date=trade_date,
            stock_frames=stock_frames,
        )
        shutdown_reconciled = bool(cleanup_summary.get("shutdown_reconciled", False))
        ending_equity = session.virtual_cash
        ledger.realized_equity = ending_equity
        ledger.high_watermark = max(ledger.high_watermark, ending_equity)
        closed_day_entry = {
            "trade_date": session.trade_date,
            "starting_equity": session.starting_equity,
            "ending_equity": ending_equity,
            "net_pnl": ending_equity - session.starting_equity,
            "completed_trades": len(session.completed_trades),
            "blocked_new_entries": session.blocked_new_entries,
            "block_reason": session.block_reason,
        }
        ledger.closed_days = [
            row for row in ledger.closed_days if str(row.get("trade_date")) != session.trade_date
        ]
        ledger.closed_days.append(closed_day_entry)
        self.save_ledger(ledger)
        self.save_session(session)
        run_dir = self._session_run_dir(date.fromisoformat(session.trade_date))
        completed_df = pd.DataFrame(session.completed_trades)
        strategy_ledger_summary = self._update_strategy_performance_ledgers(
            trade_date=trade_date,
            completed_df=completed_df,
        )
        (
            reconciliation_events_df,
            reconciliation_df,
            ticker_performance_df,
            strategy_performance_df,
            reconciliation_summary,
        ) = self._build_trade_reconciliation_outputs(
            session=session,
            trade_date=date.fromisoformat(session.trade_date),
        )
        ending_broker_positions = self._active_broker_positions()
        broker_order_audit_df, broker_order_audit_summary = self._build_broker_order_audit_outputs(
            trade_date=date.fromisoformat(session.trade_date),
            events_df=reconciliation_events_df,
            active_positions=ending_broker_positions,
        )
        broker_activity_df, broker_activity_summary = self._build_broker_activity_outputs(
            trade_date=date.fromisoformat(session.trade_date),
            events_df=reconciliation_events_df,
        )
        ending_broker_positions_df = pd.DataFrame(ending_broker_positions)
        summary = {
            "trade_date": session.trade_date,
            "submit_paper_orders": self.submit_paper_orders,
            "starting_equity": round(session.starting_equity, 2),
            "ending_equity": round(ending_equity, 2),
            "net_pnl": round(ending_equity - session.starting_equity, 2),
            "completed_trade_count": len(session.completed_trades),
            "blocked_new_entries": session.blocked_new_entries,
            "block_reason": session.block_reason,
            "last_symbol_regimes": session.last_symbol_regimes,
            "startup_check_status": session.startup_check_status,
        }
        summary.update(_runner_execution_metadata())
        if cleanup_summary:
            summary["end_of_day_cleanup"] = cleanup_summary
        summary["shutdown_reconciled"] = shutdown_reconciled
        summary.update(strategy_ledger_summary)
        summary.update(reconciliation_summary)
        summary.update(broker_order_audit_summary)
        summary.update(broker_activity_summary)
        guardrail_summary, guardrail_tables = self._build_guardrail_scorecard_outputs(
            session=session,
            trade_date=trade_date,
            events_df=reconciliation_events_df,
            cleanup_summary=cleanup_summary,
        )
        summary["guardrail_fire_count"] = guardrail_summary["guardrail_fire_count"]
        summary["guardrail_reason_count"] = guardrail_summary["guardrail_reason_count"]
        summary["guardrail_manual_review_count"] = guardrail_summary["manual_review_recommendation_count"]
        summary["guardrail_auto_fixed_count"] = guardrail_summary["already_auto_fixed_count"]
        summary["guardrail_needs_manual_review"] = guardrail_summary["needs_manual_review"]
        write_summary_bundle(
            run_dir,
            name="multi_ticker_portfolio_session_summary",
            summary=summary,
            table_map={
                "completed_trades": completed_df,
                "trade_reconciliation": reconciliation_df,
                "trade_reconciliation_events": reconciliation_events_df,
                "broker_order_audit": broker_order_audit_df,
                "broker_account_activities": broker_activity_df,
                "ending_broker_positions": ending_broker_positions_df,
                "ticker_performance": ticker_performance_df,
                "strategy_performance": strategy_performance_df,
            },
        )
        write_summary_bundle(
            run_dir,
            name="multi_ticker_portfolio_guardrail_scorecard",
            summary=guardrail_summary,
            table_map=guardrail_tables,
        )
        write_alert_queue(run_dir / "alerts.json", session.alerts)
        failed_phases = getattr(self, "_failed_notification_phases", set())
        if shutdown_reconciled and not session.notified_end_of_day and "end-of-day" not in failed_phases:
            delivered = self._notify_lines(
                *self._build_end_of_day_notification_lines(session, ending_equity=ending_equity)
            )
            if delivered:
                session.notified_end_of_day = True
            else:
                self._record_notification_failure(session, "end-of-day")
            self.save_session(session)
        return summary

    def run(self, *, run_once: bool = False) -> dict[str, Any]:
        clock = self.broker.get_clock()
        trade_date = _trade_date_from_clock(clock)
        now_et = _now_et()
        ledger = self.load_ledger()
        session = self.load_or_create_session(trade_date, ledger)
        lease_status = self.acquire_runtime_ownership(role="portfolio_trader")
        if lease_status.blocked:
            return {
                "status": "ownership_blocked",
                "trade_date": trade_date.isoformat(),
                "lease": {
                    "lease_path": lease_status.lease_path,
                    "blocked_by_owner_id": lease_status.blocked_by_owner_id,
                    "blocked_by_owner_label": lease_status.blocked_by_owner_label,
                    "expires_at": lease_status.expires_at,
                },
            }
        if self._backfill_open_trade_reconciliation(session):
            self.save_session(session)
        startup_block_markers = (
            "option inventory incomplete",
            "stock data stale",
            "latest stock bar timestamp",
            "buying power",
            "unexpected open positions",
        )
        if (
            session.startup_check_status == "passed"
            and session.blocked_new_entries
            and not session.open_trades
            and not session.completed_trades
            and session.block_reason
            and any(marker in session.block_reason.lower() for marker in startup_block_markers)
        ):
            session.blocked_new_entries = False
            session.block_reason = None
            self.save_session(session)
        while not bool(clock.get("is_open", False)):
            lease_status = self.acquire_runtime_ownership(role="portfolio_trader")
            if lease_status.blocked:
                return {
                    "status": "ownership_blocked",
                    "trade_date": trade_date.isoformat(),
                    "lease": {
                        "lease_path": lease_status.lease_path,
                        "blocked_by_owner_id": lease_status.blocked_by_owner_id,
                        "blocked_by_owner_label": lease_status.blocked_by_owner_label,
                        "expires_at": lease_status.expires_at,
                    },
                }
            if now_et < _rth_open_for(trade_date):
                seconds_to_open = (_rth_open_for(trade_date) - now_et).total_seconds()
                if run_once:
                    return {
                        "status": "before_open",
                        "trade_date": trade_date.isoformat(),
                        "seconds_to_open": int(seconds_to_open),
                    }
                time.sleep(max(1.0, min(60.0, seconds_to_open)))
                clock = self.broker.get_clock()
                refreshed_trade_date = _trade_date_from_clock(clock)
                if refreshed_trade_date != trade_date:
                    trade_date = refreshed_trade_date
                    session = self.load_or_create_session(trade_date, ledger)
                    if self._backfill_open_trade_reconciliation(session):
                        self.save_session(session)
                now_et = _now_et()
                continue

            stock_frames = self._fetch_today_stock_frames(trade_date)
            summary = self.finalize_session(session, ledger, stock_frames=stock_frames)
            summary["status"] = "after_close"
            return summary

        while True:
            lease_status = self.acquire_runtime_ownership(role="portfolio_trader")
            if lease_status.blocked:
                return {
                    "status": "ownership_blocked",
                    "trade_date": trade_date.isoformat(),
                    "lease": {
                        "lease_path": lease_status.lease_path,
                        "blocked_by_owner_id": lease_status.blocked_by_owner_id,
                        "blocked_by_owner_label": lease_status.blocked_by_owner_label,
                        "expires_at": lease_status.expires_at,
                    },
                }
            stock_frames = self._fetch_today_stock_frames(trade_date)
            broker_account = self.broker.get_account()
            broker_equity = self._extract_broker_equity(broker_account)
            snapshots = {
                symbol: snapshot
                for symbol, snapshot in (
                    (
                        symbol,
                        self._build_symbol_snapshot(
                            trade_date=trade_date,
                            underlying_symbol=symbol,
                            stock_frame=stock_frame,
                            session=session,
                        ),
                    )
                    for symbol, stock_frame in stock_frames.items()
                )
                if snapshot is not None
            }
            if session.startup_check_status != "passed":
                status, details = self._perform_startup_check(
                    session=session,
                    trade_date=trade_date,
                    snapshots=snapshots,
                )
                session.startup_check_status = status
                session.startup_check_details = details
                self.save_session(session)
                if status == "pending":
                    if run_once:
                        return {
                            "status": "startup_check_pending",
                            "trade_date": trade_date.isoformat(),
                            "details": details,
                        }
                    time.sleep(self.portfolio_config.execution.poll_interval_seconds)
                    continue
                if status == "failed":
                    session.blocked_new_entries = True
                    session.block_reason = "; ".join(details.get("failures", []))
                    self._alert(session, "error", session.block_reason or "startup check failed")
                    self._notify_lines(
                        "**Multi-Ticker Portfolio Morning Check Failed**",
                        f"Trade date: {session.trade_date}",
                        *(details.get("failures", []) or ["startup check failed"]),
                    )
                    self.save_session(session)
                    return {
                        "status": "startup_check_failed",
                        "trade_date": trade_date.isoformat(),
                        "details": details,
                    }
                self._send_morning_notification(session, details)
                session.blocked_new_entries = False
                session.block_reason = None
                self.save_session(session)

            if any(not frame.empty for frame in stock_frames.values()):
                _, current_equity = self._reconcile_and_trade(
                    session=session,
                    ledger=ledger,
                    stock_frames=stock_frames,
                    broker_equity=broker_equity,
                )
                self.save_session(session)
            else:
                self._maybe_run_scheduled_eod_flatten(
                    session=session,
                    trade_date=trade_date,
                    stock_frames=stock_frames,
                    current_minute=self._current_rth_minute(trade_date),
                )
                self.save_session(session)
                current_equity = _current_equity(session)
            if run_once:
                return {
                    "status": "ran_once",
                    "trade_date": trade_date.isoformat(),
                    "open_trades": len(session.open_trades),
                    "completed_trades": len(session.completed_trades),
                    "blocked_new_entries": session.blocked_new_entries,
                    "current_equity": round(current_equity, 2),
                    "startup_check_status": session.startup_check_status,
                }
            clock = self.broker.get_clock()
            if not bool(clock.get("is_open", False)):
                summary = self.finalize_session(session, ledger, stock_frames=stock_frames)
                summary["status"] = "session_complete"
                return summary
            time.sleep(self.portfolio_config.execution.poll_interval_seconds)

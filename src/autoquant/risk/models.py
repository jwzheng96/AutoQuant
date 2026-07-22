from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from autoquant.backtest.models import InstrumentRules, OrderSide
from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash, _decimal_text, _require_nonblank


def _finite(value: Decimal, *, name: str, minimum: Decimal | None = None) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


class ExecutionMode(StrEnum):
    PAPER = "paper"
    LIVE = "live"


class RiskDecisionState(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class RiskCode(StrEnum):
    DAILY_LOSS_LIMIT = "daily_loss_limit"
    DAILY_TURNOVER_LIMIT = "daily_turnover_limit"
    DRAWDOWN_LIMIT = "drawdown_limit"
    DUPLICATE_ORDER = "duplicate_order"
    FUTURE_ORDER = "future_order"
    GROSS_EXPOSURE_LIMIT = "gross_exposure_limit"
    INSUFFICIENT_CASH = "insufficient_cash"
    INSTRUMENT_NOT_ALLOWED = "instrument_not_allowed"
    INVALID_QUANTITY = "invalid_quantity"
    KILL_SWITCH_ACTIVE = "kill_switch_active"
    LIVE_MODE_LOCKED = "live_mode_locked"
    MARKET_CLOSED = "market_closed"
    NO_POSITION = "no_position"
    NOT_SELLABLE = "not_sellable"
    OPEN_ORDER_LIMIT = "open_order_limit"
    ORDER_NOTIONAL_LIMIT = "order_notional_limit"
    POSITION_WEIGHT_LIMIT = "position_weight_limit"
    PRICE_DEVIATION_LIMIT = "price_deviation_limit"
    RECONCILIATION_UNHEALTHY = "reconciliation_unhealthy"
    STALE_ACCOUNT_STATE = "stale_account_state"
    STALE_MARKET_DATA = "stale_market_data"


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    allowed_instruments: tuple[str, ...]
    max_quote_age: timedelta = timedelta(seconds=3)
    max_account_state_age: timedelta = timedelta(seconds=5)
    max_order_notional: Decimal = Decimal("100000")
    max_position_weight: Decimal = Decimal("0.20")
    max_gross_exposure: Decimal = Decimal("0.80")
    max_daily_turnover: Decimal = Decimal("1.00")
    max_daily_loss: Decimal = Decimal("0.03")
    max_drawdown: Decimal = Decimal("0.10")
    max_open_orders: int = 20
    max_price_deviation_bps: Decimal = Decimal("100")
    fee_buffer_rate: Decimal = Decimal("0.001")
    version: str = "paper-pretrade-risk-v1"
    policy_hash: str = field(init=False)

    def __post_init__(self) -> None:
        instruments = tuple(sorted(self.allowed_instruments))
        object.__setattr__(self, "allowed_instruments", instruments)
        if not instruments or len(set(instruments)) != len(instruments):
            raise ValueError("allowed_instruments must be nonempty and unique")
        if any(not value.strip() for value in instruments):
            raise ValueError("allowed instruments cannot be blank")
        if self.max_quote_age <= timedelta(0):
            raise ValueError("max_quote_age must be positive")
        if self.max_account_state_age <= timedelta(0):
            raise ValueError("max_account_state_age must be positive")
        _finite(self.max_order_notional, name="max_order_notional", minimum=Decimal("0"))
        if self.max_order_notional <= 0:
            raise ValueError("max_order_notional must be positive")
        for name, value in (
            ("max_position_weight", self.max_position_weight),
            ("max_gross_exposure", self.max_gross_exposure),
            ("max_daily_turnover", self.max_daily_turnover),
            ("max_daily_loss", self.max_daily_loss),
            ("max_drawdown", self.max_drawdown),
            ("fee_buffer_rate", self.fee_buffer_rate),
        ):
            _finite(value, name=name, minimum=Decimal("0"))
        if not 0 < self.max_position_weight <= 1:
            raise ValueError("max_position_weight must be between zero and one")
        if not 0 < self.max_gross_exposure <= 1:
            raise ValueError("max_gross_exposure must be between zero and one")
        if self.max_daily_turnover <= 0:
            raise ValueError("max_daily_turnover must be positive")
        if not 0 < self.max_daily_loss < 1:
            raise ValueError("max_daily_loss must be between zero and one")
        if not 0 < self.max_drawdown < 1:
            raise ValueError("max_drawdown must be between zero and one")
        if self.fee_buffer_rate >= 1:
            raise ValueError("fee_buffer_rate must be smaller than one")
        if not isinstance(self.max_open_orders, int) or self.max_open_orders < 1:
            raise ValueError("max_open_orders must be a positive integer")
        _finite(
            self.max_price_deviation_bps,
            name="max_price_deviation_bps",
            minimum=Decimal("0"),
        )
        if self.max_price_deviation_bps > 10_000:
            raise ValueError("max_price_deviation_bps cannot exceed 10000")
        _require_nonblank(self.version, name="risk policy version")
        object.__setattr__(
            self,
            "policy_hash",
            _canonical_hash(
                {
                    "allowed_instruments": list(instruments),
                    "fee_buffer_rate": _decimal_text(self.fee_buffer_rate),
                    "max_account_state_age_ms": int(
                        self.max_account_state_age.total_seconds() * 1000
                    ),
                    "max_daily_loss": _decimal_text(self.max_daily_loss),
                    "max_daily_turnover": _decimal_text(self.max_daily_turnover),
                    "max_drawdown": _decimal_text(self.max_drawdown),
                    "max_gross_exposure": _decimal_text(self.max_gross_exposure),
                    "max_open_orders": self.max_open_orders,
                    "max_order_notional": _decimal_text(self.max_order_notional),
                    "max_position_weight": _decimal_text(self.max_position_weight),
                    "max_price_deviation_bps": _decimal_text(
                        self.max_price_deviation_bps
                    ),
                    "max_quote_age_ms": int(self.max_quote_age.total_seconds() * 1000),
                    "version": self.version,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class RiskPosition:
    instrument: str
    total_quantity: int
    sellable_quantity: int
    market_value: Decimal

    def __post_init__(self) -> None:
        _require_nonblank(self.instrument, name="position instrument")
        if self.total_quantity < 0 or self.sellable_quantity < 0:
            raise ValueError("position quantities cannot be negative")
        if self.sellable_quantity > self.total_quantity:
            raise ValueError("sellable quantity cannot exceed total quantity")
        _finite(self.market_value, name="market_value", minimum=Decimal("0"))


@dataclass(frozen=True, slots=True)
class RiskAccountState:
    account_id: str
    as_of: datetime
    cash: Decimal
    equity: Decimal
    day_start_equity: Decimal
    peak_equity: Decimal
    gross_exposure: Decimal
    daily_turnover: Decimal
    open_order_count: int
    reconciled: bool
    kill_switch: bool
    positions: tuple[RiskPosition, ...] = ()
    seen_client_order_ids: tuple[str, ...] = ()
    state_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        as_of = to_utc(self.as_of, name="account as_of")
        object.__setattr__(self, "as_of", as_of)
        positions = tuple(sorted(self.positions, key=lambda item: item.instrument))
        seen = tuple(sorted(self.seen_client_order_ids))
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "seen_client_order_ids", seen)
        if len({item.instrument for item in positions}) != len(positions):
            raise ValueError("positions must contain unique instruments")
        if len(set(seen)) != len(seen):
            raise ValueError("seen_client_order_ids must be unique")
        for name, value in (
            ("cash", self.cash),
            ("equity", self.equity),
            ("day_start_equity", self.day_start_equity),
            ("peak_equity", self.peak_equity),
            ("gross_exposure", self.gross_exposure),
            ("daily_turnover", self.daily_turnover),
        ):
            _finite(value, name=name, minimum=Decimal("0"))
        if self.equity <= 0 or self.day_start_equity <= 0 or self.peak_equity <= 0:
            raise ValueError("equity reference values must be positive")
        if self.peak_equity < self.equity:
            raise ValueError("peak_equity cannot be below current equity")
        if not isinstance(self.open_order_count, int) or self.open_order_count < 0:
            raise ValueError("open_order_count must be a nonnegative integer")
        if type(self.reconciled) is not bool or type(self.kill_switch) is not bool:
            raise TypeError("reconciled and kill_switch must be bools")
        position_value = sum(
            (position.market_value for position in positions), Decimal("0")
        )
        if position_value != self.gross_exposure:
            raise ValueError("gross_exposure must equal long position market value")
        if (self.cash + self.gross_exposure - self.equity).copy_abs() > Decimal("0.01"):
            raise ValueError("equity must reconcile to cash plus gross exposure")
        object.__setattr__(self, "state_hash", _canonical_hash(_account_payload(self)))


@dataclass(frozen=True, slots=True)
class MarketQuote:
    instrument: str
    as_of: datetime
    last_price: Decimal
    bid_price: Decimal
    ask_price: Decimal
    market_open: bool
    quote_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.instrument, name="quote instrument")
        as_of = to_utc(self.as_of, name="quote as_of")
        object.__setattr__(self, "as_of", as_of)
        for name, value in (
            ("last_price", self.last_price),
            ("bid_price", self.bid_price),
            ("ask_price", self.ask_price),
        ):
            _finite(value, name=name, minimum=Decimal("0"))
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.bid_price > self.ask_price:
            raise ValueError("bid_price cannot exceed ask_price")
        if type(self.market_open) is not bool:
            raise TypeError("market_open must be a bool")
        object.__setattr__(
            self,
            "quote_hash",
            _canonical_hash(
                {
                    "ask_price": _decimal_text(self.ask_price),
                    "as_of": as_of.isoformat(timespec="microseconds"),
                    "bid_price": _decimal_text(self.bid_price),
                    "instrument": self.instrument,
                    "last_price": _decimal_text(self.last_price),
                    "market_open": self.market_open,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class ProposedOrder:
    client_order_id: str
    instrument: str
    side: OrderSide
    quantity: int
    submitted_at: datetime
    limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        _require_nonblank(self.client_order_id, name="client_order_id")
        _require_nonblank(self.instrument, name="order instrument")
        if len(self.client_order_id) > 128:
            raise ValueError("client_order_id cannot exceed 128 characters")
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be OrderSide")
        if not isinstance(self.quantity, int) or isinstance(self.quantity, bool):
            raise TypeError("quantity must be an integer")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        object.__setattr__(
            self,
            "submitted_at",
            to_utc(self.submitted_at, name="submitted_at"),
        )
        if self.limit_price is not None:
            _finite(self.limit_price, name="limit_price", minimum=Decimal("0"))
            if self.limit_price <= 0:
                raise ValueError("limit_price must be positive")


@dataclass(frozen=True, slots=True)
class RiskDecision:
    account_id: str
    mode: ExecutionMode
    order: ProposedOrder
    evaluated_at: datetime
    state: RiskDecisionState
    violations: tuple[RiskCode, ...]
    policy_hash: str
    account_state_hash: str
    quote_hash: str
    rules_version: str
    estimated_price: Decimal
    order_notional: Decimal
    projected_cash: Decimal
    projected_gross_exposure: Decimal
    projected_position_weight: Decimal
    projected_daily_turnover: Decimal
    decision_hash: str = field(init=False)

    def __post_init__(self) -> None:
        evaluated = to_utc(self.evaluated_at, name="evaluated_at")
        object.__setattr__(self, "evaluated_at", evaluated)
        violations = tuple(self.violations)
        object.__setattr__(self, "violations", violations)
        if len(set(violations)) != len(violations):
            raise ValueError("risk violations must be unique")
        if (self.state is RiskDecisionState.ACCEPTED) != (not violations):
            raise ValueError("accepted decision must have no violations")
        for name, value in (
            ("estimated_price", self.estimated_price),
            ("order_notional", self.order_notional),
            ("projected_cash", self.projected_cash),
            ("projected_gross_exposure", self.projected_gross_exposure),
            ("projected_position_weight", self.projected_position_weight),
            ("projected_daily_turnover", self.projected_daily_turnover),
        ):
            _finite(value, name=name)
        object.__setattr__(
            self,
            "decision_hash",
            _canonical_hash(risk_decision_payload(self)),
        )


@dataclass(frozen=True, slots=True)
class RiskEvaluationInput:
    mode: ExecutionMode
    policy: RiskPolicy
    account: RiskAccountState
    order: ProposedOrder
    quote: MarketQuote
    rules: InstrumentRules
    now: datetime


def _account_payload(account: RiskAccountState) -> dict[str, object]:
    return {
        "account_id": account.account_id,
        "as_of": account.as_of.isoformat(timespec="microseconds"),
        "cash": _decimal_text(account.cash),
        "daily_turnover": _decimal_text(account.daily_turnover),
        "day_start_equity": _decimal_text(account.day_start_equity),
        "equity": _decimal_text(account.equity),
        "gross_exposure": _decimal_text(account.gross_exposure),
        "kill_switch": account.kill_switch,
        "open_order_count": account.open_order_count,
        "peak_equity": _decimal_text(account.peak_equity),
        "positions": [
            {
                "instrument": position.instrument,
                "market_value": _decimal_text(position.market_value),
                "sellable_quantity": position.sellable_quantity,
                "total_quantity": position.total_quantity,
            }
            for position in account.positions
        ],
        "reconciled": account.reconciled,
        "seen_client_order_ids": list(account.seen_client_order_ids),
    }


def _order_payload(order: ProposedOrder) -> dict[str, object]:
    return {
        "client_order_id": order.client_order_id,
        "instrument": order.instrument,
        "limit_price": (
            None if order.limit_price is None else _decimal_text(order.limit_price)
        ),
        "quantity": order.quantity,
        "side": order.side.value,
        "submitted_at": order.submitted_at.isoformat(timespec="microseconds"),
    }


def risk_decision_payload(decision: RiskDecision) -> dict[str, object]:
    return {
        "account_id": decision.account_id,
        "account_state_hash": decision.account_state_hash,
        "estimated_price": _decimal_text(decision.estimated_price),
        "evaluated_at": decision.evaluated_at.isoformat(timespec="microseconds"),
        "mode": decision.mode.value,
        "order": _order_payload(decision.order),
        "order_notional": _decimal_text(decision.order_notional),
        "policy_hash": decision.policy_hash,
        "projected_cash": _decimal_text(decision.projected_cash),
        "projected_daily_turnover": _decimal_text(
            decision.projected_daily_turnover
        ),
        "projected_gross_exposure": _decimal_text(
            decision.projected_gross_exposure
        ),
        "projected_position_weight": _decimal_text(
            decision.projected_position_weight
        ),
        "quote_hash": decision.quote_hash,
        "rules_version": decision.rules_version,
        "state": decision.state.value,
        "violations": [value.value for value in decision.violations],
    }

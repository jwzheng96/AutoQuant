from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from autoquant.clock import to_utc
from autoquant.data.daily_models import DailyBarRevision
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)


def _finite_decimal(
    value: Decimal, *, name: str, minimum: Decimal | None = None
) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class ExecutionState(StrEnum):
    FILLED = "filled"
    REJECTED = "rejected"


class RejectionCode(StrEnum):
    CASH_INSUFFICIENT = "cash_insufficient"
    DUPLICATE_ORDER = "duplicate_order"
    INVALID_BUY_QUANTITY = "invalid_buy_quantity"
    INVALID_SELL_QUANTITY = "invalid_sell_quantity"
    LIMIT_DOWN_LOCKED = "limit_down_locked"
    LIMIT_UP_LOCKED = "limit_up_locked"
    LIQUIDITY_LIMIT = "liquidity_limit"
    NO_POSITION = "no_position"
    NOT_SELLABLE = "not_sellable"
    OUTSIDE_SESSION = "outside_session"
    PRICE_UNAVAILABLE = "price_unavailable"
    SUSPENDED = "suspended"
    UNKNOWN_INSTRUMENT = "unknown_instrument"


@dataclass(frozen=True, slots=True)
class PriceLimit:
    rate: Decimal | None
    reason: str
    rule_version: str

    def __post_init__(self) -> None:
        _require_nonblank(self.reason, name="price-limit reason")
        _require_nonblank(self.rule_version, name="price-limit rule_version")
        if self.rate is not None:
            _finite_decimal(self.rate, name="price-limit rate", minimum=Decimal("0"))
            if self.rate <= 0 or self.rate >= 1:
                raise ValueError("price-limit rate must be between zero and one")


@dataclass(frozen=True, slots=True)
class InstrumentRules:
    instrument: str
    buy_minimum: int
    buy_step: int
    sell_step: int
    price_tick: Decimal
    max_order_quantity: int
    t_plus_one: bool
    price_limit: PriceLimit
    effective_from: date
    rule_version: str

    def __post_init__(self) -> None:
        _require_nonblank(self.instrument, name="instrument")
        _require_nonblank(self.rule_version, name="rule_version")
        for name, value in (
            ("buy_minimum", self.buy_minimum),
            ("buy_step", self.buy_step),
            ("sell_step", self.sell_step),
            ("max_order_quantity", self.max_order_quantity),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.buy_minimum > self.max_order_quantity:
            raise ValueError("buy_minimum cannot exceed max_order_quantity")
        _finite_decimal(self.price_tick, name="price_tick", minimum=Decimal("0"))
        if self.price_tick <= 0:
            raise ValueError("price_tick must be positive")
        if type(self.t_plus_one) is not bool:
            raise TypeError("t_plus_one must be a bool")
        if not isinstance(self.price_limit, PriceLimit):
            raise TypeError("price_limit must be PriceLimit")


@dataclass(frozen=True, slots=True)
class MarketState:
    bar: DailyBarRevision
    rules: InstrumentRules
    suspended: bool

    def __post_init__(self) -> None:
        if not isinstance(self.bar, DailyBarRevision):
            raise TypeError("bar must be DailyBarRevision")
        if not isinstance(self.rules, InstrumentRules):
            raise TypeError("rules must be InstrumentRules")
        if self.rules.instrument != self.bar.instrument:
            raise ValueError("rules and bar instrument must match")
        if self.rules.effective_from > self.bar.session_date:
            raise ValueError("rules are not effective for the bar session")
        if type(self.suspended) is not bool:
            raise TypeError("suspended must be a bool")


@dataclass(frozen=True, slots=True)
class OrderIntent:
    client_order_id: str
    instrument: str
    side: OrderSide
    quantity: int
    session_date: date
    submitted_at: datetime

    def __post_init__(self) -> None:
        _require_nonblank(self.client_order_id, name="client_order_id")
        _require_nonblank(self.instrument, name="instrument")
        if len(self.client_order_id) > 128:
            raise ValueError("client_order_id cannot exceed 128 characters")
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be OrderSide")
        if not isinstance(self.quantity, int) or isinstance(self.quantity, bool):
            raise TypeError("quantity must be an integer")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        object.__setattr__(
            self, "submitted_at", to_utc(self.submitted_at, name="submitted_at")
        )


@dataclass(frozen=True, slots=True)
class FeeBreakdown:
    commission: Decimal
    stamp_duty: Decimal
    transfer_fee: Decimal

    def __post_init__(self) -> None:
        for name, value in (
            ("commission", self.commission),
            ("stamp_duty", self.stamp_duty),
            ("transfer_fee", self.transfer_fee),
        ):
            _finite_decimal(value, name=name, minimum=Decimal("0"))

    @property
    def total(self) -> Decimal:
        return self.commission + self.stamp_duty + self.transfer_fee


@dataclass(frozen=True, slots=True)
class ExecutionReport:
    client_order_id: str
    instrument: str
    side: OrderSide
    requested_quantity: int
    state: ExecutionState
    session_date: date
    filled_quantity: int = 0
    fill_price: Decimal | None = None
    gross_amount: Decimal = Decimal("0")
    fees: FeeBreakdown = FeeBreakdown(Decimal("0"), Decimal("0"), Decimal("0"))
    rejection_code: RejectionCode | None = None
    ledger_hash: str = ""

    def __post_init__(self) -> None:
        if self.state is ExecutionState.FILLED:
            if self.filled_quantity != self.requested_quantity or self.fill_price is None:
                raise ValueError("filled report must contain the full requested quantity")
            if self.rejection_code is not None:
                raise ValueError("filled report cannot contain a rejection code")
        else:
            if self.filled_quantity != 0 or self.fill_price is not None:
                raise ValueError("rejected report cannot contain a fill")
            if self.rejection_code is None:
                raise ValueError("rejected report requires a rejection code")
        if self.ledger_hash:
            _require_lowercase_sha256(self.ledger_hash, name="ledger_hash")


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    instrument: str
    total_quantity: int
    sellable_quantity: int
    average_cost: Decimal
    market_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    session_date: date
    cash: Decimal
    market_value: Decimal
    equity: Decimal
    positions: tuple[PositionSnapshot, ...]
    ledger_hash: str

    def __post_init__(self) -> None:
        _require_lowercase_sha256(self.ledger_hash, name="ledger_hash")


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    sequence: int
    event_type: str
    session_date: date
    client_order_id: str
    payload: tuple[tuple[str, str], ...]
    previous_hash: str
    event_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("sequence must be positive")
        _require_nonblank(self.event_type, name="event_type")
        _require_nonblank(self.client_order_id, name="client_order_id")
        _require_lowercase_sha256(self.previous_hash, name="previous_hash")
        if tuple(sorted(self.payload)) != self.payload:
            raise ValueError("payload must be sorted")
        if len({key for key, _ in self.payload}) != len(self.payload):
            raise ValueError("payload keys must be unique")
        payload = {
            "client_order_id": self.client_order_id,
            "event_type": self.event_type,
            "payload": dict(self.payload),
            "previous_hash": self.previous_hash,
            "sequence": self.sequence,
            "session_date": self.session_date.isoformat(),
        }
        object.__setattr__(self, "event_hash", _canonical_hash(payload))


@dataclass(frozen=True, slots=True)
class BacktestSession:
    session_date: date
    markets: tuple[MarketState, ...]
    orders: tuple[OrderIntent, ...]

    def __post_init__(self) -> None:
        markets = tuple(self.markets)
        orders = tuple(self.orders)
        object.__setattr__(self, "markets", markets)
        object.__setattr__(self, "orders", orders)
        instruments = tuple(market.bar.instrument for market in markets)
        if not markets or len(set(instruments)) != len(instruments):
            raise ValueError("markets must be nonempty with unique instruments")
        if any(market.bar.session_date != self.session_date for market in markets):
            raise ValueError("market bar must belong to the session")
        if any(order.session_date != self.session_date for order in orders):
            raise ValueError("order must belong to the session")


@dataclass(frozen=True, slots=True)
class BacktestResult:
    strategy_id: str
    manifest_hash: str
    as_of: datetime
    initial_cash: Decimal
    ending_equity: Decimal
    total_return: Decimal
    max_drawdown: Decimal
    turnover: Decimal
    total_fees: Decimal
    reports: tuple[ExecutionReport, ...]
    snapshots: tuple[AccountSnapshot, ...]
    rule_versions: tuple[str, ...]
    fee_version: str
    execution_version: str
    ledger_hash: str
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.strategy_id, name="strategy_id")
        _require_lowercase_sha256(self.manifest_hash, name="manifest_hash")
        _require_lowercase_sha256(self.ledger_hash, name="ledger_hash")
        object.__setattr__(self, "as_of", to_utc(self.as_of, name="as_of"))
        payload = {
            "as_of": self.as_of.isoformat(timespec="microseconds"),
            "ending_equity": _decimal_text(self.ending_equity),
            "execution_version": self.execution_version,
            "fee_version": self.fee_version,
            "initial_cash": _decimal_text(self.initial_cash),
            "ledger_hash": self.ledger_hash,
            "manifest_hash": self.manifest_hash,
            "max_drawdown": _decimal_text(self.max_drawdown),
            "rule_versions": list(self.rule_versions),
            "strategy_id": self.strategy_id,
            "total_fees": _decimal_text(self.total_fees),
            "total_return": _decimal_text(self.total_return),
            "turnover": _decimal_text(self.turnover),
        }
        object.__setattr__(self, "result_hash", _canonical_hash(payload))

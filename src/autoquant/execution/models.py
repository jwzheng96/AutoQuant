from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from autoquant.backtest.models import OrderSide
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.risk.models import ExecutionMode, RiskDecision, RiskDecisionState

ZERO_HASH = "0" * 64


def _finite(value: Decimal, *, name: str, minimum: Decimal | None = None) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


class PaperOrderState(StrEnum):
    APPROVED = "approved"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


TERMINAL_STATES = frozenset(
    {PaperOrderState.FILLED, PaperOrderState.CANCELLED, PaperOrderState.REJECTED}
)


@dataclass(frozen=True, slots=True)
class ApprovedPaperOrder:
    account_id: str
    client_order_id: str
    risk_decision_hash: str
    instrument: str
    side: OrderSide
    quantity: int
    limit_price: Decimal | None
    approved_at: datetime
    order_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("client_order_id", self.client_order_id),
            ("instrument", self.instrument),
        ):
            _require_nonblank(value, name=name)
        _require_lowercase_sha256(
            self.risk_decision_hash, name="risk_decision_hash"
        )
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be OrderSide")
        if not isinstance(self.quantity, int) or isinstance(self.quantity, bool):
            raise TypeError("quantity must be an integer")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.limit_price is not None:
            _finite(self.limit_price, name="limit_price", minimum=Decimal("0"))
            if self.limit_price == 0:
                raise ValueError("limit_price must be positive")
        approved_at = to_utc(self.approved_at, name="approved_at")
        object.__setattr__(self, "approved_at", approved_at)
        object.__setattr__(self, "order_hash", _canonical_hash(order_payload(self)))

    @classmethod
    def from_risk_decision(cls, decision: RiskDecision) -> ApprovedPaperOrder:
        if decision.mode is not ExecutionMode.PAPER:
            raise ValueError("only paper risk decisions can approve paper orders")
        if decision.state is not RiskDecisionState.ACCEPTED:
            raise ValueError("rejected risk decisions cannot approve paper orders")
        return cls(
            account_id=decision.account_id,
            client_order_id=decision.order.client_order_id,
            risk_decision_hash=decision.decision_hash,
            instrument=decision.order.instrument,
            side=decision.order.side,
            quantity=decision.order.quantity,
            limit_price=decision.order.limit_price,
            approved_at=decision.evaluated_at,
        )


@dataclass(frozen=True, slots=True)
class BrokerOrderUpdate:
    account_id: str
    client_order_id: str
    broker_order_id: str
    broker_sequence: int
    state: PaperOrderState
    cumulative_filled_quantity: int
    average_fill_price: Decimal | None
    occurred_at: datetime
    rejection_code: str | None = None
    update_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("client_order_id", self.client_order_id),
            ("broker_order_id", self.broker_order_id),
        ):
            _require_nonblank(value, name=name)
        if not isinstance(self.broker_sequence, int) or isinstance(
            self.broker_sequence, bool
        ):
            raise TypeError("broker_sequence must be an integer")
        if self.broker_sequence < 1:
            raise ValueError("broker_sequence must be positive")
        if self.state is PaperOrderState.APPROVED:
            raise ValueError("broker cannot report the local approved state")
        if (
            not isinstance(self.cumulative_filled_quantity, int)
            or isinstance(self.cumulative_filled_quantity, bool)
            or self.cumulative_filled_quantity < 0
        ):
            raise ValueError("cumulative_filled_quantity must be a nonnegative integer")
        if self.cumulative_filled_quantity == 0:
            if self.average_fill_price is not None:
                raise ValueError("zero fills cannot have an average_fill_price")
        else:
            if self.average_fill_price is None:
                raise ValueError("filled quantity requires average_fill_price")
            _finite(
                self.average_fill_price,
                name="average_fill_price",
                minimum=Decimal("0"),
            )
            if self.average_fill_price == 0:
                raise ValueError("average_fill_price must be positive")
        if self.state is PaperOrderState.REJECTED:
            _require_nonblank(self.rejection_code or "", name="rejection_code")
        elif self.rejection_code is not None:
            raise ValueError("rejection_code is only valid for rejected updates")
        occurred_at = to_utc(self.occurred_at, name="occurred_at")
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "update_hash", _canonical_hash(update_payload(self)))


@dataclass(frozen=True, slots=True)
class PaperOrderProjection:
    order: ApprovedPaperOrder
    state: PaperOrderState
    broker_order_id: str | None
    cumulative_filled_quantity: int
    average_fill_price: Decimal | None
    last_broker_sequence: int
    last_update_hash: str
    last_event_hash: str
    updated_at: datetime
    version: int
    projection_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.state, PaperOrderState):
            raise TypeError("state must be PaperOrderState")
        if self.state is not PaperOrderState.APPROVED and self.broker_order_id is None:
            raise ValueError("broker_order_id is required after approval")
        if self.broker_order_id is not None:
            _require_nonblank(self.broker_order_id, name="broker_order_id")
        if self.version < 0 or self.last_broker_sequence < 0:
            raise ValueError("projection counters cannot be negative")
        if self.version == 0:
            if self.state is not PaperOrderState.APPROVED:
                raise ValueError("version zero must be approved")
            if self.last_broker_sequence != 0:
                raise ValueError("new projection cannot have broker sequence")
        if (
            not isinstance(self.cumulative_filled_quantity, int)
            or isinstance(self.cumulative_filled_quantity, bool)
            or not 0 <= self.cumulative_filled_quantity <= self.order.quantity
        ):
            raise ValueError("projection cumulative fill is outside order quantity")
        if self.cumulative_filled_quantity == 0:
            if self.average_fill_price is not None:
                raise ValueError("zero fills cannot have an average fill price")
        else:
            if self.average_fill_price is None:
                raise ValueError("filled projection requires average fill price")
            _finite(
                self.average_fill_price,
                name="average_fill_price",
                minimum=Decimal("0"),
            )
            if self.average_fill_price == 0:
                raise ValueError("average_fill_price must be positive")
        if self.state in {PaperOrderState.APPROVED, PaperOrderState.SUBMITTED}:
            if self.cumulative_filled_quantity != 0:
                raise ValueError("unfilled projection state cannot contain fills")
        elif self.state is PaperOrderState.PARTIALLY_FILLED:
            if not 0 < self.cumulative_filled_quantity < self.order.quantity:
                raise ValueError("partial projection must contain a partial fill")
        elif self.state is PaperOrderState.FILLED:
            if self.cumulative_filled_quantity != self.order.quantity:
                raise ValueError("filled projection must equal order quantity")
        elif self.state is PaperOrderState.REJECTED:
            if self.cumulative_filled_quantity != 0:
                raise ValueError("rejected projection cannot contain fills")
        elif self.state is PaperOrderState.CANCELLED:
            if self.cumulative_filled_quantity == self.order.quantity:
                raise ValueError("cancelled projection cannot be fully filled")
        _require_lowercase_sha256(self.last_update_hash, name="last_update_hash")
        _require_lowercase_sha256(self.last_event_hash, name="last_event_hash")
        updated_at = to_utc(self.updated_at, name="updated_at")
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(
            self,
            "projection_hash",
            _canonical_hash(projection_payload(self)),
        )

    @classmethod
    def create(cls, order: ApprovedPaperOrder) -> PaperOrderProjection:
        return cls(
            order=order,
            state=PaperOrderState.APPROVED,
            broker_order_id=None,
            cumulative_filled_quantity=0,
            average_fill_price=None,
            last_broker_sequence=0,
            last_update_hash=ZERO_HASH,
            last_event_hash=ZERO_HASH,
            updated_at=order.approved_at,
            version=0,
        )


@dataclass(frozen=True, slots=True)
class PaperOrderEvent:
    sequence: int
    client_order_id: str
    previous_hash: str
    update_hash: str
    resulting_state: PaperOrderState
    projection_hash: str
    event_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("event sequence must be positive")
        _require_nonblank(self.client_order_id, name="client_order_id")
        for name, value in (
            ("previous_hash", self.previous_hash),
            ("update_hash", self.update_hash),
            ("projection_hash", self.projection_hash),
        ):
            _require_lowercase_sha256(value, name=name)
        object.__setattr__(
            self,
            "event_hash",
            _canonical_hash(
                {
                    "client_order_id": self.client_order_id,
                    "previous_hash": self.previous_hash,
                    "projection_hash": self.projection_hash,
                    "resulting_state": self.resulting_state.value,
                    "sequence": self.sequence,
                    "update_hash": self.update_hash,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class PaperOrderTransition:
    projection: PaperOrderProjection
    event: PaperOrderEvent | None
    applied: bool

    def __post_init__(self) -> None:
        if self.applied != (self.event is not None):
            raise ValueError("applied must match event presence")


@dataclass(frozen=True, slots=True)
class PaperOrderHistory:
    order: ApprovedPaperOrder
    state: PaperOrderState
    updates: tuple[BrokerOrderUpdate, ...]

    def __post_init__(self) -> None:
        updates = tuple(self.updates)
        object.__setattr__(self, "updates", updates)
        if not updates:
            if self.state is not PaperOrderState.APPROVED:
                raise ValueError("history without broker facts must remain approved")
            return
        if self.state is PaperOrderState.APPROVED:
            raise ValueError("history with broker facts cannot remain approved")
        if updates[-1].state is not self.state:
            raise ValueError("history state must match the latest broker update")
        broker_order_id = updates[0].broker_order_id
        previous_sequence = 0
        previous_filled = 0
        previous_time = self.order.approved_at
        for update in updates:
            if (
                update.account_id != self.order.account_id
                or update.client_order_id != self.order.client_order_id
            ):
                raise ValueError("history update does not match order identity")
            if update.broker_order_id != broker_order_id:
                raise ValueError("history broker_order_id cannot change")
            if update.broker_sequence <= previous_sequence:
                raise ValueError("history broker sequence must increase")
            if update.cumulative_filled_quantity < previous_filled:
                raise ValueError("history cumulative fill cannot decrease")
            if update.cumulative_filled_quantity > self.order.quantity:
                raise ValueError("history cumulative fill exceeds order quantity")
            if update.occurred_at < previous_time:
                raise ValueError("history update time cannot move backwards")
            previous_sequence = update.broker_sequence
            previous_filled = update.cumulative_filled_quantity
            previous_time = update.occurred_at


def order_payload(order: ApprovedPaperOrder) -> dict[str, object]:
    return {
        "account_id": order.account_id,
        "approved_at": order.approved_at.isoformat(timespec="microseconds"),
        "client_order_id": order.client_order_id,
        "instrument": order.instrument,
        "limit_price": (
            None if order.limit_price is None else _decimal_text(order.limit_price)
        ),
        "quantity": order.quantity,
        "risk_decision_hash": order.risk_decision_hash,
        "side": order.side.value,
    }


def update_payload(update: BrokerOrderUpdate) -> dict[str, object]:
    return {
        "account_id": update.account_id,
        "average_fill_price": (
            None
            if update.average_fill_price is None
            else _decimal_text(update.average_fill_price)
        ),
        "broker_order_id": update.broker_order_id,
        "broker_sequence": update.broker_sequence,
        "client_order_id": update.client_order_id,
        "cumulative_filled_quantity": update.cumulative_filled_quantity,
        "occurred_at": update.occurred_at.isoformat(timespec="microseconds"),
        "rejection_code": update.rejection_code,
        "state": update.state.value,
    }


def projection_payload(projection: PaperOrderProjection) -> dict[str, object]:
    return {
        "average_fill_price": (
            None
            if projection.average_fill_price is None
            else _decimal_text(projection.average_fill_price)
        ),
        "broker_order_id": projection.broker_order_id,
        "cumulative_filled_quantity": projection.cumulative_filled_quantity,
        "last_broker_sequence": projection.last_broker_sequence,
        "last_event_hash": projection.last_event_hash,
        "last_update_hash": projection.last_update_hash,
        "order_hash": projection.order.order_hash,
        "state": projection.state.value,
        "updated_at": projection.updated_at.isoformat(timespec="microseconds"),
        "version": projection.version,
    }

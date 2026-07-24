from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Final

from autoquant.backtest.models import OrderSide
from autoquant.clock import to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.models import ZERO_HASH, PaperOrderState
from autoquant.execution.qmt_callback_inbox import (
    QmtCallbackInboxEvent,
    QmtCallbackPersistenceReceipt,
)
from autoquant.execution.qmt_gateway import QmtCallbackKind
from autoquant.execution.qmt_models import from_qmt_instrument, map_qmt_order_state

QMT_CALLBACK_PROCESSING_VERSION: Final = "qmt-callback-processing-event-v1"
QMT_BROKER_ORDER_PROJECTION_VERSION: Final = "qmt-broker-order-projection-v1"
QMT_BROKER_TRADE_FACT_VERSION: Final = "qmt-broker-trade-fact-v1"
MONEY_TOLERANCE: Final = Decimal("0.01")


class QmtCallbackDisposition(StrEnum):
    OBSERVED = "observed"
    ASYNC_BOUND = "async_bound"
    ORDER_APPLIED = "order_applied"
    TRADE_APPLIED = "trade_applied"
    DUPLICATE_TRADE = "duplicate_trade"
    PENDING_RECONCILIATION = "pending_reconciliation"
    BROKER_STATE_UNKNOWN = "broker_state_unknown"


class QmtOrderConvergence(StrEnum):
    PENDING = "pending"
    CONVERGED = "converged"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class QmtBrokerTradeFact:
    account_id: str
    candidate_hash: str
    client_order_id: str
    broker_order_id: str
    trade_id: str
    instrument: str
    side: OrderSide
    volume: int
    price: Decimal
    amount: Decimal
    order_remark: str
    callback_event_hash: str
    observed_at: datetime
    version: str = QMT_BROKER_TRADE_FACT_VERSION
    fact_hash: str = field(init=False)

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        for value, name in (
            (self.account_id, "QMT trade account_id"),
            (self.client_order_id, "QMT trade client_order_id"),
            (self.broker_order_id, "QMT trade broker_order_id"),
            (self.trade_id, "QMT trade trade_id"),
        ):
            _require_nonblank(value, name=name)
        _require_lowercase_sha256(self.candidate_hash, name="QMT candidate_hash")
        _require_lowercase_sha256(
            self.callback_event_hash,
            name="QMT callback_event_hash",
        )
        if not isinstance(self.side, OrderSide):
            raise TypeError("QMT trade side must be OrderSide")
        if not isinstance(self.volume, int) or isinstance(self.volume, bool) or self.volume < 1:
            raise ValueError("QMT trade volume must be positive")
        for money, money_name in ((self.price, "price"), (self.amount, "amount")):
            if not isinstance(money, Decimal) or not money.is_finite() or money <= 0:
                raise ValueError(f"QMT trade {money_name} must be positive and finite")
        if abs(self.price * self.volume - self.amount) > MONEY_TOLERANCE:
            raise ValueError("QMT trade amount does not reconcile to price times volume")
        if len(self.order_remark.encode("utf-8")) > 24:
            raise ValueError("QMT trade order_remark exceeds 24 bytes")
        observed_at = to_utc(self.observed_at, name="QMT trade observed_at")
        object.__setattr__(self, "observed_at", observed_at)
        if self.version != QMT_BROKER_TRADE_FACT_VERSION:
            raise ValueError("unsupported QMT broker trade fact version")
        object.__setattr__(self, "fact_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "amount": _decimal_text(self.amount),
            "broker_mutation_allowed": False,
            "broker_order_id": self.broker_order_id,
            "callback_event_hash": self.callback_event_hash,
            "candidate_hash": self.candidate_hash,
            "client_order_id": self.client_order_id,
            "instrument": self.instrument,
            "observed_at": _datetime_text(self.observed_at),
            "order_remark": self.order_remark,
            "price": _decimal_text(self.price),
            "side": self.side.value,
            "trade_id": self.trade_id,
            "version": self.version,
            "volume": self.volume,
        }


@dataclass(frozen=True, slots=True)
class QmtBrokerOrderProjection:
    account_id: str
    candidate_hash: str
    client_order_id: str
    broker_order_id: str
    instrument: str
    side: OrderSide
    quantity: int
    limit_price: Decimal
    order_remark: str
    reported_traded_volume: int | None
    reported_average_price: Decimal | None
    raw_order_status: int | None
    order_state: PaperOrderState
    trade_volume: int
    trade_amount: Decimal
    convergence: QmtOrderConvergence
    last_callback_sequence: int
    last_callback_event_hash: str
    updated_at: datetime
    version: str = QMT_BROKER_ORDER_PROJECTION_VERSION
    projection_hash: str = field(init=False)

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        for value, name in (
            (self.account_id, "QMT projection account_id"),
            (self.client_order_id, "QMT projection client_order_id"),
            (self.broker_order_id, "QMT projection broker_order_id"),
            (self.instrument, "QMT projection instrument"),
        ):
            _require_nonblank(value, name=name)
        _require_lowercase_sha256(self.candidate_hash, name="QMT candidate_hash")
        _require_lowercase_sha256(
            self.last_callback_event_hash,
            name="QMT last_callback_event_hash",
        )
        if not isinstance(self.side, OrderSide):
            raise TypeError("QMT projection side must be OrderSide")
        if (
            not isinstance(self.quantity, int)
            or isinstance(self.quantity, bool)
            or self.quantity < 1
        ):
            raise ValueError("QMT projection quantity must be positive")
        if (
            not isinstance(self.limit_price, Decimal)
            or not self.limit_price.is_finite()
            or self.limit_price <= 0
        ):
            raise ValueError("QMT projection limit_price must be positive and finite")
        if len(self.order_remark.encode("utf-8")) > 24:
            raise ValueError("QMT projection order_remark exceeds 24 bytes")
        if self.reported_traded_volume is not None and not (
            isinstance(self.reported_traded_volume, int)
            and not isinstance(self.reported_traded_volume, bool)
            and 0 <= self.reported_traded_volume <= self.quantity
        ):
            raise ValueError("QMT reported traded volume is outside order quantity")
        if self.reported_traded_volume in {None, 0}:
            if self.reported_average_price is not None:
                raise ValueError("QMT unreported or zero fills cannot have an average price")
        elif (
            self.reported_average_price is None
            or not self.reported_average_price.is_finite()
            or self.reported_average_price <= 0
        ):
            raise ValueError("QMT reported fills require a positive average price")
        if (
            not isinstance(self.trade_volume, int)
            or isinstance(self.trade_volume, bool)
            or not 0 <= self.trade_volume <= self.quantity
        ):
            raise ValueError("QMT aggregate trade volume is outside order quantity")
        if (
            not isinstance(self.trade_amount, Decimal)
            or not self.trade_amount.is_finite()
            or self.trade_amount < 0
            or (self.trade_volume == 0) != (self.trade_amount == 0)
        ):
            raise ValueError("QMT aggregate trade amount conflicts with trade volume")
        if not isinstance(self.order_state, PaperOrderState):
            raise TypeError("QMT order_state must be PaperOrderState")
        if not isinstance(self.convergence, QmtOrderConvergence):
            raise TypeError("QMT convergence must be QmtOrderConvergence")
        if (
            not isinstance(self.last_callback_sequence, int)
            or isinstance(self.last_callback_sequence, bool)
            or self.last_callback_sequence < 1
        ):
            raise ValueError("QMT callback sequence must be positive")
        updated_at = to_utc(self.updated_at, name="QMT projection updated_at")
        object.__setattr__(self, "updated_at", updated_at)
        if self.version != QMT_BROKER_ORDER_PROJECTION_VERSION:
            raise ValueError("unsupported QMT broker order projection version")
        object.__setattr__(self, "projection_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "broker_mutation_allowed": False,
            "broker_order_id": self.broker_order_id,
            "candidate_hash": self.candidate_hash,
            "client_order_id": self.client_order_id,
            "convergence": self.convergence.value,
            "instrument": self.instrument,
            "last_callback_event_hash": self.last_callback_event_hash,
            "last_callback_sequence": self.last_callback_sequence,
            "limit_price": _decimal_text(self.limit_price),
            "order_remark": self.order_remark,
            "order_state": self.order_state.value,
            "quantity": self.quantity,
            "raw_order_status": self.raw_order_status,
            "reported_average_price": (
                None
                if self.reported_average_price is None
                else _decimal_text(self.reported_average_price)
            ),
            "reported_traded_volume": self.reported_traded_volume,
            "side": self.side.value,
            "trade_amount": _decimal_text(self.trade_amount),
            "trade_volume": self.trade_volume,
            "updated_at": _datetime_text(self.updated_at),
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class QmtCallbackProcessingRecord:
    event: QmtCallbackInboxEvent
    receipt: QmtCallbackPersistenceReceipt
    disposition: QmtCallbackDisposition
    reason: str
    previous_hash: str
    candidate_hash: str | None = None
    client_order_id: str | None = None
    broker_order_id: str | None = None
    projection_hash: str | None = None
    version: str = QMT_CALLBACK_PROCESSING_VERSION
    processing_hash: str = field(init=False)

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        if self.receipt.event != self.event:
            raise ValueError("QMT processing receipt belongs to another callback")
        if not isinstance(self.disposition, QmtCallbackDisposition):
            raise TypeError("QMT callback disposition is invalid")
        if (
            not isinstance(self.reason, str)
            or not self.reason
            or not self.reason.replace("_", "").isalnum()
            or len(self.reason) > 64
        ):
            raise ValueError("QMT callback processing reason must be a safe identifier")
        _require_lowercase_sha256(self.previous_hash, name="QMT processing previous_hash")
        for value, name in (
            (self.candidate_hash, "QMT processing candidate_hash"),
            (self.projection_hash, "QMT processing projection_hash"),
        ):
            if value is not None:
                _require_lowercase_sha256(value, name=name)
        if self.candidate_hash is None and any(
            item is not None
            for item in (
                self.client_order_id,
                self.broker_order_id,
                self.projection_hash,
            )
        ):
            raise ValueError("QMT processing order identity must be complete")
        if self.version != QMT_CALLBACK_PROCESSING_VERSION:
            raise ValueError("unsupported QMT callback processing version")
        object.__setattr__(self, "processing_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.event.callback.account_id,
            "broker_mutation_allowed": False,
            "broker_order_id": self.broker_order_id,
            "callback_event_hash": self.event.event_hash,
            "callback_receipt_hash": self.receipt.receipt_hash,
            "candidate_hash": self.candidate_hash,
            "client_order_id": self.client_order_id,
            "disposition": self.disposition.value,
            "local_sequence": self.event.callback.local_sequence,
            "previous_hash": self.previous_hash,
            "projection_hash": self.projection_hash,
            "reason": self.reason,
            "version": self.version,
        }


def qmt_trade_fact_from_callback(
    event: QmtCallbackInboxEvent,
    *,
    candidate_hash: str,
    client_order_id: str,
) -> QmtBrokerTradeFact:
    if event.callback.kind is not QmtCallbackKind.TRADE:
        raise TypeError("QMT trade fact requires a trade callback")
    payload = event.callback.redacted_payload
    return QmtBrokerTradeFact(
        account_id=event.callback.account_id,
        candidate_hash=candidate_hash,
        client_order_id=client_order_id,
        broker_order_id=str(payload["order_id"]),
        trade_id=str(payload["traded_id"]),
        instrument=from_qmt_instrument(str(payload["stock_code"])),
        side=OrderSide(str(payload["side"])),
        volume=_callback_int(payload["traded_volume"], name="traded_volume"),
        price=Decimal(str(payload["traded_price"])),
        amount=Decimal(str(payload["traded_amount"])),
        order_remark=str(payload["order_remark"]),
        callback_event_hash=event.event_hash,
        observed_at=event.callback.received_at,
    )


def initial_qmt_order_projection(
    *,
    event: QmtCallbackInboxEvent,
    candidate_hash: str,
    client_order_id: str,
    broker_order_id: str,
    instrument: str,
    side: OrderSide,
    quantity: int,
    limit_price: Decimal,
    order_remark: str,
) -> QmtBrokerOrderProjection:
    return QmtBrokerOrderProjection(
        account_id=event.callback.account_id,
        candidate_hash=candidate_hash,
        client_order_id=client_order_id,
        broker_order_id=broker_order_id,
        instrument=instrument,
        side=side,
        quantity=quantity,
        limit_price=limit_price,
        order_remark=order_remark,
        reported_traded_volume=None,
        reported_average_price=None,
        raw_order_status=None,
        order_state=PaperOrderState.UNKNOWN,
        trade_volume=0,
        trade_amount=Decimal("0"),
        convergence=QmtOrderConvergence.PENDING,
        last_callback_sequence=event.callback.local_sequence,
        last_callback_event_hash=event.event_hash,
        updated_at=event.callback.received_at,
    )


def apply_qmt_order_callback(
    projection: QmtBrokerOrderProjection,
    event: QmtCallbackInboxEvent,
) -> QmtBrokerOrderProjection:
    if event.callback.kind is not QmtCallbackKind.ORDER:
        raise TypeError("QMT order reducer requires an order callback")
    payload = event.callback.redacted_payload
    _require_matching_callback_identity(projection, event)
    if projection.convergence is QmtOrderConvergence.UNKNOWN:
        raise ValueError("QMT unknown order projection cannot recover in-place")
    reported_volume = _callback_int(
        payload["traded_volume"],
        name="traded_volume",
    )
    if (
        projection.reported_traded_volume is not None
        and reported_volume < projection.reported_traded_volume
    ):
        raise ValueError("QMT reported traded volume regressed")
    average = None if reported_volume == 0 else Decimal(str(payload["traded_price"]))
    state = map_qmt_order_state(
        _callback_int(payload["order_status"], name="order_status"),
        order_volume=_callback_int(payload["order_volume"], name="order_volume"),
        traded_volume=reported_volume,
    )
    if (
        projection.order_state
        in {
            PaperOrderState.FILLED,
            PaperOrderState.CANCELLED,
            PaperOrderState.REJECTED,
        }
        and state is not projection.order_state
    ):
        raise ValueError("QMT terminal order state cannot change")
    convergence = _convergence(
        quantity=projection.quantity,
        state=state,
        reported_volume=reported_volume,
        reported_average=average,
        trade_volume=projection.trade_volume,
        trade_amount=projection.trade_amount,
    )
    return QmtBrokerOrderProjection(
        account_id=projection.account_id,
        candidate_hash=projection.candidate_hash,
        client_order_id=projection.client_order_id,
        broker_order_id=projection.broker_order_id,
        instrument=projection.instrument,
        side=projection.side,
        quantity=projection.quantity,
        limit_price=projection.limit_price,
        order_remark=projection.order_remark,
        reported_traded_volume=reported_volume,
        reported_average_price=average,
        raw_order_status=_callback_int(payload["order_status"], name="order_status"),
        order_state=state,
        trade_volume=projection.trade_volume,
        trade_amount=projection.trade_amount,
        convergence=convergence,
        last_callback_sequence=event.callback.local_sequence,
        last_callback_event_hash=event.event_hash,
        updated_at=event.callback.received_at,
    )


def apply_qmt_trade_fact(
    projection: QmtBrokerOrderProjection,
    fact: QmtBrokerTradeFact,
    event: QmtCallbackInboxEvent,
) -> QmtBrokerOrderProjection:
    if event.callback.kind is not QmtCallbackKind.TRADE:
        raise TypeError("QMT trade reducer requires a trade callback")
    _require_matching_callback_identity(projection, event)
    if projection.convergence is QmtOrderConvergence.UNKNOWN:
        raise ValueError("QMT unknown order projection cannot recover in-place")
    if (
        fact.candidate_hash != projection.candidate_hash
        or fact.client_order_id != projection.client_order_id
        or fact.broker_order_id != projection.broker_order_id
    ):
        raise ValueError("QMT trade fact belongs to another order")
    trade_volume = projection.trade_volume + fact.volume
    trade_amount = projection.trade_amount + fact.amount
    if trade_volume > projection.quantity:
        raise ValueError("QMT aggregate trades exceed order quantity")
    convergence = _convergence(
        quantity=projection.quantity,
        state=projection.order_state,
        reported_volume=projection.reported_traded_volume,
        reported_average=projection.reported_average_price,
        trade_volume=trade_volume,
        trade_amount=trade_amount,
    )
    return QmtBrokerOrderProjection(
        account_id=projection.account_id,
        candidate_hash=projection.candidate_hash,
        client_order_id=projection.client_order_id,
        broker_order_id=projection.broker_order_id,
        instrument=projection.instrument,
        side=projection.side,
        quantity=projection.quantity,
        limit_price=projection.limit_price,
        order_remark=projection.order_remark,
        reported_traded_volume=projection.reported_traded_volume,
        reported_average_price=projection.reported_average_price,
        raw_order_status=projection.raw_order_status,
        order_state=projection.order_state,
        trade_volume=trade_volume,
        trade_amount=trade_amount,
        convergence=convergence,
        last_callback_sequence=event.callback.local_sequence,
        last_callback_event_hash=event.event_hash,
        updated_at=event.callback.received_at,
    )


def unknown_qmt_order_projection(
    projection: QmtBrokerOrderProjection,
    event: QmtCallbackInboxEvent,
) -> QmtBrokerOrderProjection:
    return QmtBrokerOrderProjection(
        account_id=projection.account_id,
        candidate_hash=projection.candidate_hash,
        client_order_id=projection.client_order_id,
        broker_order_id=projection.broker_order_id,
        instrument=projection.instrument,
        side=projection.side,
        quantity=projection.quantity,
        limit_price=projection.limit_price,
        order_remark=projection.order_remark,
        reported_traded_volume=projection.reported_traded_volume,
        reported_average_price=projection.reported_average_price,
        raw_order_status=projection.raw_order_status,
        order_state=PaperOrderState.UNKNOWN,
        trade_volume=projection.trade_volume,
        trade_amount=projection.trade_amount,
        convergence=QmtOrderConvergence.UNKNOWN,
        last_callback_sequence=event.callback.local_sequence,
        last_callback_event_hash=event.event_hash,
        updated_at=event.callback.received_at,
    )


def _require_matching_callback_identity(
    projection: QmtBrokerOrderProjection,
    event: QmtCallbackInboxEvent,
) -> None:
    payload = event.callback.redacted_payload
    if (
        event.callback.account_id != projection.account_id
        or str(payload["order_id"]) != projection.broker_order_id
        or from_qmt_instrument(str(payload["stock_code"])) != projection.instrument
        or OrderSide(str(payload["side"])) is not projection.side
        or str(payload["order_remark"]) != projection.order_remark
    ):
        raise ValueError("QMT callback identity conflicts with its staged order")
    if event.callback.kind is QmtCallbackKind.ORDER and (
        _callback_int(payload["order_volume"], name="order_volume") != projection.quantity
        or Decimal(str(payload["price"])) != projection.limit_price
    ):
        raise ValueError("QMT order callback terms conflict with its staged order")


def _convergence(
    *,
    quantity: int,
    state: PaperOrderState,
    reported_volume: int | None,
    reported_average: Decimal | None,
    trade_volume: int,
    trade_amount: Decimal,
) -> QmtOrderConvergence:
    if reported_volume is None:
        return QmtOrderConvergence.PENDING
    if state is PaperOrderState.UNKNOWN:
        return QmtOrderConvergence.UNKNOWN
    if reported_volume != trade_volume:
        return QmtOrderConvergence.PENDING
    if trade_volume == 0:
        return QmtOrderConvergence.CONVERGED
    assert reported_average is not None
    if abs(reported_average * trade_volume - trade_amount) > MONEY_TOLERANCE:
        return QmtOrderConvergence.UNKNOWN
    if state is PaperOrderState.FILLED and trade_volume != quantity:
        return QmtOrderConvergence.UNKNOWN
    return QmtOrderConvergence.CONVERGED


def _callback_int(value: object, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"QMT callback {name} must be an integer")
    return value


__all__ = [
    "QMT_BROKER_ORDER_PROJECTION_VERSION",
    "QMT_BROKER_TRADE_FACT_VERSION",
    "QMT_CALLBACK_PROCESSING_VERSION",
    "ZERO_HASH",
    "QmtBrokerOrderProjection",
    "QmtBrokerTradeFact",
    "QmtCallbackDisposition",
    "QmtCallbackProcessingRecord",
    "QmtOrderConvergence",
    "apply_qmt_order_callback",
    "apply_qmt_trade_fact",
    "initial_qmt_order_projection",
    "qmt_trade_fact_from_callback",
    "unknown_qmt_order_projection",
]

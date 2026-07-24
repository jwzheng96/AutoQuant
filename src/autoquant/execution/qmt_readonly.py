from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from autoquant.backtest.models import OrderSide
from autoquant.clock import to_utc
from autoquant.data.models import _canonical_hash, _decimal_text, _require_nonblank
from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.models import PaperOrderState
from autoquant.execution.qmt_gateway import (
    QmtCallbackEnvelope,
    QmtCallbackKind,
    QmtCallbackValue,
)
from autoquant.execution.qmt_models import (
    QmtAssetSnapshot,
    QmtOrderObservation,
    QmtPositionSnapshot,
    from_qmt_instrument,
    require_qmt_query_records,
)
from autoquant.execution.reconciliation import (
    AccountPosition,
    ExecutionAccountSnapshot,
)

QmtRawPayload = Mapping[str, QmtCallbackValue]
_MONEY_TOLERANCE = Decimal("0.01")
_TERMINAL_STATES = {
    PaperOrderState.FILLED,
    PaperOrderState.CANCELLED,
    PaperOrderState.REJECTED,
}


def _raw(payload: QmtRawPayload, name: str) -> QmtCallbackValue:
    try:
        return payload[name]
    except KeyError:
        raise ValueError(f"QMT payload omitted required field: {name}") from None


def _text(value: object, *, name: str, allow_blank: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError(f"QMT {name} must be a string")
    normalized = value.strip()
    if not allow_blank and not normalized:
        raise ValueError(f"QMT {name} cannot be blank")
    return normalized


def _integer(
    value: object,
    *,
    name: str,
    minimum: int = 0,
) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
    ):
        raise ValueError(f"QMT {name} must be an integer of at least {minimum}")
    return value


def _decimal(
    value: object,
    *,
    name: str,
    positive: bool = False,
) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"QMT {name} must be numeric")
    try:
        converted = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"QMT {name} must be numeric") from None
    if not converted.is_finite() or converted < 0 or (positive and converted == 0):
        qualifier = "positive" if positive else "nonnegative"
        raise ValueError(f"QMT {name} must be finite and {qualifier}")
    return converted


def _account_id(
    payload: QmtRawPayload,
    *,
    expected_account_id: str,
) -> str:
    _require_nonblank(expected_account_id, name="expected_account_id")
    supplied = _text(_raw(payload, "account_id"), name="account_id")
    if supplied != expected_account_id:
        raise BrokerStateUnknownError("QMT query returned another account")
    return supplied


def _side(value: object) -> OrderSide:
    normalized = _text(value, name="side").casefold()
    try:
        return OrderSide(normalized)
    except ValueError:
        raise ValueError("QMT Windows shim side must be buy or sell") from None


def normalize_qmt_asset(
    payload: QmtRawPayload,
    *,
    expected_account_id: str,
    observed_at: datetime,
) -> QmtAssetSnapshot:
    return QmtAssetSnapshot(
        account_id=_account_id(payload, expected_account_id=expected_account_id),
        cash=_decimal(_raw(payload, "cash"), name="cash"),
        frozen_cash=_decimal(_raw(payload, "frozen_cash"), name="frozen_cash"),
        market_value=_decimal(_raw(payload, "market_value"), name="market_value"),
        total_asset=_decimal(_raw(payload, "total_asset"), name="total_asset"),
        observed_at=observed_at,
    )


def normalize_qmt_position(
    payload: QmtRawPayload,
    *,
    expected_account_id: str,
    observed_at: datetime,
) -> QmtPositionSnapshot:
    return QmtPositionSnapshot(
        account_id=_account_id(payload, expected_account_id=expected_account_id),
        instrument=from_qmt_instrument(
            _text(_raw(payload, "stock_code"), name="stock_code")
        ),
        total_volume=_integer(_raw(payload, "volume"), name="volume"),
        available_volume=_integer(
            _raw(payload, "can_use_volume"),
            name="can_use_volume",
        ),
        frozen_volume=_integer(
            _raw(payload, "frozen_volume"),
            name="frozen_volume",
        ),
        average_price=_decimal(_raw(payload, "avg_price"), name="avg_price"),
        market_value=_decimal(
            _raw(payload, "market_value"),
            name="market_value",
        ),
        observed_at=observed_at,
    )


def normalize_qmt_order(
    payload: QmtRawPayload,
    *,
    expected_account_id: str,
    observed_at: datetime,
    client_order_ids: Mapping[int, str] | None = None,
    require_client_order_mapping: bool = False,
) -> QmtOrderObservation:
    if type(require_client_order_mapping) is not bool:
        raise TypeError("require_client_order_mapping must be a bool")
    order_id = _integer(_raw(payload, "order_id"), name="order_id", minimum=1)
    order_volume = _integer(
        _raw(payload, "order_volume"),
        name="order_volume",
        minimum=1,
    )
    traded_volume = _integer(
        _raw(payload, "traded_volume"),
        name="traded_volume",
    )
    raw_average = _raw(payload, "traded_price")
    average = (
        None
        if traded_volume == 0
        else _decimal(raw_average, name="traded_price", positive=True)
    )
    if traded_volume == 0 and _decimal(raw_average, name="traded_price") != 0:
        raise ValueError("QMT unfilled order must have zero traded_price")
    mapped = None if client_order_ids is None else client_order_ids.get(order_id)
    if mapped is None and require_client_order_mapping:
        raise BrokerStateUnknownError(
            "QMT order has no trusted client_order_id correlation"
        )
    client_order_id = (
        f"qmt-unmapped-{order_id}"
        if mapped is None
        else _text(mapped, name="mapped client_order_id")
    )
    status_message = _text(
        _raw(payload, "status_msg"),
        name="status_msg",
        allow_blank=True,
    )
    order_remark = _text(
        _raw(payload, "order_remark"),
        name="order_remark",
        allow_blank=True,
    )
    return QmtOrderObservation(
        account_id=_account_id(payload, expected_account_id=expected_account_id),
        client_order_id=client_order_id,
        broker_order_id=str(order_id),
        instrument=from_qmt_instrument(
            _text(_raw(payload, "stock_code"), name="stock_code")
        ),
        side=_side(_raw(payload, "side")),
        order_volume=order_volume,
        traded_volume=traded_volume,
        average_traded_price=average,
        raw_status=_integer(_raw(payload, "order_status"), name="order_status"),
        status_message=status_message or "qmt_no_status_message",
        observed_at=observed_at,
        order_remark=order_remark,
    )


@dataclass(frozen=True, slots=True)
class QmtTradeObservation:
    account_id: str
    trade_id: str
    broker_order_id: str
    instrument: str
    side: OrderSide
    price: Decimal
    volume: int
    amount: Decimal
    observed_at: datetime
    order_remark: str = ""
    trade_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("trade_id", self.trade_id),
            ("broker_order_id", self.broker_order_id),
            ("instrument", self.instrument),
        ):
            _require_nonblank(value, name=name)
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be OrderSide")
        if not isinstance(self.order_remark, str):
            raise TypeError("trade order_remark must be a string")
        order_remark = self.order_remark.strip()
        if len(order_remark.encode("utf-8")) > 24:
            raise ValueError("QMT trade order_remark must fit the documented 24-byte limit")
        if not isinstance(self.volume, int) or isinstance(self.volume, bool) or self.volume < 1:
            raise ValueError("trade volume must be a positive integer")
        for name, money in (("price", self.price), ("amount", self.amount)):
            if not isinstance(money, Decimal) or not money.is_finite() or money <= 0:
                raise ValueError(f"trade {name} must be a positive finite Decimal")
        if (self.price * self.volume - self.amount).copy_abs() > _MONEY_TOLERANCE:
            raise ValueError("QMT trade amount does not reconcile to price times volume")
        observed_at = to_utc(self.observed_at, name="QMT trade observed_at")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "order_remark", order_remark)
        object.__setattr__(
            self,
            "trade_hash",
            _canonical_hash(
                {
                    "account_id": self.account_id,
                    "amount": _decimal_text(self.amount),
                    "broker_order_id": self.broker_order_id,
                    "instrument": self.instrument,
                    "observed_at": observed_at.isoformat(timespec="microseconds"),
                    "order_remark": order_remark,
                    "price": _decimal_text(self.price),
                    "side": self.side.value,
                    "trade_id": self.trade_id,
                    "volume": self.volume,
                }
            ),
        )


def normalize_qmt_trade(
    payload: QmtRawPayload,
    *,
    expected_account_id: str,
    observed_at: datetime,
) -> QmtTradeObservation:
    return QmtTradeObservation(
        account_id=_account_id(payload, expected_account_id=expected_account_id),
        trade_id=_text(_raw(payload, "traded_id"), name="traded_id"),
        broker_order_id=str(
            _integer(_raw(payload, "order_id"), name="order_id", minimum=1)
        ),
        instrument=from_qmt_instrument(
            _text(_raw(payload, "stock_code"), name="stock_code")
        ),
        side=_side(_raw(payload, "side")),
        price=_decimal(
            _raw(payload, "traded_price"),
            name="traded_price",
            positive=True,
        ),
        volume=_integer(
            _raw(payload, "traded_volume"),
            name="traded_volume",
            minimum=1,
        ),
        amount=_decimal(
            _raw(payload, "traded_amount"),
            name="traded_amount",
            positive=True,
        ),
        observed_at=observed_at,
        order_remark=_text(
            _raw(payload, "order_remark"),
            name="order_remark",
            allow_blank=True,
        ),
    )


@dataclass(frozen=True, slots=True)
class QmtReadOnlyBaseline:
    baseline_id: str
    generation: int
    logical_account_id: str
    query_started_at: datetime
    query_completed_at: datetime
    callback_cursor: int
    asset: QmtAssetSnapshot
    positions: tuple[QmtPositionSnapshot, ...]
    orders: tuple[QmtOrderObservation, ...]
    trades: tuple[QmtTradeObservation, ...]
    evidence_hash: str = field(init=False)
    account_snapshot: ExecutionAccountSnapshot = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.baseline_id, name="QMT baseline_id")
        _require_nonblank(self.logical_account_id, name="logical_account_id")
        if (
            not isinstance(self.generation, int)
            or isinstance(self.generation, bool)
            or self.generation < 1
        ):
            raise ValueError("QMT baseline generation must be positive")
        if (
            not isinstance(self.callback_cursor, int)
            or isinstance(self.callback_cursor, bool)
            or self.callback_cursor < 0
        ):
            raise ValueError("QMT callback cursor must be nonnegative")
        started = to_utc(self.query_started_at, name="QMT query_started_at")
        completed = to_utc(self.query_completed_at, name="QMT query_completed_at")
        if completed < started:
            raise ValueError("QMT query completion cannot precede its start")
        object.__setattr__(self, "query_started_at", started)
        object.__setattr__(self, "query_completed_at", completed)
        positions = tuple(sorted(self.positions, key=lambda item: item.instrument))
        orders = tuple(sorted(self.orders, key=lambda item: item.broker_order_id))
        trades = tuple(sorted(self.trades, key=lambda item: item.trade_id))
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "orders", orders)
        object.__setattr__(self, "trades", trades)
        self._validate_identity_and_uniqueness(completed)
        self._validate_balance()
        self._validate_order_trade_convergence()
        evidence_hash = _canonical_hash(self._evidence_payload())
        object.__setattr__(self, "evidence_hash", evidence_hash)
        account_positions = tuple(
            AccountPosition(
                instrument=position.instrument,
                total_quantity=position.total_volume,
                sellable_quantity=position.available_volume,
                market_value=position.market_value,
            )
            for position in positions
            if position.total_volume > 0
        )
        open_orders = tuple(
            order.client_order_id
            for order in orders
            if order.state not in _TERMINAL_STATES
        )
        object.__setattr__(
            self,
            "account_snapshot",
            ExecutionAccountSnapshot(
                account_id=self.logical_account_id,
                as_of=completed,
                cash=self.asset.cash + self.asset.frozen_cash,
                equity=self.asset.total_asset,
                positions=account_positions,
                open_client_order_ids=open_orders,
                projection_version="qmt-readonly-query-v1",
                evidence_hash=evidence_hash,
            ),
        )

    @property
    def broker_account_id(self) -> str:
        return self.asset.account_id

    def _validate_identity_and_uniqueness(self, completed: datetime) -> None:
        if (
            any(item.account_id != self.broker_account_id for item in self.positions)
            or any(item.account_id != self.broker_account_id for item in self.orders)
            or any(item.account_id != self.broker_account_id for item in self.trades)
        ):
            raise BrokerStateUnknownError("QMT query records span multiple accounts")
        if (
            self.asset.observed_at != completed
            or any(item.observed_at != completed for item in self.positions)
            or any(item.observed_at != completed for item in self.orders)
            or any(item.observed_at != completed for item in self.trades)
        ):
            raise BrokerStateUnknownError("QMT query records have inconsistent observation times")
        if len({item.instrument for item in self.positions}) != len(self.positions):
            raise BrokerStateUnknownError("QMT positions contain duplicate instruments")
        if len({item.broker_order_id for item in self.orders}) != len(self.orders):
            raise BrokerStateUnknownError("QMT orders contain duplicate order identifiers")
        if len({item.client_order_id for item in self.orders}) != len(self.orders):
            raise BrokerStateUnknownError("QMT orders contain duplicate client identifiers")
        if len({item.trade_id for item in self.trades}) != len(self.trades):
            raise BrokerStateUnknownError("QMT trades contain duplicate trade identifiers")
        if any(item.state is PaperOrderState.UNKNOWN for item in self.orders):
            raise BrokerStateUnknownError("QMT order state is unknown or internally inconsistent")
        for position in self.positions:
            if position.total_volume == 0 and (
                position.available_volume != 0
                or position.frozen_volume != 0
                or position.market_value != 0
            ):
                raise BrokerStateUnknownError("QMT zero position contains nonzero account facts")
            if position.total_volume > 0 and (
                position.average_price <= 0 or position.market_value <= 0
            ):
                raise BrokerStateUnknownError(
                    "QMT nonzero position requires positive cost and market value"
                )

    def _validate_balance(self) -> None:
        position_value = sum(
            (
                position.market_value
                for position in self.positions
                if position.total_volume > 0
            ),
            Decimal("0"),
        )
        if (position_value - self.asset.market_value).copy_abs() > _MONEY_TOLERANCE:
            raise BrokerStateUnknownError("QMT asset and position market values do not reconcile")
        expected_total = self.asset.cash + self.asset.frozen_cash + position_value
        if (expected_total - self.asset.total_asset).copy_abs() > _MONEY_TOLERANCE:
            raise BrokerStateUnknownError("QMT asset total does not reconcile")

    def _validate_order_trade_convergence(self) -> None:
        orders = {item.broker_order_id: item for item in self.orders}
        volumes: dict[str, int] = {}
        amounts: dict[str, Decimal] = {}
        for trade in self.trades:
            order = orders.get(trade.broker_order_id)
            if order is None:
                raise BrokerStateUnknownError("QMT trade has no matching daily order")
            if trade.instrument != order.instrument or trade.side is not order.side:
                raise BrokerStateUnknownError("QMT trade conflicts with its matching order")
            volumes[trade.broker_order_id] = volumes.get(trade.broker_order_id, 0) + trade.volume
            amounts[trade.broker_order_id] = (
                amounts.get(trade.broker_order_id, Decimal("0")) + trade.amount
            )
        for order in self.orders:
            volume = volumes.get(order.broker_order_id, 0)
            if volume != order.traded_volume:
                raise BrokerStateUnknownError(
                    "QMT order cumulative volume does not match daily trades"
                )
            if volume == 0:
                continue
            average = order.average_traded_price
            if average is None:
                raise BrokerStateUnknownError("QMT filled order omitted its average price")
            tolerance = _MONEY_TOLERANCE * volume
            if (
                average * volume - amounts[order.broker_order_id]
            ).copy_abs() > tolerance:
                raise BrokerStateUnknownError(
                    "QMT order average price does not match daily trades"
                )

    def _evidence_payload(self) -> dict[str, object]:
        return {
            "asset": {
                "account_id": self.asset.account_id,
                "cash": _decimal_text(self.asset.cash),
                "frozen_cash": _decimal_text(self.asset.frozen_cash),
                "market_value": _decimal_text(self.asset.market_value),
                "total_asset": _decimal_text(self.asset.total_asset),
            },
            "baseline_id": self.baseline_id,
            "callback_cursor": self.callback_cursor,
            "generation": self.generation,
            "logical_account_id": self.logical_account_id,
            "orders": [
                {
                    "average_traded_price": (
                        None
                        if item.average_traded_price is None
                        else _decimal_text(item.average_traded_price)
                    ),
                    "broker_order_id": item.broker_order_id,
                    "client_order_id": item.client_order_id,
                    "instrument": item.instrument,
                    "order_volume": item.order_volume,
                    "order_remark": item.order_remark,
                    "raw_status": item.raw_status,
                    "side": item.side.value,
                    "state": item.state.value,
                    "traded_volume": item.traded_volume,
                }
                for item in self.orders
            ],
            "positions": [
                {
                    "available_volume": item.available_volume,
                    "frozen_volume": item.frozen_volume,
                    "instrument": item.instrument,
                    "market_value": _decimal_text(item.market_value),
                    "total_volume": item.total_volume,
                }
                for item in self.positions
            ],
            "query_completed_at": self.query_completed_at.isoformat(
                timespec="microseconds"
            ),
            "query_started_at": self.query_started_at.isoformat(
                timespec="microseconds"
            ),
            "trades": [item.trade_hash for item in self.trades],
        }


def build_qmt_readonly_baseline(
    *,
    baseline_id: str,
    generation: int,
    logical_account_id: str,
    query_started_at: datetime,
    query_completed_at: datetime,
    callback_cursor_before: int,
    callback_cursor_after: int,
    callback_stream_healthy: bool,
    asset: QmtAssetSnapshot | None,
    positions: Iterable[QmtPositionSnapshot] | None,
    orders: Iterable[QmtOrderObservation] | None,
    trades: Iterable[QmtTradeObservation] | None,
) -> QmtReadOnlyBaseline:
    if asset is None:
        raise BrokerStateUnknownError("QMT asset query returned None")
    if callback_stream_healthy is not True:
        raise BrokerStateUnknownError("QMT callback stream is unavailable")
    if callback_cursor_before != callback_cursor_after:
        raise BrokerStateUnknownError("QMT callback arrived while baseline queries were running")
    return QmtReadOnlyBaseline(
        baseline_id=baseline_id,
        generation=generation,
        logical_account_id=logical_account_id,
        query_started_at=query_started_at,
        query_completed_at=query_completed_at,
        callback_cursor=callback_cursor_after,
        asset=asset,
        positions=require_qmt_query_records(positions, query_name="positions"),
        orders=require_qmt_query_records(orders, query_name="orders"),
        trades=require_qmt_query_records(trades, query_name="trades"),
    )


class QmtReadOnlyRecoveryCode(StrEnum):
    NOT_INITIALIZED = "not_initialized"
    DISCONNECTED = "disconnected"
    ACCOUNT_UNAVAILABLE = "account_unavailable"
    CALLBACK_GAP = "callback_gap"
    FULL_REFRESH_REQUIRED = "full_refresh_required"


class QmtReadOnlyRecoveryState:
    """Fence query baselines against callbacks; any state change requires full re-query."""

    def __init__(self) -> None:
        self._baseline: QmtReadOnlyBaseline | None = None
        self._cursor = 0
        self._generation = 0
        self._code = QmtReadOnlyRecoveryCode.NOT_INITIALIZED

    @property
    def callback_cursor(self) -> int:
        return self._cursor

    @property
    def trusted(self) -> bool:
        return self._baseline is not None

    def install(self, baseline: QmtReadOnlyBaseline) -> None:
        if not isinstance(baseline, QmtReadOnlyBaseline):
            raise TypeError("baseline must be QmtReadOnlyBaseline")
        if baseline.generation <= self._generation:
            raise BrokerStateUnknownError("QMT baseline generation did not advance")
        if baseline.callback_cursor < self._cursor:
            raise BrokerStateUnknownError("QMT baseline callback cursor moved backwards")
        self._baseline = baseline
        self._cursor = baseline.callback_cursor
        self._generation = baseline.generation

    def snapshot(self) -> QmtReadOnlyBaseline:
        if self._baseline is None:
            raise BrokerStateUnknownError(
                f"QMT read-only snapshot requires a full refresh: {self._code.value}"
            )
        return self._baseline

    def apply(self, events: tuple[QmtCallbackEnvelope, ...]) -> None:
        for event in events:
            if event.local_sequence != self._cursor + 1:
                self._invalidate(QmtReadOnlyRecoveryCode.CALLBACK_GAP)
                raise BrokerStateUnknownError("QMT callback sequence gap detected")
            self._cursor = event.local_sequence
            if event.kind is QmtCallbackKind.DISCONNECTED:
                self._invalidate(QmtReadOnlyRecoveryCode.DISCONNECTED)
                raise BrokerStateUnknownError("QMT disconnected")
            if event.kind is QmtCallbackKind.ACCOUNT_STATUS:
                self._apply_account_status(event.payload)
                continue
            self._invalidate(QmtReadOnlyRecoveryCode.FULL_REFRESH_REQUIRED)
            raise BrokerStateUnknownError(
                "QMT account facts changed and require a full query refresh"
            )

    def _apply_account_status(self, payload: Mapping[str, QmtCallbackValue]) -> None:
        baseline = self._baseline
        if baseline is None:
            self._invalidate(QmtReadOnlyRecoveryCode.NOT_INITIALIZED)
            raise BrokerStateUnknownError("QMT account status arrived before a baseline")
        account_id = _text(_raw(payload, "account_id"), name="account_id")
        status = _integer(_raw(payload, "status"), name="status")
        if account_id != baseline.broker_account_id or status != 0:
            self._invalidate(QmtReadOnlyRecoveryCode.ACCOUNT_UNAVAILABLE)
            raise BrokerStateUnknownError("QMT account is not in the normal state")

    def _invalidate(self, code: QmtReadOnlyRecoveryCode) -> None:
        self._baseline = None
        self._code = code


class QmtReadOnlyBrokerReader:
    """Expose only trusted QMT baselines to the generic reconciliation supervisor."""

    def __init__(
        self,
        *,
        recovery: QmtReadOnlyRecoveryState,
        logical_account_id: str,
    ) -> None:
        _require_nonblank(logical_account_id, name="logical_account_id")
        self._recovery = recovery
        self._logical_account_id = logical_account_id

    async def __call__(
        self,
        account_id: str,
        now: datetime,
    ) -> ExecutionAccountSnapshot:
        instant = to_utc(now, name="QMT broker snapshot read time")
        if account_id != self._logical_account_id:
            raise BrokerStateUnknownError("QMT reader was asked for another logical account")
        baseline = self._recovery.snapshot()
        snapshot = baseline.account_snapshot
        if snapshot.account_id != account_id or snapshot.as_of > instant:
            raise BrokerStateUnknownError("QMT baseline identity or time is inconsistent")
        return snapshot

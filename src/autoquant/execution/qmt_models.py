from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import IntEnum
from typing import TypeVar

from autoquant.backtest.models import OrderSide
from autoquant.clock import to_utc
from autoquant.data.models import _require_nonblank
from autoquant.errors import BrokerStateUnknownError
from autoquant.execution.models import BrokerOrderUpdate, PaperOrderState


class QmtOrderStatus(IntEnum):
    """XtQuant order status values documented by the native trading API."""

    UNREPORTED = 48
    WAIT_REPORTING = 49
    REPORTED = 50
    REPORTED_CANCEL_PENDING = 51
    PARTIALLY_FILLED_CANCEL_PENDING = 52
    PARTIALLY_CANCELLED = 53
    CANCELLED = 54
    PARTIALLY_FILLED = 55
    FILLED = 56
    REJECTED = 57
    UNKNOWN = 255


def to_qmt_instrument(instrument: str) -> str:
    """Translate the internal A-share code to XtQuant without guessing exchanges."""

    _require_nonblank(instrument, name="instrument")
    if instrument.endswith(".XSHG"):
        code = instrument.removesuffix(".XSHG")
        suffix = ".SH"
    elif instrument.endswith(".XSHE"):
        code = instrument.removesuffix(".XSHE")
        suffix = ".SZ"
    else:
        raise ValueError(f"unsupported internal instrument: {instrument}")
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise ValueError(f"invalid A-share instrument: {instrument}")
    return f"{code}{suffix}"


def from_qmt_instrument(instrument: str) -> str:
    """Translate an XtQuant Shanghai/Shenzhen code to the internal code system."""

    _require_nonblank(instrument, name="instrument")
    normalized = instrument.upper()
    if normalized.endswith(".SH"):
        code = normalized.removesuffix(".SH")
        suffix = ".XSHG"
    elif normalized.endswith(".SZ"):
        code = normalized.removesuffix(".SZ")
        suffix = ".XSHE"
    else:
        raise ValueError(f"unsupported QMT instrument: {instrument}")
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise ValueError(f"invalid QMT instrument: {instrument}")
    return f"{code}{suffix}"


def _finite_nonnegative(value: Decimal, *, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be a finite nonnegative Decimal")


@dataclass(frozen=True, slots=True)
class QmtAssetSnapshot:
    account_id: str
    cash: Decimal
    frozen_cash: Decimal
    market_value: Decimal
    total_asset: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        for name, value in (
            ("cash", self.cash),
            ("frozen_cash", self.frozen_cash),
            ("market_value", self.market_value),
            ("total_asset", self.total_asset),
        ):
            _finite_nonnegative(value, name=name)
        object.__setattr__(self, "observed_at", to_utc(self.observed_at, name="observed_at"))


@dataclass(frozen=True, slots=True)
class QmtPositionSnapshot:
    account_id: str
    instrument: str
    total_volume: int
    available_volume: int
    frozen_volume: int
    average_price: Decimal
    market_value: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="account_id")
        to_qmt_instrument(self.instrument)
        for name, value in (
            ("total_volume", self.total_volume),
            ("available_volume", self.available_volume),
            ("frozen_volume", self.frozen_volume),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.available_volume + self.frozen_volume > self.total_volume:
            raise ValueError("available and frozen position exceed total volume")
        _finite_nonnegative(self.average_price, name="average_price")
        _finite_nonnegative(self.market_value, name="market_value")
        object.__setattr__(self, "observed_at", to_utc(self.observed_at, name="observed_at"))


def map_qmt_order_state(
    raw_status: int,
    *,
    order_volume: int,
    traded_volume: int,
) -> PaperOrderState:
    """Map QMT status conservatively; inconsistent or unknown facts stay unknown."""

    if (
        not isinstance(order_volume, int)
        or isinstance(order_volume, bool)
        or order_volume <= 0
        or not isinstance(traded_volume, int)
        or isinstance(traded_volume, bool)
        or not 0 <= traded_volume <= order_volume
    ):
        return PaperOrderState.UNKNOWN
    try:
        status = QmtOrderStatus(raw_status)
    except ValueError:
        return PaperOrderState.UNKNOWN

    if status is QmtOrderStatus.UNKNOWN:
        return PaperOrderState.UNKNOWN
    if status is QmtOrderStatus.FILLED:
        return PaperOrderState.FILLED if traded_volume == order_volume else PaperOrderState.UNKNOWN
    if status in {
        QmtOrderStatus.PARTIALLY_FILLED,
        QmtOrderStatus.PARTIALLY_FILLED_CANCEL_PENDING,
    }:
        return (
            PaperOrderState.PARTIALLY_FILLED
            if 0 < traded_volume < order_volume
            else PaperOrderState.UNKNOWN
        )
    if status is QmtOrderStatus.PARTIALLY_CANCELLED:
        return (
            PaperOrderState.CANCELLED
            if 0 < traded_volume < order_volume
            else PaperOrderState.UNKNOWN
        )
    if status is QmtOrderStatus.CANCELLED:
        return PaperOrderState.CANCELLED if traded_volume == 0 else PaperOrderState.UNKNOWN
    if status is QmtOrderStatus.REJECTED:
        return PaperOrderState.REJECTED if traded_volume == 0 else PaperOrderState.UNKNOWN
    if traded_volume != 0:
        return PaperOrderState.UNKNOWN
    return PaperOrderState.SUBMITTED


@dataclass(frozen=True, slots=True)
class QmtOrderObservation:
    account_id: str
    client_order_id: str
    broker_order_id: str
    instrument: str
    side: OrderSide
    order_volume: int
    traded_volume: int
    average_traded_price: Decimal | None
    raw_status: int
    status_message: str
    observed_at: datetime
    order_remark: str = ""

    def __post_init__(self) -> None:
        for name, value in (
            ("account_id", self.account_id),
            ("client_order_id", self.client_order_id),
            ("broker_order_id", self.broker_order_id),
            ("status_message", self.status_message),
        ):
            _require_nonblank(value, name=name)
        if not isinstance(self.order_remark, str):
            raise TypeError("order_remark must be a string")
        order_remark = self.order_remark.strip()
        if len(order_remark.encode("utf-8")) > 24:
            raise ValueError("QMT order_remark must fit the documented 24-byte limit")
        to_qmt_instrument(self.instrument)
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be OrderSide")
        if (
            not isinstance(self.order_volume, int)
            or isinstance(self.order_volume, bool)
            or self.order_volume <= 0
        ):
            raise ValueError("order_volume must be a positive integer")
        if (
            not isinstance(self.traded_volume, int)
            or isinstance(self.traded_volume, bool)
            or not 0 <= self.traded_volume <= self.order_volume
        ):
            raise ValueError("traded_volume must be within order_volume")
        if self.traded_volume == 0:
            if self.average_traded_price is not None:
                raise ValueError("an unfilled order cannot have an average traded price")
        else:
            if self.average_traded_price is None:
                raise ValueError("a filled quantity requires an average traded price")
            _finite_nonnegative(self.average_traded_price, name="average_traded_price")
            if self.average_traded_price == 0:
                raise ValueError("average_traded_price must be positive")
        object.__setattr__(self, "observed_at", to_utc(self.observed_at, name="observed_at"))
        object.__setattr__(self, "order_remark", order_remark)

    @property
    def state(self) -> PaperOrderState:
        return map_qmt_order_state(
            self.raw_status,
            order_volume=self.order_volume,
            traded_volume=self.traded_volume,
        )

    def to_broker_update(self, *, broker_sequence: int) -> BrokerOrderUpdate:
        rejection_code = "qmt_rejected" if self.state is PaperOrderState.REJECTED else None
        return BrokerOrderUpdate(
            account_id=self.account_id,
            client_order_id=self.client_order_id,
            broker_order_id=self.broker_order_id,
            broker_sequence=broker_sequence,
            state=self.state,
            cumulative_filled_quantity=self.traded_volume,
            average_fill_price=self.average_traded_price,
            occurred_at=self.observed_at,
            rejection_code=rejection_code,
        )


T = TypeVar("T")


def require_qmt_query_records(records: Iterable[T] | None, *, query_name: str) -> tuple[T, ...]:
    """Preserve QMT's documented failure/empty ambiguity by failing on ``None``."""

    _require_nonblank(query_name, name="query_name")
    if records is None:
        raise BrokerStateUnknownError(
            f"QMT {query_name} returned None; failure cannot be distinguished from empty"
        )
    return tuple(records)

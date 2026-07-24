from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from math import isfinite
from types import MappingProxyType
from typing import Final

from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.models import (
    _canonical_hash,
    _datetime_text,
    _require_lowercase_sha256,
    _require_nonblank,
)
from autoquant.execution.models import ZERO_HASH
from autoquant.execution.qmt_gateway import (
    QmtCallbackEnvelope,
    QmtCallbackKind,
    QmtCallbackValue,
)

QMT_CALLBACK_INBOX_VERSION: Final = "qmt-callback-inbox-event-v1"
QMT_CALLBACK_PERSISTENCE_RECEIPT_VERSION: Final = (
    "qmt-callback-persistence-receipt-v1"
)
MAXIMUM_CALLBACK_PERSISTENCE_AGE: Final = timedelta(seconds=5)
_SAFE_REASON = re.compile(r"[a-z0-9_]{1,64}\Z")
_HOLDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_STOCK_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ)\Z")
_INPUT_FIELDS: Final[dict[QmtCallbackKind, frozenset[str]]] = {
    QmtCallbackKind.DISCONNECTED: frozenset({"reason"}),
    QmtCallbackKind.ACCOUNT_STATUS: frozenset({"account_id", "status"}),
    QmtCallbackKind.ORDER: frozenset(
        {
            "account_id",
            "order_id",
            "order_remark",
            "order_status",
            "order_volume",
            "price",
            "side",
            "status_msg",
            "stock_code",
            "traded_price",
            "traded_volume",
        }
    ),
    QmtCallbackKind.TRADE: frozenset(
        {
            "account_id",
            "order_id",
            "order_remark",
            "side",
            "stock_code",
            "traded_amount",
            "traded_id",
            "traded_price",
            "traded_volume",
        }
    ),
    QmtCallbackKind.ORDER_ERROR: frozenset({"account_id", "error_id", "order_id"}),
    QmtCallbackKind.CANCEL_ERROR: frozenset({"account_id", "error_id", "order_id"}),
    QmtCallbackKind.ASYNC_ORDER_RESPONSE: frozenset(
        {"account_id", "order_id", "order_remark", "seq"}
    ),
}
_OMITTED_FIELDS: Final = frozenset({"account_id", "status_msg"})


def _validate_scalar(value: QmtCallbackValue, *, name: str) -> None:
    if type(value) not in {str, int, float, bool, type(None)}:
        raise TypeError(f"QMT callback {name} must be scalar")
    if isinstance(value, float) and not isfinite(value):
        raise ValueError(f"QMT callback {name} must be finite")
    if isinstance(value, str) and len(value) > 256:
        raise ValueError(f"QMT callback {name} is too long")


def _require_integer(
    payload: Mapping[str, QmtCallbackValue],
    name: str,
    *,
    minimum: int | None = None,
) -> int:
    value = payload[name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"QMT callback {name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"QMT callback {name} must be at least {minimum}")
    return value


def _require_number(
    payload: Mapping[str, QmtCallbackValue],
    name: str,
    *,
    minimum: float,
) -> None:
    value = payload[name]
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or (isinstance(value, float) and not isfinite(value))
    ):
        raise TypeError(f"QMT callback {name} must be a finite number")
    if value < minimum:
        raise ValueError(f"QMT callback {name} must be at least {minimum}")


def _validate_redacted_contract(
    *,
    kind: QmtCallbackKind,
    payload: Mapping[str, QmtCallbackValue],
) -> None:
    fields = set(payload)
    if fields == {"reason"}:
        reason = payload["reason"]
        if (
            kind not in {QmtCallbackKind.DISCONNECTED, QmtCallbackKind.ORDER_ERROR}
            or not isinstance(reason, str)
            or _SAFE_REASON.fullmatch(reason) is None
        ):
            raise ValueError("QMT callback reason is unsupported")
        return
    expected = _INPUT_FIELDS[kind] - _OMITTED_FIELDS
    if fields != expected:
        raise ValueError("QMT callback evidence fields do not match its kind")
    if kind is QmtCallbackKind.ACCOUNT_STATUS:
        _require_integer(payload, "status")
        return
    if kind in {QmtCallbackKind.ORDER_ERROR, QmtCallbackKind.CANCEL_ERROR}:
        _require_integer(payload, "error_id")
        _require_integer(payload, "order_id", minimum=0)
        return
    if kind is QmtCallbackKind.ASYNC_ORDER_RESPONSE:
        _require_integer(payload, "order_id", minimum=1)
        _require_integer(payload, "seq", minimum=1)
    else:
        stock_code = payload["stock_code"]
        side = payload["side"]
        if not isinstance(stock_code, str) or _STOCK_CODE.fullmatch(stock_code) is None:
            raise ValueError("QMT callback stock_code is unsupported")
        if side not in {"buy", "sell"}:
            raise ValueError("QMT callback side is unsupported")
        _require_integer(payload, "order_id", minimum=1)
        _require_integer(payload, "traded_volume", minimum=0)
        _require_number(payload, "traded_price", minimum=0)
        if kind is QmtCallbackKind.ORDER:
            _require_integer(payload, "order_status")
            _require_integer(payload, "order_volume", minimum=0)
            _require_number(payload, "price", minimum=0)
        else:
            traded_id = payload["traded_id"]
            if not isinstance(traded_id, str) or not traded_id.strip():
                raise ValueError("QMT callback traded_id must be nonblank")
            _require_number(payload, "traded_amount", minimum=0)
    remark = payload["order_remark"]
    if not isinstance(remark, str):
        raise TypeError("QMT callback order_remark must be a string")
    if len(remark.encode("utf-8")) > 24:
        raise ValueError("QMT callback order_remark exceeds the documented limit")


@dataclass(frozen=True, slots=True)
class QmtSanitizedCallback:
    account_id: str
    local_sequence: int
    kind: QmtCallbackKind
    received_at: datetime
    broker_session_date: date
    redacted_payload: Mapping[str, QmtCallbackValue]
    payload_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.account_id, name="QMT callback logical account_id")
        if self.account_id != self.account_id.strip() or len(self.account_id) > 128:
            raise ValueError(
                "QMT callback logical account_id must be trimmed and at most 128 characters"
            )
        if (
            not isinstance(self.local_sequence, int)
            or isinstance(self.local_sequence, bool)
            or self.local_sequence < 1
        ):
            raise ValueError("QMT callback local_sequence must be positive")
        if not isinstance(self.kind, QmtCallbackKind):
            raise TypeError("QMT callback kind must be QmtCallbackKind")
        received_at = to_utc(self.received_at, name="QMT callback received_at")
        if not isinstance(self.broker_session_date, date):
            raise TypeError("QMT callback broker_session_date must be a date")
        if received_at.astimezone(SHANGHAI).date() != self.broker_session_date:
            raise ValueError("QMT callback belongs to another Shanghai session")
        copied: dict[str, QmtCallbackValue] = {}
        for key, value in self.redacted_payload.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError("QMT callback evidence keys must be nonblank")
            if key in _OMITTED_FIELDS:
                raise ValueError("QMT callback evidence contains a prohibited field")
            _validate_scalar(value, name=key)
            copied[key] = value
        _validate_redacted_contract(kind=self.kind, payload=copied)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(
            self,
            "redacted_payload",
            MappingProxyType(copied),
        )
        object.__setattr__(
            self,
            "payload_hash",
            _canonical_hash(
                {
                    "account_id": self.account_id,
                    "broker_session_date": self.broker_session_date.isoformat(),
                    "kind": self.kind.value,
                    "local_sequence": self.local_sequence,
                    "received_at": _datetime_text(received_at),
                    "redacted_payload": copied,
                }
            ),
        )


def sanitize_qmt_callback(
    envelope: QmtCallbackEnvelope,
    *,
    expected_broker_account_id: str,
    logical_account_id: str,
) -> QmtSanitizedCallback:
    """Validate one callback and remove the broker account and free-form messages."""

    if not isinstance(envelope, QmtCallbackEnvelope):
        raise TypeError("envelope must be QmtCallbackEnvelope")
    if not isinstance(envelope.kind, QmtCallbackKind):
        raise TypeError("QMT callback kind must be QmtCallbackKind")
    _require_nonblank(
        expected_broker_account_id,
        name="expected QMT broker account_id",
    )
    if (
        expected_broker_account_id != expected_broker_account_id.strip()
        or len(expected_broker_account_id) > 256
    ):
        raise ValueError(
            "expected QMT broker account_id must be trimmed and at most 256 characters"
        )
    _require_nonblank(logical_account_id, name="QMT logical account_id")
    supplied = dict(envelope.payload)
    if set(supplied) == {"reason"}:
        reason = supplied["reason"]
        if (
            envelope.kind not in {QmtCallbackKind.DISCONNECTED, QmtCallbackKind.ORDER_ERROR}
            or not isinstance(reason, str)
            or _SAFE_REASON.fullmatch(reason) is None
        ):
            raise ValueError("QMT callback reason is unsupported")
        redacted: dict[str, QmtCallbackValue] = {"reason": reason}
    else:
        expected_fields = _INPUT_FIELDS[envelope.kind]
        if set(supplied) != expected_fields:
            raise ValueError("QMT callback fields do not match the documented contract")
        account_id = supplied["account_id"]
        if not isinstance(account_id, str) or account_id != expected_broker_account_id:
            raise ValueError("QMT callback belongs to another broker account")
        redacted = {}
        for key, value in supplied.items():
            _validate_scalar(value, name=key)
            if key not in _OMITTED_FIELDS:
                redacted[key] = value
    received_at = to_utc(envelope.received_at, name="QMT callback received_at")
    return QmtSanitizedCallback(
        account_id=logical_account_id,
        local_sequence=envelope.local_sequence,
        kind=envelope.kind,
        received_at=received_at,
        broker_session_date=received_at.astimezone(SHANGHAI).date(),
        redacted_payload=redacted,
    )


@dataclass(frozen=True, slots=True)
class QmtCallbackInboxEvent:
    callback: QmtSanitizedCallback
    gateway_holder_id: str
    qmt_session_id: int
    qmt_lease_generation: int
    previous_hash: str
    version: str = QMT_CALLBACK_INBOX_VERSION
    event_hash: str = field(init=False)

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        if not isinstance(self.callback, QmtSanitizedCallback):
            raise TypeError("callback must be QmtSanitizedCallback")
        _require_nonblank(
            self.gateway_holder_id,
            name="QMT callback gateway_holder_id",
        )
        if _HOLDER_ID.fullmatch(self.gateway_holder_id) is None:
            raise ValueError("QMT callback gateway_holder_id is unsupported")
        if (
            not isinstance(self.qmt_session_id, int)
            or isinstance(self.qmt_session_id, bool)
            or not 1 <= self.qmt_session_id <= 2_147_483_647
        ):
            raise ValueError("qmt_session_id must be a positive 32-bit integer")
        if (
            not isinstance(self.qmt_lease_generation, int)
            or isinstance(self.qmt_lease_generation, bool)
            or self.qmt_lease_generation < 1
        ):
            raise ValueError("qmt_lease_generation must be positive")
        _require_lowercase_sha256(
            self.previous_hash,
            name="QMT callback previous_hash",
        )
        if self.version != QMT_CALLBACK_INBOX_VERSION:
            raise ValueError("QMT callback inbox version is unsupported")
        object.__setattr__(self, "event_hash", _canonical_hash(self.payload()))

    def payload(self) -> dict[str, object]:
        return {
            "account_id": self.callback.account_id,
            "broker_mutation_allowed": False,
            "broker_session_date": self.callback.broker_session_date.isoformat(),
            "callback_payload_hash": self.callback.payload_hash,
            "gateway_holder_id": self.gateway_holder_id,
            "kind": self.callback.kind.value,
            "local_sequence": self.callback.local_sequence,
            "previous_hash": self.previous_hash,
            "qmt_lease_generation": self.qmt_lease_generation,
            "qmt_session_id": self.qmt_session_id,
            "received_at": _datetime_text(self.callback.received_at),
            "redacted_payload": dict(self.callback.redacted_payload),
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class QmtCallbackPersistenceReceipt:
    event: QmtCallbackInboxEvent
    persisted_at: datetime
    version: str = QMT_CALLBACK_PERSISTENCE_RECEIPT_VERSION
    receipt_hash: str = field(init=False)

    @property
    def broker_mutation_allowed(self) -> bool:
        return False

    def __post_init__(self) -> None:
        if not isinstance(self.event, QmtCallbackInboxEvent):
            raise TypeError("event must be QmtCallbackInboxEvent")
        persisted_at = to_utc(
            self.persisted_at,
            name="QMT callback persistence time",
        )
        latency = persisted_at - self.event.callback.received_at
        if (
            latency < timedelta(0)
            or latency > MAXIMUM_CALLBACK_PERSISTENCE_AGE
            or persisted_at.astimezone(SHANGHAI).date()
            != self.event.callback.broker_session_date
        ):
            raise ValueError(
                "QMT callback persistence receipt must be within five seconds"
            )
        if self.version != QMT_CALLBACK_PERSISTENCE_RECEIPT_VERSION:
            raise ValueError(
                "QMT callback persistence receipt version is unsupported"
            )
        object.__setattr__(self, "persisted_at", persisted_at)
        object.__setattr__(
            self,
            "receipt_hash",
            _canonical_hash(self.payload()),
        )

    def payload(self) -> dict[str, object]:
        callback = self.event.callback
        return {
            "account_id": callback.account_id,
            "broker_mutation_allowed": False,
            "broker_session_date": callback.broker_session_date.isoformat(),
            "event_hash": self.event.event_hash,
            "gateway_holder_id": self.event.gateway_holder_id,
            "local_sequence": callback.local_sequence,
            "persisted_at": _datetime_text(self.persisted_at),
            "qmt_lease_generation": self.event.qmt_lease_generation,
            "qmt_session_id": self.event.qmt_session_id,
            "received_at": _datetime_text(callback.received_at),
            "version": self.version,
        }


def replay_qmt_callback_inbox(
    events: tuple[QmtCallbackInboxEvent, ...],
) -> tuple[QmtCallbackInboxEvent, ...]:
    expected_scope: tuple[str, str, int, int, date] | None = None
    previous_hash = ZERO_HASH
    for expected_sequence, event in enumerate(events, start=1):
        if not isinstance(event, QmtCallbackInboxEvent):
            raise TypeError("events must contain QmtCallbackInboxEvent values")
        scope = (
            event.callback.account_id,
            event.gateway_holder_id,
            event.qmt_session_id,
            event.qmt_lease_generation,
            event.callback.broker_session_date,
        )
        if expected_scope is None:
            expected_scope = scope
        if (
            scope != expected_scope
            or event.callback.local_sequence != expected_sequence
            or event.previous_hash != previous_hash
            or event.event_hash != _canonical_hash(event.payload())
        ):
            raise ValueError("QMT callback inbox chain failed replay")
        previous_hash = event.event_hash
    return events

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from queue import Empty, SimpleQueue
from threading import Lock
from types import MappingProxyType
from typing import TypeAlias

from autoquant.clock import to_utc
from autoquant.errors import LiveTradingLockedError

QmtCallbackValue: TypeAlias = str | int | float | bool | None


def _freeze_payload(
    payload: Mapping[str, QmtCallbackValue],
) -> Mapping[str, QmtCallbackValue]:
    copied: dict[str, QmtCallbackValue] = {}
    for key, value in payload.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("QMT callback payload keys must be nonblank strings")
        if type(value) not in {str, int, float, bool, type(None)}:
            raise TypeError("QMT callback payload values must be scalar")
        if isinstance(value, float) and not isfinite(value):
            raise ValueError("QMT callback float values must be finite")
        copied[key] = value
    return MappingProxyType(copied)


class QmtCallbackKind(StrEnum):
    DISCONNECTED = "disconnected"
    ACCOUNT_STATUS = "account_status"
    ORDER = "order"
    TRADE = "trade"
    ORDER_ERROR = "order_error"
    CANCEL_ERROR = "cancel_error"
    ASYNC_ORDER_RESPONSE = "async_order_response"


@dataclass(frozen=True, slots=True)
class QmtCallbackEnvelope:
    local_sequence: int
    kind: QmtCallbackKind
    received_at: datetime
    payload: Mapping[str, QmtCallbackValue]


class QmtCallbackBuffer:
    """Thread-safe callback capture with a single serialized drain boundary.

    XtQuant callback threads only copy facts into this queue. Broker queries and durable
    state transitions belong to the coordinator consuming the queue, never to callbacks.
    """

    def __init__(self) -> None:
        self._queue: SimpleQueue[QmtCallbackEnvelope] = SimpleQueue()
        self._sequence_lock = Lock()
        self._drain_lock = Lock()
        self._sequence = 0

    def capture(
        self,
        kind: QmtCallbackKind,
        payload: Mapping[str, QmtCallbackValue],
        *,
        received_at: datetime | None = None,
    ) -> QmtCallbackEnvelope:
        if not isinstance(kind, QmtCallbackKind):
            raise TypeError("kind must be QmtCallbackKind")
        frozen_payload = _freeze_payload(payload)
        captured_at = (
            datetime.now(UTC) if received_at is None else to_utc(received_at, name="received_at")
        )
        with self._sequence_lock:
            self._sequence += 1
            envelope = QmtCallbackEnvelope(
                local_sequence=self._sequence,
                kind=kind,
                received_at=captured_at,
                payload=frozen_payload,
            )
            self._queue.put(envelope)
        return envelope

    def drain(self, *, limit: int = 1000) -> tuple[QmtCallbackEnvelope, ...]:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if not self._drain_lock.acquire(blocking=False):
            raise RuntimeError("QMT callback buffer already has an active consumer")
        try:
            events: list[QmtCallbackEnvelope] = []
            while len(events) < limit:
                try:
                    events.append(self._queue.get_nowait())
                except Empty:
                    break
            return tuple(events)
        finally:
            self._drain_lock.release()


class LockedQmtGateway:
    """QMT integration boundary for this release; all broker mutations are prohibited."""

    gateway_available = False

    def __init__(self) -> None:
        self.callbacks = QmtCallbackBuffer()

    def submit_order(self, *_args: object, **_kwargs: object) -> None:
        raise LiveTradingLockedError("QMT order submission is hard-locked in this release")

    def cancel_order(self, *_args: object, **_kwargs: object) -> None:
        raise LiveTradingLockedError("QMT cancellation is hard-locked in this release")

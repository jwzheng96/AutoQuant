from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from queue import Empty, Full, Queue
from threading import Lock
from types import MappingProxyType
from typing import TypeAlias

from autoquant.clock import to_utc
from autoquant.errors import BrokerStateUnknownError, LiveTradingLockedError

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


@dataclass(frozen=True, slots=True)
class QmtCallbackReservation:
    reservation_id: int
    events: tuple[QmtCallbackEnvelope, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.reservation_id, int)
            or isinstance(self.reservation_id, bool)
            or self.reservation_id < 1
        ):
            raise ValueError("QMT callback reservation_id must be positive")
        if not self.events:
            raise ValueError("QMT callback reservation must contain events")
        if any(not isinstance(event, QmtCallbackEnvelope) for event in self.events):
            raise TypeError("QMT callback reservation must contain callback envelopes")


class QmtCallbackBuffer:
    """Thread-safe callback capture with a single serialized drain boundary.

    XtQuant callback threads only copy facts into this queue. Broker queries and durable
    state transitions belong to the coordinator consuming the queue, never to callbacks.
    """

    def __init__(self, *, capacity: int = 10_000) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self._queue: Queue[QmtCallbackEnvelope] = Queue(maxsize=capacity)
        self._sequence_lock = Lock()
        self._drain_lock = Lock()
        self._sequence = 0
        self._overflowed = False
        self._reservation_sequence = 0
        self._active_reservation_id: int | None = None
        self._pending_events: tuple[QmtCallbackEnvelope, ...] = ()

    @property
    def cursor(self) -> int:
        with self._sequence_lock:
            return self._sequence

    @property
    def healthy(self) -> bool:
        with self._sequence_lock:
            return not self._overflowed

    @property
    def queued_count(self) -> int:
        """Return a scheduling hint; durable reservation still owns consumption."""

        return self._queue.qsize()

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
            if self._overflowed:
                raise BrokerStateUnknownError(
                    "QMT callback buffer overflow requires a full reconnect"
                )
            self._sequence += 1
            envelope = QmtCallbackEnvelope(
                local_sequence=self._sequence,
                kind=kind,
                received_at=captured_at,
                payload=frozen_payload,
            )
            try:
                self._queue.put_nowait(envelope)
            except Full:
                self._overflowed = True
                raise BrokerStateUnknownError(
                    "QMT callback buffer overflow requires a full reconnect"
                ) from None
        return envelope

    def drain(self, *, limit: int = 1000) -> tuple[QmtCallbackEnvelope, ...]:
        _validate_drain_limit(limit)
        if not self._drain_lock.acquire(blocking=False):
            raise RuntimeError("QMT callback buffer already has an active consumer")
        try:
            with self._sequence_lock:
                self._require_healthy()
                if self._active_reservation_id is not None or self._pending_events:
                    raise RuntimeError("QMT callback buffer has a durable reservation pending")
                return self._take(limit=limit)
        finally:
            self._drain_lock.release()

    def reserve_durable(
        self,
        *,
        limit: int = 1000,
    ) -> QmtCallbackReservation | None:
        """Reserve one ordered batch until durable persistence is acknowledged."""

        _validate_drain_limit(limit)
        if not self._drain_lock.acquire(blocking=False):
            raise RuntimeError("QMT callback buffer already has an active consumer")
        try:
            with self._sequence_lock:
                self._require_healthy()
                if self._active_reservation_id is not None:
                    raise RuntimeError("QMT callback buffer already has a durable reservation")
                if not self._pending_events:
                    self._pending_events = self._take(limit=limit)
                if not self._pending_events:
                    return None
                self._reservation_sequence += 1
                self._active_reservation_id = self._reservation_sequence
                return QmtCallbackReservation(
                    reservation_id=self._reservation_sequence,
                    events=self._pending_events,
                )
        finally:
            self._drain_lock.release()

    def acknowledge_durable(self, *, reservation_id: int) -> None:
        """Remove a batch only after every callback fact is durable."""

        self._complete_reservation(
            reservation_id=reservation_id,
            acknowledge=True,
        )

    def release_durable(self, *, reservation_id: int) -> None:
        """Make an unacknowledged batch available for an exact retry."""

        self._complete_reservation(
            reservation_id=reservation_id,
            acknowledge=False,
        )

    def restore_cursor(self, *, local_sequence: int) -> None:
        """Restore a fresh buffer cursor from a verified durable inbox."""

        if (
            not isinstance(local_sequence, int)
            or isinstance(local_sequence, bool)
            or local_sequence < 0
        ):
            raise ValueError("local_sequence must be nonnegative")
        if not self._drain_lock.acquire(blocking=False):
            raise RuntimeError("QMT callback buffer already has an active consumer")
        try:
            with self._sequence_lock:
                self._require_healthy()
                if (
                    self._sequence != 0
                    or self._active_reservation_id is not None
                    or self._pending_events
                    or not self._queue.empty()
                ):
                    raise RuntimeError(
                        "QMT callback cursor can be restored only into a fresh empty buffer"
                    )
                self._sequence = local_sequence
        finally:
            self._drain_lock.release()

    def _complete_reservation(
        self,
        *,
        reservation_id: int,
        acknowledge: bool,
    ) -> None:
        if (
            not isinstance(reservation_id, int)
            or isinstance(reservation_id, bool)
            or reservation_id < 1
        ):
            raise ValueError("reservation_id must be positive")
        if not self._drain_lock.acquire(blocking=False):
            raise RuntimeError("QMT callback buffer already has an active consumer")
        try:
            with self._sequence_lock:
                if self._active_reservation_id != reservation_id:
                    raise RuntimeError("QMT callback reservation is not the active batch")
                if acknowledge:
                    self._require_healthy()
                    self._pending_events = ()
                self._active_reservation_id = None
        finally:
            self._drain_lock.release()

    def _require_healthy(self) -> None:
        if self._overflowed:
            raise BrokerStateUnknownError("QMT callback buffer overflow requires a full reconnect")

    def _take(self, *, limit: int) -> tuple[QmtCallbackEnvelope, ...]:
        events: list[QmtCallbackEnvelope] = []
        while len(events) < limit:
            try:
                events.append(self._queue.get_nowait())
            except Empty:
                break
        return tuple(events)


def _validate_drain_limit(limit: int) -> None:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")


class LockedQmtGateway:
    """QMT integration boundary for this release; all broker mutations are prohibited."""

    gateway_available = False

    def __init__(self) -> None:
        self.callbacks = QmtCallbackBuffer()

    def submit_order(self, *_args: object, **_kwargs: object) -> None:
        raise LiveTradingLockedError("QMT order submission is hard-locked in this release")

    def cancel_order(self, *_args: object, **_kwargs: object) -> None:
        raise LiveTradingLockedError("QMT cancellation is hard-locked in this release")

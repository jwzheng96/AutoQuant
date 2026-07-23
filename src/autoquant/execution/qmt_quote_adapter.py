from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from math import isfinite
from queue import Empty, Full, Queue
from threading import Lock, RLock
from types import MappingProxyType
from typing import TypeAlias

from autoquant.clock import to_utc
from autoquant.errors import QuoteStreamUnavailableError
from autoquant.execution.market_clock import AShareTradingPhase
from autoquant.execution.qmt_models import from_qmt_instrument, to_qmt_instrument
from autoquant.execution.quote_book import ContinuousQuoteBook, QuoteObservation
from autoquant.risk.models import MarketQuote

QmtQuoteScalar: TypeAlias = str | int | float | Decimal | bool | None
QmtQuoteValue: TypeAlias = QmtQuoteScalar | tuple[QmtQuoteScalar, ...]
FrozenQmtTick: TypeAlias = Mapping[str, QmtQuoteValue]
FrozenQmtQuoteBatch: TypeAlias = Mapping[str, FrozenQmtTick]

_MINIMUM_QMT_TIMESTAMP_MS = 946_684_800_000  # 2000-01-01T00:00:00Z
_CONTINUOUS_TRADING_STATUS = 13


def _freeze_value(value: object) -> QmtQuoteValue:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError("QMT quote float values must be finite")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("QMT quote Decimal values must be finite")
        return value
    if isinstance(value, (list, tuple)):
        copied: list[QmtQuoteScalar] = []
        for item in value:
            frozen = _freeze_value(item)
            if isinstance(frozen, tuple):
                raise TypeError("QMT quote sequences cannot be nested")
            copied.append(frozen)
        return tuple(copied)
    raise TypeError("QMT quote values must be scalar or scalar sequences")


def _freeze_batch(payload: Mapping[str, object]) -> FrozenQmtQuoteBatch:
    if not isinstance(payload, Mapping) or not payload:
        raise ValueError("QMT quote callback payload must be a nonempty mapping")
    copied: dict[str, FrozenQmtTick] = {}
    for qmt_instrument, raw_tick in payload.items():
        if not isinstance(qmt_instrument, str) or not qmt_instrument.strip():
            raise ValueError("QMT quote instrument keys must be nonblank strings")
        if not isinstance(raw_tick, Mapping) or not raw_tick:
            raise ValueError("QMT quote tick must be a nonempty mapping")
        tick: dict[str, QmtQuoteValue] = {}
        for key, value in raw_tick.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError("QMT quote field names must be nonblank strings")
            tick[key] = _freeze_value(value)
        copied[qmt_instrument] = MappingProxyType(tick)
    return MappingProxyType(copied)


def _decimal(value: object, *, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"QMT {name} must be numeric")
    try:
        converted = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"QMT {name} must be numeric") from None
    if not converted.is_finite() or converted <= 0:
        raise ValueError(f"QMT {name} must be positive and finite")
    return converted


def _best_price(value: object, *, name: str) -> Decimal:
    if not isinstance(value, tuple) or not value:
        raise ValueError(f"QMT {name} must be a nonempty price sequence")
    return _decimal(value[0], name=f"{name}[0]")


def _event_time(value: object, *, received_at: datetime) -> datetime:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError("QMT time must be a millisecond timestamp")
    numeric = float(value)
    if not isfinite(numeric) or numeric < _MINIMUM_QMT_TIMESTAMP_MS:
        raise ValueError("QMT time is outside the supported range")
    try:
        event_time = datetime.fromtimestamp(numeric / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        raise ValueError("QMT time is outside the supported range") from None
    if event_time > received_at:
        raise ValueError("QMT event time cannot follow receipt time")
    return event_time


def normalize_qmt_tick(
    *,
    qmt_instrument: str,
    tick: Mapping[str, QmtQuoteValue],
    received_at: datetime,
    phase: AShareTradingPhase,
) -> MarketQuote:
    """Normalize one documented XtData tick without guessing missing facts."""

    if not isinstance(phase, AShareTradingPhase):
        raise TypeError("phase must be AShareTradingPhase")
    received = to_utc(received_at, name="QMT quote received_at")
    try:
        raw_status = tick["stockStatus"]
        if (
            isinstance(raw_status, bool)
            or not isinstance(raw_status, (int, float, Decimal))
            or int(raw_status) != raw_status
        ):
            raise ValueError("QMT stockStatus must be an integer")
        status = int(raw_status)
        return MarketQuote(
            instrument=from_qmt_instrument(qmt_instrument),
            as_of=_event_time(tick["time"], received_at=received),
            last_price=_decimal(tick["lastPrice"], name="lastPrice"),
            bid_price=_best_price(tick["bidPrice"], name="bidPrice"),
            ask_price=_best_price(tick["askPrice"], name="askPrice"),
            market_open=(
                phase.accepts_strategy_orders and status == _CONTINUOUS_TRADING_STATUS
            ),
        )
    except KeyError as error:
        raise ValueError(f"QMT tick omitted required field: {error.args[0]}") from None


@dataclass(frozen=True, slots=True)
class QmtQuoteCallbackEnvelope:
    callback_sequence: int
    received_at: datetime
    phase: AShareTradingPhase
    payload: FrozenQmtQuoteBatch


class QmtWholeQuoteBridge:
    """Thread-safe XtData callback ingress with serialized quote-book publication.

    Vendor callback threads only validate and copy facts into a bounded queue. A single
    consumer establishes the full-tick baseline and publishes locally sequenced updates.
    """

    def __init__(
        self,
        *,
        quote_book: ContinuousQuoteBook,
        instruments: tuple[str, ...],
        queue_capacity: int = 10_000,
    ) -> None:
        normalized = tuple(sorted(instruments))
        if not normalized or len(set(normalized)) != len(normalized):
            raise ValueError("QMT quote universe must be nonempty and unique")
        qmt_universe = tuple(to_qmt_instrument(instrument) for instrument in normalized)
        if (
            not isinstance(queue_capacity, int)
            or isinstance(queue_capacity, bool)
            or queue_capacity < 1
        ):
            raise ValueError("queue_capacity must be a positive integer")
        if quote_book.source != "qmt":
            raise ValueError("QMT quote bridge requires a quote book with source='qmt'")
        self._quote_book = quote_book
        self._universe = frozenset(qmt_universe)
        self._queue_capacity = queue_capacity
        self._queue: Queue[QmtQuoteCallbackEnvelope] = Queue(maxsize=queue_capacity)
        self._state_lock = RLock()
        self._drain_lock = Lock()
        self._callback_sequence = 0
        self._source_sequence = 0
        self._baselined = False

    def reset_from_full_tick(
        self,
        payload: Mapping[str, object],
        *,
        received_at: datetime,
        phase: AShareTradingPhase,
        reset_id: str,
    ) -> None:
        try:
            frozen = self._validated_batch(payload, require_complete=True)
            received = to_utc(received_at, name="QMT baseline received_at")
            quotes = tuple(
                normalize_qmt_tick(
                    qmt_instrument=instrument,
                    tick=frozen[instrument],
                    received_at=received,
                    phase=phase,
                )
                for instrument in sorted(frozen)
            )
        except Exception:
            self._invalidate(reason="invalid_qmt_quote_baseline")
            raise
        if not self._drain_lock.acquire(blocking=False):
            self._invalidate(reason="qmt_baseline_during_active_drain")
            raise QuoteStreamUnavailableError(
                "QMT baseline cannot be installed during an active drain"
            )
        try:
            with self._state_lock:
                self._queue = Queue(maxsize=self._queue_capacity)
                self._callback_sequence = 0
                self._source_sequence = 1
                self._quote_book.reset(
                    quotes=quotes,
                    source_sequence=self._source_sequence,
                    received_at=received,
                    reset_id=reset_id,
                )
                self._baselined = True
        except Exception:
            self._invalidate(reason="invalid_qmt_quote_baseline")
            raise
        finally:
            self._drain_lock.release()

    def capture(
        self,
        payload: Mapping[str, object],
        *,
        phase: AShareTradingPhase,
        received_at: datetime | None = None,
    ) -> QmtQuoteCallbackEnvelope:
        try:
            if not isinstance(phase, AShareTradingPhase):
                raise TypeError("phase must be AShareTradingPhase")
            frozen = self._validated_batch(payload, require_complete=False)
            captured_at = (
                datetime.now(UTC)
                if received_at is None
                else to_utc(received_at, name="QMT callback received_at")
            )
            with self._state_lock:
                if not self._baselined:
                    raise QuoteStreamUnavailableError(
                        "QMT quote callback arrived before a full-tick baseline"
                    )
                self._callback_sequence += 1
                envelope = QmtQuoteCallbackEnvelope(
                    callback_sequence=self._callback_sequence,
                    received_at=captured_at,
                    phase=phase,
                    payload=frozen,
                )
                self._queue.put_nowait(envelope)
                return envelope
        except Full:
            self._invalidate(reason="qmt_quote_callback_queue_overflow")
            raise QuoteStreamUnavailableError("QMT quote callback queue overflow") from None
        except Exception:
            self._invalidate(reason="invalid_qmt_quote_callback")
            raise

    def drain(self, *, limit: int = 1_000) -> tuple[QuoteObservation, ...]:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if not self._drain_lock.acquire(blocking=False):
            raise RuntimeError("QMT quote bridge already has an active consumer")
        try:
            published: list[QuoteObservation] = []
            while len(published) < limit:
                try:
                    envelope = self._queue.get_nowait()
                except Empty:
                    break
                try:
                    for instrument in sorted(envelope.payload):
                        if len(published) >= limit:
                            raise QuoteStreamUnavailableError(
                                "QMT quote drain limit split a callback batch"
                            )
                        quote = normalize_qmt_tick(
                            qmt_instrument=instrument,
                            tick=envelope.payload[instrument],
                            received_at=envelope.received_at,
                            phase=envelope.phase,
                        )
                        self._source_sequence += 1
                        observation = QuoteObservation(
                            source="qmt",
                            source_sequence=self._source_sequence,
                            received_at=envelope.received_at,
                            quote=quote,
                        )
                        self._quote_book.publish(observation)
                        published.append(observation)
                except Exception:
                    self._invalidate(reason="invalid_qmt_quote_callback")
                    raise QuoteStreamUnavailableError(
                        "QMT quote callback violated the normalized contract"
                    ) from None
            return tuple(published)
        finally:
            self._drain_lock.release()

    def _validated_batch(
        self,
        payload: Mapping[str, object],
        *,
        require_complete: bool,
    ) -> FrozenQmtQuoteBatch:
        frozen = _freeze_batch(payload)
        normalized_keys: dict[str, str] = {}
        for supplied in frozen:
            canonical = to_qmt_instrument(from_qmt_instrument(supplied))
            if canonical in normalized_keys:
                raise ValueError("QMT quote batch contains duplicate normalized instruments")
            normalized_keys[canonical] = supplied
        supplied_universe = set(normalized_keys)
        if not supplied_universe <= self._universe:
            raise ValueError(
                "QMT quote batch contains an instrument outside the configured universe"
            )
        if require_complete and supplied_universe != self._universe:
            raise ValueError("QMT full-tick baseline does not cover the configured universe")
        return MappingProxyType(
            {canonical: frozen[supplied] for canonical, supplied in normalized_keys.items()}
        )

    def _invalidate(self, *, reason: str) -> None:
        with self._state_lock:
            self._baselined = False
            self._queue = Queue(maxsize=self._queue_capacity)
        self._quote_book.disconnect(reason=reason)

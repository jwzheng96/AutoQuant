from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, TypeVar

from autoquant.clock import to_shanghai, to_utc
from autoquant.data.models import (
    SourceEvidence,
    _canonical_hash,
    _datetime_text,
    _decimal_text,
    _require_lowercase_sha256,
    _require_nonblank,
)


def _require_decimal(value: Decimal, *, name: str, positive: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if positive and value <= 0:
        raise ValueError(f"{name} must be positive")


def _require_revision_fields(
    *,
    source: str,
    instrument: str,
    event_time: datetime,
    session_date: date,
    available_at: datetime,
    ingested_at: datetime,
    source_revision: str,
    availability_policy: str,
    evidence_hash: str,
) -> tuple[datetime, datetime, datetime]:
    _require_nonblank(source, name="source")
    _require_nonblank(instrument, name="instrument")
    _require_nonblank(source_revision, name="source_revision")
    _require_nonblank(availability_policy, name="availability_policy")
    _require_lowercase_sha256(evidence_hash, name="evidence_hash")
    normalized_event = to_utc(event_time, name="event_time")
    normalized_available = to_utc(available_at, name="available_at")
    normalized_ingested = to_utc(ingested_at, name="ingested_at")
    if to_shanghai(normalized_event).date() != session_date:
        raise ValueError("event_time must belong to session_date in Asia/Shanghai")
    if normalized_available < normalized_event:
        raise ValueError("available_at cannot precede event_time")
    if normalized_ingested < normalized_event:
        raise ValueError("ingested_at cannot precede event_time")
    return normalized_event, normalized_available, normalized_ingested


@dataclass(frozen=True, slots=True)
class DailyBarRevision:
    source: str
    instrument: str
    session_date: date
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    source_revision: str
    availability_policy: str
    evidence_hash: str
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    pre_close: Decimal
    volume: int
    turnover: Decimal
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        event_time, available_at, ingested_at = _require_revision_fields(
            source=self.source,
            instrument=self.instrument,
            event_time=self.event_time,
            session_date=self.session_date,
            available_at=self.available_at,
            ingested_at=self.ingested_at,
            source_revision=self.source_revision,
            availability_policy=self.availability_policy,
            evidence_hash=self.evidence_hash,
        )
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "ingested_at", ingested_at)

        prices = {
            "open_price": self.open_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "close_price": self.close_price,
            "pre_close": self.pre_close,
        }
        for _name, price in prices.items():
            _require_decimal(price, name="prices", positive=True)
        if self.high_price < max(self.open_price, self.close_price):
            raise ValueError("high_price cannot be below open_price or close_price")
        if self.low_price > min(self.open_price, self.close_price):
            raise ValueError("low_price cannot be above open_price or close_price")
        if not isinstance(self.volume, int) or isinstance(self.volume, bool):
            raise TypeError("volume must be an integer")
        if self.volume < 0:
            raise ValueError("volume cannot be negative")
        _require_decimal(self.turnover, name="turnover")
        if self.turnover < 0:
            raise ValueError("turnover cannot be negative")

        payload = {
            "availability_policy": self.availability_policy,
            "available_at": _datetime_text(available_at),
            "close_price": _decimal_text(self.close_price),
            "event_time": _datetime_text(event_time),
            "evidence_hash": self.evidence_hash,
            "high_price": _decimal_text(self.high_price),
            "instrument": self.instrument,
            "low_price": _decimal_text(self.low_price),
            "open_price": _decimal_text(self.open_price),
            "pre_close": _decimal_text(self.pre_close),
            "session_date": self.session_date.isoformat(),
            "source": self.source,
            "source_revision": self.source_revision,
            "turnover": _decimal_text(self.turnover),
            "volume": self.volume,
        }
        object.__setattr__(self, "content_hash", _canonical_hash(payload))

    @classmethod
    def from_values(
        cls,
        *,
        source: str,
        instrument: str,
        session_date: date,
        event_time: datetime,
        available_at: datetime,
        ingested_at: datetime,
        source_revision: str,
        availability_policy: str,
        evidence_hash: str,
        open_price: Decimal | str,
        high_price: Decimal | str,
        low_price: Decimal | str,
        close_price: Decimal | str,
        pre_close: Decimal | str,
        volume: int,
        turnover: Decimal | str,
    ) -> DailyBarRevision:
        return cls(
            source=source,
            instrument=instrument,
            session_date=session_date,
            event_time=event_time,
            available_at=available_at,
            ingested_at=ingested_at,
            source_revision=source_revision,
            availability_policy=availability_policy,
            evidence_hash=evidence_hash,
            open_price=Decimal(open_price),
            high_price=Decimal(high_price),
            low_price=Decimal(low_price),
            close_price=Decimal(close_price),
            pre_close=Decimal(pre_close),
            volume=volume,
            turnover=Decimal(turnover),
        )


@dataclass(frozen=True, slots=True)
class AdjustmentFactorRevision:
    source: str
    instrument: str
    session_date: date
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    source_revision: str
    availability_policy: str
    evidence_hash: str
    factor: Decimal
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        event_time, available_at, ingested_at = _require_revision_fields(
            source=self.source,
            instrument=self.instrument,
            event_time=self.event_time,
            session_date=self.session_date,
            available_at=self.available_at,
            ingested_at=self.ingested_at,
            source_revision=self.source_revision,
            availability_policy=self.availability_policy,
            evidence_hash=self.evidence_hash,
        )
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "ingested_at", ingested_at)
        _require_decimal(self.factor, name="factor", positive=True)
        payload = {
            "availability_policy": self.availability_policy,
            "available_at": _datetime_text(available_at),
            "event_time": _datetime_text(event_time),
            "evidence_hash": self.evidence_hash,
            "factor": _decimal_text(self.factor),
            "instrument": self.instrument,
            "session_date": self.session_date.isoformat(),
            "source": self.source,
            "source_revision": self.source_revision,
        }
        object.__setattr__(self, "content_hash", _canonical_hash(payload))

    @classmethod
    def from_values(
        cls,
        *,
        source: str,
        instrument: str,
        session_date: date,
        event_time: datetime,
        available_at: datetime,
        ingested_at: datetime,
        source_revision: str,
        availability_policy: str,
        evidence_hash: str,
        factor: Decimal | str,
    ) -> AdjustmentFactorRevision:
        return cls(
            source=source,
            instrument=instrument,
            session_date=session_date,
            event_time=event_time,
            available_at=available_at,
            ingested_at=ingested_at,
            source_revision=source_revision,
            availability_policy=availability_policy,
            evidence_hash=evidence_hash,
            factor=Decimal(factor),
        )


@dataclass(frozen=True, slots=True)
class TradingSession:
    source: str
    session_date: date
    is_open: bool
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="source")
        if not isinstance(self.is_open, bool):
            raise TypeError("is_open must be a bool")
        object.__setattr__(self, "available_at", to_utc(self.available_at))
        _require_lowercase_sha256(self.response_hash)


@dataclass(frozen=True, slots=True)
class InstrumentLifecycle:
    source: str
    instrument: str
    list_date: date
    delist_date: date | None
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="source")
        _require_nonblank(self.instrument, name="instrument")
        if self.delist_date is not None and self.delist_date < self.list_date:
            raise ValueError("delist_date cannot precede list_date")
        object.__setattr__(self, "available_at", to_utc(self.available_at))
        _require_lowercase_sha256(self.response_hash)


@dataclass(frozen=True, slots=True)
class DailySuspensionStatus:
    source: str
    instrument: str
    session_date: date
    suspended: bool
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="source")
        _require_nonblank(self.instrument, name="instrument")
        if not isinstance(self.suspended, bool):
            raise TypeError("suspended must be a bool")
        object.__setattr__(self, "available_at", to_utc(self.available_at))
        _require_lowercase_sha256(self.response_hash)


@dataclass(frozen=True, slots=True)
class DailyPriceLimit:
    source: str
    instrument: str
    session_date: date
    pre_close: Decimal
    up_limit: Decimal
    down_limit: Decimal
    available_at: datetime
    response_hash: str
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="source")
        _require_nonblank(self.instrument, name="instrument")
        for name, value in (
            ("pre_close", self.pre_close),
            ("up_limit", self.up_limit),
            ("down_limit", self.down_limit),
        ):
            _require_decimal(value, name=name, positive=True)
        if self.down_limit >= self.up_limit:
            raise ValueError("down_limit must be below up_limit")
        available_at = to_utc(self.available_at, name="available_at")
        object.__setattr__(self, "available_at", available_at)
        _require_lowercase_sha256(self.response_hash)
        payload = {
            "available_at": _datetime_text(available_at),
            "down_limit": _decimal_text(self.down_limit),
            "instrument": self.instrument,
            "pre_close": _decimal_text(self.pre_close),
            "response_hash": self.response_hash,
            "session_date": self.session_date.isoformat(),
            "source": self.source,
            "up_limit": _decimal_text(self.up_limit),
        }
        object.__setattr__(self, "content_hash", _canonical_hash(payload))


CoverageItem = TypeVar(
    "CoverageItem",
    TradingSession,
    InstrumentLifecycle,
    DailySuspensionStatus,
    DailyPriceLimit,
)


def _reject_conflicts(
    values: tuple[CoverageItem, ...], *, key: Any
) -> None:
    seen: dict[object, CoverageItem] = {}
    for value in values:
        identity = key(value)
        previous = seen.setdefault(identity, value)
        if previous != value:
            raise ValueError(f"conflicting coverage evidence for {identity!r}")


@dataclass(frozen=True, slots=True)
class DailyCoverageEvidence:
    sessions: tuple[TradingSession, ...]
    lifecycles: tuple[InstrumentLifecycle, ...]
    suspensions: tuple[DailySuspensionStatus, ...]
    price_limits: tuple[DailyPriceLimit, ...] = ()

    def __post_init__(self) -> None:
        sessions = tuple(self.sessions)
        lifecycles = tuple(self.lifecycles)
        suspensions = tuple(self.suspensions)
        price_limits = tuple(self.price_limits)
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "lifecycles", lifecycles)
        object.__setattr__(self, "suspensions", suspensions)
        object.__setattr__(self, "price_limits", price_limits)
        if any(not isinstance(value, TradingSession) for value in sessions):
            raise TypeError("sessions must contain TradingSession values")
        if any(not isinstance(value, InstrumentLifecycle) for value in lifecycles):
            raise TypeError("lifecycles must contain InstrumentLifecycle values")
        if any(not isinstance(value, DailySuspensionStatus) for value in suspensions):
            raise TypeError("suspensions must contain DailySuspensionStatus values")
        if any(not isinstance(value, DailyPriceLimit) for value in price_limits):
            raise TypeError("price_limits must contain DailyPriceLimit values")
        _reject_conflicts(sessions, key=lambda value: (value.source, value.session_date))
        _reject_conflicts(
            lifecycles, key=lambda value: (value.source, value.instrument)
        )
        _reject_conflicts(
            suspensions,
            key=lambda value: (value.source, value.instrument, value.session_date),
        )
        _reject_conflicts(
            price_limits,
            key=lambda value: (value.source, value.instrument, value.session_date),
        )


@dataclass(frozen=True, slots=True)
class DailyDatasetBatch:
    bars: tuple[DailyBarRevision, ...]
    factors: tuple[AdjustmentFactorRevision, ...]
    coverage: DailyCoverageEvidence
    source_evidence: tuple[SourceEvidence, ...]

    def __post_init__(self) -> None:
        bars = tuple(self.bars)
        factors = tuple(self.factors)
        source_evidence = tuple(self.source_evidence)
        object.__setattr__(self, "bars", bars)
        object.__setattr__(self, "factors", factors)
        object.__setattr__(self, "source_evidence", source_evidence)
        if any(not isinstance(value, DailyBarRevision) for value in bars):
            raise TypeError("bars must contain DailyBarRevision values")
        if any(not isinstance(value, AdjustmentFactorRevision) for value in factors):
            raise TypeError("factors must contain AdjustmentFactorRevision values")
        if not isinstance(self.coverage, DailyCoverageEvidence):
            raise TypeError("coverage must be DailyCoverageEvidence")
        if not source_evidence or any(
            not isinstance(value, SourceEvidence) for value in source_evidence
        ):
            raise ValueError("source_evidence must contain SourceEvidence values")
        self._reject_duplicate_records(bars, "daily bar")
        self._reject_duplicate_records(factors, "adjustment factor")
        self._validate_evidence(source_evidence)

    @staticmethod
    def _reject_duplicate_records(
        records: tuple[DailyBarRevision, ...] | tuple[AdjustmentFactorRevision, ...],
        label: str,
    ) -> None:
        seen: set[tuple[str, str, date]] = set()
        for record in records:
            key = (record.source, record.instrument, record.session_date)
            if key in seen:
                raise ValueError(f"duplicate {label} for {key!r}")
            seen.add(key)

    def _validate_evidence(self, evidence: tuple[SourceEvidence, ...]) -> None:
        allowed = {
            "daily",
            "adj_factor",
            "trade_cal",
            "stock_basic",
            "suspend_d",
            "stk_limit",
        }
        if any(value.method not in allowed for value in evidence):
            raise ValueError("unsupported daily source evidence method")
        keys = {
            (value.source, value.method, value.response_hash) for value in evidence
        }
        if any(
            (record.source, "daily", record.evidence_hash) not in keys
            for record in self.bars
        ):
            raise ValueError("daily evidence does not back every bar")
        if any(
            (record.source, "adj_factor", record.evidence_hash) not in keys
            for record in self.factors
        ):
            raise ValueError("adj_factor evidence does not back every factor")
        coverage_checks = (
            (self.coverage.sessions, "trade_cal"),
            (self.coverage.lifecycles, "stock_basic"),
            (self.coverage.suspensions, "suspend_d"),
            (self.coverage.price_limits, "stk_limit"),
        )
        for records, method in coverage_checks:
            if any(
                (record.source, method, record.response_hash) not in keys
                for record in records
            ):
                raise ValueError(f"{method} evidence does not back coverage")

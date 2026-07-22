from __future__ import annotations

import hashlib
import json
import string
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import Any

from autoquant.clock import to_utc


def _decimal_text(value: Decimal) -> str:
    components = value.as_tuple()
    exponent = components.exponent
    if not isinstance(exponent, int):
        raise ValueError("decimal values must be finite")

    digits = list(components.digits)
    if not any(digits):
        return "0"
    while digits[-1] == 0:
        digits.pop()
        exponent += 1

    coefficient = "".join(str(digit) for digit in digits)
    if exponent >= 0:
        text = coefficient + ("0" * exponent)
    else:
        decimal_position = len(coefficient) + exponent
        if decimal_position > 0:
            text = f"{coefficient[:decimal_position]}.{coefficient[decimal_position:]}"
        else:
            text = f"0.{('0' * -decimal_position)}{coefficient}"
    return f"-{text}" if components.sign else text


def _datetime_text(value: datetime) -> str:
    return to_utc(value).isoformat(timespec="microseconds")


def _canonical_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_nonblank(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be empty")


def _require_lowercase_sha256(value: str, *, name: str = "response_hash") -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")


@dataclass(frozen=True, slots=True)
class MinuteBarRevision:
    source: str
    instrument: str
    event_time: datetime
    published_at: datetime | None
    available_at: datetime
    ingested_at: datetime
    source_revision: str
    availability_policy: str
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    turnover: Decimal
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        event_time = to_utc(self.event_time, name="event_time")
        published_at = (
            None
            if self.published_at is None
            else to_utc(self.published_at, name="published_at")
        )
        available_at = to_utc(self.available_at, name="available_at")
        ingested_at = to_utc(self.ingested_at, name="ingested_at")

        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "published_at", published_at)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "ingested_at", ingested_at)

        if not self.availability_policy.strip():
            raise ValueError("availability_policy cannot be empty")
        if any(
            price <= 0
            for price in (self.open_price, self.high_price, self.low_price, self.close_price)
        ):
            raise ValueError("prices must be positive")
        if self.high_price < max(self.open_price, self.close_price):
            raise ValueError("high_price cannot be below open_price or close_price")
        if self.low_price > min(self.open_price, self.close_price):
            raise ValueError("low_price cannot be above open_price or close_price")
        if self.volume < 0:
            raise ValueError("volume cannot be negative")
        if self.turnover < 0:
            raise ValueError("turnover cannot be negative")
        if available_at < event_time:
            raise ValueError("available_at cannot precede event_time")
        if self.availability_policy.startswith("live-") and ingested_at < available_at:
            raise ValueError("live ingested_at cannot precede available_at")

        payload = {
            "availability_policy": self.availability_policy,
            "available_at": _datetime_text(available_at),
            "close_price": _decimal_text(self.close_price),
            "event_time": _datetime_text(event_time),
            "high_price": _decimal_text(self.high_price),
            "instrument": self.instrument,
            "low_price": _decimal_text(self.low_price),
            "open_price": _decimal_text(self.open_price),
            "published_at": None if published_at is None else _datetime_text(published_at),
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
        event_time: datetime,
        published_at: datetime | None,
        available_at: datetime,
        ingested_at: datetime,
        source_revision: str,
        availability_policy: str,
        open_price: Decimal | str,
        high_price: Decimal | str,
        low_price: Decimal | str,
        close_price: Decimal | str,
        volume: int,
        turnover: Decimal | str,
    ) -> MinuteBarRevision:
        return cls(
            source=source,
            instrument=instrument,
            event_time=event_time,
            published_at=published_at,
            available_at=available_at,
            ingested_at=ingested_at,
            source_revision=source_revision,
            availability_policy=availability_policy,
            open_price=Decimal(open_price),
            high_price=Decimal(high_price),
            low_price=Decimal(low_price),
            close_price=Decimal(close_price),
            volume=volume,
            turnover=Decimal(turnover),
        )


@dataclass(frozen=True, slots=True)
class TradingPeriod:
    source: str
    instrument: str
    session_date: date
    minute_ends: tuple[datetime, ...]
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="source")
        _require_nonblank(self.instrument, name="instrument")
        minute_ends = tuple(
            to_utc(value, name="minute_end") for value in self.minute_ends
        )
        object.__setattr__(
            self,
            "minute_ends",
            minute_ends,
        )
        object.__setattr__(self, "available_at", to_utc(self.available_at, name="available_at"))
        if not minute_ends or any(
            current >= following
            for current, following in pairwise(minute_ends)
        ):
            raise ValueError("minute_ends must be nonempty, strictly ordered, and unique")
        _require_lowercase_sha256(self.response_hash)


@dataclass(frozen=True, slots=True)
class SuspensionStatus:
    source: str
    instrument: str
    session_date: date
    suspended: bool
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="source")
        _require_nonblank(self.instrument, name="instrument")
        object.__setattr__(self, "available_at", to_utc(self.available_at, name="available_at"))
        _require_lowercase_sha256(self.response_hash)


@dataclass(frozen=True, slots=True)
class MarketCoverageEvidence:
    periods: tuple[TradingPeriod, ...]
    suspensions: tuple[SuspensionStatus, ...]

    def __post_init__(self) -> None:
        periods = tuple(self.periods)
        suspensions = tuple(self.suspensions)
        object.__setattr__(self, "periods", periods)
        object.__setattr__(self, "suspensions", suspensions)
        if any(not isinstance(item, TradingPeriod) for item in periods):
            raise TypeError("periods must contain TradingPeriod values")
        if any(not isinstance(item, SuspensionStatus) for item in suspensions):
            raise TypeError("suspensions must contain SuspensionStatus values")
        self._reject_conflicts(periods)
        self._reject_conflicts(suspensions)

    @staticmethod
    def _reject_conflicts(
        evidence: tuple[TradingPeriod, ...] | tuple[SuspensionStatus, ...],
    ) -> None:
        seen: dict[tuple[str, str, date], TradingPeriod | SuspensionStatus] = {}
        for item in evidence:
            key = (item.source, item.instrument, item.session_date)
            previous = seen.setdefault(key, item)
            if previous != item:
                raise ValueError(f"conflicting coverage evidence for {key!r}")


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    source: str
    method: str
    requested_at: datetime
    response_body: bytes
    response_hash: str

    def __post_init__(self) -> None:
        _require_nonblank(self.source, name="source")
        _require_nonblank(self.method, name="method")
        object.__setattr__(self, "requested_at", to_utc(self.requested_at, name="requested_at"))
        if not isinstance(self.response_body, bytes):
            raise TypeError("response_body must be bytes")
        _require_lowercase_sha256(self.response_hash)
        if hashlib.sha256(self.response_body).hexdigest() != self.response_hash:
            raise ValueError("response_hash does not match response_body")


@dataclass(frozen=True, slots=True)
class MinuteBarBatch:
    records: tuple[MinuteBarRevision, ...]
    source_evidence: tuple[SourceEvidence, ...]

    def __post_init__(self) -> None:
        records = tuple(self.records)
        source_evidence = tuple(self.source_evidence)
        object.__setattr__(self, "records", records)
        object.__setattr__(self, "source_evidence", source_evidence)
        if any(not isinstance(item, MinuteBarRevision) for item in records):
            raise TypeError("records must contain MinuteBarRevision values")
        if any(not isinstance(item, SourceEvidence) for item in source_evidence):
            raise TypeError("source_evidence must contain SourceEvidence values")
        if not source_evidence:
            raise ValueError("source_evidence cannot be empty")
        if any(item.method != "get_price" for item in source_evidence):
            raise ValueError("source_evidence for minute bars must use get_price")
        if records:
            record_sources = {record.source for record in records}
            evidence_sources = {item.source for item in source_evidence}
            if record_sources != evidence_sources:
                raise ValueError("source_evidence sources must match minute-bar record sources")


@dataclass(frozen=True, slots=True)
class CoverageBatch:
    coverage: MarketCoverageEvidence
    source_evidence: tuple[SourceEvidence, ...]

    def __post_init__(self) -> None:
        source_evidence = tuple(self.source_evidence)
        object.__setattr__(self, "source_evidence", source_evidence)
        if not isinstance(self.coverage, MarketCoverageEvidence):
            raise TypeError("coverage must be MarketCoverageEvidence")
        if any(not isinstance(item, SourceEvidence) for item in source_evidence):
            raise TypeError("source_evidence must contain SourceEvidence values")
        if not source_evidence:
            raise ValueError("source_evidence cannot be empty")
        if any(
            item.method not in {"get_trading_periods", "is_suspended"}
            for item in source_evidence
        ):
            raise ValueError("source_evidence method must be a coverage API method")

        periods = self.coverage.periods
        suspensions = self.coverage.suspensions
        coverage_sources = {item.source for item in periods} | {
            item.source for item in suspensions
        }
        evidence_keys = {
            (item.source, item.method, item.response_hash) for item in source_evidence
        }
        if coverage_sources:
            evidence_sources = {item.source for item in source_evidence}
            if not evidence_sources <= coverage_sources:
                raise ValueError("source_evidence contains a source absent from coverage")
            allowed_methods = {
                (item.source, "get_trading_periods") for item in periods
            } | {(item.source, "is_suspended") for item in suspensions}
            if any(
                (item.source, item.method) not in allowed_methods for item in source_evidence
            ):
                raise ValueError("source_evidence method does not match coverage evidence")
        if any(
            (item.source, "get_trading_periods", item.response_hash) not in evidence_keys
            for item in periods
        ):
            raise ValueError("period response_hash is not backed by source_evidence")
        if any(
            (item.source, "is_suspended", item.response_hash) not in evidence_keys
            for item in suspensions
        ):
            raise ValueError("suspension response_hash is not backed by source_evidence")


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    source: str
    instruments: tuple[str, ...]
    start_time: datetime
    end_time: datetime
    as_of: datetime
    record_hashes: tuple[str, ...]
    quality_report_hash: str
    production_complete: bool
    row_count: int
    manifest_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if isinstance(self.instruments, (str, bytes)):
            raise ValueError("instruments must be a sequence of instrument strings")
        instruments = tuple(self.instruments)
        record_hashes = tuple(self.record_hashes)
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "record_hashes", record_hashes)
        start_time = to_utc(self.start_time, name="start_time")
        end_time = to_utc(self.end_time, name="end_time")
        as_of = to_utc(self.as_of, name="as_of")
        object.__setattr__(self, "start_time", start_time)
        object.__setattr__(self, "end_time", end_time)
        object.__setattr__(self, "as_of", as_of)

        if not self.source.strip():
            raise ValueError("source cannot be empty")
        if not instruments or any(
            not isinstance(instrument, str) or not instrument.strip()
            for instrument in instruments
        ):
            raise ValueError("instruments cannot be empty")
        if start_time > end_time:
            raise ValueError("start_time cannot follow end_time")
        if as_of < end_time:
            raise ValueError("as_of cannot precede end_time")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        if self.row_count != len(record_hashes):
            raise ValueError("row_count must match record_hashes")
        if any(
            len(record_hash) != 64 or any(char not in string.hexdigits for char in record_hash)
            for record_hash in record_hashes
        ):
            raise ValueError("record_hashes must contain 64-character hexadecimal hashes")
        if len({record_hash.lower() for record_hash in record_hashes}) != len(record_hashes):
            raise ValueError("record_hashes must be unique")
        if self.production_complete and not self.quality_report_hash.strip():
            raise ValueError("quality_report_hash is required for a production-complete manifest")

        payload = {
            "as_of": _datetime_text(as_of),
            "end_time": _datetime_text(end_time),
            "instruments": list(self.instruments),
            "production_complete": self.production_complete,
            "quality_report_hash": self.quality_report_hash,
            "record_hashes": list(self.record_hashes),
            "row_count": self.row_count,
            "source": self.source,
            "start_time": _datetime_text(start_time),
        }
        object.__setattr__(self, "manifest_hash", _canonical_hash(payload))

from __future__ import annotations

import hashlib
import json
import string
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from open_quant.clock import to_utc


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


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
        object.__setattr__(
            self,
            "minute_ends",
            tuple(to_utc(value, name="minute_end") for value in self.minute_ends),
        )
        object.__setattr__(self, "available_at", to_utc(self.available_at, name="available_at"))


@dataclass(frozen=True, slots=True)
class SuspensionStatus:
    source: str
    instrument: str
    session_date: date
    suspended: bool
    available_at: datetime
    response_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "available_at", to_utc(self.available_at, name="available_at"))


@dataclass(frozen=True, slots=True)
class MarketCoverageEvidence:
    periods: tuple[TradingPeriod, ...]
    suspensions: tuple[SuspensionStatus, ...]

    def __post_init__(self) -> None:
        self._reject_conflicts(self.periods)
        self._reject_conflicts(self.suspensions)

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
        object.__setattr__(self, "requested_at", to_utc(self.requested_at, name="requested_at"))


@dataclass(frozen=True, slots=True)
class MinuteBarBatch:
    records: tuple[MinuteBarRevision, ...]
    source_evidence: tuple[SourceEvidence, ...]


@dataclass(frozen=True, slots=True)
class CoverageBatch:
    coverage: MarketCoverageEvidence
    source_evidence: tuple[SourceEvidence, ...]


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
        start_time = to_utc(self.start_time, name="start_time")
        end_time = to_utc(self.end_time, name="end_time")
        as_of = to_utc(self.as_of, name="as_of")
        object.__setattr__(self, "start_time", start_time)
        object.__setattr__(self, "end_time", end_time)
        object.__setattr__(self, "as_of", as_of)

        if not self.source.strip():
            raise ValueError("source cannot be empty")
        if not self.instruments or any(not instrument.strip() for instrument in self.instruments):
            raise ValueError("instruments cannot be empty")
        if start_time > end_time:
            raise ValueError("start_time cannot follow end_time")
        if as_of < end_time:
            raise ValueError("as_of cannot precede end_time")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        if self.row_count != len(self.record_hashes):
            raise ValueError("row_count must match record_hashes")
        if any(
            len(record_hash) != 64 or any(char not in string.hexdigits for char in record_hash)
            for record_hash in self.record_hashes
        ):
            raise ValueError("record_hashes must contain 64-character hexadecimal hashes")
        if len({record_hash.lower() for record_hash in self.record_hashes}) != len(
            self.record_hashes
        ):
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

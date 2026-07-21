from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from open_quant.clock import to_utc
from open_quant.data.models import MinuteBarRevision


class AvailabilityPolicy(Protocol):
    version: str

    def assign(self, *, bar_end: datetime) -> datetime: ...


@dataclass(frozen=True, slots=True)
class HistoricalMinutePolicy:
    version: str
    delay: timedelta

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("version cannot be empty")
        if self.delay <= timedelta(0):
            raise ValueError("delay must be positive")

    def assign(self, *, bar_end: datetime) -> datetime:
        return to_utc(bar_end, name="bar_end") + self.delay


@dataclass(frozen=True, slots=True)
class LiveArrivalPolicy:
    version: str = "live-arrival-v1"

    def assign(self, *, bar_end: datetime, received_at: datetime) -> datetime:
        normalized_bar_end = to_utc(bar_end, name="bar_end")
        normalized_received_at = to_utc(received_at, name="received_at")
        if normalized_received_at < normalized_bar_end:
            raise ValueError("received_at cannot precede bar_end")
        return normalized_received_at


def visible_as_of(
    revisions: Sequence[MinuteBarRevision], as_of: datetime
) -> tuple[MinuteBarRevision, ...]:
    normalized_as_of = to_utc(as_of, name="as_of")
    return tuple(record for record in revisions if record.available_at <= normalized_as_of)

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Protocol, TypeVar

from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.daily_models import TradingSession


class AvailableRevision(Protocol):
    available_at: datetime


Revision = TypeVar("Revision", bound=AvailableRevision)


@dataclass(frozen=True, slots=True)
class NextTradingSessionOpenPolicy:
    version: str = "tushare-daily-v1"

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("version cannot be empty")

    def assign(
        self, *, session_date: date, sessions: Sequence[TradingSession]
    ) -> datetime:
        later_open_dates = sorted(
            value.session_date
            for value in sessions
            if value.is_open and value.session_date > session_date
        )
        if not later_open_dates:
            raise ValueError("next open session is required")
        shanghai_open = datetime.combine(
            later_open_dates[0], time(9, 30), tzinfo=SHANGHAI
        )
        return to_utc(shanghai_open)


def visible_daily_as_of(
    revisions: Sequence[Revision], as_of: datetime
) -> tuple[Revision, ...]:
    cutoff = to_utc(as_of, name="as_of")
    return tuple(value for value in revisions if value.available_at <= cutoff)

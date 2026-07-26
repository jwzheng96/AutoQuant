from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Protocol, TypeVar

from autoquant.clock import SHANGHAI, to_shanghai, to_utc
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


@dataclass(frozen=True, slots=True)
class CompletedDailySessionAvailability:
    """Completed open sessions split by conservative daily-data visibility."""

    eligible_open_dates: tuple[date, ...]
    pending_open_dates: tuple[date, ...]
    next_eligible_at: datetime | None


def completed_daily_session_availability(
    sessions: Sequence[TradingSession],
    as_of: datetime,
    *,
    policy: NextTradingSessionOpenPolicy | None = None,
) -> CompletedDailySessionAvailability:
    """Require the next open before treating a completed daily session as visible."""

    cutoff = to_utc(as_of, name="as_of")
    values = tuple(sessions)
    session_dates = tuple(value.session_date for value in values)
    if session_dates != tuple(sorted(session_dates)) or len(set(session_dates)) != len(
        session_dates
    ):
        raise ValueError("trading sessions must be date-sorted and unique")
    local_date = to_shanghai(cutoff).date()
    completed_open_dates = tuple(
        value.session_date for value in values if value.is_open and value.session_date < local_date
    )
    availability_policy = NextTradingSessionOpenPolicy() if policy is None else policy
    eligible: list[date] = []
    pending: list[date] = []
    pending_instants: list[datetime] = []
    for session_date in completed_open_dates:
        try:
            eligible_at = availability_policy.assign(
                session_date=session_date,
                sessions=values,
            )
        except ValueError:
            pending.append(session_date)
            continue
        if eligible_at <= cutoff:
            eligible.append(session_date)
        else:
            pending.append(session_date)
            pending_instants.append(eligible_at)
    return CompletedDailySessionAvailability(
        eligible_open_dates=tuple(eligible),
        pending_open_dates=tuple(pending),
        next_eligible_at=min(pending_instants) if pending_instants else None,
    )


def visible_daily_as_of(
    revisions: Sequence[Revision], as_of: datetime
) -> tuple[Revision, ...]:
    cutoff = to_utc(as_of, name="as_of")
    return tuple(value for value in revisions if value.available_at <= cutoff)

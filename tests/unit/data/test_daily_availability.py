from datetime import UTC, date, datetime

import pytest

from autoquant.data.daily_availability import (
    NextTradingSessionOpenPolicy,
    completed_daily_session_availability,
    visible_daily_as_of,
)
from autoquant.data.daily_models import TradingSession

HASH = "a" * 64


def session(day: int, *, is_open: bool) -> TradingSession:
    return TradingSession(
        source="tushare",
        session_date=date(2026, 7, day),
        is_open=is_open,
        available_at=datetime(2026, 7, 22, tzinfo=UTC),
        response_hash=HASH,
    )


def test_policy_assigns_next_open_session_at_shanghai_open() -> None:
    policy = NextTradingSessionOpenPolicy()

    assigned = policy.assign(
        session_date=date(2026, 7, 20),
        sessions=(session(21, is_open=False), session(22, is_open=True)),
    )

    assert policy.version == "tushare-daily-v1"
    assert assigned == datetime(2026, 7, 22, 1, 30, tzinfo=UTC)


def test_policy_rejects_missing_next_open_session() -> None:
    with pytest.raises(ValueError, match="next open session"):
        NextTradingSessionOpenPolicy().assign(
            session_date=date(2026, 7, 20),
            sessions=(session(20, is_open=True), session(21, is_open=False)),
        )


def test_completed_session_availability_waits_through_weekend() -> None:
    sessions = (
        session(23, is_open=True),
        session(24, is_open=True),
        session(25, is_open=False),
        session(26, is_open=False),
        session(27, is_open=True),
    )

    waiting = completed_daily_session_availability(
        sessions,
        datetime(2026, 7, 26, 2, tzinfo=UTC),
    )
    available = completed_daily_session_availability(
        sessions,
        datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
    )

    assert waiting.eligible_open_dates == (date(2026, 7, 23),)
    assert waiting.pending_open_dates == (date(2026, 7, 24),)
    assert waiting.next_eligible_at == datetime(2026, 7, 27, 1, 30, tzinfo=UTC)
    assert available.eligible_open_dates == (
        date(2026, 7, 23),
        date(2026, 7, 24),
    )
    assert available.pending_open_dates == ()
    assert available.next_eligible_at is None


def test_completed_session_availability_fails_closed_without_next_open() -> None:
    result = completed_daily_session_availability(
        (session(24, is_open=True), session(25, is_open=False)),
        datetime(2026, 7, 26, 2, tzinfo=UTC),
    )

    assert result.eligible_open_dates == ()
    assert result.pending_open_dates == (date(2026, 7, 24),)
    assert result.next_eligible_at is None


def test_completed_session_availability_rejects_ambiguous_calendar() -> None:
    with pytest.raises(ValueError, match="date-sorted and unique"):
        completed_daily_session_availability(
            (session(24, is_open=True), session(23, is_open=True)),
            datetime(2026, 7, 26, 2, tzinfo=UTC),
        )


def test_visible_daily_as_of_preserves_input_type_and_order() -> None:
    class Revision:
        def __init__(self, available_at: datetime) -> None:
            self.available_at = available_at

    first = Revision(datetime(2026, 7, 21, 1, 30, tzinfo=UTC))
    future = Revision(datetime(2026, 7, 22, 1, 30, tzinfo=UTC))

    assert visible_daily_as_of(
        (first, future), datetime(2026, 7, 21, 2, 0, tzinfo=UTC)
    ) == (first,)


def test_visible_daily_as_of_requires_aware_cutoff() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        visible_daily_as_of((), datetime(2026, 7, 21, 2, 0))

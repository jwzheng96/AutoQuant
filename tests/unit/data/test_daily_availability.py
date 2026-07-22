from datetime import UTC, date, datetime

import pytest

from autoquant.data.daily_availability import (
    NextTradingSessionOpenPolicy,
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

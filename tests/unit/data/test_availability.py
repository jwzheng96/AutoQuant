from datetime import UTC, datetime, timedelta

import pytest

from autoquant.data.availability import (
    HistoricalMinutePolicy,
    LiveArrivalPolicy,
    visible_as_of,
)
from autoquant.data.models import MinuteBarRevision


def make_bar(available_at: datetime) -> MinuteBarRevision:
    return MinuteBarRevision.from_values(
        source="rqdata",
        instrument="000001.XSHE",
        event_time=datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        published_at=None,
        available_at=available_at,
        ingested_at=datetime(2026, 7, 21, 8, 0, tzinfo=UTC),
        source_revision="initial",
        availability_policy="rqdata-minute-v1",
        open_price="10.00",
        high_price="10.10",
        low_price="9.99",
        close_price="10.05",
        volume=1000,
        turnover="10050.00",
    )


def test_historical_policy_never_makes_bar_visible_at_bar_end() -> None:
    policy = HistoricalMinutePolicy(version="rqdata-minute-v1", delay=timedelta(seconds=5))
    bar_end = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    assert policy.assign(bar_end=bar_end) == bar_end + timedelta(seconds=5)


def test_visible_as_of_excludes_future_available_revision() -> None:
    cutoff = datetime(2026, 7, 20, 1, 31, 4, tzinfo=UTC)
    assert visible_as_of([make_bar(cutoff + timedelta(seconds=1))], cutoff) == ()


def test_visible_as_of_includes_revision_at_cutoff() -> None:
    cutoff = datetime(2026, 7, 20, 1, 31, 5, tzinfo=UTC)
    bar = make_bar(cutoff)
    assert visible_as_of([bar], cutoff) == (bar,)


def test_live_policy_uses_arrival_time() -> None:
    bar_end = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    received_at = bar_end + timedelta(milliseconds=250)
    assert LiveArrivalPolicy().assign(bar_end=bar_end, received_at=received_at) == received_at


def test_live_policy_rejects_arrival_before_bar_end() -> None:
    bar_end = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    with pytest.raises(ValueError, match="cannot precede"):
        LiveArrivalPolicy().assign(
            bar_end=bar_end,
            received_at=bar_end - timedelta(microseconds=1),
        )


def test_policies_reject_naive_datetimes() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        HistoricalMinutePolicy(
            version="rqdata-minute-v1", delay=timedelta(seconds=5)
        ).assign(bar_end=datetime(2026, 7, 20, 1, 31))


@pytest.mark.parametrize("version", ["", " ", "\t"])
def test_historical_policy_rejects_blank_version(version: str) -> None:
    with pytest.raises(ValueError, match="version"):
        HistoricalMinutePolicy(version=version, delay=timedelta(seconds=5))


@pytest.mark.parametrize("delay", [timedelta(0), timedelta(microseconds=-1)])
def test_historical_policy_requires_positive_delay(delay: timedelta) -> None:
    with pytest.raises(ValueError, match="delay"):
        HistoricalMinutePolicy(version="rqdata-minute-v1", delay=delay)

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from open_quant.clock import to_shanghai, to_utc
from open_quant.data.models import (
    DatasetManifest,
    MarketCoverageEvidence,
    MinuteBarRevision,
    SuspensionStatus,
    TradingPeriod,
)


def revision_values() -> dict[str, object]:
    return {
        "source": "rqdata",
        "instrument": "000001.XSHE",
        "event_time": datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        "published_at": None,
        "available_at": datetime(2026, 7, 20, 1, 31, 5, tzinfo=UTC),
        "ingested_at": datetime(2026, 7, 21, 8, 0, tzinfo=UTC),
        "source_revision": "initial",
        "availability_policy": "rqdata-minute-v1",
        "open_price": "10",
        "high_price": "10.1",
        "low_price": "9.9",
        "close_price": "10.05",
        "volume": 1000,
        "turnover": "10050",
    }


def make_revision(**updates: object) -> MinuteBarRevision:
    values = revision_values()
    values.update(updates)
    return MinuteBarRevision.from_values(**values)  # type: ignore[arg-type]


def test_revision_is_immutable_and_hash_is_deterministic() -> None:
    values = dict(
        source="rqdata",
        instrument="000001.XSHE",
        event_time=datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        published_at=None,
        available_at=datetime(2026, 7, 20, 1, 31, 5, tzinfo=UTC),
        ingested_at=datetime(2026, 7, 21, 8, 0, tzinfo=UTC),
        source_revision="initial",
        availability_policy="rqdata-minute-v1",
        open_price="10",
        high_price="10.1",
        low_price="9.9",
        close_price="10.05",
        volume=1000,
        turnover="10050",
    )
    first = MinuteBarRevision.from_values(**values)
    second = MinuteBarRevision.from_values(**values)
    assert first.content_hash == second.content_hash
    with pytest.raises(FrozenInstanceError):
        first.volume = 1  # type: ignore[misc]


def test_content_hash_uses_canonical_decimal_values() -> None:
    first = make_revision(open_price="10", turnover="10050")
    second = make_revision(open_price="10.000", turnover="10050.00")
    assert first.content_hash == second.content_hash


def test_content_hash_excludes_operational_ingestion_time() -> None:
    first = make_revision()
    second = make_revision(ingested_at=first.ingested_at + timedelta(days=1))
    assert first.content_hash == second.content_hash


def test_revision_normalizes_aware_datetimes_to_utc() -> None:
    shanghai = ZoneInfo("Asia/Shanghai")
    event_time = datetime(2026, 7, 20, 9, 31, tzinfo=shanghai)
    revision = make_revision(
        event_time=event_time,
        available_at=event_time + timedelta(seconds=5),
    )
    assert revision.event_time.tzinfo is UTC
    assert revision.event_time == datetime(2026, 7, 20, 1, 31, tzinfo=UTC)


def test_clock_requires_awareness_and_converts_explicitly() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        to_utc(datetime(2026, 7, 20, 1, 31))
    instant = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
    assert to_shanghai(instant).hour == 9


@pytest.mark.parametrize("field", ["event_time", "published_at", "available_at", "ingested_at"])
def test_revision_rejects_naive_datetimes(field: str) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        make_revision(**{field: datetime(2026, 7, 20, 1, 31)})


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"availability_policy": "  "}, "availability_policy"),
        ({"open_price": "0"}, "prices must be positive"),
        ({"high_price": "10.04"}, "high_price"),
        ({"low_price": "10.01"}, "low_price"),
        ({"volume": -1}, "volume"),
        ({"turnover": "-0.01"}, "turnover"),
        (
            {"available_at": datetime(2026, 7, 20, 1, 30, tzinfo=UTC)},
            "available_at",
        ),
    ],
)
def test_revision_rejects_invalid_values(updates: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        make_revision(**updates)


def test_live_revision_rejects_ingestion_before_availability() -> None:
    available_at = datetime(2026, 7, 20, 1, 31, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="live ingested_at"):
        make_revision(
            availability_policy="live-arrival-v1",
            available_at=available_at,
            ingested_at=available_at - timedelta(microseconds=1),
        )


def test_historical_revision_retains_historical_availability() -> None:
    revision = make_revision()
    assert revision.available_at < revision.ingested_at


def test_prices_and_turnover_are_decimal() -> None:
    revision = make_revision()
    assert revision.open_price == Decimal("10")
    assert isinstance(revision.turnover, Decimal)


def test_coverage_rejects_conflicting_periods_for_same_session() -> None:
    available_at = datetime(2026, 7, 20, 10, tzinfo=UTC)
    first = TradingPeriod(
        source="rqdata",
        instrument="000001.XSHE",
        session_date=date(2026, 7, 20),
        minute_ends=(datetime(2026, 7, 20, 1, 31, tzinfo=UTC),),
        available_at=available_at,
        response_hash="a",
    )
    conflicting = TradingPeriod(
        source=first.source,
        instrument=first.instrument,
        session_date=first.session_date,
        minute_ends=(datetime(2026, 7, 20, 1, 32, tzinfo=UTC),),
        available_at=available_at,
        response_hash="b",
    )
    with pytest.raises(ValueError, match="conflicting coverage evidence"):
        MarketCoverageEvidence(periods=(first, conflicting), suspensions=())


def test_period_and_suspension_are_complementary_for_same_session() -> None:
    available_at = datetime(2026, 7, 20, 10, tzinfo=UTC)
    period = TradingPeriod(
        source="rqdata",
        instrument="000001.XSHE",
        session_date=date(2026, 7, 20),
        minute_ends=(),
        available_at=available_at,
        response_hash="a",
    )
    suspension = SuspensionStatus(
        source=period.source,
        instrument=period.instrument,
        session_date=period.session_date,
        suspended=True,
        available_at=available_at,
        response_hash="b",
    )
    coverage = MarketCoverageEvidence(periods=(period,), suspensions=(suspension,))
    assert coverage.periods == (period,)
    assert coverage.suspensions == (suspension,)


def manifest_values() -> dict[str, object]:
    return {
        "source": "rqdata",
        "instruments": ("000001.XSHE",),
        "start_time": datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        "end_time": datetime(2026, 7, 20, 7, 0, tzinfo=UTC),
        "as_of": datetime(2026, 7, 21, 8, 0, tzinfo=UTC),
        "record_hashes": ("a" * 64,),
        "quality_report_hash": "b" * 64,
        "production_complete": True,
        "row_count": 1,
    }


def make_manifest(**updates: object) -> DatasetManifest:
    values = manifest_values()
    values.update(updates)
    return DatasetManifest(**values)  # type: ignore[arg-type]


def test_manifest_is_immutable_and_hash_is_deterministic() -> None:
    first = make_manifest()
    second = make_manifest()
    assert first.manifest_hash == second.manifest_hash
    with pytest.raises(FrozenInstanceError):
        first.row_count = 2  # type: ignore[misc]


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"source": " "}, "source"),
        ({"instruments": ()}, "instruments"),
        ({"instruments": ("",)}, "instruments"),
        (
            {"record_hashes": ("a" * 64, "a" * 64), "row_count": 2},
            "record_hashes",
        ),
        ({"record_hashes": ("not-a-hash",)}, "record_hashes"),
        (
            {"production_complete": True, "quality_report_hash": " "},
            "quality_report_hash",
        ),
        ({"row_count": 2}, "row_count"),
    ],
)
def test_manifest_rejects_invalid_identity(updates: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        make_manifest(**updates)


def test_manifest_rejects_invalid_temporal_bounds() -> None:
    end_time = datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="start_time"):
        make_manifest(start_time=end_time + timedelta(seconds=1), end_time=end_time)
    with pytest.raises(ValueError, match="as_of"):
        make_manifest(end_time=end_time, as_of=end_time - timedelta(seconds=1))


@pytest.mark.parametrize("field", ["start_time", "end_time", "as_of"])
def test_manifest_rejects_naive_temporal_bounds(field: str) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        make_manifest(**{field: datetime(2026, 7, 20, 7, 0)})

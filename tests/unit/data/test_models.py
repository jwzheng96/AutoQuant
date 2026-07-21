import hashlib
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from zoneinfo import ZoneInfo

import pytest

from open_quant.clock import to_shanghai, to_utc
from open_quant.data.models import (
    CoverageBatch,
    DatasetManifest,
    MarketCoverageEvidence,
    MinuteBarBatch,
    MinuteBarRevision,
    SourceEvidence,
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


def test_content_hash_is_independent_of_decimal_context_precision() -> None:
    exact_turnover = Decimal("123456789.1234567800")
    with localcontext() as context:
        context.prec = 6
        low_precision = make_revision(turnover=exact_turnover)
    with localcontext() as context:
        context.prec = 50
        high_precision = make_revision(turnover=exact_turnover)
    assert low_precision.content_hash == high_precision.content_hash


def test_distinct_exact_decimals_never_share_content_hash_at_low_precision() -> None:
    with localcontext() as context:
        context.prec = 6
        first = make_revision(turnover=Decimal("123456789.123456780"))
        second = make_revision(turnover=Decimal("123456789.123456781"))
    assert first.content_hash != second.content_hash


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
        response_hash="a" * 64,
    )
    conflicting = TradingPeriod(
        source=first.source,
        instrument=first.instrument,
        session_date=first.session_date,
        minute_ends=(datetime(2026, 7, 20, 1, 32, tzinfo=UTC),),
        available_at=available_at,
        response_hash="b" * 64,
    )
    with pytest.raises(ValueError, match="conflicting coverage evidence"):
        MarketCoverageEvidence(periods=(first, conflicting), suspensions=())


def test_period_and_suspension_are_complementary_for_same_session() -> None:
    available_at = datetime(2026, 7, 20, 10, tzinfo=UTC)
    period = TradingPeriod(
        source="rqdata",
        instrument="000001.XSHE",
        session_date=date(2026, 7, 20),
        minute_ends=(datetime(2026, 7, 20, 1, 31, tzinfo=UTC),),
        available_at=available_at,
        response_hash="a" * 64,
    )
    suspension = SuspensionStatus(
        source=period.source,
        instrument=period.instrument,
        session_date=period.session_date,
        suspended=True,
        available_at=available_at,
        response_hash="b" * 64,
    )
    coverage = MarketCoverageEvidence(periods=(period,), suspensions=(suspension,))
    assert coverage.periods == (period,)
    assert coverage.suspensions == (suspension,)


def make_period(**updates: object) -> TradingPeriod:
    values: dict[str, object] = {
        "source": "rqdata",
        "instrument": "000001.XSHE",
        "session_date": date(2026, 7, 20),
        "minute_ends": (
            datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
            datetime(2026, 7, 20, 1, 32, tzinfo=UTC),
        ),
        "available_at": datetime(2026, 7, 20, 10, tzinfo=UTC),
        "response_hash": "a" * 64,
    }
    values.update(updates)
    return TradingPeriod(**values)  # type: ignore[arg-type]


def make_suspension(**updates: object) -> SuspensionStatus:
    values: dict[str, object] = {
        "source": "rqdata",
        "instrument": "000001.XSHE",
        "session_date": date(2026, 7, 20),
        "suspended": False,
        "available_at": datetime(2026, 7, 20, 10, tzinfo=UTC),
        "response_hash": "b" * 64,
    }
    values.update(updates)
    return SuspensionStatus(**values)  # type: ignore[arg-type]


def make_source_evidence(body: bytes = b"response", **updates: object) -> SourceEvidence:
    values: dict[str, object] = {
        "source": "rqdata",
        "method": "get_price",
        "requested_at": datetime(2026, 7, 20, 10, tzinfo=UTC),
        "response_body": body,
        "response_hash": hashlib.sha256(body).hexdigest(),
    }
    values.update(updates)
    return SourceEvidence(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["source", "instrument"])
def test_period_rejects_blank_identity(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        make_period(**{field: " "})


@pytest.mark.parametrize("field", ["source", "instrument"])
def test_suspension_rejects_blank_identity(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        make_suspension(**{field: " "})


@pytest.mark.parametrize(
    "minute_ends",
    [
        (),
        (
            datetime(2026, 7, 20, 1, 32, tzinfo=UTC),
            datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        ),
        (
            datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
            datetime(2026, 7, 20, 1, 31, tzinfo=UTC),
        ),
    ],
)
def test_period_requires_nonempty_strictly_ordered_unique_endpoints(
    minute_ends: tuple[datetime, ...],
) -> None:
    with pytest.raises(ValueError, match="minute_ends"):
        make_period(minute_ends=minute_ends)


@pytest.mark.parametrize(
    "response_hash",
    ["a", "A" * 64, "g" * 64],
)
def test_coverage_evidence_requires_lowercase_sha256(response_hash: str) -> None:
    with pytest.raises(ValueError, match="response_hash"):
        make_period(response_hash=response_hash)
    with pytest.raises(ValueError, match="response_hash"):
        make_suspension(response_hash=response_hash)


@pytest.mark.parametrize("field", ["source", "method"])
def test_source_evidence_rejects_blank_identity(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        make_source_evidence(**{field: " "})


def test_source_evidence_rejects_naive_requested_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        make_source_evidence(requested_at=datetime(2026, 7, 20, 10))


@pytest.mark.parametrize("response_hash", ["a", "A" * 64, "g" * 64])
def test_source_evidence_requires_lowercase_sha256(response_hash: str) -> None:
    with pytest.raises(ValueError, match="response_hash"):
        make_source_evidence(response_hash=response_hash)


def test_source_evidence_rejects_tampered_body() -> None:
    with pytest.raises(ValueError, match="response_hash"):
        make_source_evidence(response_hash=hashlib.sha256(b"different").hexdigest())


def test_batches_reject_values_of_the_wrong_evidence_type() -> None:
    revision = make_revision()
    evidence = make_source_evidence()
    coverage = MarketCoverageEvidence(periods=(make_period(),), suspensions=(make_suspension(),))
    with pytest.raises(TypeError, match="records"):
        MinuteBarBatch(records=(object(),), source_evidence=(evidence,))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="source_evidence"):
        MinuteBarBatch(records=(revision,), source_evidence=(object(),))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="coverage"):
        CoverageBatch(coverage=object(), source_evidence=(evidence,))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="source_evidence"):
        CoverageBatch(coverage=coverage, source_evidence=(object(),))  # type: ignore[arg-type]


def test_coverage_aggregate_detaches_from_input_lists() -> None:
    period = make_period()
    suspension = make_suspension()
    periods = [period]
    suspensions = [suspension]
    coverage = MarketCoverageEvidence(periods=periods, suspensions=suspensions)  # type: ignore[arg-type]
    periods.clear()
    suspensions.clear()
    assert coverage.periods == (period,)
    assert coverage.suspensions == (suspension,)


def test_batches_detach_from_input_lists() -> None:
    revision = make_revision()
    evidence = make_source_evidence()
    records = [revision]
    bar_evidence = [evidence]
    bar_batch = MinuteBarBatch(records=records, source_evidence=bar_evidence)  # type: ignore[arg-type]
    period_evidence = make_source_evidence(b"periods", method="get_trading_periods")
    suspension_evidence = make_source_evidence(b"suspensions", method="is_suspended")
    coverage = MarketCoverageEvidence(
        periods=(make_period(response_hash=period_evidence.response_hash),),
        suspensions=(make_suspension(response_hash=suspension_evidence.response_hash),),
    )
    coverage_evidence = [period_evidence, suspension_evidence]
    coverage_batch = CoverageBatch(coverage=coverage, source_evidence=coverage_evidence)  # type: ignore[arg-type]
    records.clear()
    bar_evidence.clear()
    coverage_evidence.clear()
    assert bar_batch.records == (revision,)
    assert bar_batch.source_evidence == (evidence,)
    assert coverage_batch.source_evidence == (period_evidence, suspension_evidence)


def test_batches_require_nonempty_source_evidence() -> None:
    with pytest.raises(ValueError, match="source_evidence"):
        MinuteBarBatch(records=(), source_evidence=())
    with pytest.raises(ValueError, match="source_evidence"):
        CoverageBatch(
            coverage=MarketCoverageEvidence(periods=(), suspensions=()),
            source_evidence=(),
        )


def test_empty_market_data_response_retains_get_price_evidence() -> None:
    evidence = make_source_evidence(method="get_price")
    batch = MinuteBarBatch(records=(), source_evidence=(evidence,))
    assert batch.records == ()
    assert batch.source_evidence == (evidence,)


def test_minute_batch_rejects_wrong_evidence_source() -> None:
    evidence = make_source_evidence(source="other", method="get_price")
    with pytest.raises(ValueError, match="source"):
        MinuteBarBatch(records=(make_revision(),), source_evidence=(evidence,))


def test_minute_batch_rejects_wrong_evidence_method() -> None:
    evidence = make_source_evidence(method="get_trading_periods")
    with pytest.raises(ValueError, match="get_price"):
        MinuteBarBatch(records=(make_revision(),), source_evidence=(evidence,))


def test_minute_batch_rejects_evidence_for_an_unrepresented_source() -> None:
    matching = make_source_evidence(b"matching")
    unrelated = make_source_evidence(b"unrelated", source="other")
    with pytest.raises(ValueError, match="source"):
        MinuteBarBatch(
            records=(make_revision(),),
            source_evidence=(matching, unrelated),
        )


def valid_coverage_and_evidence() -> tuple[
    MarketCoverageEvidence, tuple[SourceEvidence, SourceEvidence]
]:
    period_evidence = make_source_evidence(b"periods", method="get_trading_periods")
    suspension_evidence = make_source_evidence(b"suspensions", method="is_suspended")
    coverage = MarketCoverageEvidence(
        periods=(make_period(response_hash=period_evidence.response_hash),),
        suspensions=(make_suspension(response_hash=suspension_evidence.response_hash),),
    )
    return coverage, (period_evidence, suspension_evidence)


def test_coverage_batch_accepts_matching_source_method_and_hash_evidence() -> None:
    coverage, evidence = valid_coverage_and_evidence()
    batch = CoverageBatch(coverage=coverage, source_evidence=evidence)
    assert batch.source_evidence == evidence


def test_coverage_batch_rejects_wrong_evidence_source() -> None:
    evidence = make_source_evidence(b"periods", source="other", method="get_trading_periods")
    coverage = MarketCoverageEvidence(
        periods=(make_period(response_hash=evidence.response_hash),),
        suspensions=(),
    )
    with pytest.raises(ValueError, match="source"):
        CoverageBatch(coverage=coverage, source_evidence=(evidence,))


@pytest.mark.parametrize(
    ("coverage_kind", "wrong_method"),
    [("period", "get_price"), ("suspension", "get_trading_periods")],
)
def test_coverage_batch_rejects_wrong_evidence_method(
    coverage_kind: str, wrong_method: str
) -> None:
    evidence = make_source_evidence(method=wrong_method)
    coverage = MarketCoverageEvidence(
        periods=(make_period(response_hash=evidence.response_hash),)
        if coverage_kind == "period"
        else (),
        suspensions=(make_suspension(response_hash=evidence.response_hash),)
        if coverage_kind == "suspension"
        else (),
    )
    with pytest.raises(ValueError, match="evidence"):
        CoverageBatch(coverage=coverage, source_evidence=(evidence,))


def test_coverage_batch_rejects_unreferenced_coverage_hash() -> None:
    evidence = make_source_evidence(b"periods", method="get_trading_periods")
    coverage = MarketCoverageEvidence(
        periods=(make_period(response_hash="f" * 64),),
        suspensions=(),
    )
    with pytest.raises(ValueError, match="response_hash"):
        CoverageBatch(coverage=coverage, source_evidence=(evidence,))


def test_coverage_batch_rejects_evidence_for_an_unrepresented_source() -> None:
    coverage, evidence = valid_coverage_and_evidence()
    unrelated = make_source_evidence(b"other", source="other", method="get_trading_periods")
    with pytest.raises(ValueError, match="source"):
        CoverageBatch(coverage=coverage, source_evidence=(*evidence, unrelated))


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


def test_manifest_detaches_sequences_before_hashing() -> None:
    instruments = ["000001.XSHE"]
    record_hashes = ["a" * 64]
    manifest = make_manifest(instruments=instruments, record_hashes=record_hashes)
    original_hash = manifest.manifest_hash
    instruments.append("000002.XSHE")
    record_hashes[0] = "b" * 64
    assert manifest.instruments == ("000001.XSHE",)
    assert manifest.record_hashes == ("a" * 64,)
    assert manifest.manifest_hash == original_hash


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"source": " "}, "source"),
        ({"instruments": ()}, "instruments"),
        ({"instruments": "000001.XSHE"}, "instruments"),
        ({"instruments": b"000001.XSHE"}, "instruments"),
        ({"instruments": ("",)}, "instruments"),
        ({"instruments": (1,)}, "instruments"),
        ({"instruments": (b"000001.XSHE",)}, "instruments"),
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

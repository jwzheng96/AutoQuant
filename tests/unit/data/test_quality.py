from datetime import UTC, date, datetime, timedelta
from decimal import localcontext
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from autoquant.data.models import (
    MarketCoverageEvidence,
    MinuteBarRevision,
    SuspensionStatus,
    TradingPeriod,
)
from autoquant.data.quality import (
    MinuteBarQualityGate,
    QualityIssue,
    QualityReport,
    QualitySeverity,
)

BAR_END = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)


def make_bar(**updates: object) -> MinuteBarRevision:
    values: dict[str, object] = {
        "source": "rqdata",
        "instrument": "000001.XSHE",
        "event_time": BAR_END,
        "published_at": None,
        "available_at": BAR_END + timedelta(seconds=5),
        "ingested_at": datetime(2026, 7, 21, 8, tzinfo=UTC),
        "source_revision": "initial",
        "availability_policy": "rqdata-minute-v1",
        "open_price": "10",
        "high_price": "10.1",
        "low_price": "9.9",
        "close_price": "10.05",
        "volume": 1000,
        "turnover": "10050",
    }
    values.update(updates)
    return MinuteBarRevision.from_values(**values)  # type: ignore[arg-type]


@pytest.fixture
def bar() -> MinuteBarRevision:
    return make_bar()


def coverage_for(
    bar: MinuteBarRevision,
    *,
    suspended: bool,
    minute_ends: tuple[datetime, ...] | None = None,
    available_at: datetime | None = None,
    session_date: date = date(2026, 7, 20),
) -> MarketCoverageEvidence:
    evidence_available_at = available_at or bar.available_at
    return MarketCoverageEvidence(
        periods=(
            TradingPeriod(
                source=bar.source,
                instrument=bar.instrument,
                session_date=session_date,
                minute_ends=minute_ends or (bar.event_time,),
                available_at=evidence_available_at,
                response_hash="a" * 64,
            ),
        ),
        suspensions=(
            SuspensionStatus(
                source=bar.source,
                instrument=bar.instrument,
                session_date=session_date,
                suspended=suspended,
                available_at=evidence_available_at,
                response_hash="b" * 64,
            ),
        ),
    )


def evaluate(
    records: tuple[MinuteBarRevision, ...],
    *,
    requested_instruments: tuple[str, ...] = ("000001.XSHE",),
    start: datetime = BAR_END,
    end: datetime = BAR_END,
    coverage: MarketCoverageEvidence | None = None,
    as_of: datetime | None = None,
) -> QualityReport:
    return MinuteBarQualityGate().evaluate(
        records=records,
        requested_instruments=requested_instruments,
        start=start,
        end=end,
        coverage=coverage or coverage_for(records[0], suspended=False),
        as_of=as_of,
    )


def issue_codes(report: QualityReport) -> set[str]:
    return {issue.code for issue in report.issues}


def direct_report(
    *,
    issues: tuple[QualityIssue, ...] = (),
    as_of: datetime | None = BAR_END + timedelta(seconds=5),
    production_complete: bool = True,
) -> QualityReport:
    return QualityReport(
        requested_instruments=("000001.XSHE",),
        start=BAR_END,
        end=BAR_END,
        as_of=as_of,
        issues=issues,
        production_complete=production_complete,
    )


def test_public_report_normalizes_error_complete_claim_before_hashing() -> None:
    error = QualityIssue(
        severity=QualitySeverity.ERROR,
        code="test_error",
        instrument="000001.XSHE",
        event_time=BAR_END,
        message="test error",
    )
    claimed_complete = direct_report(issues=(error,), production_complete=True)
    explicit_incomplete = direct_report(issues=(error,), production_complete=False)
    assert claimed_complete.production_complete is False
    assert claimed_complete.report_hash == explicit_incomplete.report_hash


def test_public_report_normalizes_missing_as_of_complete_claim_before_hashing() -> None:
    claimed_complete = direct_report(as_of=None, production_complete=True)
    explicit_incomplete = direct_report(as_of=None, production_complete=False)
    assert claimed_complete.production_complete is False
    assert claimed_complete.report_hash == explicit_incomplete.report_hash


@pytest.mark.parametrize("value", [0, 1, None, "true"])
def test_public_report_requires_a_real_bool_for_production_complete(
    value: object,
) -> None:
    with pytest.raises(TypeError, match="production_complete"):
        direct_report(production_complete=value)  # type: ignore[arg-type]


def test_duplicate_revision_fails_quality_gate(bar: MinuteBarRevision) -> None:
    report = evaluate((bar, bar), as_of=bar.available_at)
    assert report.passed is False
    assert report.production_complete is False
    assert issue_codes(report) == {"duplicate_revision"}


def test_conflicting_ohlc_for_same_source_revision_is_a_schema_error(
    bar: MinuteBarRevision,
) -> None:
    conflict = make_bar(close_price="10.06")
    report = evaluate((bar, conflict), as_of=bar.available_at)
    assert report.passed is False
    assert report.production_complete is False
    assert issue_codes(report) == {"schema_conflict"}


def test_non_monotonic_event_times_fail_quality_gate() -> None:
    first = make_bar()
    second = make_bar(
        event_time=BAR_END + timedelta(minutes=1),
        available_at=BAR_END + timedelta(minutes=1, seconds=5),
        source_revision="second",
    )
    report = evaluate(
        (second, first),
        end=second.event_time,
        coverage=coverage_for(
            second,
            suspended=False,
            minute_ends=(first.event_time, second.event_time),
        ),
    )
    assert report.passed is False
    assert issue_codes(report) == {"non_monotonic_event_time"}


def test_bar_outside_requested_interval_fails_quality_gate(
    bar: MinuteBarRevision,
) -> None:
    report = evaluate(
        (bar,),
        start=bar.event_time + timedelta(minutes=1),
        end=bar.event_time + timedelta(minutes=1),
    )
    assert report.passed is False
    assert "outside_requested_interval" in issue_codes(report)


def test_unexpected_instrument_fails_quality_gate(bar: MinuteBarRevision) -> None:
    unexpected = make_bar(instrument="600000.XSHG")
    report = evaluate(
        (bar, unexpected),
        coverage=coverage_for(bar, suspended=False),
    )
    assert report.passed is False
    assert "unexpected_instrument" in issue_codes(report)


def test_missing_requested_instrument_fails_quality_gate(bar: MinuteBarRevision) -> None:
    missing_instrument = "600000.XSHG"
    coverage = MarketCoverageEvidence(
        periods=(
            *coverage_for(bar, suspended=False).periods,
            TradingPeriod(
                source=bar.source,
                instrument=missing_instrument,
                session_date=date(2026, 7, 20),
                minute_ends=(bar.event_time,),
                available_at=bar.available_at,
                response_hash="c" * 64,
            ),
        ),
        suspensions=(
            *coverage_for(bar, suspended=False).suspensions,
            SuspensionStatus(
                source=bar.source,
                instrument=missing_instrument,
                session_date=date(2026, 7, 20),
                suspended=False,
                available_at=bar.available_at,
                response_hash="d" * 64,
            ),
        ),
    )
    report = evaluate(
        (bar,),
        requested_instruments=(bar.instrument, missing_instrument),
        coverage=coverage,
    )
    assert report.passed is False
    assert "missing_instrument" in issue_codes(report)


def test_missing_expected_bar_and_off_session_bar_are_errors() -> None:
    expected = make_bar()
    off_session = make_bar(
        event_time=BAR_END + timedelta(minutes=1),
        available_at=BAR_END + timedelta(minutes=1, seconds=5),
        source_revision="off-session",
    )
    report = evaluate(
        (off_session,),
        end=off_session.event_time,
        coverage=coverage_for(
            off_session,
            suspended=False,
            minute_ends=(expected.event_time,),
        ),
        as_of=off_session.available_at,
    )
    assert report.passed is False
    assert report.production_complete is False
    assert {"missing_bar", "off_session_bar"} <= issue_codes(report)


def test_shanghai_session_can_start_on_the_previous_utc_date() -> None:
    event_time = datetime(2026, 7, 19, 16, 31, tzinfo=UTC)
    bar = make_bar(
        event_time=event_time,
        available_at=event_time + timedelta(seconds=5),
    )
    report = evaluate(
        (bar,),
        start=event_time,
        end=event_time,
        coverage=coverage_for(
            bar,
            suspended=False,
            session_date=date(2026, 7, 20),
        ),
        as_of=bar.available_at,
    )
    assert report.passed is True
    assert report.production_complete is True


def test_suspended_shanghai_session_at_utc_boundary_accepts_empty_records() -> None:
    event_time = datetime(2026, 7, 19, 16, 31, tzinfo=UTC)
    evidence_bar = make_bar(
        event_time=event_time,
        available_at=event_time + timedelta(seconds=5),
    )
    report = evaluate(
        (),
        start=event_time,
        end=event_time,
        coverage=coverage_for(
            evidence_bar,
            suspended=True,
            session_date=date(2026, 7, 20),
        ),
        as_of=evidence_bar.available_at,
    )
    assert report.passed is True
    assert report.production_complete is True


def test_previous_utc_date_endpoints_participate_in_shanghai_session_checks() -> None:
    expected_time = datetime(2026, 7, 19, 16, 31, tzinfo=UTC)
    actual_time = expected_time + timedelta(minutes=1)
    off_session = make_bar(
        event_time=actual_time,
        available_at=actual_time + timedelta(seconds=5),
    )
    report = evaluate(
        (off_session,),
        start=expected_time,
        end=actual_time,
        coverage=coverage_for(
            off_session,
            suspended=False,
            minute_ends=(expected_time,),
            session_date=date(2026, 7, 20),
        ),
        as_of=off_session.available_at,
    )
    assert {"missing_bar", "off_session_bar"} <= issue_codes(report)


def test_suspended_session_must_not_contain_traded_bars(
    bar: MinuteBarRevision,
) -> None:
    report = evaluate((bar,), coverage=coverage_for(bar, suspended=True))
    assert report.passed is False
    assert "suspended_session_has_bars" in issue_codes(report)


@pytest.mark.parametrize("missing_kind", ["period", "suspension"])
def test_missing_coverage_fails_closed(
    bar: MinuteBarRevision,
    missing_kind: str,
) -> None:
    complete = coverage_for(bar, suspended=False)
    coverage = MarketCoverageEvidence(
        periods=() if missing_kind == "period" else complete.periods,
        suspensions=() if missing_kind == "suspension" else complete.suspensions,
    )
    report = evaluate((bar,), coverage=coverage)
    assert report.passed is False
    assert report.production_complete is False
    assert "missing_coverage" in issue_codes(report)


def test_conflicting_coverage_fails_closed(bar: MinuteBarRevision) -> None:
    period = coverage_for(bar, suspended=False).periods[0]
    conflicting_source = SuspensionStatus(
        source="other-source",
        instrument=bar.instrument,
        session_date=period.session_date,
        suspended=False,
        available_at=bar.available_at,
        response_hash="e" * 64,
    )
    report = evaluate(
        (bar,),
        coverage=MarketCoverageEvidence(
            periods=(period,),
            suspensions=(conflicting_source,),
        ),
    )
    assert report.passed is False
    assert report.production_complete is False
    assert "conflicting_coverage" in issue_codes(report)


def test_coverage_visible_after_batch_cutoff_fails_closed(
    bar: MinuteBarRevision,
) -> None:
    report = evaluate(
        (bar,),
        coverage=coverage_for(
            bar,
            suspended=False,
            available_at=bar.available_at + timedelta(microseconds=1),
        ),
        as_of=bar.available_at,
    )
    assert report.passed is False
    assert report.production_complete is False
    assert "future_coverage" in issue_codes(report)


def test_invalid_interval_is_an_error(bar: MinuteBarRevision) -> None:
    report = evaluate(
        (bar,),
        start=bar.event_time + timedelta(minutes=1),
        end=bar.event_time,
    )
    assert report.passed is False
    assert issue_codes(report) == {"invalid_interval"}


def test_quality_gate_preserves_aware_utc_boundary_contract(
    bar: MinuteBarRevision,
) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate((bar,), start=datetime(2026, 7, 20, 1, 31))
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate((bar,), as_of=datetime(2026, 7, 20, 1, 31, 5))


def test_non_utc_as_of_is_normalized_to_the_same_instant(
    bar: MinuteBarRevision,
) -> None:
    shanghai_as_of = bar.available_at.astimezone(ZoneInfo("Asia/Shanghai"))
    report = evaluate((bar,), as_of=shanghai_as_of)
    assert report.passed is True
    assert report.production_complete is True


def test_missing_as_of_passes_quality_but_is_not_production_complete(
    bar: MinuteBarRevision,
) -> None:
    report = evaluate((bar,))
    assert report.passed is True
    assert report.production_complete is False
    assert report.issues == ()


def test_complete_ordered_batch_passes_and_has_stable_hash() -> None:
    first = make_bar()
    second = make_bar(
        event_time=BAR_END + timedelta(minutes=1),
        available_at=BAR_END + timedelta(minutes=1, seconds=5),
        source_revision="second",
    )
    coverage = coverage_for(
        second,
        suspended=False,
        minute_ends=(first.event_time, second.event_time),
    )
    first_report = evaluate(
        (first, second),
        end=second.event_time,
        coverage=coverage,
        as_of=second.available_at,
    )
    second_report = evaluate(
        (first, second),
        end=second.event_time,
        coverage=coverage,
        as_of=second.available_at,
    )
    assert first_report.passed is True
    assert first_report.production_complete is True
    assert first_report.issues == ()
    assert first_report.report_hash == second_report.report_hash
    assert len(first_report.report_hash) == 64


def test_report_hash_is_bound_to_requested_instruments_and_bounds(
    bar: MinuteBarRevision,
) -> None:
    baseline = evaluate((bar,), as_of=bar.available_at)
    wider_bounds = evaluate(
        (bar,),
        start=bar.event_time - timedelta(minutes=1),
        as_of=bar.available_at,
    )
    later_as_of = evaluate(
        (bar,),
        as_of=bar.available_at + timedelta(seconds=1),
    )
    other_bar = make_bar(instrument="600000.XSHG")
    other_instrument = evaluate(
        (other_bar,),
        requested_instruments=(other_bar.instrument,),
        coverage=coverage_for(other_bar, suspended=False),
        as_of=other_bar.available_at,
    )
    assert (
        baseline.passed
        is wider_bounds.passed
        is later_as_of.passed
        is other_instrument.passed
        is True
    )
    assert len(
        {
            baseline.report_hash,
            wider_bounds.report_hash,
            later_as_of.report_hash,
            other_instrument.report_hash,
        }
    ) == 4


def test_requested_instrument_order_does_not_change_report_hash(
    bar: MinuteBarRevision,
) -> None:
    other_instrument = "600000.XSHG"
    coverage = coverage_for(bar, suspended=False)
    first = evaluate(
        (bar,),
        requested_instruments=(bar.instrument, other_instrument),
        coverage=coverage,
        as_of=bar.available_at,
    )
    second = evaluate(
        (bar,),
        requested_instruments=(other_instrument, bar.instrument),
        coverage=coverage,
        as_of=bar.available_at,
    )
    assert first.report_hash == second.report_hash


def test_requested_instrument_order_does_not_change_invalid_interval_report(
    bar: MinuteBarRevision,
) -> None:
    other_instrument = "600000.XSHG"
    first = evaluate(
        (bar,),
        requested_instruments=(bar.instrument, other_instrument),
        start=bar.event_time + timedelta(minutes=1),
        end=bar.event_time,
    )
    second = evaluate(
        (bar,),
        requested_instruments=(other_instrument, bar.instrument),
        start=bar.event_time + timedelta(minutes=1),
        end=bar.event_time,
    )
    assert first.issues == second.issues
    assert first.report_hash == second.report_hash


def test_duplicate_requested_instruments_are_normalized_once(
    bar: MinuteBarRevision,
) -> None:
    coverage = coverage_for(
        bar,
        suspended=False,
        minute_ends=(bar.event_time, bar.event_time + timedelta(minutes=1)),
    )
    single = evaluate(
        (bar,),
        end=bar.event_time + timedelta(minutes=1),
        coverage=coverage,
        as_of=bar.available_at,
    )
    duplicate = evaluate(
        (bar,),
        requested_instruments=(bar.instrument, bar.instrument),
        end=bar.event_time + timedelta(minutes=1),
        coverage=coverage,
        as_of=bar.available_at,
    )
    assert duplicate.issues == single.issues
    assert duplicate.report_hash == single.report_hash


@given(trailing_zeroes=st.integers(min_value=0, max_value=12))
def test_report_hash_is_stable_across_decimal_contexts(trailing_zeroes: int) -> None:
    decimal_suffix = "." + ("0" * trailing_zeroes) if trailing_zeroes else ""
    with localcontext() as context:
        context.prec = 6
        bar = make_bar(open_price=f"10{decimal_suffix}")
        low_precision = evaluate((bar,), as_of=bar.available_at)
    with localcontext() as context:
        context.prec = 50
        canonical = make_bar(open_price="10")
        high_precision = evaluate((canonical,), as_of=canonical.available_at)
    assert low_precision.passed is True
    assert low_precision.report_hash == high_precision.report_hash


def test_issues_are_sorted_deterministically(bar: MinuteBarRevision) -> None:
    unexpected = make_bar(instrument="600000.XSHG")
    report = evaluate(
        (unexpected, bar, bar),
        start=bar.event_time + timedelta(minutes=1),
        end=bar.event_time + timedelta(minutes=1),
    )
    keys = [(issue.instrument, issue.event_time, issue.code) for issue in report.issues]
    assert keys == sorted(keys)
    assert report.report_hash == evaluate(
        (unexpected, bar, bar),
        start=bar.event_time + timedelta(minutes=1),
        end=bar.event_time + timedelta(minutes=1),
    ).report_hash

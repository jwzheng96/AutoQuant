from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime

from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyDatasetBatch,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.daily_quality import DailyQualityGate
from autoquant.data.models import SourceEvidence

SESSION = date(2026, 7, 20)
EVENT = datetime(2026, 7, 20, 7, 0, tzinfo=UTC)
AVAILABLE = datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
AS_OF = datetime(2026, 7, 22, 8, 0, tzinfo=UTC)
INSTRUMENT = "000001.XSHE"


def evidence(method: str, *, requested_at: datetime = AS_OF) -> SourceEvidence:
    body = f"{method}:{requested_at.isoformat()}".encode()
    return SourceEvidence(
        source="tushare",
        method=method,
        requested_at=requested_at,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )


def make_batch(
    *,
    include_bar: bool = True,
    include_factor: bool = True,
    suspended: bool = False,
    open_session: bool = True,
    include_suspension: bool = True,
    include_lifecycle: bool = True,
    omitted_method: str | None = None,
    coverage_available_at: datetime = AS_OF,
    record_available_at: datetime = AVAILABLE,
) -> DailyDatasetBatch:
    methods = ("daily", "adj_factor", "trade_cal", "stock_basic", "suspend_d")
    evidence_by_method = {
        method: evidence(method) for method in methods if method != omitted_method
    }
    daily_evidence = evidence_by_method.get("daily", evidence("daily"))
    factor_evidence = evidence_by_method.get("adj_factor", evidence("adj_factor"))
    trade_evidence = evidence_by_method.get("trade_cal", evidence("trade_cal"))
    basic_evidence = evidence_by_method.get("stock_basic", evidence("stock_basic"))
    suspend_evidence = evidence_by_method.get("suspend_d", evidence("suspend_d"))

    bars = (
        (
            DailyBarRevision.from_values(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=SESSION,
                event_time=EVENT,
                available_at=record_available_at,
                ingested_at=AS_OF,
                source_revision="tushare:daily:1",
                availability_policy="tushare-daily-v1",
                evidence_hash=daily_evidence.response_hash,
                open_price="10",
                high_price="10.2",
                low_price="9.9",
                close_price="10.1",
                pre_close="9.95",
                volume=100,
                turnover="1000",
            ),
        )
        if include_bar
        else ()
    )
    factors = (
        (
            AdjustmentFactorRevision.from_values(
                source="tushare",
                instrument=INSTRUMENT,
                session_date=SESSION,
                event_time=EVENT,
                available_at=record_available_at,
                ingested_at=AS_OF,
                source_revision="tushare:adj:1",
                availability_policy="tushare-daily-v1",
                evidence_hash=factor_evidence.response_hash,
                factor="123.4",
            ),
        )
        if include_factor
        else ()
    )
    coverage = DailyCoverageEvidence(
        sessions=(
            TradingSession(
                source="tushare",
                session_date=SESSION,
                is_open=open_session,
                available_at=coverage_available_at,
                response_hash=trade_evidence.response_hash,
            ),
        ),
        lifecycles=(
            (
                InstrumentLifecycle(
                    source="tushare",
                    instrument=INSTRUMENT,
                    list_date=date(1991, 4, 3),
                    delist_date=None,
                    available_at=coverage_available_at,
                    response_hash=basic_evidence.response_hash,
                ),
            )
            if include_lifecycle
            else ()
        ),
        suspensions=(
            (
                DailySuspensionStatus(
                    source="tushare",
                    instrument=INSTRUMENT,
                    session_date=SESSION,
                    suspended=suspended,
                    available_at=coverage_available_at,
                    response_hash=suspend_evidence.response_hash,
                ),
            )
            if include_suspension
            else ()
        ),
    )
    return DailyDatasetBatch(
        bars=bars,
        factors=factors,
        coverage=coverage,
        source_evidence=tuple(evidence_by_method.values()),
    )


def evaluate(batch: DailyDatasetBatch, *, as_of: datetime | None = AS_OF):
    return DailyQualityGate().evaluate(
        batch=batch,
        requested_instruments=(INSTRUMENT,),
        start=SESSION,
        end=SESSION,
        as_of=as_of,
    )


def issue_codes(batch: DailyDatasetBatch, *, as_of: datetime | None = AS_OF) -> set[str]:
    return {value.code for value in evaluate(batch, as_of=as_of).issues}


def test_complete_open_session_passes_with_deterministic_hash() -> None:
    first = evaluate(make_batch())
    second = evaluate(make_batch())

    assert first.passed is True
    assert first.production_complete is True
    assert first.issues == ()
    assert first.report_hash == second.report_hash


def test_full_day_suspension_explains_missing_bar_and_factor() -> None:
    report = evaluate(
        make_batch(include_bar=False, include_factor=False, suspended=True)
    )

    assert report.passed is True
    assert report.production_complete is True


def test_closed_session_and_outside_lifecycle_explain_absence() -> None:
    closed = evaluate(
        make_batch(
            include_bar=False,
            include_factor=False,
            open_session=False,
            include_suspension=False,
        )
    )
    before_listing = DailyQualityGate().evaluate(
        batch=make_batch(include_bar=False, include_factor=False),
        requested_instruments=(INSTRUMENT,),
        start=date(1990, 1, 1),
        end=date(1990, 1, 1),
        as_of=AS_OF,
    )

    assert closed.passed is True
    assert before_listing.passed is False
    assert "missing_trading_session" in {value.code for value in before_listing.issues}


def test_unexplained_gap_and_missing_factor_fail_closed() -> None:
    assert "missing_daily_bar" in issue_codes(
        make_batch(include_bar=False, include_factor=False)
    )
    assert "missing_adjustment_factor" in issue_codes(
        make_batch(include_factor=False)
    )


def test_missing_coverage_or_required_endpoint_evidence_fails_closed() -> None:
    assert "missing_suspension_status" in issue_codes(
        make_batch(include_suspension=False)
    )
    assert "missing_lifecycle" in issue_codes(make_batch(include_lifecycle=False))
    assert "missing_suspend_d_evidence" in issue_codes(
        make_batch(include_suspension=False, omitted_method="suspend_d")
    )


def test_future_records_and_coverage_are_not_visible_as_of() -> None:
    future = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)

    assert "record_not_visible" in issue_codes(
        make_batch(record_available_at=future)
    )
    assert "coverage_not_visible" in issue_codes(
        make_batch(coverage_available_at=future)
    )


def test_missing_as_of_can_pass_basic_checks_but_is_never_production_complete() -> None:
    report = evaluate(make_batch(), as_of=None)

    assert report.passed is True
    assert report.production_complete is False

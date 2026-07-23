from __future__ import annotations

from datetime import UTC, date, datetime, time
from unittest.mock import AsyncMock, MagicMock

import pytest

from autoquant.data.daily_models import (
    DailyBarRevision,
    DailyCoverageEvidence,
    TradingSession,
)
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.errors import MarketCalendarUnavailableError, PersistenceUnavailableError
from autoquant.execution.pre_open_marks import DailyClosePreOpenMarkReader

SESSION_DATE = date(2026, 7, 23)
PRE_OPEN = datetime(2026, 7, 23, 1, tzinfo=UTC)
INSTRUMENTS = ("000001.XSHE", "600000.XSHG")


def _session(session_date: date, *, is_open: bool = True) -> TradingSession:
    return TradingSession(
        source="tushare",
        session_date=session_date,
        is_open=is_open,
        available_at=datetime(2026, 7, 1, tzinfo=UTC),
        response_hash="a" * 64,
    )


def _bar(
    instrument: str,
    session_date: date,
    *,
    close: str = "10",
    ingested_at: datetime | None = None,
) -> DailyBarRevision:
    event_time = datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        7,
        tzinfo=UTC,
    )
    return DailyBarRevision.from_values(
        source="tushare",
        instrument=instrument,
        session_date=session_date,
        event_time=event_time,
        available_at=event_time,
        ingested_at=ingested_at or event_time,
        source_revision="test-revision",
        availability_policy="tushare-daily-v1",
        evidence_hash="b" * 64,
        open_price=close,
        high_price=close,
        low_price=close,
        close_price=close,
        pre_close=close,
        volume=100,
        turnover="1000",
    )


def _repository(
    *,
    sessions: tuple[TradingSession, ...],
    bars: tuple[DailyBarRevision, ...],
) -> MagicMock:
    repository = MagicMock()
    repository.query_coverage_as_of = AsyncMock(
        side_effect=(
            DailyCoverageEvidence(
                sessions=tuple(
                    value for value in sessions if value.session_date == SESSION_DATE
                ),
                lifecycles=(),
                suspensions=(),
            ),
            DailyCoverageEvidence(
                sessions=tuple(
                    value for value in sessions if value.session_date < SESSION_DATE
                ),
                lifecycles=(),
                suspensions=(),
            ),
        )
    )
    repository.query_bars_as_of = AsyncMock(return_value=bars)
    return repository


def _reader(
    repository: MagicMock,
    *,
    sessions: tuple[TradingSession, ...],
    bars: tuple[DailyBarRevision, ...],
    back_records_with_manifest: bool = True,
) -> DailyClosePreOpenMarkReader:
    prior_sessions = tuple(
        value for value in sessions if value.session_date < SESSION_DATE
    )
    start_date = min(
        (value.session_date for value in prior_sessions),
        default=SESSION_DATE - date.resolution,
    )
    manifest_as_of = datetime(2026, 7, 22, 8, tzinfo=UTC)
    quality = QualityReport(
        requested_instruments=INSTRUMENTS,
        start=datetime.combine(start_date, time.min, tzinfo=UTC),
        end=datetime.combine(start_date, time(7), tzinfo=UTC),
        as_of=manifest_as_of,
        issues=(),
        production_complete=True,
    )
    record_hashes = (
        tuple(value.content_hash for value in (*bars, *prior_sessions))
        if back_records_with_manifest
        else ()
    )
    manifest = DatasetManifest(
        source="tushare",
        instruments=INSTRUMENTS,
        start_time=datetime.combine(start_date, time.min, tzinfo=UTC),
        end_time=datetime(2026, 7, 22, 7, tzinfo=UTC),
        as_of=manifest_as_of,
        record_hashes=record_hashes,
        quality_report_hash=quality.report_hash,
        production_complete=True,
        row_count=len(record_hashes),
    )
    evidence = MagicMock()
    evidence.read_manifest = AsyncMock(return_value=manifest)
    evidence.read_quality_report = AsyncMock(return_value=quality)

    async def source_evidence(evidence_hash: str) -> MagicMock:
        item = MagicMock()
        item.response_hash = evidence_hash
        item.source = "tushare"
        item.method = "trade_cal" if evidence_hash == "a" * 64 else "daily"
        item.requested_at = datetime(2026, 7, 1, tzinfo=UTC)
        return item

    evidence.read_source_evidence = AsyncMock(side_effect=source_evidence)
    return DailyClosePreOpenMarkReader(
        repository=repository,
        evidence_repository=evidence,
        manifest_hash=manifest.manifest_hash,
        source="tushare",
    )


@pytest.mark.asyncio
async def test_reader_selects_latest_complete_visible_session_and_commits_evidence() -> None:
    prior = date(2026, 7, 22)
    repository = _repository(
        sessions=(_session(prior), _session(SESSION_DATE)),
        bars=(
            _bar(INSTRUMENTS[0], prior, close="12.34"),
            _bar(INSTRUMENTS[1], prior, close="9.87"),
        ),
    )
    reader = _reader(
        repository,
        sessions=(_session(prior), _session(SESSION_DATE)),
        bars=(
            _bar(INSTRUMENTS[0], prior, close="12.34"),
            _bar(INSTRUMENTS[1], prior, close="9.87"),
        ),
    )

    marks = await reader(SESSION_DATE, tuple(reversed(INSTRUMENTS)), PRE_OPEN)

    assert marks.session_date == SESSION_DATE
    assert marks.valuation_session_date == prior
    assert tuple(marks.marks) == INSTRUMENTS
    assert str(marks.marks[INSTRUMENTS[0]]) == "12.34"
    assert len(marks.source_evidence_hash) == 64
    assert len(marks.marks_hash) == 64
    repository.query_bars_as_of.assert_awaited_once()


@pytest.mark.asyncio
async def test_reader_falls_back_only_to_an_older_complete_common_session() -> None:
    latest = date(2026, 7, 22)
    older = date(2026, 7, 21)
    repository = _repository(
        sessions=(_session(older), _session(latest), _session(SESSION_DATE)),
        bars=(
            _bar(INSTRUMENTS[0], latest),
            _bar(INSTRUMENTS[0], older, close="8"),
            _bar(INSTRUMENTS[1], older, close="9"),
        ),
    )
    reader = _reader(
        repository,
        sessions=(_session(older), _session(latest), _session(SESSION_DATE)),
        bars=(
            _bar(INSTRUMENTS[0], latest),
            _bar(INSTRUMENTS[0], older, close="8"),
            _bar(INSTRUMENTS[1], older, close="9"),
        ),
    )

    marks = await reader(SESSION_DATE, INSTRUMENTS, PRE_OPEN)

    assert marks.valuation_session_date == older
    assert {str(value) for value in marks.marks.values()} == {"8", "9"}


@pytest.mark.asyncio
async def test_reader_rejects_mixed_session_marks_without_a_complete_common_date() -> None:
    latest = date(2026, 7, 22)
    older = date(2026, 7, 21)
    repository = _repository(
        sessions=(_session(older), _session(latest), _session(SESSION_DATE)),
        bars=(
            _bar(INSTRUMENTS[0], latest),
            _bar(INSTRUMENTS[1], older),
        ),
    )
    reader = _reader(
        repository,
        sessions=(_session(older), _session(latest), _session(SESSION_DATE)),
        bars=(
            _bar(INSTRUMENTS[0], latest),
            _bar(INSTRUMENTS[1], older),
        ),
    )

    with pytest.raises(
        PersistenceUnavailableError,
        match="no complete visible pre-open valuation session",
    ):
        await reader(SESSION_DATE, INSTRUMENTS, PRE_OPEN)


@pytest.mark.asyncio
async def test_reader_rejects_backdated_bar_ingested_after_query_time() -> None:
    prior = date(2026, 7, 22)
    repository = _repository(
        sessions=(_session(prior), _session(SESSION_DATE)),
        bars=(
            _bar(
                INSTRUMENTS[0],
                prior,
                ingested_at=datetime(2026, 7, 23, 2, tzinfo=UTC),
            ),
            _bar(INSTRUMENTS[1], prior),
        ),
    )
    bars = (
        _bar(
            INSTRUMENTS[0],
            prior,
            ingested_at=datetime(2026, 7, 23, 2, tzinfo=UTC),
        ),
        _bar(INSTRUMENTS[1], prior),
    )
    reader = _reader(
        repository,
        sessions=(_session(prior), _session(SESSION_DATE)),
        bars=bars,
    )

    with pytest.raises(PersistenceUnavailableError, match="evidence is inconsistent"):
        await reader(SESSION_DATE, INSTRUMENTS, PRE_OPEN)


@pytest.mark.asyncio
async def test_reader_requires_point_in_time_proof_that_current_session_is_open() -> None:
    prior = date(2026, 7, 22)
    repository = _repository(
        sessions=(_session(prior), _session(SESSION_DATE, is_open=False)),
        bars=tuple(_bar(instrument, prior) for instrument in INSTRUMENTS),
    )
    reader = _reader(
        repository,
        sessions=(_session(prior), _session(SESSION_DATE, is_open=False)),
        bars=tuple(_bar(instrument, prior) for instrument in INSTRUMENTS),
    )

    with pytest.raises(MarketCalendarUnavailableError, match="not proven open"):
        await reader(SESSION_DATE, INSTRUMENTS, PRE_OPEN)


@pytest.mark.asyncio
async def test_reader_rejects_use_outside_the_pre_open_window() -> None:
    repository = _repository(sessions=(), bars=())
    reader = _reader(repository, sessions=(), bars=())

    with pytest.raises(ValueError, match="pre-open window"):
        await reader(
            SESSION_DATE,
            INSTRUMENTS,
            datetime(2026, 7, 23, 1, 15, tzinfo=UTC),
        )
    repository.query_coverage_as_of.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_rejects_rows_absent_from_production_manifest() -> None:
    prior = date(2026, 7, 22)
    sessions = (_session(prior), _session(SESSION_DATE))
    bars = tuple(_bar(instrument, prior) for instrument in INSTRUMENTS)
    repository = _repository(sessions=sessions, bars=bars)
    reader = _reader(
        repository,
        sessions=sessions,
        bars=bars,
        back_records_with_manifest=False,
    )

    with pytest.raises(PersistenceUnavailableError, match="production manifest"):
        await reader(SESSION_DATE, INSTRUMENTS, PRE_OPEN)

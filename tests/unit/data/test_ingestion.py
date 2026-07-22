from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest

from open_quant.data.ingestion import (
    IngestionRequest,
    IngestionService,
    ValidatedDatasetReader,
)
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
from open_quant.data.quality import MinuteBarQualityGate, QualityReport
from open_quant.errors import PersistenceUnavailableError

EVENT_TIME = datetime(2026, 7, 20, 1, 31, tzinfo=UTC)
AS_OF = datetime(2026, 7, 21, 8, tzinfo=UTC)


def evidence(method: str, body: bytes) -> SourceEvidence:
    return SourceEvidence(
        source="rqdata",
        method=method,
        requested_at=AS_OF,
        response_body=body,
        response_hash=hashlib.sha256(body).hexdigest(),
    )


def bar() -> MinuteBarRevision:
    return MinuteBarRevision.from_values(
        source="rqdata",
        instrument="000001.XSHE",
        event_time=EVENT_TIME,
        published_at=None,
        available_at=EVENT_TIME.replace(second=5),
        ingested_at=AS_OF,
        source_revision="initial",
        availability_policy="rqdata-minute-v1",
        open_price="10",
        high_price="10.1",
        low_price="9.9",
        close_price="10.05",
        volume=1000,
        turnover="10050",
    )


class RecordingSource:
    def __init__(self, calls: list[str], *, complete: bool) -> None:
        self.calls = calls
        self.complete = complete
        self.price_evidence = evidence("get_price", b"price")
        self.period_evidence = evidence("get_trading_periods", b"period")
        self.suspension_evidence = evidence("is_suspended", b"suspension")

    async def fetch_minute_bars(
        self, instruments: tuple[str, ...], start: datetime, end: datetime
    ) -> MinuteBarBatch:
        self.calls.append("fetch_bars")
        return MinuteBarBatch(records=(bar(),), source_evidence=(self.price_evidence,))

    async def fetch_coverage_evidence(
        self, instruments: tuple[str, ...], start: datetime, end: datetime
    ) -> CoverageBatch:
        self.calls.append("fetch_coverage")
        item = bar()
        periods = (
            TradingPeriod(
                source="rqdata",
                instrument=item.instrument,
                session_date=datetime(2026, 7, 20, tzinfo=UTC).date(),
                minute_ends=(item.event_time,),
                available_at=AS_OF,
                response_hash=self.period_evidence.response_hash,
            ),
        ) if self.complete else ()
        suspensions = (
            SuspensionStatus(
                source="rqdata",
                instrument=item.instrument,
                session_date=datetime(2026, 7, 20, tzinfo=UTC).date(),
                suspended=False,
                available_at=AS_OF,
                response_hash=self.suspension_evidence.response_hash,
            ),
        ) if self.complete else ()
        return CoverageBatch(
            coverage=MarketCoverageEvidence(periods=periods, suspensions=suspensions),
            source_evidence=(self.period_evidence, self.suspension_evidence),
        )


class RecordingBars:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.records: tuple[MinuteBarRevision, ...] = ()

    async def append(self, records: tuple[MinuteBarRevision, ...]) -> int:
        self.calls.append("append_raw_bars")
        self.records = records
        return len(records)

    async def query_as_of(
        self, instruments: tuple[str, ...], start: datetime, end: datetime, as_of: datetime
    ) -> tuple[MinuteBarRevision, ...]:
        self.calls.append("query_bars")
        return self.records


class RecordingControl:
    def __init__(self, calls: list[str], *, fail_finalization: bool = False) -> None:
        self.calls = calls
        self.fail_finalization = fail_finalization
        self.manifests: dict[str, DatasetManifest] = {}
        self.reports: dict[str, QualityReport] = {}

    async def save_source_evidence(self, item: SourceEvidence) -> None:
        if not self.calls or self.calls[-1] != "save_source_evidence":
            self.calls.append("save_source_evidence")

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[RecordingControl]:
        self.calls.append("begin_finalization")
        try:
            yield self
            if self.fail_finalization:
                raise PersistenceUnavailableError("finalization unavailable")
            self.calls.append("commit_finalization")
        except Exception:
            self.calls.append("rollback_finalization")
            raise

    async def save_quality_report(self, report: QualityReport) -> None:
        self.calls.append("save_quality_report")
        self.reports[report.report_hash] = report

    async def save_manifest(self, manifest: DatasetManifest) -> None:
        self.calls.append("save_manifest")
        self.manifests[manifest.manifest_hash] = manifest

    async def advance_checkpoint(
        self,
        source: str,
        stream: str,
        instrument: str,
        event_time: datetime,
        content_hash: str,
    ) -> None:
        self.calls.append("advance_checkpoint")

    async def append_audit_event(
        self, event_type: str, occurred_at: datetime, payload: object
    ) -> str:
        self.calls.append("audit_rejection" if event_type.endswith("rejected") else "audit_success")
        return "f" * 64

    async def read_manifest(self, manifest_hash: str) -> DatasetManifest:
        self.calls.append("read_manifest")
        return self.manifests[manifest_hash]

    async def read_quality_report(self, report_hash: str) -> QualityReport:
        self.calls.append("read_quality_report")
        return self.reports[report_hash]


def service(*, complete: bool, fail_finalization: bool = False) -> tuple[
    IngestionService, IngestionRequest, list[str], RecordingBars, RecordingControl
]:
    calls: list[str] = []
    bars = RecordingBars(calls)
    control = RecordingControl(calls, fail_finalization=fail_finalization)
    item = IngestionService(
        source=RecordingSource(calls, complete=complete),
        quality_gate=MinuteBarQualityGate(),
        minute_repository=bars,
        control_repository=control,
        now=lambda: AS_OF,
    )
    request = IngestionRequest(
        instruments=("000001.XSHE",),
        start=EVENT_TIME,
        end=EVENT_TIME,
        as_of=AS_OF,
        production_complete_requested=True,
    )
    return item, request, calls, bars, control


@pytest.mark.asyncio
async def test_failed_quality_retains_raw_data_but_never_manifest_or_checkpoint() -> None:
    item, request, calls, _, _ = service(complete=False)

    result = await item.run(request)

    assert result.status == "quality_rejected"
    assert calls == [
        "fetch_bars",
        "fetch_coverage",
        "save_source_evidence",
        "append_raw_bars",
        "begin_finalization",
        "save_quality_report",
        "audit_rejection",
        "commit_finalization",
    ]
    assert result.manifest_hash is None


@pytest.mark.asyncio
async def test_passing_quality_finalizes_manifest_checkpoint_and_audit_atomically() -> None:
    item, request, calls, _, _ = service(complete=True)

    result = await item.run(request)

    assert result.status == "completed"
    assert result.manifest_hash is not None
    assert calls == [
        "fetch_bars",
        "fetch_coverage",
        "save_source_evidence",
        "append_raw_bars",
        "begin_finalization",
        "save_quality_report",
        "save_manifest",
        "advance_checkpoint",
        "audit_success",
        "commit_finalization",
    ]


@pytest.mark.asyncio
async def test_finalization_failure_rolls_back_checkpoint_and_returns_failure() -> None:
    item, request, calls, _, _ = service(complete=True, fail_finalization=True)

    result = await item.run(request)

    assert result.status == "persistence_failed"
    assert calls[-1] == "rollback_finalization"
    assert result.manifest_hash is None


def test_request_rejects_unsafe_bounds_and_empty_instruments() -> None:
    with pytest.raises(ValueError, match="instruments"):
        IngestionRequest((), EVENT_TIME, EVENT_TIME, AS_OF, True)
    with pytest.raises(ValueError, match="start"):
        IngestionRequest(("x",), AS_OF, EVENT_TIME, AS_OF, True)
    with pytest.raises(ValueError, match="as_of"):
        IngestionRequest(("x",), EVENT_TIME, AS_OF, EVENT_TIME, True)


@pytest.mark.asyncio
async def test_validated_reader_requires_complete_manifest_and_exact_record_hashes() -> None:
    item, request, _, bars, control = service(complete=True)
    result = await item.run(request)
    assert result.manifest_hash is not None
    reader = ValidatedDatasetReader(control_repository=control, minute_repository=bars)

    records = await reader.query(result.manifest_hash, AS_OF)

    assert records == (bar(),)
    assert "read_quality_report" in control.calls
    manifest = control.manifests[result.manifest_hash]
    forged = object.__new__(DatasetManifest)
    for name in manifest.__dataclass_fields__:
        object.__setattr__(forged, name, getattr(manifest, name))
    object.__setattr__(forged, "production_complete", False)
    control.manifests[result.manifest_hash] = forged
    with pytest.raises(ValueError, match="production-complete"):
        await reader.query(result.manifest_hash, AS_OF)

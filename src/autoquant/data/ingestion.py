from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from autoquant.clock import to_utc
from autoquant.data.models import DatasetManifest, MinuteBarRevision, SourceEvidence
from autoquant.data.ports import MarketDataSource, MinuteBarRepository
from autoquant.data.quality import MinuteBarQualityGate, QualityReport
from autoquant.errors import PersistenceUnavailableError


class ControlTransaction(Protocol):
    async def save_quality_report(self, report: QualityReport) -> None: ...

    async def save_manifest(self, manifest: DatasetManifest) -> None: ...

    async def advance_checkpoint(
        self,
        source: str,
        stream: str,
        instrument: str,
        event_time: datetime,
        content_hash: str,
    ) -> None: ...

    async def append_audit_event(
        self, event_type: str, occurred_at: datetime, payload: object
    ) -> str: ...


class ControlRepository(Protocol):
    async def save_source_evidence(self, evidence: SourceEvidence) -> None: ...

    def transaction(self) -> AbstractAsyncContextManager[ControlTransaction]: ...

    async def read_manifest(self, manifest_hash: str) -> DatasetManifest: ...

    async def read_quality_report(self, report_hash: str) -> QualityReport: ...


@dataclass(frozen=True, slots=True)
class IngestionRequest:
    instruments: tuple[str, ...]
    start: datetime
    end: datetime
    as_of: datetime
    production_complete_requested: bool

    def __post_init__(self) -> None:
        if isinstance(self.instruments, (str, bytes)):
            raise ValueError("instruments must be a sequence of instrument strings")
        instruments = tuple(self.instruments)
        if not instruments or any(
            not isinstance(instrument, str) or not instrument.strip()
            for instrument in instruments
        ):
            raise ValueError("instruments cannot be empty")
        if len(set(instruments)) != len(instruments):
            raise ValueError("instruments must be unique")
        if type(self.production_complete_requested) is not bool:
            raise TypeError("production_complete_requested must be a bool")
        start = to_utc(self.start, name="start")
        end = to_utc(self.end, name="end")
        as_of = to_utc(self.as_of, name="as_of")
        if start > end:
            raise ValueError("start cannot follow end")
        if as_of < end:
            raise ValueError("as_of cannot precede end")
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "as_of", as_of)


@dataclass(frozen=True, slots=True)
class IngestionResult:
    status: str
    fetched_count: int
    persisted_count: int
    quality_hash: str | None
    manifest_hash: str | None


class IngestionService:
    def __init__(
        self,
        *,
        source: MarketDataSource,
        quality_gate: MinuteBarQualityGate,
        minute_repository: MinuteBarRepository,
        control_repository: ControlRepository,
        now: Callable[[], datetime],
    ) -> None:
        self._source = source
        self._quality_gate = quality_gate
        self._minute_repository = minute_repository
        self._control_repository = control_repository
        self._now = now

    async def run(self, request: IngestionRequest) -> IngestionResult:
        if not isinstance(request, IngestionRequest):
            raise TypeError("request must be IngestionRequest")
        bars = await self._source.fetch_minute_bars(
            request.instruments, request.start, request.end
        )
        coverage = await self._source.fetch_coverage_evidence(
            request.instruments, request.start, request.end
        )
        all_evidence = bars.source_evidence + coverage.source_evidence
        try:
            for item in all_evidence:
                await self._control_repository.save_source_evidence(item)
            persisted_count = await self._minute_repository.append(bars.records)
        except PersistenceUnavailableError:
            return IngestionResult(
                status="persistence_failed",
                fetched_count=len(bars.records),
                persisted_count=0,
                quality_hash=None,
                manifest_hash=None,
            )

        report = self._quality_gate.evaluate(
            records=bars.records,
            requested_instruments=request.instruments,
            start=request.start,
            end=request.end,
            coverage=coverage.coverage,
            as_of=request.as_of,
        )
        if not report.passed or (
            request.production_complete_requested and not report.production_complete
        ):
            try:
                async with self._control_repository.transaction() as transaction:
                    await transaction.save_quality_report(report)
                    await transaction.append_audit_event(
                        "ingestion_rejected",
                        to_utc(self._now(), name="audit time"),
                        {
                            "quality_hash": report.report_hash,
                            "requested_instruments": list(request.instruments),
                            "row_count": len(bars.records),
                        },
                    )
            except PersistenceUnavailableError:
                return IngestionResult(
                    status="persistence_failed",
                    fetched_count=len(bars.records),
                    persisted_count=persisted_count,
                    quality_hash=report.report_hash,
                    manifest_hash=None,
                )
            return IngestionResult(
                status="quality_rejected",
                fetched_count=len(bars.records),
                persisted_count=persisted_count,
                quality_hash=report.report_hash,
                manifest_hash=None,
            )

        manifest = DatasetManifest(
            source=self._single_source(bars.records),
            instruments=request.instruments,
            start_time=request.start,
            end_time=request.end,
            as_of=request.as_of,
            record_hashes=tuple(record.content_hash for record in bars.records),
            quality_report_hash=report.report_hash,
            production_complete=(
                request.production_complete_requested and report.production_complete
            ),
            row_count=len(bars.records),
        )
        try:
            async with self._control_repository.transaction() as transaction:
                await transaction.save_quality_report(report)
                await transaction.save_manifest(manifest)
                for instrument in request.instruments:
                    records = [
                        record for record in bars.records if record.instrument == instrument
                    ]
                    latest = max(
                        records,
                        key=lambda record: (record.event_time, record.content_hash),
                    )
                    await transaction.advance_checkpoint(
                        latest.source,
                        "minute",
                        latest.instrument,
                        latest.event_time,
                        latest.content_hash,
                    )
                await transaction.append_audit_event(
                    "ingestion_completed",
                    to_utc(self._now(), name="audit time"),
                    {
                        "manifest_hash": manifest.manifest_hash,
                        "quality_hash": report.report_hash,
                        "row_count": len(bars.records),
                    },
                )
        except PersistenceUnavailableError:
            return IngestionResult(
                status="persistence_failed",
                fetched_count=len(bars.records),
                persisted_count=persisted_count,
                quality_hash=report.report_hash,
                manifest_hash=None,
            )
        return IngestionResult(
            status="completed",
            fetched_count=len(bars.records),
            persisted_count=persisted_count,
            quality_hash=report.report_hash,
            manifest_hash=manifest.manifest_hash,
        )

    @staticmethod
    def _single_source(records: tuple[MinuteBarRevision, ...]) -> str:
        sources = {record.source for record in records}
        if len(sources) != 1:
            raise ValueError("passing ingestion must contain exactly one source")
        return next(iter(sources))


class ValidatedDatasetReader:
    def __init__(
        self,
        *,
        control_repository: ControlRepository,
        minute_repository: MinuteBarRepository,
    ) -> None:
        self._control_repository = control_repository
        self._minute_repository = minute_repository

    async def query(
        self, manifest_hash: str, as_of: datetime
    ) -> tuple[MinuteBarRevision, ...]:
        cutoff = to_utc(as_of, name="as_of")
        manifest = await self._control_repository.read_manifest(manifest_hash)
        if not manifest.production_complete:
            raise ValueError("research requires a production-complete manifest")
        report = await self._control_repository.read_quality_report(
            manifest.quality_report_hash
        )
        if (
            report.report_hash != manifest.quality_report_hash
            or not report.passed
            or not report.production_complete
        ):
            raise ValueError("research requires a passing production-complete quality report")
        if cutoff > manifest.as_of:
            raise ValueError("as_of cannot exceed the manifest cutoff")
        records = await self._minute_repository.query_as_of(
            manifest.instruments,
            manifest.start_time,
            manifest.end_time,
            cutoff,
        )
        if tuple(record.content_hash for record in records) != manifest.record_hashes:
            raise PersistenceUnavailableError(
                "validated dataset rows do not match the manifest"
            )
        return records

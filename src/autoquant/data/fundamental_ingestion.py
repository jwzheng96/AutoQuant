from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time

from autoquant.clock import SHANGHAI, to_utc
from autoquant.data.fundamental_models import FundamentalDatasetBatch
from autoquant.data.fundamental_ports import (
    FundamentalDataSource,
    FundamentalRepository,
)
from autoquant.data.fundamental_quality import FundamentalQualityGate
from autoquant.data.ingestion import ControlRepository
from autoquant.data.models import DatasetManifest
from autoquant.errors import PersistenceUnavailableError


@dataclass(frozen=True, slots=True)
class FundamentalIngestionRequest:
    instruments: tuple[str, ...]
    start: date
    end: date
    as_of: datetime | None
    production_complete_requested: bool

    def __post_init__(self) -> None:
        instruments = tuple(self.instruments)
        object.__setattr__(self, "instruments", instruments)
        if (
            not instruments
            or len(set(instruments)) != len(instruments)
            or any(
                not isinstance(value, str) or not value.strip()
                for value in instruments
            )
        ):
            raise ValueError(
                "instruments must be nonempty and unique"
            )
        if self.start > self.end:
            raise ValueError("start cannot follow end")
        if self.as_of is not None:
            cutoff = to_utc(self.as_of, name="as_of")
            object.__setattr__(self, "as_of", cutoff)
            if cutoff < _event_time(self.end):
                raise ValueError(
                    "as_of cannot precede requested end"
                )
        if type(self.production_complete_requested) is not bool:
            raise TypeError(
                "production_complete_requested must be a bool"
            )


@dataclass(frozen=True, slots=True)
class FundamentalIngestionResult:
    status: str
    fetched_valuations: int
    fetched_indicators: int
    persisted_valuations: int
    persisted_indicators: int
    quality_hash: str | None
    manifest_hash: str | None


class FundamentalIngestionService:
    def __init__(
        self,
        *,
        source: FundamentalDataSource,
        quality_gate: FundamentalQualityGate,
        fundamental_repository: FundamentalRepository,
        control_repository: ControlRepository,
        now: Callable[[], datetime],
    ) -> None:
        self._source = source
        self._quality_gate = quality_gate
        self._fundamentals = fundamental_repository
        self._control = control_repository
        self._now = now

    async def run(
        self,
        request: FundamentalIngestionRequest,
    ) -> FundamentalIngestionResult:
        if not isinstance(request, FundamentalIngestionRequest):
            raise TypeError(
                "request must be FundamentalIngestionRequest"
            )
        batch = await self._source.fetch_fundamental_dataset(
            request.instruments,
            request.start,
            request.end,
        )
        effective_as_of = (
            to_utc(self._now(), name="as_of")
            if request.as_of is None
            else request.as_of
        )
        if effective_as_of < _event_time(request.end):
            raise ValueError("as_of cannot precede requested end")
        persisted_valuations = 0
        persisted_indicators = 0
        try:
            for evidence in batch.source_evidence:
                await self._control.save_source_evidence(evidence)
            persisted_valuations = (
                await self._fundamentals.append_valuations(
                    batch.valuations
                )
            )
            persisted_indicators = (
                await self._fundamentals.append_indicators(
                    batch.indicators
                )
            )
        except PersistenceUnavailableError:
            return self._result(
                "persistence_failed",
                batch,
                persisted_valuations,
                persisted_indicators,
            )

        report = self._quality_gate.evaluate(
            batch=batch,
            requested_instruments=request.instruments,
            start=request.start,
            end=request.end,
            as_of=effective_as_of,
        )
        if not report.passed or (
            request.production_complete_requested
            and not report.production_complete
        ):
            try:
                async with self._control.transaction() as transaction:
                    await transaction.save_quality_report(report)
                    await transaction.append_audit_event(
                        "fundamental_ingestion_rejected",
                        to_utc(self._now(), name="audit time"),
                        {
                            "indicator_count": len(batch.indicators),
                            "quality_hash": report.report_hash,
                            "valuation_count": len(batch.valuations),
                        },
                    )
            except PersistenceUnavailableError:
                return self._result(
                    "persistence_failed",
                    batch,
                    persisted_valuations,
                    persisted_indicators,
                    quality_hash=report.report_hash,
                )
            return self._result(
                "quality_rejected",
                batch,
                persisted_valuations,
                persisted_indicators,
                quality_hash=report.report_hash,
            )

        record_hashes = tuple(
            value.content_hash for value in batch.valuations
        ) + tuple(
            value.content_hash for value in batch.indicators
        )
        manifest = DatasetManifest(
            source="tushare-fundamental",
            instruments=request.instruments,
            start_time=_start_time(request.start),
            end_time=_event_time(request.end),
            as_of=effective_as_of,
            record_hashes=record_hashes,
            quality_report_hash=report.report_hash,
            production_complete=(
                request.production_complete_requested
                and report.production_complete
            ),
            row_count=len(record_hashes),
        )
        try:
            async with self._control.transaction() as transaction:
                await transaction.save_quality_report(report)
                await transaction.save_manifest(manifest)
                for instrument in request.instruments:
                    valuations = tuple(
                        value
                        for value in batch.valuations
                        if value.instrument == instrument
                    )
                    if valuations:
                        latest_valuation = max(
                            valuations,
                            key=lambda value: (
                                value.event_time,
                                value.content_hash,
                            ),
                        )
                        await transaction.advance_checkpoint(
                            "tushare",
                            "daily-basic",
                            instrument,
                            latest_valuation.event_time,
                            latest_valuation.content_hash,
                        )
                    indicators = tuple(
                        value
                        for value in batch.indicators
                        if value.instrument == instrument
                    )
                    if indicators:
                        latest_indicator = max(
                            indicators,
                            key=lambda value: (
                                value.event_time,
                                value.content_hash,
                            ),
                        )
                        await transaction.advance_checkpoint(
                            "tushare",
                            "fina-indicator",
                            instrument,
                            latest_indicator.event_time,
                            latest_indicator.content_hash,
                        )
                await transaction.append_audit_event(
                    "fundamental_ingestion_completed",
                    to_utc(self._now(), name="audit time"),
                    {
                        "indicator_count": len(batch.indicators),
                        "manifest_hash": manifest.manifest_hash,
                        "quality_hash": report.report_hash,
                        "valuation_count": len(batch.valuations),
                    },
                )
        except PersistenceUnavailableError:
            return self._result(
                "persistence_failed",
                batch,
                persisted_valuations,
                persisted_indicators,
                quality_hash=report.report_hash,
            )
        return self._result(
            "completed",
            batch,
            persisted_valuations,
            persisted_indicators,
            quality_hash=report.report_hash,
            manifest_hash=manifest.manifest_hash,
        )

    @staticmethod
    def _result(
        status: str,
        batch: FundamentalDatasetBatch,
        persisted_valuations: int,
        persisted_indicators: int,
        *,
        quality_hash: str | None = None,
        manifest_hash: str | None = None,
    ) -> FundamentalIngestionResult:
        return FundamentalIngestionResult(
            status=status,
            fetched_valuations=len(batch.valuations),
            fetched_indicators=len(batch.indicators),
            persisted_valuations=persisted_valuations,
            persisted_indicators=persisted_indicators,
            quality_hash=quality_hash,
            manifest_hash=manifest_hash,
        )


def _start_time(value: date) -> datetime:
    return to_utc(
        datetime.combine(value, time.min, tzinfo=SHANGHAI)
    )


def _event_time(value: date) -> datetime:
    return to_utc(
        datetime.combine(value, time(15), tzinfo=SHANGHAI)
    )

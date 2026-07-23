from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time

from autoquant.clock import SHANGHAI, to_shanghai, to_utc
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyDatasetBatch,
)
from autoquant.data.daily_ports import DailyDataSource, DailyMarketRepository
from autoquant.data.daily_quality import DailyQualityGate
from autoquant.data.ingestion import ControlRepository
from autoquant.data.models import DatasetManifest
from autoquant.errors import (
    ManifestIntegrityError,
    PersistenceUnavailableError,
)


@dataclass(frozen=True, slots=True)
class DailyIngestionRequest:
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
            or any(not isinstance(value, str) or not value.strip() for value in instruments)
            or len(set(instruments)) != len(instruments)
        ):
            raise ValueError("instruments must be nonempty and unique")
        if self.start > self.end:
            raise ValueError("start cannot follow end")
        if self.as_of is not None:
            as_of = to_utc(self.as_of, name="as_of")
            object.__setattr__(self, "as_of", as_of)
            if as_of < _event_time(self.end):
                raise ValueError("as_of cannot precede the requested end")
        if type(self.production_complete_requested) is not bool:
            raise TypeError("production_complete_requested must be a bool")


@dataclass(frozen=True, slots=True)
class DailyIngestionResult:
    status: str
    fetched_bars: int
    fetched_factors: int
    persisted_bars: int
    persisted_factors: int
    quality_hash: str | None
    manifest_hash: str | None


@dataclass(frozen=True, slots=True)
class ValidatedDailyDataset:
    bars: tuple[DailyBarRevision, ...]
    factors: tuple[AdjustmentFactorRevision, ...]
    coverage: DailyCoverageEvidence


class DailyIngestionService:
    def __init__(
        self,
        *,
        source: DailyDataSource,
        quality_gate: DailyQualityGate,
        market_repository: DailyMarketRepository,
        control_repository: ControlRepository,
        now: Callable[[], datetime],
    ) -> None:
        self._source = source
        self._quality_gate = quality_gate
        self._market_repository = market_repository
        self._control_repository = control_repository
        self._now = now

    async def run(self, request: DailyIngestionRequest) -> DailyIngestionResult:
        if not isinstance(request, DailyIngestionRequest):
            raise TypeError("request must be DailyIngestionRequest")
        dataset = await self._source.fetch_daily_dataset(
            request.instruments, request.start, request.end
        )
        effective_as_of = (
            to_utc(self._now(), name="as_of")
            if request.as_of is None
            else request.as_of
        )
        if effective_as_of < _event_time(request.end):
            raise ValueError("as_of cannot precede the requested end")
        persisted_bars = 0
        persisted_factors = 0
        try:
            for evidence in dataset.source_evidence:
                await self._control_repository.save_source_evidence(evidence)
            persisted_bars = await self._market_repository.append_bars(dataset.bars)
            persisted_factors = await self._market_repository.append_factors(
                dataset.factors
            )
            await self._market_repository.append_coverage(dataset.coverage)
        except PersistenceUnavailableError:
            return self._result(
                "persistence_failed",
                dataset,
                persisted_bars,
                persisted_factors,
            )

        report = self._quality_gate.evaluate(
            batch=dataset,
            requested_instruments=request.instruments,
            start=request.start,
            end=request.end,
            as_of=effective_as_of,
        )
        if not report.passed or (
            request.production_complete_requested and not report.production_complete
        ):
            try:
                async with self._control_repository.transaction() as transaction:
                    await transaction.save_quality_report(report)
                    await transaction.append_audit_event(
                        "daily_ingestion_rejected",
                        to_utc(self._now(), name="audit time"),
                        {
                            "quality_hash": report.report_hash,
                            "bar_count": len(dataset.bars),
                            "factor_count": len(dataset.factors),
                        },
                    )
            except PersistenceUnavailableError:
                return self._result(
                    "persistence_failed",
                    dataset,
                    persisted_bars,
                    persisted_factors,
                    quality_hash=report.report_hash,
                )
            return self._result(
                "quality_rejected",
                dataset,
                persisted_bars,
                persisted_factors,
                quality_hash=report.report_hash,
            )

        record_hashes = tuple(value.content_hash for value in dataset.bars) + tuple(
            value.content_hash for value in dataset.factors
        ) + tuple(
            value.content_hash
            for values in (
                tuple(
                    value
                    for value in dataset.coverage.sessions
                    if request.start <= value.session_date <= request.end
                ),
                dataset.coverage.lifecycles,
                dataset.coverage.suspensions,
                dataset.coverage.price_limits,
            )
            for value in values
        )
        manifest = DatasetManifest(
            source=self._single_source(dataset),
            instruments=request.instruments,
            start_time=_start_time(request.start),
            end_time=_event_time(request.end),
            as_of=effective_as_of,
            record_hashes=record_hashes,
            quality_report_hash=report.report_hash,
            production_complete=(
                request.production_complete_requested and report.production_complete
            ),
            row_count=len(record_hashes),
        )
        try:
            async with self._control_repository.transaction() as transaction:
                await transaction.save_quality_report(report)
                await transaction.save_manifest(manifest)
                for instrument in request.instruments:
                    instrument_bars = tuple(
                        value for value in dataset.bars if value.instrument == instrument
                    )
                    if instrument_bars:
                        latest_bar = max(
                            instrument_bars,
                            key=lambda value: (value.event_time, value.content_hash),
                        )
                        await transaction.advance_checkpoint(
                            latest_bar.source,
                            "daily",
                            latest_bar.instrument,
                            latest_bar.event_time,
                            latest_bar.content_hash,
                        )
                    instrument_factors = tuple(
                        value for value in dataset.factors if value.instrument == instrument
                    )
                    if instrument_factors:
                        latest_factor = max(
                            instrument_factors,
                            key=lambda value: (value.event_time, value.content_hash),
                        )
                        await transaction.advance_checkpoint(
                            latest_factor.source,
                            "adj-factor",
                            latest_factor.instrument,
                            latest_factor.event_time,
                            latest_factor.content_hash,
                        )
                await transaction.append_audit_event(
                    "daily_ingestion_completed",
                    to_utc(self._now(), name="audit time"),
                    {
                        "manifest_hash": manifest.manifest_hash,
                        "quality_hash": report.report_hash,
                        "bar_count": len(dataset.bars),
                        "factor_count": len(dataset.factors),
                    },
                )
        except PersistenceUnavailableError:
            return self._result(
                "persistence_failed",
                dataset,
                persisted_bars,
                persisted_factors,
                quality_hash=report.report_hash,
            )
        return self._result(
            "completed",
            dataset,
            persisted_bars,
            persisted_factors,
            quality_hash=report.report_hash,
            manifest_hash=manifest.manifest_hash,
        )

    @staticmethod
    def _single_source(dataset: DailyDatasetBatch) -> str:
        sources = {value.source for value in dataset.source_evidence}
        if len(sources) != 1:
            raise ValueError("daily ingestion requires exactly one source")
        return next(iter(sources))

    @staticmethod
    def _result(
        status: str,
        dataset: DailyDatasetBatch,
        persisted_bars: int,
        persisted_factors: int,
        *,
        quality_hash: str | None = None,
        manifest_hash: str | None = None,
    ) -> DailyIngestionResult:
        return DailyIngestionResult(
            status=status,
            fetched_bars=len(dataset.bars),
            fetched_factors=len(dataset.factors),
            persisted_bars=persisted_bars,
            persisted_factors=persisted_factors,
            quality_hash=quality_hash,
            manifest_hash=manifest_hash,
        )


class ValidatedDailyDatasetReader:
    def __init__(
        self,
        *,
        control_repository: ControlRepository,
        market_repository: DailyMarketRepository,
    ) -> None:
        self._control_repository = control_repository
        self._market_repository = market_repository

    async def query(
        self, manifest_hash: str, as_of: datetime
    ) -> ValidatedDailyDataset:
        cutoff = to_utc(as_of, name="as_of")
        manifest = await self._control_repository.read_manifest(manifest_hash)
        if not manifest.production_complete:
            raise ValueError("research requires a production-complete manifest")
        report = await self._control_repository.read_quality_report(
            manifest.quality_report_hash
        )
        if not report.passed or not report.production_complete:
            raise ValueError("research requires a passing production-complete report")
        if cutoff > manifest.as_of:
            raise ValueError("as_of cannot exceed the manifest cutoff")
        start = to_shanghai(manifest.start_time).date()
        end = to_shanghai(manifest.end_time).date()
        bars = await self._market_repository.query_bars_as_of(
            manifest.instruments, start, end, cutoff
        )
        factors = await self._market_repository.query_factors_as_of(
            manifest.instruments, start, end, cutoff
        )
        coverage = await self._market_repository.query_coverage_as_of(
            manifest.instruments, start, end, cutoff
        )
        bar_keys = {
            (value.instrument, value.session_date)
            for value in bars
        }
        factors = tuple(
            value
            for value in factors
            if (value.instrument, value.session_date) in bar_keys
        )
        groups = (
            ("bars", tuple(value.content_hash for value in bars)),
            (
                "factors",
                tuple(value.content_hash for value in factors),
            ),
            (
                "sessions",
                tuple(
                    value.content_hash
                    for value in coverage.sessions
                ),
            ),
            (
                "lifecycles",
                tuple(
                    value.content_hash
                    for value in coverage.lifecycles
                ),
            ),
            (
                "suspensions",
                tuple(
                    value.content_hash
                    for value in coverage.suspensions
                ),
            ),
            (
                "price_limits",
                tuple(
                    value.content_hash
                    for value in coverage.price_limits
                ),
            ),
        )
        hashes = tuple(
            value
            for _, values in groups
            for value in values
        )
        if hashes != manifest.record_hashes:
            raise ManifestIntegrityError(
                _daily_manifest_mismatch(
                    expected=manifest.record_hashes,
                    actual=hashes,
                    groups=groups,
                    actual_identities={
                        value.content_hash: (
                            f"bar:{value.instrument}:"
                            f"{value.session_date.isoformat()}"
                        )
                        for value in bars
                    }
                    | {
                        value.content_hash: (
                            f"factor:{value.instrument}:"
                            f"{value.session_date.isoformat()}"
                        )
                        for value in factors
                    }
                    | {
                        value.content_hash: (
                            f"session:{value.session_date.isoformat()}"
                        )
                        for value in coverage.sessions
                    }
                    | {
                        value.content_hash: (
                            f"lifecycle:{value.instrument}"
                        )
                        for value in coverage.lifecycles
                    }
                    | {
                        value.content_hash: (
                            f"suspension:{value.instrument}:"
                            f"{value.session_date.isoformat()}"
                        )
                        for value in coverage.suspensions
                    }
                    | {
                        value.content_hash: (
                            f"price_limit:{value.instrument}:"
                            f"{value.session_date.isoformat()}"
                        )
                        for value in coverage.price_limits
                    },
                )
            )
        return ValidatedDailyDataset(bars=bars, factors=factors, coverage=coverage)


def _start_time(session_date: date) -> datetime:
    return to_utc(datetime.combine(session_date, time.min, tzinfo=SHANGHAI))


def _daily_manifest_mismatch(
    *,
    expected: tuple[str, ...],
    actual: tuple[str, ...],
    groups: tuple[tuple[str, tuple[str, ...]], ...],
    actual_identities: dict[str, str],
) -> str:
    mismatch = next(
        (
            index
            for index in range(min(len(expected), len(actual)))
            if expected[index] != actual[index]
        ),
        min(len(expected), len(actual)),
    )
    counts = ",".join(
        f"{name}:{len(values)}"
        for name, values in groups
    )
    expected_set = set(expected)
    actual_set = set(actual)
    missing = sorted(expected_set - actual_set)
    extra = sorted(actual_set - expected_set)
    missing_text = ",".join(value[:12] for value in missing[:3])
    extra_text = ",".join(
        f"{actual_identities.get(value, 'unknown')}:{value[:12]}"
        for value in extra[:3]
    )
    return (
        "validated daily rows do not match the manifest "
        f"(first_mismatch={mismatch},expected_total={len(expected)},"
        f"actual_total={len(actual)},actual_counts={counts},"
        f"missing={missing_text or 'none'},"
        f"extra={extra_text or 'none'})"
    )


def _event_time(session_date: date) -> datetime:
    return to_utc(datetime.combine(session_date, time(15, 0), tzinfo=SHANGHAI))

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest

from autoquant.data.daily_ingestion import (
    ExactDailyRecordBatch,
    ValidatedDailyDataset,
)
from autoquant.data.daily_models import (
    AdjustmentFactorRevision,
    DailyBarRevision,
    DailyCoverageEvidence,
    DailyPriceLimit,
    DailySuspensionStatus,
    InstrumentLifecycle,
    TradingSession,
)
from autoquant.data.models import DatasetManifest
from autoquant.data.quality import QualityReport
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.data.research_input import (
    ExactManifestResearchDatasetReader,
    ResearchUniverseBinding,
    ValidatedResearchDatasetReader,
    compile_research_input_plan,
)
from autoquant.errors import (
    ManifestIntegrityError,
    PersistenceUnavailableError,
)


def _manifest(
    shard_hashes: tuple[str, str] = ("f" * 64, "0" * 64),
) -> ResearchDatasetManifest:
    return ResearchDatasetManifest(
        campaign_hash="a" * 64,
        policy_hash="b" * 64,
        snapshot_hashes=("c" * 64, "d" * 64, "e" * 64),
        start_date=date(2020, 1, 1),
        end_date=date(2020, 3, 31),
        shards=(
            ResearchDatasetShard(
                1,
                "000001.XSHE",
                shard_hashes[0],
            ),
            ResearchDatasetShard(
                2,
                "600000.XSHG",
                shard_hashes[1],
            ),
        ),
    )


def _universes() -> tuple[ResearchUniverseBinding, ...]:
    return (
        ResearchUniverseBinding(
            sequence=1,
            snapshot_hash="c" * 64,
            policy_hash="b" * 64,
            reference_date=date(2020, 1, 31),
            knowledge_as_of=datetime(2026, 7, 23, tzinfo=UTC),
            members=("000001.XSHE",),
        ),
        ResearchUniverseBinding(
            sequence=2,
            snapshot_hash="d" * 64,
            policy_hash="b" * 64,
            reference_date=date(2020, 2, 29),
            knowledge_as_of=datetime(2026, 7, 23, tzinfo=UTC),
            members=("000001.XSHE", "600000.XSHG"),
        ),
        ResearchUniverseBinding(
            sequence=3,
            snapshot_hash="e" * 64,
            policy_hash="b" * 64,
            reference_date=date(2020, 3, 31),
            knowledge_as_of=datetime(2026, 7, 23, tzinfo=UTC),
            members=("600000.XSHG",),
        ),
    )


def test_plan_binds_shards_and_activates_snapshots_strictly_after_cutoff() -> None:
    manifest = _manifest()

    plan = compile_research_input_plan(
        manifest=manifest,
        universes=_universes(),
    )

    assert plan.dataset_manifest_hash == manifest.manifest_hash
    assert plan.members_for(date(2020, 1, 31)) == ()
    assert plan.members_for(date(2020, 2, 1)) == ("000001.XSHE",)
    assert plan.members_for(date(2020, 2, 29)) == ("000001.XSHE",)
    assert plan.members_for(date(2020, 3, 1)) == (
        "000001.XSHE",
        "600000.XSHG",
    )
    assert plan.shard_manifest_for("600000.XSHG") == "0" * 64
    assert len(plan.plan_hash) == 64
    assert (
        compile_research_input_plan(
            manifest=manifest,
            universes=_universes(),
        ).plan_hash
        == plan.plan_hash
    )


def test_plan_rejects_a_missing_calendar_month() -> None:
    with pytest.raises(ValueError, match="bindings do not match"):
        compile_research_input_plan(
            manifest=_manifest(),
            universes=(_universes()[0], _universes()[2]),
        )


def test_plan_rejects_policy_drift() -> None:
    values = list(_universes())
    values[1] = ResearchUniverseBinding(
        sequence=2,
        snapshot_hash="d" * 64,
        policy_hash="9" * 64,
        reference_date=date(2020, 2, 29),
        knowledge_as_of=datetime(2026, 7, 23, tzinfo=UTC),
        members=("000001.XSHE", "600000.XSHG"),
    )

    with pytest.raises(ValueError, match="manifest policy"):
        compile_research_input_plan(
            manifest=_manifest(),
            universes=tuple(values),
        )


def test_plan_requires_universe_union_to_equal_shards() -> None:
    values = tuple(
        ResearchUniverseBinding(
            sequence=value.sequence,
            snapshot_hash=value.snapshot_hash,
            policy_hash=value.policy_hash,
            reference_date=value.reference_date,
            knowledge_as_of=value.knowledge_as_of,
            members=("000001.XSHE",),
        )
        for value in _universes()
    )

    with pytest.raises(ValueError, match="union"):
        compile_research_input_plan(
            manifest=_manifest(),
            universes=values,
        )


def test_plan_rejects_out_of_bounds_session_lookup() -> None:
    plan = compile_research_input_plan(
        manifest=_manifest(),
        universes=_universes(),
    )

    with pytest.raises(ValueError, match="outside plan bounds"):
        plan.members_for(date(2019, 12, 31))
    with pytest.raises(LookupError, match="not bound"):
        plan.shard_manifest_for("000002.XSHE")


def _daily_manifest(instrument: str) -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=(instrument,),
        start_time=datetime(2019, 12, 31, 16, tzinfo=UTC),
        end_time=datetime(2020, 3, 31, 7, tzinfo=UTC),
        as_of=datetime(2026, 7, 23, tzinfo=UTC),
        record_hashes=(),
        quality_report_hash="quality-report",
        production_complete=True,
        row_count=0,
    )


class _ManifestReader:
    def __init__(self, values: tuple[DatasetManifest, ...]) -> None:
        self.values = {value.manifest_hash: value for value in values}

    async def read_manifest(
        self,
        manifest_hash: str,
    ) -> DatasetManifest:
        return self.values[manifest_hash]


class _DatasetReader:
    def __init__(self) -> None:
        self.dataset = cast(ValidatedDailyDataset, object())
        self.calls: list[tuple[str, datetime]] = []

    async def query(
        self,
        manifest_hash: str,
        as_of: datetime,
    ) -> ValidatedDailyDataset:
        self.calls.append((manifest_hash, as_of))
        return self.dataset


@pytest.mark.asyncio
async def test_validated_reader_lazily_reads_only_active_members() -> None:
    first = _daily_manifest("000001.XSHE")
    second = _daily_manifest("600000.XSHG")
    plan = compile_research_input_plan(
        manifest=_manifest((first.manifest_hash, second.manifest_hash)),
        universes=_universes(),
    )
    datasets = _DatasetReader()
    reader = ValidatedResearchDatasetReader(
        plan=plan,
        manifest_reader=_ManifestReader((first, second)),
        dataset_reader=datasets,
    )

    values = [value async for value in reader.iter_members(date(2020, 2, 1))]

    assert [value.instrument for value in values] == ["000001.XSHE"]
    assert values[0].dataset is datasets.dataset
    assert datasets.calls == [(first.manifest_hash, first.as_of)]


@pytest.mark.asyncio
async def test_validated_reader_fails_closed_on_shard_metadata_drift() -> None:
    first = _daily_manifest("000001.XSHE")
    second = _daily_manifest("600000.XSHG")
    plan = compile_research_input_plan(
        manifest=_manifest((first.manifest_hash, second.manifest_hash)),
        universes=_universes(),
    )
    manifests = _ManifestReader((first, second))
    manifests.values[first.manifest_hash] = second
    reader = ValidatedResearchDatasetReader(
        plan=plan,
        manifest_reader=manifests,
        dataset_reader=_DatasetReader(),
    )

    with pytest.raises(ValueError, match="does not match"):
        await reader.query_instrument("000001.XSHE")


class _TransientDatasetReader(_DatasetReader):
    def __init__(
        self,
        *,
        error: PersistenceUnavailableError,
    ) -> None:
        super().__init__()
        self.error = error
        self.attempts = 0

    async def query(
        self,
        manifest_hash: str,
        as_of: datetime,
    ) -> ValidatedDailyDataset:
        self.attempts += 1
        if self.attempts == 1:
            raise self.error
        return await super().query(manifest_hash, as_of)


@pytest.mark.asyncio
async def test_validated_reader_retries_only_transient_persistence() -> None:
    first = _daily_manifest("000001.XSHE")
    second = _daily_manifest("600000.XSHG")
    plan = compile_research_input_plan(
        manifest=_manifest((first.manifest_hash, second.manifest_hash)),
        universes=_universes(),
    )
    datasets = _TransientDatasetReader(
        error=PersistenceUnavailableError("temporary"),
    )
    reader = ValidatedResearchDatasetReader(
        plan=plan,
        manifest_reader=_ManifestReader((first, second)),
        dataset_reader=datasets,
        retry_delay_seconds=0,
    )

    result = await reader.query_instrument("000001.XSHE")

    assert result.instrument == "000001.XSHE"
    assert datasets.attempts == 2


@pytest.mark.asyncio
async def test_validated_reader_never_retries_manifest_integrity() -> None:
    first = _daily_manifest("000001.XSHE")
    second = _daily_manifest("600000.XSHG")
    plan = compile_research_input_plan(
        manifest=_manifest((first.manifest_hash, second.manifest_hash)),
        universes=_universes(),
    )
    datasets = _TransientDatasetReader(
        error=ManifestIntegrityError("deterministic"),
    )
    reader = ValidatedResearchDatasetReader(
        plan=plan,
        manifest_reader=_ManifestReader((first, second)),
        dataset_reader=datasets,
        retry_delay_seconds=0,
    )

    with pytest.raises(
        ManifestIntegrityError,
        match="row-hash verification",
    ):
        await reader.query_instrument("000001.XSHE")
    assert datasets.attempts == 1


def _exact_rows(
    instruments: tuple[str, ...],
) -> ValidatedDailyDataset:
    available_at = datetime(2020, 4, 1, 8, tzinfo=UTC)
    sessions = tuple(
        TradingSession(
            "tushare",
            date(2020, 1, 1) + timedelta(days=offset),
            (date(2020, 1, 1) + timedelta(days=offset)).weekday() < 5,
            available_at,
            f"{offset + 1:064x}",
        )
        for offset in range(91)
    )
    bars = tuple(
        DailyBarRevision.from_values(
            source="tushare",
            instrument=instrument,
            session_date=date(2020, 2, 3),
            event_time=datetime(2020, 2, 3, 7, tzinfo=UTC),
            available_at=available_at,
            ingested_at=available_at,
            source_revision=f"daily-{instrument}",
            availability_policy="tushare-daily-v1",
            evidence_hash="2" * 64,
            open_price="10",
            high_price="11",
            low_price="9",
            close_price="10",
            pre_close="10",
            volume=1000,
            turnover="10000",
        )
        for instrument in instruments
    )
    factors = tuple(
        AdjustmentFactorRevision.from_values(
            source="tushare",
            instrument=instrument,
            session_date=date(2020, 2, 3),
            event_time=datetime(2020, 2, 3, 7, tzinfo=UTC),
            available_at=available_at,
            ingested_at=available_at,
            source_revision=f"factor-{instrument}",
            availability_policy="tushare-daily-v1",
            evidence_hash="3" * 64,
            factor="1",
        )
        for instrument in instruments
    )
    return ValidatedDailyDataset(
        bars=bars,
        factors=factors,
        coverage=DailyCoverageEvidence(
            sessions=sessions,
            lifecycles=tuple(
                InstrumentLifecycle(
                    "tushare",
                    instrument,
                    date(1990, 1, 1),
                    None,
                    available_at,
                    "4" * 64,
                )
                for instrument in instruments
            ),
            suspensions=tuple(
                DailySuspensionStatus(
                    "tushare",
                    instrument,
                    date(2020, 2, 3),
                    False,
                    available_at,
                    "5" * 64,
                )
                for instrument in instruments
            ),
            price_limits=tuple(
                DailyPriceLimit(
                    "tushare",
                    instrument,
                    date(2020, 2, 3),
                    Decimal("10"),
                    Decimal("11"),
                    Decimal("9"),
                    available_at,
                    "6" * 64,
                )
                for instrument in instruments
            ),
        ),
    )


def _exact_manifest(
    instrument: str,
    dataset: ValidatedDailyDataset,
    report: QualityReport,
) -> DatasetManifest:
    return DatasetManifest(
        source="tushare",
        instruments=(instrument,),
        start_time=datetime(2019, 12, 31, 16, tzinfo=UTC),
        end_time=datetime(2020, 3, 31, 7, tzinfo=UTC),
        as_of=datetime(2026, 7, 23, tzinfo=UTC),
        record_hashes=(
            next(value.content_hash for value in dataset.bars if value.instrument == instrument),
            next(value.content_hash for value in dataset.factors if value.instrument == instrument),
            *tuple(value.content_hash for value in dataset.coverage.sessions),
            next(
                value.content_hash
                for value in dataset.coverage.lifecycles
                if value.instrument == instrument
            ),
            next(
                value.content_hash
                for value in dataset.coverage.suspensions
                if value.instrument == instrument
            ),
            next(
                value.content_hash
                for value in dataset.coverage.price_limits
                if value.instrument == instrument
            ),
        ),
        quality_report_hash=report.report_hash,
        production_complete=True,
        row_count=96,
    )


class _ExactControl(_ManifestReader):
    def __init__(
        self,
        values: tuple[DatasetManifest, ...],
        report: QualityReport,
    ) -> None:
        super().__init__(values)
        self.report = report

    async def read_quality_report(
        self,
        report_hash: str,
    ) -> QualityReport:
        assert report_hash == self.report.report_hash
        return self.report


class _ExactRecordReader:
    def __init__(self, dataset: ValidatedDailyDataset) -> None:
        self.dataset = dataset
        self.calls: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

    async def query_exact_records(
        self,
        *,
        instruments: tuple[str, ...],
        start: date,
        end: date,
        record_hash_groups: tuple[tuple[str, ...], ...],
    ) -> ExactDailyRecordBatch:
        assert start == date(2020, 1, 1)
        assert end == date(2020, 3, 31)
        record_hashes = tuple(
            sorted({content_hash for group in record_hash_groups for content_hash in group})
        )
        self.calls.append((instruments, record_hashes))
        return ExactDailyRecordBatch(
            bars=self.dataset.bars,
            factors=self.dataset.factors,
            sessions=self.dataset.coverage.sessions,
            lifecycles=self.dataset.coverage.lifecycles,
            suspensions=self.dataset.coverage.suspensions,
            price_limits=self.dataset.coverage.price_limits,
        )


@pytest.mark.asyncio
async def test_exact_manifest_reader_batches_and_verifies_hash_rows() -> None:
    instruments = ("000001.XSHE", "600000.XSHG")
    dataset = _exact_rows(instruments)
    report = QualityReport(
        requested_instruments=instruments,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2020, 3, 31, tzinfo=UTC),
        as_of=datetime(2026, 7, 23, tzinfo=UTC),
        issues=(),
        production_complete=True,
    )
    manifests = tuple(_exact_manifest(instrument, dataset, report) for instrument in instruments)
    plan = compile_research_input_plan(
        manifest=_manifest(
            (
                manifests[0].manifest_hash,
                manifests[1].manifest_hash,
            )
        ),
        universes=_universes(),
    )
    records = _ExactRecordReader(dataset)
    reader = ExactManifestResearchDatasetReader(
        plan=plan,
        control_reader=_ExactControl(manifests, report),
        record_reader=records,
        batch_size=2,
    )

    shards = [value async for value in reader.iter_all()]

    assert tuple(value.instrument for value in shards) == instruments
    assert all(value.dataset.bars[0].instrument == value.instrument for value in shards)
    assert len(records.calls) == 1
    assert records.calls[0][0] == instruments
    assert len(records.calls[0][1]) == 101


@pytest.mark.asyncio
async def test_exact_manifest_reader_rejects_a_missing_hash_row() -> None:
    instruments = ("000001.XSHE", "600000.XSHG")
    complete = _exact_rows(instruments)
    report = QualityReport(
        requested_instruments=instruments,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2020, 3, 31, tzinfo=UTC),
        as_of=datetime(2026, 7, 23, tzinfo=UTC),
        issues=(),
        production_complete=True,
    )
    manifests = tuple(_exact_manifest(instrument, complete, report) for instrument in instruments)
    incomplete = ValidatedDailyDataset(
        bars=complete.bars[1:],
        factors=complete.factors,
        coverage=complete.coverage,
    )
    plan = compile_research_input_plan(
        manifest=_manifest(
            (
                manifests[0].manifest_hash,
                manifests[1].manifest_hash,
            )
        ),
        universes=_universes(),
    )
    reader = ExactManifestResearchDatasetReader(
        plan=plan,
        control_reader=_ExactControl(manifests, report),
        record_reader=_ExactRecordReader(incomplete),
        batch_size=2,
    )

    with pytest.raises(
        ManifestIntegrityError,
        match="do not match",
    ):
        _ = [value async for value in reader.iter_all()]

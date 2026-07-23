from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast

import pytest

from autoquant.data.daily_ingestion import ValidatedDailyDataset
from autoquant.data.models import DatasetManifest
from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.data.research_input import (
    ResearchUniverseBinding,
    ValidatedResearchDatasetReader,
    compile_research_input_plan,
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
        self.values = {
            value.manifest_hash: value
            for value in values
        }

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
        manifest=_manifest(
            (first.manifest_hash, second.manifest_hash)
        ),
        universes=_universes(),
    )
    datasets = _DatasetReader()
    reader = ValidatedResearchDatasetReader(
        plan=plan,
        manifest_reader=_ManifestReader((first, second)),
        dataset_reader=datasets,
    )

    values = [
        value
        async for value in reader.iter_members(date(2020, 2, 1))
    ]

    assert [value.instrument for value in values] == [
        "000001.XSHE"
    ]
    assert values[0].dataset is datasets.dataset
    assert datasets.calls == [
        (first.manifest_hash, first.as_of)
    ]


@pytest.mark.asyncio
async def test_validated_reader_fails_closed_on_shard_metadata_drift() -> None:
    first = _daily_manifest("000001.XSHE")
    second = _daily_manifest("600000.XSHG")
    plan = compile_research_input_plan(
        manifest=_manifest(
            (first.manifest_hash, second.manifest_hash)
        ),
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

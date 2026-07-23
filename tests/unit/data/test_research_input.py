from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from autoquant.data.research_data_campaign import (
    ResearchDatasetManifest,
    ResearchDatasetShard,
)
from autoquant.data.research_input import (
    ResearchUniverseBinding,
    compile_research_input_plan,
)


def _manifest() -> ResearchDatasetManifest:
    return ResearchDatasetManifest(
        campaign_hash="a" * 64,
        policy_hash="b" * 64,
        snapshot_hashes=("c" * 64, "d" * 64, "e" * 64),
        start_date=date(2020, 1, 1),
        end_date=date(2020, 3, 31),
        shards=(
            ResearchDatasetShard(1, "000001.XSHE", "f" * 64),
            ResearchDatasetShard(2, "600000.XSHG", "0" * 64),
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

from __future__ import annotations

from datetime import date

import pytest

from autoquant.data.research_data_campaign import (
    ResearchDataCampaignSpec,
    ResearchDatasetManifest,
    ResearchDatasetShard,
)

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64


def campaign_spec() -> ResearchDataCampaignSpec:
    return ResearchDataCampaignSpec(
        campaign_key="csi300-history-2020-v1",
        policy_hash=HASH_A,
        snapshot_hashes=(HASH_B, HASH_C),
        instruments=("600000.XSHG", "000001.XSHE"),
        start_date=date(2020, 1, 1),
        end_date=date(2026, 7, 22),
        requested_by="operator",
    )


def test_campaign_spec_is_canonical_and_round_trips() -> None:
    value = campaign_spec()

    restored = ResearchDataCampaignSpec.from_payload(value.payload())

    assert restored == value
    assert restored.instruments == ("000001.XSHE", "600000.XSHG")
    assert restored.campaign_hash == value.campaign_hash


@pytest.mark.parametrize(
    "change",
    [
        {"snapshot_hashes": ()},
        {"instruments": ("000001.XSHE", "000001.XSHE")},
        {"max_attempts": 0},
        {"end_date": date(2019, 12, 31)},
    ],
)
def test_campaign_spec_rejects_unsafe_plans(change: dict[str, object]) -> None:
    values = {
        "campaign_key": "csi300-history-2020-v1",
        "policy_hash": HASH_A,
        "snapshot_hashes": (HASH_B, HASH_C),
        "instruments": ("000001.XSHE", "600000.XSHG"),
        "start_date": date(2020, 1, 1),
        "end_date": date(2026, 7, 22),
        "requested_by": "operator",
        "max_attempts": 3,
    }
    values.update(change)

    with pytest.raises(ValueError):
        ResearchDataCampaignSpec(**values)  # type: ignore[arg-type]


def test_research_dataset_manifest_binds_snapshots_and_shards() -> None:
    spec = campaign_spec()
    value = ResearchDatasetManifest(
        campaign_hash=spec.campaign_hash,
        policy_hash=spec.policy_hash,
        snapshot_hashes=spec.snapshot_hashes,
        start_date=spec.start_date,
        end_date=spec.end_date,
        shards=(
            ResearchDatasetShard(1, "000001.XSHE", HASH_C),
            ResearchDatasetShard(2, "600000.XSHG", HASH_D),
        ),
    )

    restored = ResearchDatasetManifest.from_payload(value.payload())

    assert restored == value
    assert restored.instruments == spec.instruments
    assert restored.manifest_hash == value.manifest_hash


def test_research_dataset_manifest_rejects_noncanonical_shard_order() -> None:
    spec = campaign_spec()

    with pytest.raises(ValueError, match="instrument-sorted"):
        ResearchDatasetManifest(
            campaign_hash=spec.campaign_hash,
            policy_hash=spec.policy_hash,
            snapshot_hashes=spec.snapshot_hashes,
            start_date=spec.start_date,
            end_date=spec.end_date,
            shards=(
                ResearchDatasetShard(1, "600000.XSHG", HASH_C),
                ResearchDatasetShard(2, "000001.XSHE", HASH_D),
            ),
        )
